import json
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, BinaryIO, Dict, Generator, Optional, Tuple

import pandas as pd
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage
from google.resumable_media.requests import ResumableUpload


@dataclass
class StorageConfig:
    allowed_origin: str = "http://localhost:5000"
    timeout: int = 3600
    chunk_size: int = 5 * 1024 * 1024  # 5MB chunks
    max_retries: int = 3
    retry_delay: int = 1  # seconds
    cors_headers: list = None
    
    def __post_init__(self):
        if self.cors_headers is None:
            self.cors_headers = ["Content-Type", "x-goog-resumable"]

class StorageService:
    def __init__(self, bucket: str, logger=None):
        
        self.config = StorageConfig()
        self._client = storage.Client()
        self._bucket = self._client.bucket(bucket)
        self._blob = self._bucket.blob(bucket) if bucket else None
        self._session = AuthorizedSession(self._client._credentials)
        self.logger = logger

    def configure_cors(self) -> bool:
        """Configure CORS for bucket"""
        try:
            self._bucket.cors = [{
                "origin": ["*"],
                "responseHeader": self.config.cors_headers,
                "method": ["PUT", "POST", "OPTIONS"],
                "maxAgeSeconds": self.config.timeout
            }]
            self._bucket.patch()
            return True
        except Exception as e:
            if self.logger:
                self.logger.error(f"CORS configuration failed: {str(e)}")
            raise

    def create_upload_session(self) -> ResumableUpload:
        """Create resumable upload session"""
        upload_url = (
            f"https://storage.googleapis.com/upload/storage/v1/b/"
            f"{self._bucket}/o?uploadType=resumable"
        )
        return ResumableUpload(upload_url=upload_url, chunk_size=self.config.chunk_size)


    def generate_upload_url(
        self, 
        filename: str, 
        content_type: str,
        origin: Optional[str] = None
    ) -> str:
        """Create resumable upload session"""
        try:
            blob = self._bucket.blob(filename)
            return blob.create_resumable_upload_session(
                content_type=content_type,
                origin=origin or self.config.allowed_origin,
                timeout=self.config.timeout
            )
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to generate upload URL: {str(e)}")
            raise

    def initiate_upload(
        self,
        upload: ResumableUpload,
        filename: str,
        stream: Optional[BinaryIO] = None,
        content_type: str = "text/csv"
    ):
        """Initialize upload session"""
        upload.initiate(
            transport=self._session,
            stream=stream,
            stream_final=False,
            metadata={"name": filename, "bucket": self._bucket},
            content_type=content_type,
            timeout=self.config.timeout,
        )

    def upload_chunk(
        self,
        upload: ResumableUpload,
        chunk_data: bytes,
        retry_count: int = 0
    ) -> bool:
        """Upload chunk with retry logic"""
        try:
            response = upload.transmit_next_chunk(self._session, chunk_data)
            return response is not None and response.status_code in [200, 201]
        except Exception as e:
            if retry_count < self.config.max_retries:
                time.sleep(self.config.retry_delay * (2**retry_count))
                return self.upload_chunk(upload, chunk_data, retry_count + 1)
            raise

    def read_chunks(self, source_path: str) -> Generator[pd.DataFrame, None, None]:
        """Read file in chunks"""
        try:
            for chunk in pd.read_csv(source_path, chunksize=self.config.chunk_size):
                yield chunk
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error reading {source_path}: {str(e)}")
            raise

    def save_metadata(self, filename: str, metadata: Dict[str, Any]) -> Tuple[bool, str]:
        """Save metadata to bucket"""
        try:
            metadata["created"] = datetime.now().isoformat()
            metadata_filename = f"{filename}.metadata.json"
            blob = self._bucket.blob(metadata_filename)
            
            blob.upload_from_string(
                data=json.dumps(metadata, indent=2),
                content_type="application/json"
            )
            
            return True, metadata_filename
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error storing metadata: {str(e)}")
            return False, str(e)