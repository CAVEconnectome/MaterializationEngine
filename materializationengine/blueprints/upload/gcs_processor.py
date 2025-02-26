from google.cloud import storage
from google.auth.transport.requests import AuthorizedSession
from google.resumable_media.requests import ResumableUpload
import pandas as pd
import io
from typing import Iterator, Optional, Callable
from dataclasses import dataclass
import time

@dataclass
class UploadConfig:
    """Configuration for GCS uploads"""
    chunk_size: int = 5 * 1024 * 1024  # 5MB chunks
    timeout: int = 3600  # 1 hour
    max_retries: int = 3
    retry_delay: int = 1  # seconds

class GCSCsvProcessor:
    def __init__(self, bucket_name: str, chunk_size: int = 10000):
        """
        Initialize the GCS CSV processor.

        Args:
            bucket_name: Name of the Google Cloud Storage bucket
            chunk_size: Number of rows to process at once for CSV reading
        """
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.chunk_size = chunk_size
        self.upload_config = UploadConfig()
        self._session = AuthorizedSession(self.client._credentials)

    def _create_upload_session(self) -> ResumableUpload:
            """
            Create a new resumable upload session.

            Returns:
                ResumableUpload object
            """
            upload_url = (
                f"https://storage.googleapis.com/upload/storage/v1/b/"
                f"{self.bucket.name}/o?uploadType=resumable"
            )

            return ResumableUpload(
                upload_url=upload_url,
                chunk_size=self.upload_config.chunk_size
            )


    def _initiate_upload(
        self,
        upload: ResumableUpload,
        blob_name: str
    ):
        """
        Initialize the upload session.

        Args:
            upload: ResumableUpload object
            blob_name: Name to give the blob
        """
        empty_stream = io.BytesIO()
        upload.initiate(
            transport=self._session,
            stream=empty_stream,
            metadata={"name": blob_name},
            content_type="text/csv",
            total_bytes=None,
            timeout=self.upload_config.timeout
        )

    def _upload_chunk(
        self,
        upload_session: ResumableUpload,
        chunk_data: bytes
    ):
        """
        Upload a chunk of processed CSV data to GCS.

        Args:
            upload_session: The resumable upload session
            chunk_data: Byte-encoded CSV chunk to upload
        """
        retry_count = 0
        buffer = io.BytesIO(chunk_data)

        while retry_count <= self.upload_config.max_retries:
            try:
                buffer.seek(0)
                upload_session.transmit_next_chunk(
                    transport=self._session,
                    timeout=self.upload_config.timeout
                )
                return
            except Exception as e:
                retry_count += 1
                if retry_count > self.upload_config.max_retries:
                    raise ValueError(f"Failed to upload chunk after {retry_count} retries: {str(e)}")
                time.sleep(self.upload_config.retry_delay)

    def _get_csv_row_count(self, blob: storage.Blob) -> int:
        """
        Get the total number of rows in a CSV file stored in GCS.

        Args:
            blob: The GCS blob representing the CSV file.

        Returns:
            int: Total number of rows in the file (excluding the header).
        """
        with blob.open("r") as blob_stream:
            return sum(1 for _ in blob_stream) - 1

    def process_csv_in_chunks(
        self,
        source_blob_name: str,
        destination_blob_name: str,
        transform_function: Callable[[pd.DataFrame], pd.DataFrame],
        chunk_upload_callback: Optional[Callable[[float], None]] = None
    ) -> None:
        """
        Process a CSV file from GCS in chunks and track progress using row count.

        Args:
            source_blob_name: Name of the source CSV in GCS
            destination_blob_name: Name for the processed CSV in GCS
            transform_function: Function to apply to each chunk of data
            chunk_upload_callback: Optional callback function to track upload progress
        """
        source_blob = self.bucket.blob(source_blob_name)
        destination_blob = self.bucket.blob(destination_blob_name)

        total_rows = self._get_csv_row_count(source_blob)
        total_processed_rows = 0

        try:
            with destination_blob.open("wb") as output_stream:
                for idx, chunk in enumerate(self._download_in_chunks(source_blob)):
                    processed_chunk = transform_function(chunk)

                    csv_buffer = io.StringIO()
                    processed_chunk.to_csv(csv_buffer, index=False, header=False)
                    chunk_bytes = csv_buffer.getvalue().encode("utf-8")

                    output_stream.write(chunk_bytes)

                    total_processed_rows += len(processed_chunk)

                    if total_rows and chunk_upload_callback:
                        progress = min((total_processed_rows / total_rows) * 100, 100)
                        chunk_upload_callback(progress)
                        print(f"Upload formatted progress: {progress:.2f}% ({total_processed_rows}/{total_rows} rows)")

            if chunk_upload_callback:
                chunk_upload_callback(100)
                print("Upload completed at 100% (All rows processed)")

        except Exception as e:
            raise ValueError(f"Failed to process and upload file: {str(e)}")

           
    def _download_in_chunks(self, blob: storage.Blob) -> Iterator[pd.DataFrame]:
        """
        Stream-download CSV from GCS in chunks.

        Args:
            blob: GCS blob object

        Yields:
            DataFrame chunks
        """
        with blob.open("r") as blob_stream:
            for chunk in pd.read_csv(blob_stream, chunksize=self.chunk_size):
                yield chunk