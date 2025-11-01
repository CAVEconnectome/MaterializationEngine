import io
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterator, Optional

import pandas as pd
from google.cloud import storage

# @dataclass
# class UploadConfig:
#     """Configuration for GCS uploads"""
#     chunk_size: int = 5 * 1024 * 1024  # 5MB chunks
#     timeout: int = 3600  # 1 hour
#     max_retries: int = 3
#     retry_delay: int = 1  # seconds


class GCSCsvProcessor:
    def __init__(self, bucket_name: str, chunk_size: int = 10000):
        """
        Initialize the GCS CSV processor.

        Args:
            bucket_name: Name of the Google Cloud Storage bucket.
            chunk_size: Number of rows to process at once for CSV reading (pandas chunksize).
        """
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.pandas_chunk_size = (
            chunk_size 
        )

    def _get_csv_row_count(self, blob: storage.Blob) -> int:
        """
        Get the total number of rows in a CSV file stored in GCS.

        Args:
            blob: The GCS blob representing the CSV file.

        Returns:
            int: Total number of data rows in the file (excluding the header).
        """
        count = 0
        with blob.open("rt", encoding="utf-8", errors="replace") as blob_stream:
            next(blob_stream, None)
            for _ in blob_stream:
                count += 1
        return count

    def process_csv_in_chunks(
        self,
        source_blob_name: str,
        destination_blob_name: str,
        transform_function: Callable[[pd.DataFrame], pd.DataFrame],
        chunk_upload_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        """
        Process a CSV file from GCS in chunks and upload the transformed data.
        Uses blob.open('wb') for efficient and resumable uploads.

        Args:
            source_blob_name: Name of the source CSV in GCS.
            destination_blob_name: Name for the processed CSV in GCS.
            transform_function: Function to apply to each chunk of data.
            chunk_upload_callback: Optional callback. Called with a dict:
                                   {'progress': float, 'processed_rows': int,
                                    'total_rows': int, 'current_chunk_num': int, 'total_chunks': int}
        """
        source_blob = self.bucket.blob(source_blob_name)

        if not source_blob.exists():
            raise FileNotFoundError(
                f"Source blob '{source_blob_name}' not found in bucket '{self.bucket.name}'."
            )

        total_rows = self._get_csv_row_count(source_blob)
        total_processed_rows = 0

        estimated_total_chunks = 0
        if self.pandas_chunk_size > 0 and total_rows > 0:
            estimated_total_chunks = (
                total_rows + self.pandas_chunk_size - 1
            ) // self.pandas_chunk_size
        elif total_rows == 0:
            estimated_total_chunks = (
                1  
            )

        current_chunk_num = 0

        destination_blob = self.bucket.blob(destination_blob_name)

        try:
            with destination_blob.open(
                "wb", content_type="text/csv", ignore_flush=True
            ) as gcs_writer_stream:
                for chunk_df in self._download_in_chunks(source_blob):
                    current_chunk_num += 1
                    processed_chunk_df = transform_function(chunk_df)

                    with io.StringIO() as csv_buffer:
    
                        processed_chunk_df.to_csv(
                            csv_buffer, index=False, header=False
                        )
                       

                        chunk_bytes = csv_buffer.getvalue().encode("utf-8")

                    if chunk_bytes: 
                        gcs_writer_stream.write(chunk_bytes)

                    processed_rows_in_chunk = len(processed_chunk_df)
                    total_processed_rows += processed_rows_in_chunk

                    progress_percentage = 0.0
                    if total_rows > 0:
                        progress_percentage = min(
                            (total_processed_rows / total_rows) * 100, 100.0
                        )
                    elif total_rows == 0 and current_chunk_num == estimated_total_chunks:
                        progress_percentage = 100.0

                    if chunk_upload_callback:
                        progress_details = {
                            "progress": progress_percentage,
                            "processed_rows": total_processed_rows,
                            "total_rows": total_rows,
                            "current_chunk_num": current_chunk_num,
                            "total_chunks": estimated_total_chunks,
                        }
                        chunk_upload_callback(progress_details)

            if chunk_upload_callback:
                final_progress_details = {
                    "progress": 100.0,
                    "processed_rows": total_processed_rows,
                    "total_rows": total_rows,
                    "current_chunk_num": (
                        current_chunk_num if total_rows > 0 else 0
                    ),  
                    "total_chunks": estimated_total_chunks,
                }
                chunk_upload_callback(final_progress_details)

        except Exception as e:
            raise ValueError(
                f"Failed to process and upload file to GCS using blob.open: {str(e)}"
            )

    def _download_in_chunks(self, blob: storage.Blob) -> Iterator[pd.DataFrame]:
        """
        Stream-download CSV from GCS in pandas DataFrame chunks.

        Args:
            blob: GCS blob object representing the source CSV.

        Yields:
            DataFrame chunks.
        """
        with blob.open("rb") as blob_stream:
            for chunk in pd.read_csv(blob_stream, chunksize=self.pandas_chunk_size):
                yield chunk
