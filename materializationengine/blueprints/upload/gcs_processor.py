from concurrent.futures import ThreadPoolExecutor

from google.api_core.exceptions import NotFound
from google.cloud import storage
import pandas as pd
import io
from typing import Iterator, List, Optional, Callable, Dict, Any, Tuple
from dataclasses import dataclass
import time

# GCS compose accepts at most this many source objects per call.
MAX_COMPOSE_SOURCES = 32
# How much to fetch at a time while looking for the end of a line.
LINE_SEARCH_BYTES = 64 * 1024
COUNT_READ_BYTES = 8 * 1024 * 1024
BOUNDARY_LOOKUP_THREADS = 16


class _RangeReader(io.RawIOBase):
    """A read-only file over bytes [start, end) of a seekable stream.

    Counts the lines it hands out, so a caller can check that a parser read one row
    per line.
    """

    def __init__(self, stream, start: int, end: int):
        stream.seek(start)
        self._stream = stream
        self._remaining = end - start
        self._newlines = 0
        self._any = False
        self._last = b""

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if self._remaining <= 0:
            return b""
        if size is None or size < 0 or size > self._remaining:
            size = self._remaining
        data = self._stream.read(size)
        self._remaining -= len(data)
        if not data:
            self._remaining = 0
            return b""
        self._newlines += data.count(b"\n")
        self._any = True
        self._last = data[-1:]
        return data

    def readinto(self, buffer) -> int:
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)

    def lines(self) -> int:
        """Lines read so far, counting a last line that has no newline."""
        return self._newlines + (1 if self._any and self._last != b"\n" else 0)

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
            ) from e

    def plan_csv_parts(
        self, source_blob_name: str, part_size_bytes: int
    ) -> Tuple[str, List[Tuple[int, int]]]:
        """Split a CSV object into byte ranges that each start at the beginning of a line.

        Returns the header line and a list of (start, end) ranges, end exclusive, that
        together cover every byte after the header exactly once. Each boundary is moved
        forward to just after the next newline, so a range never cuts a line in two.
        That assumes no quoted field holds a newline; process_csv_range detects a file
        where one does.
        """
        blob = self.bucket.get_blob(source_blob_name)
        if blob is None:
            raise FileNotFoundError(
                f"Source blob '{source_blob_name}' not found in bucket '{self.bucket.name}'."
            )
        size = blob.size
        header_end = self._next_line_start(blob, 0, size)
        header = blob.download_as_bytes(start=0, end=header_end - 1).decode("utf-8-sig")
        if header_end >= size:
            return header, []

        # Each boundary is one small ranged read, independent of the others.
        part_size_bytes = max(int(part_size_bytes), 1)
        offsets = range(header_end + part_size_bytes, size, part_size_bytes)
        with ThreadPoolExecutor(max_workers=BOUNDARY_LOOKUP_THREADS) as pool:
            line_starts = pool.map(
                lambda offset: self._next_line_start(blob, offset, size), offsets
            )
            boundaries = sorted({b for b in line_starts if header_end < b < size})
        edges = [header_end] + boundaries + [size]
        return header, list(zip(edges[:-1], edges[1:]))

    def _next_line_start(self, blob: storage.Blob, offset: int, size: int) -> int:
        """The first line start at or after offset (size when none is left)."""
        if offset >= size:
            return size
        if offset == 0:
            position = 0
        else:
            # a newline just before offset makes offset itself a line start
            position = offset - 1
        while position < size:
            end = min(position + LINE_SEARCH_BYTES, size)
            window = blob.download_as_bytes(start=position, end=end - 1)
            newline = window.find(b"\n")
            if newline >= 0:
                return position + newline + 1
            position = end
        return size

    def process_csv_range(
        self,
        source_blob_name: str,
        start: int,
        end: int,
        header: str,
        destination_blob_name: str,
        transform_function: Callable[[pd.DataFrame], pd.DataFrame],
    ) -> Dict[str, Any]:
        """Transform the lines in bytes [start, end) of a CSV and write them to one object.

        Reads and writes in chunks of the processor's chunk size, as process_csv_in_chunks
        does for a whole file. Returns the number of data rows read and the number of
        lines in the range. They differ when a quoted field holds a newline or the range
        has blank lines; the caller must not use the output then.
        """
        columns = list(pd.read_csv(io.StringIO(header), nrows=0).columns)
        source_blob = self.bucket.blob(source_blob_name)
        destination_blob = self.bucket.blob(destination_blob_name)
        rows_read = 0
        rows_written = 0
        with source_blob.open("rb") as blob_stream:
            reader = _RangeReader(blob_stream, start, end)
            with destination_blob.open(
                "wb", content_type="text/csv", ignore_flush=True
            ) as gcs_writer_stream:
                if end > start:
                    for chunk_df in pd.read_csv(
                        reader,
                        header=None,
                        names=columns,
                        chunksize=self.pandas_chunk_size,
                    ):
                        rows_read += len(chunk_df)
                        processed_chunk_df = transform_function(chunk_df)
                        chunk_bytes = processed_chunk_df.to_csv(
                            index=False, header=False
                        ).encode("utf-8")
                        if chunk_bytes:
                            gcs_writer_stream.write(chunk_bytes)
                        rows_written += len(processed_chunk_df)
        return {
            "rows_read": rows_read,
            "rows_written": rows_written,
            "lines": reader.lines(),
        }

    def count_csv_range_lines(self, source_blob_name: str, start: int, end: int) -> int:
        """The number of lines in bytes [start, end) of an object."""
        with self.bucket.blob(source_blob_name).open("rb") as blob_stream:
            reader = _RangeReader(blob_stream, start, end)
            while reader.read(COUNT_READ_BYTES):
                pass
        return reader.lines()

    def compose(self, part_blob_names: List[str], destination_blob_name: str) -> None:
        """Concatenate objects, in order, into one object, then delete the parts.

        GCS composes at most 32 objects per call, so more parts are combined in rounds
        through intermediate objects named after the destination.
        """
        names = list(part_blob_names)
        intermediates = []
        level = 0
        while len(names) > MAX_COMPOSE_SOURCES:
            grouped = []
            for i in range(0, len(names), MAX_COMPOSE_SOURCES):
                name = f"{destination_blob_name}.compose-{level}-{i // MAX_COMPOSE_SOURCES:05d}"
                self._compose_once(names[i : i + MAX_COMPOSE_SOURCES], name)
                grouped.append(name)
            intermediates.extend(grouped)
            names = grouped
            level += 1
        self._compose_once(names, destination_blob_name)
        self.delete_blobs(list(part_blob_names) + intermediates)

    def _compose_once(self, source_names: List[str], destination_blob_name: str) -> None:
        destination = self.bucket.blob(destination_blob_name)
        destination.content_type = "text/csv"
        destination.compose([self.bucket.blob(name) for name in source_names])

    def delete_blobs(self, blob_names: List[str]) -> None:
        for name in blob_names:
            try:
                self.bucket.blob(name).delete()
            except NotFound:
                pass

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
