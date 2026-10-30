from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack

from google.api_core.exceptions import NotFound
from google.cloud import storage
import pandas as pd
import io
from typing import Iterator, List, Optional, Callable, Dict, Any, Sequence, Tuple
from dataclasses import dataclass
import time

from materializationengine.blueprints.upload.source import (
    GCSSource,
    S3Source,
    parse_source_uri,
)

# GCS compose accepts at most this many source objects per call.
MAX_COMPOSE_SOURCES = 32
# How much to fetch at a time while looking for the end of a line.
LINE_SEARCH_BYTES = 64 * 1024
COUNT_READ_BYTES = 8 * 1024 * 1024
BOUNDARY_LOOKUP_THREADS = 16


class _RangeReader(io.RawIOBase):
    """A read-only file over the next `length` bytes of a stream.

    Counts the lines it hands out, so a caller can check that a parser read one row
    per line.
    """

    def __init__(self, stream, length: int):
        self._stream = stream
        self._remaining = length
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

def segmentation_blob_name(name: str) -> str:
    """Where the segmentation rows of a processed CSV go: beside it."""
    base = name[: -len(".csv")] if name.endswith(".csv") else name
    return f"{base}.segmentation.csv"


class _ProcessedWriter:
    """The processed CSV object, and with segmentation=True its segmentation object
    (segmentation_blob_name) too: the transform then returns (rows, segmentation rows).

    Both objects are opened up front, so a part with no rows still writes an empty
    segmentation object for compose to find.
    """

    def __init__(self, bucket, name: str, segmentation: bool = False):
        self._bucket = bucket
        self._name = name
        self._segmentation = segmentation
        self._stack = ExitStack()

    def _open(self, name: str):
        return self._stack.enter_context(
            self._bucket.blob(name).open("wb", content_type="text/csv", ignore_flush=True)
        )

    def __enter__(self):
        self._rows = self._open(self._name)
        self._segmentation_rows = (
            self._open(segmentation_blob_name(self._name)) if self._segmentation else None
        )
        return self

    def write(self, result) -> int:
        """Write one transformed chunk; returns its number of rows."""
        rows, segmentation = result if isinstance(result, tuple) else (result, None)
        data = rows.to_csv(index=False, header=False).encode("utf-8")
        if data:
            self._rows.write(data)
        if self._segmentation_rows is not None:
            if segmentation is None or len(segmentation) != len(rows):
                raise ValueError("the transform gave no segmentation rows for a chunk")
            data = segmentation.to_csv(index=False, header=False).encode("utf-8")
            if data:
                self._segmentation_rows.write(data)
        return len(rows)

    def __exit__(self, *exc):
        return self._stack.__exit__(*exc)


def _text_dtypes(text_columns: Sequence[str]) -> Optional[Dict[str, Any]]:
    """read_csv dtypes keeping text_columns as text, or None for none."""
    return {c: str for c in text_columns} or None


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

    def source(self, name: str):
        """The CSV to read: a name in the upload bucket, or a gs:// or s3:// location."""
        if "://" not in name:
            return GCSSource(self.bucket, name, f"gs://{self.bucket.name}/{name}")
        scheme, bucket_name, key = parse_source_uri(name)
        if scheme == "s3":
            return S3Source(bucket_name, key, name)
        bucket = self.bucket if bucket_name == self.bucket.name else self.client.bucket(bucket_name)
        return GCSSource(bucket, key, name)

    def _get_csv_row_count(self, source) -> int:
        """The number of data rows in a CSV (its lines, less the header)."""
        with source.open_range(0, source.size) as stream:
            reader = _RangeReader(stream, source.size)
            while reader.read(COUNT_READ_BYTES):
                pass
        return max(reader.lines() - 1, 0)

    def process_csv_in_chunks(
        self,
        source_blob_name: str,
        destination_blob_name: str,
        transform_function: Callable[[pd.DataFrame], pd.DataFrame],
        chunk_upload_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        segmentation: bool = False,
        text_columns: Sequence[str] = (),
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
            segmentation: The transform also gives segmentation rows, written beside
                          the destination (segmentation_blob_name).
            text_columns: Source columns read as text (supplied ids, which floats
                          would round).
        """
        source = self.source(source_blob_name)

        total_rows = self._get_csv_row_count(source)
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

        try:
            with _ProcessedWriter(
                self.bucket, destination_blob_name, segmentation
            ) as writer:
                for chunk_df in self._download_in_chunks(source, text_columns):
                    current_chunk_num += 1
                    total_processed_rows += writer.write(transform_function(chunk_df))

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
        source = self.source(source_blob_name)
        size = source.size
        header_end = self._next_line_start(source, 0, size)
        header = source.read(0, header_end).decode("utf-8-sig")
        if header_end >= size:
            return header, []

        # Each boundary is one small ranged read, independent of the others.
        part_size_bytes = max(int(part_size_bytes), 1)
        offsets = range(header_end + part_size_bytes, size, part_size_bytes)
        with ThreadPoolExecutor(max_workers=BOUNDARY_LOOKUP_THREADS) as pool:
            line_starts = pool.map(
                lambda offset: self._next_line_start(source, offset, size), offsets
            )
            boundaries = sorted({b for b in line_starts if header_end < b < size})
        edges = [header_end] + boundaries + [size]
        return header, list(zip(edges[:-1], edges[1:]))

    def _next_line_start(self, source, offset: int, size: int) -> int:
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
            window = source.read(position, end)
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
        segmentation: bool = False,
        text_columns: Sequence[str] = (),
    ) -> Dict[str, Any]:
        """Transform the lines in bytes [start, end) of a CSV and write them to one object.

        Reads and writes in chunks of the processor's chunk size, as process_csv_in_chunks
        does for a whole file. Returns the number of data rows read and the number of
        lines in the range. They differ when a quoted field holds a newline or the range
        has blank lines; the caller must not use the output then.
        """
        columns = list(pd.read_csv(io.StringIO(header), nrows=0).columns)
        source = self.source(source_blob_name)
        rows_read = 0
        rows_written = 0
        with source.open_range(start, end) as blob_stream:
            reader = _RangeReader(blob_stream, end - start)
            with _ProcessedWriter(
                self.bucket, destination_blob_name, segmentation
            ) as writer:
                if end > start:
                    for chunk_df in pd.read_csv(
                        reader,
                        header=None,
                        names=columns,
                        chunksize=self.pandas_chunk_size,
                        dtype=_text_dtypes(text_columns),
                    ):
                        rows_read += len(chunk_df)
                        rows_written += writer.write(transform_function(chunk_df))
        return {
            "rows_read": rows_read,
            "rows_written": rows_written,
            "lines": reader.lines(),
        }

    def process_parquet_part(
        self,
        source_uri: str,
        root: str,
        pieces: List[List[Any]],
        destination_blob_name: str,
        transform_function: Callable[[pd.DataFrame], pd.DataFrame],
        segmentation: bool = False,
        text_columns: Sequence[str] = (),
    ) -> Dict[str, Any]:
        """Transform a parquet part's row groups and write them to one CSV object.

        text_columns are integer columns kept exact where they have nulls (as Python
        ints, not float64)."""
        from materializationengine.blueprints.upload.parquet_source import (
            iter_parquet_part,
        )

        rows_read = 0
        rows_written = 0
        with _ProcessedWriter(self.bucket, destination_blob_name, segmentation) as writer:
            for chunk_df in iter_parquet_part(
                source_uri, root, pieces, self.pandas_chunk_size, exact_columns=text_columns
            ):
                rows_read += len(chunk_df)
                rows_written += writer.write(transform_function(chunk_df))
        return {"rows_read": rows_read, "rows_written": rows_written}

    def count_csv_range_lines(self, source_blob_name: str, start: int, end: int) -> int:
        """The number of lines in bytes [start, end) of an object."""
        with self.source(source_blob_name).open_range(start, end) as blob_stream:
            reader = _RangeReader(blob_stream, end - start)
            while reader.read(COUNT_READ_BYTES):
                pass
        return reader.lines()

    def compose(self, part_blob_names: List[str], destination_blob_name: str) -> None:
        """Concatenate objects, in order, into one object, then delete the parts.

        GCS composes at most 32 objects per call, so more parts are combined in rounds
        through intermediate objects named after the destination.

        Safe to repeat: the parts are deleted only once the destination is complete, so
        a destination that exists while a part is missing was composed by an earlier
        run that stopped while deleting; this finishes the deleting.
        """
        if self.bucket.blob(destination_blob_name).exists() and not all(
            self.bucket.blob(name).exists() for name in part_blob_names
        ):
            self.delete_blobs(list(part_blob_names))
            return
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

    def _download_in_chunks(
        self, source, text_columns: Sequence[str] = ()
    ) -> Iterator[pd.DataFrame]:
        """Stream a whole CSV source as pandas DataFrame chunks."""
        with source.open_range(0, source.size) as stream:
            reader = _RangeReader(stream, source.size)
            for chunk in pd.read_csv(
                reader, chunksize=self.pandas_chunk_size, dtype=_text_dtypes(text_columns)
            ):
                yield chunk
