"""Where an upload's CSV is read from.

An upload either sits in the deployment's upload bucket (the wizard's browser upload) or
is read in place from a cloud location the user gives:

- gs://bucket/path.csv: any GCS object this deployment's service account can read,
  which includes every publicly readable object.
- s3://bucket/path.csv: a publicly readable S3 object, read with unsigned requests.
- A parquet file, a folder of parquet files or a Delta table at such a location:
  see parquet_source.py.

Every source offers its size, a ranged read and a stream over a byte range, which is all
the CSV processing needs. Only processed output is written, and always to the upload
bucket.
"""

import contextlib
import functools
import io
from typing import Iterator, Tuple
from urllib.parse import urlparse

from google.api_core import exceptions as google_exceptions

SUPPORTED_SCHEMES = ("gs", "s3")


class SourceAccessError(PermissionError):
    """The source exists, or may exist, but this deployment cannot read it."""


def parse_source_uri(uri: str) -> Tuple[str, str, str]:
    """Split gs://bucket/key or s3://bucket/key into (scheme, bucket, key)."""
    parsed = urlparse((uri or "").strip())
    scheme = parsed.scheme.lower()
    if scheme not in SUPPORTED_SCHEMES:
        raise ValueError(
            f"Unsupported location {uri!r}: use gs://bucket/path.csv or s3://bucket/path.csv"
        )
    bucket, key = parsed.netloc, parsed.path.strip("/")
    if not bucket or not key:
        raise ValueError(
            f"{uri!r} does not name a file or folder: use {scheme}://bucket/path"
        )
    return scheme, bucket, key


class GCSSource:
    """A GCS object, read with the deployment's storage client."""

    def __init__(self, bucket, key: str, uri: str):
        self.uri = uri
        try:
            blob = bucket.get_blob(key)
        except google_exceptions.Forbidden as e:
            raise SourceAccessError(f"Permission denied reading {uri}: {e.message}") from e
        if blob is None:
            raise FileNotFoundError(f"{uri} does not exist")
        self.blob = blob
        self.size = blob.size

    def read(self, start: int, end: int) -> bytes:
        """Bytes [start, end)."""
        if end <= start:
            return b""
        return self.blob.download_as_bytes(start=start, end=end - 1)

    @contextlib.contextmanager
    def open_range(self, start: int, end: int) -> Iterator[io.RawIOBase]:
        """A stream positioned at start; the caller reads at most end - start bytes."""
        with self.blob.open("rb") as stream:
            stream.seek(start)
            yield stream


class S3Source:
    """A publicly readable S3 object, read with unsigned requests."""

    def __init__(self, bucket: str, key: str, uri: str, client=None):
        from botocore.exceptions import ClientError

        self.uri = uri
        self.bucket = bucket
        self.key = key
        self.client = client or anonymous_s3_client(bucket)
        try:
            head = self.client.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            code = str(e.response.get("Error", {}).get("Code", ""))
            if code in ("404", "NoSuchKey", "NotFound"):
                raise FileNotFoundError(f"{uri} does not exist") from e
            if code in ("403", "AccessDenied", "Forbidden"):
                # S3 answers 403 for a missing key when the bucket cannot be listed
                raise SourceAccessError(
                    f"{uri} does not exist or is not publicly readable"
                ) from e
            raise
        self.size = head["ContentLength"]

    def _get(self, start: int, end: int):
        return self.client.get_object(
            Bucket=self.bucket, Key=self.key, Range=f"bytes={start}-{end - 1}"
        )["Body"]

    def read(self, start: int, end: int) -> bytes:
        """Bytes [start, end)."""
        if end <= start:
            return b""
        return self._get(start, end).read()

    @contextlib.contextmanager
    def open_range(self, start: int, end: int) -> Iterator[io.RawIOBase]:
        """A stream over bytes [start, end), one ranged GET."""
        if end <= start:
            yield io.BytesIO(b"")
            return
        body = self._get(start, end)
        try:
            yield body
        finally:
            body.close()


@functools.lru_cache(maxsize=32)
def anonymous_s3_client(bucket: str):
    """An S3 client that sends unsigned requests to the bucket's own region.

    Cached per bucket: finding the region is a request of its own, and boto3 clients
    are safe to share between threads.
    """
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config
    from pyarrow.fs import resolve_s3_region

    try:
        region = resolve_s3_region(bucket)
    except Exception:
        region = "us-east-1"
    return boto3.client(
        "s3",
        region_name=region,
        config=Config(signature_version=UNSIGNED, retries={"max_attempts": 5, "mode": "standard"}),
    )
