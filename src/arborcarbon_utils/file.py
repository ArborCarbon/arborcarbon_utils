"""File path utilities supporting local and S3 paths."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, TextIO
from urllib.parse import urlparse

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from mypy_boto3_s3.type_defs import ObjectTypeDef

##################################################################################################
# Types
##################################################################################################
# Recursive JSON type alias
type JSON = dict[str, JSON] | list[JSON] | str | int | float | bool | None

_S3_SCHEME = "s3"


##################################################################################################
# Classes
##################################################################################################
class FilePath:
    """
    Unified file path wrapper supporting local paths, S3 URIs, and GDAL virtual paths.

    Examples:
        Local path:
            >>> fp = FilePath("surveys/J00001/raw/image.tif")
            >>> fp.gdal_path
            'surveys/J00001/raw/image.tif'

        S3 URI:
            >>> fp = FilePath("s3://mybucket/surveys/J00001/raw/image.tif")
            >>> fp.gdal_path
            '/vsis3/mybucket/surveys/J00001/raw/image.tif'

        Explicit bucket:
            >>> fp = FilePath("surveys/J00001/raw/image.tif", bucket="mybucket")
            >>> fp.s3_uri
            's3://mybucket/surveys/J00001/raw/image.tif'
    """

    __slots__ = ("bucket", "file_object")

    ##############################################################################################
    # Constructors
    ##############################################################################################
    def __init__(
        self,
        file_path: str | Path | FilePath,
        bucket: str | None = None,
        *,
        override_bucket: bool = False,
    ):
        """
        Initialise a FilePath.

        Args:
            file_path: Local path string, pathlib.Path, S3 URI (s3://bucket/key),
                or another FilePath instance.
            bucket: Optional bucket name. Inferred automatically from S3 URIs.
            override_bucket: If True, override any bucket inferred from an S3 URI
                with the explicitly supplied bucket value.
        """
        _path: str
        self.bucket: str | None = None
        self.file_object: dict[str, Any]

        if isinstance(file_path, FilePath):
            _path = file_path.file_path
            self.bucket = file_path.bucket
        elif isinstance(file_path, Path):
            _path = str(file_path)
        else:
            parsed = urlparse(file_path)
            if parsed.scheme == _S3_SCHEME:
                _path = parsed.path.strip("/")
                self.bucket = parsed.netloc
            elif parsed.scheme:
                msg = f"Unsupported FilePath scheme: {parsed.scheme!r}"
                raise ValueError(msg)
            else:
                _path = file_path

        if override_bucket or self.bucket is None:
            self.bucket = bucket

        self.file_object = {"Key": _path}

    @classmethod
    def from_s3_object(cls, obj: ObjectTypeDef) -> FilePath:
        """Create a FilePath from a boto3 S3 object dict."""
        fp = cls("")
        fp.file_object = dict(obj)
        return fp

    ##############################################################################################
    # Built-in methods
    ##############################################################################################
    def __add__(self, other: object) -> FilePath:
        """Concatenate a string suffix onto the file path."""
        fp = FilePath(self.file_path, bucket=self.bucket)
        if isinstance(other, str):
            fp.file_path = fp.file_path + other
        return fp

    def __eq__(self, other: object) -> bool:
        """Compare by file path key."""
        if not isinstance(other, FilePath):
            return NotImplemented
        return self.file_path == other.file_path

    def __hash__(self) -> int:
        """Hash by file path key."""
        return hash(self.file_path)

    def __repr__(self) -> str:
        """Represent as S3 URI if bucketed, otherwise plain path."""
        return self.s3_uri if self.bucket else self.file_path

    def __str__(self) -> str:
        """Return the raw file path key."""
        return self.file_path

    ##############################################################################################
    # Attributes
    ##############################################################################################
    @property
    def as_path(self) -> Path:
        """Return a pathlib.Path for local operations."""
        return Path(self.file_path)

    @property
    def copy(self) -> FilePath:
        """Return a shallow copy."""
        return FilePath(self.file_path, bucket=self.bucket)

    @property
    def date_modified(self) -> datetime | None:
        """Last-modified timestamp from the S3 object metadata."""
        return self.file_object.get("LastModified")

    @date_modified.setter
    def date_modified(self, value: datetime):
        self.file_object["LastModified"] = value

    @property
    def dir_name(self) -> str:
        """Directory portion of the path."""
        return file_dir_name(self.file_path)

    @property
    def etag(self) -> str | None:
        """S3 ETag from object metadata."""
        return self.file_object.get("ETag")

    @property
    def ext(self) -> str:
        """File extension including the leading dot."""
        return file_ext(self.file_path)

    @property
    def filename(self) -> str:
        """Base filename with extension."""
        return file_name_with_ext(self.file_path)

    @property
    def filename_no_ext(self) -> str:
        """Base filename without extension."""
        return file_name_no_ext(self.file_path)

    @property
    def file_path(self) -> str:
        """The raw key / path string."""
        return self.file_object.get("Key", "")

    @file_path.setter
    def file_path(self, value: str):
        self.file_object["Key"] = value

    @property
    def gdal_path(self) -> str:
        """
        GDAL-compatible path.

        Returns ``/vsis3/{bucket}/{key}`` for S3 paths,
        or the plain key for local paths.
        Automatically wraps ``.zip`` / ``.shz`` files with ``/vsizip/``.
        """
        path = self.file_path
        if self.bucket:
            path = path_join("/vsis3", self.bucket, path).replace("\\", "/")
        if path.lower().endswith((".zip", ".shz")):
            prefix = "/vsizip/" if path.startswith("/") else path_join("/vsizip", "")
            path = prefix + path
        return path

    @property
    def gdal_mem(self) -> str:
        """GDAL in-memory virtual path (``/vsimem/{key}``)."""
        return path_join("/vsimem", self.file_path).replace("\\", "/")

    @property
    def is_local(self) -> bool:
        """True when no bucket is set (local filesystem path)."""
        return self.bucket is None

    @property
    def regex(self) -> str:
        """Exact-match regex pattern for this path."""
        return f"^{self.file_path}$"

    @property
    def size(self) -> int | None:
        """Object size in bytes from S3 metadata."""
        return self.file_object.get("Size")

    @size.setter
    def size(self, value: int):
        self.file_object["Size"] = value

    @property
    def s3_uri(self) -> str:
        """S3 URI (``s3://bucket/key``). Returns plain key if no bucket is set."""
        if not self.bucket:
            return self.file_path
        return f"s3://{self.bucket}/{self.file_path}"

    @property
    def storage_class(self) -> str | None:
        """S3 storage class from object metadata."""
        return self.file_object.get("StorageClass")

    ##############################################################################################
    # Path methods
    ##############################################################################################
    def append(self, suffix: str, *, replace: bool = False) -> FilePath:
        """
        Append a string to the filename stem (before the extension).

        Args:
            suffix: String to append before the file extension.
            replace: If True, mutate this instance in addition to returning
                the new path.

        Returns:
            New FilePath with the suffix applied.
        """
        new_path = file_name_append(self.file_path, suffix)
        if replace:
            self.file_path = new_path
        return FilePath(new_path, bucket=self.bucket)

    def change_path(self, old: str, new: str, *, replace: bool = False) -> FilePath:
        """
        Replace a substring within the path.

        Args:
            old: Substring to replace.
            new: Replacement string.
            replace: If True, mutate this instance in addition to returning
                the new path.

        Returns:
            New FilePath with the substitution applied.
        """
        new_path = self.file_path.replace(old, new)
        if replace:
            self.file_path = new_path
        return FilePath(new_path, bucket=self.bucket)

    def ext_swap(self, new_ext: str, *, replace: bool = False) -> FilePath:
        """
        Replace the file extension.

        Args:
            new_ext: New extension, e.g. ``".tif"``.
            replace: If True, mutate this instance in addition to returning
                the new path.

        Returns:
            New FilePath with the extension swapped.
        """
        new_path = file_ext_swap(self.file_path, new_ext)
        if replace:
            self.file_path = new_path
        return FilePath(new_path, bucket=self.bucket)

    def with_bucket(self, bucket: str) -> FilePath:
        """
        Return a copy of this path associated with the given bucket.

        Args:
            bucket: Bucket name to attach.

        Returns:
            New FilePath with the bucket set.
        """
        return FilePath(self.file_path, bucket=bucket)


##################################################################################################
# Module level helpers
##################################################################################################
def _file_name_no_ext_full(filename: str) -> str:
    return str(Path(filename).with_suffix(""))


def file_dir_name(filename: str) -> str:
    """Return the directory portion of a path."""
    return os.path.dirname(filename)  # noqa: PTH120


def file_exists(filename: str) -> bool:
    """Return True if the local file exists."""
    return Path(filename).is_file()


def file_ext(filename: str) -> str:
    """Return the file extension including the leading dot."""
    return Path(filename).suffix


def file_ext_swap(filename: str, new_ext: str) -> str:
    """Return the filename with its extension replaced by new_ext."""
    return _file_name_no_ext_full(filename) + new_ext


def file_name_append(filename: str, suffix: str) -> str:
    """Append suffix to the filename stem, preserving directory and extension."""
    directory = file_dir_name(filename)
    base = file_name_no_ext(filename) + suffix + file_ext(filename)
    return path_join(directory, base) if directory else base


def file_name_no_ext(filename: str) -> str:
    """Return the base filename without extension."""
    return Path(filename).stem


def file_name_with_ext(filename: str) -> str:
    """Return the base filename including extension."""
    return Path(filename).name


def json_read(text_read: TextIO) -> JSON:
    """
    Deserialise a JSON text stream.

    Args:
        text_read: Open text file object.

    Returns:
        Parsed JSON value.

    Raises:
        OSError: If the file cannot be read.
    """
    try:
        return json.load(text_read)
    except OSError as exc:
        msg = "Error reading JSON file"
        raise OSError(msg) from exc


def json_write(
    text_write: TextIO,
    data: JSON,
    separators: tuple[str, str] | None = None,
    default: Callable[..., Any] | None = None,
    *,
    indent: bool = False,
):
    """
    Serialise data to a JSON text stream.

    Args:
        text_write: Open text file object.
        data: JSON-serialisable value.
        separators: Custom ``(item_sep, key_sep)`` tuple.
        default: Callable for non-serialisable objects.
        indent: If True, pretty-print with 4-space indentation.
    """
    json.dump(
        data,
        text_write,
        separators=separators,
        indent=4 if indent else None,
        default=default,
    )


def open_text_read(
    filename: str,
    encoding: str | None = None,
    newline: str | None = None,
) -> TextIO:
    """
    Open a local text file for reading.

    Args:
        filename: Path to the file.
        encoding: Text encoding (defaults to locale encoding).
        newline: Newline handling override.

    Returns:
        Open text file object.
    """
    return Path(filename).open(encoding=encoding, newline=newline)


def path_join(*parts: str) -> str:
    """Join path components using the OS separator."""
    return os.path.join(*parts)  # noqa: PTH118
