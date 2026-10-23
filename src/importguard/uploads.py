"""Safe handling of uploaded files.

The client-supplied file name is used for display only. The stored file gets a
random server-generated name inside ``upload_dir``; the format is detected from
the content, never from the extension.
"""

from __future__ import annotations

import hashlib
import os
import unicodedata
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from importguard.readers import FileFormat, sniff_format

CHUNK_BYTES = 1024 * 1024
SNIFF_BYTES = 8192
MAX_DISPLAY_NAME = 255
FALLBACK_NAME = "upload"


class UploadRejected(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message


@dataclass(frozen=True, slots=True)
class StoredUpload:
    stored_filename: str
    original_filename: str
    size_bytes: int
    sha256: str
    file_format: FileFormat


def sanitize_filename(name: str | None) -> str:
    """Reduce a client-supplied name to a harmless display string (no path, no controls)."""
    if not name:
        return FALLBACK_NAME
    base = unicodedata.normalize("NFC", name).replace("\\", "/").rsplit("/", 1)[-1]
    cleaned = "".join(ch for ch in base if ch.isprintable()).strip().lstrip(".")
    if not cleaned:
        return FALLBACK_NAME
    return cleaned[:MAX_DISPLAY_NAME]


def store_upload(stream: BinaryIO, original_name: str | None, *, upload_dir: Path, max_bytes: int) -> StoredUpload:
    upload_dir.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex
    partial = upload_dir / f"{token}.part"
    digest = hashlib.sha256()
    size = 0
    head = b""
    tail = b""
    try:
        with partial.open("xb") as out:
            while chunk := stream.read(CHUNK_BYTES):
                size += len(chunk)
                if size > max_bytes:
                    raise UploadRejected(413, f"The file is larger than the {max_bytes // (1024 * 1024)} MiB limit.")
                if len(head) < SNIFF_BYTES:
                    head += chunk[: SNIFF_BYTES - len(head)]
                tail = (tail + chunk)[-8:]
                digest.update(chunk)
                out.write(chunk)
        if size == 0:
            raise UploadRejected(400, "The file is empty.")
        file_format = sniff_format(head, tail)
        if file_format is None:
            raise UploadRejected(415, "Unsupported file: expected UTF-8 CSV text or an Apache Parquet file.")
        stored_filename = f"{token}.{file_format}"
        final = upload_dir / stored_filename
        os.replace(partial, final)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    return StoredUpload(
        stored_filename=stored_filename,
        original_filename=sanitize_filename(original_name),
        size_bytes=size,
        sha256=digest.hexdigest(),
        file_format=file_format,
    )
