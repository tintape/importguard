"""Streaming readers for the two supported file formats.

Neither reader loads the whole file: CSV is read record by record with the
standard library, Parquet batch by batch with ``ParquetFile.iter_batches``.
Each yields :class:`RawRow` items holding raw (not yet validated) cell values.
Problems that make the whole file unreadable raise :class:`FileRejected`.
"""

from __future__ import annotations

import csv
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import BinaryIO, Literal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from importguard.contract import FIELDS

FileFormat = Literal["csv", "parquet"]

PARQUET_MAGIC = b"PAR1"
CSV_FIELD_SIZE_LIMIT = 4096
MAX_TEXT_CELL_CHARS = CSV_FIELD_SIZE_LIMIT
# Decompression-bomb guards: a small compressed file must not expand into gigabytes.
MAX_ROW_GROUP_UNCOMPRESSED_BYTES = 256 * 1024 * 1024
MAX_TOTAL_UNCOMPRESSED_BYTES = 1024 * 1024 * 1024
MAX_COMPRESSION_RATIO = 200
_UNITS_PER_SECOND = {"s": 1, "ms": 1_000, "us": 1_000_000, "ns": 1_000_000_000}


class FileRejected(Exception):
    """The file as a whole cannot be processed (bad header, schema, encoding...)."""

    def __init__(self, code: str, message: str, row_number: int | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.row_number = row_number


@dataclass(frozen=True, slots=True)
class RawRow:
    number: int  # 1-based data row number (CSV header not counted)
    values: dict[str, object] | None
    error_code: str | None = None
    error_message: str | None = None


def sniff_format(head: bytes, tail: bytes) -> FileFormat | None:
    """Detect the format from file content; names and extensions are not trusted."""
    if head.startswith(PARQUET_MAGIC) and tail.endswith(PARQUET_MAGIC):
        return "parquet"
    if b"\x00" in head:
        return None
    sample = head.removeprefix(b"\xef\xbb\xbf")
    try:
        sample.decode("utf-8")
    except UnicodeDecodeError as exc:
        # A multi-byte character may be cut at the end of the sample.
        if exc.start < len(sample) - 3:
            return None
    return "csv"


def check_header(names: list[str]) -> list[str]:
    normalized = [name.strip().lower() for name in names]
    duplicates = sorted({n for n in normalized if normalized.count(n) > 1})
    if duplicates:
        raise FileRejected("header_duplicate_columns", f"duplicate columns: {', '.join(duplicates)}")
    missing = [f for f in FIELDS if f not in normalized]
    if missing:
        raise FileRejected("header_missing_columns", f"missing columns: {', '.join(missing)}")
    unknown = [n for n in normalized if n not in FIELDS]
    if unknown:
        shown = ", ".join(repr(n) for n in unknown[:5])
        raise FileRejected("header_unknown_columns", f"unexpected columns: {shown}")
    return normalized


def _decoded_lines(handle: BinaryIO) -> Iterator[str]:
    """Decode line by line so an encoding error can name the exact line of the file."""
    for line_number, raw in enumerate(handle, start=1):
        line = raw.removeprefix(b"\xef\xbb\xbf") if line_number == 1 else raw
        try:
            yield line.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise FileRejected(
                "file_encoding", f"the file is not valid UTF-8 text (line {line_number} of the file)"
            ) from exc


def iter_csv(path: Path) -> Iterator[RawRow]:
    csv.field_size_limit(CSV_FIELD_SIZE_LIMIT)
    row_number = 0
    try:
        with path.open("rb") as handle:
            reader = csv.reader(_decoded_lines(handle), strict=True)
            header = next(reader, None)
            if header is None:
                raise FileRejected("empty_file", "the file is empty")
            columns = check_header(header)
            for record in reader:
                row_number += 1
                if not record or all(not cell.strip() for cell in record):
                    yield RawRow(row_number, None, "empty_row", "the row is empty")
                elif len(record) != len(columns):
                    yield RawRow(
                        row_number,
                        None,
                        "column_count",
                        f"expected {len(columns)} values, found {len(record)}",
                    )
                else:
                    yield RawRow(row_number, dict(zip(columns, record, strict=True)))
    except csv.Error as exc:
        raise FileRejected("csv_syntax", f"malformed CSV: {exc}", row_number + 1) from exc
    if row_number == 0:
        raise FileRejected("empty_file", "the file has a header but no data rows")


def _check_parquet_type(name: str, kind: pa.DataType) -> None:
    if pa.types.is_dictionary(kind):
        kind = kind.value_type
    is_text = pa.types.is_string(kind) or pa.types.is_large_string(kind) or pa.types.is_string_view(kind)
    allowed = {
        "sold_at": is_text or (pa.types.is_timestamp(kind) and kind.tz is not None),
        "quantity": is_text or pa.types.is_integer(kind),
        "unit_price": is_text or pa.types.is_decimal(kind),
    }.get(name, is_text)
    if not allowed:
        hint = {
            "sold_at": "a time-zone-aware timestamp or ISO 8601 text",
            "quantity": "an integer or text",
            "unit_price": "a decimal or text (floating point is not accepted for money)",
        }.get(name, "text")
        raise FileRejected("schema_type", f"column {name!r} has type {kind}; expected {hint}")


def _timestamp_values(column: pa.Array) -> list[object]:
    """Convert to UTC datetimes without pandas; sub-second parts survive as microseconds."""
    per_second = _UNITS_PER_SECOND[column.type.unit]
    values: list[object] = []
    for raw in column.cast(pa.int64()).to_pylist():
        if raw is None:
            values.append(None)
            continue
        seconds, fraction = divmod(raw, per_second)
        micros = fraction * 1_000_000 // per_second or (1 if fraction else 0)
        try:
            values.append(datetime.fromtimestamp(seconds, UTC) + timedelta(microseconds=micros))
        except (OverflowError, OSError, ValueError):
            values.append(f"<out-of-range timestamp {raw}>")
    return values


def _check_text_lengths(column: pa.Array, first_row: int) -> None:
    """Reject overlong text before converting to Python objects (dictionaries are never expanded)."""
    is_dictionary = pa.types.is_dictionary(column.type)
    kind = column.type.value_type if is_dictionary else column.type
    if not (pa.types.is_string(kind) or pa.types.is_large_string(kind) or pa.types.is_string_view(kind)):
        return
    values = column.dictionary if is_dictionary else column
    if not len(values):
        return
    lengths = pc.utf8_length(values)
    longest = pc.max(lengths).as_py()
    if longest is None or longest <= MAX_TEXT_CELL_CHARS:
        return
    position = pc.index(lengths, longest).as_py()
    if is_dictionary:  # find the first row that references the overlong dictionary entry
        position = pc.index(column.indices, pa.scalar(position, column.indices.type)).as_py()
    raise FileRejected(
        "value_too_long", f"a text value is longer than {MAX_TEXT_CELL_CHARS} characters", first_row + max(position, 0)
    )


def _column_values(column: pa.Array, first_row: int) -> list[object]:
    if pa.types.is_timestamp(column.type):
        return _timestamp_values(column)
    _check_text_lengths(column, first_row)
    return list(column.to_pylist())


def iter_parquet(path: Path, *, batch_rows: int) -> Iterator[RawRow]:
    try:
        parquet = pq.ParquetFile(path)
        schema = parquet.schema_arrow
    except (pa.ArrowException, OSError) as exc:
        raise FileRejected("file_corrupt", f"not a readable Parquet file: {exc}") from exc
    columns = check_header(list(schema.names))
    original = dict(zip(columns, schema.names, strict=True))
    for name in FIELDS:
        _check_parquet_type(name, schema.field(original[name]).type)
    if parquet.metadata.num_rows == 0:
        raise FileRejected("empty_file", "the file has no data rows")
    _check_uncompressed_size(parquet.metadata)
    row_number = 0
    try:
        for batch in parquet.iter_batches(batch_size=batch_rows, columns=list(original.values())):
            data = {name: _column_values(batch.column(original[name]), row_number + 1) for name in FIELDS}
            for index in range(batch.num_rows):
                row_number += 1
                yield RawRow(row_number, {name: data[name][index] for name in FIELDS})
    except pa.ArrowException as exc:
        raise FileRejected("file_corrupt", f"cannot read Parquet data: {exc}", row_number + 1) from exc


def _check_uncompressed_size(metadata: pq.FileMetaData) -> None:
    """Footer-based limits. The footer could lie; the worker's memory limit is the last line of defence."""
    total = 0
    for index in range(metadata.num_row_groups):
        group = metadata.row_group(index)
        if group.total_byte_size > MAX_ROW_GROUP_UNCOMPRESSED_BYTES:
            raise FileRejected("file_too_large_uncompressed", "a Parquet row group is too large when uncompressed")
        for column_index in range(group.num_columns):
            chunk = group.column(column_index)
            if chunk.total_uncompressed_size > MAX_COMPRESSION_RATIO * max(chunk.total_compressed_size, 1):
                raise FileRejected("file_too_large_uncompressed", "a Parquet column is suspiciously highly compressed")
        total += group.total_byte_size
    if total > MAX_TOTAL_UNCOMPRESSED_BYTES:
        raise FileRejected("file_too_large_uncompressed", "the Parquet file is too large when uncompressed")


def parquet_row_count(path: Path) -> int:
    try:
        return int(pq.ParquetFile(path).metadata.num_rows)
    except (pa.ArrowException, OSError) as exc:
        raise FileRejected("file_corrupt", f"not a readable Parquet file: {exc}") from exc


def iter_rows(path: Path, file_format: FileFormat, *, batch_rows: int) -> Iterator[RawRow]:
    if file_format == "parquet":
        return iter_parquet(path, batch_rows=batch_rows)
    return iter_csv(path)
