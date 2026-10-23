from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from importguard import readers
from importguard.readers import FileRejected, RawRow, iter_csv, iter_parquet, sniff_format

HEADER = "sale_id,sold_at,store_id,product_category,quantity,unit_price,currency\n"
ROW = "S-1,2026-09-01T10:00:00Z,BER-01,coffee,2,3.50,EUR\n"


def write(tmp_path: Path, text: str | bytes, name: str = "f.csv") -> Path:
    path = tmp_path / name
    path.write_bytes(text.encode() if isinstance(text, str) else text)
    return path


def parquet(tmp_path: Path, columns: dict[str, pa.Array], **kw: object) -> Path:
    path = tmp_path / "f.parquet"
    pq.write_table(pa.table(columns), path, **kw)  # type: ignore[arg-type]
    return path


def good_columns(n: int = 2) -> dict[str, pa.Array]:
    return {
        "sale_id": pa.array([f"S-{i}" for i in range(n)]),
        "sold_at": pa.array([datetime(2026, 9, 1, 10, tzinfo=UTC)] * n, pa.timestamp("ms", tz="Europe/Berlin")),
        "store_id": pa.array(["BER-01"] * n),
        "product_category": pa.array(["coffee"] * n).dictionary_encode(),
        "quantity": pa.array([2] * n, pa.int16()),
        "unit_price": pa.array([Decimal("3.50")] * n, pa.decimal128(9, 2)),
        "currency": pa.array(["EUR"] * n),
    }


class TestCsv:
    def test_reads_rows_with_numbers_and_bom(self, tmp_path: Path) -> None:
        rows = list(iter_csv(write(tmp_path, "\ufeff" + HEADER + ROW + ROW)))
        assert [r.number for r in rows] == [1, 2]
        assert rows[0].values == {
            "sale_id": "S-1",
            "sold_at": "2026-09-01T10:00:00Z",
            "store_id": "BER-01",
            "product_category": "coffee",
            "quantity": "2",
            "unit_price": "3.50",
            "currency": "EUR",
        }

    def test_crlf_line_endings(self, tmp_path: Path) -> None:
        rows = list(iter_csv(write(tmp_path, (HEADER + ROW + ROW).replace("\n", "\r\n"))))
        assert [r.values["currency"] for r in rows if r.values] == ["EUR", "EUR"]

    def test_header_order_and_case_do_not_matter(self, tmp_path: Path) -> None:
        text = "CURRENCY, Sale_ID ,sold_at,store_id,product_category,quantity,unit_price\nEUR,S-1,t,s,c,1,1\n"
        [row] = iter_csv(write(tmp_path, text))
        assert row.values is not None and row.values["currency"] == "EUR" and row.values["sale_id"] == "S-1"

    @pytest.mark.parametrize(
        ("text", "code"),
        [
            ("", "empty_file"),
            (HEADER, "empty_file"),
            ("sale_id,sold_at\nx,y\n", "header_missing_columns"),
            (HEADER.strip() + ",extra\n", "header_unknown_columns"),
            (HEADER.strip() + ",currency\n", "header_duplicate_columns"),
        ],
    )
    def test_file_level_rejections(self, tmp_path: Path, text: str, code: str) -> None:
        with pytest.raises(FileRejected) as info:
            list(iter_csv(write(tmp_path, text)))
        assert info.value.code == code

    def test_trailing_blank_lines_are_tolerated(self, tmp_path: Path) -> None:
        rows = list(iter_csv(write(tmp_path, HEADER + ROW + "\n\n" + ",,,,,,\n")))
        assert [(r.number, r.error_code) for r in rows] == [(1, None)]
        with pytest.raises(FileRejected) as info:
            list(iter_csv(write(tmp_path, HEADER + "\n\n")))
        assert info.value.code == "empty_file"

    def test_bad_rows_are_reported_individually(self, tmp_path: Path) -> None:
        rows = list(iter_csv(write(tmp_path, HEADER + ROW + "\n" + "a,b\n" + ROW)))
        assert [(r.number, r.error_code) for r in rows] == [(1, None), (2, "empty_row"), (3, "column_count"), (4, None)]

    def test_invalid_utf8_is_rejected_with_row(self, tmp_path: Path) -> None:
        with pytest.raises(FileRejected) as info:
            list(iter_csv(write(tmp_path, (HEADER + ROW).encode() + b"S-2,\xff\xfe,x\n")))
        assert info.value.code == "file_encoding"
        assert "line 3 of the file" in info.value.message

    def test_malformed_quotes_and_huge_fields(self, tmp_path: Path) -> None:
        with pytest.raises(FileRejected) as info:
            list(iter_csv(write(tmp_path, HEADER + 'S-1,"unterminated\n')))
        assert info.value.code == "csv_syntax"
        with pytest.raises(FileRejected):
            list(iter_csv(write(tmp_path, HEADER + "S-1," + "x" * 10_000 + ",a,b,c,d,e\n")))


class TestParquet:
    def test_reads_typed_values_in_batches(self, tmp_path: Path) -> None:
        rows = list(iter_parquet(parquet(tmp_path, good_columns(5)), batch_rows=2))
        assert [r.number for r in rows] == [1, 2, 3, 4, 5]
        first = rows[0].values
        assert first is not None
        assert first["sold_at"] == datetime(2026, 9, 1, 10, tzinfo=UTC)
        assert first["unit_price"] == Decimal("3.50") and first["quantity"] == 2
        assert first["product_category"] == "coffee"

    def test_text_columns_are_accepted(self, tmp_path: Path) -> None:
        cols = good_columns(1) | {"sold_at": pa.array(["2026-09-01T10:00:00Z"]), "unit_price": pa.array(["3.5"])}
        [row] = iter_parquet(parquet(tmp_path, cols), batch_rows=10)
        assert row.values is not None and row.values["unit_price"] == "3.5"

    @pytest.mark.parametrize(
        ("column", "array"),
        [
            ("unit_price", pa.array([3.5, 3.5], pa.float64())),
            ("sold_at", pa.array([datetime(2026, 9, 1), datetime(2026, 9, 1)], pa.timestamp("us"))),
            ("quantity", pa.array([2.0, 2.0])),
            ("store_id", pa.array([1, 2])),
        ],
    )
    def test_unsafe_types_are_rejected_up_front(self, tmp_path: Path, column: str, array: pa.Array) -> None:
        with pytest.raises(FileRejected) as info:
            list(iter_parquet(parquet(tmp_path, good_columns() | {column: array}), batch_rows=10))
        assert info.value.code == "schema_type"

    def test_sub_second_nanoseconds_survive_as_a_detectable_fraction(self, tmp_path: Path) -> None:
        stamps = pa.array([1_788_256_800_000_000_001, 1_788_256_800_000_000_000], pa.timestamp("ns", tz="UTC"))
        rows = list(iter_parquet(parquet(tmp_path, good_columns() | {"sold_at": stamps}), batch_rows=10))
        assert rows[0].values is not None and rows[0].values["sold_at"].microsecond == 1  # type: ignore[union-attr]
        assert rows[1].values is not None and rows[1].values["sold_at"].microsecond == 0  # type: ignore[union-attr]

    def test_corrupt_and_empty_files(self, tmp_path: Path) -> None:
        with pytest.raises(FileRejected) as info:
            list(iter_parquet(write(tmp_path, b"PAR1garbagePAR1", "x.parquet"), batch_rows=10))
        assert info.value.code == "file_corrupt"
        empty = {k: v.slice(0, 0) for k, v in good_columns().items()}
        with pytest.raises(FileRejected) as info:
            list(iter_parquet(parquet(tmp_path, empty), batch_rows=10))
        assert info.value.code == "empty_file"

    def test_overlong_text_cell_is_rejected_with_row(self, tmp_path: Path) -> None:
        cols = good_columns(3) | {"product_category": pa.array(["a", "b", "x" * 5000])}
        with pytest.raises(FileRejected) as info:
            list(iter_parquet(parquet(tmp_path, cols), batch_rows=10))
        assert (info.value.code, info.value.row_number) == ("value_too_long", 3)

    def test_overlong_dictionary_value_is_found_without_decoding(self, tmp_path: Path) -> None:
        values = pa.array(["a", "b", "a", "x" * 5000, "b"]).dictionary_encode()
        with pytest.raises(FileRejected) as info:
            list(iter_parquet(parquet(tmp_path, good_columns(5) | {"product_category": values}), batch_rows=10))
        assert (info.value.code, info.value.row_number) == ("value_too_long", 4)

    def test_extreme_compression_ratio_is_rejected(self, tmp_path: Path) -> None:
        cols = good_columns(200_000) | {"store_id": pa.array(["A" * 1000] * 200_000)}
        path = parquet(tmp_path, cols, compression="zstd", use_dictionary=False)
        with pytest.raises(FileRejected) as info:
            list(iter_parquet(path, batch_rows=10))
        assert info.value.code == "file_too_large_uncompressed"

    def test_decompression_bomb_guard(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(readers, "MAX_TOTAL_UNCOMPRESSED_BYTES", 100)
        with pytest.raises(FileRejected) as info:
            list(iter_parquet(parquet(tmp_path, good_columns(50)), batch_rows=10))
        assert info.value.code == "file_too_large_uncompressed"


def test_sniff_format() -> None:
    assert sniff_format(b"PAR1....", b"..PAR1") == "parquet"
    assert sniff_format(HEADER.encode(), b"") == "csv"
    assert sniff_format("sale_id,città\n".encode()[:-2], b"") == "csv"  # cut inside a multi-byte char
    assert sniff_format(b"\x89PNG\r\n\x1a\n\x00", b"") is None
    assert sniff_format(b"\xff\xfe\xfdabc" * 10, b"") is None


def test_raw_row_is_immutable() -> None:
    row = RawRow(1, {})
    with pytest.raises(AttributeError):
        row.number = 2  # type: ignore[misc]


def test_nul_bytes_anywhere_in_csv_are_rejected(tmp_path: Path) -> None:
    padding = "".join(f"S-{i},2026-09-01T10:00:00Z,BER-01,coffee,2,3.50,EUR\n" for i in range(400))  # > 8 KiB
    with pytest.raises(FileRejected) as info:
        list(iter_csv(write(tmp_path, HEADER + padding + "S-x,\x00,a,b,c,d,e\n")))
    assert info.value.code == "file_encoding" and "NUL" in info.value.message


def test_header_messages_are_sanitized(tmp_path: Path) -> None:
    hostile = "a\x07b" + "x" * 500
    with pytest.raises(FileRejected) as info:
        list(iter_csv(write(tmp_path, f"{HEADER.strip()},{hostile},{hostile}\n")))
    assert "\x07" not in info.value.message and len(info.value.message) < 300


def test_decoded_batch_size_guard(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(readers, "MAX_BATCH_BYTES", 10)
    with pytest.raises(FileRejected) as info:
        list(iter_parquet(parquet(tmp_path, good_columns(5)), batch_rows=10))
    assert (info.value.code, info.value.row_number) == ("file_too_large_uncompressed", 1)
