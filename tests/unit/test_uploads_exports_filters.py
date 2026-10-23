import io
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from importguard import faults
from importguard.exports import errors_csv, safe_cell, sales_csv
from importguard.reports import parse_filter
from importguard.uploads import UploadRejected, sanitize_filename, store_upload


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("sales.csv", "sales.csv"),
        ("../../etc/passwd", "passwd"),
        ("C:\\Users\\x\\report.parquet", "report.parquet"),
        ("..", "upload"),
        ("", "upload"),
        (None, "upload"),
        (".hidden.csv", "hidden.csv"),
        ("bad\x00name\n.csv", "badname.csv"),
        ("x" * 300 + ".csv", "x" * 255),
    ],
)
def test_sanitize_filename(raw: str | None, expected: str) -> None:
    assert sanitize_filename(raw) == expected


def test_store_upload_hashes_detects_format_and_uses_random_names(tmp_path: Path) -> None:
    data = b"sale_id,sold_at\n" * 1000
    stored = store_upload(io.BytesIO(data), "../evil.parquet", upload_dir=tmp_path, max_bytes=10**6)
    assert stored.file_format == "csv"  # content wins over the extension
    assert stored.size_bytes == len(data)
    assert len(stored.sha256) == 64
    assert stored.original_filename == "evil.parquet"
    assert [p.name for p in tmp_path.iterdir()] == [stored.stored_filename]


def test_store_upload_limits_and_cleanup(tmp_path: Path) -> None:
    with pytest.raises(UploadRejected) as info:
        store_upload(io.BytesIO(b"a" * 2048), "x.csv", upload_dir=tmp_path, max_bytes=1024)
    assert info.value.status_code == 413
    with pytest.raises(UploadRejected) as info:
        store_upload(io.BytesIO(b"\x00\x01binary"), "x.csv", upload_dir=tmp_path, max_bytes=1024)
    assert info.value.status_code == 415
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("=SUM(A1)", "'=SUM(A1)"),
        ("+1", "'+1"),
        ("-1", "'-1"),
        ("@x", "'@x"),
        ("\tx", "'\tx"),
        ("\rx", "'\rx"),
        ("safe", "safe"),
        (5, 5),
        (Decimal("1.00"), Decimal("1.00")),
    ],
)
def test_safe_cell(value: object, expected: object) -> None:
    assert safe_cell(value) == expected


def test_sales_csv_formats_money_and_times() -> None:
    row = SimpleNamespace(
        source_code="pos",
        sale_id="S-1",
        sold_at=datetime(2026, 9, 1, 22, 30, tzinfo=UTC),
        store_id="BER-01",
        product_category="=cmd",
        quantity=2,
        unit_price=Decimal("3.5"),
        currency="EUR",
        amount=Decimal("7"),
        import_job_id=1,
    )
    lines = list(sales_csv([row], ZoneInfo("Europe/Berlin")))
    assert lines[1] == "pos,S-1,2026-09-01T22:30:00Z,2026-09-02T00:30:00+02:00,BER-01,'=cmd,2,3.50,EUR,7.00,1\r\n"


def test_errors_csv() -> None:
    rows = [SimpleNamespace(row_number=None, field=None, code="empty_file", message="m", raw_value=None)]
    assert list(errors_csv(rows))[1] == ",,empty_file,m,\r\n"


def test_parse_filter() -> None:
    parsed = parse_filter(
        {"date_from": "2026-09-01", "date_to": "2026-09-30", "source": "2", "store": " ber-01 ", "currency": "eur"},
        valid_source_ids={1, 2},
    )
    assert parsed.errors == []
    assert parsed.value.date_from == date(2026, 9, 1) and parsed.value.store_id == "BER-01"
    assert parsed.value.currency == "EUR" and parsed.value.source_id == 2
    assert parsed.value.query_items()["store"] == "BER-01"


def test_parse_filter_reports_and_ignores_bad_values() -> None:
    parsed = parse_filter(
        {"date_from": "2026-10-01", "date_to": "2026-09-01", "source": "7", "store": "a b", "currency": "BTC"},
        valid_source_ids={1},
    )
    assert len(parsed.errors) == 4
    assert parsed.value.query_items() == {}
    assert parse_filter({"date_from": "x"}, valid_source_ids=set()).errors == [
        "From date must be a date like 2026-09-01."
    ]


def test_faults(monkeypatch: pytest.MonkeyPatch) -> None:
    faults.checkpoint("before_commit")  # disabled by default
    monkeypatch.setenv(faults.ENV_VAR, "before_commit=raise")
    with pytest.raises(faults.InjectedFault):
        faults.checkpoint("before_commit")
    faults.checkpoint("after_commit")
    monkeypatch.setenv(faults.ENV_VAR, "after_claim=sleep:0")
    faults.checkpoint("after_claim")
    monkeypatch.setenv(faults.ENV_VAR, "nowhere=crash")
    with pytest.raises(ValueError, match="unknown checkpoint"):
        faults.checkpoint("after_claim")
    monkeypatch.setenv(faults.ENV_VAR, "after_claim=explode")
    with pytest.raises(ValueError, match="unknown action"):
        faults.checkpoint("after_claim")
