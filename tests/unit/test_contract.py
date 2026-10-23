from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from importguard.contract import (
    InvalidField,
    normalize_category,
    normalize_currency,
    normalize_quantity,
    normalize_record,
    normalize_sale_id,
    normalize_sold_at,
    normalize_store_id,
    normalize_unit_price,
    preview,
)

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def raw(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "sale_id": "S-1",
        "sold_at": "2026-09-01T10:00:00Z",
        "store_id": "berlin-01",
        "product_category": "Coffee",
        "quantity": "2",
        "unit_price": "3.50",
        "currency": "eur",
    }
    return {**base, **overrides}


def code_of(func, value: object) -> str:  # type: ignore[no-untyped-def]
    with pytest.raises(InvalidField) as info:
        func(value)
    return info.value.code


class TestSoldAt:
    def test_offsets_normalize_to_the_same_utc_instant(self) -> None:
        a = normalize_sold_at("2026-09-01T12:00:00+02:00", now=NOW)
        b = normalize_sold_at("2026-09-01T10:00:00Z", now=NOW)
        c = normalize_sold_at("  2026-09-01T05:00:00-05:00 ", now=NOW)
        assert a == b == c == datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
        assert a.tzinfo is UTC

    def test_aware_datetime_from_parquet_is_accepted(self) -> None:
        moment = datetime(2026, 9, 1, 12, 0, tzinfo=timezone(timedelta(hours=2)))
        assert normalize_sold_at(moment, now=NOW) == datetime(2026, 9, 1, 10, 0, tzinfo=UTC)

    @pytest.mark.parametrize(
        ("value", "code"),
        [
            ("2026-09-01T10:00:00", "naive_timestamp"),
            ("2026-09-01 10:00", "naive_timestamp"),
            (datetime(2026, 9, 1, 10, 0), "naive_timestamp"),
            ("2026-02-30T10:00:00Z", "invalid_date"),
            ("2026-13-01T10:00:00Z", "invalid_date"),
            ("01.09.2026 10:00", "invalid_format"),
            ("2026-09-01T10:00:00.123Z", "invalid_format"),
            (datetime(2026, 9, 1, 10, 0, 0, 5, tzinfo=UTC), "invalid_format"),
            ("2026-09-01T10:00:00+15:00", "invalid_format"),
            ("0001-01-01T00:00:00+14:00", "out_of_range"),  # overflows when converted to UTC
            ("9999-12-31T23:59:59-14:00", "out_of_range"),
            ("1999-12-31T23:59:59Z", "out_of_range"),
            ("2026-10-03T12:00:01Z", "out_of_range"),
            ("", "required"),
            (None, "required"),
        ],
    )
    def test_rejects(self, value: object, code: str) -> None:
        assert code_of(lambda v: normalize_sold_at(v, now=NOW), value) == code

    def test_allows_one_day_of_clock_skew(self) -> None:
        assert normalize_sold_at("2026-10-02T12:00:00Z", now=NOW)


class TestUnitPrice:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("19.99", Decimal("19.99")),
            ("12.5", Decimal("12.50")),
            ("7", Decimal("7.00")),
            (" 0.01 ", Decimal("0.01")),
            ("12.500", Decimal("12.50")),
            ("1000000.00", Decimal("1000000.00")),
            (Decimal("4.2000"), Decimal("4.20")),
        ],
    )
    def test_accepts_and_quantizes(self, value: object, expected: Decimal) -> None:
        result = normalize_unit_price(value)
        assert result == expected
        assert result.as_tuple().exponent == -2

    @pytest.mark.parametrize(
        ("value", "code"),
        [
            ("12,50", "invalid_format"),
            ("1e3", "invalid_format"),
            ("-5.00", "invalid_format"),
            ("+5.00", "invalid_format"),
            ("$5.00", "invalid_format"),
            ("1,000.00", "invalid_format"),
            ("NaN", "invalid_format"),
            ("12.345", "precision"),
            (Decimal("1.001"), "precision"),
            (Decimal("0.001"), "out_of_range"),
            (Decimal("1E+30"), "out_of_range"),  # too big to quantize: must not crash
            (Decimal("-1E+30"), "out_of_range"),
            ("0.00", "out_of_range"),
            ("1000000.01", "out_of_range"),
            (Decimal("NaN"), "invalid_format"),
            (12.5, "invalid_type"),
            ("", "required"),
            (None, "required"),
        ],
    )
    def test_rejects(self, value: object, code: str) -> None:
        assert code_of(normalize_unit_price, value) == code


class TestQuantity:
    @pytest.mark.parametrize(("value", "expected"), [("1", 1), (" 25 ", 25), (10_000, 10_000)])
    def test_accepts(self, value: object, expected: int) -> None:
        assert normalize_quantity(value) == expected

    @pytest.mark.parametrize(
        ("value", "code"),
        [
            ("0", "out_of_range"),
            ("10001", "out_of_range"),
            ("-1", "invalid_format"),
            ("2.0", "invalid_format"),
            ("two", "invalid_format"),
            (True, "invalid_type"),
            ("", "required"),
        ],
    )
    def test_rejects(self, value: object, code: str) -> None:
        assert code_of(normalize_quantity, value) == code


class TestTextFields:
    def test_sale_id_is_trimmed_but_case_sensitive(self) -> None:
        assert normalize_sale_id("  ab-1.X_2 ") == "ab-1.X_2"
        assert normalize_sale_id("AB-1") != normalize_sale_id("ab-1")

    @pytest.mark.parametrize("value", ["-starts-with-dash", "has space", "=cmd()", "x" * 65])
    def test_sale_id_rejects(self, value: str) -> None:
        assert code_of(normalize_sale_id, value) == "invalid_format"

    def test_store_id_is_uppercased(self) -> None:
        assert normalize_store_id(" berlin-01 ") == "BERLIN-01"
        assert code_of(normalize_store_id, "berlin 01") == "invalid_format"

    def test_category_whitespace_case_and_unicode_are_normalized(self) -> None:
        assert normalize_category("  Hot   Drinks ") == "hot drinks"
        assert normalize_category("ＣＯＦＦＥＥ") == "coffee"  # full-width letters (NFKC)
        assert code_of(normalize_category, "bad\x07bell") == "invalid_format"
        assert code_of(normalize_category, "x" * 65) == "out_of_range"

    def test_currency(self) -> None:
        assert normalize_currency(" usd ") == "USD"
        assert code_of(normalize_currency, "JPY") == "unsupported_currency"
        assert code_of(normalize_currency, "EURO") == "unsupported_currency"


class TestRecord:
    def test_equivalent_spellings_normalize_to_equal_sales(self) -> None:
        a, _ = normalize_record(raw(), now=NOW)
        b, _ = normalize_record(
            raw(
                sold_at="2026-09-01T12:00:00+02:00",
                store_id="BERLIN-01",
                product_category=" coffee ",
                quantity=2,
                unit_price=Decimal("3.5"),
                currency="EUR",
            ),
            now=NOW,
        )
        assert a is not None
        assert a == b

    def test_collects_every_field_error(self) -> None:
        sale, errors = normalize_record(raw(sold_at="yesterday", unit_price="1,5", currency=None), now=NOW)
        assert sale is None
        assert [(e.field, e.code) for e in errors] == [
            ("sold_at", "invalid_format"),
            ("unit_price", "invalid_format"),
            ("currency", "required"),
        ]
        assert errors[0].raw_value == "yesterday"

    def test_missing_field_is_required_error(self) -> None:
        values = raw()
        del values["store_id"]
        _, errors = normalize_record(values, now=NOW)
        assert [(e.field, e.code) for e in errors] == [("store_id", "required")]


def test_preview_truncates_and_masks_control_characters() -> None:
    assert preview(None) is None
    assert preview("a\nb") == "a�b"
    long = preview("x" * 500)
    assert long is not None and len(long) == 120 and long.endswith("…")
