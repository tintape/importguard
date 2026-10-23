"""The input data contract: field rules and normalization of one sale record.

Both readers (CSV and Parquet) hand raw cell values to :func:`normalize_record`.
CSV cells are always ``str``; Parquet cells arrive typed (``int``, ``Decimal``,
``datetime``). Every accepted value is turned into one canonical form, so two
records describing the same sale compare equal regardless of file format or
cosmetic formatting (whitespace, letter case, UTC offset spelling, ``12.5`` vs
``12.50``).
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation

FIELDS: tuple[str, ...] = (
    "sale_id",
    "sold_at",
    "store_id",
    "product_category",
    "quantity",
    "unit_price",
    "currency",
)

# Fields compared to decide "exact duplicate" vs "conflict" (everything except the key).
SIGNIFICANT_FIELDS: tuple[str, ...] = FIELDS[1:]

SALE_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
STORE_ID_RE = re.compile(r"[A-Z0-9][A-Z0-9_-]{0,31}")
TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})")
QUANTITY_RE = re.compile(r"[0-9]{1,9}")
PRICE_RE = re.compile(r"[0-9]{1,12}(\.[0-9]{1,6})?")

MAX_CATEGORY_LENGTH = 64
MIN_QUANTITY = 1
MAX_QUANTITY = 10_000
MIN_PRICE = Decimal("0.01")
MAX_PRICE = Decimal("1000000.00")
PRICE_STEP = Decimal("0.01")
EARLIEST_SALE = datetime(2000, 1, 1, tzinfo=UTC)
FUTURE_TOLERANCE = timedelta(days=1)

# ISO 4217 currencies with exactly two minor units. Different currencies are never summed.
SUPPORTED_CURRENCIES: frozenset[str] = frozenset(
    {"AUD", "CAD", "CHF", "CZK", "DKK", "EUR", "GBP", "NOK", "PLN", "SEK", "USD"}
)

RAW_VALUE_PREVIEW = 120


@dataclass(frozen=True, slots=True)
class Sale:
    """A validated, normalized sale record."""

    sale_id: str
    sold_at: datetime  # timezone-aware, UTC
    store_id: str
    product_category: str
    quantity: int
    unit_price: Decimal  # exactly two decimal places
    currency: str

    def significant(self) -> tuple[object, ...]:
        return tuple(getattr(self, name) for name in SIGNIFICANT_FIELDS)


@dataclass(frozen=True, slots=True)
class FieldError:
    field: str
    code: str
    message: str
    raw_value: str | None


class InvalidField(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def preview(value: object) -> str | None:
    """Short printable representation of a raw value, for error reports."""
    if value is None:
        return None
    text = value if isinstance(value, str) else str(value)
    text = "".join(ch if ch.isprintable() else "�" for ch in text)
    return text if len(text) <= RAW_VALUE_PREVIEW else text[: RAW_VALUE_PREVIEW - 1] + "…"


def _require_text(value: object) -> str:
    if value is None:
        raise InvalidField("required", "value is required")
    if not isinstance(value, str):
        raise InvalidField("invalid_type", f"expected text, got {type(value).__name__}")
    stripped = value.strip()
    if not stripped:
        raise InvalidField("required", "value is required")
    return stripped


def normalize_sale_id(value: object) -> str:
    text = _require_text(value)
    if not SALE_ID_RE.fullmatch(text):
        raise InvalidField(
            "invalid_format",
            "sale_id must be 1-64 characters: letters, digits, '.', '_' or '-', starting with a letter or digit",
        )
    return text


def normalize_store_id(value: object) -> str:
    text = _require_text(value).upper()
    if not STORE_ID_RE.fullmatch(text):
        raise InvalidField(
            "invalid_format",
            "store_id must be 1-32 characters: letters, digits, '_' or '-'",
        )
    return text


def normalize_category(value: object) -> str:
    text = unicodedata.normalize("NFKC", _require_text(value))
    if any(not ch.isprintable() for ch in text):
        raise InvalidField("invalid_format", "product_category contains control characters")
    text = " ".join(text.split()).casefold()
    if len(text) > MAX_CATEGORY_LENGTH:
        raise InvalidField("out_of_range", f"product_category is longer than {MAX_CATEGORY_LENGTH} characters")
    return text


def normalize_sold_at(value: object, *, now: datetime) -> datetime:
    if isinstance(value, datetime):
        moment = value
        if moment.tzinfo is None or moment.utcoffset() is None:
            raise InvalidField("naive_timestamp", "timestamp has no time zone")
        if moment.microsecond:
            raise InvalidField("invalid_format", "sub-second precision is not allowed")
    else:
        text = _require_text(value)
        if not TIMESTAMP_RE.fullmatch(text):
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2})?", text):
                raise InvalidField(
                    "naive_timestamp",
                    "timestamp has no UTC offset; expected e.g. 2026-09-01T14:05:00+02:00",
                )
            raise InvalidField(
                "invalid_format",
                "expected ISO 8601 'YYYY-MM-DDTHH:MM:SS' with 'Z' or '+HH:MM' offset",
            )
        try:
            moment = datetime.fromisoformat(text)
        except ValueError as exc:
            raise InvalidField("invalid_date", f"not a real calendar date/time: {exc}") from exc
        offset = moment.utcoffset()
        if offset is None or abs(offset) > timedelta(hours=14):
            raise InvalidField("invalid_format", "UTC offset must be between -14:00 and +14:00")
    moment = moment.astimezone(UTC)
    if moment < EARLIEST_SALE:
        raise InvalidField("out_of_range", "sold_at is before 2000-01-01")
    if moment > now + FUTURE_TOLERANCE:
        raise InvalidField("out_of_range", "sold_at is in the future")
    return moment


def normalize_quantity(value: object) -> int:
    if isinstance(value, bool):
        raise InvalidField("invalid_type", "expected an integer")
    if isinstance(value, int):
        number = value
    else:
        text = _require_text(value)
        if not QUANTITY_RE.fullmatch(text):
            raise InvalidField("invalid_format", "quantity must be a whole number, e.g. 3")
        number = int(text)
    if not MIN_QUANTITY <= number <= MAX_QUANTITY:
        raise InvalidField("out_of_range", f"quantity must be between {MIN_QUANTITY} and {MAX_QUANTITY}")
    return number


def normalize_unit_price(value: object) -> Decimal:
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise InvalidField("invalid_format", "unit_price must be a finite number")
        amount = value
    elif isinstance(value, str):
        text = _require_text(value)
        if not PRICE_RE.fullmatch(text):
            raise InvalidField(
                "invalid_format",
                "unit_price must be a plain decimal with '.' separator, e.g. 19.99 "
                "(no signs, currency symbols, thousand separators or exponents)",
            )
        try:
            amount = Decimal(text)
        except InvalidOperation as exc:  # pragma: no cover - guarded by PRICE_RE
            raise InvalidField("invalid_format", "unit_price is not a number") from exc
    elif value is None:
        raise InvalidField("required", "value is required")
    else:
        raise InvalidField(
            "invalid_type",
            f"unit_price must be a decimal or text, got {type(value).__name__} "
            "(binary floating point is not accepted for money)",
        )
    quantized = amount.quantize(PRICE_STEP)
    if quantized != amount:
        raise InvalidField("precision", "unit_price has more than 2 decimal places")
    if not MIN_PRICE <= quantized <= MAX_PRICE:
        raise InvalidField("out_of_range", f"unit_price must be between {MIN_PRICE} and {MAX_PRICE}")
    return quantized


def normalize_currency(value: object) -> str:
    text = _require_text(value).upper()
    if text not in SUPPORTED_CURRENCIES:
        raise InvalidField(
            "unsupported_currency",
            "currency must be one of " + ", ".join(sorted(SUPPORTED_CURRENCIES)),
        )
    return text


def normalize_record(raw: dict[str, object], *, now: datetime) -> tuple[Sale | None, list[FieldError]]:
    """Validate and normalize one record. Returns the sale or the list of field errors."""
    normalizers = {
        "sale_id": normalize_sale_id,
        "sold_at": lambda v: normalize_sold_at(v, now=now),
        "store_id": normalize_store_id,
        "product_category": normalize_category,
        "quantity": normalize_quantity,
        "unit_price": normalize_unit_price,
        "currency": normalize_currency,
    }
    values: dict[str, object] = {}
    errors: list[FieldError] = []
    for name in FIELDS:
        raw_value = raw.get(name)
        try:
            values[name] = normalizers[name](raw_value)
        except InvalidField as exc:
            errors.append(FieldError(name, exc.code, exc.message, preview(raw_value)))
    if errors:
        return None, errors
    return Sale(**values), []  # type: ignore[arg-type]
