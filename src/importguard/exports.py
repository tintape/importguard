"""CSV exports, protected against spreadsheet formula injection.

Any text cell starting with ``=``, ``+``, ``-``, ``@``, tab or carriage return
is prefixed with a single quote so spreadsheet applications show it as text
instead of evaluating it (OWASP "CSV Injection" guidance).
"""

from __future__ import annotations

import csv
import io
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")

SALES_HEADER = (
    "source",
    "sale_id",
    "sold_at_utc",
    "sold_at_local",
    "store_id",
    "product_category",
    "quantity",
    "unit_price",
    "currency",
    "amount",
    "import_id",
)
ERRORS_HEADER = ("row", "field", "code", "message", "raw_value")


def safe_cell(value: object) -> object:
    if isinstance(value, str) and value.startswith(FORMULA_PREFIXES):
        return "'" + value
    return value


def _csv_lines(rows: Iterable[Iterable[object]]) -> Iterator[str]:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\r\n")
    for row in rows:
        writer.writerow([safe_cell(v) for v in row])
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate()


def sales_csv(rows: Iterable[Any], tz: ZoneInfo) -> Iterator[str]:
    def records() -> Iterator[tuple[object, ...]]:
        yield SALES_HEADER
        for r in rows:
            sold_at: datetime = r.sold_at
            yield (
                r.source_code,
                r.sale_id,
                sold_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                sold_at.astimezone(tz).isoformat(),
                r.store_id,
                r.product_category,
                r.quantity,
                f"{r.unit_price:.2f}",
                r.currency,
                f"{r.amount:.2f}",
                r.import_job_id,
            )

    return _csv_lines(records())


def errors_csv(rows: Iterable[Any]) -> Iterator[str]:
    def records() -> Iterator[tuple[object, ...]]:
        yield ERRORS_HEADER
        for r in rows:
            yield (
                r.row_number if r.row_number is not None else "",
                r.field or "",
                r.code,
                r.message,
                r.raw_value or "",
            )

    return _csv_lines(records())
