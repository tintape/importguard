"""Read-side queries for the dashboard, the sales table and CSV export.

Only the ``sales`` table is read, and it only ever contains sales from fully
successful imports, so partially processed files can never show up here.
Money is aggregated in PostgreSQL ``numeric`` and returned as ``Decimal``;
currencies are always reported separately.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import Connection, Date, Select, and_, cast, func, select

from importguard.contract import STORE_ID_RE, SUPPORTED_CURRENCIES
from importguard.models import sales, sources

CENT = Decimal("0.01")
MAX_CHART_DAYS = 120
EXPORT_BATCH_ROWS = 2_000


@dataclass(frozen=True, slots=True)
class SalesFilter:
    date_from: date | None = None
    date_to: date | None = None
    source_id: int | None = None
    store_id: str | None = None
    currency: str | None = None

    def query_items(self) -> dict[str, str]:
        """Non-empty filter values, for building links that keep the current filters."""
        items = {
            "date_from": self.date_from.isoformat() if self.date_from else "",
            "date_to": self.date_to.isoformat() if self.date_to else "",
            "source": str(self.source_id) if self.source_id else "",
            "store": self.store_id or "",
            "currency": self.currency or "",
        }
        return {k: v for k, v in items.items() if v}


@dataclass(frozen=True, slots=True)
class ParsedFilter:
    value: SalesFilter
    errors: list[str] = field(default_factory=list)


def parse_filter(params: Mapping[str, str], *, valid_source_ids: set[int]) -> ParsedFilter:
    """Parse query parameters leniently: invalid values are reported and ignored."""
    errors: list[str] = []

    def parse_date(name: str, label: str) -> date | None:
        raw = (params.get(name) or "").strip()
        if not raw:
            return None
        try:
            return date.fromisoformat(raw)
        except ValueError:
            errors.append(f"{label} must be a date like 2026-09-01.")
            return None

    date_from = parse_date("date_from", "From date")
    date_to = parse_date("date_to", "To date")
    if date_from and date_to and date_from > date_to:
        errors.append("From date is after To date; the date filter was ignored.")
        date_from = date_to = None

    source_id: int | None = None
    raw_source = (params.get("source") or "").strip()
    if raw_source:
        if raw_source.isdigit() and int(raw_source) in valid_source_ids:
            source_id = int(raw_source)
        else:
            errors.append("Unknown source; the source filter was ignored.")

    store_id: str | None = (params.get("store") or "").strip().upper() or None
    if store_id and not STORE_ID_RE.fullmatch(store_id):
        errors.append("Store IDs contain only letters, digits, '_' and '-'.")
        store_id = None

    currency: str | None = (params.get("currency") or "").strip().upper() or None
    if currency and currency not in SUPPORTED_CURRENCIES:
        errors.append("Unsupported currency; the currency filter was ignored.")
        currency = None

    return ParsedFilter(SalesFilter(date_from, date_to, source_id, store_id, currency), errors)


def _conditions(flt: SalesFilter, tz: ZoneInfo) -> list[Any]:
    conditions: list[Any] = []
    if flt.date_from:
        conditions.append(sales.c.sold_at >= datetime.combine(flt.date_from, time.min, tz))
    if flt.date_to:
        conditions.append(sales.c.sold_at < datetime.combine(flt.date_to + timedelta(days=1), time.min, tz))
    if flt.source_id:
        conditions.append(sales.c.source_id == flt.source_id)
    if flt.store_id:
        conditions.append(sales.c.store_id == flt.store_id)
    if flt.currency:
        conditions.append(sales.c.currency == flt.currency)
    return conditions


def _filtered(query: Select[Any], flt: SalesFilter, tz: ZoneInfo) -> Select[Any]:
    conditions = _conditions(flt, tz)
    return query.where(and_(*conditions)) if conditions else query


@dataclass(frozen=True, slots=True)
class CurrencyKpi:
    currency: str
    sales_count: int
    revenue: Decimal
    average_sale: Decimal


@dataclass(frozen=True, slots=True)
class DailyPoint:
    day: date
    sales_count: int
    revenue: Decimal


@dataclass(frozen=True, slots=True)
class DailySeries:
    currency: str
    points: list[DailyPoint]
    max_revenue: Decimal
    truncated: bool


def currency_kpis(conn: Connection, flt: SalesFilter, tz: ZoneInfo) -> list[CurrencyKpi]:
    query = _filtered(
        select(sales.c.currency, func.count().label("n"), func.sum(sales.c.amount).label("revenue")),
        flt,
        tz,
    ).group_by(sales.c.currency)
    rows = conn.execute(query.order_by(func.sum(sales.c.amount).desc(), sales.c.currency)).all()
    return [
        CurrencyKpi(
            currency=r.currency,
            sales_count=int(r.n),
            revenue=Decimal(r.revenue),
            average_sale=(Decimal(r.revenue) / int(r.n)).quantize(CENT, rounding=ROUND_HALF_UP),
        )
        for r in rows
    ]


def daily_series(conn: Connection, flt: SalesFilter, tz: ZoneInfo) -> list[DailySeries]:
    local_day = cast(func.timezone(tz.key, sales.c.sold_at), Date).label("day")
    query = _filtered(
        select(local_day, sales.c.currency, func.count().label("n"), func.sum(sales.c.amount).label("revenue")),
        flt,
        tz,
    ).group_by(local_day, sales.c.currency)
    grouped: dict[str, list[DailyPoint]] = {}
    for row in conn.execute(query.order_by(local_day)):
        grouped.setdefault(row.currency, []).append(DailyPoint(row.day, int(row.n), Decimal(row.revenue)))
    result = []
    for currency, points in grouped.items():
        filled = _fill_gaps(points)
        truncated = len(filled) > MAX_CHART_DAYS
        shown = filled[-MAX_CHART_DAYS:]
        result.append(DailySeries(currency, shown, max(p.revenue for p in shown), truncated))
    return sorted(result, key=lambda s: -sum(p.revenue for p in s.points))


def _fill_gaps(points: list[DailyPoint]) -> list[DailyPoint]:
    """Insert zero days so the chart's x axis is continuous."""
    by_day = {p.day: p for p in points}
    day, last = points[0].day, points[-1].day
    filled = []
    while day <= last:
        filled.append(by_day.get(day, DailyPoint(day, 0, Decimal("0.00"))))
        day += timedelta(days=1)
    return filled


def store_ids(conn: Connection) -> list[str]:
    return list(conn.execute(select(sales.c.store_id).distinct().order_by(sales.c.store_id)).scalars())


def currencies_in_use(conn: Connection) -> list[str]:
    return list(conn.execute(select(sales.c.currency).distinct().order_by(sales.c.currency)).scalars())


_SALE_COLUMNS = (
    sources.c.code.label("source_code"),
    sources.c.name.label("source_name"),
    sales.c.sale_id,
    sales.c.sold_at,
    sales.c.store_id,
    sales.c.product_category,
    sales.c.quantity,
    sales.c.unit_price,
    sales.c.currency,
    sales.c.amount,
    sales.c.import_job_id,
)


def _sales_query(flt: SalesFilter, tz: ZoneInfo) -> Select[Any]:
    base = select(*_SALE_COLUMNS).select_from(sales.join(sources, sources.c.id == sales.c.source_id))
    return _filtered(base, flt, tz).order_by(sales.c.sold_at.desc(), sales.c.id.desc())


def count_sales(conn: Connection, flt: SalesFilter, tz: ZoneInfo) -> int:
    return int(conn.execute(_filtered(select(func.count()).select_from(sales), flt, tz)).scalar_one())


def list_sales(conn: Connection, flt: SalesFilter, tz: ZoneInfo, *, limit: int, offset: int) -> list[Any]:
    return list(conn.execute(_sales_query(flt, tz).limit(limit).offset(offset)).all())


def iter_sales(conn: Connection, flt: SalesFilter, tz: ZoneInfo) -> Iterator[Any]:
    """Stream all matching sales with a server-side cursor (constant memory)."""
    result = conn.execution_options(stream_results=True, yield_per=EXPORT_BATCH_ROWS).execute(_sales_query(flt, tz))
    yield from result
