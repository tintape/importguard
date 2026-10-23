"""Deterministic synthetic demo data with independently computed expected results.

Run ``python -m importguard.sampledata --out demo-data``.

This module deliberately imports nothing from the import pipeline. Sales are
generated as canonical records with integer cents, then written to files with
cosmetic variations. Expected outcomes and totals are derived from the
canonical records, so tests that compare them with what the importer stored do
not repeat the importer's own logic (or its mistakes).

All data is synthetic. Store codes, categories and amounts are invented.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import shutil
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

SEED = 20260901
HEADER = ("sale_id", "sold_at", "store_id", "product_category", "quantity", "unit_price", "currency")
CATEGORIES = ("coffee", "tea", "pastry", "sandwiches", "cold drinks", "merchandise", "gift cards")
LARGE_ROWS = 100_000
START = datetime(2026, 8, 1, tzinfo=UTC)
DAYS = 31


@dataclass(frozen=True)
class Sale:
    source: str
    sale_id: str
    sold_at: datetime  # UTC
    store_id: str
    category: str
    quantity: int
    price_cents: int
    currency: str

    @property
    def amount_cents(self) -> int:
        return self.quantity * self.price_cents


@dataclass(frozen=True)
class Store:
    code: str
    currency: str
    utc_offset_hours: int


POS_STORES = (
    Store("BER-01", "EUR", 2),
    Store("BER-02", "EUR", 2),
    Store("MUC-01", "EUR", 2),
    Store("LON-01", "GBP", 1),
)
WEB_STORES = (Store("WEB-EU", "EUR", 2), Store("WEB-UK", "GBP", 1))
MARKET_STORES = (Store("MKT-DE", "EUR", 2), Store("MKT-PL", "PLN", 2), Store("MKT-SE", "SEK", 2))


def _price(rng: random.Random, currency: str) -> int:
    base = rng.choice((190, 250, 320, 450, 590, 790, 1290, 1990, 2490, 4990))
    factor = {"PLN": 4, "SEK": 11}.get(currency, 1)
    return base * factor + rng.choice((0, 0, 5, 9))


def _make_sales(
    rng: random.Random, *, source: str, prefix: str, start: int, count: int, stores: Sequence[Store]
) -> list[Sale]:
    sales = []
    for n in range(start, start + count):
        store = rng.choice(stores)
        sold_at = START + timedelta(days=rng.randrange(DAYS), seconds=rng.randrange(7 * 3600, 21 * 3600))
        sales.append(
            Sale(
                source=source,
                sale_id=f"{prefix}-{n:06d}",
                sold_at=sold_at.replace(microsecond=0),
                store_id=store.code,
                category=rng.choice(CATEGORIES),
                quantity=rng.choice((1, 1, 1, 2, 2, 3, 4, 6)),
                price_cents=_price(rng, store.currency),
                currency=store.currency,
            )
        )
    return sales


# --------------------------------------------------------------------------- formatting


def _price_text(cents: int, *, short: bool = False) -> str:
    text = f"{cents // 100}.{cents % 100:02d}"
    if short:  # "12.50" -> "12.5", "7.00" -> "7"
        text = text.rstrip("0").rstrip(".")
    return text


def _local_offset(store_id: str) -> int:
    for store in (*POS_STORES, *WEB_STORES, *MARKET_STORES):
        if store.code == store_id:
            return store.utc_offset_hours
    return 0


def canonical_row(sale: Sale) -> list[str]:
    return [
        sale.sale_id,
        sale.sold_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        sale.store_id,
        sale.category,
        str(sale.quantity),
        _price_text(sale.price_cents),
        sale.currency,
    ]


def cosmetic_row(sale: Sale) -> list[str]:
    """Same sale, written differently: local offset, other letter case, extra spaces, short price."""
    local = sale.sold_at.astimezone(timezone(timedelta(hours=_local_offset(sale.store_id))))
    return [
        f" {sale.sale_id}",
        local.isoformat(),
        sale.store_id.lower(),
        f"  {sale.category.title()} ",
        str(sale.quantity),
        _price_text(sale.price_cents, short=True),
        sale.currency.lower(),
    ]


def write_csv(path: Path, rows: Iterable[Sequence[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(HEADER)
        writer.writerows(rows)


def write_parquet(path: Path, sales: Sequence[Sale], *, row_group_size: int = 50_000) -> None:
    table = pa.table(
        {
            "sale_id": pa.array([s.sale_id for s in sales], pa.string()),
            "sold_at": pa.array([s.sold_at for s in sales], pa.timestamp("s", tz="UTC")),
            "store_id": pa.array([s.store_id for s in sales], pa.string()),
            "product_category": pa.array([s.category for s in sales], pa.string()),
            "quantity": pa.array([s.quantity for s in sales], pa.int32()),
            "unit_price": pa.array([Decimal(s.price_cents).scaleb(-2) for s in sales], pa.decimal128(12, 2)),
            "currency": pa.array([s.currency for s in sales], pa.string()),
        }
    )
    pq.write_table(table, path, row_group_size=row_group_size, compression="zstd")


# --------------------------------------------------------------------------- scenario


@dataclass
class Step:
    file: str
    source: str
    expect_status: str
    expect_inserted: int = 0
    expect_skipped: int = 0
    expect_failure_kind: str | None = None
    expect_errors: list[tuple[int | None, str]] | None = None
    note: str = ""
    adds: list[Sale] | None = None

    def as_json(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "file": self.file,
            "source": self.source,
            "expect_status": self.expect_status,
            "expect_inserted": self.expect_inserted,
            "expect_skipped": self.expect_skipped,
            "note": self.note,
        }
        if self.expect_failure_kind:
            data["expect_failure_kind"] = self.expect_failure_kind
        if self.expect_errors is not None:
            data["expect_errors"] = [{"row": r, "code": c} for r, c in self.expect_errors]
        return data


INVALID_ROWS: tuple[tuple[int, str, str, str], ...] = (
    # (data row number, broken field, value written, expected error code)
    (3, "sold_at", "2026-08-20T10:15:00", "naive_timestamp"),  # no UTC offset
    (8, "sold_at", "2026-02-30T10:15:00Z", "invalid_date"),  # impossible date
    (15, "sold_at", "20.08.2026 10:15", "invalid_format"),  # day-first format
    (21, "unit_price", "12,50", "invalid_format"),  # comma decimal separator
    (34, "unit_price", "1e3", "invalid_format"),  # exponent
    (40, "unit_price", "-4.00", "invalid_format"),  # negative price (a refund)
    (55, "unit_price", "3.999", "precision"),  # three decimals
    (61, "unit_price", "", "required"),  # missing price
    (77, "unit_price", "0.00", "out_of_range"),  # zero price
    (90, "currency", "JPY", "unsupported_currency"),  # no two minor units
)


def build(out: Path) -> dict[str, Any]:
    rng = random.Random(SEED)  # noqa: S311 - reproducible demo data, not security-related
    out.mkdir(parents=True, exist_ok=True)  # files with the same names are overwritten; nothing else is touched

    pos_day1 = _make_sales(rng, source="pos", prefix="POS", start=1, count=1_000, stores=POS_STORES)
    pos_new = _make_sales(rng, source="pos", prefix="POS", start=1_001, count=700, stores=POS_STORES)
    overlap = rng.sample(pos_day1, 300)
    pos_more = _make_sales(rng, source="pos", prefix="POS", start=1_701, count=40, stores=POS_STORES)
    web = _make_sales(rng, source="webshop", prefix="WEB", start=1, count=200, stores=WEB_STORES)
    web_eq = _make_sales(rng, source="webshop", prefix="WEB", start=201, count=300, stores=WEB_STORES)
    market_same_ids = [
        replace(s, source="marketplace", store_id="MKT-DE", currency="EUR") for s in rng.sample(pos_day1, 50)
    ]
    large = _make_sales(rng, source="marketplace", prefix="MKT", start=1, count=LARGE_ROWS, stores=MARKET_STORES)

    steps = _pos_steps(out, rng, pos_day1=pos_day1, pos_new=pos_new, overlap=overlap, pos_more=pos_more)
    steps += _other_steps(out, rng, web=web, web_eq=web_eq, market_same_ids=market_same_ids, large=large)
    expected = _expected(steps)
    (out / "expected.json").write_text(json.dumps(expected, indent=2) + "\n", encoding="utf-8")
    return expected


def _pos_steps(
    out: Path,
    rng: random.Random,
    *,
    pos_day1: list[Sale],
    pos_new: list[Sale],
    overlap: list[Sale],
    pos_more: list[Sale],
) -> list[Step]:
    steps: list[Step] = []

    # 1. A valid daily export.
    write_csv(out / "01_pos_2026-08-31_valid.csv", (canonical_row(s) for s in pos_day1))
    steps.append(Step("01_pos_2026-08-31_valid.csv", "pos", "succeeded", 1_000, 0, note="valid export", adds=pos_day1))

    # 2. The same bytes under another name.
    shutil.copyfile(out / "01_pos_2026-08-31_valid.csv", out / "02_pos_2026-08-31_valid (copy).csv")
    steps.append(
        Step("02_pos_2026-08-31_valid (copy).csv", "pos", "succeeded", 0, 1_000, note="identical content, renamed")
    )

    # 3. Next export overlaps the previous one; overlapping rows are spelled differently.
    rows = [cosmetic_row(s) for s in overlap] + [canonical_row(s) for s in pos_new]
    rng.shuffle(rows)
    write_csv(out / "03_pos_2026-09-01_overlapping.csv", rows)
    steps.append(
        Step(
            "03_pos_2026-09-01_overlapping.csv", "pos", "succeeded", 700, 300, note="300 already imported", adds=pos_new
        )
    )

    # 4. A corrected amount for an existing sale: a conflict, whole file rejected.
    changed = pos_day1[0]
    conflicting = replace(changed, price_cents=changed.price_cents + 100)
    rows = [canonical_row(s) for s in pos_more] + [canonical_row(s) for s in pos_day1[1:60]]
    rows.insert(17, canonical_row(conflicting))
    write_csv(out / "04_pos_2026-09-02_conflicting_amount.csv", rows)
    steps.append(
        Step(
            "04_pos_2026-09-02_conflicting_amount.csv",
            "pos",
            "failed",
            expect_failure_kind="conflict",
            expect_errors=[(18, "conflict_existing")],
            note=f"{changed.sale_id} re-sent with a different price; the 40 new sales are NOT imported",
        )
    )

    # 5. Invalid dates and money values.
    good = _make_sales(rng, source="pos", prefix="POS", start=2_001, count=100, stores=POS_STORES)
    rows = [canonical_row(s) for s in good]
    for position, field, value, _ in INVALID_ROWS:
        rows[position - 1][HEADER.index(field)] = value
    write_csv(out / "05_pos_2026-09-03_invalid_values.csv", rows)
    steps.append(
        Step(
            "05_pos_2026-09-03_invalid_values.csv",
            "pos",
            "failed",
            expect_failure_kind="validation",
            expect_errors=[(pos, code) for pos, _, _, code in INVALID_ROWS],
            note="dates and money values that break the contract",
        )
    )

    return steps


def _other_steps(
    out: Path,
    rng: random.Random,
    *,
    web: list[Sale],
    web_eq: list[Sale],
    market_same_ids: list[Sale],
    large: list[Sale],
) -> list[Step]:
    steps: list[Step] = []
    # 6. Repeated IDs inside one file: exact repeats are skipped.
    repeats = rng.sample(web, 20)
    rows = [canonical_row(s) for s in web] + [cosmetic_row(s) for s in repeats]
    write_csv(out / "06_webshop_repeated_ids.csv", rows)
    steps.append(
        Step(
            "06_webshop_repeated_ids.csv",
            "webshop",
            "succeeded",
            200,
            20,
            note="20 exact repeats inside the file",
            adds=web,
        )
    )

    # 6b. A repeated ID with different data inside one file: rejected.
    fresh = _make_sales(rng, source="webshop", prefix="WEB", start=900, count=10, stores=WEB_STORES)
    rows = [canonical_row(s) for s in fresh] + [canonical_row(replace(fresh[3], quantity=fresh[3].quantity + 1))]
    write_csv(out / "07_webshop_conflicting_repeat.csv", rows)
    steps.append(
        Step(
            "07_webshop_conflicting_repeat.csv",
            "webshop",
            "failed",
            expect_failure_kind="conflict",
            expect_errors=[(11, "conflict_in_file")],
            note=f"{fresh[3].sale_id} appears twice with different quantity",
        )
    )

    # 7. Equivalent CSV and Parquet.
    write_csv(out / "08_webshop_equivalent.csv", (canonical_row(s) for s in web_eq))
    write_parquet(out / "08_webshop_equivalent.parquet", web_eq)
    steps.append(Step("08_webshop_equivalent.csv", "webshop", "succeeded", 300, 0, note="CSV version", adds=web_eq))
    steps.append(
        Step(
            "08_webshop_equivalent.parquet",
            "webshop",
            "succeeded",
            0,
            300,
            note="same sales as Parquet: all duplicates",
        )
    )

    # 8. The same sale IDs in another source are independent.
    write_csv(out / "09_marketplace_ids_shared_with_pos.csv", (canonical_row(s) for s in market_same_ids))
    steps.append(
        Step(
            "09_marketplace_ids_shared_with_pos.csv",
            "marketplace",
            "succeeded",
            50,
            0,
            note="IDs exist in pos, not a conflict",
            adds=market_same_ids,
        )
    )

    # 9. Large file, both formats.
    write_parquet(out / "10_marketplace_large_100k.parquet", large)
    write_csv(out / "10_marketplace_large_100k.csv", (canonical_row(s) for s in large))
    steps.append(
        Step(
            "10_marketplace_large_100k.parquet",
            "marketplace",
            "succeeded",
            LARGE_ROWS,
            0,
            note="100,000 rows",
            adds=large,
        )
    )

    return steps


def _expected(steps: Sequence[Step]) -> dict[str, Any]:
    ledger: dict[tuple[str, str], Sale] = {}
    for step in steps:
        for sale in step.adds or []:
            key = (sale.source, sale.sale_id)
            if key in ledger:  # pragma: no cover - generator invariant
                raise AssertionError(f"generator produced {key} twice")
            ledger[key] = sale
    by_currency: dict[str, dict[str, Any]] = {}
    for sale in ledger.values():
        bucket = by_currency.setdefault(sale.currency, {"sales": 0, "revenue_cents": 0})
        bucket["sales"] += 1
        bucket["revenue_cents"] += sale.amount_cents
    totals = {
        cur: {"sales": b["sales"], "revenue": _price_text(b["revenue_cents"])} for cur, b in sorted(by_currency.items())
    }
    by_source: dict[str, int] = {}
    for source, _ in ledger:
        by_source[source] = by_source.get(source, 0) + 1
    return {
        "seed": SEED,
        "generated_by": "importguard.sampledata (independent of the import code)",
        "steps": [s.as_json() for s in steps],
        "final": {"sales": len(ledger), "by_source": dict(sorted(by_source.items())), "by_currency": totals},
        "small_scenario_final": _subtotal(ledger, exclude_prefix="MKT-"),
    }


def _subtotal(ledger: dict[tuple[str, str], Sale], *, exclude_prefix: str) -> dict[str, Any]:
    """Totals without the large file, for fast tests that skip step 10."""
    kept = [s for (_, sid), s in ledger.items() if not sid.startswith(exclude_prefix)]
    cents: dict[str, list[int]] = {}
    for s in kept:
        cents.setdefault(s.currency, [0, 0])
        cents[s.currency][0] += 1
        cents[s.currency][1] += s.amount_cents
    return {
        "sales": len(kept),
        "by_currency": {c: {"sales": n, "revenue": _price_text(v)} for c, (n, v) in sorted(cents.items())},
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate ImportGuard demo data (synthetic, deterministic).")
    parser.add_argument("--out", type=Path, default=Path("demo-data"))
    args = parser.parse_args(argv)
    expected = build(args.out)
    print(f"wrote {len(expected['steps'])} scenario files to {args.out}/ (seed {SEED})")
    for step in expected["steps"]:
        outcome = step["expect_status"]
        print(f"  {step['file']:<42} {step['source']:<12} -> {outcome:<9} {step['note']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
