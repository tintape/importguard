"""Duplicate, conflict and atomicity rules, end to end through the worker."""

from __future__ import annotations

from decimal import Decimal

from sqlalchemy import Engine, text

from importguard.config import Settings
from tests.integration.conftest import drain, errors_of, job_row, scalar, submit

HEADER = "sale_id,sold_at,store_id,product_category,quantity,unit_price,currency\n"


def csv(*rows: str) -> bytes:
    return (HEADER + "".join(r + "\n" for r in rows)).encode()


A = "A-1,2026-09-01T10:00:00Z,BER-01,coffee,2,3.50,EUR"
B = "A-2,2026-09-01T11:00:00Z,BER-01,tea,1,2.10,EUR"
C = "A-3,2026-09-01T12:00:00Z,LON-01,coffee,3,4.00,GBP"
# A again, spelled differently but equal after normalization:
A_EQUIVALENT = " A-1 ,2026-09-01T12:00:00+02:00,ber-01, Coffee ,2,3.5,eur"
A_CHANGED_PRICE = "A-1,2026-09-01T10:00:00Z,BER-01,coffee,2,3.90,EUR"


def sales_count(engine: Engine) -> int:
    return int(scalar(engine, "SELECT count(*) FROM sales"))  # type: ignore[arg-type]


def test_new_sales_are_added_with_exact_decimal_amounts(
    engine: Engine, worker_engine: Engine, settings: Settings
) -> None:
    job = submit(engine, settings, "pos", csv(A, B, C))
    [outcome] = drain(worker_engine, settings)

    assert (outcome.status, outcome.rows_read, outcome.rows_inserted, outcome.rows_skipped) == ("succeeded", 3, 3, 0)
    row = job_row(engine, job)
    assert (row.status, row.rows_inserted, row.error_count, row.attempts) == ("succeeded", 3, 0, 1)
    assert row.finished_at is not None
    eur = scalar(engine, "SELECT sum(amount) FROM sales WHERE currency = 'EUR'")
    assert eur == Decimal("9.10")  # 2 * 3.50 + 1 * 2.10, exact
    assert isinstance(eur, Decimal)


def test_reuploading_identical_content_adds_nothing(engine: Engine, worker_engine: Engine, settings: Settings) -> None:
    submit(engine, settings, "pos", csv(A, B), name="day1.csv")
    submit(engine, settings, "pos", csv(A, B), name="day1 (copy).csv")
    first, second = drain(worker_engine, settings)

    assert (first.rows_inserted, first.rows_skipped) == (2, 0)
    assert (second.status, second.rows_inserted, second.rows_skipped) == ("succeeded", 0, 2)
    assert sales_count(engine) == 2


def test_equivalent_spelling_is_an_exact_duplicate_not_a_conflict(
    engine: Engine, worker_engine: Engine, settings: Settings
) -> None:
    submit(engine, settings, "pos", csv(A))
    submit(engine, settings, "pos", csv(A_EQUIVALENT, B))
    _, second = drain(worker_engine, settings)

    assert (second.status, second.rows_inserted, second.rows_skipped) == ("succeeded", 1, 1)
    assert sales_count(engine) == 2


def test_conflict_with_existing_sale_rejects_whole_file_and_changes_nothing(
    engine: Engine, worker_engine: Engine, settings: Settings
) -> None:
    submit(engine, settings, "pos", csv(A, B))
    drain(worker_engine, settings)
    before = scalar(
        engine, "SELECT string_agg(concat_ws('|', sale_id, unit_price, import_job_id), ',' ORDER BY sale_id) FROM sales"
    )

    job = submit(engine, settings, "pos", csv(C, A_CHANGED_PRICE))  # C is new, A conflicts
    [outcome] = drain(worker_engine, settings)

    assert (outcome.status, outcome.failure_kind, outcome.error_count) == ("failed", "conflict", 1)
    assert errors_of(engine, job) == [(2, "unit_price", "conflict_existing")]
    assert sales_count(engine) == 2  # C was NOT imported: strict mode
    after = scalar(
        engine, "SELECT string_agg(concat_ws('|', sale_id, unit_price, import_job_id), ',' ORDER BY sale_id) FROM sales"
    )
    assert after == before
    with engine.connect() as conn:
        message = conn.execute(text("SELECT message FROM import_errors WHERE job_id = :id"), {"id": job}).scalar_one()
    assert "existing 3.50, file 3.90" in message


def test_exact_repeat_inside_one_file_is_skipped(engine: Engine, worker_engine: Engine, settings: Settings) -> None:
    submit(engine, settings, "pos", csv(A, B, A, A_EQUIVALENT))
    [outcome] = drain(worker_engine, settings)

    assert (outcome.status, outcome.rows_read, outcome.rows_inserted, outcome.rows_skipped) == ("succeeded", 4, 2, 2)
    assert sales_count(engine) == 2


def test_conflicting_repeat_inside_one_file_rejects_the_file(
    engine: Engine, worker_engine: Engine, settings: Settings
) -> None:
    job = submit(engine, settings, "pos", csv(A, B, A_CHANGED_PRICE))
    [outcome] = drain(worker_engine, settings)

    assert (outcome.status, outcome.failure_kind) == ("failed", "conflict")
    assert errors_of(engine, job) == [(3, "unit_price", "conflict_in_file")]
    assert sales_count(engine) == 0


def test_same_sale_id_in_different_sources_is_independent(
    engine: Engine, worker_engine: Engine, settings: Settings
) -> None:
    submit(engine, settings, "pos", csv(A))
    submit(engine, settings, "webshop", csv(A_CHANGED_PRICE))  # same id, other source: not a conflict
    outcomes = drain(worker_engine, settings)

    assert [o.status for o in outcomes] == ["succeeded", "succeeded"]
    assert sales_count(engine) == 2


def test_invalid_rows_reject_the_file_with_row_numbers(
    engine: Engine, worker_engine: Engine, settings: Settings
) -> None:
    job = submit(
        engine,
        settings,
        "pos",
        csv(
            A,
            "A-9,2026-02-30T10:00:00Z,BER-01,coffee,1,1.00,EUR",
            "A-10,2026-09-01T10:00:00,BER-01,coffee,1,1.00,EUR",
            'A-11,2026-09-01T10:00:00Z,BER-01,coffee,1,"1,50",EUR',
            "A-12,2026-09-01T10:00:00Z,BER-01,coffee,1,1.005,JPY",
            "only,three,cells",
        ),
    )
    [outcome] = drain(worker_engine, settings)

    assert (outcome.status, outcome.failure_kind, outcome.rows_read) == ("failed", "validation", 6)
    assert errors_of(engine, job) == [
        (2, "sold_at", "invalid_date"),
        (3, "sold_at", "naive_timestamp"),
        (4, "unit_price", "invalid_format"),
        (5, "unit_price", "precision"),
        (5, "currency", "unsupported_currency"),
        (6, None, "column_count"),
    ]
    assert sales_count(engine) == 0


def test_validation_and_conflicts_are_reported_together(
    engine: Engine, worker_engine: Engine, settings: Settings
) -> None:
    submit(engine, settings, "pos", csv(A))
    drain(worker_engine, settings)
    job = submit(engine, settings, "pos", csv(A_CHANGED_PRICE, "bad id!,2026-09-01T10:00:00Z,X,y,1,1.00,EUR"))
    [outcome] = drain(worker_engine, settings)

    assert outcome.failure_kind == "validation"
    assert errors_of(engine, job) == [(1, "unit_price", "conflict_existing"), (2, "sale_id", "invalid_format")]


def test_bad_header_is_a_file_level_error(engine: Engine, worker_engine: Engine, settings: Settings) -> None:
    job = submit(engine, settings, "pos", b"sale_id,sold_at,store,amount\nX,2026-09-01T10:00:00Z,S,1\n")
    [outcome] = drain(worker_engine, settings)

    assert outcome.status == "failed"
    assert errors_of(engine, job) == [(None, None, "header_missing_columns")]


def test_error_storage_is_capped_but_counted(engine: Engine, worker_engine: Engine, settings: Settings) -> None:
    rows = [f"X-{i},not-a-date,S,c,1,1.00,EUR" for i in range(250)]
    job = submit(engine, settings, "pos", csv(*rows))
    [outcome] = drain(worker_engine, settings)

    assert outcome.error_count == 250
    assert len(errors_of(engine, job)) == settings.max_stored_errors
    assert job_row(engine, job).error_count == 250


def test_row_limit(engine: Engine, worker_engine: Engine, settings: Settings) -> None:
    limited = settings.model_copy(update={"max_rows": 2})
    job = submit(engine, limited, "pos", csv(A, B, C))
    [outcome] = drain(worker_engine, limited)

    assert outcome.status == "failed"
    assert errors_of(engine, job) == [(3, None, "too_many_rows")]
    assert sales_count(engine) == 0
