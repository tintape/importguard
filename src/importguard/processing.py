"""Claiming and processing import jobs.

The integrity mechanism, in short (details in docs/architecture-decisions.md):

* A worker claims a job by taking a *session-level* PostgreSQL advisory lock
  keyed by the job id, on a dedicated connection, and holds it until it is
  done. If the worker dies, PostgreSQL drops the connection and the lock with
  it, so a job in ``processing`` whose lock is free is an orphan and is simply
  claimed again. No lease timeouts are needed.
* Each claim increments ``attempts``; the attempt number is a fencing token
  checked (under ``SELECT ... FOR UPDATE``) before anything is written.
* Validation streams normalized rows into a ``TEMP`` staging table. Nothing in
  ``sales`` changes during validation.
* Publication runs in the same transaction: lock the source row (serializes
  publications per source), detect conflicts, insert only new sales, mark the
  job ``succeeded``. Sales and the job result become visible in one commit.
* Re-processing a file is harmless: rows whose ``(source_id, sale_id)`` already
  exists with identical data are skipped, so nothing is ever counted twice.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import Connection, Row, text

from importguard import faults
from importguard.config import Settings
from importguard.contract import SIGNIFICANT_FIELDS, Sale, normalize_record
from importguard.readers import FileRejected, iter_rows, parquet_row_count

log = logging.getLogger(__name__)

STAGING_COLUMNS = ("row_number", "sale_id", *SIGNIFICANT_FIELDS)
_SIG = ", ".join(SIGNIFICANT_FIELDS)
_S_SIG = ", ".join(f"s.{c}" for c in SIGNIFICANT_FIELDS)


class LostClaim(RuntimeError):
    """The job is no longer ours (status or attempt changed). Nothing was written."""


@dataclass(frozen=True, slots=True)
class Claim:
    job_id: int
    attempt: int


@dataclass(frozen=True, slots=True)
class RowError:
    row_number: int | None
    field: str | None
    code: str
    message: str
    raw_value: str | None = None


MAX_MESSAGE_CHARS = 1_000


def _clean(text: str | None) -> str | None:
    """PostgreSQL text cannot hold NUL; messages are capped so a hostile file cannot bloat the database."""
    if text is None:
        return None
    text = text.replace("\x00", "\ufffd")
    return text if len(text) <= MAX_MESSAGE_CHARS else text[: MAX_MESSAGE_CHARS - 1] + "\u2026"


@dataclass(slots=True)
class ErrorCollector:
    limit: int
    stored: list[RowError] = field(default_factory=list)
    total: int = 0
    validation_total: int = 0

    def add(self, error: RowError, *, validation: bool = True) -> None:
        error = replace(error, message=_clean(error.message) or "", raw_value=_clean(error.raw_value))
        self.total += 1
        if validation:
            self.validation_total += 1
        if len(self.stored) < self.limit:
            self.stored.append(error)

    def count_only(self, count: int) -> None:
        """Count conflicts beyond the storage limit without keeping them."""
        self.total += count

    @property
    def room(self) -> int:
        return max(self.limit - len(self.stored), 0)


@dataclass(frozen=True, slots=True)
class Outcome:
    job_id: int
    status: str
    failure_kind: str | None
    rows_read: int
    rows_inserted: int
    rows_skipped: int
    error_count: int
    message: str


# --------------------------------------------------------------------------- claiming


def claim_next_job(conn: Connection, *, worker_id: str, max_attempts: int) -> Claim | None:
    """Claim the oldest runnable job. ``conn`` must stay open while the job is processed."""
    candidates = (
        conn.execute(
            text("SELECT id FROM import_jobs WHERE status IN ('queued', 'processing') ORDER BY created_at, id LIMIT 50")
        )
        .scalars()
        .all()
    )
    conn.commit()
    for job_id in candidates:
        if not conn.execute(text("SELECT pg_try_advisory_lock(:id)"), {"id": job_id}).scalar_one():
            conn.commit()
            continue  # another live worker owns it
        claim = _take_locked_job(conn, job_id, worker_id=worker_id, max_attempts=max_attempts)
        conn.commit()
        if claim is not None:
            return claim
        release_claim(conn, job_id)
    return None


def _take_locked_job(conn: Connection, job_id: int, *, worker_id: str, max_attempts: int) -> Claim | None:
    row = conn.execute(
        text("SELECT status, attempts FROM import_jobs WHERE id = :id FOR UPDATE"), {"id": job_id}
    ).one_or_none()
    if row is None or row.status not in ("queued", "processing"):
        return None  # finished by someone else between our SELECT and the lock
    if row.status == "processing":
        log.warning("job %s: previous attempt %s was interrupted; recovering", job_id, row.attempts)
    if row.attempts >= max_attempts:
        _finish_failed(
            conn,
            job_id,
            kind="internal",
            message=(
                f"Gave up after {row.attempts} interrupted attempts. No sales were imported. "
                "Check the worker logs, then retry."
            ),
            rows_read=None,
            error_count=0,
        )
        return None
    attempt = conn.execute(
        text(
            "UPDATE import_jobs SET status = 'processing', attempts = attempts + 1, "
            "worker_id = :worker, started_at = clock_timestamp(), finished_at = NULL "
            "WHERE id = :id RETURNING attempts"
        ),
        {"id": job_id, "worker": worker_id},
    ).scalar_one()
    return Claim(job_id=job_id, attempt=attempt)


def release_claim(conn: Connection, job_id: int) -> None:
    conn.execute(text("SELECT pg_advisory_unlock(:id)"), {"id": job_id})
    conn.commit()


def _lock_claimed_job(conn: Connection, claim: Claim) -> Row[Any]:
    row = conn.execute(
        text(
            "SELECT j.id, j.status, j.attempts, j.source_id, j.stored_filename, j.file_format, "
            "src.code AS source_code FROM import_jobs j JOIN sources src ON src.id = j.source_id "
            "WHERE j.id = :id FOR UPDATE OF j"
        ),
        {"id": claim.job_id},
    ).one_or_none()
    if row is None or row.status != "processing" or row.attempts != claim.attempt:
        raise LostClaim(f"job {claim.job_id} attempt {claim.attempt} is no longer current")
    return row


# --------------------------------------------------------------------------- processing


def process_claimed_job(conn: Connection, claim: Claim, settings: Settings, *, now: datetime | None = None) -> Outcome:
    """Validate, stage and publish one claimed job. Strict: any error rejects the file."""
    now = now or datetime.now(UTC)
    job = _lock_claimed_job(conn, claim)
    conn.commit()
    faults.checkpoint("after_claim")
    path = settings.upload_dir / job.stored_filename

    errors = ErrorCollector(limit=settings.max_stored_errors)
    _create_staging(conn)
    rows_read = _stage_file(conn, path, job.file_format, settings, errors, now=now)
    conn.execute(text("ANALYZE staging"))
    faults.checkpoint("after_staging")

    # Serialize publication per source: conflict checks and inserts see a stable set of sales.
    conn.execute(text("SELECT id FROM sources WHERE id = :sid FOR UPDATE"), {"sid": job.source_id})
    _collect_in_file_conflicts(conn, errors)
    _collect_existing_conflicts(conn, job.source_id, errors)

    if errors.total:
        conn.rollback()  # drops staging; sales untouched
        kind = "validation" if errors.validation_total else "conflict"
        _lock_claimed_job(conn, claim)
        _store_errors(conn, claim.job_id, errors.stored)
        message = _failure_message(kind, errors)
        _finish_failed(conn, claim.job_id, kind=kind, message=message, rows_read=rows_read, error_count=errors.total)
        conn.commit()
        return Outcome(claim.job_id, "failed", kind, rows_read, 0, 0, errors.total, message)

    _lock_claimed_job(conn, claim)
    inserted = _publish(conn, job.source_id, claim.job_id)
    skipped = rows_read - inserted
    message = _success_message(inserted, skipped, claim.attempt)
    conn.execute(
        text(
            "UPDATE import_jobs SET status = 'succeeded', finished_at = clock_timestamp(), rows_read = :read, "
            "rows_inserted = :inserted, rows_skipped = :skipped, error_count = 0, message = :message "
            "WHERE id = :id"
        ),
        {"id": claim.job_id, "read": rows_read, "inserted": inserted, "skipped": skipped, "message": message},
    )
    faults.checkpoint("before_commit")
    conn.commit()
    faults.checkpoint("after_commit")
    return Outcome(claim.job_id, "succeeded", None, rows_read, inserted, skipped, 0, message)


def mark_internal_failure(conn: Connection, claim: Claim, message: str) -> None:
    """Record an unexpected error. Retrying such a job is allowed (and safe)."""
    conn.rollback()
    _lock_claimed_job(conn, claim)
    _finish_failed(conn, claim.job_id, kind="internal", message=message, rows_read=None, error_count=0)
    conn.commit()


def _create_staging(conn: Connection) -> None:
    conn.execute(
        text(
            "CREATE TEMP TABLE staging ("
            " row_number integer NOT NULL, sale_id varchar(64) NOT NULL,"
            " sold_at timestamptz NOT NULL, store_id varchar(32) NOT NULL,"
            " product_category varchar(64) NOT NULL, quantity integer NOT NULL,"
            " unit_price numeric(12,2) NOT NULL, currency char(3) NOT NULL"
            ") ON COMMIT DROP"
        )
    )


def _stage_file(
    conn: Connection,
    path: Path,
    file_format: str,
    settings: Settings,
    errors: ErrorCollector,
    *,
    now: datetime,
) -> int:
    """Stream the file, validate every row and COPY valid rows into staging."""
    if not path.is_file():
        raise FileNotFoundError(f"stored upload {path.name} is missing")
    rows_read = 0
    driver = conn.connection.driver_connection
    if driver is None:  # pragma: no cover - only if the pool hands out a closed connection
        raise RuntimeError("no DBAPI connection available")
    with driver.cursor() as cursor, cursor.copy(f"COPY staging ({', '.join(STAGING_COLUMNS)}) FROM STDIN") as copy:
        try:
            if file_format == "parquet" and parquet_row_count(path) > settings.max_rows:
                raise FileRejected("too_many_rows", f"the file has more than {settings.max_rows} data rows")
            for raw in iter_rows(path, file_format, batch_rows=settings.parquet_batch_rows):  # type: ignore[arg-type]
                if raw.number > settings.max_rows:
                    raise FileRejected(
                        "too_many_rows", f"the file has more than {settings.max_rows} data rows", raw.number
                    )
                rows_read = raw.number
                if raw.values is None:
                    errors.add(RowError(raw.number, None, raw.error_code or "invalid_row", raw.error_message or ""))
                    continue
                sale, field_errors = normalize_record(raw.values, now=now)
                for fe in field_errors:
                    errors.add(RowError(raw.number, fe.field, fe.code, fe.message, fe.raw_value))
                if sale is not None:
                    copy.write_row(_staging_row(raw.number, sale))
        except FileRejected as exc:
            errors.add(RowError(exc.row_number, None, exc.code, exc.message))
    return rows_read


def _staging_row(number: int, sale: Sale) -> tuple[object, ...]:
    return (number, sale.sale_id, *sale.significant())


def _diff(a: Row[Any], prefix_b: str) -> list[str]:
    mapping = a._mapping
    return [c for c in SIGNIFICANT_FIELDS if mapping[c] != mapping[f"{prefix_b}{c}"]]


def _describe(row: Row[Any], fields: Sequence[str], prefix_b: str, label_a: str, label_b: str) -> str:
    mapping = row._mapping
    return "; ".join(f"{f}: {label_b} {_fmt(mapping[prefix_b + f])}, {label_a} {_fmt(mapping[f])}" for f in fields)


def _fmt(value: object) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(value)


def _collect_in_file_conflicts(conn: Connection, errors: ErrorCollector) -> None:
    first_cols = ", ".join(f"f.{c} AS first_{c}" for c in SIGNIFICANT_FIELDS)
    rows = conn.execute(
        text(
            "WITH firsts AS (SELECT DISTINCT ON (sale_id) * FROM staging ORDER BY sale_id, row_number) "
            f"SELECT s.row_number, s.sale_id, {_S_SIG}, f.row_number AS first_row, {first_cols}, "
            "count(*) OVER () AS total "
            "FROM staging s JOIN firsts f USING (sale_id) "
            f"WHERE s.row_number <> f.row_number AND ({_S_SIG}) IS DISTINCT FROM "
            f"({', '.join('f.' + c for c in SIGNIFICANT_FIELDS)}) "
            "ORDER BY s.row_number LIMIT :limit"
        ),
        {"limit": max(errors.room, 1)},
    ).all()
    _add_conflicts(
        errors,
        rows,
        prefix="first_",
        code="conflict_in_file",
        describe=lambda r, d: (
            f"sale_id {r.sale_id!r} appears again with different data than row {r.first_row} ("
            + _describe(r, d, "first_", "here", f"row {r.first_row}")
            + ")"
        ),
    )


def _collect_existing_conflicts(conn: Connection, source_id: int, errors: ErrorCollector) -> None:
    existing_cols = ", ".join(f"x.{c} AS existing_{c}" for c in SIGNIFICANT_FIELDS)
    rows = conn.execute(
        text(
            f"SELECT s.row_number, s.sale_id, {_S_SIG}, x.import_job_id, {existing_cols}, "
            "count(*) OVER () AS total "
            "FROM staging s JOIN sales x ON x.source_id = :sid AND x.sale_id = s.sale_id "
            f"WHERE ({_S_SIG}) IS DISTINCT FROM ({', '.join('x.' + c for c in SIGNIFICANT_FIELDS)}) "
            "ORDER BY s.row_number LIMIT :limit"
        ),
        {"sid": source_id, "limit": max(errors.room, 1)},
    ).all()
    _add_conflicts(
        errors,
        rows,
        prefix="existing_",
        code="conflict_existing",
        describe=lambda r, d: (
            f"sale_id {r.sale_id!r} was already imported (import #{r.import_job_id}) with different data ("
            + _describe(r, d, "existing_", "file", "existing")
            + "). Existing sales are never overwritten."
        ),
    )


def _add_conflicts(errors: ErrorCollector, rows: Sequence[Row[Any]], *, prefix: str, code: str, describe: Any) -> None:
    if not rows:
        return
    kept = rows[: errors.room]
    for row in kept:
        changed = _diff(row, prefix)
        errors.add(RowError(row.row_number, ", ".join(changed)[:32], code, describe(row, changed)), validation=False)
    errors.count_only(int(rows[0].total) - len(kept))


def _publish(conn: Connection, source_id: int, job_id: int) -> int:
    result = conn.execute(
        text(
            f"INSERT INTO sales (source_id, sale_id, {_SIG}, import_job_id) "
            f"SELECT DISTINCT ON (s.sale_id) :sid, s.sale_id, {_S_SIG}, :job FROM staging s "
            "WHERE NOT EXISTS (SELECT 1 FROM sales x WHERE x.source_id = :sid AND x.sale_id = s.sale_id) "
            "ORDER BY s.sale_id, s.row_number "
            "ON CONFLICT (source_id, sale_id) DO NOTHING"
        ),
        {"sid": source_id, "job": job_id},
    )
    return int(result.rowcount)


def _store_errors(conn: Connection, job_id: int, stored: Sequence[RowError]) -> None:
    conn.execute(text("DELETE FROM import_errors WHERE job_id = :id"), {"id": job_id})
    if not stored:
        return
    conn.execute(
        text(
            "INSERT INTO import_errors (job_id, row_number, field, code, message, raw_value) "
            "VALUES (:job_id, :row_number, :field, :code, :message, :raw_value)"
        ),
        [
            {
                "job_id": job_id,
                "row_number": e.row_number,
                "field": e.field,
                "code": e.code,
                "message": e.message,
                "raw_value": e.raw_value,
            }
            for e in sorted(stored, key=lambda e: (e.row_number is not None, e.row_number or 0))
        ],
    )


def _finish_failed(
    conn: Connection, job_id: int, *, kind: str, message: str, rows_read: int | None, error_count: int
) -> None:
    conn.execute(
        text(
            "UPDATE import_jobs SET status = 'failed', failure_kind = :kind, finished_at = clock_timestamp(), "
            "rows_read = :read, rows_inserted = 0, rows_skipped = 0, error_count = :errors, message = :message "
            "WHERE id = :id"
        ),
        {"id": job_id, "kind": kind, "read": rows_read, "errors": error_count, "message": message},
    )


def _plural(count: int, word: str) -> str:
    return f"{count:,} {word}{'' if count == 1 else 's'}"


def _failure_message(kind: str, errors: ErrorCollector) -> str:
    conflicts = errors.total - errors.validation_total
    parts = []
    if errors.validation_total:
        parts.append(_plural(errors.validation_total, "validation error"))
    if conflicts:
        parts.append(_plural(conflicts, "conflicting row"))
    return f"Rejected: {' and '.join(parts)}. The whole file was rejected; no sales were changed."


def _success_message(inserted: int, skipped: int, attempt: int) -> str:
    message = f"Imported {_plural(inserted, 'new sale')}; skipped {_plural(skipped, 'exact duplicate')}."
    if attempt > 1:
        message += f" Completed on attempt {attempt} after an interrupted attempt."
    return message
