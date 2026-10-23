"""Import job records: creation, lookup and operator retry."""

from __future__ import annotations

from typing import Any

from sqlalchemy import Connection, Row, text

from importguard.uploads import StoredUpload

ACTIVE_STATUSES = ("queued", "processing")

_JOB_COLUMNS = (
    "j.id, j.source_id, s.code AS source_code, s.name AS source_name, j.original_filename, "
    "j.file_format, j.size_bytes, j.content_sha256, j.status, j.failure_kind, j.attempts, "
    "j.created_at, j.started_at, j.finished_at, j.rows_read, j.rows_inserted, j.rows_skipped, "
    "j.error_count, j.message"
)


def list_sources(conn: Connection) -> list[Row[Any]]:
    return list(conn.execute(text("SELECT id, code, name, description FROM sources ORDER BY id")).all())


def get_source(conn: Connection, source_id: int) -> Row[Any] | None:
    return conn.execute(text("SELECT id, code, name FROM sources WHERE id = :id"), {"id": source_id}).one_or_none()


def create_job(conn: Connection, source_id: int, upload: StoredUpload) -> int:
    return int(
        conn.execute(
            text(
                "INSERT INTO import_jobs (source_id, original_filename, stored_filename, file_format, "
                "size_bytes, content_sha256) VALUES (:source_id, :original, :stored, :fmt, :size, :sha) "
                "RETURNING id"
            ),
            {
                "source_id": source_id,
                "original": upload.original_filename,
                "stored": upload.stored_filename,
                "fmt": upload.file_format,
                "size": upload.size_bytes,
                "sha": upload.sha256,
            },
        ).scalar_one()
    )


def get_job(conn: Connection, job_id: int) -> Row[Any] | None:
    return conn.execute(
        text(f"SELECT {_JOB_COLUMNS} FROM import_jobs j JOIN sources s ON s.id = j.source_id WHERE j.id = :id"),
        {"id": job_id},
    ).one_or_none()


def list_jobs(conn: Connection, *, limit: int, offset: int = 0) -> list[Row[Any]]:
    return list(
        conn.execute(
            text(
                f"SELECT {_JOB_COLUMNS} FROM import_jobs j JOIN sources s ON s.id = j.source_id "
                "ORDER BY j.id DESC LIMIT :limit OFFSET :offset"
            ),
            {"limit": limit, "offset": offset},
        ).all()
    )


def count_jobs(conn: Connection) -> int:
    return int(conn.execute(text("SELECT count(*) FROM import_jobs")).scalar_one())


def earlier_identical_job(conn: Connection, job: Row[Any]) -> Row[Any] | None:
    """An earlier upload of byte-identical content for the same source, if any."""
    return conn.execute(
        text(
            "SELECT id, status, original_filename FROM import_jobs "
            "WHERE source_id = :sid AND content_sha256 = :sha AND id < :id ORDER BY id LIMIT 1"
        ),
        {"sid": job.source_id, "sha": job.content_sha256, "id": job.id},
    ).one_or_none()


def job_errors(conn: Connection, job_id: int, *, limit: int | None = None) -> list[Row[Any]]:
    return list(
        conn.execute(
            text(
                "SELECT row_number, field, code, message, raw_value FROM import_errors "
                "WHERE job_id = :id ORDER BY row_number NULLS FIRST, id LIMIT :limit"
            ),
            {"id": job_id, "limit": limit},
        ).all()
    )


def is_retryable(job: Row[Any]) -> bool:
    """Only unexpected (internal) failures are worth retrying: the file itself is immutable,
    so a validation or conflict rejection would simply repeat."""
    return bool(job.status == "failed" and job.failure_kind == "internal")


def retry_job(conn: Connection, job_id: int) -> bool:
    """Re-queue an internally failed job. Atomic: double submits re-queue at most once."""
    requeued = conn.execute(
        text(
            "UPDATE import_jobs SET status = 'queued', failure_kind = NULL, attempts = 0, worker_id = NULL, "
            "started_at = NULL, finished_at = NULL, rows_read = NULL, rows_inserted = NULL, "
            "rows_skipped = NULL, error_count = 0, message = 'Re-queued by the operator.' "
            "WHERE id = :id AND status = 'failed' AND failure_kind = 'internal' RETURNING id"
        ),
        {"id": job_id},
    ).one_or_none()
    if requeued is None:
        return False
    conn.execute(text("DELETE FROM import_errors WHERE job_id = :id"), {"id": job_id})
    return True
