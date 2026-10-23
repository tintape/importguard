"""Crash recovery, re-processing and concurrent workers.

Hard crashes are real: the worker runs as a separate process and either exits
abruptly at a fault-injection checkpoint (``os._exit``, no cleanup) or is
killed with SIGKILL while it holds a job.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import Engine, text

from importguard.config import Settings
from importguard.faults import CRASH_EXIT_CODE
from importguard.jobs import retry_job
from importguard.processing import Claim, LostClaim, claim_next_job, process_claimed_job
from importguard.worker import run_once
from tests.integration.conftest import drain, job_row, scalar, submit

HEADER = "sale_id,sold_at,store_id,product_category,quantity,unit_price,currency\n"


def make_csv(start: int, count: int, price: str = "1.25") -> bytes:
    rows = "".join(
        f"S-{i:06d},2026-09-{1 + i % 28:02d}T{i % 24:02d}:00:00Z,ST-{i % 5},cat{i % 7},{1 + i % 3},{price},EUR\n"
        for i in range(start, start + count)
    )
    return (HEADER + rows).encode()


def expected_total(start: int, count: int, price: str = "1.25") -> str:
    cents = sum((1 + i % 3) for i in range(start, start + count)) * int(price.replace(".", ""))
    return f"{cents // 100}.{cents % 100:02d}"


def worker_env(settings: Settings, faults: str = "") -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("IMPORTGUARD_")}
    env.update(
        IMPORTGUARD_DATABASE_URL=settings.database_url,
        IMPORTGUARD_UPLOAD_DIR=str(settings.upload_dir),
        IMPORTGUARD_WORKER_POLL_SECONDS="0.05",
    )
    if faults:
        env["IMPORTGUARD_FAULTS"] = faults
    return env


def run_worker(settings: Settings, *args: str, faults: str = "", cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "importguard.worker", *args],
        env=worker_env(settings, faults),
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def start_worker(settings: Settings, *args: str, faults: str = "", cwd: Path) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, "-m", "importguard.worker", *args],
        env=worker_env(settings, faults),
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def wait_for(predicate, timeout: float = 30.0) -> None:  # type: ignore[no-untyped-def]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("condition not reached in time")


def sales_summary(engine: Engine) -> tuple[int, str]:
    with engine.connect() as conn:
        count, total = conn.execute(text("SELECT count(*), coalesce(sum(amount), 0) FROM sales")).one()
    return int(count), f"{total:.2f}"


def test_reprocessing_a_succeeded_job_does_not_double_count(
    engine: Engine, worker_engine: Engine, settings: Settings
) -> None:
    job = submit(engine, settings, "pos", make_csv(0, 50))
    drain(worker_engine, settings)
    before = sales_summary(engine)

    # Simulate a lost acknowledgement: the result is committed, but the job is run again anyway.
    with engine.begin() as conn:
        conn.execute(text("UPDATE import_jobs SET status = 'queued', attempts = 0 WHERE id = :id"), {"id": job})
    [again] = drain(worker_engine, settings)

    assert (again.status, again.rows_inserted, again.rows_skipped) == ("succeeded", 0, 50)
    assert sales_summary(engine) == before == (50, expected_total(0, 50))


def test_exception_before_commit_leaves_no_sales_and_retry_succeeds(
    engine: Engine, worker_engine: Engine, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = submit(engine, settings, "pos", make_csv(0, 30))
    monkeypatch.setenv("IMPORTGUARD_FAULTS", "before_commit=raise")
    [outcome] = drain(worker_engine, settings)

    assert (outcome.status, outcome.failure_kind) == ("failed", "internal")
    assert "retrying is safe" in job_row(engine, job).message
    assert sales_summary(engine) == (0, "0.00")

    monkeypatch.delenv("IMPORTGUARD_FAULTS")
    with engine.begin() as conn:
        assert retry_job(conn, job) is True
        assert retry_job(conn, job) is False  # second click is a no-op
    [retried] = drain(worker_engine, settings)
    assert (retried.status, retried.rows_inserted) == ("succeeded", 30)
    assert sales_summary(engine) == (30, expected_total(0, 30))


def test_validation_failures_are_not_retryable(engine: Engine, worker_engine: Engine, settings: Settings) -> None:
    job = submit(engine, settings, "pos", (HEADER + "bad,row,,,,,\n").encode())
    drain(worker_engine, settings)
    with engine.begin() as conn:
        assert retry_job(conn, job) is False
    assert job_row(engine, job).status == "failed"


def test_hard_crash_before_commit_publishes_nothing_and_restart_completes(
    engine: Engine, settings: Settings, tmp_path: Path
) -> None:
    job = submit(engine, settings, "pos", make_csv(0, 2_000))

    crashed = run_worker(settings, "--once", faults="before_commit=crash", cwd=tmp_path)
    assert crashed.returncode == CRASH_EXIT_CODE, crashed.stderr
    row = job_row(engine, job)
    assert (row.status, row.attempts) == ("processing", 1)  # orphaned, lock released by PostgreSQL
    assert sales_summary(engine) == (0, "0.00")  # nothing partially published

    restarted = run_worker(settings, "--drain", cwd=tmp_path)
    assert restarted.returncode == 0, restarted.stderr
    row = job_row(engine, job)
    assert (row.status, row.attempts, row.rows_inserted) == ("succeeded", 2, 2_000)
    assert "attempt 2" in row.message
    assert sales_summary(engine) == (2_000, expected_total(0, 2_000))


def test_crash_after_commit_does_not_cause_duplicates(engine: Engine, settings: Settings, tmp_path: Path) -> None:
    job = submit(engine, settings, "pos", make_csv(0, 500))

    crashed = run_worker(settings, "--once", faults="after_commit=crash", cwd=tmp_path)
    assert crashed.returncode == CRASH_EXIT_CODE, crashed.stderr
    assert job_row(engine, job).status == "succeeded"  # result and acknowledgement are one commit

    restarted = run_worker(settings, "--drain", cwd=tmp_path)
    assert restarted.returncode == 0
    assert job_row(engine, job).attempts == 1  # not processed again
    assert sales_summary(engine) == (500, expected_total(0, 500))


def test_sigkill_during_processing_then_restart(engine: Engine, settings: Settings, tmp_path: Path) -> None:
    job = submit(engine, settings, "pos", make_csv(0, 5_000))
    worker = start_worker(settings, faults="after_staging=sleep:60", cwd=tmp_path)
    try:
        wait_for(lambda: job_row(engine, job).status == "processing")
        time.sleep(0.5)  # let it reach the staging checkpoint
        worker.send_signal(signal.SIGKILL)
        worker.wait(timeout=10)
    finally:
        worker.kill()
    assert sales_summary(engine) == (0, "0.00")

    restarted = run_worker(settings, "--drain", cwd=tmp_path)
    assert restarted.returncode == 0, restarted.stderr
    assert job_row(engine, job).status == "succeeded"
    assert sales_summary(engine) == (5_000, expected_total(0, 5_000))


def test_finished_at_reflects_real_processing_time(engine: Engine, settings: Settings, tmp_path: Path) -> None:
    job = submit(engine, settings, "pos", make_csv(0, 10))
    assert run_worker(settings, "--once", faults="after_staging=sleep:1", cwd=tmp_path).returncode == 0
    row = job_row(engine, job)
    assert (row.finished_at - row.started_at).total_seconds() >= 1


def test_queued_job_survives_until_a_worker_runs(engine: Engine, settings: Settings, tmp_path: Path) -> None:
    # The web process only writes the job row; nothing runs in-process. Any later worker picks it up.
    job = submit(engine, settings, "pos", make_csv(0, 10))
    engine.dispose()  # no connection or in-memory state survives
    assert job_row(engine, job).status == "queued"

    assert run_worker(settings, "--drain", cwd=tmp_path).returncode == 0
    assert job_row(engine, job).status == "succeeded"


def test_second_worker_cannot_take_a_job_held_by_a_live_worker(
    engine: Engine, worker_engine: Engine, settings: Settings, tmp_path: Path
) -> None:
    job = submit(engine, settings, "pos", make_csv(0, 100))
    first = start_worker(settings, "--once", faults="after_staging=sleep:3", cwd=tmp_path)
    try:
        wait_for(lambda: job_row(engine, job).status == "processing")
        assert run_once(worker_engine, settings, worker_id="intruder") is None
        assert job_row(engine, job).attempts == 1
        first.wait(timeout=60)
    finally:
        first.kill()
    row = job_row(engine, job)
    assert (row.status, row.attempts, row.worker_id) == ("succeeded", 1, row.worker_id)
    assert row.worker_id != "intruder"
    assert sales_summary(engine) == (100, expected_total(0, 100))


def test_concurrent_workers_keep_totals_exact(engine: Engine, settings: Settings, tmp_path: Path) -> None:
    # Ten overlapping files for the same source: every sale appears in two or three files.
    for n in range(10):
        submit(engine, settings, "pos", make_csv(n * 100, 300), name=f"part-{n}.csv")
    workers = [start_worker(settings, "--drain", cwd=tmp_path) for _ in range(3)]
    for w in workers:
        assert w.wait(timeout=120) == 0, w.stdout.read() if w.stdout else ""

    with engine.connect() as conn:
        statuses = conn.execute(text("SELECT status, attempts, count(*) FROM import_jobs GROUP BY 1, 2")).all()
        inserted = conn.execute(text("SELECT sum(rows_inserted) FROM import_jobs")).scalar_one()
    assert [tuple(s) for s in statuses] == [("succeeded", 1, 10)]
    assert inserted == 1_200
    assert sales_summary(engine) == (1_200, expected_total(0, 1_200))


def test_stale_attempt_is_fenced_off(engine: Engine, settings: Settings) -> None:
    job = submit(engine, settings, "pos", make_csv(0, 10))
    with engine.connect() as conn:
        claim = claim_next_job(conn, worker_id="w1", max_attempts=3)
        assert claim == Claim(job_id=job, attempt=1)
        with engine.begin() as other:  # someone else bumped the attempt (e.g. a recovery)
            other.execute(text("UPDATE import_jobs SET attempts = 2 WHERE id = :id"), {"id": job})
        with pytest.raises(LostClaim):
            process_claimed_job(conn, claim, settings)
        conn.rollback()
    assert sales_summary(engine) == (0, "0.00")


def test_gives_up_after_max_interrupted_attempts(engine: Engine, worker_engine: Engine, settings: Settings) -> None:
    job = submit(engine, settings, "pos", make_csv(0, 10))
    with engine.begin() as conn:  # three attempts that all died mid-way
        conn.execute(text("UPDATE import_jobs SET status = 'processing', attempts = 3 WHERE id = :id"), {"id": job})
    assert drain(worker_engine, settings) == []
    row = job_row(engine, job)
    assert (row.status, row.failure_kind) == ("failed", "internal")
    assert "Gave up after 3" in row.message
    with engine.begin() as conn:
        assert retry_job(conn, job)
    [outcome] = drain(worker_engine, settings)
    assert outcome.status == "succeeded"


def test_missing_stored_file_is_an_internal_failure(engine: Engine, worker_engine: Engine, settings: Settings) -> None:
    job = submit(engine, settings, "pos", make_csv(0, 10))
    for path in settings.upload_dir.iterdir():
        path.unlink()
    [outcome] = drain(worker_engine, settings)
    assert (outcome.status, outcome.failure_kind) == ("failed", "internal")
    assert "FileNotFoundError" in job_row(engine, job).message
    assert scalar(engine, "SELECT count(*) FROM sales") == 0


def test_long_running_worker_processes_jobs_and_stops_gracefully(
    engine: Engine, settings: Settings, tmp_path: Path
) -> None:
    worker = start_worker(settings, cwd=tmp_path)
    try:
        job = submit(engine, settings, "pos", make_csv(0, 20))  # submitted while the worker is polling
        wait_for(lambda: job_row(engine, job).status == "succeeded")
        worker.send_signal(signal.SIGTERM)
        assert worker.wait(timeout=10) == 0
    finally:
        worker.kill()
    output = worker.stdout.read() if worker.stdout else ""
    assert "finishing the current job, then exiting" in output
    assert sales_summary(engine) == (20, expected_total(0, 20))
