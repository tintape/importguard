"""The import worker: a separate process that polls PostgreSQL for queued jobs.

Run with ``python -m importguard.worker``. Jobs live in the database, so they
survive restarts of both the web process and the worker.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import socket
import threading
import time
from types import FrameType

from sqlalchemy import Connection, Engine
from sqlalchemy.pool import NullPool

from importguard.config import Settings, get_settings
from importguard.db import make_engine
from importguard.processing import (
    Claim,
    LostClaim,
    Outcome,
    claim_next_job,
    mark_internal_failure,
    process_claimed_job,
    release_claim,
)

log = logging.getLogger("importguard.worker")


def default_worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def make_worker_engine(settings: Settings) -> Engine:
    # NullPool: closing the connection really disconnects, which releases advisory locks.
    # TCP keepalives let PostgreSQL notice a vanished worker host, not just a dead process.
    return make_engine(
        settings.database_url,
        poolclass=NullPool,
        connect_args={"keepalives": 1, "keepalives_idle": 30, "keepalives_interval": 10, "keepalives_count": 3},
    )


def run_once(engine: Engine, settings: Settings, *, worker_id: str) -> Outcome | None:
    """Claim and process at most one job. Returns None when there was nothing to do."""
    with engine.connect() as conn:
        claim = claim_next_job(conn, worker_id=worker_id, max_attempts=settings.max_attempts)
        if claim is None:
            return None
        log.info("job %s: claimed (attempt %s)", claim.job_id, claim.attempt)
        try:
            outcome = process_claimed_job(conn, claim, settings)
            log.info("job %s: %s - %s", claim.job_id, outcome.status, outcome.message)
            return outcome
        except LostClaim:
            log.warning("job %s: claim lost; nothing written", claim.job_id)
            conn.rollback()
            return None
        except Exception as exc:
            log.exception("job %s: unexpected error", claim.job_id)
            return _record_internal_failure(conn, claim, exc)
        finally:
            try:
                release_claim(conn, claim.job_id)
            except Exception:  # the connection is closed right after; that releases the lock too
                log.warning("job %s: could not release the advisory lock explicitly", claim.job_id)


def _record_internal_failure(conn: Connection, claim: Claim, exc: Exception) -> Outcome:
    detail = str(exc).splitlines()[0][:200] if str(exc) else ""
    reason = f"{type(exc).__name__}: {detail}" if detail else type(exc).__name__
    message = f"Unexpected error ({reason}). No sales were imported; retrying is safe."
    try:
        mark_internal_failure(conn, claim, message)
    except Exception:
        log.exception("job %s: could not record the failure; it will be recovered on the next claim", claim.job_id)
    return Outcome(claim.job_id, "failed", "internal", 0, 0, 0, 0, message)


def run_forever(engine: Engine, settings: Settings, *, worker_id: str, stop: threading.Event) -> None:
    log.info("worker %s started; polling every %ss", worker_id, settings.worker_poll_seconds)
    while not stop.is_set():
        try:
            outcome = run_once(engine, settings, worker_id=worker_id)
        except Exception:
            log.exception("worker loop error (database unavailable?); retrying")
            outcome = None
        if outcome is None:
            stop.wait(settings.worker_poll_seconds)
    log.info("worker %s stopped", worker_id)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="ImportGuard import worker")
    parser.add_argument("--once", action="store_true", help="process at most one job and exit")
    parser.add_argument("--drain", action="store_true", help="process jobs until the queue is empty, then exit")
    parser.add_argument("--worker-id", default=None)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    settings = get_settings()
    engine = make_worker_engine(settings)
    worker_id = args.worker_id or default_worker_id()
    if args.once:
        run_once(engine, settings, worker_id=worker_id)
        return 0
    if args.drain:
        started = time.monotonic()
        while run_once(engine, settings, worker_id=worker_id) is not None:
            pass
        log.info("queue drained in %.2fs", time.monotonic() - started)
        return 0

    stop = threading.Event()

    def _request_stop(signum: int, _frame: FrameType | None) -> None:
        log.info("signal %s received; finishing the current job, then exiting", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    run_forever(engine, settings, worker_id=worker_id, stop=stop)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
