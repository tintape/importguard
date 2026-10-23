"""Measure import time and peak memory for the 100,000-row demo files.

Usage:
    uv run python scripts/benchmark.py [--runs 3] [--data demo-data]

It creates a separate database ``<name>_benchmark`` next to the one in
IMPORTGUARD_DATABASE_URL (dropping it first), so your working data is not
touched. Each measured import runs in a fresh worker process
(``python -m importguard.worker --once``); wall time and peak RSS come from
``os.wait4`` for exactly that process. Processing time is the job's
``finished_at - started_at`` recorded by the worker.

One machine, a few runs: this is a local measurement, not a benchmark suite.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import time
from importlib import metadata
from pathlib import Path
from typing import Any

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, make_url, text
from sqlalchemy.pool import NullPool

from importguard.config import get_settings
from importguard.jobs import create_job
from importguard.uploads import store_upload

ROOT = Path(__file__).resolve().parents[1]
MARKETPLACE = 3
CASES = (
    ("Parquet, 100k new rows", "10_marketplace_large_100k.parquet", True),
    ("Parquet, same file again (100k duplicates)", "10_marketplace_large_100k.parquet", False),
    ("CSV, 100k new rows", "10_marketplace_large_100k.csv", True),
    ("CSV, same file again (100k duplicates)", "10_marketplace_large_100k.csv", False),
)


def peak_rss_mib(rusage: Any) -> float:
    # ru_maxrss is bytes on macOS and kilobytes on Linux.
    scale = 1 if sys.platform == "darwin" else 1024
    return rusage.ru_maxrss * scale / (1024 * 1024)


def prepare_database(base_url: str) -> str:
    url = make_url(base_url)
    bench = url.set(database=f"{url.database}_benchmark")
    admin = create_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool)
    with admin.connect() as conn:
        conn.execute(text(f'DROP DATABASE IF EXISTS "{bench.database}" WITH (FORCE)'))
        conn.execute(text(f'CREATE DATABASE "{bench.database}"'))
    admin.dispose()
    rendered = bench.render_as_string(hide_password=False)
    config = Config(str(ROOT / "alembic.ini"))
    config.attributes["database_url"] = rendered
    config.attributes["configure_logger"] = False
    command.upgrade(config, "head")
    return rendered


def run_worker(env: dict[str, str]) -> tuple[float, float]:
    started = time.perf_counter()
    proc = subprocess.Popen(
        [sys.executable, "-m", "importguard.worker", "--once"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    _, status, rusage = os.wait4(proc.pid, 0)
    wall = time.perf_counter() - started
    if os.waitstatus_to_exitcode(status) != 0:
        raise RuntimeError("worker failed")
    return wall, peak_rss_mib(rusage)


def measure(url: str, data: Path, upload_dir: Path, runs: int) -> list[dict[str, Any]]:
    engine = create_engine(url, poolclass=NullPool)
    env = {**os.environ, "IMPORTGUARD_DATABASE_URL": url, "IMPORTGUARD_UPLOAD_DIR": str(upload_dir)}
    env.pop("IMPORTGUARD_FAULTS", None)
    results = []
    baseline = [run_worker(env)[1] for _ in range(runs)]  # no job queued: interpreter + imports only
    results.append(
        {"case": "Worker start-up only (no job)", "peak_rss_mib": baseline, "wall_s": [], "processing_s": []}
    )
    for label, filename, fresh in CASES:
        walls, peaks, processing = [], [], []
        for _ in range(runs):
            if fresh:  # start from an empty database for every measured fresh import
                with engine.begin() as conn:
                    conn.execute(text("TRUNCATE sales, import_errors, import_jobs RESTART IDENTITY CASCADE"))
                _submit(engine, data / filename, upload_dir)
            else:
                _submit(engine, data / filename, upload_dir)  # the file was imported by the previous case
            wall, peak = run_worker(env)
            with engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT status, rows_inserted, rows_skipped, "
                        "extract(epoch FROM finished_at - started_at) AS secs FROM import_jobs ORDER BY id DESC LIMIT 1"
                    )
                ).one()
            if row.status != "succeeded":
                raise RuntimeError(f"{label}: job ended as {row.status}")
            expected = (100_000, 0) if fresh else (0, 100_000)
            if (row.rows_inserted, row.rows_skipped) != expected:
                raise RuntimeError(f"{label}: unexpected counts {row.rows_inserted}/{row.rows_skipped}")
            walls.append(wall)
            peaks.append(peak)
            processing.append(float(row.secs))
        results.append({"case": label, "wall_s": walls, "processing_s": processing, "peak_rss_mib": peaks})
    engine.dispose()
    return results


def _submit(engine: Any, path: Path, upload_dir: Path) -> None:
    stored = store_upload(io.BytesIO(path.read_bytes()), path.name, upload_dir=upload_dir, max_bytes=10**9)
    with engine.begin() as conn:
        create_job(conn, MARKETPLACE, stored)


def environment(url: str) -> dict[str, str]:
    engine = create_engine(url, poolclass=NullPool)
    with engine.connect() as conn:
        server = conn.execute(text("SHOW server_version")).scalar_one()
    engine.dispose()
    cpu = platform.processor() or platform.machine()
    if sys.platform == "darwin":
        cpu = (
            subprocess.run(
                ["/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True, check=False
            ).stdout.strip()
            or cpu
        )
        mem = int(
            subprocess.run(["/usr/sbin/sysctl", "-n", "hw.memsize"], capture_output=True, text=True, check=False).stdout
            or 0
        )
    else:
        mem = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    return {
        "os": f"{platform.system()} {platform.release()} ({platform.machine()})",
        "cpu": cpu,
        "cpu_count": str(os.cpu_count()),
        "memory_gib": f"{mem / 1024**3:.0f}",
        "python": platform.python_version(),
        "postgresql": server,
        "pyarrow": metadata.version("pyarrow"),
        "sqlalchemy": metadata.version("sqlalchemy"),
        "psycopg": metadata.version("psycopg"),
    }


def summarize(results: list[dict[str, Any]]) -> str:
    def fmt(values: list[float], digits: int) -> str:
        if not values:
            return "—"
        return f"{statistics.median(values):.{digits}f} ({min(values):.{digits}f}–{max(values):.{digits}f})"

    lines = [
        "| Case | Worker process wall time, s | Job processing time, s | Peak RSS, MiB |",
        "|---|---|---|---|",
    ]
    for r in results:
        lines.append(
            f"| {r['case']} | {fmt(r['wall_s'], 2)} | {fmt(r['processing_s'], 2)} | {fmt(r['peak_rss_mib'], 0)} |"
        )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--data", type=Path, default=ROOT / "demo-data")
    parser.add_argument("--out", type=Path, default=ROOT / "benchmark-results")
    args = parser.parse_args()
    if not (args.data / "10_marketplace_large_100k.parquet").exists():
        parser.error(f"{args.data} has no demo files; run: uv run python -m importguard.sampledata")

    url = prepare_database(get_settings().database_url)
    with tempfile.TemporaryDirectory() as upload_dir:
        results = measure(url, args.data, Path(upload_dir), args.runs)
    env = environment(url)
    table = summarize(results)
    args.out.mkdir(exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    (args.out / f"benchmark-{stamp}.json").write_text(json.dumps({"environment": env, "results": results}, indent=2))
    print(f"Runs per case: {args.runs} (median, min–max)\n")
    print(table)
    print("\n" + "\n".join(f"- {k}: {v}" for k, v in env.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
