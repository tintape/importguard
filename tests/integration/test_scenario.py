"""The demo scenario, checked against totals computed independently by the generator."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, text

from importguard import sampledata
from importguard.config import Settings
from importguard.contract import normalize_record
from importguard.readers import iter_rows
from tests.integration.conftest import drain, errors_of, submit


@pytest.fixture(scope="session")
def demo(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict[str, Any]]:
    out = tmp_path_factory.mktemp("demo-data")
    sampledata.build(out)
    return out, json.loads((out / "expected.json").read_text())


def run_steps(
    engine: Engine, worker_engine: Engine, settings: Settings, folder: Path, steps: list[dict[str, Any]]
) -> None:
    for step in steps:
        job = submit(engine, settings, step["source"], folder / step["file"], name=step["file"])
        [outcome] = drain(worker_engine, settings)
        label = step["file"]
        assert outcome.status == step["expect_status"], (label, outcome.message)
        if outcome.status == "succeeded":
            assert (outcome.rows_inserted, outcome.rows_skipped) == (step["expect_inserted"], step["expect_skipped"]), (
                label
            )
        else:
            assert outcome.failure_kind == step["expect_failure_kind"], label
            got = [(row, code) for row, _field, code in errors_of(engine, job)]
            assert got == [(e["row"], e["code"]) for e in step["expect_errors"]], label


def totals(engine: Engine) -> dict[str, dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT currency, count(*), sum(amount) FROM sales GROUP BY currency ORDER BY currency")
        )
        return {cur: {"sales": n, "revenue": f"{total:.2f}"} for cur, n, total in rows}


def test_scenario_without_large_file(
    engine: Engine, worker_engine: Engine, settings: Settings, demo: tuple[Path, dict[str, Any]]
) -> None:
    folder, expected = demo
    steps = [s for s in expected["steps"] if "large" not in s["file"]]
    run_steps(engine, worker_engine, settings, folder, steps)
    assert totals(engine) == expected["small_scenario_final"]["by_currency"]


def test_full_scenario_including_100k_rows(
    engine: Engine, worker_engine: Engine, settings: Settings, demo: tuple[Path, dict[str, Any]]
) -> None:
    folder, expected = demo
    big = settings.model_copy(update={"max_upload_bytes": 50 * 1024 * 1024})
    run_steps(engine, worker_engine, big, folder, expected["steps"])
    assert totals(engine) == expected["final"]["by_currency"]
    with engine.connect() as conn:
        by_source = dict(
            conn.execute(
                text("SELECT s.code, count(*) FROM sales x JOIN sources s ON s.id = x.source_id GROUP BY 1")
            ).all()
        )
    assert by_source == expected["final"]["by_source"]


def test_large_csv_and_parquet_are_equivalent(
    engine: Engine, worker_engine: Engine, settings: Settings, demo: tuple[Path, dict[str, Any]]
) -> None:
    folder, _ = demo
    big = settings.model_copy(update={"max_upload_bytes": 50 * 1024 * 1024})
    submit(engine, big, "marketplace", folder / "10_marketplace_large_100k.csv")
    submit(engine, big, "marketplace", folder / "10_marketplace_large_100k.parquet")
    first, second = drain(worker_engine, big)
    assert (first.rows_inserted, first.rows_skipped) == (100_000, 0)
    assert (second.status, second.rows_inserted, second.rows_skipped) == ("succeeded", 0, 100_000)


def test_readers_produce_identical_normalized_records(demo: tuple[Path, dict[str, Any]]) -> None:
    folder, _ = demo
    now = datetime(2026, 10, 1, tzinfo=UTC)

    def normalized(name: str) -> list[object]:
        fmt = "parquet" if name.endswith(".parquet") else "csv"
        return [normalize_record(r.values or {}, now=now) for r in iter_rows(folder / name, fmt, batch_rows=64)]

    csv_records = normalized("08_webshop_equivalent.csv")
    parquet_records = normalized("08_webshop_equivalent.parquet")
    assert len(csv_records) == 300
    assert csv_records == parquet_records
    assert all(sale is not None for sale, _ in csv_records)
