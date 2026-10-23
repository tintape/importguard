"""Integration tests run against a real PostgreSQL database (never SQLite).

Set IMPORTGUARD_TEST_DATABASE_URL to a database the tests may drop and recreate.
"""

from __future__ import annotations

import io
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, make_url, text
from sqlalchemy.pool import NullPool

from importguard.config import Settings
from importguard.jobs import create_job
from importguard.processing import Outcome
from importguard.uploads import store_upload
from importguard.worker import make_worker_engine, run_once
from tests.conftest import TEST_DATABASE_URL

ROOT = Path(__file__).resolve().parents[2]
SOURCE_IDS = {"pos": 1, "webshop": 2, "marketplace": 3}


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "tests/integration" in str(item.path) or "tests/e2e" in str(item.path):
            item.add_marker(pytest.mark.integration)


def _recreate_database(url: str) -> None:
    target = make_url(url)
    admin = create_engine(target.set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool)
    try:
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{target.database}" WITH (FORCE)'))
            conn.execute(text(f'CREATE DATABASE "{target.database}"'))
    except Exception as exc:  # pragma: no cover - environment problem
        pytest.exit(f"Cannot prepare the test database at {target.render_as_string()}: {exc}", returncode=3)
    finally:
        admin.dispose()


def migrate(url: str, revision: str = "head", *, downgrade: bool = False) -> None:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "migrations"))
    config.attributes["database_url"] = url
    config.attributes["configure_logger"] = False
    (command.downgrade if downgrade else command.upgrade)(config, revision)


@pytest.fixture(scope="session")
def database_url() -> str:
    _recreate_database(TEST_DATABASE_URL)
    migrate(TEST_DATABASE_URL)
    return TEST_DATABASE_URL


@pytest.fixture(scope="session")
def engine(database_url: str) -> Iterator[Engine]:
    eng = create_engine(database_url, poolclass=NullPool)
    yield eng
    eng.dispose()


@pytest.fixture(autouse=True)
def _clean_tables(request: pytest.FixtureRequest) -> None:
    if "engine" not in request.fixturenames:
        return
    eng: Engine = request.getfixturevalue("engine")
    with eng.begin() as conn:
        conn.execute(text("TRUNCATE sales, import_errors, import_jobs RESTART IDENTITY CASCADE"))


@pytest.fixture
def settings(database_url: str, tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,  # type: ignore[call-arg]
        database_url=database_url,
        upload_dir=tmp_path / "uploads",
        max_upload_bytes=5 * 1024 * 1024,
        max_rows=200_000,
        max_stored_errors=100,
        worker_poll_seconds=0.05,
    )


@pytest.fixture
def worker_engine(settings: Settings) -> Iterator[Engine]:
    eng = make_worker_engine(settings)
    yield eng
    eng.dispose()


def submit(engine: Engine, settings: Settings, source: str, content: bytes | Path, name: str = "sales.csv") -> int:
    data = content.read_bytes() if isinstance(content, Path) else content
    upload = store_upload(io.BytesIO(data), name, upload_dir=settings.upload_dir, max_bytes=settings.max_upload_bytes)
    with engine.begin() as conn:
        return create_job(conn, SOURCE_IDS[source], upload)


def drain(worker_engine: Engine, settings: Settings, worker_id: str = "test-worker") -> list[Outcome]:
    outcomes = []
    while (outcome := run_once(worker_engine, settings, worker_id=worker_id)) is not None:
        outcomes.append(outcome)
    return outcomes


def scalar(engine: Engine, sql: str, **params: object) -> object:
    with engine.connect() as conn:
        return conn.execute(text(sql), params).scalar_one()


def job_row(engine: Engine, job_id: int):  # type: ignore[no-untyped-def]
    with engine.connect() as conn:
        return conn.execute(text("SELECT * FROM import_jobs WHERE id = :id"), {"id": job_id}).one()


def errors_of(engine: Engine, job_id: int) -> list[tuple[object, ...]]:
    with engine.connect() as conn:
        return [
            tuple(r)
            for r in conn.execute(
                text(
                    "SELECT row_number, field, code FROM import_errors WHERE job_id = :id "
                    "ORDER BY row_number NULLS FIRST, id"
                ),
                {"id": job_id},
            )
        ]
