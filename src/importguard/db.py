"""Engine construction. The web app uses a pool; the worker uses NullPool (see worker.py)."""

from __future__ import annotations

from typing import Any

from sqlalchemy import Engine, create_engine

from importguard.config import get_settings


def make_engine(url: str | None = None, **kwargs: Any) -> Engine:
    return create_engine(url or get_settings().database_url, pool_pre_ping=True, **kwargs)
