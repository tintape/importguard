"""Runtime settings, read from environment variables (prefix ``IMPORTGUARD_``)."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

MIB = 1024 * 1024


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="IMPORTGUARD_", env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://importguard:importguard@localhost:5432/importguard"
    upload_dir: Path = Path("var/uploads")

    max_upload_bytes: int = Field(default=50 * MIB, gt=0)
    max_rows: int = Field(default=200_000, gt=0)
    max_stored_errors: int = Field(default=1_000, gt=0)
    max_attempts: int = Field(default=3, gt=0)

    report_timezone: str = "UTC"
    allowed_hosts: list[str] = ["localhost", "127.0.0.1"]

    worker_poll_seconds: float = Field(default=1.0, gt=0)
    parquet_batch_rows: int = Field(default=10_000, gt=0)

    @field_validator("report_timezone")
    @classmethod
    def _known_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(f"unknown time zone: {value!r}") from exc
        return value

    @property
    def report_tz(self) -> ZoneInfo:
        return ZoneInfo(self.report_timezone)


@lru_cache
def get_settings() -> Settings:
    return Settings()
