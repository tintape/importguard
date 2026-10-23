"""Shared per-application state for the web routes (settings, engine, templates)."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import Engine

from importguard import jobs
from importguard.config import Settings

HERE = Path(__file__).resolve().parent
MAX_PAGE = 100_000

FAILURE_LABELS = {
    "validation": "Invalid data",
    "conflict": "Conflicts with existing sales",
    "internal": "Processing error",
}


def money(value: Decimal | None) -> str:
    return "" if value is None else f"{value:,.2f}"


def number(value: int | None) -> str:
    return "" if value is None else f"{value:,}"


def filesize(value: int) -> str:
    if value < 1024:
        return f"{value} B"
    if value < 1024 * 1024:
        return f"{value / 1024:.1f} KB"
    return f"{value / (1024 * 1024):.1f} MB"


def make_templates(settings: Settings) -> Jinja2Templates:
    templates = Jinja2Templates(directory=HERE / "templates")
    tz = settings.report_tz
    templates.env.filters.update(
        money=money,
        number=number,
        filesize=filesize,
        localtime=lambda v: "" if v is None else v.astimezone(tz).strftime("%Y-%m-%d %H:%M"),
    )
    templates.env.globals.update(
        failure_labels=FAILURE_LABELS,
        report_timezone=settings.report_timezone,
        max_upload_mib=settings.max_upload_bytes // (1024 * 1024),
        max_rows=settings.max_rows,
    )
    return templates


@dataclass(frozen=True)
class WebContext:
    settings: Settings
    engine: Engine
    templates: Jinja2Templates

    @property
    def tz(self) -> ZoneInfo:
        return self.settings.report_tz

    def render(self, request: Request, name: str, context: dict[str, Any], status_code: int = 200) -> HTMLResponse:
        page = {"active": name.split(".", maxsplit=1)[0], **context}
        return self.templates.TemplateResponse(request, name, page, status_code)

    def source_ids(self) -> set[int]:
        with self.engine.connect() as conn:
            return {s.id for s in jobs.list_sources(conn)}


def web_context(request: Request) -> WebContext:
    ctx: WebContext = request.app.state.ctx
    return ctx


def page_number(request: Request) -> int:
    raw = request.query_params.get("page", "1")
    return min(int(raw), MAX_PAGE) if raw.isdigit() and int(raw) > 0 else 1
