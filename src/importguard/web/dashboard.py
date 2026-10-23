"""Dashboard: revenue, sales count and average sale per currency."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from importguard import jobs, reports
from importguard.web.context import WebContext, web_context

router = APIRouter()
RECENT_JOBS = 6


@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request, ctx: Annotated[WebContext, Depends(web_context)]) -> HTMLResponse:
    parsed = reports.parse_filter(request.query_params, valid_source_ids=ctx.source_ids())
    with ctx.engine.connect() as conn:
        context = {
            "filters": parsed.value,
            "filter_errors": parsed.errors,
            "kpis": reports.currency_kpis(conn, parsed.value, ctx.tz),
            "series": reports.daily_series(conn, parsed.value, ctx.tz),
            "recent_jobs": jobs.list_jobs(conn, limit=RECENT_JOBS),
            "sources": jobs.list_sources(conn),
            "stores": reports.store_ids(conn),
            "has_any_sales": bool(reports.currencies_in_use(conn)),
        }
    return ctx.render(request, "dashboard.html", context)
