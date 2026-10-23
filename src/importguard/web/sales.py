"""Sales: paginated, filterable table and CSV export of the filtered rows."""

from __future__ import annotations

import math
from collections.abc import Iterator
from datetime import datetime
from typing import Annotated
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse

from importguard import jobs, reports
from importguard.exports import sales_csv
from importguard.web.context import WebContext, page_number, web_context

router = APIRouter()
Ctx = Annotated[WebContext, Depends(web_context)]
PAGE_SIZE = 50


@router.get("/sales", response_class=HTMLResponse)
def sales_page(request: Request, ctx: Ctx) -> HTMLResponse:
    parsed = reports.parse_filter(request.query_params, valid_source_ids=ctx.source_ids())
    with ctx.engine.connect() as conn:
        total = reports.count_sales(conn, parsed.value, ctx.tz)
        pages = max(math.ceil(total / PAGE_SIZE), 1)
        page = min(page_number(request), pages)
        context = {
            "filters": parsed.value,
            "filter_errors": parsed.errors,
            "rows": reports.list_sales(conn, parsed.value, ctx.tz, limit=PAGE_SIZE, offset=(page - 1) * PAGE_SIZE),
            "total": total,
            "page": page,
            "pages": pages,
            "page_size": PAGE_SIZE,
            "query": urlencode(parsed.value.query_items()),
            "sources": jobs.list_sources(conn),
            "stores": reports.store_ids(conn),
            "currencies": reports.currencies_in_use(conn),
        }
    return ctx.render(request, "sales.html", context)


@router.get("/sales/export.csv")
def sales_export(request: Request, ctx: Ctx) -> Response:
    parsed = reports.parse_filter(request.query_params, valid_source_ids=ctx.source_ids())
    if parsed.errors:
        raise HTTPException(400, " ".join(parsed.errors))

    def stream() -> Iterator[str]:
        with ctx.engine.connect() as conn:
            yield from sales_csv(reports.iter_sales(conn, parsed.value, ctx.tz), ctx.tz)

    stamp = datetime.now(ctx.tz).strftime("%Y%m%d-%H%M%S")
    return StreamingResponse(
        stream(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="sales-{stamp}.csv"'},
    )
