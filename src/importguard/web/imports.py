"""Imports: upload, history, details, error report and safe retry."""

from __future__ import annotations

import logging
import math
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from sqlalchemy import Connection

from importguard import jobs
from importguard.exports import errors_csv
from importguard.uploads import StoredUpload, UploadRejected, store_upload
from importguard.web.context import WebContext, page_number, web_context

log = logging.getLogger(__name__)
router = APIRouter()
Ctx = Annotated[WebContext, Depends(web_context)]

IMPORTS_PAGE_SIZE = 25
ERROR_PREVIEW_ROWS = 50


def _history(conn: Connection, page: int) -> dict[str, Any]:
    total = jobs.count_jobs(conn)
    return {
        "jobs": jobs.list_jobs(conn, limit=IMPORTS_PAGE_SIZE, offset=(page - 1) * IMPORTS_PAGE_SIZE),
        "sources": jobs.list_sources(conn),
        "page": page,
        "pages": max(math.ceil(total / IMPORTS_PAGE_SIZE), 1),
        "total": total,
    }


def _load_job(conn: Connection, job_id: int) -> Any:
    job = jobs.get_job(conn, job_id)
    if job is None:
        raise HTTPException(404, "Import not found")
    return job


@router.get("/imports", response_class=HTMLResponse)
def imports_page(request: Request, ctx: Ctx) -> HTMLResponse:
    with ctx.engine.connect() as conn:
        context = _history(conn, page_number(request))
    return ctx.render(request, "imports.html", context)


def _accept_upload(ctx: WebContext, source_id: str, file: UploadFile | None) -> StoredUpload:
    if not source_id.isdigit():
        raise UploadRejected(400, "Choose the source system this file came from.")
    if file is None or not file.filename:
        raise UploadRejected(400, "Choose a CSV or Parquet file to upload.")
    with ctx.engine.connect() as conn:
        if jobs.get_source(conn, int(source_id)) is None:
            raise UploadRejected(400, "Unknown source.")
    settings = ctx.settings
    return store_upload(file.file, file.filename, upload_dir=settings.upload_dir, max_bytes=settings.max_upload_bytes)


@router.post("/imports", response_model=None)
def upload(
    request: Request, ctx: Ctx, source_id: Annotated[str, Form()] = "", file: UploadFile | None = None
) -> HTMLResponse | RedirectResponse:
    try:
        stored = _accept_upload(ctx, source_id, file)
    except UploadRejected as exc:
        with ctx.engine.connect() as conn:
            context = {**_history(conn, 1), "upload_error": exc.message, "selected_source": source_id}
        return ctx.render(request, "imports.html", context, status_code=exc.status_code)
    try:
        with ctx.engine.begin() as conn:
            job_id = jobs.create_job(conn, int(source_id), stored)
    except Exception:
        (ctx.settings.upload_dir / stored.stored_filename).unlink(missing_ok=True)
        raise
    log.info("job %s queued: %s (%s bytes)", job_id, stored.original_filename, stored.size_bytes)
    return RedirectResponse(f"/imports/{job_id}", status_code=303)


@router.get("/imports/{job_id}", response_class=HTMLResponse)
def import_detail(request: Request, job_id: int, ctx: Ctx) -> HTMLResponse:
    with ctx.engine.connect() as conn:
        job = _load_job(conn, job_id)
        context = {
            "job": job,
            "errors": jobs.job_errors(conn, job_id, limit=ERROR_PREVIEW_ROWS),
            "identical": jobs.earlier_identical_job(conn, job),
            "retryable": jobs.is_retryable(job),
            "active_job": job.status in jobs.ACTIVE_STATUSES,
            "retried": request.query_params.get("retried") == "1",
        }
    return ctx.render(request, "import_detail.html", context)


@router.get("/imports/{job_id}/status")
def import_status(job_id: int, ctx: Ctx) -> JSONResponse:
    with ctx.engine.connect() as conn:
        job = _load_job(conn, job_id)
    return JSONResponse({"id": job.id, "status": job.status, "failure_kind": job.failure_kind})


@router.post("/imports/{job_id}/retry")
def retry(job_id: int, ctx: Ctx) -> RedirectResponse:
    with ctx.engine.begin() as conn:
        _load_job(conn, job_id)
        requeued = jobs.retry_job(conn, job_id)
    return RedirectResponse(f"/imports/{job_id}{'?retried=1' if requeued else ''}", status_code=303)


@router.get("/imports/{job_id}/errors.csv")
def errors_report(job_id: int, ctx: Ctx) -> Response:
    with ctx.engine.connect() as conn:
        _load_job(conn, job_id)
        rows = jobs.job_errors(conn, job_id, limit=None)
    return StreamingResponse(
        errors_csv(rows),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="import-{job_id}-errors.csv"'},
    )
