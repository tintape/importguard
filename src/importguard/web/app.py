"""FastAPI application: three server-rendered screens (Dashboard, Imports, Sales).

The web process never processes files. An upload is stored on disk and recorded
as a ``queued`` job; the separate worker process does the rest. This follows
the FastAPI guidance that heavy work belongs in a separate process rather than
``BackgroundTasks``, and it lets queued jobs survive restarts.
"""

from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from sqlalchemy import Engine, text
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.trustedhost import TrustedHostMiddleware

from importguard.config import Settings, get_settings
from importguard.db import make_engine
from importguard.web import dashboard, imports, sales
from importguard.web.context import HERE, WebContext, make_templates
from importguard.web.security import BodySizeLimit, SameOriginPosts, SecurityHeaders

log = logging.getLogger(__name__)


def create_app(settings: Settings | None = None, engine: Engine | None = None) -> FastAPI:
    settings = settings or get_settings()
    ctx = WebContext(settings, engine or make_engine(settings.database_url), make_templates(settings))

    app = FastAPI(title="ImportGuard", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.ctx = ctx
    app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")
    for module in (dashboard, imports, sales):
        app.include_router(module.router)
    _add_health_check(app, ctx)
    _add_error_pages(app, ctx)

    app.add_middleware(SecurityHeaders)
    app.add_middleware(SameOriginPosts)
    app.add_middleware(BodySizeLimit, max_body_bytes=settings.max_upload_bytes)
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=settings.allowed_hosts)
    return app


def _add_health_check(app: FastAPI, ctx: WebContext) -> None:
    @app.get("/healthz")
    def healthz() -> JSONResponse:
        try:
            with ctx.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception:
            log.exception("health check failed")
            return JSONResponse({"status": "error", "database": "unavailable"}, status_code=503)
        return JSONResponse({"status": "ok"})


def _add_error_pages(app: FastAPI, ctx: WebContext) -> None:
    def error_page(request: Request, status_code: int, detail: str) -> Response:
        if request.url.path.endswith((".csv", "/status")):
            return JSONResponse({"detail": detail}, status_code=status_code)
        return ctx.render(request, "error.html", {"status_code": status_code, "detail": detail}, status_code)

    @app.exception_handler(StarletteHTTPException)
    def http_error(request: Request, exc: StarletteHTTPException) -> Response:
        return error_page(request, exc.status_code, str(exc.detail))

    @app.exception_handler(RequestValidationError)
    def validation_error(request: Request, exc: RequestValidationError) -> Response:
        return error_page(request, 400, "The request has an invalid parameter.")


def app_factory() -> FastAPI:
    """Entry point for ``uvicorn --factory importguard.web.app:app_factory``."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    return create_app()
