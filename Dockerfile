# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:0.12.23 AS uv

FROM python:3.12.14-slim-bookworm

COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    IMPORTGUARD_UPLOAD_DIR=/data/uploads

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY alembic.ini ./
COPY migrations ./migrations
COPY src ./src
RUN uv sync --frozen --no-dev

RUN useradd --create-home --uid 10001 importguard \
    && mkdir -p /data/uploads \
    && chown importguard:importguard /data/uploads
USER importguard

EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/healthz', timeout=2)"]
# Inside the container the server listens on all interfaces; docker-compose.yml publishes it on 127.0.0.1 only.
CMD ["uvicorn", "--factory", "importguard.web.app:app_factory", "--host", "0.0.0.0", "--port", "8000"]
