"""Database schema (SQLAlchemy Core). Alembic migrations must match this file."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    Computed,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    MetaData,
    Numeric,
    SmallInteger,
    String,
    Table,
    Text,
    UniqueConstraint,
    func,
)

JOB_STATUSES = ("queued", "processing", "succeeded", "failed")
FAILURE_KINDS = ("validation", "conflict", "internal")
FILE_FORMATS = ("csv", "parquet")

metadata = MetaData(
    naming_convention={
        "ix": "ix_%(table_name)s_%(column_0_N_name)s",
        "uq": "uq_%(table_name)s_%(column_0_N_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    }
)


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


sources = Table(
    "sources",
    metadata,
    Column("id", SmallInteger, primary_key=True, autoincrement=False),
    Column("code", String(32), nullable=False, unique=True),
    Column("name", String(100), nullable=False),
    Column("description", Text, nullable=False, server_default=""),
)

import_jobs = Table(
    "import_jobs",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("source_id", SmallInteger, ForeignKey("sources.id"), nullable=False),
    Column("original_filename", String(255), nullable=False),
    Column("stored_filename", String(64), nullable=False, unique=True),
    Column("file_format", String(16), nullable=False),
    Column("size_bytes", BigInteger, nullable=False),
    Column("content_sha256", String(64), nullable=False),
    Column("status", String(16), nullable=False, server_default="queued"),
    Column("failure_kind", String(16)),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("worker_id", String(100)),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("started_at", DateTime(timezone=True)),
    Column("finished_at", DateTime(timezone=True)),
    Column("rows_read", Integer),
    Column("rows_inserted", Integer),
    Column("rows_skipped", Integer),
    Column("error_count", Integer, nullable=False, server_default="0"),
    Column("message", Text),
    CheckConstraint(_in("status", JOB_STATUSES), name="status"),
    CheckConstraint(_in("failure_kind", FAILURE_KINDS), name="failure_kind"),
    CheckConstraint(_in("file_format", FILE_FORMATS), name="file_format"),
    CheckConstraint("(status = 'failed') = (failure_kind IS NOT NULL)", name="failure_kind_iff_failed"),
    CheckConstraint(
        "status <> 'succeeded' OR rows_read = rows_inserted + rows_skipped",
        name="succeeded_counts_add_up",
    ),
    Index(None, "status", "created_at"),
    Index(None, "source_id", "content_sha256"),
)

import_errors = Table(
    "import_errors",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("job_id", BigInteger, ForeignKey("import_jobs.id", ondelete="CASCADE"), nullable=False, index=True),
    Column("row_number", Integer),
    Column("field", String(32)),
    Column("code", String(40), nullable=False),
    Column("message", Text, nullable=False),
    Column("raw_value", String(200)),
)

sales = Table(
    "sales",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("source_id", SmallInteger, ForeignKey("sources.id"), nullable=False),
    Column("sale_id", String(64), nullable=False),
    Column("sold_at", DateTime(timezone=True), nullable=False),
    Column("store_id", String(32), nullable=False),
    Column("product_category", String(64), nullable=False),
    Column("quantity", Integer, nullable=False),
    Column("unit_price", Numeric(12, 2), nullable=False),
    Column("currency", String(3), nullable=False),
    Column("amount", Numeric(14, 2), Computed("quantity * unit_price", persisted=True), nullable=False),
    Column("import_job_id", BigInteger, ForeignKey("import_jobs.id"), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("source_id", "sale_id"),
    CheckConstraint("quantity BETWEEN 1 AND 10000", name="quantity_range"),
    CheckConstraint("unit_price > 0 AND unit_price <= 1000000", name="unit_price_range"),
    CheckConstraint("currency ~ '^[A-Z]{3}$'", name="currency_format"),
    Index(None, "sold_at"),
    Index(None, "store_id", "sold_at"),
    Index(None, "import_job_id"),
)
