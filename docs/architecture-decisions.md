# Architecture decisions

Short records of the decisions that shape ImportGuard. Each lists what was chosen, why, and what it costs.

## 1. PostgreSQL is the job queue

**Decision.** Jobs are rows in `import_jobs`; the worker polls every second. No Redis, Celery or message broker.

**Why.** The job record, its result and the published sales must change in one transaction. With the queue in the
same database this is a single `COMMIT`; with an external broker it becomes a distributed-commit problem.
One worker polling once a second is negligible load.

**Cost.** Polling latency of up to a second; not designed for hundreds of workers.

## 2. A separate worker process, not FastAPI `BackgroundTasks`

**Decision.** The web process stores the file and inserts a `queued` job; `python -m importguard.worker` does the work.

**Why.** `BackgroundTasks` run inside the web process after the response: a restart or crash of the web process loses
the work, and heavy CPU work competes with requests. The FastAPI documentation itself points to bigger tools for heavy
background computation. A job stored in the database survives restarts of either process.

**Cost.** Two processes to run (Compose starts both).

## 3. Session-level advisory locks as claims; no leases

**Decision.** A worker claims a job with `pg_try_advisory_lock(job_id)` on a dedicated connection (SQLAlchemy
`NullPool`) and holds it while processing. A `processing` job whose lock can be taken is orphaned and is re-claimed.

**Why.** PostgreSQL releases the lock when the session ends, which is exactly "the worker died". There is no lease
duration to tune, no heartbeat thread, no clock skew. `attempts` is incremented at each claim and checked under
`SELECT … FOR UPDATE` before writing (fencing), so even a worker that lost its claim cannot publish.

**Cost.** Incompatible with transaction-mode connection poolers. A worker on another host that disappears without
closing TCP holds its lock until keepalive detects it (the worker sets `tcp_keepalives_*` for its server
session, about one minute). Advisory lock keys
share one namespace per database (only ImportGuard uses them here).

## 4. Validate into a TEMP table, publish in the same transaction

**Decision.** Valid normalized rows are streamed with `COPY` into a `TEMP … ON COMMIT DROP` table. Conflict detection
is SQL over staging and `sales`. Publication is `INSERT … SELECT DISTINCT ON (sale_id) … WHERE NOT EXISTS … ON CONFLICT
DO NOTHING`, followed by the job update, then one `COMMIT`.

**Why.** Validation is separated from publication, yet nothing partially processed can ever be visible: staging is
private to the session and `sales` changes only at the final commit together with the job result. `COPY` keeps
100,000 rows fast; SQL does the set comparisons PostgreSQL is good at. The source row is locked first, so conflict
checks and inserts for a source never interleave.

**Cost.** One transaction per file; very large files mean long transactions. Imports for the same source are
serialized.

## 5. Idempotent publication instead of "exactly once"

**Decision.** Re-processing any file is harmless: rows that already exist with identical normalized data are skipped.

**Why.** Crash recovery re-runs jobs, so processing is at least once. Because publication is idempotent and atomic with
the job result, the effect on `sales` happens once. This also makes uploading the same file twice safe without
special-casing file hashes (the SHA-256 is still shown to the operator).

**Cost.** Corrections cannot be expressed by re-sending a sale; they are conflicts (see limitations in the README).

## 6. Strict mode: the file is the unit of acceptance

**Decision.** Any invalid row or conflict rejects the whole file; all problems are reported at once.

**Why.** Partially imported daily exports are the hardest state to reason about ("which half got in?"). Rejecting the
file keeps the rule simple for the operator: fix and re-upload. Reporting validation errors and conflicts together
avoids fix-one-error-at-a-time loops.

**Cost.** One bad row blocks the good ones until the file is fixed.

## 7. Normalize before comparing; compare all significant fields

**Decision.** Each field has one canonical form (UTC instant, upper-case store, NFKC lower-case category, `Decimal`
with two places, upper-case currency). Duplicate vs conflict is decided by comparing all non-key fields after
normalization; `sale_id` stays case-sensitive.

**Why.** Different systems print the same sale differently (`+02:00` vs `Z`, `12.5` vs `12.50`). Without
normalization those would be false conflicts; with too much normalization real differences would be hidden. The
rules are listed in the README and tested field by field.

**Cost.** A system that changes case in IDs between exports would produce new sales; this is documented.

## 8. Money: `Decimal` and `numeric`, never float; currencies never mixed

**Decision.** Prices are `numeric(12,2)`, amounts a generated `numeric(14,2)` column; Parquet `float` columns are
rejected at the schema level; every aggregate is grouped by currency.

**Why.** Binary floating point cannot represent most cent values exactly; summing different currencies produces a
number with no meaning.

**Cost.** Currencies with zero or three minor units are not supported in this version.

## 9. Streaming readers without pandas

**Decision.** CSV is decoded line by line from bytes (exact line numbers for encoding errors) and parsed with the
standard `csv` module; Parquet is read with `ParquetFile.iter_batches` (10,000 rows), with schema checks before
reading and footer-based decompression guards.

**Why.** Constant memory regardless of file size, and precise error locations. pandas would load whole files and
coerce types silently (e.g. prices to float).

**Cost.** Per-row validation in Python is the main CPU cost (about 3.5 s per 100,000 rows on an M1 Pro).

## 10. Server-rendered UI with a little JavaScript

**Decision.** Jinja2 templates, one stylesheet, about 50 lines of vanilla JavaScript for progressive enhancement. No SPA,
no CDN assets.

**Why.** Three screens of tables and forms do not need a client framework. Everything works without JavaScript, the
strict CSP (`script-src 'self'`) holds, and the tool runs offline.

**Cost.** Live updates are done by polling/refresh rather than push.
