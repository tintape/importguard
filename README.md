# ImportGuard

[![CI](https://github.com/tintape/importguard/actions/workflows/ci.yml/badge.svg)](https://github.com/tintape/importguard/actions/workflows/ci.yml)

Import, validate and reconcile daily sales exports (CSV and Parquet) **without double-counting revenue**.

A shop owner with several stores gets daily sales exports from different systems: the till, the web shop, a
marketplace. The files overlap, contain repeats and typos, and sometimes re-send a sale with a corrected amount.
Merging them by hand is exactly how the same revenue gets counted twice. ImportGuard is a small, self-hosted tool for
one operator that makes that merge safe and explainable:

- upload an export and pick the system it came from;
- see whether it was accepted and, if not, **which rows and why** (row numbers, downloadable error report);
- upload the same or an overlapping file again **without creating duplicates**;
- get revenue, sales count and average sale **per currency**, filter and export the sales;
- see what happened to every upload, including uploads interrupted by a crash.

It is a portfolio project: a deliberately small but complete product. **All data in this repository is synthetic.**
It does not describe a real customer, a commercial deployment or proven savings.

![Dashboard](docs/screenshots/dashboard.png)

| A conflicting correction is rejected as a whole | An import that survived a killed worker |
|---|---|
| ![Conflict](docs/screenshots/import-conflict.png) | ![Recovered](docs/screenshots/import-recovered.png) |

| Imports and history | Sales table with filters and CSV export |
|---|---|
| ![Imports](docs/screenshots/imports.png) | ![Sales](docs/screenshots/sales.png) |

More: [validation errors with row numbers](docs/screenshots/import-invalid.png),
[phone layout](docs/screenshots/dashboard-mobile.png). All screenshots were taken from the running application with the
demo data below (Playwright + Chrome, 1440 px wide).

## Contents

- [Quick start](#quick-start)
- [Demo scenario](#demo-scenario)
- [Architecture](#architecture)
- [Input data contract](#input-data-contract)
- [Duplicates, conflicts and re-runs](#duplicates-conflicts-and-re-runs)
- [Crash recovery](#crash-recovery)
- [Tests](#tests)
- [Measured performance](#measured-performance)
- [Security](#security)
- [Limitations and next steps](#limitations-and-next-steps)

## Quick start

### With Docker Compose

```bash
git clone https://github.com/tintape/importguard.git
cd importguard
docker compose up --build
```

Open <http://127.0.0.1:8000>. Compose starts PostgreSQL 16, applies migrations, then runs the web app and the worker.
Everything is published on `127.0.0.1` only.

Demo files are generated on the host (needs [uv](https://docs.astral.sh/uv/)):

```bash
uv run python -m importguard.sampledata      # writes ./demo-data and demo-data/expected.json
```

> The Compose stack was not run on the author's machine (no Docker there). It is exercised in CI instead: the
> `docker` job builds the image, starts the stack, uploads the demo files over HTTP, kills the worker container with
> `SIGKILL` during the 100k-row import and checks that the totals are exact afterwards
> ([scripts/compose_smoke.sh](scripts/compose_smoke.sh)).

### Without Docker

Requirements: Python 3.12, [uv](https://docs.astral.sh/uv/), PostgreSQL 15 or newer.

```bash
cp .env.example .env                       # adjust IMPORTGUARD_DATABASE_URL if needed
createuser importguard --createdb --pwprompt   # password "importguard" matches .env.example
createdb importguard --owner importguard
uv sync
uv run alembic upgrade head

uv run uvicorn --factory importguard.web.app:app_factory --host 127.0.0.1 --port 8000   # terminal 1
uv run python -m importguard.worker                                                     # terminal 2
```

## Demo scenario

The generator creates deterministic files (fixed seed) for every case the product has to handle:

| File | Source | Expected result |
|---|---|---|
| `01_pos_2026-08-31_valid.csv` | POS | 1,000 new sales |
| `02_pos_2026-08-31_valid (copy).csv` | POS | byte-identical copy: 0 new, 1,000 duplicates |
| `03_pos_2026-09-01_overlapping.csv` | POS | 700 new, 300 duplicates (written with other offsets, case and spacing) |
| `04_pos_2026-09-02_conflicting_amount.csv` | POS | **rejected**: one known sale re-sent with another price; its 40 new sales are not imported |
| `05_pos_2026-09-03_invalid_values.csv` | POS | **rejected**: 10 invalid dates and money values, reported by row |
| `06_webshop_repeated_ids.csv` | Web shop | 200 new, 20 exact repeats inside the file skipped |
| `07_webshop_conflicting_repeat.csv` | Web shop | **rejected**: the same ID twice in one file with different data |
| `08_webshop_equivalent.csv` / `.parquet` | Web shop | same 300 sales in both formats: 300 new, then 0 new |
| `09_marketplace_ids_shared_with_pos.csv` | Marketplace | 50 new: the same IDs in another source are independent |
| `10_marketplace_large_100k.parquet` / `.csv` | Marketplace | 100,000 rows |

Expected outcomes and final totals are written to `demo-data/expected.json`. They are computed by the generator from
its own records in integer cents, **independently of the import code**, and the test suite compares the database
against them.

To walk through the scenario (valid → repeat → overlap → conflict → kill the worker during the 100k import → restart),
start the web app on an empty database, stop any running worker, and run:

```bash
uv run python scripts/demo_scenario.py
```

Real output from the author's machine (blank lines removed):

```text
1. Import a valid daily export
   import #1: Imported: Imported 1,000 new sales; skipped 0 exact duplicates.
2. Upload the same content again under another name
   import #2: Imported: Imported 0 new sales; skipped 1,000 exact duplicates.
3. Import the next export, which overlaps the previous one
   import #3: Imported: Imported 700 new sales; skipped 300 exact duplicates.
4. Upload a file that changes the amount of an existing sale
   import #4: Conflicts with existing sales: Rejected: 1 conflicting sale ID. The whole file was rejected; no sales were changed.
   sales before: 1,700, after: 1,700 -> unchanged
5. Kill the worker (SIGKILL) while it processes the 100,000-row file
   killed the worker; job status at that moment: processing
   marketplace sales visible while the job is orphaned: 0
6. Restart the worker and check the result
   import #5: Imported: Imported 100,000 new sales; skipped 0 exact duplicates. Completed on attempt 2 after an interrupted attempt.

Total sales on the dashboard: 101,700; expected from the generator: 101,700 -> OK
```

## Architecture

```mermaid
flowchart LR
    B[Browser] -->|upload, filters, export| W["Web app<br/>FastAPI + Jinja2"]
    W -->|"stream to disk, random name, SHA-256"| F[("Upload folder")]
    W -->|"INSERT job (queued)<br/>read reports"| P[("PostgreSQL")]
    K["Worker process<br/>python -m importguard.worker"] -->|"claim job: advisory lock"| P
    K -->|"stream CSV / Parquet batches"| F
    K -->|"COPY into TEMP staging,<br/>validate, publish in one transaction"| P
```

- **Web app** (`importguard.web`): three server-rendered screens, a few lines of plain JavaScript (busy state, drag
  and drop, polling while a job runs). It never processes files; it stores the upload and inserts a `queued` job.
  FastAPI's [`BackgroundTasks`](https://fastapi.tiangolo.com/tutorial/background-tasks/) are not used: the FastAPI
  docs suggest a separate process for heavy work, and a job must survive a web restart.
- **Worker** (`importguard.worker`, `importguard.processing`): a separate process polling PostgreSQL. One worker is
  enough; a second one is safe (see below).
- **Readers** (`importguard.readers`): CSV is read record by record from bytes; Parquet with
  [`ParquetFile.iter_batches`](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html). No
  pandas, no whole-file loads.
- **Contract** (`importguard.contract`): one function per field that validates and normalizes a raw value.
- **Database**: four tables, one Alembic migration.

| Table | Purpose |
|---|---|
| `sources` | the three pre-created source systems (`pos`, `webshop`, `marketplace`) |
| `import_jobs` | durable job record: status, failure kind, source, original name, stored name, SHA-256, size, attempts, timestamps, rows read / inserted / skipped, error count, summary |
| `import_errors` | up to 1,000 stored problems per job: row, field, code, message, value |
| `sales` | published sales; `UNIQUE (source_id, sale_id)`, `numeric(12,2)` prices, `amount` as a generated column |

Design decisions and their trade-offs are in [docs/architecture-decisions.md](docs/architecture-decisions.md).

## Input data contract

One documented schema for both formats. Column names are matched case-insensitively and in any order; missing,
duplicate or extra columns reject the file. The source system is chosen at upload time, not stored in the file.

| Column | Rule | Normalized to | Example |
|---|---|---|---|
| `sale_id` | 1–64 characters `A-Z a-z 0-9 . _ -`, starting with a letter or digit | trimmed, **case-sensitive** | `POS-000123` |
| `sold_at` | ISO 8601 `YYYY-MM-DDTHH:MM:SS` with `Z` or `±HH:MM` offset (±14:00 max); whole seconds; not before 2000-01-01, not more than one day in the future. Parquet: time-zone-aware timestamp (any unit) or the same text | UTC instant | `2026-09-01T14:05:00+02:00` |
| `store_id` | 1–32 characters `A-Z 0-9 _ -` | upper-case | `BER-01` |
| `product_category` | 1–64 printable characters | Unicode NFKC, whitespace collapsed, lower-case | `cold drinks` |
| `quantity` | whole number 1–10,000. Parquet: integer or text | integer | `2` |
| `unit_price` | plain decimal with `.`: no sign, symbol, thousand separator or exponent; value must fit 2 decimal places; 0.01–1,000,000.00. Parquet: decimal or text, **never float** | `Decimal` with exactly 2 places (`12.5` → `12.50`) | `19.99` |
| `currency` | ISO 4217 code with two minor units: AUD, CAD, CHF, CZK, DKK, EUR, GBP, NOK, PLN, SEK, USD | upper-case | `EUR` |

- **Positive sales only** in this version: refunds (negative prices) and zero quantities are rejected.
- **Time zone**: everything is stored in UTC (`timestamptz`). The dashboard groups days in
  `IMPORTGUARD_REPORT_TIMEZONE` (default `UTC`). Timestamps without an offset are rejected because they are ambiguous.
- **Money** is `Decimal` in Python and `numeric` in PostgreSQL end to end; `amount = quantity × unit_price` is a
  generated `numeric(14,2)` column. **Different currencies are never summed**: every total is per currency.
- **CSV**: UTF-8 (BOM allowed), comma-separated, `"` quoting, LF or CRLF line endings, one header row. Row numbers in
  error reports count data rows (the header is not a row).
- **Limits** (configurable): 50 MiB per upload, 200,000 data rows per file, 4,096 characters per text value.

Normalization is tested field by field in [tests/unit/test_contract.py](tests/unit/test_contract.py), including
"different spelling, same sale" cases.

## Duplicates, conflicts and re-runs

A sale is identified by `(source, sale_id)`. Two records are **the same sale** when all normalized fields are equal.

1. A new `(source, sale_id)` is **added**.
2. An exact repeat of an existing sale is **skipped** and counted as a duplicate.
3. A known `(source, sale_id)` with different data is a **conflict**. Existing sales are **never overwritten**.
4. Repeats **inside one file** follow the same rules: exact repeats are skipped, a different repeat is a conflict.
5. Uploading identical content again for the same source creates no sales (every row is an exact duplicate). The job
   page also points out the earlier upload with the same SHA-256.
6. The same `sale_id` in different sources is two different sales.

**Strict mode**: one invalid row or one conflict rejects the whole file and nothing from it is published. All problems
found are reported together (up to 1,000 stored, all counted), with row numbers where they exist, and can be
downloaded as CSV.

**Retry** is offered only for jobs that failed for an internal reason (database error, missing file, a crash loop).
Rejected files are immutable, so re-processing them would only repeat the rejection; the fix is to upload a corrected
file. Retrying is always safe because of rule 2.

## Crash recovery

The mechanism is deliberately small: PostgreSQL is the queue, the lock manager and the publication point.

1. **Claim.** The worker takes a *session-level* advisory lock `pg_try_advisory_lock(job_id)` on a dedicated
   connection, sets the job to `processing` and increments `attempts`. It holds the lock until it is done.
2. **Validate.** The file is streamed; valid rows are `COPY`'d into a `TEMP` staging table. `sales` is not touched.
3. **Publish.** In the *same transaction*: lock the source row (`SELECT … FOR UPDATE`, which serializes publications
   per source), detect conflicts against staging and existing sales, re-check that the job is still ours (status and
   attempt number as a fencing token), insert only new sales (`ON CONFLICT DO NOTHING` as a last guard), and mark the
   job `succeeded` with its counts. One `COMMIT` makes the sales and the job result visible together.
4. **Recover.** If the worker dies at any point, PostgreSQL closes its session: the transaction is rolled back and the
   advisory lock is released. A job that is `processing` but whose lock is free is therefore orphaned, and the next
   claim simply takes it again (attempt 2). No lease timeouts or heartbeats are needed. After 3 interrupted attempts
   the job is marked failed (internal) instead of looping.

What the tests prove (real PostgreSQL, real worker processes, `os._exit` or `SIGKILL` for crashes):

| Property | Test |
|---|---|
| re-processing does not double data | `test_reprocessing_a_succeeded_job_does_not_double_count` |
| a crash during processing leaves no partially published sales | `test_hard_crash_before_commit_publishes_nothing_and_restart_completes`, `test_sigkill_during_processing_then_restart` |
| a job is not lost after restarts | `test_queued_job_survives_until_a_worker_runs`, `test_long_running_worker_processes_jobs_and_stops_gracefully` |
| a failure after the result is committed but before the worker acknowledges it does not duplicate | `test_crash_after_commit_does_not_cause_duplicates` |
| a second worker does not break integrity | `test_second_worker_cannot_take_a_job_held_by_a_live_worker`, `test_concurrent_workers_keep_totals_exact`, `test_stale_attempt_is_fenced_off` |

**What this is not.** Processing is *at least once*: a job may be processed again after a crash, and its effect is
applied once because publication is idempotent (rule 2) and atomic with the job result. This is "effectively once" for
the sales table, not a general exactly-once guarantee. Limits of the approach:

- advisory locks are tied to a database session: a connection pooler in transaction mode (e.g. PgBouncer) would break
  them; the worker must connect directly;
- a worker on a *different host* that vanishes without closing its TCP connection keeps its lock until TCP keepalive
  (configured to ~1 minute) notices; locally a killed process releases it immediately;
- publication holds a row lock per source, so imports for one source are serialized (fine for daily files);
- a job is one transaction: a 200,000-row file is one transaction of that size;
- uploaded files are kept on local disk; losing the upload folder makes queued jobs fail with an internal error.

## Tests

161 tests: 110 unit tests and 51 integration and end-to-end tests that run against **real PostgreSQL** (no SQLite)
and real worker processes. The integration suite drops and recreates the database named in
`IMPORTGUARD_TEST_DATABASE_URL`.

```bash
uv sync
export IMPORTGUARD_TEST_DATABASE_URL=postgresql+psycopg://importguard:importguard@localhost:5432/importguard_test
uv run pytest                          # all tests (about 40 s on the author's machine)
uv run pytest tests/unit               # unit tests only, no database
uv run pytest --cov                    # with coverage
uv run ruff check src tests scripts migrations && uv run ruff format --check src tests scripts migrations
uv run mypy                            # strict mode
```

What is covered: field validation and exact money arithmetic; CSV/Parquet equivalence (normalized records and import
results); repeats within and across files; independence of sources; conflicts and strict-mode atomicity; crash
recovery and concurrent workers; filters, pagination and CSV export; upload limits, content sniffing, unsafe file
names, cross-site posts, host checks, formula injection; and the full demo scenario against the generator's
independently computed totals (including the 100k file).

Line + branch coverage measured locally: **96%** (worker subprocesses included via coverage's subprocess support). The
coverage number is a by-product; the tests above are the point.

CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)) runs ruff, ruff format, strict mypy, migrations on an empty
PostgreSQL 16 database (upgrade, model check, downgrade, upgrade), the full test suite with coverage, and the Docker
Compose smoke test. Locally the suite was run against PostgreSQL 15.

## Measured performance

`uv run python scripts/benchmark.py --runs 3` imports the 100,000-row demo files, each in a fresh worker process
(`python -m importguard.worker --once`) against a separate benchmark database. Wall time and peak RSS come from
`os.wait4` for that process; processing time is the job's `finished_at − started_at`.

One run of the script, 3 runs per case (median, min–max):

| Case | Worker process wall time, s | Job processing time, s | Peak RSS, MiB |
|---|---|---|---|
| Worker start-up only (no job) | — | — | 102 (101–102) |
| Parquet, 100k new rows | 4.49 (4.36–4.61) | 3.67 (3.54–3.80) | 128 (126–128) |
| Parquet, same file again (100k duplicates) | 1.87 (1.85–1.88) | 1.06 (1.06–1.07) | 127 (126–127) |
| CSV, 100k new rows | 4.23 (4.10–4.29) | 3.43 (3.28–3.49) | 103 (102–103) |
| CSV, same file again (100k duplicates) | 2.09 (2.02–2.21) | 1.26 (1.22–1.39) | 102 (102–103) |

Environment: Apple M1 Pro (10 cores), 16 GiB RAM, macOS 26.6 (arm64), Python 3.12.8, PostgreSQL 15.19
(Homebrew, default settings, same machine), pyarrow 26.0.0, SQLAlchemy 2.1.6, psycopg 3.3.6.

How to read it: about 100 MiB of the peak is the interpreter with pyarrow and SQLAlchemy loaded; memory stays flat
because files are streamed (Parquet in 10,000-row batches). Most of the time is per-row validation in Python and the
`COPY` + insert in PostgreSQL. This is a single local measurement on a laptop, not an industrial benchmark; numbers
on other hardware, PostgreSQL settings or data shapes will differ.

## Security

ImportGuard is a **local demonstration tool for one operator, not a public SaaS**. By default it listens on
`127.0.0.1` only (Compose publishes `127.0.0.1:8000`; the database is not published).

Built in:

- upload size limit enforced while the request streams in (not after buffering), plus a row limit and a per-value
  length limit; Parquet decompression guards (uncompressed size and compression ratio from the footer) and a memory
  limit on the worker container;
- the client's file name and extension are never trusted: files are stored under random server-generated names,
  the format is detected from content, the original name is only displayed (escaped);
- parameterized SQL everywhere (SQLAlchemy Core / bound parameters);
- CSV exports and error reports are protected against formula injection (`=`, `+`, `-`, `@`, tab, CR prefixed with
  `'`);
- cross-site form posts are rejected (Origin / Fetch Metadata check), only `localhost` / `127.0.0.1` host headers are
  accepted (DNS rebinding), strict Content-Security-Policy, `nosniff`, `frame-ancestors 'none'`;
- no secrets in the repository; `.env.example` holds local-only defaults.

**Required before exposing it to a network:** authentication and authorization (there are no users at all), HTTPS,
real CSRF tokens, rate limiting on uploads, secrets from a secret manager instead of default passwords, malware
scanning or isolation of uploads, retention and deletion of uploaded files, database backups, audit logging and
monitoring of the worker.

## Limitations and next steps

Limitations of this version:

- one organization, one operator, no login; three fixed sources;
- positive sales only: no refunds, cancellations or corrections; a corrected amount is a conflict by design;
- one schema; no column mapping;
- files and the database must be on the same machine (local upload folder);
- conflicts must be resolved outside the tool (there is no "accept the new value" action);
- the dashboard is computed on request; fine for hundreds of thousands of rows, not for many millions.

Reasonable next steps, in order of value:

1. explicit corrections: a reviewed "replace" action for conflicts with an audit trail, and refunds as negative lines;
2. per-source column mapping for exports whose headers differ;
3. object storage for uploads and a retention policy;
4. authentication for a small team, then multi-tenancy;
5. pre-aggregated daily totals if data volume grows.

## Project layout

```text
src/importguard/
  contract.py      field rules and normalization
  readers.py       streaming CSV / Parquet readers
  processing.py    claim, validate, stage, publish
  worker.py        the worker process
  uploads.py       safe upload storage
  jobs.py          job records and retry
  reports.py       dashboard and sales queries
  exports.py       CSV export with formula-injection protection
  sampledata.py    deterministic demo data + independent expected totals
  web/             FastAPI routes, templates, CSS, a little JavaScript
migrations/        Alembic
scripts/           demo scenario, benchmark, Compose smoke test
tests/             unit/ and integration/ (real PostgreSQL)
docs/              architecture decisions, video script, screenshots
```

## License

[MIT](LICENSE)
