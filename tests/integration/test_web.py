"""HTTP-level tests: end-to-end user flow, filters, export and upload safety."""

from __future__ import annotations

import csv
import io
from collections.abc import Iterator

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, text

from importguard.config import Settings
from importguard.web.app import create_app
from tests.integration.conftest import drain, job_row

HEADER = "sale_id,sold_at,store_id,product_category,quantity,unit_price,currency\n"
GOOD = (
    HEADER
    + "W-1,2026-09-01T10:00:00Z,BER-01,coffee,2,3.50,EUR\n"
    + "W-2,2026-09-02T23:30:00Z,BER-02,tea,1,2.10,EUR\n"
    + "W-3,2026-09-02T12:00:00Z,LON-01,coffee,3,4.00,GBP\n"
).encode()


@pytest.fixture
def client(engine: Engine, settings: Settings) -> Iterator[TestClient]:
    app = create_app(settings, engine)
    with TestClient(app, base_url="http://localhost") as test_client:
        yield test_client


def upload(client: TestClient, content: bytes, *, name: str = "sales.csv", source: str = "1", **kw: object):  # type: ignore[no-untyped-def]
    return client.post(
        "/imports",
        data={"source_id": source},
        files={"file": (name, content, "text/csv")},
        follow_redirects=False,
        **kw,
    )


def read_csv(body: str) -> list[list[str]]:
    return list(csv.reader(io.StringIO(body)))


# ------------------------------------------------------------------ end-to-end flow


def test_upload_process_report_and_export(
    client: TestClient, engine: Engine, worker_engine: Engine, settings: Settings
) -> None:
    response = upload(client, GOOD, name="day-1.csv")
    assert response.status_code == 303
    location = response.headers["location"]
    assert location == "/imports/1"

    page = client.get(location)
    assert "Queued" in page.text and 'http-equiv="refresh"' in page.text

    drain(worker_engine, settings)
    page = client.get(location)
    assert "Imported 3 new sales; skipped 0 exact duplicates." in page.text
    assert 'http-equiv="refresh"' not in page.text

    dashboard = client.get("/")
    assert dashboard.status_code == 200
    assert "9.10" in dashboard.text and "EUR" in dashboard.text  # 2*3.50 + 2.10
    assert "12.00" in dashboard.text and "GBP" in dashboard.text  # reported separately

    export = client.get("/sales/export.csv")
    assert export.headers["content-type"].startswith("text/csv")
    rows = read_csv(export.text)
    assert rows[0][:3] == ["source", "sale_id", "sold_at_utc"]
    assert sorted(r[1] for r in rows[1:]) == ["W-1", "W-2", "W-3"]


def test_reupload_shows_identical_content_and_adds_nothing(
    client: TestClient, engine: Engine, worker_engine: Engine, settings: Settings
) -> None:
    upload(client, GOOD, name="day-1.csv")
    second = upload(client, GOOD, name="day-1 renamed.csv")
    drain(worker_engine, settings)

    page = client.get(second.headers["location"])
    assert "Same content as import #1" in page.text
    assert "Imported 0 new sales; skipped 3 exact duplicates." in page.text
    assert client.get("/sales").text.count("<tr>") == 1 + 3  # header + 3 rows


def test_conflict_page_and_error_report(
    client: TestClient, engine: Engine, worker_engine: Engine, settings: Settings
) -> None:
    upload(client, GOOD)
    bad = upload(client, (HEADER + "W-1,2026-09-01T10:00:00Z,BER-01,coffee,2,3.99,EUR\n").encode())
    drain(worker_engine, settings)

    job_id = bad.headers["location"].rsplit("/", 1)[1]
    page = client.get(f"/imports/{job_id}")
    assert "Conflicts with existing sales" in page.text
    assert "existing 3.50, file 3.99" in page.text
    assert "Retry processing" not in page.text  # rejections are not retryable

    report = client.get(f"/imports/{job_id}/errors.csv")
    assert report.status_code == 200
    rows = read_csv(report.text)
    assert rows[0] == ["row", "field", "code", "message", "raw_value"]
    assert rows[1][:3] == ["1", "unit_price", "conflict_existing"]


def test_retry_button_requeues_internal_failure(
    client: TestClient, engine: Engine, worker_engine: Engine, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    response = upload(client, GOOD)
    monkeypatch.setenv("IMPORTGUARD_FAULTS", "before_commit=raise")
    drain(worker_engine, settings)
    monkeypatch.delenv("IMPORTGUARD_FAULTS")
    location = response.headers["location"]
    assert "Retry processing" in client.get(location).text

    retried = client.post(f"{location}/retry", follow_redirects=False)
    assert retried.headers["location"] == f"{location}?retried=1"
    assert client.get(retried.headers["location"]).text.count("Re-queued") >= 1
    drain(worker_engine, settings)
    assert job_row(engine, 1).status == "succeeded"
    again = client.post(f"{location}/retry", follow_redirects=False)  # nothing to retry any more
    assert again.headers["location"] == location


def test_status_endpoint_and_missing_job(client: TestClient) -> None:
    upload(client, GOOD)
    assert client.get("/imports/1/status").json() == {"id": 1, "status": "queued", "failure_kind": None}
    assert client.get("/imports/999").status_code == 404
    assert client.get("/imports/999/status").status_code == 404
    assert client.get("/imports/not-a-number").status_code == 400


# ------------------------------------------------------------------ filters, pagination, export


@pytest.fixture
def seeded(client: TestClient, worker_engine: Engine, settings: Settings) -> TestClient:
    rows = "".join(
        f"P-{i:03d},2026-09-{1 + i % 10:02d}T12:00:00Z,{'BER-01' if i % 2 else 'MUC-01'},c,1,1.00,EUR\n"
        for i in range(120)
    )
    upload(client, (HEADER + rows).encode())
    upload(client, GOOD, source="2")
    drain(worker_engine, settings)
    return client


def test_sales_filters_and_pagination(seeded: TestClient) -> None:
    page1 = seeded.get("/sales")
    assert "1–50 of 123" in page1.text
    last = seeded.get("/sales?page=3")
    assert "101–123 of 123" in last.text
    assert "121–123 of 123" not in seeded.get("/sales?page=99").text  # clamps to the last page

    by_store = seeded.get("/sales?store=ber-01")  # case-insensitive store filter
    assert "of 61" in by_store.text  # 60 POS rows + W-1 from the web shop
    by_source = seeded.get("/sales?source=2")
    assert "1–3 of 3" in by_source.text
    by_day = seeded.get("/sales?date_from=2026-09-02&date_to=2026-09-02")
    assert "1–14 of 14" in by_day.text  # 12 POS rows + W-2 and W-3
    by_currency = seeded.get("/sales?currency=GBP")
    assert "1–1 of 1" in by_currency.text


def test_invalid_filters_are_reported_not_fatal(seeded: TestClient) -> None:
    page = seeded.get("/sales?date_from=yesterday&source=99&store=<script>&currency=XXX")
    assert page.status_code == 200
    assert "Some filters were ignored" in page.text
    assert "<script>" not in page.text
    assert "of 123" in page.text
    assert seeded.get("/sales/export.csv?date_from=nope").status_code == 400


def test_export_respects_filters(seeded: TestClient) -> None:
    rows = read_csv(seeded.get("/sales/export.csv?store=MUC-01&date_from=2026-09-01&date_to=2026-09-03").text)
    assert len(rows) == 1 + 24  # even P-ids on days 1 and 3 (2 is odd -> BER-01)
    assert {r[4] for r in rows[1:]} == {"MUC-01"}


def test_dashboard_filters(seeded: TestClient) -> None:
    page = seeded.get("/?source=1&store=BER-01")
    assert "60.00" in page.text
    assert "GBP" not in page.text.split("Revenue by day")[0].split("Sales overview")[1]
    empty = seeded.get("/?date_from=2020-01-01&date_to=2020-01-31")
    assert "No sales match these filters" in empty.text


@pytest.mark.parametrize(
    "path", ["/sales?page=\u00b2", "/?source=\u00b2", "/sales?date_to=9999-12-31", "/imports?page=99999999999999999999"]
)
def test_odd_query_values_never_cause_a_server_error(client: TestClient, path: str) -> None:
    assert client.get(path).status_code == 200


def test_non_ascii_digit_source_is_rejected(client: TestClient) -> None:
    assert upload(client, GOOD, source="\u00b2").status_code == 400


def test_empty_states(client: TestClient) -> None:
    assert "No sales yet" in client.get("/").text
    assert "No imports yet" in client.get("/imports").text
    assert "No sales yet" in client.get("/sales").text


# ------------------------------------------------------------------ upload safety


def test_unsafe_filename_is_neutralized(client: TestClient, settings: Settings) -> None:
    response = upload(client, GOOD, name="..\\..\\etc\\<b>passwd.csv")
    assert response.status_code == 303
    stored = list(settings.upload_dir.iterdir())
    assert len(stored) == 1 and stored[0].parent == settings.upload_dir
    assert stored[0].name.endswith(".csv") and "passwd" not in stored[0].name
    page = client.get(response.headers["location"]).text
    assert "&lt;b&gt;passwd.csv" in page and "<b>passwd" not in page and "etc" not in page.split("<h1>")[1][:200]


def test_extension_is_not_trusted(client: TestClient, settings: Settings) -> None:
    table = pa.table({"sale_id": ["X-1"]})
    sink = io.BytesIO()
    pq.write_table(table, sink)
    response = upload(client, sink.getvalue(), name="looks-like.csv")
    assert response.status_code == 303
    assert next(settings.upload_dir.iterdir()).suffix == ".parquet"
    assert "PARQUET" in client.get(response.headers["location"]).text

    binary = upload(client, b"\x89PNG\r\n\x1a\n\x00\x00\x00binary", name="sales.csv")
    assert binary.status_code == 415
    assert "Unsupported file" in binary.text


def test_upload_validation_messages(client: TestClient) -> None:
    assert "Choose the source" in upload(client, GOOD, source="").text
    assert upload(client, GOOD, source="42").status_code == 400
    assert "The file is empty" in upload(client, b"").text
    no_file = client.post("/imports", data={"source_id": "1"}, follow_redirects=False)
    assert no_file.status_code == 400


def test_upload_size_limit(engine: Engine, settings: Settings) -> None:
    small = settings.model_copy(update={"max_upload_bytes": 1024 * 1024})
    with TestClient(create_app(small, engine), base_url="http://localhost") as client:
        too_big = HEADER.encode() + b"x" * (2 * 1024 * 1024)
        response = upload(client, too_big)
        assert response.status_code == 413
        just_over = HEADER.encode() + b"x" * (1024 * 1024)  # passes the transport limit, caught while storing
        response = upload(client, just_over)
        assert response.status_code == 413
        assert "larger than the 1 MiB limit" in response.text
    assert not small.upload_dir.exists() or not any(small.upload_dir.iterdir())  # partial files removed


def test_chunked_oversized_upload_gets_413(engine: Engine, settings: Settings) -> None:
    small = settings.model_copy(update={"max_upload_bytes": 1024 * 1024})

    def chunks():  # type: ignore[no-untyped-def]  # no Content-Length: transfer is chunked
        boundary = b'--x\r\nContent-Disposition: form-data; name="file"; filename="a.csv"\r\n\r\n'
        yield boundary
        for _ in range(3):
            yield b"y" * (1024 * 1024)

    with TestClient(create_app(small, engine), base_url="http://localhost") as client:
        response = client.post(
            "/imports", content=chunks(), headers={"Content-Type": "multipart/form-data; boundary=x"}
        )
    assert response.status_code == 413
    assert "content-security-policy" in response.headers  # headers also cover middleware answers


def test_nul_bytes_are_rejected_as_invalid_data(
    client: TestClient, engine: Engine, worker_engine: Engine, settings: Settings
) -> None:
    assert upload(client, GOOD + b"W-9,\x00,a,b,c,d,e\n").status_code == 415  # caught by the content sniffer
    padding = "".join(f"W-{i},2026-09-01T10:00:00Z,BER-01,c,1,1.00,EUR\n" for i in range(300)).encode()
    job = upload(client, GOOD + padding + b"W-x,2026-09-01T10:00:00Z,BER-01,c,1,1\x00,EUR\n")  # beyond 8 KiB
    [outcome] = drain(worker_engine, settings)
    assert (outcome.status, outcome.failure_kind) == ("failed", "validation")  # not an internal error
    assert "NUL byte" in client.get(job.headers["location"]).text


def test_cross_site_post_is_rejected(client: TestClient) -> None:
    evil = upload(client, GOOD, headers={"Origin": "https://evil.example"})
    assert evil.status_code == 403
    fetch_meta = upload(client, GOOD, headers={"Sec-Fetch-Site": "cross-site"})
    assert fetch_meta.status_code == 403
    same = upload(client, GOOD, headers={"Origin": "http://localhost"})
    assert same.status_code == 303


def test_unknown_host_is_rejected(engine: Engine, settings: Settings) -> None:
    with TestClient(create_app(settings, engine), base_url="http://attacker.example") as client:
        assert client.get("/").status_code == 400


def test_security_headers(client: TestClient) -> None:
    response = client.get("/")
    assert "default-src 'self'" in response.headers["content-security-policy"]
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"


def test_export_is_protected_against_formula_injection(
    client: TestClient, engine: Engine, worker_engine: Engine, settings: Settings
) -> None:
    upload(client, (HEADER + "F-1,2026-09-01T10:00:00Z,BER-01,=HYPERLINK(1),1,1.00,EUR\n").encode())
    upload(client, (HEADER + 'F-2,nope,BER-01,c,1,"=1+1",EUR\n').encode())
    drain(worker_engine, settings)

    exported = read_csv(client.get("/sales/export.csv").text)
    assert exported[1][5] == "'=hyperlink(1)"
    report = read_csv(client.get("/imports/2/errors.csv").text)
    assert [r[4] for r in report[1:] if r[2] == "invalid_format" and r[1] == "unit_price"] == ["'=1+1"]


def test_health(client: TestClient) -> None:
    assert client.get("/healthz").json() == {"status": "ok"}


def test_static_assets(client: TestClient) -> None:
    assert client.get("/static/app.css").status_code == 200
    assert client.get("/static/app.js").status_code == 200
    assert client.get("/static/../config.py").status_code == 404


def test_database_has_no_partial_rows_after_rejected_upload(
    client: TestClient, engine: Engine, worker_engine: Engine, settings: Settings
) -> None:
    upload(client, (HEADER + "Z-1,2026-09-01T10:00:00Z,BER-01,c,1,1.00,EUR\nZ-2,bad,BER-01,c,1,1.00,EUR\n").encode())
    drain(worker_engine, settings)
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM sales")).scalar_one() == 0
