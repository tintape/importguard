"""Walk through the demo scenario against a running ImportGuard web app.

    uv run python scripts/demo_scenario.py [--base-url http://127.0.0.1:8000] [--data demo-data]

Prerequisites: the web app is running, the database is empty (fresh
``alembic upgrade head``), demo files exist (``python -m importguard.sampledata``)
and NO other worker is running: this script starts and kills its own worker
process to demonstrate crash recovery.

Steps: 1) valid file  2) the same file again  3) overlapping file
4) conflicting file  5) kill the worker (SIGKILL) during the 100k-row import
6) restart it and check the final totals against demo-data/expected.json.
"""

from __future__ import annotations

import argparse
import json
import re
import signal
import subprocess
import sys
import time
import urllib.request
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SOURCES = {"pos": "1", "webshop": "2", "marketplace": "3"}
POLL_SECONDS = 0.2


class Client:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.opener = urllib.request.build_opener(_NoRedirect)

    def upload(self, path: Path, source: str) -> int:
        boundary = uuid.uuid4().hex
        body = b"".join(
            [
                f'--{boundary}\r\nContent-Disposition: form-data; name="source_id"\r\n\r\n'.encode(),
                f"{SOURCES[source]}\r\n".encode(),
                f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{path.name}"\r\n'.encode(),
                b"Content-Type: application/octet-stream\r\n\r\n",
                path.read_bytes(),
                f"\r\n--{boundary}--\r\n".encode(),
            ]
        )
        request = urllib.request.Request(  # noqa: S310 - URL comes from the operator's --base-url
            f"{self.base_url}/imports",
            data=body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            method="POST",
        )
        try:
            self.opener.open(request)
        except urllib.error.HTTPError as exc:
            if exc.code != 303:
                raise SystemExit(f"upload of {path.name} failed: HTTP {exc.code}: {exc.read()[:300]!r}") from exc
            return int(exc.headers["Location"].rsplit("/", 1)[1])
        raise SystemExit("expected a redirect after upload")

    def status(self, job_id: int) -> str:
        with urllib.request.urlopen(f"{self.base_url}/imports/{job_id}/status") as response:  # noqa: S310
            return str(json.load(response)["status"])

    def page(self, path: str) -> str:
        with urllib.request.urlopen(f"{self.base_url}{path}") as response:  # noqa: S310
            return str(response.read().decode())

    def wait(self, job_id: int, *, until: tuple[str, ...] = ("succeeded", "failed"), timeout: float = 120) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self.status(job_id)
            if status in until:
                return status
            time.sleep(POLL_SECONDS)
        raise SystemExit(f"job {job_id} did not reach {until} in {timeout}s")

    def summary(self, job_id: int) -> str:
        html = self.page(f"/imports/{job_id}")
        match = re.search(r'class="alert (?:ok|error|info)"[^>]*>\s*<strong>(.*?)</strong>\s*<p>(.*?)</p>', html, re.S)
        return f"{match.group(1)}: {match.group(2)}" if match else "(no summary)"


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


def sales_count(client: Client, path: str) -> int:
    match = re.search(r"Export ([\d,]+) row", client.page(path))
    return int(match.group(1).replace(",", "")) if match else 0


def start_worker() -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-m", "importguard.worker"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )


def step(number: int, title: str) -> None:
    print(f"\n{number}. {title}")


def run(client: Client, data: Path, expected: dict[str, Any]) -> bool:
    worker = start_worker()
    try:
        step(1, "Import a valid daily export")
        job = client.upload(data / "01_pos_2026-08-31_valid.csv", "pos")
        client.wait(job)
        print(f"   import #{job}: {client.summary(job)}")

        step(2, "Upload the same content again under another name")
        job = client.upload(data / "02_pos_2026-08-31_valid (copy).csv", "pos")
        client.wait(job)
        print(f"   import #{job}: {client.summary(job)}")

        step(3, "Import the next export, which overlaps the previous one")
        job = client.upload(data / "03_pos_2026-09-01_overlapping.csv", "pos")
        client.wait(job)
        print(f"   import #{job}: {client.summary(job)}")

        step(4, "Upload a file that changes the amount of an existing sale")
        before = sales_count(client, "/sales")
        job = client.upload(data / "04_pos_2026-09-02_conflicting_amount.csv", "pos")
        client.wait(job)
        print(f"   import #{job}: {client.summary(job)}")
        after = sales_count(client, "/sales")
        print(f"   sales before: {before:,}, after: {after:,} -> {'unchanged' if before == after else 'CHANGED'}")

        step(5, "Kill the worker (SIGKILL) while it processes the 100,000-row file")
        job = client.upload(data / "10_marketplace_large_100k.parquet", "marketplace")
        client.wait(job, until=("processing",))
        time.sleep(1.0)
        status_at_kill = client.status(job)
        worker.send_signal(signal.SIGKILL)
        worker.wait()
        print(f"   killed the worker; job status at that moment: {status_at_kill}")
        if status_at_kill != "processing":
            print("   (the import finished before the kill; re-run on a fresh database to see recovery)")
        visible = sales_count(client, "/sales?source=3")
        print(f"   marketplace sales visible while the job is orphaned: {visible:,}")

        step(6, "Restart the worker and check the result")
        worker = start_worker()
        client.wait(job)
        print(f"   import #{job}: {client.summary(job)}")
    finally:
        worker.terminate()
        worker.wait(timeout=30)

    return check_totals(client, expected)


def check_totals(client: Client, expected: dict[str, Any]) -> bool:
    html = client.page("/")
    steps_run = {"01", "02", "03", "04", "10"}
    expected_files = [s for s in expected["steps"] if s["file"][:2] in steps_run]
    expected_sales = sum(s["expect_inserted"] for s in expected_files)
    total = re.findall(r'<div class="kpi-label">Sales</div><div class="kpi-value">([\d,]+)</div>', html)
    shown = sum(int(t.replace(",", "")) for t in total)
    ok = shown == expected_sales
    print(
        f"\nTotal sales on the dashboard: {shown:,}; expected from the generator: {expected_sales:,}",
        "-> OK" if ok else "-> MISMATCH",
    )
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--data", type=Path, default=ROOT / "demo-data")
    args = parser.parse_args()
    expected = json.loads((args.data / "expected.json").read_text())
    return 0 if run(Client(args.base_url), args.data, expected) else 1


if __name__ == "__main__":
    raise SystemExit(main())
