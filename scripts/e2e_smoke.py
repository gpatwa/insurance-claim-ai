"""End-to-end smoke verification against the REAL local stack.

Drives the platform the way production traffic would — through every seam:

    FNOL intake → API → Temporal workflow → OCR (MinIO) → mock LLM → adjudication
    grounded in SEEDED REFERENCE DATA → review gate → Postgres event log →
    outbox → relay → Redpanda → notifier → HMAC webhook → rendered documents

Prereqs: `make up` (Temporal + Postgres + MinIO + Redpanda). The script applies
migrations, seeds reference data, spawns worker/api/relay/notifier subprocesses with
mock LLMs (no API keys), runs three scenarios, and verifies every layer:

    A. active policy, routine amount  → APPROVE  → EOB payable, webhook delivered
    B. lapsed policy                  → DENY POLICY_INACTIVE → denial letter renders
    C. unknown policy                 → PEND POLICY_NOT_FOUND → review queue →
       human APPROVE → webhook delivered

Run:  uv run python scripts/e2e_smoke.py   (or: make smoke)
Exit code 0 = every check passed.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import asyncpg
import httpx
from aiohttp import web

REPO = Path(__file__).resolve().parent.parent
API_PORT = 8123
API = f"http://127.0.0.1:{API_PORT}"
HOOK_PORT = 8099
HOOK_URL = f"http://127.0.0.1:{HOOK_PORT}/hook"
PG_DSN = "postgresql://claimpipe:claimpipe@localhost:5433/claimpipe"
HMAC_SECRET = "dev-secret-change-me"

# predefined dev customers (see claimpipe.customers.DEV_KEYS)
SUBMITTER = {"X-API-Key": "ck_lakeside_clearing_01"}
REVIEWER = {"X-API-Key": "ck_payer_reviewer_01"}

SEED_POLICIES = [
    {"policy_number": "POL-ACTIVE", "status": "active", "line": "auto", "limit": 50000},
    {"policy_number": "POL-DEAD", "status": "lapsed", "line": "auto"},
]

CHECKS: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    CHECKS.append((name, ok, detail))
    mark = "\033[32m✓\033[0m" if ok else "\033[31m✗\033[0m"
    print(f"  {mark} {name}" + (f"  ({detail})" if detail and not ok else ""))


# ------------------------------------------------------------------ infra readiness


async def wait_port(host: str, port: int, name: str, deadline_s: float = 120) -> None:
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        try:
            _, w = await asyncio.open_connection(host, port)
            w.close()
            await w.wait_closed()
            print(f"  · {name} port {port} open")
            return
        except OSError:
            await asyncio.sleep(1)
    raise RuntimeError(f"{name} not reachable on {host}:{port}")


async def wait_temporal(deadline_s: float = 120) -> None:
    from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest
    from temporalio.client import Client

    deadline = time.monotonic() + deadline_s
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            client = await Client.connect("localhost:7233")
            await client.workflow_service.describe_namespace(
                DescribeNamespaceRequest(namespace="default")
            )
            print("  · temporal namespace 'default' ready")
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            await asyncio.sleep(2)
    raise RuntimeError(f"temporal not ready: {last}")


async def apply_migrations() -> None:
    conn = await asyncpg.connect(PG_DSN)
    try:
        for sql in sorted((REPO / "migrations").glob("*.sql")):
            await conn.execute(sql.read_text())
            print(f"  · applied {sql.name}")
    finally:
        await conn.close()


async def ensure_bucket() -> None:
    import aioboto3

    session = aioboto3.Session()
    async with session.client(
        "s3",
        endpoint_url="http://localhost:9000",
        region_name="us-east-1",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    ) as s3:
        try:
            await s3.create_bucket(Bucket="claims")
            print("  · bucket 'claims' created")
        except Exception:  # noqa: BLE001 - already exists
            print("  · bucket 'claims' exists")


# ------------------------------------------------------------------ webhook receiver

RECEIVED: list[dict] = []


async def start_hook_server() -> web.AppRunner:
    from claimpipe.notifier import SIGNATURE_HEADER, verify

    async def hook(request: web.Request) -> web.Response:
        body = await request.read()
        sig = request.headers.get(SIGNATURE_HEADER, "")
        RECEIVED.append(
            {"payload": json.loads(body), "sig_ok": verify(HMAC_SECRET, body, sig)}
        )
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_post("/hook", hook)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", HOOK_PORT).start()
    return runner


def webhook_for(claim_id: str) -> dict | None:
    for r in RECEIVED:
        if r["payload"]["claim_id"] == claim_id:
            return r
    return None


async def wait_webhook(claim_id: str, desc: str) -> dict:
    async def _try():
        return webhook_for(claim_id)

    return await poll(_try, desc=desc)


# ------------------------------------------------------------------ service processes


def spawn(role: str, module: str, env: dict, logdir: Path) -> subprocess.Popen:
    logfile = (logdir / f"{role}.log").open("w")
    proc = subprocess.Popen(
        [sys.executable, "-m", module],
        env=env,
        stdout=logfile,
        stderr=subprocess.STDOUT,
        cwd=REPO,
    )
    print(f"  · spawned {role} (pid {proc.pid})")
    return proc


# ------------------------------------------------------------------ scenario helpers


async def poll(fn, *, deadline_s: float = 60, interval: float = 0.5, desc: str = ""):
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        result = await fn()
        if result is not None:
            return result
        await asyncio.sleep(interval)
    raise TimeoutError(f"timed out waiting for {desc}")


async def submit_fnol(ac: httpx.AsyncClient, policy: str, amount: float) -> str:
    fnol = {
        "policyNo": policy,
        "lossDate": "2026-06-20",
        "estimatedAmount": amount,
        "reporter": {"id": f"smoke-{policy}", "callbackUrl": HOOK_URL},
    }
    r = await ac.post(
        f"{API}/intake/fnol", content=json.dumps(fnol).encode(), headers=SUBMITTER
    )
    assert r.status_code == 202, f"submit failed: {r.status_code} {r.text}"
    body = r.json()
    claim_id = body["claim_id"]
    # REAL presigned upload: plain HTTP PUT straight to MinIO — no S3 SDK, no credentials
    put = await ac.put(body["upload_url"], content=b"%PDF-1.7 smoke claim")
    assert put.status_code == 200, f"presigned PUT failed: {put.status_code} {put.text}"
    r = await ac.post(f"{API}/claims/{claim_id}/uploaded", headers=SUBMITTER)
    assert r.status_code == 202
    return claim_id


async def poll_claim(
    ac: httpx.AsyncClient,
    claim_id: str,
    decision: str,
    statuses: set[str] = frozenset({"PERSISTED", "NOTIFIED"}),
    desc: str = "",
) -> dict:
    """Wait until the claim carries `decision` and a post-decision status.

    NOTIFIED is accepted alongside PERSISTED: the notifier consumes CLAIM_PERSISTED off
    Kafka within milliseconds, so demanding exactly PERSISTED is a race the poller loses.
    """

    async def _try():
        body = (await ac.get(f"{API}/claims/{claim_id}", headers=SUBMITTER)).json()
        if body.get("decision") == decision and body.get("status") in statuses:
            return body
        return None

    return await poll(_try, desc=f"{desc} (want decision={decision})")


# ------------------------------------------------------------------ scenarios


async def scenario_a(ac: httpx.AsyncClient) -> None:
    print("\nScenario A — active policy, routine amount → APPROVE")
    anon = await ac.post(f"{API}/claims", json={"metadata": {}})
    check("A: unauthenticated submit rejected (401)", anon.status_code == 401)
    cid = await submit_fnol(ac, "POL-ACTIVE", 1200.0)
    body = await poll_claim(ac, cid, "APPROVE", desc="A decided")
    check("A: decision APPROVE", body["decision"] == "APPROVE")
    check("A: reason AUTO_APPROVED", body["reason_codes"] == ["AUTO_APPROVED"])

    eob = (await ac.get(f"{API}/claims/{cid}/documents/eob", headers=SUBMITTER)).json()
    check("A: EOB payable == claimed", eob["payable_amount"] == 1200.0, str(eob))

    hook = await wait_webhook(cid, "A webhook")
    check("A: webhook delivered", hook is not None)
    check("A: webhook HMAC valid", bool(hook and hook["sig_ok"]))
    check(
        "A: webhook carries decision",
        bool(hook and hook["payload"]["decision"] == "APPROVE"),
    )


async def scenario_b(ac: httpx.AsyncClient) -> None:
    print("\nScenario B — lapsed policy → DENY POLICY_INACTIVE (refdata outranks claimant)")
    cid = await submit_fnol(ac, "POL-DEAD", 900.0)
    body = await poll_claim(ac, cid, "DENY", desc="B decided")
    check("B: decision DENY", body["decision"] == "DENY")
    check("B: reason POLICY_INACTIVE", body["reason_codes"] == ["POLICY_INACTIVE"])

    letter = await ac.get(f"{API}/claims/{cid}/documents/denial-letter", headers=SUBMITTER)
    check("B: denial letter renders", letter.status_code == 200)
    check("B: letter cites reason", "POLICY_INACTIVE" in letter.text)

    hook = await wait_webhook(cid, "B webhook")
    check("B: webhook DENY delivered", bool(hook and hook["payload"]["decision"] == "DENY"))


async def scenario_c(ac: httpx.AsyncClient) -> None:
    print("\nScenario C — unknown policy → PEND → human review → APPROVE")
    cid = await submit_fnol(ac, "POL-GHOST", 800.0)
    body = await poll_claim(
        ac, cid, "PEND", statuses={"ADJUDICATED"}, desc="C pended"
    )
    check("C: pends POLICY_NOT_FOUND", body["reason_codes"] == ["POLICY_NOT_FOUND"])

    queue = (await ac.get(f"{API}/review-queue", headers=REVIEWER)).json()["pending"]
    check("C: appears in review queue", cid in [c["claim_id"] for c in queue])

    # role separation is real: the submitter key cannot review
    forbidden = await ac.post(
        f"{API}/claims/{cid}/review",
        json={"decision": "APPROVE", "reviewer": "x"},
        headers=SUBMITTER,
    )
    check("C: submitter key blocked from reviewing (403)", forbidden.status_code == 403)

    r = await ac.post(
        f"{API}/claims/{cid}/review",
        json={"decision": "APPROVE", "reason_code": "VERIFIED_OK", "reviewer": "smoke@test"},
        headers=REVIEWER,
    )
    check("C: review accepted", r.status_code == 202)

    body = await poll_claim(ac, cid, "APPROVE", desc="C decided")
    check("C: human APPROVE wins", body["reason_codes"] == ["VERIFIED_OK"])
    queue = (await ac.get(f"{API}/review-queue", headers=REVIEWER)).json()["pending"]
    check("C: queue drained", cid not in [c["claim_id"] for c in queue])

    hook = await wait_webhook(cid, "C webhook")
    check("C: webhook delivered post-review", bool(hook and hook["sig_ok"]))


async def verify_event_log(claim_count: int) -> None:
    print("\nEvent log & outbox (Postgres → relay → Redpanda)")
    conn = await asyncpg.connect(PG_DSN)
    try:
        cids = [r["payload"]["claim_id"] for r in RECEIVED]
        rows = await conn.fetch(
            "SELECT claim_id, count(*) AS n FROM claim_events "
            "WHERE claim_id = ANY($1::text[]) GROUP BY claim_id",
            cids,
        )
        check(
            "event log has full history per claim",
            len(rows) == claim_count and all(r["n"] >= 5 for r in rows),
            str([(r["claim_id"][:8], r["n"]) for r in rows]),
        )

        async def _outbox_drained():
            n = await conn.fetchval(
                "SELECT count(*) FROM outbox WHERE claim_id = ANY($1::text[]) "
                "AND NOT published",
                cids,
            )
            return True if n == 0 else None

        await poll(_outbox_drained, deadline_s=20, desc="outbox drained")
        check("outbox fully relayed to Kafka", True)
    finally:
        await conn.close()


# ------------------------------------------------------------------ main


async def main() -> int:
    print("=== claimpipe E2E smoke: real stack + reference data ===\n")
    print("Waiting for infrastructure (make up)…")
    await wait_port("localhost", 5433, "postgres")
    await wait_port("localhost", 9000, "minio")
    await wait_port("localhost", 19092, "redpanda")
    await wait_port("localhost", 7233, "temporal")
    await wait_temporal()

    print("\nPreparing state…")
    await apply_migrations()
    await ensure_bucket()

    seed = Path(tempfile.gettempdir()) / "claimpipe-refdata-seed.json"
    seed.write_text(json.dumps(SEED_POLICIES))
    print(f"  · reference data seeded: {[p['policy_number'] for p in SEED_POLICIES]}")

    logdir = REPO / "smoke-logs"
    logdir.mkdir(exist_ok=True)
    env = {
        **os.environ,
        "CLAIMPIPE_USE_MOCK_LLM": "1",
        "CLAIMPIPE_REFDATA_FILE": str(seed),
        "CLAIMPIPE_WEBHOOK_BACKOFF_SECONDS": "0.2",
        "CLAIMPIPE_API_PORT": str(API_PORT),
    }

    hook_runner = await start_hook_server()
    print(f"  · webhook receiver on {HOOK_URL}")

    print("\nStarting services…")
    procs = [
        spawn("worker", "claimpipe.temporal.worker", env, logdir),
        spawn("api", "claimpipe.api.main", env, logdir),
        spawn("relay", "claimpipe.relay", env, logdir),
        spawn("notifier", "claimpipe.notifier", env, logdir),
    ]
    try:
        async with httpx.AsyncClient(timeout=10) as ac:

            async def _api_up():
                if procs[1].poll() is not None:
                    raise RuntimeError("api exited early — see smoke-logs/api.log")
                try:
                    r = await ac.get(f"{API}/healthz")
                    return True if r.json() == {"status": "ok"} else None
                except (httpx.TransportError, ValueError):
                    return None

            await poll(_api_up, deadline_s=60, desc="api /healthz")
            print("  · api ready; letting notifier join its consumer group…")
            await asyncio.sleep(6)

            for proc, role in zip(procs, ["worker", "api", "relay", "notifier"], strict=True):
                if proc.poll() is not None:
                    raise RuntimeError(f"{role} exited early — see smoke-logs/{role}.log")

            await scenario_a(ac)
            await scenario_b(ac)
            await scenario_c(ac)
        await verify_event_log(claim_count=3)
    finally:
        for proc in procs:
            proc.terminate()
        for proc in procs:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        await hook_runner.cleanup()

    failed = [c for c in CHECKS if not c[1]]
    print(f"\n=== {len(CHECKS) - len(failed)}/{len(CHECKS)} checks passed ===")
    if failed:
        for name, _, detail in failed:
            print(f"  FAILED: {name}  {detail}")
        return 1
    print("End-to-end verification with reference data: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
