# Verifying claimpipe end to end

Two verification layers. Both must be green before a change ships.

## Layer 1 — hermetic tests (every commit, CI)

```bash
make test        # 65 tests, ~10s, no Docker
```

In-process Temporal time-skipping server + in-memory fakes for Postgres/S3/Kafka/LLM/refdata.
Covers all pipeline logic: state machine, routing, adjudication, review gates, adapters.
Fast and deterministic — but it injects dependencies directly, so it cannot see wiring bugs
in the real entrypoints.

## Layer 2 — full-stack smoke with reference data (`make smoke`)

```bash
make up          # Temporal + Postgres(5433) + MinIO + Redpanda (Docker)
make smoke       # uv run python scripts/e2e_smoke.py
```

The harness exercises the platform the way production traffic would:

```
FNOL intake → API → Temporal workflow → OCR (MinIO) → mock LLM →
ADJUDICATE grounded in seeded reference data → REVIEW gate →
Postgres event log → outbox → relay → Redpanda → notifier → HMAC webhook →
rendered documents (EOB / denial letter)
```

What it does:

1. Waits for infra, applies migrations, creates the bucket.
2. **Seeds reference data** (`POL-ACTIVE` active, `POL-DEAD` lapsed) via
   `CLAIMPIPE_REFDATA_FILE`; enables `CLAIMPIPE_USE_MOCK_LLM=1` (no API keys needed).
3. Spawns the four real service entrypoints (worker, api, relay, notifier) as
   subprocesses — logs land in `smoke-logs/`.
4. Runs a local webhook receiver and verifies **HMAC signatures** on every delivery.
5. Exercises the **customer front door**: rejects anonymous submits (401), submits with a
   submitter API key, uploads the PDF through the **real presigned URL** (plain HTTP PUT,
   no S3 credentials), and proves role separation (submitter key blocked from reviewing).
6. Runs three refdata-grounded scenarios and asserts at every layer:

| Scenario | Reference data says | Expected outcome |
|---|---|---|
| A: routine claim | policy **active** | APPROVE → EOB payable, webhook delivered |
| B: lapsed policy | policy **lapsed** | DENY `POLICY_INACTIVE` → denial letter renders |
| C: unknown policy | **not found** | PEND `POLICY_NOT_FOUND` → review queue → human APPROVE → webhook |

7. Verifies the **event log** has full per-claim history in Postgres and the **outbox is
   fully relayed** to Kafka.

Exit code 0 + `21/21 checks passed` = the deployed wiring works, not just the logic.

### Why both layers

The smoke harness caught a real bug the hermetic suite structurally could not: the API
entrypoint built its asyncpg pool on one event loop and served requests on another
(`got Future attached to a different loop` on every request). Hermetic tests inject the
store, so the entrypoint wiring is only exercised by Layer 2.

### Troubleshooting

- `api exited early` → `smoke-logs/api.log` (port conflict? set `CLAIMPIPE_API_PORT`).
- Webhooks missing → notifier consumer joins its Kafka group a few seconds after start;
  the harness waits, but a slow machine may need a longer grace period.
- App Postgres listens on host **5433** (5432 is often taken by a local Postgres).
- Temporal Web UI at http://localhost:8233 shows every workflow's full history —
  the best place to watch a claim move through its stages.
