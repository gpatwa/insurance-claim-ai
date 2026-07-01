# claimpipe

Cloud-agnostic **claim-ingestion pipeline**: a PDF + JSON metadata "claim" is logged, run
through OCR, scored by multiple LLM models (tiered routing), persisted, and the customer is
notified by webhook.

Built on a **portable, self-hosted stack** — the same code runs on a laptop and on any cloud:

| Concern | Tech |
|---|---|
| Durable orchestration / decider | **Temporal** (self-hosted) |
| Source of truth | **event-sourced** append-only `claim_events` (Postgres) |
| Read models | projections folded from events (status, etc.) |
| Event bus (fan-out) | **Kafka / Redpanda** via transactional **outbox** + relay |
| Workers / API | **Python** (`temporalio`, FastAPI) |
| Object storage | **S3 API** behind an adapter (MinIO local) |
| LLMs | mix of providers behind one `ModelClient` adapter (Claude via Bedrock/Vertex/API) |
| Agent reasoning (escalation) | **LangGraph** inside a Temporal activity |

**State model:** the Temporal workflow is the authoritative decider and emits **domain events**;
`claim_events` is the append-only source of truth; the status enum and other read models are
**projections** folded from events (transitions validated in one place). Kafka is the fan-out
**bus** — not the source of truth — fed by a transactional outbox so no event is ever lost.

Design rationale: [`docs/DESIGN.md`](docs/DESIGN.md).

## Quickstart

```bash
make install          # uv sync (all extras + dev)
make test             # hermetic tests (Temporal in-process test env — no Docker needed)
make up               # bring up Temporal + Postgres + MinIO locally
make worker           # run the Temporal worker
make down             # tear down
```

Temporal Web UI: http://localhost:8233 · MinIO console: http://localhost:9001

## Milestones

Each milestone is independently end-to-end tested and pushed.

- [x] **M0** — Repo scaffold + local harness (Docker Compose, adapter Protocols, CI, smoke workflow)
- [x] **M1** — Ingestion API + claim record (FastAPI, idempotent submit, start workflow, status endpoint)
- [x] **M2** — OCR activity + object storage (upload dormancy gate, retry/backoff, S3 adapter)
- [x] **M2.5** — Event-sourced foundation (append-only `claim_events`, validated projection, outbox, Kafka/Redpanda bus, relay)
- [ ] **M3** — LLM tiered routing + persistence
- [ ] **M4** — Webhook notification (HMAC, retries, DLQ)
- [ ] **M5** — LangGraph escalation agent
- [ ] **M6** — Observability + resilience + load
- [ ] **M7** — Cloud deploy (Terraform + Helm, one target)

## Design principles

- **Enum state machine is the single source of truth** — the LLM never drives transitions;
  it only fills structured fields. The workflow advances `ClaimStatus` deterministically.
- **Adapters everywhere** — OCR, object store, and LLM providers sit behind Protocols, so
  swapping cloud/provider is a config change, not a rewrite.
- **Local/prod parity** — the workflow you debug locally is the workflow that runs in prod.

## License

MIT
