# claimpipe

Cloud-agnostic **claim-ingestion pipeline**: a PDF + JSON metadata "claim" is logged, run
through OCR, scored by multiple LLM models (tiered routing), persisted, and the customer is
notified by webhook.

Built on a **portable, self-hosted stack** — the same code runs on a laptop and on any cloud:

| Concern | Tech |
|---|---|
| Durable orchestration | **Temporal** (self-hosted) |
| Workers / API | **Python** (`temporalio`, FastAPI) |
| Object storage | **S3 API** behind an adapter (MinIO local) |
| Relational store | **Postgres** |
| LLMs | mix of providers behind one `ModelClient` adapter (Claude via Bedrock/Vertex/API) |
| Agent reasoning (escalation) | **LangGraph** inside a Temporal activity |

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
- [ ] **M1** — Ingestion API + claim record (FastAPI, signed upload, start workflow)
- [ ] **M2** — OCR activity + object storage
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
