# Claim Ingestion Pipeline — Design Doc (Cloud-Agnostic)

## Context

We need a data-ingestion pipeline for **insurance claims**, where a claim = one **PDF**
(text-only, ~1 MB) + one **JSON metadata** file (semi-structured, ~100 KB). For each claim the
pipeline must: (1) log claim metadata, (2) run the PDF through an external **OCR** service
(blackbox), (3) run the OCR text through **multiple LLM models**, (4) persist every model's
output, and (5) notify the customer (success/failure + model output details) via **webhook**.

Non-functional target: **1,000 claims/minute** sustained. Submission is asynchronous —
the API returns immediately and the customer is notified later via webhook callback.

**This revision is deliberately cloud-agnostic and locally runnable.** The foundation is
portable open-source components (Temporal, containers, Postgres, S3-compatible storage). The
same workflow code runs on a laptop and on **GCP, AWS, or Azure** — those are *deployment
targets*, not the foundation. **Deliverable: design document only (no code).**

### Locked decisions
- **Foundation:** Temporal + containers + Postgres + S3-adapter (portable, OSS, local-first)
- **Temporal:** **self-hosted** (own the cluster; no Temporal Cloud dependency)
- **Workers / SDK:** **Python** (`temporalio` SDK; async workers)
- **Deployment targets:** local (Docker Compose) → any K8s (GKE/EKS/AKS)
- **LLMs:** mix of providers behind one `ModelClient` adapter (Claude via Bedrock/Vertex/Azure AI, or Anthropic API direct)
- **I/O:** REST API submission, **webhook** notification to customer callback URL

---

## Why this shape (and why no separate queue)

- **17 req/s sustained (1K/min), ~50–85 peak.** Modest throughput — the challenge is not
  arrival rate, it's that each claim does **slow external work** (OCR seconds + several LLM
  calls seconds–minutes). By Little's Law, at ~1 min/claim you have **~1,000 claims in flight**,
  so processing **must** be async and durable.
- **Temporal already contains a durable queue.** Every in-flight claim is a persisted workflow
  execution. A separate Pub/Sub / Kafka isn't needed — the engine *is* the buffer, retry
  manager, and DLQ. It also absorbs flaky OCR / LLM rate-limit outages: a stalled step just
  waits and retries while the execution sits durably, costing nothing.
- **One component replaces three cloud-locked ones.** Temporal subsumes what would otherwise be
  orchestration (Workflows/Step Functions) + queue (Pub/Sub/SQS) + retrying webhook delivery
  (Cloud Tasks). Fewer moving parts *and* portable.

---

## Scale envelope

| Quantity | Value |
|---|---|
| Sustained / peak | 1,000/min ≈ **17 rps** / **50–85 rps** |
| Daily volume | ~**1.44M claims/day** |
| In-flight (Little's Law, ~1 min each) | ~**1,000** concurrent executions (more during a downstream outage) |
| Payload | ~1.1 MB/claim |
| Raw ingress | ~1.6 TB/day → ~48 TB/month before lifecycle/dedup |
| LLM work | OCR-text tokens × **N models** × 1.44M/day → **the cost driver** |

Temporal comfortably holds far more than 1,000 in-flight workflows, so even multi-hour
downstream outages just grow the backlog rather than dropping work. The real limits remain
**external**: OCR throughput and **LLM provider rate/token limits** (handled below).

---

## Architecture

```
                         ┌──────── Object store (S3-compatible, via adapter) ────────┐
                         │   MinIO (local) │ S3 (AWS) │ GCS (GCP) │ Blob (Azure)      │
                         │   pdf / ocr-text / raw-llm-output  (SSE/CMEK)              │
                         └─────────────────────────────────────────────────────────────┘
                               ▲ signed-URL upload (1MB PDF)      ▲ reads/writes
                               │                                  │
 Customer ──POST claim──► [API service (container)]
   │ (JSON 100KB inline,         │ validate JSON schema
   │  PDF via signed URL)        │ INSERT claim row (RECEIVED) in Postgres
   │                             │ StartWorkflow(claim_id)  ◄── idempotent (claim_id = workflow id)
   │                             ▼
   │                 ┌──────────────── Temporal ─────────────────┐
   │                 │  ClaimWorkflow(claim_id):                 │
   │                 │   A. log metadata   → Postgres + audit    │
   │                 │   B. OCR activity   → S3   [retry/backoff]│
   │                 │   C. ROUTE via Batch API:                 │
   │                 │        • cost-tier model(s) first         │
   │                 │        • escalate to accuracy tier only   │
   │                 │          if confidence low (2nd batch)    │
   │                 │   D. persist activity → Postgres + S3     │
   │                 │   E. notify activity  → webhook (retried) │
   │                 └─────────────────────────────────────────────┘
   │                       ▲ workers (containers) poll task queues; autoscale on backlog
   ▼                       │
 [Worker pool: API / OCR / LLM / Notify workers — plain containers on K8s or Docker]
                           │
                           └── notify activity ──HMAC-signed POST──► Customer callback URL
                                                  (Temporal retries w/ backoff; DLQ on exhaustion)
```

### Stage by stage

**1. Submit (REST API, container).**
API issues a **signed object-storage upload URL** for the 1 MB PDF (client PUTs directly to the
bucket — keeps the API light); 100 KB JSON posted inline and validated against a **versioned
schema**. INSERT `claims` row (`RECEIVED`), then `StartWorkflowExecution` with
**`workflow_id = claim_id`** → Temporal dedupes duplicate submissions for free. Return
**202 + claim_id**.

**2. Orchestration (Temporal).**
`ClaimWorkflow` is plain code (Go/Python/TS/Java SDK). Temporal persists every step, so the
workflow survives worker crashes and resumes exactly where it left off. Retries, backoff,
timeouts, and DLQ (via failed-workflow handling) are configured per activity.

**3. Log metadata activity.** Persist metadata to **Postgres (JSONB)**; emit structured log;
write an immutable audit record (Postgres audit table, optionally streamed to a warehouse).

**4. OCR activity.** Calls the blackbox OCR behind an **adapter interface** (swappable). OCR
text → object store. Exponential backoff + jitter, circuit breaker, timeout; honors `Retry-After`
on 429; exhaustion → workflow failure path + alert.

**5. Multi-LLM via routing + Batch API.** All models sit behind a common **`ModelClient`**
interface (the "mix of providers" stays uniform). **Strategy = tiered routing, not fan-out to
everything:**
- **Tier 1 (cost):** submit the claim to the cost-tier model(s) via the **Batch API**.
- **Evaluate confidence** on tier-1 output (model-reported confidence + validation rules).
- **Tier 2 (accuracy):** **only when confidence is low** (or the claim is flagged high-value),
  escalate to the accuracy-tier model in a **second batch round**.

This runs the expensive model on the minority of claims that need it. Both batch rounds are
async and fit the minutes-long webhook latency budget; the second round adds one batch cycle of
latency for escalated claims only. Each call records **output + confidence + latency + token
cost + model version**. **Partial-failure tolerant**: a tier failure persists what's available
and marks `PARTIAL_SUCCESS`. When multiple models do run, an aggregation step computes
consensus.

**6. Persist activity.** Each output → `model_outputs` (Postgres JSONB), key
`(claim_id, model_name, model_version)`; large raw blobs → object store. Idempotent upserts.

**7. Notify activity (webhook).** HMAC-signed POST to the customer callback URL, carrying an
idempotency id. **Temporal owns the retry/backoff** (this is what Cloud Tasks did in the GCP
version — now folded in). On exhaustion → `NOTIFY_FAILED` + alert. The activity runs *inside*
the durable workflow, so a notification is never lost on crash (no outbox needed).

### Status model
`RECEIVED → OCR_RUNNING → OCR_DONE → LLM_RUNNING → LLM_DONE → PERSISTED → NOTIFIED`,
terminal `FAILED` / `PARTIAL_SUCCESS` / `NOTIFY_FAILED`. Stored in Postgres for status queries;
Temporal's own history is the source of truth for execution state.

---

## Handling LLM rate limits & flaky OCR (no extra queue)

1. **Temporal durability = the buffer.** Stalled/rate-limited steps wait and retry while the
   execution sits durably; backlog grows safely instead of dropping work.
2. **Activity retry + backoff + circuit breaker**; honor HTTP `Retry-After` on 429.
3. **Client-side token-bucket limiter + bounded worker concurrency** sized to each provider's
   quota → you *never exceed* the limit; calls block and backpressure into Temporal. This is the
   real fix for rate limits, not reacting to 429s.
4. **LLM Batch API** for non-urgent claims (notification is async) → sidesteps per-minute limits
   and ~halves cost.
5. **Raise the ceiling**: request higher provider quota / provisioned throughput.

---

## Technology choices (portable core, per-cloud adapters)

| Concern | Portable choice | Local | AWS | GCP | Azure |
|---|---|---|---|---|---|
| Orchestration | **Temporal** (self-hosted, OSS) | `temporal server start-dev` | EKS (Helm) | GKE (Helm) | AKS (Helm) |
| Workers / API | **Python containers** | Docker Compose | EKS / ECS | GKE / Cloud Run | AKS / ACA |
| Object storage | **S3 API behind adapter** | MinIO | S3 | GCS | Blob |
| Relational DB | **Postgres** | Docker | RDS / Aurora | Cloud SQL | Azure DB for Postgres |
| LLMs (mix) | **`ModelClient` adapter** | Anthropic API | Bedrock | Vertex AI | Azure AI Foundry |
| OCR | **OCR adapter** | mock/stub | blackbox OCR | blackbox OCR | blackbox OCR |
| Webhook delivery | **Temporal activity** | — | — | — | — |
| Secrets | **Vault** (or cloud-native) | Vault dev | Secrets Mgr | Secret Mgr | Key Vault |
| Observability | **OpenTelemetry** | Jaeger/Prometheus | CloudWatch/X-Ray | Cloud Ops | Azure Monitor |
| IaC / deploy | **Terraform + Helm** | Compose | Terraform | Terraform | Terraform |

**No proprietary queue, scheduler, or task service anywhere** — the only thing that changes
between targets is the adapter config (bucket, DB endpoint, LLM endpoint, secrets backend).

---

## Concrete stack (Python + self-hosted Temporal)

**Self-hosted Temporal topology.** Run the Temporal server (frontend / history / matching /
internal-worker services — single `temporal` image or split per service at scale) backed by
its own **Postgres** persistence store, plus **Elasticsearch** for advanced visibility/search
(optional but recommended for ops at this volume). Deploy via the official **Helm chart** on
K8s; locally via the dev server. Keep Temporal's persistence DB **separate** from the
application Postgres so workflow history and business data scale and back up independently.

**Python services (all containers, `temporalio` SDK):**

| Component | Key libraries | Notes |
|---|---|---|
| API service | **FastAPI** + **uvicorn/gunicorn**, **Pydantic** | JSON schema validation (Pydantic models, versioned); issues signed upload URL; `client.start_workflow(id=claim_id)` |
| Workflow & workers | **`temporalio`** (async workers) | `ClaimWorkflow` = deterministic orchestration; I/O lives in activities |
| OCR adapter | **httpx** (async), **tenacity** for app-level retry tuning | behind `OCRClient` protocol; Temporal owns durable retry, tenacity/circuit-breaker for in-call behavior |
| `ModelClient` adapter | **anthropic** SDK / **boto3** (Bedrock) / google-genai (Vertex) / azure SDK | one `Protocol`, provider chosen by config; token-bucket limiter (e.g. `aiolimiter`) per provider |
| Object store adapter | **aioboto3** / **minio** | S3 API → MinIO local, S3/GCS/Blob in cloud |
| Postgres access | **asyncpg** + **SQLAlchemy 2.0** (async) or **psycopg 3** | `claims`, `model_outputs` (JSONB); idempotent upserts |
| Webhook notify | **httpx** + HMAC (`hmac`/`hashlib`) | activity; Temporal retry policy owns backoff/DLQ |
| Packaging | **uv**/**poetry**, multi-stage Docker, one image per worker role | scale worker roles independently by task queue |

**Worker layout.** Separate **task queues** per stage (`ocr`, `llm`, `notify`) so each scales
independently — e.g. LLM workers scale on provider concurrency, OCR workers on OCR throughput.
Activities are `async def`; CPU-light, I/O-bound work fits Python's async model well. Determinism
stays in the workflow; all network/LLM/OCR calls are activities (Temporal requirement).

## Local-first dev story (a real maintainability win)

`docker-compose up` brings up **Temporal + Postgres + MinIO + mock OCR + workers**. The **exact
same workflow and activity code** that runs in production runs on the laptop — true dev/prod
parity, so the full pipeline (including retries and fan-out) is debuggable locally. Swapping to
a cloud is changing endpoints in config/Helm values, **not a rewrite**.

---

## Cost

**LLM inference is ~90% of the bill.** Sensitivity:

> cost ≈ (OCR-text tokens) × (number of models) × (1.44M claims/day) × (price/token)

A typical claim's OCR output is a few K–20K tokens (a dense 1 MB PDF could be ~250K — chunk it).
At ~10K tokens × 3 models × 1.44M/day you are in the tens of billions of tokens/day — i.e.
without controls, easily five figures/day. **Adopted controls** (the first two are committed
design decisions):

1. **Batch API (~50% off)** — committed. Async webhook gives a minutes-long latency budget, so
   all LLM calls go through batch.
2. **Tiered routing** — committed. Cost-tier model first; **escalate to the accuracy tier only
   when confidence is low**. The expensive model runs on a minority of claims, not all 1.44M.
3. **Prompt caching** on the shared system prompt/instructions.
4. **Section extraction / chunking** — send only relevant claim sections.
5. **Result dedup cache** — hash (OCR text + model + prompt version) to skip re-inference.

Combined, batch (~50%) × routing (expensive model on, say, 10–30% of claims) is roughly an
order-of-magnitude reduction versus running all models synchronously on every claim.

Infra (self-hosted Temporal, Postgres, Python workers) is minor at this volume.

**Object-store retention (committed — see Retention horizons below):**
- **Raw PDFs: 7-day TTL** — caps raw-PDF storage at ~**11 TB** steady-state
  (1.44M/day × 1 MB × 7) instead of growing unbounded.
- **OCR text: 7 years** from claim closure, tiered to **cold/archive after 90 days**. At ~50 KB
  avg this is ~26 TB/year, but mostly in archive (Glacier-class ≈ $1/TB-mo) → low hundreds of
  $/month even at full horizon. Negligible vs LLM spend.
- **Model outputs + audit: 7 years** — small JSON, negligible storage.

**Self-hosting Temporal** means you own the cluster (server + persistence Postgres + optional
Elasticsearch) and its ops/on-call — the trade for no per-action managed fee and full
portability. Budget for that operational cost explicitly.

---

## Maintainability

- **Adapter interfaces** for OCR, object store, secrets, and **`ModelClient`** → swap any
  provider or cloud by config, no rewrite. This is the backbone of portability.
- **Config-as-data:** prompts, model selection, routing thresholds, retry policies live in
  versioned config, not code.
- **Schema versioning** for the semi-structured JSON (registry + edge validation); quarantine on
  mismatch.
- **Idempotency everywhere** (`workflow_id = claim_id`, idempotent upserts) → safe retries and
  at-least-once delivery.
- **DLQ + replay**: failed workflows are inspectable and re-runnable from Temporal.
- **Observability:** OpenTelemetry trace per `claim_id` across stages; per-model dashboards
  (latency, token cost, confidence, error rate); SLOs + alerts on backlog depth and
  `NOTIFY_FAILED`.
- **IaC + CI/CD** (Terraform + Helm) with identical local/dev/prod topology; canary deploys for
  prompt/model changes; Temporal's workflow **versioning** for safe in-flight changes.

---

## Reliability & security (insurance = PII/PHI)

- **Encryption** at rest (SSE/CMEK) and in transit; least-privilege IAM; network isolation.
- **Keep claim data in-region**; prefer the in-VPC Claude endpoint native to the deployment
  cloud (**Bedrock / Vertex / Azure AI**) over an external API for PII; redact before any
  out-of-boundary call if required.
- **Webhook security:** HMAC-signed payloads, replay protection, customer idempotency id,
  bounded retries → DLQ + alert.
- **Audit trail** for every state transition.

### Failure-handling summary

| Failure | Behavior |
|---|---|
| OCR fails | retry/backoff → exhaustion → `FAILED`, notify failure |
| One LLM fails | persist the rest → `PARTIAL_SUCCESS`, notify with available outputs |
| All LLMs fail | `FAILED`, notify failure |
| Persist fails | activity retry; workflow durability guarantees no lost notification |
| Webhook fails | Temporal retry/backoff → DLQ → `NOTIFY_FAILED` + alert |
| Worker crash | Temporal resumes the workflow on another worker from last completed step |

---

## Decisions (resolved)

- **Foundation:** self-hosted **Temporal** + **Python** (`temporalio`) workers + Postgres +
  S3-adapter; clouds are deployment targets.
- **LLM execution:** **Batch API** for all calls + **tiered routing** (cost-tier first, escalate
  to accuracy tier only on low confidence).
- **Raw PDF retention:** deleted after **7 days** (lifecycle TTL).
- **Escalation policy** and **retention horizons** — decided below (launch values, then tune).

### Escalation policy *(decided as DS + risk + finance)*

**Accuracy floor (the SLA tier-1 must meet to auto-process):** ≥ **99%** field accuracy on
**payout-critical fields** (amounts, claimant, policy #, dates), ≥ **95%** on the rest. Below
that, escalate. **Target escalation rate ≈ 15%** (inside the 10–30% budget).

**Escalate a claim to the accuracy tier if *any* trigger fires:**
| Trigger | Launch threshold |
|---|---|
| Model self-confidence (calibrated) below cutoff | **< 0.85** |
| Any **validation rule** fails (missing required field, totals don't reconcile, cross-field inconsistency, schema gap) | any failure |
| **OCR quality** low | OCR confidence **< 0.90** *or* garbled-char ratio **> 5%** |
| Two cost-tier samples disagree on a payout-critical field | any disagreement |
| **High-value claim** (always escalate, regardless of confidence) | claim value **≥ $25,000** |

These are **starting values**. After launch, calibrate the 0.85 cutoff on the labeled set via a
reliability curve — sweep for the lowest-cost point that still clears the accuracy floor; start
conservative (escalate more) and tighten as calibration is trusted. Monitor production escalation
rate + sampled post-hoc error and re-tune on drift.

### Retention horizons *(decided as Legal/Compliance + DPO)*

Clock starts at **claim closure** (until closed, retain). All tiers encrypted; deletion via
**crypto-shredding** (drop the KMS key). **Legal hold** suspends deletion. Regulatory retention
prevails over GDPR/CCPA erasure; minimize and honor erasure outside the mandated set.

| Artifact | Retention | Rationale |
|---|---|---|
| Raw PDF | **7 days** from ingest | re-OCR window for immediate disputes/debug; caps raw storage |
| **OCR text** | **7 years** from closure (→ cold/archive after 90 days) | source-of-record text once the PDF is gone; needed for audit/litigation defense; cheap in archive |
| **Model outputs + audit trail** | **7 years** from closure | canonical decision record |

7 years covers common US state-DOI ranges (5–10y) and HIPAA (6y) with margin. Horizon is a
**config value** so Legal can change it per jurisdiction without a code change.

### Still to confirm (engineering, low-risk)
- **Temporal persistence store** — Postgres is fine at 1K/min; revisit Cassandra only if
  workflow-history write volume ever outgrows a single Postgres (not expected).

---

## Reference architectures & framework choice (ADK vs LangGraph)

The LLM stage above is deterministic tiered routing. If/when a step becomes genuinely
**agentic** (agents that reason over multiple steps, call tools, and loop — e.g. a
medical-necessity / complex-review path), that reasoning belongs in an **agent framework
running inside a Temporal activity**, not in the orchestrator. Two credible frameworks:

**Reference sample reviewed:** Google's ADK *new-hire-onboarding* example
(`GoogleCloudPlatform/generative-ai/agents/adk/new-hire-onboarding`). It is a **single
coordinator agent** driving a long-running workflow via an **enum-based durable state machine**,
pausing at external-dependency points as **event-driven dormancy gates** (webhook-resumed,
scale-to-zero — no polling/blocked threads), persisting **structured state, not chat logs**,
with **golden eval sets** for transitions. It runs on GCP **Agent Runtime** (managed durable
sessions + webhooks). That pattern **corroborates this design** — it is the same
durable-state-machine + dormancy-gate + coordinator shape, just GCP-managed instead of portable
(Agent Runtime ≈ our self-hosted Temporal; both fill the durable-orchestration slot).

### The two layers (don't conflate them)
```
agent-reasoning layer:   ADK  ≈  LangGraph            ← pick ONE (only for agentic steps)
durable orchestration:   Agent Runtime (GCP)  ≈  Temporal (portable, chosen)
```
ADK is **not** a Temporal replacement — the ADK sample only avoids Temporal because Agent Runtime
does that job (and is GCP-locked).

| | ADK | LangGraph |
|---|---|---|
| Affinity | Google, Gemini-native (other models via LiteLLM) | Provider-neutral |
| Durability pairing | leans on Agent Runtime (GCP-managed) | pairs with self-hosted Temporal |
| Best fit if… | standardizing on GCP + Gemini | portability + the chosen **Claude mix** |

**Decision:** **LangGraph** for the agent-reasoning layer — it is provider-neutral, composes with
self-hosted Temporal, and fits the locked cloud-agnostic + Claude-mix + Python decisions. ADK
would only win if the cloud-agnostic constraint were dropped in favor of GCP + Gemini +
Agent Runtime.

### Adopt regardless of framework (from the ADK sample)
1. **Enum state machine is the single source of truth; the LLM may not drive transitions** — it
   only fills structured fields, the state machine (Temporal) advances deterministically. Keeps
   control flow auditable.
2. **Event-driven dormancy gates, never polling** — long waits (human-in-the-loop, additional
   docs) are Temporal timers / `await signal`, resumed over the same webhook channel used for
   results.
3. **Persist structured state, not conversation logs** — cheaper, cleaner context, and
   audit/compliance-friendly (already reflected in `model_outputs` + structured outputs).

---

## Verification (test strategy the build must meet)

Design-only deliverable, so "verification" = how we'd validate once built:
- **Local smoke run** via Docker Compose: submit a sample claim end-to-end (mock OCR + a real
  LLM call) and confirm all status transitions + webhook delivery.
- **Load test** at 1K/min sustained and 3–5× peak; confirm backlog drains, workers autoscale,
  and measure end-to-end p50/p95/p99.
- **Chaos/fault injection** on OCR and each LLM (timeouts, 5xx, 429) → confirm backoff, circuit
  breaker, `PARTIAL_SUCCESS`/`FAILED`, and worker-crash resume.
- **Cost benchmark** on a representative sample with/without batch, routing, and caching to
  validate the spend model before scaling traffic.
- **Webhook delivery test** including signature verification, retries, and DLQ on a dead
  endpoint.
- **Portability check:** deploy the same images to one cloud target (e.g. EKS) with only adapter
  config changed; confirm parity with local.
