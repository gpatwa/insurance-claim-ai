"""FastAPI ingestion service.

Submission is async and event-sourced: the API validates JSON metadata, appends a
CLAIM_RECEIVED event (which creates the projection), starts the durable workflow
(workflow_id = claim_id → dedup/idempotency), and returns 202.
"""

from __future__ import annotations

from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Request
from temporalio.client import Client

from claimpipe.adapters.intake import IntakeAdapter, IntakeError, default_intake_adapters
from claimpipe.adapters.output import OutputAdapter, OutputError, default_output_adapters
from claimpipe.api.schemas import (
    ClaimStatusResponse,
    ReviewRequest,
    SubmitClaimRequest,
    SubmitClaimResponse,
)
from claimpipe.claimtypes import (
    ClaimTypeRegistry,
    Stage,
    UnknownClaimType,
    default_registry,
    validate_attributes,
)
from claimpipe.config import Settings
from claimpipe.domain.events import EventType
from claimpipe.domain.models import ClaimMetadata, ClaimStatus
from claimpipe.eventstore import EventStore
from claimpipe.temporal.workflows import ClaimWorkflow
from claimpipe.tenancy import (
    DEFAULT_TENANT,
    TENANT_HEADER,
    TenantDirectory,
    UnknownTenant,
    default_directory,
)


def _upload_url(settings: Settings, claim_id: str) -> str:
    # M2: placeholder. Real presigned S3/MinIO PUT URL is a later refinement.
    return f"{settings.s3_endpoint_url}/{settings.s3_bucket}/{claim_id}/source.pdf"


def create_app(
    *,
    store: EventStore,
    temporal_client: Client,
    settings: Settings,
    registry: ClaimTypeRegistry | None = None,
    intake_adapters: dict[str, IntakeAdapter] | None = None,
    output_adapters: dict[str, OutputAdapter] | None = None,
    tenants: TenantDirectory | None = None,
) -> FastAPI:
    app = FastAPI(title="claimpipe ingestion")
    app.state.store = store
    app.state.temporal_client = temporal_client
    app.state.settings = settings
    app.state.registry = registry or default_registry()
    app.state.tenants = tenants if tenants is not None else default_directory()
    app.state.intake_adapters = (
        intake_adapters if intake_adapters is not None else default_intake_adapters()
    )
    app.state.output_adapters = (
        output_adapters if output_adapters is not None else default_output_adapters()
    )

    def _tenant(name: str):
        """Resolve the tenant's config (registry + rule sets) or 404."""
        directory: TenantDirectory = app.state.tenants
        try:
            return directory.get(name)
        except UnknownTenant:
            raise HTTPException(
                status_code=404,
                detail=f"unknown tenant '{name}'; known: {directory.names()}",
            ) from None

    async def _submit(
        metadata: ClaimMetadata, idempotency_key: str | None, tenant_id: str
    ) -> SubmitClaimResponse:
        """Shared submit path: resolve tenant + type, validate schema, emit, start."""
        tenant = _tenant(tenant_id)
        metadata = metadata.model_copy(update={"tenant_id": tenant_id})
        reg = tenant.registry
        try:
            defn = reg.get(metadata.claim_type)
        except UnknownClaimType:
            raise HTTPException(
                status_code=422,
                detail=f"unknown claim_type '{metadata.claim_type}'; known: {reg.names()}",
            ) from None
        errors = validate_attributes(defn, metadata.attributes)
        if errors:
            raise HTTPException(status_code=422, detail={"attribute_errors": errors})

        stages = [str(s) for s in defn.stages]
        has_upload = str(Stage.UPLOAD) in stages

        if idempotency_key:
            existing = await store.find_by_idempotency_key(idempotency_key)
            if existing is not None:
                return SubmitClaimResponse(
                    claim_id=existing.claim_id,
                    status=existing.status,
                    claim_type=existing.metadata.claim_type,
                    stages=stages,
                    upload_url=(
                        _upload_url(settings, existing.claim_id) if has_upload else None
                    ),
                    idempotent=True,
                )

        claim_id = str(uuid4())
        await store.append(
            claim_id,
            EventType.CLAIM_RECEIVED,
            {"metadata": metadata.model_dump()},
            idempotency_key=idempotency_key,
        )
        # Pipeline-as-config: pin the claim type's stage list into the workflow input, so
        # later registry changes never affect in-flight claims.
        await temporal_client.start_workflow(
            ClaimWorkflow.run,
            args=[claim_id, stages],
            id=claim_id,
            task_queue=settings.temporal_task_queue,
        )
        return SubmitClaimResponse(
            claim_id=claim_id,
            status=ClaimStatus.RECEIVED,
            claim_type=defn.name,
            stages=stages,
            upload_url=_upload_url(settings, claim_id) if has_upload else None,
        )

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/claim-types")
    async def list_claim_types(
        tenant_id: str = Header(default=DEFAULT_TENANT, alias=TENANT_HEADER),
    ) -> dict:
        reg = _tenant(tenant_id).registry
        return {
            "tenant": tenant_id,
            "claim_types": [reg.get(name).model_dump() for name in reg.names()],
        }

    @app.post("/claims", response_model=SubmitClaimResponse, status_code=202)
    async def submit_claim(
        req: SubmitClaimRequest,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
        tenant_id: str = Header(default=DEFAULT_TENANT, alias=TENANT_HEADER),
    ) -> SubmitClaimResponse:
        return await _submit(req.metadata, idempotency_key, tenant_id)

    @app.post("/intake/{adapter_name}", response_model=SubmitClaimResponse, status_code=202)
    async def intake_submit(
        adapter_name: str,
        request: Request,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
        tenant_id: str = Header(default=DEFAULT_TENANT, alias=TENANT_HEADER),
    ) -> SubmitClaimResponse:
        """Format-specific intake: the adapter normalizes the raw body into a canonical
        claim, then the shared submit path takes over."""
        adapters: dict[str, IntakeAdapter] = request.app.state.intake_adapters
        adapter = adapters.get(adapter_name)
        if adapter is None:
            raise HTTPException(
                status_code=404,
                detail=f"unknown intake adapter '{adapter_name}'; known: {sorted(adapters)}",
            )
        try:
            metadata = adapter.normalize(await request.body())
        except IntakeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
        return await _submit(metadata, idempotency_key, tenant_id)

    @app.post("/claims/{claim_id}/uploaded", status_code=202)
    async def mark_uploaded(claim_id: str, request: Request) -> dict[str, str]:
        client: Client = request.app.state.temporal_client
        store: EventStore = request.app.state.store
        if await store.get(claim_id) is None:
            raise HTTPException(status_code=404, detail="claim not found")
        handle = client.get_workflow_handle(claim_id)
        await handle.signal(ClaimWorkflow.pdf_uploaded, f"{claim_id}/source.pdf")
        return {"claim_id": claim_id, "signal": "pdf_uploaded"}

    @app.get("/claims/{claim_id}", response_model=ClaimStatusResponse)
    async def get_claim(claim_id: str, request: Request) -> ClaimStatusResponse:
        store: EventStore = request.app.state.store
        claim = await store.get(claim_id)
        if claim is None:
            raise HTTPException(status_code=404, detail="claim not found")
        return ClaimStatusResponse(
            claim_id=claim.claim_id,
            status=claim.status,
            decision=claim.decision,
            reason_codes=claim.reason_codes,
        )

    @app.get("/review-queue")
    async def review_queue(
        request: Request,
        tenant_id: str = Header(default=DEFAULT_TENANT, alias=TENANT_HEADER),
    ) -> dict:
        """Work queue: adjudicated claims PENDing for a human reviewer (tenant-scoped)."""
        _tenant(tenant_id)  # 404 on unknown tenant
        store: EventStore = request.app.state.store
        pending = [
            c for c in await store.list_pending_review() if c.metadata.tenant_id == tenant_id
        ]
        return {
            "tenant": tenant_id,
            "pending": [
                {
                    "claim_id": c.claim_id,
                    "claim_type": c.metadata.claim_type,
                    "reason_codes": c.reason_codes,
                    "attributes": c.metadata.attributes,
                }
                for c in pending
            ]
        }

    @app.post("/claims/{claim_id}/review", status_code=202)
    async def submit_review(claim_id: str, req: ReviewRequest, request: Request) -> dict:
        """Reviewer's verdict for a PENDed claim — signals the workflow's REVIEW gate."""
        store: EventStore = request.app.state.store
        client: Client = request.app.state.temporal_client
        claim = await store.get(claim_id)
        if claim is None:
            raise HTTPException(status_code=404, detail="claim not found")
        if claim.decision != "PEND":
            raise HTTPException(
                status_code=409,
                detail=f"claim is not pending review (decision={claim.decision})",
            )
        handle = client.get_workflow_handle(claim_id)
        await handle.signal(ClaimWorkflow.review_completed, req.model_dump())
        return {"claim_id": claim_id, "signal": "review_completed"}

    @app.get("/claims/{claim_id}/documents/{adapter_name}")
    async def render_document(claim_id: str, adapter_name: str, request: Request):
        """Render the claim's outbound document (EOB, denial letter, ...) on demand."""
        from fastapi.responses import Response

        adapters: dict[str, OutputAdapter] = request.app.state.output_adapters
        adapter = adapters.get(adapter_name)
        if adapter is None:
            raise HTTPException(
                status_code=404,
                detail=f"unknown document format '{adapter_name}'; known: {sorted(adapters)}",
            )
        claim = await store.get(claim_id)
        if claim is None:
            raise HTTPException(status_code=404, detail="claim not found")
        preds = await store.predictions(claim_id)
        try:
            body = adapter.render(claim, preds)
        except OutputError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None
        return Response(content=body, media_type=adapter.content_type)

    @app.get("/claims/{claim_id}/predictions")
    async def get_predictions(claim_id: str, request: Request) -> dict:
        store: EventStore = request.app.state.store
        if await store.get(claim_id) is None:
            raise HTTPException(status_code=404, detail="claim not found")
        preds = await store.predictions(claim_id)
        return {"claim_id": claim_id, "predictions": [p.model_dump() for p in preds]}

    return app
