"""FastAPI ingestion service — the customer front door.

Every endpoint (except /healthz) requires an API key (X-API-Key). The key resolves to a
Customer that carries the customer_id (stamped onto claims — never trusted from the body),
the tenant (never trusted from a header), and roles:

  - submit: submit claims / intake, upload documents, read OWN claims + documents
  - review: work the tenant's review queue, post verdicts, read the tenant's claims

Submission is async and event-sourced: validate metadata against the tenant's claim-type
schema, append CLAIM_RECEIVED, start the durable workflow (workflow_id = claim_id), return
202 with a real presigned upload URL for the document.
"""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse
from temporalio.client import Client

from claimpipe.adapters.intake import IntakeAdapter, IntakeError, default_intake_adapters
from claimpipe.adapters.object_store import ObjectStore
from claimpipe.adapters.output import OutputAdapter, OutputError, default_output_adapters
from claimpipe.api.schemas import (
    ClaimStatusResponse,
    ReviewRequest,
    SubmitClaimRequest,
    SubmitClaimResponse,
)
from claimpipe.claimtypes import (
    Stage,
    UnknownClaimType,
    validate_attributes,
)
from claimpipe.config import Settings
from claimpipe.customers import (
    API_KEY_HEADER,
    ROLE_REVIEW,
    ROLE_SUBMIT,
    Customer,
    CustomerRegistry,
    default_customers,
)
from claimpipe.domain.events import EventType
from claimpipe.domain.models import Claim, ClaimMetadata, ClaimStatus
from claimpipe.eventstore import EventStore
from claimpipe.temporal.workflows import ClaimWorkflow
from claimpipe.tenancy import TenantDirectory, UnknownTenant, default_directory

UPLOAD_URL_TTL_S = 900  # 15 minutes to PUT the document

# Self-contained portal page (submitter + reviewer UI). Served as static HTML — the page
# itself is public; every API call it makes carries the user's X-API-Key.
_PORTAL_HTML = (Path(__file__).parent / "static" / "portal.html").read_text(encoding="utf-8")


def create_app(
    *,
    store: EventStore,
    temporal_client: Client,
    settings: Settings,
    intake_adapters: dict[str, IntakeAdapter] | None = None,
    output_adapters: dict[str, OutputAdapter] | None = None,
    tenants: TenantDirectory | None = None,
    customers: CustomerRegistry | None = None,
    object_store: ObjectStore | None = None,
) -> FastAPI:
    app = FastAPI(title="claimpipe ingestion")
    app.state.store = store
    app.state.temporal_client = temporal_client
    app.state.settings = settings
    app.state.tenants = tenants if tenants is not None else default_directory()
    app.state.customers = customers if customers is not None else default_customers()
    app.state.object_store = object_store
    app.state.intake_adapters = (
        intake_adapters if intake_adapters is not None else default_intake_adapters()
    )
    app.state.output_adapters = (
        output_adapters if output_adapters is not None else default_output_adapters()
    )

    # ------------------------------------------------------------------ auth dependencies

    async def require_customer(
        api_key: str | None = Header(default=None, alias=API_KEY_HEADER),
    ) -> Customer:
        customer = app.state.customers.authenticate(api_key)
        if customer is None:
            raise HTTPException(status_code=401, detail=f"missing or invalid {API_KEY_HEADER}")
        return customer

    def require_role(role: str):
        async def dep(customer: Customer = Depends(require_customer)) -> Customer:
            if not customer.can(role):
                raise HTTPException(
                    status_code=403, detail=f"API key lacks the '{role}' role"
                )
            return customer

        return dep

    require_submit = require_role(ROLE_SUBMIT)
    require_review = require_role(ROLE_REVIEW)

    def _tenant(name: str):
        directory: TenantDirectory = app.state.tenants
        try:
            return directory.get(name)
        except UnknownTenant:
            raise HTTPException(
                status_code=404,
                detail=f"tenant '{name}' is not configured on this deployment",
            ) from None

    async def _visible_claim(customer: Customer, claim_id: str) -> Claim:
        """Load a claim the customer may see: submitters see their OWN claims; reviewers
        see their tenant's. Anything else is a 404 (no existence leak)."""
        claim = await store.get(claim_id)
        if claim is not None:
            if customer.can(ROLE_SUBMIT) and claim.metadata.customer_id == customer.customer_id:
                return claim
            if customer.can(ROLE_REVIEW) and claim.metadata.tenant_id == customer.tenant_id:
                return claim
        raise HTTPException(status_code=404, detail="claim not found")

    async def _upload_url(claim_id: str) -> str:
        obj: ObjectStore | None = app.state.object_store
        key = f"{claim_id}/source.pdf"
        if obj is not None:
            return await obj.presigned_put(key, expires_s=UPLOAD_URL_TTL_S)
        # no object store wired (unit-test apps): deterministic placeholder
        return f"{settings.s3_endpoint_url}/{settings.s3_bucket}/{key}"

    # ------------------------------------------------------------------ shared submit path

    async def _submit(
        metadata: ClaimMetadata, idempotency_key: str | None, customer: Customer
    ) -> SubmitClaimResponse:
        """Resolve tenant + type from the AUTHENTICATED customer, validate, emit, start."""
        tenant = _tenant(customer.tenant_id)
        updates: dict = {"tenant_id": customer.tenant_id, "customer_id": customer.customer_id}
        if metadata.customer_id and metadata.customer_id != customer.customer_id:
            # e.g. a clearinghouse submitting on behalf of a provider: keep the reference,
            # but the authenticated identity owns the claim.
            updates["attributes"] = {
                **metadata.attributes,
                "submitted_customer_ref": metadata.customer_id,
            }
        metadata = metadata.model_copy(update=updates)

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
                        await _upload_url(existing.claim_id) if has_upload else None
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
            upload_url=await _upload_url(claim_id) if has_upload else None,
        )

    # ------------------------------------------------------------------ endpoints

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/portal", response_class=HTMLResponse)
    async def portal() -> str:
        return _PORTAL_HTML

    @app.get("/claim-types")
    async def list_claim_types(customer: Customer = Depends(require_customer)) -> dict:
        reg = _tenant(customer.tenant_id).registry
        return {
            "tenant": customer.tenant_id,
            "claim_types": [reg.get(name).model_dump() for name in reg.names()],
        }

    @app.post("/claims", response_model=SubmitClaimResponse, status_code=202)
    async def submit_claim(
        req: SubmitClaimRequest,
        customer: Customer = Depends(require_submit),
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> SubmitClaimResponse:
        return await _submit(req.metadata, idempotency_key, customer)

    @app.post("/intake/{adapter_name}", response_model=SubmitClaimResponse, status_code=202)
    async def intake_submit(
        adapter_name: str,
        request: Request,
        customer: Customer = Depends(require_submit),
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
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
        return await _submit(metadata, idempotency_key, customer)

    @app.post("/claims/{claim_id}/uploaded", status_code=202)
    async def mark_uploaded(
        claim_id: str, customer: Customer = Depends(require_submit)
    ) -> dict[str, str]:
        claim = await _visible_claim(customer, claim_id)
        handle = temporal_client.get_workflow_handle(claim.claim_id)
        await handle.signal(ClaimWorkflow.pdf_uploaded, f"{claim.claim_id}/source.pdf")
        return {"claim_id": claim.claim_id, "signal": "pdf_uploaded"}

    @app.get("/claims/{claim_id}", response_model=ClaimStatusResponse)
    async def get_claim(
        claim_id: str, customer: Customer = Depends(require_customer)
    ) -> ClaimStatusResponse:
        claim = await _visible_claim(customer, claim_id)
        return ClaimStatusResponse(
            claim_id=claim.claim_id,
            status=claim.status,
            decision=claim.decision,
            reason_codes=claim.reason_codes,
        )

    @app.get("/review-queue")
    async def review_queue(customer: Customer = Depends(require_review)) -> dict:
        """Work queue: adjudicated claims PENDing for a human reviewer (tenant-scoped)."""
        _tenant(customer.tenant_id)
        pending = [
            c
            for c in await store.list_pending_review()
            if c.metadata.tenant_id == customer.tenant_id
        ]
        return {
            "tenant": customer.tenant_id,
            "pending": [
                {
                    "claim_id": c.claim_id,
                    "claim_type": c.metadata.claim_type,
                    "reason_codes": c.reason_codes,
                    "attributes": c.metadata.attributes,
                }
                for c in pending
            ],
        }

    @app.post("/claims/{claim_id}/review", status_code=202)
    async def submit_review(
        claim_id: str, req: ReviewRequest, customer: Customer = Depends(require_review)
    ) -> dict:
        """Reviewer's verdict for a PENDed claim — signals the workflow's REVIEW gate."""
        claim = await _visible_claim(customer, claim_id)
        if claim.decision != "PEND":
            raise HTTPException(
                status_code=409,
                detail=f"claim is not pending review (decision={claim.decision})",
            )
        handle = temporal_client.get_workflow_handle(claim.claim_id)
        await handle.signal(ClaimWorkflow.review_completed, req.model_dump())
        return {"claim_id": claim.claim_id, "signal": "review_completed"}

    @app.get("/claims/{claim_id}/documents/{adapter_name}")
    async def render_document(
        claim_id: str, adapter_name: str, customer: Customer = Depends(require_customer)
    ):
        """Render the claim's outbound document (EOB, denial letter, ...) on demand."""
        from fastapi.responses import Response

        adapters: dict[str, OutputAdapter] = app.state.output_adapters
        adapter = adapters.get(adapter_name)
        if adapter is None:
            raise HTTPException(
                status_code=404,
                detail=f"unknown document format '{adapter_name}'; known: {sorted(adapters)}",
            )
        claim = await _visible_claim(customer, claim_id)
        preds = await store.predictions(claim.claim_id)
        try:
            body = adapter.render(claim, preds)
        except OutputError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None
        return Response(content=body, media_type=adapter.content_type)

    @app.get("/claims/{claim_id}/predictions")
    async def get_predictions(
        claim_id: str, customer: Customer = Depends(require_customer)
    ) -> dict:
        claim = await _visible_claim(customer, claim_id)
        preds = await store.predictions(claim.claim_id)
        return {"claim_id": claim.claim_id, "predictions": [p.model_dump() for p in preds]}

    return app
