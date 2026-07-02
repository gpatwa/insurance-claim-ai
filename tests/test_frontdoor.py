"""M14: customer front door — API-key auth, roles, ownership, presigned uploads.

The key is the identity: customer_id is stamped onto claims (never trusted from the body),
tenant comes from the key (never from a header), and visibility is owner-or-reviewer.
"""

from __future__ import annotations

import httpx
from temporalio.testing import WorkflowEnvironment

from claimpipe.adapters.object_store import InMemoryObjectStore
from claimpipe.api.app import create_app
from claimpipe.config import Settings
from claimpipe.customers import default_customers, hash_key
from claimpipe.eventstore import InMemoryEventStore
from tests.helpers import META, TASK_QUEUE, make_worker

SUBMIT_A = {"X-API-Key": "ck_acme_submitter_01"}  # acme-carrier, submit only
SUBMIT_B = {"X-API-Key": "ck_lakeside_clearing_01"}  # lakeside-clearinghouse, submit only
REVIEWER = {"X-API-Key": "ck_payer_reviewer_01"}  # payer-adjusters, review only


def _anon(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


def _app(env, store, obj=None):
    settings = Settings(temporal_task_queue=TASK_QUEUE)
    return create_app(
        store=store, temporal_client=env.client, settings=settings, object_store=obj
    )


async def test_missing_or_unknown_key_401() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        app = _app(env, InMemoryEventStore())
        async with _anon(app) as ac:
            assert (await ac.get("/claim-types")).status_code == 401
            r = await ac.post("/claims", json={"metadata": META})
            assert r.status_code == 401
            bad = {"X-API-Key": "ck_wrong"}
            assert (await ac.get("/claim-types", headers=bad)).status_code == 401
            # health stays open
            assert (await ac.get("/healthz")).status_code == 200


async def test_role_enforcement() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        app = _app(env, InMemoryEventStore())
        async with _anon(app) as ac:
            # reviewer key cannot submit
            r = await ac.post("/claims", json={"metadata": META}, headers=REVIEWER)
            assert r.status_code == 403
            # submitter key cannot work the review queue
            assert (await ac.get("/review-queue", headers=SUBMIT_A)).status_code == 403
            r = await ac.post(
                "/claims/x/review",
                json={"decision": "APPROVE", "reviewer": "r"},
                headers=SUBMIT_A,
            )
            assert r.status_code == 403


async def test_identity_stamped_and_ownership_enforced() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        async with make_worker(env, store):
            app = _app(env, store)
            async with _anon(app) as ac:
                # customer A submits (body claims a different customer_id — ignored)
                meta = {**META, "claim_type": "metadata-only", "customer_id": "spoofed-id"}
                r = await ac.post("/claims", json={"metadata": meta}, headers=SUBMIT_A)
                assert r.status_code == 202
                cid = r.json()["claim_id"]

                claim = await store.get(cid)
                assert claim.metadata.customer_id == "acme-carrier"  # from the KEY
                # the body's id is preserved as an on-behalf-of reference only
                assert claim.metadata.attributes["submitted_customer_ref"] == "spoofed-id"

                # owner sees it
                assert (await ac.get(f"/claims/{cid}", headers=SUBMIT_A)).status_code == 200
                # a DIFFERENT submitter gets 404 (no existence leak)
                assert (await ac.get(f"/claims/{cid}", headers=SUBMIT_B)).status_code == 404
                assert (
                    await ac.get(f"/claims/{cid}/predictions", headers=SUBMIT_B)
                ).status_code == 404
                # but the tenant's reviewer can see it
                assert (await ac.get(f"/claims/{cid}", headers=REVIEWER)).status_code == 200


async def test_presigned_upload_url_issued() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        obj = InMemoryObjectStore()
        async with make_worker(env, store, obj_store=obj):
            app = _app(env, store, obj=obj)
            async with _anon(app) as ac:
                r = await ac.post("/claims", json={"metadata": META}, headers=SUBMIT_A)
                assert r.status_code == 202
                url = r.json()["upload_url"]
                # a real presign call went through the ObjectStore adapter
                assert url.startswith("mem://claims/presigned/")
                assert "source.pdf" in url and "expires=900" in url


def test_keys_are_stored_hashed() -> None:
    reg = default_customers()
    # authenticate works with the plaintext key…
    assert reg.authenticate("ck_dev_all_01") is not None
    # …but the registry holds only hashes (no plaintext keys anywhere)
    assert "ck_dev_all_01" not in str(vars(reg))
    assert hash_key("ck_dev_all_01") in reg._by_key_hash  # noqa: SLF001

async def test_portal_page_served() -> None:
    """The portal page itself is public; its API calls carry the user's key."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        app = _app(env, InMemoryEventStore())
        async with _anon(app) as ac:
            r = await ac.get("/portal")
            assert r.status_code == 200
            assert "text/html" in r.headers["content-type"]
            assert "claimpipe portal" in r.text
            assert "X-API-Key" in r.text  # the page authenticates via the front door
