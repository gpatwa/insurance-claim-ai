# Deploy & portability

One container image, four roles (`api` / `worker` / `relay` / `notifier`), selected by the
command arg: `python -m claimpipe <role>`. The same image runs everywhere; **only adapter
config differs between clouds** — that is the portability guarantee.

## Build & push the image

```bash
docker build -t ghcr.io/gpatwa/claimpipe:latest .
docker push ghcr.io/gpatwa/claimpipe:latest
```

## Deploy with Helm

```bash
helm upgrade --install claimpipe charts/claimpipe \
  --namespace claimpipe --create-namespace \
  --set image.tag=latest
```

Scale each role independently (per-stage task queues):

```bash
kubectl scale deploy/claimpipe-claimpipe-worker --replicas=8
```

## Deploy with Terraform (any cluster)

```bash
cd deploy/terraform
terraform init
terraform apply -var 'image_tag=latest' \
  -var 'config={CLAIMPIPE_TEMPORAL_ADDRESS="temporal.svc:7233", ...}'
```

## Backing services (provisioned separately / managed)

| Component | Local | AWS | GCP | Azure |
|---|---|---|---|---|
| Temporal | self-hosted (compose) | EKS + Helm | GKE + Helm | AKS + Helm |
| Postgres | compose | RDS / Aurora | Cloud SQL | Azure DB for Postgres |
| Object store | MinIO | S3 | GCS | Blob |
| Event bus | Redpanda | MSK / Redpanda | Managed Kafka / Redpanda | Event Hubs (Kafka) / Redpanda |

## Portability check

The deploy is identical across targets — the diff between a local run and any cloud is confined
to the `config` map (endpoints) and `secret` values. No workflow, activity, or business code
changes. To verify: deploy the same image tag to one cluster changing only `config`, run a claim
end-to-end, and confirm parity with `docker compose` locally.
