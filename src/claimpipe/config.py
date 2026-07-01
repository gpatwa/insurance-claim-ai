"""Runtime configuration, loaded from environment (12-factor).

All external endpoints are adapter-config only: swapping cloud/provider is a change
here, not in code.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CLAIMPIPE_", env_file=".env", extra="ignore")

    # Temporal
    temporal_address: str = "localhost:7233"
    temporal_namespace: str = "default"
    temporal_task_queue: str = "claimpipe"

    # Postgres (application DB — separate from Temporal's persistence store)
    postgres_dsn: str = "postgresql://claimpipe:claimpipe@localhost:5432/claimpipe"

    # Object store (S3-compatible; MinIO locally)
    s3_endpoint_url: str | None = "http://localhost:9000"
    s3_region: str = "us-east-1"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_bucket: str = "claims"

    # OCR (blackbox)
    ocr_base_url: str = "http://localhost:8080"

    # Message bus (Kafka / Redpanda) — event fan-out transport
    kafka_bootstrap: str = "localhost:19092"
    event_topic: str = "claim-events"

    # LLM tiered routing (see design doc: cost-tier first, escalate on low confidence)
    confidence_threshold: float = 0.85
    high_value_amount: float = 25000.0

    # Webhook notification
    webhook_hmac_secret: str = "dev-secret-change-me"
    webhook_max_attempts: int = 5
    webhook_backoff_seconds: float = 0.5

    # Retention (days) — see design doc
    raw_pdf_retention_days: int = 7


def get_settings() -> Settings:
    return Settings()
