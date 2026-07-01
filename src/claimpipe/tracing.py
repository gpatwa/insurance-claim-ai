"""OpenTelemetry tracing (deployment). Optional — enabled only when the `obs` extra is
installed and an OTLP endpoint is configured. No-op otherwise so CI/local stay light.
"""

from __future__ import annotations


def setup_tracing(service_name: str = "claimpipe", endpoint: str | None = None) -> bool:
    """Configure an OTLP tracer provider. Returns True if tracing was enabled."""
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        return False

    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    exporter = OTLPSpanExporter(endpoint=endpoint) if endpoint else OTLPSpanExporter()
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return True
