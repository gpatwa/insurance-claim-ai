# Multi-stage build. One image, four roles (api/worker/relay/notifier).
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS build
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml README.md ./
COPY src ./src
RUN uv sync --all-extras --no-dev --frozen || uv sync --all-extras --no-dev

FROM python:3.12-slim-bookworm
WORKDIR /app
COPY --from=build /app /app
ENV PATH="/app/.venv/bin:$PATH"
# default role is the worker; override in the Helm chart per deployment
ENTRYPOINT ["python", "-m", "claimpipe"]
CMD ["worker"]
