.PHONY: install lint test typecheck up down worker fmt

install:
	uv sync --all-extras --dev

lint:
	uv run ruff check .

fmt:
	uv run ruff format .
	uv run ruff check --fix .

typecheck:
	uv run mypy src

test:
	uv run pytest

up:
	docker compose up -d

down:
	docker compose down -v

worker:
	uv run python -m claimpipe.temporal.worker
