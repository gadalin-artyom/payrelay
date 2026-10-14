# syntax=docker/dockerfile:1

FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /usr/local/bin/uv

COPY pyproject.toml README.md uv.lock ./
COPY src ./src
COPY alembic.ini ./
COPY alembic ./alembic

FROM base AS runtime

RUN uv sync --frozen --no-dev

EXPOSE 8000

CMD ["python", "-m", "payrelay.entrypoints.api"]

FROM base AS test

COPY tests ./tests

RUN uv sync --frozen --group dev

CMD ["uv", "run", "pytest", "-m", "unit"]
