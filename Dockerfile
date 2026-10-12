FROM python:3.12-slim AS base

COPY --from=ghcr.io/astral-sh/uv:0.13.0 /uv /bin/uv

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY pyproject.toml uv.lock .python-version README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project

COPY log_service ./log_service
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --no-create-home app \
    && chown -R app:app /app

FROM base AS test

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-editable
COPY --chown=app:app tests ./tests

USER app
CMD ["pytest", "-q"]

FROM base AS runtime

USER app

EXPOSE 8000
CMD ["uvicorn", "log_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
