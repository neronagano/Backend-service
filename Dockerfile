FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY log_service ./log_service
RUN pip install --no-cache-dir . \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --no-create-home app \
    && chown -R app:app /app

FROM base AS test

RUN pip install --no-cache-dir '.[dev]'
COPY --chown=app:app tests ./tests

USER app
CMD ["pytest", "-q"]

FROM base AS runtime

USER app

EXPOSE 8000
CMD ["uvicorn", "log_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
