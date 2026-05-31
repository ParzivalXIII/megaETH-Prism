# Stage 1: Build
FROM ghcr.io/astral-sh/uv:python3.11-bookworm AS builder

WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install production dependencies only
RUN uv sync --no-dev --frozen

# Stage 2: Runtime
FROM python:3.11-slim

WORKDIR /app

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser -d /app -s /bin/false appuser

# Copy virtual env from builder
COPY --from=builder /app/.venv /app/.venv

# Copy application code
COPY app/ /app/app/
COPY .env /app/.env

# Set PATH to include venv
ENV PATH="/app/.venv/bin:$PATH"
ENV DOCKER_ENV=true
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=15s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health/live')"

USER appuser

EXPOSE 8080

CMD ["python", "-m", "app.api.main"]
