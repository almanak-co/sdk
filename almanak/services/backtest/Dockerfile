# Standalone BacktestService container
#
# Exposes the FastAPI HTTP API for backtesting, paper trading, and fee models.
# No gateway or Anvil required — uses CoinGecko for price data.
#
# Build:
#   docker build -f almanak/services/backtest/Dockerfile -t almanak-backtest-service .
#
# Run:
#   docker run -p 8000:8000 \
#     -e COINGECKO_API_KEY=xxx \
#     almanak-backtest-service
#
# Health check:
#   curl http://localhost:8000/api/v1/health

# ---------------------------------------------------------------------------
# Stage 1: Build
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS builder

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock PYPI.md ./
COPY almanak ./almanak
COPY strategies ./strategies

RUN uv sync --frozen --no-cache

# ---------------------------------------------------------------------------
# Stage 2: Runtime
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS runtime

WORKDIR /app

# Minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user
RUN groupadd --gid 1000 almanak && \
    useradd --uid 1000 --gid 1000 --create-home --shell /bin/bash almanak && \
    chown almanak:almanak /app

# Copy venv and source from builder
COPY --from=builder --chown=almanak:almanak /app/.venv /app/.venv
COPY --from=builder --chown=almanak:almanak /app/almanak /app/almanak
COPY --from=builder --chown=almanak:almanak /app/strategies /app/strategies
COPY --from=builder --chown=almanak:almanak /app/pyproject.toml /app/

USER almanak

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app"

# Service config defaults (overridable via env)
ENV BACKTEST_SERVICE_HOST=0.0.0.0
ENV BACKTEST_SERVICE_PORT=8000
ENV BACKTEST_SERVICE_WORKERS=1
ENV BACKTEST_MAX_JOBS=4
ENV BACKTEST_MAX_PAPER_SESSIONS=2

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/api/v1/health || exit 1

CMD ["python", "-m", "almanak.services.backtest.server"]
