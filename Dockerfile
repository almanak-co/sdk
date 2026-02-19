# All-in-one development workstation for Almanak SDK
#
# Contains the full SDK, gateway, CLI, Foundry (anvil/forge/cast), and demo
# strategies. Designed for interactive use: deploy on k8s, exec in, run strategies.
#
# Build:
#   docker build -t almanak-workstation .
#
# Run:
#   docker run -d --name almanak-ws \
#     -e ALCHEMY_API_KEY=xxx \
#     -e ALMANAK_PRIVATE_KEY=0x... \
#     almanak-workstation
#   docker exec -it almanak-ws bash

# ---------------------------------------------------------------------------
# Stage 1: Build dependencies
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS builder

WORKDIR /app

# Install uv
RUN pip install --no-cache-dir uv

# Copy dependency files first for caching
COPY pyproject.toml uv.lock PYPI.md ./

# Copy source (needed for editable install)
COPY almanak ./almanak

# Install all dependencies into .venv
RUN uv sync --frozen --no-cache

# ---------------------------------------------------------------------------
# Stage 2: Runtime image
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS runtime

ARG TARGETARCH

WORKDIR /app

# System packages for interactive development
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    make \
    procps \
    less \
    vim-tiny \
    && rm -rf /var/lib/apt/lists/*

# Install uv for dependency management
RUN pip install --no-cache-dir uv

# Install grpc_health_probe (for gateway health checks)
ENV GRPC_HEALTH_PROBE_VERSION=v0.4.24
RUN ARCH=$(case "${TARGETARCH}" in arm64) echo "arm64" ;; *) echo "amd64" ;; esac) && \
    curl -fsSL "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-${ARCH}" \
    -o /tmp/grpc_health_probe-linux-${ARCH} && \
    curl -fsSL "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/checksums.txt" \
    -o /tmp/checksums.txt && \
    cd /tmp && \
    grep "grpc_health_probe-linux-${ARCH}" checksums.txt | sha256sum -c - && \
    mv /tmp/grpc_health_probe-linux-${ARCH} /usr/local/bin/grpc_health_probe && \
    chmod +x /usr/local/bin/grpc_health_probe && \
    rm -f /tmp/checksums.txt

# Install Foundry (anvil, forge, cast) via foundryup
RUN curl -fsSL https://foundry.paradigm.xyz | SHELL=/bin/bash bash && \
    /root/.foundry/bin/foundryup && \
    mv /root/.foundry/bin/anvil /root/.foundry/bin/forge /root/.foundry/bin/cast /usr/local/bin/ && \
    rm -rf /root/.foundry

# Create non-root user
RUN groupadd --gid 1000 almanak && \
    useradd --uid 1000 --gid 1000 --create-home --shell /bin/bash almanak && \
    chown almanak:almanak /app

# Copy venv from builder
COPY --from=builder --chown=almanak:almanak /app/.venv /app/.venv

# Copy all project files (filtered by .dockerignore)
COPY --chown=almanak:almanak . .

# Create user workspace for custom strategies
RUN mkdir -p /app/strategies/my_strategies && \
    chown almanak:almanak /app/strategies/my_strategies

# Create CLI config directory
RUN mkdir -p /home/almanak/.config/almanak && \
    chown -R almanak:almanak /home/almanak/.config

# Switch to non-root user
USER almanak

# Environment
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app"
ENV GATEWAY_HOST=127.0.0.1
ENV GATEWAY_PORT=50051
ENV ALMANAK_GATEWAY_ALLOW_INSECURE=true

# Welcome message and convenience aliases
RUN printf '\n# Almanak Workstation\nalias ll="ls -la"\nalias gs="almanak strat run --once"\nalias gw="almanak gateway"\n\necho ""\necho "  Almanak Strategy Workstation"\necho "  ----------------------------"\necho "  almanak --help          CLI help"\necho "  almanak strat run       Run a strategy (auto-starts gateway)"\necho "  almanak strat new       Create a new strategy"\necho "  anvil --version         Foundry toolchain"\necho ""\necho "  Demo strategies:        /app/strategies/demo/"\necho "  Your strategies:        /app/strategies/my_strategies/"\necho ""\necho "  Quick start:"\necho "    cd /app/strategies/demo/uniswap_rsi"\necho "    almanak strat run --network anvil --once"\necho ""\n' >> /home/almanak/.bashrc

CMD ["sleep", "infinity"]
