
# almanak gateway

Start the Almanak Gateway gRPC server.

The gateway is a sidecar service that mediates all external access for
strategy containers. It must be running before any strategy can execute.

## What the Gateway Provides

The gateway exposes gRPC services for:

- **Market data** - prices, balances, technical indicators
- **State persistence** - strategy state load/save
- **Transaction execution** - intent compilation and on-chain execution
- **RPC proxy** - controlled JSON-RPC access to blockchain nodes
- **External integrations** - CoinGecko, Binance, TheGraph, Enso, Polymarket
- **Observability** - logging, alerts, timeline events, metrics

The gateway holds all platform secrets (API keys, private keys, RPC credentials).
Strategy containers connect to the gateway via gRPC and have no direct external access.

## Usage

```
Usage: almanak gateway [OPTIONS]
```

## Options

* `port`:
    * Type: INT
    * Default: `50051`
    * Env: `GATEWAY_PORT`
    * Usage: `--port`
    gRPC port number (default: 50051).

* `network`:
    * Type: Choice
    * Choices: `mainnet`, `anvil`
    * Default: `mainnet`
    * Env: `ALMANAK_GATEWAY_NETWORK`
    * Usage: `--network`
    Network environment: 'mainnet' for production RPC, 'anvil' for local fork.

* `metrics`:
    * Type: BOOL
    * Default: `True`
    * Env: `GATEWAY_METRICS_ENABLED`
    * Usage: `--metrics` / `--no-metrics`
    Enable Prometheus metrics endpoint (default: enabled).

* `metrics_port`:
    * Type: INT
    * Default: `9090`
    * Env: `GATEWAY_METRICS_PORT`
    * Usage: `--metrics-port`
    Prometheus metrics port (default: 9090).

* `log_level`:
    * Type: Choice
    * Choices: `debug`, `info`, `warning`, `error`
    * Default: `info`
    * Usage: `--log-level`
    Log level.

* `help`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--help`
    Show this message and exit.

## Environment Variables

The gateway reads additional configuration from environment variables.
These are separate from the CLI options above.

### Required (one of these must be set)

| Variable | Description |
|----------|-------------|
| `ALMANAK_GATEWAY_AUTH_TOKEN` | Shared secret for client authentication. When set, all gRPC clients must provide this token. |
| `ALMANAK_GATEWAY_ALLOW_INSECURE` | Set to `true` to bypass auth requirement (development only). |

### API Keys

| Variable | Description |
|----------|-------------|
| `ALMANAK_GATEWAY_ALCHEMY_API_KEY` | Alchemy API key for RPC access. Optional fallback for blockchain operations (not needed if `RPC_URL` or per-chain RPC URLs are set). |
| `ALMANAK_GATEWAY_COINGECKO_API_KEY` | CoinGecko API key. Optional - falls back to free tier (30 req/min). |
| `ALMANAK_GATEWAY_PRIVATE_KEY` | Private key for transaction signing. Falls back to `ALMANAK_PRIVATE_KEY` if not set. |

### Persistence

| Variable | Description |
|----------|-------------|
| `ALMANAK_GATEWAY_DATABASE_URL` | PostgreSQL URL for state persistence. |
| `ALMANAK_GATEWAY_TIMELINE_DB_PATH` | SQLite path for timeline event storage. |

### Alerting

| Variable | Description |
|----------|-------------|
| `ALMANAK_GATEWAY_SLACK_WEBHOOK_URL` | Slack webhook for alert delivery. |
| `ALMANAK_GATEWAY_TELEGRAM_BOT_TOKEN` | Telegram bot token for alerts. |
| `ALMANAK_GATEWAY_TELEGRAM_CHAT_ID` | Telegram chat ID for alerts. |

### Audit Logging

| Variable | Default | Description |
|----------|---------|-------------|
| `ALMANAK_GATEWAY_AUDIT_ENABLED` | `true` | Enable structured JSON audit logs. |
| `ALMANAK_GATEWAY_AUDIT_LOG_LEVEL` | `info` | Audit log level (debug, info, warning, error). |

## CLI Help

```
Usage: almanak gateway [OPTIONS]

  Start the Almanak Gateway gRPC server.

  The gateway is a sidecar service that mediates all external access for
  strategy containers. It provides gRPC services for:

  - Market data (prices, balances, indicators)
  - State persistence
  - Transaction execution
  - RPC proxy to blockchain nodes
  - External integrations (CoinGecko, TheGraph, etc.)

  The gateway holds all platform secrets (API keys, RPC credentials).
  Strategy containers connect to the gateway and have no direct external access.

  Examples:

      # Start gateway with defaults
      almanak gateway

      # Start gateway for Anvil testing
      almanak gateway --network anvil

      # Start gateway on custom port
      almanak gateway --port 50052

Options:
  --port INTEGER          gRPC port number (default: 50051).
  --network [mainnet|anvil]
                          Network environment.
  --metrics / --no-metrics
                          Enable Prometheus metrics endpoint (default: enabled).
  --metrics-port INTEGER  Prometheus metrics port (default: 9090).
  --log-level [debug|info|warning|error]
                          Log level.
  --help                  Show this message and exit.
```

## Examples

```bash
# Start gateway with defaults (mainnet, port 50051)
almanak gateway

# Start for local Anvil testing
almanak gateway --network anvil

# Custom port with debug logging
almanak gateway --port 50052 --log-level debug

# Disable metrics
almanak gateway --no-metrics

# Set via environment variables
GATEWAY_PORT=50052 ALMANAK_GATEWAY_NETWORK=anvil almanak gateway
```

## Metrics Endpoints

When metrics are enabled (default), the gateway exposes an HTTP server:

| Endpoint | Description |
|----------|-------------|
| `GET /metrics` | Prometheus metrics in standard format |
| `GET /health` | Plain text health check ("OK") |

Default metrics URL: `http://localhost:9090/metrics`

## gRPC Reflection

The gateway supports gRPC reflection for debugging with tools like `grpcurl`:

```bash
# List available services
grpcurl -plaintext localhost:50051 list

# Describe a service
grpcurl -plaintext localhost:50051 describe almanak.gateway.MarketService

# Call a method
grpcurl -plaintext -d '{"chain": "arbitrum", "token": "ETH"}' \
  localhost:50051 almanak.gateway.MarketService/GetPrice
```

## Typical Development Workflow

```bash
# Run your strategy (auto-starts a managed gateway in the background)
cd strategies/demo/uniswap_rsi
almanak strat run --once

# Or start a standalone gateway for shared use
almanak gateway --network anvil
```

See also: [Gateway API Reference](../gateway/api-reference.md) | [Gateway Troubleshooting](../gateway/troubleshooting.md)
