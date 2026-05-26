
# almanak gateway

Start the Almanak Gateway gRPC server.

    The gateway is a sidecar service that mediates all external access for
    strategy containers. It provides gRPC services for:

    
    - Market data (prices, balances, indicators)
    - State persistence
    - Transaction execution
    - RPC proxy to blockchain nodes
    - External integrations (CoinGecko, TheGraph, etc.)

    The gateway holds all platform secrets (API keys, RPC credentials).
    Strategy containers connect to the gateway and have no direct external access.

    Examples:

    
        # Start gateway with defaults
        almanak gateway

    
        # Start gateway for Anvil testing
        almanak gateway --network anvil

    
        # Start gateway on custom port
        almanak gateway --port 50052
    

## Usage

```
Usage: almanak gateway [OPTIONS]
```

## Arguments


## Options

* `port`:
    * Type: INT
    * Default: `50051`
    * Usage: `--port`
    gRPC port number (default: 50051).


* `network`:
    * Type: Choice(['mainnet', 'anvil'])
    * Default: `None`
    * Usage: `--network`
    Network environment: 'mainnet' for production RPC, 'anvil' for local fork.


* `metrics`:
    * Type: BOOL
    * Default: `True`
    * Usage: `--metrics`
    Enable Prometheus metrics endpoint (default: enabled).


* `metrics_port`:
    * Type: INT
    * Default: `9090`
    * Usage: `--metrics-port`
    Prometheus metrics port (default: 9090).


* `log_level`:
    * Type: Choice(['debug', 'info', 'warning', 'error'])
    * Default: `info`
    * Usage: `--log-level`
    Log level.


* `chains`:
    * Type: STRING
    * Default: `None`
    * Usage: `--chains`
    Comma-separated chains to pre-initialize (e.g., 'arbitrum,base').


* `insecure`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--insecure`
    Disable auth token requirement for local development. Also set via ALMANAK_GATEWAY_ALLOW_INSECURE env var.


* `standalone`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--standalone`
    Run the gateway in standalone mode (utility DB, no strategy folder). Required when starting the gateway outside any strategy folder for ad-hoc use (e.g., `almanak ax`). Without this flag, the gateway refuses to start outside a strategy folder so it cannot silently write to the per-user utility DB instead of the strategy-anchored one.


* `help`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--help`
    Show this message and exit.


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

  The gateway holds all platform secrets (API keys, RPC credentials). Strategy
  containers connect to the gateway and have no direct external access.

  Examples:

      # Start gateway with defaults
      almanak gateway

      # Start gateway for Anvil testing
      almanak gateway --network anvil

      # Start gateway on custom port
      almanak gateway --port 50052

Options:
  --port INTEGER                  gRPC port number (default: 50051).
  --network [mainnet|anvil]       Network environment: 'mainnet' for
                                  production RPC, 'anvil' for local fork.
  --metrics / --no-metrics        Enable Prometheus metrics endpoint (default:
                                  enabled).
  --metrics-port INTEGER          Prometheus metrics port (default: 9090).
  --log-level [debug|info|warning|error]
                                  Log level.
  --chains TEXT                   Comma-separated chains to pre-initialize
                                  (e.g., 'arbitrum,base').
  --insecure                      Disable auth token requirement for local
                                  development. Also set via
                                  ALMANAK_GATEWAY_ALLOW_INSECURE env var.
  --standalone                    Run the gateway in standalone mode (utility
                                  DB, no strategy folder). Required when
                                  starting the gateway outside any strategy
                                  folder for ad-hoc use (e.g., `almanak ax`).
                                  Without this flag, the gateway refuses to
                                  start outside a strategy folder so it cannot
                                  silently write to the per-user utility DB
                                  instead of the strategy-anchored one.
  --help                          Show this message and exit.
```

