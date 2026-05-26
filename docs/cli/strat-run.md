
# strat run

Run a strategy from its working directory.

    By default, a managed gateway is auto-started in the background.
    Use --no-gateway to connect to an existing gateway instead.

    Prerequisites:
        - Environment variables: ALMANAK_PRIVATE_KEY, RPC_URL (or ALCHEMY_API_KEY)
        - For anvil mode: Foundry installed (Anvil is auto-started by managed gateway)

    Examples:

        # Run from strategy directory (auto-starts gateway)
        cd almanak/demo_strategies/uniswap_rsi
        almanak strat run --once

        # Run with explicit working directory
        almanak strat run -d almanak/demo_strategies/uniswap_rsi --once

        # Connect to an existing gateway
        almanak strat run --no-gateway --once

        # Run continuously
        almanak strat run --interval 30

        # Dry run (no transactions)
        almanak strat run --dry-run --once

        # Fresh start (clear stale state, useful for Anvil forks)
        almanak strat run --fresh --once

        # Run with live dashboard
        almanak strat run -d almanak/demo_strategies/uniswap_lp --network anvil --dashboard
    

## Usage

```
Usage: almanak strat run [OPTIONS]
```

## Arguments


## Options

* `working_dir`:
    * Type: `Path`
    * Default: `.`
    * Usage: `--working-dir
-d`
    Working directory containing the strategy files. Defaults to the current directory.


* `config_file`:
    * Type: `Path`
    * Default: `None`
    * Usage: `--config
-c`
    Path to strategy config JSON file.


* `once`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--once`
    Run single iteration then exit.


* `interval`:
    * Type: INT
    * Default: `None`
    * Usage: `--interval
-i`
    Loop interval in seconds. Defaults to [tool.almanak.run].interval or 60.


* `dry_run`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--dry-run`
    Execute decide() but don't submit transactions.


* `fresh`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--fresh`
    Clear strategy state before running (useful for fresh Anvil forks).


* `verbose`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--verbose
-v`
    Enable verbose output.


* `network`:
    * Type: Choice(['mainnet', 'anvil'])
    * Default: `None`
    * Usage: `--network
-n`
    Network environment: 'mainnet' for production RPC, 'anvil' for local fork testing. For paper trading with PnL tracking, use 'almanak strat backtest paper'.


* `gateway_host`:
    * Type: STRING
    * Default: `127.0.0.1`
    * Usage: `--gateway-host`
    Gateway gRPC host.


* `gateway_port`:
    * Type: INT
    * Default: `50051`
    * Usage: `--gateway-port`
    Gateway gRPC port.


* `no_gateway`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--no-gateway`
    Do not auto-start a gateway; connect to an existing one.


* `copy_mode`:
    * Type: Choice(['live', 'shadow', 'replay'])
    * Default: `None`
    * Usage: `--copy-mode`
    Copy-trading mode override for this run.


* `copy_shadow`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--copy-shadow`
    Enable copy-trading shadow mode (decisioning only, no submissions).


* `copy_replay_file`:
    * Type: `Path`
    * Default: `None`
    * Usage: `--copy-replay-file`
    Replay file (JSON/JSONL CopySignal fixtures) for copy-trading replay mode.


* `copy_strict`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--copy-strict`
    Enable strict copy-trading validation and fail-closed behavior.


* `dashboard`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--dashboard`
    Launch live dashboard alongside strategy execution.


* `dashboard_port`:
    * Type: INT
    * Default: `8501`
    * Usage: `--dashboard-port`
    Port to run the dashboard on (default: 8501).


* `wallet`:
    * Type: Choice(['default', 'isolated'])
    * Default: `default`
    * Usage: `--wallet`
    Wallet mode for Anvil: 'isolated' derives a unique wallet per strategy for balance isolation.


* `log_file`:
    * Type: `Path`
    * Default: `None`
    * Usage: `--log-file`
    Write JSON logs to this file (in addition to console output). Useful for AI agent analysis.


* `reset_fork`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--reset-fork`
    Reset Anvil fork to latest mainnet block before each iteration (requires --network anvil).


* `max_iterations`:
    * Type: INT
    * Default: `None`
    * Usage: `--max-iterations`
    Maximum number of iterations to run before exiting cleanly. Without this flag, continuous mode runs indefinitely.


* `teardown_after`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--teardown-after`
    After --once iteration, automatically teardown (close all positions). Useful for CI/testing to avoid accumulating stale positions on-chain.


* `anvil_ports`:
    * Type: STRING
    * Default: `None`
    * Usage: `--anvil-port`
    Use existing Anvil instance: CHAIN=PORT (e.g., --anvil-port arbitrum=8545). Repeatable.


* `help`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--help`
    Show this message and exit.


## CLI Help

```
Usage: almanak strat run [OPTIONS]

  Run a strategy from its working directory.

  By default, a managed gateway is auto-started in the background. Use --no-
  gateway to connect to an existing gateway instead.

  Prerequisites:     - Environment variables: ALMANAK_PRIVATE_KEY, RPC_URL (or
  ALCHEMY_API_KEY)     - For anvil mode: Foundry installed (Anvil is auto-
  started by managed gateway)

  Examples:

      # Run from strategy directory (auto-starts gateway)     cd
      almanak/demo_strategies/uniswap_rsi     almanak strat run --once

      # Run with explicit working directory     almanak strat run -d
      almanak/demo_strategies/uniswap_rsi --once

      # Connect to an existing gateway     almanak strat run --no-gateway
      --once

      # Run continuously     almanak strat run --interval 30

      # Dry run (no transactions)     almanak strat run --dry-run --once

      # Fresh start (clear stale state, useful for Anvil forks)     almanak
      strat run --fresh --once

      # Run with live dashboard     almanak strat run -d
      almanak/demo_strategies/uniswap_lp --network anvil --dashboard

Options:
  -d, --working-dir PATH          Working directory containing the strategy
                                  files. Defaults to the current directory.
  -c, --config PATH               Path to strategy config JSON file.
  --once                          Run single iteration then exit.
  -i, --interval INTEGER          Loop interval in seconds. Defaults to
                                  [tool.almanak.run].interval or 60.
  --dry-run                       Execute decide() but don't submit
                                  transactions.
  --fresh                         Clear strategy state before running (useful
                                  for fresh Anvil forks).
  -v, --verbose                   Enable verbose output.
  -n, --network [mainnet|anvil]   Network environment: 'mainnet' for
                                  production RPC, 'anvil' for local fork
                                  testing. For paper trading with PnL
                                  tracking, use 'almanak strat backtest
                                  paper'.
  --gateway-host TEXT             Gateway gRPC host.  [env var:
                                  ALMANAK_GATEWAY_HOST, GATEWAY_HOST; default:
                                  127.0.0.1]
  --gateway-port INTEGER          Gateway gRPC port.  [env var:
                                  ALMANAK_GATEWAY_PORT, GATEWAY_PORT; default:
                                  50051]
  --no-gateway                    Do not auto-start a gateway; connect to an
                                  existing one.
  --copy-mode [live|shadow|replay]
                                  Copy-trading mode override for this run.
  --copy-shadow                   Enable copy-trading shadow mode (decisioning
                                  only, no submissions).
  --copy-replay-file PATH         Replay file (JSON/JSONL CopySignal fixtures)
                                  for copy-trading replay mode.
  --copy-strict                   Enable strict copy-trading validation and
                                  fail-closed behavior.
  --dashboard                     Launch live dashboard alongside strategy
                                  execution.
  --dashboard-port INTEGER        Port to run the dashboard on (default:
                                  8501).
  --wallet [default|isolated]     Wallet mode for Anvil: 'isolated' derives a
                                  unique wallet per strategy for balance
                                  isolation.
  --log-file PATH                 Write JSON logs to this file (in addition to
                                  console output). Useful for AI agent
                                  analysis.
  --reset-fork                    Reset Anvil fork to latest mainnet block
                                  before each iteration (requires --network
                                  anvil).
  --max-iterations INTEGER        Maximum number of iterations to run before
                                  exiting cleanly. Without this flag,
                                  continuous mode runs indefinitely.
  --teardown-after                After --once iteration, automatically
                                  teardown (close all positions). Useful for
                                  CI/testing to avoid accumulating stale
                                  positions on-chain.
  --anvil-port TEXT               Use existing Anvil instance: CHAIN=PORT
                                  (e.g., --anvil-port arbitrum=8545).
                                  Repeatable.
  --help                          Show this message and exit.
```

