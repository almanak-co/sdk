
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

* `teardown_after`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--teardown-after`
    After --once iteration, automatically teardown (close all positions). Useful for CI/testing to avoid accumulating stale positions on-chain.


* `max_iterations`:
    * Type: INT
    * Default: `None`
    * Usage: `--max-iterations`
    Maximum number of iterations to run before exiting cleanly. Without this flag, continuous mode runs indefinitely.


* `reset_fork`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--reset-fork`
    Reset Anvil fork to latest mainnet block before each iteration (requires --network anvil). Gives live on-chain state for fork testing.


* `log_file`:
    * Type: `Path`
    * Default: `None`
    * Usage: `--log-file`
    Write JSON logs to this file (in addition to console output). Useful for AI agent analysis.


* `wallet`:
    * Type: Choice(['default', 'isolated'])
    * Default: `default`
    * Usage: `--wallet`
    Wallet mode for Anvil: 'isolated' derives a unique wallet per strategy for balance isolation.


* `keep_anvil`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--keep-anvil`
    Keep managed Anvil fork(s) running after the runner exits (incl. after a graceful teardown), detached in their own session, for post-run/post-teardown inspection or a sealed audit. You must kill the fork PID(s) yourself afterwards.


* `anvil_ports`:
    * Type: STRING
    * Default: `None`
    * Usage: `--anvil-port`
    Use existing Anvil instance: CHAIN=PORT (e.g., --anvil-port arbitrum=8545). Repeatable.


* `no_gateway`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--no-gateway`
    Do not auto-start a gateway; connect to an existing one.


* `copy_strict`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--copy-strict`
    Enable strict copy-trading schema + fail-closed validation.


* `copy_replay_file`:
    * Type: `Path`
    * Default: `None`
    * Usage: `--copy-replay-file`
    Replay file (JSON/JSONL CopySignal fixtures) for copy-trading replay mode.


* `copy_shadow`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--copy-shadow`
    Enable copy-trading shadow mode (decisioning only, no submissions).


* `copy_mode`:
    * Type: Choice(['live', 'shadow', 'replay'])
    * Default: `None`
    * Usage: `--copy-mode`
    Copy-trading mode override for this run.


* `fresh`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--fresh`
    Start from a clean slate: clear persisted strategy state before running instead of resuming it (useful for fresh Anvil forks, or to recover from a desynced restart). Default is to resume existing state; the boot banner and log report whether this run RESUMED or started FRESH.


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


* `network`:
    * Type: Choice(['mainnet', 'anvil'])
    * Default: `None`
    * Usage: `--network
-n`
    Network environment: 'mainnet' for production RPC, 'anvil' for local fork testing (auto-starts Anvil on a free port). For paper trading with PnL tracking, use 'almanak strat backtest paper'. Overrides config.json 'network' field.


* `simulate_tx`:
    * Type: BOOL
    * Default: `None`
    * Usage: `--simulate-tx`
    Enable/disable transaction simulation via Tenderly/Alchemy before submission. Default: use SIMULATION_ENABLED env var


* `dashboard_mode`:
    * Type: Choice(['hosted-parity', 'command-center'])
    * Default: `hosted-parity`
    * Usage: `--dashboard-mode`
    Dashboard layout. 'hosted-parity' (default) mirrors the hosted platform: one strategy, one gateway, no multi-strategy navigation. 'command-center' opens the repo-wide browser. Standalone mode (--dashboard from a non-strategy folder) always uses Command Center.


* `dashboard_port`:
    * Type: INT
    * Default: `8501`
    * Usage: `--dashboard-port`
    Port to run the dashboard on (default: 8501).


* `dashboard`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--dashboard`
    Launch live dashboard alongside strategy execution.


* `debug`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--debug`
    Enable debug output (includes Web3/HTTP logs).


* `verbose`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--verbose
-v`
    Enable verbose output (detailed strategy info).


* `list_all`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--list`
    List all available strategies and exit.


* `dry_run`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--dry-run`
    Execute decide() but don't submit transactions.


* `interval`:
    * Type: INT
    * Default: `None`
    * Usage: `--interval
-i`
    Loop interval in seconds. Defaults to [tool.almanak.run].interval or 60.


* `once`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--once`
    Run single iteration then exit.


* `config_file`:
    * Type: `Path`
    * Default: `None`
    * Usage: `--config
-c`
    Path to strategy config JSON file.


* `working_dir`:
    * Type: `Path`
    * Default: `.`
    * Usage: `--working-dir
-d`
    Working directory containing the strategy files. Defaults to the current directory.


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
  --teardown-after                After --once iteration, automatically
                                  teardown (close all positions). Useful for
                                  CI/testing to avoid accumulating stale
                                  positions on-chain.
  --max-iterations INTEGER        Maximum number of iterations to run before
                                  exiting cleanly. Without this flag,
                                  continuous mode runs indefinitely.
  --reset-fork                    Reset Anvil fork to latest mainnet block
                                  before each iteration (requires --network
                                  anvil). Gives live on-chain state for fork
                                  testing.
  --log-file FILE                 Write JSON logs to this file (in addition to
                                  console output). Useful for AI agent
                                  analysis.
  --wallet [default|isolated]     Wallet mode for Anvil: 'isolated' derives a
                                  unique wallet per strategy for balance
                                  isolation.
  --keep-anvil                    Keep managed Anvil fork(s) running after the
                                  runner exits (incl. after a graceful
                                  teardown), detached in their own session,
                                  for post-run/post-teardown inspection or a
                                  sealed audit. You must kill the fork PID(s)
                                  yourself afterwards.
  --anvil-port TEXT               Use existing Anvil instance: CHAIN=PORT
                                  (e.g., --anvil-port arbitrum=8545).
                                  Repeatable.
  --no-gateway                    Do not auto-start a gateway; connect to an
                                  existing one.
  --copy-strict                   Enable strict copy-trading schema + fail-
                                  closed validation.
  --copy-replay-file PATH         Replay file (JSON/JSONL CopySignal fixtures)
                                  for copy-trading replay mode.
  --copy-shadow                   Enable copy-trading shadow mode (decisioning
                                  only, no submissions).
  --copy-mode [live|shadow|replay]
                                  Copy-trading mode override for this run.
  --fresh                         Start from a clean slate: clear persisted
                                  strategy state before running instead of
                                  resuming it (useful for fresh Anvil forks,
                                  or to recover from a desynced restart).
                                  Default is to resume existing state; the
                                  boot banner and log report whether this run
                                  RESUMED or started FRESH.
  --gateway-host TEXT             Gateway gRPC host.  [env var:
                                  ALMANAK_GATEWAY_HOST, GATEWAY_HOST; default:
                                  127.0.0.1]
  --gateway-port INTEGER          Gateway gRPC port.  [env var:
                                  ALMANAK_GATEWAY_PORT, GATEWAY_PORT; default:
                                  50051]
  -n, --network [mainnet|anvil]   Network environment: 'mainnet' for
                                  production RPC, 'anvil' for local fork
                                  testing (auto-starts Anvil on a free port).
                                  For paper trading with PnL tracking, use
                                  'almanak strat backtest paper'. Overrides
                                  config.json 'network' field.
  --simulate-tx / --no-simulate-tx
                                  Enable/disable transaction simulation via
                                  Tenderly/Alchemy before submission. Default:
                                  use SIMULATION_ENABLED env var
  --dashboard-mode [hosted-parity|command-center]
                                  Dashboard layout. 'hosted-parity' (default)
                                  mirrors the hosted platform: one strategy,
                                  one gateway, no multi-strategy navigation.
                                  'command-center' opens the repo-wide
                                  browser. Standalone mode (--dashboard from a
                                  non-strategy folder) always uses Command
                                  Center.
  --dashboard-port INTEGER        Port to run the dashboard on (default:
                                  8501).
  --dashboard                     Launch live dashboard alongside strategy
                                  execution.
  --debug                         Enable debug output (includes Web3/HTTP
                                  logs).
  -v, --verbose                   Enable verbose output (detailed strategy
                                  info).
  --list                          List all available strategies and exit.
  --dry-run                       Execute decide() but don't submit
                                  transactions.
  -i, --interval INTEGER          Loop interval in seconds. Defaults to
                                  [tool.almanak.run].interval or 60.
  --once                          Run single iteration then exit.
  -c, --config PATH               Path to strategy config JSON file.
  -d, --working-dir PATH          Working directory containing the strategy
                                  files. Defaults to the current directory.
  --help                          Show this message and exit.
```

