
# strat teardown

Safely close all positions for a strategy.

The teardown system unwinds positions in a safe order and converts holdings back to stable tokens. Strategies must implement the three teardown methods (`supports_teardown`, `get_open_positions`, `generate_teardown_intents`) for this command to work.

## Usage

```text
Usage: almanak strat teardown execute [OPTIONS]
```

## Prerequisites

- A running gateway (auto-started by default, or use an existing one)
- Environment variables: `ALMANAK_PRIVATE_KEY`
- The strategy must implement teardown methods (see [Implementing Teardown](../api/strategies.md#implementing-teardown))

## Options

* `working_dir`:
    * Type: `Path`
    * Default: `.`
    * Usage: `--working-dir`, `-d`
    Working directory containing the strategy files.

* `config_file`:
    * Type: `Path`
    * Default: `None`
    * Usage: `--config`, `-c`
    Path to strategy config JSON file.

* `mode`:
    * Type: Choice
    * Choices: `graceful`, `emergency`
    * Default: `graceful`
    * Usage: `--mode`, `-m`
    Teardown mode. `graceful` uses normal slippage tolerance; `emergency` accepts higher slippage (3%) for faster exit.

* `preview`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--preview`
    Preview what positions will be closed without executing.

* `force`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--force`, `-f`
    Skip the confirmation prompt.

* `gateway_host`:
    * Type: STRING
    * Default: `localhost`
    * Env: `GATEWAY_HOST`
    * Usage: `--gateway-host`
    Gateway sidecar hostname.

* `gateway_port`:
    * Type: INT
    * Default: `50051`
    * Env: `GATEWAY_PORT`
    * Usage: `--gateway-port`
    Gateway sidecar gRPC port.

## Examples

```bash
# Preview what will be closed (no transactions)
almanak strat teardown execute --preview

# Graceful teardown from strategy directory
cd strategies/demo/uniswap_rsi
almanak strat teardown execute

# Graceful teardown with explicit path
almanak strat teardown execute -d strategies/demo/aerodrome_lp

# Emergency teardown (higher slippage, faster exit)
almanak strat teardown execute -d strategies/demo/aave_borrow --mode emergency

# Skip confirmation prompt
almanak strat teardown execute -d strategies/demo/uniswap_lp --force
```

## How It Works

1. Loads the strategy from its working directory
2. Calls `get_open_positions()` to discover what needs closing
3. Displays a preview of positions and estimated values
4. Calls `generate_teardown_intents(mode)` to build the unwind plan
5. Compiles and executes each intent through the normal execution pipeline

### Position Closing Order

When a strategy holds multiple position types, teardown follows a strict order to avoid liquidation during unwind:

1. **Perps** -- close perpetual positions first (highest risk)
2. **Borrows** -- repay borrows to free collateral
3. **Supplies** -- withdraw supplied collateral
4. **LPs** -- close liquidity positions
5. **Tokens** -- swap remaining tokens to stable

## CLI Help

```text
Usage: almanak strat teardown execute [OPTIONS]

  Execute teardown directly from a strategy working directory.

  This command loads a strategy from its working directory and immediately
  executes a teardown to close all open positions. The gateway must be running.

  Examples:

      # Preview what will be closed
      almanak strat teardown execute -d strategies/demo/aerodrome_lp --preview

      # Execute graceful teardown
      almanak strat teardown execute -d strategies/demo/aerodrome_lp

      # Emergency teardown (faster, accepts higher slippage)
      almanak strat teardown execute -d strategies/demo/aave_borrow --mode emergency

      # Skip confirmation
      almanak strat teardown execute -d strategies/demo/uniswap_lp --force

Options:
  -d, --working-dir PATH           Working directory containing the strategy files.
  -c, --config PATH                Path to strategy config JSON file.
  -m, --mode [graceful|emergency]  Teardown mode (default: graceful).
  --preview                        Preview teardown without executing.
  -f, --force                      Skip confirmation prompt.
  --gateway-host TEXT               Gateway sidecar hostname.
  --gateway-port INTEGER            Gateway sidecar gRPC port.
  --help                            Show this message and exit.
```
