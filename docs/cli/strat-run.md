
# strat run

Run a strategy from its working directory.

By default, a managed gateway is auto-started in the background.
Use `--no-gateway` to connect to an existing gateway instead.

## Usage

```
Usage: almanak strat run [OPTIONS]
```

## Prerequisites

- Environment variables: `ALMANAK_PRIVATE_KEY` (RPC_URL recommended; free public RPCs used if nothing is set)
- For anvil mode: Anvil is auto-started by the managed gateway (requires Foundry installed)

## Options

* `working_dir`:
    * Type: `Path`
    * Default: `.`
    * Usage: `--working-dir`, `-d`
    Working directory containing the strategy files. Defaults to the current directory.

* `id`:
    * Type: STRING
    * Default: `None`
    * Usage: `--id`
    Strategy instance ID to resume a previous run.

* `config_file`:
    * Type: `Path`
    * Default: `None`
    * Usage: `--config`, `-c`
    Path to strategy config JSON file. Auto-detected from working directory if not provided (looks for config.json, config.yaml, config.yml).

* `once`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--once`
    Run single iteration then exit.

* `interval`:
    * Type: INT
    * Default: `60`
    * Usage: `--interval`, `-i`
    Loop interval in seconds (default: 60).

* `dry_run`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--dry-run`
    Execute decide() but don't submit transactions.

* `verbose`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--verbose`, `-v`
    Enable verbose output.

* `network`:
    * Type: Choice
    * Choices: `mainnet`, `anvil`
    * Default: `None`
    * Usage: `--network`, `-n`
    Network environment: 'mainnet' for production RPC, 'anvil' for local fork. When using anvil with a managed gateway, Anvil forks are auto-started for the chains specified in the strategy config.

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

* `no_gateway`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--no-gateway`
    Do not auto-start a gateway; connect to an existing one.

* `help`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--help`
    Show this message and exit.


## CLI Help

```
Usage: almanak strat run [OPTIONS]

  Run a strategy from its working directory.

  By default, a managed gateway is auto-started in the background.
  Use --no-gateway to connect to an existing gateway instead.

  Prerequisites:
      - Environment variables: ALMANAK_PRIVATE_KEY (RPC_URL recommended; public RPCs used if unset)
      - For anvil mode: Foundry installed (Anvil is auto-started)

  Examples:

      # Run from strategy directory
      cd strategies/demo/uniswap_rsi
      almanak strat run --once

      # Run with explicit working directory
      almanak strat run -d strategies/demo/uniswap_rsi --once

      # Run continuously
      almanak strat run --interval 30

      # Dry run (no transactions)
      almanak strat run --dry-run --once

      # Resume a previous run
      almanak strat run --id abc123 --once

Options:
  -d, --working-dir PATH  Working directory containing the strategy files.
                           Defaults to the current directory.
  --id TEXT                Strategy instance ID to resume a previous run.
  -c, --config PATH        Path to strategy config JSON file.
  --once                   Run single iteration then exit.
  -i, --interval INTEGER   Loop interval in seconds (default: 60).
  --dry-run                Execute decide() but don't submit transactions.
  -v, --verbose            Enable verbose output.
  -n, --network [mainnet|anvil]
                           Network environment.
  --gateway-host TEXT      Gateway sidecar hostname.
  --gateway-port INTEGER   Gateway sidecar gRPC port.
  --no-gateway             Do not auto-start a gateway; connect to an existing one.
  --help                   Show this message and exit.
```
