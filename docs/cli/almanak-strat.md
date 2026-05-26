
# almanak strat

Commands for managing strategies.

## Usage

```
Usage: almanak strat [OPTIONS] COMMAND [ARGS]...
```

## Arguments


## Options

* `help`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--help`
    Show this message and exit.


## CLI Help

```
Usage: almanak strat [OPTIONS] COMMAND [ARGS]...

  Commands for managing strategies.

Options:
  --help  Show this message and exit.

Commands:
  backtest     Run backtests for Almanak strategies.
  check        Pre-flight validation for a strategy.
  demo         Browse and copy a demo strategy to get started quickly.
  export       Export strategy data to CSV or JSON.
  list         List all strategies registered with the gateway.
  logs         Show timeline events for a strategy.
  new          Create a new strategy from template.
  pause        Suspend a strategy's iteration loop without closing...
  permissions  Generate a Zodiac Roles permission manifest for a strategy.
  pnl          Per-strategy PnL breakdown from persisted accounting data...
  resume       Resume a previously paused strategy.
  run          Run a strategy from its working directory.
  status       Get detailed status of a strategy.
  teardown     Manage strategy teardowns.
  test         Run a force-action lifecycle test for a strategy on a...
```

