
# almanak strat

Commands for managing strategies.

## Usage

```
Usage: almanak strat [OPTIONS] COMMAND [ARGS]...
```

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
  backtest  Backtesting commands (pnl, sweep, paper, etc.)
  describe  Retrieve the details of a strategy.
  list      List all available strategies on the Almanak platform.
  new       Create a new v2 IntentStrategy from template.
  pull      Downloads your strategy.
  push      Push a new version of your strategy.
  run       Run a strategy from its working directory.
```
