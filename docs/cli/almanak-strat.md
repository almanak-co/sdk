
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
  demo      Browse and copy a demo strategy to get started quickly.
  describe  Retrieve the details of a strategy.
  list      List all available strategies on the Almanak platform.
  new       Create a new v2 IntentStrategy from template.
  pull      Downloads your strategy.
  push      Push a new version of your strategy.
  run       Run a strategy from its working directory.
```

## almanak strat demo

Browse and copy a working demo strategy into your directory, ready to run.

### Usage

```bash
# Interactive arrow-key selection
almanak strat demo

# Copy a specific strategy by name
almanak strat demo --name uniswap_rsi

# Copy into a specific parent directory
almanak strat demo --name aave_borrow --output-dir ./my-strategies

# List available demo strategies
almanak strat demo --list
```

### Options

* `--name`, `-n`: Demo strategy name (skips interactive selection)
* `--output-dir`, `-o`: Parent directory for the copied strategy folder (default: `.`)
* `--list`: List available demo strategies and exit
