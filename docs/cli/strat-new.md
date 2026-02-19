
# strat new

Create a new v2 IntentStrategy from template.

This creates a strategy using the v2 intent-based framework with:
- strategy.py: Main strategy with decide() method
- config.py: Configuration dataclass
- tests/: Test scaffolding

## Usage

```
Usage: almanak strat new [OPTIONS]
```

## Options

* `name`:
    * Type: STRING
    * Default: `None`
    * Usage: `--name`, `-n`
    Name for the new strategy. If not provided, will prompt interactively.

* `working_dir`:
    * Type: `Path`
    * Default: `None`
    * Usage: `--working-dir`, `-o`
    Output directory for the new strategy. Defaults to current directory.

* `template`:
    * Type: Choice
    * Choices: `blank`, `dynamic_lp`, `mean_reversion`, `basis_trade`, `lending_loop`
    * Default: `blank`
    * Usage: `--template`, `-t`
    Strategy template to use.

* `chain`:
    * Type: Choice
    * Choices: `ethereum`, `arbitrum`, `optimism`, `polygon`, `base`, `avalanche`
    * Default: `arbitrum`
    * Usage: `--chain`, `-c`
    Target blockchain network.

* `help`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--help`
    Show this message and exit.

## Templates

| Template | Description | Protocol |
|----------|-------------|----------|
| `blank` | Minimal template for custom implementations | custom |
| `mean_reversion` | RSI-based trading strategy | Uniswap V3 |
| `dynamic_lp` | Volatility-based LP strategy | Uniswap V3 |
| `basis_trade` | Spot+perp funding arbitrage | GMX V2 |
| `lending_loop` | Aave/Morpho leverage looping | Aave V3 |

## CLI Help

```
Usage: almanak strat new [OPTIONS]

  Create a new v2 IntentStrategy from template.

  This creates a strategy using the v2 intent-based framework with:
  - strategy.py: Main strategy with decide() method
  - config.py: Configuration dataclass
  - tests/: Test scaffolding

  Templates:
  - blank: Minimal strategy for custom implementations
  - dynamic_lp: Volatility-based LP strategy
  - mean_reversion: RSI-based trading strategy
  - basis_trade: Spot+perp funding arbitrage
  - lending_loop: Aave/Morpho leverage looping

Options:
  -n, --name TEXT         Name for the new strategy.
  -o, --working-dir PATH  Output directory for the new strategy.
  -t, --template [blank|dynamic_lp|mean_reversion|basis_trade|lending_loop]
                           Strategy template to use (default: blank).
  -c, --chain [ethereum|arbitrum|optimism|polygon|base|avalanche]
                           Target blockchain network (default: arbitrum).
  --help                   Show this message and exit.
```

## Examples

```bash
# Interactive (prompts for name)
almanak strat new

# Specify all options
almanak strat new --name my_strategy --template mean_reversion --chain arbitrum

# Short form
almanak strat new -n my_lp -t dynamic_lp -c ethereum -o ./strategies/my_lp
```
