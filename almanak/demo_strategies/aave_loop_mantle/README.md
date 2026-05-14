# Aave Loop Mantle (Demo)

Aave V3 looping strategy on Mantle - recursive supply/borrow for leveraged yield

## Chain

mantle

## Quick Start

```bash
almanak strat demo --name aave_loop_mantle
cd aave_loop_mantle
uv run almanak strat run --network anvil --interval 15
```

## Configuration

Edit `config.json` to adjust strategy parameters. See `strategy.py` for details.
