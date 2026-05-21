# Camelot Swap (Demo)

Camelot swap demo — BUY/SELL lifecycle on Arbitrum (Algebra V3)

## Chain

arbitrum

## Quick Start

```bash
almanak strat demo --name camelot_swap
cd camelot_swap
almanak strat run --network anvil --once
```

## Configuration

Edit `config.json` to adjust strategy parameters. See `strategy.py` for details.

The Camelot connector supports `SWAP` only; LP / collect-fees paths are
fail-closed by design.
