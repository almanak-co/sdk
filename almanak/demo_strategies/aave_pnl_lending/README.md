# Aave Pnl Lending (Demo)

Aave V3 lending strategy for PnL backtesting - supply WETH, borrow USDC on dips

## Chain

arbitrum

## Quick Start

```bash
almanak strat demo --name aave_pnl_lending
cd aave_pnl_lending
almanak strat run --network anvil --once
```

## Configuration

Edit `config.json` to adjust strategy parameters. See `strategy.py` for details.
