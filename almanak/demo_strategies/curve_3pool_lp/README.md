# Curve 3pool LP (Demo)

Tutorial LP strategy - provides 3-coin liquidity to Curve 3pool (DAI/USDC/USDT) on Ethereum.

Unlike the other LP demos (`uniswap_lp`, `traderjoe_lp`), which manage a
2-token pool, this demo exercises Curve's distinctive **3-coin stableswap**
structure: it deposits DAI + USDC + USDT into the canonical Ethereum 3pool
(`0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7`) using the full per-coin
allocation vector `coin_amounts`, and receives a single fungible 3Crv LP token.

## Chain

ethereum

## Quick Start

```bash
almanak strat demo --name curve_3pool_lp
cd curve_3pool_lp
almanak strat run --network anvil
```

## Configuration

Edit `config.json` to adjust strategy parameters. See `strategy.py` for details.

| Key | Meaning |
|---|---|
| `pool` | Curve pool nickname (`"3pool"`) |
| `amount_dai` / `amount_usdc` / `amount_usdt` | Per-coin deposit amounts (one per pool coin index) |
| `min_position_usd` | Minimum total stablecoin inventory (USD) to open a position |
| `force_action` | Force `"open"` or `"close"` for testing |
