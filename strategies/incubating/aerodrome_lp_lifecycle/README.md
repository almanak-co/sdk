# Aerodrome LP Lifecycle Strategy

Full LP lifecycle on Aerodrome (Base): open a liquidity position, then close it.

## Purpose

Tests LPCloseIntent on Aerodrome's Solidly-style fungible LP tokens. Unlike V3 protocols (Uniswap, SushiSwap) that use NFT position managers, Aerodrome's pool contract IS the LP token (ERC-20). This means LP_CLOSE follows a different path:

- **Aerodrome**: approve LP token for router + `removeLiquidity` (2 txs)
- **V3 (Uniswap/SushiSwap)**: `decreaseLiquidity` + `collect` + `burn` (3 txs, NFT-based)

## Usage

```bash
# Full lifecycle test (open then close)
# Uses --interval so decide() runs twice: open on first, close on second
almanak strat run -d strategies/incubating/aerodrome_lp_lifecycle --network anvil --interval 15

# Test LP_OPEN only (set force_action="open" in config.json)
almanak strat run -d strategies/incubating/aerodrome_lp_lifecycle --network anvil --once

# Test LP_CLOSE only (set force_action="close" in config.json)
# Requires LP tokens already in wallet
almanak strat run -d strategies/incubating/aerodrome_lp_lifecycle --network anvil --once
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| chain | base | Target chain |
| pool | WETH/USDC | Token pair |
| stable | false | Pool type (volatile/stable) |
| amount0 | 0.001 | WETH amount for LP |
| amount1 | 2 | USDC amount for LP |
| force_action | lifecycle | Testing mode: open, close, lifecycle, or empty for RSI |

## Kitchen Loop

- **Iteration**: 13
- **Source**: VIB-167 (Linear ticket)
- **Gap filled**: First LPCloseIntent test on Aerodrome (fungible LP tokens)
