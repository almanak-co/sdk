# TraderJoe V2 LP Lifecycle Strategy

Full LP lifecycle on TraderJoe V2 Liquidity Book (Avalanche): open a bin-based
liquidity position, then close it.

## Purpose

Tests LPCloseIntent on TraderJoe V2's unique mechanics:
- **TraderJoe V2**: approveForAll (LBPair -> Router) + removeLiquidity (2 txs, bin-based ERC1155 tokens)
- **V3 (Uniswap/SushiSwap)**: decreaseLiquidity + collect + burn (3 txs, NFT-based)

## Usage

```bash
# Full lifecycle test (open then close)
almanak strat run -d strategies/incubating/traderjoe_lp_close --network anvil --interval 15

# Test LP_OPEN only (set force_action="open" in config.json)
almanak strat run -d strategies/incubating/traderjoe_lp_close --network anvil --once

# Test LP_CLOSE only (set force_action="close" in config.json, requires prior LP_OPEN)
almanak strat run -d strategies/incubating/traderjoe_lp_close --network anvil --once
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| pool | WAVAX/USDC/20 | Pool identifier (TOKEN_X/TOKEN_Y/BIN_STEP) |
| range_width_pct | 0.10 | Total width of price range (10%) |
| amount_x | 0.1 | Amount of token X (WAVAX) |
| amount_y | 3 | Amount of token Y (USDC) |
| num_bins | 11 | Number of bins for liquidity distribution |
| force_action | lifecycle | "open", "close", "lifecycle", or "" for RSI mode |

## Kitchen Loop Context

- **Iteration**: 13
- **Ticket**: VIB-195
- **Gap Filled**: First test of LPCloseIntent on TraderJoe V2 bin-based model
