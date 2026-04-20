# Pendle Basics Strategy - Integration Notes

## Overview

This strategy demonstrates Pendle Protocol integration on Arbitrum, swapping wstETH for PT-wstETH (Principal Token) to lock in a fixed yield until market maturity.

## Market Configuration

| Item | Value |
|------|-------|
| Chain | Arbitrum |
| Market | PT-wstETH-25JUN2026 |
| Market Address | 0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b |
| Base Token | wstETH |
| PT Token | PT-wstETH |
| Maturity | June 25, 2026 |

## Key Learnings

### 1. Token Decimals Matter

Always use `get_token_resolver()` for token decimals. Never default to 18.

### 2. Yield-Bearing Token Markets Require Special Handling

The Pendle Router's `swapExactTokenForPtSimple` function has a `TokenInput` struct:
```
(tokenIn, netTokenIn, tokenMintSy, pendleSwap, swapData)
```

- `tokenIn`: The token the user provides
- `tokenMintSy`: The token that actually mints SY (must match market's SY contract)
- `pendleSwap`: Address of contract to swap tokenIn -> tokenMintSy (or address(0) for direct)

For the wstETH market, wstETH is the tokenMintSy, so no external routing is needed.

### 3. Market Expiry

Pendle markets expire. When a market expires, the demo strategy breaks. Always use a market
with expiry well into the future. Current market expires June 25, 2026.

## Running the Strategy

```bash
# Auto-starts gateway + Anvil fork
almanak strat run -d strategies/demo/pendle_basics --network anvil --once
```

## Common Errors

### "ERC20: transfer from the zero address"
**Cause**: `tokenMintSy` in TokenInput doesn't match what the SY contract expects
**Solution**: Use the correct yield-bearing token for the market

### Amount too large (e.g., 1e18 instead of 1e6)
**Cause**: Token decimals not configured
**Solution**: Add token to the token resolver registry
