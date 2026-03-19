# Pendle Basics Strategy - Integration Notes

## Overview

This strategy demonstrates Pendle Protocol integration on Plasma chain, swapping yield-bearing tokens for Principal Tokens (PT).

## Key Learnings

### 1. Token Decimals Matter

USDT0 and FUSDT0 on Plasma have **6 decimals**, not 18. The compiler's `_get_token_decimals()` method defaults to 18 for unknown tokens, which caused amount calculation errors.

**Fix**: Added to `compiler.py` decimals_map:
```python
"usdt0": 6,
"fusdt0": 6,
```

### 2. Yield-Bearing Token Markets Require Special Handling

For the PT-fUSDT0 market:
- **SY (Standardized Yield)** is minted from **FUSDT0** (Fluid USDT0), NOT from USDT0
- The Pendle Router's `swapExactTokenForPtSimple` function has a `TokenInput` struct with a `tokenMintSy` field
- If `tokenMintSy` doesn't match what the SY contract expects, the transaction reverts with "ERC20: transfer from the zero address"

**TokenInput struct:**
```
(tokenIn, netTokenIn, tokenMintSy, pendleSwap, swapData)
```

- `tokenIn`: The token the user provides
- `tokenMintSy`: The token that actually mints SY (must match market's SY contract)
- `pendleSwap`: Address of contract to swap tokenIn -> tokenMintSy (or address(0) for direct)

### 3. Direct vs Routed Swaps

**Direct swap (what works now):**
- Use FUSDT0 as input token
- FUSDT0 directly mints SY
- Simple flow: FUSDT0 -> SY -> PT

**Routed swap (TODO for USDT0 input):**
- Would need to set up `pendleSwap` to convert USDT0 -> FUSDT0 via Fluid protocol
- Or integrate with Pendle's internal routing if available

### 4. Plasma Chain Configuration

| Item | Value |
|------|-------|
| Chain ID | 9745 |
| Anvil Port | 8554 |
| RPC | http://127.0.0.1:8554 |
| Pendle Router | 0x888888888889758F76e7103c6CbF23ABbF58F946 |

### 5. Token Addresses (Plasma)

| Token | Address | Decimals | Storage Slot |
|-------|---------|----------|--------------|
| USDT0 | 0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb | 6 | 51 |
| FUSDT0 | 0x1dd4b13fcae900c60a350589be8052959d2ed27b | 6 | 0 |
| PT-fUSDT0 | 0xbe45f6f17b81571fc30253bdae0a2a6f7b04d60f | 6 | - |
| SY-fUSDT0 | 0xff3ccc1245d59b21b6ec4a597557e748f8311e8c | - | - |
| Market | 0x0cb289e9df2d0dcfe13732638c89655fb80c2be2 | - | - |

### 6. Gateway Configuration

The gateway must use the same private key as the strategy:

```bash
ALMANAK_GATEWAY_PRIVATE_KEY=0x... \
ALMANAK_GATEWAY_NETWORK=anvil \
ALMANAK_GATEWAY_ALLOW_INSECURE=true \
uv run almanak gateway
```

If the gateway's signer address doesn't match the strategy's wallet address, you'll get:
```
SigningError: Transaction from_address (0x...) does not match signer address (0x...)
```

### 7. Anvil Wallet Funding

Use `cast index` to calculate storage slots for ERC20 balanceOf mappings:

```bash
# Fund FUSDT0 (slot 0)
SLOT=$(cast index address "$WALLET" 0)
cast rpc anvil_setStorageAt "$TOKEN" "$SLOT" "0x$(printf '%064x' $AMOUNT)" --rpc-url $RPC

# Fund USDT0 (slot 51)
SLOT=$(cast index address "$WALLET" 51)
cast rpc anvil_setStorageAt "$TOKEN" "$SLOT" "0x$(printf '%064x' $AMOUNT)" --rpc-url $RPC
```

## Common Errors and Solutions

### "ERC20: transfer from the zero address"
**Cause**: `tokenMintSy` in TokenInput doesn't match what the SY contract expects
**Solution**: Use the correct yield-bearing token (FUSDT0 for fUSDT0 market)

### "ERC20: transfer amount exceeds balance"
**Cause**: Wallet doesn't have enough of the input token
**Solution**: Fund wallet with correct token using storage slot manipulation

### "Transaction from_address does not match signer address"
**Cause**: Gateway private key differs from strategy's expected wallet
**Solution**: Set `ALMANAK_GATEWAY_PRIVATE_KEY` to match

### Amount too large (e.g., 1e18 instead of 1e6)
**Cause**: Token decimals not configured, defaulting to 18
**Solution**: Add token to `_get_token_decimals()` in compiler.py

## Files Modified for Integration

1. **`almanak/framework/intents/compiler.py`**
   - Added Plasma tokens to `TOKEN_ADDRESSES`
   - Added USDT0/FUSDT0 to decimals map
   - Added `token_mint_sy` lookup for Pendle markets

2. **`almanak/framework/connectors/pendle/sdk.py`**
   - Added `MARKET_TOKEN_MINT_SY` mapping
   - Added `token_mint_sy` parameter to `build_swap_exact_token_for_pt()`

3. **`almanak/framework/connectors/pendle/adapter.py`**
   - Added `token_mint_sy` to `PendleSwapParams`
   - Pass through to SDK

4. **`almanak/framework/execution/orchestrator.py`**
   - Added Plasma chain ID (9745)

## Running the Strategy

```bash
# Terminal 1: Start Anvil fork
anvil --fork-url https://rpc.plasma.to --port 8554 --chain-id 9745

# Terminal 2: Fund wallet
# Set WALLET to the address derived from your private key
WALLET=$YOUR_WALLET_ADDRESS
FUSDT0=0x1dd4b13fcae900c60a350589be8052959d2ed27b
SLOT=$(cast index address "$WALLET" 0)
cast rpc anvil_setStorageAt "$FUSDT0" "$SLOT" "0x$(printf '%064x' 10000000)" --rpc-url http://127.0.0.1:8554

# Terminal 3: Start gateway
# Use the private key that matches WALLET address above
ALMANAK_GATEWAY_PRIVATE_KEY=$YOUR_PRIVATE_KEY \
ALMANAK_GATEWAY_NETWORK=anvil \
ALMANAK_GATEWAY_ALLOW_INSECURE=true \
uv run almanak gateway

# Terminal 4: Run strategy
cd strategies/demo/pendle_basics
uv run almanak strat run --once
```

## Successful Execution Output

```
Status: SUCCESS | Intent: SWAP | Gas used: 354101 | Duration: 624ms

On-chain verification:
- FUSDT0: 9,000,000 (swapped 1 of 10)
- PT-fUSDT0: 1,015,967 (~1.016 received at yield discount)
```

## Future Improvements

1. **USDT0 -> PT-fUSDT0 route**: Set up `pendleSwap` to convert USDT0 -> FUSDT0 via Fluid
2. **Price oracle**: Add USDT0/FUSDT0 price support (currently fails CoinGecko lookup)
3. **PT pricing**: Integrate Pendle API for accurate PT quotes and min output calculation
4. **Multi-market support**: Extend `MARKET_TOKEN_MINT_SY` for other yield-bearing markets
