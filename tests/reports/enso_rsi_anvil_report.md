# Anvil Test Report: enso_rsi

**Date:** 2026-02-08 14:55 UTC
**Result:** PASS
**Duration:** ~90 seconds

---

## Summary

The `enso_rsi` demo strategy was successfully tested on an Anvil fork of Base chain. The strategy correctly:
1. Connected to the Gateway with Enso service enabled
2. Evaluated market conditions and returned HOLD when RSI was in neutral zone (51.21)
3. Successfully executed a forced BUY swap using Enso aggregator
4. Compiled the swap intent into transactions (APPROVE + SWAP)
5. Executed both transactions successfully on Anvil
6. Verified correct token balance changes

---

## Configuration

| Field | Value |
|-------|-------|
| Strategy | enso_rsi |
| Chain | Base (8453) |
| Network | Anvil fork |
| Port | 8548 |
| Protocol | Enso (DEX aggregator) |
| Wallet | 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 |

---

## Test Phases

### Phase 1: Setup
- [x] Anvil started on port 8548 (Base fork)
- [x] Gateway started on port 50051 with Enso service
- [x] Wallet funded: 100 ETH, 10,000 USDC, 1 WETH

### Phase 2: Initial Strategy Run (HOLD Test)
- [x] Strategy loaded successfully
- [x] RSI calculated: 51.21 (neutral zone)
- [x] Strategy returned HOLD intent
- [x] Duration: 475ms

### Phase 3: Forced Buy Test (Swap Execution)
- [x] Config modified with `force_action: "buy"`
- [x] Strategy created BUY intent
- [x] Intent compiled successfully (APPROVE + SWAP via Enso)
- [x] Enso route found: USDC -> WETH
- [x] Both transactions executed successfully
- [x] Gas used: 760,771
- [x] Duration: 13,489ms

### Phase 4: Verification
- [x] USDC balance: 9,999,960,000 (10,000 - 0.04 = 9,999.96 USDC) ✅
- [x] WETH balance: 1,000,019,435,665,352,447 (~1.0000194 WETH) ✅
- [x] Original config restored

---

## Execution Log Highlights

### Initial Run (HOLD)
```
[2026-02-08T14:54:59.637377Z] [info] Starting iteration for strategy: EnsoRSIStrategy:2455127c8193
[2026-02-08T14:55:00.111968Z] [info] ⏸️ EnsoRSIStrategy:2455127c8193 HOLD: RSI 51.21 in neutral zone
Status: HOLD | Intent: HOLD | Duration: 475ms
```

### Forced Buy Run (SWAP)
```
[2026-02-08T14:55:24.317118Z] [info] Force action requested: buy
[2026-02-08T14:55:24.317158Z] [info] 🔄 BUY via Enso: $0.04 USDC → WETH, slippage=1.0%
[2026-02-08T14:55:24.317255Z] [info] 📈 EnsoRSIStrategy:20b5ef4dfb23 intent: 🔄 SWAP: $0.04 USDC → WETH (slippage: 1.00%) via enso
[2026-02-08T14:55:24.326411Z] [info] Getting Enso route: USDC -> WETH, amount=40000
[2026-02-08T14:55:27.217912Z] [info] Route found: 0x833589fC... -> 0x42000000..., amount_out=19435665352447, price_impact=0bp
[2026-02-08T14:55:27.277727Z] [info] ✅ Compiled SWAP (Enso): 0.0400 USDC → 0.00001944 WETH (min: 0.00001924 WETH)
[2026-02-08T14:55:27.277858Z] [info]    Slippage: 1.00% | Impact: N/A | Txs: 2 | Gas: 957,871
[2026-02-08T14:55:37.805528Z] [info] Execution successful for EnsoRSIStrategy:20b5ef4dfb23: gas_used=760771, tx_count=2
Status: SUCCESS | Intent: SWAP | Gas used: 760771 | Duration: 13489ms
```

---

## Transactions

| Phase | Intent | Gas Used | Status |
|-------|--------|----------|--------|
| HOLD Test | HOLD | 0 | ✅ |
| Swap Test | APPROVE | ~196,900 | ✅ |
| Swap Test | SWAP (Enso) | ~563,871 | ✅ |
| **Total** | - | **760,771** | ✅ |

---

## Enso Integration Details

### Route Details
- **From token:** USDC (0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913)
- **To token:** WETH (0x4200000000000000000000000000000000000006)
- **Amount in:** 40,000 (0.04 USDC with 6 decimals)
- **Amount out:** 19,435,665,352,447 (~0.00001944 WETH)
- **Minimum out:** 19,244,308,698,923 (~0.00001924 WETH)
- **Slippage:** 1.00%
- **Price impact:** 0 basis points
- **Route compilation time:** 2.89 seconds

### Enso Service Status
- Enso API key configured: ✅
- Enso service available in Gateway: ✅
- Route finding successful: ✅
- Transaction calldata generated: ✅

---

## Final Verification

### Token Balances

**Before:**
- ETH: 100 ETH (gas)
- USDC: 10,000.00 USDC
- WETH: 1.00000000 WETH

**After:**
- USDC: 9,999.96 USDC (spent 0.04 USDC)
- WETH: 1.00001944 WETH (gained ~0.00001944 WETH)

**Delta:**
- USDC: -0.04 USDC ✅ (exactly as configured in trade_size_usd)
- WETH: +0.00001944 WETH ✅ (received from Enso swap)

---

## Conclusion

**PASS** - The `enso_rsi` strategy successfully completed all test phases:

1. ✅ Strategy initialization with correct configuration
2. ✅ Gateway connection with Enso service enabled
3. ✅ RSI calculation and HOLD logic working correctly
4. ✅ Forced buy action triggering swap intent
5. ✅ Enso route compilation and transaction generation
6. ✅ Successful on-chain execution (APPROVE + SWAP)
7. ✅ Correct token balance changes verified
8. ✅ Original configuration restored

**Key Features Verified:**
- Enso DEX aggregator integration working correctly
- Intent-based architecture (SwapIntent -> ActionBundle -> Transactions)
- Gateway-backed execution pipeline
- RSI indicator calculation via gateway
- Token resolution for Base chain (WETH, USDC)
- Slippage protection and minimum output calculation
- Multi-transaction compilation (approve + swap)

**No Issues Detected.**

---

## Recommendations

1. Consider increasing `trade_size_usd` for production (current: $0.04 is very small)
2. The strategy correctly handles neutral RSI zones with HOLD logic
3. Enso aggregator is working as expected with proper route finding
4. Gas usage is reasonable for a simple swap (760,771 gas total)

---

## Appendix: Commands Used

```bash
# Start Anvil (Base fork)
anvil -f https://base-mainnet.g.alchemy.com/v2/$ALCHEMY_API_KEY --port 8548

# Fund wallet
cast rpc anvil_setBalance $WALLET 0x56BC75E2D63100000 --rpc-url http://127.0.0.1:8548
cast send $WETH --value 1000000000000000000 --from $WALLET --private-key $PRIVKEY --rpc-url http://127.0.0.1:8548

# Start Gateway
ALMANAK_GATEWAY_NETWORK=anvil \
ALMANAK_GATEWAY_ALLOW_INSECURE=true \
ALMANAK_GATEWAY_PRIVATE_KEY=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
ALMANAK_BASE_RPC_URL=http://127.0.0.1:8548 \
ENSO_API_KEY=$ENSO_API_KEY \
uv run almanak gateway

# Run strategy
uv run almanak strat run -d strategies/demo/enso_rsi --once

# Verify balances
cast call $USDC "balanceOf(address)(uint256)" $WALLET --rpc-url http://127.0.0.1:8548
cast call $WETH "balanceOf(address)(uint256)" $WALLET --rpc-url http://127.0.0.1:8548
```
