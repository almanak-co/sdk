# Anvil Test Report: uniswap_rsi Strategy

**Date:** 2026-02-08 15:53
**Result:** PASS
**Duration:** ~2 minutes (including setup)

---

## Summary

The `uniswap_rsi` demo strategy was successfully tested on an Anvil fork of Ethereum mainnet. The strategy executed correctly, calculated RSI from market data, and returned the appropriate HOLD intent since the RSI value (49.63) was in the neutral zone (40-70).

---

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_uniswap_rsi |
| Chain | ethereum |
| Network | Anvil fork (port 8549) |
| Trade Size | $3 USD |
| RSI Period | 14 |
| RSI Oversold | 40 |
| RSI Overbought | 70 |
| Base Token | WETH |
| Quote Token | USDC |
| Max Slippage | 100 bps (1%) |

---

## Test Phases

### Phase 1: Setup
- [x] Anvil started on port 8549 (Ethereum fork)
- [x] Wallet funded with 100 ETH for gas
- [x] Wallet funded with 10 WETH (wrapped from ETH)
- [x] Wallet funded with 10,000 USDC (via storage slot)
- [x] Gateway started on port 50051

**Wallet Address:** `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266`

**Initial Balances:**
- ETH: 100 (gas)
- WETH: 10.0
- USDC: 10,000

### Phase 2: Strategy Execution
- [x] Strategy loaded successfully: `UniswapRSIStrategy`
- [x] Config loaded from: `strategies/demo/uniswap_rsi/config.json`
- [x] Gateway connection established
- [x] Market data retrieved successfully
- [x] RSI calculated: 49.63
- [x] Decision made: HOLD (neutral zone)

**Strategy Output:**
```
⏸️ demo_uniswap_rsi HOLD: RSI=49.63 in neutral zone [40-70] (hold #1)
Status: HOLD | Intent: HOLD | Duration: 1763ms
```

### Phase 3: Cleanup
- [x] Anvil process killed
- [x] Gateway process killed

---

## Execution Log Highlights

### Strategy Initialization
```
Strategy: UniswapRSIStrategy
Instance ID: demo_uniswap_rsi
Mode: FRESH START (no existing state)
Chain: ethereum
Wallet: 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
Execution: Single run
Dry run: False
Gateway: localhost:50051
```

### Strategy Configuration
```
UniswapRSIStrategy initialized: trade_size=$3, RSI period=14, oversold=40, overbought=70, pair=WETH/USDC
```

### Decision Logic
```
⏸️ demo_uniswap_rsi HOLD: RSI=49.63 in neutral zone [40-70] (hold #1)
```

---

## Validation

### Gateway Integration
- [x] Gateway gRPC connection established successfully
- [x] Market data provider (OHLCVProvider) initialized
- [x] RSI calculator initialized with period=14
- [x] Price data retrieved for WETH
- [x] RSI indicator calculated successfully

### Strategy Behavior
- [x] RSI value: 49.63 (within neutral zone [40-70])
- [x] No trade signal generated (correct behavior)
- [x] HOLD intent returned with clear reason
- [x] No transaction execution attempted

### Token Balances (Post-Run)
No changes expected since strategy returned HOLD intent:
- WETH: 10.0 (unchanged)
- USDC: 10,000 (unchanged)

---

## Decision Logic Validation

The strategy correctly evaluated the RSI-based trading logic:

| Condition | Threshold | Actual Value | Expected Action | Actual Action |
|-----------|-----------|--------------|-----------------|---------------|
| RSI < Oversold | RSI < 40 | RSI = 49.63 | No action | ✅ Correct |
| RSI > Overbought | RSI > 70 | RSI = 49.63 | No action | ✅ Correct |
| Neutral Zone | 40 ≤ RSI ≤ 70 | RSI = 49.63 | HOLD | ✅ Correct |

**Result:** Strategy decision logic is working as expected.

---

## Performance Metrics

| Metric | Value |
|--------|-------|
| Total execution time | 1763ms (~1.8 seconds) |
| Gateway connection | Success |
| Market data fetch | Success |
| RSI calculation | Success |
| Decision latency | < 2 seconds |

---

## Conclusion

**PASS** - The `uniswap_rsi` strategy executed successfully on Anvil fork.

### What Worked
- Anvil fork started and maintained stable connection
- Wallet funding (ETH, WETH, USDC) succeeded
- Gateway started and provided data services
- Strategy loaded configuration correctly
- Market data (prices, RSI) retrieved successfully
- Decision logic executed correctly (HOLD when RSI in neutral zone)
- Clean shutdown of all processes

### Expected Behavior
The strategy returned HOLD because:
- Current RSI (49.63) is between oversold (40) and overbought (70) thresholds
- This is the neutral zone where the strategy waits for better entry/exit signals
- No trade is executed, which is the correct behavior

### Next Steps for Full Testing
To test the complete trading logic, you would need to:
1. Run the strategy over a longer period or with different market conditions
2. Test with RSI < 40 to trigger a buy signal (USDC → WETH)
3. Test with RSI > 70 to trigger a sell signal (WETH → USDC)
4. Verify that swaps are executed correctly on Uniswap V3

### Files Generated
- `/tmp/anvil_ethereum.log` - Anvil fork logs
- `/tmp/gateway.log` - Gateway service logs
- `/tmp/strategy_run.log` - Strategy execution logs
- `/Users/nick/Documents/Almanak/src/almanak-sdk/tests/reports/uniswap_rsi_anvil_report.md` - This report
