# Anvil Test Report: enso_uniswap_arbitrage

**Date:** 2026-02-08 14:59 PST
**Result:** PARTIAL - First swap executed successfully, sequence not fully supported
**Duration:** ~10 minutes

---

## Summary

The `enso_uniswap_arbitrage` strategy executed its first swap successfully via the Enso DEX aggregator. However, the current single-chain orchestrator only executes the first intent in a sequence, so the second swap (via Uniswap V3) was not executed. This is expected behavior for the current runner implementation.

**Key Finding**: Intent sequences require multi-chain orchestrator support for full execution.

---

## Configuration

| Field | Value |
|-------|-------|
| Strategy | enso_uniswap_arbitrage |
| Chain | Base (8453) |
| Network | Anvil fork |
| Port | 8548 |
| Mode | buy_enso_sell_uniswap |
| Trade Size | $0.40 USD |
| Max Slippage | 1.0% |
| Token Pair | WETH/USDC |

---

## Test Phases

### Phase 1: Setup
- [x] Anvil started on port 8548 (Base fork)
- [x] Gateway started on port 50051
- [x] Wallet funded: 10,000 USDC, 10 WETH, 100 ETH for gas

**Funding Method**: Whale transfer from `0x4e65fE4DbA92790696d040ac24Aa414708F5c0AB`

### Phase 2: Strategy Execution
- [x] Strategy loaded successfully
- [x] Intent sequence created (2 swaps)
- [x] First swap executed: USDC → WETH via Enso
- [ ] Second swap NOT executed (orchestrator limitation)

**Execution Details**:
- Intent 1: Swap $0.40 USDC → WETH via Enso (SUCCESS)
- Intent 2: Swap ALL WETH → USDC via Uniswap V3 (NOT EXECUTED)

### Phase 3: Results Verification
- [x] Transaction confirmed on-chain
- [x] Balances updated correctly
- [x] Gas usage reasonable

---

## Execution Log Highlights

### Strategy Initialization
```
EnsoUniswapArbitrageStrategy initialized:
  trade_size=$0.4, slippage=1.0%, pair=WETH/USDC, mode=buy_enso_sell_uniswap
```

### Intent Sequence Creation
```
📈 demo_enso_uniswap_arbitrage intent sequence (2 steps):
   1. 🔄 SWAP: $0.40 USDC → WETH (slippage: 1.00%) via enso
   2. 🔄 SWAP: ALL WETH → USDC (slippage: 1.00%) via uniswap_v3

Note: decide() returned 2 intents but single-chain orchestrator only
      executes the first. Use multi-chain config for full support.
```

### Enso Route Discovery
```
Getting Enso route: USDC -> WETH, amount=400000
EnsoClient initialized for chain=base (chain_id=8453)
Route found: 0x833589fC... -> 0x42000000...,
  amount_out=188644957609264, price_impact=0bp
```

### Compilation
```
✅ Compiled SWAP (Enso): 0.4000 USDC → 0.0002 WETH (min: 0.0002 WETH)
   Slippage: 1.00% | Impact: N/A | Txs: 2 | Gas: 708,195
```

### Execution Success
```
Execution successful for demo_enso_uniswap_arbitrage:
  gas_used=569893, tx_count=2
Status: SUCCESS | Intent: SWAP | Gas used: 569893 | Duration: 9983ms
```

---

## Transaction Details

| Phase | Intent | Protocol | Gas Estimated | Gas Used | Status |
|-------|--------|----------|---------------|----------|--------|
| Execute | SWAP (step 1) | Enso | 708,195 | 569,893 | ✅ SUCCESS |
| Execute | SWAP (step 2) | Uniswap V3 | - | - | ⏭️ SKIPPED |

**Gas Efficiency**: Used 19.5% less gas than estimated (569,893 vs 708,195)

---

## Balance Verification

### Initial Balances
```
USDC: 10,000.0000 USDC
WETH: 10.0000 WETH
ETH:  ~90 ETH (for gas)
```

### Final Balances
```
USDC: 9,999.6000 USDC  (Δ -0.4000)
WETH: 10.000189 WETH   (Δ +0.000189)
```

### Balance Change Analysis
- USDC spent: 0.4000 USDC (exact match to trade_size_usd)
- WETH received: 0.000189 WETH (~$0.40 at Base WETH prices)
- Net trade: $0.40 USDC → $0.40 WETH equivalent
- Price impact: 0 basis points (per Enso routing)

---

## Known Limitations

### 1. Intent Sequence Support
**Issue**: Single-chain orchestrator only executes first intent in sequence

**Workaround**: Strategy would need to be run multiple times, or use multi-chain orchestrator

**Impact**: Arbitrage cannot complete in single run (no round-trip)

### 2. Sequence vs Single Intent
**Design**: Strategy uses `Intent.sequence()` which returns `IntentSequence`

**Runner Behavior**: Current runner expects single Intent, executes only first

**Solution Needed**: Enhanced orchestrator to handle sequences atomically

---

## Test Conclusions

### What Worked ✅
1. Strategy loaded and initialized correctly
2. Enso DEX aggregator integration functional
3. Route discovery and price calculation working
4. Intent compilation successful (2 transactions)
5. Transaction execution and confirmation working
6. Balance updates accurate
7. Gas estimation and usage within expected range
8. Error-free execution (no exceptions or reverts)

### What Didn't Work ❌
1. Second swap not executed (orchestrator limitation)
2. Full arbitrage cycle not completed
3. Intent sequence not fully supported

### Is This a Strategy Bug? ❌
No, this is **NOT a bug in the strategy**. The strategy code is correct and follows the documented `Intent.sequence()` pattern. The limitation is in the runner/orchestrator, which currently only supports single-intent execution.

---

## Recommendations

### For Framework Development
1. **Priority: Intent Sequence Support**
   - Implement sequential execution in orchestrator
   - Execute all intents in sequence before completion
   - Handle amount="all" chaining between steps

2. **Runner Enhancement**
   - Detect `IntentSequence` vs single `Intent`
   - Execute all steps atomically
   - Support dependent step execution

3. **Documentation**
   - Clarify sequence support status
   - Document workarounds for multi-step strategies

### For Strategy Users
1. **Current Workaround**: Run strategy multiple times to execute all steps
2. **Alternative**: Use single-intent strategies until sequence support is added
3. **Testing**: Always verify full execution on Anvil before mainnet

---

## Technical Details

### Environment
- **Anvil Fork**: Base mainnet (via Alchemy)
- **Block Number**: 41,886,658+ (Base mainnet state)
- **Gateway**: gRPC on localhost:50051
- **RPC**: http://127.0.0.1:8548

### Token Contracts (Base)
- **USDC**: `0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913` (6 decimals)
- **WETH**: `0x4200000000000000000000000000000000000006` (18 decimals)

### Wallet
- **Address**: `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266`
- **Private Key**: Anvil default test key (local only)

---

## Final Assessment

**Result Code**: PARTIAL

**Reason**: Strategy code is correct and first intent executed successfully. The limitation is in the runner/orchestrator, not the strategy. The test validates that:
- Enso integration works on Base
- Intent compilation works for sequences
- First step executes correctly
- Balances are tracked accurately

**Next Steps**:
1. Enhance orchestrator to support IntentSequence
2. Re-test after orchestrator update
3. Validate full arbitrage cycle completion

---

## Appendix: Full Execution Log

```
Using config: strategies/demo/enso_uniswap_arbitrage/config.json
Connecting to gateway at localhost:50051...
Connected to gateway at localhost:50051

Loaded strategy: EnsoUniswapArbitrageStrategy
Network: ANVIL (local fork at http://127.0.0.1:8545)

Strategy: EnsoUniswapArbitrageStrategy
Instance ID: demo_enso_uniswap_arbitrage
Mode: FRESH START (no existing state)
Chain: base
Wallet: 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
Execution: Single run
Dry run: False

Starting iteration...
Executing buy_enso_sell_uniswap arbitrage: USDC -> WETH -> USDC
🔄 ARB SEQUENCE: Buy $0.40 WETH via Enso → Sell on Uniswap V3

Intent sequence (2 steps):
   1. SWAP: $0.40 USDC → WETH (slippage: 1.00%) via enso
   2. SWAP: ALL WETH → USDC (slippage: 1.00%) via uniswap_v3

Note: single-chain orchestrator only executes the first.

Getting Enso route: USDC -> WETH, amount=400000
Route found: amount_out=188644957609264, price_impact=0bp
✅ Compiled SWAP (Enso): 0.4000 USDC → 0.0002 WETH
   Gas: 708,195

Execution successful: gas_used=569893, tx_count=2
Status: SUCCESS | Duration: 9983ms

Iteration completed successfully.
```

---

**Report Generated**: 2026-02-08 15:00 PST
**Tested By**: Claude Code Strategy Tester Agent
**Test Framework**: Almanak SDK v2 on Anvil
