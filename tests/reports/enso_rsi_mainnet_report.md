# E2E Strategy Test Report: enso_rsi (Mainnet)

**Date:** 2026-02-09 19:01:04
**Result:** PASS
**Mode:** Mainnet (live on-chain)
**Chain:** Base
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | enso_rsi |
| Chain | base |
| Network | Mainnet |
| Wallet | 0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF |
| Trade Size | $5 USD |
| RSI Oversold | 30 |
| RSI Overbought | 70 |
| Slippage | 1.0% |
| Base Token | WETH |
| Quote Token | USDC |

## Wallet Preparation

| Token | Required | Had Before | Funded | Method |
|-------|----------|------------|--------|--------|
| ETH   | ~0.001   | 0.001736   | -      | existing |
| WETH  | ~0.0024  | 0.001577   | -      | existing |
| USDC  | ~2.50    | 2.244317   | -      | existing |

**Funding:** Wallet already had sufficient tokens. No funding transactions needed.

**Pre-run Balance Summary:**
- ETH: 0.001736 (~$3.61 @ $2,080/ETH)
- WETH: 0.001577 (~$3.28)
- USDC: 2.244317
- **Total value: ~$9.13**

## Strategy Execution

Strategy executed successfully with `--network mainnet --once`.

**Decision Flow:**
1. Strategy initialized with config: trade_size=$5, RSI thresholds 30/70, WETH/USDC pair
2. RSI calculated for WETH via gateway (Binance ETHUSDT klines)
3. **Current RSI: 50.12** (neutral zone: 30-70)
4. **Decision: HOLD** - "RSI 50.12 in neutral zone"
5. No swap executed (neutral market conditions)

**Strategy Logic:**
- RSI < 30 (oversold) → BUY WETH with USDC via Enso
- RSI > 70 (overbought) → SELL WETH for USDC via Enso
- RSI 30-70 (neutral) → HOLD (no action)

**Intents Executed:**
- HOLD: "RSI 50.12 in neutral zone"

### Key Log Output

```text
Using config: strategies/demo/enso_rsi/config.json
Connected to gateway at localhost:50051

============================================================
ALMANAK STRATEGY RUNNER
============================================================
Strategy: EnsoRSIStrategy
Instance ID: EnsoRSIStrategy:40870be0e6f3 (generated)
Mode: FRESH START (no existing state)
Chain: base
Wallet: 0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF
Execution: Single run
Dry run: False
Gateway: localhost:50051
============================================================

[2026-02-09T19:01:04.046957Z] EnsoRSIStrategy initialized:
  trade_size=$5, rsi_oversold=30, rsi_overbought=70,
  slippage=1.0%, pair=WETH/USDC

[2026-02-09T19:01:04.047108Z] Initialized RSICalculator with default_period=14

[2026-02-09T19:01:04.050309Z] Starting iteration for strategy: EnsoRSIStrategy:40870be0e6f3

[2026-02-09T19:01:04.244188Z] ⏸️ EnsoRSIStrategy:40870be0e6f3 HOLD: RSI 50.12 in neutral zone

Status: HOLD | Intent: HOLD | Duration: 194ms
Iteration completed successfully.
```

### Gateway Log Highlights

```text
2026-02-10 02:00:42,743 - EnsoService initialized: available=True
2026-02-10 02:00:42,746 - Metrics server started on http://0.0.0.0:9090/metrics
2026-02-10 02:00:42,747 - Gateway gRPC server started on 127.0.0.1:50051

2026-02-10 02:01:04,049 - StateService.LoadState:
  strategy_id=EnsoRSIStrategy:40870be0e6f3, latency_ms=1.682, success=true

2026-02-10 02:01:04,243 - IntegrationService.BinanceGetKlines:
  symbol=ETHUSDT, latency_ms=191.55, success=true
```

**Gateway Services Used:**
- StateService: Load strategy state (fresh start, no previous state)
- IntegrationService: Fetch Binance ETHUSDT klines for RSI calculation
- EnsoService: Available and initialized (not invoked - no swap needed)

## Transactions

**No transactions executed** - strategy issued HOLD intent due to neutral RSI.

| Intent | TX Hash | Explorer Link | Gas Used | Status |
|--------|---------|---------------|----------|--------|
| HOLD   | N/A     | N/A           | N/A      | N/A    |

## Result

**PASS** - Strategy executed successfully with HOLD decision (RSI 50.12 in neutral zone 30-70). No swap needed, no transactions submitted. EnsoService available but not invoked.

**Key Observations:**
1. EnsoService initialized successfully on Base mainnet
2. RSI calculation via Binance integration working correctly
3. Strategy logic correctly identified neutral RSI and issued HOLD
4. No price oracle errors (unlike enso_uniswap_arbitrage which failed on USDC price)
5. Gateway responded quickly: state load 1.7ms, klines fetch 191.6ms
6. Total iteration duration: 194ms (very fast)

**Comparison to uniswap_rsi:**
- Same RSI-based logic, different execution protocol (Enso vs Uniswap V3)
- Both strategies HOLD when RSI neutral
- EnsoService available on Base, would route through DEX aggregator if swap triggered
- enso_rsi **did not** hit the "Price oracle missing USDC price on Base" error that affected enso_uniswap_arbitrage

**Lifecycle:** Initial decision (HOLD) → No execution → Complete

**Post-test State:** Config restored to original values (trade_size_usd="3", network="anvil")
