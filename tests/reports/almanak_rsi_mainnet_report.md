# E2E Strategy Test Report: almanak_rsi (Mainnet)

**Date:** 2026-02-16 09:47 UTC
**Result:** PASS
**Mode:** Mainnet (live on-chain)
**Chain:** Base
**Duration:** ~22 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | almanak_rsi |
| Chain | Base |
| Network | Mainnet |
| Wallet | 0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF |
| Trading Pair | ALMANAK/USDC |
| Pool Address | 0xbDbC38652D78AF0383322bBc823E06FA108d0874 |
| Protocol | Uniswap V3 |

## Config Changes Made

Modified `strategies/demo/almanak_rsi/config.json` for test:

| Parameter | Original | Test Value | Reason |
|-----------|----------|------------|--------|
| `network` | (not set) | `"mainnet"` | Enable mainnet execution |
| `initial_capital_usdc` | 20 | 1.5 | Stay under $6 budget cap |
| `cooldown_hours` | 1 | 0 | Force immediate trade |

**Config restored** to original values after test.

## Wallet Preparation

### Initial Balance Check (Step 4)

Portfolio across all chains: **$52.51 total**

| Chain | USD Value | Key Assets |
|-------|-----------|------------|
| Ethereum | $27.83 | 2.96 USDC, 0.89 DAI, 0.002 WETH |
| Base | $8.80 | 0.88 USDC, 0.0015 WETH |
| Avalanche | $8.36 | - |
| Arbitrum | $5.47 | - |
| Plasma | $2.05 | - |

### Funding Actions (Step 5)

**Target**: 1.5 USDC on Base for strategy initial capital (0.75 USDC first trade)
**Available on Base**: 0.88 USDC (shortfall: ~0.62 USDC)

**Funding Method**: Cross-chain bridge (Ethereum → Base) via Enso/Stargate

1. **Ethereum USDC → Permit2 approval**:
   - TX: [0xc2a570e3054f8aab441bb9f8309d39ce9598687160efa57fff8eb827de70f5fa](https://etherscan.io/tx/0xc2a570e3054f8aab441bb9f8309d39ce9598687160efa57fff8eb827de70f5fa)
   - Status: SUCCESS
   - Gas: 35,946

2. **Permit2 internal approval for Enso router**:
   - TX: [0x9cc7bc45d1330468ee26431d51e9a95ad98cbf474ad8057cb77f425690f739fd](https://etherscan.io/tx/0x9cc7bc45d1330468ee26431d51e9a95ad98cbf474ad8057cb77f425690f739fd)
   - Status: SUCCESS
   - Gas: 27,918

3. **Cross-chain bridge: 2.9 USDC from Ethereum to Base**:
   - Source TX: [0xc992d45d55607058cc9ac8c5e3fed3534367370c7aa9706acb07c1868276803d](https://etherscan.io/tx/0xc992d45d55607058cc9ac8c5e3fed3534367370c7aa9706acb07c1868276803d)
   - Status: SUCCESS
   - Gas: 394,539
   - Bridge fee: ~0.00003 ETH
   - Expected delivery: 2.898 USDC on Base
   - Delivery time: ~150 seconds via Stargate

4. **Final Base balance after bridge**:
   - USDC: **3.78 USDC** (original 0.88 + bridged 2.9)
   - ETH: 0.000312 ETH (sufficient for gas)
   - **Balance gate: PASS** ✅

## Strategy Execution

Strategy ran with:
```
uv run almanak strat run -d strategies/demo/almanak_rsi --network mainnet --once
```

**Intent executed**: Initialization swap (SWAP)
- **Description**: First-run initialization - buy ALMANAK with half of initial capital
- **Amount in**: 0.75 USDC
- **Amount out**: 350.67 ALMANAK tokens
- **Min amount out** (with 1% slippage): 346.64 ALMANAK
- **Execution price**: $0.00214 per ALMANAK
- **Slippage**: 1.00%

### Key Log Output

```
[info] INITIALIZATION: First run - buying ALMANAK for $0.75 (half of initial capital)
[info] 📈 almanak_rsi intent: 🔄 SWAP: 0.750000 USDC → ALMANAK (slippage: 1.00%) via uniswap_v3
[info] ✅ Compiled SWAP: 0.7500 USDC → 350.1443 ALMANAK (min: 346.6429 ALMANAK)
[info] Transaction submitted: tx_hash=0c47d52d62007397a9d7a56126fe71daf99ed4585377886fa1f9f17ca9474f8b
[info] Transaction confirmed: block=42210353, gas_used=136742
[info] ✅ EXECUTED: SWAP completed successfully
[info] Initialization swap succeeded - strategy is now initialized
[info] Trade executed successfully (total trades: 1)
```

### Gateway Log Highlights

```
Gateway gRPC server started on 127.0.0.1:50051
MarketService initialized with price aggregator
Aggregated price for USDC/USD: 0.999898 (confidence: 1.00, sources: 1/1)
Aggregated price for ALMANAK/USD: 0.00213533 (confidence: 1.00, sources: 1/1)
ExecutionOrchestrator initialized: chain=base, wallet=0x54776446...
Transaction submitted: latency=781.5ms
Transaction confirmed: block=42210353, gas_used=136742
```

## Transactions

| Intent | TX Hash | Explorer Link | Gas Used | Status |
|--------|---------|---------------|----------|--------|
| SWAP (init) | 0x0c47d52d62007397a9d7a56126fe71daf99ed4585377886fa1f9f17ca9474f8b | [BaseScan](https://basescan.org/tx/0x0c47d52d62007397a9d7a56126fe71daf99ed4585377886fa1f9f17ca9474f8b) | 136,742 | SUCCESS ✅ |

## Result

**PASS** - Strategy executed successfully on mainnet.

The almanak_rsi strategy completed its initialization phase by purchasing 350.67 ALMANAK tokens with 0.75 USDC on Uniswap V3 (Base chain). The transaction executed successfully with gas usage of 136,742, staying well within the $6 budget cap. The strategy is now initialized and ready for subsequent RSI-based trading decisions.

---

## PREFLIGHT_CHECKLIST

```yaml
workflow_version: "mainnet_v1"
steps_completed:
  - step: "1_read_config"
    status: "DONE"
    details: "Read config.json and strategy.py, identified Base chain and USDC requirement"
  
  - step: "2_load_env"
    status: "DONE"
    details: "Wallet 0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF, passed Anvil guard check"
  
  - step: "3_kill_gateway"
    status: "DONE"
    details: "Killed processes on ports 50051, 9090"
  
  - step: "3b_clear_state"
    status: "DONE"
    details: "Checked almanak_state.db for stale state (none found)"
  
  - step: "4_check_balances"
    status: "DONE"
    details: "Used DeBank API to check Base (0.88 USDC) and other chains (Ethereum had 2.96 USDC)"
  
  - step: "5_fund_wallet"
    status: "DONE"
    details: "Bridged 2.9 USDC from Ethereum to Base via Enso/Stargate (TX: 0xc992d45d...)"
  
  - step: "5_balance_gate"
    status: "PASS"
    details: "Final balance 3.78 USDC on Base (required: 0.75 USDC)"
  
  - step: "6_start_gateway"
    status: "DONE"
    details: "Auto-started managed gateway on localhost:50051"
  
  - step: "7_run_strategy"
    status: "SUCCESS"
    details: "Executed SWAP intent, TX 0x0c47d52d..., gas 136,742"
  
  - step: "8_cleanup"
    status: "DONE"
    details: "Stopped gateway, restored config.json to original values"

gate_checks:
  - check: "anvil_wallet_guard"
    result: "PASS"
    details: "Wallet is not Anvil default (0xf39Fd...)"
  
  - check: "balance_gate"
    result: "PASS"
    details: "USDC 3.78 >= 0.75 (required), ETH 0.000312 >= 0.0003 (gas)"
  
  - check: "cross_chain_check"
    result: "EXECUTED"
    details: "Checked Ethereum, Base, Avalanche, Arbitrum via DeBank"
  
  - check: "funding_attempt"
    result: "SUCCESS"
    details: "Bridged USDC from Ethereum to Base via Enso"

errors_encountered: []
workflow_skipped: false
```
