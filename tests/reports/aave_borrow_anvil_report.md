# E2E Strategy Test Report: aave_borrow (Anvil)

**Date:** 2026-03-04 23:43
**Result:** FAIL
**Mode:** Anvil
**Duration:** ~7 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_aave_borrow |
| Chain | arbitrum |
| Network | Anvil fork (managed, public RPC via publicnode.com) |
| Anvil Port | Managed (auto-selected 61293) |

## Config Changes Made

| Field | Before | After (test) | Restored |
|-------|--------|--------------|---------|
| `force_action` | not set | `"supply"` | removed (restored) |

`force_action` was added temporarily to trigger an immediate SUPPLY on the first `--once` run.
Collateral amount: 0.002 WETH (~$4.30 at $2151/ETH). Well under $50 budget cap.

## Execution

### Setup
- [x] Strategy runner auto-started managed gateway on 127.0.0.1:50052
- [x] Managed Anvil fork started on port 61293, forked from Arbitrum via `arbitrum-one-rpc.publicnode.com`
- [x] Wallet auto-funded by managed gateway: 100 ETH, 1 WETH, 10,000 USDC

Note: `.env` in this worktree has an empty `ALCHEMY_API_KEY`. The SDK fell back to the public
`publicnode.com` RPC endpoint. The Alchemy key is present in `.power-env` but not `.env`.

### Strategy Run

The strategy compiled the SUPPLY intent successfully into 3 transactions:

| TX # | Description | Outcome |
|------|-------------|---------|
| TX 1 | ERC20 approve: WETH to Aave V3 pool | Executed (state-setup phase) |
| TX 2 | Aave V3 `supply(WETH, 0.002, wallet, 0)` | FAILED: not in chain after 10s (x4 attempts) |
| TX 3 | Aave V3 `setUserUseReserveAsCollateral(WETH, true)` | Never reached |

TX 2 failed with "Transaction not in chain after 10 seconds" on all 4 attempts (3 retries).
The intent compiled cleanly and wallet balances were confirmed sufficient (1.000 WETH vs 0.002 needed).
This is a simulation/confirmation timeout, not a contract revert.

### Root Cause Analysis

The `LocalSimulator` executes each TX against the managed Anvil fork to build pre/post state
snapshots ("state setup"). TX 2 is skipped for `eth_estimateGas` (marked multi-TX dependent,
uses compiler value 450,000 gas), but the simulator still submits it and waits up to 10 seconds
for the TX to appear in the chain. The managed Anvil fork (using free publicnode.com RPC) is
slow to mine under load, causing TX 2 to time out before being included in a block.

This is the same failure mode documented in the previous aave_borrow Anvil run (2026-03-04 15:14).

**Possible fixes:**
1. Increase `LocalSimulator` state-setup confirmation timeout (10s -> 30s)
2. Use `anvil --block-time 1` to force fast block production on the managed fork
3. Skip state-setup execution for `--network anvil` runs and rely on gas estimation only
4. Set `ALCHEMY_API_KEY` in `.env` (currently empty; only present in `.power-env`)

### Key Log Output

```text
Compiled SUPPLY: 0.0020 WETH to aave_v3 (as collateral)
   Txs: 3 | Gas: 530,000
Simulating 3 transaction(s) via eth_estimateGas
Transaction 2/3: skipping estimation (multi-TX dependent), using compiler gas_limit=450000
WARNING: Failed to execute tx 2 for state setup: Transaction 0xde036... is not in the chain after 10 seconds
WARNING: Execution failed for demo_aave_borrow: ... (retry 0/3)
... [3 more retry cycles, same error]
ERROR: Intent failed after 3 retries: Simulation failed: Transaction 2 execution failed

REVERT DIAGNOSTIC:
  WETH: 1.000000 (need 0.002000)  -- balances sufficient
Likely Cause: Unknown - balances appear sufficient

Status: EXECUTION_FAILED | Intent: SUPPLY | Duration: 99582ms
```

## On-Chain Transactions

No TX hashes produced. Execution failed before any transaction was mined on the fork.

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | ERROR | TX confirmation timeout x4 | `Transaction HexBytes('0x...') is not in the chain after 10 seconds` (4 attempts across 3 retries) |
| 2 | strategy | WARNING | Circular import on startup | `Failed to import strategy strategies.incubating.pendle_pt_swap_arbitrum.strategy: cannot import name 'IntentStrategy' from partially initialized module 'almanak'` |
| 3 | gateway | INFO | No Alchemy key / public RPC | `No API key configured -- using free public RPC for arbitrum (rate limits may apply)` |
| 4 | gateway | INFO | No CoinGecko key (non-blocking) | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback` |

**Finding #1**: Core failure. LocalSimulator 10s timeout too short on public-RPC-backed Anvil fork.

**Finding #2**: Pre-existing issue. The `pendle_pt_swap_arbitrum` incubating strategy has a circular
import that logs a warning every time any strategy is loaded. Non-blocking but noisy.

**Findings #3, #4**: Informational. No zero prices, no token resolution failures, no API errors.
WETH price: $2151.14, USDC price: $0.9999 (both correctly aggregated from 2 sources: Chainlink + CoinGecko free tier).

## Result

**FAIL** - The SUPPLY intent compiled correctly (3 TXs, 530,000 gas estimated) but TX 2 (Aave V3
`supply()`) consistently timed out at the `LocalSimulator` state-setup confirmation phase across
4 attempts (3 retries). This is a repeat of the prior run failure. Root cause is the 10-second TX
confirmation window being too short for the managed Anvil fork when backed by the public
publicnode.com RPC. No on-chain transaction was produced.

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 1
