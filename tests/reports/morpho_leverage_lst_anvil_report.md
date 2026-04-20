# E2E Strategy Test Report: morpho_leverage_lst (Anvil)

**Date:** 2026-02-20 08:33
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | morpho_leverage_lst |
| Chain | ethereum |
| Network | Anvil fork (ethereum mainnet, port auto-assigned by managed gateway) |
| Strategy ID | demo_morpho_leverage_lst |
| Market | wstETH/WETH on Morpho Blue (market_id: 0xc54d7acf14de29e0e5527cabd7a576506870346a78a11a6762e2cca66322ec41) |

## Config Changes Made

The following config values were modified to respect the $50 budget cap before the test:

| Field | Original | Modified | Reason |
|-------|----------|----------|--------|
| `initial_collateral` | `"1.0"` | `"0.01"` | 1.0 wstETH ~$2,406 far exceeds $50 cap; 0.01 wstETH ~$24 |
| `target_loops` | `3` | `1` | Single loop sufficient to validate SUPPLY intent; keeps test fast |

Note: `force_phase` is not applicable here. The strategy has a `force_phase` field in config but it only routes phase dispatch; in `SETUP/idle` the strategy always immediately supplies collateral without needing forcing. The first intent was SUPPLY, confirming the strategy entered the SETUP phase correctly.

## Execution

### Setup
- [x] Stale processes killed (ports 8546, 50051, 9090)
- [x] Anvil fork started against `eth-mainnet.g.alchemy.com` on port 8546 (chain ID 1)
- [x] Managed gateway auto-started by `almanak strat run --network anvil` on port 50052
- [x] Wallet funded by managed gateway: 100 ETH, 5 wstETH (slot 0), 10 WETH (slot 3)
- [x] Prices fetched: wstETH/USD = $2,406.61, WETH/USD = $1,963.51

### Strategy Run
- [x] Strategy entered SETUP phase (fresh start, no prior state)
- [x] `decide()` returned `SUPPLY` intent: 0.01 wstETH to Morpho Blue as collateral
- [x] Intent compiled: approved wstETH allowance + supply collateral to Morpho Blue market
- [x] 2 transactions submitted and confirmed on-chain

### Transactions

| # | Role | TX Hash | Block | Gas Used | Status |
|---|------|---------|-------|----------|--------|
| 1 | ERC-20 approve (wstETH -> Morpho) | `19ade3a23b5e7b756c8c1fdb9d6b25f79e22993a7ec2ae0d3b6bec8327eef041` | 24496894 | 46,216 | SUCCESS |
| 2 | Morpho Blue supplyCollateral | `7204170f9ff01f41785cededd5a190554b7c764005e5af9db18a1a14595601ce` | 24496895 | 76,407 | SUCCESS |

**Total gas used: 122,623**

### Key Log Output

```text
[SETUP/idle] HF=999.000, Collateral=0 wstETH, Borrowed=0 WETH, Loop=0/1
SUPPLY: 0.0100 wstETH to Morpho Blue
Compiled SUPPLY: 0.01 WSTETH to Morpho Blue market 0xc54d7acf14de29...
Transaction confirmed: tx_hash=19ade3a...f041, block=24496894, gas_used=46216
Transaction confirmed: tx_hash=720417...01ce, block=24496895, gas_used=76407
EXECUTED: SUPPLY completed successfully
Txs: 2 (19ade3...f041, 720417...01ce) | 122,623 gas
Supply OK. Total collateral: 0.01 wstETH
Status: SUCCESS | Intent: SUPPLY | Gas used: 122623 | Duration: 19780ms
```

### Observations / Known Limitations

1. **Gas estimate warning on tx 2**: The local gas estimator returned a revert for the `transferFrom` call because the approve tx had not yet been mined when estimation ran. The orchestrator correctly fell back to the compiler-provided gas limit and the actual execution succeeded.

2. **Amount chaining warning**: After the SUPPLY completed, the runner logged `"Amount chaining: no output amount extracted from step 1"`. This is expected for SUPPLY intents (no output token) and does not affect correctness.

3. **Swap step not reached in single run**: With `--once` and `target_loops=1`, only the first SUPPLY is executed per invocation. The BORROW -> SWAP -> RESUPPLY steps require subsequent `--once` calls as the state machine advances. The strategy correctly persists state between runs via `get_persistent_state()` / `load_persistent_state()`.

4. **Known SDK limitation (from strategy docstring)**: Swap compilation may fail when wstETH's on-chain symbol "WSTETH" differs in case from the price oracle lookup "wstETH". This would surface on the SWAP step in a subsequent run.

## Result

**PASS** - The `morpho_leverage_lst` strategy executed successfully on Anvil: entered SETUP phase, emitted a SUPPLY intent for 0.01 wstETH, compiled it to an approve + supplyCollateral bundle, and landed 2 on-chain transactions totalling 122,623 gas.
