# E2E Strategy Test Report: morpho_aave_arb (Anvil)

**Date:** 2026-02-20 08:29
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | morpho_aave_arb |
| Strategy ID | demo_morpho_aave_arb |
| Chain | ethereum |
| Network | Anvil fork (mainnet) |
| Anvil Port | 8546 (managed gateway spawned its own on 65254) |
| Strategy File | `strategies/incubating/morpho_aave_arb/` |

## Config Changes Made

The following changes were made to `config.json` before the test to satisfy the $50 budget cap and trigger an immediate trade. All changes were reverted after the test.

| Field | Original Value | Test Value | Reason |
|-------|---------------|------------|--------|
| `deploy_amount` | `"0.5"` | `"0.013"` | 0.5 wstETH ~$1,750 exceeds $50 cap; 0.013 wstETH ~$31 at $2,407/wstETH |
| `morpho_apy_override` | `null` | `4.5` | Strategy raises ValueError and returns HOLD when APY unavailable from market |
| `aave_apy_override` | `null` | `3.0` | Same -- market indicators do not provide lending rates on Anvil |
| `force_protocol` | `""` | `"morpho"` | Forces immediate supply to Morpho Blue without APY comparison |

## Execution

### Setup

- Anvil on port 8546 (Ethereum mainnet fork, block 24496873) started externally; managed gateway also started its own Anvil fork on port 65254
- Managed gateway auto-started on port 50052
- Wallet funded via `anvil_funding` config: 100 ETH, 2 wstETH, 10,000 USDC
- Wallet: `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266`

### Strategy Run

- Strategy executed with `--network anvil --once`
- wstETH price fetched from CoinGecko: **$2,407.51**
- APY comparison (via overrides): Morpho=4.50%, Aave=3.00%
- `force_protocol = "morpho"` triggered immediate supply to Morpho Blue
- Intent type: **SUPPLY** -- 0.013 wstETH to Morpho Blue (market ID `0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc`)
- 2 transactions submitted and confirmed

### Transactions

| Step | TX Hash | Gas Used | Status |
|------|---------|----------|--------|
| 1 - Approve (wstETH -> Morpho) | `0x47e9e0bfe64b825224e7da59931d20f356332aa9e17f9bbb5d5fe705dda7e082` | 46,228 | SUCCESS |
| 2 - supplyCollateral | `0x34a1a636e160d451195cb5250514f4e76c6834c348f44ad5822e5e135c461e1b` | 76,407 | SUCCESS |
| **Total** | | **122,635** | |

### Key Log Output

```text
APY comparison: Morpho=4.50%, Aave=3.00% | Active: wallet, Amount: 0
SUPPLY: 0.0130 wstETH to Morpho Blue
Compiled SUPPLY: 0.013 WSTETH to Morpho Blue market 0xb323495f7e4148...
Transaction submitted: tx_hash=47e9e0...e082
Transaction submitted: tx_hash=34a1a6...1e1b
Transaction confirmed: tx_hash=47e9e0...e082, block=24496876, gas_used=46228
Transaction confirmed: tx_hash=34a1a6...1e1b, block=24496877, gas_used=76407
EXECUTED: SUPPLY completed successfully
   Txs: 2 (47e9e0...e082, 34a1a6...1e1b) | 122,635 gas
Supply successful -> active_protocol=morpho_blue, amount=0.013
Status: SUCCESS | Intent: SUPPLY | Gas used: 122635 | Duration: 19341ms
```

## Observations / Minor Issues

1. **APY data unavailable on Anvil**: The strategy raises `ValueError` ("Morpho Blue APY unavailable") when market indicators don't provide lending rates. This means `morpho_apy_override` and `aave_apy_override` are required for Anvil testing without live protocol integrations. This is by design (the strategy intentionally refuses to use hardcoded fallbacks).

2. **Amount chaining warning**: After the supply, the runner logged:
   ```
   Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail
   ```
   This is non-fatal for the initial SUPPLY (single step), but means the rebalancing IntentSequence (withdraw -> supply with `amount="all"`) may not correctly chain amounts. Not a blocker for the initial deployment test.

3. **Gas estimation warning on tx[1]**: The `transferFrom reverted` gas estimation error on the second transaction was handled gracefully -- the compiler-provided gas limit (compiler=88,000) was used instead, and the transaction confirmed successfully.

## Result

**PASS** -- The strategy successfully supplied 0.013 wstETH (~$31.30 at $2,407/wstETH) to Morpho Blue via two confirmed on-chain transactions (approve + supplyCollateral) totalling 122,635 gas on the Ethereum Anvil fork.
