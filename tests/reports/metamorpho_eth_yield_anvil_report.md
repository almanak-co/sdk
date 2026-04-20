# E2E Strategy Test Report: metamorpho_eth_yield (Anvil)

**Date:** 2026-03-16 01:17
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | metamorpho_eth_yield |
| Chain | ethereum |
| Network | Anvil fork (Alchemy, Ethereum mainnet) |
| Anvil Port | 53675 (auto-assigned by managed gateway) |
| Vault | Steakhouse USDC (0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB) |

## Config Changes Made

None. The `deposit_amount` of 100 USDC is within the $1000 budget cap. No `force_action` field exists in this strategy -- the strategy triggered a deposit automatically because the funded wallet had sufficient USDC.

## Execution

### Setup
- Anvil auto-started by managed gateway on port 53675 (Ethereum mainnet fork via Alchemy)
- Wallet auto-funded from `anvil_funding` in config.json: 10 ETH + 50,000 USDC
- Wallet: `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266` (Anvil default)

### Strategy Run
- Strategy executed with `--network anvil --once`
- Initial state: `idle` (fresh start, no prior state)
- USDC price: $0.999951 (confidence 1.00, sources 4/4 via Chainlink + Binance + DexScreener + CoinGecko)
- Decision: VAULT_DEPOSIT (100 USDC -- within 80% allocation cap of 50,000 available)

### Intents and Transactions

| Step | Action | TX Hash | Gas Used | Block | Status |
|------|--------|---------|----------|-------|--------|
| 1 | USDC approve (Permit2) | `ec09eee6f62f52e8b69cb0faa46b9ad46eab75fa270868f87ef03e136bea3f82` | 55,570 | 24664633 | SUCCESS |
| 2 | VAULT_DEPOSIT (ERC-4626 deposit) | `3209518a5e70ec2036a508e84b203145e38bb91c263d365a8cc6d339a336c1df` | 357,414 | 24664634 | SUCCESS |

- Total gas used: 412,984
- Shares received: 89,263,004,182,652,118,805 (ERC-4626 vault shares, 18 decimals)
- Assets deposited: 100,000,000 (100 USDC in 6-decimal units)
- Final state: `deposited`

### Key Log Output

```text
Aggregated price for USDC/USD: 0.999951 (confidence: 1.00, sources: 4/4, outliers: 0)
DEPOSIT: 100 USDC into Steakhouse vault (available: 50000, alloc cap: 80%)
Compiled VAULT_DEPOSIT: 100 USDC into vault 0xBEEF0173...
Simulation successful: 2 transaction(s), total gas: 550949
Transaction confirmed: tx=ec09ee...3f82, block=24664633, gas_used=55570
Transaction confirmed: tx=320951...c1df, block=24664634, gas_used=357414
EXECUTED: VAULT_DEPOSIT completed successfully
Txs: 2 (ec09ee...3f82, 320951...c1df) | 412,984 gas
Enriched VAULT_DEPOSIT result with: deposit_data (protocol=metamorpho, chain=ethereum)
Deposit confirmed: assets=100000000, shares=89263004182652118805
VAULT_DEPOSIT successful -> state=deposited
Status: SUCCESS | Intent: VAULT_DEPOSIT | Gas used: 412984 | Duration: 25862ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Placeholder prices | `IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT. This is only acceptable for unit tests.` |
| 2 | strategy | INFO | No CoinGecko API key / fallback to Chainlink | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |

Notes:
- Finding #1 (placeholder prices) is a known WARNING for Anvil mode. The vault deposit does not rely on slippage calculations so this did not affect correctness, but is unsafe on mainnet.
- Finding #2 is informational: USDC was correctly priced at $0.999951 via 4-source aggregation (Chainlink + Binance + DexScreener + CoinGecko). Not a real issue.
- No zero prices, no reverts, no token resolution failures, no timeouts, no API errors.

## Result

**PASS** - The `metamorpho_eth_yield` strategy successfully executed a VAULT_DEPOSIT of 100 USDC into the Steakhouse USDC MetaMorpho vault on an Ethereum Anvil fork, producing 2 confirmed on-chain transactions (USDC approval + ERC-4626 deposit) with 412,984 total gas and transitioning state from `idle` to `deposited`.

SUSPICIOUS_BEHAVIOUR_COUNT: 2
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
