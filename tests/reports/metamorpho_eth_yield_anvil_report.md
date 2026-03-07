# E2E Strategy Test Report: metamorpho_eth_yield (Anvil)

**Date:** 2026-03-06 22:24
**Result:** PASS
**Mode:** Anvil
**Duration:** ~1 minute

## Configuration

| Field | Value |
|-------|-------|
| Strategy | metamorpho_eth_yield |
| Chain | ethereum |
| Network | Anvil fork (publicnode.com free RPC, Ethereum mainnet) |
| Anvil Port | 58622 (auto-assigned by managed gateway) |
| Vault | Steakhouse USDC (0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB) |

## Config Changes Made

| Field | Original | Changed To | Reason |
|-------|----------|------------|--------|
| `deposit_amount` | `"1000"` | `"40"` | Budget cap ($50 max per trade) |
| `min_deposit_usd` | `"100"` | `"10"` | Allow $40 deposit to pass the threshold check |

Config restored to original values after test.

## Execution

### Setup
- Anvil auto-started by managed gateway on port 58622 (Ethereum mainnet fork via publicnode.com free RPC)
- No Alchemy API key available; public RPC fallback used successfully
- Wallet auto-funded by managed gateway from `anvil_funding` in config.json: 10 ETH + 50,000 USDC
- Wallet: `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266` (Anvil default)

### Strategy Run
- Strategy executed with `--network anvil --once`
- Initial state: `idle` (fresh start, no prior state)
- USDC price: $0.9999985 (confidence 1.00, sources 2/2 via Chainlink + CoinGecko)
- Decision: VAULT_DEPOSIT (40 USDC — within 80% allocation cap of 50,000 available)

### Intents and Transactions

| Step | Action | TX Hash | Gas Used | Block | Status |
|------|--------|---------|----------|-------|--------|
| 1 | USDC approve (Permit2) | `657da044544edb5b9b2fedd041addbe94b68c218b9a74b73298470906044c0e3` | 55,558 | 24594234 | SUCCESS |
| 2 | VAULT_DEPOSIT (ERC-4626 deposit) | `06d78d535965767160bfdebc863d7478d7c2cbfef9ce8e78a3b584e2ec63f299` | 357,414 | 24594235 | SUCCESS |

- Total gas used: 412,972
- Shares received: 35,732,947,282,709,343,475 (ERC-4626 vault shares, 18 decimals)
- Assets deposited: 40,000,000 (40 USDC in 6-decimal units)
- Final state: `deposited`

### Key Log Output

```text
Aggregated price for USDC/USD: 0.9999985 (confidence: 1.00, sources: 2/2, outliers: 0)
DEPOSIT: 40 USDC into Steakhouse vault (available: 50000, alloc cap: 80%)
Compiled VAULT_DEPOSIT: 40 USDC into vault 0xBEEF0173...
Simulation successful: 2 transaction(s), total gas: 550937
Transaction confirmed: tx=657da0...c0e3, block=24594234, gas_used=55558
Transaction confirmed: tx=06d78d...f299, block=24594235, gas_used=357414
EXECUTED: VAULT_DEPOSIT completed successfully
Txs: 2 (657da0...c0e3, 06d78d...f299) | 412,972 gas
Enriched VAULT_DEPOSIT result with: deposit_data (protocol=metamorpho, chain=ethereum)
Deposit confirmed: assets=40000000, shares=35732947282709343475
VAULT_DEPOSIT successful -> state=deposited
Status: SUCCESS | Intent: VAULT_DEPOSIT | Gas used: 412972 | Duration: 36666ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Placeholder prices | `IntentCompiler using PLACEHOLDER PRICES. Slippage calculations will be INCORRECT. This is only acceptable for unit tests.` |
| 2 | strategy | INFO | No Alchemy API key / free public RPC | `No API key configured -- using free public RPC for ethereum (rate limits may apply)` |
| 3 | strategy | INFO | No CoinGecko API key / fallback to Chainlink | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 4 | strategy | ERROR | Circular import in pendle incubating strategy (unrelated) | `Failed to import strategy strategies.incubating.pendle_pt_swap_arbitrum.strategy (retry failed): cannot import name 'IntentStrategy' from partially initialized module 'almanak'` |

Notes:
- Finding #1 (placeholder prices) is a known WARNING for Anvil mode. The vault deposit does not rely on slippage calculations so this did not affect correctness, but is unsafe on mainnet.
- Findings #2 and #3 are informational: USDC was correctly priced at $0.9999985 via Chainlink + CoinGecko aggregation.
- Finding #4 is a pre-existing issue in the unrelated `pendle_pt_swap_arbitrum` incubating strategy (circular import at startup scan). It does not affect this test.
- No zero prices, no reverts, no token resolution failures, no timeouts.

## Result

**PASS** - The `metamorpho_eth_yield` strategy successfully executed a VAULT_DEPOSIT of 40 USDC into the Steakhouse USDC MetaMorpho vault on an Ethereum Anvil fork, producing 2 confirmed on-chain transactions (USDC approval + ERC-4626 deposit) with 412,972 total gas and transitioning state from `idle` to `deposited`.

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 1
