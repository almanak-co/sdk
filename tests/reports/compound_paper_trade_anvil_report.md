# E2E Strategy Test Report: compound_paper_trade (Anvil)

**Date:** 2026-03-16 00:41
**Result:** PASS
**Mode:** Anvil
**Duration:** ~3 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | compound_paper_trade |
| Chain | base |
| Network | Anvil fork |
| Anvil Port | auto (managed gateway, port 62986) |

## Config Changes Made

- `network` changed from `"mainnet"` to `"anvil"` before run (restored after)
- `supply_amount: 100` is within the 1000 USD budget cap — no change needed

## Execution

### Setup
- [x] Managed gateway auto-started on port 50052 (Anvil mode)
- [x] Anvil fork of Base auto-started on port 62986 at block 43403563
- [x] Wallet funded by managed gateway: 100 ETH + 10,000 USDC

### Strategy Run
- [x] ETH price fetched: $2096.68 (above supply threshold of $2000)
- [x] Strategy decided: SUPPLY 100 USDC to Compound V3 (usdc market)
- [x] Intent compiled: 2 transactions (approve + supply), gas estimate 230,000
- [x] Simulation passed via LocalSimulator
- [x] TX 1/2 confirmed: `4469dabb40f57e4918aeb42b7b99bdd9be5c5603369eba9aa15399a2cf8f5ea1` (block 43403565, 55,449 gas)
- [x] TX 2/2 confirmed: `a6e5fa85a5899f24cc7ed98ba1913b5d617fa4a3c02d1b7b154b121932254137` (block 43403566, 115,434 gas)
- [x] Total gas used: 170,883
- [x] Receipt parsed: Compound V3 SUPPLY 100,000,000 (100 USDC with 6 decimals)
- [x] Result enriched: `supply_amount` extracted by ResultEnricher

### Key Log Output

```text
ETH price = $2096.68
ETH $2097 > $2000, supplying 100 USDC
SUPPLY: 100.0000 USDC to Compound V3 (usdc market)
Compiled SUPPLY: 100.0000 USDC to Compound V3 (usdc market)
   Txs: 2 | Gas: 230,000
EXECUTED: SUPPLY completed successfully
   Txs: 2 (4469da...5ea1, a6e5fa...4137) | 170,883 gas
Parsed Compound V3: SUPPLY 100,000,000, tx=0xa6e5...4137, 115,434 gas
Enriched SUPPLY result with: supply_amount (protocol=compound_v3, chain=base)
Supplied 100 USDC to Compound V3
Status: SUCCESS | Intent: SUPPLY | Gas used: 170883 | Duration: 21010ms
```

## Transaction Summary

| Step | TX Hash | Block | Gas Used | Status |
|------|---------|-------|----------|--------|
| APPROVE (USDC) | `4469dabb...5ea1` | 43403565 | 55,449 | SUCCESS |
| SUPPLY (Compound V3) | `a6e5fa85...4137` | 43403566 | 115,434 | SUCCESS |

Note: These are Anvil fork transactions (not on mainnet).

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | INFO | No CoinGecko API key — fallback to Chainlink+free tier | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 2 | strategy | INFO | Insecure mode warning (expected for Anvil) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |

No ERROR-severity findings. Finding #1 is expected behavior when `ALMANAK_GATEWAY_COINGECKO_API_KEY` is not set — pricing still succeeded via 3/4 sources (Chainlink + Binance + DexScreener). Finding #2 is the standard Anvil insecure mode notice, not an issue.

## Result

**PASS** — Strategy successfully supplied 100 USDC to Compound V3 on Base (Anvil fork). Both transactions confirmed on-chain (approve + supply), receipt parsed correctly, and state updated. ETH price at $2096.68 triggered the supply threshold of $2000 as expected.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 2
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
