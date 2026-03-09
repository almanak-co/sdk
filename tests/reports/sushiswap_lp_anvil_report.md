# E2E Strategy Test Report: sushiswap_lp (Anvil)

**Date:** 2026-03-03 12:21
**Result:** PASS
**Mode:** Anvil
**Duration:** ~2.5 minutes

## Configuration

| Field | Value |
|-------|-------|
| Strategy | SushiSwapLPStrategy |
| Chain | arbitrum |
| Network | Anvil fork (managed, via publicnode) |
| Anvil Port | 56549 (auto-assigned by managed gateway) |
| Fork Block | 437896065 |
| Pool | WETH/USDC/3000 |
| amount0 | 0.001 WETH |
| amount1 | 3 USDC |
| force_action | open |

**Config changes:** None. Amounts (0.001 WETH ~$2 at WETH=$1974, 3 USDC) are well under the $500 budget cap. `force_action` was already set to `"open"`.

Note: `ALCHEMY_API_KEY` is empty in `.env`; the framework automatically fell back to public Arbitrum RPC (arbitrum-one-rpc.publicnode.com). The strategy was run with the Anvil default private key (`0xac0974bec...ff80`) per standard Anvil workflow.

## Execution

### Setup

- [x] Managed gateway auto-started Anvil fork on port 56549 (arbitrum, block 437896065, chain_id=42161)
- [x] Wallet (`0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266`) funded by managed gateway: 100 ETH, 1 WETH (slot 51), 10,000 USDC (slot 9)
- [x] Gateway started on 127.0.0.1:50051 (insecure/anvil mode, acceptable for local dev)

### Strategy Run

- [x] Strategy executed: `uv run almanak strat run -d strategies/demo/sushiswap_lp --network anvil --once`
- [x] Intent returned: LP_OPEN (triggered by `force_action=open`)
- [x] WETH price fetched: $1973.98 (Chainlink on-chain primary, 2 sources, confidence 1.00)
- [x] USDC price fetched: $0.999972 (2 sources, confidence 1.00)
- [x] Tick range calculated: [-200940, -199980] (price range $1875.33 - $2072.74)
- [x] Compiled: 3 transactions (approve WETH + approve USDC + lp_mint), estimated 660,000 gas
- [x] Simulation successful: 3 transactions, 923,788 total gas
- [x] All 3 transactions confirmed sequentially on-chain (Anvil fork)
- [x] LP position opened: **position_id = 34626**, liquidity = 1,886,857,029,086
- [x] Result enrichment: position_id, tick_lower, tick_upper, liquidity extracted from receipt

### Transactions (Anvil fork - local only)

| # | Purpose | TX Hash | Gas Used | Block |
|---|---------|---------|----------|-------|
| 1 | WETH approval (Permit2) | `3d17700db46cabe8ac9716a55f8d8667162e199ea523096822006c387df0bf7a` | 53,440 | 437896068 |
| 2 | Permit2 internal approval | `9d9771b068ad2983ee440f8f90930b939b5df554c1896a55011709f180ea1f88` | 55,437 | 437896069 |
| 3 | SushiSwap V3 mint (LP_OPEN) | `7692fe6752fb0ec43f8abab51f83ddaafa8950b3334977e67e7bf829a57296eb` | 506,675 | 437896070 |

**Total gas used:** 615,552

### Key Log Output

```text
info  Anvil fork started: port=56549, block=437896065, chain_id=42161
info  Funded 0xf39Fd6e5... with 100 ETH
info  Funded 0xf39Fd6e5... with WETH via known slot 51
info  Funded 0xf39Fd6e5... with USDC via known slot 9
info  Aggregated price for WETH/USD: 1973.98137486 (confidence: 1.00, sources: 2/2, outliers: 0)
info  Aggregated price for USDC/USD: 0.9999720000000001 (confidence: 1.00, sources: 2/2, outliers: 0)
info  Forced action: OPEN LP position
info  LP_OPEN: 0.0010 WETH + 3.0000 USDC, price range [1875.3348 - 2072.7385], ticks [-200940 - -199980]
info  Compiled LP_OPEN intent: WETH/USDC, range [1875.33-2072.74], 3 txs (approve + approve + lp_mint), 660000 gas
info  Simulation successful: 3 transaction(s), total gas: 923788
info  EXECUTED: LP_OPEN completed successfully
info     Txs: 3 (3d1770...bf7a, 9d9771...1f88, 7692fe...96eb) | 615,552 gas
info  Extracted LP position ID from receipt: 34626
info  Enriched LP_OPEN result with: position_id, tick_lower, tick_upper, liquidity (protocol=sushiswap_v3, chain=arbitrum)
info  SushiSwap V3 LP position opened: position_id=34626, liquidity=1886857029086
Status: SUCCESS | Intent: LP_OPEN | Gas used: 615552 | Duration: 37006ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | INFO | No Alchemy key; public RPC fallback (rate limits may apply) | `No API key configured -- using free public RPC for arbitrum (rate limits may apply)` |
| 2 | strategy | INFO | No CoinGecko key; on-chain Chainlink pricing primary | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback` |
| 3 | strategy | WARNING | Port not freed within 5s after Anvil shutdown | `Port 56549 not freed after 5.0s` |

**Analysis of findings:**

- Findings 1-2 are informational configuration notices, not bugs. The strategy correctly fell back to public RPC and on-chain Chainlink pricing. Prices were healthy (WETH $1973.98, USDC $1.00) with full confidence (2/2 sources), no zero prices, no outliers.
- Finding 3 (port not freed) is a minor cosmetic race condition in Anvil process shutdown cleanup. The Anvil process was successfully terminated shortly after; this does not affect results.
- No zero prices, no token resolution failures, no API errors, no transaction reverts, and no NaN/None values were detected.
- No ERROR-level log entries.

## Result

**PASS** - SushiSwapLPStrategy successfully opened a WETH/USDC concentrated liquidity position on SushiSwap V3 (Arbitrum Anvil fork). 3 transactions executed (2 approvals + LP mint). LP position NFT #34626 minted with 1,886,857,029,086 liquidity units in the [$1875 - $2073] price range.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 3
SUSPICIOUS_BEHAVIOUR_ERRORS: 0
