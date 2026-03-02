# E2E Strategy Test Report: traderjoe_lp (Anvil)

**Date:** 2026-02-25 06:54
**Result:** PASS
**Mode:** Anvil
**Duration:** ~2 minutes
**Branch:** fix/public-rpc-fallback

## Configuration

| Field | Value |
|-------|-------|
| Strategy | traderjoe_lp |
| Chain | avalanche |
| Chain ID | 43114 |
| Network | Anvil fork (managed gateway, port 50052) |
| Pool | WAVAX/USDC/20 |
| amount_x | 0.001 WAVAX |
| amount_y | 3 USDC |
| num_bins | 11 |

## Config Changes Made

| Field | Original | Changed To | Reason |
|-------|----------|------------|--------|
| force_action | (absent) | "open" | Trigger immediate LP_OPEN trade on first run |

Config restored to original values after the test.

Trade sizes (0.001 WAVAX + 3 USDC ~= $3.01 at $8.30/AVAX) are well under the $100 budget cap.

Note: ALCHEMY_API_KEY is not set in .env. The managed gateway used the public Avalanche RPC
(https://avalanche-c-chain-rpc.publicnode.com) via the fix/public-rpc-fallback feature.

## Execution

### Setup
- Managed gateway auto-started on port 50052 by the `uv run almanak strat run` CLI
- Anvil fork started internally by gateway on Avalanche (chain ID 43114, port 59279)
- Wallet funded via anvil_funding config: 100 AVAX + 100 WAVAX (slot 3) + 10,000 USDC (slot 9)

### Strategy Run
- Strategy executed with `--network anvil --once`
- force_action = "open" triggered LP_OPEN immediately
- WAVAX price fetched via CoinGecko free tier: $8.30
- USDC price: $0.9999
- Price ratio: 8.3 USDC/WAVAX
- Price range calculated: [7.8856 - 8.7157] USDC/WAVAX (10% range, +/-5%)
- First compile attempt failed with PoA ExtraData error (retry 1/3 -- see Suspicious Behaviour)
- Second compile succeeded with PoA middleware applied
- LP_OPEN intent compiled to 3 transactions (WAVAX approve + USDC approve + addLiquidity)
- All 3 transactions confirmed on Anvil fork

### Transactions

| Step | TX Hash | Block | Gas Used | Status |
|------|---------|-------|----------|--------|
| TX 1/3 WAVAX approve | eafa3137dad62abe1ab223171c9fc7926b3e797a8b98973abe3f6ec5c95e98d0 | 78917761 | 46,123 | SUCCESS |
| TX 2/3 USDC approve | a2cfe842d37263705de441550a57de6d2ff1407c934d5576bd8fcc40d973fc92 | 78917762 | 55,437 | SUCCESS |
| TX 3/3 addLiquidity | face69656ea650050ad62a84b2ed79440b829107da8127e173a37751a00bc19a | 78917763 | 597,904 | SUCCESS |
| Total | | | 699,464 | SUCCESS |

### Key Log Output

```text
Aggregated price for WAVAX/USD: 8.3 (confidence: 1.00, sources: 1/1, outliers: 0)
Aggregated price for USDC/USD: 0.999919 (confidence: 1.00, sources: 1/1, outliers: 0)
Forced action: OPEN LP position
LP_OPEN: 0.0010 WAVAX + 3.0000 USDC, price range [7.8856 - 8.7157], bin_step=20
[error] Failed to compile TraderJoe V2 LP_OPEN intent: The field extraData is 90 bytes...
[info] Retrying intent (attempt 1/3, delay=1.06s)
Compiled TraderJoe V2 LP_OPEN intent: WAVAX/USDC, bin_step=20, 3 txs, 860000 gas
Sequential submit: TX 3/3 confirmed (block=78917763, gas=597904)
EXECUTED: LP_OPEN completed successfully
Txs: 3 (eafa31...98d0, a2cfe8...fc92, face69...c19a) | 699,464 gas
Enriched LP_OPEN result with: bin_ids (protocol=traderjoe_v2, chain=avalanche)
TraderJoe LP position opened successfully
Status: SUCCESS | Intent: LP_OPEN | Gas used: 699464 | Duration: 48221ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | ERROR | PoA chain ExtraData error on first compile (recovered via retry) | `ExtraDataLengthError: The field extraData is 90 bytes, but should be 32. It is quite likely that you are connected to a POA chain.` |
| 2 | strategy | WARNING | Gas estimation failed for addLiquidity tx | `Gas estimation failed for tx 3/3: ('0xe6907f56', '0xe6907f56'). Using compiler-provided gas limit.` |
| 3 | strategy | WARNING | Amount chaining: no output extracted from LP_OPEN step | `Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail` |
| 4 | strategy | WARNING | Gas estimates for TX 1-2 below compiler limit | `Gas estimate tx[0]: raw=46,123 buffered=50,735 (x1.1) < compiler=88,000, using compiler limit` |
| 5 | gateway | WARNING | No COINGECKO_API_KEY - using free tier | `COINGECKO_API_KEY not configured - CoinGecko will use free tier API (30 requests/minute limit)` |
| 6 | strategy | INFO | No ALCHEMY_API_KEY - using public RPC | `No API key configured -- using free public RPC for avalanche (rate limits may apply)` (4 occurrences) |
| 7 | strategy | INFO | Port cleanup delay after managed gateway shutdown | `Port 59279 not freed after 5.0s` |

Notes:
- Finding 1 (PoA ExtraData error, ERROR severity): Avalanche C-Chain uses PoA consensus and
  returns 90-byte extraData fields that exceed the standard 32-byte EIP-1559 limit. The web3.py
  ExtraData middleware was not applied on the first compile attempt, causing an exception. The
  framework retried (1/3) and succeeded because the middleware was applied on the retry path.
  This is a latent bug -- PoA middleware should be applied unconditionally when chain=avalanche,
  not reactively on retry. Added ~7 seconds of unnecessary latency.
- Finding 2: The LocalSimulator could not estimate gas for the addLiquidity call (selector
  0xe6907f56). Fell back to compiler's 860,000 limit. Actual usage was 597,904 (30% headroom).
  Non-blocking; tx succeeded.
- Finding 3: Amount chaining warning is expected here -- LP_OPEN is the only intent in this
  run. Would matter if a follow-up intent used amount='all'.
- Finding 4: Gas estimates for approve TXs (46K/55K) were below the compiler's conservative
  88K limit. Compiler limit was used. Functionally fine; just slightly over-estimated gas.
- Findings 5-6: Missing API keys are expected in this environment. Free-tier CoinGecko and
  public RPC were sufficient.
- Finding 7: Cosmetic cleanup delay, no functional impact.

## Result

**PASS** - traderjoe_lp on Avalanche (Anvil fork) executed LP_OPEN successfully.
3 transactions confirmed, 699,464 gas total. TraderJoe V2 Liquidity Book position opened
in WAVAX/USDC/20 pool with 0.001 WAVAX + 3 USDC across price range [7.886, 8.716].
One retry occurred due to PoA middleware not applied on first compile -- a latent bug
worth ticketing (but non-blocking for this test).

---

SUSPICIOUS_BEHAVIOUR_COUNT: 7
SUSPICIOUS_BEHAVIOUR_ERRORS: 1
