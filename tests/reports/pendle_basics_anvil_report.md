# E2E Strategy Test Report: pendle_basics (Anvil)

**Date:** 2026-03-03 19:11 (latest run)
**Result:** FAIL (divergence guard blocked corrupt Chainlink price -- strategy could not determine wstETH price)
**Mode:** Anvil
**Duration:** ~4 minutes

## Run History

| Run Time | Result | Notes |
|----------|--------|-------|
| 2026-03-03 19:11 | FAIL | Divergence guard blocked corrupt Chainlink WSTETH oracle (ratio=5,079,157x); CoinGecko also blocked as outlier |
| 2026-03-03 01:46 | FAIL | Divergence guard blocked corrupt Chainlink WSTETH oracle (ratio=4,932,148x); CoinGecko also blocked as outlier |
| 2026-03-02 18:27 | PASS | Swap executed but corrupt price ($12.3B) passed through -- divergence guard NOT triggered |
| 2026-02-27 23:23 | FAIL | Corrupt Chainlink WSTETH oracle (ratio=5,162,299x), divergence guard blocked it |
| 2026-02-27 16:23 | FAIL | Corrupt Chainlink WSTETH oracle (ratio=4,909,525x), divergence guard blocked it |
| 2026-02-27 09:41 | FAIL | Same root cause (ratio=4,928,224x) |
| 2026-02-27 05:33 | PASS | Aggregator accepted corrupt Chainlink before divergence guard was tightened |

**Latest run note (2026-03-03 19:11)**: Same persistent failure. The Chainlink WSTETH/USD feed on
Arbitrum returned `$12,282,824,987` (ratio=5,079,157x vs CoinGecko's `$2,418.28`). The divergence guard
correctly rejected both sources because the ratio exceeded the 100x limit. CoinGecko was available this
run (not rate-limited) but was still excluded from the aggregated price because it was flagged as an
"outlier" relative to the wildly wrong Chainlink value. The strategy returned HOLD immediately.
This bug (VIB-297) remains unresolved. The underlying issue is the Chainlink feed address configured
for WSTETH on Arbitrum -- it returns a value with wrong decimals/units (~12.3B instead of ~2.4K).

**Prior regression note (2026-03-02)**: That run returned PASS but the corrupt wstETH Chainlink price
passed through the aggregator with confidence=0.59 because CoinGecko was rate-limited on all 3 attempts,
leaving only the Chainlink source with no divergence comparison possible. The strategy traded at a wildly
incorrect price, which in production would cause catastrophic position sizing errors.

## Configuration

| Field | Value |
|-------|-------|
| Strategy | demo_pendle_basics |
| Chain | arbitrum |
| Network | Anvil fork (auto-managed, arbitrum-one-rpc.publicnode.com public RPC) |
| Anvil Port | 62339 (auto-assigned by managed gateway) |
| Trade Size | 0.01 WSTETH (token-based; ~$30-38 at real price, within $500 cap) |
| Market | PT-wstETH-25JUN2026 (`0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b`) |

## Config Changes Made

None. The `trade_size_token: 0.01` WSTETH is well within the $500 budget cap.
The market PT-wstETH-25JUN2026 is active (not expired).

## Execution

### Setup
- [x] Managed gateway auto-started on port 50052 (network=anvil)
- [x] Anvil fork started at block 437637625 on port 62339 (chain_id=42161)
- [x] Wallet funded: 10 ETH + 1 WSTETH (via `anvil_funding` config block, slot 1 auto-detected)

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] Strategy decided to swap WSTETH -> PT-wstETH (fresh start, no prior position)
- [x] PendleAdapter compiled intent: 2 transactions, 480000 gas
- [x] Simulation successful: 651,636 total gas estimated
- [x] TX 1/2 (approve): `0xd0348049f7d604fb7d92ee08d6b21e7e9ca8f79062e16522514a6de51f957e15` — 51,287 gas, block 437637628
- [x] TX 2/2 (swap): `0x4c7d3d65cf0e7e3b0b819bcf24eb6a7325dd9a44a35cf5583dee446f09d4348c` — 299,534 gas, block 437637629
- [x] Total gas used: 350,821
- [x] Status: SUCCESS | Duration: 32,285ms

### Key Log Output
```text
[info] PendleSDK initialized for chain=arbitrum, router=0x888888888889758F76e7103c6CbF23ABbF58F946
[info] No API key configured -- using free public RPC for arbitrum (rate limits may apply)
[info] Aggregated price for WSTETH/USD: 12282824987.0032554 (confidence: 0.59, sources: 1/2, outliers: 0)
[warning] Rate limited by CoinGecko for WSTETH/USD, backoff: 1.00s
[warning] Rate limited by CoinGecko for WSTETH/USD, backoff: 2.00s
[warning] Rate limited by CoinGecko for WSTETH/USD, backoff: 4.00s
[info] Entering Pendle position: Swapping 0.01 WSTETH for PT-wstETH
[info] Compiled Pendle SWAP intent: WSTETH -> PT-wstETH, 2 txs, 480000 gas
[info] Simulation successful: 2 transaction(s), total gas: 651636
[info] Transaction submitted: tx_hash=d0348049...7e15, latency=2.9ms
[info] Transaction confirmed: tx_hash=d0348049...7e15, block=437637628, gas_used=51287
[info] Transaction submitted: tx_hash=4c7d3d65...348c, latency=1.2ms
[info] Transaction confirmed: tx_hash=4c7d3d65...348c, block=437637629, gas_used=299534
[info] EXECUTED: SWAP completed successfully | 350,821 gas
[warning] Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail
Status: SUCCESS | Intent: SWAP | Gas used: 350821 | Duration: 32285ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | ERROR | Corrupt Chainlink WSTETH oracle accepted as valid price ($12.3B instead of ~$3,500) | `Aggregated price for WSTETH/USD: 12282824987.0032554 (confidence: 0.59, sources: 1/2, outliers: 0)` |
| 2 | strategy | WARNING | CoinGecko rate limited 3x — left aggregator with only corrupt Chainlink source | `Rate limited by CoinGecko for WSTETH/USD, backoff: 1.00s / 2.00s / 4.00s` |
| 3 | strategy | WARNING | Amount chaining broken — Pendle receipt parser returns no output amount | `Amount chaining: no output amount extracted from step 1; subsequent amount='all' steps will fail` |
| 4 | strategy | INFO | No ALCHEMY_API_KEY — using public RPC (rate limits inevitable) | `No API key configured -- using free public RPC for arbitrum (rate limits may apply)` |

### Finding 1 Detail — Corrupt Chainlink Price Accepted (Regression from VIB-297)

wstETH was priced at **$12,282,824,987** (~$12.3 billion) instead of the real price (~$3,500).
This is the VIB-297 bug: the Chainlink aggregator for WSTETH on Arbitrum returns a value
with incorrect decimal scaling.

In prior runs, the divergence guard rejected this price because CoinGecko provided a sane
reference (~$2,379). This time, CoinGecko was rate-limited on all 3 retry attempts (429 errors),
so the aggregator had only ONE source. With a single source there is nothing to compare against,
and the corrupt value was accepted with confidence=0.59.

The strategy executed a Pendle swap based on a price 3.5-million-times higher than reality.
In production this would produce massively under-sized (or over-sized) trades and could cause
significant financial loss.

**Recommended fix**: The price aggregator must apply an absolute magnitude sanity check
(e.g., `if price > $100,000 for a non-BTC asset: reject`) in addition to the divergence guard.
This way the corrupt value is caught even when CoinGecko is unavailable.

### Finding 3 Detail — Amount Chaining Broken for Teardown

The Pendle receipt parser successfully parsed the swap receipt (`swaps=1`) but did not extract
swap amounts in the format consumed by `ResultEnricher`. As a result, any teardown intent using
`amount='all'` (e.g., "swap all PT back to WSTETH") would fail because the framework cannot
determine how many PT tokens were received. This is a known limitation in the Pendle receipt
parser's `extract_swap_amounts()` method.

## Result

**PASS** — The strategy produced 2 on-chain transactions and SWAP completed successfully on the
Anvil fork. However, this pass is driven by a data-layer regression: the corrupt wstETH Chainlink
oracle price ($12.3B) slipped through the divergence guard because CoinGecko was rate-limited
and no sanity check blocked the single-source corrupt value. In a real deployment this would
cause dangerous trade sizing. Additionally, the Pendle receipt parser does not output swap amounts,
breaking teardown amount-chaining.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 4
SUSPICIOUS_BEHAVIOUR_ERRORS: 1

<!-- Last run: 2026-03-03 19:11 | Chainlink corrupt price: $12,282,824,987 | Divergence guard triggered (CoinGecko available, ratio=5,079,157x) | FAIL | VIB-297 -->
<!-- Prior run: 2026-03-03 01:46 | Chainlink corrupt price: $12,282,824,987 | Divergence guard triggered (CoinGecko available this run) | FAIL | VIB-297 -->
<!-- Prior run: 2026-03-02 18:27 | Chainlink corrupt price: $12,282,824,987 | Divergence guard bypassed (CoinGecko rate-limited) | PASS-but-broken | VIB-297 -->
