# E2E Strategy Test Report: pendle_basics (Anvil)

**Date:** 2026-03-06 05:39 (latest run)
**Result:** PASS
**Mode:** Anvil
**Duration:** ~4 minutes

## Run History

| Run Time | Result | Notes |
|----------|--------|-------|
| 2026-03-06 05:39 | PASS | Swap executed cleanly; Chainlink oracle returned correct wstETH price ($2,556.50); 2 txs confirmed (approve + swap) |
| 2026-03-05 14:07 | PASS | Swap executed cleanly; Chainlink oracle returned correct wstETH price ($2,604); 2 txs confirmed |
| 2026-03-05 02:38 | FAIL | Divergence guard blocked corrupt Chainlink WSTETH oracle (ratio=4,716,704x); CoinGecko also blocked as outlier |
| 2026-03-05 01:08 | FAIL | Divergence guard blocked corrupt Chainlink WSTETH oracle (ratio=4,657,989x); CoinGecko also blocked as outlier |
| 2026-03-03 19:11 | FAIL | Divergence guard blocked corrupt Chainlink WSTETH oracle (ratio=5,079,157x); CoinGecko also blocked as outlier |
| 2026-03-03 01:46 | FAIL | Divergence guard blocked corrupt Chainlink WSTETH oracle (ratio=4,932,148x); CoinGecko also blocked as outlier |
| 2026-03-02 18:27 | PASS | Swap executed but corrupt price ($12.3B) passed through -- divergence guard NOT triggered |
| 2026-02-27 23:23 | FAIL | Corrupt Chainlink WSTETH oracle (ratio=5,162,299x), divergence guard blocked it |
| 2026-02-27 16:23 | FAIL | Corrupt Chainlink WSTETH oracle (ratio=4,909,525x), divergence guard blocked it |
| 2026-02-27 09:41 | FAIL | Same root cause (ratio=4,928,224x) |
| 2026-02-27 05:33 | PASS | Aggregator accepted corrupt Chainlink before divergence guard was tightened |

**Latest run note (2026-03-15 18:34)**: PASS. Clean pass. wstETH/USD priced via Chainlink ($2,594.78) + on-chain derivation (confidence=0.65, 2/4 sources). CoinGecko free tier rate-limited 4 times (backoffs 1s/2s/4s/8s). Post-swap PT-wstETH price lookup failed across all 4 sources (Chainlink, Binance, DexScreener, CoinGecko) -- expected for Pendle derivative tokens with no oracle coverage. TX1 (approve): `0xc04b948b3ab5e97cf539cc6280542c4286bcabf080ccf896a7471241f8383ed5` -- 51,287 gas. TX2 (swap): `0xfa811c867c811e5faa929d3ef0e4bf081c31ac7b64c97ee27902d6448b7d597a` -- 297,940 gas. Total: 349,227 gas. Duration: 36,287ms.

**Prior run note (2026-03-06 05:39)**: PASS. Second consecutive clean pass.
Chainlink WSTETH/USD returned $2,556.50 (WSTETH/ETH ratio 1.2276 x ETH/USD $2,082.51, confidence=0.81).
CoinGecko free tier was rate-limited (3x, backoffs 1s/2s/4s) so the aggregator used only 1/2 sources
(confidence=0.57). Despite reduced confidence, the on-chain Chainlink price was valid and the swap
executed correctly. TX1 (approve): `0xbb27fc75815ad9078c1c6f5f8c9203b94e73166284045dbb6e2f3c235cc2b81e` --
51,287 gas. TX2 (swap): `0x7b3e05621cbb783ef2618c51a0b37880587ae3b8c62b8491352ed98bfd2c78c7` -- 298,737 gas.
Total: 350,024 gas. The incubating `pendle_pt_swap_arbitrum` circular import error persists.

**Prior run note (2026-03-05 14:07)**: PASS. This is the first clean pass since 2026-03-02.
The Chainlink WSTETH/USD feed returned the correct value -- `$2,604.52` (WSTETH/ETH ratio 1.2276 x
ETH/USD $2,121.63). CoinGecko free tier provided a consistent second source at $2,604.38, giving
confidence=0.85 across 2/2 sources. The strategy entered a Pendle PT position by swapping 0.01 wstETH.
Both transactions confirmed on-chain (approve + swap via Pendle router 0x888888...).

The Chainlink feed instability (VIB-297) appears to have resolved itself on this fork block
(438614531). It is possible the corrupt price was returned only for certain historical blocks.

Also noted this run: `strategies/incubating/pendle_pt_swap_arbitrum/strategy.py` has a persistent
circular import bug (`cannot import name 'IntentStrategy' from partially initialized module 'almanak'`).
This does NOT affect the demo strategy but logs an error at gateway startup.

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
| Anvil Port | 56699 (auto-assigned by managed gateway) |
| Fork Block | 438614531 |
| Trade Size | 0.01 WSTETH (token-based; ~$26 at real price, within $50 cap) |
| Market | PT-wstETH-25JUN2026 (`0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b`) |

## Config Changes Made

None. The `trade_size_token: 0.01` WSTETH is within the $50 budget cap (~$26 at $2,604/wstETH).
The market PT-wstETH-25JUN2026 is active (not expired). No `force_action` field exists; the
strategy naturally triggers a swap on first run when balance is sufficient.

## Execution

### Setup
- [x] Managed gateway auto-started on port 50052 (network=anvil)
- [x] Anvil fork started at block 438614531 on port 56699 (chain_id=42161)
- [x] Wallet funded: 10 ETH + 1 WSTETH (via `anvil_funding` config block, slot 1 auto-detected)

### Strategy Run
- [x] Strategy executed with `--network anvil --once`
- [x] wstETH price: $2,604.38 (Chainlink on-chain primary + CoinGecko free fallback, confidence=0.85, 2/2 sources)
- [x] wstETH balance: 1.0 WSTETH -- sufficient for 0.01 WSTETH trade
- [x] Strategy decided to swap WSTETH -> PT-wstETH (fresh start, no prior position)
- [x] PendleSDK initialized: router=0x888888888889758F76e7103c6CbF23ABbF58F946
- [x] PendleAdapter compiled intent: 2 transactions, 480,000 gas
- [x] Simulation successful: 651,636 total gas estimated
- [x] TX 1/2 (approve): `0x3839fdb317c2d0871ef7eb5488386979b0c65acd815329113c7ddda046d7caa4` -- 51,287 gas, block 438614534
- [x] TX 2/2 (swap): `0x9dc8d5815da2e2493f434db16c5f77ddd1398ad46fe7ea7f547b1222e64bcd52` -- 298,722 gas, block 438614535
- [x] Total gas used: 350,009
- [x] Pendle receipt parsed: swaps=1 on TX 2
- [x] Status: SUCCESS | Duration: 31,898ms

### Key Log Output

```text
[info] Derived WSTETH/USD: 2604.521992924844606... (= 1.2276... × 2121.62..., confidence=0.81)
[info] Aggregated price for WSTETH/USD: 2604.3759964624223 (confidence: 0.85, sources: 2/2, outliers: 0)
[info] Entering Pendle position: Swapping 0.01 WSTETH for PT-wstETH
[info] Compiled Pendle SWAP intent: WSTETH -> PT-wstETH, 2 txs, 480000 gas
[info] Simulation successful: 2 transaction(s), total gas: 651636
[info] Transaction submitted: tx_hash=3839fdb3...caa4, latency=3.0ms
[info] Transaction confirmed: tx_hash=3839fdb3...caa4, block=438614534, gas_used=51287
[info] Transaction submitted: tx_hash=9dc8d581...cd52, latency=1.0ms
[info] Transaction confirmed: tx_hash=9dc8d581...cd52, block=438614535, gas_used=298722
[info] EXECUTED: SWAP completed successfully
[info] Txs: 2 (3839fd...caa4, 9dc8d5...cd52) | 350,009 gas
[info] Parsed Pendle receipt: tx=0x9dc8d581..., swaps=1, mints=0, burns=0, redeems=0
Status: SUCCESS | Intent: SWAP | Gas used: 350009 | Duration: 31898ms
```

## Suspicious Behaviour

| # | Source | Severity | Pattern | Log Line |
|---|--------|----------|---------|----------|
| 1 | strategy | WARNING | Insecure mode (expected for Anvil) | `INSECURE MODE: Auth interceptor disabled - no auth_token configured. This is acceptable for local development on 'anvil'.` |
| 2 | strategy | INFO | No CoinGecko API key -- fallback mode | `No CoinGecko API key -- using on-chain pricing (Chainlink oracles) with free CoinGecko as fallback.` |
| 3 | strategy | INFO | No Alchemy API key -- public RPC (rate limits possible) | `No API key configured -- using free public RPC for arbitrum (rate limits may apply)` |
| 4 | gateway | ERROR | Circular import in incubating strategy `pendle_pt_swap_arbitrum` | `Failed to import strategy strategies.incubating.pendle_pt_swap_arbitrum.strategy: cannot import name 'IntentStrategy' from partially initialized module 'almanak'` |
| 5 | gateway | ERROR | Port 9090 address conflict (manual gateway vs managed gateway metrics) | `OSError: [Errno 48] Address already in use` |

**Notes:**
- Findings 1, 2, 3 are expected for local Anvil testing with no API keys configured.
- Finding 4 is a real bug: `strategies/incubating/pendle_pt_swap_arbitrum/strategy.py` has a circular import. This is non-blocking for the demo strategy but should be investigated.
- Finding 5 was caused by the test pre-starting a standalone gateway; the managed gateway correctly used port 50052 and the strategy ran cleanly. The port conflict is a test-harness artifact, not a strategy bug.

## Result

**PASS** - The `pendle_basics` strategy successfully executed a Pendle SWAP on an Arbitrum Anvil fork,
swapping 0.01 wstETH for PT-wstETH via the Pendle router in 2 transactions (approve + swap) with 350,009
total gas confirmed on-chain. The Chainlink wstETH oracle returned a correct price ($2,604.38) this run
(the VIB-297 corrupt oracle issue was not reproduced). One actionable finding: the incubating strategy
`pendle_pt_swap_arbitrum` has a circular import error that should be resolved separately.

---

SUSPICIOUS_BEHAVIOUR_COUNT: 2
SUSPICIOUS_BEHAVIOUR_ERRORS: 1

<!-- Run: 2026-03-15 18:34 | wstETH price: $2,593.27 | TX1: 0xc04b948b...3ed5 | TX2: 0xfa811c86...597a | Gas: 349,227 | PASS -->
<!-- Run: 2026-03-06 05:39 | wstETH price: $2,556.50 | TX1: 0xbb27fc75...b81e | TX2: 0x7b3e0562...78c7 | Gas: 350,024 | PASS -->
<!-- Run: 2026-03-05 14:07 | wstETH price: $2,604.38 | TX1: 0x3839fdb3...caa4 | TX2: 0x9dc8d581...cd52 | Gas: 350,009 | PASS -->
<!-- Prior run: 2026-03-05 02:38 | Chainlink corrupt price: $12,284,513,449 | Divergence guard triggered (CoinGecko available, ratio=4,716,704x) | FAIL | VIB-297 -->
<!-- Prior run: 2026-03-05 01:08 | Chainlink corrupt price: $12,284,513,449 | Divergence guard triggered (CoinGecko available, ratio=4,657,989x) | FAIL | VIB-297 -->
<!-- Prior run: 2026-03-03 19:11 | Chainlink corrupt price: $12,282,824,987 | Divergence guard triggered (CoinGecko available, ratio=5,079,157x) | FAIL | VIB-297 -->
<!-- Prior run: 2026-03-03 01:46 | Chainlink corrupt price: $12,282,824,987 | Divergence guard triggered (CoinGecko available this run) | FAIL | VIB-297 -->
<!-- Prior run: 2026-03-02 18:27 | Chainlink corrupt price: $12,282,824,987 | Divergence guard bypassed (CoinGecko rate-limited) | PASS-but-broken | VIB-297 -->
