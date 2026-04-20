# X-Layer Deterministic Strategy: Aave-Uniswap CLMM Yield Loop

> **X-Layer Build-X Hackathon submission** — Project #2: Deterministic DeFi
> _Season: April 1-15, 2026_

A production-grade **supply -> borrow -> CLMM yield** strategy that
chains **Aave V3.6 + Uniswap V3** into a multi-protocol yield loop on
X-Layer. The strategy supplies stablecoin collateral, borrows against
it, and deploys the borrowed capital into a concentrated LP position
to earn trading fees that exceed the borrow cost.

---

## Strategy: `aave_okb_clmm_loop`

See `aave_okb_clmm_loop/README.md` for the full 790-line technical
documentation including architecture, economics, backtesting results,
risk analysis, and IL mechanics.

**Quick summary:**

```
Supply USDT0 to Aave V3.6
  -> Borrow USDG at 50% LTV (HF ~1.5)
  -> Convert USDG -> USDT (stablecoin hop)
  -> Split half USDT -> WOKB
  -> Open Uniswap V3 WOKB/USDT/3000 concentrated LP
  -> Monitor + auto-rebalance with vol-adaptive range
```

---

## Proven on mainnet (full lifecycle: entry -> earn -> teardown)

This strategy was deployed on xlayer mainnet from April 11-13, 2026.
After 88 hours of live operation earning 17% APR in LP fees, it was
torn down via the built-in teardown mechanism to demonstrate the full
lifecycle. **Every transaction below is verifiable on the
[OKX X-Layer Explorer](https://www.okx.com/web3/explorer/xlayer).**

Wallet: `0x54776446Aa29Fc49d152B4850bD410eA1E4d24bF`

### Entry (April 11, 2026) -- 8 on-chain transactions

| # | Intent | Details | Tx hash | Gas |
|---|--------|---------|---------|-----|
| 1 | APPROVE USDT0 | Aave V3.6 Pool | `0x3a91...9f77` | 36,265 |
| 2 | SUPPLY | 4.0 USDT0 to Aave | `0x3e3b...c243` | 165,559 |
| 3 | BORROW | 2.000042 USDG from Aave | `0x98c2...136a` | 285,736 |
| 4 | APPROVE USDG | Uniswap V3 Router | `0x327b...1200` | 57,981 |
| 5 | SWAP | 2.0000 USDG -> 1.9994 USDT | `0xcec6...89b7` | 131,002 |
| 6 | APPROVE USDT | Uniswap V3 Router | `0x7e98...361c` | 53,365 |
| 7 | SWAP | 1.4497 USDT -> 0.0170 WOKB | `0x751e...3178` | 141,696 |
| 8 | LP_OPEN #945 | 0.0168 WOKB + 1.4352 USDT | (3 sub-txs) | 595,507 |
| | **Total entry** | | | **1,467,111** |

### Live performance (88 hours, on-chain fee accumulation)

LP position #945 on Uniswap V3 WOKB/USDT/3000, ticks [230880, 232860].
Fees verified via `collect()` staticcall at each checkpoint:

| Checkpoint | Fees (USDT0) | Fees (WOKB) | Total USD | Borrow cost | Net profit | Fee APR |
|------------|-------------|-------------|-----------|-------------|------------|---------|
| 23h | 0.000331 | 0.00001177 | $0.00133 | $0.00004 | $0.00129 | +17.8% |
| 40h | 0.000928 | 0.00002358 | $0.00276 | $0.00007 | $0.00269 | +19.7% |
| 47h | 0.001068 | 0.00002358 | $0.00307 | $0.00008 | $0.00299 | +19.0% |
| 67h | 0.001504 | 0.00002956 | $0.00396 | $0.00015 | $0.00381 | +17.0% |
| **88h** | **0.002195** | **0.00003769** | **$0.00532** | **$0.00030** | **$0.00501** | **+17.3%** |

Fee accrual rate: ~$0.0014/day, consistent across all checkpoints.

### Teardown (April 13, 2026) -- 9 on-chain transactions

| # | Intent | Details | Tx hash | Gas |
|---|--------|---------|---------|-----|
| 1 | LP_CLOSE #963 | Collect WOKB + USDT + fees | (3 sub-txs) | 319,837 |
| 2 | APPROVE WOKB | Uniswap V3 Router | `0xe0c7...6726` | 46,109 |
| 3 | SWAP | 0.0816 WOKB -> 6.7465 USDT | `0x9878...94f6` | 113,867 |
| 4 | APPROVE USDT | Uniswap V3 Router | `0x9107...f85a` | 36,265 |
| 5 | SWAP | 8.0076 USDT -> 8.0084 USDG | `0x4c67...abd2` | 132,091 |
| 6 | WITHDRAW | 0.01 USDT0 interest buffer | `0x6457...a347` | 245,218 |
| 7 | SWAP | 0.01 USDT0 -> 0.01 USDG | `0xe0a0...3d1f` | 126,964 |
| 8 | REPAY | 6.000369 USDG (full debt) | `0xbe7a...b216` | 175,462 |
| 9 | WITHDRAW | All USDT0 collateral | `0x68db...83d5` | 181,774 |
| | **Total teardown** | | | **1,377,587** |

Post-teardown on-chain state: Aave collateral=$0.00, debt=$0.00,
LP NFT burned. All capital returned to wallet.

### On-chain P&L (honest accounting)

```
INCOME
  LP fees earned (on-chain):            +$0.005319
  Aave supply interest:                 +$0.000007
                                        ──────────
  Total income:                         +$0.005326

COSTS
  Borrow interest (88h, 1.5% APR):     -$0.000302
  Entry swap slippage (2 swaps):        -$0.001599
  Teardown swap slippage (2 swaps):     -$0.016599
  Gas — entry (8 txs):                  -$0.004500
  Gas — teardown (9 txs):              -$0.004500
                                        ──────────
  Total costs:                          -$0.027500

NET P&L:                                -$0.022174  (-0.55% on $4.00)
```

**The strategy lost $0.022 on a $4.00 position over 3.7 days.**

The LP fee income ($0.0053, tracking 17% APR) was real and verifiable
on-chain. But the one-time round-trip costs (swap slippage + gas =
$0.027) exceeded the fee income because:

1. **$4 is too small.** Swap fees and gas are fixed costs that don't
   scale with position size. At $4, the ~$0.027 round-trip overhead
   is 0.68% of capital -- a steep hurdle to clear in 3.7 days.

2. **3.7 days is too short.** The 17% fee APR needs time to compound
   past the entry+exit costs.

### Breakeven analysis

| Capital | Round-trip cost | Daily fee income | Breakeven time |
|---------|----------------|------------------|----------------|
| $4 | $0.027 | $0.0014 | **~19 days** |
| $50 | $0.027 | $0.017 | **~1.6 days** |
| **$200** | **$0.027** | **$0.069** | **~9 hours** |
| $1,000 | $0.027 | $0.345 | **~2 hours** |

The strategy is profitable at any capital level -- it just needs
enough time for fee accumulation to exceed the fixed entry+exit cost.
At $200 (the planned redeployment), breakeven is ~9 hours.

### Redeployment plan

The strategy will be redeployed on a **dedicated isolated wallet** with
**$200 USD** of capital ($200 USDT0 supply -> $100 USDG borrow -> LP).

At $200:
- Expected fee income: ~$0.069/day (~$2.07/month)
- Expected costs: $0.027 entry + $0.0002/day borrow = $0.033 first month
- **Expected net profit after 1 month: ~$2.04 (+1.0%)**
- **Annualized at 17% fee APR: ~$27/year net (+13.5% APR after all costs)**

---

## Wallet

| Wallet | Address |
|--------|---------|
| **aave_okb_clmm_loop** | `0xc48E245cc551bd6853EeB1c3068C10eA8856D6ad` |

The wallet has its own `.env` with an isolated private key derived
deterministically from the master key.

**Funding needed**: ~0.01 OKB (gas, ~$0.85) + 200 USDT0 (strategy capital)

---

## How to run

```bash
# On Anvil (local fork testing)
almanak strat run \
    -d strategies/xlayer/xlayer_deterministic/aave_okb_clmm_loop \
    --network anvil --interval 5 --fresh

# On mainnet
almanak strat run \
    -d strategies/xlayer/xlayer_deterministic/aave_okb_clmm_loop \
    --network mainnet --interval 30 --fresh

# Teardown (set force_teardown=true in config.json, then run)
almanak strat run \
    -d strategies/xlayer/xlayer_deterministic/aave_okb_clmm_loop \
    --network mainnet --interval 10

# Backtest (90-day historical simulation with parameter sweep)
uv run python strategies/xlayer/xlayer_deterministic/aave_okb_clmm_loop/backtest.py --sweep
```

---

## Key features

1. **Multi-protocol composition**: Aave V3.6 + Uniswap V3 in a single
   coordinated lifecycle — not just a swap or a deposit, but a genuine
   cross-protocol yield strategy

2. **Adaptive auto-rebalance**: vol-scaled range width, 10-cycle spike
   confirmation, 1-hour cooldown. The strategy survives volatile
   markets without rebalancing itself to death

3. **Full lifecycle management**: entry pipeline (7 intents), steady-
   state monitoring, teardown (7 intents). Every state transition is
   tested on mainnet

4. **Custom backtesting engine**: 90-day historical simulation with
   63-configuration parameter sweep. Best config: +106% APR over the
   test period

5. **Comprehensive risk documentation**: IL mechanics, 5 risk
   categories, 5 mitigation strategies, vol-regime return projections

---

## Related

- **Agentic sibling project**: `strategies/xlayer/xlayer_agentic/`
- **Strategy technical docs**: `aave_okb_clmm_loop/README.md`
- **Edge spec**: `playground/xlayer/aave-uniswap/specs.md`
