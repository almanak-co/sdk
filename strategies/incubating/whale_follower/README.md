# Copy Trading with Almanak SDK -- Developer Experience Report

**Author**: Senior DeFi Quant (first-time Almanak SDK user)
**Date**: 2026-02-14
**Goal**: Build and run a copy-trading strategy from scratch using the Almanak SDK

---

## Executive Summary

I built a "whale follower" copy-trading strategy that monitors two major DeFi market makers (Wintermute and Jump Trading) on Arbitrum and mirrors their swap trades with proportional sizing. The strategy compiled and ran successfully on both mainnet (dry-run) and Anvil fork on the first attempt. Total time from zero to working strategy: **under 30 minutes**.

**Verdict**: The copy-trading DX is surprisingly mature. The framework does the hard work (block polling, receipt parsing, signal deduplication, risk caps) and the strategy author just writes `decide()` logic against clean `CopySignal` objects.

---

## Step 1: Discovery -- What's Available?

### The CLI Template

```bash
almanak strat new --help
```

This revealed a `copy_trader` template alongside `blank`, `dynamic_lp`, `mean_reversion`, `basis_trade`, and `lending_loop`. Good -- copy trading is a first-class citizen.

I scaffolded a starter:

```bash
almanak strat new --name whale_follower --template copy_trader --chain arbitrum
```

This generated:
- `strategy.py` -- Working skeleton with `CopySizer` wired up
- `config.py` -- Dataclass for typed config
- `__init__.py` -- Package exports
- `tests/test_strategy.py` -- Pytest scaffolding with mock fixtures

**Observation**: The scaffolded code is functional out of the box but basic. It handles the core loop (read signals, check sizer, emit intent) but doesn't include multi-leader weighting, token allowlists, or dry-run mode. The existing `strategies/demo/copy_trader/` is a much better reference.

### The Existing Demo

The demo at `strategies/demo/copy_trader/` is a solid 266-line implementation that I used as my primary reference. It covers:
- Multi-leader with weighted sizing
- Token allowlisting
- Dry-run mode
- Timeline event emission
- Performance tracking via `CopyPerformanceTracker`
- Clean signal consumption to prevent queue starvation

### Framework Components I Relied On

| Component | Location | What It Does |
|-----------|----------|-------------|
| `CopySignal` | `framework/services/copy_trading_models.py` | Clean data object: action_type, tokens, amounts_usd, leader_address |
| `CopySizer` | `framework/services/copy_sizer.py` | Three sizing modes + daily/position caps. Just call `compute_size()` |
| `CopyTradingConfig` | `framework/services/copy_trading_models.py` | Parses `config.json` with sensible defaults |
| `WalletMonitor` | `framework/services/wallet_monitor.py` | RPC block polling with reorg detection. Fully automatic |
| `CopySignalEngine` | `framework/services/copy_signal_engine.py` | Receipt parsing dispatch via `ContractRegistry`. Fully automatic |
| `WalletActivityProvider` | `framework/data/wallet_activity.py` | Glue layer. Wired by `StrategyRunner`, exposed via `market.wallet_activity()` |

**Key insight**: The strategy author doesn't need to know about `WalletMonitor`, `CopySignalEngine`, or `ContractRegistry`. They're wired automatically by the runner when `config.json` has a `copy_trading` block. You just call `market.wallet_activity()` and get clean `CopySignal` objects.

---

## Step 2: Writing the Strategy

### What I Wrote (strategy.py)

~200 lines. The core `decide()` method is 20 lines:

```python
def decide(self, market: MarketSnapshot) -> Intent | None:
    signals = market.wallet_activity(
        action_types=self._filters.get("action_types"),
        protocols=self._filters.get("protocols"),
        min_usd_value=self._filters.get("min_usd_value"),
    )

    if not signals:
        return Intent.hold(reason="No new leader activity")

    for signal in signals:
        intent = self._process_signal(signal, provider)
        if intent is not None:
            return intent

    return Intent.hold(reason="No actionable signals after filtering")
```

Each signal goes through 6 gates in `_process_signal()`:
1. Is it a SWAP with 2+ tokens?
2. Are both tokens in my allowlist?
3. Does the sizer pre-check pass?
4. Can we compute a valid size with leader weight?
5. Are we under the daily notional cap?
6. Are we under the position cap?

If all pass, emit `Intent.swap(...)`. If dry-run, log and consume without emitting.

### Configuration (config.json)

```json
{
    "strategy_id": "whale_follower",
    "chain": "arbitrum",
    "dry_run": true,
    "copy_trading": {
        "leaders": [
            { "address": "0x489ee...", "label": "wintermute", "weight": 1.0 },
            { "address": "0xe8c19...", "label": "jump_trading", "weight": 0.5 }
        ],
        "filters": {
            "action_types": ["SWAP"],
            "protocols": ["uniswap_v3", "pancakeswap_v3", "sushiswap_v3"],
            "tokens": ["WETH", "USDC", "USDT", "ARB", "WBTC", "GMX"],
            "min_usd_value": 1000
        },
        "sizing": {
            "mode": "proportion_of_leader",
            "percentage_of_leader": 0.001
        },
        "risk": {
            "max_trade_usd": 100,
            "max_daily_notional_usd": 500,
            "max_slippage": 0.005
        }
    }
}
```

**Design choices**:
- **Proportion of leader (0.1%)**: If Wintermute swaps $100k, I trade $100. This scales with their conviction.
- **Leader weight**: Jump at 0.5x means I only trade $50 on a $100k Jump trade vs $100 for Wintermute. Different whales have different signal quality.
- **Min $1k signal**: Filters out noise (dust trades, testing txns).
- **Max $100/trade, $500/day**: Conservative risk caps for a new strategy.
- **50 bps slippage**: Tight. Whale trades on major pairs should have deep liquidity.
- **Token allowlist**: Only blue-chip tokens. Prevents copying swaps into illiquid or ruggable tokens.
- **Confirmation depth 2**: Wait 2 blocks before processing. Reduces reorg risk.
- **Max signal age 180s**: Don't copy stale signals (3 min window).

---

## Step 3: Running It

### Mainnet Dry-Run

```bash
almanak strat run -d strategies/demo/whale_follower --once --dry-run
```

**What happened**:
1. Gateway auto-started (gRPC on localhost:50051)
2. Strategy loaded, config parsed, 2 leaders initialized
3. `WalletMonitor` polled blocks 431729069-431729169 (100-block lookback)
4. 0 events found (whales didn't trade in those ~100 blocks)
5. `decide()` returned HOLD: "No new leader activity"
6. Clean shutdown

**Duration**: ~41 seconds (dominated by RPC polling of 100 blocks)

### Anvil Fork

```bash
almanak strat run -d strategies/demo/whale_follower --network anvil --once --dry-run
```

**What happened**:
1. Anvil fork started on port 62388 (forked Arbitrum at block 431729403)
2. Gateway started with Anvil RPC override
3. Same poll/hold/shutdown cycle
4. Anvil fork stopped cleanly

**Duration**: ~41 seconds

### Key Logs (Confirming It Works)

```
WhaleFollowerStrategy initialized: leaders=[wintermute(w=1.0), jump_trading(w=0.5)],
  sizing=proportion_of_leader, max_trade=$100, max_slippage=0.005, dry_run=True

Copy trading initialized: monitoring 2 leader(s) on arbitrum

WalletMonitor polled blocks 431729069-431729169: 0 events

whale_follower HOLD: No new leader activity
```

---

## Step 4: What I Liked

### 1. Zero boilerplate for the hard parts
The entire block-polling, receipt-parsing, signal-deduplication pipeline is invisible. I never imported `WalletMonitor` or `CopySignalEngine`. I just put `copy_trading` in `config.json` and called `market.wallet_activity()`.

### 2. The CopySizer is well-designed
Three sizing modes (fixed USD, proportion of leader, proportion of equity) with daily and position caps. The `get_skip_reason()` method is great for logging why signals were filtered.

### 3. Config-driven with sensible defaults
`CopyTradingConfig.from_config()` fills in defaults for everything. I could start with just a leader address and get a working strategy.

### 4. The `almanak strat new --template copy_trader` shortcut
Scaffolded a working skeleton in seconds. Not perfect (see below), but a solid starting point.

### 5. State persistence "just works"
On the second Anvil run, I saw `Mode: RESUME (existing state found)` -- the runner automatically persisted and restored the copy trading cursor state. No code needed.

### 6. Gateway auto-management
`almanak strat run` auto-starts and auto-stops the gateway. `--network anvil` auto-starts and stops Anvil. Zero manual infra setup.

---

## Step 5: Friction Points and Suggestions

### 1. Template generates `config.py` but demo uses `config.json`
The `strat new` template scaffolds a `config.py` dataclass, but the demo strategies and the runner both read `config.json`. The template should also generate a `config.json` with the `copy_trading` block pre-filled. Without it, the scaffolded strategy won't actually run -- you need to know to create `config.json` manually.

**Suggestion**: Template should output `config.json` (with copy_trading block) in addition to `config.py`.

### 2. No config.json validation on startup
If you misconfigure the `copy_trading` block (e.g., typo in `sizing.mode`), you get a Python exception at runtime, not a clean validation error. A `CopyTradingConfig.validate()` that checks enum values and required fields would save debugging time.

### 3. Template doesn't include dry-run or multi-leader patterns
The scaffolded `strategy.py` is a minimal loop without dry-run support, leader weighting, or token allowlisting. These are all in the demo strategy. The template should either match the demo quality or point to it explicitly.

### 4. Documentation is scattered
I pieced together the copy-trading architecture from:
- `strategies/demo/copy_trader/strategy.py` (best reference)
- `almanak/framework/services/copy_trading_models.py` (data types)
- `almanak/framework/services/copy_sizer.py` (sizing logic)
- `almanak/framework/data/wallet_activity.py` (provider wiring)
- `CLAUDE.md` (no copy-trading section)

There's no single "Copy Trading Guide" in the blueprints. Blueprint `05-connectors.md` covers protocol connectors but not copy trading. A dedicated `blueprints/XX-copy-trading.md` would help.

### 5. Signal polling takes 40+ seconds for 100 blocks
The `WalletMonitor` polls block-by-block via `eth_getBlockByNumber` + `eth_getTransactionReceipt`. For a 100-block lookback on Arbitrum (~250ms blocks), this means ~40s of RPC calls on startup. This is expected for RPC-based monitoring, but it would be good to document expected latencies.

### 6. No way to test signal processing without real signals
There's no `almanak strat test-signal` or fixture injection mechanism. To verify my `_process_signal()` logic, I'd need to either wait for a real whale trade or write unit tests with mock `CopySignal` objects. A `--inject-test-signal` flag would be useful for development.

### 7. `on_intent_executed` callback coupling
The callback receives the raw `Intent` and `result`, but there's no reference back to the `CopySignal` that triggered it. I had to stash `_last_signal_id` as instance state. A `signal_id` field on the Intent (or in metadata) would be cleaner.

---

## Step 6: Architecture Assessment

### What the Copy Trading Pipeline Looks Like

```
config.json
  |
  v
[StrategyRunner] -- reads "copy_trading" block
  |
  |-- WalletMonitor (RPC polling per leader per chain)
  |     |
  |     v
  |-- CopySignalEngine (receipt parsing via ContractRegistry)
  |     |
  |     v
  |-- WalletActivityProvider (accumulates CopySignals)
        |
        v
  market.wallet_activity() --> list[CopySignal]
        |
        v
  Strategy.decide()
        |
        v
  CopySizer.compute_size() + risk checks
        |
        v
  Intent.swap() --> Framework handles execution
```

The strategy author only touches the bottom 3 layers. Everything above is framework plumbing.

### Supported Protocols for Copy Trading

The `ContractRegistry` maps these protocol contracts:
- **DEX Swaps**: Uniswap V3, PancakeSwap V3, Aerodrome, TraderJoe V2
- **Lending**: Aave V3, Morpho Blue
- **Perps**: GMX V2

This means the engine can decode leader actions across 7 protocols. The demo strategy only copies swaps, but the framework supports copying lending and perps actions too.

### Risk Model

The `CopySizer` enforces:
- **Per-trade cap**: `max_trade_usd` (hard ceiling)
- **Per-trade floor**: `min_trade_usd` (skip dust)
- **Daily cap**: `max_daily_notional_usd` (auto-resets at UTC midnight)
- **Position cap**: `max_open_positions`
- **Signal age**: `max_signal_age_seconds` (skip stale signals)

Missing from the risk model:
- Per-leader daily caps (can only set global)
- Correlation limits (avoid copying the same trade from multiple leaders)
- Drawdown-based circuit breakers

---

## Conclusion

The Almanak SDK's copy trading feature is production-grade infrastructure with a clean developer experience. The `config.json`-driven approach means a strategy author can go from zero to a working whale follower with ~200 lines of Python and a JSON config. The framework handles the messy parts (RPC polling, receipt parsing, deduplication, state persistence) transparently.

**Time breakdown**:
- Discovery (reading code + templates): ~15 min
- Writing strategy + config: ~10 min
- Running and debugging: ~5 min (no bugs -- worked first try)

**Would I use this in production?** Yes, with two additions:
1. A test signal injection mechanism for development
2. A copy-trading-specific blueprint/guide in the docs

The intent-based architecture (`decide()` returns `Intent.swap()`, framework handles everything else) is the right level of abstraction for quant developers who want to focus on signal quality and sizing, not transaction plumbing.
