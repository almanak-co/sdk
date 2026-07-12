# Getting Started

This guide walks you through installing the Almanak SDK, scaffolding your first strategy, and running it locally on an Anvil fork -- no wallet or API keys required.

## Prerequisites

- **Python 3.12+**
- **uv** (Python package manager):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

- **Foundry** (provides Anvil for local fork testing):

```bash
curl -L https://foundry.paradigm.xyz | bash
foundryup
```

## Installation

```bash
pipx install almanak
```

Need the web dashboard or backtest charts/optimization? Install the extras:
`pipx install 'almanak[dashboard,backtest]'`.

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv tool install almanak
```

This installs the `almanak` CLI globally. Each scaffolded strategy also has `almanak` as a local dependency in its own `.venv/` -- this two-install pattern is standard (same as CrewAI, Dagster, etc.).

**Using an AI coding agent?** Teach it the SDK in one command:

```bash
almanak agent install
```

This auto-detects your platform (Claude Code, Codex, Cursor, Copilot, and [6 more](agent-skills.md)) and installs the strategy builder skill.

## 1. Get a Strategy

### Option A: Copy a working demo (recommended for beginners)

```bash
almanak strat demo
```

This shows an interactive menu of working demo strategies. Pick one and it gets copied into your current directory, ready to run. You can also skip the menu:

```bash
almanak strat demo --name uniswap_rsi
```

### Option B: Scaffold from a template

```bash
almanak strat new
```

Follow the interactive prompts to pick a template, chain, and name. This creates a **self-contained Python project** with:

- `strategy.py` - Your strategy implementation with `decide()` method
- `config.json` - Runtime parameters (tokens, thresholds, funding)
- `pyproject.toml` - Dependencies and `[tool.almanak]` metadata
- `uv.lock` - Locked dependencies (created by `uv sync`)
- `.venv/` - Per-strategy virtual environment (created by `uv sync`)
- `.env` - Environment variables (fill in your keys later)
- `.gitignore` - Git ignore rules
- `.python-version` - Python version pin (3.12)
- `__init__.py` - Package exports
- `tests/` - Test scaffolding
- `AGENTS.md` - AI agent guide

The scaffold runs `uv sync` automatically to install dependencies. To add extra packages later:

```bash
uv add pandas-ta          # Updates pyproject.toml + uv.lock + .venv/
uv run pytest tests/ -v   # Run tests in the strategy's venv
```

## 2. Run on a Local Anvil Fork

The fastest way to test your strategy -- no wallet keys, no real funds, no risk:

```bash
cd my_strategy
almanak strat run --network anvil --once
```

This command automatically:

1. **Starts an Anvil fork** of the chain specified in your strategy (free public RPCs are used by default)
2. **Uses a default Anvil wallet** -- no `ALMANAK_PRIVATE_KEY` needed
3. **Starts the gateway** sidecar in the background
4. **Funds your wallet** with tokens listed in `anvil_funding` (see below)
5. **Runs one iteration** of your strategy's `decide()` method

### Wallet Funding on Anvil

Add an `anvil_funding` block to your `config.json` to automatically fund your wallet when the fork starts:

```json
{
    "anvil_funding": {
        "ETH": 10,
        "USDC": 10000,
        "WETH": 5
    }
}
```

Native tokens (ETH, AVAX, etc.) are funded via `anvil_setBalance`. ERC-20 tokens are funded via storage slot manipulation. This happens automatically each time the fork starts.

### Better RPC Performance (Optional)

Free public RPCs work but are rate-limited. For faster forking, set an Alchemy key in your `.env`:

```bash
ALCHEMY_API_KEY=your_alchemy_key
```

This auto-constructs RPC URLs for all supported chains. Any provider works -- see [Environment Variables](environment-variables.md) for the full priority order.

## 3. Run on Mainnet

!!! warning
    Mainnet execution uses **real funds**. Start with small amounts and use a dedicated wallet.

To run against live chains, you need a wallet private key in your `.env`:

```bash
# .env
ALMANAK_PRIVATE_KEY=0xYOUR_PRIVATE_KEY

# RPC access (pick one)
ALCHEMY_API_KEY=your_alchemy_key
# or: RPC_URL=https://your-rpc-provider.com/v1/your-key
```

Then run without the `--network anvil` flag:

```bash
almanak strat run --once
```

!!! tip
    Test with `--dry-run` first to simulate without submitting transactions:

    ```bash
    almanak strat run --dry-run --once
    ```

See [Environment Variables](environment-variables.md) for the full list of configuration options including protocol-specific API keys.

!!! info "Before going live"
    - Always run `--dry-run --once` before your first live execution to verify intent compilation
      without submitting transactions.
    - If swaps revert with "Too little received", switch from `amount_usd=` to `amount=` (token
      units). `amount_usd=` relies on the gateway price oracle for USD-to-token conversion, which
      may diverge from the DEX price.
    - Start with small amounts and monitor the first few iterations. The deployment ID is derived
      deterministically from your wallet and chain, so a restart resumes the same run automatically.

## Strategy Structure

A strategy implements the `decide()` method, which receives a `MarketSnapshot` and returns an `Intent`:

```python
from decimal import Decimal
from almanak import IntentStrategy, Intent, MarketSnapshot

class MyStrategy(IntentStrategy):
    def decide(self, market: MarketSnapshot) -> Intent | None:
        price = market.price("ETH")
        balance = market.balance("USDC")

        if price < Decimal("2000") and balance.balance_usd > Decimal("500"):
            return Intent.swap(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("500"),
            )
        return Intent.hold(reason="No opportunity")
```

### Strategy Metadata (`@almanak_strategy`)

The `@almanak_strategy` decorator attaches metadata used by the framework, CLI, and hosted platform:

```python
@almanak_strategy(
    name="my_strategy",                  # Unique identifier
    description="What it does",          # Human-readable description
    version="1.0.0",
    tags=["trading", "rsi"],             # Optional tags for discovery
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],       # Every intent the strategy may return
    default_chain="arbitrum",
    quote_asset="USD",                   # Asset performance is measured in
)
class MyStrategy(IntentStrategy): ...
```

#### Quote asset (performance denomination)

`quote_asset` declares the asset your strategy's performance (PnL / ROI) is measured in. It
defaults to **USD** and is definition-only: the hosted platform reads it for performance
reporting; it does not change valuation or execution behaviour. Declare it explicitly so the
choice is visible.

- **USD:** `quote_asset="USD"`. Correct for LP strategies (any pair), USD/stable lending and
  vault yield, delta-neutral and basis trades, USD-collateral perps, and TA swaps that trade
  for USD profit.
- **Token:** `quote_asset={"type": "token", "chain_id": <int>, "address": "0x..."}`. Only for
  strategies whose goal is to grow a quantity of that token — pure accumulators, native-asset
  or liquid staking, same-asset-family leverage loops (e.g. wstETH collateral / WETH borrow),
  and token-denominated yield (e.g. Pendle YT on wstETH). Use a **numeric `chain_id`**, never
  a chain name, and represent native gas tokens by their wrapped ERC-20 (ETH→WETH, MNT→WMNT;
  Solana uses `chain_id: 0` with WSOL).

`quote_asset` is distinct from `quote_token` (a trading-pair leg in config). It can also be
overridden per-deployment in `config.json` (`"quote_asset": "USD"` or the token object); the
value is frozen at boot and is not hot-reloadable.

## Available Intents

| Intent | Description |
|--------|-------------|
| `SwapIntent` | Token swaps on DEXs |
| `HoldIntent` | No action, wait for next cycle |
| `LPOpenIntent` | Open liquidity position |
| `LPCloseIntent` | Close liquidity position |
| `BorrowIntent` | Borrow from lending protocols |
| `RepayIntent` | Repay borrowed assets |
| `DeleverageIntent` | Emergency repay triggered by risk management. Factory: `Intent.deleverage()` |
| `SupplyIntent` | Supply to lending protocols |
| `WithdrawIntent` | Withdraw from lending protocols |
| `StakeIntent` | Stake tokens |
| `UnstakeIntent` | Unstake tokens |
| `PerpOpenIntent` | Open perpetuals position |
| `PerpCloseIntent` | Close perpetuals position |
| `FlashLoanIntent` | Flash loan operations _(experimental — pending testing; not listed in `almanak info matrix`)_ |
| `CollectFeesIntent` | Collect LP fees |
| `PredictionBuyIntent` | Buy prediction market shares _(experimental — pending testing; not listed in `almanak info matrix`)_ |
| `PredictionSellIntent` | Sell prediction market shares _(experimental — pending testing; not listed in `almanak info matrix`)_ |
| `PredictionRedeemIntent` | Redeem prediction market winnings _(experimental — pending testing; not listed in `almanak info matrix`)_ |
| `VaultDepositIntent` | Deposit into a vault |
| `VaultRedeemIntent` | Redeem from a vault |
| `WrapNativeIntent` | Wrap native tokens (e.g., ETH to WETH). Factory: `Intent.wrap()` |
| `UnwrapNativeIntent` | Unwrap native tokens (e.g., WETH to ETH). Factory: `Intent.unwrap()` |
| `Intent.bridge()` | Bridge tokens cross-chain (factory method returning a composite intent) |
| `Intent.ensure_balance()` | Ensure minimum token balance on a target chain (factory method resolving to a bridge or hold) |
| `Intent.sequence()` | Atomic multi-step composite (`IntentSequence`) executing child intents in order with shared rollback semantics |

## State Persistence (Required for Stateful Strategies)

The framework automatically persists runner-level metadata (iteration counts, error counters) after each iteration. However, **strategy-specific state** -- position IDs, trade counts, phase tracking, cooldown timers -- is only saved if you implement two hooks:

```python
from typing import Any
from decimal import Decimal

class MyStrategy(IntentStrategy):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._position_id: int | None = None
        self._trades_today: int = 0

    def get_persistent_state(self) -> dict[str, Any]:
        """Return state to save. Called after each iteration."""
        return {
            "position_id": self._position_id,
            "trades_today": self._trades_today,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Restore state on startup. Called when resuming a run."""
        self._position_id = state.get("position_id")
        self._trades_today = state.get("trades_today", 0)
```

Without these hooks, your strategy will lose all internal state on restart. This is especially dangerous for LP strategies where losing the `position_id` means the strategy cannot close its own positions.

!!! warning "What gets lost without persistence"
    If you store state in instance variables (e.g., `self._position_id`) but don't implement `get_persistent_state()` and `load_persistent_state()`, that state is lost when the process stops. On restart, your strategy starts from scratch with no memory of open positions, completed trades, or internal phase.

!!! tip "Tips"
    - Use defensive `.get()` with defaults in `load_persistent_state()` so older state dicts don't crash on missing keys.
    - Store `Decimal` values as strings (`str(amount)`) and parse them back (`Decimal(state["amount"])`) for safe JSON round-tripping.
    - The `on_intent_executed()` callback is the natural place to update state after a trade (e.g., storing a new position ID), and `get_persistent_state()` then picks it up for saving.
    - Persist identity and phase (position IDs, cooldowns, workflow step) — not market exposure. Values that feed `decide()` triggers (debt, exposures, hedge deltas) should be re-read from the market snapshot each cycle: cached intent-derived amounts drift from on-chain reality as interest accrues and prices move.
    - Persisted cooldown/cadence timestamps must be **market-clock** values (`market.timestamp`), never wall-clock — see [Time in Strategies](#time-in-strategies).

## Time in Strategies

Every time-based rule in a strategy — cooldowns, trade cadences, daily counters, "N hours since last fill" — must be computed from **`market.timestamp`**, the snapshot's clock. Never call `datetime.now()` (or `time.time()`) inside `decide()` or `on_intent_executed()` for anything that feeds a decision.

Why: in live trading the two clocks agree, so wall-clock code *appears* to work. In a backtest, weeks of simulated time replay in minutes of wall time — a 24-hour cooldown measured against `datetime.now()` never expires, so the strategy trades once and holds forever. The bug is invisible in unit tests and single-iteration smoke runs; it only surfaces as a silently degenerate backtest.

`on_intent_executed()` receives no market snapshot, so capture the snapshot's timestamp in `decide()` and reuse the captured value when stamping state in the callback:

```python
class MyStrategy(IntentStrategy):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._last_buy_ts: datetime | None = None
        self._pending_ts: datetime | None = None

    def decide(self, market: MarketSnapshot) -> Intent:
        now = market.timestamp  # simulated time in backtests, real time live
        if self._last_buy_ts and now - self._last_buy_ts < timedelta(hours=24):
            return Intent.hold(reason="Cadence: waiting for next 24h window")
        self._pending_ts = now  # capture for the fill callback
        return Intent.swap(...)

    def on_intent_executed(self, intent, success, result) -> None:
        if success and self._pending_ts is not None:
            self._last_buy_ts = self._pending_ts  # market clock, NOT datetime.now()
            self._pending_ts = None
```

Wall-clock time is acceptable only for **reporting** fields that never feed a decision (e.g. the `timestamp` on a `TeardownPositionSummary`, log lines, dashboard "generated at" labels).

!!! warning "Unit tests can hide this bug"
    A test that monkeypatches a `_now()` helper on the strategy validates the internal logic while hiding the clock-domain defect. Drive time through the snapshot instead: build test snapshots with `almanak.framework.market.testing.seeded(timestamp=...)` and advance the `timestamp` between calls to `decide()` — see [Unit Testing Strategies](#unit-testing-strategies).

## Strategy Teardown (Required)

Every strategy must implement teardown so operators can safely close positions. Without teardown, close-requests are silently ignored and positions remain open. The `almanak strat new` templates include stubs -- fill them in as you build your strategy.

```python
class MyStrategy(IntentStrategy):
    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Query on-chain state and return open positions."""
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary
        # ... return TeardownPositionSummary with your positions

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Return ordered intents to unwind all positions."""
        from almanak.framework.teardown import TeardownMode
        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")
        return [Intent.swap(from_token="WETH", to_token="USDC", amount="all", max_slippage=max_slippage)]
```

If your strategy holds multiple position types, close them in order: **perps -> borrows -> supplies -> LPs -> tokens**. See the [Teardown CLI](cli/strat-teardown.md) for how operators trigger teardown.

Two contract points that are easy to get wrong:

- **Teardown is one-shot.** The list returned by `generate_teardown_intents()` is the *entire* unwind plan — the framework executes it and stops. There are no follow-up `decide()` iterations, and no "next pass" that continues a partial unwind. A strategy that returns only its LP close and assumes the borrow/supply legs get repaid "on a subsequent iteration" strands live debt and collateral. If exact downstream amounts aren't knowable up front (e.g. how much WETH an LP close returns), use `amount="all"` / `repay_full=True` style intents rather than deferring legs to an iteration that will never run. For leveraged positions, `almanak.framework.teardown.generate_lending_unwind(...)` builds the sanctioned repay-and-withdraw sequence — compose it: `[lp_close_intent, *generate_lending_unwind(...)]`.
- **Report every leg in `get_open_positions()`.** A leveraged LP strategy holds three positions — the LP (`PositionType.LP`), the debt (`PositionType.BORROW`), and the collateral (`PositionType.SUPPLY`) — and all of them belong in the `TeardownPositionSummary`. The framework's teardown-completeness verification compares closed positions against this summary; omitting the lending legs blinds that check, and a half-unwound position gets reported as cleanly COMPLETED.

## Unit Testing Strategies

Build test snapshots with the real `MarketSnapshot` seeding API — not `unittest.mock.MagicMock`:

```python
from decimal import Decimal
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from almanak.framework.market.testing import seeded

def test_cadence_gates_second_buy(strategy):     # `strategy` = your fixture
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    market = seeded(
        chain="base",
        prices={"WETH": Decimal("2000")},
        timestamp=t0,
    )
    intent = strategy.decide(market)             # first buy fires
    assert intent.intent_type.value == "SWAP"
    result = SimpleNamespace(swap_amounts=None)  # minimal execution result
    strategy.on_intent_executed(intent, True, result)

    later = seeded(chain="base", prices={"WETH": Decimal("2000")}, timestamp=t0 + timedelta(hours=1))
    assert strategy.decide(later).intent_type.value == "HOLD"   # cadence gate holds
```

`seeded(...)` accepts `prices`, `balances` (real `TokenBalance` objects), `indicators` (real `RSIData` / `MACDData` / `BollingerBandsData`, keyed `"TOKEN:indicator:period:timeframe"`), and a `timestamp`. Individual `seed_price(...)` / `seed_balance(...)` / `seed_rsi(...)` calls on the snapshot cover the rest.

Why not `MagicMock`? A mocked market answers *any* method call — a misspelled accessor, a wrong keyword argument, or an API that doesn't exist all pass green, so the tests validate nothing about your integration with the SDK. And mocking time helpers on the strategy (instead of driving `timestamp` through the snapshot) hides wall-clock bugs that break backtests — see [Time in Strategies](#time-in-strategies). Reserve `MagicMock` for execution-result objects in `on_intent_executed()` tests, where no seeding API exists.

## Generating Permissions (Safe Wallets)

When deploying a strategy through a Safe wallet with Zodiac Roles restrictions, the agent needs an explicit set of contract permissions. The SDK can generate this manifest automatically by inspecting which contracts and function selectors your strategy's intents compile to:

```bash
# From your strategy directory
almanak strat permissions

# Explicit directory
almanak strat permissions -d almanak/demo_strategies/uniswap_rsi

# Override chain
almanak strat permissions --chain base

# Write to file
almanak strat permissions -o permissions.json
```

The command reads `supported_protocols` and `intent_types` from your `@almanak_strategy` decorator, compiles synthetic intents through the real compiler, and extracts the minimum set of contract addresses and function selectors needed. The output is a JSON manifest you can apply to a Zodiac Roles module. If the strategy supports multiple chains, the output is a JSON array with one manifest per chain; use `--chain` to generate for a single chain.

!!! note "Only for Safe/Zodiac deployments"
    Permission manifests are only needed when running through a Safe wallet with Zodiac Roles. For local Anvil testing or direct-key execution, no permissions are required.

!!! note "Backtest CLI"
    Unlike `almanak strat run` which auto-discovers the strategy from the current directory,
    backtest commands require an explicit strategy name: `almanak strat backtest pnl -s my_strategy`.
    Use `--list-strategies` to see available strategies.

## Next Steps

- [Environment Variables](environment-variables.md) - All configuration options
- [API Reference](api/index.md) - Full Python API documentation
- [CLI Reference](cli/almanak.md) - All CLI commands
- [Gateway API](gateway/api-reference.md) - Gateway gRPC services

## Want an LLM to Make the Decisions?

The SDK also supports **agentic strategies** where an LLM autonomously decides
what to do using Almanak's 39 built-in tools. Instead of writing `decide()` logic
in Python, you write a system prompt and let the LLM reason over market data.

This approach requires **your own LLM API key** (OpenAI, Anthropic, or any
OpenAI-compatible provider).

| | Deterministic (this guide) | Agentic |
|---|---|---|
| **You write** | Python `decide()` method | System prompt + policy |
| **Decision maker** | Your code | LLM (GPT-4, Claude, etc.) |
| **Requires** | Just the SDK | SDK + LLM API key |
| **Best for** | Known rules, quantitative signals | Complex reasoning, multi-step plans |

Both paths share the same gateway, connectors, and execution pipeline.

**Get started:** [Agentic Trading Guide](agentic/index.md)
