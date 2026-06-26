# Strategies

Strategy base classes and the market snapshot interface.

## Implementing Teardown

To support teardown, a strategy implements `get_open_positions()` and `generate_teardown_intents()` so operators can safely close positions. **Implementing `get_open_positions()` is what enables teardown** — the runner checks for that method. Without it, close-requests are silently ignored.

### Teardown Methods

| Method | Required? | Purpose |
|--------|-----------|---------|
| `get_open_positions()` | **Yes** | Return a `TeardownPositionSummary` listing all open positions. The runner gates teardown on the presence of this method. |
| `generate_teardown_intents(mode, market)` | **Yes** | Return an ordered list of `Intent` objects that unwind positions |
| `supports_teardown() -> bool` | Optional | Author-side convenience guard (return `False` to short-circuit your own teardown path). The runner does **not** gate on this flag — it is a convention many demos follow, not a framework requirement. |

### Execution Order

If your strategy holds multiple position types, teardown intents must follow this order:

1. **PERP** -- close perpetual positions first (highest risk)
2. **BORROW** -- repay borrows to free collateral
3. **SUPPLY** -- withdraw supplied collateral
4. **LP** -- close liquidity positions
5. **TOKEN** -- swap remaining tokens to stable

### Example: Swap Strategy Teardown

```python
from decimal import Decimal
from almanak import IntentStrategy, Intent

class MyStrategy(IntentStrategy):
    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from datetime import UTC, datetime
        from almanak.framework.teardown import (
            PositionInfo, PositionType, TeardownPositionSummary,
        )
        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "my_strategy"),
            timestamp=datetime.now(UTC),
            positions=[
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="my_strategy_eth",
                    chain=self.chain,
                    protocol="uniswap_v3",
                    value_usd=Decimal("1000"),  # query on-chain balance, not cache
                    details={"asset": "WETH"},
                )
            ],
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode
        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")
        return [
            Intent.swap(
                from_token="WETH", to_token="USDC",
                amount="all", max_slippage=max_slippage, protocol="uniswap_v3",
            )
        ]
```

!!! warning "Always query on-chain state"
    `get_open_positions()` must query live on-chain balances, not cached values. Stale data can cause teardown to skip positions or attempt to close positions that no longer exist.

## Testing with `force_action`

Real strategies gate their `decide()` output behind signals — RSI thresholds, MACD crosses, balance checks, cooldowns. That makes them hard to test on a forked block where those signals may not fire. The convention is a `force_action` config field that **bypasses signal gates** and lets you drive a known intent through the production code path.

```python
@dataclass
class MyStrategyConfig:
    # Production tuning
    rsi_oversold: int = 30
    rsi_overbought: int = 70
    trade_size_usd: Decimal = Decimal("100")
    # Testing affordance — empty in production
    force_action: str = ""

class MyStrategy(IntentStrategy):
    def __init__(self, config, ...):
        super().__init__(...)
        self.force_action = str(self.get_config("force_action", "") or "").lower()
        # ... other config fields ...

    def decide(self, market: MarketSnapshot) -> Intent:
        # Test affordance: short-circuit BEFORE any signal logic.
        if self.force_action:
            return self._forced_intent(market)

        # Production decide() begins here.
        rsi = market.indicators.rsi(self.base_token, ...)
        if rsi.value < self.rsi_oversold:
            return Intent.swap(...)
        return Intent.hold(reason="No signal")

    def _forced_intent(self, market: MarketSnapshot) -> Intent:
        """Map free-form force_action strings to concrete Intents.

        Values are strategy-specific. Pick names that match what each branch
        DOES (verbs: "buy", "sell", "open", "close", "supply"), not internal
        state machine values.
        """
        if self.force_action == "buy":
            return Intent.swap(
                from_token=self.quote_token,
                to_token=self.base_token,
                amount_usd=self.trade_size_usd,
                protocol=self.protocol,
                chain=self.chain,
            )
        if self.force_action == "sell":
            return Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount="all",
                protocol=self.protocol,
                chain=self.chain,
            )
        raise ValueError(f"Unknown force_action: {self.force_action!r}")
```

### Driving force_action from the CLI

The `almanak strat test` command mutates `force_action` between iterations and runs each through the production code path on a managed Anvil fork:

```bash
# Single forced action
almanak strat test --actions buy --teardown --json

# Multiple actions in sequence (one iteration each)
almanak strat test --actions open,collect_fees --teardown --json

# Teardown only (no force_actions)
almanak strat test --teardown --json
```

The CLI emits structured JSON: every step's `status`, `failure_logs`, and a `summary.all_passed` bool. See [CLI Reference: strat test](../cli/strat-test.md) for full flag docs.

### Authoring rules

| Rule | Why |
|---|---|
| **Values are free-form strings, strategy-specific.** Pick names that describe the action verb (`"buy"`, `"open"`, `"supply"`, `"close"`), not state-machine internals (`"phase_2"`, `"step_3a"`). | Anyone reading your code (or grepping `_forced_intent` to find supported values) should be able to map name → intent shape without context. |
| **One value per non-HOLD intent type.** If your strategy emits both `Intent.lp_open` and `Intent.swap`, expose both as force_action values (e.g. `"open"` and `"buy"`). | Each path needs to be drivable independently. Missing branches mean missing test coverage. |
| **Each branch must produce a non-HOLD Intent.** If a forced branch returns `Intent.hold(...)`, the test passes silently without exercising the path you wanted. | The whole point of force_action is to bypass gates — returning HOLD defeats it. |
| **Order matters; document prerequisites.** `force_action="close"` requires an open position; `force_action="collect_fees"` requires an LP NFT. | The CLI runs values in `--actions` order against one Anvil instance, so prereqs must be driven first. |
| **Skip values that overlap with `generate_teardown_intents()`.** If teardown emits an unwind swap, don't drive the same intent via `--actions` — the position will be closed before teardown runs. | `--teardown` is the canonical unwind path; double-driving it leaves teardown nothing to do. |
| **Empty default in config (`force_action: str = ""`).** Production deploys must run signal-gated logic; force_action is testing-only. | A non-empty default would bypass signals in production and execute on every iteration. |

### Example: LP strategy with three force_action values

```python
def _forced_intent(self, market: MarketSnapshot) -> Intent:
    if self.force_action == "open":
        # Opens an LP position.
        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=self._compute_lower(market),
            range_upper=self._compute_upper(market),
            protocol=self.protocol,
            chain=self.chain,
        )
    if self.force_action == "collect_fees":
        # Collects fees on an existing position. Drive AFTER "open".
        # NFT-based protocols (Uniswap V3 family, Uniswap V4, Aerodrome
        # Slipstream) require the position's tokenId via protocol_params.
        return Intent.collect_fees(
            pool=self.pool,
            protocol=self.protocol,
            chain=self.chain,
            protocol_params={
                "position_id": self.force_position_id or self._position_id,
            },
        )
    if self.force_action == "close":
        # Closes the position. SKIP this in --actions; teardown emits the same intent.
        return Intent.lp_close(
            pool=self.pool,
            position_id=self.force_position_id or self._position_id,
            protocol=self.protocol,
            chain=self.chain,
        )
    raise ValueError(f"Unknown force_action: {self.force_action!r}")
```

CLI to test the lifecycle without double-closing:

```bash
almanak strat test --actions open,collect_fees --teardown --json
# `close` is skipped from --actions because teardown emits Intent.lp_close
```

### Companion override fields

Strategies that track on-chain identifiers (NFT IDs, vault shares) often need a paired override so a forced action can target an existing position rather than the strategy's cached state:

```python
@dataclass
class MyLPConfig:
    force_action: str = ""
    force_position_id: str | None = None  # override the cached self._position_id

# In _forced_intent:
position_id = self.force_position_id or self._position_id
```

Only add fields like this when a forced branch genuinely needs them. A swap-only strategy doesn't track position IDs, so it shouldn't carry `force_position_id`.

### Common mistakes

- **Returning `Intent.hold()` from a forced branch** — the test passes vacuously, you haven't tested anything.
- **Using state-machine values as force_action names** (`"phase_b"`, `"step_3"`) — opaque to anyone trying to figure out what each value does.
- **Forgetting to short-circuit at the top of `decide()`** — if signal gates run before the force_action check, the gate's HOLD return value wins and the forced intent never fires.
- **Reusing values that teardown emits** — running `--actions close --teardown` unwinds the position twice (or once and then has nothing to test on teardown).
- **Setting `force_action` in the production config** — bypasses every safety gate in deployment. Always default to `""`.

## State Persistence

Strategies that track internal state across iterations (position IDs, phase tracking, trade counters) must implement two hooks so that state survives restarts.

The framework persists runner-level metadata automatically (iteration counts, consecutive errors, execution progress for multi-step intents). But **strategy-specific state is opt-in** -- without these hooks, instance variables are lost when the process stops.

### Required Hooks

| Method | Called | Purpose |
|--------|--------|---------|
| `get_persistent_state() -> dict` | After each iteration | Return a dict of state to save |
| `load_persistent_state(state: dict)` | On startup / resume | Restore state from the saved dict |

### Example: LP Strategy with Position Tracking

```python
from decimal import Decimal
from typing import Any
from almanak import IntentStrategy, Intent, MarketSnapshot

class MyLPStrategy(IntentStrategy):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._position_id: int | None = None
        self._has_position: bool = False
        self._total_fees_collected: Decimal = Decimal("0")

    def decide(self, market: MarketSnapshot) -> Intent | None:
        if self._has_position:
            # Uniswap V3 / V4 / Aerodrome Slipstream identify the position by
            # NFT tokenId — pass it through protocol_params, not as a
            # top-level argument.
            return Intent.collect_fees(
                pool="WETH/USDC/3000",
                protocol="uniswap_v3",
                protocol_params={"position_id": self._position_id},
            )
        return Intent.lp_open(
            pool="WETH/USDC/3000",
            amount0=Decimal("0.5"),
            amount1=Decimal("1000"),
            range_lower=Decimal("1800"),
            range_upper=Decimal("2200"),
            protocol="uniswap_v3",
        )

    def on_intent_executed(self, intent, success: bool, result):
        """Update state after execution -- persisted via get_persistent_state()."""
        if success and result.position_id:
            self._position_id = result.position_id
            self._has_position = True

    # -- State persistence hooks --

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "position_id": self._position_id,
            "has_position": self._has_position,
            "total_fees_collected": str(self._total_fees_collected),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        self._position_id = state.get("position_id")
        self._has_position = state.get("has_position", False)
        self._total_fees_collected = Decimal(state.get("total_fees_collected", "0"))
```

### What the Framework Persists Automatically

These are saved by the runner without any strategy-author code:

| State | Persisted to | Survives restart |
|-------|-------------|-----------------|
| Iteration count, success count, error count | `strategy_state` table | Yes |
| Multi-step execution progress (which step completed, serialized intents) | `strategy_state.execution_progress` | Yes -- runner resumes from the failed step |
| Operator pause flag | `strategy_state.is_paused` | Yes |
| Portfolio snapshots (total value, positions) | `portfolio_snapshots` table | Yes |
| Portfolio PnL baseline (initial value, deposits, gas) | `portfolio_metrics` table | Yes |
| Timeline events (execution audit trail) | `timeline_events` table | Yes |

### Guidelines

- **Use defensive `.get()` with defaults** in `load_persistent_state()` so older saved state doesn't crash when you add new fields.
- **Store `Decimal` as strings** (`str(amount)`) and parse back (`Decimal(state["amount"])`) for safe JSON round-tripping.
- **`on_intent_executed()` is the natural place to update state** after a trade (e.g., storing a position ID). `get_persistent_state()` then picks it up for the next save.
- **All values must be JSON-serializable.** The state dict is stored as a JSON blob in the database.

!!! warning "Without these hooks, strategy state is lost on restart"
    If you store state in instance variables but don't implement the persistence hooks, a restart means your strategy has no memory of open positions, completed trades, or internal phase. This is especially dangerous for LP and lending strategies where losing a position ID means the strategy cannot close its own positions.

## IntentStrategy

The primary base class for writing strategies. Implement the `decide()` method to return an `Intent`.

::: almanak.framework.strategies.IntentStrategy
    options:
      show_root_heading: true
      members_order: source

## StrategyBase

Lower-level base class for strategies that need direct action control.

::: almanak.framework.strategies.StrategyBase
    options:
      show_root_heading: true
      members_order: source

## RiskGuard

Non-bypassable risk validation that runs before every execution.

::: almanak.framework.strategies.RiskGuard
    options:
      show_root_heading: true
      members_order: source

## RiskGuardConfig

::: almanak.framework.strategies.RiskGuardConfig
    options:
      show_root_heading: true
      members_order: source

## DecideResult

::: almanak.framework.strategies.DecideResult
    options:
      show_root_heading: true
      members_order: source

## IntentSequence

::: almanak.framework.strategies.IntentSequence
    options:
      show_root_heading: true
      members_order: source

## ExecutionResult

::: almanak.framework.strategies.ExecutionResult
    options:
      show_root_heading: true
      members_order: source

## Market Data Types

Data types returned by `MarketSnapshot` getters and accepted by `set_*` methods for unit testing.

### TokenBalance

::: almanak.framework.market.TokenBalance
    options:
      show_root_heading: true
      members_order: source

### PriceData

::: almanak.framework.market.PriceData
    options:
      show_root_heading: true
      members_order: source

### RSIData

::: almanak.framework.market.RSIData
    options:
      show_root_heading: true
      members_order: source

### MACDData

::: almanak.framework.market.MACDData
    options:
      show_root_heading: true
      members_order: source

### BollingerBandsData

::: almanak.framework.market.BollingerBandsData
    options:
      show_root_heading: true
      members_order: source

### StochasticData

::: almanak.framework.market.StochasticData
    options:
      show_root_heading: true
      members_order: source

### ATRData

::: almanak.framework.market.ATRData
    options:
      show_root_heading: true
      members_order: source

### MAData

::: almanak.framework.market.MAData
    options:
      show_root_heading: true
      members_order: source

### ADXData

::: almanak.framework.market.ADXData
    options:
      show_root_heading: true
      members_order: source

### OBVData

::: almanak.framework.market.OBVData
    options:
      show_root_heading: true
      members_order: source

### CCIData

::: almanak.framework.market.CCIData
    options:
      show_root_heading: true
      members_order: source

### IchimokuData

::: almanak.framework.market.IchimokuData
    options:
      show_root_heading: true
      members_order: source

### ChainHealthStatus

::: almanak.framework.strategies.ChainHealthStatus
    options:
      show_root_heading: true
      members_order: source

### ChainHealth

::: almanak.framework.strategies.ChainHealth
    options:
      show_root_heading: true
      members_order: source
