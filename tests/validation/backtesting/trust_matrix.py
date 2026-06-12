"""Backtest Trust Matrix (VIB-5081) - cell registry, harness, and scoreboard.

This module is the backtesting analog of the Accountant Test
(``almanak/framework/accounting/accountant_test.py``): a scored matrix of
conservation and math invariants evaluated against the REAL PnL engine and
portfolio code with synthetic (network-free) price data.

Rows are invariants (blueprint 31 section 4.3 plus closed-form math checks);
columns are strategy types (swap/TA, LP, lending, perp). Each cell is a
pytest test in ``test_trust_matrix.py`` tagged ``@pytest.mark.trust_cell``;
the conftest in this directory aggregates cell outcomes and prints the
scoreboard at the end of the run (and writes JSON when ``TRUST_MATRIX_JSON``
is set), so CI logs always carry the current matrix state and PRs can state
"matrix moved forward on cell X".

Two tiers (see README.md in this directory):

- **Network-free tier** (this matrix): runs on every PR, no API keys, no
  network. Synthetic price providers drive the real engine iteration loop.
- **Keyed tier** (``-m validation``): nightly accuracy benchmarks plus the
  trust-protocol Phase 3/4 checks (CoinGecko data integrity, fixed-seed
  reproducibility). Skips cleanly when ``COINGECKO_API_KEY`` is absent.

Validation contract (blueprint 31 section 9): every backtesting PR must move
this matrix forward on the affected surface, or explain why it cannot. A
PASS -> FAIL transition is a stop-the-line event. Known-bug cells are
``xfail(strict=True)`` with the tracking ticket in the reason - NEVER weaken
an assertion to make a cell pass; the assertion is the spec.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import (
    HistoricalDataConfig,
    MarketState,
)
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)

# =============================================================================
# Matrix registry
# =============================================================================

#: Column order in the scoreboard. Perp is v1-beta (blueprint 31 section 8).
COLUMNS: tuple[str, ...] = ("swap", "lp", "lending", "perp")

#: Row order in the scoreboard. Each row is one invariant family.
INVARIANT_ROWS: tuple[str, ...] = (
    "no_trade_conservation",
    "entry_value_neutral",
    "single_trade_closed_form",
    "round_trip_conservation",
    "generic_lane_entry",
    "rejection_no_state_change",
    "cost_accounting",
    "yield_tie_out",
    "trade_pnl_attribution",
    "math_il_closed_form",
    "math_sharpe",
    "math_max_drawdown",
)


@dataclass(frozen=True)
class TrustCell:
    """One cell of the trust matrix: (invariant row, strategy-type column)."""

    invariant: str
    column: str
    description: str
    #: Tracking ticket for a known-bug (strict-xfail) cell, or None.
    xfail_ticket: str | None = None

    @property
    def cell_id(self) -> str:
        return f"{self.column}:{self.invariant}"


def _cell(
    invariant: str,
    column: str,
    description: str,
    xfail_ticket: str | None = None,
) -> TrustCell:
    if invariant not in INVARIANT_ROWS:
        raise ValueError(f"unknown invariant row: {invariant}")
    if column not in COLUMNS:
        raise ValueError(f"unknown column: {column}")
    return TrustCell(invariant=invariant, column=column, description=description, xfail_ticket=xfail_ticket)


#: The full matrix. Every entry here must have exactly one test in
#: test_trust_matrix.py tagged with its cell_id (enforced by a meta test).
CELLS: tuple[TrustCell, ...] = (
    # --- swap / TA column (generic engine lane: the default path) ---
    _cell(
        "no_trade_conservation",
        "swap",
        "Hold-only run through the real engine loop ends with equity == initial capital, Decimal-exact.",
    ),
    _cell(
        "single_trade_closed_form",
        "swap",
        "Buy at P0, mark at P1: equity delta == position x (P1 - P0) - costs, exact.",
    ),
    _cell(
        "round_trip_conservation",
        "swap",
        "Buy then sell through the real engine loop with the default portfolio construction "
        "(cash_usd + stables-as-cash) returns initial capital minus exactly the execution costs.",
    ),
    _cell(
        "rejection_no_state_change",
        "swap",
        "A SWAP overspend fill is recorded as a failed trade with zeroed costs and zero state mutation.",
    ),
    _cell(
        "cost_accounting",
        "swap",
        "N trades x fee model == total fees (and slippage), exact; equity delta equals the cost sum.",
    ),
    _cell(
        "trade_pnl_attribution",
        "swap",
        "A profitable closing swap records positive per-trade pnl_usd (win_rate is not degenerate).",
        xfail_ticket="VIB-5083",
    ),
    _cell(
        "math_sharpe",
        "swap",
        "Sharpe ratio from a known equity curve matches the independent closed-form calculation.",
    ),
    _cell(
        "math_max_drawdown",
        "swap",
        "Max drawdown of the protocol reference curve is exactly 25%.",
    ),
    # --- LP column ---
    _cell(
        "entry_value_neutral",
        "lp",
        "LP adapter open is value-neutral at the open instant: position value == deposited notional.",
        # Fixed by VIB-5096: producers convert the USD deposit into true V3
        # L-units via ImpermanentLossCalculator.liquidity_for_target_value.
    ),
    _cell(
        "round_trip_conservation",
        "lp",
        "LP adapter open -> close at flat price with zero pool volume returns initial capital minus gas.",
    ),
    _cell(
        "generic_lane_entry",
        "lp",
        "Generic-lane (no adapter) LP_OPEN does not mint: equity stays at initial capital on open.",
    ),
    _cell(
        "rejection_no_state_change",
        "lp",
        "An LP_OPEN beyond available cash is rejected with zero state mutation.",
    ),
    _cell(
        "math_il_closed_form",
        "lp",
        "Full-range IL for a 50% price move is ~2.02% per the V2 closed form (trust protocol Phase 2.1).",
    ),
    # --- lending column ---
    _cell(
        "entry_value_neutral",
        "lending",
        "SUPPLY is value-neutral at the supply instant: cash converts 1:1 into the supply position.",
    ),
    _cell(
        "yield_tie_out",
        "lending",
        "Equity growth of an open supply position ties exactly to the engine's own interest accrual.",
    ),
    _cell(
        "round_trip_conservation",
        "lending",
        "SUPPLY then WITHDRAW returns initial capital plus accrued interest - principal must not double-count.",
        # Candidate stop-the-line finding (VIB-5081 PR): WITHDRAW never closes
        # the supply position (generic flows carry no position_close_id and the
        # lending adapter defers to them), so a round trip mints the principal.
        xfail_ticket="VIB-5097",
    ),
    _cell(
        "rejection_no_state_change",
        "lending",
        "A SUPPLY beyond available cash is rejected with zero state mutation.",
    ),
    # --- perp column (v1 beta) ---
    _cell(
        "entry_value_neutral",
        "perp",
        "PERP_OPEN is value-neutral at the open instant: collateral moves cash -> position, no minting.",
    ),
    _cell(
        "round_trip_conservation",
        "perp",
        "Perp open -> close at flat price returns initial capital minus exactly the modeled funding.",
    ),
    _cell(
        "rejection_no_state_change",
        "perp",
        "A PERP_OPEN whose collateral exceeds cash is rejected with zero state mutation.",
    ),
)

CELLS_BY_ID: dict[str, TrustCell] = {c.cell_id: c for c in CELLS}


# =============================================================================
# Scoreboard rendering
# =============================================================================

#: Map pytest terminalreporter stats categories to matrix statuses.
_CATEGORY_TO_STATUS: dict[str, str] = {
    "passed": "PASS",
    "failed": "FAIL",
    "error": "FAIL",
    "xfailed": "XFAIL",
    "xpassed": "XPASS",
    "skipped": "SKIP",
}


def status_from_category(category: str) -> str:
    return _CATEGORY_TO_STATUS.get(category, category.upper())


def render_scoreboard(statuses: Mapping[str, str]) -> str:
    """Render the matrix as a markdown table (rows=invariants, cols=strategy types).

    ``statuses`` maps cell_id -> PASS/FAIL/XFAIL/XPASS/SKIP. Cells in the
    registry that did not run this session render as ``not-run``; (row, col)
    combinations with no registered cell render as ``-``.
    """
    lines: list[str] = []
    lines.append("# Backtest Trust Matrix (VIB-5081)")
    lines.append("")
    lines.append("| Invariant | " + " | ".join(COLUMNS) + " |")
    lines.append("|---|" + "---|" * len(COLUMNS))
    for invariant in INVARIANT_ROWS:
        row_cells: list[str] = []
        for column in COLUMNS:
            cell = CELLS_BY_ID.get(f"{column}:{invariant}")
            if cell is None:
                row_cells.append("-")
                continue
            status = statuses.get(cell.cell_id, "not-run")
            if status == "XFAIL" and cell.xfail_ticket:
                row_cells.append(f"XFAIL ({cell.xfail_ticket})")
            else:
                row_cells.append(status)
        lines.append(f"| {invariant} | " + " | ".join(row_cells) + " |")
    lines.append("")
    ran = {cid: s for cid, s in statuses.items() if cid in CELLS_BY_ID}
    counts: dict[str, int] = {}
    for s in ran.values():
        counts[s] = counts.get(s, 0) + 1
    summary = ", ".join(f"{n} {s}" for s, n in sorted(counts.items())) or "no cells ran"
    lines.append(f"Score: {summary} (of {len(CELLS)} registered cells)")
    lines.append(
        "Rule (blueprint 31 section 9): every backtesting PR must move this matrix forward "
        "on the affected surface, or explain why it cannot. PASS -> FAIL is stop-the-line."
    )
    return "\n".join(lines)


def scoreboard_json(statuses: Mapping[str, str]) -> dict[str, Any]:
    """JSON artifact for CI consumers, mirroring the Accountant Test report shape."""
    ran = {cid: s for cid, s in statuses.items() if cid in CELLS_BY_ID}
    return {
        "matrix": "backtest-trust-matrix",
        "ticket": "VIB-5081",
        "timestamp": datetime.now(UTC).isoformat(),
        "cells": {
            cell.cell_id: {
                "invariant": cell.invariant,
                "column": cell.column,
                "status": statuses.get(cell.cell_id, "not-run"),
                "xfail_ticket": cell.xfail_ticket,
                "description": cell.description,
            }
            for cell in CELLS
        },
        "scores": {
            "passed": sum(1 for s in ran.values() if s == "PASS"),
            "failed": sum(1 for s in ran.values() if s == "FAIL"),
            "xfailed": sum(1 for s in ran.values() if s == "XFAIL"),
            "xpassed": sum(1 for s in ran.values() if s == "XPASS"),
            "skipped": sum(1 for s in ran.values() if s == "SKIP"),
            "total": len(CELLS),
        },
    }


# =============================================================================
# Network-free engine harness
# =============================================================================

#: Fixed simulation start for deterministic runs.
START = datetime(2024, 1, 1, tzinfo=UTC)

#: Default initial capital for matrix runs.
INITIAL_CAPITAL = Decimal("10000")

TICK_SECONDS = 3600


class SyntheticPriceProvider:
    """Deterministic, network-free HistoricalDataProvider.

    Yields one MarketState per hourly tick from a pre-defined per-token price
    series. This is REAL engine input (the same protocol production providers
    implement), not a mock of engine behaviour.
    """

    def __init__(self, price_series: dict[str, list[Decimal]]) -> None:
        self._series = {token.upper(): list(series) for token, series in price_series.items()}

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        current = config.start_time
        index = 0
        while current <= config.end_time:
            prices: dict[str, Decimal] = {}
            for token in config.tokens:
                series = self._series.get(token.upper())
                if series:
                    prices[token.upper()] = series[min(index, len(series) - 1)]
                else:
                    prices[token.upper()] = Decimal("1")
            yield (
                current,
                MarketState(
                    timestamp=current,
                    prices=prices,
                    chain=config.chains[0] if config.chains else "arbitrum",
                    block_number=1_000_000 + index,
                    gas_price_gwei=Decimal("30"),
                ),
            )
            index += 1
            current += timedelta(seconds=config.interval_seconds)

    @property
    def provider_name(self) -> str:
        return "trust-matrix-synthetic"

    @property
    def supported_tokens(self) -> list[str]:
        return list(self._series.keys())

    @property
    def supported_chains(self) -> list[str]:
        return ["arbitrum"]

    @property
    def min_timestamp(self) -> datetime:
        return START

    @property
    def max_timestamp(self) -> datetime:
        n_points = max((len(s) for s in self._series.values()), default=1)
        return START + timedelta(seconds=(n_points - 1) * TICK_SECONDS)


class ScriptedStrategy:
    """Minimal deterministic strategy: returns a fixed intent sequence.

    Note on timing: the engine queues the intent returned by ``decide`` at
    tick T and executes it at tick T+1 even with ``inclusion_delay_blocks=0``
    (pending intents are processed before ``decide`` each tick). Closed-form
    expectations in the cells account for this.
    """

    def __init__(self, intents: list[Any], deployment_id: str = "trust-matrix") -> None:
        self._intents = list(intents)
        self._cursor = 0
        self.deployment_id = deployment_id

    def decide(self, market: Any) -> Any:
        if self._cursor < len(self._intents):
            intent = self._intents[self._cursor]
            self._cursor += 1
            return intent
        return None


# --- duck-typed intents exercising the engine's generic execution lane ---


@dataclass
class SwapDuck:
    intent_type: str = "SWAP"
    from_token: str = "USDC"
    to_token: str = "WETH"
    amount_usd: Decimal = Decimal("1000")
    protocol: str = "uniswap_v3"


@dataclass
class LPOpenDuck:
    intent_type: str = "LP_OPEN"
    token0: str = "WETH"
    token1: str = "USDC"
    amount_usd: Decimal = Decimal("5000")
    protocol: str = "uniswap_v3"
    tick_lower: int = -887272
    tick_upper: int = 887272
    fee_tier: Decimal = Decimal("0.003")


@dataclass
class SupplyDuck:
    intent_type: str = "SUPPLY"
    token: str = "USDC"
    amount_usd: Decimal = Decimal("5000")
    protocol: str = "aave_v3"
    apy: Decimal = Decimal("0.05")


@dataclass
class PerpOpenDuck:
    intent_type: str = "PERP_OPEN"
    token: str = "WETH"
    size_usd: Decimal = Decimal("5000")
    collateral_usd: Decimal = Decimal("1000")
    leverage: Decimal = Decimal("5")
    side: str = "long"
    protocol: str = "gmx"


@dataclass
class PerpCloseDuck:
    intent_type: str = "PERP_CLOSE"
    token: str = "WETH"
    side: str = "long"
    protocol: str = "gmx"


def flat_series(n_ticks: int, weth: str = "2000") -> dict[str, list[Decimal]]:
    """Flat WETH/USDC price series: the conservation baseline."""
    return {
        "WETH": [Decimal(weth)] * n_ticks,
        "USDC": [Decimal("1")] * n_ticks,
    }


def run_backtest(
    strategy: Any,
    price_series: dict[str, list[Decimal]],
    hours: int,
    *,
    fee_pct: Decimal = Decimal("0"),
    slippage_pct: Decimal = Decimal("0"),
    strategy_type: str | None = None,
    **config_overrides: Any,
) -> Any:
    """Run the REAL PnL engine loop over synthetic data and return BacktestResult.

    Defaults isolate conservation: zero fees/slippage/gas and immediate
    (next-tick) execution. Cells that test cost accounting override them.
    """
    config_kwargs: dict[str, Any] = {
        "start_time": START,
        "end_time": START + timedelta(hours=hours),
        "interval_seconds": TICK_SECONDS,
        "initial_capital_usd": INITIAL_CAPITAL,
        "tokens": sorted(price_series),
        "include_gas_costs": False,
        "inclusion_delay_blocks": 0,
    }
    config_kwargs.update(config_overrides)
    config = PnLBacktestConfig(**config_kwargs)
    backtester = PnLBacktester(
        data_provider=SyntheticPriceProvider(price_series),
        fee_models={"default": DefaultFeeModel(fee_pct=fee_pct)},
        slippage_models={"default": DefaultSlippageModel(slippage_pct=slippage_pct)},
        strategy_type=strategy_type,
    )
    return asyncio.run(backtester.backtest(strategy, config))
