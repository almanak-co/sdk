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
    TokenRef,
    normalize_token_ref,
    token_ref_display,
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
    "borrow_repay_conservation",
    "generic_lane_entry",
    "funding_gated_entry",
    "funding_lane_coherence",
    "rejection_no_state_change",
    "cost_accounting",
    "gas_native_asset_pricing",
    "yield_tie_out",
    "fee_share_scaling",
    "fungible_close_by_pool_id",
    "fee_reporting_tie_out",
    "snapshot_price_case_insensitive",
    "trade_pnl_attribution",
    "math_il_closed_form",
    "math_sharpe",
    "math_max_drawdown",
    "round_trip_conservation_numeraire",
    "fiat_usd_pin",
    "unsupported_intent_refused",
    "price_series_consistency",
    "numeraire_canonical_metrics",
    "key_plane_uniqueness",
    "gas_tank_conservation",
    "single_owner_resolution",
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
        "gas_native_asset_pricing",
        "swap",
        "Gas cost uses the chain's native gas asset price, not an ETH/WETH price that happens to be tracked.",
    ),
    _cell(
        "trade_pnl_attribution",
        "swap",
        "A profitable closing swap records positive per-trade pnl_usd (win_rate is not degenerate).",
        # Fixed by VIB-5083: a disposing SWAP realizes proceeds - units x
        # average-cost basis; opening/inventory-building swaps carry
        # pnl_usd=None (unknown, not a fabricated 0) and the metrics layer
        # excludes them from win/loss stats.
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
    _cell(
        "round_trip_conservation_numeraire",
        "swap",
        "Buy then sell at a flat price with a token (WETH) numeraire conserves value "
        "in the numeraire unit: equity is a flat 5 WETH at every mark (VIB-5127).",
    ),
    _cell(
        "fiat_usd_pin",
        "swap",
        "A default (fiat_usd) strategy emits no numeraire fields and serializes "
        "byte-for-byte as pre-VIB-5127 (no numeraire* keys in the artifact).",
    ),
    _cell(
        "unsupported_intent_refused",
        "swap",
        "An intent type outside the simulated envelope stops the run with a fatal "
        "UnsupportedIntentError and zero state mutation — never a costed no-op.",
        # Design decision 2026-07-02: the generic lane used to record ANY intent
        # type as a trade with fees/gas charged but empty token flows and no
        # position (~15 vocabulary types affected). A backtest that silently
        # skips part of the strategy certifies numbers it never earned.
    ),
    _cell(
        "price_series_consistency",
        "swap",
        "The result's price_series is aligned 1:1 with the equity curve and carries "
        "exactly the prices the engine valued the portfolio with — holdings x emitted "
        "price reproduces the marked equity.",
    ),
    _cell(
        "numeraire_canonical_metrics",
        "swap",
        "Numeraire-canonical merge (blueprint 31 §7): with a moving numeraire price, "
        "the primary metrics tell the numeraire story and every USD PnL figure equals "
        "its numeraire sibling x the emitted end reference price, Decimal-exact.",
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
        # Fixed under VIB-5096: the generic lane stores true V3 liquidity
        # derived from the USD notional, so the marker conserves at open.
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
    _cell(
        "fee_share_scaling",
        "lp",
        "LP fee accrual scales with the REAL share of pool liquidity in BOTH "
        "lanes: the adapter path (closed form + 10x-TVL inverse scaling) and "
        "the generic/fallback path (_simulate_lp_fee_accrual closed form).",
        # Guards the removed max(0.1, liquidity_share) floor (epic VIB-5079;
        # blocked the VIB-5130 flag removal). The floor lived in THREE places -
        # adapter _calculate_fee_accrual, adapter _estimate_heuristic_fees, and
        # SimulatedPortfolio._simulate_lp_fee_accrual (the generic/fallback lane,
        # caught in PR review) - each crediting any sub-10% position with 10% of
        # the ENTIRE pool's fees, minting value on essentially every LP backtest.
    ),
    _cell(
        "fungible_close_by_pool_id",
        "lp",
        "Fungible-LP LP_CLOSE carrying a pool-descriptor position_id "
        "('TOKEN0/TOKEN1/pool_type', != the synthetic open id) round-trips: the "
        "adapter matches the open position by pair+protocol, closes it, the "
        "position count returns to zero, and equity conserves.",
        # Guards the fungible-LP close-matching bug (sibling of VIB-5097/VIB-5098;
        # blocked the VIB-5130 flag removal): the LP adapter matched LP_CLOSE by
        # exact id only, so every Aerodrome/V2-style close ("WETH/USDC/volatile")
        # missed the synthetic open id ("LP_aerodrome_WETH_USDC_<ts>") and the
        # position never closed. Fixed by find_lp_close_position_id.
    ),
    _cell(
        "fee_reporting_tie_out",
        "lp",
        "The summary metric total_fees_earned_usd (and fees_by_pool) returned by "
        "the engine-result path (metrics_calculator.calculate_metrics) equals the "
        "sum of the per-trade fees_earned_usd across an LP round-trip that "
        "demonstrably accrued fees, and matches SimulatedPortfolio.get_metrics().",
        # Guards the LP fee-reporting bug (VIB-5079 v1.1 reporting): calculate_metrics
        # -- the path finalize_backtest_result uses for the engine result -- never
        # aggregated position.fees_earned, so total_fees_earned_usd / fees_by_pool
        # stayed at their dataclass defaults (0 / {}) on EVERY LP backtest even
        # though per-trade fees were correct and credited into equity at close.
        # Surfaced after #2832 (fungible-LP positions now close instead of
        # accumulating). Fixed by sourcing the position block from the shared
        # SimulatedPortfolio.aggregate_position_metrics so the two metric paths
        # cannot drift. Reporting/KPI bug only -- conservation was always exact.
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
    ),
    _cell(
        "borrow_repay_conservation",
        "lending",
        "BORROW opens debt and credits cash; REPAY extinguishes it: a borrow -> repay "
        "round trip conserves equity to within accrued interest (VIB-5098).",
    ),
    _cell(
        "rejection_no_state_change",
        "lending",
        "A SUPPLY beyond available cash is rejected with zero state mutation.",
    ),
    _cell(
        "snapshot_price_case_insensitive",
        "lending",
        "The engine seeds the snapshot with upper-cased symbols, but a strategy queries "
        "its config casing (market.price('wstETH')). The price MUST resolve case-insensitively "
        "instead of raising and silently executing zero intents while the run still reports "
        "institutional_compliance=true / 100% coverage (the silent false-clean lending backtest).",
    ),
    # --- perp column (v1 beta) ---
    _cell(
        "entry_value_neutral",
        "perp",
        "PERP_OPEN is value-neutral at the open instant: collateral moves cash -> position, no minting.",
    ),
    _cell(
        "funding_gated_entry",
        "perp",
        "A strategy that gates entries on market.funding_rate(...) receives the engine-configured "
        "rate from the snapshot's backtest funding lane and enters (>= 1 successful trade).",
        # Guards the unwired strategy-facing funding read: the engine's
        # create_market_snapshot_from_state handed decide() a snapshot with no
        # funding_rate_provider, so every funding read raised "No funding rate
        # provider configured for MarketSnapshot" and funding-gated perp
        # strategies (e.g. gmx_v2_directional_perp) produced 0-trade backtests
        # over any window. Fixed by SnapshotFundingRateSource / view_at wiring.
    ),
    _cell(
        "funding_lane_coherence",
        "perp",
        "With historical funding enabled, the rate decide() gates on and the rate the open "
        "position accrues resolve from the SAME measured source: closed form ties final "
        "capital to exactly two funding-hour applications at the measured (not fallback) rate.",
        # Guards the async-context skip (PR `#3153` review): the perp adapter's
        # update_position runs inside the engine's async task and used to skip
        # historical funding fetches there (fallback:async_context, low
        # confidence), while the snapshot lane thread-bridged and served
        # measured history - so a strategy could enter on the measured rate
        # while its position accrued the fallback. The adapter now bridges
        # through the same worker-thread path.
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
    # --- key-plane uniqueness (re-cut phase 1: single-owner token identity) ---
    _cell(
        "key_plane_uniqueness",
        "swap",
        "REGISTERED-plane swap round trip through the real loop conserves equity Decimal-exact (the 2960 erasure class is unwritable).",
    ),
    _cell(
        "key_plane_uniqueness",
        "lp",
        "REGISTERED-plane LP open/close: credits land on the funding identity key, never a parallel symbol key, and value conserves.",
    ),
    _cell(
        "gas_tank_conservation",
        "swap",
        "Gas draws from the operational tank, never strategy capital: charged-gas equity equals zero-gas equity; metered gas is reported.",
    ),
    _cell(
        "single_owner_resolution",
        "swap",
        "amount=\"all\" resolves once at lane ingress: a sell-all through the real loop fills at full held size and conserves.",
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
USDC_ARBITRUM = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
USDC_POLYGON = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"
USDC_BY_CHAIN = {
    "arbitrum": USDC_ARBITRUM,
    "polygon": USDC_POLYGON,
}

TICK_SECONDS = 3600


class SyntheticPriceProvider:
    """Deterministic, network-free HistoricalDataProvider.

    Yields one MarketState per hourly tick from a pre-defined per-token price
    series. This is REAL engine input (the same protocol production providers
    implement), not a mock of engine behaviour.
    """

    def __init__(self, price_series: dict[TokenRef, list[Decimal]]) -> None:
        self._series = {
            token_ref_display(normalize_token_ref(token, "arbitrum")).upper(): list(series)
            for token, series in price_series.items()
        }

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        current = config.start_time
        index = 0
        while current <= config.end_time:
            chain = config.chains[0] if config.chains else "arbitrum"
            prices: dict[TokenRef, Decimal] = {}
            for token in config.tokens:
                normalized_token = normalize_token_ref(token, chain)
                series_key = token_ref_display(normalized_token).upper()
                series = self._series.get(series_key)
                if series:
                    prices[normalized_token] = series[min(index, len(series) - 1)]
                else:
                    prices[normalized_token] = Decimal("1")
            yield (
                current,
                MarketState(
                    timestamp=current,
                    prices=prices,
                    chain=chain,
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
        return ["arbitrum", "polygon"]

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

    def __init__(
        self,
        intents: list[Any],
        deployment_id: str = "trust-matrix",
        quote_asset: Any = None,
    ) -> None:
        self._intents = list(intents)
        self._cursor = 0
        self.deployment_id = deployment_id
        # Mirrors IntentStrategy.quote_asset so the engine reads the real
        # getattr(strategy, "quote_asset", None) numeraire path (VIB-5127).
        # None == the USD default; the attribute only exists when set so the
        # default path also exercises the "no quote_asset attribute" branch.
        if quote_asset is not None:
            self.quote_asset = quote_asset

    def decide(self, market: Any) -> Any:
        if self._cursor < len(self._intents):
            intent = self._intents[self._cursor]
            self._cursor += 1
            return intent
        return None


class FundingGatedPerpStrategy:
    """Perp strategy gated on ``market.funding_rate`` — the demo-seed entry gate.

    Mirrors ``gmx_v2_directional_perp._funding_hourly``: a funding read failure
    is treated as "funding unavailable" and the strategy refuses to open blind.
    Before the snapshot funding lane was wired, EVERY tick took the except
    branch, so a funding-gated strategy could never trade in a backtest.
    """

    deployment_id = "trust-matrix-funding-gated"

    def __init__(self, entry_threshold_hourly: Decimal = Decimal("0.0005")) -> None:
        self._entry_threshold = entry_threshold_hourly
        self._opened = False
        self.rates_seen: list[Decimal] = []

    def decide(self, market: Any) -> Any:
        try:
            rate = market.funding_rate("gmx_v2", "ETH-USD").rate_hourly
        except Exception:  # noqa: BLE001 - funding unavailable -> refuse to open blind
            return None
        self.rates_seen.append(rate)
        if self._opened or rate > self._entry_threshold:
            return None
        self._opened = True
        return PerpOpenDuck()


class FundingCoherenceProbeStrategy:
    """Funding-reading perp round trip: open on the first tick, close on the third.

    Reads ``market.funding_rate`` on EVERY tick without a guard — a funding-lane
    failure fails the cell loudly — and records what decide() saw so the cell
    can tie the decide-visible rate to the funding the position accrued.
    """

    deployment_id = "trust-matrix-funding-coherence"

    def __init__(self, notional: Decimal = Decimal("5000")) -> None:
        self._notional = notional
        self._ticks = 0
        self.rates_seen: list[Decimal] = []

    def decide(self, market: Any) -> Any:
        self.rates_seen.append(market.funding_rate("gmx_v2", "ETH-USD").rate_hourly)
        self._ticks += 1
        if self._ticks == 1:
            return PerpOpenDuck(size_usd=self._notional, collateral_usd=Decimal("1000"))
        if self._ticks == 3:
            return PerpCloseDuck()
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


@dataclass
class StakeDuck:
    """An intent type the engine does NOT simulate (maps to IntentType.UNKNOWN)."""

    intent_type: str = "STAKE"
    token: str = "WETH"
    amount_usd: Decimal = Decimal("1000")
    protocol: str = "lido"


def flat_series(n_ticks: int, weth: str = "2000") -> dict[TokenRef, list[Decimal]]:
    """Flat WETH/USDC price series: the conservation baseline."""
    return {
        "WETH": [Decimal(weth)] * n_ticks,
        "USDC": [Decimal("1")] * n_ticks,
        ("arbitrum", USDC_ARBITRUM): [Decimal("1")] * n_ticks,
    }


def run_backtest(
    strategy: Any,
    price_series: dict[TokenRef, list[Decimal]],
    hours: int,
    *,
    fee_pct: Decimal = Decimal("0"),
    slippage_pct: Decimal = Decimal("0"),
    strategy_type: str | None = None,
    data_config: Any | None = None,
    token_addresses: dict[str, tuple[str, str]] | None = None,
    **config_overrides: Any,
) -> Any:
    """Run the REAL PnL engine loop over synthetic data and return BacktestResult.

    Defaults isolate conservation: zero fees/slippage/gas and immediate
    (next-tick) execution. Cells that test cost accounting override them.
    Cells that pass a ``data_config`` must keep it network-free
    (``use_historical_funding=False`` etc.) — this is the no-network tier.
    """
    funding_chain = str(config_overrides.get("chain", "arbitrum")).lower()
    funding_address = USDC_BY_CHAIN[funding_chain]
    provider_series = dict(price_series)
    provider_series.setdefault(
        (funding_chain, funding_address),
        list(provider_series.get("USDC", [Decimal("1")] * (hours + 1))),
    )
    config_kwargs: dict[str, Any] = {
        "start_time": START,
        "end_time": START + timedelta(hours=hours),
        "interval_seconds": TICK_SECONDS,
        "token_funding": [
            {
                "symbol": "USDC",
                "address": funding_address,
                "chain": funding_chain,
                "amount": str(INITIAL_CAPITAL),
                "amount_type": "token",
            }
        ],
        "tokens": list(provider_series),
        "include_gas_costs": False,
        "inclusion_delay_blocks": 0,
    }
    config_kwargs.update(config_overrides)
    config = PnLBacktestConfig(**config_kwargs)
    provider = SyntheticPriceProvider(provider_series)
    if token_addresses:
        # REGISTERED-plane runs: production ingress attaches the run's
        # identity map on the provider (CoinGeckoDataProvider(token_addresses=...)
        # in backtest_runner) and the engine reads it back — without this,
        # cells exercise only the unregistered legacy world.
        provider._token_addresses = dict(token_addresses)
    backtester = PnLBacktester(
        data_provider=provider,
        fee_models={"default": DefaultFeeModel(fee_pct=fee_pct)},
        slippage_models={"default": DefaultSlippageModel(slippage_pct=slippage_pct)},
        strategy_type=strategy_type,
        data_config=data_config,
    )
    return asyncio.run(backtester.backtest(strategy, config))
