"""Unit tests for VIB-4926 — iteration-lane post-execution snapshot must
read FRESH (post-trade) wallet balances, not the cache warmed during
``decide()`` (pre-trade).

Without the fix, ``capture_snapshot_with_accounting`` calls
``runner._capture_portfolio_snapshot`` while the strategy's
``_cached_market_snapshot`` still holds the per-iteration snapshot warmed
PRE-execution (during ``decide()``).  Loose-wallet balances are read through
that stale cache while LP positions are re-priced fresh, so the swapped
tokens get counted in BOTH lanes — an intra-run NAV double-count (mainnet
repro: iter-1 NAV $31.40 vs true ~$25.4; corrupts G6 wallet PnL to exactly
``final − stale-snapshot1``).

The fix re-opens the per-iteration MarketSnapshot scope with a fresh
``{cycle_id}:post-exec`` token before the post-execution capture — but ONLY
on trade iterations (``_iteration_had_trade``) so idle iterations keep
VIB-4843's warm price cache.

These tests pin the contract:

* POSITIVE: on a trade iteration the post-exec snapshot reflects POST-trade
  balances (USDC≈17.65, NOT the pre-trade 23.65), and NAV ≈ true (~25.4) not
  the inflated double-count (~31.40).
* PARTIAL EXECUTION: an iteration where an earlier intent traded but the final
  result carries no ``execution_result`` STILL re-stamps — the gate matches the
  force-snapshot condition (``_iteration_had_trade`` alone), not a stricter one.
* NEGATIVE: on an idle iteration the helper does NOT re-stamp a ``:post-exec``
  token, so VIB-4843's warm cache survives.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.runner._run_loop_helpers import (
    capture_snapshot_with_accounting,
)
from almanak.framework.runner.runner_models import IterationResult, IterationStatus

# True post-trade wallet (after a $6 USDC → 0.0024 WETH swap, ignoring fees).
PRE_TRADE = {"USDC": 23.65, "WETH": 0.0}
POST_TRADE = {"USDC": 17.65, "WETH": 6.00}  # WETH valued at $1 each here for arithmetic clarity
TRUE_NAV = 23.65  # USDC 17.65 + WETH-as-$6.00 — each token counted ONCE
DOUBLE_COUNT_NAV = 29.65  # stale USDC 23.65 + post WETH 6.00 — USDC counted on both swap sides


class _MemoStrategy:
    """Faithful stand-in for IntentStrategy's per-iteration snapshot memo.

    Mirrors ``begin_market_snapshot_iteration`` (token-keyed idempotent
    invalidation) and ``create_market_snapshot`` (memoized rebuild) so the
    test exercises the real cache contract the production fix depends on.
    ``create_market_snapshot`` reads from a *mutable* balance source: a stale
    cache will keep serving the snapshot built from the pre-trade balances;
    a rebuilt cache reads the current (post-trade) balances.
    """

    deployment_id = "dep-vib4926"
    chain = "arbitrum"
    wallet_address = "0xWALLET"

    def __init__(self, balance_source: dict[str, float]) -> None:
        self._balance_source = balance_source
        self._cached_market_snapshot: dict[str, float] | None = None
        self._cached_market_snapshot_token: object | None = None
        self._cached_market_snapshot_at: float | None = None
        self.build_count = 0

    def begin_market_snapshot_iteration(self, token: object) -> None:
        # Same token == no-op; different token invalidates the memo.
        if token is not None and token == self._cached_market_snapshot_token:
            return
        self._cached_market_snapshot = None
        self._cached_market_snapshot_token = token
        self._cached_market_snapshot_at = None

    def create_market_snapshot(self) -> dict[str, float]:
        if self._cached_market_snapshot is not None:
            return self._cached_market_snapshot
        self.build_count += 1
        # Snapshot a COPY of the current balances — proves freshness.
        snapshot = dict(self._balance_source)
        self._cached_market_snapshot = snapshot
        return snapshot


def _make_runner(strategy: _MemoStrategy, *, had_trade: bool, cycle_id: str) -> MagicMock:
    runner = MagicMock(name="StrategyRunner")
    runner.config = SimpleNamespace(enable_state_persistence=True, chain="arbitrum")
    runner._is_live_mode.return_value = False  # paper: snapshot failures don't escalate
    runner._total_iterations = 1
    runner._iteration_had_trade = had_trade
    runner._last_cycle_id = cycle_id

    # Wire the runner helper to the real @staticmethod behaviour: proxy to the
    # strategy's begin_market_snapshot_iteration (mirrors
    # StrategyRunner._begin_market_snapshot_iteration).
    def _real_begin(strat, token):
        strat.begin_market_snapshot_iteration(token)

    runner._begin_market_snapshot_iteration = MagicMock(side_effect=_real_begin)

    # _capture_portfolio_snapshot reads the (possibly rebuilt) snapshot and
    # records the wallet balances + NAV it would persist. This is the surface
    # the double-count manifests on.
    persisted: dict[str, object] = {}

    async def _capture(strategy, iteration_number):
        snap = strategy.create_market_snapshot()
        persisted["wallet"] = dict(snap)
        persisted["nav"] = snap["USDC"] + snap["WETH"]
        return SimpleNamespace(deployment_id=strategy.deployment_id)

    runner._capture_portfolio_snapshot = MagicMock(side_effect=_capture)
    runner._persisted = persisted
    return runner


def _make_result(*, executed: bool) -> IterationResult:
    return IterationResult(
        status=IterationStatus.SUCCESS,
        deployment_id="dep-vib4926",
        execution_result=SimpleNamespace(tx_hash="0xabc") if executed else None,
    )


@pytest.mark.asyncio
async def test_trade_iteration_post_exec_snapshot_reads_fresh_balances() -> None:
    """POSITIVE: a trade iteration rebuilds the cache against post-trade state.

    Without the fix the persisted snapshot would carry the PRE-trade USDC
    (23.65) and NAV would double-count to ~29.65. With the fix the cache is
    invalidated with a ``:post-exec`` token, rebuilt from the post-trade
    balances, so USDC≈17.65 and NAV≈true.
    """
    cycle_id = "cycle-7"
    # The balance source the snapshot reads from. Starts at pre-trade and is
    # mutated to post-trade after decide() warms the cache.
    live = dict(PRE_TRADE)
    strategy = _MemoStrategy(live)

    # decide() phase: stamp the iteration token and warm the cache PRE-trade.
    strategy.begin_market_snapshot_iteration(cycle_id)
    warm = strategy.create_market_snapshot()
    assert warm["USDC"] == PRE_TRADE["USDC"]  # baseline: cache holds pre-trade

    # The swap executes on-chain: mutate the live balance source to post-trade.
    live.clear()
    live.update(POST_TRADE)

    runner = _make_runner(strategy, had_trade=True, cycle_id=cycle_id)
    result = _make_result(executed=True)

    await capture_snapshot_with_accounting(
        runner,
        strategy,  # type: ignore[arg-type]
        deployment_id="dep-vib4926",
        result=result,
    )

    # The fix re-stamped a fresh :post-exec token and rebuilt the snapshot.
    runner._begin_market_snapshot_iteration.assert_called_once_with(strategy, f"{cycle_id}:post-exec")
    persisted = runner._persisted
    assert persisted["wallet"]["USDC"] == pytest.approx(POST_TRADE["USDC"])  # 17.65, NOT 23.65
    assert persisted["wallet"]["WETH"] == pytest.approx(POST_TRADE["WETH"])
    assert persisted["nav"] == pytest.approx(TRUE_NAV)  # ~23.65, NOT the ~29.65 double-count
    assert persisted["nav"] != pytest.approx(DOUBLE_COUNT_NAV)


@pytest.mark.asyncio
async def test_idle_iteration_does_not_restamp_post_exec_token() -> None:
    """NEGATIVE: an idle iteration keeps VIB-4843's warm cache.

    With ``_iteration_had_trade=False`` the helper must NOT re-stamp a
    ``:post-exec`` token — dropping the warm price/balance cache on every idle
    iteration would force needless re-fetches on cold forks (VIB-4843).
    """
    cycle_id = "cycle-idle"
    strategy = _MemoStrategy(dict(PRE_TRADE))
    strategy.begin_market_snapshot_iteration(cycle_id)
    warm_snapshot = strategy.create_market_snapshot()
    builds_before = strategy.build_count

    runner = _make_runner(strategy, had_trade=False, cycle_id=cycle_id)
    result = _make_result(executed=False)

    await capture_snapshot_with_accounting(
        runner,
        strategy,  # type: ignore[arg-type]
        deployment_id="dep-vib4926",
        result=result,
    )

    # No :post-exec re-stamp; warm cache survives (no rebuild).
    for call in runner._begin_market_snapshot_iteration.call_args_list:
        assert ":post-exec" not in str(call.args[1])
    assert strategy._cached_market_snapshot is warm_snapshot
    assert strategy.build_count == builds_before


@pytest.mark.asyncio
async def test_partial_execution_iteration_restamps_without_final_execution_result() -> None:
    """PARTIAL EXECUTION: a trade happened but the final result has no
    execution_result — the re-stamp MUST still fire (VIB-4926 / Codex P1).

    In a multi-intent iteration an earlier intent can execute successfully
    (setting ``_iteration_had_trade``) while a later intent fails before
    producing an ``execution_result``, so the final ``IterationResult`` carries
    ``execution_result is None``. ``_capture_portfolio_snapshot`` still FORCES
    the snapshot on ``_iteration_had_trade`` alone, so the re-stamp gate must
    match that condition — gating on ``execution_result`` too would skip the
    re-stamp here and persist the stale pre-trade balances (the bug). The cache
    is warmed PRE-trade, the balance source mutates to post-trade, and the
    persisted snapshot must reflect post-trade state.
    """
    cycle_id = "cycle-partial"
    live = dict(PRE_TRADE)
    strategy = _MemoStrategy(live)
    strategy.begin_market_snapshot_iteration(cycle_id)
    warm = strategy.create_market_snapshot()
    assert warm["USDC"] == PRE_TRADE["USDC"]

    live.clear()
    live.update(POST_TRADE)

    runner = _make_runner(strategy, had_trade=True, cycle_id=cycle_id)
    result = _make_result(executed=False)  # earlier intent traded; final result has no execution_result

    await capture_snapshot_with_accounting(
        runner,
        strategy,  # type: ignore[arg-type]
        deployment_id="dep-vib4926",
        result=result,
    )

    runner._begin_market_snapshot_iteration.assert_called_once_with(strategy, f"{cycle_id}:post-exec")
    persisted = runner._persisted
    assert persisted["wallet"]["USDC"] == pytest.approx(POST_TRADE["USDC"])  # 17.65, NOT stale 23.65
    assert persisted["nav"] == pytest.approx(TRUE_NAV)
    assert persisted["nav"] != pytest.approx(DOUBLE_COUNT_NAV)


@pytest.mark.asyncio
async def test_restamp_failure_is_swallowed_and_snapshot_still_captures() -> None:
    """Defensive: a flaky re-stamp must not break the snapshot capture.

    The runner's own ``_begin_market_snapshot_iteration`` never raises, but
    the call site wraps too (belt-and-suspenders). On failure the capture
    still runs against whatever cache exists — degrade to stale, never crash.
    """
    cycle_id = "cycle-flaky"
    strategy = _MemoStrategy(dict(POST_TRADE))
    runner = _make_runner(strategy, had_trade=True, cycle_id=cycle_id)
    runner._begin_market_snapshot_iteration.side_effect = RuntimeError("boom")
    result = _make_result(executed=True)

    await capture_snapshot_with_accounting(
        runner,
        strategy,  # type: ignore[arg-type]
        deployment_id="dep-vib4926",
        result=result,
    )

    # Snapshot still captured despite the re-stamp failure.
    runner._capture_portfolio_snapshot.assert_called_once()
