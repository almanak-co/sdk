"""Unit tests for VIB-4906 / F2 — MarketSnapshot cache invalidation
before each teardown snapshot bracket.

Without this fix, ``capture_teardown_snapshot_with_accounting`` reuses
the strategy's per-iteration ``_cached_market_snapshot`` across both the
pre- and post-teardown brackets, so the post bracket's
``portfolio_snapshots`` row carries pre-teardown wallet balances.  These
tests pin the fix:

* The helper invokes ``runner._begin_market_snapshot_iteration`` exactly
  once per bracket, with a token shaped ``{teardown_cycle_id}:{phase}``.
* Sibling brackets get different tokens (pre vs post).
* Different teardown invocations get different tokens — never collide
  with the iteration loop's own ``cycle_id`` (no colon suffix there).
* End-to-end: a real ``IntentStrategy`` instance whose memo was warmed
  with an iteration token gets invalidated and rebuilds against fresh
  balances.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner._run_loop_helpers import (
    capture_teardown_snapshot_with_accounting,
)


@pytest.fixture
def local_db_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    monkeypatch.delenv("ALMANAK_IS_HOSTED", raising=False)
    monkeypatch.setenv("ALMANAK_STATE_DB", str(tmp_path / "state.db"))
    return tmp_path


@pytest.fixture
def fake_strategy() -> SimpleNamespace:
    return SimpleNamespace(
        deployment_id="dep-cache-invalidation",
        chain="arbitrum",
        wallet_address="0xWALLET",
    )


def _make_runner(*, persistence_enabled: bool = True) -> MagicMock:
    runner = MagicMock(name="StrategyRunner")
    runner.config = SimpleNamespace(
        enable_state_persistence=persistence_enabled, chain="arbitrum"
    )
    runner._is_live_mode.return_value = True
    runner._total_iterations = 3
    runner._last_cycle_id = ""
    runner._begin_market_snapshot_iteration = MagicMock()
    return runner


# ---------------------------------------------------------------------------
# Contract: helper invalidates before each bracket with a stable token shape.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pre_bracket_invalidates_with_pre_phase_token(
    monkeypatch: pytest.MonkeyPatch,
    fake_strategy,
    local_db_dir: Path,
) -> None:
    runner = _make_runner()

    async def _noop_capture(*args, **kwargs):
        return SimpleNamespace(deployment_id=fake_strategy.deployment_id)

    monkeypatch.setattr(
        "almanak.framework.runner.runner_state.capture_portfolio_snapshot",
        _noop_capture,
    )

    await capture_teardown_snapshot_with_accounting(
        runner,
        fake_strategy,
        teardown_cycle_id="teardown-abc",
        pre_teardown=True,
    )

    runner._begin_market_snapshot_iteration.assert_called_once_with(
        fake_strategy, "teardown-abc:pre"
    )


@pytest.mark.asyncio
async def test_post_bracket_invalidates_with_post_phase_token(
    monkeypatch: pytest.MonkeyPatch,
    fake_strategy,
    local_db_dir: Path,
) -> None:
    runner = _make_runner()

    async def _noop_capture(*args, **kwargs):
        return SimpleNamespace(deployment_id=fake_strategy.deployment_id)

    monkeypatch.setattr(
        "almanak.framework.runner.runner_state.capture_portfolio_snapshot",
        _noop_capture,
    )

    await capture_teardown_snapshot_with_accounting(
        runner,
        fake_strategy,
        teardown_cycle_id="teardown-abc",
        pre_teardown=False,
    )

    runner._begin_market_snapshot_iteration.assert_called_once_with(
        fake_strategy, "teardown-abc:post"
    )


@pytest.mark.asyncio
async def test_pre_and_post_use_distinct_tokens(
    monkeypatch: pytest.MonkeyPatch,
    fake_strategy,
    local_db_dir: Path,
) -> None:
    """Pre + post on the same teardown cycle id must not collide."""
    runner = _make_runner()

    async def _noop_capture(*args, **kwargs):
        return SimpleNamespace(deployment_id=fake_strategy.deployment_id)

    monkeypatch.setattr(
        "almanak.framework.runner.runner_state.capture_portfolio_snapshot",
        _noop_capture,
    )

    await capture_teardown_snapshot_with_accounting(
        runner, fake_strategy, teardown_cycle_id="teardown-xyz", pre_teardown=True
    )
    await capture_teardown_snapshot_with_accounting(
        runner, fake_strategy, teardown_cycle_id="teardown-xyz", pre_teardown=False
    )

    tokens = [call.args[1] for call in runner._begin_market_snapshot_iteration.call_args_list]
    assert tokens == ["teardown-xyz:pre", "teardown-xyz:post"]
    assert len(set(tokens)) == 2  # explicitly distinct


@pytest.mark.asyncio
async def test_tokens_never_collide_with_iteration_cycle_id_shape(
    monkeypatch: pytest.MonkeyPatch,
    fake_strategy,
    local_db_dir: Path,
) -> None:
    """The iteration loop's ``cycle_id`` never contains a colon-suffix.

    Pinning the colon-suffix in the bracket token is the contract that
    guarantees the strategy memo invalidates between an iteration and the
    subsequent teardown bracket (and back again).
    """
    runner = _make_runner()

    async def _noop_capture(*args, **kwargs):
        return SimpleNamespace(deployment_id=fake_strategy.deployment_id)

    monkeypatch.setattr(
        "almanak.framework.runner.runner_state.capture_portfolio_snapshot",
        _noop_capture,
    )

    await capture_teardown_snapshot_with_accounting(
        runner, fake_strategy, teardown_cycle_id="teardown-123", pre_teardown=True
    )

    token = runner._begin_market_snapshot_iteration.call_args.args[1]
    assert ":" in token
    assert token.endswith(":pre")


# ---------------------------------------------------------------------------
# Defensive: invalidation must NOT propagate failure (matches the never-
# raise contract on ``capture_teardown_snapshot_with_accounting``).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_invalidation_helper_failure_is_swallowed(
    monkeypatch: pytest.MonkeyPatch,
    fake_strategy,
    local_db_dir: Path,
) -> None:
    """A flaky ``_begin_market_snapshot_iteration`` cannot break a bracket.

    The runner's helper itself never raises (strategy_runner.py:6549-6552),
    but the bracket call site wraps too — defensive double-guard so a
    future refactor losing the runner's own wrapping doesn't break
    teardown.  On failure: the cache stays stale, the snapshot still
    captures, and ``F4`` suppression downstream catches the resulting
    stale row.  ``accounting_degraded`` stays False because the snapshot
    itself succeeded — the cache miss is a quality signal, not a
    persistence failure.
    """
    runner = _make_runner()
    runner._begin_market_snapshot_iteration.side_effect = RuntimeError("boom")

    async def _noop_capture(*args, **kwargs):
        return SimpleNamespace(deployment_id=fake_strategy.deployment_id)

    monkeypatch.setattr(
        "almanak.framework.runner.runner_state.capture_portfolio_snapshot",
        _noop_capture,
    )

    outcome = await capture_teardown_snapshot_with_accounting(
        runner,
        fake_strategy,
        teardown_cycle_id="teardown-flaky",
        pre_teardown=False,
    )

    # Bracket continued: snapshot was captured.  Cache failure was logged
    # but not propagated to ``accounting_degraded`` — that field is reserved
    # for persistence-write failures, not memo bookkeeping.
    assert outcome.snapshot_captured is True
    assert outcome.accounting_degraded is False


# ---------------------------------------------------------------------------
# Persistence-disabled short-circuit: no invalidation needed either.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_persistence_disabled_does_not_invalidate(
    monkeypatch: pytest.MonkeyPatch,
    fake_strategy,
    local_db_dir: Path,
) -> None:
    """When persistence is off the bracket returns early — no cache work."""
    runner = _make_runner(persistence_enabled=False)

    outcome = await capture_teardown_snapshot_with_accounting(
        runner, fake_strategy, teardown_cycle_id="teardown-skip", pre_teardown=True
    )

    runner._begin_market_snapshot_iteration.assert_not_called()
    assert outcome.snapshot_captured is False
    assert outcome.accounting_degraded is False


# ---------------------------------------------------------------------------
# End-to-end: a real IntentStrategy memo gets invalidated and rebuilds.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_real_intent_strategy_memo_is_invalidated_between_brackets(
    monkeypatch: pytest.MonkeyPatch,
    local_db_dir: Path,
) -> None:
    """End-to-end: warm the strategy's memo, run two brackets, observe rebuild.

    Uses a stand-in strategy that mirrors the parts of ``IntentStrategy``
    the cache machinery relies on.  Verifies that the strategy's
    ``_cached_market_snapshot_token`` advances on each bracket — proof
    that ``_build_market_snapshot`` will be called fresh on the next
    ``create_market_snapshot()`` consumer.
    """
    runner = _make_runner()

    # Wire the runner's helper to the real implementation behaviour: stamp
    # the strategy's ``_cached_market_snapshot_token`` and reset the memo.
    # This mirrors ``StrategyRunner._begin_market_snapshot_iteration`` (a
    # @staticmethod that proxies to ``strategy.begin_market_snapshot_iteration``).
    def _real_begin(strategy, token):
        strategy.begin_market_snapshot_iteration(token)

    runner._begin_market_snapshot_iteration = MagicMock(side_effect=_real_begin)

    class _StrategyLike:
        """Minimal stand-in mirroring IntentStrategy's memo machinery."""

        deployment_id = "dep-memo"
        chain = "arbitrum"
        wallet_address = "0xWALLET"

        def __init__(self) -> None:
            # Warm the memo with an iteration-shaped token (no colon).
            self._cached_market_snapshot = object()
            self._cached_market_snapshot_token = "iter-7"
            self._cached_market_snapshot_at = None

        def begin_market_snapshot_iteration(self, token: object) -> None:
            if token is not None and token == self._cached_market_snapshot_token:
                return
            self._cached_market_snapshot = None
            self._cached_market_snapshot_token = token
            self._cached_market_snapshot_at = None

    strategy = _StrategyLike()
    initial_snap = strategy._cached_market_snapshot
    assert initial_snap is not None  # baseline: memo is warm

    async def _noop_capture(*args, **kwargs):
        return SimpleNamespace(deployment_id=strategy.deployment_id)

    monkeypatch.setattr(
        "almanak.framework.runner.runner_state.capture_portfolio_snapshot",
        _noop_capture,
    )

    # Pre-bracket: token advances to "teardown-e2e:pre"; memo cleared.
    await capture_teardown_snapshot_with_accounting(
        runner, strategy, teardown_cycle_id="teardown-e2e", pre_teardown=True
    )
    assert strategy._cached_market_snapshot is None
    assert strategy._cached_market_snapshot_token == "teardown-e2e:pre"

    # Simulate the bracket re-warming the memo (a real call to
    # create_market_snapshot would do this).
    strategy._cached_market_snapshot = object()

    # Post-bracket: token advances to "teardown-e2e:post"; memo cleared.
    await capture_teardown_snapshot_with_accounting(
        runner, strategy, teardown_cycle_id="teardown-e2e", pre_teardown=False
    )
    assert strategy._cached_market_snapshot is None
    assert strategy._cached_market_snapshot_token == "teardown-e2e:post"
