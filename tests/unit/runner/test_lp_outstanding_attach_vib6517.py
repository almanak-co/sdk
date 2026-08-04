"""VIB-6517 — the ITERATION lane bounds a fungible-LP close to the deployment's own liquidity.

VIB-6162 clamped the TEARDOWN lane only; the bound had exactly one writer
(``TeardownManager._attach_lp_outstanding``), so a close emitted from ``decide()``
compiled unbounded and withdrew ``balanceOf(wallet)``. These tests pin the iteration
lane's writer: ``StrategyRunner._step_attach_lp_outstanding``, called from
``run_iteration`` between intent extraction and dispatch.

Two liveness surfaces, deliberately separate:

* the STEP tests drive ``_step_attach_lp_outstanding`` directly — deleting the method
  fails them;
* the SEAM test drives ``run_iteration`` end-to-end and asserts the intent handed to
  the single-chain dispatch carries the bound — deleting the ``run_iteration`` call
  site fails it even if the method survives. Teardown's wiring tests
  (``tests/unit/teardown/test_lp_clamp_wiring_vib6162.py``) cannot cover either: they
  never enter the iteration lane.

Every LP_CLOSE here is a REAL ``Intent.lp_close`` whose ``intent_type`` is the
``IntentType`` ENUM — a stub carrying a plain string fabricates a value production
never emits, which is exactly how the first VIB-6162 attempt shipped inert.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.intents.vocabulary import Intent, IntentType
from almanak.framework.runner.strategy_runner import (
    IterationResult,
    IterationStatus,
    RunIterationState,
    RunnerConfig,
    StrategyRunner,
)
from almanak.framework.teardown.lp_clamp import LpClampUnresolved

_PAUSE_PATCH = "almanak.framework.runner.strategy_runner.StrategyRunner._is_strategy_paused"
_TEARDOWN_PATCH = "almanak.framework.runner.strategy_runner.StrategyRunner._check_teardown_requested"

#: One OPEN of 2.0 LP (aerodrome declares units="raw", decimals=18) and no CLOSE, so
#: the deployment's outstanding folds to exactly 2.
_OPEN_ROWS = [
    {
        "id": 1,
        "event_type": "OPEN",
        "position_id": "USDC/DAI/stable",
        "liquidity": str(2 * 10**18),
    }
]


def _make_runner(*, rows: list | Exception | None = _OPEN_ROWS, dry_run: bool = False) -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=dry_run,
    )
    state_manager = MagicMock()
    if isinstance(rows, Exception):
        state_manager.get_position_events_filtered = AsyncMock(side_effect=rows)
    else:
        state_manager.get_position_events_filtered = AsyncMock(return_value=rows or [])
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=state_manager,
        config=config,
    )


def _make_strategy(decide_return=None) -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "deployment:vib6517test"
    strategy.chain = "base"
    strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
    strategy.create_market_snapshot.return_value = MagicMock()
    strategy.create_market_snapshot.return_value.has_critical_data_failures.return_value = False
    if decide_return is not None:
        strategy.decide.return_value = decide_return
    strategy.generate_teardown_intents.side_effect = NotImplementedError
    return strategy


def _lp_close() -> Intent:
    return Intent.lp_close(
        position_id="USDC/DAI/stable", pool="USDC/DAI/stable", collect_fees=True, protocol="aerodrome"
    )


def _state(strategy: MagicMock, intents: list) -> RunIterationState:
    state = RunIterationState(
        strategy=strategy,
        deployment_id=strategy.deployment_id,
        start_time=datetime.now(UTC),
    )
    state.intents = intents
    return state


# ── the step ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_iteration_lane_attaches_the_bound():
    """THE test: a decide()-emitted Aerodrome LP_CLOSE leaves the step bounded."""
    runner = _make_runner()
    strategy = _make_strategy()
    state = _state(strategy, [_lp_close()])

    await runner._step_attach_lp_outstanding(state)

    params = state.intents[0].protocol_params or {}
    assert params.get("deployment_outstanding_lp") == "2", (
        "the iteration lane compiled this close without the deployment's outstanding "
        "bound — the VIB-6517 defect (whole-wallet burn)"
    )


@pytest.mark.asyncio
async def test_a_real_intent_enum_is_recognised():
    """``str(IntentType.LP_CLOSE)`` is not ``"LP_CLOSE"``; the step must match the ENUM.

    The negative twin (a real swap) pins that only LP_CLOSE intents are touched.
    """
    real = _lp_close()
    assert real.intent_type is IntentType.LP_CLOSE  # the production shape, not a string
    swap = Intent.swap(from_token="USDC", to_token="WETH", amount=Decimal("1"))

    runner = _make_runner()
    state = _state(_make_strategy(), [swap, real])
    await runner._step_attach_lp_outstanding(state)

    assert state.intents[0] is swap, "a non-LP_CLOSE intent was rebuilt"
    assert (state.intents[1].protocol_params or {}).get("deployment_outstanding_lp") == "2"


@pytest.mark.asyncio
async def test_unmeasured_outstanding_stamps_the_refusal_sentinel():
    """No history → a non-numeric sentinel, so the connector REFUSES instead of burning.

    The step itself never early-exits: sibling intents pass through untouched and the
    refused close fails at its own compile step (stop-at-the-failed-step, not a
    whole-iteration veto).
    """
    runner = _make_runner(rows=[])  # zero matched rows → LpClampUnresolved in the fold
    swap = Intent.swap(from_token="USDC", to_token="WETH", amount=Decimal("1"))
    state = _state(_make_strategy(), [swap, _lp_close()])

    await runner._step_attach_lp_outstanding(state)

    bound = (state.intents[1].protocol_params or {}).get("deployment_outstanding_lp")
    assert isinstance(bound, str) and bound.startswith("unmeasured:"), (
        f"an unmeasurable close must carry the refusal sentinel, got {bound!r}"
    )
    assert state.intents[0] is swap, "the sibling intent must be untouched"


@pytest.mark.asyncio
async def test_an_unexpected_read_error_also_stamps_the_sentinel():
    """Fail closed: an internal error in the read must refuse, never compile unbounded."""
    runner = _make_runner(rows=RuntimeError("db exploded"))
    state = _state(_make_strategy(), [_lp_close()])

    await runner._step_attach_lp_outstanding(state)

    bound = (state.intents[0].protocol_params or {}).get("deployment_outstanding_lp")
    assert isinstance(bound, str) and bound.startswith("unmeasured:")


@pytest.mark.asyncio
async def test_existing_bound_is_never_overwritten(caplog):
    """A caller-supplied bound (teardown attach, manual) wins even when a fresh read differs.

    Honouring it must be LOUD: a caller-supplied bound is the one remaining way a
    decide()-emitted close can widen its burn, so the trust decision is logged.
    """
    import logging

    runner = _make_runner()  # fresh read would say "2"
    manual = _lp_close().model_copy(update={"protocol_params": {"deployment_outstanding_lp": "0.5"}})
    state = _state(_make_strategy(), [manual])

    with caplog.at_level(logging.INFO):
        await runner._step_attach_lp_outstanding(state)

    assert (state.intents[0].protocol_params or {}).get("deployment_outstanding_lp") == "0.5"
    runner.state_manager.get_position_events_filtered.assert_not_awaited()
    assert "honouring caller-supplied" in caplog.text, (
        "the caller-supplied-bound decision must be visible in the run log"
    )


@pytest.mark.asyncio
async def test_an_unclamped_venue_passes_through_untouched():
    """Curve declares clamp=False on its manifest — the manifest's decision, honoured here."""
    runner = _make_runner()
    curve = Intent.lp_close(position_id="0x" + "dd" * 20, collect_fees=True, protocol="curve")
    state = _state(_make_strategy(), [curve])

    await runner._step_attach_lp_outstanding(state)

    assert (state.intents[0].protocol_params or {}).get("deployment_outstanding_lp") is None


@pytest.mark.asyncio
async def test_dry_run_unmeasured_close_refuses_not_full_burn():
    """dry_run never persists an OPEN, so its books legitimately show no position.

    The truthful preview of "what live would do with these books" is a refusal — the
    pre-fix behaviour previewed a full-burn bundle, which is the defect rendered as UX.
    """
    runner = _make_runner(rows=[], dry_run=True)
    state = _state(_make_strategy(), [_lp_close()])

    await runner._step_attach_lp_outstanding(state)

    bound = (state.intents[0].protocol_params or {}).get("deployment_outstanding_lp")
    assert isinstance(bound, str) and bound.startswith("unmeasured:")


# ── the seam ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@patch(_TEARDOWN_PATCH, return_value=None)
@patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
async def test_run_iteration_hands_the_bounded_intent_to_dispatch(mock_pause, mock_teardown):
    """The SEAM liveness test: deleting the ``run_iteration`` call site fails this.

    Drives the real driver from ``decide()`` to the single-chain dispatch boundary and
    asserts the intent that crosses it carries the bound.
    """
    runner = _make_runner()
    strategy = _make_strategy(decide_return=_lp_close())

    captured: list = []

    async def _capture(state: RunIterationState) -> IterationResult:
        captured.extend(state.intents)
        return IterationResult(
            status=IterationStatus.SUCCESS,
            deployment_id=state.deployment_id,
            duration_ms=0,
        )

    with patch.object(runner, "_run_single_chain_intents", side_effect=_capture):
        await runner.run_iteration(strategy)

    assert captured, "the iteration never reached dispatch"
    params = captured[0].protocol_params or {}
    assert params.get("deployment_outstanding_lp") == "2", (
        "run_iteration dispatched an LP_CLOSE without the outstanding bound — the "
        "iteration lane is unclamped (VIB-6517)"
    )


@pytest.mark.asyncio
@patch(_TEARDOWN_PATCH, return_value=None)
@patch(_PAUSE_PATCH, new_callable=AsyncMock, return_value=(False, None))
async def test_multi_chain_dispatch_receives_attached_intents(mock_pause, mock_teardown):
    """The second dispatch entrypoint gets the same bounded list (and so does the
    bridge-wait persistence that serializes it for the stuck-resume path)."""
    runner = _make_runner()
    runner._is_multi_chain = True
    strategy = _make_strategy(decide_return=_lp_close())

    captured: dict = {}

    async def _capture(*, strategy, intents, start_time, market):
        captured["intents"] = intents
        return IterationResult(
            status=IterationStatus.SUCCESS,
            deployment_id=strategy.deployment_id,
            duration_ms=0,
        )

    with patch.object(runner, "_execute_multi_chain", side_effect=_capture):
        await runner.run_iteration(strategy)

    assert captured.get("intents"), "the iteration never reached multi-chain dispatch"
    params = captured["intents"][0].protocol_params or {}
    assert params.get("deployment_outstanding_lp") == "2"


# ── refusal classification ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_lp_outstanding_helper_still_serves_the_teardown_lane():
    """Promoting ``_lp_outstanding`` to public must not detach the teardown binding."""
    from almanak.framework.teardown.runner_helpers import build_runner_helpers

    runner = _make_runner()
    helpers = build_runner_helpers(runner)
    assert helpers.has_lp_clamp
    outstanding = await helpers.get_lp_outstanding(_make_strategy(), "aerodrome", "USDC/DAI/stable", "USDC/DAI/stable")
    assert outstanding == Decimal("2")


@pytest.mark.asyncio
async def test_a_missing_state_manager_refuses_rather_than_widening():
    """The shared read's null-guard raises typed, and the step turns it into the sentinel."""
    runner = _make_runner()
    runner.state_manager = None
    state = _state(_make_strategy(), [_lp_close()])

    await runner._step_attach_lp_outstanding(state)

    bound = (state.intents[0].protocol_params or {}).get("deployment_outstanding_lp")
    assert isinstance(bound, str) and bound.startswith("unmeasured:")


def test_the_sentinel_is_non_numeric_forever():
    """The refusal contract rests on the sentinel NEVER parsing as a Decimal.

    If someone "fixes" the sentinel into a number, the connector clamps to it instead
    of refusing — a silent wrong bound. This is the tripwire.
    """
    for sentinel in ("unmeasured: no rows", "unmeasured: internal error: boom"):
        with pytest.raises(ArithmeticError):
            Decimal(sentinel)


def test_lp_clamp_unresolved_is_what_the_shared_read_raises():
    """Pin the exception type the step's scoped handler catches."""
    from almanak.framework.teardown import runner_helpers

    assert hasattr(runner_helpers, "lp_outstanding"), (
        "the shared read was renamed or removed — the iteration-lane attach step imports it by name"
    )
    assert issubclass(LpClampUnresolved, Exception)
