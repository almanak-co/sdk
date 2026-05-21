"""Unit tests for the ``multi_lp_dual_range`` demo's phase machine.

Modeled on ``tests/unit/strategies/test_lp_triple_phase_machine.py``. This
demo is the *canonical template* for multi-position LP dispatch — copied
verbatim by users via ``almanak strat demo multi_lp_dual_range`` — so the
phase transitions, partial-success guard, position-id-keyed close, and
persistence round-trip MUST be pinned. A regression here propagates to
every user-built strategy.

We construct the strategy without going through ``IntentStrategy.__init__``
(which needs runner / gateway / config-loader scaffolding) and inject only
the attributes the phase machine reads. Each test exercises the public
``on_intent_executed`` / ``get_persistent_state`` / ``load_persistent_state``
hooks against synthetic intents + results.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from almanak.demo_strategies.multi_lp_dual_range.strategy import (
    HANDLE_NARROW,
    HANDLE_WIDE,
    PHASE_BLOCKED,
    PHASE_BOTH_OPEN,
    PHASE_INIT,
    PHASE_LP1_OPEN,
    MultiLPDualRangeConfig,
    MultiLPDualRangeStrategy,
)


# ----------------------------------------------------------------------------
# Test fixtures — synthetic strategy + synthetic intents / receipts
# ----------------------------------------------------------------------------


def _bare_strategy() -> MultiLPDualRangeStrategy:
    """Skip ``__init__`` (which wants runner scaffolding) and set the
    attributes the phase machine reads."""
    obj = MultiLPDualRangeStrategy.__new__(MultiLPDualRangeStrategy)
    obj._phase = PHASE_INIT
    obj._position_id_narrow = None
    obj._position_id_wide = None
    obj._range_lower_narrow = None
    obj._range_upper_narrow = None
    obj._range_lower_wide = None
    obj._range_upper_wide = None
    obj.pool = "WETH/USDC/500"
    obj.token0_symbol = "WETH"
    obj.token1_symbol = "USDC"
    obj.fee_tier = 500
    obj._chain = "arbitrum"  # `.chain` is a read-only property; back it directly
    obj._wallet_address = "0x" + "0" * 40  # same — read-only property
    obj.config = MultiLPDualRangeConfig()  # default values; needed by get_status()
    return obj


def _lp_open_intent(range_lower: Decimal, range_upper: Decimal) -> SimpleNamespace:
    """Synthetic ``LPOpenIntent``-shape with the two fields the phase
    machine reads back: ``intent_type.value`` + ``range_lower`` /
    ``range_upper``."""
    return SimpleNamespace(
        intent_type=SimpleNamespace(value="LP_OPEN"),
        range_lower=range_lower,
        range_upper=range_upper,
    )


def _lp_close_intent(position_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        intent_type=SimpleNamespace(value="LP_CLOSE"),
        position_id=position_id,
    )


def _receipt_with_position_id(position_id: str) -> Any:
    """Synthetic ExecutionResult-shape carrying ``position_id``. The phase
    machine reads ``getattr(result, 'position_id', None)``."""
    return SimpleNamespace(position_id=position_id)


def _receipt_without_position_id() -> Any:
    """Synthetic ExecutionResult-shape where ``position_id`` is absent —
    the failure mode the PHASE_BLOCKED state exists to handle."""
    return SimpleNamespace(position_id=None)


# ----------------------------------------------------------------------------
# Happy path — LP_OPEN narrow → LP_OPEN wide → BOTH_OPEN
# ----------------------------------------------------------------------------


def test_lp_open_narrow_transitions_init_to_lp1_open():
    strat = _bare_strategy()
    intent = _lp_open_intent(Decimal("2000"), Decimal("2200"))

    strat.on_intent_executed(
        intent, success=True, result=_receipt_with_position_id("nft-narrow")
    )

    assert strat._phase == PHASE_LP1_OPEN
    assert strat._position_id_narrow == "nft-narrow"
    assert strat._position_id_wide is None
    assert strat._range_lower_narrow == Decimal("2000")
    assert strat._range_upper_narrow == Decimal("2200")
    assert strat._range_lower_wide is None
    assert strat._range_upper_wide is None


def test_lp_open_wide_transitions_lp1_open_to_both_open():
    strat = _bare_strategy()
    # Drive through narrow first.
    strat.on_intent_executed(
        _lp_open_intent(Decimal("2000"), Decimal("2200")),
        success=True,
        result=_receipt_with_position_id("nft-narrow"),
    )
    # Now wide.
    strat.on_intent_executed(
        _lp_open_intent(Decimal("1700"), Decimal("2500")),
        success=True,
        result=_receipt_with_position_id("nft-wide"),
    )

    assert strat._phase == PHASE_BOTH_OPEN
    assert strat._position_id_narrow == "nft-narrow"
    assert strat._position_id_wide == "nft-wide"
    assert strat._range_lower_narrow == Decimal("2000")
    assert strat._range_upper_narrow == Decimal("2200")
    assert strat._range_lower_wide == Decimal("1700")
    assert strat._range_upper_wide == Decimal("2500")


# ----------------------------------------------------------------------------
# Partial-success guard — PHASE_BLOCKED on LP_OPEN success without position_id
# ----------------------------------------------------------------------------


def test_lp_open_success_without_position_id_transitions_to_blocked_from_init():
    """The advertised partial-success guard: a mint that landed on-chain but
    whose receipt has no ``position_id`` MUST NOT cause the next iteration
    to re-emit the same LP_OPEN (which would duplicate the position). The
    phase moves to PHASE_BLOCKED instead.
    """
    strat = _bare_strategy()
    intent = _lp_open_intent(Decimal("2000"), Decimal("2200"))

    strat.on_intent_executed(intent, success=True, result=_receipt_without_position_id())

    assert strat._phase == PHASE_BLOCKED
    assert strat._position_id_narrow is None
    assert strat._range_lower_narrow is None


def test_lp_open_success_without_position_id_transitions_to_blocked_from_lp1_open():
    """Same guard, but applied after the narrow leg already opened. The
    wide-leg mint succeeds but reports no position_id → blocked, narrow
    state retained (so teardown can still close it)."""
    strat = _bare_strategy()
    # Successfully open narrow.
    strat.on_intent_executed(
        _lp_open_intent(Decimal("2000"), Decimal("2200")),
        success=True,
        result=_receipt_with_position_id("nft-narrow"),
    )
    assert strat._phase == PHASE_LP1_OPEN

    # Wide leg mint reports no position_id.
    strat.on_intent_executed(
        _lp_open_intent(Decimal("1700"), Decimal("2500")),
        success=True,
        result=_receipt_without_position_id(),
    )

    assert strat._phase == PHASE_BLOCKED
    # Narrow leg state retained — teardown still closes it.
    assert strat._position_id_narrow == "nft-narrow"
    assert strat._position_id_wide is None


def test_blocked_phase_is_sticky_across_more_lp_open_attempts():
    """Once in PHASE_BLOCKED, additional LP_OPEN events (whether reporting
    a position_id or not) must NOT advance the phase. Operator intervention
    is the only exit."""
    strat = _bare_strategy()
    strat._phase = PHASE_BLOCKED

    strat.on_intent_executed(
        _lp_open_intent(Decimal("2000"), Decimal("2200")),
        success=True,
        result=_receipt_with_position_id("nft-X"),
    )

    # Phase stays blocked; slot fields not overwritten.
    assert strat._phase == PHASE_BLOCKED
    assert strat._position_id_narrow is None
    assert strat._position_id_wide is None


# ----------------------------------------------------------------------------
# Failure handling — success=False, phase stays put (retry next iteration)
# ----------------------------------------------------------------------------


def test_lp_open_failure_does_not_advance_phase():
    """A failed LP_OPEN (success=False) is recoverable — the runner will
    re-emit the same intent next iteration with a fresh market snapshot.
    Phase MUST stay at PHASE_INIT so decide() emits LP_OPEN narrow again."""
    strat = _bare_strategy()
    intent = _lp_open_intent(Decimal("2000"), Decimal("2200"))

    strat.on_intent_executed(intent, success=False, result=None)

    assert strat._phase == PHASE_INIT
    assert strat._position_id_narrow is None
    assert strat._range_lower_narrow is None


# ----------------------------------------------------------------------------
# LP_CLOSE — keyed on intent.position_id, NOT slot order
# ----------------------------------------------------------------------------


def test_lp_close_keyed_on_position_id_clears_narrow_slot():
    strat = _bare_strategy()
    # Both legs open.
    strat.on_intent_executed(
        _lp_open_intent(Decimal("2000"), Decimal("2200")),
        success=True,
        result=_receipt_with_position_id("nft-narrow"),
    )
    strat.on_intent_executed(
        _lp_open_intent(Decimal("1700"), Decimal("2500")),
        success=True,
        result=_receipt_with_position_id("nft-wide"),
    )

    # Close narrow.
    strat.on_intent_executed(
        _lp_close_intent("nft-narrow"), success=True, result=None
    )

    assert strat._position_id_narrow is None
    assert strat._position_id_wide == "nft-wide"
    assert strat._range_lower_narrow is None
    assert strat._range_upper_narrow is None
    # Wide slot's ranges retained.
    assert strat._range_lower_wide == Decimal("1700")
    assert strat._range_upper_wide == Decimal("2500")


def test_lp_close_keyed_on_position_id_clears_wide_slot_first():
    """Close wide before narrow — out-of-order closes must not desync the
    phase machine. The remaining narrow slot retains its state for the
    subsequent close."""
    strat = _bare_strategy()
    strat.on_intent_executed(
        _lp_open_intent(Decimal("2000"), Decimal("2200")),
        success=True,
        result=_receipt_with_position_id("nft-narrow"),
    )
    strat.on_intent_executed(
        _lp_open_intent(Decimal("1700"), Decimal("2500")),
        success=True,
        result=_receipt_with_position_id("nft-wide"),
    )

    # Close wide FIRST.
    strat.on_intent_executed(
        _lp_close_intent("nft-wide"), success=True, result=None
    )

    assert strat._position_id_narrow == "nft-narrow"
    assert strat._position_id_wide is None
    assert strat._range_lower_wide is None
    assert strat._range_upper_wide is None


def test_lp_close_unrecognized_position_id_does_not_corrupt_state():
    strat = _bare_strategy()
    strat.on_intent_executed(
        _lp_open_intent(Decimal("2000"), Decimal("2200")),
        success=True,
        result=_receipt_with_position_id("nft-narrow"),
    )

    # An LP_CLOSE for a position the strategy doesn't track must NOT
    # silently clear either slot.
    strat.on_intent_executed(
        _lp_close_intent("nft-foreign"), success=True, result=None
    )

    assert strat._position_id_narrow == "nft-narrow"
    assert strat._range_lower_narrow == Decimal("2000")


# ----------------------------------------------------------------------------
# Persistent state — round-trip + the `positions` list-of-dicts for dashboard
# ----------------------------------------------------------------------------


def test_persistent_state_roundtrip_both_legs_open():
    """``get_persistent_state`` + ``load_persistent_state`` round-trip MUST
    fully preserve _phase, both position ids, both leg ranges, AND emit the
    ``positions`` list-of-dicts the LP dashboard's multi-position panel
    reads (lp_dashboard.py:642-688)."""
    src = _bare_strategy()
    src.on_intent_executed(
        _lp_open_intent(Decimal("2000.0"), Decimal("2200.0")),
        success=True,
        result=_receipt_with_position_id("nft-narrow"),
    )
    src.on_intent_executed(
        _lp_open_intent(Decimal("1700.0"), Decimal("2500.0")),
        success=True,
        result=_receipt_with_position_id("nft-wide"),
    )

    state = src.get_persistent_state()
    assert state["_phase"] == PHASE_BOTH_OPEN
    assert state["position_id_narrow"] == "nft-narrow"
    assert state["position_id_wide"] == "nft-wide"
    assert state["range_lower_narrow"] == "2000.0"
    assert state["range_upper_narrow"] == "2200.0"
    assert state["range_lower_wide"] == "1700.0"
    assert state["range_upper_wide"] == "2500.0"

    # ``positions`` list — the contract the multi-position dashboard reads.
    positions = state["positions"]
    assert len(positions) == 2
    by_handle = {p["registry_handle"]: p for p in positions}
    assert by_handle[HANDLE_NARROW]["position_id"] == "nft-narrow"
    assert by_handle[HANDLE_NARROW]["range_lower"] == "2000.0"
    assert by_handle[HANDLE_NARROW]["range_upper"] == "2200.0"
    assert by_handle[HANDLE_WIDE]["position_id"] == "nft-wide"
    assert by_handle[HANDLE_WIDE]["range_lower"] == "1700.0"
    assert by_handle[HANDLE_WIDE]["range_upper"] == "2500.0"

    # Restore into a fresh strategy.
    dst = _bare_strategy()
    dst.load_persistent_state(state)
    assert dst._phase == PHASE_BOTH_OPEN
    assert dst._position_id_narrow == "nft-narrow"
    assert dst._position_id_wide == "nft-wide"
    assert dst._range_lower_narrow == Decimal("2000.0")
    assert dst._range_upper_narrow == Decimal("2200.0")
    assert dst._range_lower_wide == Decimal("1700.0")
    assert dst._range_upper_wide == Decimal("2500.0")


def test_persistent_state_positions_list_excludes_closed_legs():
    """After LP_CLOSE on narrow, the ``positions`` list MUST only contain
    the still-open wide leg."""
    strat = _bare_strategy()
    strat.on_intent_executed(
        _lp_open_intent(Decimal("2000"), Decimal("2200")),
        success=True,
        result=_receipt_with_position_id("nft-narrow"),
    )
    strat.on_intent_executed(
        _lp_open_intent(Decimal("1700"), Decimal("2500")),
        success=True,
        result=_receipt_with_position_id("nft-wide"),
    )
    strat.on_intent_executed(
        _lp_close_intent("nft-narrow"), success=True, result=None
    )

    state = strat.get_persistent_state()
    positions = state["positions"]
    assert len(positions) == 1
    assert positions[0]["registry_handle"] == HANDLE_WIDE
    assert positions[0]["position_id"] == "nft-wide"


def test_persistent_state_positions_list_empty_in_init():
    """A fresh strategy in PHASE_INIT MUST emit an empty positions list (not
    omit the key, not emit placeholder dicts with None position_id)."""
    strat = _bare_strategy()
    state = strat.get_persistent_state()
    assert state["positions"] == []


def test_load_persistent_state_blocked_phase_survives_restart():
    """If the strategy hit PHASE_BLOCKED before restart, the loaded state
    MUST re-enter PHASE_BLOCKED — operator intervention is still required."""
    src = _bare_strategy()
    src.on_intent_executed(
        _lp_open_intent(Decimal("2000"), Decimal("2200")),
        success=True,
        result=_receipt_without_position_id(),
    )
    assert src._phase == PHASE_BLOCKED

    state = src.get_persistent_state()
    dst = _bare_strategy()
    dst.load_persistent_state(state)
    assert dst._phase == PHASE_BLOCKED


# ----------------------------------------------------------------------------
# Teardown — generate_teardown_intents emits LP_CLOSE per still-open leg
# ----------------------------------------------------------------------------


def test_generate_teardown_intents_both_legs_open():
    """Both legs open → 2 LP_CLOSE intents, one per leg, position-id-keyed."""
    from almanak.framework.teardown import TeardownMode

    strat = _bare_strategy()
    strat.on_intent_executed(
        _lp_open_intent(Decimal("2000"), Decimal("2200")),
        success=True,
        result=_receipt_with_position_id("nft-narrow"),
    )
    strat.on_intent_executed(
        _lp_open_intent(Decimal("1700"), Decimal("2500")),
        success=True,
        result=_receipt_with_position_id("nft-wide"),
    )

    intents = strat.generate_teardown_intents(mode=TeardownMode.SOFT, market=None)
    assert len(intents) == 2
    position_ids = {intent.position_id for intent in intents}
    assert position_ids == {"nft-narrow", "nft-wide"}
    # All intents are LP_CLOSE on the configured pool.
    for intent in intents:
        assert intent.intent_type.value == "LP_CLOSE"
        assert intent.pool == "WETH/USDC/500"
        assert intent.protocol == "uniswap_v3"
        assert intent.collect_fees is True


def test_generate_teardown_intents_one_leg_closed():
    """Narrow already closed → only 1 LP_CLOSE for wide."""
    from almanak.framework.teardown import TeardownMode

    strat = _bare_strategy()
    strat.on_intent_executed(
        _lp_open_intent(Decimal("2000"), Decimal("2200")),
        success=True,
        result=_receipt_with_position_id("nft-narrow"),
    )
    strat.on_intent_executed(
        _lp_open_intent(Decimal("1700"), Decimal("2500")),
        success=True,
        result=_receipt_with_position_id("nft-wide"),
    )
    strat.on_intent_executed(
        _lp_close_intent("nft-narrow"), success=True, result=None
    )

    intents = strat.generate_teardown_intents(mode=TeardownMode.SOFT, market=None)
    assert len(intents) == 1
    assert intents[0].position_id == "nft-wide"


def test_generate_teardown_intents_no_legs_open_emits_empty_list():
    """Fresh strategy (no positions open) → empty teardown intent list."""
    from almanak.framework.teardown import TeardownMode

    strat = _bare_strategy()
    intents = strat.generate_teardown_intents(mode=TeardownMode.SOFT, market=None)
    assert intents == []


# ----------------------------------------------------------------------------
# Decide() — sanity for the non-LP-OPEN paths (HOLD-producing phases)
# ----------------------------------------------------------------------------


def test_decide_both_open_returns_hold():
    """In PHASE_BOTH_OPEN, decide() emits HOLD with a clear reason — no
    speculative re-open or rebalance."""
    strat = _bare_strategy()
    strat._phase = PHASE_BOTH_OPEN

    intent = strat.decide(market=None)
    assert intent.intent_type.value == "HOLD"
    assert "Both LP legs open" in (intent.reason or "")


def test_decide_blocked_returns_hold_with_reconciliation_reason():
    """In PHASE_BLOCKED, decide() emits HOLD with a reconciliation message
    so the operator knows manual action is required."""
    strat = _bare_strategy()
    strat._phase = PHASE_BLOCKED

    intent = strat.decide(market=None)
    assert intent.intent_type.value == "HOLD"
    reason = intent.reason or ""
    assert "position_id" in reason
    assert "reconciliation" in reason.lower() or "operator" in reason.lower()


# ----------------------------------------------------------------------------
# Teardown lifecycle hooks + get_status() — observable behaviour
# ----------------------------------------------------------------------------


def test_on_teardown_started_does_not_clear_state():
    """``on_teardown_started`` is informational (logs the teardown mode) —
    it MUST NOT clear position state, since the teardown intents have not
    yet executed."""
    from almanak.framework.teardown import TeardownMode

    strat = _bare_strategy()
    strat.on_intent_executed(
        _lp_open_intent(Decimal("2000"), Decimal("2200")),
        success=True,
        result=_receipt_with_position_id("nft-narrow"),
    )
    strat.on_intent_executed(
        _lp_open_intent(Decimal("1700"), Decimal("2500")),
        success=True,
        result=_receipt_with_position_id("nft-wide"),
    )

    strat.on_teardown_started(mode=TeardownMode.SOFT)

    # No state mutation — both slots still populated.
    assert strat._position_id_narrow == "nft-narrow"
    assert strat._position_id_wide == "nft-wide"
    assert strat._range_lower_narrow == Decimal("2000")
    assert strat._range_lower_wide == Decimal("1700")


def test_on_teardown_completed_clears_position_ids_on_success():
    """On successful teardown, ``on_teardown_completed`` clears both
    position ids — a fresh decide() iteration would then return to
    PHASE_INIT-like behaviour (though the runner typically tears the
    strategy down before that happens)."""
    strat = _bare_strategy()
    strat._position_id_narrow = "nft-narrow"
    strat._position_id_wide = "nft-wide"
    strat._range_lower_narrow = Decimal("2000")
    strat._range_upper_narrow = Decimal("2200")

    strat.on_teardown_completed(success=True, recovered_usd=Decimal("100"))

    assert strat._position_id_narrow is None
    assert strat._position_id_wide is None


def test_on_teardown_completed_failure_retains_position_ids():
    """A failed (partial) teardown MUST NOT clear position state — the
    runner may retry the close on the remaining legs, and the strategy
    needs the ids to do so."""
    strat = _bare_strategy()
    strat._position_id_narrow = "nft-narrow"
    strat._position_id_wide = "nft-wide"

    strat.on_teardown_completed(success=False, recovered_usd=Decimal("50"))

    assert strat._position_id_narrow == "nft-narrow"
    assert strat._position_id_wide == "nft-wide"


def test_get_status_contains_phase_and_position_ids():
    """``get_status`` is consumed by monitoring dashboards. It must expose
    the current phase and both position ids in a discoverable form."""
    strat = _bare_strategy()
    strat._phase = PHASE_BOTH_OPEN
    strat._position_id_narrow = "nft-narrow"
    strat._position_id_wide = "nft-wide"

    status = strat.get_status()
    state = status["state"]
    assert state["phase"] == PHASE_BOTH_OPEN
    assert state["position_id_narrow"] == "nft-narrow"
    assert state["position_id_wide"] == "nft-wide"
    assert status["chain"] == "arbitrum"
    # config is exposed for monitoring dashboards.
    assert status["config"]["pool"] == "WETH/USDC/500"
