"""Unit tests for the lp_triple phase machine.

The dual fixture's phase machine has its own coverage; lp_triple introduces a
shape neither lp nor lp_dual exercise: a **mid-iteration LP_CLOSE of the
middle position while the other two remain open**. These tests pin the
phase transitions for that out-of-order close path so a refactor of the
``on_intent_executed`` handler can't silently regress it.

We construct the strategy without going through ``IntentStrategy.__init__``
(which needs a runner / gateway / config-loader scaffold) and inject only the
attributes the phase machine reads. Each test exercises the public
``on_intent_executed`` hook against a synthetic intent + result.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from strategies.accounting.lp_triple.strategy import (
    PHASE_A_OPEN,
    PHASE_AB_OPEN,
    PHASE_ABC_OPEN,
    PHASE_ALL_CLOSED,
    PHASE_B_CLOSED_AWAIT_TEARDOWN,
    PHASE_DONE,
    PHASE_INIT,
    PHASE_SWAPPED_IN,
    AccountingQuantLPTripleStrategy,
)


def _bare_strategy() -> AccountingQuantLPTripleStrategy:
    """Skip ``__init__`` (which wants runner scaffolding) and set the
    attributes the phase machine reads. We only care about ``_phase`` and
    the ``_position_ids`` / ``_pool_addresses`` slots here.
    """
    obj = AccountingQuantLPTripleStrategy.__new__(AccountingQuantLPTripleStrategy)
    obj._phase = PHASE_INIT
    obj._position_ids = [None, None, None]
    obj._pool_addresses = [None, None, None]
    obj._initial_balance_usd = Decimal("100")
    obj._initial_balance_token = Decimal("100")
    obj.pool = "WETH/USDC/500"
    obj._strategy_id = "test"
    return obj


def _swap_intent() -> SimpleNamespace:
    return SimpleNamespace(intent_type=SimpleNamespace(value="SWAP"))


def _lp_open_intent() -> SimpleNamespace:
    return SimpleNamespace(intent_type=SimpleNamespace(value="LP_OPEN"))


def _lp_close_intent(position_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        intent_type=SimpleNamespace(value="LP_CLOSE"),
        position_id=position_id,
    )


def _lp_open_result(position_id: str, pool_address: str | None = None) -> Any:
    """Synthetic ExecutionResult-shape with ``position_id`` + optional
    ``lp_open_data.pool_address``. Matches what ``_capture_pool_address_from_result``
    reads.
    """
    return SimpleNamespace(
        position_id=position_id,
        lp_open_data=(SimpleNamespace(pool_address=pool_address) if pool_address else None),
        extracted_data={},
    )


def _drive_through_abc_open(strat: AccountingQuantLPTripleStrategy) -> None:
    """Walk the strategy from PHASE_INIT to PHASE_ABC_OPEN via three
    successful LP_OPENs and one initial SWAP."""
    strat.on_intent_executed(_swap_intent(), success=True, result=None)
    assert strat._phase == PHASE_SWAPPED_IN

    strat.on_intent_executed(_lp_open_intent(), success=True, result=_lp_open_result("nft-A"))
    assert strat._phase == PHASE_A_OPEN
    assert strat._position_ids == ["nft-A", None, None]

    strat.on_intent_executed(_lp_open_intent(), success=True, result=_lp_open_result("nft-B"))
    assert strat._phase == PHASE_AB_OPEN
    assert strat._position_ids == ["nft-A", "nft-B", None]

    strat.on_intent_executed(_lp_open_intent(), success=True, result=_lp_open_result("nft-C"))
    assert strat._phase == PHASE_ABC_OPEN
    assert strat._position_ids == ["nft-A", "nft-B", "nft-C"]


# ---------------------------------------------------------------------------
# Happy path: open A/B/C, mid-iteration close B, teardown closes A then C
# ---------------------------------------------------------------------------


def test_mid_iteration_b_close_transitions_to_b_closed_await_teardown():
    strat = _bare_strategy()
    _drive_through_abc_open(strat)

    strat.on_intent_executed(_lp_close_intent("nft-B"), success=True, result=None)

    assert strat._phase == PHASE_B_CLOSED_AWAIT_TEARDOWN
    assert strat._position_ids == ["nft-A", None, "nft-C"]


def test_teardown_close_a_keeps_phase_pending_until_c_also_closed():
    strat = _bare_strategy()
    _drive_through_abc_open(strat)
    strat.on_intent_executed(_lp_close_intent("nft-B"), success=True, result=None)

    # Teardown closes A first.
    strat.on_intent_executed(_lp_close_intent("nft-A"), success=True, result=None)

    assert strat._phase == PHASE_B_CLOSED_AWAIT_TEARDOWN  # not yet all-closed
    assert strat._position_ids == [None, None, "nft-C"]

    # Then C.
    strat.on_intent_executed(_lp_close_intent("nft-C"), success=True, result=None)

    assert strat._phase == PHASE_ALL_CLOSED
    assert strat._position_ids == [None, None, None]


def test_final_swap_back_transitions_to_done():
    strat = _bare_strategy()
    _drive_through_abc_open(strat)
    for pid in ("nft-B", "nft-A", "nft-C"):
        strat.on_intent_executed(_lp_close_intent(pid), success=True, result=None)
    assert strat._phase == PHASE_ALL_CLOSED

    strat.on_intent_executed(_swap_intent(), success=True, result=None)
    assert strat._phase == PHASE_DONE


# ---------------------------------------------------------------------------
# Out-of-order close on the close side (A before B before C, or C before B)
# ---------------------------------------------------------------------------


import itertools

ALL_CLOSE_PERMUTATIONS = list(itertools.permutations(("nft-A", "nft-B", "nft-C")))
PERMUTATION_IDS = ["-".join(p).replace("nft-", "") for p in ALL_CLOSE_PERMUTATIONS]


@pytest.mark.parametrize("close_order", ALL_CLOSE_PERMUTATIONS, ids=PERMUTATION_IDS)
def test_all_close_orderings_eventually_reach_all_closed(close_order):
    """Every one of the 6 close permutations must drain to PHASE_ALL_CLOSED
    with identical observable state. This is the unit-level proof that close
    order has no impact on the phase machine's terminal state."""
    strat = _bare_strategy()
    _drive_through_abc_open(strat)

    for pid in close_order:
        strat.on_intent_executed(_lp_close_intent(pid), success=True, result=None)

    assert strat._phase == PHASE_ALL_CLOSED
    assert strat._position_ids == [None, None, None]
    assert strat._pool_addresses == [None, None, None]


def test_close_order_does_not_affect_terminal_state():
    """All 6 close orderings produce identical terminal state across the
    strategy's full observable surface. Catches a regression where some
    orderings would leave residual state (e.g. stale pool_address slots,
    phase-machine drift). The assertion is bit-identical equality, not
    a structural sanity check."""
    terminal_states = []
    for perm in ALL_CLOSE_PERMUTATIONS:
        strat = _bare_strategy()
        _drive_through_abc_open(strat)
        for pid in perm:
            strat.on_intent_executed(_lp_close_intent(pid), success=True, result=None)
        terminal_states.append(
            {
                "phase": strat._phase,
                "position_ids": list(strat._position_ids),
                "pool_addresses": list(strat._pool_addresses),
                "persistent_state": {
                    k: v
                    for k, v in strat.get_persistent_state().items()
                    # initial_balance_* are baked in by _bare_strategy() — not
                    # affected by close ordering — but exclude them for clarity
                    if not k.startswith("initial_balance_")
                },
            }
        )

    # Every permutation produces the same terminal state — order-invariance.
    reference = terminal_states[0]
    for i, state in enumerate(terminal_states[1:], start=1):
        assert state == reference, (
            f"Permutation {PERMUTATION_IDS[i]!r} produced different terminal "
            f"state than {PERMUTATION_IDS[0]!r}:\n  diff: {state} vs {reference}"
        )


def test_close_a_before_b_does_not_transition_to_b_closed_await_teardown():
    """The B_CLOSED_AWAIT_TEARDOWN phase is specifically about the mid-loop
    out-of-order shape: ABC_OPEN -> middle close. Other close orderings must
    not co-opt that phase label."""
    strat = _bare_strategy()
    _drive_through_abc_open(strat)

    strat.on_intent_executed(_lp_close_intent("nft-A"), success=True, result=None)

    # Phase stays at PHASE_ABC_OPEN because we transition out of it only via
    # the B-mid-loop branch or the all-cleared branch.
    assert strat._phase == PHASE_ABC_OPEN
    assert strat._position_ids == [None, "nft-B", "nft-C"]


# ---------------------------------------------------------------------------
# Defensive paths
# ---------------------------------------------------------------------------


def test_unrecognized_position_id_leaves_phase_untouched_with_warning(caplog):
    strat = _bare_strategy()
    _drive_through_abc_open(strat)

    caplog.set_level("WARNING")
    strat.on_intent_executed(_lp_close_intent("nft-UNKNOWN"), success=True, result=None)

    assert strat._phase == PHASE_ABC_OPEN
    assert strat._position_ids == ["nft-A", "nft-B", "nft-C"]
    assert any("unrecognized position_id" in r.message for r in caplog.records)


def test_failed_intent_is_a_noop():
    """success=False must not advance phase nor mutate slot state. The runner
    retries on the next iteration."""
    strat = _bare_strategy()
    strat.on_intent_executed(_swap_intent(), success=False, result=None)
    assert strat._phase == PHASE_INIT
    assert strat._position_ids == [None, None, None]


def test_lp_open_in_unexpected_phase_is_a_noop(caplog):
    """If LP_OPEN succeeds in a phase that has no slot to populate (e.g.,
    ABC_OPEN), the handler must not over-write a populated slot or advance
    the phase."""
    strat = _bare_strategy()
    _drive_through_abc_open(strat)

    caplog.set_level("WARNING")
    strat.on_intent_executed(_lp_open_intent(), success=True, result=_lp_open_result("nft-D"))

    assert strat._phase == PHASE_ABC_OPEN
    assert strat._position_ids == ["nft-A", "nft-B", "nft-C"]
    assert any("LP_OPEN succeeded in unexpected phase" in r.message for r in caplog.records)


def test_lp_open_without_position_id_is_a_noop(caplog):
    """If LP_OPEN succeeds but the result lacks a ``position_id`` (a real
    on-chain mint with a parser miss), the handler must hold the phase open
    and leave slots untouched. Otherwise the phase advances while the slot
    stays None, and teardown silently skips closing the real on-chain
    position — stranding capital. Locks in the no-strand contract."""
    strat = _bare_strategy()
    strat.on_intent_executed(_swap_intent(), success=True, result=None)
    assert strat._phase == PHASE_SWAPPED_IN

    caplog.set_level("WARNING")
    # Result is shaped like an ExecutionResult but with no position_id.
    result_missing_pid = SimpleNamespace(position_id=None, lp_open_data=None, extracted_data={})
    strat.on_intent_executed(_lp_open_intent(), success=True, result=result_missing_pid)

    # Phase MUST stay at SWAPPED_IN — the next iteration retries the LP_OPEN A.
    assert strat._phase == PHASE_SWAPPED_IN
    assert strat._position_ids == [None, None, None]
    assert any("no position_id" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Teardown intent generation
# ---------------------------------------------------------------------------


def test_generate_teardown_intents_skips_closed_slots():
    """After mid-loop B close, teardown must emit only two LP_CLOSE intents
    (A and C) plus the final SWAP back."""
    from almanak.framework.teardown import TeardownMode

    strat = _bare_strategy()
    strat.other_asset = "WETH"
    strat.starting_asset = "USDC"
    strat._chain = "arbitrum"
    strat.max_slippage = Decimal("0.005")

    _drive_through_abc_open(strat)
    strat.on_intent_executed(_lp_close_intent("nft-B"), success=True, result=None)

    intents = strat.generate_teardown_intents(TeardownMode.SOFT)

    # Two LP_CLOSE (A, C) + one final SWAP back.
    intent_types = [i.intent_type.value for i in intents]
    assert intent_types == ["LP_CLOSE", "LP_CLOSE", "SWAP"]

    # LP_CLOSE position_ids match the still-open slots, in slot order.
    close_pids = [i.position_id for i in intents if i.intent_type.value == "LP_CLOSE"]
    assert close_pids == ["nft-A", "nft-C"]


def test_generate_teardown_intents_handles_pre_b_close_teardown():
    """If teardown fires before the mid-iteration B close lands, all three
    slots are still populated — teardown must close all three in slot order
    plus the swap-back. This guards against an operator tearing down the
    fixture early."""
    from almanak.framework.teardown import TeardownMode

    strat = _bare_strategy()
    strat.other_asset = "WETH"
    strat.starting_asset = "USDC"
    strat._chain = "arbitrum"
    strat.max_slippage = Decimal("0.005")

    _drive_through_abc_open(strat)
    # Note: NOT calling the mid-loop B close — emulate early teardown.

    intents = strat.generate_teardown_intents(TeardownMode.SOFT)

    intent_types = [i.intent_type.value for i in intents]
    assert intent_types == ["LP_CLOSE", "LP_CLOSE", "LP_CLOSE", "SWAP"]

    close_pids = [i.position_id for i in intents if i.intent_type.value == "LP_CLOSE"]
    assert close_pids == ["nft-A", "nft-B", "nft-C"]


# ---------------------------------------------------------------------------
# Persistent state round-trip
# ---------------------------------------------------------------------------


def test_persistent_state_round_trips_phase_and_slots():
    strat = _bare_strategy()
    _drive_through_abc_open(strat)
    strat.on_intent_executed(_lp_close_intent("nft-B"), success=True, result=None)

    state = strat.get_persistent_state()

    fresh = _bare_strategy()
    fresh.load_persistent_state(state)

    assert fresh._phase == PHASE_B_CLOSED_AWAIT_TEARDOWN
    assert fresh._position_ids == ["nft-A", None, "nft-C"]
    assert fresh._initial_balance_usd == strat._initial_balance_usd


def test_persistent_state_drops_empty_slot_addresses():
    """After closing B mid-iteration, the persistent state must keep A's and
    C's tracking intact AND drop B's slot to empty for both ``position_id_*``
    and ``pool_address_*`` — a serialization regression that dropped only
    one of the two paired keys would let teardown emit a stale pool address
    for a closed slot on the next restart."""
    strat = _bare_strategy()
    addr_a = "0x" + "a" * 40
    addr_b = "0x" + "b" * 40
    addr_c = "0x" + "c" * 40

    strat.on_intent_executed(_swap_intent(), success=True, result=None)
    strat.on_intent_executed(_lp_open_intent(), success=True, result=_lp_open_result("nft-A", addr_a))
    strat.on_intent_executed(_lp_open_intent(), success=True, result=_lp_open_result("nft-B", addr_b))
    strat.on_intent_executed(_lp_open_intent(), success=True, result=_lp_open_result("nft-C", addr_c))
    strat.on_intent_executed(_lp_close_intent("nft-B"), success=True, result=None)

    state = strat.get_persistent_state()

    # Cleared slot stores empty string for BOTH paired keys; populated slots
    # keep their values.
    assert state["position_id_a"] == "nft-A"
    assert state["position_id_b"] == ""
    assert state["position_id_c"] == "nft-C"
    assert state["pool_address_a"] == addr_a
    assert state["pool_address_b"] == ""
    assert state["pool_address_c"] == addr_c
