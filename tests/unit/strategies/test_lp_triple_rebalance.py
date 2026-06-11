"""Tests for lp_triple's Tier-1 hardening (AccountingStrats.md D3).

Mirror of ``test_lp_dual_rebalance.py`` adapted to the triple fixture's
list-based slot state (indexes 0/1/2 → labels A/B/C). Covers:

* per-slot range capture from executed LP_OPEN intents + persistence
  round-trip (list-based ranges),
* the config-gated rebalance machine: default-off determinism, exit
  hysteresis (buffer + consecutive-confirmation), interval and daily-cap
  churn brakes, and the full close -> deficit-swap -> reopen cycle —
  allowed ONLY in PHASE_B_CLOSED_AWAIT_TEARDOWN, never in PHASE_ABC_OPEN
  (whose job is the canonical mid-iteration close of slot B),
* the init deficit-only swap (skip when balanced, loud hold when
  underfunded),
* the teardown consolidation dust guard.

Construction mirrors ``test_lp_triple_phase_machine.py``: skip
``IntentStrategy.__init__`` and inject only what the code under test reads.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from strategies.accounting.lp_triple.strategy import (
    PHASE_A_OPEN,
    PHASE_AB_OPEN,
    PHASE_ABC_OPEN,
    PHASE_B_CLOSED_AWAIT_TEARDOWN,
    PHASE_INIT,
    PHASE_SWAPPED_IN,
    AccountingQuantLPTripleStrategy,
)

_T0 = datetime(2026, 6, 11, 12, 0, 0, tzinfo=UTC)


def _bare_strategy(**overrides: Any) -> AccountingQuantLPTripleStrategy:
    """Long-hold shape by default: B (index 1) already closed mid-iteration,
    A (index 0, narrow) and C (index 2, wide) still open."""
    obj = AccountingQuantLPTripleStrategy.__new__(AccountingQuantLPTripleStrategy)
    obj.pool = "WETH/USDC/500"
    obj.token0_symbol = "WETH"
    obj.token1_symbol = "USDC"
    obj.fee_tier = 500
    obj.starting_asset = "USDC"
    obj.other_asset = "WETH"
    obj.total_value_usd = Decimal("12")
    obj.swap_split_pct = Decimal("0.50")
    obj.lp_a_range_width_pct = Decimal("0.10")
    obj.lp_b_range_width_pct = Decimal("0.20")
    obj.lp_c_range_width_pct = Decimal("0.40")
    obj.lp_a_capital_split_pct = Decimal("0.33")
    obj.lp_b_capital_split_pct = Decimal("0.50")
    obj.max_slippage = Decimal("0.005")
    obj.inventory_skew_tolerance_pct = Decimal("0.10")
    obj.rebalance_enabled = True
    obj.rebalance_exit_buffer_pct = Decimal("0.01")
    obj.rebalance_confirm_iterations = 3
    obj.min_rebalance_interval_seconds = 600
    obj.max_rebalances_per_day = 6
    obj._chain = "arbitrum"
    obj._deployment_id = "test"
    obj._phase = PHASE_B_CLOSED_AWAIT_TEARDOWN
    obj._position_ids = ["111", None, "333"]
    obj._pool_addresses = [None, None, None]
    obj._range_lowers = [Decimal("2400"), None, Decimal("2000")]
    obj._range_uppers = [Decimal("2600"), None, Decimal("3000")]
    obj._rebalancing_slot = None
    obj._oor_streaks = {0: 0, 1: 0, 2: 0}
    obj._last_rebalance_at = None
    obj._rebalance_day = ""
    obj._rebalances_today = 0
    obj._initial_balance_usd = Decimal("12")
    obj._initial_balance_token = Decimal("12")
    for key, value in overrides.items():
        setattr(obj, key, value)
    return obj


class _Market:
    def __init__(
        self,
        weth_price: str = "2500",
        balances: dict[str, Decimal] | None = None,
        at: datetime | None = None,
    ) -> None:
        self._weth_price = Decimal(weth_price)
        self._balances = balances or {}
        self.timestamp = at or _T0

    def price(self, token: str) -> Decimal:
        return self._weth_price if token == "WETH" else Decimal("1")

    def balance(self, token: str) -> SimpleNamespace:
        return SimpleNamespace(balance=self._balances.get(token, Decimal("0")))


def _lp_open_result(position_id: str) -> SimpleNamespace:
    return SimpleNamespace(position_id=position_id, lp_open_data=None, extracted_data={})


# ---------------------------------------------------------------------------
# Range capture + persistence
# ---------------------------------------------------------------------------


def test_initial_opens_capture_ranges_from_intent() -> None:
    strat = _bare_strategy(
        _phase=PHASE_SWAPPED_IN,
        _position_ids=[None, None, None],
        _range_lowers=[None, None, None],
        _range_uppers=[None, None, None],
    )
    market = _Market(balances={"WETH": Decimal("1"), "USDC": Decimal("100")})

    open_a = strat._build_lp_open(market, slot_index=0)
    strat.on_intent_executed(open_a, True, _lp_open_result("111"))
    assert strat._phase == PHASE_A_OPEN
    # width 0.10 -> half 0.05 around price 2500
    assert strat._range_lowers[0] == Decimal("2500") * Decimal("0.95")
    assert strat._range_uppers[0] == Decimal("2500") * Decimal("1.05")

    open_b = strat._build_lp_open(market, slot_index=1)
    strat.on_intent_executed(open_b, True, _lp_open_result("222"))
    assert strat._phase == PHASE_AB_OPEN
    assert strat._range_lowers[1] == Decimal("2500") * Decimal("0.90")
    assert strat._range_uppers[1] == Decimal("2500") * Decimal("1.10")

    open_c = strat._build_lp_open(market, slot_index=2)
    strat.on_intent_executed(open_c, True, _lp_open_result("333"))
    assert strat._phase == PHASE_ABC_OPEN
    assert strat._range_lowers[2] == Decimal("2500") * Decimal("0.80")
    assert strat._range_uppers[2] == Decimal("2500") * Decimal("1.20")


def test_ranges_and_rebalance_state_persist_round_trip() -> None:
    # _rebalancing_slot=0 deliberately: slot A's index is falsy, which is
    # exactly what the -1 sentinel (vs lp_dual's 0 sentinel) must survive.
    strat = _bare_strategy(
        _last_rebalance_at=_T0,
        _rebalance_day="2026-06-11",
        _rebalances_today=2,
        _rebalancing_slot=0,
    )
    state = strat.get_persistent_state()

    restored = _bare_strategy(
        _range_lowers=[None, None, None],
        _range_uppers=[None, None, None],
        _last_rebalance_at=None,
        _rebalance_day="",
        _rebalances_today=0,
        _rebalancing_slot=None,
    )
    restored.load_persistent_state(state)

    assert restored._range_lowers == [Decimal("2400"), None, Decimal("2000")]
    assert restored._range_uppers == [Decimal("2600"), None, Decimal("3000")]
    assert restored._last_rebalance_at == _T0
    assert restored._rebalance_day == "2026-06-11"
    assert restored._rebalances_today == 2
    assert restored._rebalancing_slot == 0


def test_no_rebalance_in_flight_persists_as_none() -> None:
    strat = _bare_strategy(_rebalancing_slot=None)
    state = strat.get_persistent_state()
    assert state["rebalancing_slot"] == -1

    restored = _bare_strategy(_rebalancing_slot=2)
    restored.load_persistent_state(state)
    assert restored._rebalancing_slot is None


# ---------------------------------------------------------------------------
# Rebalance trigger — default off, phase-scoped, hysteresis, churn brakes
# ---------------------------------------------------------------------------


def test_rebalance_disabled_holds_even_when_out_of_range() -> None:
    strat = _bare_strategy(rebalance_enabled=False)
    intent = strat.decide(_Market(weth_price="2700"))  # outside slot-A range
    assert intent.intent_type.value == "HOLD"
    assert strat._rebalancing_slot is None


def test_no_rebalance_in_abc_open_even_when_out_of_range() -> None:
    """PHASE_ABC_OPEN's job is the canonical mid-iteration close of slot B;
    a rebalance there would corrupt the fixture's out-of-order-close shape.
    Even with rebalancing enabled and slot A far out of range, decide() must
    emit the B close — not a rebalance."""
    strat = _bare_strategy(
        _phase=PHASE_ABC_OPEN,
        _position_ids=["111", "222", "333"],
        _range_lowers=[Decimal("2400"), Decimal("2250"), Decimal("2000")],
        _range_uppers=[Decimal("2600"), Decimal("2750"), Decimal("3000")],
    )
    intent = strat.decide(_Market(weth_price="2700"))  # far outside slot A
    assert intent.intent_type.value == "LP_CLOSE"
    assert intent.position_id == "222"  # the canonical B close, not slot A
    assert strat._rebalancing_slot is None


def test_within_buffer_requires_consecutive_confirmations() -> None:
    strat = _bare_strategy()
    # 2601 is out of A's [2400, 2600] but inside the 1% buffer (~26): needs streak.
    first = strat.decide(_Market(weth_price="2601"))
    second = strat.decide(_Market(weth_price="2601"))
    assert first.intent_type.value == "HOLD"
    assert second.intent_type.value == "HOLD"
    third = strat.decide(_Market(weth_price="2601"))
    assert third.intent_type.value == "LP_CLOSE"
    assert third.position_id == "111"
    assert strat._rebalancing_slot == 0


def test_in_range_tick_resets_confirmation_streak() -> None:
    strat = _bare_strategy()
    strat.decide(_Market(weth_price="2601"))
    strat.decide(_Market(weth_price="2601"))
    strat.decide(_Market(weth_price="2500"))  # back in range — streak resets
    assert strat._oor_streaks[0] == 0
    after_reset = strat.decide(_Market(weth_price="2601"))
    assert after_reset.intent_type.value == "HOLD"


def test_beyond_buffer_triggers_immediately() -> None:
    strat = _bare_strategy()
    intent = strat.decide(_Market(weth_price="2700"))  # > 2600 * 1.01
    assert intent.intent_type.value == "LP_CLOSE"
    assert strat._rebalancing_slot == 0


def test_min_interval_blocks_back_to_back_rebalances() -> None:
    strat = _bare_strategy(_last_rebalance_at=_T0 - timedelta(seconds=100))
    intent = strat.decide(_Market(weth_price="2700", at=_T0))
    assert intent.intent_type.value == "HOLD"

    later = strat.decide(_Market(weth_price="2700", at=_T0 + timedelta(seconds=700)))
    assert later.intent_type.value == "LP_CLOSE"


def test_daily_cap_blocks_further_rebalances() -> None:
    strat = _bare_strategy(_rebalance_day=_T0.strftime("%Y-%m-%d"), _rebalances_today=6)
    intent = strat.decide(_Market(weth_price="2700", at=_T0))
    assert intent.intent_type.value == "HOLD"

    # The cap resets on day rollover.
    next_day = strat.decide(_Market(weth_price="2700", at=_T0 + timedelta(days=1)))
    assert next_day.intent_type.value == "LP_CLOSE"


# ---------------------------------------------------------------------------
# Full rebalance cycle: close -> deficit swap -> reopen (slot A, index 0)
# ---------------------------------------------------------------------------


def test_full_rebalance_cycle() -> None:
    strat = _bare_strategy()

    close_intent = strat.decide(_Market(weth_price="2700"))
    assert close_intent.intent_type.value == "LP_CLOSE"
    assert close_intent.position_id == "111"

    # Close confirmed: slot cleared, phase parked, rebalance still in flight.
    strat.on_intent_executed(close_intent, True, object())
    assert strat._position_ids[0] is None
    assert strat._range_lowers[0] is None
    assert strat._phase == PHASE_B_CLOSED_AWAIT_TEARDOWN
    assert strat._rebalancing_slot == 0

    # Wallet is one-sided (out-of-range close returned only USDC):
    # deficit swap of the surplus toward 50/50.
    skewed = _Market(weth_price="2700", balances={"WETH": Decimal("0"), "USDC": Decimal("8")})
    swap_intent = strat.decide(skewed)
    assert swap_intent.intent_type.value == "SWAP"
    assert swap_intent.from_token == "USDC" and swap_intent.to_token == "WETH"
    assert swap_intent.amount_usd == Decimal("4")  # surplus over the 50/50 split

    # Balanced within tolerance: reopen the slot centered on current price.
    balanced = _Market(
        weth_price="2700",
        balances={"WETH": Decimal("8") / 2 / Decimal("2700"), "USDC": Decimal("4")},
    )
    reopen_intent = strat.decide(balanced)
    assert reopen_intent.intent_type.value == "LP_OPEN"

    strat.on_intent_executed(reopen_intent, True, _lp_open_result("444"))
    assert strat._position_ids[0] == "444"
    assert strat._rebalancing_slot is None
    assert strat._phase == PHASE_B_CLOSED_AWAIT_TEARDOWN
    assert strat._rebalances_today == 1
    assert strat._last_rebalance_at is not None
    # New range captured centered on the reopen price (slot A width 0.10).
    assert strat._range_lowers[0] == Decimal("2700") * Decimal("0.95")
    assert strat._range_uppers[0] == Decimal("2700") * Decimal("1.05")
    # Slot C untouched throughout.
    assert strat._position_ids[2] == "333"
    assert strat._range_lowers[2] == Decimal("2000")


def test_failed_close_is_retried() -> None:
    strat = _bare_strategy(_rebalancing_slot=0)  # close emitted but not confirmed
    retry = strat.decide(_Market(weth_price="2700"))
    assert retry.intent_type.value == "LP_CLOSE"
    assert retry.position_id == "111"


# ---------------------------------------------------------------------------
# Init deficit-only swap
# ---------------------------------------------------------------------------


def test_init_swaps_only_the_deficit() -> None:
    strat = _bare_strategy(_phase=PHASE_INIT, _position_ids=[None, None, None])
    # Wallet pre-holds $1 of WETH; the 50/50 target is $6 -> deficit $5.
    market = _Market(balances={"WETH": Decimal("1") / Decimal("2500"), "USDC": Decimal("20")})
    intent = strat._build_initial_swap(market)
    assert intent.intent_type.value == "SWAP"
    assert intent.amount_usd == Decimal("5")


def test_init_skips_swap_when_already_balanced() -> None:
    strat = _bare_strategy(_phase=PHASE_INIT, _position_ids=[None, None, None])
    market = _Market(balances={"WETH": Decimal("6") / Decimal("2500"), "USDC": Decimal("20")})
    intent = strat._build_initial_swap(market)
    assert intent.intent_type.value == "LP_OPEN"
    assert strat._phase == PHASE_SWAPPED_IN


def test_init_holds_loud_when_underfunded() -> None:
    strat = _bare_strategy(_phase=PHASE_INIT, _position_ids=[None, None, None])
    market = _Market(balances={"WETH": Decimal("0"), "USDC": Decimal("1")})
    intent = strat._build_initial_swap(market)
    assert intent.intent_type.value == "HOLD"
    assert "Insufficient" in intent.reason


# ---------------------------------------------------------------------------
# Teardown consolidation dust guard
# ---------------------------------------------------------------------------


def test_teardown_skips_dust_consolidation_when_nothing_to_close() -> None:
    strat = _bare_strategy(_position_ids=[None, None, None])
    dust_market = _Market(balances={"WETH": Decimal("0.0001")})  # $0.25 at 2500
    intents = strat.generate_teardown_intents(mode=None, market=dust_market)
    assert intents == []


def test_teardown_keeps_consolidation_above_dust() -> None:
    strat = _bare_strategy(_position_ids=[None, None, None])
    market = _Market(balances={"WETH": Decimal("0.002")})  # $5 at 2500
    intents = strat.generate_teardown_intents(mode=None, market=market)
    assert [i.intent_type.value for i in intents] == ["SWAP"]
    assert intents[0].amount == "all"


def test_teardown_with_open_slots_always_includes_consolidation() -> None:
    strat = _bare_strategy()  # A and C open, B already closed
    dust_market = _Market(balances={"WETH": Decimal("0.0001")})
    intents = strat.generate_teardown_intents(mode=None, market=dust_market)
    kinds = [i.intent_type.value for i in intents]
    assert kinds == ["LP_CLOSE", "LP_CLOSE", "SWAP"], (
        "with closes pending, amount='all' resolves post-close at execution time "
        f"so the sweep must stay unconditional: {kinds}"
    )
