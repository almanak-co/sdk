"""Tests for lp_dual's Tier-1 hardening (AccountingStrats.md D3).

Covers:
* per-leg range capture from executed LP_OPEN intents + persistence round-trip,
* the config-gated rebalance machine: default-off determinism, exit hysteresis
  (buffer + consecutive-confirmation), interval and daily-cap churn brakes,
  and the full close -> deficit-swap -> reopen cycle,
* the init deficit-only swap (skip when balanced, loud hold when underfunded),
* the teardown consolidation dust guard.

Construction mirrors ``test_lp_triple_phase_machine.py``: skip
``IntentStrategy.__init__`` and inject only what the code under test reads.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from strategies.accounting.lp_dual.strategy import (
    PHASE_BOTH_OPEN,
    PHASE_INIT,
    PHASE_LP1_OPEN,
    PHASE_SWAPPED_IN,
    AccountingQuantLPDualStrategy,
)

_T0 = datetime(2026, 6, 11, 12, 0, 0, tzinfo=UTC)


def _bare_strategy(**overrides: Any) -> AccountingQuantLPDualStrategy:
    obj = AccountingQuantLPDualStrategy.__new__(AccountingQuantLPDualStrategy)
    obj.pool = "WETH/USDC/500"
    obj.token0_symbol = "WETH"
    obj.token1_symbol = "USDC"
    obj.fee_tier = 500
    obj.starting_asset = "USDC"
    obj.other_asset = "WETH"
    obj.total_value_usd = Decimal("8")
    obj.swap_split_pct = Decimal("0.50")
    obj.lp1_range_width_pct = Decimal("0.10")
    obj.lp2_range_width_pct = Decimal("0.40")
    obj.lp_capital_split_pct = Decimal("0.50")
    obj.max_slippage = Decimal("0.005")
    obj.protocol = "uniswap_v3"
    obj.inventory_skew_tolerance_pct = Decimal("0.10")
    obj.rebalance_enabled = True
    obj.rebalance_exit_buffer_pct = Decimal("0.01")
    obj.rebalance_confirm_iterations = 3
    obj.min_rebalance_interval_seconds = 600
    obj.max_rebalances_per_day = 6
    obj._chain = "arbitrum"
    obj._deployment_id = "test"
    obj._phase = PHASE_BOTH_OPEN
    obj._position_id_1 = "111"
    obj._position_id_2 = "222"
    obj._pool_address_1 = None
    obj._pool_address_2 = None
    obj._range_lower_1 = Decimal("2400")
    obj._range_upper_1 = Decimal("2600")
    obj._range_lower_2 = Decimal("2000")
    obj._range_upper_2 = Decimal("3000")
    obj._rebalancing_slot = None
    obj._oor_streaks = {1: 0, 2: 0}
    obj._last_rebalance_at = None
    obj._rebalance_day = ""
    obj._rebalances_today = 0
    obj._initial_balance_usd = Decimal("8")
    obj._initial_balance_token = Decimal("8")
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
        _position_id_1=None,
        _position_id_2=None,
        _range_lower_1=None,
        _range_upper_1=None,
        _range_lower_2=None,
        _range_upper_2=None,
    )
    market = _Market(balances={"WETH": Decimal("0.0016"), "USDC": Decimal("4")})

    open_1 = strat._build_lp_open(market, position_index=1)
    strat.on_intent_executed(open_1, True, _lp_open_result("111"))
    assert strat._phase == PHASE_LP1_OPEN
    # width 0.10 -> half 0.05 around price 2500
    assert strat._range_lower_1 == Decimal("2500") * Decimal("0.95")
    assert strat._range_upper_1 == Decimal("2500") * Decimal("1.05")

    open_2 = strat._build_lp_open(market, position_index=2)
    strat.on_intent_executed(open_2, True, _lp_open_result("222"))
    assert strat._phase == PHASE_BOTH_OPEN
    assert strat._range_lower_2 == Decimal("2500") * Decimal("0.80")
    assert strat._range_upper_2 == Decimal("2500") * Decimal("1.20")


def test_ranges_and_rebalance_state_persist_round_trip() -> None:
    strat = _bare_strategy(
        _last_rebalance_at=_T0,
        _rebalance_day="2026-06-11",
        _rebalances_today=2,
        _rebalancing_slot=1,
    )
    state = strat.get_persistent_state()

    restored = _bare_strategy(
        _range_lower_1=None,
        _range_upper_1=None,
        _range_lower_2=None,
        _range_upper_2=None,
        _last_rebalance_at=None,
        _rebalance_day="",
        _rebalances_today=0,
        _rebalancing_slot=None,
    )
    restored.load_persistent_state(state)

    assert restored._range_lower_1 == Decimal("2400")
    assert restored._range_upper_1 == Decimal("2600")
    assert restored._range_lower_2 == Decimal("2000")
    assert restored._range_upper_2 == Decimal("3000")
    assert restored._last_rebalance_at == _T0
    assert restored._rebalance_day == "2026-06-11"
    assert restored._rebalances_today == 2
    assert restored._rebalancing_slot == 1


# ---------------------------------------------------------------------------
# Rebalance trigger — default off, hysteresis, churn brakes
# ---------------------------------------------------------------------------


def test_rebalance_disabled_holds_even_when_out_of_range() -> None:
    strat = _bare_strategy(rebalance_enabled=False)
    intent = strat.decide(_Market(weth_price="2700"))  # outside leg-1 range
    assert intent.intent_type.value == "HOLD"
    assert strat._rebalancing_slot is None


def test_within_buffer_requires_consecutive_confirmations() -> None:
    strat = _bare_strategy()
    # 2601 is out of [2400, 2600] but inside the 1% buffer (~26): needs streak.
    first = strat.decide(_Market(weth_price="2601"))
    second = strat.decide(_Market(weth_price="2601"))
    assert first.intent_type.value == "HOLD"
    assert second.intent_type.value == "HOLD"
    third = strat.decide(_Market(weth_price="2601"))
    assert third.intent_type.value == "LP_CLOSE"
    assert third.position_id == "111"
    assert strat._rebalancing_slot == 1


def test_in_range_tick_resets_confirmation_streak() -> None:
    strat = _bare_strategy()
    strat.decide(_Market(weth_price="2601"))
    strat.decide(_Market(weth_price="2601"))
    strat.decide(_Market(weth_price="2500"))  # back in range — streak resets
    assert strat._oor_streaks[1] == 0
    after_reset = strat.decide(_Market(weth_price="2601"))
    assert after_reset.intent_type.value == "HOLD"


def test_beyond_buffer_triggers_immediately() -> None:
    strat = _bare_strategy()
    intent = strat.decide(_Market(weth_price="2700"))  # > 2600 * 1.01
    assert intent.intent_type.value == "LP_CLOSE"
    assert strat._rebalancing_slot == 1


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
# Full rebalance cycle: close -> deficit swap -> reopen
# ---------------------------------------------------------------------------


def test_full_rebalance_cycle() -> None:
    strat = _bare_strategy()

    close_intent = strat.decide(_Market(weth_price="2700"))
    assert close_intent.intent_type.value == "LP_CLOSE"

    # Close confirmed: slot cleared, phase parked, rebalance still in flight.
    strat.on_intent_executed(close_intent, True, object())
    assert strat._position_id_1 is None
    assert strat._range_lower_1 is None
    assert strat._phase == PHASE_BOTH_OPEN
    assert strat._rebalancing_slot == 1

    # Wallet is one-sided (out-of-range close returned only USDC):
    # deficit swap of the surplus toward 50/50.
    skewed = _Market(weth_price="2700", balances={"WETH": Decimal("0"), "USDC": Decimal("8")})
    swap_intent = strat.decide(skewed)
    assert swap_intent.intent_type.value == "SWAP"
    assert swap_intent.from_token == "USDC" and swap_intent.to_token == "WETH"
    assert swap_intent.amount_usd == Decimal("4")  # surplus over the 50/50 split

    # Balanced within tolerance: reopen the leg centered on current price.
    balanced = _Market(
        weth_price="2700",
        balances={"WETH": Decimal("8") / 2 / Decimal("2700"), "USDC": Decimal("4")},
    )
    reopen_intent = strat.decide(balanced)
    assert reopen_intent.intent_type.value == "LP_OPEN"

    strat.on_intent_executed(reopen_intent, True, _lp_open_result("333"))
    assert strat._position_id_1 == "333"
    assert strat._rebalancing_slot is None
    assert strat._phase == PHASE_BOTH_OPEN
    assert strat._rebalances_today == 1
    assert strat._last_rebalance_at is not None
    # New range captured centered on the reopen price.
    assert strat._range_lower_1 == Decimal("2700") * Decimal("0.95")
    assert strat._range_upper_1 == Decimal("2700") * Decimal("1.05")


def test_failed_close_is_retried() -> None:
    strat = _bare_strategy(_rebalancing_slot=1)  # close emitted but not confirmed
    retry = strat.decide(_Market(weth_price="2700"))
    assert retry.intent_type.value == "LP_CLOSE"
    assert retry.position_id == "111"


# ---------------------------------------------------------------------------
# Init deficit-only swap
# ---------------------------------------------------------------------------


def test_init_swaps_only_the_deficit() -> None:
    strat = _bare_strategy(_phase=PHASE_INIT, _position_id_1=None, _position_id_2=None)
    # Wallet pre-holds $1 of WETH; the 50/50 target is $4 -> deficit $3.
    market = _Market(balances={"WETH": Decimal("1") / Decimal("2500"), "USDC": Decimal("20")})
    intent = strat._build_initial_swap(market)
    assert intent.intent_type.value == "SWAP"
    assert intent.amount_usd == Decimal("3")


def test_init_skips_swap_when_already_balanced() -> None:
    strat = _bare_strategy(_phase=PHASE_INIT, _position_id_1=None, _position_id_2=None)
    market = _Market(balances={"WETH": Decimal("4") / Decimal("2500"), "USDC": Decimal("20")})
    intent = strat._build_initial_swap(market)
    assert intent.intent_type.value == "LP_OPEN"
    assert strat._phase == PHASE_SWAPPED_IN


def test_init_holds_loud_when_underfunded() -> None:
    strat = _bare_strategy(_phase=PHASE_INIT, _position_id_1=None, _position_id_2=None)
    market = _Market(balances={"WETH": Decimal("0"), "USDC": Decimal("1")})
    intent = strat._build_initial_swap(market)
    assert intent.intent_type.value == "HOLD"
    assert "Insufficient" in intent.reason


# ---------------------------------------------------------------------------
# Teardown consolidation dust guard
# ---------------------------------------------------------------------------


def test_teardown_skips_dust_consolidation_when_nothing_to_close() -> None:
    strat = _bare_strategy(_position_id_1=None, _position_id_2=None)
    dust_market = _Market(balances={"WETH": Decimal("0.0001")})  # $0.25 at 2500
    intents = strat.generate_teardown_intents(mode=None, market=dust_market)
    assert intents == []


def test_teardown_keeps_consolidation_above_dust() -> None:
    strat = _bare_strategy(_position_id_1=None, _position_id_2=None)
    market = _Market(balances={"WETH": Decimal("0.002")})  # $5 at 2500
    intents = strat.generate_teardown_intents(mode=None, market=market)
    assert [i.intent_type.value for i in intents] == ["SWAP"]
    assert intents[0].amount == "all"


def test_teardown_with_open_legs_always_includes_consolidation() -> None:
    strat = _bare_strategy()
    dust_market = _Market(balances={"WETH": Decimal("0.0001")})
    intents = strat.generate_teardown_intents(mode=None, market=dust_market)
    kinds = [i.intent_type.value for i in intents]
    assert kinds == ["LP_CLOSE", "LP_CLOSE", "SWAP"], (
        "with closes pending, amount='all' resolves post-close at execution time "
        f"so the sweep must stay unconditional: {kinds}"
    )
