"""VIB-5664 — unit tests for the vault-wrapped rebalancing-LP strategy.

Covers the state machine + rebalance trigger, the LP-inclusive ``valuate()``
(including the Empty ≠ Zero raise-on-unmeasured rule), the settlement/rebalance
interleave gate, and the tick-range snapping.
"""

from __future__ import annotations

import importlib.util
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

_STRAT_PATH = (
    Path(__file__).resolve().parents[3] / "strategies" / "internal" / "vaults" / "lagoon_lp_rebalance" / "strategy.py"
)
_spec = importlib.util.spec_from_file_location("lagoon_lp_rebalance_strategy", _STRAT_PATH)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["lagoon_lp_rebalance_strategy"] = _mod
_spec.loader.exec_module(_mod)

LagoonLpRebalanceStrategy = _mod.LagoonLpRebalanceStrategy
LPValuationUnmeasuredError = _mod.LPValuationUnmeasuredError
PHASE_INIT = _mod.PHASE_INIT
PHASE_SWAPPED_IN = _mod.PHASE_SWAPPED_IN
PHASE_LP_ACTIVE = _mod.PHASE_LP_ACTIVE
PHASE_REBALANCING = _mod.PHASE_REBALANCING
PHASE_DONE = _mod.PHASE_DONE


def _make(**overrides):
    config = {
        "pool": "WETH/USDC/100",
        "starting_asset": "USDC",
        "chain": "base",
        "range_width_pct": 0.30,
        "rebalance_threshold_pct": 0.90,
        "swap_split_pct": 0.50,
        "max_slippage": 0.005,
    }
    config.update(overrides)
    return LagoonLpRebalanceStrategy(config=config, chain="base", wallet_address="0x" + "11" * 20)


def _market(*, total_usd=Decimal("1000"), prices=None, balances_usd=None, lp=None):
    prices = prices or {"WETH": Decimal("3500"), "USDC": Decimal("1")}
    balances_usd = balances_usd or {}
    m = MagicMock()
    m.total_portfolio_usd.return_value = total_usd

    def price(sym, quote="USD"):
        return prices[sym]

    def balance(sym):
        return SimpleNamespace(balance=Decimal("1"), balance_usd=balances_usd.get(sym, Decimal("0")))

    m.price = price
    m.balance = balance
    m.lp_position_value.return_value = lp
    return m


def _lp(value=Decimal("500"), fees=Decimal("2"), in_range=True):
    return SimpleNamespace(value_usd=value, fees_usd=fees, in_range=in_range)


class TestStateMachine:
    def test_init_returns_initial_swap(self):
        s = _make()
        intent = s.decide(_market(total_usd=Decimal("1000")))
        assert intent.intent_type.value == "SWAP"
        # 50% of NAV
        assert intent.from_token == "USDC" and intent.to_token == "WETH"

    def test_swap_from_init_advances_to_swapped_in(self):
        s = _make()
        swap = SimpleNamespace(intent_type=SimpleNamespace(value="SWAP"))
        s.on_intent_executed(swap, True, None)
        assert s._phase == PHASE_SWAPPED_IN

    def test_swapped_in_returns_lp_open(self):
        s = _make()
        s._phase = PHASE_SWAPPED_IN
        intent = s.decide(_market())
        assert intent.intent_type.value == "LP_OPEN"

    def test_lp_open_captures_position_and_activates(self):
        s = _make()
        s._phase = PHASE_SWAPPED_IN
        lp_open = SimpleNamespace(intent_type=SimpleNamespace(value="LP_OPEN"))
        result = SimpleNamespace(position_id=999, lp_open_data=None, extracted_data=None)
        s.on_intent_executed(lp_open, True, result)
        assert s._phase == PHASE_LP_ACTIVE
        assert s._position_id == "999"

    def test_lp_open_without_position_id_raises(self):
        s = _make()
        s._phase = PHASE_SWAPPED_IN
        lp_open = SimpleNamespace(intent_type=SimpleNamespace(value="LP_OPEN"))
        result = SimpleNamespace(position_id=None)
        with pytest.raises(RuntimeError, match="no position_id"):
            s.on_intent_executed(lp_open, True, result)

    def test_lp_close_clears_position_and_goes_rebalancing(self):
        s = _make()
        s._phase = PHASE_LP_ACTIVE
        s._position_id = "999"
        s._pool_address = "0xpool"
        lp_close = SimpleNamespace(intent_type=SimpleNamespace(value="LP_CLOSE"))
        s.on_intent_executed(lp_close, True, None)
        assert s._phase == PHASE_REBALANCING
        assert s._position_id is None
        assert s._rebalance_count == 1

    def test_rebalance_swap_returns_to_swapped_in(self):
        s = _make()
        s._phase = PHASE_REBALANCING
        swap = SimpleNamespace(intent_type=SimpleNamespace(value="SWAP"))
        s.on_intent_executed(swap, True, None)
        assert s._phase == PHASE_SWAPPED_IN

    def test_rebalancing_builds_balancing_swap(self):
        s = _make()
        s._phase = PHASE_REBALANCING
        # Skewed heavily to WETH after out-of-range close.
        m = _market(balances_usd={"WETH": Decimal("900"), "USDC": Decimal("100")})
        intent = s.decide(m)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "WETH" and intent.to_token == "USDC"


class TestRebalanceTrigger:
    def test_in_range_holds(self):
        s = _make()
        s._phase = PHASE_LP_ACTIVE
        s._position_id = "999"
        intent = s.decide(_market(lp=_lp(in_range=True)))
        assert intent.intent_type.value == "HOLD"

    def test_out_of_range_closes(self):
        s = _make()
        s._phase = PHASE_LP_ACTIVE
        s._position_id = "999"
        intent = s.decide(_market(lp=_lp(in_range=False)))
        assert intent.intent_type.value == "LP_CLOSE"

    def test_unmeasured_lp_holds_not_churn(self):
        s = _make()
        s._phase = PHASE_LP_ACTIVE
        s._position_id = "999"
        intent = s.decide(_market(lp=None))
        assert intent.intent_type.value == "HOLD"


class TestValuate:
    def test_all_cash_phase_excludes_lp(self):
        s = _make()
        s._phase = PHASE_INIT
        m = _market(total_usd=Decimal("1000"), lp=_lp(value=Decimal("500")))
        assert s.valuate(m) == Decimal("1000")
        m.lp_position_value.assert_not_called()

    def test_lp_active_adds_lp_value_and_fees(self):
        s = _make()
        s._phase = PHASE_LP_ACTIVE
        s._position_id = "999"
        m = _market(total_usd=Decimal("10"), lp=_lp(value=Decimal("500"), fees=Decimal("2")))
        # free cash 10 + lp 500 + fees 2
        assert s.valuate(m) == Decimal("512")

    def test_lp_active_read_failure_raises(self):
        """Empty ≠ Zero — a None LP read while LP_ACTIVE must RAISE, not fall back."""
        s = _make()
        s._phase = PHASE_LP_ACTIVE
        s._position_id = "999"
        m = _market(total_usd=Decimal("10"), lp=None)
        with pytest.raises(LPValuationUnmeasuredError):
            s.valuate(m)

    def test_swapped_in_phase_is_cash_only(self):
        s = _make()
        s._phase = PHASE_SWAPPED_IN
        m = _market(total_usd=Decimal("1000"), lp=_lp())
        assert s.valuate(m) == Decimal("1000")
        m.lp_position_value.assert_not_called()

    def test_lp_active_without_position_id_raises(self):
        """LP_ACTIVE with no captured position id is an inconsistent state — valuate
        must RAISE (refuse a free-cash-only NAV) rather than silently understate assets."""
        s = _make()
        s._phase = PHASE_LP_ACTIVE
        s._position_id = None
        m = _market(total_usd=Decimal("10"), lp=_lp(value=Decimal("500")))
        with pytest.raises(LPValuationUnmeasuredError):
            s.valuate(m)
        m.lp_position_value.assert_not_called()


class TestSettlementGate:
    @pytest.mark.parametrize(
        "phase,allowed",
        [
            (PHASE_INIT, True),
            (PHASE_LP_ACTIVE, True),
            (PHASE_DONE, True),
            (PHASE_SWAPPED_IN, False),
            (PHASE_REBALANCING, False),
        ],
    )
    def test_gate(self, phase, allowed):
        s = _make()
        s._phase = phase
        assert s.vault_settlement_allowed() is allowed


class TestTickRange:
    def test_snaps_to_spacing_and_orders(self):
        s = _make()
        lower, upper = s._compute_tick_range(_market())
        assert lower % s.tick_spacing == 0
        assert upper % s.tick_spacing == 0
        assert upper > lower

    def test_non_positive_price_raises(self):
        """Both legs must be positive — a stale-oracle 0 on EITHER token must raise
        (a 0 token0 would blow up price_to_tick's log with a math-domain error)."""
        s = _make()
        with pytest.raises(ValueError, match="Non-positive price"):
            s._compute_tick_range(_market(prices={"WETH": Decimal("0"), "USDC": Decimal("1")}))
        with pytest.raises(ValueError, match="Non-positive price"):
            s._compute_tick_range(_market(prices={"WETH": Decimal("3500"), "USDC": Decimal("0")}))


class TestDustSwapGuard:
    def test_zero_nav_init_holds_instead_of_swapping(self):
        """An unfunded vault ($0 NAV) must HOLD, not emit a $0 swap that reverts."""
        s = _make()
        intent = s.decide(_market(total_usd=Decimal("0")))
        assert intent.intent_type.value == "HOLD"


class TestPersistence:
    def test_round_trips_phase_and_position(self):
        s = _make()
        s._phase = PHASE_LP_ACTIVE
        s._position_id = "12345"
        s._pool_address = "0xpool"
        s._range_lower, s._range_upper = -200, 200
        s._rebalance_count = 3
        state = s.get_persistent_state()

        s2 = _make()
        s2.load_persistent_state(state)
        assert s2._phase == PHASE_LP_ACTIVE
        assert s2._position_id == "12345"
        assert s2._pool_address == "0xpool"
        assert (s2._range_lower, s2._range_upper) == (-200, 200)
        assert s2._rebalance_count == 3


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
