"""Unit tests for the metamorpho_base_yield demo — yield-floor entry/exit gate.

Drives the state machine with a mocked ``lending_rate`` (APY feed) and balance:
enter only when APY >= floor, exit after N consecutive sub-floor reads, never
churn on an unavailable read.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.demo_strategies.metamorpho_base_yield.strategy import MetaMorphoBaseYield

_WALLET = "0x" + "1" * 40
_CFG = {
    "deposit_amount": "50",
    "min_deposit_usd": "25",
    "min_apy_floor": "3.0",
    "exit_confirm_checks": 2,
    "rate_protocol": "morpho_blue",
    "rate_token": "USDC",
}


def _make() -> MetaMorphoBaseYield:
    return MetaMorphoBaseYield(chain="base", wallet_address=_WALLET, config=_CFG)


def _market(apy, bal_usd=100.0):
    m = MagicMock()
    m.lending_rate.return_value = SimpleNamespace(apy_percent=apy)
    m.balance.return_value = SimpleNamespace(balance=Decimal("100"), balance_usd=Decimal(str(bal_usd)))
    return m


def _market_no_rate(bal_usd=100.0):
    m = MagicMock()
    m.lending_rate.side_effect = ValueError("rate unavailable")
    m.balance.return_value = SimpleNamespace(balance=Decimal("100"), balance_usd=Decimal(str(bal_usd)))
    return m


def _fill_deposit(s):
    intent = s.decide(_market(5.0))
    s.on_intent_executed(intent, True, SimpleNamespace(extracted_data={"deposit_data": {"assets": 50, "shares": 48}}))
    return s


def test_rejects_bad_config():
    with pytest.raises(ValueError, match="min_apy_floor"):
        MetaMorphoBaseYield(chain="base", wallet_address=_WALLET, config={**_CFG, "min_apy_floor": "-1"})
    with pytest.raises(ValueError, match="exit_confirm_checks"):
        MetaMorphoBaseYield(chain="base", wallet_address=_WALLET, config={**_CFG, "exit_confirm_checks": 0})


# ----------------------------------------------------------------- entry gate


def test_no_deposit_when_apy_below_floor():
    s = _make()
    intent = s.decide(_market(2.0))  # 2% < 3% floor
    assert intent.intent_type.value == "HOLD"
    assert s._state == "idle"


def test_deposits_when_apy_above_floor():
    s = _make()
    intent = s.decide(_market(5.0))
    assert intent.intent_type.value == "VAULT_DEPOSIT"


def test_entry_not_blocked_when_apy_unavailable():
    """A missing rate read must NOT block entry (deposit is low-risk; don't NULL the demo)."""
    s = _make()
    intent = s.decide(_market_no_rate())
    assert intent.intent_type.value == "VAULT_DEPOSIT"


# ------------------------------------------------------------------ exit gate


def test_holds_while_apy_healthy():
    s = _fill_deposit(_make())
    intent = s.decide(_market(5.0))
    assert intent.intent_type.value == "HOLD"
    assert s._below_floor_count == 0


def test_exits_after_consecutive_subfloor_reads():
    s = _fill_deposit(_make())
    first = s.decide(_market(1.0))  # #1 — confirm, not exit yet
    assert first.intent_type.value == "HOLD"
    assert s._below_floor_count == 1
    second = s.decide(_market(1.0))  # #2 — exit
    assert second.intent_type.value == "VAULT_REDEEM"
    assert s._state == "redeeming"


def test_transient_dip_resets_and_does_not_exit():
    s = _fill_deposit(_make())
    s.decide(_market(1.0))  # dip #1
    assert s._below_floor_count == 1
    s.decide(_market(5.0))  # recovers -> counter resets
    assert s._below_floor_count == 0
    intent = s.decide(_market(1.0))  # dip again, only #1 -> no exit
    assert intent.intent_type.value == "HOLD"


def test_never_exits_on_unavailable_apy():
    s = _fill_deposit(_make())
    intent = s.decide(_market_no_rate())
    assert intent.intent_type.value == "HOLD"
    assert s._below_floor_count == 0  # missing read does not count toward exit


def test_redeem_resets_exit_state():
    s = _fill_deposit(_make())
    s.decide(_market(1.0))
    redeem = s.decide(_market(1.0))
    assert redeem.intent_type.value == "VAULT_REDEEM"
    s.on_intent_executed(redeem, True, SimpleNamespace(extracted_data={"redeem_data": {"assets_received": 50}}))
    assert s._state == "idle"
    assert s._below_floor_count == 0


def test_redeem_clears_compound_timers():
    """A redeem must clear the compound timers so a later re-entry waits a fresh
    interval instead of compounding immediately off a stale _last_compound_time."""
    s = _fill_deposit(_make())
    s._last_compound_time = datetime.now(UTC)
    s.decide(_market(1.0))
    redeem = s.decide(_market(1.0))
    s.on_intent_executed(redeem, True, SimpleNamespace(extracted_data={"redeem_data": {"assets_received": 50}}))
    assert s._deposit_timestamp is None
    assert s._last_compound_time is None


def test_unavailable_apy_holds_when_compound_interval_elapsed():
    """Even with the compound interval elapsed, an unavailable APY must HOLD —
    never add capital (a fresh VAULT_DEPOSIT) without a readable yield signal."""
    s = _fill_deposit(_make())
    s._last_compound_time = datetime.now(UTC) - timedelta(hours=999)
    intent = s.decide(_market_no_rate())
    assert intent.intent_type.value == "HOLD"


def test_status_and_persistence_expose_apy_gate_state():
    s = _fill_deposit(_make())
    s.decide(_market(1.0))  # one sub-floor read -> below_floor_count == 1, current_apy == 1.0

    status = s.get_status()
    assert status["current_apy"] == "1.0"
    assert status["below_floor_count"] == 1
    assert status["min_apy_floor"] == "3.0"

    restored = _make()
    restored.load_persistent_state(s.get_persistent_state())
    assert restored._below_floor_count == 1
    assert restored._current_apy == Decimal("1.0")


def test_non_finite_apy_propagates():
    """A malformed (non-finite) rate is a bug, not 'unavailable' — it must propagate,
    not silently degrade to a capital-adding deposit."""
    s = _fill_deposit(_make())  # deposited -> _handle_deposited reads APY first
    m = _market(5.0)
    m.lending_rate.return_value = SimpleNamespace(apy_percent="NaN")
    with pytest.raises(ValueError, match="non-finite"):
        s.decide(m)
