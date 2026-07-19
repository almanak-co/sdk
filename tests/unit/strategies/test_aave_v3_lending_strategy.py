"""VIB-5916 — ``lifecycle_stop_after`` gate for the chain-generic Aave V3 lending strategy.

Covers the Phase-4 (strategy proofability) contract for the Linea active-pair work:

* Config validation — an unknown ``lifecycle_stop_after`` value fails at boot, the
  option is rejected outside ``force_action="lifecycle"`` mode, and a valid config is
  accepted (default unset is inert).
* State-persistence round-trip including the ``borrowed`` state, and the fact that
  the gate is *config, not state* (it never appears in the persisted snapshot).
* The deliberate HOLD at ``borrowed`` when the gate is set — ``decide()`` returns a
  HOLD, the state stays ``borrowed``, and repeated iterations keep HOLDing.
* Teardown ordering from a ``borrowed`` position: ``REPAY`` (repay_full) then
  ``WITHDRAW`` (withdraw_all); from a supply-only position: only ``WITHDRAW``.
* Restart recovery — a fresh instance under the gate that reloads a ``borrowed``
  state resumes HOLDing, never repaying.

The clean-path lifecycle (gate unset) and the frozen-reserve guard are covered by
``test_aave_v3_lending_lifecycle.py`` / ``test_aave_v3_lending_linea.py``; this file
is scoped to the stop-after gate so a regression in it is easy to localize.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from strategies.incubating.aave_v3_lending.strategy import AaveV3LendingStrategy

_WALLET = "0x" + "aa" * 20

# Mirrors the live-approved Linea pair (USDC collateral -> WETH variable debt).
_BASE_CONFIG = {
    "chain": "linea",
    "wallet_address": _WALLET,
    "collateral_token": "USDC",
    "collateral_amount": "10",
    "borrow_token": "WETH",
    "ltv_target": "0.2",
    "force_action": "lifecycle",
    "check_frozen_reserve": False,
}


def _make_strategy(**overrides) -> AaveV3LendingStrategy:
    """Construct the strategy through the real ``__init__`` (exercises validation)."""
    cfg = {**_BASE_CONFIG, **overrides}
    return AaveV3LendingStrategy(config=cfg, chain=cfg["chain"], wallet_address=_WALLET)


def _market() -> MagicMock:
    market = MagicMock()

    def price(token: str):
        return {"USDC": Decimal("1"), "WETH": Decimal("1854.27")}.get(token, Decimal("0"))

    market.price.side_effect = price
    # Default CLI path wires no aave_health_factor_provider — the accessor
    # returns None (see MarketSnapshot.aave_health_factor).
    market.aave_health_factor.return_value = None
    return market


# =============================================================================
# (a) Config validation
# =============================================================================
class TestLifecycleStopAfterValidation:
    def test_valid_borrowed_accepted(self):
        s = _make_strategy(lifecycle_stop_after="borrowed")
        assert s._lifecycle_stop_after == "borrowed"

    def test_unset_defaults_to_none(self):
        # Neither key present nor empty string -> gate inert.
        assert _make_strategy()._lifecycle_stop_after is None
        assert _make_strategy(lifecycle_stop_after="")._lifecycle_stop_after is None

    def test_value_is_normalized_case_insensitively(self):
        assert _make_strategy(lifecycle_stop_after="BORROWED")._lifecycle_stop_after == "borrowed"
        assert _make_strategy(lifecycle_stop_after="  borrowed  ")._lifecycle_stop_after == "borrowed"

    def test_invalid_value_raises_at_boot(self):
        # Any non-empty value other than "borrowed" must fail at __init__.
        with pytest.raises(ValueError, match="lifecycle_stop_after"):
            _make_strategy(lifecycle_stop_after="repaid")
        with pytest.raises(ValueError, match="lifecycle_stop_after"):
            _make_strategy(lifecycle_stop_after="supplied")

    def test_stop_after_without_lifecycle_mode_raises(self):
        # The gate is meaningless outside force_action="lifecycle".
        with pytest.raises(ValueError, match="requires force_action='lifecycle'"):
            _make_strategy(lifecycle_stop_after="borrowed", force_action="supply")
        with pytest.raises(ValueError, match="requires force_action='lifecycle'"):
            _make_strategy(lifecycle_stop_after="borrowed", force_action="")


# =============================================================================
# (b) State persistence round-trip (incl. borrowed state)
# =============================================================================
class TestPersistenceRoundTrip:
    def test_borrowed_state_round_trips(self):
        s = _make_strategy(lifecycle_stop_after="borrowed")
        s._state = "borrowed"
        s._previous_stable_state = "borrowing"
        s._supplied_amount = Decimal("10")
        s._borrowed_amount = Decimal("0.001078")

        snapshot = s.get_persistent_state()
        assert snapshot["state"] == "borrowed"
        assert snapshot["supplied_amount"] == "10"
        assert snapshot["borrowed_amount"] == "0.001078"

        s2 = _make_strategy(lifecycle_stop_after="borrowed")
        s2.load_persistent_state(snapshot)
        assert s2._state == "borrowed"
        assert s2._previous_stable_state == "borrowing"
        assert s2._supplied_amount == Decimal("10")
        assert s2._borrowed_amount == Decimal("0.001078")

    def test_gate_is_config_not_persisted_state(self):
        # lifecycle_stop_after is config, never persisted — the snapshot must not
        # carry it, so it cannot drift from the config on reload.
        s = _make_strategy(lifecycle_stop_after="borrowed")
        s._state = "borrowed"
        assert "lifecycle_stop_after" not in s.get_persistent_state()


# =============================================================================
# (c) Deliberate HOLD at borrowed when the gate is set
# =============================================================================
class TestDeliberateHoldAtBorrowed:
    _HOLD_REASON = "lifecycle stopped after borrow per config; awaiting teardown signal"

    def test_borrowed_holds_and_stays_borrowed(self):
        s = _make_strategy(lifecycle_stop_after="borrowed")
        s._state = "borrowed"
        s._supplied_amount = Decimal("10")
        s._borrowed_amount = Decimal("0.001")

        intent = s.decide(_market())
        assert intent.intent_type.value == "HOLD"
        assert intent.reason == self._HOLD_REASON
        assert s._state == "borrowed"

    def test_repeated_iterations_keep_holding(self):
        s = _make_strategy(lifecycle_stop_after="borrowed")
        s._state = "borrowed"
        s._supplied_amount = Decimal("10")
        s._borrowed_amount = Decimal("0.001")

        for _ in range(3):
            intent = s.decide(_market())
            assert intent.intent_type.value == "HOLD"
            assert s._state == "borrowed"


# =============================================================================
# (c2) Health-factor telemetry on the stop-after HOLD (warn-only — VIB-5916
#      audit item: a mainnet HOLD on leverage must not be HF-blind)
# =============================================================================
class TestStopAfterHealthTelemetry:
    _HOLD_REASON = "lifecycle stopped after borrow per config; awaiting teardown signal"

    def _borrowed_strategy(self, **overrides) -> AaveV3LendingStrategy:
        s = _make_strategy(lifecycle_stop_after="borrowed", **overrides)
        s._state = "borrowed"
        s._supplied_amount = Decimal("10")
        s._borrowed_amount = Decimal("0.001")
        return s

    def test_hf_below_floor_warns_and_still_holds(self, caplog):
        s = self._borrowed_strategy()
        market = _market()
        market.aave_health_factor.return_value = Decimal("1.2")

        with caplog.at_level("WARNING"):
            intent = s.decide(market)

        assert intent.intent_type.value == "HOLD"
        assert intent.reason == self._HOLD_REASON, "telemetry must not mutate the HOLD reason"
        assert s._state == "borrowed"
        assert any("BELOW" in r.getMessage() for r in caplog.records), (
            "HF 1.2 under the 1.5 default floor must emit the degradation WARNING"
        )
        market.aave_health_factor.assert_called_with(chain="linea")

    def test_hf_above_floor_logs_info_not_warning(self, caplog):
        s = self._borrowed_strategy()
        market = _market()
        market.aave_health_factor.return_value = Decimal("3.9")

        with caplog.at_level("INFO"):
            intent = s.decide(market)

        assert intent.intent_type.value == "HOLD"
        assert not any("BELOW" in r.getMessage() for r in caplog.records)
        assert any("health factor" in r.getMessage() for r in caplog.records), (
            "every stop-after HOLD iteration must surface the live HF"
        )

    def test_hf_unavailable_is_loud_but_not_warning(self, caplog):
        s = self._borrowed_strategy()
        market = _market()
        market.aave_health_factor.return_value = None

        with caplog.at_level("INFO"):
            intent = s.decide(market)

        assert intent.intent_type.value == "HOLD"
        assert any("unavailable" in r.getMessage() for r in caplog.records)
        assert not any(r.levelname == "WARNING" and "BELOW" in r.getMessage() for r in caplog.records)

    def test_hf_read_failure_never_breaks_the_hold(self, caplog):
        s = self._borrowed_strategy()
        market = _market()
        market.aave_health_factor.side_effect = RuntimeError("gateway hiccup")

        with caplog.at_level("WARNING"):
            intent = s.decide(market)

        assert intent.intent_type.value == "HOLD", "a failed HF read must not change the decision"
        assert s._state == "borrowed"
        assert any("read failed" in r.getMessage() for r in caplog.records)

    def test_invalid_min_health_factor_raises_at_boot(self):
        with pytest.raises(ValueError, match="stop_after_min_health_factor"):
            _make_strategy(lifecycle_stop_after="borrowed", stop_after_min_health_factor="0")

    def test_gate_unset_still_transitions_to_repaying(self):
        # Regression guard: with the gate unset behaviour is exactly the current
        # lifecycle — borrowed advances to repaying (no HOLD injected).
        s = _make_strategy()  # no lifecycle_stop_after
        s._state = "borrowed"
        s._borrowed_amount = Decimal("0.001")

        intent = s.decide(_market())
        assert intent.intent_type.value == "REPAY"
        assert s._state == "repaying"


# =============================================================================
# (d) Teardown ordering
# =============================================================================
class TestTeardownOrdering:
    def test_teardown_from_borrowed_is_repay_then_withdraw(self):
        from almanak.framework.teardown import TeardownMode

        s = _make_strategy(lifecycle_stop_after="borrowed")
        s._state = "borrowed"
        s._supplied_amount = Decimal("10")
        s._borrowed_amount = Decimal("0.001")

        intents = s.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 2
        repay, withdraw = intents
        assert repay.intent_type.value == "REPAY"
        assert repay.repay_full is True
        assert repay.token == "WETH"
        assert withdraw.intent_type.value == "WITHDRAW"
        assert withdraw.withdraw_all is True
        assert withdraw.token == "USDC"

    def test_teardown_from_supplied_only_is_withdraw_only(self):
        from almanak.framework.teardown import TeardownMode

        s = _make_strategy(lifecycle_stop_after="borrowed")
        s._state = "supplied"
        s._supplied_amount = Decimal("10")
        s._borrowed_amount = Decimal("0")

        intents = s.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"
        assert intents[0].withdraw_all is True


# =============================================================================
# (e) Restart recovery under the gate
# =============================================================================
class TestRestartRecoveryUnderGate:
    def test_reload_borrowed_resumes_holding_not_repaying(self):
        # Simulate a restart: a fresh instance (gate re-read from config) reloads
        # a persisted borrowed state and must HOLD, never repay.
        persisted = {
            "state": "borrowed",
            "previous_stable_state": "borrowing",
            "supplied_amount": "10",
            "borrowed_amount": "0.001078",
            "frozen_detected": False,
            "failure_details": [],
        }

        fresh = _make_strategy(lifecycle_stop_after="borrowed")
        fresh.load_persistent_state(persisted)
        assert fresh._state == "borrowed"

        intent = fresh.decide(_market())
        assert intent.intent_type.value == "HOLD"
        assert intent.reason == "lifecycle stopped after borrow per config; awaiting teardown signal"
        assert fresh._state == "borrowed"
