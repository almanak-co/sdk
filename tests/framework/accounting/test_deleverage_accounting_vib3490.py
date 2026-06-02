"""Tests for VIB-3490: DELEVERAGE event enrichment and runner detection.

Covers:
  - test_deleverage_intent_emits_deleverage_event_type
  - test_deleverage_event_distinguishable_from_repay
  - test_deleverage_warning_logged
"""

from __future__ import annotations

import logging
from decimal import Decimal
from unittest.mock import MagicMock


def _make_basis_store():
    from almanak.framework.accounting.basis import FIFOBasisStore

    return FIFOBasisStore()


def _make_result(tx_hash: str = "0xdeadc0de12345678"):
    result = MagicMock()
    result.tx_hash = tx_hash
    result.extracted_data = {}
    result.total_gas_cost_wei = None
    result.transaction_results = []
    return result


def _make_deleverage_intent(
    *,
    trigger_reason: str = "HF 1.05 < emergency_threshold 1.2",
    observed_hf: Decimal | None = Decimal("1.05"),
    target_hf: Decimal | None = Decimal("2.0"),
):
    """Build a real DeleverageIntent for use in tests."""
    from almanak.framework.intents.vocabulary import DeleverageIntent

    return DeleverageIntent(
        protocol="aave_v3",
        token="USDC",
        amount=Decimal("0"),
        repay_full=True,
        market_id=None,
        chain="arbitrum",
        trigger_reason=trigger_reason,
        observed_hf=observed_hf,
        target_hf=target_hf,
    )


def _make_repay_intent():
    """Build a mock RepayIntent (intent_type = REPAY) for comparison tests."""
    intent = MagicMock()
    it = MagicMock()
    it.value = "REPAY"
    intent.intent_type = it
    intent.protocol = "aave_v3"
    intent.token = "USDC"
    intent.borrow_token = None
    intent.market_id = None
    # No deleverage-specific fields
    return intent


def _call_build(intent, tx_hash: str = "0xdeadc0de12345678"):
    from almanak.framework.accounting.lending_accounting import build_lending_accounting_event

    result = _make_result(tx_hash)
    return build_lending_accounting_event(
        intent=intent,
        result=result,
        deployment_id="strat-1",
        cycle_id="cycle-001",
        execution_mode="paper",
        chain="arbitrum",
        wallet_address="0xwallet",
        gateway_client=None,
        basis_store=_make_basis_store(),
        price_oracle=None,
        ledger_entry_id="led-001",
    )


class TestDeleverageEventType:
    """VIB-3490: DELEVERAGE intent produces LendingEventType.DELEVERAGE (not REPAY)."""

    def test_deleverage_intent_emits_deleverage_event_type(self):
        """A DeleverageIntent must produce event_type=DELEVERAGE, not REPAY."""
        from almanak.framework.accounting.models import LendingEventType

        intent = _make_deleverage_intent()
        event = _call_build(intent)

        assert event is not None, "build_lending_accounting_event returned None for DELEVERAGE intent"
        assert event.event_type == LendingEventType.DELEVERAGE, (
            f"Expected LendingEventType.DELEVERAGE, got {event.event_type!r}"
        )

    def test_deleverage_event_distinguishable_from_repay(self):
        """DELEVERAGE and REPAY events produced side-by-side differ in event_type."""
        from almanak.framework.accounting.models import LendingEventType

        deleverage_intent = _make_deleverage_intent()
        repay_intent = _make_repay_intent()

        deleverage_event = _call_build(deleverage_intent, tx_hash="0xdeadc0de00000001")
        repay_event = _call_build(repay_intent, tx_hash="0xdeadc0de00000002")

        assert deleverage_event is not None
        assert repay_event is not None

        # Core distinguishability assertion
        assert deleverage_event.event_type == LendingEventType.DELEVERAGE
        assert repay_event.event_type == LendingEventType.REPAY
        assert deleverage_event.event_type != repay_event.event_type

    def test_deleverage_trigger_context_in_unavailable_reason(self):
        """DELEVERAGE event must include trigger context in unavailable_reason."""
        intent = _make_deleverage_intent(
            trigger_reason="HF 1.08 < emergency_threshold 1.2",
            observed_hf=Decimal("1.08"),
            target_hf=Decimal("2.0"),
        )
        event = _call_build(intent)

        assert event is not None
        # trigger context must be present
        assert "DELEVERAGE" in event.unavailable_reason, (
            f"Expected 'DELEVERAGE' in unavailable_reason, got: {event.unavailable_reason!r}"
        )
        assert "1.08" in event.unavailable_reason, (
            f"Expected observed_hf in unavailable_reason, got: {event.unavailable_reason!r}"
        )
        assert "2.0" in event.unavailable_reason, (
            f"Expected target_hf in unavailable_reason, got: {event.unavailable_reason!r}"
        )

    def test_deleverage_observed_hf_persisted_as_hf_before(self):
        """observed_hf from the intent is stored as health_factor_before."""
        intent = _make_deleverage_intent(observed_hf=Decimal("1.07"))
        event = _call_build(intent)

        assert event is not None
        assert event.health_factor_before == Decimal("1.07"), (
            f"Expected health_factor_before=1.07, got {event.health_factor_before!r}"
        )

    def test_deleverage_without_observed_hf_has_none_hf_before(self):
        """When observed_hf is not set, health_factor_before is None."""
        intent = _make_deleverage_intent(observed_hf=None)
        event = _call_build(intent)

        assert event is not None
        assert event.health_factor_before is None

    def test_deleverage_no_trigger_reason_uses_default_context(self):
        """DELEVERAGE without trigger_reason still sets default context."""
        intent = _make_deleverage_intent(trigger_reason="")
        event = _call_build(intent)

        assert event is not None
        assert "emergency-triggered" in event.unavailable_reason, (
            f"Expected 'emergency-triggered' in unavailable_reason, got: {event.unavailable_reason!r}"
        )


class TestDeleverageProtocolValidation:
    """VIB-3490: DeleverageIntent enforces the same protocol-param rules as RepayIntent."""

    def test_deleverage_morpho_blue_without_market_id_raises(self):
        """DeleverageIntent with morpho_blue protocol must require market_id."""
        import pytest
        from almanak.framework.intents.vocabulary import DeleverageIntent
        from almanak.framework.intents.intent_errors import InvalidProtocolParameterError

        with pytest.raises((InvalidProtocolParameterError, ValueError)):
            DeleverageIntent(
                protocol="morpho_blue",
                token="USDC",
                amount=Decimal("500"),
                repay_full=False,
                market_id=None,  # morpho_blue requires market_id
            )

    def test_deleverage_unavailable_reason_not_set_on_high_confidence_event(self):
        """unavailable_reason must remain empty for HIGH-confidence DELEVERAGE events.

        This ensures we don't misuse unavailable_reason as a metadata carrier
        when the on-chain read succeeded (got_after_state=True). In test context,
        gateway_client=None means got_after_state=False; this test documents the
        contract via a mock that simulates a successful read.
        """
        from almanak.framework.accounting.lending_accounting import build_lending_accounting_event
        from almanak.framework.accounting.models import AccountingConfidence
        from unittest.mock import patch

        intent = _make_deleverage_intent(
            trigger_reason="HF_below_threshold",
            observed_hf=Decimal("1.1"),
            target_hf=Decimal("1.5"),
        )
        result = _make_result()

        # Simulate a successful after-state read by patching the generic
        # read_lending_account_state to return a non-None Aave-family
        # LendingAccountState (VIB-4929 PR-3a — the per-protocol reader is gone).
        mock_aave_state = MagicMock()
        mock_aave_state.collateral_usd = Decimal("10000")
        mock_aave_state.debt_usd = Decimal("5000")
        mock_aave_state.health_factor = Decimal("1.5")
        mock_aave_state.liquidation_threshold_bps = 8000
        mock_aave_state.lltv = None  # Aave carries the threshold as bps, not lltv
        mock_aave_state.family = "aave"  # structural discriminator the unify/dict path reads

        with patch(
            "almanak.framework.accounting.lending_accounting.read_lending_account_state",
            return_value=mock_aave_state,
        ):
            event = build_lending_accounting_event(
                intent=intent,
                result=result,
                deployment_id="strat-1",
                cycle_id="cycle-001",
                execution_mode="live",
                chain="arbitrum",
                wallet_address="0xwallet",
                gateway_client=MagicMock(),  # triggers the read path
                basis_store=_make_basis_store(),
                price_oracle=None,
                ledger_entry_id="led-001",
            )

        assert event is not None
        assert event.confidence == AccountingConfidence.HIGH
        # unavailable_reason must be empty for HIGH-confidence events
        assert event.unavailable_reason == "", (
            f"unavailable_reason must be empty for HIGH-confidence events, got: {event.unavailable_reason!r}"
        )
        # But health_factor_before must still be set from observed_hf
        assert event.health_factor_before == Decimal("1.1"), (
            f"Expected health_factor_before=1.1, got {event.health_factor_before!r}"
        )


class TestDeleverageWarningLogged:
    """VIB-3490: WARNING is logged when a DELEVERAGE intent is processed by the runner."""

    def test_deleverage_warning_logged(self, caplog):
        """_maybe_warn_deleverage must log WARNING when a DELEVERAGE intent is detected."""
        # VIB-3478: warning moved from _try_write_lending_accounting (removed) to
        # _maybe_warn_deleverage, called on the success path in _single_chain_handle_success.

        from almanak.framework.runner.strategy_runner import StrategyRunner
        from almanak.framework.intents.vocabulary import DeleverageIntent

        runner = StrategyRunner.__new__(StrategyRunner)

        strategy = MagicMock()
        strategy.deployment_id = "test-strat"

        intent = DeleverageIntent(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("0"),
            repay_full=True,
            trigger_reason="HF 1.09 < emergency_threshold 1.2",
            observed_hf=Decimal("1.09"),
            target_hf=Decimal("2.0"),
        )

        with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
            runner._maybe_warn_deleverage(intent, strategy)

        warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        deleverage_warnings = [m for m in warning_messages if "DELEVERAGE" in m]
        assert deleverage_warnings, (
            f"Expected at least one WARNING log containing 'DELEVERAGE'. "
            f"Got warning messages: {warning_messages!r}"
        )
        # Verify trigger context is in the log message
        assert any("1.09" in m for m in deleverage_warnings), (
            f"Expected observed_hf '1.09' in WARNING log, got: {deleverage_warnings!r}"
        )
