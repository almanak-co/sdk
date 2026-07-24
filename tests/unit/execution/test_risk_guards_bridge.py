"""Branch coverage for CrossChainRiskGuard bridge validation.

Covers every rule in ``_validate_bridge_intent`` (single/daily limits,
in-flight cap, absolute + percentage balance retention, allowlist), the
``amount='all'`` estimation guard, and the cumulative daily check in
``validate_intents``. Pure in-memory validation — no chain, no oracles.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.execution.risk_guards import (
    BridgeHistoryEntry,
    ChainBalance,
    CrossChainRiskConfig,
    CrossChainRiskGuard,
    InFlightTransfer,
    RiskContext,
)
from almanak.framework.intents import Intent


def _bridge(amount=Decimal("1000"), preferred_bridge=None):
    return Intent.bridge(
        token="USDC",
        amount=amount,
        from_chain="base",
        to_chain="arbitrum",
        preferred_bridge=preferred_bridge,
    )


def _context(*, balance=Decimal("100000"), bridged_24h=None, in_flight=None):
    context = RiskContext()
    if balance is not None:
        context.chain_balances["base"] = ChainBalance(chain="base", total_balance_usd=balance)
    for amount in bridged_24h or []:
        context.bridge_history_24h.append(
            BridgeHistoryEntry(
                timestamp=datetime.now(UTC),
                amount_usd=amount,
                from_chain="base",
                to_chain="arbitrum",
                bridge="across",
            )
        )
    for amount in in_flight or []:
        context.in_flight_transfers.append(
            InFlightTransfer(
                transfer_id="tr-1",
                token="USDC",
                amount_usd=amount,
                from_chain="base",
                to_chain="arbitrum",
                bridge="across",
            )
        )
    return context


def _rules_violated(result):
    return [violation.rule for violation in result.violations]


@pytest.fixture
def guard() -> CrossChainRiskGuard:
    return CrossChainRiskGuard(CrossChainRiskConfig())


class TestValidateBridgeIntent:
    def test_passes_within_all_limits(self, guard):
        result = guard.validate_intent(_bridge(), _context())
        assert result.passed
        assert result.intent_value_usd == Decimal("1000")
        assert "single_bridge_limit" in result.checked_rules
        assert "bridge_allowlist" in result.checked_rules

    def test_precalculated_value_overrides_estimate(self, guard):
        result = guard.validate_intent(
            _bridge(), _context(), intent_value_usd=Decimal("123")
        )
        assert result.intent_value_usd == Decimal("123")

    def test_chained_amount_estimates_zero(self, guard):
        result = guard.validate_intent(_bridge(amount="all"), _context())
        assert result.passed
        assert result.intent_value_usd == Decimal("0")

    def test_single_bridge_limit_violation(self, guard):
        result = guard.validate_intent(
            _bridge(amount=Decimal("60000")), _context(balance=Decimal("1000000"))
        )
        assert not result.passed
        assert "single_bridge_limit" in _rules_violated(result)
        violation = result.violations[0]
        assert violation.current_value == Decimal("60000")
        assert violation.chain == "base"

    def test_daily_bridge_limit_violation(self, guard):
        context = _context(balance=Decimal("10000000"), bridged_24h=[Decimal("499500")])
        result = guard.validate_intent(_bridge(), context)
        assert "daily_bridge_limit" in _rules_violated(result)

    def test_in_flight_exposure_violation(self, guard):
        context = _context(balance=Decimal("10000000"), in_flight=[Decimal("99500")])
        result = guard.validate_intent(_bridge(), context)
        assert "in_flight_exposure_limit" in _rules_violated(result)

    def test_absolute_balance_retention_violation(self, guard):
        # 1050 - 1000 leaves $50, below the $100 minimum.
        result = guard.validate_intent(_bridge(), _context(balance=Decimal("1050")))
        assert "min_balance_retention" in _rules_violated(result)

    def test_percentage_balance_retention_violation(self, guard):
        # Leaves $200 of $100200 (~0.2%), above the absolute $100 minimum but
        # below the 1% retention requirement.
        guard = CrossChainRiskGuard(
            CrossChainRiskConfig(max_single_bridge_usd=Decimal("200000"))
        )
        result = guard.validate_intent(
            _bridge(amount=Decimal("100000")), _context(balance=Decimal("100200"))
        )
        violated = _rules_violated(result)
        assert "min_balance_retention_pct" in violated
        assert "min_balance_retention" not in violated

    def test_unknown_source_chain_skips_retention(self, guard):
        result = guard.validate_intent(_bridge(), _context(balance=None))
        assert result.passed
        assert "min_balance_retention" in result.checked_rules

    def test_allowlist_blocks_unlisted_bridge(self):
        guard = CrossChainRiskGuard(CrossChainRiskConfig(allowed_bridges=["across"]))
        result = guard.validate_intent(
            _bridge(preferred_bridge="stargate"), _context()
        )
        assert "bridge_allowlist" in _rules_violated(result)

    def test_allowlist_is_case_insensitive(self):
        guard = CrossChainRiskGuard(CrossChainRiskConfig(allowed_bridges=["Across"]))
        result = guard.validate_intent(_bridge(preferred_bridge="ACROSS"), _context())
        assert result.passed

    def test_allowlist_skips_when_no_preference(self):
        guard = CrossChainRiskGuard(CrossChainRiskConfig(allowed_bridges=["across"]))
        result = guard.validate_intent(_bridge(), _context())
        assert result.passed


class TestValidateIntents:
    def test_cumulative_daily_limit_violation(self, guard):
        # Two bridges individually under the daily cap, cumulatively over it.
        context = _context(balance=Decimal("10000000"), bridged_24h=[Decimal("450000")])
        intents = [_bridge(amount=Decimal("30000")), _bridge(amount=Decimal("30000"))]
        result = guard.validate_intents(intents, context)
        assert not result.passed
        assert "cumulative_daily_bridge_limit" in _rules_violated(result)

    def test_hold_intents_pass_through(self, guard):
        result = guard.validate_intents([Intent.hold(reason="idle")], _context())
        assert result.passed
        assert "hold_passthrough" in result.checked_rules
