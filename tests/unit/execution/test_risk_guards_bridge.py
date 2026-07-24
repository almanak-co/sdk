"""Branch coverage for CrossChainRiskGuard validation.

Covers every rule in ``_validate_bridge_intent`` (single/daily limits,
in-flight cap, absolute + percentage balance retention, allowlist), the
``amount='all'`` estimation guard, the cumulative daily check in
``validate_intents``, the execution-intent rules in
``_validate_execution_intent`` (total / per-chain exposure, concentration),
``_estimate_intent_value_usd`` fallbacks, and the tracker integration in
``validate_with_in_flight_exposure``. Pure in-memory validation — no chain,
no oracles.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

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
from almanak.framework.intents.lending_intents import SupplyIntent
from almanak.framework.intents.vocabulary import SwapIntent
from almanak.framework.state.in_flight import InFlightSummary


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


def _swap(amount_usd=Decimal("1000"), chain="arbitrum", **overrides):
    fields = {
        "from_token": "USDC",
        "to_token": "WETH",
        "amount_usd": amount_usd,
        "chain": chain,
    }
    fields.update(overrides)
    return SwapIntent(**fields)


def _exposure_context(*, total=Decimal("0"), per_chain=None):
    context = RiskContext()
    context.total_exposure_usd = total
    context.per_chain_exposure_usd = dict(per_chain or {})
    return context


class TestValidateExecutionIntent:
    def test_swap_passes_within_limits(self, guard):
        result = guard.validate_intent(_swap(), _exposure_context())
        assert result.passed
        assert result.intent_value_usd == Decimal("1000")
        assert "total_exposure_limit" in result.checked_rules
        assert "position_concentration_limit" in result.checked_rules

    def test_precalculated_value_overrides_estimate(self, guard):
        result = guard.validate_intent(
            _swap(), _exposure_context(), intent_value_usd=Decimal("42")
        )
        assert result.intent_value_usd == Decimal("42")

    def test_total_exposure_violation(self):
        guard = CrossChainRiskGuard(
            CrossChainRiskConfig(max_total_exposure_usd=Decimal("1000"))
        )
        result = guard.validate_intent(
            _swap(amount_usd=Decimal("200")), _exposure_context(total=Decimal("900"))
        )
        assert not result.passed
        assert "total_exposure_limit" in _rules_violated(result)
        violation = result.violations[0]
        assert violation.current_value == Decimal("1100")
        assert violation.limit_value == Decimal("1000")

    def test_per_chain_exposure_violation(self, guard):
        guard = CrossChainRiskGuard(
            CrossChainRiskConfig(per_chain_max_exposure_usd={"arbitrum": Decimal("500")})
        )
        result = guard.validate_intent(
            _swap(amount_usd=Decimal("200")),
            _exposure_context(per_chain={"arbitrum": Decimal("400")}),
        )
        assert "per_chain_exposure_limit_arbitrum" in _rules_violated(result)
        violation = result.violations[0]
        assert violation.current_value == Decimal("600")
        assert violation.chain == "arbitrum"

    def test_per_chain_exposure_within_limit(self):
        guard = CrossChainRiskGuard(
            CrossChainRiskConfig(per_chain_max_exposure_usd={"arbitrum": Decimal("5000")})
        )
        result = guard.validate_intent(
            _swap(amount_usd=Decimal("200")),
            _exposure_context(per_chain={"arbitrum": Decimal("400")}),
        )
        assert result.passed
        assert "per_chain_exposure_limit_arbitrum" in result.checked_rules

    def test_chain_without_configured_limit_skips_rule(self):
        guard = CrossChainRiskGuard(
            CrossChainRiskConfig(per_chain_max_exposure_usd={"base": Decimal("500")})
        )
        result = guard.validate_intent(_swap(chain="arbitrum"), _exposure_context())
        assert result.passed
        assert "per_chain_exposure_limit_arbitrum" not in result.checked_rules

    def test_concentration_violation(self, guard):
        # 200 / (100 + 200) = 66.7% > 50% default limit.
        result = guard.validate_intent(
            _swap(amount_usd=Decimal("200")), _exposure_context(total=Decimal("100"))
        )
        assert "position_concentration_limit" in _rules_violated(result)
        violation = result.violations[0]
        assert violation.limit_value == Decimal("0.5")
        assert violation.chain == "arbitrum"

    def test_concentration_skipped_at_zero_exposure(self, guard):
        result = guard.validate_intent(
            _swap(amount_usd=Decimal("5000")), _exposure_context(total=Decimal("0"))
        )
        assert result.passed

    def test_supply_intent_estimated_from_token_amount(self, guard):
        intent = SupplyIntent(protocol="aave_v3", token="USDC", amount=Decimal("250"))
        result = guard.validate_intent(intent, _exposure_context())
        assert result.passed
        assert result.intent_value_usd == Decimal("250")


class TestEstimateIntentValueUsd:
    def test_swap_amount_usd_preferred(self, guard):
        assert guard._estimate_intent_value_usd(_swap(amount_usd=Decimal("777"))) == Decimal("777")

    def test_swap_token_amount_fallback(self, guard):
        intent = _swap(amount_usd=None, amount=Decimal("3"))
        assert guard._estimate_intent_value_usd(intent) == Decimal("3")

    def test_chained_amount_estimates_zero(self, guard):
        intent = SupplyIntent(protocol="aave_v3", token="USDC", amount="all")
        assert guard._estimate_intent_value_usd(intent) == Decimal("0")

    def test_collateral_amount_fallback(self, guard):
        intent = SimpleNamespace(collateral_amount=Decimal("55"))
        assert guard._estimate_intent_value_usd(intent) == Decimal("55")

    def test_non_decimal_collateral_estimates_zero(self, guard):
        intent = SimpleNamespace(collateral_amount="all")
        assert guard._estimate_intent_value_usd(intent) == Decimal("0")

    def test_no_amount_fields_estimates_zero(self, guard):
        assert guard._estimate_intent_value_usd(SimpleNamespace()) == Decimal("0")


def _summary(*, total=Decimal("0"), stale=0):
    return InFlightSummary(
        total_in_flight_usd=total,
        active_transfer_count=1 if total > 0 else 0,
        per_chain_in_flight_usd={},
        per_bridge_in_flight_usd={},
        oldest_transfer_age=timedelta(minutes=5) if total > 0 else None,
        stale_transfer_count=stale,
    )


class TestValidateWithInFlightExposure:
    def test_non_bridge_within_limits(self, guard):
        result = guard.validate_with_in_flight_exposure(
            Intent.hold(reason="idle"), _context(), _summary(total=Decimal("100"))
        )
        assert result.passed
        assert "in_flight_total_exposure" in result.checked_rules
        assert "combined_in_flight_limit" not in result.checked_rules

    def test_in_flight_total_exposure_violation(self):
        guard = CrossChainRiskGuard(
            CrossChainRiskConfig(max_total_exposure_usd=Decimal("1000"))
        )
        context = _exposure_context(total=Decimal("800"))
        result = guard.validate_with_in_flight_exposure(
            Intent.hold(reason="idle"), context, _summary(total=Decimal("300"))
        )
        assert not result.passed
        assert "in_flight_total_exposure" in _rules_violated(result)
        violation = result.violations[0]
        assert violation.current_value == Decimal("1100")

    def test_bridge_combined_in_flight_violation(self):
        guard = CrossChainRiskGuard(
            CrossChainRiskConfig(max_in_flight_exposure_usd=Decimal("1000"))
        )
        context = _context(in_flight=[Decimal("300")])
        result = guard.validate_with_in_flight_exposure(
            _bridge(amount=Decimal("600")), context, _summary(total=Decimal("200"))
        )
        assert not result.passed
        assert "combined_in_flight_limit" in _rules_violated(result)

    def test_bridge_within_limits_checks_combined_rule(self, guard):
        result = guard.validate_with_in_flight_exposure(
            _bridge(), _context(), _summary(total=Decimal("100"))
        )
        assert result.passed
        assert "combined_in_flight_limit" in result.checked_rules

    def test_bridge_uses_provided_value(self):
        guard = CrossChainRiskGuard(
            CrossChainRiskConfig(max_in_flight_exposure_usd=Decimal("1000"))
        )
        # amount=1 would pass; the provided USD value triggers the violation.
        result = guard.validate_with_in_flight_exposure(
            _bridge(amount=Decimal("1")),
            _context(),
            _summary(total=Decimal("500")),
            intent_value_usd=Decimal("600"),
        )
        assert "combined_in_flight_limit" in _rules_violated(result)

    def test_stale_transfers_emit_warning_only(self, guard):
        result = guard.validate_with_in_flight_exposure(
            _bridge(), _context(), _summary(total=Decimal("100"), stale=2)
        )
        assert result.passed
        warning_rules = [w.rule for w in result.warnings]
        assert "stale_transfers_warning" in warning_rules
        warning = result.warnings[0]
        assert warning.severity == "warning"
        assert warning.current_value == Decimal("2")
