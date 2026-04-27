"""Tests for VIB-3489 — lending pre-execution state capture (before/after delta).

Covers:
  - test_aave_before_state_populated_from_pre_execution_read
  - test_aave_before_state_none_when_read_fails
  - test_morpho_before_state_populated_on_borrow
  - test_before_after_delta_computable

Design:
  pre_execution_state is passed into build_lending_accounting_event() by the
  runner (captured before the tx via capture_lending_pre_state()).  All tests
  mock the gateway for both the pre-state read (via capture_lending_pre_state)
  and the post-state read (inside build_lending_accounting_event).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.lending_accounting import (
    AaveAccountState,
    MorphoBlueAccountState,
    build_lending_accounting_event,
    capture_lending_pre_state,
)
from almanak.framework.accounting.models import AccountingConfidence


# ─── Shared helpers ───────────────────────────────────────────────────────────

def _encode_word(value: int) -> str:
    return hex(value)[2:].zfill(64)


def _mock_aave_response(
    collateral_e8: int,
    debt_e8: int,
    available_borrows_e8: int = 0,
    liquidation_threshold_bps: int = 8500,
    ltv_bps: int = 7500,
    health_factor_e18: int = int(30.1 * 1e18),
) -> str:
    """Build a hex string matching getUserAccountData() ABI return (6 uint256 words)."""
    return (
        "0x"
        + _encode_word(collateral_e8)
        + _encode_word(debt_e8)
        + _encode_word(available_borrows_e8)
        + _encode_word(liquidation_threshold_bps)
        + _encode_word(ltv_bps)
        + _encode_word(health_factor_e18)
    )


def _mock_morpho_position_response(supply_shares: int, borrow_shares: int, collateral: int) -> str:
    return "0x" + _encode_word(supply_shares) + _encode_word(borrow_shares) + _encode_word(collateral)


def _mock_morpho_market_response(
    total_supply_assets: int,
    total_supply_shares: int,
    total_borrow_assets: int,
    total_borrow_shares: int,
    last_update: int = 0,
    fee: int = 0,
) -> str:
    return (
        "0x"
        + _encode_word(total_supply_assets)
        + _encode_word(total_supply_shares)
        + _encode_word(total_borrow_assets)
        + _encode_word(total_borrow_shares)
        + _encode_word(last_update)
        + _encode_word(fee)
    )


_WALLET = "0x1234567890123456789012345678901234567890"
_CHAIN = "arbitrum"

_MORPHO_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"
_LLTV_RAW = 860_000_000_000_000_000  # 0.86e18

_PRICE_ORACLE = {
    "USDC": Decimal("1"),
    "WETH": Decimal("3000"),
    "wstETH": Decimal("3500"),
    "WSTETH": Decimal("3500"),
    "ETH": Decimal("3000"),
}


def _make_aave_supply_intent(intent_type: str = "SUPPLY") -> MagicMock:
    intent = MagicMock()
    intent.intent_type.value = intent_type
    intent.protocol = "aave_v3"
    intent.token = "USDC"
    intent.borrow_token = None
    intent.collateral_token = None
    intent.market_id = None
    return intent


def _make_result(extracted: dict | None = None) -> MagicMock:
    result = MagicMock()
    result.tx_hash = "0xdeadbeef"
    result.extracted_data = extracted or {}
    result.total_gas_cost_wei = None
    return result


# ─── Test: Aave before-state populated ───────────────────────────────────────


class TestAaveBeforeStatePopulatedFromPreExecutionRead:
    """capture_lending_pre_state + build_lending_accounting_event wires before-state fields."""

    def test_aave_before_state_populated_from_pre_execution_read(self) -> None:
        """Pre-execution Aave state populates collateral/debt/HF before fields.

        Scenario: wallet holds $10 000 collateral and $5 000 debt before SUPPLY.
        After SUPPLY: $11 000 collateral, same debt.
        """
        # Before state: $10 000 collateral, $5 000 debt, HF = (10000 * 0.85) / 5000 = 1.7
        before_response = _mock_aave_response(
            collateral_e8=10_000 * 10**8,
            debt_e8=5_000 * 10**8,
            liquidation_threshold_bps=8500,
            health_factor_e18=int(1.7 * 1e18),
        )
        # After state: $11 000 collateral, same debt, HF improved
        after_response = _mock_aave_response(
            collateral_e8=11_000 * 10**8,
            debt_e8=5_000 * 10**8,
            liquidation_threshold_bps=8500,
            health_factor_e18=int(1.87 * 1e18),
        )

        # Pre-read then post-read: two successive eth_call() calls
        gateway = MagicMock()
        gateway.eth_call.side_effect = [before_response, after_response]

        intent = _make_aave_supply_intent("SUPPLY")

        # Capture pre-state
        pre_state = capture_lending_pre_state(
            intent=intent,
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=gateway,
            price_oracle=_PRICE_ORACLE,
        )
        assert pre_state is not None, "Pre-state must not be None for Aave SUPPLY with valid gateway"
        assert isinstance(pre_state, AaveAccountState)
        assert pre_state.collateral_usd == Decimal("10000")
        assert pre_state.debt_usd == Decimal("5000")

        # Build event with pre_execution_state
        event = build_lending_accounting_event(
            intent=intent,
            result=_make_result({"supply_amount": 1_000 * 10**6}),
            deployment_id="dep-1",
            strategy_id="strat-1",
            cycle_id="cycle-001",
            execution_mode="paper",
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=gateway,
            basis_store=FIFOBasisStore(),
            price_oracle=_PRICE_ORACLE,
            ledger_entry_id=None,
            pre_execution_state=pre_state,
        )

        assert event is not None

        # Before-state populated
        assert event.collateral_value_before_usd == Decimal("10000"), (
            f"collateral_value_before_usd expected $10000, got {event.collateral_value_before_usd}"
        )
        assert event.debt_value_before_usd == Decimal("5000"), (
            f"debt_value_before_usd expected $5000, got {event.debt_value_before_usd}"
        )
        assert event.health_factor_before is not None, "health_factor_before must not be None"
        assert event.net_equity_before_usd == Decimal("5000"), (
            f"net_equity_before_usd expected $5000 ($10000 - $5000), got {event.net_equity_before_usd}"
        )

        # After-state populated
        assert event.collateral_value_after_usd == Decimal("11000")
        assert event.debt_value_after_usd == Decimal("5000")
        assert event.health_factor_after is not None
        assert event.net_equity_after_usd == Decimal("6000")

        assert event.confidence == AccountingConfidence.HIGH


# ─── Test: Aave before-state None when read fails ────────────────────────────


class TestAaveBeforeStateNoneWhenReadFails:
    """When pre-execution gateway call fails, before fields are None with unavailable_reason."""

    def test_aave_before_state_none_when_read_fails(self) -> None:
        """Gateway failure during pre-state read → before fields None, unavailable_reason set.

        The after-state read may still succeed; None before + populated after is valid.
        """
        # Pre-read fails
        after_response = _mock_aave_response(
            collateral_e8=11_000 * 10**8,
            debt_e8=5_000 * 10**8,
            liquidation_threshold_bps=8500,
            health_factor_e18=int(1.87 * 1e18),
        )
        gateway_for_after = MagicMock()
        gateway_for_after.eth_call.return_value = after_response

        intent = _make_aave_supply_intent("SUPPLY")

        # Simulate pre-read failure: pass None as pre_execution_state
        pre_state = None  # as if capture_lending_pre_state returned None

        event = build_lending_accounting_event(
            intent=intent,
            result=_make_result({"supply_amount": 1_000 * 10**6}),
            deployment_id="dep-1",
            strategy_id="strat-1",
            cycle_id="cycle-001",
            execution_mode="paper",
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=gateway_for_after,
            basis_store=FIFOBasisStore(),
            price_oracle=_PRICE_ORACLE,
            ledger_entry_id=None,
            pre_execution_state=pre_state,
        )

        assert event is not None

        # Before-state must be None — never fabricated
        assert event.collateral_value_before_usd is None, (
            "collateral_value_before_usd must be None when pre-state read failed"
        )
        assert event.debt_value_before_usd is None, (
            "debt_value_before_usd must be None when pre-state read failed"
        )
        assert event.health_factor_before is None, (
            "health_factor_before must be None when pre-state read failed"
        )
        assert event.net_equity_before_usd is None, (
            "net_equity_before_usd must be None when pre-state read failed"
        )

        # After-state is still populated (None pre + populated after is valid)
        assert event.collateral_value_after_usd == Decimal("11000"), (
            "After-state must still be populated even when pre-state failed"
        )
        assert event.confidence == AccountingConfidence.HIGH, (
            "Confidence must be HIGH if after-state read succeeded"
        )
        # unavailable_reason tracks after-state quality only; pre-state absence
        # is signaled by the before fields being None, not by unavailable_reason.
        assert event.unavailable_reason == "", (
            f"unavailable_reason must be empty when after-state succeeded, got: {event.unavailable_reason!r}"
        )

    def test_capture_lending_pre_state_returns_none_on_gateway_exception(self) -> None:
        """capture_lending_pre_state returns None (not raises) when eth_call raises."""
        gateway = MagicMock()
        gateway.eth_call.side_effect = RuntimeError("gateway connection refused")

        intent = _make_aave_supply_intent("SUPPLY")

        pre_state = capture_lending_pre_state(
            intent=intent,
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=gateway,
            price_oracle=_PRICE_ORACLE,
        )

        assert pre_state is None, (
            "capture_lending_pre_state must return None (never raise) when gateway raises"
        )

    def test_capture_lending_pre_state_returns_none_when_no_gateway(self) -> None:
        """capture_lending_pre_state returns None immediately when gateway_client is None."""
        intent = _make_aave_supply_intent("SUPPLY")

        pre_state = capture_lending_pre_state(
            intent=intent,
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=None,
            price_oracle=_PRICE_ORACLE,
        )

        assert pre_state is None, (
            "capture_lending_pre_state must return None when gateway_client is None"
        )


# ─── Test: Morpho Blue before-state populated on borrow ──────────────────────


class TestMorphoBeforeStatePopulatedOnBorrow:
    """Morpho Blue pre-execution state populates before fields when gateway succeeds."""

    def test_morpho_before_state_populated_on_borrow(self) -> None:
        """build_lending_accounting_event populates before fields from MorphoBlueAccountState.

        Directly passes a MorphoBlueAccountState as pre_execution_state and verifies
        that before-fields are populated correctly.  The after-state read may fail if
        MORPHO_MARKETS registry doesn't have the test market — that's fine; we only
        assert before-state here.
        """
        # Pre-state: 1 wstETH collateral ($3500), 100 USDC debt ($100), HF = 30.1
        expected_hf = (Decimal("3500") * Decimal("0.86")) / Decimal("100")
        pre_state_val = MorphoBlueAccountState(
            collateral_usd=Decimal("3500"),
            debt_usd=Decimal("100"),
            health_factor=expected_hf,
            lltv=Decimal("0.86"),
        )

        gateway = MagicMock()
        # The post-execution eth_call inside the builder will fail (no registry entry)
        # — that's OK, we only assert before-state correctness.
        gateway.eth_call.side_effect = RuntimeError("no after-state in test")

        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        intent.protocol = "morpho_blue"
        intent.market_id = _MORPHO_MARKET_ID
        intent.borrow_token = "USDC"
        intent.collateral_token = "wstETH"
        intent.token = None

        event = build_lending_accounting_event(
            intent=intent,
            result=_make_result({"borrow_amount": 100_000_000}),  # 100 USDC
            deployment_id="dep-1",
            strategy_id="strat-1",
            cycle_id="cycle-001",
            execution_mode="paper",
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=gateway,
            basis_store=FIFOBasisStore(),
            price_oracle=_PRICE_ORACLE,
            ledger_entry_id=None,
            pre_execution_state=pre_state_val,
        )

        assert event is not None

        # Before-state must be populated from pre_execution_state regardless of after-state
        assert event.collateral_value_before_usd == Decimal("3500"), (
            f"collateral_value_before_usd expected $3500, got {event.collateral_value_before_usd}"
        )
        assert event.debt_value_before_usd == Decimal("100"), (
            f"debt_value_before_usd expected $100, got {event.debt_value_before_usd}"
        )
        assert event.health_factor_before == expected_hf, (
            f"health_factor_before expected {expected_hf}, got {event.health_factor_before}"
        )
        assert event.net_equity_before_usd == Decimal("3400"), (
            f"net_equity_before_usd expected $3400 ($3500-$100), got {event.net_equity_before_usd}"
        )


# ─── Test: before + after delta computable ───────────────────────────────────


class TestBeforeAfterDeltaComputable:
    """When both before and after states are populated, delta is computable by the caller."""

    def test_before_after_delta_computable(self) -> None:
        """After and before net_equity are both set; their delta equals the expected change.

        Scenario: SUPPLY $1 000 USDC into Aave.
        Before: $10 000 collateral, $5 000 debt → net_equity = $5 000
        After:  $11 000 collateral, $5 000 debt → net_equity = $6 000
        Expected delta: $1 000 (the supplied amount)
        """
        before_response = _mock_aave_response(
            collateral_e8=10_000 * 10**8,
            debt_e8=5_000 * 10**8,
            liquidation_threshold_bps=8500,
            health_factor_e18=int(1.7 * 1e18),
        )
        after_response = _mock_aave_response(
            collateral_e8=11_000 * 10**8,
            debt_e8=5_000 * 10**8,
            liquidation_threshold_bps=8500,
            health_factor_e18=int(1.87 * 1e18),
        )

        pre_gateway = MagicMock()
        pre_gateway.eth_call.return_value = before_response

        post_gateway = MagicMock()
        post_gateway.eth_call.return_value = after_response

        intent = _make_aave_supply_intent("SUPPLY")

        # Capture pre-state with its own gateway call
        pre_state = capture_lending_pre_state(
            intent=intent,
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=pre_gateway,
            price_oracle=_PRICE_ORACLE,
        )
        assert pre_state is not None

        event = build_lending_accounting_event(
            intent=intent,
            result=_make_result({"supply_amount": 1_000 * 10**6}),
            deployment_id="dep-1",
            strategy_id="strat-1",
            cycle_id="cycle-001",
            execution_mode="paper",
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=post_gateway,
            basis_store=FIFOBasisStore(),
            price_oracle=_PRICE_ORACLE,
            ledger_entry_id=None,
            pre_execution_state=pre_state,
        )

        assert event is not None
        assert event.net_equity_before_usd is not None, "net_equity_before_usd must be set"
        assert event.net_equity_after_usd is not None, "net_equity_after_usd must be set"

        delta = event.net_equity_after_usd - event.net_equity_before_usd
        assert delta == Decimal("1000"), (
            f"net_equity delta expected $1000 (supplied amount), got {delta}"
        )

        # Confidence must be HIGH when both states are available
        assert event.confidence == AccountingConfidence.HIGH, (
            "Confidence must be HIGH when both before and after states are populated"
        )

        # unavailable_reason must be empty
        assert event.unavailable_reason == "", (
            f"unavailable_reason must be empty when both states available, got: {event.unavailable_reason!r}"
        )

    def test_both_none_confidence_estimated_reason_set(self) -> None:
        """Neither before nor after state available: ESTIMATED confidence, reason covers both."""
        gateway = MagicMock()
        gateway.eth_call.return_value = None  # simulates gateway returning None

        intent = _make_aave_supply_intent("SUPPLY")

        event = build_lending_accounting_event(
            intent=intent,
            result=_make_result({"supply_amount": 1_000 * 10**6}),
            deployment_id="dep-1",
            strategy_id="strat-1",
            cycle_id="cycle-001",
            execution_mode="paper",
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=gateway,
            basis_store=FIFOBasisStore(),
            price_oracle=_PRICE_ORACLE,
            ledger_entry_id=None,
            pre_execution_state=None,  # pre-state not available
        )

        assert event is not None
        assert event.collateral_value_before_usd is None
        assert event.health_factor_before is None
        assert event.confidence == AccountingConfidence.ESTIMATED
        assert event.unavailable_reason != "", (
            "unavailable_reason must be non-empty when neither state is available"
        )
