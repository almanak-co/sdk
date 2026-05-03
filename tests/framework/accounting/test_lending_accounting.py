"""Unit tests for handle_lending() from lending_handler.py (VIB-3477).

Tests all 5 lending intent types:
  SUPPLY, BORROW, REPAY, DELEVERAGE, WITHDRAW

No live DB, no gateway, no network. Token resolver is mocked at the source
module so _extract_amount_human can scale raw integers to human decimals.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.category_handlers.lending_handler import handle_lending
from almanak.framework.accounting.models import LendingEventType


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _make_outbox_row(
    ledger_entry_id: str,
    intent_type: str = "SUPPLY",
    wallet_address: str = "0xwallet",
    position_key: str = "lending:arbitrum:aave_v3:0xwallet:usdc",
    deployment_id: str = "dep-1",
    strategy_id: str = "strat-1",
    cycle_id: str = "cycle-1",
    market_id: str = "",
) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "ledger_entry_id": ledger_entry_id,
        "deployment_id": deployment_id,
        "strategy_id": strategy_id,
        "cycle_id": cycle_id,
        "intent_type": intent_type,
        "wallet_address": wallet_address,
        "position_key": position_key,
        "market_id": market_id,
        "status": "pending",
        "attempts": 0,
        "error": "",
        "created_at": datetime.now(UTC).isoformat(),
        "updated_at": datetime.now(UTC).isoformat(),
    }


def _make_ledger_row(
    ledger_entry_id: str,
    intent_type: str = "SUPPLY",
    protocol: str = "aave_v3",
    chain: str = "arbitrum",
    extracted_data_json: str = "",
    price_inputs_json: str = "",
    post_state_json: str = "",
    tx_hash: str = "0xdeadbeef",
    token_in: str = "USDC",
    deployment_id: str = "dep-1",
    strategy_id: str = "strat-1",
    cycle_id: str = "cycle-1",
) -> dict[str, Any]:
    return {
        "id": ledger_entry_id,
        "strategy_id": strategy_id,
        "deployment_id": deployment_id,
        "cycle_id": cycle_id,
        "execution_mode": "live",
        "timestamp": datetime.now(UTC).isoformat(),
        "intent_type": intent_type,
        "token_in": token_in,
        "amount_in": "100",
        "token_out": "",
        "amount_out": "",
        "effective_price": "",
        "slippage_bps": None,
        "gas_used": 0,
        "gas_usd": "0.01",
        "tx_hash": tx_hash,
        "chain": chain,
        "protocol": protocol,
        "success": True,
        "error": "",
        "extracted_data_json": extracted_data_json,
        "price_inputs_json": price_inputs_json,
        "pre_state_json": "",
        "post_state_json": post_state_json,
    }


def _mock_resolver(decimals: int = 6) -> MagicMock:
    """Return a mock token resolver that returns the given decimals for any token."""
    token_info = MagicMock()
    token_info.decimals = decimals
    resolver = MagicMock()
    resolver.resolve.return_value = token_info
    return resolver


def _usdc_price_json() -> str:
    return json.dumps({"USDC": "1.0"})


def _weth_price_json() -> str:
    return json.dumps({"WETH": "3000.0"})


# ──────────────────────────────────────────────────────────────────────────────
# SUPPLY
# ──────────────────────────────────────────────────────────────────────────────


class TestHandleLendingSupply:
    def test_supply_writes_event_with_correct_asset_and_amount(self) -> None:
        """SUPPLY 100 USDC (6 decimals → 100_000_000 raw) at $1.0 → amount_token ≈ 100, principal_delta_usd ≈ 100."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"supply_amount": 100_000_000})
        outbox = _make_outbox_row(led_id, intent_type="SUPPLY")
        ledger = _make_ledger_row(
            led_id,
            intent_type="SUPPLY",
            extracted_data_json=extracted,
            price_inputs_json=_usdc_price_json(),
        )
        basis = FIFOBasisStore()

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(outbox, ledger, basis)

        assert event is not None
        assert event.event_type == LendingEventType.SUPPLY
        assert event.asset == "USDC"
        assert event.amount_token == Decimal("100")
        assert event.principal_delta_usd == Decimal("100")

    def test_supply_event_carries_identity_fields(self) -> None:
        """SUPPLY event has deployment_id, strategy_id, chain, protocol populated."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"supply_amount": 50_000_000})
        outbox = _make_outbox_row(led_id, intent_type="SUPPLY")
        ledger = _make_ledger_row(
            led_id,
            intent_type="SUPPLY",
            extracted_data_json=extracted,
            price_inputs_json=_usdc_price_json(),
            chain="arbitrum",
            protocol="aave_v3",
        )
        basis = FIFOBasisStore()

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(outbox, ledger, basis)

        assert event is not None
        assert event.identity.deployment_id == "dep-1"
        assert event.identity.chain == "arbitrum"
        assert event.identity.protocol == "aave_v3"

    def test_supply_does_not_record_fifo_lot(self) -> None:
        """SUPPLY must not add any lots to the FIFO store — only BORROW does."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"supply_amount": 100_000_000})
        outbox = _make_outbox_row(led_id, intent_type="SUPPLY")
        ledger = _make_ledger_row(
            led_id,
            intent_type="SUPPLY",
            extracted_data_json=extracted,
            price_inputs_json=_usdc_price_json(),
        )
        basis = FIFOBasisStore()

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            handle_lending(outbox, ledger, basis)

        assert basis._lots == {}


# ──────────────────────────────────────────────────────────────────────────────
# BORROW
# ──────────────────────────────────────────────────────────────────────────────


class TestHandleLendingBorrow:
    def test_borrow_records_fifo_lot(self) -> None:
        """BORROW 1000 USDC → FIFOBasisStore should contain one open lot."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"borrow_amount": 1_000_000_000})  # 1000 USDC raw
        outbox = _make_outbox_row(
            led_id,
            intent_type="BORROW",
            position_key="lending:arbitrum:aave_v3:0xwallet:usdc",
        )
        ledger = _make_ledger_row(
            led_id,
            intent_type="BORROW",
            extracted_data_json=extracted,
            price_inputs_json=_usdc_price_json(),
        )
        basis = FIFOBasisStore()

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(outbox, ledger, basis)

        assert event is not None
        # Lot must exist so future REPAY can match interest.
        key = "dep-1:lending:arbitrum:aave_v3:0xwallet:usdc:usdc"
        assert key in basis._lots
        lots = basis._lots[key]
        assert len(lots) == 1
        assert lots[0]["remaining"] == Decimal("1000")

    def test_borrow_event_has_correct_principal_delta_usd(self) -> None:
        """BORROW 100 USDC at $1.0 → principal_delta_usd ≈ 100."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"borrow_amount": 100_000_000})
        outbox = _make_outbox_row(led_id, intent_type="BORROW")
        ledger = _make_ledger_row(
            led_id,
            intent_type="BORROW",
            extracted_data_json=extracted,
            price_inputs_json=_usdc_price_json(),
        )
        basis = FIFOBasisStore()

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(outbox, ledger, basis)

        assert event is not None
        assert event.event_type == LendingEventType.BORROW
        assert event.principal_delta_usd == Decimal("100")
        assert event.interest_delta_usd is None

    def test_borrow_with_18_decimal_token(self) -> None:
        """BORROW 2.0 WETH (18 decimals → 2e18 raw) at $3000 → principal_delta_usd ≈ 6000."""
        led_id = str(uuid.uuid4())
        raw_amount = int(2 * 10**18)
        extracted = json.dumps({"borrow_amount": raw_amount})
        price_json = json.dumps({"WETH": "3000.0"})
        outbox = _make_outbox_row(
            led_id,
            intent_type="BORROW",
            position_key="lending:arbitrum:aave_v3:0xwallet:weth",
        )
        ledger = _make_ledger_row(
            led_id,
            intent_type="BORROW",
            token_in="WETH",
            extracted_data_json=extracted,
            price_inputs_json=price_json,
        )
        basis = FIFOBasisStore()

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(18)):
            event = handle_lending(outbox, ledger, basis)

        assert event is not None
        assert event.amount_token == Decimal("2")
        assert event.principal_delta_usd == Decimal("6000")


# ──────────────────────────────────────────────────────────────────────────────
# REPAY
# ──────────────────────────────────────────────────────────────────────────────


class TestHandleLendingRepay:
    def test_repay_matches_prior_borrow_lot(self) -> None:
        """After BORROW 100 USDC, REPAY 110 USDC → principal = 100, interest = 10."""
        basis = FIFOBasisStore()
        dep = "dep-1"
        pk = "lending:arbitrum:aave_v3:0xwallet:usdc"

        # First, record a BORROW to set up the lot.
        borrow_id = str(uuid.uuid4())
        borrow_extracted = json.dumps({"borrow_amount": 100_000_000})
        borrow_outbox = _make_outbox_row(borrow_id, intent_type="BORROW", position_key=pk)
        borrow_ledger = _make_ledger_row(
            borrow_id,
            intent_type="BORROW",
            extracted_data_json=borrow_extracted,
            price_inputs_json=_usdc_price_json(),
        )

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            handle_lending(borrow_outbox, borrow_ledger, basis)

        # Now REPAY 110 USDC (100 principal + 10 interest).
        repay_id = str(uuid.uuid4())
        repay_extracted = json.dumps({"repay_amount": 110_000_000})
        repay_outbox = _make_outbox_row(repay_id, intent_type="REPAY", position_key=pk)
        repay_ledger = _make_ledger_row(
            repay_id,
            intent_type="REPAY",
            extracted_data_json=repay_extracted,
            price_inputs_json=_usdc_price_json(),
        )

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(repay_outbox, repay_ledger, basis)

        assert event is not None
        assert event.event_type == LendingEventType.REPAY
        # principal_delta_usd = matched principal (100 USDC at $1)
        assert event.principal_delta_usd == Decimal("100")
        # interest_delta_usd = excess over principal (10 USDC at $1)
        assert event.interest_delta_usd == Decimal("10")

    def test_repay_with_no_prior_lot_has_none_interest_delta(self) -> None:
        """REPAY with no prior BORROW lot → unmatched_amount > 0 → interest_delta_usd = None."""
        basis = FIFOBasisStore()
        led_id = str(uuid.uuid4())
        repay_extracted = json.dumps({"repay_amount": 100_000_000})
        outbox = _make_outbox_row(led_id, intent_type="REPAY")
        ledger = _make_ledger_row(
            led_id,
            intent_type="REPAY",
            extracted_data_json=repay_extracted,
            price_inputs_json=_usdc_price_json(),
        )

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(outbox, ledger, basis)

        assert event is not None
        assert event.event_type == LendingEventType.REPAY
        # No lot to match — interest is indeterminate
        assert event.interest_delta_usd is None

    def test_repay_exact_principal_has_zero_interest(self) -> None:
        """REPAY exactly the borrowed amount → interest_delta_usd = 0 (not None)."""
        basis = FIFOBasisStore()
        pk = "lending:arbitrum:aave_v3:0xwallet:usdc"

        borrow_id = str(uuid.uuid4())
        borrow_extracted = json.dumps({"borrow_amount": 100_000_000})
        borrow_outbox = _make_outbox_row(borrow_id, intent_type="BORROW", position_key=pk)
        borrow_ledger = _make_ledger_row(
            borrow_id,
            intent_type="BORROW",
            extracted_data_json=borrow_extracted,
            price_inputs_json=_usdc_price_json(),
        )

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            handle_lending(borrow_outbox, borrow_ledger, basis)

        repay_id = str(uuid.uuid4())
        repay_extracted = json.dumps({"repay_amount": 100_000_000})
        repay_outbox = _make_outbox_row(repay_id, intent_type="REPAY", position_key=pk)
        repay_ledger = _make_ledger_row(
            repay_id,
            intent_type="REPAY",
            extracted_data_json=repay_extracted,
            price_inputs_json=_usdc_price_json(),
        )

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(repay_outbox, repay_ledger, basis)

        assert event is not None
        assert event.interest_delta_usd == Decimal("0")


# ──────────────────────────────────────────────────────────────────────────────
# WITHDRAW
# ──────────────────────────────────────────────────────────────────────────────


class TestHandleLendingWithdraw:
    def test_withdraw_writes_event_with_principal_delta(self) -> None:
        """WITHDRAW 200 USDC at $1.0 → event_type == WITHDRAW, principal_delta_usd ≈ 200."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"withdraw_amount": 200_000_000})
        outbox = _make_outbox_row(led_id, intent_type="WITHDRAW")
        ledger = _make_ledger_row(
            led_id,
            intent_type="WITHDRAW",
            extracted_data_json=extracted,
            price_inputs_json=_usdc_price_json(),
        )
        basis = FIFOBasisStore()

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(outbox, ledger, basis)

        assert event is not None
        assert event.event_type == LendingEventType.WITHDRAW
        assert event.amount_token == Decimal("200")
        assert event.principal_delta_usd == Decimal("200")

    def test_withdraw_does_not_record_fifo_lot(self) -> None:
        """WITHDRAW must not record FIFO lots — no REPAY matching needed."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"withdraw_amount": 100_000_000})
        outbox = _make_outbox_row(led_id, intent_type="WITHDRAW")
        ledger = _make_ledger_row(
            led_id,
            intent_type="WITHDRAW",
            extracted_data_json=extracted,
            price_inputs_json=_usdc_price_json(),
        )
        basis = FIFOBasisStore()

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            handle_lending(outbox, ledger, basis)

        assert basis._lots == {}


# ──────────────────────────────────────────────────────────────────────────────
# DELEVERAGE
# ──────────────────────────────────────────────────────────────────────────────


class TestHandleLendingDeleverage:
    def test_deleverage_matches_borrow_lot(self) -> None:
        """DELEVERAGE is structurally a repay: it matches the BORROW lot and yields interest."""
        basis = FIFOBasisStore()
        pk = "lending:arbitrum:aave_v3:0xwallet:usdc"

        # Set up an open BORROW lot.
        borrow_id = str(uuid.uuid4())
        borrow_extracted = json.dumps({"borrow_amount": 500_000_000})  # 500 USDC
        borrow_outbox = _make_outbox_row(borrow_id, intent_type="BORROW", position_key=pk)
        borrow_ledger = _make_ledger_row(
            borrow_id,
            intent_type="BORROW",
            extracted_data_json=borrow_extracted,
            price_inputs_json=_usdc_price_json(),
        )

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            handle_lending(borrow_outbox, borrow_ledger, basis)

        # DELEVERAGE 520 USDC (20 USDC interest).
        delev_id = str(uuid.uuid4())
        delev_extracted = json.dumps({"repay_amount": 520_000_000})
        delev_outbox = _make_outbox_row(delev_id, intent_type="DELEVERAGE", position_key=pk)
        delev_ledger = _make_ledger_row(
            delev_id,
            intent_type="DELEVERAGE",
            extracted_data_json=delev_extracted,
            price_inputs_json=_usdc_price_json(),
        )

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(delev_outbox, delev_ledger, basis)

        assert event is not None
        assert event.event_type == LendingEventType.DELEVERAGE
        assert event.principal_delta_usd == Decimal("500")
        assert event.interest_delta_usd == Decimal("20")

    def test_deleverage_with_no_prior_lot_has_none_interest(self) -> None:
        """DELEVERAGE with no prior BORROW → unmatched → interest_delta_usd = None."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"repay_amount": 100_000_000})
        outbox = _make_outbox_row(led_id, intent_type="DELEVERAGE")
        ledger = _make_ledger_row(
            led_id,
            intent_type="DELEVERAGE",
            extracted_data_json=extracted,
            price_inputs_json=_usdc_price_json(),
        )
        basis = FIFOBasisStore()

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(outbox, ledger, basis)

        assert event is not None
        assert event.interest_delta_usd is None


# ──────────────────────────────────────────────────────────────────────────────
# Non-lending intent
# ──────────────────────────────────────────────────────────────────────────────


class TestHandleLendingNonLending:
    def test_returns_none_for_swap_intent(self) -> None:
        """SWAP intent → handle_lending returns None (only processes lending intents)."""
        led_id = str(uuid.uuid4())
        outbox = _make_outbox_row(led_id, intent_type="SWAP")
        ledger = _make_ledger_row(led_id, intent_type="SWAP")
        basis = FIFOBasisStore()

        event = handle_lending(outbox, ledger, basis)

        assert event is None

    def test_returns_none_for_bridge_intent(self) -> None:
        """BRIDGE intent → handle_lending returns None."""
        led_id = str(uuid.uuid4())
        outbox = _make_outbox_row(led_id, intent_type="BRIDGE")
        ledger = _make_ledger_row(led_id, intent_type="BRIDGE")
        basis = FIFOBasisStore()

        event = handle_lending(outbox, ledger, basis)

        assert event is None

    def test_returns_none_for_lp_open_intent(self) -> None:
        """LP_OPEN intent → handle_lending returns None."""
        led_id = str(uuid.uuid4())
        outbox = _make_outbox_row(led_id, intent_type="LP_OPEN")
        ledger = _make_ledger_row(led_id, intent_type="LP_OPEN")
        basis = FIFOBasisStore()

        event = handle_lending(outbox, ledger, basis)

        assert event is None

    def test_returns_none_for_empty_intent_type(self) -> None:
        """Empty intent_type → handle_lending returns None (not a crash)."""
        led_id = str(uuid.uuid4())
        outbox = _make_outbox_row(led_id, intent_type="")
        ledger = _make_ledger_row(led_id, intent_type="")
        basis = FIFOBasisStore()

        event = handle_lending(outbox, ledger, basis)

        assert event is None


# ──────────────────────────────────────────────────────────────────────────────
# Post-state (VIB-3474 fields)
# ──────────────────────────────────────────────────────────────────────────────


class TestHandleLendingPostState:
    def test_post_state_json_populates_hf_and_collateral(self) -> None:
        """When post_state_json is provided, health_factor_after and collateral_value_after_usd are populated."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"supply_amount": 100_000_000})
        post_state = json.dumps({
            "collateral_usd": "5000.0",
            "debt_usd": "2000.0",
            "health_factor": "2.5",
            "liquidation_threshold_bps": 8000,
        })
        outbox = _make_outbox_row(led_id, intent_type="SUPPLY")
        ledger = _make_ledger_row(
            led_id,
            intent_type="SUPPLY",
            extracted_data_json=extracted,
            price_inputs_json=_usdc_price_json(),
            post_state_json=post_state,
        )
        basis = FIFOBasisStore()

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(outbox, ledger, basis)

        assert event is not None
        assert event.collateral_value_after_usd == Decimal("5000")
        assert event.debt_value_after_usd == Decimal("2000")
        assert event.health_factor_after == Decimal("2.5")

    def test_missing_post_state_json_yields_estimated_confidence(self) -> None:
        """Without post_state_json, confidence is ESTIMATED (gateway read unavailable)."""
        from almanak.framework.accounting.models import AccountingConfidence

        led_id = str(uuid.uuid4())
        extracted = json.dumps({"supply_amount": 100_000_000})
        outbox = _make_outbox_row(led_id, intent_type="SUPPLY")
        ledger = _make_ledger_row(
            led_id,
            intent_type="SUPPLY",
            extracted_data_json=extracted,
            price_inputs_json=_usdc_price_json(),
        )
        basis = FIFOBasisStore()

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(outbox, ledger, basis)

        assert event is not None
        assert event.confidence == AccountingConfidence.ESTIMATED
