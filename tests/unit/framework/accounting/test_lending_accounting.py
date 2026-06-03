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
    cycle_id: str = "cycle-1",
    market_id: str = "",
) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "ledger_entry_id": ledger_entry_id,
        "deployment_id": deployment_id,
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
    pre_state_json: str = "",
    post_state_json: str = "",
    tx_hash: str = "0xdeadbeef",
    token_in: str = "USDC",
    deployment_id: str = "dep-1",
    cycle_id: str = "cycle-1",
) -> dict[str, Any]:
    return {
        "id": ledger_entry_id,
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
        "pre_state_json": pre_state_json,
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
        """SUPPLY event has deployment_id, chain, protocol populated."""
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

    def test_supply_records_supply_principal_lot(self) -> None:
        """VIB-3964: SUPPLY records a principal lot under ``supply:<lending_pk>``.

        Symmetric to BORROW/REPAY — the lot is consumed by a later WITHDRAW so
        ``interest_accrued_usd`` (withdraw - principal) becomes computable. Without
        this, every WITHDRAW carries a null ``interest_delta_usd`` and G6 fails on
        ``Σ_interest_supply_null_count > 0``.
        """
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"supply_amount": 100_000_000})
        outbox = _make_outbox_row(
            led_id,
            intent_type="SUPPLY",
            position_key="lending:arbitrum:aave_v3:0xwallet:usdc",
        )
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
            handle_lending(outbox, ledger, basis)

        # SUPPLY principal lot is keyed under the supply: prefix so it doesn't
        # collide with BORROW lots on the same lending position.
        supply_key = "dep-1:supply:lending:arbitrum:aave_v3:0xwallet:usdc:usdc"
        assert supply_key in basis._lots
        lots = basis._lots[supply_key]
        assert len(lots) == 1
        assert lots[0]["remaining"] == Decimal("100")


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

    def test_withdraw_credits_wallet_basis_lot(self) -> None:
        """VIB-3964: WITHDRAW credits a swap-key acquisition lot for the
        withdrawn token so a follow-up SWAP that disposes it gets a non-null
        realized_pnl_usd. (It also drains the supply-key lot that a prior
        SUPPLY would have minted; with no SUPPLY in this test, the match
        leaves ``interest_delta_usd=None`` — honest absence.)
        """
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"withdraw_amount": 100_000_000})
        outbox = _make_outbox_row(
            led_id,
            intent_type="WITHDRAW",
            position_key="lending:arbitrum:aave_v3:0xwallet:usdc",
        )
        ledger = _make_ledger_row(
            led_id,
            intent_type="WITHDRAW",
            extracted_data_json=extracted,
            price_inputs_json=_usdc_price_json(),
            chain="arbitrum",
            protocol="aave_v3",
        )
        basis = FIFOBasisStore()

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            handle_lending(outbox, ledger, basis)

        # Wallet-basis pool now holds the withdrawn USDC at its USD basis.
        wallet_key = "dep-1:swap:arbitrum:0xwallet:usdc"
        assert wallet_key in basis._lots
        lots = basis._lots[wallet_key]
        assert len(lots) == 1
        assert lots[0]["remaining"] == Decimal("100")
        assert lots[0]["source"] == "WITHDRAW"

    def test_withdraw_does_not_fabricate_interest_when_supply_lots_partial(self) -> None:
        """VIB-3964 / Codex 2026-05-04 P2.

        ``match_repay`` returns ``unmatched=0`` whenever it consumed at least
        one lot. If a strategy was deployed with a pre-existing on-chain
        supplied position (or earlier SUPPLYs were not tracked), the residual
        of ``withdraw - matched_principal`` would otherwise be persisted as
        ``interest_accrued_usd`` even though it's untracked principal.

        Repro: SUPPLY 10 USDC tracked, then WITHDRAW 110 USDC. The matcher
        returns ``repaid_principal=10, interest_or_yield=100``. Interest
        ratio (100 / 10 = 1000% of principal) is well past any plausible
        rate — must surface as None, not 100.
        """
        # SUPPLY 10 USDC under the lending position key.
        supply_id = str(uuid.uuid4())
        supply_extracted = json.dumps({"supply_amount": 10_000_000})  # 10 USDC raw (6 decimals)
        supply_outbox = _make_outbox_row(
            supply_id,
            intent_type="SUPPLY",
            position_key="lending:arbitrum:aave_v3:0xwallet:usdc",
        )
        supply_ledger = _make_ledger_row(
            supply_id,
            intent_type="SUPPLY",
            extracted_data_json=supply_extracted,
            price_inputs_json=_usdc_price_json(),
            chain="arbitrum",
            protocol="aave_v3",
        )
        basis = FIFOBasisStore()

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            handle_lending(supply_outbox, supply_ledger, basis)

        # WITHDRAW 110 USDC — way more than the tracked 10 USDC supply.
        withdraw_id = str(uuid.uuid4())
        withdraw_extracted = json.dumps({"withdraw_amount": 110_000_000})  # 110 USDC raw
        withdraw_outbox = _make_outbox_row(
            withdraw_id,
            intent_type="WITHDRAW",
            position_key="lending:arbitrum:aave_v3:0xwallet:usdc",
        )
        withdraw_ledger = _make_ledger_row(
            withdraw_id,
            intent_type="WITHDRAW",
            extracted_data_json=withdraw_extracted,
            price_inputs_json=_usdc_price_json(),
            chain="arbitrum",
            protocol="aave_v3",
        )

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(withdraw_outbox, withdraw_ledger, basis)

        assert event is not None
        assert event.event_type == LendingEventType.WITHDRAW
        # Implied "interest" of 100 USDC against 10 USDC principal is unrealistic
        # — the guard must mark this as unmeasured rather than persist it.
        assert event.interest_delta_usd is None

    def test_withdraw_principal_plus_interest_equals_total_amount(self) -> None:
        """VIB-3964 / pr-auditor 2026-05-04 item 2.

        WITHDRAW must split into ``principal_delta_usd`` + ``interest_delta_usd``
        such that the two sum to the actual cash flow. Pre-fix, WITHDRAW emitted
        ``principal_delta_usd = total_withdraw`` AND
        ``interest_delta_usd = excess`` — the sum was over by the interest
        amount and broke double-entry reconciliation.

        Repro: SUPPLY 100 USDC, WITHDRAW 100.5 USDC (principal 100 + 0.50
        accrued). Expected: principal_delta_usd ≈ 100, interest_delta_usd
        ≈ 0.50, sum = 100.50 = total.
        """
        supply_id = str(uuid.uuid4())
        supply_extracted = json.dumps({"supply_amount": 100_000_000})  # 100 USDC raw
        supply_outbox = _make_outbox_row(
            supply_id,
            intent_type="SUPPLY",
            position_key="lending:arbitrum:aave_v3:0xwallet:usdc",
        )
        supply_ledger = _make_ledger_row(
            supply_id,
            intent_type="SUPPLY",
            extracted_data_json=supply_extracted,
            price_inputs_json=_usdc_price_json(),
            chain="arbitrum",
            protocol="aave_v3",
        )
        basis = FIFOBasisStore()

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            handle_lending(supply_outbox, supply_ledger, basis)

        # WITHDRAW 100.5 USDC — small interest accrued vs. a 100 USDC supply.
        withdraw_id = str(uuid.uuid4())
        withdraw_extracted = json.dumps({"withdraw_amount": 100_500_000})
        withdraw_outbox = _make_outbox_row(
            withdraw_id,
            intent_type="WITHDRAW",
            position_key="lending:arbitrum:aave_v3:0xwallet:usdc",
        )
        withdraw_ledger = _make_ledger_row(
            withdraw_id,
            intent_type="WITHDRAW",
            extracted_data_json=withdraw_extracted,
            price_inputs_json=_usdc_price_json(),
            chain="arbitrum",
            protocol="aave_v3",
        )

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(withdraw_outbox, withdraw_ledger, basis)

        assert event is not None
        assert event.principal_delta_usd is not None
        assert event.interest_delta_usd is not None
        # principal = matched supply principal in USD (100 USDC at $1 = $100)
        assert event.principal_delta_usd == Decimal("100")
        # interest = excess (0.50 USDC at $1 = $0.50)
        assert event.interest_delta_usd == Decimal("0.50")
        # Double-entry sanity: principal + interest = total cash flow
        assert event.principal_delta_usd + event.interest_delta_usd == Decimal("100.50")


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

    def test_present_post_state_yields_high_confidence_and_no_warning(self) -> None:
        """VIB-4985 / ALM-2777 — the user-visible outcome.

        A SUPPLY whose after-state read succeeded (post_state_json present — the
        end state after the gateway's lag retry recovers the read) produces a
        ``confidence=HIGH`` row with NO ``unavailable_reason``. This is the row
        the field report saw stuck at ESTIMATED; with the read populated it must
        be HIGH and carry no warning. Paired with the gateway-retry tests
        (``test_rpc_service_retry.py::TestRpcServiceIndexerLagRetry``) this closes
        the full lag→recover→HIGH chain.
        """
        from almanak.framework.accounting.models import AccountingConfidence

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
        assert event.confidence == AccountingConfidence.HIGH
        assert event.unavailable_reason == ""

    def test_missing_post_state_emits_exact_field_report_warning(self) -> None:
        """The ESTIMATED row carries the exact warning string from the field report.

        Pins the user-visible message so the symptom (and its disappearance once
        the read recovers — see the test above) is unambiguous.
        """
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
        assert event.unavailable_reason == "post_state_json missing or invalid (gateway read unavailable for this row)"


# ──────────────────────────────────────────────────────────────────────────────
# VIB-4257 — Pre-state lane symmetry (regression guard)
# ──────────────────────────────────────────────────────────────────────────────


class TestHandleLendingPreStateLaneSymmetry:
    """Pre-state JSON populates `_before` payload fields symmetrically with post-state.

    Pre-VIB-4257, the handler hardcoded all `_before` fields to None even when
    `pre_state_json` carried valid readings. This class is the regression guard
    — every test here MUST fail on a build that drops the pre-state read.
    """

    def _aave_state_json(
        self, *, collateral_usd: str, debt_usd: str, health_factor: str, lt_bps: int = 8500
    ) -> str:
        return json.dumps(
            {
                "protocol": "aave_v3",
                "collateral_usd": collateral_usd,
                "debt_usd": debt_usd,
                "health_factor": health_factor,
                "liquidation_threshold_bps": lt_bps,
            }
        )

    def test_borrow_event_carries_pre_state_health_factor(self) -> None:
        """BORROW: pre_state.health_factor → event.health_factor_before (non-null)."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"borrow_amount": 1_000_000_000})
        pre = self._aave_state_json(collateral_usd="5000", debt_usd="0", health_factor="999999")
        post = self._aave_state_json(collateral_usd="5000", debt_usd="1000", health_factor="2.6")
        outbox = _make_outbox_row(led_id, intent_type="BORROW")
        ledger = _make_ledger_row(
            led_id,
            intent_type="BORROW",
            extracted_data_json=extracted,
            price_inputs_json=_usdc_price_json(),
            pre_state_json=pre,
            post_state_json=post,
        )
        basis = FIFOBasisStore()

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(outbox, ledger, basis)

        assert event is not None
        assert event.health_factor_before == Decimal("999999")
        assert event.health_factor_after == Decimal("2.6")
        assert event.collateral_value_before_usd == Decimal("5000")
        assert event.debt_value_before_usd == Decimal("0")
        assert event.net_equity_before_usd == Decimal("5000")
        assert event.collateral_value_after_usd == Decimal("5000")
        assert event.debt_value_after_usd == Decimal("1000")
        assert event.net_equity_after_usd == Decimal("4000")

    def test_supply_borrow_repay_withdraw_all_carry_pre_state_hf(self) -> None:
        """Loop through the four lending intents and assert each carries hf_before."""
        rows: list[tuple[str, str, str, str]] = [
            ("SUPPLY", "0", "0", "999999"),
            ("BORROW", "5000", "0", "999999"),
            ("WITHDRAW", "5000", "1000", "2.6"),
            ("REPAY", "5000", "1500", "1.7"),
        ]
        basis = FIFOBasisStore()
        for intent_type, _coll, _debt, _hf in rows:
            led_id = str(uuid.uuid4())
            # extracted_data field names match _AMOUNT_KEY_BY_INTENT in
            # almanak/framework/accounting/category_handlers/lending_handler.py:448
            # — WITHDRAW reads "withdraw_amount" (NOT "supply_amount").
            extracted_field = {
                "SUPPLY": "supply_amount",
                "BORROW": "borrow_amount",
                "WITHDRAW": "withdraw_amount",
                "REPAY": "repay_amount",
            }[intent_type]
            extracted = json.dumps({extracted_field: 1_000_000})
            pre = self._aave_state_json(
                collateral_usd=_coll, debt_usd=_debt, health_factor=_hf
            )
            outbox = _make_outbox_row(led_id, intent_type=intent_type)
            ledger = _make_ledger_row(
                led_id,
                intent_type=intent_type,
                extracted_data_json=extracted,
                price_inputs_json=_usdc_price_json(),
                pre_state_json=pre,
            )

            with patch(
                "almanak.framework.data.tokens.resolver.get_token_resolver",
                return_value=_mock_resolver(6),
            ):
                event = handle_lending(outbox, ledger, basis)

            assert event is not None, f"intent {intent_type} produced None"
            assert event.health_factor_before == Decimal(_hf), (
                f"intent {intent_type}: hf_before mismatch (got {event.health_factor_before})"
            )

    def test_pre_state_only_path_post_state_missing(self) -> None:
        """If post_state_json is empty but pre_state_json is populated, _before fields populate.

        VIB-4257 D1.2: the bug pre-fix was that the handler dropped pre-state entirely
        any time post-state failed (gateway error at post-state-capture time). Decoupling
        the two reads is the property under test.
        """
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"supply_amount": 100_000_000})
        pre = self._aave_state_json(collateral_usd="123", debt_usd="45", health_factor="3.14")
        outbox = _make_outbox_row(led_id, intent_type="SUPPLY")
        ledger = _make_ledger_row(
            led_id,
            intent_type="SUPPLY",
            extracted_data_json=extracted,
            price_inputs_json=_usdc_price_json(),
            pre_state_json=pre,
            post_state_json="",
        )
        basis = FIFOBasisStore()

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(outbox, ledger, basis)

        assert event is not None
        assert event.health_factor_before == Decimal("3.14")
        assert event.collateral_value_before_usd == Decimal("123")
        assert event.debt_value_before_usd == Decimal("45")
        assert event.net_equity_before_usd == Decimal("78")
        # Post-state side stays None — no fabrication.
        assert event.health_factor_after is None
        assert event.collateral_value_after_usd is None
        assert event.debt_value_after_usd is None

    def test_missing_pre_state_json_leaves_before_fields_none(self) -> None:
        """F1: pre_state_json='' → all _before fields stay None. No fabrication."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"supply_amount": 100_000_000})
        outbox = _make_outbox_row(led_id, intent_type="SUPPLY")
        ledger = _make_ledger_row(
            led_id,
            intent_type="SUPPLY",
            extracted_data_json=extracted,
            price_inputs_json=_usdc_price_json(),
            pre_state_json="",
        )
        basis = FIFOBasisStore()

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(outbox, ledger, basis)

        assert event is not None
        assert event.health_factor_before is None
        assert event.collateral_value_before_usd is None
        assert event.debt_value_before_usd is None
        assert event.net_equity_before_usd is None

    def test_pre_state_with_partial_fields_does_not_fabricate(self) -> None:
        """F2: pre_state has collateral_usd but missing health_factor → hf_before=None."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"supply_amount": 100_000_000})
        pre = json.dumps(
            {
                "protocol": "compound_v3",
                "collateral_usd": "500",
                "debt_usd": "100",
                # health_factor intentionally missing
            }
        )
        outbox = _make_outbox_row(led_id, intent_type="SUPPLY")
        ledger = _make_ledger_row(
            led_id,
            intent_type="SUPPLY",
            extracted_data_json=extracted,
            price_inputs_json=_usdc_price_json(),
            pre_state_json=pre,
        )
        basis = FIFOBasisStore()

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(outbox, ledger, basis)

        assert event is not None
        assert event.collateral_value_before_usd == Decimal("500")
        assert event.debt_value_before_usd == Decimal("100")
        assert event.net_equity_before_usd == Decimal("400")
        assert event.health_factor_before is None  # Empty ≠ zero — never fabricated.

    def test_invalid_pre_state_json_does_not_raise(self) -> None:
        """F3: malformed pre_state_json → handler logs and emits event with None _before fields."""
        led_id = str(uuid.uuid4())
        extracted = json.dumps({"supply_amount": 100_000_000})
        outbox = _make_outbox_row(led_id, intent_type="SUPPLY")
        ledger = _make_ledger_row(
            led_id,
            intent_type="SUPPLY",
            extracted_data_json=extracted,
            price_inputs_json=_usdc_price_json(),
            pre_state_json="not-json{",
        )
        basis = FIFOBasisStore()

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=_mock_resolver(6)):
            event = handle_lending(outbox, ledger, basis)  # MUST NOT raise

        assert event is not None
        assert event.health_factor_before is None
        assert event.collateral_value_before_usd is None
