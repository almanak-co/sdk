"""Unit tests for the swap accounting handler (VIB-3473).

Tests:
  - test_handle_swap_basic
  - test_handle_swap_realized_pnl
  - test_handle_swap_no_prior_lot
  - test_handle_swap_pendle_skip
  - test_handle_swap_missing_prices
  - test_record_swap_acquisition_and_match_disposal

No live chain calls, no SQLite, no gateway.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.category_handlers.swap_handler import handle_swap
from almanak.framework.accounting.models import AccountingConfidence, SwapAccountingEvent, SwapEventType


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

_DEPLOYMENT_ID = "dep-swap-test"
_STRATEGY_ID = "strat-swap-test"
_CYCLE_ID = "cycle-1"
_WALLET = "0xabcdef1234567890abcdef1234567890abcdef12"
_CHAIN = "arbitrum"
_TX_HASH = "0xdeadbeef1234"


def _make_outbox_row(
    intent_type: str = "SWAP",
    wallet_address: str = _WALLET,
    position_key: str = "",
) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "ledger_entry_id": str(uuid.uuid4()),
        "deployment_id": _DEPLOYMENT_ID,
        "strategy_id": _STRATEGY_ID,
        "cycle_id": _CYCLE_ID,
        "intent_type": intent_type,
        "wallet_address": wallet_address,
        "position_key": position_key,
        "market_id": "",
        "status": "pending",
        "attempts": 0,
        "error": "",
        "created_at": datetime.now(UTC).isoformat(),
        "updated_at": datetime.now(UTC).isoformat(),
    }


def _make_ledger_row(
    token_in: str = "USDC",
    amount_in: str = "100",
    token_out: str = "WETH",
    amount_out: str = "0.05",
    protocol: str = "enso",
    chain: str = _CHAIN,
    price_inputs_json: str = "",
    effective_price: str = "",
    slippage_bps: int | None = 30,
    gas_usd: str = "0.50",
    tx_hash: str = _TX_HASH,
    ledger_entry_id: str | None = None,
    intent_type: str = "SWAP",
) -> dict[str, Any]:
    lid = ledger_entry_id or str(uuid.uuid4())
    return {
        "id": lid,
        "strategy_id": _STRATEGY_ID,
        "deployment_id": _DEPLOYMENT_ID,
        "cycle_id": _CYCLE_ID,
        "execution_mode": "live",
        "timestamp": datetime.now(UTC).isoformat(),
        "intent_type": intent_type,
        "token_in": token_in,
        "amount_in": amount_in,
        "token_out": token_out,
        "amount_out": amount_out,
        "effective_price": effective_price,
        "slippage_bps": slippage_bps,
        "gas_used": 0,
        "gas_usd": gas_usd,
        "tx_hash": tx_hash,
        "chain": chain,
        "protocol": protocol,
        "success": True,
        "error": "",
        "extracted_data_json": "",
        "price_inputs_json": price_inputs_json,
        "pre_state_json": "",
        "post_state_json": "",
    }


def _price_json(prices: dict[str, str]) -> str:
    return json.dumps(prices)


# ──────────────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────────────


class TestHandleSwapBasic:
    """Basic event fields and amounts are correct."""

    def test_handle_swap_basic(self) -> None:
        price_json = _price_json({"USDC": "1.0", "WETH": "2000.0"})
        outbox = _make_outbox_row()
        ledger = _make_ledger_row(
            token_in="USDC",
            amount_in="100",
            token_out="WETH",
            amount_out="0.05",
            price_inputs_json=price_json,
        )
        basis = FIFOBasisStore()

        event = handle_swap(outbox, ledger, basis)

        assert isinstance(event, SwapAccountingEvent)
        assert event.event_type == SwapEventType.SWAP
        assert event.token_in == "USDC"
        assert event.token_out == "WETH"
        assert event.amount_in == Decimal("100")
        assert event.amount_out == Decimal("0.05")
        assert event.amount_in_usd == Decimal("100")  # 100 * 1.0
        assert event.amount_out_usd == Decimal("100")  # 0.05 * 2000
        # Effective price = amount_out / amount_in = 0.05 / 100 = 0.0005
        assert event.effective_price == Decimal("0.0005")
        assert event.slippage_bps == 30
        assert event.gas_usd == Decimal("0.50")
        assert event.confidence == AccountingConfidence.HIGH
        assert event.unavailable_reason == ""
        assert event.identity.deployment_id == _DEPLOYMENT_ID
        assert event.identity.chain == "arbitrum"
        assert event.identity.protocol == "enso"

    def test_effective_price_from_ledger_row(self) -> None:
        """When effective_price is present in the ledger row, use it directly."""
        outbox = _make_outbox_row()
        ledger = _make_ledger_row(
            token_in="USDC",
            amount_in="100",
            token_out="WETH",
            amount_out="0.05",
            effective_price="0.000500",
            price_inputs_json=_price_json({"USDC": "1.0", "WETH": "2000.0"}),
        )
        event = handle_swap(outbox, ledger, None)
        assert event is not None
        assert event.effective_price == Decimal("0.000500")

    def test_cost_basis_recorded_with_basis_store(self) -> None:
        """Token_out acquisition lot is recorded when basis_store is provided."""
        basis = FIFOBasisStore()
        outbox = _make_outbox_row()
        ledger = _make_ledger_row(
            price_inputs_json=_price_json({"USDC": "1.0", "WETH": "2000.0"}),
        )
        event = handle_swap(outbox, ledger, basis)
        assert event is not None
        assert event.cost_basis_recorded is True

    def test_cost_basis_not_recorded_without_basis_store(self) -> None:
        """No lot is recorded (or matched) when basis_store is None."""
        outbox = _make_outbox_row()
        ledger = _make_ledger_row(
            price_inputs_json=_price_json({"USDC": "1.0", "WETH": "2000.0"}),
        )
        event = handle_swap(outbox, ledger, None)
        assert event is not None
        assert event.cost_basis_recorded is False
        assert event.realized_pnl_usd is None


class TestHandleSwapRealizedPnL:
    """Realized PnL is computed correctly when a prior acquisition lot exists."""

    def test_handle_swap_realized_pnl(self) -> None:
        """Buy USDC, then swap USDC → WETH: realized_pnl_usd = amount_in_usd - cost_basis."""
        basis = FIFOBasisStore()
        swap_pk = f"swap:{_CHAIN.lower()}:{_WALLET.lower()}"

        # Simulate having acquired 100 USDC at cost $95 previously.
        basis.record_swap_acquisition(
            deployment_id=_DEPLOYMENT_ID,
            position_key=swap_pk,
            token="USDC",
            amount=Decimal("100"),
            cost_usd=Decimal("95"),
        )

        price_json = _price_json({"USDC": "1.0", "WETH": "2000.0"})
        outbox = _make_outbox_row(wallet_address=_WALLET)
        ledger = _make_ledger_row(
            token_in="USDC",
            amount_in="100",
            token_out="WETH",
            amount_out="0.05",
            price_inputs_json=price_json,
        )

        event = handle_swap(outbox, ledger, basis)

        assert event is not None
        assert event.realized_pnl_usd is not None
        # amount_in_usd = 100 * 1.0 = 100; cost_basis_consumed = 95
        # realized_pnl_usd = 100 - 95 = 5
        assert event.realized_pnl_usd == Decimal("5")

    def test_realized_pnl_loss(self) -> None:
        """Negative realized_pnl_usd when selling below cost basis."""
        basis = FIFOBasisStore()
        swap_pk = f"swap:{_CHAIN.lower()}:{_WALLET.lower()}"

        # Acquired WETH at $2100 each, but current price is $2000.
        basis.record_swap_acquisition(
            deployment_id=_DEPLOYMENT_ID,
            position_key=swap_pk,
            token="WETH",
            amount=Decimal("1"),
            cost_usd=Decimal("2100"),
        )

        price_json = _price_json({"WETH": "2000.0", "USDC": "1.0"})
        outbox = _make_outbox_row(wallet_address=_WALLET)
        ledger = _make_ledger_row(
            token_in="WETH",
            amount_in="1",
            token_out="USDC",
            amount_out="2000",
            price_inputs_json=price_json,
        )

        event = handle_swap(outbox, ledger, basis)

        assert event is not None
        assert event.realized_pnl_usd is not None
        # amount_in_usd = 1 * 2000 = 2000; cost_basis_consumed = 2100
        # realized_pnl_usd = 2000 - 2100 = -100
        assert event.realized_pnl_usd == Decimal("-100")


class TestHandleSwapNoPriorLot:
    """When no prior lot exists for token_in, realized_pnl_usd is None."""

    def test_handle_swap_no_prior_lot(self) -> None:
        basis = FIFOBasisStore()
        price_json = _price_json({"USDC": "1.0", "WETH": "2000.0"})
        outbox = _make_outbox_row()
        ledger = _make_ledger_row(
            token_in="USDC",
            amount_in="100",
            token_out="WETH",
            amount_out="0.05",
            price_inputs_json=price_json,
        )

        event = handle_swap(outbox, ledger, basis)

        assert event is not None
        # No prior lot for USDC → realized_pnl_usd is None
        assert event.realized_pnl_usd is None
        # But token_out lot WAS recorded
        assert event.cost_basis_recorded is True


class TestHandleSwapPendleSkip:
    """Pendle SWAPs return None (owned by the Pendle PT handler)."""

    def test_handle_swap_pendle_skip(self) -> None:
        outbox = _make_outbox_row()
        ledger = _make_ledger_row(protocol="pendle")

        result = handle_swap(outbox, ledger, FIFOBasisStore())

        assert result is None

    def test_handle_swap_pendle_mixed_case(self) -> None:
        outbox = _make_outbox_row()
        ledger = _make_ledger_row(protocol="Pendle")

        result = handle_swap(outbox, ledger, None)

        assert result is None


class TestHandleSwapMissingPrices:
    """Missing prices produce ESTIMATED confidence."""

    def test_handle_swap_missing_prices(self) -> None:
        outbox = _make_outbox_row()
        ledger = _make_ledger_row(price_inputs_json="{}")

        event = handle_swap(outbox, ledger, None)

        assert event is not None
        assert event.confidence == AccountingConfidence.ESTIMATED
        assert event.amount_in_usd is None
        assert event.amount_out_usd is None
        assert "price" in event.unavailable_reason.lower()

    def test_handle_swap_partial_prices(self) -> None:
        """Only token_in price known → amount_out_usd is None, confidence ESTIMATED."""
        price_json = _price_json({"USDC": "1.0"})
        outbox = _make_outbox_row()
        ledger = _make_ledger_row(price_inputs_json=price_json)

        event = handle_swap(outbox, ledger, None)

        assert event is not None
        assert event.confidence == AccountingConfidence.ESTIMATED
        assert event.amount_in_usd == Decimal("100")  # USDC price known
        assert event.amount_out_usd is None  # WETH price unknown


class TestSwapPayloadRoundtrip:
    """to_payload_json / from_payload_json roundtrip preserves all fields."""

    def test_payload_roundtrip(self) -> None:
        from almanak.framework.accounting.models import AccountingIdentity

        identity = AccountingIdentity(
            id="test-id",
            deployment_id="dep-1",
            strategy_id="strat-1",
            cycle_id="cycle-1",
            execution_mode="live",
            timestamp=datetime.now(UTC),
            chain="arbitrum",
            protocol="enso",
            wallet_address=_WALLET,
            tx_hash=_TX_HASH,
            ledger_entry_id="led-1",
        )
        event = SwapAccountingEvent(
            identity=identity,
            event_type=SwapEventType.SWAP,
            protocol="enso",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("100"),
            amount_out=Decimal("0.05"),
            amount_in_usd=Decimal("100"),
            amount_out_usd=Decimal("100"),
            effective_price=Decimal("0.0005"),
            slippage_bps=30,
            realized_pnl_usd=Decimal("5"),
            cost_basis_recorded=True,
            gas_usd=Decimal("0.50"),
            confidence=AccountingConfidence.HIGH,
            unavailable_reason="",
            swap_position_key=f"swap:{_CHAIN.lower()}:{_WALLET.lower()}",
        )

        payload = event.to_payload_json()
        restored = SwapAccountingEvent.from_payload_json(identity, payload)

        assert restored.event_type == SwapEventType.SWAP
        assert restored.token_in == "USDC"
        assert restored.token_out == "WETH"
        assert restored.amount_in == Decimal("100")
        assert restored.amount_out == Decimal("0.05")
        assert restored.amount_in_usd == Decimal("100")
        assert restored.amount_out_usd == Decimal("100")
        assert restored.effective_price == Decimal("0.0005")
        assert restored.slippage_bps == 30
        assert restored.realized_pnl_usd == Decimal("5")
        assert restored.cost_basis_recorded is True
        assert restored.gas_usd == Decimal("0.50")
        assert restored.confidence == AccountingConfidence.HIGH
        assert restored.swap_position_key == event.swap_position_key


class TestRecordSwapAcquisitionAndMatchDisposal:
    """Unit tests for FIFOBasisStore swap lot methods."""

    def test_record_and_full_match(self) -> None:
        """Full match: consume exactly the recorded amount."""
        basis = FIFOBasisStore()
        basis.record_swap_acquisition(
            deployment_id="dep-1",
            position_key="swap:arbitrum:0xabc",
            token="USDC",
            amount=Decimal("100"),
            cost_usd=Decimal("98"),
        )

        cost_consumed, unmatched = basis.match_swap_disposal(
            deployment_id="dep-1",
            position_key="swap:arbitrum:0xabc",
            token="USDC",
            amount=Decimal("100"),
        )

        assert cost_consumed is not None
        assert cost_consumed == Decimal("98")
        assert unmatched == Decimal("0")

    def test_no_prior_lot_returns_none(self) -> None:
        """No lots → (None, amount) signals unknown basis."""
        basis = FIFOBasisStore()

        cost_consumed, unmatched = basis.match_swap_disposal(
            deployment_id="dep-1",
            position_key="swap:arbitrum:0xabc",
            token="USDC",
            amount=Decimal("50"),
        )

        assert cost_consumed is None
        assert unmatched == Decimal("50")

    def test_partial_match(self) -> None:
        """Partial match: lot has 50 but disposal tries 80."""
        basis = FIFOBasisStore()
        basis.record_swap_acquisition(
            deployment_id="dep-1",
            position_key="swap:arbitrum:0xabc",
            token="USDC",
            amount=Decimal("50"),
            cost_usd=Decimal("49"),
        )

        cost_consumed, unmatched = basis.match_swap_disposal(
            deployment_id="dep-1",
            position_key="swap:arbitrum:0xabc",
            token="USDC",
            amount=Decimal("80"),
        )

        # 50 of 80 matched — $49 cost
        assert cost_consumed == Decimal("49")
        assert unmatched == Decimal("30")

    def test_fifo_ordering(self) -> None:
        """Two lots consumed FIFO: older lot exhausted first."""
        basis = FIFOBasisStore()
        basis.record_swap_acquisition(
            deployment_id="dep-1",
            position_key="swap:arbitrum:0xabc",
            token="USDC",
            amount=Decimal("60"),
            cost_usd=Decimal("60"),  # $1/USDC
        )
        basis.record_swap_acquisition(
            deployment_id="dep-1",
            position_key="swap:arbitrum:0xabc",
            token="USDC",
            amount=Decimal("40"),
            cost_usd=Decimal("42"),  # $1.05/USDC
        )

        cost_consumed, unmatched = basis.match_swap_disposal(
            deployment_id="dep-1",
            position_key="swap:arbitrum:0xabc",
            token="USDC",
            amount=Decimal("100"),
        )

        # First lot: 60 USDC at $60. Second lot: 40 USDC at $42. Total = $102.
        assert cost_consumed == Decimal("102")
        assert unmatched == Decimal("0")

    def test_lot_id_returned(self) -> None:
        """record_swap_acquisition returns the lot_id used."""
        basis = FIFOBasisStore()
        my_id = "my-custom-lot-id"
        returned = basis.record_swap_acquisition(
            deployment_id="dep-1",
            position_key="swap:arbitrum:0xabc",
            token="ETH",
            amount=Decimal("1"),
            cost_usd=Decimal("2000"),
            lot_id=my_id,
        )
        assert returned == my_id

    def test_lot_id_auto_generated(self) -> None:
        """When lot_id is empty string, a UUID is auto-generated."""
        basis = FIFOBasisStore()
        returned = basis.record_swap_acquisition(
            deployment_id="dep-1",
            position_key="swap:arbitrum:0xabc",
            token="ETH",
            amount=Decimal("1"),
            cost_usd=None,
            lot_id="",
        )
        assert returned != ""
        # Should be a valid UUID string
        import uuid as _uuid

        _uuid.UUID(returned)  # raises if not valid

    def test_cost_usd_none_lot_returns_none_basis(self) -> None:
        """Lot with cost_usd=None causes match_swap_disposal to return None for cost_consumed.

        The caller cannot compute reliable realized PnL when the acquisition cost is
        unknown — returning None (rather than Decimal("0")) makes the unknown signal
        explicit so the handler sets confidence=ESTIMATED and realized_pnl_usd=None.
        """
        basis = FIFOBasisStore()
        basis.record_swap_acquisition(
            deployment_id="dep-1",
            position_key="swap:arbitrum:0xabc",
            token="USDC",
            amount=Decimal("100"),
            cost_usd=None,  # price was unavailable when acquired
        )

        cost_consumed, unmatched = basis.match_swap_disposal(
            deployment_id="dep-1",
            position_key="swap:arbitrum:0xabc",
            token="USDC",
            amount=Decimal("100"),
        )

        # cost_consumed is None — unknown basis, not zero basis
        assert cost_consumed is None
        assert unmatched == Decimal("0")


class TestReconstructFromEvents:
    """SWAP events are replayed correctly by reconstruct_from_events."""

    def test_reconstruct_swap_acquisition(self) -> None:
        """A SWAP event written to storage is replayed into the lot store."""
        basis = FIFOBasisStore()
        swap_pk = "swap:arbitrum:0xabc"
        events = [
            {
                "event_type": "SWAP",
                "deployment_id": "dep-1",
                "position_key": swap_pk,
                "timestamp": datetime.now(UTC).isoformat(),
                "ledger_entry_id": "led-1",
                "payload_json": json.dumps(
                    {
                        "token_out": "WETH",
                        "amount_out": "0.5",
                        "amount_out_usd": "1000",
                        "swap_position_key": swap_pk,
                    }
                ),
            }
        ]

        replayed = basis.reconstruct_from_events(events)

        assert replayed == 1
        # Verify the lot is now in the store
        cost_consumed, unmatched = basis.match_swap_disposal(
            deployment_id="dep-1",
            position_key=swap_pk,
            token="WETH",
            amount=Decimal("0.5"),
        )
        assert cost_consumed == Decimal("1000")
        assert unmatched == Decimal("0")

    def test_reconstruct_skips_missing_token_out(self) -> None:
        """SWAP events without token_out are silently skipped."""
        basis = FIFOBasisStore()
        events = [
            {
                "event_type": "SWAP",
                "deployment_id": "dep-1",
                "position_key": "swap:arbitrum:0xabc",
                "timestamp": datetime.now(UTC).isoformat(),
                "ledger_entry_id": "led-1",
                "payload_json": json.dumps(
                    {
                        "token_out": "",  # empty
                        "amount_out": "0.5",
                    }
                ),
            }
        ]

        replayed = basis.reconstruct_from_events(events)

        assert replayed == 0

    def test_reconstruct_skips_zero_amount(self) -> None:
        """SWAP events with zero or negative amount_out are skipped."""
        basis = FIFOBasisStore()
        events = [
            {
                "event_type": "SWAP",
                "deployment_id": "dep-1",
                "position_key": "swap:arbitrum:0xabc",
                "timestamp": datetime.now(UTC).isoformat(),
                "ledger_entry_id": "led-1",
                "payload_json": json.dumps(
                    {
                        "token_out": "WETH",
                        "amount_out": "0",
                    }
                ),
            }
        ]

        replayed = basis.reconstruct_from_events(events)

        assert replayed == 0
