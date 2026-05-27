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
_DEPLOYMENT_ID = "strat-swap-test"
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
        "deployment_id": _DEPLOYMENT_ID,
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
        "deployment_id": _DEPLOYMENT_ID,
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

    def test_unmeasured_amounts_yield_none_throughout(self) -> None:
        """Empty != zero — unmeasured ledger amounts must propagate as None
        through every field of the SwapAccountingEvent (not just
        effective_price). Previously the swap handler treated empty strings
        as zeros and emitted a row with ``amount_in=Decimal(0)``,
        ``amount_in_usd=Decimal(0)``, ``effective_price=Decimal(0)`` — a
        sentinel indistinguishable from a measured zero swap. Pin the
        corrected contract: ``None`` for every measured-amount field, plus
        ``confidence=ESTIMATED`` with a clear ``unavailable_reason``.
        """
        basis = FIFOBasisStore()
        outbox = _make_outbox_row()
        ledger = _make_ledger_row(
            token_in="USDC",
            amount_in="",  # ledger marks unmeasured per Empty != zero
            token_out="WETH",
            amount_out="",
            effective_price="",
            price_inputs_json=_price_json({"USDC": "1.0", "WETH": "2000.0"}),
        )
        event = handle_swap(outbox, ledger, basis)
        assert event is not None
        # Every measured-amount field must be None — no fake measured zero.
        assert event.amount_in is None, "amount_in must be None when ledger is unmeasured"
        assert event.amount_out is None, "amount_out must be None when ledger is unmeasured"
        assert event.amount_in_usd is None, "USD conversion must skip when amount is None"
        assert event.amount_out_usd is None
        assert event.effective_price is None
        assert event.confidence == AccountingConfidence.ESTIMATED
        assert "unmeasured" in event.unavailable_reason
        # FIFO matching + lot recording must skip on unmeasured rows so we
        # don't consume or record fake-zero lots.
        assert event.realized_pnl_usd is None
        assert event.cost_basis_recorded is False

    def test_unmeasured_with_prices_present_does_not_falsely_report_missing_prices(self) -> None:
        """When amounts are unmeasured we force amount_*_usd to None for the
        Empty != zero contract. The confidence helper must still distinguish
        that case from "prices were genuinely missing in price_inputs_json"
        — otherwise the unavailable_reason on a perfectly-priced exotic-token
        swap would falsely claim the price oracle was missing inputs it
        actually had. Pin: when both prices are present and amounts are
        unmeasured, unavailable_reason mentions "unmeasured" but does NOT
        mention "missing prices".
        """
        basis = FIFOBasisStore()
        outbox = _make_outbox_row()
        ledger = _make_ledger_row(
            token_in="USDC",
            amount_in="",
            token_out="WETH",
            amount_out="",
            effective_price="",
            # Both prices ARE present in the row — pricing was not the gap.
            price_inputs_json=_price_json({"USDC": "1.0", "WETH": "2000.0"}),
        )
        event = handle_swap(outbox, ledger, basis)
        assert event is not None
        assert event.confidence == AccountingConfidence.ESTIMATED
        assert "unmeasured" in event.unavailable_reason
        assert "missing prices" not in event.unavailable_reason, (
            "Forcing amount_*_usd to None on unmeasured rows must NOT cause "
            "the confidence helper to falsely report missing prices when "
            "the prices were actually present in price_inputs_json."
        )

    def test_unmeasured_amounts_override_stale_effective_price(self) -> None:
        """A stale or non-empty ``effective_price`` in the ledger row must
        NOT leak into an unmeasured event. Unmeasured amounts make any
        ``effective_price`` unverifiable, so the row's measured-state
        contract requires None — not the upstream-emitted value.

        Pin: when ``amount_in`` is unparsable (unmeasured) but the ledger
        row carries ``effective_price="1000"``, the resulting event has
        ``effective_price=None``. Empty != zero applies to leak-through too.
        """
        basis = FIFOBasisStore()
        outbox = _make_outbox_row()
        ledger = _make_ledger_row(
            token_in="USDC",
            amount_in="not-a-number",  # unmeasured
            token_out="WETH",
            amount_out="0.05",
            effective_price="1000",  # stale upstream value
            price_inputs_json=_price_json({"USDC": "1.0", "WETH": "2000.0"}),
        )
        event = handle_swap(outbox, ledger, basis)
        assert event is not None
        assert event.amount_in is None
        assert event.effective_price is None, (
            "An unmeasured row must NOT propagate a stale ledger "
            "effective_price; the unmeasured contract overrides the upstream "
            "value. See docs/internal/blueprints/27-accounting.md 'Empty != zero'."
        )
        assert event.confidence == AccountingConfidence.ESTIMATED
        assert "unmeasured" in event.unavailable_reason

    def test_unparsable_amount_in_yields_none_amount_in(self) -> None:
        """Empty != zero applies to unparsable strings, not just empty ones.

        A ledger row with ``amount_in="NaN"`` (or any non-decimal-parseable
        string) historically coerced to ``Decimal(0)`` via
        ``_parse_decimal(...) or Decimal("0")`` — flagging the row as a
        measured-zero swap. After this fix, parse failure is treated
        identically to empty string: the row is unmeasured.
        """
        basis = FIFOBasisStore()
        outbox = _make_outbox_row()
        ledger = _make_ledger_row(
            token_in="USDC",
            amount_in="not-a-number",  # parse failure
            token_out="WETH",
            amount_out="0.05",
            effective_price="",
            price_inputs_json=_price_json({"USDC": "1.0", "WETH": "2000.0"}),
        )
        event = handle_swap(outbox, ledger, basis)
        assert event is not None
        assert event.amount_in is None
        # amount_out parses fine; only the unparsable side is None.
        assert event.amount_out == Decimal("0.05")
        # But effective_price must still be None — we cannot compute
        # amount_out / amount_in when one side is unmeasured.
        assert event.effective_price is None
        # USD on the unmeasured side is None; the parsed side is fine.
        assert event.amount_in_usd is None
        # FIFO must skip on unmeasured to avoid wrong lot consumption.
        assert event.realized_pnl_usd is None
        assert event.cost_basis_recorded is False
        assert event.confidence == AccountingConfidence.ESTIMATED
        assert "unmeasured" in event.unavailable_reason

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
            deployment_id="strat-1",
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

    def test_payload_roundtrip_preserves_unmeasured_none(self) -> None:
        """An unmeasured swap (decimals could not be resolved by the receipt
        parser) MUST roundtrip through to/from_payload_json without
        substituting None back into ``Decimal(0)``. The "Empty != zero"
        invariant from docs/internal/blueprints/27-accounting.md says ``None`` =
        unmeasured and ``Decimal(0)`` = measured zero — persistence cannot
        silently conflate them.
        """
        from almanak.framework.accounting.models import AccountingIdentity

        identity = AccountingIdentity(
            id="test-id-unmeasured",
            deployment_id="strat-1",
            cycle_id="cycle-1",
            execution_mode="live",
            timestamp=datetime.now(UTC),
            chain="arbitrum",
            protocol="pancakeswap_v3",
            wallet_address=_WALLET,
            tx_hash=_TX_HASH,
            ledger_entry_id="led-1",
        )
        event = SwapAccountingEvent(
            identity=identity,
            event_type=SwapEventType.SWAP,
            protocol="pancakeswap_v3",
            token_in="EXOTIC",
            token_out="WETH",
            # Unmeasured row: every measured-amount field is None per the
            # Empty != zero contract.
            amount_in=None,
            amount_out=None,
            amount_in_usd=None,
            amount_out_usd=None,
            effective_price=None,
            slippage_bps=None,
            realized_pnl_usd=None,
            cost_basis_recorded=False,
            gas_usd=Decimal("0.25"),
            confidence=AccountingConfidence.ESTIMATED,
            unavailable_reason="swap amounts unmeasured (token decimals could not be resolved by receipt parser)",
            swap_position_key=f"swap:{_CHAIN.lower()}:{_WALLET.lower()}",
        )

        payload = event.to_payload_json()
        restored = SwapAccountingEvent.from_payload_json(identity, payload)

        # Every nullable measured field MUST come back as None — never Decimal(0).
        assert restored.amount_in is None, (
            "amount_in roundtrip must preserve None (unmeasured); got "
            f"{restored.amount_in!r}. See docs/internal/blueprints/27-accounting.md "
            "'Empty != zero'."
        )
        assert restored.amount_out is None
        assert restored.amount_in_usd is None
        assert restored.amount_out_usd is None
        assert restored.effective_price is None
        assert restored.realized_pnl_usd is None
        # Belt-and-braces: a measured field (gas_usd) must continue to
        # roundtrip correctly so we know the test's None expectations
        # aren't a false negative from a broader serialization bug.
        assert restored.gas_usd == Decimal("0.25")
        assert restored.confidence == AccountingConfidence.ESTIMATED
        assert "unmeasured" in restored.unavailable_reason


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


# ──────────────────────────────────────────────────────────────────────────────
# VIB-4304: address-keyed token_in / token_out must resolve to symbol before
# the price_oracle lookup. The ``price_inputs_json`` ledger column is
# symbol-keyed (``"WETH"``); several connector receipt parsers (Aerodrome
# confirmed; suspected for PancakeSwap, Sushi, Uniswap V3, Curve) stamp the
# contract address into ``swap_amounts.token_in``. Without resolution, every
# SWAP accounting row landed at ESTIMATED with a misleading "missing prices"
# reason — even when both prices were present (just keyed by symbol).
# ──────────────────────────────────────────────────────────────────────────────


class TestSwapAddressKeyedTokenResolution:
    """VIB-4304: address-keyed token_in / token_out must be resolved to a
    symbol via the token resolver before lookup against the symbol-keyed
    ``price_inputs_json``. Failed resolution must fall through cleanly and
    preserve the original address in ``unavailable_reason`` (Empty != zero —
    no fabricated symbol substitution).
    """

    # Real Base mainnet addresses for USDC / WETH — both are in the static
    # token registry, so the resolver can map them under ``skip_gateway=True``
    # (no live gateway / no test fixtures needed).
    _USDC_BASE = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
    _WETH_BASE = "0x4200000000000000000000000000000000000006"

    def test_handle_swap_address_keyed_token_in_resolves_to_symbol_and_renders_high(self) -> None:
        """The reproduction case from VIB-4304: an Aerodrome SWAP row whose
        ``token_in`` / ``token_out`` are stamped as addresses and whose
        ``price_inputs_json`` is symbol-keyed. Before the fix this rendered
        ESTIMATED with "missing prices in price_inputs_json:
        0X833589FCD6... price, 0X4200...0006 price" even though both prices
        were present. After the fix the addresses resolve to ``USDC`` and
        ``WETH``, the prices land, and the row renders HIGH with empty
        ``unavailable_reason`` and non-null USD amounts.
        """
        price_json = _price_json({"USDC": "1.0", "WETH": "2000.0"})
        outbox = _make_outbox_row()
        ledger = _make_ledger_row(
            token_in=self._USDC_BASE,
            amount_in="2",
            token_out=self._WETH_BASE,
            amount_out="0.001",
            protocol="aerodrome",
            chain="base",
            price_inputs_json=price_json,
        )

        event = handle_swap(outbox, ledger, FIFOBasisStore())

        assert event is not None
        assert event.confidence == AccountingConfidence.HIGH
        assert event.unavailable_reason == ""
        # USD amounts must be priced — 2 * $1 and 0.001 * $2000 = $2 each.
        assert event.amount_in_usd == Decimal("2.0")
        assert event.amount_out_usd == Decimal("2.000")
        # The event still surfaces the ORIGINAL address values on the
        # ``token_in`` / ``token_out`` fields (uppercased, same as today).
        # The resolved symbol is used ONLY as the price lookup key — it is
        # NOT a substitute for the source-of-truth ledger value.
        assert event.token_in == self._USDC_BASE.upper()
        assert event.token_out == self._WETH_BASE.upper()

    def test_handle_swap_address_keyed_only_one_side_still_resolves(self) -> None:
        """A row where one side is an address and the other is a symbol —
        both legs must still price correctly. Defensive against connectors
        that mix the two shapes per intent direction.
        """
        price_json = _price_json({"USDC": "1.0", "WETH": "2000.0"})
        outbox = _make_outbox_row()
        ledger = _make_ledger_row(
            token_in="USDC",  # symbol
            amount_in="2",
            token_out=self._WETH_BASE,  # address
            amount_out="0.001",
            protocol="aerodrome",
            chain="base",
            price_inputs_json=price_json,
        )

        event = handle_swap(outbox, ledger, FIFOBasisStore())

        assert event is not None
        assert event.confidence == AccountingConfidence.HIGH
        assert event.amount_in_usd == Decimal("2.0")
        assert event.amount_out_usd == Decimal("2.000")

    def test_handle_swap_symbol_keyed_token_in_still_renders_high(self) -> None:
        """Regression guard: pre-fix behaviour for symbol-keyed ``token_in`` /
        ``token_out`` must be preserved. This is the baseline that the
        existing ``test_handle_swap_basic`` exercises — re-asserted here
        with a Base-chain ledger row so the contract is pinned end-to-end.
        """
        price_json = _price_json({"USDC": "1.0", "WETH": "2000.0"})
        outbox = _make_outbox_row()
        ledger = _make_ledger_row(
            token_in="USDC",
            amount_in="2",
            token_out="WETH",
            amount_out="0.001",
            protocol="aerodrome",
            chain="base",
            price_inputs_json=price_json,
        )

        event = handle_swap(outbox, ledger, FIFOBasisStore())

        assert event is not None
        assert event.confidence == AccountingConfidence.HIGH
        assert event.amount_in_usd == Decimal("2.0")
        assert event.amount_out_usd == Decimal("2.000")

    def test_handle_swap_unresolvable_address_falls_through_to_estimated(self) -> None:
        """Resolver miss → ESTIMATED with the ORIGINAL address in
        ``unavailable_reason``. Empty != zero — never fabricate a phantom
        symbol the auditor can't trace back to chain state.
        """
        # An address that is NOT in the static registry on Base.
        unknown_addr = "0x000000000000000000000000000000000000dEAD"
        price_json = _price_json({"USDC": "1.0", "WETH": "2000.0"})
        outbox = _make_outbox_row()
        ledger = _make_ledger_row(
            token_in=unknown_addr,
            amount_in="2",
            token_out="WETH",
            amount_out="0.001",
            protocol="aerodrome",
            chain="base",
            price_inputs_json=price_json,
        )

        event = handle_swap(outbox, ledger, FIFOBasisStore())

        assert event is not None
        assert event.confidence == AccountingConfidence.ESTIMATED
        # token_out resolves fine → USD amount is computed.
        assert event.amount_out_usd == Decimal("2.000")
        # token_in misses → USD amount is None.
        assert event.amount_in_usd is None
        # The unavailable_reason names the ORIGINAL token (uppercased
        # address, same shape as the event.token_in field), not a phantom
        # symbol nor an empty string.
        assert "missing prices" in event.unavailable_reason
        assert unknown_addr.upper() in event.unavailable_reason

    def test_handle_swap_no_chain_falls_through(self) -> None:
        """Defensive: a ledger row with no ``chain`` cannot be resolved
        (cross-chain address ambiguity). The handler must fall through to
        ESTIMATED with the original address rather than raising.
        """
        price_json = _price_json({"USDC": "1.0", "WETH": "2000.0"})
        outbox = _make_outbox_row()
        ledger = _make_ledger_row(
            token_in=self._USDC_BASE,
            amount_in="2",
            token_out=self._WETH_BASE,
            amount_out="0.001",
            protocol="aerodrome",
            chain="",  # missing
            price_inputs_json=price_json,
        )

        event = handle_swap(outbox, ledger, FIFOBasisStore())

        assert event is not None
        assert event.confidence == AccountingConfidence.ESTIMATED
        # Both lookups miss → both USD amounts None.
        assert event.amount_in_usd is None
        assert event.amount_out_usd is None
        # Both addresses surface in the unavailable_reason.
        assert self._USDC_BASE.upper() in event.unavailable_reason
        assert self._WETH_BASE.upper() in event.unavailable_reason

    def test_resolve_price_lookup_key_helper_pass_through_for_symbol(self) -> None:
        """Direct unit test for ``_resolve_price_lookup_key``: symbol-shaped
        inputs must pass through unchanged (no resolver call, no exception)
        so the existing symbol-keyed code path stays bit-identical.
        """
        from almanak.framework.accounting.category_handlers.swap_handler import _resolve_price_lookup_key

        assert _resolve_price_lookup_key("USDC", "base") == "USDC"
        assert _resolve_price_lookup_key("WETH", "arbitrum") == "WETH"
        # Empty / whitespace pass through (caller emits its own diagnostic).
        assert _resolve_price_lookup_key("", "base") == ""

    def test_resolve_price_lookup_key_helper_resolves_address(self) -> None:
        """Direct unit test: address-shaped inputs resolve to the canonical
        symbol via the static registry (no gateway needed).
        """
        from almanak.framework.accounting.category_handlers.swap_handler import _resolve_price_lookup_key

        # Uppercased EVM address — helper lowercases internally before
        # passing to the resolver, so this still resolves cleanly.
        assert _resolve_price_lookup_key(self._USDC_BASE.upper(), "base") == "USDC"
        assert _resolve_price_lookup_key(self._WETH_BASE.upper(), "base") == "WETH"
        # Mixed-case EVM (canonical EIP-55 checksum form) must also resolve.
        assert _resolve_price_lookup_key(self._USDC_BASE, "base") == "USDC"
        assert _resolve_price_lookup_key(self._WETH_BASE, "base") == "WETH"

    def test_resolve_price_lookup_key_helper_handles_solana_addresses(self) -> None:
        """PR #2250 review: Solana base58 addresses must reach the resolver
        with their case preserved.

        Before the review fix, the handler uppercased ``token_in`` /
        ``token_out`` upstream of ``_resolve_price_lookup_key``. The helper
        already special-cased EVM (lowercases internally), but Solana
        base58 is case-sensitive — passing an uppercased base58 to the
        resolver is semantically a different mint address. After the fix,
        the handler passes the **raw** (un-uppercased) value through and
        the helper preserves case for Solana.

        Regression guard: case-preserved Solana address must resolve to
        the canonical uppercase symbol.
        """
        from almanak.framework.accounting.category_handlers.swap_handler import _resolve_price_lookup_key

        # Solana USDC mainnet mint (case-sensitive base58, mixed case).
        usdc_sol = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        assert _resolve_price_lookup_key(usdc_sol, "solana") == "USDC"

    def test_resolve_price_lookup_key_helper_lowercase_symbol_upcased(self) -> None:
        """A lowercase / mixed-case symbol input must canonicalise to upper.

        The price_oracle dict (parsed by ``parse_price_inputs``) is uppercase-
        keyed; a lowercase symbol arriving via the new raw-pass-through path
        would otherwise miss.
        """
        from almanak.framework.accounting.category_handlers.swap_handler import _resolve_price_lookup_key

        assert _resolve_price_lookup_key("usdc", "base") == "USDC"
        assert _resolve_price_lookup_key("WeTh", "base") == "WETH"
