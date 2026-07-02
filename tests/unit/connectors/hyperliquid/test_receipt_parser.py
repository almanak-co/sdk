"""Unit tests for the Hyperliquid CoreWriter receipt parser.

The receipt proves submission, not fill: it decodes the order we sent from the
RawAction payload but reports all fill-economics as None (off-EVM settlement).
"""

from __future__ import annotations

from decimal import Decimal

from eth_abi import encode as abi_encode

from almanak.connectors.hyperliquid import sdk
from almanak.connectors.hyperliquid.addresses import CORE_WRITER_ADDRESS, RAW_ACTION_EVENT_TOPIC
from almanak.connectors.hyperliquid.receipt_parser import HyperliquidReceiptParser


def _raw_action_receipt(action: sdk.LimitOrderAction) -> dict:
    blob = sdk.encode_limit_order_action(action)
    log_data = "0x" + abi_encode(["bytes"], [blob]).hex()
    return {
        "logs": [
            {
                "address": CORE_WRITER_ADDRESS,
                "topics": [RAW_ACTION_EVENT_TOPIC, "0x" + "11" * 32],
                "data": log_data,
            }
        ]
    }


def _order(**kw) -> sdk.LimitOrderAction:
    base = dict(
        asset=0,
        is_buy=True,
        limit_px=sdk.price_to_wire(Decimal("60000"), 5),
        sz=sdk.size_to_wire(Decimal("0.01"), 5),
        reduce_only=False,
        tif=sdk.TIF_IOC,
        cloid=42,
    )
    base.update(kw)
    return sdk.LimitOrderAction(**base)


class TestParse:
    def test_roundtrip_decodes_submitted_order(self) -> None:
        p = HyperliquidReceiptParser()
        parsed = p.parse_receipt(_raw_action_receipt(_order()))
        assert len(parsed.limit_orders) == 1
        o = parsed.limit_orders[0]
        assert (o.asset, o.is_buy, o.sz, o.reduce_only, o.cloid) == (0, True, 1_000_000, False, 42)

    def test_no_raw_action_returns_empty(self) -> None:
        p = HyperliquidReceiptParser()
        assert p.parse_receipt({"logs": [{"topics": ["0xdead"], "data": "0x"}]}).limit_orders == []

    def test_malformed_payload_skipped_not_raised(self) -> None:
        p = HyperliquidReceiptParser()
        bad = {"logs": [{"topics": [RAW_ACTION_EVENT_TOPIC], "data": "0x1234"}]}
        assert p.parse_receipt(bad).limit_orders == []


class TestExtractions:
    def setup_method(self) -> None:
        self.p = HyperliquidReceiptParser()
        self.receipt = _raw_action_receipt(_order())

    def test_size_delta_is_unmeasured(self) -> None:
        # The shared perp accounting path reads size_delta as USD notional; the
        # EVM receipt carries only a submitted BASE size and no fill notional
        # (settlement is off-EVM), so size_delta must be unmeasured (None) rather
        # than leak a base quantity that would be misread as ~$0.01 of notional.
        assert self.p.extract_size_delta(self.receipt) is None

    def test_position_id_is_cloid_hex(self) -> None:
        assert self.p.extract_position_id(self.receipt) == hex(42)

    def test_fill_economics_are_none_not_zero(self) -> None:
        # Empty != Zero: everything settled off-EVM is unmeasured, never a fake 0.
        assert self.p.extract_entry_price(self.receipt) is None
        assert self.p.extract_exit_price(self.receipt) is None
        assert self.p.extract_realized_pnl(self.receipt) is None
        assert self.p.extract_collateral(self.receipt) is None
        assert self.p.extract_fees_paid(self.receipt) is None
        assert self.p.extract_funding_fee_usd(self.receipt) is None

    def test_empty_receipt_extractions_are_none(self) -> None:
        empty = {"logs": []}
        assert self.p.extract_size_delta(empty) is None
        assert self.p.extract_position_id(empty) is None
