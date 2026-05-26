"""Tests for AsterPerpsReceiptParser stub extraction methods (VIB-3204, VIB-3520).

Verifies that the no-op stubs for ``extract_protocol_fees`` and
``extract_funding_fee_usd`` return ``None`` for any receipt, consistent with
the placeholder-returning-None contract used across other parsers (Enso,
UniswapV3, etc.) while full USD-conversion support is deferred.
"""

from __future__ import annotations

import pytest

from almanak.connectors.aster_perps.receipt_parser import AsterPerpsReceiptParser


@pytest.fixture()
def parser() -> AsterPerpsReceiptParser:
    return AsterPerpsReceiptParser(chain="bsc")


@pytest.fixture()
def empty_receipt() -> dict:
    return {"logs": []}


class TestAsterPerpsProtocolFees:
    """extract_protocol_fees stub — VIB-3204."""

    def test_returns_none_on_empty_receipt(self, parser, empty_receipt):
        assert parser.extract_protocol_fees(empty_receipt) is None

    def test_returns_none_on_receipt_with_logs(self, parser):
        receipt = {
            "logs": [
                {
                    "topics": ["0xdeadbeef"],
                    "data": "0x",
                    "logIndex": 0,
                }
            ]
        }
        assert parser.extract_protocol_fees(receipt) is None

    def test_returns_none_on_none_receipt(self, parser):
        # Defensive: _receipt param is ignored; should not raise
        assert parser.extract_protocol_fees({}) is None


class TestAsterPerpsFundingFeeUsd:
    """extract_funding_fee_usd stub — VIB-3520."""

    def test_returns_none_on_empty_receipt(self, parser, empty_receipt):
        assert parser.extract_funding_fee_usd(empty_receipt) is None

    def test_returns_none_on_receipt_with_logs(self, parser):
        receipt = {
            "logs": [
                {
                    "topics": ["0xdeadbeef"],
                    "data": "0x",
                    "logIndex": 0,
                }
            ]
        }
        assert parser.extract_funding_fee_usd(receipt) is None

    def test_returns_none_on_empty_dict(self, parser):
        assert parser.extract_funding_fee_usd({}) is None


class TestAsterPerpsSupportedExtractions:
    """Verify SUPPORTED_EXTRACTIONS declares both stub fields."""

    def test_protocol_fees_declared(self, parser):
        assert "protocol_fees" in parser.SUPPORTED_EXTRACTIONS

    def test_funding_fee_usd_declared(self, parser):
        assert "funding_fee_usd" in parser.SUPPORTED_EXTRACTIONS
