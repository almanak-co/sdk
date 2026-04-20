"""Tests for Morpho Blue ``extract_protocol_fees`` (VIB-3204).

Morpho Blue charges zero protocol fees at the market layer on SUPPLY /
WITHDRAW / BORROW / REPAY — the parser returns ProtocolFees(0) for any
recognised operation.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.connectors.morpho_blue.receipt_parser import (
    MorphoBlueReceiptParser,
)
from almanak.framework.execution.extracted_data import ProtocolFees


class TestMorphoBlueProtocolFees:
    def test_returns_none_on_empty_receipt(self):
        parser = MorphoBlueReceiptParser(chain="ethereum")
        assert parser.extract_protocol_fees({"logs": []}) is None

    def test_returns_none_when_no_morpho_events(self):
        parser = MorphoBlueReceiptParser(chain="ethereum")
        receipt = {
            "status": 1,
            "logs": [
                {
                    "address": "0x" + "ff" * 20,
                    "topics": ["0x" + "ab" * 32],
                    "data": "0x",
                }
            ],
        }
        assert parser.extract_protocol_fees(receipt) is None

    def test_zero_fees_round_trip_through_to_dict(self):
        """Validates the ProtocolFees dataclass itself round-trips
        through to_dict for the ALL-NONE construction shape (not what
        Morpho returns — see ``test_returns_measured_zero_on_supply``
        for the real parser contract)."""
        fees = ProtocolFees(total_usd=Decimal(0))
        as_dict = fees.to_dict()
        assert as_dict["total_usd"] == "0"
        assert as_dict["lending_origination_fee_usd"] is None

    def test_returns_measured_zero_on_supply(self):
        """CodeRabbit audit fix: positive-path test that actually calls
        the parser. A regression to "always None" would have slipped
        through with only the None-path tests. Asserts the parser emits
        ``lending_origination_fee_usd=Decimal(0)`` — matching Aave V3's
        shape so both lending protocols are indistinguishable in the
        downstream PnL attribution layer.
        """
        parser = MorphoBlueReceiptParser(chain="ethereum")
        # Minimal Supply log. Morpho Blue event signature
        # (keccak of "Supply(bytes32,address,address,uint256,uint256)").
        supply_topic = "0xedf8870433c83823eb071d3df1caa8d008f12f6440918c20d75a3602cda30fe0"
        receipt = {
            "status": 1,
            "logs": [
                {
                    "address": "0x" + "aa" * 20,  # Morpho Blue contract
                    "topics": [
                        supply_topic,
                        "0x" + "11" * 32,  # market_id
                        "0x" + "00" * 12 + "22" * 20,  # caller (padded address)
                        "0x" + "00" * 12 + "33" * 20,  # onBehalfOf (padded address)
                    ],
                    "data": "0x" + "0" * 64 * 2,  # assets + shares
                }
            ],
        }
        fees = parser.extract_protocol_fees(receipt)
        assert isinstance(fees, ProtocolFees), (
            "Morpho Supply receipt must yield a ProtocolFees struct (not None) — "
            "Morpho charges zero protocol fees at the market layer and this is "
            "MEASURED, not unknown."
        )
        assert fees.total_usd == Decimal(0)
        assert fees.lending_origination_fee_usd == Decimal(0)
