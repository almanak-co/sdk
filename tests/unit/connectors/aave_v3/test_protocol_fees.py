"""Tests for Aave V3 ``extract_protocol_fees`` (VIB-3204).

Aave V3 charges zero protocol fees at the pool layer on SUPPLY /
WITHDRAW / BORROW / REPAY — the parser returns ProtocolFees(0,0) for
any recognised operation so downstream code can distinguish
"measured-to-be-zero" from "not measured".
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from almanak.connectors.aave_v3.receipt_parser import (
    EVENT_TOPICS,
    AaveV3ReceiptParser,
)
from almanak.framework.execution.extracted_data import ProtocolFees


USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
USER_ADDRESS = "0x1234567890abcdef1234567890abcdef12345678"
POOL_ADDRESS = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"


def _pad_address(addr: str) -> str:
    return "0x" + addr.lower().replace("0x", "").zfill(64)


def _encode_uint256(value: int) -> str:
    return hex(value)[2:].zfill(64)


def _supply_receipt() -> dict[str, Any]:
    """Minimal Aave V3 Supply event receipt."""
    # Supply(address indexed reserve, address user, address indexed onBehalfOf,
    #        uint256 amount, uint16 indexed referralCode)
    supply_data = (
        _encode_uint256(int(USER_ADDRESS, 16))  # user (non-indexed)
        + _encode_uint256(1_000_000_000)  # amount
    )
    return {
        "status": 1,
        "logs": [
            {
                "address": POOL_ADDRESS,
                "topics": [
                    EVENT_TOPICS["Supply"],
                    _pad_address(USDC_ADDRESS),  # reserve
                    _pad_address(USER_ADDRESS),  # onBehalfOf
                    _encode_uint256(0),  # referralCode
                ],
                "data": "0x" + supply_data,
            }
        ],
    }


class TestAaveV3ProtocolFees:
    def test_returns_none_on_empty_receipt(self):
        parser = AaveV3ReceiptParser(chain="arbitrum")
        assert parser.extract_protocol_fees({"logs": []}) is None

    def test_returns_none_when_no_aave_events(self):
        parser = AaveV3ReceiptParser(chain="arbitrum")
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

    def test_returns_zero_fees_on_supply(self):
        """A recognised Supply operation yields ProtocolFees(0, origination=0).

        VIB-3204 audit fix (pr-auditor Important #5, CodeRabbit P1):
        previously this guarded with ``if result is not None:`` — which let
        the test pass even when the parser returned None, masking a real
        regression. Now we require the positive path and fail loudly
        otherwise.
        """
        parser = AaveV3ReceiptParser(chain="arbitrum")
        result = parser.extract_protocol_fees(_supply_receipt())
        assert isinstance(result, ProtocolFees), (
            "Aave V3 Supply receipt must produce a ProtocolFees struct — "
            "returning None would tell PnL attribution 'fee unknown', but "
            "Aave V3 origination is measured-to-be-zero and should be "
            "reported as such."
        )
        assert result.total_usd == Decimal(0)
        assert result.lending_origination_fee_usd == Decimal(0)
