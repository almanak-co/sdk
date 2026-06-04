"""Unit tests for Morpho Blue receipt parser enrichment methods.

Tests cover:
- extract_supply_amount: Extracts assets from Supply event
- extract_a_token_received: Returns shares from Supply event (Morpho equivalent of aTokens)
- extract_supply_rate: Returns None (Morpho events don't include rate info)
- extract_supply_collateral_amount: Extracts assets from SupplyCollateral event
- extract_withdraw_collateral_amount: Extracts assets from WithdrawCollateral event
- extract_shares_received: Extracts shares from Supply event
- Edge cases: empty receipts, no matching events, malformed data

Addresses VIB-515: Add SUPPLY result enrichment to Morpho Blue receipt parser.
Addresses VIB-4635: Add WITHDRAW collateral enrichment (WITHDRAW-side mirror of
the SUPPLY collateral path).
"""

from __future__ import annotations

from typing import Any

import pytest

from almanak.connectors.morpho_blue.receipt_parser import (
    EVENT_TOPICS,
    MorphoBlueReceiptParser,
)

# =============================================================================
# Test Fixtures
# =============================================================================

MORPHO_BLUE_ADDRESS = "0xBBBBBBBBbb9cC5e90e3b3Af64bdAF62C37EEFFCb"
MARKET_ID = "0x" + "ab" * 32
USER_ADDRESS = "0x1234567890abcdef1234567890abcdef12345678"
CALLER_ADDRESS = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"


def _pad_address(addr: str) -> str:
    """Pad an address to 32 bytes for topic encoding."""
    clean = addr.lower().replace("0x", "")
    return "0x" + clean.zfill(64)


def _encode_uint256(value: int) -> str:
    """Encode an integer as a 32-byte hex string (no 0x prefix)."""
    return hex(value)[2:].zfill(64)


def _make_supply_receipt(
    assets: int = 1_000_000_000,  # 1000 USDC (6 decimals)
    shares: int = 999_000_000_000_000_000_000,  # ~999 shares (18 decimals)
) -> dict[str, Any]:
    """Build a Morpho Blue Supply transaction receipt.

    Supply(Id indexed id, address indexed caller, address indexed onBehalfOf, uint256 assets, uint256 shares)
    """
    data = "0x" + _encode_uint256(assets) + _encode_uint256(shares)
    return {
        "status": 1,
        "transactionHash": "0x" + "aa" * 32,
        "gasUsed": 200000,
        "logs": [
            {
                "address": MORPHO_BLUE_ADDRESS,
                "topics": [
                    EVENT_TOPICS["Supply"],
                    MARKET_ID,
                    _pad_address(CALLER_ADDRESS),
                    _pad_address(USER_ADDRESS),
                ],
                "data": data,
                "logIndex": 0,
            }
        ],
    }


def _make_supply_collateral_receipt(
    assets: int = 5_000_000_000_000_000_000,  # 5 WETH (18 decimals)
) -> dict[str, Any]:
    """Build a Morpho Blue SupplyCollateral transaction receipt.

    SupplyCollateral(Id indexed id, address indexed caller, address indexed onBehalfOf, uint256 assets)
    """
    data = "0x" + _encode_uint256(assets)
    return {
        "status": 1,
        "transactionHash": "0x" + "bb" * 32,
        "gasUsed": 180000,
        "logs": [
            {
                "address": MORPHO_BLUE_ADDRESS,
                "topics": [
                    EVENT_TOPICS["SupplyCollateral"],
                    MARKET_ID,
                    _pad_address(CALLER_ADDRESS),
                    _pad_address(USER_ADDRESS),
                ],
                "data": data,
                "logIndex": 0,
            }
        ],
    }


def _make_withdraw_collateral_receipt(
    assets: int = 200_000_000_000_000_000,  # 0.2 wstETH (18 decimals)
) -> dict[str, Any]:
    """Build a Morpho Blue WithdrawCollateral transaction receipt.

    WithdrawCollateral(Id indexed id, address caller, address indexed onBehalfOf,
    address indexed receiver, uint256 assets)

    ``caller`` is the first (non-indexed) data word; ``assets`` is the second
    word — mirrors ``_parse_withdraw_collateral`` in the parser.
    """
    data = "0x" + _pad_address(CALLER_ADDRESS).replace("0x", "") + _encode_uint256(assets)
    return {
        "status": 1,
        "transactionHash": "0x" + "dd" * 32,
        "gasUsed": 180000,
        "logs": [
            {
                "address": MORPHO_BLUE_ADDRESS,
                "topics": [
                    EVENT_TOPICS["WithdrawCollateral"],
                    MARKET_ID,
                    _pad_address(USER_ADDRESS),  # onBehalfOf (indexed)
                    _pad_address(USER_ADDRESS),  # receiver (indexed)
                ],
                "data": data,
                "logIndex": 0,
            }
        ],
    }


def _make_empty_receipt() -> dict[str, Any]:
    """Build a receipt with no relevant events."""
    return {
        "status": 1,
        "transactionHash": "0x" + "cc" * 32,
        "gasUsed": 21000,
        "logs": [],
    }


# =============================================================================
# Tests
# =============================================================================


@pytest.fixture
def parser() -> MorphoBlueReceiptParser:
    return MorphoBlueReceiptParser()


class TestExtractSupplyAmount:
    def test_extracts_assets_from_supply_event(self, parser):
        receipt = _make_supply_receipt(assets=1_000_000_000)
        result = parser.extract_supply_amount(receipt)
        assert result == 1_000_000_000

    def test_returns_none_for_empty_receipt(self, parser):
        result = parser.extract_supply_amount(_make_empty_receipt())
        assert result is None

    def test_returns_none_for_collateral_only_receipt(self, parser):
        result = parser.extract_supply_amount(_make_supply_collateral_receipt())
        assert result is None


class TestExtractATokenReceived:
    def test_returns_shares_from_supply_event(self, parser):
        shares = 999_000_000_000_000_000_000
        receipt = _make_supply_receipt(shares=shares)
        result = parser.extract_a_token_received(receipt)
        assert result == shares

    def test_returns_none_for_empty_receipt(self, parser):
        result = parser.extract_a_token_received(_make_empty_receipt())
        assert result is None

    def test_delegates_to_extract_shares_received(self, parser):
        shares = 500_000_000_000_000_000_000
        receipt = _make_supply_receipt(shares=shares)
        assert parser.extract_a_token_received(receipt) == parser.extract_shares_received(receipt)


class TestExtractSupplyRate:
    def test_always_returns_none(self, parser):
        result = parser.extract_supply_rate(_make_supply_receipt())
        assert result is None

    def test_returns_none_for_empty_receipt(self, parser):
        result = parser.extract_supply_rate(_make_empty_receipt())
        assert result is None


class TestExtractSupplyCollateralAmount:
    def test_extracts_assets_from_supply_collateral_event(self, parser):
        assets = 5_000_000_000_000_000_000
        receipt = _make_supply_collateral_receipt(assets=assets)
        result = parser.extract_supply_collateral_amount(receipt)
        assert result == assets

    def test_returns_none_for_empty_receipt(self, parser):
        result = parser.extract_supply_collateral_amount(_make_empty_receipt())
        assert result is None

    def test_returns_none_for_supply_only_receipt(self, parser):
        result = parser.extract_supply_collateral_amount(_make_supply_receipt())
        assert result is None


class TestExtractWithdrawCollateralAmount:
    """VIB-4635: WITHDRAW-side mirror of ``extract_supply_collateral_amount``.

    Morpho collateral withdrawals emit ``WithdrawCollateral`` (not the
    loan-side ``Withdraw``); the extractor must read the ``assets`` field of
    that event so downstream accounting can record ``amount_token``.
    """

    def test_extracts_assets_from_withdraw_collateral_event(self, parser):
        assets = 200_000_000_000_000_000  # 0.2 wstETH
        receipt = _make_withdraw_collateral_receipt(assets=assets)
        result = parser.extract_withdraw_collateral_amount(receipt)
        assert result == assets

    def test_returns_none_for_empty_receipt(self, parser):
        # Empty ≠ Zero ≠ None: no WithdrawCollateral event means unmeasured.
        result = parser.extract_withdraw_collateral_amount(_make_empty_receipt())
        assert result is None

    def test_returns_none_for_supply_collateral_only_receipt(self, parser):
        # A SupplyCollateral-only receipt must not satisfy the WITHDRAW
        # extractor — the legs are directionally distinct.
        result = parser.extract_withdraw_collateral_amount(_make_supply_collateral_receipt())
        assert result is None

    def test_supply_collateral_extractor_ignores_withdraw_collateral(self, parser):
        # Symmetric guard: the SUPPLY extractor must return None for a
        # WithdrawCollateral-only receipt.
        result = parser.extract_supply_collateral_amount(_make_withdraw_collateral_receipt())
        assert result is None


class TestExtractSharesReceived:
    def test_extracts_shares_from_supply_event(self, parser):
        shares = 999_000_000_000_000_000_000
        receipt = _make_supply_receipt(shares=shares)
        result = parser.extract_shares_received(receipt)
        assert result == shares

    def test_returns_none_for_empty_receipt(self, parser):
        result = parser.extract_shares_received(_make_empty_receipt())
        assert result is None
