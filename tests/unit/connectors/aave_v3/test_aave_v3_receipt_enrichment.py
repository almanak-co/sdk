"""Unit tests for Aave V3 receipt parser enrichment methods.

Tests cover:
- extract_debt_token: Finds debt token contract from mint Transfer during Borrow
- extract_supply_rate: Reads liquidity_rate from ReserveDataUpdated event
- extract_remaining_debt: Best-effort detection of full repayment via debt token burn
- SUPPORTED_EXTRACTIONS declaration includes all new fields
- Edge cases: missing logs, no matching events, malformed data

Addresses VIB-238: Add remaining_debt/debt_token/supply_rate enrichment.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.connectors.aave_v3.receipt_parser import (
    EVENT_TOPICS,
    AaveV3ReceiptParser,
    BorrowAmountsResult,
    RepayAmountsResult,
    SupplyAmountsResult,
    WithdrawAmountsResult,
)


# =============================================================================
# Test Fixtures
# =============================================================================

# Common addresses used in test receipts
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
WETH_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USER_ADDRESS = "0x1234567890abcdef1234567890abcdef12345678"
DEBT_TOKEN_ADDRESS = "0xDebtDebtDebtDebtDebtDebtDebtDebtDebtDebt"
ATOKEN_ADDRESS = "0xaTokenaTokenaTokenaTokenaTokenaTokenaToke"
POOL_ADDRESS = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def _pad_address(addr: str) -> str:
    """Pad an address to 32 bytes (64 hex chars) for topic encoding."""
    clean = addr.lower().replace("0x", "")
    return "0x" + clean.zfill(64)


def _encode_uint256(value: int) -> str:
    """Encode an integer as a 32-byte hex string (no 0x prefix)."""
    return hex(value)[2:].zfill(64)


def _make_borrow_receipt(
    borrow_amount: int = 1000000000,  # 1000 USDC (6 decimals)
    interest_rate_mode: int = 2,  # variable
    borrow_rate_ray: int = 50000000000000000000000000,  # 5% in ray (1e27)
    include_debt_token_mint: bool = True,
    debt_token_address: str = DEBT_TOKEN_ADDRESS,
    include_reserve_data_updated: bool = False,
) -> dict[str, Any]:
    """Build a realistic Aave V3 Borrow transaction receipt."""
    logs: list[dict] = []

    # Borrow event
    # Borrow(address indexed reserve, address user, address indexed onBehalfOf,
    #        uint256 amount, uint256 interestRateMode, uint256 borrowRate, uint16 referralCode)
    borrow_data = (
        _encode_uint256(int(USER_ADDRESS, 16))  # user (non-indexed address)
        + _encode_uint256(borrow_amount)  # amount
        + _encode_uint256(interest_rate_mode)  # interestRateMode
        + _encode_uint256(borrow_rate_ray)  # borrowRate
        + _encode_uint256(0)  # referralCode
    )
    logs.append({
        "address": POOL_ADDRESS,
        "topics": [
            EVENT_TOPICS["Borrow"],
            _pad_address(USDC_ADDRESS),  # reserve (indexed)
            _pad_address(USER_ADDRESS),  # onBehalfOf (indexed)
        ],
        "data": "0x" + borrow_data,
        "logIndex": 0,
    })

    # Debt token mint (Transfer from 0x0 to user)
    if include_debt_token_mint:
        transfer_data = _encode_uint256(borrow_amount)
        logs.append({
            "address": debt_token_address,
            "topics": [
                EVENT_TOPICS["Transfer"],
                _pad_address(ZERO_ADDRESS),  # from (mint)
                _pad_address(USER_ADDRESS),  # to
            ],
            "data": "0x" + transfer_data,
            "logIndex": 1,
        })

    if include_reserve_data_updated:
        logs.extend(_make_reserve_data_updated_logs())

    return {
        "transactionHash": "0x" + "ab" * 32,
        "blockNumber": 12345678,
        "status": 1,
        "logs": logs,
    }


def _make_supply_receipt(
    supply_amount: int = 1000000000000000000,  # 1 WETH (18 decimals)
    include_reserve_data_updated: bool = True,
    liquidity_rate_ray: int = 35000000000000000000000000,  # 3.5% in ray
) -> dict[str, Any]:
    """Build a realistic Aave V3 Supply transaction receipt."""
    logs: list[dict] = []

    # Supply event
    supply_data = (
        _encode_uint256(int(USER_ADDRESS, 16))  # user
        + _encode_uint256(supply_amount)  # amount
        + _encode_uint256(0)  # referralCode
    )
    logs.append({
        "address": POOL_ADDRESS,
        "topics": [
            EVENT_TOPICS["Supply"],
            _pad_address(WETH_ADDRESS),  # reserve (indexed)
            _pad_address(USER_ADDRESS),  # onBehalfOf (indexed)
        ],
        "data": "0x" + supply_data,
        "logIndex": 0,
    })

    # aToken mint
    transfer_data = _encode_uint256(supply_amount)
    logs.append({
        "address": ATOKEN_ADDRESS,
        "topics": [
            EVENT_TOPICS["Transfer"],
            _pad_address(ZERO_ADDRESS),  # from (mint)
            _pad_address(USER_ADDRESS),  # to
        ],
        "data": "0x" + transfer_data,
        "logIndex": 1,
    })

    if include_reserve_data_updated:
        logs.extend(_make_reserve_data_updated_logs(
            liquidity_rate_ray=liquidity_rate_ray,
        ))

    return {
        "transactionHash": "0x" + "cd" * 32,
        "blockNumber": 12345679,
        "status": 1,
        "logs": logs,
    }


def _make_repay_receipt(
    repay_amount: int = 500000000,  # 500 USDC
    include_debt_token_burn: bool = True,
    burn_amount: int | None = None,
    debt_token_address: str = DEBT_TOKEN_ADDRESS,
) -> dict[str, Any]:
    """Build a realistic Aave V3 Repay transaction receipt."""
    logs: list[dict] = []

    # Repay event
    # Repay(address indexed reserve, address indexed user, address indexed repayer,
    #       uint256 amount, bool useATokens)
    repay_data = (
        _encode_uint256(repay_amount)  # amount
        + _encode_uint256(0)  # useATokens = false
    )
    logs.append({
        "address": POOL_ADDRESS,
        "topics": [
            EVENT_TOPICS["Repay"],
            _pad_address(USDC_ADDRESS),  # reserve (indexed)
            _pad_address(USER_ADDRESS),  # user (indexed)
            _pad_address(USER_ADDRESS),  # repayer (indexed)
        ],
        "data": "0x" + repay_data,
        "logIndex": 0,
    })

    # Debt token burn (Transfer from user to 0x0)
    if include_debt_token_burn:
        actual_burn = burn_amount if burn_amount is not None else repay_amount
        transfer_data = _encode_uint256(actual_burn)
        logs.append({
            "address": debt_token_address,
            "topics": [
                EVENT_TOPICS["Transfer"],
                _pad_address(USER_ADDRESS),  # from (user)
                _pad_address(ZERO_ADDRESS),  # to (burn)
            ],
            "data": "0x" + transfer_data,
            "logIndex": 1,
        })

    return {
        "transactionHash": "0x" + "ef" * 32,
        "blockNumber": 12345680,
        "status": 1,
        "logs": logs,
    }


def _make_withdraw_receipt(
    withdraw_amount: int = 1000000000000000000,  # 1 WETH (18 decimals)
    include_atoken_burn: bool = True,
    burn_amount: int | None = None,
    atoken_address: str = ATOKEN_ADDRESS,
) -> dict[str, Any]:
    """Build a realistic Aave V3 Withdraw transaction receipt."""
    logs: list[dict] = []

    # Withdraw event
    # Withdraw(address indexed reserve, address indexed user, address indexed to, uint256 amount)
    withdraw_data = _encode_uint256(withdraw_amount)
    logs.append({
        "address": POOL_ADDRESS,
        "topics": [
            EVENT_TOPICS["Withdraw"],
            _pad_address(WETH_ADDRESS),   # reserve (indexed)
            _pad_address(USER_ADDRESS),   # user (indexed)
            _pad_address(USER_ADDRESS),   # to (indexed)
        ],
        "data": "0x" + withdraw_data,
        "logIndex": 0,
    })

    # aToken burn (Transfer from user to 0x0)
    if include_atoken_burn:
        actual_burn = burn_amount if burn_amount is not None else withdraw_amount
        transfer_data = _encode_uint256(actual_burn)
        logs.append({
            "address": atoken_address,
            "topics": [
                EVENT_TOPICS["Transfer"],
                _pad_address(USER_ADDRESS),   # from (user)
                _pad_address(ZERO_ADDRESS),   # to (burn)
            ],
            "data": "0x" + transfer_data,
            "logIndex": 1,
        })

    return {
        "transactionHash": "0x" + "12" * 32,
        "blockNumber": 12345681,
        "status": 1,
        "logs": logs,
    }


def _make_reserve_data_updated_logs(
    reserve: str = WETH_ADDRESS,
    liquidity_rate_ray: int = 35000000000000000000000000,  # 3.5% in ray
    stable_borrow_rate_ray: int = 60000000000000000000000000,  # 6% in ray
    variable_borrow_rate_ray: int = 50000000000000000000000000,  # 5% in ray
    liquidity_index_ray: int = 1000000000000000000000000000,  # 1.0 in ray
    variable_borrow_index_ray: int = 1000000000000000000000000000,  # 1.0 in ray
) -> list[dict]:
    """Create ReserveDataUpdated event logs."""
    data = (
        _encode_uint256(liquidity_rate_ray)
        + _encode_uint256(stable_borrow_rate_ray)
        + _encode_uint256(variable_borrow_rate_ray)
        + _encode_uint256(liquidity_index_ray)
        + _encode_uint256(variable_borrow_index_ray)
    )
    return [{
        "address": POOL_ADDRESS,
        "topics": [
            EVENT_TOPICS["ReserveDataUpdated"],
            _pad_address(reserve),  # reserve (indexed)
        ],
        "data": "0x" + data,
        "logIndex": 10,
    }]


# =============================================================================
# Tests: SUPPORTED_EXTRACTIONS
# =============================================================================


class TestSupportedExtractions:
    """Tests for SUPPORTED_EXTRACTIONS declaration."""

    def test_includes_debt_token(self) -> None:
        """debt_token should be in SUPPORTED_EXTRACTIONS."""
        assert "debt_token" in AaveV3ReceiptParser.SUPPORTED_EXTRACTIONS

    def test_includes_supply_rate(self) -> None:
        """supply_rate should be in SUPPORTED_EXTRACTIONS."""
        assert "supply_rate" in AaveV3ReceiptParser.SUPPORTED_EXTRACTIONS

    def test_includes_remaining_debt(self) -> None:
        """remaining_debt should be in SUPPORTED_EXTRACTIONS."""
        assert "remaining_debt" in AaveV3ReceiptParser.SUPPORTED_EXTRACTIONS

    def test_still_includes_original_fields(self) -> None:
        """Original extraction fields should still be present."""
        for field in ("supply_amount", "borrow_amount", "repay_amount", "borrow_rate", "a_token_received"):
            assert field in AaveV3ReceiptParser.SUPPORTED_EXTRACTIONS


# =============================================================================
# Tests: extract_debt_token
# =============================================================================


class TestExtractDebtToken:
    """Tests for debt token extraction from Borrow receipts."""

    def test_extracts_debt_token_from_mint_transfer(self) -> None:
        """Should find debt token address from Transfer mint event matching borrow amount."""
        parser = AaveV3ReceiptParser()
        receipt = _make_borrow_receipt(borrow_amount=1000000000)
        result = parser.extract_debt_token(receipt)
        assert result is not None
        assert result.lower() == DEBT_TOKEN_ADDRESS.lower()

    def test_returns_none_when_no_borrow_event(self) -> None:
        """Should return None if receipt has no Borrow event."""
        parser = AaveV3ReceiptParser()
        receipt = _make_supply_receipt()  # Supply, not Borrow
        result = parser.extract_debt_token(receipt)
        assert result is None

    def test_returns_none_when_no_mint_transfer(self) -> None:
        """Should return None if no debt token mint Transfer is found."""
        parser = AaveV3ReceiptParser()
        receipt = _make_borrow_receipt(include_debt_token_mint=False)
        result = parser.extract_debt_token(receipt)
        assert result is None

    def test_returns_none_for_empty_receipt(self) -> None:
        """Should return None for receipt with no logs."""
        parser = AaveV3ReceiptParser()
        receipt = {"transactionHash": "0x" + "00" * 32, "blockNumber": 1, "status": 1, "logs": []}
        result = parser.extract_debt_token(receipt)
        assert result is None


# =============================================================================
# Tests: extract_supply_rate
# =============================================================================


class TestExtractSupplyRate:
    """Tests for supply rate extraction from ReserveDataUpdated event."""

    def test_extracts_supply_rate_from_reserve_data_updated(self) -> None:
        """Should extract liquidity_rate from ReserveDataUpdated event."""
        parser = AaveV3ReceiptParser()
        # 3.5% APY = 0.035 (after dividing by 1e27)
        receipt = _make_supply_receipt(
            include_reserve_data_updated=True,
            liquidity_rate_ray=35000000000000000000000000,
        )
        result = parser.extract_supply_rate(receipt)
        assert result is not None
        assert isinstance(result, Decimal)
        assert Decimal("0.03") < result < Decimal("0.04")

    def test_returns_none_when_no_reserve_data_updated(self) -> None:
        """Should return None if no ReserveDataUpdated event in receipt."""
        parser = AaveV3ReceiptParser()
        receipt = _make_supply_receipt(include_reserve_data_updated=False)
        result = parser.extract_supply_rate(receipt)
        assert result is None

    def test_returns_none_for_empty_receipt(self) -> None:
        """Should return None for receipt with no logs."""
        parser = AaveV3ReceiptParser()
        receipt = {"transactionHash": "0x" + "00" * 32, "blockNumber": 1, "status": 1, "logs": []}
        result = parser.extract_supply_rate(receipt)
        assert result is None

    def test_supply_rate_with_borrow_receipt(self) -> None:
        """Should also extract supply rate from Borrow receipts with ReserveDataUpdated."""
        parser = AaveV3ReceiptParser()
        receipt = _make_borrow_receipt(include_reserve_data_updated=True)
        result = parser.extract_supply_rate(receipt)
        assert result is not None
        assert isinstance(result, Decimal)


# =============================================================================
# Tests: extract_remaining_debt
# =============================================================================


class TestExtractRemainingDebt:
    """Tests for remaining debt extraction from Repay receipts."""

    def test_returns_none_for_full_repay(self) -> None:
        """Should return None even for full repay (scaled amounts make receipt inference unreliable)."""
        parser = AaveV3ReceiptParser()
        receipt = _make_repay_receipt(repay_amount=500000000)
        result = parser.extract_remaining_debt(receipt)
        assert result is None

    def test_returns_none_when_burn_does_not_match(self) -> None:
        """Should return None when burn amount differs from repay (partial repay)."""
        parser = AaveV3ReceiptParser()
        receipt = _make_repay_receipt(
            repay_amount=500000000,
            include_debt_token_burn=True,
            burn_amount=300000000,  # Burn != repay
        )
        result = parser.extract_remaining_debt(receipt)
        assert result is None

    def test_returns_none_when_no_burn_event(self) -> None:
        """Should return None when no debt token burn Transfer is found."""
        parser = AaveV3ReceiptParser()
        receipt = _make_repay_receipt(include_debt_token_burn=False)
        result = parser.extract_remaining_debt(receipt)
        assert result is None

    def test_returns_none_when_no_repay_event(self) -> None:
        """Should return None if receipt has no Repay event."""
        parser = AaveV3ReceiptParser()
        receipt = _make_supply_receipt()  # Supply, not Repay
        result = parser.extract_remaining_debt(receipt)
        assert result is None

    def test_returns_none_for_empty_receipt(self) -> None:
        """Should return None for receipt with no logs."""
        parser = AaveV3ReceiptParser()
        receipt = {"transactionHash": "0x" + "00" * 32, "blockNumber": 1, "status": 1, "logs": []}
        result = parser.extract_remaining_debt(receipt)
        assert result is None


# =============================================================================
# Tests: Integration with existing extraction methods
# =============================================================================


class TestExistingExtractions:
    """Verify existing extraction methods still work after changes."""

    def test_extract_supply_amount(self) -> None:
        """extract_supply_amount should still work."""
        parser = AaveV3ReceiptParser()
        receipt = _make_supply_receipt(supply_amount=1000000000000000000)
        result = parser.extract_supply_amount(receipt)
        assert result == 1000000000000000000

    def test_extract_borrow_amount(self) -> None:
        """extract_borrow_amount should still work."""
        parser = AaveV3ReceiptParser()
        receipt = _make_borrow_receipt(borrow_amount=1000000000)
        result = parser.extract_borrow_amount(receipt)
        assert result == 1000000000

    def test_extract_repay_amount(self) -> None:
        """extract_repay_amount should still work."""
        parser = AaveV3ReceiptParser()
        receipt = _make_repay_receipt(repay_amount=500000000)
        result = parser.extract_repay_amount(receipt)
        assert result == 500000000

    def test_extract_borrow_rate(self) -> None:
        """extract_borrow_rate should still work."""
        parser = AaveV3ReceiptParser()
        receipt = _make_borrow_receipt(borrow_rate_ray=50000000000000000000000000)
        result = parser.extract_borrow_rate(receipt)
        assert result is not None
        assert Decimal("0.04") < result < Decimal("0.06")


# =============================================================================
# Tests: extract_supply_amounts (VIB-1585)
# =============================================================================


class TestExtractSupplyAmounts:
    """Tests for the aggregated supply_amounts extraction method."""

    def test_returns_supply_amounts_result(self) -> None:
        """Should return a SupplyAmountsResult with all fields populated."""
        parser = AaveV3ReceiptParser()
        receipt = _make_supply_receipt(
            supply_amount=1000000000000000000,
            include_reserve_data_updated=True,
            liquidity_rate_ray=35000000000000000000000000,
        )
        result = parser.extract_supply_amounts(receipt)
        assert result is not None
        assert isinstance(result, SupplyAmountsResult)
        assert result.supply_amount == 1000000000000000000
        assert result.a_token_received == 1000000000000000000
        assert result.supply_rate is not None
        assert Decimal("0.03") < result.supply_rate < Decimal("0.04")

    def test_returns_none_when_no_supply_event(self) -> None:
        """Should return None if receipt has no Supply event."""
        parser = AaveV3ReceiptParser()
        receipt = _make_borrow_receipt()
        result = parser.extract_supply_amounts(receipt)
        assert result is None

    def test_supply_amounts_without_reserve_data_updated(self) -> None:
        """Should return result with supply_rate=None when ReserveDataUpdated missing."""
        parser = AaveV3ReceiptParser()
        receipt = _make_supply_receipt(include_reserve_data_updated=False)
        result = parser.extract_supply_amounts(receipt)
        assert result is not None
        assert result.supply_amount == 1000000000000000000
        assert result.supply_rate is None

    def test_supply_amounts_in_supported_extractions(self) -> None:
        """supply_amounts should be declared in SUPPORTED_EXTRACTIONS."""
        assert "supply_amounts" in AaveV3ReceiptParser.SUPPORTED_EXTRACTIONS

    def test_returns_none_for_empty_receipt(self) -> None:
        """Should return None for receipt with no logs."""
        parser = AaveV3ReceiptParser()
        receipt = {"transactionHash": "0x" + "00" * 32, "blockNumber": 1, "status": 1, "logs": []}
        result = parser.extract_supply_amounts(receipt)
        assert result is None


# =============================================================================
# Tests: extract_a_token_burned (VIB-534)
# =============================================================================


class TestExtractATokenBurned:
    """Tests for aToken burn extraction from Withdraw receipts."""

    def test_extracts_atoken_burn_amount(self) -> None:
        """Should find aToken burn amount from Transfer-to-zero event."""
        parser = AaveV3ReceiptParser()
        receipt = _make_withdraw_receipt(withdraw_amount=1000000000000000000)
        result = parser.extract_a_token_burned(receipt)
        assert result is not None
        assert result == 1000000000000000000

    def test_returns_none_when_no_burn_event(self) -> None:
        """Should return None if no Transfer-to-zero event in receipt."""
        parser = AaveV3ReceiptParser()
        receipt = _make_withdraw_receipt(include_atoken_burn=False)
        result = parser.extract_a_token_burned(receipt)
        assert result is None

    def test_different_burn_amount_than_withdraw(self) -> None:
        """Should return the burn Transfer amount even if it differs from withdraw amount."""
        parser = AaveV3ReceiptParser()
        receipt = _make_withdraw_receipt(
            withdraw_amount=1000000000000000000,
            burn_amount=999999999999999999,  # Slightly less due to interest accrual
        )
        result = parser.extract_a_token_burned(receipt)
        assert result == 999999999999999999

    def test_returns_none_for_supply_receipt(self) -> None:
        """Should return None for supply receipts (Transfer-from-zero, not to)."""
        parser = AaveV3ReceiptParser()
        receipt = _make_supply_receipt()
        result = parser.extract_a_token_burned(receipt)
        assert result is None

    def test_returns_none_for_empty_receipt(self) -> None:
        """Should return None for receipt with no logs."""
        parser = AaveV3ReceiptParser()
        receipt = {"transactionHash": "0x" + "00" * 32, "blockNumber": 1, "status": 1, "logs": []}
        result = parser.extract_a_token_burned(receipt)
        assert result is None

    def test_a_token_burned_in_supported_extractions(self) -> None:
        """a_token_burned should be declared in SUPPORTED_EXTRACTIONS."""
        assert "a_token_burned" in AaveV3ReceiptParser.SUPPORTED_EXTRACTIONS


# =============================================================================
# Tests: extract_borrow_amounts (VIB-1641)
# =============================================================================


class TestExtractBorrowAmounts:
    """Tests for the aggregated borrow_amounts extraction method."""

    def test_returns_borrow_amounts_result(self) -> None:
        """Should return a BorrowAmountsResult with all fields populated."""
        parser = AaveV3ReceiptParser()
        receipt = _make_borrow_receipt(borrow_amount=1000000000)
        result = parser.extract_borrow_amounts(receipt)
        assert result is not None
        assert isinstance(result, BorrowAmountsResult)
        assert result.borrow_amount == 1000000000
        assert result.debt_token is not None
        assert result.debt_token.lower() == DEBT_TOKEN_ADDRESS.lower()

    def test_includes_borrow_rate(self) -> None:
        """Should include borrow_rate when available."""
        parser = AaveV3ReceiptParser()
        receipt = _make_borrow_receipt(borrow_rate_ray=50000000000000000000000000)
        result = parser.extract_borrow_amounts(receipt)
        assert result is not None
        assert result.borrow_rate is not None
        assert Decimal("0.04") < result.borrow_rate < Decimal("0.06")

    def test_returns_none_when_no_borrow_event(self) -> None:
        """Should return None if receipt has no Borrow event."""
        parser = AaveV3ReceiptParser()
        receipt = _make_supply_receipt()
        result = parser.extract_borrow_amounts(receipt)
        assert result is None

    def test_returns_none_for_empty_receipt(self) -> None:
        """Should return None for receipt with no logs."""
        parser = AaveV3ReceiptParser()
        receipt = {"transactionHash": "0x" + "00" * 32, "blockNumber": 1, "status": 1, "logs": []}
        result = parser.extract_borrow_amounts(receipt)
        assert result is None

    def test_borrow_amounts_in_supported_extractions(self) -> None:
        """borrow_amounts should be declared in SUPPORTED_EXTRACTIONS."""
        assert "borrow_amounts" in AaveV3ReceiptParser.SUPPORTED_EXTRACTIONS

    def test_to_dict(self) -> None:
        """BorrowAmountsResult.to_dict() should serialize correctly."""
        result = BorrowAmountsResult(
            borrow_amount=1000000000,
            borrow_rate=Decimal("0.05"),
            debt_token="0xDebt",
        )
        d = result.to_dict()
        assert d["borrow_amount"] == 1000000000
        assert d["borrow_rate"] == "0.05"
        assert d["debt_token"] == "0xDebt"


# =============================================================================
# Tests: extract_repay_amounts (VIB-1641)
# =============================================================================


class TestExtractRepayAmounts:
    """Tests for the aggregated repay_amounts extraction method."""

    def test_returns_repay_amounts_result(self) -> None:
        """Should return a RepayAmountsResult with repay_amount populated."""
        parser = AaveV3ReceiptParser()
        receipt = _make_repay_receipt(repay_amount=500000000)
        result = parser.extract_repay_amounts(receipt)
        assert result is not None
        assert isinstance(result, RepayAmountsResult)
        assert result.repay_amount == 500000000
        # remaining_debt is always None (requires on-chain query)
        assert result.remaining_debt is None

    def test_returns_none_when_no_repay_event(self) -> None:
        """Should return None if receipt has no Repay event."""
        parser = AaveV3ReceiptParser()
        receipt = _make_supply_receipt()
        result = parser.extract_repay_amounts(receipt)
        assert result is None

    def test_returns_none_for_empty_receipt(self) -> None:
        """Should return None for receipt with no logs."""
        parser = AaveV3ReceiptParser()
        receipt = {"transactionHash": "0x" + "00" * 32, "blockNumber": 1, "status": 1, "logs": []}
        result = parser.extract_repay_amounts(receipt)
        assert result is None

    def test_repay_amounts_in_supported_extractions(self) -> None:
        """repay_amounts should be declared in SUPPORTED_EXTRACTIONS."""
        assert "repay_amounts" in AaveV3ReceiptParser.SUPPORTED_EXTRACTIONS

    def test_to_dict(self) -> None:
        """RepayAmountsResult.to_dict() should serialize correctly."""
        result = RepayAmountsResult(repay_amount=500000000, remaining_debt=None)
        d = result.to_dict()
        assert d["repay_amount"] == 500000000
        assert d["remaining_debt"] is None


# =============================================================================
# Tests: extract_withdraw_amounts (VIB-1641)
# =============================================================================


class TestExtractWithdrawAmounts:
    """Tests for the aggregated withdraw_amounts extraction method."""

    def test_returns_withdraw_amounts_result(self) -> None:
        """Should return a WithdrawAmountsResult with all fields populated."""
        parser = AaveV3ReceiptParser()
        receipt = _make_withdraw_receipt(withdraw_amount=1000000000000000000)
        result = parser.extract_withdraw_amounts(receipt)
        assert result is not None
        assert isinstance(result, WithdrawAmountsResult)
        assert result.withdraw_amount == 1000000000000000000
        assert result.a_token_burned == 1000000000000000000

    def test_returns_none_when_no_withdraw_event(self) -> None:
        """Should return None if receipt has no Withdraw event."""
        parser = AaveV3ReceiptParser()
        receipt = _make_supply_receipt()
        result = parser.extract_withdraw_amounts(receipt)
        assert result is None

    def test_returns_none_for_empty_receipt(self) -> None:
        """Should return None for receipt with no logs."""
        parser = AaveV3ReceiptParser()
        receipt = {"transactionHash": "0x" + "00" * 32, "blockNumber": 1, "status": 1, "logs": []}
        result = parser.extract_withdraw_amounts(receipt)
        assert result is None

    def test_withdraw_amounts_without_atoken_burn(self) -> None:
        """Should return result with a_token_burned=None when burn event missing."""
        parser = AaveV3ReceiptParser()
        receipt = _make_withdraw_receipt(include_atoken_burn=False)
        result = parser.extract_withdraw_amounts(receipt)
        assert result is not None
        assert result.withdraw_amount == 1000000000000000000
        assert result.a_token_burned is None

    def test_withdraw_amounts_in_supported_extractions(self) -> None:
        """withdraw_amounts should be declared in SUPPORTED_EXTRACTIONS."""
        assert "withdraw_amounts" in AaveV3ReceiptParser.SUPPORTED_EXTRACTIONS

    def test_to_dict(self) -> None:
        """WithdrawAmountsResult.to_dict() should serialize correctly."""
        result = WithdrawAmountsResult(withdraw_amount=1000000000000000000, a_token_burned=999999999999999999)
        d = result.to_dict()
        assert d["withdraw_amount"] == 1000000000000000000
        assert d["a_token_burned"] == 999999999999999999


# =============================================================================
# Tests: RepayIntent interest_rate_mode (VIB-1640)
# =============================================================================


class TestRepayIntentInterestRateMode:
    """Tests for interest_rate_mode parameter on RepayIntent."""

    def test_repay_accepts_variable_interest_rate_mode(self) -> None:
        """Intent.repay() should accept interest_rate_mode='variable'."""
        from almanak.framework.intents.vocabulary import Intent

        intent = Intent.repay(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("500"),
            interest_rate_mode="variable",
        )
        assert intent.interest_rate_mode == "variable"

    def test_repay_rejects_stable_interest_rate_mode(self) -> None:
        """Intent.repay() should reject interest_rate_mode='stable' (deprecated on Aave V3)."""
        from pydantic import ValidationError

        from almanak.framework.intents.vocabulary import Intent

        with pytest.raises(ValidationError, match="Input should be 'variable'"):
            Intent.repay(
                protocol="aave_v3",
                token="USDC",
                amount=Decimal("500"),
                interest_rate_mode="stable",
            )

    def test_repay_defaults_to_none_interest_rate_mode(self) -> None:
        """Intent.repay() should default interest_rate_mode to None."""
        from almanak.framework.intents.vocabulary import Intent

        intent = Intent.repay(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("500"),
        )
        assert intent.interest_rate_mode is None

    def test_repay_rejects_invalid_interest_rate_mode(self) -> None:
        """Intent.repay() should reject invalid interest_rate_mode values."""
        from almanak.framework.intents.vocabulary import Intent

        with pytest.raises(ValueError):
            Intent.repay(
                protocol="aave_v3",
                token="USDC",
                amount=Decimal("500"),
                interest_rate_mode="fixed",  # type: ignore[arg-type]
            )

    def test_repay_rejects_interest_rate_mode_for_unsupported_protocol(self) -> None:
        """Morpho does not support interest_rate_mode."""
        from almanak.framework.intents.vocabulary import Intent

        with pytest.raises(ValueError, match="does not support interest rate mode"):
            Intent.repay(
                protocol="morpho_blue",
                token="USDC",
                amount=Decimal("500"),
                interest_rate_mode="variable",
                market_id="0x" + "ab" * 32,
            )

    def test_borrow_and_repay_api_symmetry(self) -> None:
        """BorrowIntent and RepayIntent should both accept interest_rate_mode."""
        from almanak.framework.intents.vocabulary import Intent

        borrow = Intent.borrow(
            protocol="aave_v3",
            collateral_token="WETH",
            collateral_amount=Decimal("1"),
            borrow_token="USDC",
            borrow_amount=Decimal("1000"),
            interest_rate_mode="variable",
        )
        repay = Intent.repay(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("500"),
            interest_rate_mode="variable",
        )
        assert borrow.interest_rate_mode == repay.interest_rate_mode == "variable"
