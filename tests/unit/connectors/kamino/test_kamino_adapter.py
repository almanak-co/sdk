"""Tests for KaminoAdapter intent compilation (VIB-370).

Verifies:
1. SupplyIntent compiles to deposit ActionBundle
2. BorrowIntent compiles to borrow ActionBundle
3. RepayIntent compiles to repay ActionBundle
4. WithdrawIntent compiles to withdraw ActionBundle (including withdraw_all)
5. Error handling for missing reserves and invalid amounts
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.kamino.adapter import KaminoAdapter
from almanak.framework.connectors.kamino.client import U64_MAX, KaminoConfig
from almanak.framework.connectors.kamino.models import KaminoReserve, KaminoTransactionResponse
from almanak.framework.intents.vocabulary import (
    BorrowIntent,
    IntentType,
    RepayIntent,
    SupplyIntent,
    WithdrawIntent,
)

WALLET = "KUMtRazMP7vwvc2kthnGZ9Cq6ZsGRiYC97snMYepNx9"
RESERVE_USDC = "D6q6wuQSrifJKZYpR1M8R4YawnLDtDsMmWM1NbBmgJ59"
RESERVE_SOL = "d4A2prbA2whesmvHaL88BH6Ewn5N4bTSU2Ze8P6Bc4Q"


def _mock_adapter():
    """Create a KaminoAdapter with mocked client."""
    config = KaminoConfig(wallet_address=WALLET)
    mock_resolver = MagicMock()
    adapter = KaminoAdapter(config=config, token_resolver=mock_resolver)
    return adapter


class TestCompileSupplyIntent:
    """KaminoAdapter.compile_supply_intent()."""

    def test_supply_compiles_to_deposit_bundle(self):
        adapter = _mock_adapter()
        intent = SupplyIntent(protocol="kamino", token="USDC", amount=Decimal("100"))

        # Mock reserve resolution and API call
        mock_reserve = KaminoReserve(address=RESERVE_USDC, token_symbol="USDC")
        with (
            patch.object(adapter.client, "find_reserve_by_token", return_value=mock_reserve),
            patch.object(
                adapter.client,
                "deposit",
                return_value=KaminoTransactionResponse(transaction="AQAAAA==", action="deposit"),
            ),
        ):
            bundle = adapter.compile_supply_intent(intent)

        assert bundle.intent_type == IntentType.SUPPLY.value
        assert len(bundle.transactions) == 1
        assert bundle.transactions[0]["serialized_transaction"] == "AQAAAA=="
        assert bundle.transactions[0]["tx_type"] == "deposit"
        assert bundle.metadata["protocol"] == "kamino"
        assert bundle.metadata["token"] == "USDC"
        assert bundle.metadata["amount"] == "100"

    def test_supply_all_amount_returns_error(self):
        adapter = _mock_adapter()
        intent = SupplyIntent(protocol="kamino", token="USDC", amount="all")

        bundle = adapter.compile_supply_intent(intent)

        assert bundle.transactions == []
        assert "must be resolved" in bundle.metadata["error"]


class TestCompileBorrowIntent:
    """KaminoAdapter.compile_borrow_intent()."""

    def test_borrow_compiles_to_borrow_bundle(self):
        adapter = _mock_adapter()
        intent = BorrowIntent(
            protocol="kamino",
            collateral_token="SOL",
            collateral_amount=Decimal("10"),
            borrow_token="USDC",
            borrow_amount=Decimal("500"),
        )

        mock_reserve = KaminoReserve(address=RESERVE_USDC, token_symbol="USDC")
        with (
            patch.object(adapter.client, "find_reserve_by_token", return_value=mock_reserve),
            patch.object(
                adapter.client,
                "borrow",
                return_value=KaminoTransactionResponse(transaction="BQAAAA==", action="borrow"),
            ),
        ):
            bundle = adapter.compile_borrow_intent(intent)

        assert bundle.intent_type == IntentType.BORROW.value
        assert len(bundle.transactions) == 1
        assert bundle.transactions[0]["serialized_transaction"] == "BQAAAA=="
        assert bundle.metadata["borrow_token"] == "USDC"
        assert bundle.metadata["borrow_amount"] == "500"


class TestCompileRepayIntent:
    """KaminoAdapter.compile_repay_intent()."""

    def test_repay_compiles_to_repay_bundle(self):
        adapter = _mock_adapter()
        intent = RepayIntent(protocol="kamino", token="USDC", amount=Decimal("250"))

        mock_reserve = KaminoReserve(address=RESERVE_USDC, token_symbol="USDC")
        with (
            patch.object(adapter.client, "find_reserve_by_token", return_value=mock_reserve),
            patch.object(
                adapter.client,
                "repay",
                return_value=KaminoTransactionResponse(transaction="RQAAAA==", action="repay"),
            ),
        ):
            bundle = adapter.compile_repay_intent(intent)

        assert bundle.intent_type == IntentType.REPAY.value
        assert len(bundle.transactions) == 1
        assert bundle.metadata["token"] == "USDC"
        assert bundle.metadata["amount"] == "250"


class TestCompileWithdrawIntent:
    """KaminoAdapter.compile_withdraw_intent()."""

    def test_withdraw_compiles_to_withdraw_bundle(self):
        adapter = _mock_adapter()
        intent = WithdrawIntent(protocol="kamino", token="USDC", amount=Decimal("50"))

        mock_reserve = KaminoReserve(address=RESERVE_USDC, token_symbol="USDC")
        with (
            patch.object(adapter.client, "find_reserve_by_token", return_value=mock_reserve),
            patch.object(
                adapter.client,
                "withdraw",
                return_value=KaminoTransactionResponse(transaction="WQAAAA==", action="withdraw"),
            ),
        ):
            bundle = adapter.compile_withdraw_intent(intent)

        assert bundle.intent_type == IntentType.WITHDRAW.value
        assert len(bundle.transactions) == 1
        assert bundle.metadata["withdraw_all"] is False
        assert bundle.metadata["amount"] == "50"

    def test_withdraw_all_uses_u64_max(self):
        adapter = _mock_adapter()
        intent = WithdrawIntent(protocol="kamino", token="USDC", amount=Decimal("1"), withdraw_all=True)

        mock_reserve = KaminoReserve(address=RESERVE_USDC, token_symbol="USDC")
        with (
            patch.object(adapter.client, "find_reserve_by_token", return_value=mock_reserve),
            patch.object(
                adapter.client,
                "withdraw",
                return_value=KaminoTransactionResponse(transaction="WMAX==", action="withdraw"),
            ) as mock_withdraw,
        ):
            bundle = adapter.compile_withdraw_intent(intent)

        assert bundle.metadata["withdraw_all"] is True
        assert bundle.metadata["amount"] == U64_MAX
        # Verify U64_MAX was passed to the client
        mock_withdraw.assert_called_once_with(reserve=RESERVE_USDC, amount=U64_MAX)


class TestReserveResolution:
    """KaminoAdapter reserve resolution."""

    def test_reserve_not_found_returns_error_bundle(self):
        adapter = _mock_adapter()
        intent = SupplyIntent(protocol="kamino", token="UNKNOWN", amount=Decimal("100"))

        with patch.object(adapter.client, "find_reserve_by_token", return_value=None):
            bundle = adapter.compile_supply_intent(intent)

        assert bundle.transactions == []
        assert "No Kamino reserve found" in bundle.metadata["error"]

    def test_reserve_cache_avoids_duplicate_api_calls(self):
        adapter = _mock_adapter()

        mock_reserve = KaminoReserve(address=RESERVE_USDC, token_symbol="USDC")

        with patch.object(adapter.client, "find_reserve_by_token", return_value=mock_reserve) as mock_find:
            # First call should query API
            addr1 = adapter._resolve_reserve("USDC")
            # Second call should use cache
            addr2 = adapter._resolve_reserve("USDC")

        assert addr1 == RESERVE_USDC
        assert addr2 == RESERVE_USDC
        mock_find.assert_called_once()  # Only one API call
