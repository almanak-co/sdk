"""Tests for Jupiter Lend Adapter."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.jupiter_lend.adapter import JupiterLendAdapter
from almanak.framework.connectors.jupiter_lend.client import JupiterLendConfig
from almanak.framework.connectors.jupiter_lend.models import JupiterLendTransactionResponse, JupiterLendVault
from almanak.framework.intents.vocabulary import BorrowIntent, IntentType, RepayIntent, SupplyIntent, WithdrawIntent

WALLET = "7nYBm5mW5Xr4iDxF8XfE3gVgCELphJ3TypDwQUmFjWLu"
MOCK_VAULT = JupiterLendVault(address="vaultUSDCaddr123", token_symbol="USDC")


def _make_adapter():
    config = JupiterLendConfig(wallet_address=WALLET)
    mock_resolver = MagicMock()
    # Return realistic decimals for Solana tokens
    mock_resolver.get_decimals.return_value = 6
    adapter = JupiterLendAdapter(config=config, token_resolver=mock_resolver)
    return adapter


class TestCompileSupplyIntent:
    def test_supply_produces_bundle(self):
        adapter = _make_adapter()
        intent = SupplyIntent(protocol="jupiter_lend", token="USDC", amount=Decimal("100"))

        with (
            patch.object(adapter.client, "find_vault_by_token", return_value=MOCK_VAULT),
            patch.object(
                adapter.client,
                "deposit",
                return_value=JupiterLendTransactionResponse(transaction="AQAAAA==", action="deposit"),
            ),
        ):
            bundle = adapter.compile_supply_intent(intent)
            assert bundle.intent_type == IntentType.SUPPLY.value
            assert len(bundle.transactions) == 1
            assert bundle.transactions[0]["serialized_transaction"] == "AQAAAA=="
            assert bundle.metadata["protocol"] == "jupiter_lend"
            assert bundle.metadata["vault"] == "vaultUSDCaddr123"

    def test_supply_all_returns_error(self):
        adapter = _make_adapter()
        intent = SupplyIntent(protocol="jupiter_lend", token="USDC", amount="all")
        bundle = adapter.compile_supply_intent(intent)
        assert bundle.metadata.get("error")
        assert "all" in bundle.metadata["error"]


class TestCompileBorrowIntent:
    def test_borrow_produces_bundle(self):
        adapter = _make_adapter()
        intent = BorrowIntent(
            protocol="jupiter_lend",
            collateral_token="SOL",
            collateral_amount=Decimal("10"),
            borrow_token="USDC",
            borrow_amount=Decimal("500"),
        )
        mock_vault = JupiterLendVault(address="vaultUSDCaddr123", token_symbol="USDC")

        with (
            patch.object(adapter.client, "find_vault_by_token", return_value=mock_vault),
            patch.object(
                adapter.client,
                "borrow",
                return_value=JupiterLendTransactionResponse(transaction="BQAAAA==", action="borrow"),
            ),
        ):
            bundle = adapter.compile_borrow_intent(intent)
            assert bundle.intent_type == IntentType.BORROW.value
            assert bundle.metadata["borrow_token"] == "USDC"


class TestCompileRepayIntent:
    def test_repay_produces_bundle(self):
        adapter = _make_adapter()
        intent = RepayIntent(protocol="jupiter_lend", token="USDC", amount=Decimal("50"))

        with (
            patch.object(adapter.client, "find_vault_by_token", return_value=MOCK_VAULT),
            patch.object(
                adapter.client,
                "repay",
                return_value=JupiterLendTransactionResponse(transaction="CQAAAA==", action="repay"),
            ),
        ):
            bundle = adapter.compile_repay_intent(intent)
            assert bundle.intent_type == IntentType.REPAY.value

    def test_repay_all_returns_error(self):
        adapter = _make_adapter()
        intent = RepayIntent(protocol="jupiter_lend", token="USDC", amount="all")
        bundle = adapter.compile_repay_intent(intent)
        assert bundle.metadata.get("error")


class TestCompileWithdrawIntent:
    def test_withdraw_produces_bundle(self):
        adapter = _make_adapter()
        intent = WithdrawIntent(protocol="jupiter_lend", token="USDC", amount=Decimal("50"))

        with (
            patch.object(adapter.client, "find_vault_by_token", return_value=MOCK_VAULT),
            patch.object(
                adapter.client,
                "withdraw",
                return_value=JupiterLendTransactionResponse(transaction="DQAAAA==", action="withdraw"),
            ),
        ):
            bundle = adapter.compile_withdraw_intent(intent)
            assert bundle.intent_type == IntentType.WITHDRAW.value

    def test_withdraw_all_flag(self):
        adapter = _make_adapter()
        intent = WithdrawIntent(protocol="jupiter_lend", token="USDC", amount=Decimal("0"), withdraw_all=True)

        with (
            patch.object(adapter.client, "find_vault_by_token", return_value=MOCK_VAULT),
            patch.object(
                adapter.client,
                "withdraw",
                return_value=JupiterLendTransactionResponse(transaction="DQAAAA==", action="withdraw"),
            ),
        ):
            bundle = adapter.compile_withdraw_intent(intent)
            assert bundle.metadata["withdraw_all"] is True
            assert bundle.metadata["amount"] == "18446744073709551615"


class TestVaultResolution:
    def test_vault_caching(self):
        adapter = _make_adapter()

        with patch.object(adapter.client, "find_vault_by_token", return_value=MOCK_VAULT) as mock_find:
            # First call should query API
            addr1 = adapter._resolve_vault("USDC")
            assert addr1 == "vaultUSDCaddr123"
            assert mock_find.call_count == 1

            # Second call should use cache
            addr2 = adapter._resolve_vault("USDC")
            assert addr2 == "vaultUSDCaddr123"
            assert mock_find.call_count == 1  # No additional API call

    def test_vault_not_found_raises(self):
        adapter = _make_adapter()

        with patch.object(adapter.client, "find_vault_by_token", return_value=None):
            with pytest.raises(ValueError, match="No Jupiter Lend vault found"):
                adapter._resolve_vault("NONEXISTENT")
