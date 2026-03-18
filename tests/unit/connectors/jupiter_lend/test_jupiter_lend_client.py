"""Tests for Jupiter Lend HTTP Client."""

from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.jupiter_lend.client import JupiterLendClient, JupiterLendConfig, U64_MAX
from almanak.framework.connectors.jupiter_lend.exceptions import JupiterLendAPIError, JupiterLendConfigError
from almanak.framework.connectors.jupiter_lend.models import JupiterLendTransactionResponse, JupiterLendVault

WALLET = "7nYBm5mW5Xr4iDxF8XfE3gVgCELphJ3TypDwQUmFjWLu"


class TestJupiterLendConfig:
    def test_valid_config(self):
        config = JupiterLendConfig(wallet_address=WALLET)
        assert config.wallet_address == WALLET
        assert "jup.ag" in config.base_url

    def test_empty_wallet_raises(self):
        with pytest.raises(JupiterLendConfigError):
            JupiterLendConfig(wallet_address="")

    def test_custom_base_url(self):
        config = JupiterLendConfig(wallet_address=WALLET, base_url="https://custom.api")
        assert config.base_url == "https://custom.api"


class TestJupiterLendClientVaults:
    def test_get_vaults_list(self):
        config = JupiterLendConfig(wallet_address=WALLET)
        client = JupiterLendClient(config)
        mock_response = [
            {
                "vaultAddress": "vault1addr",
                "name": "USDC Vault",
                "tokenSymbol": "USDC",
                "tokenMint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "maxLtv": "0.85",
                "supplyApy": "0.05",
                "borrowApy": "0.08",
            }
        ]
        with patch.object(client, "_make_request", return_value=mock_response):
            vaults = client.get_vaults()
            assert len(vaults) == 1
            assert vaults[0].token_symbol == "USDC"
            assert vaults[0].max_ltv == "0.85"

    def test_get_vaults_wrapped_response(self):
        config = JupiterLendConfig(wallet_address=WALLET)
        client = JupiterLendClient(config)
        mock_response = {
            "vaults": [
                {"vaultAddress": "vault1", "tokenSymbol": "SOL"},
            ]
        }
        with patch.object(client, "_make_request", return_value=mock_response):
            vaults = client.get_vaults()
            assert len(vaults) == 1

    def test_find_vault_by_token(self):
        config = JupiterLendConfig(wallet_address=WALLET)
        client = JupiterLendClient(config)
        mock_vaults = [
            {"vaultAddress": "vault1", "tokenSymbol": "USDC"},
            {"vaultAddress": "vault2", "tokenSymbol": "SOL"},
        ]
        with patch.object(client, "_make_request", return_value=mock_vaults):
            vault = client.find_vault_by_token("SOL")
            assert vault is not None
            assert vault.address == "vault2"

    def test_find_vault_not_found(self):
        config = JupiterLendConfig(wallet_address=WALLET)
        client = JupiterLendClient(config)
        with patch.object(client, "_make_request", return_value=[]):
            vault = client.find_vault_by_token("NONEXISTENT")
            assert vault is None


class TestJupiterLendClientTransactions:
    def test_deposit(self):
        config = JupiterLendConfig(wallet_address=WALLET)
        client = JupiterLendClient(config)
        mock_response = {"transaction": "AQAAAA=="}
        with patch.object(client, "_make_request", return_value=mock_response) as mock_req:
            tx = client.deposit(vault="vault1addr", amount="100.0")
            assert tx.transaction == "AQAAAA=="
            assert tx.action == "deposit"
            mock_req.assert_called_once_with(
                "POST",
                "/v1/deposit",
                json_data={"wallet": WALLET, "vault": "vault1addr", "amount": "100.0"},
            )

    def test_borrow(self):
        config = JupiterLendConfig(wallet_address=WALLET)
        client = JupiterLendClient(config)
        mock_response = {"transaction": "BQAAAA=="}
        with patch.object(client, "_make_request", return_value=mock_response) as mock_req:
            tx = client.borrow(vault="vault1addr", amount="50.0")
            assert tx.transaction == "BQAAAA=="
            assert tx.action == "borrow"

    def test_repay(self):
        config = JupiterLendConfig(wallet_address=WALLET)
        client = JupiterLendClient(config)
        mock_response = {"transaction": "CQAAAA=="}
        with patch.object(client, "_make_request", return_value=mock_response):
            tx = client.repay(vault="vault1addr", amount="50.0")
            assert tx.transaction == "CQAAAA=="
            assert tx.action == "repay"

    def test_withdraw(self):
        config = JupiterLendConfig(wallet_address=WALLET)
        client = JupiterLendClient(config)
        mock_response = {"transaction": "DQAAAA=="}
        with patch.object(client, "_make_request", return_value=mock_response):
            tx = client.withdraw(vault="vault1addr", amount=U64_MAX)
            assert tx.transaction == "DQAAAA=="
            assert tx.action == "withdraw"


class TestJupiterLendClientErrors:
    def test_api_error_includes_status_code(self):
        error = JupiterLendAPIError(
            message="Not Found",
            status_code=404,
            endpoint="/v1/deposit",
            error_code="VAULT_NOT_FOUND",
        )
        assert "404" in str(error)
        assert "VAULT_NOT_FOUND" in str(error)
