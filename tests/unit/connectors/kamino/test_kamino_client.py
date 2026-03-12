"""Tests for KaminoClient HTTP client (VIB-370).

Verifies:
1. Client initialization and config validation
2. Market listing API
3. Reserve listing API
4. Deposit/borrow/repay/withdraw transaction building
5. Error handling for API failures
"""

from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.kamino.client import (
    KAMINO_MAIN_MARKET,
    U64_MAX,
    KaminoClient,
    KaminoConfig,
)
from almanak.framework.connectors.kamino.exceptions import KaminoAPIError, KaminoConfigError
from almanak.framework.connectors.kamino.models import KaminoMarket, KaminoReserve, KaminoTransactionResponse

WALLET = "KUMtRazMP7vwvc2kthnGZ9Cq6ZsGRiYC97snMYepNx9"
RESERVE_USDC = "D6q6wuQSrifJKZYpR1M8R4YawnLDtDsMmWM1NbBmgJ59"


class TestKaminoConfig:
    """KaminoConfig validation."""

    def test_valid_config(self):
        config = KaminoConfig(wallet_address=WALLET)
        assert config.wallet_address == WALLET
        assert config.base_url == "https://api.kamino.finance"
        assert config.market == KAMINO_MAIN_MARKET

    def test_empty_wallet_raises(self):
        with pytest.raises(KaminoConfigError, match="wallet_address"):
            KaminoConfig(wallet_address="")

    def test_custom_base_url(self):
        config = KaminoConfig(wallet_address=WALLET, base_url="http://localhost:3000")
        assert config.base_url == "http://localhost:3000"


class TestKaminoClientMarkets:
    """KaminoClient.get_markets()."""

    def test_get_markets_returns_list(self):
        config = KaminoConfig(wallet_address=WALLET)
        client = KaminoClient(config)

        mock_response = [
            {
                "lendingMarket": "7u3HeHxYDLhnCoErrtycNokbQYbWGzLs6JSDqGAv5PfF",
                "name": "Main Market",
                "isPrimary": True,
                "lookupTable": "FGMSBiyVE8TvZcdQnZETAAKw28tkQJ2ccZy6pyp95URb",
            }
        ]

        with patch.object(client, "_make_request", return_value=mock_response):
            markets = client.get_markets()

        assert len(markets) == 1
        assert isinstance(markets[0], KaminoMarket)
        assert markets[0].name == "Main Market"
        assert markets[0].is_primary is True


class TestKaminoClientReserves:
    """KaminoClient.get_reserves()."""

    def test_get_reserves_returns_list(self):
        config = KaminoConfig(wallet_address=WALLET)
        client = KaminoClient(config)

        mock_response = [
            {
                "reserve": RESERVE_USDC,
                "liquidityToken": "USDC",
                "liquidityTokenMint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "maxLtv": "0.8",
                "borrowApy": "0.03",
                "supplyApy": "0.015",
                "totalSupply": "250000000",
                "totalBorrow": "160000000",
                "totalSupplyUsd": "250000000",
                "totalBorrowUsd": "160000000",
            }
        ]

        with patch.object(client, "_make_request", return_value=mock_response):
            reserves = client.get_reserves()

        assert len(reserves) == 1
        assert isinstance(reserves[0], KaminoReserve)
        assert reserves[0].token_symbol == "USDC"
        assert reserves[0].max_ltv == "0.8"

    def test_find_reserve_by_token(self):
        config = KaminoConfig(wallet_address=WALLET)
        client = KaminoClient(config)

        mock_reserves = [
            {"reserve": "sol_reserve", "liquidityToken": "SOL", "liquidityTokenMint": "So1..."},
            {"reserve": RESERVE_USDC, "liquidityToken": "USDC", "liquidityTokenMint": "EPj..."},
        ]

        with patch.object(client, "_make_request", return_value=mock_reserves):
            reserve = client.find_reserve_by_token("USDC")

        assert reserve is not None
        assert reserve.address == RESERVE_USDC

    def test_find_reserve_by_token_not_found(self):
        config = KaminoConfig(wallet_address=WALLET)
        client = KaminoClient(config)

        with patch.object(client, "_make_request", return_value=[]):
            reserve = client.find_reserve_by_token("UNKNOWN")

        assert reserve is None


class TestKaminoClientTransactions:
    """KaminoClient transaction building."""

    def test_deposit_returns_transaction(self):
        config = KaminoConfig(wallet_address=WALLET)
        client = KaminoClient(config)

        mock_response = {"transaction": "AQAAAA=="}

        with patch.object(client, "_make_request", return_value=mock_response) as mock_req:
            tx = client.deposit(reserve=RESERVE_USDC, amount="100.0")

        assert isinstance(tx, KaminoTransactionResponse)
        assert tx.transaction == "AQAAAA=="
        assert tx.action == "deposit"

        # Verify correct API call
        mock_req.assert_called_once_with(
            "POST",
            "/ktx/klend/deposit",
            json_data={
                "wallet": WALLET,
                "market": KAMINO_MAIN_MARKET,
                "reserve": RESERVE_USDC,
                "amount": "100.0",
            },
        )

    def test_borrow_returns_transaction(self):
        config = KaminoConfig(wallet_address=WALLET)
        client = KaminoClient(config)

        with patch.object(client, "_make_request", return_value={"transaction": "BQAAAA=="}) as mock_req:
            tx = client.borrow(reserve=RESERVE_USDC, amount="50.0")

        assert tx.transaction == "BQAAAA=="
        assert tx.action == "borrow"
        mock_req.assert_called_once_with(
            "POST",
            "/ktx/klend/borrow",
            json_data={
                "wallet": WALLET,
                "market": KAMINO_MAIN_MARKET,
                "reserve": RESERVE_USDC,
                "amount": "50.0",
            },
        )

    def test_repay_returns_transaction(self):
        config = KaminoConfig(wallet_address=WALLET)
        client = KaminoClient(config)

        with patch.object(client, "_make_request", return_value={"transaction": "RQAAAA=="}) as mock_req:
            tx = client.repay(reserve=RESERVE_USDC, amount="50.0")

        assert tx.transaction == "RQAAAA=="
        assert tx.action == "repay"
        mock_req.assert_called_once_with(
            "POST",
            "/ktx/klend/repay",
            json_data={
                "wallet": WALLET,
                "market": KAMINO_MAIN_MARKET,
                "reserve": RESERVE_USDC,
                "amount": "50.0",
            },
        )

    def test_withdraw_returns_transaction(self):
        config = KaminoConfig(wallet_address=WALLET)
        client = KaminoClient(config)

        with patch.object(client, "_make_request", return_value={"transaction": "WQAAAA=="}) as mock_req:
            tx = client.withdraw(reserve=RESERVE_USDC, amount="100.0")

        assert tx.transaction == "WQAAAA=="
        assert tx.action == "withdraw"
        mock_req.assert_called_once_with(
            "POST",
            "/ktx/klend/withdraw",
            json_data={
                "wallet": WALLET,
                "market": KAMINO_MAIN_MARKET,
                "reserve": RESERVE_USDC,
                "amount": "100.0",
            },
        )

    def test_withdraw_all_uses_u64_max(self):
        config = KaminoConfig(wallet_address=WALLET)
        client = KaminoClient(config)

        with patch.object(client, "_make_request", return_value={"transaction": "WMAX=="}) as mock_req:
            tx = client.withdraw(reserve=RESERVE_USDC, amount=U64_MAX)

        assert tx.transaction == "WMAX=="
        call_args = mock_req.call_args
        assert call_args[1]["json_data"]["amount"] == U64_MAX


class TestKaminoClientErrors:
    """KaminoClient error handling."""

    def test_api_error_includes_error_code(self):
        config = KaminoConfig(wallet_address=WALLET)
        client = KaminoClient(config)

        import requests as req_lib

        mock_resp = MagicMock()
        mock_resp.status_code = 400
        mock_resp.json.return_value = {
            "statusCode": 400,
            "message": "Reserve not found",
            "code": "KLEND_RESERVE_NOT_FOUND",
        }
        mock_resp.raise_for_status.side_effect = req_lib.exceptions.HTTPError(response=mock_resp)

        with patch.object(client.session, "request", return_value=mock_resp):
            with pytest.raises(KaminoAPIError) as exc_info:
                client.deposit(reserve="invalid", amount="100")

        assert exc_info.value.error_code == "KLEND_RESERVE_NOT_FOUND"
        assert exc_info.value.status_code == 400
