"""Branch coverage for KrakenSDK status/result polling.

Covers ``get_swap_status`` (viqc + base-volume modes), ``get_swap_result``
(direct/inverted markets, fee direction), ``get_withdrawal_status`` and the
API error mapping — all against mocked Kraken REST clients; no network.
"""

from decimal import Decimal
from unittest.mock import Mock

import pytest
from pydantic import SecretStr

from almanak.connectors.kraken.exceptions import (
    KrakenAPIError,
    KrakenAuthenticationError,
    KrakenInsufficientFundsError,
    KrakenMinimumOrderError,
    KrakenOrderNotFoundError,
    KrakenUnknownAssetError,
    KrakenUnknownPairError,
)
from almanak.connectors.kraken.models import KrakenCredentials
from almanak.connectors.kraken.sdk import KrakenSDK

TXID = "OABC12-DEF34-GHI56"
USERREF = 12345


@pytest.fixture
def sdk() -> KrakenSDK:
    sdk = KrakenSDK(
        credentials=KrakenCredentials(api_key=SecretStr("key"), api_secret=SecretStr("secret"))
    )
    sdk.user = Mock()
    sdk.market = Mock()
    sdk.trade = Mock()
    sdk.funding = Mock()
    return sdk


def _orders(status: str, *, oflags: str = "fciq", vol_exec: str = "0", vol: str = "1", cost: str = "0"):
    return {
        TXID: {
            "status": status,
            "oflags": oflags,
            "vol_exec": vol_exec,
            "vol": vol,
            "cost": cost,
        }
    }


class TestGetSwapStatus:
    def test_order_not_found_raises(self, sdk):
        sdk.user.get_orders_info.return_value = {}
        with pytest.raises(KrakenOrderNotFoundError):
            sdk.get_swap_status(TXID, USERREF)

    def test_api_error_is_mapped(self, sdk):
        sdk.user.get_orders_info.side_effect = Exception("EAPI:Invalid key")
        with pytest.raises(KrakenAuthenticationError):
            sdk.get_swap_status(TXID, USERREF)

    @pytest.mark.parametrize(
        ("order_kwargs", "expected"),
        [
            ({"status": "pending"}, "pending"),
            ({"status": "open"}, "pending"),
            ({"status": "closed", "cost": "100"}, "success"),
            ({"status": "closed", "cost": "0"}, "failed"),
            ({"status": "canceled", "cost": "0"}, "cancelled"),
            ({"status": "expired", "cost": "50"}, "success"),
            ({"status": "weird"}, "unknown"),
        ],
    )
    def test_volume_in_quote_mode(self, sdk, order_kwargs, expected):
        sdk.user.get_orders_info.return_value = _orders(oflags="fciq,viqc", **order_kwargs)
        assert sdk.get_swap_status(TXID, USERREF) == expected

    @pytest.mark.parametrize(
        ("order_kwargs", "expected"),
        [
            ({"status": "pending"}, "pending"),
            ({"status": "open"}, "pending"),
            ({"status": "closed", "vol_exec": "0", "vol": "1"}, "failed"),
            ({"status": "closed", "vol_exec": "1", "vol": "1"}, "success"),
            ({"status": "closed", "vol_exec": "0.5", "vol": "1"}, "partial"),
            ({"status": "closed", "vol_exec": "2", "vol": "1"}, "unknown"),
            ({"status": "canceled", "vol_exec": "0"}, "cancelled"),
            ({"status": "expired", "vol_exec": "0.5"}, "partial"),
            ({"status": "weird"}, "unknown"),
        ],
    )
    def test_volume_in_base_mode(self, sdk, order_kwargs, expected):
        sdk.user.get_orders_info.return_value = _orders(**order_kwargs)
        assert sdk.get_swap_status(TXID, USERREF) == expected


def _result_order(
    *,
    side: str = "sell",
    oflags: str = "fciq",
    vol_exec: str = "2",
    cost: str = "6000",
    fee: str = "6",
    price: str = "3000",
):
    return {
        TXID: {
            "status": "closed",
            "oflags": oflags,
            "vol_exec": vol_exec,
            "vol": vol_exec,
            "cost": cost,
            "fee": fee,
            "price": price,
            "closetm": 1700000000,
            "descr": {"type": side},
        }
    }


class TestGetSwapResult:
    def test_order_not_found_raises(self, sdk):
        sdk.user.get_orders_info.return_value = {}
        with pytest.raises(KrakenOrderNotFoundError):
            sdk.get_swap_result(TXID, USERREF, "ETH", "USDC", 18, 6)

    def test_direct_sell_fee_in_quote(self, sdk, monkeypatch):
        sdk.user.get_orders_info.return_value = _result_order(side="sell")
        monkeypatch.setattr(sdk, "is_market_inverted", lambda *a, **k: False)
        result = sdk.get_swap_result(TXID, USERREF, "ETH", "USDC", 18, 6)
        assert result["amount_in"] == 2 * 10**18
        # 6000 USDC cost minus 6 USDC fee
        assert result["amount_out"] == 5994 * 10**6
        assert result["fee"] == 6 * 10**6
        assert result["fee_asset"] == "USDC"
        assert result["average_price"] == Decimal("3000")
        assert result["timestamp"] == 1700000000

    def test_inverted_buy_fee_in_quote(self, sdk, monkeypatch):
        sdk.user.get_orders_info.return_value = _result_order(side="buy")
        monkeypatch.setattr(sdk, "is_market_inverted", lambda *a, **k: True)
        result = sdk.get_swap_result(TXID, USERREF, "USDC", "ETH", 6, 18)
        # asset_in is the quote: 6000 USDC cost plus 6 USDC fee
        assert result["amount_in"] == 6006 * 10**6
        assert result["amount_out"] == 2 * 10**18
        assert result["fee_asset"] == "USDC"
        assert result["average_price"] == Decimal("1") / Decimal("3000")

    def test_fee_in_base_leaves_quote_untouched(self, sdk, monkeypatch):
        sdk.user.get_orders_info.return_value = _result_order(side="sell", oflags="fcib")
        monkeypatch.setattr(sdk, "is_market_inverted", lambda *a, **k: False)
        result = sdk.get_swap_result(TXID, USERREF, "ETH", "USDC", 18, 6)
        assert result["amount_out"] == 6000 * 10**6
        assert result["fee"] == 6 * 10**18
        assert result["fee_asset"] == "ETH"

    def test_zero_price_inverted_returns_zero(self, sdk, monkeypatch):
        sdk.user.get_orders_info.return_value = _result_order(side="buy", price="0")
        monkeypatch.setattr(sdk, "is_market_inverted", lambda *a, **k: True)
        result = sdk.get_swap_result(TXID, USERREF, "USDC", "ETH", 6, 18)
        assert result["average_price"] == Decimal("0")


def _withdrawal(status: str, *, refid: str = "REF-1", txid: str | None = "0xhash"):
    return {"refid": refid, "txid": txid, "status": status}


class TestGetWithdrawalStatus:
    @pytest.fixture(autouse=True)
    def _stub_resolution(self, sdk):
        sdk.token_resolver = Mock()
        sdk.token_resolver.to_kraken_symbol.return_value = "USDC"
        sdk.chain_mapper = Mock()
        sdk.chain_mapper.get_withdraw_method.return_value = "Ethereum"

    def test_requires_refid_or_tx_hash(self, sdk):
        with pytest.raises(ValueError, match="either refid or tx_hash"):
            sdk.get_withdrawal_status("USDC", "ethereum")

    def test_usdce_maps_to_usdc(self, sdk):
        sdk.token_resolver.to_kraken_symbol.return_value = "USDC.e"
        sdk.funding.get_recent_withdraw_status.return_value = []
        sdk.get_withdrawal_status("USDC.e", "arbitrum", refid="REF-1")
        _, kwargs = sdk.funding.get_recent_withdraw_status.call_args
        assert kwargs["asset"] == "USDC"

    @pytest.mark.parametrize(
        ("kraken_status", "expected"),
        [
            ("Success", "success"),
            ("Failure", "failed"),
            ("Initial", "pending"),
            ("Pending", "pending"),
            ("Settled", "pending"),
            ("Anything-Else", "unknown"),
        ],
    )
    def test_status_mapping_by_refid(self, sdk, kraken_status, expected):
        sdk.funding.get_recent_withdraw_status.return_value = [_withdrawal(kraken_status)]
        assert sdk.get_withdrawal_status("USDC", "ethereum", refid="REF-1") == expected

    def test_match_by_tx_hash(self, sdk):
        sdk.funding.get_recent_withdraw_status.return_value = [_withdrawal("Success")]
        assert sdk.get_withdrawal_status("USDC", "ethereum", tx_hash="0xhash") == "success"

    def test_no_match_returns_none(self, sdk):
        sdk.funding.get_recent_withdraw_status.return_value = [_withdrawal("Success")]
        assert sdk.get_withdrawal_status("USDC", "ethereum", refid="OTHER") is None

    def test_api_error_is_mapped(self, sdk):
        sdk.funding.get_recent_withdraw_status.side_effect = Exception("EFunding:Unknown asset")
        with pytest.raises(KrakenUnknownAssetError):
            sdk.get_withdrawal_status("USDC", "ethereum", refid="REF-1")


class TestGetDepositStatus:
    @pytest.fixture(autouse=True)
    def _stub_resolution(self, sdk):
        sdk.token_resolver = Mock()
        sdk.token_resolver.to_kraken_symbol.return_value = "USDC.e"
        sdk.chain_mapper = Mock()
        sdk.chain_mapper.get_deposit_method.return_value = "Arbitrum One"

    def test_without_asset_filter_queries_all(self, sdk):
        sdk.funding.get_recent_deposits_status.return_value = []
        assert sdk.get_deposit_status("0xhash") is None
        _, kwargs = sdk.funding.get_recent_deposits_status.call_args
        assert kwargs == {"asset": None, "method": None}

    def test_usdce_maps_to_usdc(self, sdk):
        sdk.funding.get_recent_deposits_status.return_value = []
        sdk.get_deposit_status("0xhash", asset="USDC.e", chain="arbitrum")
        _, kwargs = sdk.funding.get_recent_deposits_status.call_args
        assert kwargs == {"asset": "USDC", "method": "Arbitrum One"}

    @pytest.mark.parametrize(
        ("kraken_status", "expected"),
        [
            ("Success", "success"),
            ("Failure", "failed"),
            ("Pending", "pending"),
            ("Settled", "pending"),
            ("Anything-Else", "unknown"),
        ],
    )
    def test_status_mapping(self, sdk, kraken_status, expected):
        sdk.funding.get_recent_deposits_status.return_value = [
            {"txid": "0xhash", "status": kraken_status}
        ]
        assert sdk.get_deposit_status("0xhash") == expected

    def test_unmatched_tx_hash_returns_none(self, sdk):
        sdk.funding.get_recent_deposits_status.return_value = [
            {"txid": "0xother", "status": "Success"}
        ]
        assert sdk.get_deposit_status("0xhash") is None

    def test_api_error_is_mapped(self, sdk):
        sdk.funding.get_recent_deposits_status.side_effect = Exception("boom")
        with pytest.raises(KrakenAPIError):
            sdk.get_deposit_status("0xhash")


class TestGetBalances:
    @pytest.fixture(autouse=True)
    def _stub_resolution(self, sdk):
        sdk.token_resolver = Mock()
        sdk.token_resolver.to_kraken_symbol.side_effect = lambda chain, asset: {
            "ETH": "XETH",
            "BTC": "XXBT",
            "USDC": "USDC",
        }[asset]

    def test_matches_prefixed_kraken_keys(self, sdk):
        sdk.user.get_balances.return_value = {
            "XETH": {"balance": "2.5", "hold_trade": "0.5"},
            "USDC": {"balance": "100"},
        }
        balances = sdk.get_balances(["ETH", "USDC"])
        assert balances["ETH"].total == Decimal("2.5")
        assert balances["ETH"].available == Decimal("2.0")
        assert balances["USDC"].total == Decimal("100")

    def test_missing_asset_returns_zero_balance(self, sdk):
        sdk.user.get_balances.return_value = {"XETH": {"balance": "1"}}
        balances = sdk.get_balances(["BTC"])
        assert balances["BTC"].total == Decimal("0")
        assert balances["BTC"].available == Decimal("0")

    def test_get_balance_returns_single_asset(self, sdk):
        sdk.user.get_balances.return_value = {"XETH": {"balance": "1"}}
        assert sdk.get_balance("ETH").total == Decimal("1")

    def test_api_error_is_mapped(self, sdk):
        sdk.user.get_balances.side_effect = Exception("EAPI:Invalid key")
        with pytest.raises(KrakenAuthenticationError):
            sdk.get_balances(["ETH"])

    def test_all_balances_filters_zero(self, sdk):
        sdk.user.get_balances.return_value = {
            "XETH": {"balance": "1"},
            "USDC": {"balance": "0"},
        }
        balances = sdk.get_all_balances()
        assert set(balances) == {"XETH"}


def _market_info(*, lot_decimals: int = 8, min_base: int = 10**16, min_cost: int = 10**6):
    info = Mock()
    info.lot_decimals = lot_decimals
    info.get_min_order_base.return_value = min_base
    info.get_min_cost_quote.return_value = min_cost
    return info


class TestValidateSwapAmount:
    def _wire(self, sdk, monkeypatch, *, inverted=False, info=None, available=Decimal("10")):
        monkeypatch.setattr(sdk, "is_market_inverted", lambda *a, **k: inverted)
        monkeypatch.setattr(sdk, "get_market_info", lambda *a, **k: info or _market_info())
        balance = Mock()
        balance.available = available
        monkeypatch.setattr(sdk, "get_balance", lambda *a, **k: balance)

    def test_floors_to_lot_precision(self, sdk, monkeypatch):
        self._wire(sdk, monkeypatch)
        result = sdk.validate_swap_amount("ETH", "USDC", 1234567890123456789, 18)
        assert result == 1234567890000000000

    def test_below_minimum_base_raises(self, sdk, monkeypatch):
        self._wire(sdk, monkeypatch, info=_market_info(min_base=10**18))
        with pytest.raises(KrakenMinimumOrderError):
            sdk.validate_swap_amount("ETH", "USDC", 10**16, 18)

    def test_inverted_quote_input_checks_min_cost(self, sdk, monkeypatch):
        self._wire(sdk, monkeypatch, inverted=True, info=_market_info(min_cost=100 * 10**6))
        with pytest.raises(KrakenMinimumOrderError, match="minimum cost"):
            sdk.validate_swap_amount("USDC", "ETH", 50 * 10**6, 6)

    def test_insufficient_balance_raises(self, sdk, monkeypatch):
        self._wire(sdk, monkeypatch, available=Decimal("0.5"))
        with pytest.raises(KrakenInsufficientFundsError):
            sdk.validate_swap_amount("ETH", "USDC", 10**18, 18)

    def test_valid_amount_passes(self, sdk, monkeypatch):
        self._wire(sdk, monkeypatch)
        assert sdk.validate_swap_amount("ETH", "USDC", 10**18, 18) == 10**18


class TestHandleApiError:
    @pytest.mark.parametrize(
        ("message", "expected"),
        [
            ("EAPI:Invalid key", KrakenAuthenticationError),
            ("EGeneral:Invalid arguments", KrakenAPIError),
            ("EFunding:Unknown asset", KrakenUnknownAssetError),
            ("EQuery:Unknown asset pair", KrakenUnknownPairError),
            ("Unknown asset in pair xyz", KrakenUnknownPairError),
            ("EOrder:Insufficient funds", KrakenInsufficientFundsError),
            ("ESomething:else entirely", KrakenAPIError),
        ],
    )
    def test_error_mapping(self, sdk, message, expected):
        with pytest.raises(expected):
            sdk._handle_api_error(Exception(message), "op")
