"""Unit tests for Drift models and constants."""

from decimal import Decimal

import pytest

from almanak.framework.connectors.drift.constants import (
    BASE_PRECISION,
    DIRECTION_LONG,
    DIRECTION_SHORT,
    DRIFT_PROGRAM_ID,
    ORDER_TYPE_MARKET,
    PERP_MARKET_SYMBOL_TO_INDEX,
    PERP_MARKETS,
    PRICE_PRECISION,
    QUOTE_PRECISION,
)
from almanak.framework.connectors.drift.exceptions import (
    DriftAccountNotFoundError,
    DriftAPIError,
    DriftConfigError,
    DriftError,
    DriftMarketError,
    DriftValidationError,
)
from almanak.framework.connectors.drift.models import (
    DriftConfig,
    DriftMarket,
    DriftPerpPosition,
    DriftSpotPosition,
    DriftUserAccount,
    FundingRate,
    OrderParams,
)


class TestDriftConfig:
    def test_valid_config(self):
        config = DriftConfig(wallet_address="test-wallet")
        assert config.wallet_address == "test-wallet"
        assert config.sub_account_id == 0
        assert config.timeout == 30

    def test_empty_wallet_raises(self):
        with pytest.raises(DriftConfigError, match="wallet_address"):
            DriftConfig(wallet_address="")

    def test_custom_config(self):
        config = DriftConfig(
            wallet_address="test-wallet",
            rpc_url="https://my-rpc.com",
            sub_account_id=1,
            timeout=60,
        )
        assert config.rpc_url == "https://my-rpc.com"
        assert config.sub_account_id == 1
        assert config.timeout == 60


class TestDriftMarket:
    def test_from_api_response(self):
        data = {
            "marketIndex": 0,
            "symbol": "SOL-PERP",
            "baseAssetSymbol": "SOL",
            "oraclePrice": "150.5",
            "lastFundingRate": "0.0001",
            "volume24h": "500000",
        }
        market = DriftMarket.from_api_response(data)
        assert market.market_index == 0
        assert market.symbol == "SOL-PERP"
        assert market.oracle_price == Decimal("150.5")

    def test_empty_response(self):
        market = DriftMarket.from_api_response({})
        assert market.market_index == 0
        assert market.symbol == ""


class TestOrderParams:
    def test_default_values(self):
        params = OrderParams()
        assert params.order_type == ORDER_TYPE_MARKET
        assert params.direction == DIRECTION_LONG
        assert params.base_asset_amount == 0
        assert params.reduce_only is False
        assert params.max_ts is None

    def test_custom_values(self):
        params = OrderParams(
            direction=DIRECTION_SHORT,
            base_asset_amount=1_000_000_000,
            market_index=2,
            reduce_only=True,
        )
        assert params.direction == DIRECTION_SHORT
        assert params.base_asset_amount == 1_000_000_000
        assert params.market_index == 2
        assert params.reduce_only is True


class TestDriftPerpPosition:
    def test_active_position(self):
        pos = DriftPerpPosition(
            market_index=0,
            base_asset_amount=1_000_000_000,
        )
        assert pos.is_active
        assert pos.is_long

    def test_inactive_position(self):
        pos = DriftPerpPosition(market_index=0, base_asset_amount=0)
        assert not pos.is_active

    def test_short_position(self):
        pos = DriftPerpPosition(
            market_index=0,
            base_asset_amount=-500_000_000,
        )
        assert pos.is_active
        assert not pos.is_long


class TestDriftUserAccount:
    def test_active_market_indexes(self):
        account = DriftUserAccount(
            perp_positions=[
                DriftPerpPosition(market_index=0, base_asset_amount=1_000_000_000),
                DriftPerpPosition(market_index=2, base_asset_amount=-500_000_000),
                DriftPerpPosition(market_index=1, base_asset_amount=0),
            ],
            spot_positions=[
                DriftSpotPosition(market_index=0, scaled_balance=1000),
                DriftSpotPosition(market_index=1, scaled_balance=0),
            ],
        )
        assert set(account.active_perp_market_indexes) == {0, 2}
        assert account.active_spot_market_indexes == [0]


class TestFundingRate:
    def test_from_api_response(self):
        data = {
            "ts": 1700000000,
            "fundingRate": "0.0001",
            "marketIndex": 0,
        }
        rate = FundingRate.from_api_response(data)
        assert rate.timestamp == 1700000000
        assert rate.funding_rate == Decimal("0.0001")
        assert rate.market_index == 0


class TestConstants:
    def test_program_id_format(self):
        assert len(DRIFT_PROGRAM_ID) > 30  # Base58 address
        assert DRIFT_PROGRAM_ID.startswith("dRifty")

    def test_market_indexes_unique(self):
        indexes = list(PERP_MARKETS.keys())
        assert len(indexes) == len(set(indexes))

    def test_reverse_mapping_consistent(self):
        for idx, symbol in PERP_MARKETS.items():
            assert PERP_MARKET_SYMBOL_TO_INDEX[symbol] == idx

    def test_precisions(self):
        assert BASE_PRECISION == 10**9
        assert PRICE_PRECISION == 10**6
        assert QUOTE_PRECISION == 10**6


class TestExceptions:
    def test_drift_error_hierarchy(self):
        assert issubclass(DriftAPIError, DriftError)
        assert issubclass(DriftConfigError, DriftError)
        assert issubclass(DriftValidationError, DriftError)
        assert issubclass(DriftAccountNotFoundError, DriftError)
        assert issubclass(DriftMarketError, DriftError)

    def test_api_error_str(self):
        err = DriftAPIError("Bad request", status_code=400, endpoint="/test")
        assert "400" in str(err)
        assert "Bad request" in str(err)

    def test_validation_error_str(self):
        err = DriftValidationError("Invalid", field="amount", value="abc")
        assert "Invalid" in str(err)
        assert "amount" in str(err)

    def test_config_error_str(self):
        err = DriftConfigError("Missing key", parameter="wallet_address")
        assert "wallet_address" in str(err)
