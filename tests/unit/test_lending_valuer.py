"""Tests for lending valuation: pure math, hex parsing, and portfolio integration.

Covers:
- lending_valuer.py: value_lending_position, value_lending_portfolio
- lending_position_reader.py: hex parsing, reader with/without gateway
- portfolio_valuer.py: lending position repricing integration
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.valuation.lending_valuer import (
    LendingPositionValue,
    value_lending_portfolio,
    value_lending_position,
)
from almanak.framework.valuation.lending_position_reader import (
    LendingPositionOnChain,
    LendingPositionReader,
    _decode_uint_hex,
    _pad_address,
    _parse_user_reserve_data_hex,
)


# =============================================================================
# TestValueLendingPosition — pure math
# =============================================================================


class TestValueLendingPosition:
    """Pure math tests for value_lending_position."""

    def test_supply_only(self):
        """Position with supply, no debt."""
        result = value_lending_position(
            atoken_balance=1_500_000_000,  # 1500 USDC (6 decimals)
            stable_debt=0,
            variable_debt=0,
            token_price_usd=Decimal("1.0"),
            token_decimals=6,
            collateral_enabled=True,
            asset="USDC",
        )
        assert result.supply_balance == Decimal("1500")
        assert result.supply_value_usd == Decimal("1500")
        assert result.debt_value_usd == Decimal("0")
        assert result.net_value_usd == Decimal("1500")
        assert result.collateral_enabled is True

    def test_borrow_only(self):
        """Position with only variable debt, no supply."""
        result = value_lending_position(
            atoken_balance=0,
            stable_debt=0,
            variable_debt=500_000_000_000_000_000,  # 0.5 WETH (18 decimals)
            token_price_usd=Decimal("3000"),
            token_decimals=18,
            asset="WETH",
        )
        assert result.supply_balance == Decimal("0")
        assert result.supply_value_usd == Decimal("0")
        assert result.variable_debt_balance == Decimal("0.5")
        assert result.debt_value_usd == Decimal("1500")
        assert result.net_value_usd == Decimal("-1500")

    def test_supply_and_borrow(self):
        """Typical Aave position: supply collateral, borrow against it."""
        result = value_lending_position(
            atoken_balance=1_000_000_000_000_000_000,  # 1 WETH supplied
            stable_debt=0,
            variable_debt=500_000_000_000_000_000,  # 0.5 WETH borrowed
            token_price_usd=Decimal("3000"),
            token_decimals=18,
            asset="WETH",
        )
        assert result.supply_value_usd == Decimal("3000")
        assert result.debt_value_usd == Decimal("1500")
        assert result.net_value_usd == Decimal("1500")

    def test_stable_and_variable_debt(self):
        """Position with both stable and variable debt."""
        result = value_lending_position(
            atoken_balance=10_000_000_000,  # 10000 USDC supplied
            stable_debt=2_000_000_000,  # 2000 USDC stable debt
            variable_debt=3_000_000_000,  # 3000 USDC variable debt
            token_price_usd=Decimal("1.0"),
            token_decimals=6,
            asset="USDC",
        )
        assert result.stable_debt_balance == Decimal("2000")
        assert result.variable_debt_balance == Decimal("3000")
        assert result.debt_value_usd == Decimal("5000")
        assert result.net_value_usd == Decimal("5000")  # 10000 - 5000

    def test_zero_everything(self):
        """Empty position."""
        result = value_lending_position(
            atoken_balance=0,
            stable_debt=0,
            variable_debt=0,
            token_price_usd=Decimal("3000"),
            token_decimals=18,
        )
        assert result.net_value_usd == Decimal("0")
        assert result.supply_value_usd == Decimal("0")
        assert result.debt_value_usd == Decimal("0")

    def test_wbtc_8_decimals(self):
        """WBTC with 8 decimals."""
        result = value_lending_position(
            atoken_balance=50_000_000,  # 0.5 WBTC
            stable_debt=0,
            variable_debt=0,
            token_price_usd=Decimal("60000"),
            token_decimals=8,
            asset="WBTC",
        )
        assert result.supply_balance == Decimal("0.5")
        assert result.supply_value_usd == Decimal("30000")

    def test_underwater_position(self):
        """Net value is negative when debt > supply (same asset borrow)."""
        result = value_lending_position(
            atoken_balance=100_000_000,  # 100 USDC
            stable_debt=0,
            variable_debt=200_000_000,  # 200 USDC debt
            token_price_usd=Decimal("1.0"),
            token_decimals=6,
        )
        assert result.net_value_usd == Decimal("-100")

    def test_frozen_dataclass(self):
        """LendingPositionValue is immutable."""
        result = value_lending_position(
            atoken_balance=1_000_000,
            stable_debt=0,
            variable_debt=0,
            token_price_usd=Decimal("1.0"),
            token_decimals=6,
        )
        with pytest.raises(AttributeError):
            result.net_value_usd = Decimal("999")


class TestValueLendingPortfolio:
    """Tests for aggregating multiple lending positions."""

    def test_single_position(self):
        """Portfolio with one position."""
        pos = value_lending_position(
            atoken_balance=1_000_000_000_000_000_000,
            stable_debt=0,
            variable_debt=0,
            token_price_usd=Decimal("3000"),
            token_decimals=18,
        )
        total_supply, total_debt, total_net = value_lending_portfolio([pos])
        assert total_supply == Decimal("3000")
        assert total_debt == Decimal("0")
        assert total_net == Decimal("3000")

    def test_multi_asset_portfolio(self):
        """Portfolio: supply WETH, borrow USDC."""
        weth_supply = value_lending_position(
            atoken_balance=2_000_000_000_000_000_000,  # 2 WETH supplied
            stable_debt=0,
            variable_debt=0,
            token_price_usd=Decimal("3000"),
            token_decimals=18,
            asset="WETH",
        )
        usdc_borrow = value_lending_position(
            atoken_balance=0,
            stable_debt=0,
            variable_debt=3_000_000_000,  # 3000 USDC borrowed
            token_price_usd=Decimal("1.0"),
            token_decimals=6,
            asset="USDC",
        )
        total_supply, total_debt, total_net = value_lending_portfolio([weth_supply, usdc_borrow])
        assert total_supply == Decimal("6000")  # 2 WETH * 3000
        assert total_debt == Decimal("3000")  # 3000 USDC
        assert total_net == Decimal("3000")

    def test_empty_portfolio(self):
        """Empty portfolio."""
        total_supply, total_debt, total_net = value_lending_portfolio([])
        assert total_supply == Decimal("0")
        assert total_debt == Decimal("0")
        assert total_net == Decimal("0")


# =============================================================================
# TestLendingPositionReader — hex parsing
# =============================================================================


class TestPadAddress:
    """Tests for _pad_address helper."""

    def test_with_0x_prefix(self):
        result = _pad_address("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
        assert len(result) == 64
        assert result == "000000000000000000000000af88d065e77c8cc2239327c5edb3a432268e5831"

    def test_without_prefix(self):
        result = _pad_address("af88d065e77c8cC2239327C5EDb3A432268e5831")
        assert len(result) == 64


class TestDecodeUintHex:
    """Tests for _decode_uint_hex."""

    def test_zero(self):
        data = "0" * 64  # 1 word of zeros
        assert _decode_uint_hex(data, 0) == 0

    def test_one(self):
        data = "0" * 63 + "1"
        assert _decode_uint_hex(data, 0) == 1

    def test_second_word(self):
        data = "0" * 64 + "0" * 63 + "a"  # 2 words, second = 10
        assert _decode_uint_hex(data, 1) == 10


class TestParseUserReserveDataHex:
    """Tests for _parse_user_reserve_data_hex."""

    def _build_hex_response(
        self,
        atoken_balance: int = 0,
        stable_debt: int = 0,
        variable_debt: int = 0,
        principal_stable_debt: int = 0,
        scaled_variable_debt: int = 0,
        stable_borrow_rate: int = 0,
        liquidity_rate: int = 0,
        stable_rate_last_updated: int = 0,
        collateral_enabled: bool = True,
    ) -> str:
        """Build a mock hex response for getUserReserveData."""
        words = [
            atoken_balance,
            stable_debt,
            variable_debt,
            principal_stable_debt,
            scaled_variable_debt,
            stable_borrow_rate,
            liquidity_rate,
            stable_rate_last_updated,
            1 if collateral_enabled else 0,
        ]
        return "0x" + "".join(hex(w)[2:].zfill(64) for w in words)

    def test_supply_only_position(self):
        """Parse a position with only supply."""
        hex_data = self._build_hex_response(
            atoken_balance=1_500_000_000,  # 1500 USDC
            collateral_enabled=True,
        )
        result = _parse_user_reserve_data_hex(hex_data, "0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
        assert result is not None
        assert result.current_atoken_balance == 1_500_000_000
        assert result.current_stable_debt == 0
        assert result.current_variable_debt == 0
        assert result.usage_as_collateral_enabled is True
        assert result.is_active is True

    def test_supply_and_debt(self):
        """Parse a position with supply and variable debt."""
        hex_data = self._build_hex_response(
            atoken_balance=1_000_000_000_000_000_000,  # 1 WETH
            variable_debt=500_000_000_000_000_000,  # 0.5 WETH
            liquidity_rate=30_000_000_000_000_000_000_000_000,  # 3% APY in ray
        )
        result = _parse_user_reserve_data_hex(hex_data, "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1")
        assert result is not None
        assert result.current_atoken_balance == 1_000_000_000_000_000_000
        assert result.current_variable_debt == 500_000_000_000_000_000
        assert result.total_debt == 500_000_000_000_000_000
        assert result.liquidity_rate == 30_000_000_000_000_000_000_000_000

    def test_empty_position(self):
        """Parse a position with no supply or debt."""
        hex_data = self._build_hex_response()
        result = _parse_user_reserve_data_hex(hex_data, "0xaddr")
        assert result is not None
        assert result.is_active is False

    def test_collateral_disabled(self):
        """Parse a position with collateral disabled."""
        hex_data = self._build_hex_response(
            atoken_balance=1_000_000,
            collateral_enabled=False,
        )
        result = _parse_user_reserve_data_hex(hex_data, "0xaddr")
        assert result is not None
        assert result.usage_as_collateral_enabled is False

    def test_too_short_hex(self):
        """Hex response too short should return None."""
        result = _parse_user_reserve_data_hex("0x" + "00" * 10, "0xaddr")
        assert result is None

    def test_stable_and_variable_debt(self):
        """Parse a position with both stable and variable debt."""
        hex_data = self._build_hex_response(
            stable_debt=100_000_000,
            variable_debt=200_000_000,
        )
        result = _parse_user_reserve_data_hex(hex_data, "0xaddr")
        assert result is not None
        assert result.total_debt == 300_000_000


class TestLendingPositionReaderIntegration:
    """Tests for LendingPositionReader with mocked gateway."""

    def test_no_gateway_returns_none(self):
        """Without gateway client, read_position returns None."""
        reader = LendingPositionReader(gateway_client=None)
        result = reader.read_position("arbitrum", "0xaddr", "0xwallet")
        assert result is None

    def test_unknown_chain_returns_none(self):
        """Unknown chain returns None."""
        reader = LendingPositionReader(gateway_client=MagicMock())
        result = reader.read_position("solana", "0xaddr", "0xwallet")
        assert result is None

    def test_read_positions_filters_inactive(self):
        """read_positions only returns active positions."""
        reader = LendingPositionReader(gateway_client=None)
        result = reader.read_positions("arbitrum", ["0xa", "0xb"], "0xwallet")
        assert result == []


# =============================================================================
# TestPortfolioValuerLendingRepricing
# =============================================================================


class TestPortfolioValuerLendingRepricing:
    """Integration tests for lending repricing in PortfolioValuer."""

    def _make_position(self, position_type, **kwargs):
        """Create a PositionInfo for testing."""
        from almanak.framework.teardown.models import PositionInfo, PositionType

        defaults = {
            "position_type": getattr(PositionType, position_type),
            "position_id": "test-position",
            "chain": "arbitrum",
            "protocol": "aave_v3",
            "value_usd": Decimal("999"),
            "details": {},
        }
        defaults.update(kwargs)
        return PositionInfo(**defaults)

    def test_supply_falls_back_without_gateway(self):
        """Without gateway, SUPPLY position uses strategy-reported value."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=None)
        market = MagicMock()

        position = self._make_position("SUPPLY", value_usd=Decimal("5000"))
        result = valuer._reprice_position(position, "arbitrum", market)
        assert result == Decimal("5000")

    def test_borrow_falls_back_without_gateway(self):
        """Without gateway, BORROW fallback negates positive value_usd."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=None)
        market = MagicMock()

        position = self._make_position("BORROW", value_usd=Decimal("2000"))
        result = valuer._reprice_position(position, "arbitrum", market)
        # BORROW fallback normalizes to negative so debt reduces portfolio
        assert result == Decimal("-2000")

    def test_token_position_passes_through(self):
        """TOKEN position always uses strategy-reported value."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=None)
        market = MagicMock()

        position = self._make_position("TOKEN", value_usd=Decimal("1234"))
        result = valuer._reprice_position(position, "arbitrum", market)
        assert result == Decimal("1234")

    def test_lending_repricing_with_mocked_on_chain_data(self):
        """Full lending re-pricing with mocked on-chain + market data."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=MagicMock())

        # Mock the lending reader to return on-chain data
        mock_on_chain = LendingPositionOnChain(
            asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            current_atoken_balance=5_000_000_000,  # 5000 USDC
            current_stable_debt=0,
            current_variable_debt=1_000_000_000,  # 1000 USDC debt
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
        )
        valuer._lending_reader = MagicMock()
        valuer._lending_reader.read_position.return_value = mock_on_chain

        # Mock market
        market = MagicMock()
        market.price.return_value = Decimal("1.0")

        position = self._make_position(
            "SUPPLY",
            value_usd=Decimal("999"),
            details={
                "asset_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "wallet": "0x1234567890abcdef1234567890abcdef12345678",
                "asset": "USDC",
            },
        )

        with patch.object(
            PortfolioValuer,
            "_get_token_decimals",
            return_value=6,
        ):
            result = valuer._reprice_position(position, "arbitrum", market)

        # Expected: supply 5000 - debt 1000 = 4000 (net, for SUPPLY position)
        assert result == Decimal("4000")

    def test_borrow_position_returns_debt_value(self):
        """BORROW position returns the debt value, not net."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=MagicMock())

        mock_on_chain = LendingPositionOnChain(
            asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            current_atoken_balance=0,
            current_stable_debt=0,
            current_variable_debt=2_000_000_000,  # 2000 USDC debt
            liquidity_rate=0,
            usage_as_collateral_enabled=False,
        )
        valuer._lending_reader = MagicMock()
        valuer._lending_reader.read_position.return_value = mock_on_chain

        market = MagicMock()
        market.price.return_value = Decimal("1.0")

        position = self._make_position(
            "BORROW",
            value_usd=Decimal("999"),
            details={
                "asset_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "wallet": "0x1234567890abcdef1234567890abcdef12345678",
                "asset": "USDC",
            },
        )

        with patch.object(
            PortfolioValuer,
            "_get_token_decimals",
            return_value=6,
        ):
            result = valuer._reprice_position(position, "arbitrum", market)

        # BORROW returns negative so it reduces portfolio total when summed
        assert result == Decimal("-2000")

    def test_empty_position_returns_zero(self):
        """Inactive lending position returns 0."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=MagicMock())

        mock_on_chain = LendingPositionOnChain(
            asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            current_atoken_balance=0,
            current_stable_debt=0,
            current_variable_debt=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=False,
        )
        valuer._lending_reader = MagicMock()
        valuer._lending_reader.read_position.return_value = mock_on_chain

        market = MagicMock()

        position = self._make_position(
            "SUPPLY",
            value_usd=Decimal("999"),
            details={
                "asset_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "wallet": "0x1234567890abcdef1234567890abcdef12345678",
            },
        )

        result = valuer._reprice_position(position, "arbitrum", market)
        assert result == Decimal("0")

    def test_unknown_decimals_falls_back(self):
        """If decimals can't be resolved, falls back to strategy value."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=MagicMock())

        mock_on_chain = LendingPositionOnChain(
            asset_address="0xaddr",
            current_atoken_balance=1_000_000,
            current_stable_debt=0,
            current_variable_debt=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
        )
        valuer._lending_reader = MagicMock()
        valuer._lending_reader.read_position.return_value = mock_on_chain

        market = MagicMock()
        market.price.return_value = Decimal("1.0")

        position = self._make_position(
            "SUPPLY",
            value_usd=Decimal("999"),
            details={
                "asset_address": "0xaddr_long_enough_to_be_valid_40chars",
                "wallet": "0x1234567890abcdef1234567890abcdef12345678",
                "asset": "UNKNOWN_TOKEN",
            },
        )

        with patch.object(
            PortfolioValuer,
            "_get_token_decimals",
            return_value=None,
        ):
            result = valuer._reprice_position(position, "arbitrum", market)

        # Falls back to strategy-reported value
        assert result == Decimal("999")

    def test_missing_asset_address_falls_back(self):
        """If no asset_address in details, falls back to strategy value."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=MagicMock())
        market = MagicMock()

        position = self._make_position(
            "SUPPLY",
            value_usd=Decimal("5000"),
            details={"wallet": "0x1234567890abcdef1234567890abcdef12345678"},
        )

        result = valuer._reprice_position(position, "arbitrum", market)
        assert result == Decimal("5000")

    def test_missing_wallet_falls_back(self):
        """If no wallet in details, falls back to strategy value."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=MagicMock())
        market = MagicMock()

        position = self._make_position(
            "SUPPLY",
            value_usd=Decimal("5000"),
            details={"asset_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"},
        )

        result = valuer._reprice_position(position, "arbitrum", market)
        assert result == Decimal("5000")

    def test_weth_supply_with_exact_values(self):
        """Deterministic: 2 WETH supplied at $3000 = $6000."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=MagicMock())

        mock_on_chain = LendingPositionOnChain(
            asset_address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            current_atoken_balance=2_000_000_000_000_000_000,  # 2 WETH
            current_stable_debt=0,
            current_variable_debt=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
        )
        valuer._lending_reader = MagicMock()
        valuer._lending_reader.read_position.return_value = mock_on_chain

        market = MagicMock()
        market.price.return_value = Decimal("3000")

        position = self._make_position(
            "SUPPLY",
            value_usd=Decimal("999"),
            details={
                "asset_address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                "wallet": "0x1234567890abcdef1234567890abcdef12345678",
                "asset": "WETH",
            },
        )

        with patch.object(
            PortfolioValuer,
            "_get_token_decimals",
            return_value=18,
        ):
            result = valuer._reprice_position(position, "arbitrum", market)

        assert result == Decimal("6000")


# =============================================================================
# TestExtractAssetAddress
# =============================================================================


class TestExtractAssetAddress:
    """Tests for _extract_asset_address static method."""

    def test_asset_address_key(self):
        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        pos = PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="test",
            chain="arbitrum",
            protocol="aave_v3",
            value_usd=Decimal("0"),
            details={"asset_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"},
        )
        assert PortfolioValuer._extract_asset_address(pos) == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

    def test_underlying_key(self):
        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        pos = PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="test",
            chain="arbitrum",
            protocol="aave_v3",
            value_usd=Decimal("0"),
            details={"underlying": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"},
        )
        assert PortfolioValuer._extract_asset_address(pos) == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

    def test_no_address_returns_none(self):
        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        pos = PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="test",
            chain="arbitrum",
            protocol="aave_v3",
            value_usd=Decimal("0"),
            details={},
        )
        assert PortfolioValuer._extract_asset_address(pos) is None

    def test_short_string_ignored(self):
        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        pos = PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="test",
            chain="arbitrum",
            protocol="aave_v3",
            value_usd=Decimal("0"),
            details={"asset_address": "USDC"},  # Too short to be an address
        )
        assert PortfolioValuer._extract_asset_address(pos) is None


# =============================================================================
# TestDashboardMigration
# =============================================================================


class TestDashboardMigrationRemoval:
    """Verify the migration fallback was properly removed."""

    def test_no_extract_portfolio_value_from_state(self):
        """The old _extract_portfolio_value_from_state method should not exist."""
        from almanak.gateway.services.dashboard_service import DashboardServiceServicer

        assert not hasattr(DashboardServiceServicer, "_extract_portfolio_value_from_state")
