"""Tests for GMX V2 perpetual position valuation (PnL Week 6).

Covers:
- perps_valuer: pure math for mark-to-market pricing
- perps_position_reader: on-chain position parsing
- PortfolioValuer._reprice_perps_on_chain: integration
- PositionDiscoveryService: perps discovery path
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.valuation.perps_position_reader import (
    PerpsPositionOnChain,
    PerpsPositionReader,
    _parse_position_dict,
)
from almanak.framework.valuation.perps_valuer import (
    GMX_USD_DECIMALS,
    PerpsPositionValue,
    value_perps_position,
)


# =============================================================================
# perps_valuer pure math tests
# =============================================================================


class TestValuePerpsPosition:
    """Test the mark-to-market math for GMX V2 positions."""

    def _make_long_eth(
        self,
        *,
        size_usd: int = 10_000,
        tokens: float = 5.0,
        collateral: int = 2000,
        mark_price: Decimal = Decimal("2000"),
        collateral_price: Decimal = Decimal("1"),
        collateral_decimals: int = 6,
        index_decimals: int = 18,
        funding: Decimal = Decimal("0"),
        borrowing: Decimal = Decimal("0"),
    ) -> PerpsPositionValue:
        """Helper: create a long ETH/USD position valued at given mark price."""
        return value_perps_position(
            size_in_usd=size_usd * 10**GMX_USD_DECIMALS,
            size_in_tokens=int(tokens * 10**index_decimals),
            collateral_amount=collateral * 10**collateral_decimals,
            is_long=True,
            mark_price_usd=mark_price,
            collateral_token_price_usd=collateral_price,
            collateral_token_decimals=collateral_decimals,
            index_token_decimals=index_decimals,
            pending_funding_fees_usd=funding,
            pending_borrowing_fees_usd=borrowing,
            market="ETH/USD",
        )

    def test_long_breakeven(self):
        """Long at entry = mark: PnL should be ~0."""
        result = self._make_long_eth(
            size_usd=10_000, tokens=5.0, mark_price=Decimal("2000")
        )
        assert result.is_long is True
        assert result.market == "ETH/USD"
        assert result.size_usd == Decimal("10000")
        # Entry = 10000/5 = 2000, mark = 2000 => pnl ≈ 0
        assert abs(result.unrealized_pnl_usd) < Decimal("0.01")

    def test_long_profit(self):
        """Long with price increase: positive PnL."""
        result = self._make_long_eth(
            size_usd=10_000, tokens=5.0, mark_price=Decimal("2200")
        )
        # Entry = 2000, mark = 2200, tokens = 5
        # PnL = 5 * (2200 - 2000) = 1000
        assert result.unrealized_pnl_usd == Decimal("1000")
        assert result.net_value_usd == Decimal("3000")  # 2000 collateral + 1000 pnl

    def test_long_loss(self):
        """Long with price decrease: negative PnL."""
        result = self._make_long_eth(
            size_usd=10_000, tokens=5.0, mark_price=Decimal("1800")
        )
        # PnL = 5 * (1800 - 2000) = -1000
        assert result.unrealized_pnl_usd == Decimal("-1000")
        assert result.net_value_usd == Decimal("1000")  # 2000 - 1000

    def test_short_breakeven(self):
        """Short at entry = mark: PnL should be ~0."""
        result = value_perps_position(
            size_in_usd=10_000 * 10**GMX_USD_DECIMALS,
            size_in_tokens=5 * 10**18,
            collateral_amount=2000 * 10**6,
            is_long=False,
            mark_price_usd=Decimal("2000"),
            collateral_token_price_usd=Decimal("1"),
            collateral_token_decimals=6,
            index_token_decimals=18,
        )
        assert result.is_long is False
        assert abs(result.unrealized_pnl_usd) < Decimal("0.01")

    def test_short_profit(self):
        """Short with price decrease: positive PnL."""
        result = value_perps_position(
            size_in_usd=10_000 * 10**GMX_USD_DECIMALS,
            size_in_tokens=5 * 10**18,
            collateral_amount=2000 * 10**6,
            is_long=False,
            mark_price_usd=Decimal("1800"),
            collateral_token_price_usd=Decimal("1"),
            collateral_token_decimals=6,
            index_token_decimals=18,
        )
        # PnL = 5 * (2000 - 1800) = 1000
        assert result.unrealized_pnl_usd == Decimal("1000")

    def test_short_loss(self):
        """Short with price increase: negative PnL."""
        result = value_perps_position(
            size_in_usd=10_000 * 10**GMX_USD_DECIMALS,
            size_in_tokens=5 * 10**18,
            collateral_amount=2000 * 10**6,
            is_long=False,
            mark_price_usd=Decimal("2200"),
            collateral_token_price_usd=Decimal("1"),
            collateral_token_decimals=6,
            index_token_decimals=18,
        )
        # PnL = 5 * (2000 - 2200) = -1000
        assert result.unrealized_pnl_usd == Decimal("-1000")

    def test_fees_reduce_net_value(self):
        """Pending fees reduce net value."""
        result = self._make_long_eth(
            size_usd=10_000,
            tokens=5.0,
            mark_price=Decimal("2000"),
            funding=Decimal("50"),
            borrowing=Decimal("30"),
        )
        assert result.pending_fees_usd == Decimal("80")
        # Collateral (2000) + PnL (0) - fees (80) = 1920
        assert result.net_value_usd == Decimal("1920")

    def test_leverage_calculation(self):
        """Leverage = notional / collateral value."""
        result = self._make_long_eth(
            size_usd=10_000, tokens=5.0, collateral=2000
        )
        # Size = 10000, collateral = 2000 * $1 = 2000 => leverage = 5
        assert result.leverage == Decimal("5")

    def test_non_usd_collateral(self):
        """Collateral in ETH (non-stablecoin) valued at market price."""
        result = value_perps_position(
            size_in_usd=10_000 * 10**GMX_USD_DECIMALS,
            size_in_tokens=5 * 10**18,
            collateral_amount=1 * 10**18,  # 1 ETH as collateral
            is_long=True,
            mark_price_usd=Decimal("2000"),
            collateral_token_price_usd=Decimal("2000"),  # ETH price
            collateral_token_decimals=18,
            index_token_decimals=18,
        )
        assert result.collateral_value_usd == Decimal("2000")
        assert result.leverage == Decimal("5")

    def test_btc_position_8_decimals(self):
        """BTC market uses 8 decimals for index token."""
        result = value_perps_position(
            size_in_usd=100_000 * 10**GMX_USD_DECIMALS,
            size_in_tokens=int(1.0 * 10**8),  # 1 BTC (8 decimals)
            collateral_amount=10_000 * 10**6,  # 10k USDC
            is_long=True,
            mark_price_usd=Decimal("100000"),
            collateral_token_price_usd=Decimal("1"),
            collateral_token_decimals=6,
            index_token_decimals=8,
            market="BTC/USD",
        )
        assert result.size_usd == Decimal("100000")
        assert result.entry_price_usd == Decimal("100000")
        assert abs(result.unrealized_pnl_usd) < Decimal("0.01")

    def test_zero_size_returns_zero_pnl(self):
        """Position with zero size has zero PnL."""
        result = value_perps_position(
            size_in_usd=0,
            size_in_tokens=0,
            collateral_amount=1000 * 10**6,
            is_long=True,
            mark_price_usd=Decimal("2000"),
            collateral_token_price_usd=Decimal("1"),
            collateral_token_decimals=6,
            index_token_decimals=18,
        )
        assert result.unrealized_pnl_usd == Decimal("0")
        assert result.leverage == Decimal("0")

    def test_zero_collateral_zero_leverage(self):
        """Zero collateral results in zero leverage (not division by zero)."""
        result = value_perps_position(
            size_in_usd=10_000 * 10**GMX_USD_DECIMALS,
            size_in_tokens=5 * 10**18,
            collateral_amount=0,
            is_long=True,
            mark_price_usd=Decimal("2000"),
            collateral_token_price_usd=Decimal("1"),
            collateral_token_decimals=6,
            index_token_decimals=18,
        )
        assert result.leverage == Decimal("0")


# =============================================================================
# PerpsPositionOnChain tests
# =============================================================================


class TestPerpsPositionOnChain:
    def test_is_active(self):
        pos = PerpsPositionOnChain(
            account="0x1234",
            market="0xmarket",
            collateral_token="0xusdc",
            size_in_usd=10_000 * 10**30,
            size_in_tokens=5 * 10**18,
            collateral_amount=2000 * 10**6,
            is_long=True,
            borrowing_factor=0,
            funding_fee_amount_per_size=0,
            increased_at_time=0,
            decreased_at_time=0,
        )
        assert pos.is_active is True

    def test_inactive_zero_size(self):
        pos = PerpsPositionOnChain(
            account="0x1234",
            market="0xmarket",
            collateral_token="0xusdc",
            size_in_usd=0,
            size_in_tokens=0,
            collateral_amount=0,
            is_long=True,
            borrowing_factor=0,
            funding_fee_amount_per_size=0,
            increased_at_time=0,
            decreased_at_time=0,
        )
        assert pos.is_active is False

    def test_position_key_long(self):
        pos = PerpsPositionOnChain(
            account="0x1234",
            market="0xABCD",
            collateral_token="0xUSDC",
            size_in_usd=1,
            size_in_tokens=1,
            collateral_amount=1,
            is_long=True,
            borrowing_factor=0,
            funding_fee_amount_per_size=0,
            increased_at_time=0,
            decreased_at_time=0,
        )
        assert pos.position_key == "gmx-0xabcd-0xusdc-long"

    def test_position_key_short(self):
        pos = PerpsPositionOnChain(
            account="0x1234",
            market="0xABCD",
            collateral_token="0xUSDC",
            size_in_usd=1,
            size_in_tokens=1,
            collateral_amount=1,
            is_long=False,
            borrowing_factor=0,
            funding_fee_amount_per_size=0,
            increased_at_time=0,
            decreased_at_time=0,
        )
        assert pos.position_key == "gmx-0xabcd-0xusdc-short"


# =============================================================================
# _parse_position_dict tests
# =============================================================================


class TestParsePositionDict:
    def test_parse_valid_dict(self):
        raw = {
            "account": "0xWallet",
            "market": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            "collateral_token": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "size_in_usd": 10_000 * 10**30,
            "size_in_tokens": 5 * 10**18,
            "collateral_amount": 2000 * 10**6,
            "is_long": True,
            "borrowing_factor": 123456,
            "funding_fee_amount_per_size": 789,
            "increased_at_time": 1700000000,
            "decreased_at_time": 0,
        }
        pos = _parse_position_dict(raw, "0xWallet")
        assert pos is not None
        assert pos.is_active is True
        assert pos.market == "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
        assert pos.size_in_usd == 10_000 * 10**30

    def test_parse_empty_dict_returns_inactive(self):
        pos = _parse_position_dict({}, "0xWallet")
        assert pos is not None
        assert pos.is_active is False

    def test_parse_missing_fields_uses_defaults(self):
        pos = _parse_position_dict({"market": "0xM"}, "0xFallback")
        assert pos is not None
        assert pos.account == "0xFallback"
        assert pos.market == "0xM"


# =============================================================================
# PerpsPositionReader tests
# =============================================================================


class TestPerpsPositionReader:
    def test_no_rpc_url_returns_empty(self):
        reader = PerpsPositionReader()
        assert reader.read_positions("arbitrum", "0x1234") == []

    def test_unsupported_chain_returns_empty(self):
        reader = PerpsPositionReader(rpc_url="http://localhost:8545")
        assert reader.read_positions("polygon", "0x1234") == []

    def test_supported_chains(self):
        assert "arbitrum" in PerpsPositionReader.SUPPORTED_CHAINS
        # Avalanche removed: GMXV2SDK only supports arbitrum currently
        assert "avalanche" not in PerpsPositionReader.SUPPORTED_CHAINS

    def test_from_gateway_client_none(self):
        reader = PerpsPositionReader.from_gateway_client(None)
        assert reader._rpc_url is None

    def test_from_gateway_client_direct_adapter(self):
        """DirectRpcAdapter has _rpc_stub._rpc_url."""
        mock_client = MagicMock()
        mock_client._rpc_stub._rpc_url = "http://localhost:8545"
        reader = PerpsPositionReader.from_gateway_client(mock_client)
        assert reader._rpc_url == "http://localhost:8545"

    def test_from_gateway_client_no_rpc_stub_no_env(self):
        mock_client = MagicMock(spec=[])
        with patch.dict("os.environ", {}, clear=True):
            reader = PerpsPositionReader.from_gateway_client(mock_client)
        assert reader._rpc_url is None

    def test_read_positions_with_mocked_sdk(self):
        reader = PerpsPositionReader(rpc_url="http://localhost:8545")

        mock_positions = [
            {
                "account": "0xWallet",
                "market": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
                "collateral_token": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "size_in_usd": 10_000 * 10**30,
                "size_in_tokens": 5 * 10**18,
                "collateral_amount": 2000 * 10**6,
                "is_long": True,
                "borrowing_factor": 0,
                "funding_fee_amount_per_size": 0,
                "increased_at_time": 1700000000,
                "decreased_at_time": 0,
            }
        ]

        with patch("almanak.framework.connectors.gmx_v2.sdk.GMXV2SDK") as MockSDK:
            MockSDK.return_value.get_account_positions.return_value = mock_positions
            positions = reader.read_positions("arbitrum", "0xWallet")

        assert len(positions) == 1
        assert positions[0].is_long is True
        assert positions[0].size_in_usd == 10_000 * 10**30

    def test_read_positions_sdk_exception_returns_empty(self):
        reader = PerpsPositionReader(rpc_url="http://localhost:8545")

        with patch(
            "almanak.framework.connectors.gmx_v2.sdk.GMXV2SDK",
            side_effect=Exception("connection refused"),
        ):
            positions = reader.read_positions("arbitrum", "0xWallet")

        assert positions == []


# =============================================================================
# PortfolioValuer perps integration tests
# =============================================================================


class TestPortfolioValuerPerpsIntegration:
    """Test that PortfolioValuer._reprice_perps_on_chain works end-to-end."""

    def _make_valuer(self):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        return PortfolioValuer(gateway_client=None)

    def _make_perp_position(self):
        from almanak.framework.teardown.models import PositionInfo, PositionType

        return PositionInfo(
            position_type=PositionType.PERP,
            position_id="gmx-eth-usdc-long",
            chain="arbitrum",
            protocol="gmx_v2",
            value_usd=Decimal("2500"),  # Strategy-reported fallback
            details={
                "market": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
                "collateral_token": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "is_long": True,
                "wallet_address": "0xWallet",
            },
        )

    def test_reprice_perps_no_wallet(self):
        """No wallet_address in details => returns None (fallback)."""
        from almanak.framework.teardown.models import PositionInfo, PositionType

        valuer = self._make_valuer()
        pos = PositionInfo(
            position_type=PositionType.PERP,
            position_id="test",
            chain="arbitrum",
            protocol="gmx_v2",
            value_usd=Decimal("1000"),
            details={},
        )
        market = MagicMock()
        result = valuer._reprice_perps_on_chain(pos, "arbitrum", market)
        assert result is None

    def test_reprice_perps_no_matching_position(self):
        """Reader returns positions but none match market/direction."""
        valuer = self._make_valuer()
        pos = self._make_perp_position()
        market = MagicMock()

        # Reader returns a position for different market
        different_pos = PerpsPositionOnChain(
            account="0xWallet",
            market="0xDIFFERENT",
            collateral_token="0xusdc",
            size_in_usd=10_000 * 10**30,
            size_in_tokens=5 * 10**18,
            collateral_amount=2000 * 10**6,
            is_long=True,
            borrowing_factor=0,
            funding_fee_amount_per_size=0,
            increased_at_time=0,
            decreased_at_time=0,
        )
        valuer._perps_reader = MagicMock()
        valuer._perps_reader.read_positions.return_value = [different_pos]

        result = valuer._reprice_perps_on_chain(pos, "arbitrum", market)
        assert result is None

    def test_reprice_perps_success(self):
        """Successful repricing returns mark-to-market net value."""
        valuer = self._make_valuer()
        pos = self._make_perp_position()

        # Mock perps reader to return matching position
        on_chain = PerpsPositionOnChain(
            account="0xWallet",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            size_in_usd=10_000 * 10**30,
            size_in_tokens=5 * 10**18,
            collateral_amount=2000 * 10**6,
            is_long=True,
            borrowing_factor=0,
            funding_fee_amount_per_size=0,
            increased_at_time=0,
            decreased_at_time=0,
        )
        valuer._perps_reader = MagicMock()
        valuer._perps_reader.read_positions.return_value = [on_chain]

        # Mock market data
        market = MagicMock()
        market.price.side_effect = lambda token: {
            "ETH": Decimal("2200"),
            "USDC": Decimal("1"),
        }.get(token, Decimal("0"))

        # Mock token resolution
        with (
            patch.object(
                type(valuer),
                "_resolve_perps_index_token",
                return_value="ETH",
            ),
            patch.object(
                type(valuer),
                "_resolve_token_symbol",
                return_value="USDC",
            ),
            patch.object(
                type(valuer),
                "_get_token_decimals",
                return_value=6,
            ),
            patch.object(
                type(valuer),
                "_get_perps_index_decimals",
                return_value=18,
            ),
        ):
            result = valuer._reprice_perps_on_chain(pos, "arbitrum", market)

        assert result is not None
        # Entry = 2000, mark = 2200, tokens = 5 => PnL = 1000
        # Net = 2000 (collateral) + 1000 (pnl) = 3000
        assert result == Decimal("3000")

    def test_reprice_position_delegates_to_perps(self):
        """_reprice_position dispatches PERP to _reprice_perps_on_chain."""
        from almanak.framework.teardown.models import PositionInfo, PositionType

        valuer = self._make_valuer()
        pos = self._make_perp_position()
        market = MagicMock()

        with patch.object(valuer, "_reprice_perps_on_chain", return_value=Decimal("3000")) as mock_reprice:
            result = valuer._reprice_position(pos, "arbitrum", market)

        mock_reprice.assert_called_once_with(pos, "arbitrum", market)
        assert result == Decimal("3000")

    def test_reprice_position_perps_fallback(self):
        """PERP fallback to strategy-reported value when repricing fails."""
        valuer = self._make_valuer()
        pos = self._make_perp_position()  # value_usd=2500
        market = MagicMock()

        with patch.object(valuer, "_reprice_perps_on_chain", return_value=None):
            result = valuer._reprice_position(pos, "arbitrum", market)

        assert result == Decimal("2500")


# =============================================================================
# PortfolioValuer static helpers for perps
# =============================================================================


class TestPerpsHelpers:
    def test_resolve_perps_index_token_eth(self):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        result = PortfolioValuer._resolve_perps_index_token(
            "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336", "arbitrum"
        )
        assert result == "ETH"

    def test_resolve_perps_index_token_btc(self):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        result = PortfolioValuer._resolve_perps_index_token(
            "0x47c031236e19d024b42f8AE6780E44A573170703", "arbitrum"
        )
        assert result == "BTC"

    def test_resolve_perps_index_token_unknown(self):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        result = PortfolioValuer._resolve_perps_index_token("0xunknown", "arbitrum")
        assert result is None

    def test_resolve_perps_index_token_avalanche(self):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        result = PortfolioValuer._resolve_perps_index_token(
            "0xD996ff47A1F763E1e55415BC4437c59292D1F415", "avalanche"
        )
        assert result == "AVAX"

    def test_get_perps_index_decimals_eth(self):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        result = PortfolioValuer._get_perps_index_decimals(
            "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336", "arbitrum"
        )
        assert result == 18

    def test_get_perps_index_decimals_btc(self):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        result = PortfolioValuer._get_perps_index_decimals(
            "0x47c031236e19d024b42f8AE6780E44A573170703", "arbitrum"
        )
        assert result == 8

    def test_get_perps_index_decimals_unknown(self):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        result = PortfolioValuer._get_perps_index_decimals("0xunknown", "arbitrum")
        assert result is None


# =============================================================================
# PositionDiscoveryService perps discovery tests
# =============================================================================


class TestPositionDiscoveryPerps:
    def test_has_perps_protocol(self):
        from almanak.framework.valuation.position_discovery import _has_perps_protocol

        assert _has_perps_protocol(["gmx_v2"]) is True
        assert _has_perps_protocol(["GMX_V2"]) is True
        assert _has_perps_protocol(["gmx"]) is True
        assert _has_perps_protocol(["uniswap_v3"]) is False
        assert _has_perps_protocol([]) is False

    def test_discover_perps_creates_positions(self):
        from almanak.framework.valuation.position_discovery import (
            DiscoveryConfig,
            PositionDiscoveryService,
        )

        service = PositionDiscoveryService(gateway_client=None)

        # Mock the perps reader
        mock_position = PerpsPositionOnChain(
            account="0xWallet",
            market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            size_in_usd=10_000 * 10**30,
            size_in_tokens=5 * 10**18,
            collateral_amount=2000 * 10**6,
            is_long=True,
            borrowing_factor=0,
            funding_fee_amount_per_size=0,
            increased_at_time=0,
            decreased_at_time=0,
        )
        service._perps_reader = MagicMock()
        service._perps_reader.read_positions.return_value = [mock_position]

        config = DiscoveryConfig(
            chain="arbitrum",
            wallet_address="0xWallet",
            protocols=["gmx_v2"],
            tracked_tokens=["USDC", "ETH"],
        )
        result = service.discover(config)

        assert result.perps_scanned is True
        assert len(result.positions) == 1
        pos = result.positions[0]
        assert pos.position_type.value == "PERP"
        assert pos.protocol == "gmx_v2"
        assert pos.details["is_long"] is True
        assert pos.details["wallet_address"] == "0xWallet"

    def test_discover_no_perps_protocol_skips(self):
        from almanak.framework.valuation.position_discovery import (
            DiscoveryConfig,
            PositionDiscoveryService,
        )

        service = PositionDiscoveryService(gateway_client=None)
        service._perps_reader = MagicMock()

        config = DiscoveryConfig(
            chain="arbitrum",
            wallet_address="0xWallet",
            protocols=["uniswap_v3"],
            tracked_tokens=["ETH"],
        )
        result = service.discover(config)

        assert result.perps_scanned is False
        service._perps_reader.read_positions.assert_not_called()

    def test_discover_perps_reader_exception_records_error(self):
        from almanak.framework.valuation.position_discovery import (
            DiscoveryConfig,
            PositionDiscoveryService,
        )

        service = PositionDiscoveryService(gateway_client=None)
        service._perps_reader = MagicMock()
        service._perps_reader.read_positions.side_effect = RuntimeError("boom")

        config = DiscoveryConfig(
            chain="arbitrum",
            wallet_address="0xWallet",
            protocols=["gmx_v2"],
        )
        result = service.discover(config)

        assert result.perps_scanned is True
        assert len(result.errors) == 1
        assert "boom" in result.errors[0]


# =============================================================================
# Audit fix regression tests
# =============================================================================


class TestAuditFixes:
    """Tests for issues found during multi-auditor review."""

    def test_get_perps_index_decimals_case_insensitive(self):
        """Fix B1: Case-insensitive lookup for market address decimals."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        # Lowercased address should still resolve
        result = PortfolioValuer._get_perps_index_decimals(
            "0x70d95587d40a2caf56bd97485ab3eec10bee6336", "arbitrum"
        )
        assert result == 18

        # Checksummed address should still work
        result = PortfolioValuer._get_perps_index_decimals(
            "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336", "arbitrum"
        )
        assert result == 18

    def test_is_long_missing_returns_none(self):
        """Fix B2: Missing is_long field forces fallback, not silent default."""
        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=None)
        pos = PositionInfo(
            position_type=PositionType.PERP,
            position_id="gmx-test",
            chain="arbitrum",
            protocol="gmx_v2",
            value_usd=Decimal("1000"),
            details={
                "market": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
                "wallet_address": "0xWallet",
                # No "is_long" field!
            },
        )
        market = MagicMock()
        result = valuer._reprice_perps_on_chain(pos, "arbitrum", market)
        assert result is None  # Forces fallback to strategy-reported value

    def test_avalanche_unsupported_returns_empty(self):
        """Fix P1: Avalanche removed from SUPPORTED_CHAINS."""
        reader = PerpsPositionReader(rpc_url="http://localhost:8545")
        assert reader.read_positions("avalanche", "0x1234") == []

    def test_from_gateway_client_env_fallback(self):
        """Fix P1: Extract RPC URL from ALCHEMY_API_KEY env var."""
        mock_client = MagicMock(spec=[])  # No _rpc_stub
        with patch.dict("os.environ", {"ALCHEMY_API_KEY": "test-key-123"}):
            reader = PerpsPositionReader.from_gateway_client(mock_client, chain="arbitrum")
        assert reader._rpc_url == "https://arb-mainnet.g.alchemy.com/v2/test-key-123"

    def test_from_gateway_client_no_env_no_rpc(self):
        """No DirectRpcAdapter and no ALCHEMY_API_KEY => no RPC URL."""
        mock_client = MagicMock(spec=[])
        with patch.dict("os.environ", {}, clear=True):
            reader = PerpsPositionReader.from_gateway_client(mock_client, chain="arbitrum")
        assert reader._rpc_url is None

    def test_perps_dedup_skips_discovered_when_strategy_reports(self):
        """Fix P2: Discovery perps skipped when strategy already reports PERP for same protocol."""
        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=None)

        # Strategy reports a PERP position with a custom ID format
        strategy_pos = PositionInfo(
            position_type=PositionType.PERP,
            position_id="gmx-ETH/USD-usdc-perp",  # Strategy's custom format
            chain="arbitrum",
            protocol="gmx_v2",
            value_usd=Decimal("2500"),
            details={"market": "0x70d95587", "is_long": True, "wallet_address": "0xW"},
        )

        # Discovery finds the same position with a different ID format
        discovered_pos = PositionInfo(
            position_type=PositionType.PERP,
            position_id="gmx-0x70d95587-0xusdc-long",  # Discovery format
            chain="arbitrum",
            protocol="gmx_v2",
            value_usd=Decimal("0"),
            details={},
        )

        # Mock strategy positions
        with patch.object(valuer, "_get_strategy_positions", return_value=([strategy_pos], False)):
            # Mock discovery to return the conflicting position
            mock_discovery = MagicMock()
            mock_result = MagicMock()
            mock_result.positions = [discovered_pos]
            mock_result.errors = []
            mock_discovery.discover.return_value = mock_result
            valuer._discovery = mock_discovery

            # Mock build_discovery_config to return something
            with patch.object(valuer, "_build_discovery_config", return_value=MagicMock()):
                market = MagicMock()
                market.price.return_value = Decimal("2000")
                market.balance.return_value = Decimal("0")

                # Mock strategy for the value() call
                strategy = MagicMock()
                strategy.strategy_id = "test"
                strategy.chain = "arbitrum"
                strategy._get_tracked_tokens.return_value = []

                positions, total, incomplete = valuer._get_positions(strategy, market, {})

        # Should only have 1 position (strategy's), not 2 (no double-count)
        assert len(positions) == 1
        assert positions[0].details.get("market") == "0x70d95587"
