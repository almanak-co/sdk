"""Tests for position discovery service and portfolio valuer integration.

Covers:
- position_discovery.py: DiscoveryConfig, PositionDiscoveryService, helpers
- portfolio_valuer.py: two-source position merging and discovery integration
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.position_discovery import (
    DiscoveryConfig,
    DiscoveryResult,
    PositionDiscoveryService,
    _has_lending_protocol,
    _has_lp_protocol,
    _lending_to_position_infos,
)
from almanak.framework.valuation.lending_position_reader import LendingPositionOnChain


# =============================================================================
# Helper protocol matchers
# =============================================================================


class TestHasLendingProtocol:
    def test_aave_v3(self):
        assert _has_lending_protocol(["aave_v3"]) is True

    def test_spark_not_supported(self):
        """Spark uses different contracts — not routed through Aave reader."""
        assert _has_lending_protocol(["spark"]) is False

    def test_compound_v3_not_supported(self):
        """Compound V3 uses different contracts — not routed through Aave reader."""
        assert _has_lending_protocol(["compound_v3"]) is False

    def test_case_insensitive(self):
        assert _has_lending_protocol(["AAVE_V3"]) is True

    def test_no_lending(self):
        assert _has_lending_protocol(["uniswap_v3"]) is False

    def test_empty(self):
        assert _has_lending_protocol([]) is False

    def test_mixed(self):
        assert _has_lending_protocol(["uniswap_v3", "aave_v3"]) is True


class TestHasLpProtocol:
    def test_uniswap_v3(self):
        assert _has_lp_protocol(["uniswap_v3"]) is True

    def test_sushiswap_v3(self):
        assert _has_lp_protocol(["sushiswap_v3"]) is True

    def test_aerodrome(self):
        assert _has_lp_protocol(["aerodrome"]) is True

    def test_no_lp(self):
        assert _has_lp_protocol(["aave_v3"]) is False

    def test_empty(self):
        assert _has_lp_protocol([]) is False


# =============================================================================
# Lending -> PositionInfo conversion
# =============================================================================


class TestLendingToPositionInfos:
    def test_supply_only(self):
        on_chain = LendingPositionOnChain(
            asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            current_atoken_balance=1_500_000_000,
            current_stable_debt=0,
            current_variable_debt=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
        )
        positions = _lending_to_position_infos(on_chain, "USDC", "arbitrum", "0xwallet123")
        assert len(positions) == 1
        assert positions[0].position_type == PositionType.SUPPLY
        assert positions[0].position_id == "aave-supply-USDC-arbitrum"
        assert positions[0].details["asset"] == "USDC"
        assert positions[0].details["asset_address"] == on_chain.asset_address
        assert positions[0].details["wallet_address"] == "0xwallet123"
        assert positions[0].details["collateral_enabled"] is True

    def test_borrow_only(self):
        on_chain = LendingPositionOnChain(
            asset_address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            current_atoken_balance=0,
            current_stable_debt=0,
            current_variable_debt=500_000_000_000_000_000,
            liquidity_rate=0,
            usage_as_collateral_enabled=False,
        )
        positions = _lending_to_position_infos(on_chain, "WETH", "arbitrum")
        assert len(positions) == 1
        assert positions[0].position_type == PositionType.BORROW
        assert positions[0].details["variable_debt_raw"] == "500000000000000000"

    def test_supply_and_borrow(self):
        on_chain = LendingPositionOnChain(
            asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            current_atoken_balance=2_000_000_000,
            current_stable_debt=0,
            current_variable_debt=500_000_000,
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
        )
        positions = _lending_to_position_infos(on_chain, "USDC", "arbitrum")
        assert len(positions) == 2
        types = {p.position_type for p in positions}
        assert types == {PositionType.SUPPLY, PositionType.BORROW}

    def test_empty_position_returns_empty(self):
        on_chain = LendingPositionOnChain(
            asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            current_atoken_balance=0,
            current_stable_debt=0,
            current_variable_debt=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=False,
        )
        positions = _lending_to_position_infos(on_chain, "USDC", "arbitrum")
        assert len(positions) == 0


# =============================================================================
# DiscoveryConfig
# =============================================================================


class TestDiscoveryConfig:
    def test_defaults(self):
        config = DiscoveryConfig(chain="arbitrum", wallet_address="0xabc")
        assert config.protocols == []
        assert config.tracked_tokens == []
        assert config.lp_token_ids == []
        assert config.lp_protocol == "uniswap_v3"

    def test_frozen(self):
        config = DiscoveryConfig(chain="arbitrum", wallet_address="0xabc")
        assert config.chain == "arbitrum"


# =============================================================================
# DiscoveryResult
# =============================================================================


class TestDiscoveryResult:
    def test_empty_has_no_positions(self):
        result = DiscoveryResult()
        assert result.has_positions is False

    def test_with_positions(self):
        result = DiscoveryResult(
            positions=[
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id="test",
                    chain="arbitrum",
                    protocol="aave_v3",
                    value_usd=Decimal("100"),
                )
            ]
        )
        assert result.has_positions is True


# =============================================================================
# PositionDiscoveryService
# =============================================================================


class TestPositionDiscoveryService:
    def test_no_gateway_returns_empty(self):
        """Without gateway, discovery returns empty (lending reader returns None)."""
        service = PositionDiscoveryService(gateway_client=None)
        config = DiscoveryConfig(
            chain="arbitrum",
            wallet_address="0xabc",
            protocols=["aave_v3"],
            tracked_tokens=["USDC"],
        )
        with patch(
            "almanak.framework.valuation.position_discovery.PositionDiscoveryService._resolve_token_addresses",
            return_value={"USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"},
        ):
            result = service.discover(config)
        assert result.has_positions is False
        assert result.lending_assets_scanned == 1

    def test_no_lending_protocol_skips_lending(self):
        """If protocols don't include lending, skip lending scan."""
        service = PositionDiscoveryService(gateway_client=None)
        config = DiscoveryConfig(
            chain="arbitrum",
            wallet_address="0xabc",
            protocols=["uniswap_v3"],
            tracked_tokens=["USDC", "WETH"],
        )
        result = service.discover(config)
        assert result.lending_assets_scanned == 0

    def test_no_lp_ids_skips_lp_scan(self):
        """Without LP token IDs, LP scan is skipped."""
        service = PositionDiscoveryService(gateway_client=None)
        config = DiscoveryConfig(
            chain="arbitrum",
            wallet_address="0xabc",
            protocols=["uniswap_v3"],
            tracked_tokens=["USDC", "WETH"],
            lp_token_ids=[],  # No IDs
        )
        result = service.discover(config)
        assert result.lp_ids_scanned == 0

    def test_lending_discovery_with_mock_gateway(self):
        """Mocked gateway returns active lending position."""
        service = PositionDiscoveryService(gateway_client=None)

        mock_on_chain = LendingPositionOnChain(
            asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            current_atoken_balance=1_500_000_000,
            current_stable_debt=0,
            current_variable_debt=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
        )

        config = DiscoveryConfig(
            chain="arbitrum",
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
            protocols=["aave_v3"],
            tracked_tokens=["USDC"],
        )

        with (
            patch.object(service, "_resolve_token_addresses", return_value={"USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"}),
            patch.object(service._lending_reader, "read_position", return_value=mock_on_chain),
        ):
            result = service.discover(config)

        assert result.has_positions is True
        assert len(result.positions) == 1
        assert result.positions[0].position_type == PositionType.SUPPLY
        assert result.positions[0].position_id == "aave-supply-USDC-arbitrum"
        assert result.positions[0].details["asset_address"] == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        assert result.lending_assets_scanned == 1

    def test_lending_discovery_inactive_skipped(self):
        """Inactive lending positions are not included."""
        service = PositionDiscoveryService(gateway_client=None)

        mock_on_chain = LendingPositionOnChain(
            asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            current_atoken_balance=0,
            current_stable_debt=0,
            current_variable_debt=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=False,
        )

        config = DiscoveryConfig(
            chain="arbitrum",
            wallet_address="0xwallet",
            protocols=["aave_v3"],
            tracked_tokens=["USDC"],
        )

        with (
            patch.object(service, "_resolve_token_addresses", return_value={"USDC": "0xaf88"}),
            patch.object(service._lending_reader, "read_position", return_value=mock_on_chain),
        ):
            result = service.discover(config)

        assert result.has_positions is False
        assert result.lending_assets_scanned == 1

    def test_lending_discovery_multiple_assets(self):
        """Scan multiple tokens, find positions in some."""
        service = PositionDiscoveryService(gateway_client=None)

        active = LendingPositionOnChain(
            asset_address="0xaddr_usdc",
            current_atoken_balance=1_000_000,
            current_stable_debt=0,
            current_variable_debt=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
        )
        inactive = LendingPositionOnChain(
            asset_address="0xaddr_weth",
            current_atoken_balance=0,
            current_stable_debt=0,
            current_variable_debt=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=False,
        )

        config = DiscoveryConfig(
            chain="arbitrum",
            wallet_address="0xwallet",
            protocols=["aave_v3"],
            tracked_tokens=["USDC", "WETH"],
        )

        def mock_read(chain, asset_address, wallet_address):
            if "usdc" in asset_address.lower():
                return active
            return inactive

        with (
            patch.object(
                service,
                "_resolve_token_addresses",
                return_value={"USDC": "0xaddr_usdc", "WETH": "0xaddr_weth"},
            ),
            patch.object(service._lending_reader, "read_position", side_effect=mock_read),
        ):
            result = service.discover(config)

        assert len(result.positions) == 1
        assert result.lending_assets_scanned == 2

    def test_lending_discovery_error_captured(self):
        """Reader exceptions are captured as errors, not raised."""
        service = PositionDiscoveryService(gateway_client=None)

        config = DiscoveryConfig(
            chain="arbitrum",
            wallet_address="0xwallet",
            protocols=["aave_v3"],
            tracked_tokens=["USDC"],
        )

        with (
            patch.object(service, "_resolve_token_addresses", return_value={"USDC": "0xaddr"}),
            patch.object(service._lending_reader, "read_position", side_effect=RuntimeError("RPC timeout")),
        ):
            result = service.discover(config)

        assert result.has_positions is False
        assert len(result.errors) == 1
        assert "RPC timeout" in result.errors[0]

    def test_lp_discovery_with_mock_reader(self):
        """LP discovery returns position for active token ID."""
        service = PositionDiscoveryService(gateway_client=None)

        mock_lp = MagicMock()
        mock_lp.liquidity = 1000000
        mock_lp.tokens_owed0 = 100
        mock_lp.tokens_owed1 = 200
        mock_lp.token0 = "0xtoken0"
        mock_lp.token1 = "0xtoken1"

        config = DiscoveryConfig(
            chain="arbitrum",
            wallet_address="0xwallet",
            protocols=["uniswap_v3"],
            tracked_tokens=["WETH", "USDC"],
            lp_token_ids=[12345],
            lp_protocol="uniswap_v3",
        )

        with patch.object(service._lp_reader, "read_position", return_value=mock_lp):
            result = service.discover(config)

        assert result.has_positions is True
        assert len(result.positions) == 1
        assert result.positions[0].position_type == PositionType.LP
        assert result.positions[0].position_id == "12345"
        assert result.lp_ids_scanned == 1

    def test_lp_discovery_closed_position_skipped(self):
        """LP position with zero liquidity and zero fees is skipped."""
        service = PositionDiscoveryService(gateway_client=None)

        mock_lp = MagicMock()
        mock_lp.liquidity = 0
        mock_lp.tokens_owed0 = 0
        mock_lp.tokens_owed1 = 0

        config = DiscoveryConfig(
            chain="arbitrum",
            wallet_address="0xwallet",
            protocols=["uniswap_v3"],
            lp_token_ids=[12345],
        )

        with patch.object(service._lp_reader, "read_position", return_value=mock_lp):
            result = service.discover(config)

        assert result.has_positions is False
        assert result.lp_ids_scanned == 1

    def test_set_gateway_client_updates_readers(self):
        """set_gateway_client refreshes both readers."""
        service = PositionDiscoveryService(gateway_client=None)
        mock_client = MagicMock()
        service.set_gateway_client(mock_client)
        # After set, readers should be new instances
        assert service._lp_reader is not None
        assert service._lending_reader is not None

    def test_resolve_token_addresses_uses_resolver(self):
        """Token resolution delegates to TokenResolver."""
        service = PositionDiscoveryService(gateway_client=None)

        mock_resolved = MagicMock()
        mock_resolved.address = "0xresolved_address"

        with patch("almanak.framework.data.tokens.get_token_resolver") as mock_get:
            mock_resolver = MagicMock()
            mock_resolver.resolve.return_value = mock_resolved
            mock_get.return_value = mock_resolver

            addresses = service._resolve_token_addresses(["USDC", "WETH"], "arbitrum")

        assert len(addresses) == 2
        assert addresses["USDC"] == "0xresolved_address"
        assert addresses["WETH"] == "0xresolved_address"

    def test_resolve_token_addresses_skips_unresolvable(self):
        """Tokens that can't be resolved are skipped, not errored."""
        service = PositionDiscoveryService(gateway_client=None)

        with patch("almanak.framework.data.tokens.get_token_resolver") as mock_get:
            mock_resolver = MagicMock()
            mock_resolver.resolve.side_effect = Exception("Not found")
            mock_get.return_value = mock_resolver

            addresses = service._resolve_token_addresses(["UNKNOWN_TOKEN"], "arbitrum")

        assert len(addresses) == 0


# =============================================================================
# Portfolio valuer integration — two-source merging
# =============================================================================


class TestPortfolioValuerDiscoveryIntegration:
    """Tests for the portfolio_valuer's _get_positions with discovery."""

    def _make_valuer(self):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        return PortfolioValuer(gateway_client=None)

    def _make_strategy(self, **overrides):
        """Create a mock strategy matching StrategyLike protocol."""
        strategy = MagicMock()
        strategy.strategy_id = overrides.get("strategy_id", "test-strategy")
        strategy.chain = overrides.get("chain", "arbitrum")
        strategy.wallet_address = overrides.get("wallet_address", "0x1234567890abcdef1234567890abcdef12345678")
        strategy._get_tracked_tokens.return_value = overrides.get("tracked_tokens", ["USDC", "WETH"])

        metadata = MagicMock()
        metadata.supported_protocols = overrides.get("protocols", ["aave_v3"])
        strategy.STRATEGY_METADATA = metadata

        if "positions" in overrides:
            from almanak.framework.teardown.models import TeardownPositionSummary
            from datetime import datetime, UTC

            summary = TeardownPositionSummary(
                strategy_id="test-strategy",
                timestamp=datetime.now(UTC),
                positions=overrides["positions"],
            )
            strategy.get_open_positions.return_value = summary
        else:
            del strategy.get_open_positions

        return strategy

    def _make_market(self, prices=None):
        market = MagicMock()
        price_map = prices or {"USDC": Decimal("1.0"), "WETH": Decimal("3000")}
        market.price.side_effect = lambda token, **kw: price_map.get(token, Decimal("0"))
        return market

    def test_discovery_only_no_strategy_positions(self):
        """Discovery finds lending positions without strategy cooperation."""
        valuer = self._make_valuer()
        strategy = self._make_strategy()  # No get_open_positions
        market = self._make_market()

        mock_result = DiscoveryResult(
            positions=[
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id="aave-supply-USDC-arbitrum",
                    chain="arbitrum",
                    protocol="aave_v3",
                    value_usd=Decimal("0"),
                    details={"asset": "USDC", "asset_address": "0xaddr"},
                )
            ],
            lending_assets_scanned=2,
        )

        with patch.object(valuer._discovery, "discover", return_value=mock_result):
            positions, total, unavailable = valuer._get_positions(strategy, market, {})

        assert len(positions) == 1
        assert positions[0].position_type == PositionType.SUPPLY
        assert unavailable is False

    def test_strategy_only_no_discovery(self):
        """Strategy provides positions, discovery finds nothing new."""
        valuer = self._make_valuer()

        strategy_pos = PositionInfo(
            position_type=PositionType.PERP,
            position_id="gmx-perp-1",
            chain="arbitrum",
            protocol="gmx_v2",
            value_usd=Decimal("5000"),
            details={"direction": "LONG"},
        )
        strategy = self._make_strategy(
            positions=[strategy_pos],
            protocols=["gmx_v2"],
        )
        market = self._make_market()

        with patch.object(valuer._discovery, "discover", return_value=DiscoveryResult()):
            positions, total, unavailable = valuer._get_positions(strategy, market, {})

        assert len(positions) == 1
        assert positions[0].position_type == PositionType.PERP
        # Perps pass through strategy value (no repricing for perps yet)
        assert positions[0].value_usd == Decimal("5000")

    def test_deduplication_discovery_enriches_strategy(self):
        """When both sources report the same position, discovery enriches details."""
        valuer = self._make_valuer()

        strategy_pos = PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="aave-supply-USDC-arbitrum",
            chain="arbitrum",
            protocol="aave_v3",
            value_usd=Decimal("1500"),
            details={"asset": "USDC"},  # No asset_address
        )
        strategy = self._make_strategy(
            positions=[strategy_pos],
            protocols=["aave_v3"],
        )
        market = self._make_market()

        discovery_pos = PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="aave-supply-USDC-arbitrum",
            chain="arbitrum",
            protocol="aave_v3",
            value_usd=Decimal("0"),
            details={"asset": "USDC", "asset_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"},
        )
        mock_result = DiscoveryResult(positions=[discovery_pos])

        with patch.object(valuer._discovery, "discover", return_value=mock_result):
            positions, total, unavailable = valuer._get_positions(strategy, market, {})

        assert len(positions) == 1
        # Should have merged details
        assert positions[0].details.get("asset_address") == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        assert positions[0].details.get("asset") == "USDC"

    def test_both_sources_different_positions(self):
        """Strategy reports perp, discovery finds lending — both included."""
        valuer = self._make_valuer()

        perp_pos = PositionInfo(
            position_type=PositionType.PERP,
            position_id="gmx-long-ETH",
            chain="arbitrum",
            protocol="gmx_v2",
            value_usd=Decimal("5000"),
        )
        strategy = self._make_strategy(
            positions=[perp_pos],
            protocols=["aave_v3", "gmx_v2"],
        )
        market = self._make_market()

        lending_pos = PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="aave-supply-USDC-arbitrum",
            chain="arbitrum",
            protocol="aave_v3",
            value_usd=Decimal("0"),
            details={"asset": "USDC"},
        )
        mock_result = DiscoveryResult(positions=[lending_pos])

        with patch.object(valuer._discovery, "discover", return_value=mock_result):
            positions, total, unavailable = valuer._get_positions(strategy, market, {})

        assert len(positions) == 2
        types = {p.position_type for p in positions}
        assert PositionType.PERP in types
        assert PositionType.SUPPLY in types

    def test_discovery_failure_still_returns_strategy_positions(self):
        """If discovery throws, strategy positions still work."""
        valuer = self._make_valuer()

        strategy_pos = PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="aave-supply-USDC-arbitrum",
            chain="arbitrum",
            protocol="aave_v3",
            value_usd=Decimal("1500"),
            details={"asset": "USDC"},
        )
        strategy = self._make_strategy(positions=[strategy_pos], protocols=["aave_v3"])
        market = self._make_market()

        # Discovery completely fails
        with patch.object(valuer, "_build_discovery_config", return_value=None):
            positions, total, unavailable = valuer._get_positions(strategy, market, {})

        assert len(positions) == 1
        assert unavailable is False

    def test_no_wallet_address_skips_discovery(self):
        """Missing wallet address means no discovery config."""
        valuer = self._make_valuer()
        strategy = self._make_strategy(wallet_address="")
        market = self._make_market()

        config = valuer._build_discovery_config(strategy, [])
        assert config is None

    def test_build_discovery_config_extracts_lp_token_ids(self):
        """LP token IDs from strategy positions are forwarded to discovery."""
        valuer = self._make_valuer()

        lp_pos = PositionInfo(
            position_type=PositionType.LP,
            position_id="12345",
            chain="arbitrum",
            protocol="uniswap_v3",
            value_usd=Decimal("2000"),
        )
        strategy = self._make_strategy(
            positions=[lp_pos],
            protocols=["uniswap_v3"],
        )

        config = valuer._build_discovery_config(strategy, [lp_pos])
        assert config is not None
        assert 12345 in config.lp_token_ids
        assert config.lp_protocol == "uniswap_v3"

    def test_build_discovery_config_no_protocols_no_tokens_returns_none(self):
        """No protocols + no tokens = nothing to discover."""
        valuer = self._make_valuer()
        strategy = self._make_strategy(protocols=[], tracked_tokens=[])
        strategy._get_tracked_tokens.return_value = []

        config = valuer._build_discovery_config(strategy, [])
        assert config is None

    def test_empty_result_from_both_sources(self):
        """Both sources return nothing — clean empty result."""
        valuer = self._make_valuer()
        strategy = self._make_strategy()  # No get_open_positions
        market = self._make_market()

        with patch.object(valuer._discovery, "discover", return_value=DiscoveryResult()):
            positions, total, unavailable = valuer._get_positions(strategy, market, {})

        assert positions == []
        assert total == Decimal("0")
        assert unavailable is False


# =============================================================================
# Full valuer.value() integration
# =============================================================================


class TestPortfolioValuerFullIntegration:
    """End-to-end test: strategy + discovery -> PortfolioSnapshot."""

    def test_value_includes_discovered_lending(self):
        """Full pipeline: discovery finds lending, valuer produces snapshot."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=None)

        strategy = MagicMock()
        strategy.strategy_id = "test-lending"
        strategy.chain = "arbitrum"
        strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
        strategy._get_tracked_tokens.return_value = ["USDC"]
        metadata = MagicMock()
        metadata.supported_protocols = ["aave_v3"]
        strategy.STRATEGY_METADATA = metadata
        del strategy.get_open_positions  # No strategy cooperation

        market = MagicMock()
        market.balance.return_value = Decimal("100")  # 100 USDC in wallet
        market.price.return_value = Decimal("1.0")  # $1 per USDC

        # Discovery finds a supply position
        supply_pos = PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="aave-supply-USDC-arbitrum",
            chain="arbitrum",
            protocol="aave_v3",
            value_usd=Decimal("0"),
            details={"asset": "USDC", "asset_address": "0xaddr"},
        )

        with patch.object(valuer._discovery, "discover", return_value=DiscoveryResult(positions=[supply_pos])):
            snapshot = valuer.value(strategy, market)

        # Wallet: $100 + at least one position attempted
        assert snapshot.total_value_usd >= Decimal("100")
        assert snapshot.strategy_id == "test-lending"
