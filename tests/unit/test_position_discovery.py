"""Tests for position discovery service and portfolio valuer integration.

Covers:
- position_discovery.py: DiscoveryConfig, PositionDiscoveryService, helpers
- portfolio_valuer.py: two-source position merging and discovery integration
"""

import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.position_discovery import (
    DiscoveryConfig,
    DiscoveryResult,
    PositionDiscoveryService,
    _has_lending_protocol,
    _has_lp_protocol,
    _lending_protocols_to_scan,
    _lending_to_position_infos,
)
from almanak.framework.valuation.lending_position_reader import LendingPositionOnChain


# =============================================================================
# Helper protocol matchers
# =============================================================================


class TestHasLendingProtocol:
    def test_aave_v3(self):
        assert _has_lending_protocol(["aave_v3"]) is True

    def test_spark_supported(self):
        """Spark has its own connector-owned lending read — now discoverable."""
        assert _has_lending_protocol(["spark"]) is True

    def test_compound_v3_not_supported(self):
        """Compound V3 has no connector-owned single-reserve lending read."""
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
        positions = _lending_to_position_infos(on_chain, "USDC", "arbitrum", "0xwallet123", protocol="aave_v3")
        assert len(positions) == 1
        assert positions[0].position_type == PositionType.SUPPLY
        assert positions[0].position_id == "aave_v3-supply-USDC-arbitrum"
        assert positions[0].protocol == "aave_v3"
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
        positions = _lending_to_position_infos(on_chain, "WETH", "arbitrum", protocol="aave_v3")
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
        positions = _lending_to_position_infos(on_chain, "USDC", "arbitrum", protocol="aave_v3")
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
        positions = _lending_to_position_infos(on_chain, "USDC", "arbitrum", protocol="aave_v3")
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
        assert result.positions[0].position_id == "aave_v3-supply-USDC-arbitrum"
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

        def mock_read(chain, asset_address, wallet_address, protocol=None):
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
        strategy.deployment_id = overrides.get("deployment_id", "test-strategy")
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
                deployment_id="test-strategy",
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
        # VIB-4584 / F3.1: the discovered SUPPLY has ``value_usd=0`` and the
        # lending repricer can't run without an RPC in unit tests, so no
        # source provided a value — the position is flagged
        # ``valuation_status='no_path'`` and ``unavailable=True``. Strategy-
        # reported positions with ``value_usd>0`` (see
        # ``test_discovery_failure_still_returns_strategy_positions``) take
        # the trust-the-strategy path and remain ``unavailable=False``.
        assert unavailable is True
        assert positions[0].details.get("valuation_status") == "no_path"

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

    def test_value_unknown_protocol_lp_marks_snapshot_unavailable(self):
        """VIB-4584 / F3.1 — an LP position whose protocol has no registered
        valuation path (e.g. Aerodrome CL, Uniswap V4) must yield a snapshot
        stamped ``value_confidence='UNAVAILABLE'`` and a per-position
        ``details['valuation_status'] = 'no_path'`` marker. A reader cannot
        distinguish "measured zero" from "we have no idea" without this.
        """
        from almanak.framework.teardown.models import TeardownPositionSummary
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer
        from almanak.framework.portfolio.models import ValueConfidence
        from datetime import datetime, UTC

        valuer = PortfolioValuer(gateway_client=None)

        # Strategy reports one LP on a fictional protocol. _lp_reader will
        # return None because no protocol-specific reader matches.
        lp_pos = PositionInfo(
            position_type=PositionType.LP,
            position_id="future-dex-token-1",
            chain="arbitrum",
            protocol="future_dex_v9",
            value_usd=Decimal("0"),
            details={"token_id": "1"},
        )
        strategy = MagicMock()
        strategy.deployment_id = "test-unknown-proto"
        strategy.chain = "arbitrum"
        strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
        # Empty tracked-tokens → no wallet balance → the snapshot has no
        # alternative data source. Combined with the unvalued LP, the
        # confidence MUST drop to UNAVAILABLE.
        strategy._get_tracked_tokens.return_value = []
        metadata = MagicMock()
        metadata.supported_protocols = ["future_dex_v9"]
        strategy.STRATEGY_METADATA = metadata
        summary = TeardownPositionSummary(
            deployment_id="test-unknown-proto",
            timestamp=datetime.now(UTC),
            positions=[lp_pos],
        )
        strategy.get_open_positions.return_value = summary

        market = MagicMock()
        eth_stub = MagicMock()
        eth_stub.balance = Decimal("0")
        market.balance.side_effect = lambda sym: eth_stub
        market.price.side_effect = lambda sym, *a, **kw: Decimal("0")

        # Force the LP reader's read_position to return None for the unknown
        # protocol — this is the production failure mode for V4 / Aerodrome CL.
        with (
            patch.object(valuer._lp_reader, "read_position", return_value=None),
            patch.object(valuer._discovery, "discover", return_value=DiscoveryResult()),
        ):
            snapshot = valuer.value(strategy, market)

        assert snapshot.value_confidence == ValueConfidence.UNAVAILABLE
        assert len(snapshot.positions) == 1
        assert snapshot.positions[0].details.get("valuation_status") == "no_path"
        assert snapshot.positions[0].value_usd == Decimal("0")

    def test_value_includes_discovered_lending(self):
        """Full pipeline: discovery finds lending, valuer produces snapshot."""
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer(gateway_client=None)

        strategy = MagicMock()
        strategy.deployment_id = "test-lending"
        strategy.chain = "arbitrum"
        strategy.wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
        strategy._get_tracked_tokens.return_value = ["USDC"]
        metadata = MagicMock()
        metadata.supported_protocols = ["aave_v3"]
        strategy.STRATEGY_METADATA = metadata
        del strategy.get_open_positions  # No strategy cooperation

        market = MagicMock()
        # Per-symbol balance/price so the gas-native helper (VIB-4225 ACC-02)
        # gets a deterministic ETH=0 row instead of inheriting the
        # MagicMock-default $100 USDC value, which would silently double the
        # wallet total. Tracked-token loop reads USDC; gas helper reads ETH.
        balance_stub = MagicMock()
        balance_stub.balance = Decimal("100")
        eth_stub = MagicMock()
        eth_stub.balance = Decimal("0")
        market.balance.side_effect = lambda sym: eth_stub if sym == "ETH" else balance_stub
        market.price.side_effect = lambda sym, *a, **kw: (
            Decimal("3500") if sym == "ETH" else Decimal("1.0")
        )

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

        # VIB-3614: total_value_usd is position-scoped (position has value_usd=0 here)
        # Wallet $100 shows in available_cash_usd / wallet_total_value_usd
        assert snapshot.total_value_usd == Decimal("0")
        assert snapshot.available_cash_usd == Decimal("100")
        assert snapshot.wallet_total_value_usd == Decimal("100")
        assert snapshot.deployment_id == "test-lending"


# =============================================================================
# Aave-fork protocol routing (Spark data providers) — regression
# =============================================================================

# Ethereum single-reserve data providers, sourced from each connector's
# addresses.py. DISTINCT per protocol — discovery must query each protocol's
# OWN contract, never silently default Spark to Aave V3.
# Intentionally duplicated from the connector address tables for test isolation
# (the routing assertion fails closed if a wrong provider is queried); keep
# these in sync by hand if a connector's pool_data_provider ever changes.
_ETH_AAVE_DATA_PROVIDER = "0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3"
_ETH_SPARK_DATA_PROVIDER = "0xFc21d6d146E6086B8359705C8b28512a983db0cb"


def _gateway_capturing_eth_call_target(captured: list[str], supply_wei: int = 1_000_000):
    """Fake gateway whose ``_rpc_stub.Call`` records each eth_call target.

    Records ``params[0]["to"]`` (the contract the reader queries) into
    ``captured`` and returns a valid 9-word ``getUserReserveData`` response
    (word 0 = currentATokenBalance = ``supply_wei``) so a discovery scan runs
    end-to-end and yields an active SUPPLY position.
    """

    def _call(request, timeout=None):
        params = json.loads(request.params)
        captured.append(params[0]["to"])
        hex_payload = "0x" + f"{supply_wei:064x}" + "0" * (64 * 8)
        resp = MagicMock()
        resp.success = True
        resp.result = json.dumps(hex_payload)
        return resp

    stub = MagicMock()
    stub.Call.side_effect = _call
    gw = MagicMock()
    gw._rpc_stub = stub
    gw.config = SimpleNamespace(timeout=7)
    return gw


class TestLendingDiscoveryProtocolRouting:
    """Regression (follow-up to PR #2533): discovery must scan EACH declared
    lending protocol against its OWN data provider and stamp the real protocol.

    Before ``read_position`` was threaded a ``protocol``, every discovered
    reserve defaulted to the registry's default (aave_v3) and silently queried
    Aave's ``pool_data_provider`` — wrong balances for Spark on every
    chain where the addresses differ. These tests drive the real
    ``LendingReadRegistry`` -> ``AddressRegistry`` -> connector address tables,
    so they fail closed if the routing regresses to Aave-by-default.
    """

    _WALLET = "0x" + "1" * 40
    _USDC = "0x" + "a" * 40

    def _discover(self, protocols, captured):
        gw = _gateway_capturing_eth_call_target(captured)
        service = PositionDiscoveryService(gateway_client=gw)
        config = DiscoveryConfig(
            chain="ethereum",
            wallet_address=self._WALLET,
            protocols=protocols,
            tracked_tokens=["USDC"],
        )
        with patch.object(service, "_resolve_token_addresses", return_value={"USDC": self._USDC}):
            return service.discover(config)

    def test_spark_discovery_queries_spark_provider_not_aave(self):
        captured: list[str] = []
        result = self._discover(["spark"], captured)
        assert captured, "Spark discovery made no eth_call"
        assert captured[0].lower() == _ETH_SPARK_DATA_PROVIDER.lower()
        assert captured[0].lower() != _ETH_AAVE_DATA_PROVIDER.lower()
        # The discovered position is stamped with the REAL protocol + id.
        assert result.has_positions
        assert all(p.protocol == "spark" for p in result.positions)
        assert result.positions[0].position_id == "spark-supply-USDC-ethereum"

    def test_aave_discovery_queries_aave_provider(self):
        """Control: aave_v3 still routes to Aave's provider — routing is
        protocol-sensitive, not hardcoded to either fork."""
        captured: list[str] = []
        self._discover(["aave_v3"], captured)
        assert captured
        assert captured[0].lower() == _ETH_AAVE_DATA_PROVIDER.lower()
        assert captured[0].lower() != _ETH_SPARK_DATA_PROVIDER.lower()

    def test_multi_protocol_fans_out_to_each_provider(self):
        """A strategy declaring two lending markets scans BOTH, each routed to
        its own data provider, each position stamped with its own protocol."""
        captured: list[str] = []
        result = self._discover(["spark", "aave_v3"], captured)
        targets = {c.lower() for c in captured}
        assert _ETH_AAVE_DATA_PROVIDER.lower() in targets
        assert _ETH_SPARK_DATA_PROVIDER.lower() in targets
        # Two protocols x one token = two reserve reads.
        assert result.lending_assets_scanned == 2
        assert {p.protocol for p in result.positions} == {"aave_v3", "spark"}

    def test_undeclared_lending_protocol_not_scanned(self):
        """compound_v3 has no connector-owned single-reserve read — discovery
        must not query any Aave-fork provider on its behalf."""
        captured: list[str] = []
        result = self._discover(["compound_v3"], captured)
        assert captured == []
        assert result.lending_assets_scanned == 0


class TestLendingProtocolsToScan:
    """Unit coverage for the registry-driven scan-set computation."""

    def test_intersection_with_declared(self):
        assert _lending_protocols_to_scan(["aave_v3", "uniswap_v3"]) == ["aave_v3"]

    def test_alias_resolves_to_canonical(self):
        assert _lending_protocols_to_scan(["aave"]) == ["aave_v3"]

    def test_deterministic_registry_order(self):
        # supported_protocols() order is sorted; declaration order must not leak.
        assert _lending_protocols_to_scan(["spark", "aave_v3"]) == ["aave_v3", "spark"]

    def test_unsupported_dropped(self):
        assert _lending_protocols_to_scan(["compound_v3", "morpho_blue"]) == []

    def test_empty(self):
        assert _lending_protocols_to_scan([]) == []
