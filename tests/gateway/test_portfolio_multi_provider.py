"""Tests for multi-provider portfolio valuation.

Covers:
- CircuitBreaker: open/close/recovery behavior
- PortfolioProviderChain: fallback, circuit breaking, None on all-fail
- PortfolioProviderConfig parsing: CSV, legacy single-provider, backward compat
- IntegrationService: request.provider pin vs chain semantics
- DashboardService: None-handling in external portfolio total
- MoralisIntegration: response normalization
"""

import os
import time
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.integrations.circuit_breaker import CircuitBreaker
from almanak.gateway.integrations.models import WalletPortfolioSnapshot, WalletPosition
from almanak.gateway.integrations.moralis import MoralisIntegration
from almanak.gateway.integrations.portfolio_chain import (
    PortfolioProviderChain,
    build_portfolio_chain,
    get_portfolio_provider_configs,
)


# =============================================================================
# CircuitBreaker tests
# =============================================================================


class TestCircuitBreaker:
    def test_starts_closed(self):
        cb = CircuitBreaker(failure_threshold=3)
        assert cb.is_open is False

    def test_opens_after_threshold_failures(self):
        cb = CircuitBreaker(failure_threshold=3, failure_window_seconds=60, recovery_seconds=300)
        cb.record_failure()
        cb.record_failure()
        assert cb.is_open is False
        cb.record_failure()
        assert cb.is_open is True

    def test_success_resets_failures(self):
        cb = CircuitBreaker(failure_threshold=2)
        cb.record_failure()
        cb.record_success()
        cb.record_failure()
        assert cb.is_open is False  # Only 1 failure since reset

    def test_recovers_after_cooldown(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_seconds=300)
        cb.record_failure()
        assert cb.is_open is True
        # Simulate cooldown expiry by setting _open_until to past
        cb._open_until = time.monotonic() - 1
        assert cb.is_open is False

    def test_failures_outside_window_are_pruned(self):
        cb = CircuitBreaker(failure_threshold=3, failure_window_seconds=1)
        cb.record_failure()
        cb.record_failure()
        # Simulate time passing beyond the window
        cb._failures = [time.monotonic() - 2, time.monotonic() - 2]
        cb.record_failure()
        # Only 1 failure in window, should still be closed
        assert cb.is_open is False


# =============================================================================
# PortfolioProviderChain tests
# =============================================================================


def _mock_provider(name: str, supports: bool = True, fail: bool = False):
    """Create a mock BaseIntegration that supports portfolio."""
    provider = MagicMock()
    provider.name = name
    # supports_portfolio is a regular method, not async
    provider.supports_portfolio.return_value = supports
    provider.close = AsyncMock()

    snapshot = WalletPortfolioSnapshot(
        provider=name,
        wallet_address="0x1234",
        chain="arbitrum",
        total_value_usd="100.00",
    )

    if fail:
        provider.get_wallet_portfolio = AsyncMock(side_effect=Exception(f"{name} failed"))
        provider.get_wallet_positions = AsyncMock(side_effect=Exception(f"{name} failed"))
    else:
        provider.get_wallet_portfolio = AsyncMock(return_value=snapshot)
        provider.get_wallet_positions = AsyncMock(return_value=snapshot)

    return provider


class TestPortfolioProviderChain:
    @pytest.mark.asyncio
    async def test_returns_first_successful_provider(self):
        p1 = _mock_provider("zerion")
        p2 = _mock_provider("moralis")
        chain = PortfolioProviderChain([p1, p2])

        result = await chain.get_wallet_portfolio("0x1234", "arbitrum")

        assert result is not None
        assert result.provider == "zerion"
        p2.get_wallet_portfolio.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_falls_back_to_second_provider(self):
        p1 = _mock_provider("zerion", fail=True)
        p2 = _mock_provider("moralis")
        chain = PortfolioProviderChain([p1, p2])

        result = await chain.get_wallet_portfolio("0x1234", "arbitrum")

        assert result is not None
        assert result.provider == "moralis"

    @pytest.mark.asyncio
    async def test_returns_none_when_all_fail(self):
        p1 = _mock_provider("zerion", fail=True)
        p2 = _mock_provider("moralis", fail=True)
        chain = PortfolioProviderChain([p1, p2])

        result = await chain.get_wallet_portfolio("0x1234", "arbitrum")

        assert result is None

    @pytest.mark.asyncio
    async def test_circuit_breaker_skips_failed_provider(self):
        p1 = _mock_provider("zerion", fail=True)
        p2 = _mock_provider("moralis")
        chain = PortfolioProviderChain([p1, p2])

        # Trip the circuit breaker (3 failures)
        for _ in range(3):
            await chain.get_wallet_portfolio("0x1234", "arbitrum")

        # Reset call counts
        p1.get_wallet_portfolio.reset_mock()
        p2.get_wallet_portfolio.reset_mock()

        # Now zerion should be skipped (circuit open)
        result = await chain.get_wallet_portfolio("0x1234", "arbitrum")

        assert result is not None
        assert result.provider == "moralis"
        p1.get_wallet_portfolio.assert_not_awaited()  # Skipped due to circuit

    @pytest.mark.asyncio
    async def test_filters_non_portfolio_providers(self):
        p1 = _mock_provider("binance", supports=False)
        p2 = _mock_provider("zerion")
        chain = PortfolioProviderChain([p1, p2])

        assert len(chain.providers) == 1
        assert chain.providers[0].name == "zerion"

    @pytest.mark.asyncio
    async def test_get_provider_by_name(self):
        p1 = _mock_provider("zerion")
        p2 = _mock_provider("moralis")
        chain = PortfolioProviderChain([p1, p2])

        assert chain.get_provider("zerion") is p1
        assert chain.get_provider("moralis") is p2
        assert chain.get_provider("unknown") is None

    @pytest.mark.asyncio
    async def test_get_wallet_positions_fallback(self):
        p1 = _mock_provider("zerion", fail=True)
        p2 = _mock_provider("moralis")
        chain = PortfolioProviderChain([p1, p2])

        result = await chain.get_wallet_positions("0x1234", "arbitrum")

        assert result is not None
        assert result.provider == "moralis"


# =============================================================================
# Config parsing tests
# =============================================================================


class TestPortfolioProviderConfigs:
    def test_csv_parses_multiple_providers(self):
        with patch.dict("os.environ", {"ZERION_API_KEY": "zk_test", "MORALIS_API_KEY": "mk_test"}):
            configs = get_portfolio_provider_configs(
                portfolio_providers_csv="zerion,moralis",
                portfolio_api_key=None,
            )
        assert len(configs) == 2
        assert configs[0].name == "zerion"
        assert configs[0].priority == 0
        assert configs[1].name == "moralis"
        assert configs[1].priority == 1

    def test_legacy_single_provider_fallback(self):
        # Clear ZERION_API_KEY so legacy fallback is used
        with patch.dict("os.environ", {}, clear=False):
            os.environ.pop("ZERION_API_KEY", None)
            configs = get_portfolio_provider_configs(
                portfolio_providers_csv=None,
                portfolio_api_key="zk_legacy",
                portfolio_api_provider="zerion",
            )
        assert len(configs) == 1
        assert configs[0].name == "zerion"
        assert configs[0].api_key == "zk_legacy"

    def test_no_config_returns_empty(self):
        configs = get_portfolio_provider_configs(
            portfolio_providers_csv=None,
            portfolio_api_key=None,
        )
        assert configs == []

    def test_csv_with_chain_filter(self):
        with patch.dict(
            "os.environ",
            {"ZERION_API_KEY": "zk_test", "ZERION_CHAIN_FILTER": "ethereum,arbitrum"},
        ):
            configs = get_portfolio_provider_configs(
                portfolio_providers_csv="zerion",
                portfolio_api_key=None,
            )
        assert configs[0].chain_filter == ["ethereum", "arbitrum"]

    def test_csv_with_custom_cache_ttl(self):
        with patch.dict("os.environ", {"ZERION_API_KEY": "zk_test", "ZERION_CACHE_TTL": "120"}):
            configs = get_portfolio_provider_configs(
                portfolio_providers_csv="zerion",
                portfolio_api_key=None,
            )
        assert configs[0].cache_ttl == 120


# =============================================================================
# build_portfolio_chain tests
# =============================================================================


class TestBuildPortfolioChain:
    def test_builds_zerion_only_from_legacy_key(self):
        chain = build_portfolio_chain(
            portfolio_providers_csv=None,
            portfolio_api_key="zk_test",
        )
        assert chain is not None
        assert len(chain.providers) == 1
        assert chain.providers[0].name == "zerion"

    def test_returns_none_when_no_config(self):
        chain = build_portfolio_chain(
            portfolio_providers_csv=None,
            portfolio_api_key=None,
        )
        assert chain is None

    def test_skips_unconfigured_providers(self):
        # moralis has no API key, should be skipped
        chain = build_portfolio_chain(
            portfolio_providers_csv="zerion,moralis",
            portfolio_api_key="zk_test",
        )
        assert chain is not None
        # Only zerion should be in the chain (moralis has no key)
        assert len(chain.providers) == 1
        assert chain.providers[0].name == "zerion"

    def test_builds_multi_provider_chain(self):
        with patch.dict("os.environ", {"MORALIS_API_KEY": "mk_test"}):
            chain = build_portfolio_chain(
                portfolio_providers_csv="zerion,moralis",
                portfolio_api_key="zk_test",
            )
        assert chain is not None
        assert len(chain.providers) == 2
        names = [p.name for p in chain.providers]
        assert names == ["zerion", "moralis"]


# =============================================================================
# MoralisIntegration normalization tests
# =============================================================================


class TestMoralisNormalization:
    def test_normalize_evm_response(self):
        moralis = MoralisIntegration.__new__(MoralisIntegration)
        moralis.name = "moralis"

        data = [
            {
                "token_address": "0xusdc",
                "name": "USD Coin",
                "symbol": "USDC",
                "decimals": "6",
                "balance": "1000000",
                "usd_price": 1.0,
            },
            {
                "token_address": "0xweth",
                "name": "Wrapped Ether",
                "symbol": "WETH",
                "decimals": "18",
                "balance": "500000000000000000",
                "usd_price": 2000.0,
            },
        ]

        snapshot = moralis._normalize_evm_response("0xwallet", "arbitrum", data)

        assert snapshot.provider == "moralis"
        assert snapshot.chain == "arbitrum"
        assert len(snapshot.positions) == 2
        assert snapshot.positions[0].token_symbols == ["USDC"]
        assert Decimal(snapshot.positions[0].value_usd) == Decimal("1")  # 1M / 10^6 * $1
        assert snapshot.positions[1].token_symbols == ["WETH"]
        assert Decimal(snapshot.positions[1].value_usd) == Decimal("1000")  # 0.5 * $2000
        # Total should be sum
        total = Decimal(snapshot.total_value_usd)
        assert total == Decimal("1001")

    def test_normalize_evm_empty_response(self):
        moralis = MoralisIntegration.__new__(MoralisIntegration)
        moralis.name = "moralis"

        snapshot = moralis._normalize_evm_response("0xwallet", "arbitrum", [])

        assert snapshot.total_value_usd == "0"
        assert snapshot.positions == []

    def test_normalize_solana_response(self):
        moralis = MoralisIntegration.__new__(MoralisIntegration)
        moralis.name = "moralis"

        data = [
            {
                "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "name": "USD Coin",
                "symbol": "USDC",
                "decimals": "6",
                "amount": "5000000",
                "usd_price": 1.0,
            },
        ]

        snapshot = moralis._normalize_solana_response("SolWallet123", "solana", data)

        assert snapshot.provider == "moralis"
        assert snapshot.chain == "solana"
        assert len(snapshot.positions) == 1
        assert Decimal(snapshot.positions[0].value_usd) == Decimal("5")

    def test_handles_missing_usd_price(self):
        moralis = MoralisIntegration.__new__(MoralisIntegration)
        moralis.name = "moralis"

        data = [
            {
                "token_address": "0xunknown",
                "name": "Unknown Token",
                "symbol": "UNK",
                "decimals": "18",
                "balance": "1000000000000000000",
                "usd_price": None,
            },
        ]

        snapshot = moralis._normalize_evm_response("0xwallet", "arbitrum", data)

        assert snapshot.positions[0].value_usd == "0"

    def test_supports_portfolio(self):
        moralis = MoralisIntegration.__new__(MoralisIntegration)
        assert moralis.supports_portfolio() is True

    def test_chain_id_mapping(self):
        assert MoralisIntegration._CHAIN_IDS["ethereum"] == "0x1"
        assert MoralisIntegration._CHAIN_IDS["arbitrum"] == "0xa4b1"
        assert MoralisIntegration._CHAIN_IDS["base"] == "0x2105"
        assert MoralisIntegration._CHAIN_IDS["solana"] == "solana"


# =============================================================================
# IntegrationService pin-vs-chain tests
# =============================================================================


class TestIntegrationServicePortfolio:
    @pytest.fixture
    def service(self):
        from almanak.gateway.services.integration_service import IntegrationServiceServicer

        svc = IntegrationServiceServicer.__new__(IntegrationServiceServicer)
        svc.settings = GatewaySettings(portfolio_api_key="test-key")
        svc._initialized = True
        svc._binance = None
        svc._coingecko = None
        svc._thegraph = None
        svc._zerion = AsyncMock()
        svc._zerion.is_configured = True
        svc._zerion.name = "zerion"

        # Set up portfolio chain with mock providers
        mock_chain = AsyncMock(spec=PortfolioProviderChain)
        mock_chain.get_provider = MagicMock(return_value=None)
        svc._portfolio_chain = mock_chain
        return svc

    def _make_context(self):
        ctx = MagicMock(spec=grpc.aio.ServicerContext)
        ctx.set_code = MagicMock()
        ctx.set_details = MagicMock()
        return ctx

    @pytest.mark.asyncio
    async def test_empty_provider_uses_chain(self, service):
        from almanak.gateway.proto import gateway_pb2

        ctx = self._make_context()
        snapshot = WalletPortfolioSnapshot(
            provider="zerion",
            wallet_address="0x1234567890123456789012345678901234567890",
            chain="arbitrum",
            total_value_usd="100.00",
            fetched_at=datetime(2026, 4, 5, tzinfo=UTC),
        )
        service._portfolio_chain.get_wallet_portfolio = AsyncMock(return_value=snapshot)

        request = gateway_pb2.WalletPortfolioRequest(
            wallet_address="0x1234567890123456789012345678901234567890",
            chain="arbitrum",
            provider="",  # Empty = use chain
        )
        response = await service.GetWalletPortfolio(request, ctx)

        assert response.success is True
        service._portfolio_chain.get_wallet_portfolio.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_explicit_provider_pins(self, service):
        from almanak.gateway.proto import gateway_pb2

        ctx = self._make_context()
        snapshot = WalletPortfolioSnapshot(
            provider="zerion",
            wallet_address="0x1234567890123456789012345678901234567890",
            chain="arbitrum",
            total_value_usd="100.00",
            fetched_at=datetime(2026, 4, 5, tzinfo=UTC),
        )
        # Set up zerion mock to be found via get_provider
        service._portfolio_chain.get_provider = MagicMock(return_value=service._zerion)
        service._zerion.get_wallet_portfolio = AsyncMock(return_value=snapshot)

        request = gateway_pb2.WalletPortfolioRequest(
            wallet_address="0x1234567890123456789012345678901234567890",
            chain="arbitrum",
            provider="zerion",  # Pinned
        )
        response = await service.GetWalletPortfolio(request, ctx)

        assert response.success is True
        # Chain should NOT have been used
        service._portfolio_chain.get_wallet_portfolio.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unknown_provider_returns_error(self, service):
        from almanak.gateway.proto import gateway_pb2

        ctx = self._make_context()
        request = gateway_pb2.WalletPortfolioRequest(
            wallet_address="0x1234567890123456789012345678901234567890",
            chain="arbitrum",
            provider="nonexistent",
        )
        response = await service.GetWalletPortfolio(request, ctx)

        assert response.success is False
        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_all_providers_fail_returns_error(self, service):
        from almanak.gateway.proto import gateway_pb2

        ctx = self._make_context()
        service._portfolio_chain.get_wallet_portfolio = AsyncMock(return_value=None)

        request = gateway_pb2.WalletPortfolioRequest(
            wallet_address="0x1234567890123456789012345678901234567890",
            chain="arbitrum",
        )
        response = await service.GetWalletPortfolio(request, ctx)

        assert response.success is False
        assert "All portfolio providers failed" in response.error

    @pytest.mark.asyncio
    async def test_no_chain_configured_returns_failed_precondition(self, service):
        from almanak.gateway.proto import gateway_pb2

        ctx = self._make_context()
        service._portfolio_chain = None

        request = gateway_pb2.WalletPortfolioRequest(
            wallet_address="0x1234567890123456789012345678901234567890",
            chain="arbitrum",
        )
        response = await service.GetWalletPortfolio(request, ctx)

        assert response.success is False
        ctx.set_code.assert_called_with(grpc.StatusCode.FAILED_PRECONDITION)


# =============================================================================
# DashboardService portfolio chain tests
# =============================================================================


class TestDashboardServicePortfolio:
    @pytest.mark.asyncio
    async def test_none_portfolio_chain_returns_none(self):
        from almanak.gateway.services.dashboard_service import DashboardServiceServicer

        svc = DashboardServiceServicer.__new__(DashboardServiceServicer)
        svc._portfolio_chain = None

        result = await svc._get_external_portfolio_total("arbitrum", "0x1234")
        assert result is None

    @pytest.mark.asyncio
    async def test_all_providers_fail_returns_none(self):
        from almanak.gateway.services.dashboard_service import DashboardServiceServicer

        svc = DashboardServiceServicer.__new__(DashboardServiceServicer)
        mock_chain = AsyncMock(spec=PortfolioProviderChain)
        mock_chain.get_wallet_portfolio = AsyncMock(return_value=None)
        svc._portfolio_chain = mock_chain

        result = await svc._get_external_portfolio_total("arbitrum", "0x1234")
        assert result is None

    @pytest.mark.asyncio
    async def test_successful_portfolio_returns_total(self):
        from almanak.gateway.services.dashboard_service import DashboardServiceServicer

        svc = DashboardServiceServicer.__new__(DashboardServiceServicer)
        snapshot = WalletPortfolioSnapshot(
            provider="zerion",
            wallet_address="0x1234",
            chain="arbitrum",
            total_value_usd="500.00",
        )
        mock_chain = AsyncMock(spec=PortfolioProviderChain)
        mock_chain.get_wallet_portfolio = AsyncMock(return_value=snapshot)
        svc._portfolio_chain = mock_chain

        result = await svc._get_external_portfolio_total("arbitrum", "0x1234")
        assert result == Decimal("500.00")


# =============================================================================
# GatewaySettings integration
# =============================================================================


class TestGatewaySettingsPortfolio:
    def test_portfolio_providers_field_exists(self):
        settings = GatewaySettings()
        assert settings.portfolio_providers is None

    def test_legacy_fields_still_work(self):
        settings = GatewaySettings(portfolio_api_key="zk_test")
        assert settings.portfolio_api_key == "zk_test"
        assert settings.portfolio_api_provider == "zerion"
        assert settings.portfolio_api_cache_ttl == 300


# =============================================================================
# Generic models tests
# =============================================================================


class TestGenericModels:
    def test_wallet_position_creation(self):
        pos = WalletPosition(
            position_id="test-1",
            protocol="uniswap_v3",
            label="USDC/ETH",
            position_type="liquidity_position",
            value_usd="1000.00",
        )
        assert pos.position_id == "test-1"
        assert pos.token_symbols == []
        assert pos.details == {}

    def test_wallet_portfolio_snapshot_creation(self):
        snapshot = WalletPortfolioSnapshot(
            provider="test",
            wallet_address="0x1234",
            chain="arbitrum",
            total_value_usd="5000.00",
        )
        assert snapshot.provider == "test"
        assert snapshot.cache_hit is False
        assert snapshot.positions == []
        assert snapshot.fetched_at is not None

    def test_backward_compatible_aliases(self):
        from almanak.gateway.integrations.zerion import ZerionPosition, ZerionPortfolioSnapshot

        assert ZerionPosition is WalletPosition
        assert ZerionPortfolioSnapshot is WalletPortfolioSnapshot
