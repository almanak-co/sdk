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

    def test_skips_unconfigured_providers(self, monkeypatch):
        # moralis has no API key, should be skipped
        monkeypatch.delenv("MORALIS_API_KEY", raising=False)
        monkeypatch.delenv("ALMANAK_GATEWAY_MORALIS_API_KEY", raising=False)
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
    def test_normalize_evm_response_new_format(self):
        """New v2.2 /wallets/{address}/tokens format with usd_value pre-calculated."""
        moralis = MoralisIntegration.__new__(MoralisIntegration)
        moralis.name = "moralis"

        data = {
            "result": [
                {
                    "token_address": "0xusdc",
                    "name": "USD Coin",
                    "symbol": "USDC",
                    "decimals": "6",
                    "balance": "1000000",
                    "balance_formatted": "1.0",
                    "usd_price": 1.0,
                    "usd_value": 1.0,
                    "native_token": False,
                    "possible_spam": False,
                },
                {
                    "token_address": "0xweth",
                    "name": "Wrapped Ether",
                    "symbol": "WETH",
                    "decimals": "18",
                    "balance": "500000000000000000",
                    "balance_formatted": "0.5",
                    "usd_price": 2000.0,
                    "usd_value": 1000.0,
                    "native_token": False,
                    "possible_spam": False,
                },
            ]
        }

        snapshot = moralis._normalize_evm_response("0xwallet", "arbitrum", data)

        assert snapshot.provider == "moralis"
        assert snapshot.chain == "arbitrum"
        assert len(snapshot.positions) == 2
        assert snapshot.positions[0].token_symbols == ["USDC"]
        assert Decimal(snapshot.positions[0].value_usd) == Decimal("1")
        assert snapshot.positions[1].token_symbols == ["WETH"]
        assert Decimal(snapshot.positions[1].value_usd) == Decimal("1000")
        total = Decimal(snapshot.total_value_usd)
        assert total == Decimal("1001")

    def test_normalize_evm_response_legacy_flat_list(self):
        """Legacy flat-list format (backward compatibility)."""
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
        ]

        snapshot = moralis._normalize_evm_response("0xwallet", "arbitrum", data)

        assert len(snapshot.positions) == 1
        assert Decimal(snapshot.positions[0].value_usd) == Decimal("1")

    def test_normalize_evm_filters_spam(self):
        """Spam tokens are filtered out."""
        moralis = MoralisIntegration.__new__(MoralisIntegration)
        moralis.name = "moralis"

        data = {
            "result": [
                {
                    "token_address": "0xusdc",
                    "name": "USD Coin",
                    "symbol": "USDC",
                    "usd_value": 1.0,
                    "possible_spam": False,
                },
                {
                    "token_address": "0xspam",
                    "name": "Free Money",
                    "symbol": "SCAM",
                    "usd_value": 99999.0,
                    "possible_spam": True,
                },
            ]
        }

        snapshot = moralis._normalize_evm_response("0xwallet", "arbitrum", data)

        assert len(snapshot.positions) == 1
        assert snapshot.positions[0].token_symbols == ["USDC"]

    def test_normalize_evm_native_token(self):
        """Native tokens are handled via native_token flag."""
        moralis = MoralisIntegration.__new__(MoralisIntegration)
        moralis.name = "moralis"

        data = {
            "result": [
                {
                    "token_address": "",
                    "name": "Ether",
                    "symbol": "ETH",
                    "usd_value": 2000.0,
                    "native_token": True,
                    "possible_spam": False,
                },
            ]
        }

        snapshot = moralis._normalize_evm_response("0xwallet", "ethereum", data)

        assert len(snapshot.positions) == 1
        assert snapshot.positions[0].position_id == "moralis:native"
        assert snapshot.positions[0].details.get("native_token") is True

    def test_normalize_evm_empty_response(self):
        moralis = MoralisIntegration.__new__(MoralisIntegration)
        moralis.name = "moralis"

        snapshot = moralis._normalize_evm_response("0xwallet", "arbitrum", {"result": []})

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

    def test_handles_missing_usd_price_with_fallback_calc(self):
        """When usd_value is absent, falls back to manual calc from balance+price."""
        moralis = MoralisIntegration.__new__(MoralisIntegration)
        moralis.name = "moralis"

        data = {
            "result": [
                {
                    "token_address": "0xunknown",
                    "name": "Unknown Token",
                    "symbol": "UNK",
                    "decimals": "18",
                    "balance": "1000000000000000000",
                    "usd_price": None,
                    # No usd_value field
                },
            ]
        }

        snapshot = moralis._normalize_evm_response("0xwallet", "arbitrum", data)

        assert snapshot.positions[0].value_usd == "0"

    def test_supports_portfolio(self):
        moralis = MoralisIntegration.__new__(MoralisIntegration)
        assert moralis.supports_portfolio() is True

    def test_chain_slug_mapping(self):
        """EVM chains map to Moralis slugs via the registry (VIB-4851 B1)."""
        _m = MoralisIntegration.__new__(MoralisIntegration)
        assert _m._get_chain_slug("ethereum") == "eth"
        assert _m._get_chain_slug("arbitrum") == "arbitrum"
        assert _m._get_chain_slug("base") == "base"
        assert _m._get_chain_slug("bnb") == "bsc"  # alias resolves via the registry
        assert _m._get_chain_slug("binance") == "bsc"  # bsc declares both bnb + binance aliases
        # Solana has no Moralis slug; detection routes through the registry (_is_solana).
        assert _m._get_chain_slug("solana") is None
        assert _m._is_solana("solana") is True
        assert _m._is_solana("ethereum") is False

    def test_normalize_net_worth_response(self):
        moralis = MoralisIntegration.__new__(MoralisIntegration)
        moralis.name = "moralis"

        data = {
            "total_networth_usd": "38.50",
            "chains": [{"chain": "arbitrum", "networth_usd": "38.50"}],
        }

        snapshot = moralis._normalize_net_worth_response("0xwallet", "arbitrum", data)

        assert snapshot.total_value_usd == "38.50"
        assert snapshot.positions == []

    def test_normalize_defi_response(self):
        moralis = MoralisIntegration.__new__(MoralisIntegration)
        moralis.name = "moralis"

        data = [
            {
                "protocol_name": "Aave V3",
                "protocol_id": "aave-v3",
                "position": {
                    "label": "USDC Supply",
                    "tokens": [{"symbol": "USDC"}, {"symbol": "aUSDC"}],
                    "balance_usd": 100.0,
                    "total_unclaimed_usd_value": 2.5,
                },
                "position_details": {"pool_address": "0xpool123"},
            }
        ]

        positions = moralis._normalize_defi_response(data)

        assert len(positions) == 1
        assert positions[0].protocol == "Aave V3"
        assert positions[0].label == "USDC Supply"
        assert positions[0].token_symbols == ["USDC", "aUSDC"]
        assert Decimal(positions[0].value_usd) == Decimal("102.5")
        assert positions[0].pool_address == "0xpool123"
        assert positions[0].position_type == "defi"

    def test_normalize_defi_response_empty(self):
        moralis = MoralisIntegration.__new__(MoralisIntegration)
        moralis.name = "moralis"

        assert moralis._normalize_defi_response([]) == []
        assert moralis._normalize_defi_response(None) == []

    @pytest.mark.asyncio
    async def test_get_wallet_positions_merges_tokens_and_defi(self):
        """get_wallet_positions merges token balances + DeFi positions."""
        moralis = MoralisIntegration.__new__(MoralisIntegration)
        moralis.name = "moralis"
        moralis._api_key = "test"
        moralis.default_cache_ttl = 60
        moralis._cache = {}

        token_snapshot = WalletPortfolioSnapshot(
            provider="moralis",
            wallet_address="0xwallet",
            chain="arbitrum",
            total_value_usd="10",
            positions=[
                WalletPosition(
                    position_id="moralis:0xusdc",
                    protocol="wallet",
                    label="USDC",
                    position_type="token",
                    value_usd="10",
                )
            ],
        )
        defi_positions = [
            WalletPosition(
                position_id="moralis:defi:aave:supply",
                protocol="Aave V3",
                label="USDC Supply",
                position_type="defi",
                value_usd="50",
            )
        ]

        with (
            patch.object(moralis, "_fetch_evm_tokens", return_value=token_snapshot),
            patch.object(moralis, "_fetch_defi_positions", return_value=defi_positions),
        ):
            result = await moralis.get_wallet_positions("0xwallet", "arbitrum")

        assert len(result.positions) == 2
        assert Decimal(result.total_value_usd) == Decimal("60")

    @pytest.mark.asyncio
    async def test_get_wallet_positions_defi_failure_is_best_effort(self):
        """DeFi failure should not break the whole positions call."""
        moralis = MoralisIntegration.__new__(MoralisIntegration)
        moralis.name = "moralis"
        moralis._api_key = "test"
        moralis.default_cache_ttl = 60
        moralis._cache = {}

        token_snapshot = WalletPortfolioSnapshot(
            provider="moralis",
            wallet_address="0xwallet",
            chain="arbitrum",
            total_value_usd="10",
            positions=[
                WalletPosition(
                    position_id="moralis:0xusdc",
                    protocol="wallet",
                    label="USDC",
                    position_type="token",
                    value_usd="10",
                )
            ],
        )

        with (
            patch.object(moralis, "_fetch_evm_tokens", return_value=token_snapshot),
            patch.object(moralis, "_fetch_defi_positions", side_effect=Exception("403 Forbidden")),
        ):
            result = await moralis.get_wallet_positions("0xwallet", "arbitrum")

        assert len(result.positions) == 1
        assert Decimal(result.total_value_usd) == Decimal("10")


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
