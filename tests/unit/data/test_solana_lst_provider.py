"""Tests for SolanaLSTProvider and MarketSnapshot LST integration."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.staking.solana_lst_provider import (
    _LST_MINTS,
    LSTExchangeRate,
    LSTProtocol,
    SolanaLSTProvider,
)

# =============================================================================
# Helper: mock aiohttp response
# =============================================================================


def _mock_aiohttp_response(json_data: dict | list, status: int = 200) -> MagicMock:
    response = MagicMock()
    response.status = status
    response.json = AsyncMock(return_value=json_data)
    response.text = AsyncMock(return_value="error")
    response.__aenter__ = AsyncMock(return_value=response)
    response.__aexit__ = AsyncMock(return_value=False)
    return response


def _mock_session_multi(responses: list[MagicMock]) -> MagicMock:
    """Mock session that returns different responses for sequential get() calls."""
    session = MagicMock()
    session.get = MagicMock(side_effect=responses)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    return session


# =============================================================================
# Sample API response data
# =============================================================================

_JUPITER_PRICE_RESPONSE = {
    "data": {
        _LST_MINTS["jitoSOL"]: {"price": "1.145"},
        _LST_MINTS["mSOL"]: {"price": "1.132"},
        _LST_MINTS["bSOL"]: {"price": "1.098"},
        _LST_MINTS["INF"]: {"price": "1.165"},
    }
}

_SANCTUM_APY_RESPONSE = {
    "apys": {
        _LST_MINTS["jitoSOL"]: 0.0782,  # 7.82% annualized
        _LST_MINTS["mSOL"]: 0.0745,  # 7.45%
        _LST_MINTS["bSOL"]: 0.0698,  # 6.98%
        _LST_MINTS["INF"]: 0.0815,  # 8.15%
    }
}


# =============================================================================
# LSTExchangeRate dataclass tests
# =============================================================================


class TestLSTExchangeRate:
    def test_construction(self):
        rate = LSTExchangeRate(
            symbol="jitoSOL",
            protocol=LSTProtocol.JITO,
            mint=_LST_MINTS["jitoSOL"],
            rate=1.145,
            apy=7.82,
        )
        assert rate.symbol == "jitoSOL"
        assert rate.protocol == LSTProtocol.JITO
        assert rate.rate == 1.145
        assert rate.apy == 7.82
        assert rate.tvl_sol is None

    def test_frozen(self):
        rate = LSTExchangeRate(
            symbol="mSOL",
            protocol=LSTProtocol.MARINADE,
            mint=_LST_MINTS["mSOL"],
            rate=1.132,
        )
        with pytest.raises(AttributeError):
            rate.rate = 2.0  # type: ignore[misc]

    def test_optional_fields(self):
        rate = LSTExchangeRate(
            symbol="bSOL",
            protocol=LSTProtocol.BLAZE,
            mint=_LST_MINTS["bSOL"],
            rate=1.098,
        )
        assert rate.apy is None
        assert rate.tvl_sol is None
        assert rate.observed_at is None


class TestLSTProtocol:
    def test_enum_values(self):
        assert LSTProtocol.JITO.value == "jito"
        assert LSTProtocol.MARINADE.value == "marinade"
        assert LSTProtocol.BLAZE.value == "blaze"
        assert LSTProtocol.SANCTUM_INF.value == "sanctum_inf"


# =============================================================================
# SolanaLSTProvider tests
# =============================================================================


class TestSolanaLSTProviderSymbolResolution:
    def test_resolve_canonical(self):
        provider = SolanaLSTProvider()
        assert provider._resolve_symbol("jitoSOL") == "jitoSOL"
        assert provider._resolve_symbol("mSOL") == "mSOL"
        assert provider._resolve_symbol("INF") == "INF"

    def test_resolve_case_insensitive(self):
        provider = SolanaLSTProvider()
        assert provider._resolve_symbol("jitosol") == "jitoSOL"
        assert provider._resolve_symbol("MSOL") == "mSOL"
        assert provider._resolve_symbol("inf") == "INF"

    def test_resolve_alias(self):
        provider = SolanaLSTProvider()
        assert provider._resolve_symbol("sanctum") == "INF"
        assert provider._resolve_symbol("sanctum_inf") == "INF"

    def test_resolve_unknown(self):
        provider = SolanaLSTProvider()
        with pytest.raises(ValueError, match="Unknown LST symbol"):
            provider._resolve_symbol("UNKNOWN_TOKEN")

    def test_is_lst(self):
        provider = SolanaLSTProvider()
        assert provider.is_lst("jitoSOL") is True
        assert provider.is_lst("mSOL") is True
        assert provider.is_lst("USDC") is False

    def test_get_supported_symbols(self):
        provider = SolanaLSTProvider()
        symbols = provider.get_supported_symbols()
        assert "jitoSOL" in symbols
        assert "mSOL" in symbols
        assert "bSOL" in symbols
        assert "INF" in symbols


class TestSolanaLSTProviderFetch:
    @pytest.mark.asyncio
    async def test_get_all_rates(self):
        provider = SolanaLSTProvider()

        jupiter_resp = _mock_aiohttp_response(_JUPITER_PRICE_RESPONSE)
        sanctum_resp = _mock_aiohttp_response(_SANCTUM_APY_RESPONSE)
        session = _mock_session_multi([jupiter_resp, sanctum_resp])

        with patch("aiohttp.ClientSession", return_value=session):
            rates = await provider.get_all_rates()

        assert "jitoSOL" in rates
        assert "mSOL" in rates
        assert "bSOL" in rates
        assert "INF" in rates

        jito = rates["jitoSOL"]
        assert jito.rate == 1.145
        assert jito.protocol == LSTProtocol.JITO
        assert jito.mint == _LST_MINTS["jitoSOL"]
        assert jito.observed_at is not None

    @pytest.mark.asyncio
    async def test_get_single_rate(self):
        provider = SolanaLSTProvider()

        jupiter_resp = _mock_aiohttp_response(_JUPITER_PRICE_RESPONSE)
        sanctum_resp = _mock_aiohttp_response(_SANCTUM_APY_RESPONSE)
        session = _mock_session_multi([jupiter_resp, sanctum_resp])

        with patch("aiohttp.ClientSession", return_value=session):
            rate = await provider.get_exchange_rate("jitoSOL")

        assert rate.symbol == "jitoSOL"
        assert rate.rate == 1.145

    @pytest.mark.asyncio
    async def test_get_rate_case_insensitive(self):
        provider = SolanaLSTProvider()

        jupiter_resp = _mock_aiohttp_response(_JUPITER_PRICE_RESPONSE)
        sanctum_resp = _mock_aiohttp_response(_SANCTUM_APY_RESPONSE)
        session = _mock_session_multi([jupiter_resp, sanctum_resp])

        with patch("aiohttp.ClientSession", return_value=session):
            rate = await provider.get_exchange_rate("msol")

        assert rate.symbol == "mSOL"
        assert rate.rate == 1.132

    @pytest.mark.asyncio
    async def test_apy_from_sanctum(self):
        provider = SolanaLSTProvider()

        jupiter_resp = _mock_aiohttp_response(_JUPITER_PRICE_RESPONSE)
        sanctum_resp = _mock_aiohttp_response(_SANCTUM_APY_RESPONSE)
        session = _mock_session_multi([jupiter_resp, sanctum_resp])

        with patch("aiohttp.ClientSession", return_value=session):
            rates = await provider.get_all_rates()

        # APY should come from Sanctum /apy/latest endpoint (0.0782 -> 7.82%)
        jito = rates["jitoSOL"]
        assert jito.apy is not None
        assert jito.apy == pytest.approx(7.82, abs=0.01)

    @pytest.mark.asyncio
    async def test_envelope_wrapping(self):
        provider = SolanaLSTProvider()

        jupiter_resp = _mock_aiohttp_response(_JUPITER_PRICE_RESPONSE)
        sanctum_resp = _mock_aiohttp_response(_SANCTUM_APY_RESPONSE)
        session = _mock_session_multi([jupiter_resp, sanctum_resp])

        with patch("aiohttp.ClientSession", return_value=session):
            envelope = await provider.get_exchange_rate_envelope("jitoSOL")

        assert envelope.meta.source == "sanctum_jupiter"
        assert envelope.meta.finality == "off_chain"
        assert envelope.meta.confidence == 0.9
        assert envelope.value.symbol == "jitoSOL"


class TestSolanaLSTProviderCaching:
    @pytest.mark.asyncio
    async def test_cache_hit(self):
        provider = SolanaLSTProvider(cache_ttl=300)

        jupiter_resp = _mock_aiohttp_response(_JUPITER_PRICE_RESPONSE)
        sanctum_resp = _mock_aiohttp_response(_SANCTUM_APY_RESPONSE)
        session = _mock_session_multi([jupiter_resp, sanctum_resp])

        with patch("aiohttp.ClientSession", return_value=session):
            rates1 = await provider.get_all_rates()
            rates2 = await provider.get_all_rates()

        # Second call should use cache - session.get should only be called twice
        # (once for jupiter, once for sanctum)
        assert session.get.call_count == 2
        assert rates1.keys() == rates2.keys()


class TestSolanaLSTProviderErrors:
    @pytest.mark.asyncio
    async def test_jupiter_api_error(self):
        provider = SolanaLSTProvider()

        jupiter_resp = _mock_aiohttp_response({}, status=500)
        sanctum_resp = _mock_aiohttp_response(_SANCTUM_APY_RESPONSE)
        session = _mock_session_multi([jupiter_resp, sanctum_resp])

        with patch("aiohttp.ClientSession", return_value=session):
            with pytest.raises(DataSourceUnavailable):
                await provider.get_all_rates()

    @pytest.mark.asyncio
    async def test_sanctum_api_error_graceful(self):
        """Sanctum failure should be graceful - rates still returned without APY."""
        provider = SolanaLSTProvider()

        jupiter_resp = _mock_aiohttp_response(_JUPITER_PRICE_RESPONSE)
        sanctum_resp = _mock_aiohttp_response({}, status=500)
        session = _mock_session_multi([jupiter_resp, sanctum_resp])

        with patch("aiohttp.ClientSession", return_value=session):
            rates = await provider.get_all_rates()

        # Should still have rates from Jupiter, just no APY
        assert "jitoSOL" in rates
        assert rates["jitoSOL"].rate == 1.145
        assert rates["jitoSOL"].apy is None

    @pytest.mark.asyncio
    async def test_unknown_symbol_error(self):
        provider = SolanaLSTProvider()
        with pytest.raises(ValueError, match="Unknown LST symbol"):
            await provider.get_exchange_rate("INVALID_TOKEN")

    def test_health(self):
        provider = SolanaLSTProvider()
        h = provider.health()
        assert h["successes"] == 0
        assert h["failures"] == 0


# =============================================================================
# MarketSnapshot LST integration tests
# =============================================================================


class TestMarketSnapshotLST:
    def test_lst_no_provider(self):
        from almanak.framework.data.market_snapshot import MarketSnapshot

        ms = MarketSnapshot(chain="solana", wallet_address="0x123")
        with pytest.raises(ValueError, match="No Solana LST provider"):
            ms.lst_exchange_rate("jitoSOL")

    def test_lst_all_rates_no_provider(self):
        from almanak.framework.data.market_snapshot import MarketSnapshot

        ms = MarketSnapshot(chain="solana", wallet_address="0x123")
        with pytest.raises(ValueError, match="No Solana LST provider"):
            ms.lst_all_rates()

    def test_lst_delegates(self):
        from almanak.framework.data.market_snapshot import MarketSnapshot

        provider = MagicMock()
        rate = LSTExchangeRate(
            symbol="jitoSOL",
            protocol=LSTProtocol.JITO,
            mint=_LST_MINTS["jitoSOL"],
            rate=1.145,
            apy=7.82,
        )
        provider.get_exchange_rate = AsyncMock(return_value=rate)

        ms = MarketSnapshot(
            chain="solana",
            wallet_address="0x123",
            solana_lst_provider=provider,
        )
        result = ms.lst_exchange_rate("jitoSOL")

        assert result.symbol == "jitoSOL"
        assert result.rate == 1.145
        provider.get_exchange_rate.assert_called_once_with("jitoSOL")

    def test_lst_all_rates_delegates(self):
        from almanak.framework.data.market_snapshot import MarketSnapshot

        provider = MagicMock()
        rates = {
            "jitoSOL": LSTExchangeRate(
                symbol="jitoSOL",
                protocol=LSTProtocol.JITO,
                mint=_LST_MINTS["jitoSOL"],
                rate=1.145,
            ),
        }
        provider.get_all_rates = AsyncMock(return_value=rates)

        ms = MarketSnapshot(
            chain="solana",
            wallet_address="0x123",
            solana_lst_provider=provider,
        )
        result = ms.lst_all_rates()

        assert "jitoSOL" in result
        provider.get_all_rates.assert_called_once()

    def test_lst_error_wrapping(self):
        from almanak.framework.data.market_snapshot import (
            LSTDataUnavailableError,
            MarketSnapshot,
        )

        provider = MagicMock()
        provider.get_exchange_rate = AsyncMock(side_effect=DataSourceUnavailable(source="test", reason="fail"))

        ms = MarketSnapshot(
            chain="solana",
            wallet_address="0x123",
            solana_lst_provider=provider,
        )
        with pytest.raises(LSTDataUnavailableError):
            ms.lst_exchange_rate("jitoSOL")

    def test_lst_unknown_symbol_passthrough(self):
        from almanak.framework.data.market_snapshot import MarketSnapshot

        provider = MagicMock()
        provider.get_exchange_rate = AsyncMock(side_effect=ValueError("Unknown LST symbol 'FAKE'"))

        ms = MarketSnapshot(
            chain="solana",
            wallet_address="0x123",
            solana_lst_provider=provider,
        )
        with pytest.raises(ValueError, match="Unknown LST symbol"):
            ms.lst_exchange_rate("FAKE")


# =============================================================================
# YieldAggregator Solana chain support tests
# =============================================================================


class TestYieldAggregatorSolanaSupport:
    """Verify that YieldAggregator now supports Solana chain and LST protocols."""

    def test_solana_chain_mapping(self):
        from almanak.framework.data.yields.aggregator import _CHAIN_TO_LLAMA_DISPLAY

        assert "solana" in _CHAIN_TO_LLAMA_DISPLAY
        assert _CHAIN_TO_LLAMA_DISPLAY["solana"] == "Solana"

    def test_jito_protocol_mapping(self):
        from almanak.framework.data.yields.aggregator import _PROTOCOL_TO_LLAMA

        assert "jito" in _PROTOCOL_TO_LLAMA
        assert _PROTOCOL_TO_LLAMA["jito"] == "jito"

    def test_marinade_protocol_mapping(self):
        from almanak.framework.data.yields.aggregator import _PROTOCOL_TO_LLAMA

        assert "marinade" in _PROTOCOL_TO_LLAMA
        assert _PROTOCOL_TO_LLAMA["marinade"] == "marinade-finance"

    def test_jito_staking_type(self):
        from almanak.framework.data.yields.aggregator import _PROJECT_TYPE

        assert _PROJECT_TYPE["jito"] == "staking"
        assert _PROJECT_TYPE["marinade-finance"] == "staking"
        assert _PROJECT_TYPE["sanctum-infinity"] == "staking"

    def test_solana_yield_query(self):
        """Test that Solana yields can be queried through YieldAggregator."""
        from almanak.framework.data.yields.aggregator import YieldAggregator

        agg = YieldAggregator()

        jito_pool = {
            "pool": "jito-sol-staking",
            "chain": "Solana",
            "project": "jito",
            "symbol": "JITOSOL",
            "tvlUsd": 2000000000,
            "apy": 7.82,
            "apyBase": 7.82,
            "apyReward": None,
            "ilRisk": False,
        }
        response = _mock_aiohttp_response({"data": [jito_pool]})
        session = MagicMock()
        session.get = MagicMock(return_value=response)
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession", return_value=session):
            result = agg.get_yield_opportunities("JITOSOL", chains=["solana"])

        opps = result.value
        assert len(opps) == 1
        assert opps[0].protocol == "jito"
        assert opps[0].type == "staking"
        assert opps[0].chain == "solana"
        assert opps[0].apy == 7.82
