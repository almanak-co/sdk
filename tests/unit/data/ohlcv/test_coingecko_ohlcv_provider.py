"""Tests for the CoinGecko OHLCV provider + provider-chain invariant (VIB-4847).

Covers:

- Gateway-side ``CoinGeckoOHLCVProvider`` (egress layer) against a mocked
  ``CoinGeckoIntegration`` — symbol resolution, days-window selection,
  aggregation, price-only candles, and rejection of sub-hour timeframes /
  unknown tokens.
- Framework-side thin client ``GatewayCoinGeckoOHLCVProvider`` over a mocked
  gateway gRPC channel.
- The provider-chain ↔ registry invariant guard: every name in
  ``_PROVIDER_CHAINS`` must be registered in the factory (this would have
  FAILED before VIB-4847 when ``coingecko``/``defillama`` were dangling).
- A **real-wiring** failover test: a stale Binance response falls through the
  real factory-built router to the CoinGecko provider (not a hand-mocked
  chain), and a permanent ``DATA_ERROR`` no longer results.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, OHLCVCandle
from almanak.framework.data.ohlcv.factory import (
    assert_provider_chains_registered,
    create_ohlcv_stack,
)
from almanak.framework.data.ohlcv.gateway_data_adapter import CoinGeckoGatewayDataProvider
from almanak.framework.data.ohlcv.gateway_provider import GatewayCoinGeckoOHLCVProvider
from almanak.framework.data.ohlcv.ohlcv_router import (
    _PROVIDER_CHAINS,
    OHLCVRouter,
    provider_names_in_chains,
)
from almanak.framework.gateway_client import GatewayClient
from almanak.gateway.data.ohlcv.coingecko_provider import CoinGeckoOHLCVProvider
from almanak.gateway.proto import gateway_pb2

# =============================================================================
# Helpers
# =============================================================================


def _ohlc_rows(*, count: int, end_ms: int, step_s: int) -> list[list[float]]:
    """CoinGecko-style ``[ts_ms, o, h, l, c]`` rows (price-only)."""
    rows: list[list[float]] = []
    for i in range(count):
        ts = end_ms - (count - 1 - i) * step_s * 1000
        close = 100.0 + i
        rows.append([ts, close - 1, close + 1, close - 2, close])
    return rows


def _make_mock_client() -> MagicMock:
    client = MagicMock(spec=GatewayClient)
    client.integration = MagicMock()
    client.config = MagicMock()
    client.config.timeout = 30
    return client


# =============================================================================
# Gateway-side CoinGeckoOHLCVProvider (egress layer)
# =============================================================================


class TestCoinGeckoOHLCVProvider:
    @pytest.fixture
    def mock_integration(self) -> MagicMock:
        integ = MagicMock()
        integ.get_ohlc = AsyncMock()
        return integ

    @pytest.mark.asyncio
    async def test_1h_aggregates_native_30m_candles(self, mock_integration):
        """``1h`` is served by aggregating CoinGecko's native 30m candles."""
        now_ms = int(time.time() * 1000)
        # 6 native 30m candles -> 3 hourly buckets.
        mock_integration.get_ohlc.return_value = _ohlc_rows(count=6, end_ms=now_ms, step_s=1800)
        provider = CoinGeckoOHLCVProvider(integration=mock_integration)

        candles = await provider.get_ohlcv("WETH", timeframe="1h", limit=10)

        assert all(isinstance(c, OHLCVCandle) for c in candles)
        # days=1 window was requested for 1h.
        assert mock_integration.get_ohlc.call_args.kwargs["days"] == "1"
        # All candles carry NO volume (CoinGecko OHLC is price-only).
        assert all(c.volume is None for c in candles)
        # Aggregated candle count is fewer than native input.
        assert 0 < len(candles) <= 6

    @pytest.mark.asyncio
    async def test_4h_native_no_aggregation(self, mock_integration):
        now_ms = int(time.time() * 1000)
        mock_integration.get_ohlc.return_value = _ohlc_rows(count=5, end_ms=now_ms, step_s=4 * 3600)
        provider = CoinGeckoOHLCVProvider(integration=mock_integration)

        candles = await provider.get_ohlcv("ARB", timeframe="4h", limit=5)

        assert mock_integration.get_ohlc.call_args.kwargs["days"] == "30"
        assert len(candles) == 5

    @pytest.mark.asyncio
    async def test_symbol_resolves_to_coingecko_id(self, mock_integration):
        now_ms = int(time.time() * 1000)
        mock_integration.get_ohlc.return_value = _ohlc_rows(count=2, end_ms=now_ms, step_s=1800)
        provider = CoinGeckoOHLCVProvider(integration=mock_integration)

        await provider.get_ohlcv("WETH", timeframe="1h", limit=2)

        # WETH must resolve to a CoinGecko coin id, not be passed as the symbol.
        assert mock_integration.get_ohlc.call_args.kwargs["token_id"] == "weth"

    @pytest.mark.asyncio
    async def test_sub_hour_timeframe_rejected(self, mock_integration):
        """5m/15m are below CoinGecko's 30m native floor -> clean provider miss."""
        provider = CoinGeckoOHLCVProvider(integration=mock_integration)
        for tf in ("5m", "15m", "1m"):
            with pytest.raises(DataSourceUnavailable) as exc:
                await provider.get_ohlcv("WETH", timeframe=tf, limit=10)
            assert "cannot serve" in exc.value.reason.lower()
        # The unsupported timeframe must short-circuit BEFORE egress.
        mock_integration.get_ohlc.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_token_rejected(self, mock_integration):
        provider = CoinGeckoOHLCVProvider(integration=mock_integration)
        with pytest.raises(DataSourceUnavailable) as exc:
            await provider.get_ohlcv("NOTAREALTOKEN", timeframe="1h", limit=10)
        assert "unknown token" in exc.value.reason.lower()
        mock_integration.get_ohlc.assert_not_called()

    # ------------------------------------------------------------------
    # VIB-4847 (Codex review): CEX fallback symbol coverage.
    #
    # The motivating failover is "stale Binance MATICUSDT -> CoinGecko".
    # Originally the resolver consulted ONLY the small GLOBAL_TOKEN_IDS
    # table, so every Binance-listed token absent from it (the MATIC/WMATIC
    # -> POL rebrand, OP, SUSHI, YFI, BAL, 1INCH, ...) raised "Unknown token"
    # and the failover STILL ended in DATA_ERROR.
    #
    # The first fix flipped to registry-first, which covered those misses but
    # introduced a wrong-asset regression (Codex re-audit): the registry maps
    # WBNB -> wbnb and WAVAX -> wrapped-avax (the wrapped asset's OWN coin id),
    # whereas GLOBAL_TOKEN_IDS carries the deliberate CEX proxies WBNB ->
    # binancecoin / WAVAX -> avalanche-2 (the SAME underlying the CEX leg
    # BNBUSDT/AVAXUSDT priced). The final shape is GLOBAL_TOKEN_IDS-FIRST so
    # those explicit/curated mappings win, with the registry filling only
    # genuine misses (MATIC/WMATIC/POL, OP, SUSHI, ... none of which appear in
    # GLOBAL_TOKEN_IDS). The two parametrized blocks below pin both halves of
    # that invariant.
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        ("symbol", "expected_id"),
        [
            # The rebrand that motivated VIB-4847: Binance kept the MATIC
            # ticker, but the coin is now POL == polygon-ecosystem-token.
            ("MATIC", "polygon-ecosystem-token"),
            ("WMATIC", "polygon-ecosystem-token"),
            ("POL", "polygon-ecosystem-token"),
            # Other common Binance-listed tokens missing from GLOBAL_TOKEN_IDS.
            ("OP", "optimism"),
            ("SUSHI", "sushi"),
            ("YFI", "yearn-finance"),
            ("BAL", "balancer"),
            ("1INCH", "1inch"),
        ],
    )
    def test_cex_fallback_symbols_resolve(self, mock_integration, symbol, expected_id):
        """Each CEX-fallback symbol resolves to its CoinGecko coin id."""
        provider = CoinGeckoOHLCVProvider(integration=mock_integration)
        assert provider._resolve_token_id(symbol) == expected_id

    @pytest.mark.parametrize(
        ("symbol", "expected_id"),
        [
            # Wrapped-CEX proxies: GLOBAL_TOKEN_IDS maps these to the SAME
            # underlying asset the CEX leg (BNBUSDT / AVAXUSDT) prices. The
            # token registry instead maps WBNB -> wbnb and WAVAX -> wrapped-avax
            # (the wrapped asset's own coin id), which would price a DIFFERENT
            # asset on failover. Explicit mappings MUST win (Codex re-audit P2).
            ("WBNB", "binancecoin"),
            ("WAVAX", "avalanche-2"),
            # And the plain CEX bases resolve to the same underlying.
            ("BNB", "binancecoin"),
            ("AVAX", "avalanche-2"),
        ],
    )
    def test_explicit_cex_proxy_mappings_win_over_registry(self, mock_integration, symbol, expected_id):
        """GLOBAL_TOKEN_IDS CEX proxies take precedence over the token registry.

        Regression guard for the registry-first wrong-asset bug: if the registry
        were consulted first, WBNB/WAVAX would resolve to wbnb/wrapped-avax and
        the CoinGecko failover would price a different underlying than the stale
        Binance BNBUSDT/AVAXUSDT leg the chain was failing over from.
        """
        provider = CoinGeckoOHLCVProvider(integration=mock_integration)
        assert provider._resolve_token_id(symbol) == expected_id

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("symbol", "expected_id"),
        [
            ("MATIC", "polygon-ecosystem-token"),
            ("WMATIC", "polygon-ecosystem-token"),
            ("OP", "optimism"),
            ("SUSHI", "sushi"),
        ],
    )
    async def test_cex_fallback_symbol_returns_candles(self, mock_integration, symbol, expected_id):
        """A CEX-fallback symbol fetches candles via its resolved coin id (no DATA_ERROR)."""
        now_ms = int(time.time() * 1000)
        mock_integration.get_ohlc.return_value = _ohlc_rows(count=4, end_ms=now_ms, step_s=1800)
        provider = CoinGeckoOHLCVProvider(integration=mock_integration)

        candles = await provider.get_ohlcv(symbol, timeframe="1h", limit=10)

        # The resolved coin id (NOT the raw symbol) is what egress receives.
        assert mock_integration.get_ohlc.call_args.kwargs["token_id"] == expected_id
        assert len(candles) > 0
        assert all(isinstance(c, OHLCVCandle) for c in candles)

    @pytest.mark.asyncio
    async def test_empty_rows_raises(self, mock_integration):
        mock_integration.get_ohlc.return_value = []
        provider = CoinGeckoOHLCVProvider(integration=mock_integration)
        with pytest.raises(DataSourceUnavailable) as exc:
            await provider.get_ohlcv("WETH", timeframe="1h", limit=10)
        assert "no coingecko ohlc data" in exc.value.reason.lower()

    @pytest.mark.asyncio
    async def test_stablecoin_quote_maps_to_usd(self, mock_integration):
        now_ms = int(time.time() * 1000)
        mock_integration.get_ohlc.return_value = _ohlc_rows(count=2, end_ms=now_ms, step_s=1800)
        provider = CoinGeckoOHLCVProvider(integration=mock_integration)

        await provider.get_ohlcv("WETH", quote="USDT", timeframe="1h", limit=2)
        assert mock_integration.get_ohlc.call_args.kwargs["vs_currency"] == "usd"

    def test_supported_timeframes(self, mock_integration):
        provider = CoinGeckoOHLCVProvider(integration=mock_integration)
        assert provider.supported_timeframes == ["1h", "4h", "1d"]
        assert provider.name == "coingecko"


# =============================================================================
# Framework-side thin client GatewayCoinGeckoOHLCVProvider
# =============================================================================


class TestGatewayCoinGeckoOHLCVProvider:
    @pytest.fixture
    def mock_client(self) -> MagicMock:
        return _make_mock_client()

    @pytest.fixture
    def provider(self, mock_client) -> GatewayCoinGeckoOHLCVProvider:
        return GatewayCoinGeckoOHLCVProvider(gateway_client=mock_client)

    def _grpc_response(self, n: int = 3) -> gateway_pb2.CoinGeckoOHLCVResponse:
        candles = [
            gateway_pb2.CoinGeckoOHLCVCandle(
                timestamp=1700000000 + i * 3600,
                open="100.0",
                high="101.0",
                low="99.0",
                close="100.5",
            )
            for i in range(n)
        ]
        return gateway_pb2.CoinGeckoOHLCVResponse(candles=candles)

    @pytest.mark.asyncio
    async def test_success_parses_candles_with_no_volume(self, provider, mock_client):
        mock_client.integration.CoinGeckoGetOHLCV.return_value = self._grpc_response(3)

        candles = await provider.get_ohlcv(token="WETH", timeframe="1h", limit=3)

        assert len(candles) == 3
        assert all(isinstance(c, OHLCVCandle) for c in candles)
        # Gateway proto has no volume field -> client must leave volume unmeasured.
        assert all(c.volume is None for c in candles)
        assert candles[0].close == Decimal("100.5")

    @pytest.mark.asyncio
    async def test_empty_response_raises(self, provider, mock_client):
        mock_client.integration.CoinGeckoGetOHLCV.return_value = gateway_pb2.CoinGeckoOHLCVResponse(candles=[])
        with pytest.raises(DataSourceUnavailable):
            await provider.get_ohlcv(token="WETH", timeframe="1h", limit=3)

    @pytest.mark.asyncio
    async def test_request_carries_token_and_timeframe(self, provider, mock_client):
        mock_client.integration.CoinGeckoGetOHLCV.return_value = self._grpc_response(1)
        await provider.get_ohlcv(token="ARB", timeframe="4h", limit=5)
        request = mock_client.integration.CoinGeckoGetOHLCV.call_args[0][0]
        assert request.token == "ARB"
        assert request.timeframe == "4h"
        assert request.limit == 5

    def test_adapter_name_is_coingecko(self, provider):
        adapter = CoinGeckoGatewayDataProvider(provider)
        assert adapter.name == "coingecko"


# =============================================================================
# Provider-chain ↔ registry invariant (THE durable fix)
# =============================================================================


class TestProviderChainInvariant:
    def test_every_chain_name_registered_by_factory(self):
        """Real factory build: every advertised provider name is registered.

        This is the durable guard. Before VIB-4847 the factory registered only
        ``geckoterminal`` + ``binance`` while the chains named ``coingecko`` and
        ``defillama`` — this assertion would have FAILED then.
        """
        stack = create_ohlcv_stack(gateway_client=MagicMock(), chain="arbitrum")
        advertised = provider_names_in_chains()
        registered = set(stack.router._providers.keys())
        assert advertised <= registered, f"dangling chain names: {advertised - registered}"
        # And the guard itself must pass without raising.
        assert_provider_chains_registered(stack.router)

    def test_guard_raises_on_dangling_name(self):
        """A router missing a chained provider must fail loud."""
        router = OHLCVRouter()
        # Register only one of the advertised providers.
        only = MagicMock()
        only.name = "binance"
        router.register_provider(only)
        with pytest.raises(ValueError) as exc:
            assert_provider_chains_registered(router)
        assert "invariant violated" in str(exc.value).lower()
        # The missing names are named so the fix is obvious.
        assert "coingecko" in str(exc.value) or "geckoterminal" in str(exc.value)

    def test_defillama_removed_from_chains(self):
        """VIB-4847 decision: DeFi Llama OHLCV is not implemented, so its name
        was removed from the chains (not left dangling). Re-add it together
        with a registered provider when VIB-3448 ships."""
        assert "defillama" not in provider_names_in_chains()
        assert _PROVIDER_CHAINS["cex_primary"] == ["binance", "coingecko"]
        assert _PROVIDER_CHAINS["defi_primary"] == ["geckoterminal", "binance"]


# =============================================================================
# Real-wiring failover: stale Binance -> CoinGecko (not a hand-mocked chain)
# =============================================================================


class TestRealFactoryFailover:
    def test_stale_binance_falls_through_to_coingecko(self, tmp_path, monkeypatch):
        """End-to-end through the REAL factory-built router.

        Binance returns a stale kline response (ALM-2697 guard rejects it).
        The router must fall through to the next ``cex_primary`` provider —
        the newly-wired CoinGecko provider — and return its fresh candles,
        instead of raising ``All providers failed`` (the VIB-4847 break).
        """
        client = _make_mock_client()
        now = datetime.now(UTC)

        # Binance gRPC: 30h-old klines (stale). 1h timeframe -> klines path.
        def _stale_klines(*_args, **_kwargs):
            klines = []
            for i in range(40):
                open_ms = int((now - timedelta(hours=30) - timedelta(hours=39 - i)).timestamp() * 1000)
                klines.append(
                    gateway_pb2.BinanceKline(
                        open_time=open_ms,
                        open="100",
                        high="101",
                        low="99",
                        close="100",
                        volume="10",
                    )
                )
            return gateway_pb2.BinanceKlinesResponse(klines=klines)

        # CoinGecko gRPC: fresh hourly candles ending ~now.
        def _fresh_coingecko(*_args, **_kwargs):
            candles = []
            for i in range(40):
                ts = int((now - timedelta(hours=39 - i)).timestamp())
                candles.append(
                    gateway_pb2.CoinGeckoOHLCVCandle(timestamp=ts, open="200", high="202", low="198", close="201")
                )
            return gateway_pb2.CoinGeckoOHLCVResponse(candles=candles)

        client.integration.BinanceGetKlines.side_effect = _stale_klines
        client.integration.CoinGeckoGetOHLCV.side_effect = _fresh_coingecko

        stack = create_ohlcv_stack(gateway_client=client, chain="arbitrum")
        # Point the router's disk cache at a temp dir so the test is hermetic.
        from almanak.framework.data.ohlcv.ohlcv_router import _OHLCVDiskCache

        stack.router._disk_cache = _OHLCVDiskCache(tmp_path)

        # ARB classifies cex_primary -> [binance, coingecko].
        envelope = stack.router.get_ohlcv("ARB", chain="arbitrum", timeframe="1h", limit=40)

        # Failover succeeded: source is coingecko, candles are the fresh series.
        assert envelope.meta.source == "coingecko"
        assert len(envelope.value) > 0
        assert envelope.value[-1].close == Decimal("201")
        # Binance WAS attempted (and rejected as stale) before the fallthrough.
        assert client.integration.BinanceGetKlines.called
        assert client.integration.CoinGeckoGetOHLCV.called

    def test_stale_matic_recovers_via_coingecko(self, tmp_path):
        """The exact VIB-4847 motivating case: stale Binance MATICUSDT -> CoinGecko.

        WMATIC classifies cex_primary -> [binance, coingecko] and its Binance
        ticker is the stale, pre-rebrand ``MATICUSDT``. Before the resolver fix
        the CoinGecko egress provider raised "Unknown token for CoinGecko OHLC"
        for WMATIC/MATIC (absent from GLOBAL_TOKEN_IDS), so the failover STILL
        ended in DATA_ERROR. With the resolver now reaching the DEFAULT_TOKENS
        registry (MATIC/WMATIC -> polygon-ecosystem-token, the POL rebrand),
        the recovery works.

        Router-level failover mechanics mirror
        ``test_stale_binance_falls_through_to_coingecko`` (the gRPC boundary is
        stubbed). The egress resolver is exercised end-to-end separately by
        ``test_cex_fallback_symbol_returns_candles``; here we assert the router
        no longer ends in DATA_ERROR for the MATIC-rebrand symbol.
        """
        client = _make_mock_client()
        now = datetime.now(UTC)

        def _stale_klines(*_args, **_kwargs):
            klines = []
            for i in range(40):
                open_ms = int((now - timedelta(hours=30) - timedelta(hours=39 - i)).timestamp() * 1000)
                klines.append(
                    gateway_pb2.BinanceKline(
                        open_time=open_ms,
                        open="100",
                        high="101",
                        low="99",
                        close="100",
                        volume="10",
                    )
                )
            return gateway_pb2.BinanceKlinesResponse(klines=klines)

        def _fresh_coingecko(*_args, **_kwargs):
            candles = []
            for i in range(40):
                ts = int((now - timedelta(hours=39 - i)).timestamp())
                candles.append(
                    gateway_pb2.CoinGeckoOHLCVCandle(timestamp=ts, open="200", high="202", low="198", close="201")
                )
            return gateway_pb2.CoinGeckoOHLCVResponse(candles=candles)

        client.integration.BinanceGetKlines.side_effect = _stale_klines
        client.integration.CoinGeckoGetOHLCV.side_effect = _fresh_coingecko

        stack = create_ohlcv_stack(gateway_client=client, chain="polygon")
        from almanak.framework.data.ohlcv.ohlcv_router import _OHLCVDiskCache

        stack.router._disk_cache = _OHLCVDiskCache(tmp_path)

        # WMATIC classifies cex_primary -> [binance, coingecko]; Binance ticker
        # is the stale pre-rebrand MATICUSDT.
        envelope = stack.router.get_ohlcv("WMATIC", chain="polygon", timeframe="1h", limit=40)

        # Recovery, not DATA_ERROR: CoinGecko served the fresh series.
        assert envelope.meta.source == "coingecko"
        assert len(envelope.value) > 0
        assert envelope.value[-1].close == Decimal("201")
        assert client.integration.BinanceGetKlines.called
        assert client.integration.CoinGeckoGetOHLCV.called
