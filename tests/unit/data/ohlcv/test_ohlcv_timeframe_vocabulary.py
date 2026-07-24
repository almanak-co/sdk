"""Timeframe-vocabulary invariants across every OHLCV provider surface.

``VALID_TIMEFRAMES`` (``almanak.framework.data.interfaces``) is the single
request vocabulary: ``validate_timeframe`` gates every provider's
``get_ohlcv`` entry point with it. The ``OHLCVProvider`` protocol requires
each provider's advertised ``supported_timeframes`` to be a *subset* of that
vocabulary — an advertised timeframe the validator rejects is a lie to
callers (a "30m" entry on the Binance-backed providers survived this way:
``supported_timeframes`` promised it while ``validate_timeframe`` raised
before any provider logic ran, leaving the per-provider interval guard
unreachable).

These tests pin the invariants so the two surfaces cannot drift apart again.
Widening the vocabulary is a deliberate feature that starts at
``VALID_TIMEFRAMES`` and must consider every provider's fallback story —
not a per-provider list edit.
"""

import pytest

from almanak.framework.data.interfaces import VALID_TIMEFRAMES, validate_timeframe

# Advertised-timeframe surfaces: (surface name, advertised list).
_ADVERTISED_SURFACES: list[tuple[str, list[str]]] = []


def _register_surfaces() -> None:
    from almanak.framework.data.indicators import rsi as fw_rsi
    from almanak.framework.data.ohlcv import gateway_provider as fw_gateway
    from almanak.framework.data.ohlcv import routing_provider as fw_routing
    from almanak.gateway.data.ohlcv import binance_provider as gw_binance
    from almanak.gateway.data.ohlcv import coingecko_provider as gw_coingecko
    from almanak.gateway.data.ohlcv import geckoterminal_provider as gw_geckoterminal

    _ADVERTISED_SURFACES.extend(
        [
            (
                "framework.GatewayOHLCVProvider",
                fw_gateway.GatewayOHLCVProvider._SUPPORTED_TIMEFRAMES,
            ),
            (
                "framework.GatewayGeckoTerminalOHLCVProvider",
                fw_gateway.GatewayGeckoTerminalOHLCVProvider._SUPPORTED_TIMEFRAMES,
            ),
            (
                "framework.GatewayCoinGeckoOHLCVProvider",
                fw_gateway.GatewayCoinGeckoOHLCVProvider._SUPPORTED_TIMEFRAMES,
            ),
            ("framework.RoutingOHLCVProvider", fw_routing._SUPPORTED_TIMEFRAMES),
            (
                "framework.rsi.CoinGeckoOHLCVProvider",
                fw_rsi.CoinGeckoOHLCVProvider._SUPPORTED_TIMEFRAMES,
            ),
            (
                "gateway.BinanceOHLCVProvider",
                gw_binance.BinanceOHLCVProvider.SUPPORTED_TIMEFRAMES,
            ),
            (
                "gateway.CoinGeckoOHLCVProvider",
                gw_coingecko.CoinGeckoOHLCVProvider.SUPPORTED_TIMEFRAMES,
            ),
            (
                "gateway.GeckoTerminalOHLCVProvider",
                gw_geckoterminal.GeckoTerminalOHLCVProvider.SUPPORTED_TIMEFRAMES,
            ),
        ]
    )


_register_surfaces()


@pytest.mark.parametrize(
    ("surface", "advertised"),
    _ADVERTISED_SURFACES,
    ids=[name for name, _ in _ADVERTISED_SURFACES],
)
def test_advertised_timeframes_pass_the_validator(surface: str, advertised: list[str]) -> None:
    """Every advertised timeframe must be requestable through validate_timeframe."""
    assert advertised, f"{surface} advertises no timeframes"
    for timeframe in advertised:
        validate_timeframe(timeframe)


def test_binance_backed_providers_serve_the_full_vocabulary() -> None:
    """The Binance path (primary CEX source) supports every valid timeframe."""
    from almanak.framework.data.ohlcv.gateway_provider import GatewayOHLCVProvider
    from almanak.gateway.data.ohlcv.binance_provider import BinanceOHLCVProvider

    assert GatewayOHLCVProvider._SUPPORTED_TIMEFRAMES == VALID_TIMEFRAMES
    assert BinanceOHLCVProvider.SUPPORTED_TIMEFRAMES == VALID_TIMEFRAMES


def test_framework_binance_interval_map_matches_vocabulary_exactly() -> None:
    """TIMEFRAME_TO_BINANCE_INTERVAL keys are exactly the request vocabulary,
    so the provider's per-request interval guard stays pure defense-in-depth."""
    from almanak.framework.data.ohlcv.gateway_provider import TIMEFRAME_TO_BINANCE_INTERVAL

    assert set(TIMEFRAME_TO_BINANCE_INTERVAL) == set(VALID_TIMEFRAMES)


def test_gateway_binance_interval_map_covers_vocabulary() -> None:
    """BINANCE_INTERVAL_MAP is the full native Binance interval table; it must
    at least translate every timeframe callers can request."""
    from almanak.gateway.data.ohlcv.binance_provider import BINANCE_INTERVAL_MAP

    assert set(VALID_TIMEFRAMES) <= set(BINANCE_INTERVAL_MAP)
