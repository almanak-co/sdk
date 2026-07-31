"""Canonical OHLCV timeframe contracts across SDK and gateway surfaces."""

from __future__ import annotations

from collections.abc import Sequence

import pytest

from almanak import OHLCVTimeframe as PublicOHLCVTimeframe
from almanak.framework.data.interfaces import VALID_TIMEFRAMES, validate_timeframe
from almanak.framework.data.timeframes import (
    BINANCE_OHLCV_TIMEFRAMES,
    CANONICAL_OHLCV_TIMEFRAME_VALUES,
    CANONICAL_OHLCV_TIMEFRAMES,
    COINGECKO_OHLCV_TIMEFRAMES,
    COINGECKO_ONCHAIN_OHLCV_TIMEFRAMES,
    OHLCVTimeframe,
    OHLCVTimeframeCapabilities,
    parse_ohlcv_timeframe,
)

EXPECTED_VALUES = ("1m", "5m", "15m", "1h", "4h", "1d")


def _advertised_surfaces() -> list[tuple[str, Sequence[OHLCVTimeframe]]]:
    from almanak.framework.data.indicators import rsi as fw_rsi
    from almanak.framework.data.ohlcv import gateway_provider as fw_gateway
    from almanak.framework.data.ohlcv import routing_provider as fw_routing
    from almanak.gateway.data.ohlcv import binance_provider as gw_binance
    from almanak.gateway.data.ohlcv import coingecko_onchain_provider as gw_coingecko_onchain
    from almanak.gateway.data.ohlcv import coingecko_provider as gw_coingecko

    return [
        ("framework.GatewayOHLCVProvider", fw_gateway.GatewayOHLCVProvider._SUPPORTED_TIMEFRAMES),
        (
            "framework.GatewayCoinGeckoOnchainOHLCVProvider",
            fw_gateway.GatewayCoinGeckoOnchainOHLCVProvider._SUPPORTED_TIMEFRAMES,
        ),
        (
            "framework.GatewayCoinGeckoOHLCVProvider",
            fw_gateway.GatewayCoinGeckoOHLCVProvider._SUPPORTED_TIMEFRAMES,
        ),
        ("framework.RoutingOHLCVProvider", fw_routing._SUPPORTED_TIMEFRAMES),
        ("framework.rsi.CoinGeckoOHLCVProvider", fw_rsi.CoinGeckoOHLCVProvider._SUPPORTED_TIMEFRAMES),
        ("gateway.BinanceOHLCVProvider", gw_binance.BinanceOHLCVProvider.SUPPORTED_TIMEFRAMES),
        ("gateway.CoinGeckoOHLCVProvider", gw_coingecko.CoinGeckoOHLCVProvider.SUPPORTED_TIMEFRAMES),
        (
            "gateway.CoinGeckoOnchainOHLCVProvider",
            gw_coingecko_onchain.CoinGeckoOnchainOHLCVProvider.SUPPORTED_TIMEFRAMES,
        ),
    ]


def test_public_enum_is_the_single_exact_string_vocabulary() -> None:
    assert PublicOHLCVTimeframe is OHLCVTimeframe
    assert tuple(member.value for member in OHLCVTimeframe) == EXPECTED_VALUES
    assert CANONICAL_OHLCV_TIMEFRAMES == tuple(OHLCVTimeframe)
    assert CANONICAL_OHLCV_TIMEFRAME_VALUES == EXPECTED_VALUES
    assert VALID_TIMEFRAMES is CANONICAL_OHLCV_TIMEFRAMES
    assert OHLCVTimeframe.ONE_HOUR == "1h"


@pytest.mark.parametrize("value", EXPECTED_VALUES)
def test_exact_legacy_strings_parse_without_changing_wire_value(value: str) -> None:
    parsed = validate_timeframe(value)
    assert isinstance(parsed, OHLCVTimeframe)
    assert parsed.value == value
    assert parse_ohlcv_timeframe(parsed) is parsed


@pytest.mark.parametrize("value", ["60m", "1H", " 1h", "1h ", "", "30m", "2h"])
def test_aliases_case_whitespace_and_unknown_values_are_not_normalized(value: str) -> None:
    with pytest.raises(ValueError, match=r"Expected one of: 1m, 5m, 15m, 1h, 4h, 1d"):
        parse_ohlcv_timeframe(value, field_name="config.data_granularity")


def test_non_string_boundary_value_has_actionable_type_error() -> None:
    with pytest.raises(TypeError, match=r"config.data_granularity.*got int"):
        parse_ohlcv_timeframe(3600, field_name="config.data_granularity")


@pytest.mark.parametrize(
    ("surface", "advertised"),
    _advertised_surfaces(),
    ids=[name for name, _ in _advertised_surfaces()],
)
def test_advertised_surfaces_use_enum_values(
    surface: str,
    advertised: Sequence[OHLCVTimeframe],
) -> None:
    assert advertised, f"{surface} advertises no timeframes"
    assert all(isinstance(timeframe, OHLCVTimeframe) for timeframe in advertised)
    assert tuple(advertised) == tuple(timeframe for timeframe in CANONICAL_OHLCV_TIMEFRAMES if timeframe in advertised)


@pytest.mark.parametrize(
    "capabilities",
    [BINANCE_OHLCV_TIMEFRAMES, COINGECKO_OHLCV_TIMEFRAMES, COINGECKO_ONCHAIN_OHLCV_TIMEFRAMES],
)
def test_provider_capability_specs_exhaustively_account_for_vocabulary(
    capabilities: OHLCVTimeframeCapabilities[object],
) -> None:
    assert set(capabilities.mapping) | set(capabilities.unsupported) == set(CANONICAL_OHLCV_TIMEFRAMES)
    assert set(capabilities.mapping).isdisjoint(capabilities.unsupported)


def test_provider_subsets_and_native_aliases_are_centralized() -> None:
    assert BINANCE_OHLCV_TIMEFRAMES.supported == CANONICAL_OHLCV_TIMEFRAMES
    assert COINGECKO_OHLCV_TIMEFRAMES.supported == (
        OHLCVTimeframe.ONE_HOUR,
        OHLCVTimeframe.FOUR_HOURS,
        OHLCVTimeframe.ONE_DAY,
    )
    assert COINGECKO_OHLCV_TIMEFRAMES.unsupported == frozenset(
        {
            OHLCVTimeframe.ONE_MINUTE,
            OHLCVTimeframe.FIVE_MINUTES,
            OHLCVTimeframe.FIFTEEN_MINUTES,
        }
    )
    assert COINGECKO_ONCHAIN_OHLCV_TIMEFRAMES.resolve(OHLCVTimeframe.FIFTEEN_MINUTES).aggregate == "15"
    assert COINGECKO_ONCHAIN_OHLCV_TIMEFRAMES.resolve(OHLCVTimeframe.ONE_DAY).timeframe == "day"


def test_coingecko_fixed_windows_declare_exact_output_capacity() -> None:
    assert COINGECKO_OHLCV_TIMEFRAMES.resolve(OHLCVTimeframe.ONE_HOUR).max_candles == 24
    assert COINGECKO_OHLCV_TIMEFRAMES.resolve(OHLCVTimeframe.FOUR_HOURS).max_candles == 180
    assert COINGECKO_OHLCV_TIMEFRAMES.resolve(OHLCVTimeframe.ONE_DAY).max_candles == 30


def test_capability_mapping_is_immutable() -> None:
    with pytest.raises(TypeError):
        BINANCE_OHLCV_TIMEFRAMES.mapping[OHLCVTimeframe.ONE_HOUR] = "60m"  # type: ignore[index]


def test_incomplete_or_overlapping_capability_spec_fails_at_construction() -> None:
    with pytest.raises(ValueError, match="not exhaustive"):
        OHLCVTimeframeCapabilities(
            provider="broken",
            mapping={OHLCVTimeframe.ONE_HOUR: "1h"},
            unsupported=frozenset(),
        )
    with pytest.raises(ValueError, match="both supported and unsupported"):
        OHLCVTimeframeCapabilities(
            provider="broken",
            mapping={timeframe: timeframe.value for timeframe in CANONICAL_OHLCV_TIMEFRAMES},
            unsupported=frozenset({OHLCVTimeframe.ONE_HOUR}),
        )
