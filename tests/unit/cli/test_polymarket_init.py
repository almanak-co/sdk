"""Tests for the Polymarket prediction provider init helper.

The provider is now gateway-backed. Strategies that declare
``polymarket`` support must fail-fast when no connected gateway client is
available. Non-polymarket strategies skip initialization entirely,
including on Polygon.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from almanak.framework.cli.run import _init_prediction_provider


def _make_strategy(supported_protocols: list[str] | None) -> SimpleNamespace:
    """Mock strategy with the minimal shape ``_init_prediction_provider`` reads."""
    strategy = SimpleNamespace(_prediction_provider=None)
    if supported_protocols is not None:
        strategy.STRATEGY_METADATA = SimpleNamespace(
            name="test_strategy", supported_protocols=supported_protocols
        )
    return strategy


def _make_gateway_client(*, is_connected: bool) -> SimpleNamespace:
    return SimpleNamespace(is_connected=is_connected)


class TestInitPredictionProviderFailFast:
    """Strategies declaring polymarket protocol must fail-fast on init failure."""

    def test_polymarket_strategy_missing_gateway_raises_runtimeerror(self):
        strategy = _make_strategy(supported_protocols=["polymarket"])

        with pytest.raises(RuntimeError) as exc_info:
            _init_prediction_provider(strategy, chain="polygon")

        msg = str(exc_info.value)
        assert "test_strategy" in msg
        assert "initialization failed" in msg
        assert strategy._prediction_provider is None

    def test_polymarket_strategy_disconnected_gateway_raises_runtimeerror(self):
        strategy = _make_strategy(supported_protocols=["polymarket"])
        gateway_client = _make_gateway_client(is_connected=False)

        with pytest.raises(RuntimeError) as exc_info:
            _init_prediction_provider(strategy, chain="polygon", gateway_client=gateway_client)

        msg = str(exc_info.value)
        assert "test_strategy" in msg
        assert "initialization failed" in msg

    def test_polymarket_strategy_fails_fast_on_non_polygon_chain(self):
        """Multi-chain strategy declaring polymarket fails-fast even on a non-Polygon
        chain (CodeRabbit catch on PR #1567): the old polygon-only gate at the call
        site let multi-chain polymarket strategies silently HOLD.
        """
        strategy = _make_strategy(supported_protocols=["polymarket", "aave_v3"])

        with pytest.raises(RuntimeError) as exc_info:
            _init_prediction_provider(strategy, chain="ethereum")

        assert "test_strategy" in str(exc_info.value)
        assert strategy._prediction_provider is None


class TestInitPredictionProviderSkips:
    """Helper no-ops for strategies that do not declare polymarket."""

    def test_non_polymarket_strategy_on_polygon_skips_silently(
        self, caplog: pytest.LogCaptureFixture
    ):
        strategy = _make_strategy(supported_protocols=["aave_v3"])

        with caplog.at_level("WARNING", logger="almanak.framework.cli.run"):
            _init_prediction_provider(strategy, chain="polygon")

        assert not any("Prediction market provider" in r.message for r in caplog.records)
        assert strategy._prediction_provider is None

    def test_no_metadata_strategy_on_polygon_skips_silently(
        self, caplog: pytest.LogCaptureFixture
    ):
        # A strategy with no STRATEGY_METADATA at all → can't declare polymarket
        # support, so skip rather than fail-fast.
        strategy = _make_strategy(supported_protocols=None)

        with caplog.at_level("WARNING", logger="almanak.framework.cli.run"):
            _init_prediction_provider(strategy, chain="polygon")

        assert not any("Prediction market provider" in r.message for r in caplog.records)

    def test_non_polymarket_non_polygon_skips_silently(
        self, caplog: pytest.LogCaptureFixture
    ):
        """An aave_v3 strategy on arbitrum has no business importing Polymarket
        — the helper should return early without warning or raising.
        """
        strategy = _make_strategy(supported_protocols=["aave_v3"])

        with caplog.at_level("WARNING", logger="almanak.framework.cli.run"):
            _init_prediction_provider(strategy, chain="arbitrum")

        assert not any("Prediction market provider" in r.message for r in caplog.records)
        assert strategy._prediction_provider is None

    def test_no_metadata_non_polygon_skips_silently(
        self, caplog: pytest.LogCaptureFixture
    ):
        strategy = _make_strategy(supported_protocols=None)

        with caplog.at_level("WARNING", logger="almanak.framework.cli.run"):
            _init_prediction_provider(strategy, chain="base")

        assert not any("Prediction market provider" in r.message for r in caplog.records)


class TestInitPredictionProviderHappyPath:
    """When the gateway is connected and clients construct cleanly the provider is wired."""

    def test_provider_assigned_when_init_succeeds(self):
        strategy = _make_strategy(supported_protocols=["polymarket"])
        gateway_client = _make_gateway_client(is_connected=True)

        sentinel_provider = object()

        with (
            patch("almanak.connectors.polymarket.gateway_client.GatewayPolymarketClient") as mock_clob,
            patch(
                "almanak.framework.data.prediction_provider.PredictionMarketDataProvider"
            ) as mock_pmdp,
        ):
            mock_clob.return_value = object()
            mock_pmdp.return_value = sentinel_provider

            _init_prediction_provider(strategy, chain="polygon", gateway_client=gateway_client)

        assert strategy._prediction_provider is sentinel_provider
