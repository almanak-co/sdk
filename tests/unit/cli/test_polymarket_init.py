"""Tests for the Polymarket prediction provider init helper (VIB-3132).

PM Exp 14 surfaced a silent failure mode: a Polygon strategy declaring
``polymarket`` as a supported protocol could enter the run loop with an
uninitialised prediction provider when ``POLYMARKET_*`` env vars were
missing. The failure was logged at ``DEBUG`` so end-users only saw a
silent HOLD until they re-ran with ``--verbose``.

The helper now fails-fast for strategies that declare polymarket support
and warns (not debugs) for strategies that merely run on Polygon.
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


@pytest.fixture
def clear_polymarket_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("POLYMARKET_WALLET_ADDRESS", "POLYMARKET_PRIVATE_KEY"):
        monkeypatch.delenv(var, raising=False)


class TestInitPredictionProviderFailFast:
    """Strategies declaring polymarket protocol must fail-fast on init failure."""

    def test_polymarket_strategy_missing_env_raises_runtimeerror(self, clear_polymarket_env):
        strategy = _make_strategy(supported_protocols=["polymarket"])

        with pytest.raises(RuntimeError) as exc_info:
            _init_prediction_provider(strategy, chain="polygon")

        # Error message names the strategy and enumerates missing env vars.
        msg = str(exc_info.value)
        assert "test_strategy" in msg
        assert "POLYMARKET_WALLET_ADDRESS" in msg
        assert "POLYMARKET_PRIVATE_KEY" in msg
        assert strategy._prediction_provider is None

    def test_polymarket_strategy_partial_env_raises_runtimeerror(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        # Wallet set, key missing → still required, still fail-fast.
        monkeypatch.setenv("POLYMARKET_WALLET_ADDRESS", "0xabc")
        monkeypatch.delenv("POLYMARKET_PRIVATE_KEY", raising=False)
        strategy = _make_strategy(supported_protocols=["polymarket"])

        with pytest.raises(RuntimeError) as exc_info:
            _init_prediction_provider(strategy, chain="polygon")

        msg = str(exc_info.value)
        assert "POLYMARKET_PRIVATE_KEY" in msg
        # Already-set var should not be in the missing list.
        assert "POLYMARKET_WALLET_ADDRESS" not in msg.split("Missing required env vars:")[1]

    def test_polymarket_strategy_fails_fast_on_non_polygon_chain(self, clear_polymarket_env):
        """Multi-chain strategy declaring polymarket fails-fast even on a non-Polygon
        chain (CodeRabbit catch on PR #1567): the old polygon-only gate at the call
        site let multi-chain polymarket strategies silently HOLD.
        """
        strategy = _make_strategy(supported_protocols=["polymarket", "aave_v3"])

        with pytest.raises(RuntimeError) as exc_info:
            _init_prediction_provider(strategy, chain="ethereum")

        assert "test_strategy" in str(exc_info.value)
        assert strategy._prediction_provider is None


class TestInitPredictionProviderWarns:
    """Non-polymarket strategies on Polygon get a warning (was DEBUG)."""

    def test_non_polymarket_strategy_missing_env_warns(
        self, clear_polymarket_env, caplog: pytest.LogCaptureFixture
    ):
        strategy = _make_strategy(supported_protocols=["aave_v3"])

        with caplog.at_level("WARNING", logger="almanak.framework.cli.run"):
            _init_prediction_provider(strategy, chain="polygon")

        assert any("Prediction market provider not available" in r.message for r in caplog.records)
        assert strategy._prediction_provider is None

    def test_no_metadata_strategy_on_polygon_warns_does_not_raise(
        self, clear_polymarket_env, caplog: pytest.LogCaptureFixture
    ):
        # A strategy with no STRATEGY_METADATA at all → can't declare polymarket
        # support, so warn rather than fail-fast.
        strategy = _make_strategy(supported_protocols=None)

        with caplog.at_level("WARNING", logger="almanak.framework.cli.run"):
            _init_prediction_provider(strategy, chain="polygon")

        assert any("Prediction market provider not available" in r.message for r in caplog.records)


class TestInitPredictionProviderSkips:
    """Helper no-ops for non-polymarket strategies on non-Polygon chains."""

    def test_non_polymarket_non_polygon_skips_silently(
        self, clear_polymarket_env, caplog: pytest.LogCaptureFixture
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
        self, clear_polymarket_env, caplog: pytest.LogCaptureFixture
    ):
        strategy = _make_strategy(supported_protocols=None)

        with caplog.at_level("WARNING", logger="almanak.framework.cli.run"):
            _init_prediction_provider(strategy, chain="base")

        assert not any("Prediction market provider" in r.message for r in caplog.records)


class TestInitPredictionProviderHappyPath:
    """When env is present and clients construct cleanly the provider is wired."""

    def test_provider_assigned_when_init_succeeds(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv("POLYMARKET_WALLET_ADDRESS", "0x" + "a" * 40)
        monkeypatch.setenv("POLYMARKET_PRIVATE_KEY", "0x" + "b" * 64)
        strategy = _make_strategy(supported_protocols=["polymarket"])

        sentinel_provider = object()

        with (
            patch(
                "almanak.framework.connectors.polymarket.PolymarketConfig.from_env"
            ) as mock_from_env,
            patch("almanak.framework.connectors.polymarket.ClobClient") as mock_clob,
            patch(
                "almanak.framework.data.prediction_provider.PredictionMarketDataProvider"
            ) as mock_pmdp,
        ):
            mock_from_env.return_value = object()
            mock_clob.return_value = object()
            mock_pmdp.return_value = sentinel_provider

            _init_prediction_provider(strategy, chain="polygon")

        assert strategy._prediction_provider is sentinel_provider
