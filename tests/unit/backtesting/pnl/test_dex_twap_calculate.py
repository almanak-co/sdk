"""Branch coverage for DEXTWAPDataProvider.calculate_twap / calculate_twap_sync.

The pool/observation/liquidity query seams are faked per-instance; the
existing test_dex_twap.py covers the underlying math and RPC encoding.
"""

import asyncio
from decimal import Decimal

import pytest

from almanak.framework.data.price.dex_twap import (
    DEXTWAPDataProvider,
    LowLiquidityWarning,
)

POOL = "0x" + "11" * 20


@pytest.fixture
def provider(monkeypatch) -> DEXTWAPDataProvider:
    provider = DEXTWAPDataProvider(chain="ethereum")
    monkeypatch.setattr(provider, "_get_best_quote_token", lambda token: "USDC")
    monkeypatch.setattr(provider, "get_pool_address", lambda token, quote: POOL)

    async def _observe(pool_address, seconds_agos):
        return ["obs-start", "obs-end"]

    monkeypatch.setattr(provider, "_query_observe", _observe)
    monkeypatch.setattr(
        provider, "_query_observe_sync", lambda pool_address, seconds_agos: ["a", "b"]
    )
    monkeypatch.setattr(provider, "_calculate_twap_from_observations", lambda obs: 12345)
    monkeypatch.setattr(
        provider,
        "_tick_to_price",
        lambda tick, token_decimals, quote_decimals, invert=False: Decimal("2000"),
    )

    async def _liquidity(pool_address):
        return 10**20

    monkeypatch.setattr(provider, "_query_liquidity", _liquidity)
    monkeypatch.setattr(provider, "_query_liquidity_sync", lambda pool_address: 10**20)
    return provider


def _run(provider, **kwargs):
    return asyncio.run(provider.calculate_twap("eth", **kwargs))


class TestCalculateTwap:
    def test_happy_path(self, provider):
        result = _run(provider, window_seconds=600)
        assert result is not None
        assert result.price == Decimal("2000")
        assert result.tick == 12345
        assert result.window_seconds == 600
        assert result.liquidity == 10**20
        assert not result.is_low_liquidity
        assert (result.end_time - result.start_time).total_seconds() == 600

    def test_default_window_used(self, provider):
        assert _run(provider).window_seconds == provider._twap_window_seconds

    def test_no_quote_token_returns_none(self, provider, monkeypatch):
        monkeypatch.setattr(provider, "_get_best_quote_token", lambda token: None)
        assert _run(provider) is None

    @pytest.mark.parametrize(
        "pool", [None, "0x0000000000000000000000000000000000000000"]
    )
    def test_missing_pool_returns_none(self, provider, monkeypatch, pool):
        monkeypatch.setattr(provider, "get_pool_address", lambda token, quote: pool)
        assert _run(provider) is None

    @pytest.mark.parametrize("observations", [None, ["only-one"]])
    def test_insufficient_observations_return_none(self, provider, monkeypatch, observations):
        async def _observe(pool_address, seconds_agos):
            return observations

        monkeypatch.setattr(provider, "_query_observe", _observe)
        assert _run(provider) is None

    def test_twap_math_error_returns_none(self, provider, monkeypatch):
        def _boom(obs):
            raise ValueError("accumulator overflow")

        monkeypatch.setattr(provider, "_calculate_twap_from_observations", _boom)
        assert _run(provider) is None

    def test_non_stable_quote_converts_via_quote_price(self, provider, monkeypatch):
        monkeypatch.setattr(provider, "_get_best_quote_token", lambda token: "WETH")

        async def _get_price(token):
            return Decimal("1.05")

        monkeypatch.setattr(provider, "get_price", _get_price)
        result = _run(provider)
        assert result.price == Decimal("2000") * Decimal("1.05")

    def test_non_stable_quote_without_price_keeps_raw(self, provider, monkeypatch):
        monkeypatch.setattr(provider, "_get_best_quote_token", lambda token: "WETH")

        async def _get_price(token):
            return None

        monkeypatch.setattr(provider, "get_price", _get_price)
        assert _run(provider).price == Decimal("2000")

    def test_unavailable_liquidity_skips_check(self, provider, monkeypatch):
        async def _liquidity(pool_address):
            return None

        monkeypatch.setattr(provider, "_query_liquidity", _liquidity)
        result = _run(provider)
        assert result.liquidity is None
        assert not result.is_low_liquidity

    def test_low_liquidity_flagged(self, provider, monkeypatch):
        async def _liquidity(pool_address):
            return 10

        monkeypatch.setattr(provider, "_query_liquidity", _liquidity)
        result = _run(provider)
        assert result.is_low_liquidity

    def test_low_liquidity_raises_when_requested(self, provider, monkeypatch):
        async def _liquidity(pool_address):
            return 10

        monkeypatch.setattr(provider, "_query_liquidity", _liquidity)
        with pytest.raises(LowLiquidityWarning) as excinfo:
            _run(provider, raise_on_low_liquidity=True)
        assert excinfo.value.token == "ETH"
        assert excinfo.value.pool_address == POOL


class TestCalculateTwapSync:
    def test_happy_path(self, provider):
        result = provider.calculate_twap_sync("eth", window_seconds=300)
        assert result is not None
        assert result.price == Decimal("2000")
        assert result.window_seconds == 300
        assert not result.is_low_liquidity

    def test_no_quote_token_returns_none(self, provider, monkeypatch):
        monkeypatch.setattr(provider, "_get_best_quote_token", lambda token: None)
        assert provider.calculate_twap_sync("eth") is None

    @pytest.mark.parametrize(
        "pool", [None, "0x0000000000000000000000000000000000000000"]
    )
    def test_missing_pool_returns_none(self, provider, monkeypatch, pool):
        monkeypatch.setattr(provider, "get_pool_address", lambda token, quote: pool)
        assert provider.calculate_twap_sync("eth") is None

    @pytest.mark.parametrize("observations", [None, ["only-one"]])
    def test_insufficient_observations_return_none(self, provider, monkeypatch, observations):
        monkeypatch.setattr(
            provider, "_query_observe_sync", lambda pool_address, seconds_agos: observations
        )
        assert provider.calculate_twap_sync("eth") is None

    def test_twap_math_error_returns_none(self, provider, monkeypatch):
        def _boom(obs):
            raise ValueError("accumulator overflow")

        monkeypatch.setattr(provider, "_calculate_twap_from_observations", _boom)
        assert provider.calculate_twap_sync("eth") is None

    def test_unavailable_liquidity_skips_check(self, provider, monkeypatch):
        monkeypatch.setattr(provider, "_query_liquidity_sync", lambda pool_address: None)
        result = provider.calculate_twap_sync("eth")
        assert result.liquidity is None
        assert not result.is_low_liquidity

    def test_low_liquidity_flagged(self, provider, monkeypatch):
        monkeypatch.setattr(provider, "_query_liquidity_sync", lambda pool_address: 10)
        assert provider.calculate_twap_sync("eth").is_low_liquidity

    def test_low_liquidity_raises_when_requested(self, provider, monkeypatch):
        monkeypatch.setattr(provider, "_query_liquidity_sync", lambda pool_address: 10)
        with pytest.raises(LowLiquidityWarning):
            provider.calculate_twap_sync("eth", raise_on_low_liquidity=True)
