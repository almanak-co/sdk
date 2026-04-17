"""Tests for ManualPriceOverrideSource (Bug 3 of 0G DogFooding report)."""

import os
from decimal import Decimal

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.gateway.data.price.manual_override import ManualPriceOverrideSource


@pytest.fixture
def source() -> ManualPriceOverrideSource:
    return ManualPriceOverrideSource()


@pytest.fixture
def clean_env(monkeypatch):
    """Remove any stray override env vars before/after each test."""
    for key in list(os.environ):
        if key.startswith("ALMANAK_PRICE_OVERRIDE_"):
            monkeypatch.delenv(key, raising=False)
    yield


class TestManualPriceOverrideSource:
    @pytest.mark.asyncio
    async def test_returns_usd_override(self, source, monkeypatch, clean_env):
        """ALMANAK_PRICE_OVERRIDE_W0G=0.12 → W0G/USD = 0.12 with low confidence."""
        monkeypatch.setenv("ALMANAK_PRICE_OVERRIDE_W0G", "0.12")
        result = await source.get_price("W0G", "USD")
        assert result.price == Decimal("0.12")
        assert result.source == "manual_override"
        assert result.confidence == 0.5
        assert result.stale is False

    @pytest.mark.asyncio
    async def test_explicit_pair_wins_over_usd_default(self, source, monkeypatch, clean_env):
        """_{TOKEN}_{QUOTE} takes precedence over _{TOKEN}."""
        monkeypatch.setenv("ALMANAK_PRICE_OVERRIDE_W0G", "0.12")
        monkeypatch.setenv("ALMANAK_PRICE_OVERRIDE_W0G_WBTC", "0.0000012")

        usd = await source.get_price("W0G", "USD")
        assert usd.price == Decimal("0.12")

        wbtc = await source.get_price("W0G", "WBTC")
        assert wbtc.price == Decimal("0.0000012")

    @pytest.mark.asyncio
    async def test_case_insensitive_token(self, source, monkeypatch, clean_env):
        """Token symbol casing doesn't matter for lookup."""
        monkeypatch.setenv("ALMANAK_PRICE_OVERRIDE_W0G", "0.5")
        result = await source.get_price("w0g", "usd")
        assert result.price == Decimal("0.5")

    @pytest.mark.asyncio
    async def test_missing_override_raises(self, source, clean_env):
        """Tokens without an override cascade to DataSourceUnavailable so the
        aggregator falls through to the next source."""
        with pytest.raises(DataSourceUnavailable) as exc:
            await source.get_price("UNKNOWN_TOKEN", "USD")
        assert "manual_override" == exc.value.source
        assert "No manual override" in str(exc.value.reason)

    @pytest.mark.asyncio
    async def test_invalid_value_falls_through(self, source, monkeypatch, clean_env):
        """Typos in env vars are logged and ignored — never crash."""
        monkeypatch.setenv("ALMANAK_PRICE_OVERRIDE_W0G", "not a number")
        with pytest.raises(DataSourceUnavailable):
            await source.get_price("W0G", "USD")

    @pytest.mark.asyncio
    async def test_non_positive_value_rejected(self, source, monkeypatch, clean_env):
        """Zero or negative prices are rejected (no free-money bugs)."""
        monkeypatch.setenv("ALMANAK_PRICE_OVERRIDE_W0G", "0")
        with pytest.raises(DataSourceUnavailable):
            await source.get_price("W0G", "USD")

        monkeypatch.setenv("ALMANAK_PRICE_OVERRIDE_W0G", "-1")
        with pytest.raises(DataSourceUnavailable):
            await source.get_price("W0G", "USD")

    def test_supported_tokens_reflects_env(self, source, monkeypatch, clean_env):
        """supported_tokens reports the symbols that currently have overrides."""
        assert source.supported_tokens == []
        monkeypatch.setenv("ALMANAK_PRICE_OVERRIDE_W0G", "0.12")
        monkeypatch.setenv("ALMANAK_PRICE_OVERRIDE_CUSTOM_USD", "1")
        supported = source.supported_tokens
        assert "W0G" in supported
        assert "CUSTOM" in supported

    def test_source_name(self, source):
        assert source.source_name == "manual_override"
