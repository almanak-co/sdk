"""Targeted tests for the VIB-4424 gateway config/runtime helper slice."""

from __future__ import annotations

import pytest

from almanak.config.env import gateway_config_from_env
from almanak.config.gateway_runtime import (
    gateway_wallets_configured,
    manual_price_override_keys,
    manual_price_override_raw,
    parse_gateway_wallets_json,
    portfolio_provider_cache_ttl,
    portfolio_provider_chain_filter,
)
from almanak.connectors.polymarket.gateway.service import (
    POLYMARKET_MARKET_CACHE_TTL_DEFAULT_SECONDS,
    POLYMARKET_MARKET_CACHE_TTL_MAX_SECONDS,
)

_ENV_VARS: tuple[str, ...] = (
    "THEGRAPH_API_KEY",
    "TENDERLY_ACCOUNT_SLUG",
    "TENDERLY_PROJECT_SLUG",
    "TENDERLY_ACCESS_KEY",
    "ALMANAK_DEXSCREENER_MIN_LIQUIDITY_USD",
    "ALMANAK_DEXSCREENER_MIN_VOLUME_USD",
    "ALMANAK_DEXSCREENER_MIN_TURNOVER_RATIO",
    "ALMANAK_DEXSCREENER_DOMINANCE_MULTIPLE",
    "ALMANAK_POLYMARKET_NETWORK",
    "ALMANAK_POLYMARKET_MARKET_CACHE_TTL_SECONDS",
    "ALMANAK_ANVIL_WATCHDOG_INTERVAL",
    "ALMANAK_GATEWAY_WALLETS",
    "ALMANAK_PRICE_OVERRIDE_W0G",
    "ALMANAK_PRICE_OVERRIDE_W0G_WBTC",
    "MORALIS_CHAIN_FILTER",
    "MORALIS_CACHE_TTL",
)


@pytest.fixture(autouse=True)
def _scrub_env(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    for name in _ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_gateway_config_reads_gateway_service_boot_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("THEGRAPH_API_KEY", "tg-key")
    monkeypatch.setenv("TENDERLY_ACCOUNT_SLUG", "acct")
    monkeypatch.setenv("TENDERLY_PROJECT_SLUG", "proj")
    monkeypatch.setenv("TENDERLY_ACCESS_KEY", "tenderly-key")
    monkeypatch.setenv("ALMANAK_DEXSCREENER_MIN_LIQUIDITY_USD", "25000")
    monkeypatch.setenv("ALMANAK_DEXSCREENER_MIN_VOLUME_USD", "2500")
    monkeypatch.setenv("ALMANAK_DEXSCREENER_MIN_TURNOVER_RATIO", "0.15")
    monkeypatch.setenv("ALMANAK_DEXSCREENER_DOMINANCE_MULTIPLE", "4.5")
    monkeypatch.setenv("ALMANAK_POLYMARKET_NETWORK", "anvil")
    monkeypatch.setenv("ALMANAK_POLYMARKET_MARKET_CACHE_TTL_SECONDS", "120")
    monkeypatch.setenv("ALMANAK_ANVIL_WATCHDOG_INTERVAL", "7.5")

    cfg = gateway_config_from_env()

    assert cfg.thegraph_api_key == "tg-key"
    assert cfg.tenderly_account_slug == "acct"
    assert cfg.tenderly_project_slug == "proj"
    assert cfg.tenderly_access_key == "tenderly-key"
    assert cfg.dexscreener_min_liquidity_usd == 25_000.0
    assert cfg.dexscreener_min_volume_usd == 2_500.0
    assert cfg.dexscreener_min_turnover_ratio == 0.15
    assert cfg.dexscreener_dominance_multiple == 4.5
    assert cfg.polymarket_network == "anvil"
    assert cfg.polymarket_market_cache_ttl_seconds == 120.0
    assert cfg.anvil_watchdog_interval == 7.5


def test_gateway_config_invalid_thresholds_fall_back_to_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALMANAK_DEXSCREENER_MIN_LIQUIDITY_USD", "not-a-number")
    monkeypatch.setenv("ALMANAK_DEXSCREENER_MIN_VOLUME_USD", "")
    monkeypatch.setenv("ALMANAK_DEXSCREENER_MIN_TURNOVER_RATIO", "bogus")
    monkeypatch.setenv("ALMANAK_DEXSCREENER_DOMINANCE_MULTIPLE", "still-bogus")
    monkeypatch.setenv("ALMANAK_POLYMARKET_MARKET_CACHE_TTL_SECONDS", "inf")

    cfg = gateway_config_from_env()

    assert cfg.dexscreener_min_liquidity_usd == 10_000.0
    assert cfg.dexscreener_min_volume_usd == 1_000.0
    assert cfg.dexscreener_min_turnover_ratio == 0.05
    assert cfg.dexscreener_dominance_multiple == 3.0
    assert cfg.polymarket_market_cache_ttl_seconds == POLYMARKET_MARKET_CACHE_TTL_DEFAULT_SECONDS


def test_gateway_config_clamps_polymarket_cache_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALMANAK_POLYMARKET_MARKET_CACHE_TTL_SECONDS", "86400000")
    cfg = gateway_config_from_env()
    assert cfg.polymarket_market_cache_ttl_seconds == POLYMARKET_MARKET_CACHE_TTL_MAX_SECONDS


def test_parse_gateway_wallets_json_validates_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALMANAK_GATEWAY_WALLETS", '{"polygon":{"wallet_address":"0xabc","type":"direct"}}')
    assert gateway_wallets_configured() is True
    assert parse_gateway_wallets_json() == {"polygon": {"wallet_address": "0xabc", "type": "direct"}}

    monkeypatch.setenv("ALMANAK_GATEWAY_WALLETS", "{not-json")
    with pytest.raises(ValueError, match="ALMANAK_GATEWAY_WALLETS is not valid JSON"):
        parse_gateway_wallets_json()

    monkeypatch.setenv("ALMANAK_GATEWAY_WALLETS", '["not", "a", "dict"]')
    with pytest.raises(ValueError, match="must be a JSON object keyed by chain"):
        parse_gateway_wallets_json()


def test_dynamic_gateway_helpers_reflect_live_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALMANAK_PRICE_OVERRIDE_W0G", "0.12")
    monkeypatch.setenv("ALMANAK_PRICE_OVERRIDE_W0G_WBTC", "0.0000012")
    monkeypatch.setenv("MORALIS_CHAIN_FILTER", "base, arbitrum")
    monkeypatch.setenv("MORALIS_CACHE_TTL", "45")

    assert manual_price_override_raw("ALMANAK_PRICE_OVERRIDE_W0G") == "0.12"
    assert set(manual_price_override_keys()) == {
        "ALMANAK_PRICE_OVERRIDE_W0G",
        "ALMANAK_PRICE_OVERRIDE_W0G_WBTC",
    }
    assert portfolio_provider_chain_filter("moralis") == ["base", "arbitrum"]
    assert portfolio_provider_cache_ttl("moralis", 60) == 45
