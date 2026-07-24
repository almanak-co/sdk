"""Symbol token resolution deprecation and 3.0.0 removal boundary."""

from __future__ import annotations

import logging
import warnings
from unittest.mock import patch

import pytest

from almanak.framework.data.tokens import (
    SymbolTokenResolutionError,
    SymbolTokenResolutionWarning,
    TokenResolver,
)
from almanak.framework.data.tokens import deprecation as deprecation_policy

BASE_USDC = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
BASE_USDC_CAIP19 = f"eip155:8453/erc20:{BASE_USDC}"
ARBITRUM_NATIVE_CAIP19 = "eip155:42161/slip44:60"
SOLANA_NATIVE_CAIP19 = "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp/slip44:501"


@pytest.fixture
def resolver(tmp_path) -> TokenResolver:
    return TokenResolver(cache_file=str(tmp_path / "token-cache.json"))


def _resolve_usdc_from_first_callsite(resolver: TokenResolver):
    return resolver.resolve("USDC", "base")


def _resolve_usdc_from_second_callsite(resolver: TokenResolver):
    return resolver.resolve("USDC", "base")


def test_resolver_symbol_warns_once_per_callsite_on_2x(
    monkeypatch: pytest.MonkeyPatch,
    resolver: TokenResolver,
) -> None:
    monkeypatch.setattr(deprecation_policy, "SDK_VERSION", "2.99.9")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("default", SymbolTokenResolutionWarning)
        for _ in range(2):
            resolved = _resolve_usdc_from_first_callsite(resolver)

    symbol_warnings = [item for item in caught if item.category is SymbolTokenResolutionWarning]
    assert resolved.address.lower() == BASE_USDC.lower()
    assert len(symbol_warnings) == 1
    assert "unreliable" in str(symbol_warnings[0].message)
    assert "chain-specific token contract address" in str(symbol_warnings[0].message)
    assert "3.0.0" in str(symbol_warnings[0].message)
    assert symbol_warnings[0].filename == __file__


def test_resolver_symbol_warns_at_each_external_callsite(
    monkeypatch: pytest.MonkeyPatch,
    resolver: TokenResolver,
) -> None:
    monkeypatch.setattr(deprecation_policy, "SDK_VERSION", "2.99.9")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", SymbolTokenResolutionWarning)
        _resolve_usdc_from_first_callsite(resolver)
        _resolve_usdc_from_second_callsite(resolver)

    symbol_warnings = [item for item in caught if item.category is SymbolTokenResolutionWarning]
    assert len(symbol_warnings) == 2
    assert {item.lineno for item in symbol_warnings} == {
        _resolve_usdc_from_first_callsite.__code__.co_firstlineno + 1,
        _resolve_usdc_from_second_callsite.__code__.co_firstlineno + 1,
    }


def test_resolver_address_and_caip19_are_not_deprecated(
    monkeypatch: pytest.MonkeyPatch,
    resolver: TokenResolver,
) -> None:
    monkeypatch.setattr(deprecation_policy, "SDK_VERSION", "2.99.9")

    with warnings.catch_warnings():
        warnings.simplefilter("error", SymbolTokenResolutionWarning)
        by_address = resolver.resolve(BASE_USDC, "base")
        by_caip19 = resolver.resolve(BASE_USDC_CAIP19, "ethereum")
        native = resolver.resolve(ARBITRUM_NATIVE_CAIP19, "base")
        wrapped_native = resolver.resolve_for_swap(ARBITRUM_NATIVE_CAIP19, "arbitrum")
        solana_native = resolver.resolve(SOLANA_NATIVE_CAIP19, "base")
        wrapped_solana_native = resolver.resolve_for_swap(SOLANA_NATIVE_CAIP19, "solana")

    assert by_address.address.lower() == BASE_USDC.lower()
    assert by_caip19.address.lower() == BASE_USDC.lower()
    assert native.symbol == "ETH"
    assert wrapped_native.symbol == "WETH"
    assert solana_native.symbol == "SOL"
    assert wrapped_solana_native.symbol == "WSOL"


@pytest.mark.parametrize("sdk_version", ["3.0.0", "3.0.1", "4.0.0"])
def test_resolver_rejects_symbols_from_3_0_0(
    monkeypatch: pytest.MonkeyPatch,
    resolver: TokenResolver,
    sdk_version: str,
) -> None:
    monkeypatch.setattr(deprecation_policy, "SDK_VERSION", sdk_version)

    with pytest.raises(SymbolTokenResolutionError, match="does not accept symbol-based token references"):
        resolver.resolve("USDC", "base")


@patch("almanak.framework.data.tokens.resolver._try_record_metric")
def test_resolver_rejection_preserves_error_observability(
    record_metric,
    monkeypatch: pytest.MonkeyPatch,
    resolver: TokenResolver,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(deprecation_policy, "SDK_VERSION", "3.0.0")

    with caplog.at_level(logging.WARNING, logger="almanak.framework.data.tokens.resolver"):
        with pytest.raises(SymbolTokenResolutionError):
            resolver.resolve("USDC", "base")

    assert resolver.stats()["errors"] == 1
    error_logs = [record for record in caplog.records if "token_resolution_error" in record.getMessage()]
    assert len(error_logs) == 1
    assert error_logs[0].error_type == "SymbolTokenResolutionError"
    record_metric.assert_any_call("record_token_resolution_error", "base", "SymbolTokenResolutionError")
