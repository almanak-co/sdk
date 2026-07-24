"""MarketSnapshot symbol token deprecation coverage."""

from __future__ import annotations

import warnings
from decimal import Decimal

import pytest

from almanak.framework.data.tokens import SymbolTokenResolutionError, SymbolTokenResolutionWarning
from almanak.framework.data.tokens import deprecation as deprecation_policy
from almanak.framework.market import MarketSnapshot, MarketSnapshotBuilder

BASE_USDC = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
BASE_USDC_CAIP19 = f"eip155:8453/erc20:{BASE_USDC}"


def _market(token: str = BASE_USDC) -> MarketSnapshot:
    return MarketSnapshotBuilder.seeded(chain="base", prices={token: Decimal("1")})


def test_market_snapshot_symbol_warns_on_2x(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(deprecation_policy, "SDK_VERSION", "2.99.9")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", SymbolTokenResolutionWarning)
        market = _market("USDC")
        assert market.price("USDC") == Decimal("1")

    symbol_warnings = [item for item in caught if item.category is SymbolTokenResolutionWarning]
    assert len(symbol_warnings) == 1
    assert all("unreliable" in str(item.message) for item in symbol_warnings)


@pytest.mark.parametrize("token", [BASE_USDC, f"base:{BASE_USDC}", BASE_USDC_CAIP19])
def test_market_snapshot_address_identity_is_not_deprecated(
    monkeypatch: pytest.MonkeyPatch,
    token: str,
) -> None:
    monkeypatch.setattr(deprecation_policy, "SDK_VERSION", "2.99.9")

    with warnings.catch_warnings():
        warnings.simplefilter("error", SymbolTokenResolutionWarning)
        assert _market(token).price(token) == Decimal("1")


def test_market_snapshot_rejects_symbols_from_3_0_0(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(deprecation_policy, "SDK_VERSION", "3.0.0")

    with pytest.raises(SymbolTokenResolutionError, match="MarketSnapshot"):
        _market().price("USDC")


def test_prices_accessor_rejects_symbols_from_3_0_0(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(deprecation_policy, "SDK_VERSION", "3.0.0")

    with pytest.raises(SymbolTokenResolutionError, match="MarketSnapshot"):
        _market().prices["USDC"]
