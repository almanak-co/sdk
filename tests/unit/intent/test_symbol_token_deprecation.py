"""Intent token field symbol deprecation coverage."""

from __future__ import annotations

import warnings
from decimal import Decimal

import pytest

from almanak.framework.data.tokens import SymbolTokenResolutionError, SymbolTokenResolutionWarning
from almanak.framework.data.tokens import deprecation as deprecation_policy
from almanak.framework.intents import BridgeIntent, Intent

BASE_USDC = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
BASE_WETH = "0x4200000000000000000000000000000000000006"
BASE_USDC_CAIP19 = f"eip155:8453/erc20:{BASE_USDC}"
SOLANA_USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


def test_swap_token_fields_warn_on_2x(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(deprecation_policy, "SDK_VERSION", "2.99.9")

    with pytest.warns(SymbolTokenResolutionWarning) as caught:
        intent = Intent.swap(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1"),
            chain="base",
        )
    assert intent.from_token == "USDC"
    assert len(caught) == 2
    messages = [str(item.message) for item in caught]
    assert any("SwapIntent.from_token" in message for message in messages)
    assert any("SwapIntent.to_token" in message for message in messages)
    assert all("unreliable" in message and "contract address" in message for message in messages)


def test_direct_intent_construction_uses_the_same_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(deprecation_policy, "SDK_VERSION", "2.99.9")

    with pytest.warns(SymbolTokenResolutionWarning, match="BridgeIntent.token"):
        intent = BridgeIntent(
            token="USDC",
            amount=Decimal("1"),
            from_chain="base",
            to_chain="arbitrum",
        )

    assert intent.token == "USDC"


@pytest.mark.parametrize(
    ("from_token", "to_token", "destination_chain"),
    [
        (BASE_USDC, BASE_WETH, None),
        (BASE_USDC_CAIP19, BASE_WETH, None),
        (BASE_USDC, SOLANA_USDC, "solana"),
    ],
)
def test_address_based_intent_tokens_are_not_deprecated(
    monkeypatch: pytest.MonkeyPatch,
    from_token: str,
    to_token: str,
    destination_chain: str | None,
) -> None:
    monkeypatch.setattr(deprecation_policy, "SDK_VERSION", "2.99.9")

    with warnings.catch_warnings():
        warnings.simplefilter("error", SymbolTokenResolutionWarning)
        intent = Intent.swap(
            from_token=from_token,
            to_token=to_token,
            amount=Decimal("1"),
            chain="base",
            destination_chain=destination_chain,
        )

    assert intent.from_token == from_token
    assert intent.to_token == to_token


@pytest.mark.parametrize("sdk_version", ["3.0.0", "3.0.1", "4.0.0"])
def test_intent_symbols_are_rejected_from_3_0_0(
    monkeypatch: pytest.MonkeyPatch,
    sdk_version: str,
) -> None:
    monkeypatch.setattr(deprecation_policy, "SDK_VERSION", sdk_version)

    with pytest.raises(SymbolTokenResolutionError, match="SwapIntent.from_token"):
        Intent.swap(
            from_token="USDC",
            to_token=BASE_WETH,
            amount=Decimal("1"),
            chain="base",
        )
