"""Address-only funding joins used by the on-chain intent harness."""

from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL
from almanak.framework.intents import SwapIntent
from tests.intents._permission_onchain_harness import (
    _associate_zodiac_source_intent,
    _curve_permission_binding_for_bundle,
    _funding_key_for_token_ref,
)


@pytest.mark.parametrize(
    ("chain", "label", "expected_address"),
    [
        ("base", "sUSDai", "0x0B2b2B2076d95dda7817e785989fE353fe955ef9"),
        ("ethereum", "sUSDe", "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"),
        ("ethereum", "YT-sUSDe-26NOV2026", "0x89e6e5f7c3a60e7d6347f054051a29a272f4ce44"),
        ("arbitrum", "sUSDai", "0x0B2b2B2076d95dda7817e785989fE353fe955ef9"),
        ("arbitrum", "PT-SUSDAI-15OCT2026", "0xb459db106f645d698e74027eef6019a26a0675cc"),
        ("polygon", "frxUSD", "0x80eede496655fb9047dd39d9f418d5483ed600df"),
        ("polygon", "WBTC", "0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6"),
    ],
)
def test_long_tail_and_synthetic_labels_join_to_exact_addresses(
    chain: str,
    label: str,
    expected_address: str,
) -> None:
    assert _funding_key_for_token_ref(label, chain) == expected_address.lower()


@pytest.mark.parametrize(
    ("chain", "label"),
    [
        ("arbitrum", "ETH"),
        ("base", "ETH"),
        ("avalanche", "AVAX"),
        ("bnb", "BNB"),
        ("polygon", "MATIC"),
        ("polygon", "POL"),
    ],
)
def test_native_labels_join_to_shared_sentinel(chain: str, label: str) -> None:
    assert _funding_key_for_token_ref(label, chain) == NATIVE_SENTINEL


def test_curve_bundle_joins_exact_pool_to_test_only_permission_binding() -> None:
    pool = "0x960ea3e3C7FB317332d990873d354E18d7645590"
    intent = SwapIntent(
        from_token="USDT",
        to_token="WETH",
        amount=Decimal("1"),
        protocol="curve",
        chain="arbitrum",
    )

    binding = _curve_permission_binding_for_bundle(
        SimpleNamespace(metadata={"pool_address": pool}, _zodiac_source_intent=intent),
        "arbitrum",
    )

    assert binding == {
        "protocol": "curve",
        "resource_type": "pool",
        "chain": "arbitrum",
        "address": pool,
        "coin_addresses": [
            "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
            "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        ],
    }


def test_curve_bundle_without_exact_pool_metadata_fails_closed() -> None:
    intent = SwapIntent(
        from_token="USDT",
        to_token="WETH",
        amount=Decimal("1"),
        protocol="curve",
        chain="arbitrum",
    )
    with pytest.raises(ValueError, match="exact metadata.pool_address"):
        _curve_permission_binding_for_bundle(
            SimpleNamespace(metadata={}, _zodiac_source_intent=intent),
            "arbitrum",
        )


def test_curve_bundle_cannot_self_authorize_wrong_compiler_pool() -> None:
    intent = SwapIntent(
        from_token="USDT",
        to_token="WETH",
        amount=Decimal("1"),
        protocol="curve",
        chain="arbitrum",
    )
    wrong_pool = "0x7f90122BF0700F9E7e1F688fe926940E8839F353"

    with pytest.raises(ValueError, match="recorded intent independently selects"):
        _curve_permission_binding_for_bundle(
            SimpleNamespace(metadata={"pool_address": wrong_pool}, _zodiac_source_intent=intent),
            "arbitrum",
        )


def test_curve_asset_set_independently_selects_expected_pool() -> None:
    pool = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"
    intent = SimpleNamespace(protocol="curve", pool="USDT/USDC/DAI")

    binding = _curve_permission_binding_for_bundle(
        SimpleNamespace(metadata={"pool_address": pool}, _zodiac_source_intent=intent),
        "ethereum",
    )

    assert binding["address"] == pool


def test_curve_bundle_uses_its_own_source_intent_after_later_compile() -> None:
    two_pool = "0x7f90122BF0700F9E7e1F688fe926940E8839F353"
    source_intent = SimpleNamespace(protocol="curve", pool="2pool")
    later_intent = SimpleNamespace(protocol="curve", pool="tricrypto")
    first_bundle = SimpleNamespace(metadata={"pool_address": two_pool})
    later_bundle = SimpleNamespace(metadata={"pool_address": "0x960ea3e3C7FB317332d990873d354E18d7645590"})

    # Exercise the same association helper used by the compiler recorder: a
    # later compile must attach only to its own returned bundle.
    _associate_zodiac_source_intent(SimpleNamespace(action_bundle=first_bundle), source_intent)
    _associate_zodiac_source_intent(SimpleNamespace(action_bundle=later_bundle), later_intent)

    binding = _curve_permission_binding_for_bundle(
        first_bundle,
        "arbitrum",
    )

    assert binding["address"] == two_pool
    assert later_bundle._zodiac_source_intent is later_intent
