"""VIB-4475 regression: ensure the V0 scope guards do not produce false positives.

The V0 hookless ERC20-ERC20 happy path must continue to compile cleanly after
the hooks≠0 / native-ETH guards land. A regression here would mean the guard
widened past its intended scope.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.uniswap_v4.adapter import (
    UniswapV4Adapter,
    UniswapV4Config,
    UniswapV4UnsupportedPoolError,
)


def _make_resolver():
    resolver = MagicMock()

    def resolve_for_swap(symbol, chain):
        tokens = {
            "WETH": MagicMock(address="0x82af49447d8a07e3bd95bd0d56f35241523fbab1", decimals=18, is_native=False),
            "USDC": MagicMock(address="0xaf88d065e77c8cc2239327c5edb3a432268e5831", decimals=6, is_native=False),
        }
        return tokens[symbol.upper()]

    def resolve(symbol_or_addr, chain):
        return resolve_for_swap(symbol_or_addr, chain)

    resolver.resolve_for_swap = resolve_for_swap
    resolver.resolve = resolve
    return resolver


@pytest.fixture()
def adapter():
    config = UniswapV4Config(
        chain="arbitrum",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
    )
    return UniswapV4Adapter(config=config, token_resolver=_make_resolver())


def test_hookless_erc20_erc20_pool_compiles(adapter):
    """The V0 supported shape (no hooks, both ERC20) must compile to a non-empty bundle."""
    from almanak.framework.intents.vocabulary import LPOpenIntent

    intent = LPOpenIntent(
        pool="WETH/USDC/3000",
        amount0=Decimal("0.1"),
        amount1=Decimal("200"),
        range_lower=Decimal("1500"),
        range_upper=Decimal("2500"),
        protocol="uniswap_v4",
    )
    price_oracle = {"WETH": Decimal("2000"), "USDC": Decimal("1")}

    # Must NOT raise the V0 guard.
    bundle = adapter.compile_lp_open_intent(intent, price_oracle)

    assert bundle.intent_type == "LP_OPEN"
    assert len(bundle.transactions) > 0, "Happy path must produce transactions"
    assert "error" not in bundle.metadata, "Happy path must not soft-error"
    assert bundle.metadata.get("protocol_version") == "v4"
    # The hooks field on PoolKey should be the zero address for V0.
    assert bundle.metadata.get("hooks") == "0x0000000000000000000000000000000000000000"


def test_hookless_with_explicit_zero_hooks_compiles(adapter):
    """Explicit hooks=0x0 in protocol_params must also pass the guard."""
    from almanak.framework.intents.vocabulary import LPOpenIntent

    intent = LPOpenIntent(
        pool="WETH/USDC/3000",
        amount0=Decimal("0.1"),
        amount1=Decimal("200"),
        range_lower=Decimal("1500"),
        range_upper=Decimal("2500"),
        protocol="uniswap_v4",
        protocol_params={
            "hooks": "0x0000000000000000000000000000000000000000",
        },
    )
    price_oracle = {"WETH": Decimal("2000"), "USDC": Decimal("1")}

    bundle = adapter.compile_lp_open_intent(intent, price_oracle)
    assert bundle.intent_type == "LP_OPEN"
    assert len(bundle.transactions) > 0


def test_guard_does_not_fire_on_erc20_currencies():
    """The standalone guard helper accepts ERC20-ERC20 hookless input."""
    from types import SimpleNamespace

    pool_key = SimpleNamespace(
        currency0="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        currency1="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        hooks="0x0000000000000000000000000000000000000000",
    )

    # No raise.
    UniswapV4Adapter._reject_unsupported_v0_pool(pool_key)


def test_guard_fires_on_hooks_via_helper():
    """Direct unit test of the guard helper for hooks rejection."""
    from types import SimpleNamespace

    pool_key = SimpleNamespace(
        currency0="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        currency1="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        hooks="0x0000000000000000000000000000000000000800",
    )
    with pytest.raises(UniswapV4UnsupportedPoolError, match="VIB-4485"):
        UniswapV4Adapter._reject_unsupported_v0_pool(pool_key)


def test_guard_fires_on_native_via_helper():
    """Direct unit test of the guard helper for native-ETH rejection."""
    from types import SimpleNamespace

    pool_key = SimpleNamespace(
        currency0="0x0000000000000000000000000000000000000000",
        currency1="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        hooks="0x0000000000000000000000000000000000000000",
    )
    with pytest.raises(UniswapV4UnsupportedPoolError, match="VIB-4483"):
        UniswapV4Adapter._reject_unsupported_v0_pool(pool_key)
