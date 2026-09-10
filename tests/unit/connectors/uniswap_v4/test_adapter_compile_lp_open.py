"""Hook admission preserves hookless LP compilation and explicit pool identity."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.connectors.uniswap_v4.adapter import (
    UniswapV4Adapter,
    UniswapV4Config,
)
from almanak.connectors.uniswap_v4.behavior import admit_hook


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
    """Hookless ERC20 pools compile to a non-empty bundle."""
    from almanak.framework.intents.vocabulary import LPOpenIntent

    intent = LPOpenIntent(
        pool="WETH/USDC/3000",
        amount0=Decimal("0.1"),
        amount1=Decimal("200"),
        range_lower=Decimal("1500"),
        range_upper=Decimal("2500"),
        protocol="uniswap_v4",
        # No rpc_url on this adapter → estimated price; opt in (VIB-2180).
        protocol_params={"allow_estimated_price": True},
    )
    price_oracle = {"WETH": Decimal("2000"), "USDC": Decimal("1")}

    bundle = adapter.compile_lp_open_intent(intent, price_oracle)

    assert bundle.intent_type == "LP_OPEN"
    assert len(bundle.transactions) > 0, "Happy path must produce transactions"
    assert "error" not in bundle.metadata, "Happy path must not soft-error"
    assert bundle.metadata.get("protocol_version") == "v4"
    assert bundle.metadata.get("hooks") == "0x0000000000000000000000000000000000000000"


def test_hookless_with_explicit_zero_hooks_compiles(adapter):
    """Explicit zero hooks preserve hookless admission."""
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
            # No rpc_url on this adapter → estimated price; opt in (VIB-2180).
            "allow_estimated_price": True,
        },
    )
    price_oracle = {"WETH": Decimal("2000"), "USDC": Decimal("1")}

    bundle = adapter.compile_lp_open_intent(intent, price_oracle)
    assert bundle.intent_type == "LP_OPEN"
    assert len(bundle.transactions) > 0


def test_hookless_erc20_admission_needs_no_gateway():
    """Hookless admission requires no runtime observations."""
    from types import SimpleNamespace

    pool_key = SimpleNamespace(
        currency0="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        currency1="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        hooks="0x0000000000000000000000000000000000000000",
    )

    admit_hook(chain="arbitrum", key=pool_key, operation="lp_open", route="position_manager_eoa", hook_data=b"", gateway=None, block_number=1)


def test_unreviewed_hook_is_refused():
    """An active callback requires a reviewed behavior profile."""
    from types import SimpleNamespace

    pool_key = SimpleNamespace(
        currency0="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        currency1="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        hooks="0x0000000000000000000000000000000000000800",
    )
    with pytest.raises(ValueError, match="No reviewed hook"):
        admit_hook(chain="arbitrum", key=pool_key, operation="lp_open", route="position_manager_eoa", hook_data=b"", gateway=None, block_number=1)


def test_hookless_native_currency_is_admitted():
    """Native currency identity does not require hook behavior evidence."""
    from types import SimpleNamespace

    pool_key = SimpleNamespace(
        currency0="0x0000000000000000000000000000000000000000",
        currency1="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        hooks="0x0000000000000000000000000000000000000000",
    )
    admit_hook(chain="arbitrum", key=pool_key, operation="lp_open", route="position_manager_eoa", hook_data=b"", gateway=None, block_number=1)


@pytest.mark.parametrize("pool_id", [None, 7, "0x" + "00" * 32])
def test_lp_open_rejects_conflicting_redundant_pool_id(adapter, pool_id):
    from almanak.framework.intents.vocabulary import LPOpenIntent

    intent = LPOpenIntent(
        pool="WETH/USDC/3000",
        amount0=Decimal("0.1"),
        amount1=Decimal("200"),
        range_lower=Decimal("1500"),
        range_upper=Decimal("2500"),
        protocol="uniswap_v4",
        protocol_params={"pool_id": pool_id},
    )
    with pytest.raises(ValueError, match="pool_id conflicts"):
        adapter._prepare_lp_open_pool(intent)


def test_lp_open_accepts_matching_redundant_pool_id(adapter):
    from almanak.framework.intents.vocabulary import LPOpenIntent

    intent = LPOpenIntent(
        pool="WETH/USDC/3000",
        amount0=Decimal("0.1"),
        amount1=Decimal("200"),
        range_lower=Decimal("1500"),
        range_upper=Decimal("2500"),
        protocol="uniswap_v4",
    )
    pool = adapter._prepare_lp_open_pool(intent)
    pinned = intent.model_copy(update={"protocol_params": {"pool_id": pool.pool_id}})
    assert adapter._prepare_lp_open_pool(pinned).pool_key == pool.pool_key


@pytest.mark.parametrize(
    "params",
    [
        {"fee_tier": 31100},
        {"fee_tier": True},
        {"tick_spacing": 17},
        {"hooks": "0x" + "1" * 40},
        {"pool_id": "0x" + "0" * 64},
    ],
)
def test_owned_lp_identity_rejects_conflicting_pins(params):
    from almanak.connectors.uniswap_v4.pool_key import PoolKey

    key = PoolKey("0x" + "0" * 40, "0x" + "2" * 40, 777, 10)
    with pytest.raises(ValueError, match="conflicts"):
        UniswapV4Adapter._validate_lp_pool_pins(key, params)
