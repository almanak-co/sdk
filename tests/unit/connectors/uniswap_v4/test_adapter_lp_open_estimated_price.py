"""VIB-2180: surface estimated sqrtPrice provenance + cap silent slippage override.

When the on-chain ``StateView.getSlot0()`` query is unavailable the V4 LP adapter
estimates ``sqrtPriceX96`` from oracle prices (or a range midpoint). Pre-VIB-2180 it
then silently bumped the user's slippage to 30%. These tests pin the new behaviour:

- ``metadata["price_source"]`` surfaces one of ``on_chain`` / ``oracle_estimate`` /
  ``range_midpoint_estimate`` so the strategy author can tell the LP opened on an
  estimate.
- ``metadata["estimated_sqrt_price_x96"]`` carries the estimated value (None on-chain).
- An estimated-price open with too-tight ``max_slippage`` and no opt-in fails LOUD
  (raises, not a soft-error empty bundle) rather than silently widening tolerance >2x.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.uniswap_v4.adapter import (
    UniswapV4Adapter,
    UniswapV4Config,
    UniswapV4EstimatedPriceWithoutOptInError,
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


def _make_adapter(rpc_url: str | None = None):
    config = UniswapV4Config(
        chain="arbitrum",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        rpc_url=rpc_url,
    )
    return UniswapV4Adapter(config=config, token_resolver=_make_resolver())


def _make_intent(*, max_slippage=None, protocol_params=None):
    """Build an LP_OPEN intent stand-in.

    ``LPOpenIntent`` is a frozen pydantic model with NO ``max_slippage`` field —
    the adapter reads it defensively via ``getattr(intent, "max_slippage", None)``
    (it defaults to ``Decimal("0.005")`` when absent). To exercise the slippage
    guard with arbitrary user tolerances we use a SimpleNamespace carrying exactly
    the attributes ``compile_lp_open_intent`` accesses, matching production access.
    """
    return SimpleNamespace(
        pool="WETH/USDC/3000",
        amount0=Decimal("0.1"),
        amount1=Decimal("200"),
        range_lower=Decimal("1500"),
        range_upper=Decimal("2500"),
        protocol="uniswap_v4",
        protocol_params=protocol_params,
        max_slippage=max_slippage,
        intent_id="vib2180-test",
    )


_PRICE_ORACLE = {"WETH": Decimal("2000"), "USDC": Decimal("1")}


def test_on_chain_price_sets_metadata():
    """On-chain sqrtPrice available → price_source on_chain, no estimated value."""
    adapter = _make_adapter(rpc_url="http://localhost:8545")
    intent = _make_intent()

    with patch.object(adapter._sdk, "get_pool_sqrt_price", return_value=2**96):
        bundle = adapter.compile_lp_open_intent(intent, _PRICE_ORACLE)

    assert bundle.intent_type == "LP_OPEN"
    assert len(bundle.transactions) > 0
    assert bundle.metadata["price_source"] == "on_chain"
    assert bundle.metadata["estimated_sqrt_price_x96"] is None
    # On-chain keeps the pre-existing 5% floor.
    assert bundle.metadata["effective_slippage_bps"] == 500


def test_oracle_estimate_with_opt_in_succeeds():
    """Estimated (oracle) price + opt-in + tight slippage → compiles, oracle_estimate label."""
    adapter = _make_adapter()  # no rpc_url → on-chain unavailable
    intent = _make_intent(
        max_slippage=Decimal("0.005"),
        protocol_params={"allow_estimated_price": True},
    )

    bundle = adapter.compile_lp_open_intent(intent, _PRICE_ORACLE)

    assert bundle.intent_type == "LP_OPEN"
    assert len(bundle.transactions) > 0
    assert bundle.metadata["price_source"] == "oracle_estimate"
    assert bundle.metadata["estimated_sqrt_price_x96"] is not None
    # Estimated-price floor is 10%.
    assert bundle.metadata["effective_slippage_bps"] == 1000


def test_estimated_price_tight_slippage_no_opt_in_raises():
    """Estimated price + tight slippage + no opt-in → fail loud (VIB-2180)."""
    adapter = _make_adapter()
    intent = _make_intent(max_slippage=Decimal("0.005"))

    with pytest.raises(UniswapV4EstimatedPriceWithoutOptInError, match="VIB-2180"):
        adapter.compile_lp_open_intent(intent, _PRICE_ORACLE)


def test_estimated_price_high_user_slippage_succeeds_without_opt_in():
    """Estimated price + max_slippage=10% (2x guard not tripped) → compiles, no opt-in needed."""
    adapter = _make_adapter()
    intent = _make_intent(max_slippage=Decimal("0.10"))

    bundle = adapter.compile_lp_open_intent(intent, _PRICE_ORACLE)

    assert bundle.intent_type == "LP_OPEN"
    assert len(bundle.transactions) > 0
    assert bundle.metadata["price_source"] == "oracle_estimate"
    # user_slippage (10%) == floor (10%) → floor honoured.
    assert bundle.metadata["effective_slippage_bps"] == 1000


def test_range_midpoint_estimate_label():
    """On-chain unavailable AND oracle missing a price → range_midpoint_estimate label."""
    adapter = _make_adapter()
    intent = _make_intent(
        max_slippage=Decimal("0.005"),
        protocol_params={"allow_estimated_price": True},
    )

    # Oracle missing USDC → oracle branch cannot compute mid_price, falls back to
    # the range-midpoint branch.
    price_oracle = {"WETH": Decimal("2000")}
    bundle = adapter.compile_lp_open_intent(intent, price_oracle)

    assert bundle.intent_type == "LP_OPEN"
    assert len(bundle.transactions) > 0
    assert bundle.metadata["price_source"] == "range_midpoint_estimate"
    assert bundle.metadata["estimated_sqrt_price_x96"] is not None


def test_compile_reraises_not_softbundle():
    """The tight-slippage-no-opt-in case RAISES — it must not return a soft-error bundle."""
    adapter = _make_adapter()
    intent = _make_intent(max_slippage=Decimal("0.005"))

    # Must raise, not return an ActionBundle with empty transactions + metadata["error"].
    with pytest.raises(UniswapV4EstimatedPriceWithoutOptInError):
        adapter.compile_lp_open_intent(intent, _PRICE_ORACLE)
