"""Regression tests for the CoinGecko chain-scoped address lookup (VIB-3259).

Phase 2 deletes the process-wide ``{address → coingecko_id}`` reverse map that
``_resolve_token_id`` used to consult. That map was NOT chain-scoped, so a
token whose address happened to match across two chains (USDC vs USDC.e, or
same-address same-bytecode deploys) would first-write-wins return the wrong
chain's CoinGecko ID on the losing chain — and the aggregator would happily
cache a wrong-chain price.

All address-based lookups now route through the chain-scoped
``/simple/token_price/{platform}`` endpoint via ``_try_fetch_by_address``,
keyed on ``ResolvedToken.chain``. These tests pin that behaviour.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.tokens import ResolvedToken
from almanak.gateway.data.price.coingecko import CoinGeckoPriceSource


# A collision scenario — two chains served the "same" contract address. The
# only real-world parallel is same-bytecode deploys across EVM chains, but
# for the unit test we fabricate a deliberately-identical address so the test
# is independent of any specific deploy.
COLLIDING_ADDRESS = "0x1234567890aBcDeF1234567890AbCdEf12345678"


def _mock_session_capture(source: CoinGeckoPriceSource, payload: dict):
    """Patch the CoinGecko source's session to record every GET call.

    Returns a (captured, patcher) tuple where ``captured`` collects every
    (url, params) tuple the source issues. The patcher must be used as a
    context manager (``with patcher: ...``).
    """
    captured: list[tuple[str, dict]] = []

    resp = MagicMock()
    resp.status = 200
    resp.json = AsyncMock(return_value=payload)
    resp.text = AsyncMock(return_value="")

    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=resp)
    cm.__aexit__ = AsyncMock(return_value=False)

    def _get(url, params=None):  # noqa: ANN001 - MagicMock callable
        captured.append((url, dict(params or {})))
        return cm

    session = MagicMock()
    session.get = MagicMock(side_effect=_get)
    patcher = patch.object(source, "_get_session", new_callable=AsyncMock, return_value=session)
    return captured, patcher


@pytest.mark.asyncio
async def test_same_address_two_chains_hits_correct_platform_endpoint():
    """The same address on two chains must hit DIFFERENT CoinGecko platform
    endpoints and return DIFFERENT prices — no cross-chain collision."""
    source = CoinGeckoPriceSource(cache_ttl=30)

    # Payload returns a *different* price per chain so we can prove the
    # response for arbitrum really came from the arbitrum-one endpoint.
    arb_payload = {COLLIDING_ADDRESS.lower(): {"usd": 1.00}}
    base_payload = {COLLIDING_ADDRESS.lower(): {"usd": 9.99}}

    arb_token = ResolvedToken(
        symbol="FOO",
        address=COLLIDING_ADDRESS,
        decimals=18,
        chain="arbitrum",
        chain_id=42161,
        source="test",
        is_verified=False,
    )
    base_token = ResolvedToken(
        symbol="FOO",
        address=COLLIDING_ADDRESS,
        decimals=18,
        chain="base",
        chain_id=8453,
        source="test",
        is_verified=False,
    )

    captured_arb, arb_patcher = _mock_session_capture(source, arb_payload)
    with arb_patcher:
        arb_result = await source.get_price(COLLIDING_ADDRESS, "USD", resolved_token=arb_token)

    captured_base, base_patcher = _mock_session_capture(source, base_payload)
    with base_patcher:
        base_result = await source.get_price(COLLIDING_ADDRESS, "USD", resolved_token=base_token)

    # Prices came back per-chain — no cross-chain cache pollution.
    assert arb_result.price == Decimal("1.00")
    assert base_result.price == Decimal("9.99")

    # The URLs must carry the chain-scoped platform slug. This is the real
    # correctness guarantee — proves the source actually dispatched by chain
    # rather than looking the address up in some chain-agnostic map.
    assert any("/simple/token_price/arbitrum-one" in url for url, _ in captured_arb)
    assert any("/simple/token_price/base" in url for url, _ in captured_base)


@pytest.mark.asyncio
async def test_address_input_without_resolved_token_returns_unknown_token():
    """Passing a bare address with no ResolvedToken must now raise
    DataSourceUnavailable('Unknown token: ...').

    The old reverse-map branch silently guessed the chain; Phase 2 removes
    that guess so callers MUST provide a ResolvedToken (which carries the
    chain) to get an address-based price.
    """
    from almanak.framework.data.interfaces import DataSourceUnavailable

    source = CoinGeckoPriceSource(cache_ttl=30)

    with pytest.raises(DataSourceUnavailable) as exc_info:
        await source.get_price(COLLIDING_ADDRESS, "USD", resolved_token=None)

    # Error must name the offending token so ops can debug.
    assert "Unknown token" in str(exc_info.value)
    assert COLLIDING_ADDRESS.upper() in str(exc_info.value).upper()


@pytest.mark.asyncio
async def test_symbol_lookup_unaffected_by_address_map_removal():
    """Symbol-based resolution must still work — removing the address map
    did not touch the symbol path (which is still needed for tokens without
    a ResolvedToken)."""
    source = CoinGeckoPriceSource(cache_ttl=30)

    # USDC is in the static symbol registry → resolves to 'usd-coin'.
    assert source._resolve_token_id("USDC") == "usd-coin"
