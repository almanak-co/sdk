"""Pair→poolId payload resolution for Uniswap V4 (agent-tool lane, ALM-3365).

Loaded via ``POOL_READER_SPEC.pair_resolver``. Thin adapter over the
connector-bound :class:`UniswapV4PoolReader`, which already derives the
canonical hookless PoolId per fee tier and verifies initialization on-chain
via StateView.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.connectors._strategy_base.pool_validation_base import reader_rpc_call

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient


def resolve_pair_payload(
    chain: str,
    token_a: str,
    token_b: str,
    *,
    fee_tier: int | None = None,
    gateway_client: GatewayClient | None = None,
    rpc_url: str | None = None,
    usd_price: Callable[[str], Decimal | None] | None = None,  # noqa: ARG001 — uniform pair-resolver signature
    timeout: float = 10.0,
) -> dict | None:
    from almanak.connectors.uniswap_v4.pool_reader import POOL_READER_SPEC
    from almanak.framework.data.pools.reader import UniswapV4PoolReader

    reader = UniswapV4PoolReader(
        rpc_call=reader_rpc_call(gateway_client=gateway_client, rpc_url=rpc_url, timeout=timeout),
        spec=POOL_READER_SPEC,
    )
    tiers = (fee_tier,) if fee_tier is not None else POOL_READER_SPEC.candidate_pool_keys
    best: dict | None = None
    best_liquidity = -1
    read_fault: Exception | None = None
    for tier in tiers:
        pool_id = reader.resolve_pool_address(token_a, token_b, chain, fee_tier=tier)
        if pool_id is None:
            continue
        try:
            state = reader.read_pool_price(pool_id, chain).value
        except Exception as e:  # noqa: BLE001 — an unreadable candidate is skipped unless nothing resolves
            read_fault = e
            continue
        liquidity = state.liquidity or 0
        payload = {
            "pool_address": pool_id,
            "fee_tier": tier,
            "fee_tier_source": "explicit" if fee_tier is not None else "sweep",
            "current_price": str(state.price),
            "tick": state.tick,
            "liquidity": str(liquidity),
            "token0_decimals": state.token0_decimals,
            "token1_decimals": state.token1_decimals,
            "resolved_via": "v4_pool_key_derivation",
            "notes": ["Uniswap V4 pool ids are synthetic 32-byte keys on the PoolManager singleton, not contracts."],
        }
        if liquidity > best_liquidity:
            best, best_liquidity = payload, liquidity
    if best is None and read_fault is not None:
        # A derived poolId existed but its state read faulted: transport
        # trouble, not proof the pair has no initialized pool.
        raise RuntimeError(f"Uniswap V4 pair resolution indeterminate on {chain}") from read_fault
    return best
