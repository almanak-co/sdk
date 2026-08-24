"""Identity resolution for Uniswap V4 pool ids (ALM-3368).

V4 pools are 32-byte PoolIds on the PoolManager singleton, not contracts —
so this probe claims 64-hex inputs only. A PoolId cannot be reversed to a
PoolKey off-chain (it is a one-way hash); the canonical hookless keys are
re-derived per candidate fee tier and matched, with initialization verified
via StateView (nonzero sqrtPriceX96).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from almanak.connectors._strategy_base.pool_validation_base import reader_rpc_call

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient


def identify_pool_payload(
    spec,
    chain: str,
    address: str,
    *,
    gateway_client: GatewayClient | None = None,
    rpc_url: str | None = None,
    timeout: float = 10.0,
) -> dict | None:
    body = address.removeprefix("0x").removeprefix("0X")
    if len(body) != 64:
        return None
    pool_id = "0x" + body.lower()

    from almanak.connectors._strategy_base.v4_pool_abi import encode_get_slot0
    from almanak.framework.data.pools.reader import decode_slot0

    state_view = spec.factory_addresses.get(chain.lower())
    if state_view is None:
        return None

    rpc = reader_rpc_call(gateway_client=gateway_client, rpc_url=rpc_url, timeout=timeout)
    # StateView.getSlot0 never reverts for unknown ids (it reads storage and
    # returns zeroes), so a raised fault here is transport/decode trouble —
    # propagate it for the executor's probe-fault accounting, don't abstain.
    sqrt_price_x96, tick = decode_slot0(rpc(chain.lower(), state_view, encode_get_slot0(pool_id)))
    if sqrt_price_x96 == 0:
        return {
            "kind": "pool_id",
            "family": "clamm",
            "protocol": spec.protocol,
            "pool_address": pool_id,
            "factory_verified": "mismatch",
            "identified_via": "state_view",
            "notes": ["PoolId shape, but StateView reads it as uninitialized on this chain."],
        }
    return {
        "kind": "pool_id",
        "family": "clamm",
        "protocol": spec.protocol,
        "pool_address": pool_id,
        "tick": tick,
        "factory_verified": "verified",
        "identified_via": "state_view",
        "notes": ["Uniswap V4 pool ids are synthetic 32-byte keys on the PoolManager singleton, not contracts."],
    }
