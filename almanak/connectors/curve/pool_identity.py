"""Address-form Curve pool identification (ALM-3368).

Wraps :func:`pool_resolver.resolve_pool_metadata`: the MetaRegistry IS the
provenance authority, so a successful resolution is registry-verified by
construction and a fail-closed ``None`` means "not a registered Curve pool".
"""

from __future__ import annotations

from typing import TYPE_CHECKING

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
    from almanak.connectors.curve.pool_resolver import resolution_is_definitive, resolve_pool_metadata

    meta = resolve_pool_metadata(chain, address, gateway_client=gateway_client, rpc_url=rpc_url, timeout=timeout)
    if meta is None:
        if not resolution_is_definitive(chain, address):
            # Transient transport blip, not a confirmed non-membership — a
            # silent None here would let the ERC-20 fallback misreport a
            # Curve pool/LP as a plain token.
            raise RuntimeError(f"Curve identity probe indeterminate for {address} on {chain}")
        return None
    payload = {
        "kind": "pool",
        "family": meta.pool_type,
        "protocol": spec.protocol,
        "pool_address": meta.address.lower(),
        "token0": meta.coin_addresses[0] if meta.coin_addresses else None,
        "token1": meta.coin_addresses[1] if len(meta.coin_addresses) > 1 else None,
        "coins": list(meta.coin_addresses),
        "coin_symbols": list(meta.coin_symbols),
        "coin_decimals": list(meta.coin_decimals),
        "lp_token": meta.lp_token,
        "pool_type": meta.pool_type,
        "is_metapool": meta.is_metapool,
        "factory_verified": "verified",
        "identified_via": "meta_registry",
    }
    if meta.lp_token and meta.lp_token.lower() != meta.address.lower():
        payload["notes"] = ["Pool and LP token are separate contracts — fund/track the LP token, trade the pool."]
    return payload
