"""Address-form Aerodrome classic (Solidly) pool identification (ALM-3368).

Probes the Solidly shape (``token0/token1`` + ``stable()``), then anchors the
claim by asking this connector's own factory whether it deployed the address
(``getPool(token0, token1, stable)``) — ABI shape alone is spoofable.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from almanak.connectors._strategy_base.pool_identity_base import (
    STABLE_SELECTOR,
    TOKEN0_SELECTOR,
    TOKEN1_SELECTOR,
    decode_word_address,
    decode_word_uint,
    probe_call,
)
from almanak.connectors.aerodrome.pool_validation import validate_aerodrome_pool

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
    def call(to: str, data: str) -> bytes | None:
        return probe_call(chain, to, data, gateway_client=gateway_client, rpc_url=rpc_url, timeout=timeout)

    token0 = decode_word_address(call(address, TOKEN0_SELECTOR))
    token1 = decode_word_address(call(address, TOKEN1_SELECTOR))
    stable_word = decode_word_uint(call(address, STABLE_SELECTOR))
    if token0 is None or token1 is None or stable_word is None:
        return None
    stable = bool(stable_word)

    result = validate_aerodrome_pool(chain, token0, token1, stable, rpc_url, gateway_client=gateway_client)
    if not (result.exists and result.pool_address and result.pool_address.lower() == address.lower()):
        return None

    return {
        "kind": "pool",
        "family": "solidly",
        "protocol": spec.protocol,
        "pool_address": address.lower(),
        "token0": token0,
        "token1": token1,
        "stable": stable,
        "lp_token": address.lower(),
        "factory_verified": "verified",
        "identified_via": "abi-probe+factory",
    }
