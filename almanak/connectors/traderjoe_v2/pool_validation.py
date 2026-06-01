"""TraderJoe V2 LB pool-existence validation (connector-owned).

The TraderJoe V2 connector owns its LBPair validator. It queries the LBFactory's
``getLBPairInformation(address,address,uint256)`` (selector ``0x704037bd``) to
resolve the pair address, then — unless ``allow_empty_reserves`` is set —
confirms non-zero liquidity via the pair's ``getReserves()`` (selector
``0x0902f1ac``). Factory addresses are resolved through :class:`AddressRegistry`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.connectors._strategy_base.pool_validation_base import (
    ZERO_ADDRESS,
    PoolValidationReason,
    PoolValidationResult,
    decode_address,
    eth_call,
)

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

__all__ = ["validate_traderjoe_pool"]

# getLBPairInformation(address,address,uint256) selector
# See `almanak/connectors/traderjoe_v2/abis/LBFactory.json`
_TRADERJOE_GET_LB_PAIR_INFO_SELECTOR = "0x704037bd"

# TraderJoe V2 LBPair getReserves() selector
_TRADERJOE_GET_RESERVES_SELECTOR = "0x0902f1ac"


def validate_traderjoe_pool(
    chain: str,
    token_x: str,
    token_y: str,
    bin_step: int,
    rpc_url: str | None,
    gateway_client: GatewayClient | None = None,
    *,
    allow_empty_reserves: bool = False,
) -> PoolValidationResult:
    """Validate that a TraderJoe V2 LBPair pool exists on-chain.

    Uses the factory's getLBPairInformation(address,address,uint256) method.

    Args:
        chain: Chain name (e.g. "avalanche").
        token_x: Token X address.
        token_y: Token Y address.
        bin_step: Bin step of the pair (e.g. 20).
        rpc_url: RPC URL for on-chain query. If None, returns unknown unless gateway_client is available.
        gateway_client: Optional connected gateway client for gateway-routed eth_call.
        allow_empty_reserves: If True, skip the zero-liquidity check. Set True
            for LP_OPEN flows where seeding an empty pool is valid.

    Returns:
        PoolValidationResult with exists=True/False/None.
    """
    if rpc_url is None and gateway_client is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_UNAVAILABLE,
            warning=f"No RPC URL available — cannot verify TraderJoe V2 pool existence on {chain}",
        )

    factory = AddressRegistry.resolve_contract_address("traderjoe_v2", chain, "factory")
    if not factory:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.FACTORY_MISSING,
            warning=f"No TraderJoe V2 factory address for chain '{chain}' — cannot verify pool existence",
        )

    x = token_x.lower().replace("0x", "").zfill(64)
    y = token_y.lower().replace("0x", "").zfill(64)
    bs = hex(bin_step)[2:].zfill(64)
    calldata = _TRADERJOE_GET_LB_PAIR_INFO_SELECTOR + x + y + bs

    raw = eth_call(rpc_url or "", factory, calldata, chain=chain, gateway_client=gateway_client)

    if raw is None:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.RPC_FAILED,
            warning=f"RPC call to TraderJoe V2 factory failed on {chain} — cannot verify pool existence",
        )

    # getLBPairInformation returns (uint16 binStep, address LBPair, bool createdByOwner, bool ignoredForRouting)
    # LBPair address is in the second 32-byte word (offset 32-64)
    if len(raw) < 64:
        return PoolValidationResult(
            exists=None,
            reason=PoolValidationReason.NOT_CONFIGURED,
            warning=f"Unexpected response from TraderJoe V2 factory on {chain}",
        )

    pool_address = decode_address(raw[32:64])

    if pool_address == ZERO_ADDRESS:
        return PoolValidationResult(
            exists=False,
            reason=PoolValidationReason.NOT_FOUND,
            error=(
                f"No TraderJoe V2 pool found for "
                f"{token_x[:10]}.../{token_y[:10]}... with bin step {bin_step} on {chain}. "
                f"The pool may not exist or may use a different bin step."
            ),
        )

    # Pool address exists in factory — verify it has actual liquidity.
    # Skip for LP_OPEN where seeding an empty pool is valid.
    if not allow_empty_reserves:
        reserves_raw = eth_call(
            rpc_url or "",
            pool_address,
            _TRADERJOE_GET_RESERVES_SELECTOR,
            chain=chain,
            gateway_client=gateway_client,
        )
        if reserves_raw is not None and len(reserves_raw) >= 64:
            reserve_x = int.from_bytes(reserves_raw[0:32], "big")
            reserve_y = int.from_bytes(reserves_raw[32:64], "big")
            if reserve_x == 0 and reserve_y == 0:
                return PoolValidationResult(
                    exists=False,
                    reason=PoolValidationReason.NOT_FOUND,
                    error=(
                        f"TraderJoe V2 pool exists for "
                        f"{token_x[:10]}.../{token_y[:10]}... with bin step {bin_step} on {chain}, "
                        f"but has zero liquidity. The swap would revert on-chain."
                    ),
                )

    return PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=pool_address)
