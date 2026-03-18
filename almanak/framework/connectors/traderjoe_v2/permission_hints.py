"""TraderJoe V2 permission hints for permission discovery.

TraderJoe V2 LP operations (addLiquidity, removeLiquidity) require RPC
to query the active bin and LP token balances, so compilation-based
discovery fails in offline mode.  Static permissions ensure the
manifest always includes the LBRouter selectors.

LBRouter function selectors:
- addLiquidity(LiquidityParameters)     = 0xa3c7271a
- removeLiquidity(address,address,uint16,uint256,uint256,uint256[],uint256[],address,uint256)
                                        = 0xc22159b6
"""

from almanak.framework.intents.compiler import LP_POSITION_MANAGERS
from almanak.framework.permissions.hints import PermissionHints, StaticPermissionEntry

# TraderJoe V2 LBRouter selectors that need RPC for compilation
_TRADERJOE_ADD_LIQUIDITY_SELECTOR = "0xa3c7271a"
_TRADERJOE_REMOVE_LIQUIDITY_SELECTOR = "0xc22159b6"


def _build_static_permissions() -> dict[str, list[StaticPermissionEntry]]:
    """Build static permissions for TraderJoe V2 LBRouter on all configured chains."""
    result: dict[str, list[StaticPermissionEntry]] = {}
    for chain, managers in LP_POSITION_MANAGERS.items():
        router = managers.get("traderjoe_v2")
        if not router:
            continue
        result[chain] = [
            StaticPermissionEntry(
                target=router.lower(),
                label="TraderJoe V2 LBRouter",
                selectors={
                    _TRADERJOE_ADD_LIQUIDITY_SELECTOR: "addLiquidity(LiquidityParameters)",
                    _TRADERJOE_REMOVE_LIQUIDITY_SELECTOR: "removeLiquidity(address,address,uint16,uint256,uint256,uint256[],uint256[],address,uint256)",
                },
            ),
        ]
    return result


PERMISSION_HINTS = PermissionHints(
    supports_standalone_fee_collection=True,
    static_permissions=_build_static_permissions(),
    selector_labels={
        _TRADERJOE_ADD_LIQUIDITY_SELECTOR: "addLiquidity(LiquidityParameters)",
        _TRADERJOE_REMOVE_LIQUIDITY_SELECTOR: "removeLiquidity(address,address,uint16,uint256,uint256,uint256[],uint256[],address,uint256)",
    },
)
