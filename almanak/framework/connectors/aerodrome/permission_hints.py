"""Aerodrome permission hints for permission discovery."""

from almanak.core.contracts import AERODROME
from almanak.framework.permissions.hints import PermissionHints, StaticPermissionEntry

# Build static removeLiquidity permissions for each chain where Aerodrome is deployed.
# LP_CLOSE compilation requires RPC (to query on-chain LP balance), so the compiler
# can't discover the Router's removeLiquidity selector during offline permission
# generation.  Static permissions bypass compilation entirely.
_static_permissions: dict[str, list[StaticPermissionEntry]] = {}
for _chain, _addrs in AERODROME.items():
    if "router" not in _addrs:
        continue
    _static_permissions[_chain] = [
        StaticPermissionEntry(
            target=_addrs["router"],
            label="Aerodrome Router",
            selectors={
                "0x0dede6c4": "removeLiquidity(address,address,bool,uint256,uint256,uint256,address,uint256)",
            },
        ),
    ]

PERMISSION_HINTS = PermissionHints(
    synthetic_position_id="{token0}/{token1}/volatile",
    needs_rpc_discovery=True,
    selector_labels={
        "0xa026383e": "exactInputSingle(ExactInputSingleParams)",
        "0x5a47ddc3": "addLiquidity(address,address,bool,uint256,uint256,uint256,uint256,address,uint256)",
        "0x0dede6c4": "removeLiquidity(address,address,bool,uint256,uint256,uint256,address,uint256)",
        "0xcac88ea9": "swapExactTokensForTokens(uint256,uint256,Route[],address,uint256)",
    },
    static_permissions=_static_permissions,
)
