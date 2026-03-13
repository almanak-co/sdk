"""Aerodrome permission hints for permission discovery."""

from almanak.framework.permissions.hints import PermissionHints

PERMISSION_HINTS = PermissionHints(
    synthetic_position_id="{token0}/{token1}/volatile",
    selector_labels={
        "0xa026383e": "exactInputSingle(ExactInputSingleParams)",
        "0x5a47ddc3": "addLiquidity(address,address,bool,uint256,uint256,uint256,uint256,address,uint256)",
        "0x0dede6c4": "removeLiquidity(address,address,bool,uint256,uint256,uint256,address,uint256)",
        "0xcac88ea9": "swapExactTokensForTokens(uint256,uint256,Route[],address,uint256)",
    },
)
