"""TraderJoe V2 permission hints for permission discovery.

TraderJoe V2 LP operations (addLiquidity, removeLiquidity) require RPC
to query the active bin and LP token balances, so compilation-based
discovery fails in offline mode.  Static permissions ensure the
manifest always includes the LBRouter selectors.

SWAP compilation offline succeeds (``_compile_swap_traderjoe_v2`` falls
back to a default bin_step when RPC auto-detection is unavailable), but
``_build_swap_intents`` previously skipped TJv2 because its router is
stored in ``LP_POSITION_MANAGERS`` rather than ``PROTOCOL_ROUTERS``. That
gap is now closed in ``synthetic_intents._build_swap_intents``; the
selector label below ensures the generated manifest carries a
human-readable name for ``swapExactTokensForTokens`` (issue #1841).

LBRouter function selectors:
- addLiquidity(LiquidityParameters)     = 0xa3c7271a
- removeLiquidity(address,address,uint16,uint256,uint256,uint256[],uint256[],address,uint256)
                                        = 0xc22159b6
- swapExactTokensForTokens(uint256,uint256,(uint256[],uint8[],address[]),address,uint256)
                                        = 0x2a443fae
"""

from almanak.framework.intents.compiler import LP_POSITION_MANAGERS
from almanak.framework.permissions.hints import PermissionHints, StaticPermissionEntry

# TraderJoe V2 LBRouter selectors that need RPC for compilation
_TRADERJOE_ADD_LIQUIDITY_SELECTOR = "0xa3c7271a"
_TRADERJOE_REMOVE_LIQUIDITY_SELECTOR = "0xc22159b6"
# swapExactTokensForTokens(uint256,uint256,(uint256[],uint8[],address[]),address,uint256)
# emitted by the dedicated ``_compile_swap_traderjoe_v2`` path (VIB-1928).
_TRADERJOE_SWAP_EXACT_TOKENS_FOR_TOKENS_SELECTOR = "0x2a443fae"

_TRADERJOE_SWAP_SIG = "swapExactTokensForTokens(uint256,uint256,(uint256[],uint8[],address[]),address,uint256)"


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
                # Only LP selectors here — they require RPC for offline discovery
                # (addLiquidity / removeLiquidity query active bin + LP balances).
                # SWAP's ``swapExactTokensForTokens`` is intentionally NOT here:
                # ``static_permissions`` is merged into every manifest regardless
                # of ``intent_types``, so including the swap selector would
                # over-permission LP-only strategies. Synthetic SWAP discovery
                # picks it up via ``_build_swap_intents`` (traderjoe_v2 is now
                # in the router-exemption tuple). The selector_labels entry
                # below still provides a human-readable label when the selector
                # DOES appear in a manifest generated for SWAP.
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
        _TRADERJOE_SWAP_EXACT_TOKENS_FOR_TOKENS_SELECTOR: _TRADERJOE_SWAP_SIG,
    },
)
