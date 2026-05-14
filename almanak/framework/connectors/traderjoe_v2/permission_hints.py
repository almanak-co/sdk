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

LP_CLOSE additionally needs ``approveForAll`` on the LBPair (per-pair
ERC1155-like contract). The pair address is dynamic (one contract per
``(tokenX, tokenY, binStep)`` triple) so the offline compile path cannot
resolve it without RPC. We pin the well-known LBPair addresses in
``TRADERJOE_V2_LBPAIRS`` and surface a static permission per registered
pair so the Roles modifier authorises the approval (issue #1905).

LBRouter function selectors:
- addLiquidity(LiquidityParameters)     = 0xa3c7271a
- removeLiquidity(address,address,uint16,uint256,uint256,uint256[],uint256[],address,uint256)
                                        = 0xc22159b6
- swapExactTokensForTokens(uint256,uint256,(uint256[],uint8[],address[]),address,uint256)
                                        = 0x2a443fae

LBPair function selectors:
- approveForAll(address,bool)           = 0xe584b654
- collectFees(address,uint256[])        = 0x225b20b9
"""

from almanak.core.contracts import TRADERJOE_V2_LBPAIRS
from almanak.framework.intents.compiler import LP_POSITION_MANAGERS
from almanak.framework.permissions.hints import PermissionHints, StaticPermissionEntry

# TraderJoe V2 LBRouter selectors that need RPC for compilation
_TRADERJOE_ADD_LIQUIDITY_SELECTOR = "0xa3c7271a"
_TRADERJOE_REMOVE_LIQUIDITY_SELECTOR = "0xc22159b6"
# swapExactTokensForTokens(uint256,uint256,(uint256[],uint8[],address[]),address,uint256)
# emitted by the dedicated ``_compile_swap_traderjoe_v2`` path (VIB-1928).
_TRADERJOE_SWAP_EXACT_TOKENS_FOR_TOKENS_SELECTOR = "0x2a443fae"

# approveForAll(address,bool) — emitted by LP_CLOSE compile path on the LBPair
# (per-pair ERC1155-like contract), TX 1 of the bundle. Spender is the LBRouter.
_TRADERJOE_APPROVE_FOR_ALL_SELECTOR = "0xe584b654"

# collectFees(address,uint256[]) — emitted by LP_COLLECT_FEES compile path on
# the LBPair. Standalone fee collection (no removeLiquidity / no router):
# ``LBPair.collectFees(account, binIds)`` is called directly on the per-pair
# contract by the Safe. Per-pair address is dynamic per (tokenX, tokenY,
# binStep), so we surface one static permission per registered LBPair just
# like ``approveForAll`` (issue #1855). Pinned because the offline compile
# path can't resolve the LBPair without an RPC handshake.
_TRADERJOE_COLLECT_FEES_SELECTOR = "0x225b20b9"

_TRADERJOE_SWAP_SIG = "swapExactTokensForTokens(uint256,uint256,(uint256[],uint8[],address[]),address,uint256)"
_TRADERJOE_APPROVE_FOR_ALL_SIG = "approveForAll(address,bool)"
_TRADERJOE_COLLECT_FEES_SIG = "collectFees(address,uint256[])"


def _build_static_permissions() -> dict[str, list[StaticPermissionEntry]]:
    """Build static permissions for TraderJoe V2 LBRouter + registered LBPairs.

    LBRouter entries cover ``addLiquidity`` / ``removeLiquidity`` (LP_OPEN /
    LP_CLOSE TX 2). Per-pair LBPair entries cover ``approveForAll`` (LP_CLOSE
    TX 1) for every pair registered in :data:`TRADERJOE_V2_LBPAIRS`.
    """
    result: dict[str, list[StaticPermissionEntry]] = {}
    for chain, managers in LP_POSITION_MANAGERS.items():
        router = managers.get("traderjoe_v2")
        if not router:
            continue
        entries: list[StaticPermissionEntry] = [
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

        # Per-pair LBPair entries (issue #1905). LP_CLOSE compiles to two TXs:
        #   1. LBPair.approveForAll(LBRouter, true) — target = pair contract
        #   2. LBRouter.removeLiquidity(...)        — target = router (above)
        # The LBPair target is dynamic per (tokenX, tokenY, binStep), so we
        # surface one static permission per registered pair.
        #
        # LP_COLLECT_FEES (issue #1855) compiles to a single TX:
        #   LBPair.collectFees(account, binIds) — target = pair contract
        # The router is NOT involved (TraderJoe V2 is the only LP connector
        # exposing standalone fee collection — Uni V3 / pancakeswap / sushiswap
        # / aerodrome use ``decreaseLiquidity + collect`` atomically via the
        # NPM). The LBPair target address is dynamic, so the same registry-
        # driven pinning we use for ``approveForAll`` is the only path to
        # authorise this selector under offline manifest discovery.
        #
        # Both selectors are emitted on the SAME LBPair address but for
        # different intent flows; we register one entry per (intent_type,
        # selector) so ``StaticPermissionEntry.intent_types`` can scope each
        # one independently and SWAP-only / LP_OPEN-only manifests stay at
        # least-privilege.
        for pair in TRADERJOE_V2_LBPAIRS.get(chain, []):
            address = str(pair["address"])
            label = f"TraderJoe V2 LBPair {pair['tokenX']}/{pair['tokenY']}/{pair['bin_step']}"
            entries.append(
                StaticPermissionEntry(
                    target=address.lower(),
                    label=label,
                    selectors={
                        _TRADERJOE_APPROVE_FOR_ALL_SELECTOR: _TRADERJOE_APPROVE_FOR_ALL_SIG,
                    },
                    # ``approveForAll`` is only emitted by the LP_CLOSE compile
                    # path (TX 1: ``LBPair.approveForAll(LBRouter, true)`` so
                    # the router can pull LB-tokens during ``removeLiquidity``).
                    # LP_OPEN mints LB-tokens directly to the Safe via
                    # ``addLiquidity`` and never touches this selector — see
                    # ``_compile_lp_open_traderjoe_v2`` in ``intents/compiler.py``,
                    # which only emits ERC-20 approvals for tokenX/tokenY before
                    # calling addLiquidity. SWAP doesn't touch LBPair contracts
                    # at all. Scoping to LP_CLOSE keeps SWAP-only / LP_OPEN-only
                    # manifests at least-privilege (Codex P1 / Gemini medium on
                    # PR #1923).
                    intent_types=frozenset({"LP_CLOSE"}),
                )
            )
            entries.append(
                StaticPermissionEntry(
                    target=address.lower(),
                    label=label,
                    selectors={
                        _TRADERJOE_COLLECT_FEES_SELECTOR: _TRADERJOE_COLLECT_FEES_SIG,
                    },
                    # ``collectFees`` is only emitted by the LP_COLLECT_FEES
                    # compile path (``_compile_collect_fees_traderjoe_v2`` in
                    # ``intents/compiler.py``). Scoping to LP_COLLECT_FEES
                    # keeps this selector out of SWAP / LP_OPEN / LP_CLOSE
                    # manifests where it isn't needed — same least-privilege
                    # principle as the LP_CLOSE-scoped ``approveForAll`` above.
                    intent_types=frozenset({"LP_COLLECT_FEES"}),
                )
            )

        result[chain] = entries
    return result


PERMISSION_HINTS = PermissionHints(
    supports_standalone_fee_collection=True,
    static_permissions=_build_static_permissions(),
    # Synthetic SWAP discovery defaults to ``(USDC, WETH)`` but TraderJoe V2 on
    # Ethereum has no usable WETH/USDC liquidity at any bin step at the current
    # fork block — the only liquid TJv2 pair on Ethereum is USDT/USDC bin_step=1
    # (LBPair ``0x47B1CEC2D2370E11B049c73aB6732F03E920C71a``, verified 2026-05-14).
    # Without this override, ``get_swap_quote`` raises ``DivisionByZero`` because
    # ``getSwapOut`` returns ``amount_out=0`` against an empty pool, the
    # synthetic compile aborts, and the LBRouter swap selector never lands in
    # the generated manifest — every real TJv2 SWAP on Ethereum then fails
    # ``ConditionViolation`` at ``execTransactionWithRole``. Mirror Curve's
    # approach (issue #1903) and pin a known-liquid pair per chain. Other
    # chains keep the default ``(USDC, WETH)`` pair, which compiles fine.
    synthetic_swap_pair={
        "ethereum": ("USDT", "USDC"),
    },
    selector_labels={
        _TRADERJOE_ADD_LIQUIDITY_SELECTOR: "addLiquidity(LiquidityParameters)",
        _TRADERJOE_REMOVE_LIQUIDITY_SELECTOR: "removeLiquidity(address,address,uint16,uint256,uint256,uint256[],uint256[],address,uint256)",
        _TRADERJOE_SWAP_EXACT_TOKENS_FOR_TOKENS_SELECTOR: _TRADERJOE_SWAP_SIG,
        _TRADERJOE_APPROVE_FOR_ALL_SELECTOR: _TRADERJOE_APPROVE_FOR_ALL_SIG,
        _TRADERJOE_COLLECT_FEES_SELECTOR: _TRADERJOE_COLLECT_FEES_SIG,
    },
)
