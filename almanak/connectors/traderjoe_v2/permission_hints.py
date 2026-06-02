"""TraderJoe V2 permission hints for permission discovery.

TraderJoe V2 LP operations (addLiquidity, removeLiquidity) require RPC
to query the active bin and LP token balances, so compilation-based
discovery fails in offline mode.  Static permissions ensure the
manifest always includes the LBRouter selectors.

SWAP compilation offline succeeds through the connector-owned Liquidity Book
compiler path, which falls back to a default bin_step when RPC auto-detection
is unavailable. The connector owns its synthetic-intent dispatch via
``build_discovery_vectors`` below — see
:func:`almanak.framework.permissions.hints.get_discovery_vectors_override`
for the dispatcher contract. This is the connector self-containment endpoint
for TraderJoe V2 (VIB-4121): every SWAP / LP_OPEN / LP_CLOSE /
LP_COLLECT_FEES synthetic for ``traderjoe_v2`` is produced here, and the
framework-side ``_build_swap_intents`` router-exemption tuple no longer
needs to special-case TraderJoe.

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

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.framework.intents.compiler import LP_POSITION_MANAGERS
from almanak.framework.permissions.hints import (
    DiscoveryContext,
    PermissionHints,
    StaticPermissionEntry,
)

from .addresses import TRADERJOE_V2_LBPAIRS

if TYPE_CHECKING:
    from almanak.framework.intents.vocabulary import AnyIntent

# TraderJoe V2 LBRouter selectors that need RPC for compilation
_TRADERJOE_ADD_LIQUIDITY_SELECTOR = "0xa3c7271a"
_TRADERJOE_REMOVE_LIQUIDITY_SELECTOR = "0xc22159b6"
# swapExactTokensForTokens(uint256,uint256,(uint256[],uint8[],address[]),address,uint256)
# emitted by the connector-owned Liquidity Book swap compiler path (VIB-1928).
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
                    # ``_compile_lp_open_traderjoe_v2`` in ``connectors/traderjoe_v2/compiler.py``,
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
                    # ``connectors/traderjoe_v2/compiler.py``). Scoping to LP_COLLECT_FEES
                    # keeps this selector out of SWAP / LP_OPEN / LP_CLOSE
                    # manifests where it isn't needed — same least-privilege
                    # principle as the LP_CLOSE-scoped ``approveForAll`` above.
                    intent_types=frozenset({"LP_COLLECT_FEES"}),
                )
            )

        result[chain] = entries
    return result


# Per-chain synthetic SWAP pair for TraderJoe V2. The framework default
# ``(USDC, WETH-equivalent)`` has no usable liquidity at any TJv2 bin step on
# several chains — the synthetic compile would abort before the LBRouter swap
# selector lands on the manifest and every real TJv2 SWAP on those chains
# would fail ``ConditionViolation`` (0xd0a9bf58) at
# ``execTransactionWithRole``. Pin a known-liquid pair per chain.
#
# * ``ethereum``: WETH/USDC has no liquidity at any bin step at the current
#   fork block — the only liquid TJv2 pair on Ethereum is USDT/USDC
#   bin_step=1 (LBPair ``0x47B1CEC2D2370E11B049c73aB6732F03E920C71a``,
#   verified 2026-05-14). ``get_swap_quote`` otherwise raises
#   ``DivisionByZero`` because ``getSwapOut`` returns ``amount_out=0``
#   against an empty pool. (VIB-4378.)
# * ``bsc`` / ``bnb``: The framework's chain-default pair on bsc resolves
#   to (USDC, WETH-bridged) which has no TJv2 LBPair — the canonical
#   liquid pair is (USDT, WBNB). Registered under both ``"bsc"`` (the
#   SDK canonical name, used by the compiler and intent runtime) AND
#   ``"bnb"`` (the user-facing alias used by ConnectorRegistry /
#   ``almanak ax`` / CLI surfaces). The dispatch below is a literal
#   ``dict.get(chain)`` against whatever string the caller passes, with
#   no alias normalisation upstream — omitting either key would leave
#   the corresponding call site falling back to the default and
#   silently emit an empty manifest. (VIB-4376.) Mirrors the
#   sushiswap_v3 bsc override pattern from #1902.
_SWAP_PAIR_BY_CHAIN: dict[str, tuple[str, str]] = {
    "ethereum": ("USDT", "USDC"),
    "bsc": (
        "0x55d398326f99059fF775485246999027B3197955",  # USDT (BSC, 18 decimals)
        "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",  # WBNB
    ),
    "bnb": (
        "0x55d398326f99059fF775485246999027B3197955",  # USDT (BSC, 18 decimals)
        "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",  # WBNB
    ),
}


PERMISSION_HINTS = PermissionHints(
    supports_standalone_fee_collection=True,
    static_permissions=_build_static_permissions(),
    selector_labels={
        _TRADERJOE_ADD_LIQUIDITY_SELECTOR: "addLiquidity(LiquidityParameters)",
        _TRADERJOE_REMOVE_LIQUIDITY_SELECTOR: "removeLiquidity(address,address,uint16,uint256,uint256,uint256[],uint256[],address,uint256)",
        _TRADERJOE_SWAP_EXACT_TOKENS_FOR_TOKENS_SELECTOR: _TRADERJOE_SWAP_SIG,
        _TRADERJOE_APPROVE_FOR_ALL_SELECTOR: _TRADERJOE_APPROVE_FOR_ALL_SIG,
        _TRADERJOE_COLLECT_FEES_SELECTOR: _TRADERJOE_COLLECT_FEES_SIG,
    },
    # Synthetic-discovery participation (VIB-4928): SWAP + LP via the LBRouter.
    # The SWAP synthetic is produced by ``build_discovery_vectors`` below (the
    # LBRouter address lives in LP_POSITION_MANAGERS, not PROTOCOL_ROUTERS).
    # LP_COLLECT_FEES stays gated by ``supports_standalone_fee_collection``.
    synthetic_discovery_intents=frozenset({"SWAP", "LP_OPEN", "LP_CLOSE"}),
)


def build_discovery_vectors(
    protocol: str,
    intent_type: str,
    chain: str,
    ctx: DiscoveryContext,
) -> list[AnyIntent] | None:
    """Emit synthetic intents covering every selector the manifest needs.

    TraderJoe V2 lives in ``LP_POSITION_MANAGERS`` (not ``PROTOCOL_ROUTERS``)
    because its LBRouter handles both SWAP and LP. The framework-side
    ``_build_swap_intents`` used to special-case it via a router-exemption
    tuple; with this override that tuple entry is removed and the connector
    owns the entire ``(protocol, intent_type, chain)`` synthetic dispatch.

    The compile path's per-pair liquidity check requires a chain-specific
    canonical pair (see ``_SWAP_PAIR_BY_CHAIN`` rationale) — falling back to
    the framework's chain-default ``(USDC, WETH)`` on ``ethereum`` / ``bsc`` /
    ``bnb`` aborts the synthetic and drops the LBRouter swap selector from
    the manifest. Per-pair ``approveForAll`` / ``collectFees`` are still
    declared statically on ``PERMISSION_HINTS.static_permissions`` (one entry
    per registered LBPair) — those are merged into the manifest regardless of
    this override.

    Returns ``None`` for any (intent_type, chain) the connector does not
    deploy on so the framework default returns ``[]`` for those slots —
    matching prior behaviour where the LBRouter is absent from
    ``LP_POSITION_MANAGERS`` for the chain.
    """
    from almanak.framework.intents.vocabulary import (
        CollectFeesIntent,
        LPCloseIntent,
        LPOpenIntent,
        SwapIntent,
    )

    if intent_type == "SWAP":
        from_token, to_token = _SWAP_PAIR_BY_CHAIN.get(chain, (ctx.usdc, ctx.weth))
        return [
            SwapIntent(
                from_token=from_token,
                to_token=to_token,
                amount=Decimal("1"),
                protocol=protocol,
                chain=chain,
            )
        ]

    # LP_OPEN / LP_CLOSE / LP_COLLECT_FEES all gate on the LBRouter being
    # registered in ``LP_POSITION_MANAGERS`` for ``chain`` — preserve that
    # gate here so the override's ``None`` short-circuits cleanly to the
    # framework default's empty list for unsupported chains.
    managers = LP_POSITION_MANAGERS.get(chain, {})
    if "traderjoe_v2" not in managers:
        return None

    if intent_type == "LP_OPEN":
        token0, token1 = ctx.usdc, ctx.weth
        return [
            LPOpenIntent(
                pool=f"{token0}/{token1}",
                amount0=Decimal("100"),
                amount1=Decimal("0.05"),
                range_lower=Decimal("1500"),
                range_upper=Decimal("4000"),
                protocol=protocol,
                chain=chain,
            )
        ]

    if intent_type == "LP_CLOSE":
        # TraderJoe LP_CLOSE uses ``synthetic_position_id`` default ``"1"``
        # — no protocol-specific template needed. Mirrors the framework
        # default. ``ctx`` is unused for this branch because LPCloseIntent
        # doesn't take token0/token1.
        return [
            LPCloseIntent(
                position_id="1",
                protocol=protocol,
                chain=chain,
            )
        ]

    if intent_type == "LP_COLLECT_FEES":
        token0, token1 = ctx.usdc, ctx.weth
        return [
            CollectFeesIntent(
                pool=f"{token0}/{token1}",
                protocol=protocol,
                chain=chain,
            )
        ]

    return None
