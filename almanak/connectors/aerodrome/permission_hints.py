"""Aerodrome permission hints for permission discovery.

The Aerodrome connector exposes two protocol literals (blueprint 05 §Aerodrome,
audit VIB-4434 §B6) sharing one connector directory:

- ``aerodrome``            → :data:`PERMISSION_HINTS` (Classic Solidly-fork
                              Router + LP, deployed on Base + Optimism via the
                              Velodrome alias).
- ``aerodrome_slipstream`` → :data:`PERMISSION_HINTS_SLIPSTREAM` (Uniswap V3-
                              style concentrated liquidity via the Slipstream
                              ``NonfungiblePositionManager``, Base only).

``almanak.framework.permissions.hints._PROTOCOL_CONNECTOR_MAP`` resolves the
Slipstream literal to ``("aerodrome", "PERMISSION_HINTS_SLIPSTREAM")`` so the
loader picks the right object without forcing a near-empty
``connectors/aerodrome_slipstream/`` directory (the spirit of B6).

LP compile paths for both surfaces query on-chain state:

- Classic LP_CLOSE requires RPC for ``router.removeLiquidity`` LP-balance
  reads; static permissions surface the router's ``removeLiquidity`` selector
  so offline manifests never silently omit it.
- Slipstream LP_OPEN compiles via ``validate_aerodrome_cl_pool`` (RPC) and
  LP_CLOSE/LP_COLLECT_FEES via ``adapter.remove_cl_liquidity`` /
  ``collect_cl_fees`` (RPC for position state). Static permissions therefore
  carry the NPM ``mint`` / ``decreaseLiquidity`` / ``collect`` selectors,
  each scoped to the single intent type that emits it.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.framework.intents.compiler_constants import (
    NFT_POSITION_COLLECT_SELECTOR,
    NFT_POSITION_DECREASE_SELECTOR,
)
from almanak.framework.permissions.hints import (
    DiscoveryContext,
    PermissionHints,
    StaticPermissionEntry,
)

from .adapter import CL_EXACT_INPUT_SINGLE_SELECTOR, SWAP_EXACT_TOKENS_SELECTOR
from .addresses import AERODROME

if TYPE_CHECKING:
    from almanak.framework.intents.vocabulary import AnyIntent

# Slipstream CL SwapRouter exactInputSingle — selector constant owned by the
# adapter (``adapter.CL_EXACT_INPUT_SINGLE_SELECTOR``) so the manifest label
# can never drift from the calldata the adapter actually emits. Slipstream's
# params use ``int24 tickSpacing`` where Uniswap V3 uses ``uint24 fee``, hence
# the selector differs from SwapRouter02's ``0x04e45aaf``.
_CL_EXACT_INPUT_SINGLE_SIG = "exactInputSingle((address,address,int24,address,uint256,uint256,uint256,uint160))"

# Classic (Solidly) router swap — same constant-not-literal discipline. The
# Slipstream slug needs this label too, because the shared compiler's auto
# ladder can fall back to the Classic router for a slipstream-slug swap (see
# ``build_discovery_vectors``).
_CLASSIC_SWAP_SIG = "swapExactTokensForTokens(uint256,uint256,Route[],address,uint256)"

# =========================================================================
# Classic (V1/V2 Solidly-fork) — unchanged surface
# =========================================================================

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
        CL_EXACT_INPUT_SINGLE_SELECTOR: _CL_EXACT_INPUT_SINGLE_SIG,
        "0x5a47ddc3": "addLiquidity(address,address,bool,uint256,uint256,uint256,uint256,address,uint256)",
        "0x0dede6c4": "removeLiquidity(address,address,bool,uint256,uint256,uint256,address,uint256)",
        SWAP_EXACT_TOKENS_SELECTOR: _CLASSIC_SWAP_SIG,
    },
    static_permissions=_static_permissions,
    # Synthetic-discovery participation (VIB-4928): classic Solidly-fork
    # ``aerodrome`` does SWAP + LP. As a Solidly router it has no native-in
    # msg.value auto-wrap path, so ``supports_native_in_swap`` stays False
    # (historically absent from ``_NATIVE_IN_SWAP_PROTOCOLS``).
    synthetic_discovery_intents=frozenset({"SWAP", "LP_OPEN", "LP_CLOSE"}),
)


# =========================================================================
# Slipstream CL (Uniswap V3-style NPM) — VIB-4434 W1
# =========================================================================

# Slipstream NonfungiblePositionManager ``mint`` selector.
#
# Slipstream's mint params use ``int24 tickSpacing`` where Uniswap V3 uses
# ``uint24 fee``, so this selector differs from ``NFT_POSITION_MINT_SELECTOR``
# (``0x88316456``) defined in ``compiler_constants``. Verified 2026-05-16 via
# ``keccak("mint((address,address,int24,int24,int24,uint256,uint256,uint256,uint256,address,uint256,uint160))")[:4]``
# against the compile output of ``compile_lp_open_aerodrome_slipstream``.
_SLIPSTREAM_MINT_SELECTOR = "0xb5007d1f"
_SLIPSTREAM_MINT_SIG = (
    "mint((address,address,int24,int24,int24,uint256,uint256,uint256,uint256,address,uint256,uint160))"
)
# decreaseLiquidity / collect signatures are byte-identical to Uniswap V3's
# NPM (tuple-arg variants); see ``compiler_constants`` for the canonical
# selector strings.
_SLIPSTREAM_DECREASE_SELECTOR = NFT_POSITION_DECREASE_SELECTOR  # 0x0c49ccbe
_SLIPSTREAM_COLLECT_SELECTOR = NFT_POSITION_COLLECT_SELECTOR  # 0xfc6f7865
_SLIPSTREAM_DECREASE_SIG = "decreaseLiquidity(DecreaseLiquidityParams)"
_SLIPSTREAM_COLLECT_SIG = "collect(CollectParams)"


def _build_slipstream_static_permissions() -> dict[str, list[StaticPermissionEntry]]:
    """Per-intent static permissions for Aerodrome Slipstream CL.

    Three entries per deployed chain, each scoped to the single intent type
    that emits the selector at compile time:

    - ``LP_OPEN`` → ``mint``.
    - ``LP_CLOSE`` → ``decreaseLiquidity`` + ``collect`` (two-tx teardown
      per audit B6 / compiler ``adapter.remove_cl_liquidity``).
    - ``LP_COLLECT_FEES`` → ``collect`` (standalone, no decreaseLiquidity).

    Per-intent scoping is load-bearing: a single broad entry covering all
    selectors would over-permission LP_OPEN-only / LP_CLOSE-only /
    LP_COLLECT_FEES-only manifests. The static-permission filter at
    ``discovery.py`` intersects ``entry.intent_types`` with the requested
    ``intent_types``, so the right selector set ships per manifest scope.

    Only Base has Slipstream deployed today (``cl_nft`` key in
    :data:`AERODROME`).
    """
    result: dict[str, list[StaticPermissionEntry]] = {}
    for chain, addrs in AERODROME.items():
        cl_nft = addrs.get("cl_nft")
        if not cl_nft:
            continue
        target = cl_nft.lower()
        label = "Aerodrome Slipstream NonfungiblePositionManager"
        result[chain] = [
            StaticPermissionEntry(
                target=target,
                label=label,
                selectors={_SLIPSTREAM_MINT_SELECTOR: _SLIPSTREAM_MINT_SIG},
                intent_types=frozenset({"LP_OPEN"}),
            ),
            StaticPermissionEntry(
                target=target,
                label=label,
                selectors={
                    _SLIPSTREAM_DECREASE_SELECTOR: _SLIPSTREAM_DECREASE_SIG,
                    _SLIPSTREAM_COLLECT_SELECTOR: _SLIPSTREAM_COLLECT_SIG,
                },
                intent_types=frozenset({"LP_CLOSE"}),
            ),
            StaticPermissionEntry(
                target=target,
                label=label,
                selectors={_SLIPSTREAM_COLLECT_SELECTOR: _SLIPSTREAM_COLLECT_SIG},
                intent_types=frozenset({"LP_COLLECT_FEES"}),
            ),
        ]
    return result


PERMISSION_HINTS_SLIPSTREAM = PermissionHints(
    # NFT tokenId placeholder for offline LP_CLOSE compile (the synthetic
    # discovery path substitutes a non-zero tokenId in
    # ``compile_lp_close_aerodrome_slipstream`` so the adapter can produce
    # real TXs even without RPC).
    synthetic_position_id="1",
    supports_standalone_fee_collection=True,
    needs_rpc_discovery=True,
    # Surrogates the Slipstream ``tick_spacing`` (not a Uniswap V3 fee tier)
    # so that ``synthetic_intents._build_lp_open_intents`` emits the
    # compiler-required 3-part pool string ``WETH/USDC/200`` instead of
    # the bare 2-part ``WETH/USDC`` that the compile path rejects with
    # ``Invalid pool format for aerodrome_slipstream`` (audit pr-auditor
    # finding #2). 200 matches the canonical Base WETH/USDC Slipstream
    # pool used by the lp_aerodrome / aerodrome_slipstream_lp demos.
    #
    # NOTE — residual offline-discovery noise. With this surrogate the
    # synthetic LP_OPEN compile advances past format validation but then
    # fails at ``validate_aerodrome_cl_pool`` (RPC required to confirm
    # the pool exists). Synthetic LP_CLOSE / LP_COLLECT_FEES also fail
    # offline because ``adapter.remove_cl_liquidity`` / ``collect_cl_fees``
    # need RPC to read NFT position state. In all three cases the
    # ``static_permissions`` above carry the correct selectors into the
    # manifest, so manifest output is correct; only the per-run
    # ``Compilation failed for aerodrome_slipstream/* on base: ...``
    # warning is cosmetic noise. A framework-level fix (suppress compile
    # warnings when ``needs_rpc_discovery=True`` + ``rpc_url=None`` AND
    # static_permissions cover the intent) is the clean answer and is
    # filed as follow-up scope per VIB-4434 audit report §"Pushback".
    synthetic_fee_tier={"base": 200},
    static_permissions=_build_slipstream_static_permissions(),
    selector_labels={
        _SLIPSTREAM_MINT_SELECTOR: _SLIPSTREAM_MINT_SIG,
        _SLIPSTREAM_DECREASE_SELECTOR: _SLIPSTREAM_DECREASE_SIG,
        _SLIPSTREAM_COLLECT_SELECTOR: _SLIPSTREAM_COLLECT_SIG,
        CL_EXACT_INPUT_SINGLE_SELECTOR: _CL_EXACT_INPUT_SINGLE_SIG,
        SWAP_EXACT_TOKENS_SELECTOR: _CLASSIC_SWAP_SIG,
    },
    # Synthetic-discovery participation (VIB-4928, VIB-5990): LP via the CL
    # NPM, plus SWAP. SWAP was historically excluded on the claim that classic
    # ``aerodrome`` "owns the Solidly SWAP route" — but ``AerodromeCompiler``
    # declares SWAP for BOTH slugs and dispatches ``aerodrome_slipstream``
    # swaps through the shared ``compile_swap_aerodrome`` path (CL SwapRouter
    # ``exactInputSingle``), so a strategy issuing
    # ``SwapIntent(protocol="aerodrome_slipstream")`` produced an EMPTY Zodiac
    # manifest and every Safe-path swap reverted unauthorized at
    # ``execTransactionWithRole`` (VIB-5990). The swap compile is fully
    # offline-capable (``_resolve_aerodrome_route`` degrades to CL@100 when
    # offline), so synthetic discovery — not ``static_permissions`` — is the
    # drift-proof mechanism. The SWAP synthetic is emitted by
    # ``build_discovery_vectors`` below because the CL SwapRouter lives in
    # this connector's ``AERODROME[chain]["cl_router"]``, not in the
    # framework's ``PROTOCOL_ROUTERS`` (the TraderJoe V2 precedent).
    # LP_COLLECT_FEES stays gated by
    # ``supports_standalone_fee_collection=True`` above.
    synthetic_discovery_intents=frozenset({"SWAP", "LP_OPEN", "LP_CLOSE"}),
)


def build_discovery_vectors(
    protocol: str,
    intent_type: str,
    chain: str,
    ctx: DiscoveryContext,
) -> list[AnyIntent] | None:
    """Connector-owned synthetic dispatch for the Slipstream SWAP slot (VIB-5990).

    ``_build_swap_intents`` gates the framework-default SWAP synthetic on
    ``protocol in PROTOCOL_ROUTERS[chain]`` — a table the ``aerodrome_slipstream``
    slug is deliberately absent from (its swap venue is the Slipstream CL
    SwapRouter, declared as ``cl_router`` in this connector's
    :data:`~almanak.connectors.aerodrome.addresses.AERODROME`, while the slug's
    role-registry slot is ``CL_POSITION_MANAGER``). This override runs BEFORE
    that gate and emits the SWAP synthetic directly, mirroring the
    ``traderjoe_v2`` self-containment pattern.

    Every other ``(protocol, intent_type)`` slot returns ``None`` so classic
    ``aerodrome`` SWAP/LP and Slipstream LP keep their framework-default
    dispatch unchanged.
    """
    if protocol != "aerodrome_slipstream" or intent_type != "SWAP":
        return None
    # Only chains where Slipstream CL is deployed (Base today). Velodrome on
    # Optimism is Classic-only, so the framework default (which finds no router
    # entry and returns ``[]``) is the right outcome there.
    #
    # Reuse the COMPILER's own capability predicate rather than re-deriving it:
    # it gates on ``cl_router`` AND ``cl_factory``, and a manifest built from a
    # weaker predicate than the one that picks the route is drift waiting to
    # happen (a future chain entry with a router but no factory would have the
    # override emit a CL vector while the compiler routed Classic). Imported
    # lazily — like ``SwapIntent`` below — to keep this module cheap to load
    # during manifest discovery.
    from .compiler import _aerodrome_chain_has_cl

    if not _aerodrome_chain_has_cl(AERODROME.get(chain, {})):
        return None
    from almanak.framework.intents.vocabulary import SwapIntent

    def _vector(**swap_params: object) -> AnyIntent:
        return SwapIntent(
            from_token=ctx.usdc,
            to_token=ctx.weth,
            amount=Decimal("1"),
            protocol=protocol,
            chain=chain,
            swap_params=dict(swap_params),
        )

    # TWO vectors, because ``compile_swap_aerodrome`` routes on pool existence,
    # NOT on the protocol slug: the auto ladder probes CL across the candidate
    # tick spacings and, finding none, *falls back to the Classic router*
    # (``compiler.py`` step 5). A slipstream-slug swap on a pair with no CL pool
    # therefore emits ``swapExactTokensForTokens`` on the Classic router — a
    # (target, selector) pair that a CL-only manifest does not authorise, which
    # is the same ``execTransactionWithRole`` unauthorized revert as VIB-5990,
    # merely narrowed to long-tail pairs. Offline discovery cannot observe that
    # branch (``_aerodrome_is_offline`` short-circuits to CL@100), so the only
    # drift-proof answer is to emit both route shapes and let the manifest be
    # their union. ``classic=True`` is evaluated at ladder step (2), BEFORE the
    # offline short-circuit, and ``_resolve_aerodrome_classic_route`` degrades
    # on an unverifiable probe — so this compiles offline exactly like the CL
    # vector does.
    return [_vector(), _vector(classic=True)]
