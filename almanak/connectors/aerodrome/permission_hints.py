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

from almanak.core.contracts import AERODROME
from almanak.framework.intents.compiler_constants import (
    NFT_POSITION_COLLECT_SELECTOR,
    NFT_POSITION_DECREASE_SELECTOR,
)
from almanak.framework.permissions.hints import PermissionHints, StaticPermissionEntry

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
        "0xa026383e": "exactInputSingle(ExactInputSingleParams)",
        "0x5a47ddc3": "addLiquidity(address,address,bool,uint256,uint256,uint256,uint256,address,uint256)",
        "0x0dede6c4": "removeLiquidity(address,address,bool,uint256,uint256,uint256,address,uint256)",
        "0xcac88ea9": "swapExactTokensForTokens(uint256,uint256,Route[],address,uint256)",
    },
    static_permissions=_static_permissions,
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
    },
)
