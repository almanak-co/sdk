"""Pendle permission hints for permission discovery.

Pendle is the first connector that owns synthetic discovery for THREE
different intent types (SWAP, LP_OPEN, LP_CLOSE) from a single
``build_discovery_vectors`` function. The shape of each is dictated by
Pendle's product surface rather than any framework default:

  - SWAP exposes four direction-specific selectors on its Router
    (token→PT, PT→token, token→YT, YT→token) plus a pre-swap leg via
    UniswapV3 SwapRouter02 when the user-supplied token doesn't mint SY
    directly. A single synthetic only authorises one selector, so the
    full grid is emitted; the pre-swap leg is gated on a per-chain default
    (``_PRE_SWAP_DEFAULTS``) because not every chain has a curated input.

  - LP_OPEN uses single-sided liquidity into a market contract (the market
    address IS the LP token). Pool format is ``"TOKEN/<PT-name-or-0xmarket>"``;
    the connector is excluded from ``LP_POSITION_MANAGERS`` so the override
    hook runs before the framework default's NFT-position-manager gate.

  - LP_CLOSE expects ``intent.pool`` = the 0x market address and the output
    token via ``protocol_params['token']`` (the compile path reads from
    protocol_params first before falling back to ``token_a`` getattr probes
    on the frozen intent model).

All three lookups resolve the canonical (SY-mint token, PT name, YT name,
market address) grid from the connector's own registry (``MARKET_BY_PT_TOKEN``,
``MARKET_BY_YT_TOKEN``, ``MARKET_TOKEN_MINT_SY``, ``PT_TOKEN_INFO``,
``YT_TOKEN_INFO`` in :mod:`.sdk`) so the maintainer's market priority
ordering — insertion order in the SDK registry — is the single source of
truth. No dated names hardcoded; rotating an expired market into a fresh
entry auto-propagates here without a code change.

See :func:`almanak.framework.permissions.hints.get_discovery_vectors_override`
for the dispatcher contract.

The legacy declarative ``synthetic_swap_pair`` knob on ``PERMISSION_HINTS``
is preserved for now — ``build_discovery_vectors`` does NOT consult it
(SWAP discovery walks the market grid above instead), so the field is
effectively vestigial. Removing it is a separate cleanup PR tracked in the
post-migration vestigial-field epic.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.framework.permissions.hints import DiscoveryContext, PermissionHints

if TYPE_CHECKING:
    from almanak.framework.intents.vocabulary import AnyIntent


# Pendle swaps require one token to be a PT (Principal Token).
# The default USDC/WETH pair doesn't work, so we declare known PT token pairs
# per chain. Vestigial under the current ``build_discovery_vectors`` path
# (SWAP discovery walks the market grid instead) but retained as a
# declarative knob for any future code path that consults
# ``PermissionHints.synthetic_swap_pair``. Removing it is a separate PR.
PERMISSION_HINTS = PermissionHints(
    synthetic_swap_pair={
        # arbitrum: wstETH -> PT-wstETH-25JUN2026
        "arbitrum": (
            "0x5979D7b546E38E414F7E9822514be443A4800529",  # wstETH
            "PT-wstETH",
        ),
        # ethereum: wstETH -> PT-sUSDe-7MAY2026 (uses sUSDe market)
        "ethereum": (
            "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",  # sUSDe
            "PT-sUSDe",
        ),
    },
    # Synthetic-discovery participation (VIB-4928): SWAP + LP into the Pendle
    # market contract. Discovery vectors are produced by
    # ``build_discovery_vectors`` below (Pendle has no NFT position manager, so
    # it is absent from LP_POSITION_MANAGERS and relies on the override).
    synthetic_discovery_intents=frozenset({"SWAP", "LP_OPEN", "LP_CLOSE"}),
)


# Per-chain pre-swap input for the Pendle synthetic. This token DOESN'T mint
# SY directly — supplying it forces the compiler's pre-swap path
# (UniswapV3 → SY-mint token → Pendle Router). That transitive leg is what
# authorises ``exactInputSingle`` on SwapRouter02 plus an approve on the
# SY-mint token. Tests that pass WETH/USDC straight to a Pendle SwapIntent
# rely on this entry being present in the manifest.
#
# Stable per-chain liquidity choice — not derivable from the connector
# registry. arbitrum: WETH (closest path to wstETH). ethereum: USDC (deepest
# pool to sUSDe-style markets and the value tests use first).
_PRE_SWAP_DEFAULTS: dict[str, str] = {
    "arbitrum": "WETH",
    "ethereum": "USDC",
}


def _market_grid(chain: str) -> dict[str, str] | None:
    """Pick the canonical synthetic market for ``chain`` from the SDK registry.

    Returns ``{"sy_token", "pt_name", "yt_name", "market_addr", "pre_swap_token"?}``
    or ``None`` when no fully-supported market is registered for ``chain``.

    ``pre_swap_token`` is **optional** — it's only present when
    ``_PRE_SWAP_DEFAULTS`` declares one for the chain. The four
    direct PT/YT swap selectors and the LP_OPEN/LP_CLOSE selectors don't
    need a pre-swap leg, so the resolver still returns a usable grid for
    chains without a configured default (e.g. plasma's fUSDT0 market).
    Tests that exercise the pre-swap transitive path are chain-specific
    in the existing intent-test suite — those chains MUST configure a
    default; the synthetic grid below silently skips the transitive
    intent on chains that don't.

    Selection: walk ``MARKET_BY_PT_TOKEN[chain]`` in insertion order (the
    maintainer's priority signal in this codebase — see also
    ``_morpho_blue_synthetic_market_id``) and pick the first unique market
    address with both an SY-mint registry entry AND a PT+YT counterpart in
    the *_TOKEN_INFO maps. Filters out partially-listed markets without
    parsing dated suffixes — when the maintainer rotates an expired market
    and adds a replacement above, this resolver auto-tracks without a code
    change here.

    Returns SY-mint token by **address**: the compile path's
    ``_resolve_token`` accepts both symbols and addresses, and not every
    SY-mint token is in the global token resolver, so the address is the
    universally safe handle.
    """
    registries = _load_pendle_registries(chain)
    if registries is None:
        return None
    pt_markets, yt_markets, sy_mint, pt_info, yt_info = registries
    seen: set[str] = set()
    for pt_name, market_addr in pt_markets.items():
        market_lc = market_addr.lower()
        if market_lc in seen:
            continue
        seen.add(market_lc)
        entry = _resolve_market_grid_entry(pt_name, market_lc, pt_markets, yt_markets, sy_mint, pt_info, yt_info)
        if entry is not None:
            return _attach_pre_swap(entry, chain)
    return None


def _load_pendle_registries(
    chain: str,
) -> tuple[dict[str, str], dict[str, str], dict[str, str], dict, dict] | None:
    """Load the five Pendle market registries for ``chain``.

    Returns ``None`` when the SDK isn't importable or when any registry is
    empty for the requested chain — both cases mean the chain has no
    synthesisable Pendle market.
    """
    try:
        from .sdk import (
            MARKET_BY_PT_TOKEN,
            MARKET_BY_YT_TOKEN,
            MARKET_TOKEN_MINT_SY,
            PT_TOKEN_INFO,
            YT_TOKEN_INFO,
        )
    except ImportError:
        return None
    pt_markets = MARKET_BY_PT_TOKEN.get(chain, {})
    yt_markets = MARKET_BY_YT_TOKEN.get(chain, {})
    sy_mint = MARKET_TOKEN_MINT_SY.get(chain, {})
    pt_info = PT_TOKEN_INFO.get(chain, {})
    yt_info = YT_TOKEN_INFO.get(chain, {})
    if not all((pt_markets, yt_markets, sy_mint, pt_info, yt_info)):
        return None
    return pt_markets, yt_markets, sy_mint, pt_info, yt_info


def _resolve_market_grid_entry(
    pt_name: str,
    market_lc: str,
    pt_markets: dict[str, str],
    yt_markets: dict[str, str],
    sy_mint: dict[str, str],
    pt_info: dict,
    yt_info: dict,
) -> dict[str, str] | None:
    """Resolve a single market into the grid entry, or ``None`` if incomplete.

    The compile path's PT-sell / YT-sell branches resolve token info via
    ``PT_TOKEN_INFO`` / ``YT_TOKEN_INFO``. The market registries
    (``MARKET_BY_*_TOKEN``) sometimes carry an extra all-caps alias that
    the ``*_INFO`` maps don't (e.g. ethereum's ``YT-SUSDE-7MAY2026`` is in
    ``MARKET_BY_YT_TOKEN`` but not in ``YT_TOKEN_INFO``). Picking a name
    that isn't in ``*_INFO`` causes the compile path to FAIL with "not
    found in ``*_TOKEN_INFO``" and the synthetic produces no permission
    entry. ``_resolve_canonical_name`` enforces ``*_INFO`` membership.
    """
    sy_addr = sy_mint.get(market_lc)
    if not sy_addr:
        return None
    resolved_pt = _resolve_canonical_name(pt_name, market_lc, pt_markets, pt_info)
    if resolved_pt is None:
        return None
    resolved_yt = _resolve_canonical_name(None, market_lc, yt_markets, yt_info)
    if resolved_yt is None:
        return None
    return {
        "sy_token": sy_addr,
        "pt_name": resolved_pt,
        "yt_name": resolved_yt,
        # Re-resolve from ``pt_markets[resolved_pt]`` so the grid carries
        # the canonical casing keyed by the final resolved name — same
        # value LP_CLOSE used to recompute via the exact-or-uppercase
        # fallback against the registry before this resolver hoisted it.
        "market_addr": pt_markets[resolved_pt],
    }


def _resolve_canonical_name(
    preferred: str | None,
    market_lc: str,
    name_to_addr: dict[str, str],
    info: dict,
) -> str | None:
    """Find a name registered in both ``name_to_addr`` (mapping to ``market_lc``)
    AND ``info``. ``preferred`` is tried first if given and present in ``info``.
    """
    if preferred is not None and preferred in info:
        return preferred
    return next(
        (name for name, addr in name_to_addr.items() if addr.lower() == market_lc and name in info),
        None,
    )


def _attach_pre_swap(grid: dict[str, str], chain: str) -> dict[str, str]:
    """Add ``pre_swap_token`` to ``grid`` when the chain has a configured default."""
    pre_swap = _PRE_SWAP_DEFAULTS.get(chain)
    if pre_swap:
        grid["pre_swap_token"] = pre_swap
    return grid


def _build_swap_intents(chain: str) -> list[AnyIntent]:
    """Emit Pendle synthetic SwapIntents covering every selector tests use.

    Pendle's Router exposes four direction-specific selectors (token→PT,
    PT→token, token→YT, YT→token) and the compile path may insert a
    pre-swap through UniswapV3 SwapRouter02 when the user-supplied token
    isn't the SY-mint token for the target market. A single synthetic only
    authorises one selector, so we emit the full grid.

    Order matters for ``send_allowed`` aggregation: the first synthetic is
    the SY-direct PT-buy (fewest moving parts, used by the gate test).
    Subsequent synthetics cover the remaining directions and the pre-swap
    transitive path.

    Returns an empty list for chains without a Pendle deployment so the
    discovery loop treats them as a no-op.
    """
    from almanak.framework.intents.vocabulary import SwapIntent

    market = _market_grid(chain)
    if market is None:
        return []
    sy = market["sy_token"]
    pt = market["pt_name"]
    yt = market["yt_name"]
    intents: list[AnyIntent] = [
        # Direct: SY-mint → PT (covers ``swapExactTokenForPt`` on Pendle Router
        # + approve on the SY token)
        SwapIntent(
            from_token=sy,
            to_token=pt,
            amount=Decimal("0.05"),
            protocol="pendle",
            chain=chain,
        ),
        # Direct: PT → SY-mint (covers ``swapExactPtForToken`` + approve on the
        # PT/market LP token, which IS the market address)
        SwapIntent(
            from_token=pt,
            to_token=sy,
            amount=Decimal("0.05"),
            protocol="pendle",
            chain=chain,
        ),
        # Direct: SY-mint → YT (covers ``swapExactTokenForYt``)
        SwapIntent(
            from_token=sy,
            to_token=yt,
            amount=Decimal("0.05"),
            protocol="pendle",
            chain=chain,
        ),
        # Direct: YT → SY-mint (covers ``swapExactYtForToken``)
        SwapIntent(
            from_token=yt,
            to_token=sy,
            amount=Decimal("0.05"),
            protocol="pendle",
            chain=chain,
        ),
    ]
    # Transitive: pre_swap_token → PT. The compile path emits a UniswapV3
    # ``exactInputSingle`` first (pre-swap to the SY-mint token) and then a
    # ``swapExactTokenForPt`` call on the Pendle Router. Only emitted on
    # chains where ``_PRE_SWAP_DEFAULTS`` declares an input token — chains
    # without a default still get the four direct selectors above (e.g.
    # plasma's fUSDT0 market).
    pre = market.get("pre_swap_token")
    if pre:
        intents.append(
            SwapIntent(
                from_token=pre,
                to_token=pt,
                amount=Decimal("0.05"),
                protocol="pendle",
                chain=chain,
            )
        )
    return intents


def _build_lp_open_intents(chain: str) -> list[AnyIntent]:
    """Emit a Pendle synthetic LP_OPEN intent on chains with a curated market.

    The compile path expects ``intent.pool == "TOKEN/<PT-name|0xmarket>"`` and
    ``intent.amount0`` as the deposit amount. Single-sided liquidity — the
    Pendle Router splits the deposit into SY + PT internally. The synthetic
    pulls the SY-mint token + canonical PT name from
    ``_market_grid`` so the connector registry is the single source of
    truth (no hardcoded dated names).
    """
    from almanak.framework.intents.vocabulary import LPOpenIntent

    market = _market_grid(chain)
    if market is None:
        return []
    sy = market["sy_token"]
    pt = market["pt_name"]
    # Pendle ignores the V3-style ``range_lower`` / ``range_upper`` fields
    # but pydantic's vocabulary still validates ``range_lower < range_upper``;
    # use placeholder values that satisfy the model invariant without being
    # interpreted by the connector's single-sided LP path.
    return [
        LPOpenIntent(
            pool=f"{sy}/{pt}",
            amount0=Decimal("0.05"),
            amount1=Decimal("0"),  # single-sided; compiler reads amount0 only
            range_lower=Decimal("1"),  # ignored by Pendle compile path
            range_upper=Decimal("2"),
            protocol="pendle",
            chain=chain,
        )
    ]


def _build_lp_close_intents(chain: str) -> list[AnyIntent]:
    """Emit a Pendle synthetic LP_CLOSE intent.

    The compile path expects:
    - ``intent.pool`` = the 0x market address (resolves the LP token)
    - ``intent.position_id`` = LP amount in wei (numeric string)
    - ``protocol_params['token']`` (or ``token_a``) = output token symbol

    Pulls the canonical market address + SY-mint output token from
    ``_market_grid`` — the helper already iterates ``MARKET_BY_PT_TOKEN``
    to resolve the (PT, market) pair, so re-importing the registry here
    would just re-do the same lookup.
    """
    from almanak.framework.intents.vocabulary import LPCloseIntent

    market = _market_grid(chain)
    if market is None:
        return []
    sy = market["sy_token"]
    market_addr = market["market_addr"]
    return [
        LPCloseIntent(
            position_id="1000000000000000000",  # 1 LP token in wei — synthetic floor
            pool=market_addr,
            protocol="pendle",
            chain=chain,
            # Pendle's compile path reads the output token from
            # ``protocol_params['token']`` first (canonical) before falling
            # back to ``token_a`` / ``token`` getattr probes. The model is
            # frozen so we can't mutate post-init — passing it via
            # protocol_params is the only path that doesn't require a
            # vocabulary-level shape change.
            protocol_params={"token": sy},
        )
    ]


def build_discovery_vectors(
    protocol: str,
    intent_type: str,
    chain: str,
    ctx: DiscoveryContext,
) -> list[AnyIntent] | None:
    """Emit Pendle synthetic intents covering every selector the manifest needs.

    Pendle owns dispatch across three intent types:

      - ``"SWAP"``     → four PT/YT direction selectors + optional pre-swap leg.
      - ``"LP_OPEN"``  → single-sided liquidity into the market contract.
      - ``"LP_CLOSE"`` → market-address pool + output token via
        ``protocol_params``.

    Returns ``None`` for any other ``intent_type`` (LP_COLLECT_FEES, etc.) so
    the framework default takes over — Pendle doesn't expose those discovery
    paths today.

    ``ctx`` is unused: every Pendle synthetic resolves tokens from the SDK
    market registry, not the chain-default USDC/WETH pair. Kept on the
    signature to match the dispatcher contract.
    """
    del ctx  # explicit: market grid drives token selection, not chain defaults
    del protocol  # always "pendle" via this connector path
    if intent_type == "SWAP":
        return _build_swap_intents(chain)
    if intent_type == "LP_OPEN":
        return _build_lp_open_intents(chain)
    if intent_type == "LP_CLOSE":
        return _build_lp_close_intents(chain)
    return None
