"""Registry-backed open-position enumeration for teardown — VIB-5459 / TD-01.

Routes teardown's "what LP positions are open?" question through the
``position_registry`` WARM tier (SQLite local / Postgres hosted) — the single
durable, restart-safe source of truth — for the two cut-over LP primitives
(the UniV3 LP family, ``primitive='lp'``, plus UniV4 LP, ``primitive='lp_v4'``).
The registry becomes the WARM read path for those two primitives' teardown
ENUMERATION: a restarted runner re-derives the open set from the durable
registry instead of relying solely on the strategy's in-memory ``_position_id``,
the ``position_events`` history, or the ``LPPositionTracker`` shadow (the
"single WARM read path" of the Teardown roadmap §0.1 / blueprint 28 §5 cutover).

Scope (deliberately narrow — this is the foundation read-path cutover):

* **READ PATH ONLY.** This module reads ``position_registry status='open'`` and
  reconciles the strategy's reported ``TeardownPositionSummary`` against it. It
  does NOT synthesize closing intents — the registry payload carries no
  ``protocol`` slug (only ``token_id`` / ``pool_address`` / ``pool_id`` /
  ticks / liquidity), so close-intent derivation stays with the strategy's
  ``generate_teardown_intents`` plus the existing registry-first ``position_id``
  injection in :meth:`LPPositionTracker.maybe_inject`. It also does NOT scan the
  wallet — that is Plan B (``teardown ... --discover``), a separate lane.
* **UniV3 + UniV4 LP only.** GMX perp, Pendle LP, and Aave lending are NOT cut
  over (separate tickets TD-02/03/04); their enumeration is untouched. A
  strategy-reported LP on a non-cut-over venue (e.g. TraderJoe V2 Liquidity Book
  bins) is also left to the strategy, so this change can never strand it.

Durability (blueprint 06 §multi-tier, blueprint 28 §4): ``position_registry``
rows are written atomically with the ``transaction_ledger`` row at LP_OPEN
receipt-confirmation time (``save_ledger_and_registry``) under
``PRAGMA synchronous=FULL`` inside ``BEGIN IMMEDIATE … COMMIT``. The row
therefore survives crash AND reboot, so a restarted runner re-derives the
identical open set from WARM even when every byte of in-memory state was wiped.

Fund-safety (blueprint 20 §1 Gateway : 1 Strategy): registry rows are keyed by
``deployment_id`` and one gateway serves exactly one strategy, so reading this
deployment's OPEN rows can never surface a sibling deployment's position. No
ownership scan is required (contrast the wallet-wide on-chain discovery in
:mod:`almanak.framework.teardown.lp_recovery`, which exists precisely because
the on-chain scan is wallet-scoped, not deployment-scoped).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field, replace
from decimal import Decimal
from typing import Any

from almanak.connectors._strategy_base.perp_identity import is_residual_marked
from almanak.connectors._strategy_base.teardown_post_condition import resolve_nft_token_id
from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
)

logger = logging.getLogger(__name__)


def _lp_identity(position: PositionInfo) -> str:
    """Source-independent identity discriminator for one LP position (VIB-5723).

    The same physical NFT position reaches the union under different
    ``position_id`` formats depending on which enumeration source produced it:
    a registry row carries the bare token id (``"5580510"``) while a strategy
    that echoes the framework's composite position key reports
    ``"uniswap_v3-WETH/USDC/500-5580510"`` (with the bare id mirrored in
    ``details``). Keying the union on the raw ``position_id`` string counted
    that one position twice (``positions_closed=2`` for 1 physical LP) and made
    the completeness check log a false "ABSENT from position_registry".

    Resolution is delegated to :func:`resolve_nft_token_id` — THE single
    id-resolution rule shared with the TD-14 post-condition hooks and the
    Plan-A chain-verify — so the union agrees with the verifying lanes about
    which position is which. When a numeric NFT id resolves, its ERC-721 manager
    authority is part of the identity whenever the producer supplied it. This
    matters for multi-generation venues such as Slipstream: token IDs are local
    counters on each NPM, so ``(manager A, 42)`` and ``(manager B, 42)`` are two
    physical positions. Otherwise the raw ``position_id`` remains the key
    (non-NFT LP venues, e.g. Liquidity Book).

    The protocol label is deliberately NOT part of the key: registry rows are
    labelled with the registry primitive (``lp`` / ``lp_v4``), never a real
    connector slug, so including it would re-split the pair this key exists to
    collapse. Older/single-manager rows without authority metadata retain the
    historical token-only key.

    This function returns exactly ONE key, and a manager-qualified key never
    equals the unqualified one. That is not the whole identity story any more:
    the two forms are bridged — conditionally and OUTSIDE this function — by
    :func:`_lp_bridge_tokens`, because leaving them unbridged double-counted
    every V3-family LP and failed a successful teardown (VIB-6730). Do not
    "restore" the separation here on the strength of this paragraph; the
    condition under which bridging is safe is a property of the enumerated set
    and is documented on :func:`_unambiguous_lp_nft_ids`.
    """
    parts = _lp_nft_parts(position)
    if parts is None:
        return str(position.position_id)
    token_id, manager = parts
    if manager:
        return f"nft:{manager}:{token_id}"
    return f"nft:{token_id}"


# Connector-private contract-kind names for an LP position's ERC-721 manager,
# used for slugs that declare no ``CL_POSITION_MANAGER`` role. The kind
# vocabulary is explicitly connector-private and may grow, so this asks for
# several and takes the first that resolves rather than assuming one.
_LP_MANAGER_CONTRACT_KINDS: tuple[str, ...] = (
    "position_manager",
    "nft_position_manager",
    "nft",
)

_LP_MANAGER_DETAIL_KEYS: tuple[str, ...] = ("nft_manager_addr", "position_manager", "nft_manager")


def _derived_lp_manager(protocol: str, chain: str) -> str:
    """The ERC-721 manager authority for a row whose producer supplied none (VIB-6735).

    Resolved from the CONNECTOR'S OWN address table, which is the same source the
    receipt parsers use to stamp ``nft_manager_addr`` onto the ``position_registry``
    row. That is what makes this a reconstruction of the registry row's authority
    rather than a guess about it, and it is why the two halves match by
    construction.

    **Do not route this through** ``_nft_manager_for_protocol_chain``. An earlier
    version of this function did, gated on ``PROTOCOL_FAMILY_REGISTRY`` membership,
    and it was wrong for SushiSwap V3 on every chain: Sushi is in the UniV3 LP
    family, so it passed the gate, but that helper has no Sushi entry and falls
    through to the canonical Uniswap NPM. Measured — connector truth vs. that
    helper:

    ==========  ==========================================  ==========================================
    chain       ``sushiswap_v3`` position_manager           what the helper returned
    ==========  ==========================================  ==========================================
    ethereum    ``0x2214a42d8e2a1d20635c2cb0664422c528b6a432``  ``0xc36442b4…`` (Uniswap)
    arbitrum    ``0xf0cbce1942a68beb3d1b73f0dd86c8dcc363ef49``  ``0xc36442b4…`` (Uniswap)
    base        ``0x80c7dd17b01855a6d2347444a0fcc36136a314de``  ``0x03a520b3…``
    polygon     ``0xb7402ee99f0a008e461098ac3a27f4957df89a40``  ``0xc36442b4…`` (Uniswap)
    ==========  ==========================================  ==========================================

    A false authority is worse than none: the Sushi row would fail to dedupe with
    its own registry row (VIB-6730 unfixed for Sushi) and could additionally alias
    an unrelated Uniswap NFT of the same token id and suppress it — the strand
    direction. That helper's own docstring claims Sushi shares the canonical NPM;
    it does not. The claim is stale, it also feeds ``physical_identity_hash`` via
    ``strategy_runner``, and it is tracked separately — this function must not
    inherit it.

    Returns ``""`` when no connector publishes a manager for ``(protocol, chain)``:
    an unknown protocol, a registry primitive (``lp`` / ``lp_v4``, which name no
    connector), or an unsupported chain. Those rows keep the bare key and fall back
    to :func:`_lp_bridge_tokens`.

    KNOWN RESIDUAL — a SLUG MISMATCH here is a strand vector (VIB-6752, Urgent).
    An NFT-shaped row whose protocol resolves to nothing keeps the bare key and can
    therefore be bridge-merged with a manager-qualified row of the same token id,
    dropping the latter from the enumeration. That is reachable today via one shipped
    strategy: ``agni_lp_mantle`` declares ``protocol="agni"`` while the address table
    registers ``agni_finance``, so it derives no authority — and its rows ARE
    NFT-shaped, because ``resolve_nft_token_id`` reads ``details`` BEFORE
    ``position_id`` and the strategy sets ``details["nft_id"]``. Checking
    ``position_id`` alone says "not NFT-shaped" and is wrong; that mistake is why this
    was initially assessed as harmless.

    The slug cannot simply be renamed — the SDK routes Agni *intents* under ``agni``
    while the address table uses ``agni_finance``, so the fix is alias normalisation
    or a bridge restriction, tracked on VIB-6752 with its own proof. Shipping with
    this open was an explicit operator scope decision, not a silent deferral.

    Note also that the family guard in ``test_registry_enumeration.py`` does NOT cover
    this: it checks the two LP grouping families, and ``agni_finance`` and ``camelot``
    are V3-shape forks outside them.

    Note the lowercase: connector tables store mixed-case addresses while producers
    and :func:`_lp_nft_parts` store them lowercased, and an un-normalised value
    would compare unequal to the identical address and silently re-split the pair.
    """
    from almanak.connectors._strategy_base.address_registry import addresses_for
    from almanak.connectors._strategy_base.protocol_aliases import normalize_protocol
    from almanak.connectors._strategy_contract_role_registry import CONTRACT_ROLE_REGISTRY, ContractRole

    raw_protocol = (protocol or "").strip().lower()
    chain_norm = (chain or "").strip().lower()
    if not raw_protocol or not chain_norm:
        return ""

    # Strategies declare the slug they route INTENTS under; the address table is
    # keyed by the canonical connector slug, and the two are not always the same
    # word. `agni_lp_mantle` reports ``protocol="agni"`` while the table registers
    # ``agni_finance``, so an un-normalised lookup returned nothing for a venue that
    # publishes a real manager — leaving an NFT-shaped row bare and therefore
    # bridgeable onto another manager's NFT of the same token id (VIB-6752).
    # ``normalize_protocol`` is the canonical alias seam and a documented no-op on
    # already-canonical values; measured across every LP protocol, it changes
    # exactly one result (agni/mantle) and leaves the rest byte-identical.
    protocol_norm = normalize_protocol(chain_norm, raw_protocol)

    # A slug with a CL_POSITION_MANAGER role publishes one kind per reviewed
    # manager generation on its address-owning connector's table; every one
    # of them is a candidate, so a multi-generation venue refuses below.
    cl_kinds = CONTRACT_ROLE_REGISTRY.kinds_for(protocol_norm, ContractRole.CL_POSITION_MANAGER)
    if cl_kinds:
        table = addresses_for(CONTRACT_ROLE_REGISTRY.address_protocol(protocol_norm), chain_norm) or {}
        kinds: tuple[str, ...] = tuple(cl_kinds)
    else:
        table = addresses_for(protocol_norm, chain_norm) or {}
        kinds = _LP_MANAGER_CONTRACT_KINDS
    candidates = {str(table[kind]).strip().lower() for kind in kinds if table.get(kind)}
    if len(candidates) != 1:
        # 0 -> this connector publishes no manager for the chain.
        # >1 -> the venue publishes MORE THAN ONE reviewed manager, so no single
        #       address is "the" authority for (protocol, chain) and any choice
        #       would be a guess. Refuse; the bare key plus the bounded bridge is
        #       correct here and a wrong address is not.
        return ""
    return candidates.pop()


def _lp_nft_parts(position: PositionInfo) -> tuple[str, str] | None:
    """``(token_id, manager)`` for an NFT-shaped LP row, or ``None``.

    ``manager`` is the lowercased ERC-721 manager address. It comes from the
    producer when one supplied it, and otherwise from
    :func:`_derived_lp_manager`, which reconstructs it from ``(protocol, chain)``
    using the registry's own authority (VIB-6735). ``""`` means neither source
    could name one — an unrecognised protocol, or an unknown chain. Split out of
    :func:`_lp_identity` so the bridge below can ask which of the two halves a
    row actually carries without re-parsing the rendered identity string.
    """
    token_id = resolve_nft_token_id(position)
    if token_id is None:
        return None
    details = position.details if isinstance(position.details, dict) else {}
    raw_manager = next((details.get(key) for key in _LP_MANAGER_DETAIL_KEYS if details.get(key)), None)
    if raw_manager:
        return str(token_id), str(raw_manager).strip().lower()
    return str(token_id), _derived_lp_manager(str(position.protocol or ""), str(position.chain or ""))


def _unambiguous_lp_nft_ids(positions: Iterable[PositionInfo]) -> frozenset[tuple[str, str]]:
    """``(chain, token_id)`` pairs that name AT MOST ONE manager authority (VIB-6730).

    :func:`_lp_identity` qualifies a token id with its ERC-721 manager whenever the
    producer supplies one, and leaves it bare when the producer does not. Both forms
    are correct, but they never intersect — so the identical physical NFT enumerates
    TWICE the moment the two sources disagree about whether to supply the authority.

    That disagreement is not an edge case, it is the norm. Every V3-family receipt
    parser writes ``nft_manager_addr`` into the ``position_registry`` payload
    (``uniswap_v3/receipt_parser.py``), while a strategy's ``get_open_positions()``
    reports the pool, the ticks and the amounts and essentially never the manager.
    So a plain uniswap_lp open→teardown enumerates its one NFT as two positions, one
    of which has no on-chain post-condition, and TD-15 refuses to certify a close
    that demonstrably happened (VIB-6730). Blueprint 14 §"Enumeration is ADDITIVE"
    states the intended contract — the same physical NFT never double-counts, while
    equal numeric IDs on two NPM generations remain distinct — and only the second
    half of it survived.

    Bridging the two forms unconditionally would buy the first half by giving up the
    second: Slipstream-style token ids are per-manager counters, so ``(manager A, 42)``
    and ``(manager B, 42)`` are two physical positions that a bare ``nft:42`` alias
    would silently merge — and a merge STRANDS a position, the one direction this
    module refuses to fail in.

    The ambiguity is a property of the enumerated SET, not of any single row, so it is
    decided here over the whole set: a token id seen under two or more distinct
    managers is ambiguous and no bare alias is emitted for it, leaving those rows split
    (loud). A token id seen under at most one manager cannot alias a different physical
    position, so the qualified row may also answer to its bare form and match the
    unqualified row naming the same NFT.
    """
    managers: dict[tuple[str, str], set[str]] = {}
    for position in positions:
        if position.position_type != PositionType.LP:
            continue
        parts = _lp_nft_parts(position)
        if parts is None:
            continue
        token_id, manager = parts
        seen = managers.setdefault((str(position.chain or "").lower(), token_id), set())
        if manager:
            seen.add(manager)
    return frozenset(key for key, found in managers.items() if len(found) <= 1)


def _lp_bridge_tokens(
    position: PositionInfo,
    unambiguous_nft_ids: frozenset[tuple[str, str]],
) -> frozenset[tuple[str, ...]]:
    """The bare-id alias a manager-qualified LP row may additionally answer to.

    Empty for a row that carries no manager (its identity is already the bare form)
    and for a token id :func:`_unambiguous_lp_nft_ids` found under more than one
    manager. Additive only — it never removes the row's own qualified identity, so a
    bridged row still matches another manager-qualified row exactly as before.

    KNOWN LIMITATION (VIB-6735). "At most one manager" means at most one *observed*
    manager: an unqualified row contributes none, so it is treated as belonging to
    the single observed authority. If a deployment ran two managers on one chain
    whose token ids happened to collide, and the strategy reported the row for
    manager B without an authority while the registry held only manager A's row,
    the bridge would merge them and A would leave the enumeration unclosed.

    The two-counter precondition is NOT exotic, and an earlier version of this note
    wrongly said no shipped configuration produces it. The UniV3
    NonfungiblePositionManager and the UniV4 PositionManager are two independent
    counters on the same chain, and BOTH primitives ship registry cutovers
    (``_LP_REGISTRY_SPECS``), so a deployment running V3 and V4 LP together is a
    shipped configuration — only the numeric id collision is left to chance.
    Measured: a V4 strategy row and a V3 registry row sharing an id merge, dropping
    the V3 row. The mirror case is the loud one: when BOTH registry rows are present
    two managers are observed, the bridge switches off for that id, and the original
    VIB-6730 double-count returns for it. Still pinned by
    ``test_vib6735_complementary_source_rows_are_split_not_silently_merged`` rather
    than left latent.

    WHICH VENUES STILL REACH IT, after the derivation (VIB-6735, accepted residual).
    The derivation closes this for every venue publishing exactly ONE reviewed
    manager: both arms then carry the same qualified key and the bridge is never
    consulted. What remains is the venues that deliberately derive NOTHING —
    multi-generation venues such as Aerodrome/Velodrome Slipstream, which publish a
    legacy AND a current NPM so naming either would be a guess, plus protocols no
    connector publishes a manager for.

    Measured on the shipped code: a strategy row for a CURRENT-generation Slipstream
    NFT #7 and a registry row for a LEGACY NFT #7 — two physically distinct NFTs —
    enumerate as ONE, dropping the registry row. Preconditions are compound (both
    generations in one deployment, colliding numeric ids, a missing registry write),
    and the hole PREDATES this function: the bridge behaved this way before any
    derivation existed. It is narrowed here, not introduced, and ships as an accepted
    residual by explicit operator decision.

    Closing it properly means refusing the bare alias when the unqualified arm is a
    STRATEGY row whose protocol resolves to nothing — which requires distinguishing
    strategy rows from legacy manager-less REGISTRY rows, where bridging is correct
    and load-bearing for VIB-5723. That is a change to the identity model and needs
    its own proof. Do NOT "simplify" this by refusing the bridge outright.

    CORRECTION (VIB-6735). An earlier version of this note said the hole "cannot be
    closed from the enumeration alone". That is FALSE, and it is the sentence a
    reader would use to justify never fixing this. B's authority is often derivable
    from fields already on the row, and :func:`_derived_lp_manager` does exactly
    that — see its docstring for the resolver and its limits.

    Read that function, not this paragraph, for HOW. Two earlier drafts of this note
    named a specific mechanism and both aged badly within a day:

    * they pointed at ``_nft_manager_for_protocol_chain``, which is **wrong for
      SushiSwap V3 on every chain** (it has no Sushi entry and falls through to the
      canonical Uniswap NPM) and is tracked as VIB-6750. Do not route identity
      through it;
    * they described resolution as "recognised protocol → look up an address", which
      produced a second wrong-value defect: Aerodrome Slipstream publishes a legacy
      AND a current NPM, and picking either is a guess.

    The surviving principle, which is what actually generalises: **a false authority
    is worse than none.** Where exactly one reviewed manager exists, derive it; where
    zero or several do, name nothing and let the bare key fall back to the bounded
    bridge. An over-split is loud; a wrong merge strands a position.

    Two measurements, recorded because both refute an argument that looks sound
    (VIB-6735):

    * **The id spaces overlap completely — do NOT reason "different eras cannot
      collide".** Measured 2026-08-20: Arbitrum V3 ``NonfungiblePositionManager``
      ``totalSupply()`` = 4,981,127 with live ids past 5,654,302, while the V4
      ``PositionManager`` ``nextTokenId()`` = 197,204. Every id V4 has ever
      minted on that chain also exists on V3. Ethereum is the same shape
      (1,215,211 vs 377,924). What makes a collision unlikely is only that ONE
      deployment must hold both specific NFTs, not any property of the counters.
    * **Pool identity is NOT available as a discriminator here.** The obvious
      tightening — bridge only when the two rows agree on a pool — cannot be
      implemented at this layer: ``uniswap_lp`` publishes ``details["pool"]`` as
      the human label ``"WETH/USDC/500"`` (``strategy.py`` parses it with
      ``split("/")``), not an address, so the flagship VIB-6730 case carries no
      pool identifier on the strategy arm at all. Requiring agreement would
      refuse to bridge exactly the case this function exists to fix. V4 has the
      same trap in the other direction: its ``details["pool"]`` is a label too
      and the identifier lives in ``details["pool_id"]``.
    """
    parts = _lp_nft_parts(position)
    if parts is None:
        return frozenset()
    token_id, manager = parts
    if not manager:
        return frozenset()
    if (str(position.chain or "").lower(), token_id) not in unambiguous_nft_ids:
        return frozenset()
    return frozenset({("lp", f"nft:{token_id}")})


def _lp_default_identity(position: PositionInfo) -> frozenset[tuple[str, ...]]:
    """Framework default identity for an LP position (moved verbatim, VIB-5723).

    Collapses the same physical NFT position across sources (registry bare token
    id vs strategy composite key) — see :func:`_lp_identity`.
    """
    return frozenset({("lp", _lp_identity(position))})


def _lending_default_identity(position: PositionInfo) -> frozenset[tuple[str, ...]]:
    """Framework default identity for a lending leg (moved verbatim, VIB-5523).

    The strategy and the registry name the SAME leg with DIFFERENT
    ``position_id`` formats — the strategy emits e.g.
    ``aave-supply-wstETH-arbitrum`` while the registry row stores the
    ``market_id`` (``wsteth``). Keying lending on ``position_id`` double-counts
    the leg (4 union entries for 2 real positions), and the two registry
    duplicates then get flagged uncovered by the completeness check. Key lending
    instead on the shared semantic identity ``(protocol, market_or_asset)``: the
    registry row's ``market_id`` equals the strategy's asset symbol for
    one-pool-per-chain protocols (Aave / Spark / Compound), and an
    isolated-market protocol (Morpho) carries its bytes32 ``market_id`` on BOTH
    sides, so the key matches the same leg without ever merging two
    genuinely-distinct markets.
    """
    details = position.details if isinstance(position.details, dict) else {}
    asset = ""
    for key in ("asset", "asset_symbol", "collateral_token", "supply_token", "borrow_token", "debt_token"):
        value = details.get(key)
        if value:
            asset = str(value).lower()
            break
    # Prefer market_id when present; otherwise the asset symbol. For Aave the
    # registry's market_id IS the asset symbol, so a market_id-bearing registry
    # row and an asset-only strategy row resolve to the same discriminator and
    # dedup correctly.
    #
    # Use an explicit ``is None`` check — ``market_id or ""`` would turn a
    # legitimate ``market_id == 0`` (int) into ``""`` and silently fall back to
    # ``asset``, mis-keying the leg.
    market_id_val = details.get("market_id")
    market_id_str = "" if market_id_val is None else str(market_id_val).lower()
    discriminator = market_id_str or asset
    return frozenset({("lending", str(position.protocol or "").lower(), discriminator)})


def _perp_default_identity(position: PositionInfo) -> frozenset[tuple[str, ...]]:
    """Framework default identity for a perp when its venue publishes no hook.

    Kept from the pre-VIB-6287 arm table so the four perp venues without an
    identity hook (``aster_perps`` / ``drift`` / ``hyperliquid`` /
    ``pancakeswap_perps``) keep the exact behaviour they have today. It is a
    LAST resort, consulted only when the venue publishes no identity hook,
    because it compares ``details`` values VERBATIM and the producers of a perp
    row write different value spaces under the same key names — the polysemy
    that is VIB-6287. Once a venue publishes a hook, an empty or fully rejected
    emission falls directly to raw ``position_id``; routing it through this
    coarser namespace could collapse distinct venue positions.

    Returns an empty set when any component is absent: incomplete identity must
    never collapse two positions, so the caller falls through to the raw
    ``position_id``.

    A row carrying a ``kind`` marker is never named here, mirroring the guard in
    the GMX hook. A residual — a pending unfilled order, an unverified sweep
    sentinel — can carry the SAME market, collateral and side as a real open
    position while being a different thing holding its own collateral, so naming
    it would let the union merge the two and suppress one; a suppressed residual
    is never recovered. LATENT rather than live today: residuals reach the
    enumeration through ``_union_residuals``, a separate lane, and
    ``_position_info_from_perp_registry_row`` does not copy ``kind`` onto perp
    registry rows. It is guarded anyway so the connector hook and the framework
    default cannot disagree about what a residual is — a venue with no hook
    would otherwise be the one place this hole stayed open.
    """
    details = position.details if isinstance(position.details, dict) else {}
    if is_residual_marked(details):
        return frozenset()
    market = str(details.get("market_address") or details.get("market") or "").lower()
    collateral = str(details.get("collateral_address") or details.get("collateral_token") or "").lower()
    # ``details["side"]`` is read for the same reason the GMX hook reads it: shipped
    # perp demos write it (`gmx_v2_directional_perp`, `hyperliquid_trailing_perp`).
    # Same long/short value space as ``direction``, so it cannot introduce a
    # VIB-6287-style polysemy — swept every PERP producer to confirm none uses
    # ``side`` for a different vocabulary.
    #
    # INERT for those demos today, stated rather than implied: they write no
    # collateral, and this default requires market AND collateral AND direction, so
    # their rows still fall through to raw ``position_id``. Correct to read, but it
    # does not yet serve the demos an earlier version of this comment claimed it
    # served (#3534 panel).
    direction = str(
        position.direction
        or details.get("direction")
        or details.get("side")
        or ("long" if details.get("is_long") is True else "short" if details.get("is_long") is False else "")
    ).lower()
    protocol = str(position.protocol or "").lower()
    if protocol and market and collateral and direction:
        return frozenset({("perp", protocol, market, collateral, direction)})
    return frozenset()


# Framework per-``PositionType`` identity default, consulted ONLY when the
# position's own venue publishes no identity hook. A hook that declines or fails
# validation falls directly to raw ``position_id`` so rejection cannot enter a
# coarser, potentially lossy namespace. Every entry moved here VERBATIM from the
# former ``_dedupe_key`` if/elif arm table.
_IDENTITY_DEFAULTS: dict[PositionType, Callable[[PositionInfo], frozenset[tuple[str, ...]]]] = {
    PositionType.LP: _lp_default_identity,
    PositionType.SUPPLY: _lending_default_identity,
    PositionType.BORROW: _lending_default_identity,
    PositionType.PERP: _perp_default_identity,
}

# Position types that deliberately key on the raw ``position_id`` — the
# pre-VIB-6287 fall-through, made explicit so the census test can require every
# ``PositionType`` to be a NAMED decision rather than a silent omission.
#
# These are NOT "no identity needed forever"; they are "no cross-source identity
# problem has been measured yet". Each carries the ticket that would revisit it:
#
#   * TOKEN / STAKE  — fungible holdings; the raw id is the token itself and no
#     second producer writes them, so there is nothing to reconcile (VIB-6287).
#   * VAULT          — ERC-4626 shares keyed by vault address on both sides.
#   * CEX            — off-chain venue; no registry producer exists (VIB-6287).
#   * PREDICTION     — no registry cutover; single producer (VIB-6287).
#
# Pendle carries a live duplicate under LP, which is why Pendle is NOT here.
_RAW_ID_TYPES: frozenset[PositionType] = frozenset(
    {
        PositionType.TOKEN,
        PositionType.STAKE,
        PositionType.VAULT,
        PositionType.CEX,
        PositionType.PREDICTION,
    }
)


def _dedupe_keys(
    position: PositionInfo,
    *,
    wallet_for_chain: Callable[[str], str | None] | None = None,
    unambiguous_nft_ids: frozenset[tuple[str, str]] = frozenset(),
) -> frozenset[tuple[str, ...]]:
    """Every ``(chain, position_type, alias)`` key that names ``position``.

    Two rows are the same position iff their key sets intersect. Identity is
    resolved in three steps, most-authoritative first:

    1. **The position's own venue**, through the connector-published identity
       hook. Only the venue knows how it names a position, and — critically —
       the different producers of one row write different VALUE SPACES under the
       same ``details`` key names (symbol vs address), which no framework-side
       comparison of raw fields can reconcile (VIB-6287).
    2. **The framework per-type default** (:data:`_IDENTITY_DEFAULTS`), the
       pre-VIB-6287 behaviour preserved verbatim, only for types and venues with
       no applicable hook. A registered PERP hook that returns no usable token
       skips this rung so validation failure cannot collapse positions in a
       coarser namespace.
    3. **The raw ``position_id``** — the original fall-through.

    ``chain`` and ``position_type`` scope every key. Keying on a bare id would
    let a strategy-reported LP ``token_id=N`` on chain A suppress a
    registry-open LP ``token_id=N`` on chain B → under-report → strand chain B's
    position (the inline multi-chain teardown lane, ``runner_teardown``
    §"For multi-chain strategies").

    Non-perp positions return a SINGLE key, except an LP row bridged by
    ``unambiguous_nft_ids`` (VIB-6730), which additionally answers to the bare-id
    form of its own NFT so a manager-qualified row and an unqualified one naming
    the same physical NFT stop enumerating twice. The bridge set is computed over
    the whole enumeration by :func:`_unambiguous_lp_nft_ids`; the default empty
    set reproduces the previous behaviour exactly.

    Venue tokens are placed in the key VERBATIM — never case-folded (see
    ``almanak.connectors._strategy_base.perp_identity``: ``drift``'s identity
    contains a case-SENSITIVE base58 pubkey, and folding it here would merge two
    distinct Solana accounts). Only ``chain`` — a slug, not a token — is folded.
    """
    chain = str(position.chain or "").lower()
    ptype = str(position.position_type)

    from .perp_identity import has_perp_identity_hook, venue_identity_tokens, wallet_for

    # Venue tokens are single-element tuples so every key is a flat tuple of
    # STRUCTURED parts. Joining parts into one string instead would make the
    # key ambiguous — ("a:b", "c") and ("a", "b:c") would render identically —
    # and an ambiguous key can only ever over-collapse, which suppresses a row
    # so nothing closes it.
    tokens: frozenset[tuple[str, ...]] = frozenset(
        ("venue", t) for t in venue_identity_tokens(position, wallet_for(wallet_for_chain, position.chain))
    )
    applicable_perp_hook = position.position_type == PositionType.PERP and has_perp_identity_hook(
        str(position.protocol or "").strip()
    )
    if not tokens and not applicable_perp_hook:
        default = _IDENTITY_DEFAULTS.get(position.position_type)
        if default is not None:
            tokens = default(position)
            # VIB-6730: bridge the manager-qualified and bare NFT identity forms,
            # but ONLY under the framework default. A venue that published its own
            # identity tokens owns that namespace, and grafting a framework alias
            # onto it could merge two rows the venue deliberately kept apart.
            if position.position_type == PositionType.LP:
                tokens = tokens | _lp_bridge_tokens(position, unambiguous_nft_ids)
    if not tokens:
        # Empty ≠ Zero: an UNMEASURED identity must never collapse two rows, so
        # it falls back to the raw id rather than to a permissive wildcard.
        return frozenset({(chain, ptype, "id", str(position.position_id))})
    return frozenset((chain, ptype, *parts) for parts in tokens)


@dataclass(frozen=True)
class RegistryReadResult:
    """Outcome of a ``position_registry`` WARM read for the cut-over LP primitives.

    Richer than the legacy ``(positions, available)`` tuple so the caller can
    tell a *partial* read (some primitive's SQL read raised transiently) apart
    from a clean read — the distinction TD-05 (VIB-5463) needs to stop the
    registry-read failure path being warn-only.

    Attributes:
        positions: The OPEN LP positions the registry could read.
        available: ``True`` iff at least one primitive's read returned (an
            answerable registry). ``False`` ⇒ no backend / hosted pre-T19 ⇒ the
            caller keeps the legacy enumeration unchanged.
        failed_primitives: Primitives whose read RAISED a non-cutover error
            (transient gateway / decode fault). Non-empty ⇒ the registry answer
            is **incomplete** for those primitives, so a chain-verify of the
            known set must run before the enumeration is trusted.
    """

    positions: list[PositionInfo] = field(default_factory=list)
    available: bool = False
    failed_primitives: tuple[str, ...] = ()


# (primitive, accounting_category) for the two cut-over LP primitives. Mirrors
# ``ACTIVE_CUTOVERS`` in ``almanak/framework/runner/cutover.py`` (UniV3 LP +
# UniV4 LP). We deliberately do NOT hardcode a protocol slug here: the registry
# payload does not carry the specific slug (uniswap_v3 / sushiswap_v3 /
# slipstream all share ``primitive='lp'``), and the enumerated ``PositionInfo``
# label is informational only — the actual closing intent is the strategy's own
# (with its true protocol) and the position_id is registry-resolved. The
# ``primitive`` value is used as the label, which keeps framework code free of
# protocol-name coupling (blueprint 22 / coupling ratchet).
_LP_REGISTRY_SPECS: tuple[tuple[str, str], ...] = (
    ("lp", "lp"),
    ("lp_v4", "lp_v4"),
)


# (primitive, accounting_category) for the lending cutover (TD-04 / VIB-5462).
# Mirrors the lending ``CutoverSpec`` in ``almanak/framework/runner/cutover.py``
# (``Primitive.LENDING`` / 'lending'). Aave is canonical; the registry row shape
# (market_id + leg) is protocol-agnostic, so the SAME enumeration surfaces every
# lending protocol the cutover enables — no per-protocol code here.
_LENDING_REGISTRY_SPECS: tuple[tuple[str, str], ...] = (("lending", "lending"),)


# (primitive, accounting_category) for the perp cutover (TD-02 / VIB-5460).
# Mirrors the perp ``CutoverSpec`` in ``almanak/framework/runner/cutover.py``
# (``Primitive.PERP`` / 'perp'). GMX V2 is canonical; the registry row shape
# (venue position_key anchor + market/collateral/direction/size payload) is
# protocol-agnostic, so the SAME enumeration surfaces every perp protocol the
# cutover enables — no per-protocol code here.
_PERP_REGISTRY_SPECS: tuple[tuple[str, str], ...] = (("perp", "perp"),)


# (primitive, accounting_category) for the Pendle cutover (TD-03 / VIB-5461).
# Mirrors the Pendle ``CutoverSpec`` in ``almanak/framework/runner/cutover.py``
# (``Primitive.SWAP`` / 'pendle'). Both Pendle KINDS (PT + LP) live in the
# otherwise-empty swap-primitive partition, so we read ``primitive='swap'`` with
# NO ``accounting_category`` filter (None ⇒ all) and discriminate on the payload
# ``kind`` below — the partition holds nothing but Pendle rows, but the kind
# filter keeps the read robust if a future non-Pendle swap-registry writer
# appears. No protocol-name literal is needed here (``kind`` ∈ {pt, lp} is the
# discriminator), so the framework stays free of chain/protocol coupling.
_PENDLE_REGISTRY_SPECS: tuple[tuple[str, str | None], ...] = (("swap", None),)


# A Pendle registry kind maps onto the teardown-lane position type: a PT holding
# is a held ERC-20 swapped/redeemed at teardown (TOKEN — the catch-all the
# teardown lane swaps to target last); an LP holding is a market-LP closed via
# the strategy's own LP_CLOSE (LP). Both are READ-PATH-ONLY surfaces here — the
# closing intent is the strategy's own (the registry payload carries the
# identity, not a protocol slug for synthesis).
_PENDLE_KIND_TO_POSITION_TYPE: dict[str, PositionType] = {
    "pt": PositionType.TOKEN,
    "lp": PositionType.LP,
}


# A lending registry leg maps onto the teardown-lane risk-ordered position type:
# a supply (collateral) leg is withdrawn (SUPPLY), a borrow (debt) leg is repaid
# (BORROW). The teardown ``PositionType`` priorities already close BORROW before
# SUPPLY (repay frees collateral), so surfacing the legs separately is exactly
# what the HF-safe unwind (TD-09) needs.
_LENDING_LEG_TO_POSITION_TYPE: dict[str, PositionType] = {
    "collateral": PositionType.SUPPLY,
    "debt": PositionType.BORROW,
}


def _position_info_from_registry_row(row: Any, *, primitive: str) -> PositionInfo | None:
    """Build an LP :class:`PositionInfo` from one OPEN ``position_registry`` row.

    Returns ``None`` when the row carries no usable ``token_id`` (the identity
    anchor) — a registry row without it cannot be closed and must not be
    surfaced as an open position.

    The ``protocol`` field is labelled with the registry ``primitive`` (``lp`` /
    ``lp_v4``), the most specific thing the registry actually knows — the row
    carries no protocol slug, and the framework must not invent one. The label
    is cosmetic: registry-derived positions are added to the enumeration for
    visibility / counting, never used to build closing intents.

    USD value is left at ``Decimal("0")``: the registry is the identity surface,
    not a valuation surface (blueprint 28 §2 ownership matrix). Teardown does
    not need a USD figure to close a known position; PortfolioValuer owns
    valuation and is out of scope for this read-path cutover.
    """
    if not isinstance(row, dict):
        return None
    payload = row.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    token_id = payload.get("token_id")
    if token_id is None or token_id == "":
        return None
    chain = str(row.get("chain") or "").lower()
    # V3 stores ``pool_address``; V4 stores ``pool_id`` (the PoolKey hash — V4
    # pools have no per-pool contract address). Surface whichever is present.
    pool = payload.get("pool_address") or payload.get("pool_id")
    details: dict[str, Any] = {"source": "position_registry"}
    if pool:
        details["pool"] = str(pool)
    nft_manager_addr = payload.get("nft_manager_addr") or payload.get("position_manager")
    if nft_manager_addr:
        details["nft_manager_addr"] = str(nft_manager_addr)
    for key in ("tick_lower", "tick_upper", "liquidity", "fee_tier"):
        value = payload.get(key)
        if value is not None:
            details[key] = value
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=str(token_id),
        chain=chain,
        protocol=primitive,
        value_usd=Decimal("0"),
        details=details,
    )


async def read_open_lp_positions_detailed(
    *,
    state_manager: Any,
    deployment_id: str,
    chain: str | None = None,
) -> RegistryReadResult:
    """Read this deployment's OPEN UniV3 + UniV4 LP positions from WARM (detailed).

    The richer counterpart of :func:`read_open_lp_positions_from_registry`: it
    additionally reports which primitives' reads RAISED so the caller can chain-
    verify the known set instead of silently warning (TD-05 / VIB-5463).

    Args:
        state_manager: The registry-capable :class:`StateManager` (the runner's
            accounting state manager, or the strategy's gateway-backed one). May
            be ``None`` or lack the registry accessor on a backend that has not
            shipped cutover storage.
        deployment_id: The deployment whose rows to read.
        chain: Optional chain filter. ``None`` reads every chain for the
            deployment — which, under 1 gateway : 1 strategy, is exactly this
            strategy's positions.

    Returns:
        A :class:`RegistryReadResult`. ``available`` is ``False`` when the
        backend cannot answer a registry read (no state manager, missing
        accessor, or hosted pre-T19 → :class:`CutoverStorageNotSupported`); the
        caller then keeps the strategy's own enumeration unchanged. ``available``
        ``True`` with an empty list means "registry is authoritative and this
        deployment has zero open LP". ``failed_primitives`` names any primitive
        whose read RAISED a transient fault — the registry answer is incomplete
        for those.

    Never raises — enumeration must never fault the teardown lane.
    """
    from almanak.framework.migration import CutoverStorageNotSupported

    dep = str(deployment_id or "").strip()
    if state_manager is None or not dep or not hasattr(state_manager, "get_position_registry_open_rows"):
        return RegistryReadResult(positions=[], available=False, failed_primitives=())

    positions: list[PositionInfo] = []
    available = False
    failed: list[str] = []
    for primitive, accounting_category in _LP_REGISTRY_SPECS:
        try:
            rows = await state_manager.get_position_registry_open_rows(
                dep,
                chain=chain,
                primitive=primitive,
                accounting_category=accounting_category,
            )
        except (CutoverStorageNotSupported, NotImplementedError) as exc:
            # Backend without cutover storage (hosted pre-T19). Degrade to the
            # legacy enumeration — never treat "can't read" as "nothing open".
            logger.debug(
                "Teardown registry enumeration: %s read unavailable for %s (%s)",
                primitive,
                dep,
                exc,
            )
            continue
        except Exception:  # noqa: BLE001 — enumeration must never raise into teardown
            # A genuinely-failed registry read (transient gateway error, decode
            # fault) during teardown must be OBSERVABLE — this primitive then
            # falls back to the strategy's own enumeration, but on a wiped-state
            # restart it would be invisible. Surface it as a failed primitive so
            # the caller (TD-05) chain-verifies the known set rather than trusting
            # the strategy enumeration blindly (no longer warn-only).
            logger.warning(
                "Teardown registry enumeration: %s read FAILED for %s — registry "
                "answer is incomplete; the known LP set will be chain-verified",
                primitive,
                dep,
                exc_info=True,
            )
            failed.append(primitive)
            continue
        available = True
        for row in rows or []:
            info = _position_info_from_registry_row(row, primitive=primitive)
            if info is not None:
                positions.append(info)
    return RegistryReadResult(positions=positions, available=available, failed_primitives=tuple(failed))


async def read_open_lp_positions_from_registry(
    *,
    state_manager: Any,
    deployment_id: str,
    chain: str | None = None,
) -> tuple[list[PositionInfo], bool]:
    """Read this deployment's OPEN UniV3 + UniV4 LP positions from WARM.

    Back-compat 2-tuple facade over :func:`read_open_lp_positions_detailed`.

    Returns:
        ``(positions, available)``. ``available`` is ``False`` when the backend
        cannot answer a registry read; ``True`` with an empty list means the
        registry is authoritative and this deployment has zero open LP.

    Never raises — enumeration must never fault the teardown lane.
    """
    result = await read_open_lp_positions_detailed(
        state_manager=state_manager,
        deployment_id=deployment_id,
        chain=chain,
    )
    return result.positions, result.available


def _position_info_from_lending_registry_row(row: Any) -> PositionInfo | None:
    """Build a lending :class:`PositionInfo` from one OPEN ``position_registry`` row.

    Returns ``None`` when the row carries no usable ``market_id`` (the identity
    anchor) or an unknown ``leg`` — a registry row without a resolvable
    *(market, leg)* cannot be unwound and must not be surfaced.

    USD value is left at ``Decimal("0")``: the registry is the identity surface,
    not a valuation surface (blueprint 28 §2). The reserve symbol is carried in
    ``details["asset_symbol"]`` (NOT ``details["asset"]``) so it never trips the
    PortfolioValuer wallet-overlap special-casing reserved for TOKEN
    pseudo-positions — these are real protocol legs whose valuation TD-09 / the
    valuer owns, out of scope for this read-path cutover.
    """
    if not isinstance(row, dict):
        return None
    payload = row.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    market_id = payload.get("market_id")
    if market_id is None or market_id == "":
        return None
    leg = str(payload.get("leg") or "").strip().lower()
    position_type = _LENDING_LEG_TO_POSITION_TYPE.get(leg)
    if position_type is None:
        return None
    chain = str(row.get("chain") or "").lower()
    # The protocol slug IS carried in the lending payload (unlike LP, whose
    # payload carries no slug). Prefer it so teardown / TD-09 can route the
    # closing intent to the right connector; fall back to the registry primitive.
    protocol = str(payload.get("protocol") or row.get("primitive") or "lending").lower()
    details: dict[str, Any] = {"source": "position_registry", "leg": leg, "market_id": str(market_id)}
    asset = payload.get("asset")
    if asset:
        details["asset_symbol"] = str(asset)
    return PositionInfo(
        position_type=position_type,
        position_id=str(market_id),
        chain=chain,
        protocol=protocol,
        value_usd=Decimal("0"),
        details=details,
    )


async def read_open_lending_positions_from_registry(
    *,
    state_manager: Any,
    deployment_id: str,
    chain: str | None = None,
) -> tuple[list[PositionInfo], bool]:
    """Read this deployment's OPEN lending legs from WARM (TD-04 / VIB-5462).

    The lending sibling of :func:`read_open_lp_positions_from_registry`: reads
    ``position_registry`` rows for ``primitive='lending'`` and builds one
    :class:`PositionInfo` per open leg (collateral → SUPPLY, debt → BORROW).
    Same ``(positions, available)`` contract and same never-raise discipline —
    ``available=False`` on a backend without cutover storage degrades to the
    strategy's own enumeration; it never means "nothing open".
    """
    from almanak.framework.migration import CutoverStorageNotSupported

    dep = str(deployment_id or "").strip()
    if state_manager is None or not dep or not hasattr(state_manager, "get_position_registry_open_rows"):
        return [], False

    positions: list[PositionInfo] = []
    available = False
    for primitive, accounting_category in _LENDING_REGISTRY_SPECS:
        try:
            rows = await state_manager.get_position_registry_open_rows(
                dep,
                chain=chain,
                primitive=primitive,
                accounting_category=accounting_category,
            )
        except (CutoverStorageNotSupported, NotImplementedError) as exc:
            logger.debug(
                "Teardown registry enumeration: lending read unavailable for %s (%s)",
                dep,
                exc,
            )
            continue
        except Exception:  # noqa: BLE001 — enumeration must never raise into teardown
            logger.warning(
                "Teardown registry enumeration: lending read FAILED for %s — falling back "
                "to strategy enumeration this teardown",
                dep,
                exc_info=True,
            )
            continue
        available = True
        for row in rows or []:
            info = _position_info_from_lending_registry_row(row)
            if info is not None:
                positions.append(info)
    return positions, available


def _position_info_from_perp_registry_row(row: Any) -> PositionInfo | None:
    """Build a perp :class:`PositionInfo` from one OPEN ``position_registry`` row.

    Returns ``None`` when the row carries no usable ``position_id`` (the venue
    position key — the identity anchor) — a registry row without it cannot be
    closed and must not be surfaced.

    USD value is left at ``Decimal("0")``: the registry is the identity surface,
    not a valuation surface (blueprint 28 §2). ``liquidation_risk`` is left at
    the model default (``False``) — the registry knows the position's identity,
    not its on-chain health factor; the teardown ``PositionType.PERP`` priority
    already closes perps FIRST regardless of the flag, so the registry must not
    fabricate a risk signal it cannot measure. Market / collateral / direction /
    size / is_long ride in ``details`` (best-effort: writers differ — the
    settlement reconciler carries ``is_long``, the runtime write carries
    ``size_usd``, a backfill-synthesized row carries only what
    ``position_events`` persisted).
    """
    if not isinstance(row, dict):
        return None
    payload = row.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    position_id = payload.get("position_id")
    if position_id is None or position_id == "":
        return None
    chain = str(row.get("chain") or "").lower()
    # The protocol slug IS carried in the perp payload (unlike LP). Prefer it so
    # teardown can route the closing intent to the right connector; fall back to
    # the registry primitive.
    protocol = str(payload.get("protocol") or row.get("primitive") or "perp").lower()
    details: dict[str, Any] = {"source": "position_registry"}
    # ``is_long`` rides along with the descriptive quartet so the teardown close
    # builder receives the measured boolean when the payload carries one (the
    # settlement reconciler writes it since VIB-5572; older rows carry only the
    # ``direction`` label, which the builder also accepts). The filter keeps a
    # measured ``False`` — a real short — because ``False != ""``.
    for key in ("market", "collateral_token", "direction", "size_usd", "is_long"):
        value = payload.get(key)
        if value is not None and value != "":
            details[key] = value
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id=str(position_id),
        chain=chain,
        protocol=protocol,
        value_usd=Decimal("0"),
        details=details,
    )


async def read_open_perp_positions_from_registry(
    *,
    state_manager: Any,
    deployment_id: str,
    chain: str | None = None,
) -> tuple[list[PositionInfo], bool]:
    """Read this deployment's OPEN perp positions from WARM (TD-02 / VIB-5460).

    The perp sibling of :func:`read_open_lending_positions_from_registry`: reads
    ``position_registry`` rows for ``primitive='perp'`` and builds one
    :class:`PositionInfo` per open position (venue position key → identity).
    Same ``(positions, available)`` contract and same never-raise discipline —
    ``available=False`` on a backend without cutover storage degrades to the
    strategy's own enumeration; it never means "nothing open".
    """
    from almanak.framework.migration import CutoverStorageNotSupported

    dep = str(deployment_id or "").strip()
    if state_manager is None or not dep or not hasattr(state_manager, "get_position_registry_open_rows"):
        return [], False

    positions: list[PositionInfo] = []
    available = False
    for primitive, accounting_category in _PERP_REGISTRY_SPECS:
        try:
            rows = await state_manager.get_position_registry_open_rows(
                dep,
                chain=chain,
                primitive=primitive,
                accounting_category=accounting_category,
            )
        except (CutoverStorageNotSupported, NotImplementedError) as exc:
            logger.debug(
                "Teardown registry enumeration: perp read unavailable for %s (%s)",
                dep,
                exc,
            )
            continue
        except Exception:  # noqa: BLE001 — enumeration must never raise into teardown
            logger.warning(
                "Teardown registry enumeration: perp read FAILED for %s — falling back "
                "to strategy enumeration this teardown",
                dep,
                exc_info=True,
            )
            continue
        available = True
        for row in rows or []:
            info = _position_info_from_perp_registry_row(row)
            if info is not None:
                positions.append(info)
    return positions, available


def _position_info_from_pendle_registry_row(row: Any) -> PositionInfo | None:
    """Build a Pendle :class:`PositionInfo` from one OPEN ``position_registry`` row.

    Returns ``None`` when the row carries no usable ``market_id`` (the identity
    anchor) or an unknown ``kind`` — a registry row without a resolvable
    *(market, kind)* cannot be closed/redeemed and must not be surfaced.

    USD value is left at ``Decimal("0")``: the registry is the identity surface,
    not a valuation surface (blueprint 28 §2). The PT symbol / reserve is carried
    in ``details["asset_symbol"]`` (NOT ``details["asset"]``) so it never trips
    the PortfolioValuer wallet-overlap special-casing reserved for TOKEN
    pseudo-positions — these are real Pendle holdings whose valuation the valuer
    owns, out of scope for this read-path cutover.
    """
    if not isinstance(row, dict):
        return None
    payload = row.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    kind = str(payload.get("kind") or "").strip().lower()
    position_type = _PENDLE_KIND_TO_POSITION_TYPE.get(kind)
    if position_type is None:
        return None
    market_id = payload.get("market_id")
    if market_id is None or market_id == "":
        return None
    chain = str(row.get("chain") or "").lower()
    # The protocol slug IS carried in the Pendle payload (written from the
    # intent/event, where the "pendle" string legitimately lives); prefer it,
    # fall back to the registry primitive so a label is always present — no
    # protocol-name literal in this framework module.
    protocol = str(payload.get("protocol") or row.get("primitive") or "").lower()
    details: dict[str, Any] = {
        "source": "position_registry",
        "kind": kind,
        "market_id": str(market_id),
        # A Pendle held token (PT/YT) routes its teardown close through the owning
        # protocol's swap compiler, not a generic DEX — full_close reads this flag
        # to stamp the position's own protocol on the close SWAP (VIB-5590).
        "protocol_routed_close": True,
    }
    pt_symbol = payload.get("pt_symbol")
    if pt_symbol:
        # The maturity-bearing PT symbol — maturity is intrinsic to it (no
        # separate connector-parsed maturity field on the registry).
        details["asset_symbol"] = str(pt_symbol)
    return PositionInfo(
        position_type=position_type,
        position_id=str(market_id),
        chain=chain,
        protocol=protocol,
        value_usd=Decimal("0"),
        details=details,
    )


async def read_open_pendle_positions_from_registry(
    *,
    state_manager: Any,
    deployment_id: str,
    chain: str | None = None,
) -> tuple[list[PositionInfo], bool]:
    """Read this deployment's OPEN Pendle PT/LP holdings from WARM (TD-03 / VIB-5461).

    The Pendle sibling of :func:`read_open_lp_positions_from_registry`: reads
    ``position_registry`` rows for ``primitive='swap'`` (the isolated Pendle
    partition) and builds one :class:`PositionInfo` per open holding (PT → TOKEN,
    LP → LP). Rows are filtered to the Pendle ``kind`` discriminator so a future
    non-Pendle swap-registry row can never leak in. Same ``(positions,
    available)`` contract and same never-raise discipline — ``available=False``
    on a backend without cutover storage degrades to the strategy's own
    enumeration; it never means "nothing open".
    """
    from almanak.framework.migration import CutoverStorageNotSupported

    dep = str(deployment_id or "").strip()
    if state_manager is None or not dep or not hasattr(state_manager, "get_position_registry_open_rows"):
        return [], False

    positions: list[PositionInfo] = []
    available = False
    for primitive, accounting_category in _PENDLE_REGISTRY_SPECS:
        try:
            rows = await state_manager.get_position_registry_open_rows(
                dep,
                chain=chain,
                primitive=primitive,
                accounting_category=accounting_category,
            )
        except (CutoverStorageNotSupported, NotImplementedError) as exc:
            logger.debug(
                "Teardown registry enumeration: pendle read unavailable for %s (%s)",
                dep,
                exc,
            )
            continue
        except Exception:  # noqa: BLE001 — enumeration must never raise into teardown
            logger.warning(
                "Teardown registry enumeration: pendle read FAILED for %s — falling back "
                "to strategy enumeration this teardown",
                dep,
                exc_info=True,
            )
            continue
        available = True
        for row in rows or []:
            info = _position_info_from_pendle_registry_row(row)
            if info is not None:
                positions.append(info)
    return positions, available


def reconcile_lp_with_registry(
    *,
    strategy_summary: TeardownPositionSummary | None,
    registry_positions: list[PositionInfo],
    registry_available: bool,
    wallet_for_chain: Callable[[str], str | None] | None = None,
) -> TeardownPositionSummary:
    """Fold the ``position_registry`` WARM read into the strategy's enumeration.

    Semantics are **additive (union), never subtractive**, and deliberately so:

    * Every strategy-reported position is kept (non-LP, cut-over LP, non-cut-over
      LP alike) — the read-path cutover must never *drop* a position the strategy
      believes is open, because the OPEN-rows read cannot distinguish "genuinely
      closed" from "registry write was skipped at open" (parser-no-payload
      fallback), and a false drop would under-count / hide a live position.
    * Each registry OPEN LP position the strategy did **not** already report
      (keyed on ``(chain, position_type, position_id)``, NOT bare token id —
      token ids are unique only within a chain) is appended — this is the
      restart-safe re-derivation: a runner whose in-memory state was wiped
      reports an empty (or partial) summary, and the registry — the durable WARM
      tier — supplies the open set.

    On a clean restart the strategy reports nothing, so the union IS exactly the
    registry's open set — the determinism the ticket requires. When the registry
    is NOT available (no backend / hosted pre-T19) or holds no rows, the strategy
    summary is returned unchanged (the legacy enumeration is the degrade path).

    ``wallet_for_chain`` is a CALLABLE, not a scalar wallet: the union spans
    chains and a multi-chain deployment holds a different wallet on each
    (``_teardown_wallet_for_chain``). It lets a venue that derives an identity
    from the account do so.

    **Omitting it is NOT free, and an earlier version of this note said it was.**
    It claimed omission "costs at most a derived alias and can only ever
    over-split, never over-collapse". That held for the old pairwise pass. Under
    the transitive closure a venue can no longer verify that a row's recorded key
    and its attributes name the same position, so a keyed row falls back to
    emitting only its venue key — which costs the key<->semantic join on that path
    (over-split, loud) rather than risking a merge of two distinct positions.
    Defaulting to ``None`` still keeps every existing caller working; it just
    yields less collapsing, not more.
    """
    if strategy_summary is None:
        strategy_summary = TeardownPositionSummary.empty("unknown")
    if not registry_available or not registry_positions:
        return strategy_summary

    # VIB-6730: whether a manager-qualified LP row may also answer to its bare-id
    # form is a property of the SET, not of the row, so it is decided once over
    # both arms of the union before any key is computed.
    unambiguous_nft_ids = _unambiguous_lp_nft_ids([*strategy_summary.positions, *registry_positions])

    def _keys(position: PositionInfo) -> frozenset[tuple[str, ...]]:
        return _dedupe_keys(
            position,
            wallet_for_chain=wallet_for_chain,
            unambiguous_nft_ids=unambiguous_nft_ids,
        )

    # Intersecting alias sets identify one position, and bridging sets require the
    # transitive closure of that relation. Registry row order is unspecified, so
    # deduplication must be order-independent: build every component before choosing
    # representatives. Empty alias sets are unmeasured, join no component, and remain
    # separate.
    parent: dict[tuple[str, ...], tuple[str, ...]] = {}

    def _find(alias: tuple[str, ...]) -> tuple[str, ...]:
        parent.setdefault(alias, alias)
        while parent[alias] != alias:
            parent[alias] = parent[parent[alias]]
            alias = parent[alias]
        return alias

    def _link(aliases: frozenset[tuple[str, ...]]) -> None:
        ordered = sorted(aliases)
        for other in ordered[1:]:
            root_a, root_b = _find(ordered[0]), _find(other)
            if root_a != root_b:
                parent[root_a] = root_b

    strategy_keys = [_keys(p) for p in strategy_summary.positions]
    registry_keys = [_keys(rp) for rp in registry_positions]
    for keys in (*strategy_keys, *registry_keys):
        _link(keys)

    # A registry row that MATCHES an existing strategy position is not
    # just a duplicate to discard — it is frequently the only source of the
    # position's authoritative NFT-manager identity. Every V3-family receipt
    # parser stamps ``nft_manager_addr`` (or ``position_manager``) onto the
    # ``position_registry`` row; a strategy's own ``get_open_positions()``
    # essentially never does (see :func:`_unambiguous_lp_nft_ids`). On a
    # multi-generation venue (Aerodrome/Velodrome Slipstream, which publishes a
    # legacy AND a current reviewed NPM) that missing authority is exactly what
    # makes ``teardown_post_condition`` and the LP valuation reader refuse to
    # certify a position that closed cleanly on-chain — not because closure is
    # in doubt, but because nothing on the strategy's own row says which
    # reviewed manager it lives on. Map each root back to the strategy position
    # that owns it so a matching registry row can enrich it instead of being
    # silently dropped.
    root_to_strategy_index: dict[tuple[str, ...], int] = {}
    for idx, keys in enumerate(strategy_keys):
        for k in keys:
            root_to_strategy_index.setdefault(_find(k), idx)

    claimed: set[tuple[str, ...]] = {_find(k) for keys in strategy_keys for k in keys}
    enriched_positions, any_enriched, net_new = _apply_registry_lp_enrichment(
        strategy_positions=strategy_summary.positions,
        registry_positions=registry_positions,
        registry_keys=registry_keys,
        claimed=claimed,
        root_to_strategy_index=root_to_strategy_index,
        find=_find,
    )
    if not net_new and not any_enriched:
        return strategy_summary

    return TeardownPositionSummary(
        deployment_id=strategy_summary.deployment_id,
        timestamp=strategy_summary.timestamp,
        positions=enriched_positions + net_new,
        # Preserve the strategy's explicit totals: the model recomputes
        # ``total_value_usd`` / ``has_liquidation_risk`` from positions when
        # omitted (== 0 / == False), which would silently clobber a strategy
        # that set them explicitly. Registry-derived rows carry value_usd=0 and
        # liquidation_risk=False, so they add nothing to either total, and
        # enrichment only ever touches ``details`` — never ``value_usd`` /
        # ``liquidation_risk`` — so it cannot change either total either.
        total_value_usd=strategy_summary.total_value_usd,
        has_liquidation_risk=(strategy_summary.has_liquidation_risk or any(p.liquidation_risk for p in net_new)),
    )


def _apply_registry_lp_enrichment(
    *,
    strategy_positions: list[PositionInfo],
    registry_positions: list[PositionInfo],
    registry_keys: list[frozenset[tuple[str, ...]]],
    claimed: set[tuple[str, ...]],
    root_to_strategy_index: dict[tuple[str, ...], int],
    find: Callable[[tuple[str, ...]], tuple[str, ...]],
) -> tuple[list[PositionInfo], bool, list[PositionInfo]]:
    """Partition ``registry_positions`` into (enriched strategy rows, any_enriched, net_new).

    Extracted from :func:`reconcile_lp_with_registry` to keep that function's
    complexity in gate — this is pure partitioning, no new policy. A registry
    row whose key intersects an already-claimed root enriches the matching
    strategy position's NFT-manager authority (ALM-3428) instead of being
    discarded; every other registry row is net-new, exactly as before.
    """
    enriched_positions = list(strategy_positions)
    any_enriched = False
    net_new: list[PositionInfo] = []
    claimed = set(claimed)
    for rp, keys in zip(registry_positions, registry_keys, strict=True):
        roots = {find(k) for k in keys}
        matched = roots & claimed
        if matched:
            idx = root_to_strategy_index.get(next(iter(matched)))
            if idx is not None:
                candidate = _merge_registry_lp_authority(enriched_positions[idx], rp)
                if candidate is not enriched_positions[idx]:
                    enriched_positions[idx] = candidate
                    any_enriched = True
            continue
        net_new.append(rp)
        claimed |= roots
    return enriched_positions, any_enriched, net_new


def _merge_registry_lp_authority(strategy_position: PositionInfo, registry_position: PositionInfo) -> PositionInfo:
    """Enrich a strategy-reported LP position with the registry row's NFT-manager
    authority, additive only (ALM-3428).

    The strategy's own fields — ``value_usd``, ``protocol`` (the real connector
    slug; the registry's is only the generic ``lp``/``lp_v4`` primitive label),
    ``liquidation_risk``, everything else — are the real answer for those and are
    never touched here. Manager aliases form one authority: registry metadata
    is admitted only when the strategy supplied no manager under any alias, and
    contradictory registry aliases are left unmerged so closure remains
    unproven instead of selecting by alias order.
    """
    if strategy_position.position_type != PositionType.LP:
        return strategy_position
    registry_details = registry_position.details if isinstance(registry_position.details, dict) else {}
    if not registry_details:
        return strategy_position
    current_details = strategy_position.details if isinstance(strategy_position.details, dict) else {}
    if any(str(current_details.get(key) or "").strip() for key in _LP_MANAGER_DETAIL_KEYS):
        return strategy_position

    registry_managers = {
        str(registry_details[key]).strip().lower()
        for key in _LP_MANAGER_DETAIL_KEYS
        if registry_details.get(key) and str(registry_details[key]).strip()
    }
    if len(registry_managers) != 1:
        return strategy_position

    merged_details = dict(current_details)
    changed = False
    for key in _LP_MANAGER_DETAIL_KEYS:
        value = registry_details.get(key)
        if str(value or "").strip():
            merged_details[key] = value
            changed = True
    if not changed:
        return strategy_position
    return replace(strategy_position, details=merged_details)


def _union_residuals(
    summary: TeardownPositionSummary,
    residuals: list[PositionInfo],
) -> TeardownPositionSummary:
    """Additive union of on-chain-discovered residuals into the enumeration (VIB-5116).

    Same additive-never-subtractive contract as :func:`reconcile_lp_with_registry`:
    every strategy/registry-reported position is kept, and each residual whose
    ``(chain, position_type, position_id)`` is not already present is appended.
    Residuals carry unique identifiers (e.g. GMX order keys) so they never collapse
    with a real position, but the de-dup keeps a residual from double-surfacing if a
    future path already enumerated it. Preserves the summary's explicit totals —
    residuals carry ``value_usd=0`` and no liquidation-risk signal, so they add
    nothing to either total (valuation is out of scope for this identity surface).
    """
    if not residuals:
        return summary

    def _key(position: PositionInfo) -> tuple[str, str, str, str]:
        # Include protocol: two connectors can surface a residual with the same
        # (chain, type, position_id) shape, and keying without protocol would
        # collapse them and DROP a distinct residual (VIB-5116 C3 — the exact
        # silent-drop class this lane exists to prevent).
        return (
            str(position.chain or "").lower(),
            str(position.position_type),
            str(position.protocol or "").lower(),
            str(position.position_id),
        )

    existing_positions = list(getattr(summary, "positions", None) or [])
    seen = {_key(p) for p in existing_positions}
    net_new: list[PositionInfo] = []
    for residual in residuals:
        key = _key(residual)
        if key not in seen:
            net_new.append(residual)
            seen.add(key)
    if not net_new:
        return summary

    # Read the summary's scalar fields defensively (getattr defaults) so a partial
    # / mock summary can still carry residuals — production always passes a full
    # ``TeardownPositionSummary``, but the enumeration must never fault the lane.
    from datetime import UTC, datetime

    # Capture the summary's AUTHORITATIVE totals (already finalized by its own
    # __post_init__) before reconstruction. Residuals are an identity surface only
    # (value_usd=0, no liquidation risk), so they must not change either total.
    orig_total = getattr(summary, "total_value_usd", None)
    orig_risk = getattr(summary, "has_liquidation_risk", False)
    merged = TeardownPositionSummary(
        deployment_id=getattr(summary, "deployment_id", None) or "unknown",
        timestamp=getattr(summary, "timestamp", None) or datetime.now(UTC),
        positions=existing_positions + net_new,
        total_value_usd=orig_total if isinstance(orig_total, Decimal) else Decimal("0"),
        has_liquidation_risk=bool(orig_risk),
    )
    # Re-assert the originals: __post_init__ recomputes a MEASURED-ZERO total from
    # the position list (VIB-5116 S2), which would clobber a strategy's explicit
    # zero. Restoring here keeps the strategy's totals authoritative — the residuals
    # add nothing to either (value_usd=0 / no risk).
    if isinstance(orig_total, Decimal):
        merged.total_value_usd = orig_total
    merged.has_liquidation_risk = bool(orig_risk)
    return merged


async def resolve_open_positions_with_registry(strategy: Any) -> TeardownPositionSummary:
    """Strategy enumeration reconciled against the ``position_registry`` WARM read path.

    Calls the strategy's own ``get_open_positions()`` (its authoritative,
    primitive-complete enumeration), then reconciles the cut-over LP slice
    against the registry so a restarted runner re-derives the same open LP set
    from WARM. The registry read degrades to a no-op (legacy enumeration) on a
    backend without cutover storage.

    Errors from ``strategy.get_open_positions()`` are NOT swallowed here — the
    caller (runner / CLI) owns that policy. Registry-read errors are swallowed
    inside :func:`read_open_lp_positions_from_registry`.
    """
    deployment_id = str(getattr(strategy, "deployment_id", "") or "")
    summary = strategy.get_open_positions()
    if summary is None:
        # A custom / degraded ``get_open_positions`` may return None; preserve
        # the deployment id for downstream tracking instead of falling back to
        # the bare "unknown" sentinel inside ``reconcile_lp_with_registry``.
        summary = TeardownPositionSummary.empty(deployment_id or "unknown")
    state_manager = getattr(strategy, "_state_manager", None)
    read = await read_open_lp_positions_detailed(
        state_manager=state_manager,
        deployment_id=deployment_id,
        chain=None,
    )
    # TD-04 (VIB-5462): the lending cutover surfaces open collateral/debt legs
    # through the SAME additive-union reconcile. Read both primitive streams and
    # union them so the restart-safe re-derivation is identical across LP and
    # lending; ``available`` is True if EITHER stream answered. The completeness
    # chain-verify below is TD-05's LP-only concern and stays scoped to LP
    # (lending chain-verify is TD-09's HF-safe-unwind job, not this read path).
    lending_positions, lending_available = await read_open_lending_positions_from_registry(
        state_manager=state_manager,
        deployment_id=deployment_id,
        chain=None,
    )
    # TD-02 (VIB-5460): the perp cutover surfaces open perp positions through the
    # SAME additive-union reconcile. Read the perp stream and union it so the
    # restart-safe re-derivation is identical across LP / lending / perp;
    # ``available`` is True if ANY stream answered.
    perp_positions, perp_available = await read_open_perp_positions_from_registry(
        state_manager=state_manager,
        deployment_id=deployment_id,
        chain=None,
    )
    # TD-03 (VIB-5461): the Pendle cutover surfaces open PT/LP holdings through
    # the SAME additive-union reconcile. Read it and union so the restart-safe
    # re-derivation is identical across LP / lending / perp / Pendle; ``available``
    # is True if ANY stream answered.
    pendle_positions, pendle_available = await read_open_pendle_positions_from_registry(
        state_manager=state_manager,
        deployment_id=deployment_id,
        chain=None,
    )
    # VIB-6287: give the union the deployment's wallet so a venue that derives a
    # position identity from the account can do so. Deferred import: the manager
    # imports this module, and ``_teardown_wallet_for_chain`` is the SAME
    # per-chain resolver the teardown lane stamps intents with, so the identity
    # and the execution agree about which account owns the position.
    from almanak.framework.teardown.teardown_manager import _teardown_wallet_for_chain

    def _wallet_for_chain(chain: str) -> str | None:
        return _teardown_wallet_for_chain(strategy, chain) or None

    reconciled = reconcile_lp_with_registry(
        strategy_summary=summary,
        registry_positions=read.positions + lending_positions + perp_positions + pendle_positions,
        registry_available=read.available or lending_available or perp_available or pendle_available,
        wallet_for_chain=_wallet_for_chain,
    )
    # VIB-5116: fold in off-position on-chain residuals (e.g. GMX V2 pending
    # unfilled orders holding collateral in the OrderVault) discovered directly
    # from chain, INDEPENDENT of the strategy's get_open_positions() /
    # _loop_state. This closes the enumeration-blindness that let committed-but-
    # unfilled capital be reported as no_positions success. Additive/union like
    # the registry reads; never subtracts a strategy-reported position.
    from almanak.framework.teardown.residual_discovery import discover_teardown_residuals

    reconciled = _union_residuals(reconciled, discover_teardown_residuals(strategy))
    # TD-05 (VIB-5463): chain-verify the enumeration completeness. This NEVER
    # mutates the additive union (the union→authoritative flip is TD-06's job) —
    # it (a) upgrades the registry-read-failure path from warn-only to an active
    # per-position chain-verify of the known LP set, and (b) emits the structured
    # "registry incomplete" signal TD-06 consumes to decide when the registry can
    # be trusted (a strategy LP that is open on-chain yet absent from the registry
    # is a write-skipped / pre-cutover row, not a closed position).
    await _verify_lp_enumeration_completeness(
        strategy=strategy,
        strategy_summary=summary,
        read=read,
    )
    return reconciled


def _lp_match_keys(
    position: PositionInfo,
    unambiguous_nft_ids: frozenset[tuple[str, str]],
) -> set[tuple[str, str]]:
    """Every ``(chain, identity)`` key under which this LP may be recognised.

    The row's own identity (VIB-5723) plus, for a manager-qualified NFT whose id
    is unambiguous across the enumeration, its bare-id form (VIB-6730) — so the
    completeness check matches the same physical NFT no matter which of the two
    sources supplied the manager authority.
    """
    chain = str(position.chain or "").lower()
    keys = {(chain, _lp_identity(position))}
    keys |= {(chain, alias) for _, alias in _lp_bridge_tokens(position, unambiguous_nft_ids)}
    return keys


def _registry_open_keys(
    read: RegistryReadResult,
    unambiguous_nft_ids: frozenset[tuple[str, str]] = frozenset(),
) -> set[tuple[str, str]]:
    """``(chain, identity)`` keys for the registry-reported OPEN LP positions.

    Keyed on the source-independent LP identity (VIB-5723) so a strategy LP
    reported under the composite position-key format still matches its own
    registry row instead of logging a false "ABSENT from position_registry".
    Registry rows carry the NFT manager and strategy rows typically do not, so
    the bridge aliases are folded in too (VIB-6730) — without them a registry row
    for the very NFT the strategy just reported reads as absent, which is the
    false ABSENT this function exists to prevent.
    """
    keys: set[tuple[str, str]] = set()
    for position in read.positions:
        keys |= _lp_match_keys(position, unambiguous_nft_ids)
    return keys


async def _verify_lp_enumeration_completeness(
    *,
    strategy: Any,
    strategy_summary: TeardownPositionSummary,
    read: RegistryReadResult,
) -> None:
    """Chain-verify the LP enumeration's completeness (TD-05 / VIB-5463).

    Observation-only — it NEVER mutates the returned enumeration (the additive
    union is preserved; the authoritative flip is TD-06's). It does two things,
    both bounded to the *discrepancy* set so the common matched case issues zero
    chain reads:

    1. **Registry-read-failure verification (no longer warn-only).** When a
       primitive's registry read RAISED (``read.failed_primitives``), the
       registry answer is incomplete, so the strategy-reported LP set is the only
       known identity. Each such LP is chain-verified; a structured ERROR is
       logged when a position cannot be confirmed open, so an operator sees an
       unverified teardown instead of a silent warning.

    2. **Completeness signal for TD-06 (AC3).** When the registry WAS available
       but a strategy-reported LP is ABSENT from its OPEN rows, chain-verify it:
       if the chain confirms it is open, that row is a write-skipped / pre-cutover
       gap (the registry is not yet complete) — logged so TD-06 knows the
       union→authoritative flip is not yet safe.

    Gateway boundary: verification is gateway-routed via
    :func:`live_position_reads.chain_verify_lp_open`. A strategy without a wired
    gateway client simply skips verification (the additive union still stands).
    """
    gateway_client = getattr(strategy, "_gateway_client", None)
    if gateway_client is None:
        if read.failed_primitives:
            logger.error(
                "Teardown LP enumeration: registry read failed for %s and no gateway "
                "client is available to chain-verify the known LP set — completeness "
                "is UNVERIFIED for this teardown",
                ", ".join(read.failed_primitives),
            )
        return

    from almanak.framework.teardown.live_position_reads import chain_verify_lp_open

    strategy_lp = [p for p in strategy_summary.positions if p.position_type == PositionType.LP]
    if not strategy_lp:
        if read.failed_primitives:
            logger.error(
                "Teardown LP enumeration: registry read failed for %s and the strategy "
                "reported no LP — a forgotten LP cannot be re-derived per-position "
                "(wallet-scan recovery is the separate --discover lane); completeness "
                "is UNVERIFIED",
                ", ".join(read.failed_primitives),
            )
        return

    unambiguous_nft_ids = _unambiguous_lp_nft_ids([*read.positions, *strategy_lp])
    registry_keys = _registry_open_keys(read, unambiguous_nft_ids)
    network = str(getattr(strategy, "_gateway_network", "") or "")

    for position in strategy_lp:
        absent_from_registry = not (_lp_match_keys(position, unambiguous_nft_ids) & registry_keys)
        # Only verify the discrepancy set: a strategy LP the registry already
        # confirms (matched) needs no chain read unless its primitive's read
        # failed (registry answer incomplete for it).
        if not absent_from_registry and not read.failed_primitives:
            continue

        verdict = await chain_verify_lp_open(gateway_client=gateway_client, position=position, network=network)

        if read.failed_primitives:
            if verdict is True:
                logger.warning(
                    "Teardown LP enumeration: registry read failed (%s); LP token_id=%s "
                    "on %s CONFIRMED open on-chain — retained in the teardown set",
                    ", ".join(read.failed_primitives),
                    position.position_id,
                    position.chain,
                )
            elif verdict is None:
                logger.error(
                    "Teardown LP enumeration: registry read failed (%s) AND LP token_id=%s "
                    "on %s could not be confirmed open on-chain — completeness UNVERIFIED; "
                    "manual on-chain check advised before treating teardown as complete",
                    ", ".join(read.failed_primitives),
                    position.position_id,
                    position.chain,
                )
            # verdict is False ⇒ the position is closed on-chain; it harmlessly
            # plans a no-op close — left in the union (no subtraction here).
        elif absent_from_registry and verdict is True:
            logger.warning(
                "Teardown LP enumeration: LP token_id=%s on %s is open on-chain but ABSENT "
                "from position_registry — a write-skipped / pre-cutover row. Union retained "
                "(no position dropped); registry is not yet complete, so the "
                "union→authoritative flip (TD-06) stays blocked",
                position.position_id,
                position.chain,
            )


__all__ = [
    "RegistryReadResult",
    "read_open_lending_positions_from_registry",
    "read_open_lp_positions_detailed",
    "read_open_lp_positions_from_registry",
    "read_open_pendle_positions_from_registry",
    "read_open_perp_positions_from_registry",
    "reconcile_lp_with_registry",
    "resolve_open_positions_with_registry",
]
