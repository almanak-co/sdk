"""Per-primitive ``position_registry`` backfill — VIB-4198 / T12.

Implements the cutover-spec contract from
``docs/internal/migration-cutover-position-registry.md`` §3 (idempotent
backfill from ``position_events``) for the **UniV3 LP** primitive cutover.

Hard contract:

- Backfill is **idempotent on ``physical_identity_hash``** (§3.4). A
  SIGKILL between batches followed by a clean restart produces a final
  registry state bit-identical to a single uninterrupted run.
- Identity tuple for UniV3 LP is ``(chain, nft_manager_addr.lower(),
  token_id)`` per T08 invariant #1 — **receipt-derivable only**, no
  off-chain RPC, no clock.
- The OPEN/CLOSE fold within a ``position_id`` group is **commutative**:
  presence of any CLOSE event ⇒ ``status='closed'``, otherwise
  ``status='open'``. Independent of event-arrival order so the fold is
  deterministic under restart even when two events share a timestamp.
- Backfill writes use ``INSERT OR IGNORE`` keyed by the registry's primary
  key. Existing rows are NOT overwritten — runtime maintenance (status
  flips on CLOSE) goes through the live ``save_ledger_and_registry``
  primitive (T11), not the backfill.

Boot-guard contract (cutover spec §2.2):

- ``RegistryCutoverNotDeployedError`` — the build's registry primitive is
  enabled but no cutover ticket has populated the migration_state row.
  The runner is on a stale deployment.
- ``BackfillFailedError`` — the inline backfill raised mid-run. The runner
  exits non-zero; operator restarts and the next start picks up where
  this run left.
- ``RegistryBackfillIncompleteError`` — defensive: backfill returned
  cleanly but the writer didn't flip ``complete=1``. Programmer error.

Failure semantics in **all modes** (live / paper / dry_run): backfill
failures HALT the runner. This is **stricter** than VIB-3762's general
"log+continue" rule for paper/dry_run, on purpose: a half-finished
backfill produces a corrupt-by-construction registry state. Operator
intervention is required.
"""

from __future__ import annotations

import hashlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

from almanak.framework.accounting.commit import RegistryRow
from almanak.framework.accounting.policy import MatchingPolicy
from almanak.framework.intents.compiler_constants import (
    AAVE_COMPATIBLE_PROTOCOLS,
    PANCAKESWAP_V3_NFT_POSITION_MANAGERS,
    SLIPSTREAM_NFT_POSITION_MANAGERS,
    UNIV3_LP_GROUPING_PROTOCOLS,
    UNIV3_NFT_POSITION_MANAGERS,
    UNIV4_LP_GROUPING_PROTOCOLS,
    UNIV4_NFT_POSITION_MANAGERS,
)
from almanak.framework.primitives.types import AccountingCategory, Primitive

if TYPE_CHECKING:
    from almanak.framework.state.state_manager import StateManager


logger = logging.getLogger(__name__)


# =============================================================================
# Exceptions
# =============================================================================


class RegistryCutoverNotDeployedError(RuntimeError):
    """No ``migration_state`` row exists for ``(deployment_id, primitive,
    cutover_key)``.

    The cutover ticket creates the row at deploy time with ``complete=0``.
    A missing row at runner startup means the build's registry-mode
    dispatch is enabled but the deployment is stale (the cutover ticket
    has not run its inline migration yet, or the row was deleted by an
    operator). The runner halts so the operator can investigate.
    """

    def __init__(self, deployment_id: str, primitive: Primitive, cutover_key: str) -> None:
        super().__init__(
            f"No migration_state row for "
            f"(deployment_id={deployment_id!r}, primitive={primitive.value!r}, "
            f"cutover_key={cutover_key!r}). The cutover ticket must populate "
            "this row at deploy time. Refusing to enter registry mode."
        )
        self.deployment_id = deployment_id
        self.primitive = primitive
        self.cutover_key = cutover_key


class BackfillFailedError(RuntimeError):
    """The inline backfill raised before ``complete=1`` could be set.

    Wraps the underlying exception so the runner's ``AccountingPersistenceError``
    surface stays focused on accounting writes (per VIB-3762). The runner
    exits non-zero on this exception; restart picks up where the previous
    run left off thanks to the ``DO NOTHING`` semantic on the registry
    INSERT.
    """


class RegistryBackfillIncompleteError(RuntimeError):
    """Defensive: backfill returned cleanly but ``complete`` is still 0.

    Programmer error in the writer (the ``mark_backfill_complete`` call
    is missing or the transaction was rolled back). The runner halts loud
    so the bug surfaces immediately rather than silently re-running the
    full backfill on every iteration.
    """

    def __init__(
        self,
        *,
        deployment_id: str,
        primitive: Primitive,
        cutover_key: str,
        rows_synthesized: int,
    ) -> None:
        super().__init__(
            f"Backfill returned cleanly for (deployment_id={deployment_id!r}, "
            f"primitive={primitive.value!r}, cutover_key={cutover_key!r}) but "
            f"position_registry_backfill_complete is still 0. "
            f"rows_synthesized={rows_synthesized}. "
            "This is a writer bug — investigate before re-launching."
        )


class RegistryLookupInstallError(RuntimeError):
    """Registry-lookup install / cache prime failed while the cutover is
    active.

    Raised by the runner-side bootstrap (``_run_loop_helpers``) when
    ``is_cutover_active(runner, primitive, key)`` is True but the
    registry-id cache prime (``_refresh_lp_registry_id_cache``) or the
    callback installation could not complete. Post-cutover the registry
    is the source of truth for token_id resolution; silently degrading
    to the legacy in-memory tracker would re-introduce the D3.F6 silent-
    error class — after a restart, the tracker is exactly the surface
    that has lost the open-position state.

    The runner halts on this exception. The operator either fixes the
    underlying state-manager / DB problem and restarts, OR — if the
    decision is to roll back the cutover — flips the cutover off
    explicitly. There is no third silent-degrade option.
    """

    def __init__(self, deployment_id: str, primitive: Primitive, cutover_key: str, cause: str) -> None:
        # Audit F1 (T30 anti-bypass guard): the routing-token regex
        # treats any literal "LPPositionTracker" in folded-string nodes
        # as a routing read. This is an exception MESSAGE, not a code
        # path — the class isn't referenced, only described — but the
        # guard correctly flags it as too-easy to misread. Reword to
        # use neutral language ("the legacy in-memory tracker fallback")
        # so the silent-degrade prohibition stays clear without
        # tripping the static guard.
        super().__init__(
            f"Registry-lookup install failed while cutover is active for "
            f"(deployment_id={deployment_id!r}, primitive={primitive.value!r}, "
            f"cutover_key={cutover_key!r}). Cause: {cause}. "
            "Refusing to fall back to the legacy in-memory tracker fallback — "
            "that would re-introduce the D3.F6 silent-error class post-restart. "
            "Fix the underlying state-manager / DB problem and restart, "
            "or roll back the cutover explicitly."
        )
        self.deployment_id = deployment_id
        self.primitive = primitive
        self.cutover_key = cutover_key
        self.cause = cause


class CutoverStorageNotSupported(RuntimeError):
    """The state-manager backend does not implement the cutover storage
    accessors (``upsert_migration_state``, ``get_position_registry_open_rows``,
    etc.).

    Raised by ``StateManager`` delegate methods when the WARM backend is
    a hosted/gateway-backed manager that has not yet shipped the
    Postgres equivalent of the registry tables (T19 / VIB-4205 owns
    that landing). The cutover boot guard catches this and degrades to
    ``accounting_only`` mode for the affected primitive — a controlled,
    observable degradation rather than a silent ``[]`` / ``None`` /
    ``False`` swallow.

    The local-SQLite backend always implements the full surface.
    """


# =============================================================================
# Dataclasses
# =============================================================================


@dataclass(frozen=True)
class MigrationStateRow:
    """Read shape of a ``migration_state`` row.

    Mirrors the SQLite columns from
    ``almanak/framework/state/backends/sqlite.py`` SCHEMA_SQL. ``notes``
    is exposed as the parsed dict (the SQLite column is JSON-validated;
    callers of this dataclass should not need to re-parse the raw text).
    """

    deployment_id: str
    primitive: str
    cutover_key: str
    position_registry_backfill_complete: bool
    backfill_started_at: str | None
    backfill_completed_at: str | None
    backfill_source_table: str
    backfill_reader_version: int
    rows_synthesized: int
    rows_skipped_already_present: int
    notes: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""


@dataclass(frozen=True)
class BackfillReport:
    """Summary of a single backfill driver-loop run.

    Returned by :meth:`BackfillReader.run`. Operator-facing — the runner
    logs the report at INFO level and emits a structured metric. Not
    used as a correctness invariant by the boot guard (the truth is the
    ``position_registry_backfill_complete`` flag in ``migration_state``).
    """

    deployment_id: str
    primitive: str
    cutover_key: str
    rows_synthesized: int
    rows_skipped_already_present: int
    started_at: str
    completed_at: str
    already_complete: bool = False

    @classmethod
    def already_complete_for(
        cls, *, deployment_id: str, primitive: str, cutover_key: str, completed_at: str
    ) -> BackfillReport:
        """Construct a no-op report for a re-run after ``complete=1``."""
        return cls(
            deployment_id=deployment_id,
            primitive=primitive,
            cutover_key=cutover_key,
            rows_synthesized=0,
            rows_skipped_already_present=0,
            started_at=completed_at,
            completed_at=completed_at,
            already_complete=True,
        )


# =============================================================================
# UniV3 identity helpers — receipt-fact-only
# =============================================================================


def physical_identity_hash_univ3(*, chain: str, nft_manager_addr: str, token_id: int | str) -> str:
    """Compute the canonical UniV3 LP ``physical_identity_hash``.

    Inputs are the receipt-derivable identity tuple per T08 invariant #1:

        seed = f"{chain}:{nft_manager_addr.lower()}:{token_id}"
        hash = "0x" + sha256(seed.encode()).hexdigest()

    Matches the loader test in
    ``tests/unit/multi_position_tracking/test_l1_goldens_univ3.py`` and
    the stored hash in ``tests/fixtures/multi-position-tracking/
    univ3-arbitrum/lp_open/expected_registry_row.json``.

    Args:
        chain: Lowercase chain name (``arbitrum``, ``ethereum``, …).
        nft_manager_addr: Hex address of the NonfungiblePositionManager
            (or its fork). Folded to lowercase before hashing.
        token_id: NFT tokenId — accepts ``int`` or stringified-int.

    Returns:
        ``0x``-prefixed lowercase 64-char hex digest.

    Raises:
        ValueError: ``chain`` empty, ``nft_manager_addr`` empty, or
            ``token_id`` not coercible to ``int`` or ``<=0``.
    """
    chain_norm = (chain or "").strip().lower()
    if not chain_norm:
        raise ValueError("physical_identity_hash_univ3: chain must be non-empty")
    addr_norm = (nft_manager_addr or "").strip().lower()
    if not addr_norm:
        raise ValueError("physical_identity_hash_univ3: nft_manager_addr must be non-empty")
    try:
        token_id_int = int(token_id)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"physical_identity_hash_univ3: token_id must be int-coercible, got {token_id!r}") from exc
    if token_id_int <= 0:
        raise ValueError(f"physical_identity_hash_univ3: token_id must be >0, got {token_id_int}")
    seed = f"{chain_norm}:{addr_norm}:{token_id_int}"
    return "0x" + hashlib.sha256(seed.encode()).hexdigest()


def semantic_grouping_key_univ3(*, chain: str, pool_address: str) -> str:
    """Compute the UniV3 LP ``semantic_grouping_key``.

    Per PRD §Registry Data Shape and the T08 fixture:

        f"{chain}:{pool_address.lower()}"

    The auto-mode collision predicate runs against this key; two NFTs in
    the same pool collide here and the partial unique index
    ``ix_registry_auto_mode`` forces the strategy author to supply a
    ``registry_handle`` to disambiguate (or backfill synthesizes a
    deterministic ``__legacy_…`` handle per cutover spec §3.4.1).
    """
    chain_norm = (chain or "").strip().lower()
    if not chain_norm:
        raise ValueError("semantic_grouping_key_univ3: chain must be non-empty")
    pool_norm = (pool_address or "").strip().lower()
    if not pool_norm:
        raise ValueError("semantic_grouping_key_univ3: pool_address must be non-empty")
    return f"{chain_norm}:{pool_norm}"


# =============================================================================
# UniV4 identity helpers — receipt-fact-only (VIB-4583)
# =============================================================================


def physical_identity_hash_univ4(*, chain: str, position_manager_addr: str, token_id: int | str) -> str:
    """Compute the canonical UniV4 LP ``physical_identity_hash``.

    Mirrors :func:`physical_identity_hash_univ3`'s seed-and-sha256 shape,
    swapping the identity tuple to the V4 PositionManager + NFT tokenId
    (VIB-4583 design §2.1):

        seed = f"{chain}:{position_manager_addr.lower()}:{token_id}"
        hash = "0x" + sha256(seed.encode()).hexdigest()

    The tokenId is the 1:1 lifetime identity of *this* position (same tokenId on
    OPEN and CLOSE). ``pool_id`` is shared by every position in a V4 pool, so it
    belongs in the *semantic grouping key* (:func:`semantic_grouping_key_univ4`),
    not the physical hash — exactly the V3 split.

    Args:
        chain: Lowercase chain name (``base``, ``ethereum``, …).
        position_manager_addr: Hex address of the V4 ``PositionManager`` for the
            chain. Folded to lowercase before hashing.
        token_id: NFT tokenId — accepts ``int`` or stringified-int.

    Returns:
        ``0x``-prefixed lowercase 64-char hex digest.

    Raises:
        ValueError: ``chain`` empty, ``position_manager_addr`` empty, or
            ``token_id`` not coercible to ``int`` / ``<=0``.
    """
    chain_norm = (chain or "").strip().lower()
    if not chain_norm:
        raise ValueError("physical_identity_hash_univ4: chain must be non-empty")
    addr_norm = (position_manager_addr or "").strip().lower()
    if not addr_norm:
        raise ValueError("physical_identity_hash_univ4: position_manager_addr must be non-empty")
    try:
        token_id_int = int(token_id)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"physical_identity_hash_univ4: token_id must be int-coercible, got {token_id!r}") from exc
    if token_id_int <= 0:
        raise ValueError(f"physical_identity_hash_univ4: token_id must be >0, got {token_id_int}")
    seed = f"{chain_norm}:{addr_norm}:{token_id_int}"
    return "0x" + hashlib.sha256(seed.encode()).hexdigest()


def semantic_grouping_key_univ4(*, chain: str, pool_id: str) -> str:
    """Compute the UniV4 LP ``semantic_grouping_key`` (VIB-4583 design §2.2).

        f"{chain}:{pool_id.lower()}"

    ``pool_id`` (the 32-byte V4 PoolKey hash) is the natural V4 analogue of V3's
    ``pool_address`` — V4 pools have no per-pool contract address (the
    ``PoolManager`` is a singleton). Two positions in the same V4 pool group
    here; the partial unique index ``ix_registry_auto_mode`` then forces handle
    disambiguation, exactly as for V3.
    """
    chain_norm = (chain or "").strip().lower()
    if not chain_norm:
        raise ValueError("semantic_grouping_key_univ4: chain must be non-empty")
    pool_norm = (pool_id or "").strip().lower()
    if not pool_norm:
        raise ValueError("semantic_grouping_key_univ4: pool_id must be non-empty")
    return f"{chain_norm}:{pool_norm}"


# =============================================================================
# Lending identity helpers — receipt/intent-fact-only (TD-04 / VIB-5462)
# =============================================================================
#
# A money-market position has no NFT, so its identity is the on-chain
# *(market, leg)* tuple, not a tokenId. The same shape generalises across the
# whole lending family the live-prod teardown bugs hit (Aave canonical; Spark /
# Fluid / Morpho / Compound follow): every protocol exposes a per-market
# discriminator (Aave/Spark: the reserve asset; Morpho Blue: the isolated
# market id; Compound v3: the comet/base asset) and partitions value into a
# COLLATERAL (supply) and a DEBT (borrow) leg. The registry encodes those two
# axes — ``market_id`` (opaque string) and ``leg`` — so downstream HF-safe
# unwind (TD-09) reads every ``debt`` leg to repay and every ``collateral`` leg
# to withdraw without re-deriving a protocol-specific key.

# The two canonical lending legs. ``leg`` is part of the physical identity so a
# supply and a borrow of the SAME asset (degenerate, but legal on Aave) never
# collapse onto one registry row.
_LENDING_LEG_COLLATERAL = "collateral"
_LENDING_LEG_DEBT = "debt"

# Map the durable ``position_events.position_type`` (and the taxonomy
# ``PositionKind``) lending values onto the registry leg. The runtime path maps
# intent type → leg via the canonical taxonomy (SUPPLY/WITHDRAW → collateral,
# BORROW/REPAY → debt); the backfill maps the persisted ``position_type``. Both
# funnel through this single table so the two paths can never disagree.
_LENDING_POSITION_TYPE_TO_LEG: dict[str, str] = {
    "LENDING_COLLATERAL": _LENDING_LEG_COLLATERAL,
    "LENDING_DEBT": _LENDING_LEG_DEBT,
}


def lending_leg_for_position_type(position_type: str) -> str | None:
    """Map a lending ``position_type`` to its registry leg, or ``None``.

    ``None`` for any non-lending position type — the caller treats that as
    "not this cutover" and skips the row (never fabricates a leg).
    """
    return _LENDING_POSITION_TYPE_TO_LEG.get((position_type or "").strip().upper())


def lending_registry_market_id(*, market_id: str | None, token: str) -> str:
    """Canonical per-market discriminator for a lending registry row.

    Protocol-agnostic by construction:

    * Isolated-market protocols (Morpho Blue) carry an explicit ``market_id`` on
      the intent — that id IS the discriminator.
    * Unified-pool protocols (Aave V3 / Spark / Compound v3 base asset) leave
      ``market_id`` unset; the supplied/borrowed ``token`` (the reserve) is the
      discriminator.

    The runtime path passes ``(intent.market_id, intent.token)``; the backfill
    passes ``(None, position_events.token0)`` (the asset symbol the legacy
    writer persisted). Both lowercase to one canonical string so a runtime
    UPSERT lands on the same ``physical_identity_hash`` the backfill synthesized.

    Raises ``ValueError`` when neither input yields a non-empty anchor — per
    CLAUDE.md "Empty ≠ Zero" we never hash a fabricated/blank market.
    """
    raw = (market_id or "").strip() or (token or "").strip()
    norm = raw.lower()
    if not norm:
        raise ValueError("lending_registry_market_id: market_id and token both empty")
    return norm


def physical_identity_hash_lending(*, chain: str, protocol: str, market_id: str, leg: str) -> str:
    """Compute the canonical lending ``physical_identity_hash``.

    Identity tuple (no config — blueprint 28 §6.6) is the on-chain
    *(chain, protocol, market, leg)*::

        seed = f"{chain}:{protocol}:{market_id.lower()}:{leg}"
        hash = "0x" + sha256(seed.encode()).hexdigest()

    A lending leg is a *singleton* per ``(chain, protocol, market, leg)`` — a
    second SUPPLY to the same reserve increases the aToken balance, it does not
    mint a new position — so the physical hash and the semantic grouping key
    share the same tuple (unlike LP, where many NFTs share one pool). The
    auto-mode unique index then keeps exactly one OPEN row per leg and a re-open
    UPSERTs it idempotently.

    Raises ``ValueError`` on any empty anchor or an unknown ``leg``.
    """
    chain_norm = (chain or "").strip().lower()
    if not chain_norm:
        raise ValueError("physical_identity_hash_lending: chain must be non-empty")
    protocol_norm = (protocol or "").strip().lower()
    if not protocol_norm:
        raise ValueError("physical_identity_hash_lending: protocol must be non-empty")
    market_norm = (market_id or "").strip().lower()
    if not market_norm:
        raise ValueError("physical_identity_hash_lending: market_id must be non-empty")
    leg_norm = (leg or "").strip().lower()
    if leg_norm not in (_LENDING_LEG_COLLATERAL, _LENDING_LEG_DEBT):
        raise ValueError(f"physical_identity_hash_lending: leg must be collateral/debt, got {leg!r}")
    seed = f"{chain_norm}:{protocol_norm}:{market_norm}:{leg_norm}"
    return "0x" + hashlib.sha256(seed.encode()).hexdigest()


def semantic_grouping_key_lending(*, chain: str, protocol: str, market_id: str, leg: str) -> str:
    """Compute the lending ``semantic_grouping_key``.

        f"{chain}:{protocol}:{market_id.lower()}:{leg}"

    Equal to the physical hash's seed (sans sha256): a lending leg is a
    singleton, so the auto-mode collision predicate is "one open row per
    ``(chain, protocol, market, leg)``" — exactly the desired idempotent
    re-supply / re-borrow behaviour. Raises ``ValueError`` on empty anchors or
    an unknown ``leg``.
    """
    chain_norm = (chain or "").strip().lower()
    if not chain_norm:
        raise ValueError("semantic_grouping_key_lending: chain must be non-empty")
    protocol_norm = (protocol or "").strip().lower()
    if not protocol_norm:
        raise ValueError("semantic_grouping_key_lending: protocol must be non-empty")
    market_norm = (market_id or "").strip().lower()
    if not market_norm:
        raise ValueError("semantic_grouping_key_lending: market_id must be non-empty")
    leg_norm = (leg or "").strip().lower()
    if leg_norm not in (_LENDING_LEG_COLLATERAL, _LENDING_LEG_DEBT):
        raise ValueError(f"semantic_grouping_key_lending: leg must be collateral/debt, got {leg!r}")
    return f"{chain_norm}:{protocol_norm}:{market_norm}:{leg_norm}"


# =============================================================================
# UniV3 OPEN/CLOSE fold over `position_events`
# =============================================================================


def fold_position_events_for_univ3(  # noqa: C901 — explicit identity-anchor checks; refactor would lose the load-bearing per-field skip-with-warning behavior per cutover spec §3.5
    *, deployment_id: str, group: list[dict[str, Any]]
) -> RegistryRow | None:
    """Fold a ``(deployment_id, position_id)`` group of UniV3 LP
    ``position_events`` rows into a single ``RegistryRow``.

    Cutover spec §3.5: "presence of any CLOSE event ⇒ status='closed';
    otherwise 'open'". The fold is COMMUTATIVE within the group — order
    of arrival does not affect the final shape. We DO read the OPEN event
    for identity / payload extraction (its receipt facts are required)
    and the CLOSE event (when present) for the close-side anchors.

    Returns ``None`` for:

    - Groups whose protocol is NOT in the UniV3 LP family (the caller
      filters; this is defense-in-depth).
    - Groups missing an OPEN event (corrupt leftover; do NOT synthesize a
      phantom OPEN).
    - Groups whose OPEN event is missing the load-bearing identity fields
      (token_id, pool address). Per CLAUDE.md "Empty ≠ zero" — a missing
      anchor stays missing; we do not substitute zero.

    Args:
        deployment_id: The strategy's stable deployment id.
        group: Ordered list of ``position_events`` row dicts (as returned
            by ``state_manager.get_position_events``) sharing the same
            ``position_id``. Order is non-load-bearing (fold is
            commutative) but the caller typically streams sorted.

    Returns:
        :class:`RegistryRow` ready for atomic-primitive insert, or
        ``None`` when the group does not match this cutover or lacks
        identity anchors.
    """
    if not group:
        return None

    open_ev: dict[str, Any] | None = None
    close_ev: dict[str, Any] | None = None
    for ev in group:
        et = (ev.get("event_type") or "").upper()
        if et == "OPEN" and open_ev is None:
            open_ev = ev
        elif et == "CLOSE":
            # Last CLOSE wins for close-side anchors when there are
            # multiple closes recorded (e.g. partial close + final close).
            close_ev = ev

    if open_ev is None:
        # Pathological: SNAPSHOT or CLOSE without OPEN. Log + skip; do
        # not synthesize a phantom OPEN.
        logger.warning(
            "Backfill: position_events group %s has no OPEN event; skipping",
            group[0].get("position_id", "<unknown>"),
        )
        return None

    protocol = (open_ev.get("protocol") or "").lower()
    if protocol not in _UNIV3_LP_PROTOCOLS:
        return None

    chain = (open_ev.get("chain") or "").lower()
    if not chain:
        logger.warning(
            "Backfill: UniV3 OPEN event %s missing chain; skipping",
            open_ev.get("position_id", "<unknown>"),
        )
        return None

    nft_manager_addr = _nft_manager_for_chain(chain)
    if not nft_manager_addr:
        logger.warning(
            "Backfill: no canonical NFT manager known for chain %r; skipping group %s",
            chain,
            open_ev.get("position_id", "<unknown>"),
        )
        return None

    # position_id on UniV3 LP rows IS the NFT tokenId. The legacy writer
    # stores it as a string in position_events.position_id. Empty/zero
    # means the parser couldn't identify the NFT — refuse to synthesize.
    raw_token_id = open_ev.get("position_id")
    try:
        token_id = int(raw_token_id) if raw_token_id is not None else 0
    except (TypeError, ValueError):
        logger.warning(
            "Backfill: UniV3 OPEN event has non-int position_id %r; skipping",
            raw_token_id,
        )
        return None
    if token_id <= 0:
        logger.warning(
            "Backfill: UniV3 OPEN event has non-positive position_id %r; skipping",
            token_id,
        )
        return None

    # Pool address — position_events does not carry pool_address as a
    # column today. We accept whatever the row's enrichment captured
    # under ``token0`` / ``token1`` and the convention that the position
    # was opened on a UniV3-style pool. When pool_address is missing we
    # cannot compute the semantic_grouping_key reliably, so we skip the
    # row with a structured warning. This is the documented "best-effort"
    # path for legacy data — non-LP protocols stay tracker-driven until
    # their own cutover.
    pool_address = _extract_pool_address_from_legacy_event(open_ev)
    if not pool_address:
        logger.warning(
            "Backfill: UniV3 OPEN event for token_id %s has no derivable "
            "pool_address (legacy row predates pool capture); skipping. "
            "The runtime path will pick this position up via "
            "PositionService.discover_positions on the next cycle.",
            token_id,
        )
        return None

    pih = physical_identity_hash_univ3(
        chain=chain,
        nft_manager_addr=nft_manager_addr,
        token_id=token_id,
    )
    sgk = semantic_grouping_key_univ3(chain=chain, pool_address=pool_address)
    status: Literal["open", "closed", "reorg_invalidated"] = "closed" if close_ev is not None else "open"

    payload: dict[str, Any] = {
        "token_id": str(token_id),
        "pool_address": pool_address.lower(),
        "nft_manager_addr": nft_manager_addr,
        # Persist legacy_position_id under a reserved payload key so the
        # rollback re-entry path (cutover spec §5.3.1) has a deterministic
        # bridge from the registry's hash-keyed shape back to
        # position_events.position_id. The key is a contract; future
        # rollback code reads it.
        "legacy_position_id": str(raw_token_id),
        # Mark backfilled rows so post-cutover audits can distinguish
        # them from runtime-emitted rows (PRD §"Synthesized vs author").
        "synthesized_handle": False,
        "source": "backfill",
    }
    # Best-effort range / liquidity carry. position_events has these as
    # columns when the parser captured them at OPEN time.
    for src, dst in (("tick_lower", "tick_lower"), ("tick_upper", "tick_upper"), ("liquidity", "liquidity")):
        v = open_ev.get(src)
        if v is not None and v != "":
            payload[dst] = v
    for src, dst in (("amount0", "amount0"), ("amount1", "amount1")):
        v = open_ev.get(src)
        if v is not None and v != "":
            payload[dst] = v

    opened_tx = open_ev.get("tx_hash") or None
    closed_tx = (close_ev or {}).get("tx_hash") if close_ev is not None else None

    return RegistryRow(
        deployment_id=deployment_id,
        chain=chain,
        primitive=Primitive.LP,
        accounting_category=AccountingCategory.LP,
        physical_identity_hash=pih,
        semantic_grouping_key=sgk,
        grouping_policy_version=_UNIV3_GROUPING_POLICY_VERSION,
        handle=None,
        status=status,
        payload=payload,
        opened_at_block=None,
        opened_tx=opened_tx,
        closed_at_block=None,
        closed_tx=closed_tx,
        last_reconciled_at_block=None,
        matching_policy_version=MatchingPolicy.for_primitive(Primitive.LP),
    )


# =============================================================================
# UniV4 OPEN/CLOSE fold over `position_events` (VIB-4583)
# =============================================================================


def fold_position_events_for_univ4(  # noqa: C901 — explicit identity-anchor checks; the per-field skip-with-warning behavior is load-bearing (design §5), refactor would lose it
    *, deployment_id: str, group: list[dict[str, Any]]
) -> RegistryRow | None:
    """Fold a ``(deployment_id, position_id)`` group of UniV4 LP
    ``position_events`` rows into a single ``RegistryRow`` (VIB-4583 design §5).

    Structural twin of :func:`fold_position_events_for_univ3`, with the V4
    identity split: the physical hash keys on the per-chain V4
    ``PositionManager`` + NFT tokenId, and the semantic grouping key on the
    V4 ``pool_id`` (PoolKey hash) — V4 pools have no per-pool contract address.

    Returns ``None`` (skip, with a structured WARN) for:

    - Empty groups / groups missing an OPEN event (no phantom OPEN synthesis).
    - Groups whose protocol is NOT in the UniV4 LP family (defense-in-depth —
      the caller's ``matches_this_cutover`` already filters).
    - Groups missing chain, a known V4 PositionManager for the chain
      (``v4_registry_no_position_manager`` — fail-closed, never fabricate),
      a positive int ``position_id`` (tokenId), or a derivable ``pool_id``.

    Per CLAUDE.md "Empty ≠ Zero", a missing identity anchor stays missing; we
    never substitute zero or a fabricated address (a fabricated PositionManager
    or zero tokenId would corrupt ``physical_identity_hash_univ4``).
    """
    if not group:
        return None

    open_ev: dict[str, Any] | None = None
    close_ev: dict[str, Any] | None = None
    for ev in group:
        et = (ev.get("event_type") or "").upper()
        if et == "OPEN" and open_ev is None:
            open_ev = ev
        elif et == "CLOSE":
            # Last CLOSE wins for close-side anchors (commutative fold; design §5).
            close_ev = ev

    if open_ev is None:
        logger.warning(
            "Backfill: UniV4 position_events group %s has no OPEN event; skipping (v4_backfill_no_open)",
            group[0].get("position_id", "<unknown>"),
        )
        return None

    protocol = (open_ev.get("protocol") or "").lower()
    if protocol not in _UNIV4_LP_PROTOCOLS:
        return None

    chain = (open_ev.get("chain") or "").lower()
    if not chain:
        logger.warning(
            "Backfill: UniV4 OPEN event %s missing chain; skipping (v4_backfill_incomplete_open)",
            open_ev.get("position_id", "<unknown>"),
        )
        return None

    position_manager_addr = _position_manager_for_univ4_chain(chain)
    if not position_manager_addr:
        # Fail-closed: no V4 PositionManager known for this chain. NEVER
        # fabricate an address (it would poison physical_identity_hash_univ4).
        logger.warning(
            "Backfill: no V4 PositionManager known for chain %r; skipping group %s "
            "(v4_registry_no_position_manager deployment_id=%s)",
            chain,
            open_ev.get("position_id", "<unknown>"),
            deployment_id,
        )
        return None

    # position_id on V4 LP rows IS the NFT tokenId (threaded verbatim by the
    # strategy). Empty/zero/non-int means the parser couldn't identify the NFT.
    raw_token_id = open_ev.get("position_id")
    try:
        token_id = int(raw_token_id) if raw_token_id is not None and raw_token_id != "" else 0
    except (TypeError, ValueError):
        logger.warning(
            "Backfill: UniV4 OPEN event has non-int position_id %r; skipping (v4_backfill_incomplete_open)",
            raw_token_id,
        )
        return None
    if token_id <= 0:
        logger.warning(
            "Backfill: UniV4 OPEN event has non-positive position_id %r; skipping (v4_backfill_incomplete_open)",
            token_id,
        )
        return None

    # The V4 pool_id (66-char PoolKey hash) is what the V4 receipt parser
    # persists under ``pool_address`` on the event. Missing it ⇒ we cannot
    # compute the semantic_grouping_key; skip rather than emit a partial row.
    pool_id = _extract_pool_address_from_legacy_event(open_ev)
    if not pool_id:
        logger.warning(
            "Backfill: UniV4 OPEN event for token_id %s has no derivable pool_id "
            "(legacy row predates pool capture); skipping (v4_backfill_incomplete_open)",
            token_id,
        )
        return None

    pih = physical_identity_hash_univ4(
        chain=chain,
        position_manager_addr=position_manager_addr,
        token_id=token_id,
    )
    sgk = semantic_grouping_key_univ4(chain=chain, pool_id=pool_id)
    status: Literal["open", "closed", "reorg_invalidated"] = "closed" if close_ev is not None else "open"

    payload: dict[str, Any] = {
        # V4 identity tuple — serialized into the existing opaque payload JSON
        # column (NO new schema column; design §4). pool_id replaces V3's
        # pool_address; position_manager replaces V3's nft_manager_addr.
        "token_id": str(token_id),
        "pool_id": pool_id.lower(),
        "position_manager": position_manager_addr,
        "legacy_position_id": str(raw_token_id),
        "synthesized_handle": False,
        "source": "backfill",
    }
    # Best-effort range / liquidity / amount carry (Empty ≠ Zero: only copy
    # values the parser actually captured).
    for src, dst in (("tick_lower", "tick_lower"), ("tick_upper", "tick_upper"), ("liquidity", "liquidity")):
        v = open_ev.get(src)
        if v is not None and v != "":
            payload[dst] = v
    for src, dst in (("amount0", "amount0"), ("amount1", "amount1")):
        v = open_ev.get(src)
        if v is not None and v != "":
            payload[dst] = v

    opened_tx = open_ev.get("tx_hash") or None
    closed_tx = (close_ev or {}).get("tx_hash") if close_ev is not None else None

    return RegistryRow(
        deployment_id=deployment_id,
        chain=chain,
        primitive=Primitive.LP_V4,
        accounting_category=AccountingCategory.LP,
        physical_identity_hash=pih,
        semantic_grouping_key=sgk,
        grouping_policy_version=_UNIV4_GROUPING_POLICY_VERSION,
        handle=None,
        status=status,
        payload=payload,
        opened_at_block=None,
        opened_tx=opened_tx,
        closed_at_block=None,
        closed_tx=closed_tx,
        last_reconciled_at_block=None,
        matching_policy_version=MatchingPolicy.for_primitive(Primitive.LP_V4),
    )


# =============================================================================
# Lending OPEN/CLOSE fold over `position_events` (TD-04 / VIB-5462)
# =============================================================================


def _resolve_lending_lifecycle(group: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Last-state-wins fold over a leg's chronological event group.

    Returns ``(open_ev, close_ev)``. A lending *(market, leg)* identity is REUSED
    across open→close→reopen cycles (unlike an LP NFT, which mints a fresh
    token_id per lifecycle), so the FINAL chronological state decides open vs
    closed — a CLOSE followed by a later SUPPLY/BORROW is an OPEN leg. OPEN/
    INCREASE start/continue an active period (clearing any prior close); a genuine
    CLOSE (FULL exit) terminates it; a DECREASE (partial withdraw/repay) leaves
    the leg open (closing it would strand the residual on a wiped restart). A
    non-None ``close_ev`` ⇒ the leg is currently closed.
    """
    open_ev: dict[str, Any] | None = None
    close_ev: dict[str, Any] | None = None
    for ev in sorted(group, key=lambda e: e.get("timestamp") or ""):
        et = (ev.get("event_type") or "").upper()
        if et in ("OPEN", "INCREASE"):
            open_ev = ev
            close_ev = None  # a new active period supersedes any prior close
        elif et == "CLOSE":
            close_ev = ev
        elif et == "DECREASE" and open_ev is None:
            # Defensive: a DECREASE with no preceding OPEN in the group (legacy
            # rows pre-dating the OPEN seeder) still anchors an open leg.
            open_ev = ev
    return open_ev, close_ev


def fold_position_events_for_lending(*, deployment_id: str, group: list[dict[str, Any]]) -> RegistryRow | None:
    """Fold a leg-homogeneous group of lending ``position_events`` into a ``RegistryRow``.

    The driver's group key includes ``position_type`` (see :meth:`BackfillReader.run`),
    so every event in ``group`` shares one leg (COLLATERAL or DEBT) — a SUPPLY and a
    BORROW of the same asset land in separate groups even though they share a
    ``position_id``. Cutover spec §3.5: presence of any CLOSE ⇒ ``status='closed'``,
    else ``'open'``. WITHDRAW/REPAY seed CLOSE; SUPPLY/BORROW seed OPEN (the legacy
    lending position-event seeder, ``observability/position_events.py``).

    Returns ``None`` (skip, structured WARN) for:

    * empty groups / groups whose protocol is not in the lending cutover set
      (defense-in-depth; the reader's ``matches_this_cutover`` already filters);
    * groups missing chain, a resolvable leg, or the asset anchor — per
      CLAUDE.md "Empty ≠ Zero" a missing anchor stays missing, never zero-filled.
    """
    if not group:
        return None

    # Last-state-wins lifecycle fold (see :func:`_resolve_lending_lifecycle`):
    # a lending leg is a REUSED identity, so the final chronological state — not
    # "any CLOSE ever" — decides open vs closed.
    open_ev, close_ev = _resolve_lending_lifecycle(group)
    anchor_ev = close_ev if close_ev is not None else open_ev
    if anchor_ev is None:
        logger.warning(
            "Backfill: lending position_events group %s has no usable event; skipping",
            group[0].get("position_id", "<unknown>"),
        )
        return None

    protocol = (anchor_ev.get("protocol") or "").lower()
    if protocol not in _LENDING_REGISTRY_PROTOCOLS:
        return None

    chain = (anchor_ev.get("chain") or "").lower()
    if not chain:
        logger.warning(
            "Backfill: lending event %s missing chain; skipping",
            anchor_ev.get("position_id", "<unknown>"),
        )
        return None

    leg = lending_leg_for_position_type(str(anchor_ev.get("position_type") or ""))
    if leg is None:
        logger.warning(
            "Backfill: lending event %s has non-lending position_type %r; skipping",
            anchor_ev.get("position_id", "<unknown>"),
            anchor_ev.get("position_type"),
        )
        return None

    # The legacy writer persists the reserve asset symbol on ``token0`` for the
    # lending leg. Unified-pool protocols (the only ones currently in the cutover
    # set) carry no isolated ``market_id`` column, so the asset IS the market —
    # identical to the runtime path's ``intent.market_id or intent.token``.
    asset = anchor_ev.get("token0")
    try:
        market_id = lending_registry_market_id(market_id=None, token=str(asset or ""))
    except ValueError:
        logger.warning(
            "Backfill: lending %s event for %s has no derivable asset/market; skipping",
            leg,
            anchor_ev.get("position_id", "<unknown>"),
        )
        return None

    pih = physical_identity_hash_lending(chain=chain, protocol=protocol, market_id=market_id, leg=leg)
    sgk = semantic_grouping_key_lending(chain=chain, protocol=protocol, market_id=market_id, leg=leg)
    status: Literal["open", "closed", "reorg_invalidated"] = "closed" if close_ev is not None else "open"

    payload: dict[str, Any] = {
        "protocol": protocol,
        "market_id": market_id,
        "leg": leg,
        "asset": str(asset) if asset is not None and asset != "" else None,
        "legacy_position_id": str(anchor_ev.get("position_id") or ""),
        "synthesized_handle": False,
        "source": "backfill",
    }
    # Best-effort principal carry (Empty ≠ Zero: only copy what the parser
    # actually captured for the leg).
    leg_amount = anchor_ev.get("amount0")
    if leg_amount is not None and leg_amount != "":
        payload["amount"] = leg_amount

    opened_tx = (open_ev or {}).get("tx_hash") or None
    closed_tx = (close_ev or {}).get("tx_hash") if close_ev is not None else None

    return RegistryRow(
        deployment_id=deployment_id,
        chain=chain,
        primitive=Primitive.LENDING,
        accounting_category=AccountingCategory.LENDING,
        physical_identity_hash=pih,
        semantic_grouping_key=sgk,
        grouping_policy_version=_LENDING_GROUPING_POLICY_VERSION,
        handle=None,
        status=status,
        payload=payload,
        opened_at_block=None,
        opened_tx=opened_tx,
        closed_at_block=None,
        closed_tx=closed_tx,
        last_reconciled_at_block=None,
        matching_policy_version=MatchingPolicy.for_primitive(Primitive.LENDING),
    )


# =============================================================================
# Module-level constants
# =============================================================================


# Protocols using the Uniswap-V3-shape LP grouping policy. VIB-4928
# (PR-3b): derived from each UniV3-shape DEX connector's
# ``protocol_family.PROTOCOL_FAMILY`` (``UNIV3_LP_GROUPING`` family)
# declaration via the framework's ``compiler_constants`` aggregation point
# (``PROTOCOL_FAMILY_REGISTRY``) — no longer a hardcoded list of protocol slugs
# in this module. Byte-equivalent to the pre-VIB-4864 frozenset.
_UNIV3_LP_PROTOCOLS: frozenset[str] = UNIV3_LP_GROUPING_PROTOCOLS


_UNIV3_GROUPING_POLICY_VERSION: str = "univ3_lp@v1"


# VIB-4583: Uniswap-V4 singleton-PoolManager LP family. Derived from the
# connector-self-registering ``PROTOCOL_FAMILY_REGISTRY`` (``UNIV4_LP_GROUPING``
# family) — kept SEPARATE from ``_UNIV3_LP_PROTOCOLS`` so the two grouping-policy
# versions stay independent (a V4 grouping-rule change never re-baselines V3).
_UNIV4_LP_PROTOCOLS: frozenset[str] = UNIV4_LP_GROUPING_PROTOCOLS


_UNIV4_GROUPING_POLICY_VERSION: str = "univ4_lp@v1"


# TD-04 (VIB-5462): lending family cutover. Bound to the connector-self-declared
# Aave-compatible lending family (``ProtocolFamily.AAVE_V3`` →
# ``AAVE_COMPATIBLE_PROTOCOLS``), exactly as ``_UNIV3_LP_PROTOCOLS`` binds to the
# LP grouping family — so there is NO protocol-name string literal in framework
# code (chain/protocol coupling ratchet) and enabling a new Aave-fork lending
# protocol (Spark / Fluid-as-Aave) is a one-line family declaration in that
# connector's ``protocol_family.py``, NOT a reshape: the registry row shape
# (market_id + leg + protocol) is already protocol-agnostic. Today the family is
# {aave_v3}. Isolated-market protocols (Morpho Blue) / single-base (Compound v3)
# are not Aave-ABI-compatible and join via their own enablement, but the SHAPE
# already supports them (opaque ``market_id``; proven generalisable by the Spark
# fold test). The grouping-policy version is independent of the LP families so a
# lending grouping-rule change never re-baselines LP rows.
_LENDING_REGISTRY_PROTOCOLS: frozenset[str] = AAVE_COMPATIBLE_PROTOCOLS


_LENDING_GROUPING_POLICY_VERSION: str = "lending@v1"


def _assign_synthesized_handles(rows: list[RegistryRow]) -> list[RegistryRow]:
    """Assign deterministic ``__legacy_…`` handles to colliding open
    handle-less rows.

    Per cutover spec §3.4.1: when two-or-more handle-less open rows
    share a ``(deployment_id, chain, accounting_category,
    semantic_grouping_key)`` bucket, the first (by
    ``physical_identity_hash`` ASC) keeps ``handle=None`` and every
    other row gets:

        handle = f"__legacy_{primitive.value}_{physical_identity_hash[:8]}"

    Closed rows in the same bucket are unaffected — the partial unique
    index ``ix_registry_auto_mode`` filters by ``status='open'`` so
    closed rows never collide.

    The returned list preserves input order so the caller's reporting
    counters stay consistent. Rows are returned with the synthesized
    handle materialized (``RegistryRow`` is frozen — we re-construct).
    """
    # Bucket open handle-less rows by collision key.
    buckets: dict[tuple[str, str, str, str], list[RegistryRow]] = {}
    for row in rows:
        if row.status != "open" or row.handle is not None:
            continue
        key = (
            row.deployment_id,
            row.chain,
            row.accounting_category_value(),
            row.semantic_grouping_key,
        )
        buckets.setdefault(key, []).append(row)

    # For buckets with >1 row, sort by physical_identity_hash and
    # synthesize handles for [1:].
    rows_with_handle: dict[int, str] = {}  # id(row) → handle
    for bucket_rows in buckets.values():
        if len(bucket_rows) <= 1:
            continue
        bucket_rows_sorted = sorted(bucket_rows, key=lambda r: r.physical_identity_hash)
        # First row keeps handle=None (default). Subsequent rows get a
        # deterministic synthesized handle.
        for r in bucket_rows_sorted[1:]:
            primitive_str = r.primitive_value()
            short = r.physical_identity_hash.removeprefix("0x")[:8]
            rows_with_handle[id(r)] = f"__legacy_{primitive_str}_{short}"

    if not rows_with_handle:
        return rows

    # Re-construct rows that need a synthesized handle. Frozen dataclasses
    # → use dataclasses.replace for the in-place equivalent.
    from dataclasses import replace

    out: list[RegistryRow] = []
    for row in rows:
        h = rows_with_handle.get(id(row))
        if h is None:
            out.append(row)
            continue
        new_payload = dict(row.payload)
        new_payload["synthesized_handle"] = True
        out.append(replace(row, handle=h, payload=new_payload))
    return out


def _nft_manager_for_chain(chain: str) -> str | None:
    """Look up the canonical UniV3 NPM address for ``chain``, or ``None``.

    Audit F2 (CI EIP-55 invariant): the previous implementation kept a
    parallel ``_NFT_MANAGER_BY_CHAIN`` dict in this module with
    lowercased addresses. Two problems with that:

    1. Lowercase addresses fail the EIP-55 checksum invariant
       enforced by ``tests/unit/core/test_eip55_checksum.py``.
    2. A second copy of the same chain → NPM map is a drift hazard
       — the connector's published addresses already carry the
       EIP-55-correct values and stay in sync as new chains land.

    The canonical UniV3-family NPM map is derived in
    ``almanak.framework.intents.compiler_constants`` from each
    connector's self-contained ``addresses.py`` (W1 / VIB-4853). VIB-4864
    (W2-followup) routes this lookup through that derived view instead of
    reaching into the connector's ``receipt_parser`` module — the address
    is value-bearing (the emitter component of an LP position's
    ``physical_identity_hash``), so the view is byte-equivalent to the
    pre-VIB-4864 parser map (curated chain subset, ``bnb`` alias, and the
    Agni-Finance overlay on Mantle all preserved). Returning ``None`` on
    an unrecognized chain keeps the backfill from silently synthesizing a
    row with a fabricated address (Empty ≠ zero, per the broader cutover
    rule).

    For protocol-aware lookup (Slipstream forks and PancakeSwap V3 ship
    their own NPM at a different address than canonical UniV3 on the
    same chain), use :func:`_nft_manager_for_protocol_chain` instead.
    """
    return UNIV3_NFT_POSITION_MANAGERS.get((chain or "").strip().lower())


# Per-protocol NPM-address maps for forks that deploy their own
# NonfungiblePositionManager at a different address than canonical Uniswap
# V3 on the same chain. VIB-4864 (W2-followup): these derived views live in
# ``compiler_constants`` and are sourced from each connector's
# self-contained ``addresses.py`` (W1 / VIB-4853) — the migration backfill
# no longer imports connector ``receipt_parser`` modules. Each view is
# byte-equivalent to the parser map it replaces (lowercased addresses,
# ``bnb`` alias preserved). Protocols absent from this map fall through to
# the canonical UniV3-family lookup (``uniswap_v3`` / ``sushiswap_v3`` share
# the canonical NPM on the chains they support today).
_NPM_ADDRESSES_BY_PROTOCOL: dict[str, dict[str, str]] = {
    "aerodrome_slipstream": SLIPSTREAM_NFT_POSITION_MANAGERS,
    "velodrome_slipstream": SLIPSTREAM_NFT_POSITION_MANAGERS,
    "pancakeswap_v3": PANCAKESWAP_V3_NFT_POSITION_MANAGERS,
}


def _nft_manager_for_protocol_chain(protocol: str, chain: str) -> str | None:
    """Look up the canonical NPM address for ``(protocol, chain)``.

    Several UniV3-family forks (Aerodrome Slipstream on Base, Velodrome
    Slipstream on Optimism, PancakeSwap V3 on its supported chains) deploy
    their own NonfungiblePositionManager at a different address than
    canonical Uniswap V3 on the same chain. Routing through the UniV3 map
    (``_nft_manager_for_chain``) would silently corrupt the
    ``physical_identity_hash`` tuple — the hash input would not match the
    on-chain NPM emitter, and ``position_registry`` lookups would
    consistently miss (VIB-4305).

    - Forks with their own NPM (``aerodrome_slipstream`` /
      ``velodrome_slipstream`` → Slipstream ``cl_nft``; ``pancakeswap_v3``
      → PancakeSwap V3 ``nft``) resolve through
      :data:`_NPM_ADDRESSES_BY_PROTOCOL`, whose entries are the derived
      views in ``compiler_constants`` built from each connector's
      ``addresses.py`` (W1 / VIB-4853).
    - Uniswap V4 (``protocol in _UNIV4_LP_PROTOCOLS``) → its own per-chain
      singleton ``PositionManager`` from :func:`_position_manager_for_univ4_chain`
      (VIB-4583). V4 LP positions hash on the PositionManager + tokenId, NOT a
      V3 NPM, so this branch is required for the V4 ``physical_identity_hash`` to
      match the on-chain emitter. Capability-gated on the registry-derived
      family set — no protocol-name string literal.
    - Anything else (``uniswap_v3`` / ``sushiswap_v3`` / unrecognized /
      empty) → delegate to :func:`_nft_manager_for_chain` (UniV3 family
      lookup; Sushi V3 uses the same NPM as Uniswap V3 on the chains it
      supports today).

    Returns ``None`` (NOT ``""``) on unrecognized chains, so the caller
    can distinguish "no NPM registered" from "NPM is the empty string".
    """
    chain_norm = (chain or "").strip().lower()
    protocol_norm = (protocol or "").strip().lower()
    # VIB-4583: V4 resolves through its own PositionManager view (membership
    # checked against the registry-derived family set, not a name literal).
    if protocol_norm in _UNIV4_LP_PROTOCOLS:
        return _position_manager_for_univ4_chain(chain_norm)
    fork_map = _NPM_ADDRESSES_BY_PROTOCOL.get(protocol_norm)
    if fork_map is not None:
        return fork_map.get(chain_norm) or None
    return _nft_manager_for_chain(chain_norm)


def _position_manager_for_univ4_chain(chain: str) -> str | None:
    """Look up the Uniswap V4 ``PositionManager`` address for ``chain`` (VIB-4583).

    Sourced from the ``UNIV4_NFT_POSITION_MANAGERS`` derived view in
    ``compiler_constants`` (built from ``uniswap_v4/addresses.py`` via the
    contract-role registry). Returns ``None`` (NOT ``""``) when the chain is
    absent from ``UNISWAP_V4`` or its entry lacks a ``position_manager`` — the
    caller (live runner / backfill) treats that as a fail-closed skip: emit a
    structured WARN and continue, NEVER fabricate a PositionManager (a fabricated
    address would poison ``physical_identity_hash_univ4``). The chain becomes
    registry-eligible the moment its address lands in ``addresses.py`` — no
    migration, no backfill gate (design §3.1).
    """
    return UNIV4_NFT_POSITION_MANAGERS.get((chain or "").strip().lower()) or None


def _extract_pool_address_from_legacy_event(ev: dict[str, Any]) -> str | None:
    """Recover ``pool_address`` from a legacy ``position_events`` row.

    Legacy rows never carried pool_address as a top-level column; the
    parser used to embed it in ``attribution_json`` for some protocols
    and on the row's payload extraction path for others. We try the
    standard places in order and return the first non-empty hit.
    """
    pool = ev.get("pool_address") or ev.get("pool")
    if isinstance(pool, str) and pool.startswith("0x"):
        return pool.lower()

    attribution_json = ev.get("attribution_json")
    if isinstance(attribution_json, str) and attribution_json:
        # Best-effort JSON parse — failures are silent because legacy
        # rows have inconsistent shapes and we don't want a parse error
        # in one row to abort the whole backfill.
        import json as _json

        try:
            attribution = _json.loads(attribution_json)
        except _json.JSONDecodeError:
            attribution = None
        if isinstance(attribution, dict):
            for key in ("pool_address", "pool"):
                v = attribution.get(key)
                if isinstance(v, str) and v.startswith("0x"):
                    return v.lower()
    return None


# =============================================================================
# BackfillReader
# =============================================================================


class BackfillReader(ABC):
    """Idempotent reader of ``position_events`` that emits ``position_registry`` rows.

    Per cutover spec §3.2, each per-primitive cutover subclasses this and
    implements the receipt-fact extraction hooks. T12 ships
    :class:`UniV3LPCutoverReader` (below). T16 / T23 / T28 add their own
    subclasses; the driver loop and idempotency invariants stay in the
    base class.

    The driver loop is **inline at runner startup** per cutover spec §3.7
    — there is no separate ``almanak strat migrate`` CLI in this PR. A
    future variant may add it; the reader's API does not preclude it.
    """

    primitive: Primitive
    accounting_category: AccountingCategory
    cutover_key: str
    grouping_policy_version: str
    legacy_position_types: frozenset[str]

    def __init__(self, state_manager: StateManager) -> None:
        self._state_manager = state_manager

    @abstractmethod
    def matches_this_cutover(self, ev: dict[str, Any]) -> bool:
        """Return True iff the ``position_events`` row belongs to THIS cutover.

        UniV3 cutover matches ``position_type='LP' AND protocol IN
        ('uniswap_v3', ...)``. Other cutovers narrow on different
        primitives. Required for the streaming filter so we don't emit
        rows for other primitives.
        """

    @abstractmethod
    def fold_group_to_registry_row(self, *, deployment_id: str, group: list[dict[str, Any]]) -> RegistryRow | None:
        """Per-primitive fold: ``group`` of position_events → ``RegistryRow``.

        Implements §3.5 of the cutover spec. Subclass returns ``None`` to
        skip a group (different primitive, missing identity anchors).
        """

    async def run(self, *, deployment_id: str, batch_size: int = 500) -> BackfillReport:
        """Driver loop. Runs the §3.3 contract:

        1. Read or create the migration_state row. If complete=1, return
           an :meth:`already_complete_for` report.
        2. Stream position_events for this deployment, filter by
           ``matches_this_cutover``, group by position_id.
        3. Per group, fold to a :class:`RegistryRow` and INSERT OR IGNORE.
        4. Final ``mark_backfill_complete`` with counters.

        Failure: any exception during the driver loop wraps in
        :class:`BackfillFailedError`. The migration_state flag stays at
        0; restart re-runs the read but ``DO NOTHING`` makes the inserts
        idempotent.

        Audit m4 (CodeRabbit): the wrap is uniform across setup,
        mid-loop, and completion phases. Three classes of exception
        propagate AS-IS rather than getting wrapped — they're
        already-structured errors the boot guard and operator tooling
        rely on:

        - :class:`RegistryCutoverNotDeployedError` — "row missing,
          fix deploy".
        - :class:`CutoverStorageNotSupported` — "this backend cannot
          host cutover storage; degrade".
        - :class:`BackfillFailedError` — already wrapped (e.g. by a
          recursive call); don't double-wrap.
        """
        sm = self._state_manager
        now = datetime.now(UTC).isoformat()
        rows_synthesized = 0
        rows_skipped = 0
        state: MigrationStateRow | None = None
        try:
            # 1) ensure migration_state row exists
            await sm.upsert_migration_state(
                deployment_id=deployment_id,
                primitive=self.primitive.value,
                cutover_key=self.cutover_key,
            )
            state = await sm.get_migration_state(
                deployment_id=deployment_id,
                primitive=self.primitive.value,
                cutover_key=self.cutover_key,
            )
            if state is None:  # defensive — upsert returned without producing a row
                raise RegistryCutoverNotDeployedError(deployment_id, self.primitive, self.cutover_key)
            if state.position_registry_backfill_complete:
                logger.info(
                    "Backfill: already complete for (%s, %s, %s); no-op",
                    deployment_id,
                    self.primitive.value,
                    self.cutover_key,
                )
                return BackfillReport.already_complete_for(
                    deployment_id=deployment_id,
                    primitive=self.primitive.value,
                    cutover_key=self.cutover_key,
                    completed_at=state.backfill_completed_at or now,
                )

            # Stamp backfill_started_at on first run.
            if state.backfill_started_at is None:
                await sm.update_migration_state(
                    deployment_id=deployment_id,
                    primitive=self.primitive.value,
                    cutover_key=self.cutover_key,
                    backfill_started_at=now,
                )

            # 2) read all position_events rows for this deployment + filter to our
            # legacy position_type set, then group by position_id.
            events = await sm.get_position_events_filtered(
                deployment_id=deployment_id,
                position_types=self.legacy_position_types,
            )
            # Group preserving the relative order from get_position_events_filtered.
            # Order does NOT affect correctness (fold is commutative) but a stable
            # grouping makes test comparison deterministic.
            #
            # Audit P2 (Codex): include (chain, protocol) in the group key.
            # The same NFT ``token_id`` value can appear on multiple chains
            # (different NPM contracts mint independent token_id sequences)
            # and on different NPM-family protocols on the same chain
            # (uniswap_v3 / sushiswap_v3 / pancakeswap_v3 / aerodrome_slipstream
            # / velodrome_slipstream all mint NFT positions). Grouping by
            # only ``position_id`` would merge those into one fold, where
            # any CLOSE event from one chain/protocol could incorrectly
            # close the row built from the other group's OPEN — leaving
            # only one synthesized registry row instead of one per physical
            # identity. The group key now matches
            # ``physical_identity_hash``'s identity tuple.
            # TD-04 (VIB-5462): the group key also carries ``position_type``. For
            # LP this is a NO-OP — every position_event of an LP position_id is
            # ``position_type='LP'`` (the type is derived from the intent and LP
            # intents only mint LP), so the partition is byte-identical. It is
            # load-bearing for LENDING: a SUPPLY (COLLATERAL leg) and a BORROW
            # (DEBT leg) of the SAME asset share one ``position_id`` but are two
            # distinct registry rows; without ``position_type`` in the key the
            # fold would collapse them and silently drop one leg.
            grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
            for ev in events:
                if not self.matches_this_cutover(ev):
                    continue
                pos_id = str(ev.get("position_id") or "")
                if not pos_id:
                    continue
                ev_chain = str(ev.get("chain") or "").lower()
                ev_protocol = str(ev.get("protocol") or "").lower()
                ev_position_type = str(ev.get("position_type") or "").upper()
                grouped.setdefault((pos_id, ev_chain, ev_protocol, ev_position_type), []).append(ev)

            # 3) fold + (synthesize collision-resolving handles per
            # cutover spec §3.4.1) + INSERT OR IGNORE.
            #
            # Two-pass implementation: first build all rows for the
            # group, then sort within each (deployment_id, chain,
            # accounting_category, semantic_grouping_key) bucket by
            # physical_identity_hash ASC and synthesize deterministic
            # `__legacy_…` handles for the SECOND-onwards open
            # handle-less rows. The first row in each bucket keeps
            # handle=None.
            folded: list[RegistryRow] = []
            for _, group in grouped.items():
                row = self.fold_group_to_registry_row(deployment_id=deployment_id, group=group)
                if row is None:
                    continue
                folded.append(row)
            collision_resolved = _assign_synthesized_handles(folded)
            for row in collision_resolved:
                inserted = await sm.insert_position_registry_row_if_absent(row=row)
                if inserted:
                    rows_synthesized += 1
                else:
                    rows_skipped += 1
                if (rows_synthesized + rows_skipped) % batch_size == 0:
                    await sm.update_migration_state(
                        deployment_id=deployment_id,
                        primitive=self.primitive.value,
                        cutover_key=self.cutover_key,
                        rows_synthesized=rows_synthesized,
                        rows_skipped_already_present=rows_skipped,
                    )

            # 4) Final mark complete (inside the try so its failure is
            # wrapped uniformly with setup + mid-loop failures).
            completed_at = datetime.now(UTC).isoformat()
            await sm.mark_backfill_complete(
                deployment_id=deployment_id,
                primitive=self.primitive.value,
                cutover_key=self.cutover_key,
                rows_synthesized=rows_synthesized,
                rows_skipped_already_present=rows_skipped,
                backfill_completed_at=completed_at,
            )
        except (
            BackfillFailedError,
            RegistryCutoverNotDeployedError,
            CutoverStorageNotSupported,
        ):
            # Already-structured errors propagate AS-IS — they encode
            # specific operator recovery paths that the boot guard /
            # tooling consumes. Don't double-wrap.
            raise
        except Exception as exc:
            logger.error(
                "Backfill failed for (%s, %s, %s) after %d synthesized + %d skipped",
                deployment_id,
                self.primitive.value,
                self.cutover_key,
                rows_synthesized,
                rows_skipped,
            )
            raise BackfillFailedError(
                f"Backfill failed for "
                f"(deployment_id={deployment_id!r}, primitive={self.primitive.value!r}, "
                f"cutover_key={self.cutover_key!r}): {type(exc).__name__}: {exc}"
            ) from exc

        logger.info(
            "Backfill complete for (%s, %s, %s): %d synthesized + %d skipped",
            deployment_id,
            self.primitive.value,
            self.cutover_key,
            rows_synthesized,
            rows_skipped,
        )
        # ``state`` is guaranteed non-None here (we raised
        # RegistryCutoverNotDeployedError otherwise), but the type
        # checker can't see through the long control flow — guard the
        # ``backfill_started_at`` access defensively.
        started_at = state.backfill_started_at if state is not None else now
        return BackfillReport(
            deployment_id=deployment_id,
            primitive=self.primitive.value,
            cutover_key=self.cutover_key,
            rows_synthesized=rows_synthesized,
            rows_skipped_already_present=rows_skipped,
            started_at=started_at or now,
            completed_at=completed_at,
        )


class UniV3LPCutoverReader(BackfillReader):
    """T12 (VIB-4198) — UniV3 LP per-primitive cutover backfill reader."""

    primitive = Primitive.LP
    accounting_category = AccountingCategory.LP
    cutover_key = "lp"
    grouping_policy_version = _UNIV3_GROUPING_POLICY_VERSION
    legacy_position_types = frozenset({"LP"})

    def matches_this_cutover(self, ev: dict[str, Any]) -> bool:
        if (ev.get("position_type") or "").upper() != "LP":
            return False
        protocol = (ev.get("protocol") or "").lower()
        return protocol in _UNIV3_LP_PROTOCOLS

    def fold_group_to_registry_row(self, *, deployment_id: str, group: list[dict[str, Any]]) -> RegistryRow | None:
        return fold_position_events_for_univ3(deployment_id=deployment_id, group=group)


class UniV4LPCutoverReader(BackfillReader):
    """VIB-4583 — UniV4 LP per-primitive cutover backfill reader.

    Sibling of :class:`UniV3LPCutoverReader`. Both read ``position_type='LP'``
    events but ``matches_this_cutover`` narrows on the UniV4 family, so a V4 row
    NEVER folds through the V3 reader (and vice-versa) and the two
    grouping-policy versions stay independent. The V4 reader keys its own
    ``migration_state`` row (``primitive='lp_v4'``, ``cutover_key='lp_v4'``), so
    its backfill-complete flag is tracked separately from the V3 cutover.
    """

    primitive = Primitive.LP_V4
    accounting_category = AccountingCategory.LP
    cutover_key = "lp_v4"
    grouping_policy_version = _UNIV4_GROUPING_POLICY_VERSION
    legacy_position_types = frozenset({"LP"})

    def matches_this_cutover(self, ev: dict[str, Any]) -> bool:
        if (ev.get("position_type") or "").upper() != "LP":
            return False
        protocol = (ev.get("protocol") or "").lower()
        return protocol in _UNIV4_LP_PROTOCOLS

    def fold_group_to_registry_row(self, *, deployment_id: str, group: list[dict[str, Any]]) -> RegistryRow | None:
        return fold_position_events_for_univ4(deployment_id=deployment_id, group=group)


class LendingCutoverReader(BackfillReader):
    """TD-04 (VIB-5462) — lending per-primitive cutover backfill reader.

    Aave V3 is the canonical first protocol; the reader narrows on
    :data:`_LENDING_REGISTRY_PROTOCOLS` so enabling Spark / Fluid / Morpho /
    Compound later is a one-line frozenset add, not a reshape. Reads both lending
    legs (``LENDING_COLLATERAL`` + ``LENDING_DEBT``); the driver's
    ``position_type``-aware grouping keeps the two legs as independent registry
    rows. Keys its own ``migration_state`` row (``primitive='lending'``,
    ``cutover_key='lending'``) so its backfill-complete flag is tracked
    separately from the LP cutovers.
    """

    primitive = Primitive.LENDING
    accounting_category = AccountingCategory.LENDING
    cutover_key = "lending"
    grouping_policy_version = _LENDING_GROUPING_POLICY_VERSION
    legacy_position_types = frozenset({"LENDING_COLLATERAL", "LENDING_DEBT"})

    def matches_this_cutover(self, ev: dict[str, Any]) -> bool:
        if lending_leg_for_position_type(str(ev.get("position_type") or "")) is None:
            return False
        protocol = (ev.get("protocol") or "").lower()
        return protocol in _LENDING_REGISTRY_PROTOCOLS

    def fold_group_to_registry_row(self, *, deployment_id: str, group: list[dict[str, Any]]) -> RegistryRow | None:
        return fold_position_events_for_lending(deployment_id=deployment_id, group=group)
