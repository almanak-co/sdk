"""Atomic commit primitive for ledger + position_registry + handle (VIB-4197 / T11).

This module implements the local-SQLite half of the structural fix for bug
#2130 (stranded LP NFT). The atomic primitive ``save_ledger_and_registry``
wraps the ``transaction_ledger`` row, the ``position_registry`` row, and the
optional handle mapping in **a single SQLite transaction** — three writes
land together or none of them do. The hosted-mode equivalent (single Postgres
transaction inside the gateway) ships separately as T19 / VIB-4205.

Two-mode contract (per `blueprints/28-position-registry.md` §4.1):

- ``mode='accounting_only'`` (default): writes ``transaction_ledger`` only.
  Backwards-compatible with the legacy ``_write_ledger_entry()`` path. This is
  the mode every primitive uses today; T12+ flips primitives to
  ``mode='registry'`` one at a time.
- ``mode='registry'``: writes ledger + registry + handle in **one SQLite
  transaction**. Idempotent on ``physical_identity_hash``. Used only by
  primitives that have crossed the cutover line.

Strict cross-mode discipline: passing a ``registry`` or ``handle`` argument
while ``mode='accounting_only'`` raises :class:`ValueError`. We forbid the
silent-ignore variant — the next refactor that wires a registry row into the
wrong code path is one of the silent-failure modes the registry exists to
prevent. Explicit > implicit.

Author API note: this is a **function-level primitive** (not a class).
Calling sites compose:

    await save_ledger_and_registry(state_manager, ledger=L)                      # mode default
    await save_ledger_and_registry(state_manager, ledger=L, registry=R, mode='registry')
    await save_ledger_and_registry(
        state_manager, ledger=L, registry=R, handle=H, mode='registry',
    )

See ``blueprints/28-position-registry.md`` for the full design rationale and
``docs/internal/migration-cutover-position-registry.md`` for the per-primitive
cutover protocol.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from almanak.framework.primitives.types import AccountingCategory, Primitive

if TYPE_CHECKING:
    from almanak.framework.observability.ledger import LedgerEntry
    from almanak.framework.state.state_manager import StateManager


# =============================================================================
# DATA CLASSES
# =============================================================================


@dataclass(frozen=True)
class RegistryRow:
    """A single ``position_registry`` row.

    Mirrors the 16-column schema ratified in PRD §Registry Data Shape and
    realized in ``almanak/framework/state/backends/sqlite.py:SCHEMA_SQL``.
    Frozen so the caller cannot mutate the row after passing it to the
    primitive — every value is locked at construction time.

    Attributes:
        deployment_id: Stable deployment identifier (``ClassName:hash`` per
            ``almanak/framework/runner/identity.py``).
        chain: Chain name (``arbitrum``, ``ethereum``, …).
        primitive: ``Primitive`` enum value (``Primitive.LP``, ``Primitive.PERP``,
            …). Stored as the StrEnum's string value.
        accounting_category: ``AccountingCategory`` enum value (``LP``,
            ``PENDLE_LP``, …). Stored as the StrEnum's string value.
        physical_identity_hash: Receipt-derived stable identity. Hash over
            the per-primitive identity tuple (UniV3: ``(token_id,
            nft_manager_addr, chain)``; GMX V2: ``(market, collateral,
            is_long, account)``; etc.). MUST be a non-empty string —
            empty / whitespace-only is rejected by the primitive.
        semantic_grouping_key: Auto-mode collision predicate. UniV3:
            ``chain:pool_address``. Pendle: ``chain:market_addr:expiry_ts``.
        grouping_policy_version: Versioned grouping rule (e.g. ``"univ3_lp@v1"``).
            Bumped when the auto-mode rule changes; never silently mutated.
        handle: Optional author-supplied alias (e.g. ``"leg_a"``). The handle
            is a column on this table — there is no separate
            ``position_handles`` table. ``None`` for handle-less positions.
        status: One of ``'open'``, ``'closed'``, ``'reorg_invalidated'``.
            Pinned by a CHECK constraint in the SQLite schema.
        payload: Per-primitive JSON payload (token_id for UniV3, market+
            expiry for Pendle, etc.). Must be a JSON-serializable dict.
        opened_at_block: Block number of the opening transaction. None
            permitted for backfill-synthesized rows; production opens MUST
            populate it.
        opened_tx: Tx hash of the opening transaction. None for backfill.
        closed_at_block: Block number of the close (when status is closed
            or reorg_invalidated).
        closed_tx: Tx hash of the close.
        last_reconciled_at_block: Block at which the gateway's
            reconciliation path last verified this row's on-chain truth.
            None until the first reconciliation pass runs.
        matching_policy_version: Stamped per-primitive policy version
            sourced from
            :func:`almanak.framework.accounting.policy.MatchingPolicy.for_primitive`.
            Bumped when the lot-matching algorithm changes for the primitive.
    """

    deployment_id: str
    chain: str
    primitive: Primitive | str
    accounting_category: AccountingCategory | str
    physical_identity_hash: str
    semantic_grouping_key: str
    grouping_policy_version: str
    status: Literal["open", "closed", "reorg_invalidated"]
    payload: dict[str, Any] = field(default_factory=dict)
    matching_policy_version: int = 1
    handle: str | None = None
    opened_at_block: int | None = None
    opened_tx: str | None = None
    closed_at_block: int | None = None
    closed_tx: str | None = None
    last_reconciled_at_block: int | None = None

    def primitive_value(self) -> str:
        """Return the canonical string for the ``primitive`` column.

        Accepts both the ``Primitive`` enum and a raw string for backwards
        compat with non-typed callers; resolves through the enum to ensure
        the value is canonical.
        """
        p = self.primitive
        if isinstance(p, Primitive):
            return p.value
        # Validate raw string against the enum — invalid values raise
        # ValueError at the boundary, not silently as a typo lands in the DB.
        return Primitive(p).value

    def accounting_category_value(self) -> str:
        """Return the canonical string for the ``accounting_category`` column."""
        c = self.accounting_category
        if isinstance(c, AccountingCategory):
            return c.value
        return AccountingCategory(c).value

    def payload_json(self) -> str:
        """Serialize ``payload`` for storage.

        Strict JSON: callers must pass JSON-serializable values. Non-serializable
        values (Decimal, datetime, custom dataclasses) raise ``TypeError`` here
        rather than being silently coerced via ``str()`` — silent string coercion
        is irreversible at read time and violates the CLAUDE.md "Empty ≠ zero"
        rule (a Decimal stringified to "1.5" is no longer a measured number).
        Convert numeric/temporal values to canonical strings or floats at the
        call site before passing.
        """
        return json.dumps(self.payload, sort_keys=True)


@dataclass(frozen=True)
class HandleMapping:
    """Author-supplied handle for a position.

    The handle is a column on ``position_registry``; there is no separate
    ``position_handles`` table. This dataclass is the typed argument shape
    the primitive accepts so callers cannot accidentally pass a bare string
    in the wrong slot.

    Attributes:
        handle: The author-supplied alias (e.g. ``"leg_a"``).
        deployment_id: Must match the registry row's deployment_id.
        accounting_category: Must match the registry row's accounting_category.
            Handle uniqueness is enforced by the partial unique index
            ``ix_registry_handle`` on ``(deployment_id, accounting_category,
            handle) WHERE handle IS NOT NULL``.
    """

    handle: str
    deployment_id: str
    accounting_category: AccountingCategory | str


# =============================================================================
# PRIMITIVE
# =============================================================================


CommitMode = Literal["accounting_only", "registry", "registry_reconciliation"]


async def save_ledger_and_registry(
    state_manager: StateManager,
    *,
    ledger: LedgerEntry,
    registry: RegistryRow | None = None,
    handle: HandleMapping | None = None,
    mode: CommitMode = "accounting_only",
) -> None:
    """Atomic commit of a ledger row + (optionally) a registry row + handle.

    See module docstring for the contract. The function:

    1. Validates the input shape (mode/registry/handle consistency, identity
       hash non-empty, handle/registry alignment).
    2. Delegates to ``state_manager.save_ledger_and_registry(...)`` which
       wraps the SQLite-backend transactional method.
    3. Surfaces backend failures as
       :class:`almanak.framework.state.exceptions.AccountingPersistenceError`
       (typed) so the runner's existing fail-closed pipeline (VIB-3157 /
       VIB-3762) handles it without further work.

    Args:
        state_manager: An initialized ``StateManager`` (or
            ``GatewayStateManager`` — see hosted-mode T19 ticket for the
            gRPC implementation).
        ledger: The ``LedgerEntry`` to persist. Always required — the ledger
            row is the always-on accounting record.
        registry: A ``RegistryRow`` to persist atomically with the ledger.
            REQUIRED when ``mode='registry'``; MUST be ``None`` when
            ``mode='accounting_only'``.
        handle: An optional ``HandleMapping`` to record alongside the registry
            row. Valid only when ``mode='registry'``.
        mode: ``'accounting_only'`` (default — ledger only, legacy behaviour)
            or ``'registry'`` (atomic ledger + registry + handle).

    Raises:
        ValueError: On any input-shape violation.
        AccountingPersistenceError: When the backend write fails.
    """
    _validate_inputs(ledger=ledger, registry=registry, handle=handle, mode=mode)

    if mode == "accounting_only":
        # Backwards-compatible path. The SQLite backend's existing
        # save_ledger_entry already lands the row in a single statement;
        # we wrap it for a uniform call shape so callers can switch modes
        # via a single arg flip when their primitive cuts over.
        await state_manager.save_ledger_entry(ledger)
        return

    # mode in ('registry', 'registry_reconciliation'). _validate_inputs
    # guaranteed registry is non-None for both branches; the assert narrows
    # the type for the typechecker.
    assert registry is not None  # noqa: S101 — type narrowing post-validation
    # T24 / VIB-4210: function-level mode 'registry_reconciliation' routes
    # through the storage layer's same-named mode (which SKIPS the ledger
    # write atomically). Function-level mode 'registry' uses the storage
    # layer's default three-write path; we build the kwargs dict so the
    # final call site stays a SINGLE state_manager.save_ledger_and_registry
    # invocation (preserves the single-delegation invariant enforced by
    # tests/unit/state/test_position_registry_no_writers.py::test_layer_b_
    # commit_py_delegation_shape — bug #2130 split-commit guard).
    storage_kwargs: dict[str, Any] = {
        "ledger": ledger,
        "registry": registry,
        "handle": handle,
    }
    if mode == "registry_reconciliation":
        storage_kwargs["mode"] = mode
    await state_manager.save_ledger_and_registry(**storage_kwargs)


def _validate_inputs(
    *,
    ledger: LedgerEntry,
    registry: RegistryRow | None,
    handle: HandleMapping | None,
    mode: CommitMode,
) -> None:
    """Strict input validation BEFORE we open a transaction.

    Per UAT card §D3.F4, malformed input MUST raise BEFORE any DB write so
    the failure cannot strand a partial row. The validation is mode-aware:

    - accounting_only: registry MUST be None; handle MUST be None.
    - registry: registry MUST be non-None and have a non-empty
      ``physical_identity_hash``; handle (if supplied) MUST point at the
      same deployment + accounting_category as the registry row.

    Raises:
        ValueError: On any inconsistency. Message names the offending arg.
    """
    if mode not in ("accounting_only", "registry", "registry_reconciliation"):
        raise ValueError(f"mode must be 'accounting_only', 'registry', or 'registry_reconciliation', got {mode!r}")

    if mode == "accounting_only":
        if registry is not None:
            raise ValueError(
                "save_ledger_and_registry(mode='accounting_only') forbids the "
                "'registry' argument; pass mode='registry' or omit the "
                "registry row. (Strict cross-mode discipline: silent ignore "
                "would let a future refactor wire a registry row into the "
                "wrong code path.)"
            )
        if handle is not None:
            raise ValueError(
                "save_ledger_and_registry(mode='accounting_only') forbids the "
                "'handle' argument; handles are valid only in registry mode."
            )
        return

    # mode in ('registry', 'registry_reconciliation')
    if registry is None:
        raise ValueError(f"save_ledger_and_registry(mode={mode!r}) requires the 'registry' argument.")
    pih = registry.physical_identity_hash or ""
    if not pih.strip():
        raise ValueError(
            "RegistryRow.physical_identity_hash must be a non-empty, "
            "non-whitespace string. (The hash is the durable identity key; "
            "the SQLite primary key constraint would otherwise admit "
            "ambiguous rows.)"
        )
    if registry.handle is not None and not registry.handle.strip():
        raise ValueError(
            "RegistryRow.handle must be None or a non-empty, non-whitespace "
            "string. (Empty/whitespace handles bypass the partial unique index "
            "ix_registry_handle — two open positions with handle='' would "
            "silently collide. Pass None to omit, or a real alias.)"
        )
    if handle is not None:
        if not handle.handle or not handle.handle.strip():
            raise ValueError("HandleMapping.handle must be a non-empty, non-whitespace string.")
        if handle.deployment_id != registry.deployment_id:
            raise ValueError(
                "HandleMapping.deployment_id must match RegistryRow.deployment_id "
                f"(handle={handle.deployment_id!r}, registry={registry.deployment_id!r})."
            )
        h_cat = (
            handle.accounting_category.value
            if isinstance(handle.accounting_category, AccountingCategory)
            else AccountingCategory(handle.accounting_category).value
        )
        r_cat = registry.accounting_category_value()
        if h_cat != r_cat:
            raise ValueError(
                "HandleMapping.accounting_category must match "
                "RegistryRow.accounting_category "
                f"(handle={h_cat!r}, registry={r_cat!r})."
            )
        if registry.handle is not None and registry.handle != handle.handle:
            raise ValueError(
                "RegistryRow.handle and HandleMapping.handle disagree "
                f"({registry.handle!r} vs {handle.handle!r}). The registry "
                "row's handle column is the canonical home; pass the handle "
                "in either argument, but not different values in both."
            )


__all__ = [
    "RegistryRow",
    "HandleMapping",
    "save_ledger_and_registry",
    "CommitMode",
]
