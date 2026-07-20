"""Neutral enum tier for the primitives taxonomy.

This module is intentionally thin and dependency-free. It declares the
enums and the :class:`PrimitiveRecord` dataclass that the taxonomy table
uses, and **must not grow beyond that**.

Hard Ratification Condition #3 (VIB-4159, 2026-05-08): this module forbids
growing beyond enums + ``PrimitiveRecord``. Adding behaviour, mappings, or
any code that imports from ``accounting/``, ``intents/``, ``observability/``,
or any other framework subpackage re-introduces the import cycle that the
taxonomy module exists to break. If new behaviour is required, place it in
``primitives/taxonomy.py`` (the consumer-facing API layer) or in the
appropriate downstream module — never here.

The forbidden imports are enforced by a static AST test in
``tests/unit/primitives/test_types.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Primitive(StrEnum):
    """Top-level DeFi primitive families.

    A primitive describes the *kind* of action a strategy is taking. It is
    coarser than ``IntentType`` (e.g. ``LP_OPEN``, ``LP_CLOSE``,
    ``LP_COLLECT_FEES`` all roll up to ``Primitive.LP``).

    ``CDP`` and ``LIQUIDATION`` are split from ``LENDING`` (VIB-4248):
    collateralized debt positions (Maker, Liquity, Curve crvUSD) have a
    different lifecycle and liquidation fingerprint than money-market
    lending; liquidations are third-party forced closes whose accounting
    contract differs from voluntary REPAY. The split preserves per-primitive
    matching-policy isolation so a CDP semantics change cannot retroactively
    re-baseline LENDING (or vice-versa).
    """

    SWAP = "swap"
    LP = "lp"
    # VIB-4477: parallel LP primitive for Uniswap V4 so its version stream is
    # isolated from V3 / Aerodrome / TraderJoe / etc. V4 introduces a new
    # contract shape (PoolKey-driven attribution, 32-byte pool_id, singleton
    # PoolManager) and a new lot-matching anchor (position_hash); bumping
    # Primitive.LP from v1 to v2 would silently re-baseline every legacy V3
    # row. Resolution happens via ``primitive_for(event_type, protocol)`` —
    # the augment chokepoint and the Accountant Test's per-primitive bucket
    # collector both consume that helper so V4 rows land under LP_V4 and V3
    # rows stay under LP. ``record_for(event_type)`` is unchanged (returns
    # Primitive.LP for LP_OPEN regardless of protocol) so the
    # AccountingCategory dispatcher continues routing both into
    # ``lp_handler`` — the handler logic does not differ between V3 and V4.
    LP_V4 = "lp_v4"
    LENDING = "lending"
    CDP = "cdp"
    LIQUIDATION = "liquidation"
    PERP = "perp"
    VAULT = "vault"
    # VIB-5666: vault SETTLEMENT is its own primitive, split from VAULT. A
    # depositor-facing ``VAULT_DEPOSIT``/``VAULT_REDEEM`` (the strategy putting
    # capital INTO some ERC-4626) is a position the strategy opens; a vault
    # SETTLEMENT (the strategy IS the Lagoon vault, running
    # ``settleDeposit``/``settleRedeem`` to issue/burn shares against DEPOSITOR
    # capital) is a two-phase propose→settle lifecycle that mutates AUM
    # composition and accrues fee-shares. Folding it into VAULT would hide the
    # propose/settle boundary and let a depositor inflow be mis-priced as a
    # VAULT position. A dedicated primitive keeps its ``primitive_version`` /
    # ``matching_policy_version`` streams isolated (design doc §Accounting
    # taxonomy; ship-gate #3). Settlement events are CAPITAL events, never
    # returns — they carry no ``principal_delta_usd`` / ``realized_pnl_usd``.
    SETTLEMENT = "settlement"
    STAKING = "staking"
    BRIDGE = "bridge"
    PREDICTION = "prediction"
    FLASH_LOAN = "flash_loan"
    UTILITY = "utility"


class AccountingCategory(StrEnum):
    """Accounting-handler routing key.

    Determines which ``category_handlers/<name>_handler.py`` processes the
    accounting event for a given intent. This enum is consumed by both the
    accounting processor and the gateway whitelist.

    The values are kept stable (the string form is persisted inside
    ``accounting_events.payload_json`` and the ``position_registry``
    ``accounting_category`` column — there is no ``accounting_events.category``
    column) — never rename a value without a coordinated migration.
    """

    LENDING = "lending"
    LP = "lp"
    PERP = "perp"
    VAULT = "vault"
    # VIB-5666: routes vault SETTLE_DEPOSIT / SETTLE_REDEEM events to
    # ``settlement_handler`` (distinct from ``vault_handler``, which builds the
    # depositor-facing VaultAccountingEvent). See ``Primitive.SETTLEMENT``.
    SETTLEMENT = "settlement"
    SWAP = "swap"
    PREDICTION = "prediction"
    TRANSFER = "transfer"
    NO_ACCOUNTING = "no_accounting"


class PositionKind(StrEnum):
    """Position-tracking categories.

    Mirrors ``observability.position_events.PositionType`` but lives in the
    neutral tier so the taxonomy can declare it without depending on the
    observability package. T2 migrates ``PositionType`` to delegate here.
    """

    LP = "LP"
    PERP = "PERP"
    LENDING_COLLATERAL = "LENDING_COLLATERAL"
    LENDING_DEBT = "LENDING_DEBT"
    VAULT = "VAULT"
    STAKING = "STAKING"
    # Pendle principal token (VIB-52xx). Orthogonal to ``Primitive.SWAP``: a PT
    # buy/sell IS a swap and a redeem IS a withdraw, but the holding is a tracked
    # position with an OPEN→CLOSE lifecycle the dashboard renders as one
    # position. Lives on the position axis, not the primitive axis.
    PENDLE_PT = "PENDLE_PT"


class LifecyclePhase(StrEnum):
    """Where a step sits inside its primitive's lifecycle.

    ``ATOMIC`` — the step settles in one transaction (most current intents).
    ``REQUEST`` / ``CLAIM`` / ``SETTLE`` — phases of an async lifecycle
    (Vault async withdrawals, Pendle redemptions, future Bridge settlement).

    The phase is per-primitive: a Vault async withdraw is REQUEST, the matched
    claim transaction is SETTLE; both share a primitive but differ on phase.
    """

    ATOMIC = "atomic"
    REQUEST = "request"
    CLAIM = "claim"
    SETTLE = "settle"


class EventKind(StrEnum):
    """The shape of the accounting / position event the intent emits.

    ``OPEN`` / ``CLOSE`` — open or close a position leg.
    ``ADJUST`` — change size of an existing leg (perp INCREASE, lending
        BORROW after a SUPPLY).
    ``COLLECT`` — collect rewards / fees without changing principal
        (LP_COLLECT_FEES, vault harvest).
    ``TRANSFER`` — move value across wallets / chains without a position
        change (BRIDGE).
    ``NONE`` — utility intents that do not produce position or accounting
        rows (HOLD, ENSURE_BALANCE, WRAP_NATIVE).
    """

    OPEN = "open"
    CLOSE = "close"
    ADJUST = "adjust"
    COLLECT = "collect"
    TRANSFER = "transfer"
    NONE = "none"


class WalletDeltaLane(StrEnum):
    """How a primitive's **fungible wallet-token movement** is reconstructed.

    VIB-5865 (defect #1). The teardown swap-back clamp answers one question —
    *how much of this wallet balance is provably ours to sweep?* — by
    reconstructing tracked inventory from history
    (``accounting.basis.sum_open_wallet_basis_by_token``). Before this enum the
    answer was implicit: a primitive was visible iff someone had remembered to
    add it to ``basis._REPLAY_DISPATCH``, and every other wallet-moving verb
    (the LP, vault, perp, bridge/transfer, settlement and prediction-collateral
    families — 22 rows in this revision) was **silently** invisible. The clamp then skipped the strategy's own closing swap with
    ``untracked_token`` / ``degraded=False`` and the proceeds stranded.

    Declaring the lane on every taxonomy row makes that blindness structurally
    impossible: :class:`PrimitiveRecord` has no default for ``wallet_delta``,
    so a new row without a reviewed declaration is an **import-time
    ``TypeError``**, and a row declared ``UNMEASURED`` fails the clamp CLOSED
    and VISIBLY instead of stranding in silence.

    Lanes:

    ``EVENT_REPLAY``
        The wallet delta is reconstructed from persisted ``accounting_events``
        by a handler in ``basis._REPLAY_DISPATCH`` (SWAP, the lending family,
        the Pendle PT family, the prediction family). Measured — contributes a
        ``Decimal`` quantity to the tracked map.

    ``LEDGER_PROJECTION``
        The primitive writes NO ``accounting_events`` at all
        (``AccountingCategory.NO_ACCOUNTING``), so its delta is recovered by
        projecting MEASURED ``transaction_ledger`` rows into synthetic
        ``WALLET_MOVEMENT`` events (VIB-5416 / VIB-5471 — STAKE, WRAP_NATIVE,
        CDP MINT_STABLE, …). Measured, via a different source.

    ``UNMEASURED``
        A declared wallet-mover whose delta CANNOT currently be reconstructed
        from either source. Empty ≠ Zero: the tokens such a primitive touches
        have an unprovable total, so they **poison** the tracked map to ``None``
        (overriding any measured quantity) and the clamp refuses the swap with
        ``tracked_qty_unmeasured`` / ``degraded=True``. This is the safe on-ramp
        for every new primitive: visible refusal, never a silent strand.

    ``NONE``
        Reviewed as moving no fungible wallet token at all (snapshots, risk
        updates, aggregate markers). Each such row carries a justification
        comment on its taxonomy entry.

    A row must NOT be declared ``EVENT_REPLAY`` without a matching
    ``_REPLAY_DISPATCH`` handler, and a ``NONE`` / ``LEDGER_PROJECTION`` row
    must NOT have one — the lanes are disjoint by construction and
    ``tests/unit/primitives/test_wallet_delta_lane_vib5865.py`` enforces it.
    """

    EVENT_REPLAY = "event_replay"
    LEDGER_PROJECTION = "ledger_projection"
    UNMEASURED = "unmeasured"
    NONE = "none"


@dataclass(frozen=True)
class PrimitiveRecord:
    """One row of the primitives taxonomy.

    A ``PrimitiveRecord`` describes how the framework should treat a single
    canonical intent string: which primitive it belongs to, which accounting
    handler runs, which position bucket it lands in, whether it carries an
    async settlement gap, and which lifecycle steps a fixture must exercise
    to count as a complete primitive test.

    Records are frozen so the table is hashable and shareable across modules
    without defensive copying. The taxonomy is initialised once at import
    time.

    Attributes:
        intent_type: Canonical intent string (must match
            ``IntentType.value`` for declared types). Aliases are resolved
            before lookup via :data:`ALIASES`.
        primitive: Top-level primitive family.
        accounting_category: Routing key for accounting handlers.
        position_type: Position bucket; ``None`` when the intent does not
            create or modify a tracked position (e.g. SWAP, BRIDGE, HOLD).
        event_kind: Shape of the position / accounting event emitted.
        is_async: ``True`` when the on-chain action does not settle
            atomically and a separate claim/settle step is required.
        lifecycle_phase: Where this step sits inside the primitive's
            lifecycle.
        wallet_delta: How this primitive's fungible wallet-token movement is
            reconstructed (:class:`WalletDeltaLane`). **Required, no default**
            (VIB-5865): a taxonomy row that omits it is an import-time
            ``TypeError``, so a new primitive cannot silently join the set the
            teardown clamp is blind to.
        required_lifecycle: Tuple of intent strings that a complete
            Accountant Test fixture must exercise for this primitive to be
            considered fully-covered. Empty tuple = atomic; the fixture only
            needs this one step. Used by the fixture-lifecycle harness in T2.
    """

    intent_type: str
    primitive: Primitive
    accounting_category: AccountingCategory
    position_type: PositionKind | None
    event_kind: EventKind
    is_async: bool
    lifecycle_phase: LifecyclePhase
    required_lifecycle: tuple[str, ...]
    wallet_delta: WalletDeltaLane
