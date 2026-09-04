"""Declarative primitives taxonomy table and lookup API.

The :data:`TAXONOMY` table is the single canonical mapping from canonical
intent string to :class:`~almanak.framework.primitives.types.PrimitiveRecord`.
It covers every value of ``IntentType`` declared in
``almanak/framework/intents/vocabulary.py`` (see VIB-4159 ratified design).

Design rules:
    - Keyed by **string** intent type (not the ``IntentType`` enum) to avoid
      re-introducing the import cycle the taxonomy is meant to break.
    - :data:`ALIASES` maps legacy / ghost intent strings (e.g. the never-declared
      ``"VAULT_WITHDRAW"``) to their canonical equivalents. Lookups go through
      :func:`_resolve_alias` so callers can pass either.
    - Lookups are case-sensitive on the canonical form (uppercase). Inputs
      are normalised by upper-casing, matching the existing classifier.
    - Five placeholder rows (``LIQUIDATE``, ``OPEN_CDP``, ``MINT_STABLE``,
      ``REPAY_STABLE``, ``CLOSE_CDP``) are added in T5 (VIB-4165) — they live
      in the same shred-tree.
"""

from __future__ import annotations

import logging

from almanak.connectors._strategy_base.primitive_registry import PrimitiveRegistry
from almanak.framework.primitives.types import (
    AccountingCategory,
    EventKind,
    LifecyclePhase,
    PositionKind,
    Primitive,
    PrimitiveRecord,
    WalletDeltaLane,
)

logger = logging.getLogger(__name__)

ALIASES: dict[str, str] = {
    # Legacy classifier spelling that was never a declared IntentType.
    "VAULT_WITHDRAW": "VAULT_REDEEM",
}


def _resolve_alias(intent_type: str) -> str:
    """Return the canonical (upper-cased, alias-resolved) intent string."""
    canonical = intent_type.upper()
    return ALIASES.get(canonical, canonical)


def _record(
    intent_type: str,
    primitive: Primitive,
    accounting_category: AccountingCategory,
    position_type: PositionKind | None,
    event_kind: EventKind,
    *,
    wallet_delta: WalletDeltaLane,
    is_async: bool = False,
    lifecycle_phase: LifecyclePhase = LifecyclePhase.ATOMIC,
    required_lifecycle: tuple[str, ...] = (),
) -> tuple[str, PrimitiveRecord]:
    """Construct a (key, record) pair for the TAXONOMY table.

    ``wallet_delta`` is a REQUIRED keyword (VIB-5865) — it has no default here
    and none on :class:`PrimitiveRecord`, so a row added without a reviewed
    wallet-delta declaration fails at import with a ``TypeError`` rather than
    silently joining the set of primitives the teardown swap-back clamp cannot
    see. See :class:`WalletDeltaLane` for the four lanes.
    """
    return intent_type, PrimitiveRecord(
        intent_type=intent_type,
        primitive=primitive,
        accounting_category=accounting_category,
        position_type=position_type,
        event_kind=event_kind,
        is_async=is_async,
        lifecycle_phase=lifecycle_phase,
        required_lifecycle=required_lifecycle,
        wallet_delta=wallet_delta,
    )


_LP_LIFECYCLE: tuple[str, ...] = ("LP_OPEN", "LP_CLOSE")
_LP_LIFECYCLE_WITH_FEES: tuple[str, ...] = ("LP_OPEN", "LP_COLLECT_FEES", "LP_CLOSE")
_PERP_LIFECYCLE: tuple[str, ...] = ("PERP_OPEN", "PERP_CLOSE")
_LENDING_LIFECYCLE: tuple[str, ...] = ("SUPPLY", "BORROW", "REPAY", "WITHDRAW")
_VAULT_LIFECYCLE: tuple[str, ...] = ("VAULT_DEPOSIT", "VAULT_REDEEM")
# Lagoon settlement includes only capital-moving operator legs. SETTLE_PROPOSE
# updates total assets without moving capital, and the depositor lifecycle is separate.
_SETTLEMENT_LIFECYCLE: tuple[str, ...] = ("SETTLE_DEPOSIT", "SETTLE_REDEEM")
_STAKING_LIFECYCLE: tuple[str, ...] = ("STAKE", "UNSTAKE")
_PREDICTION_LIFECYCLE: tuple[str, ...] = (
    "PREDICTION_BUY",
    "PREDICTION_SELL",
    "PREDICTION_REDEEM",
)


# Lane assignments are measurement guarantees. EVENT_REPLAY must match
# ``accounting.basis._REPLAY_DISPATCH``; LEDGER_PROJECTION must match
# ``AccountingCategory.NO_ACCOUNTING``. Rows without ledger token legs remain
# harmless projections rather than narrowing teardown fund-safety coverage.
# UNMEASURED poisons the token footprint (Empty != Zero) so missing reliable folds
# cause a visible refusal. NONE is reserved for rows that move no fungible token.
TAXONOMY: dict[str, PrimitiveRecord] = dict(
    [
        _record(
            "SWAP",
            Primitive.SWAP,
            AccountingCategory.SWAP,
            position_type=None,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.EVENT_REPLAY,
        ),
        _record(
            "LP_OPEN",
            Primitive.LP,
            AccountingCategory.LP,
            position_type=PositionKind.LP,
            event_kind=EventKind.OPEN,
            required_lifecycle=_LP_LIFECYCLE,
            wallet_delta=WalletDeltaLane.EVENT_REPLAY,
        ),
        _record(
            "LP_CLOSE",
            Primitive.LP,
            AccountingCategory.LP,
            position_type=PositionKind.LP,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_LP_LIFECYCLE,
            wallet_delta=WalletDeltaLane.EVENT_REPLAY,
        ),
        _record(
            "LP_COLLECT_FEES",
            Primitive.LP,
            AccountingCategory.LP,
            position_type=PositionKind.LP,
            event_kind=EventKind.COLLECT,
            required_lifecycle=_LP_LIFECYCLE_WITH_FEES,
            wallet_delta=WalletDeltaLane.EVENT_REPLAY,
        ),
        _record(
            "SUPPLY",
            Primitive.LENDING,
            AccountingCategory.LENDING,
            position_type=PositionKind.LENDING_COLLATERAL,
            event_kind=EventKind.OPEN,
            required_lifecycle=_LENDING_LIFECYCLE,
            wallet_delta=WalletDeltaLane.EVENT_REPLAY,
        ),
        _record(
            "WITHDRAW",
            Primitive.LENDING,
            AccountingCategory.LENDING,
            position_type=PositionKind.LENDING_COLLATERAL,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_LENDING_LIFECYCLE,
            wallet_delta=WalletDeltaLane.EVENT_REPLAY,
        ),
        _record(
            "BORROW",
            Primitive.LENDING,
            AccountingCategory.LENDING,
            position_type=PositionKind.LENDING_DEBT,
            event_kind=EventKind.OPEN,
            required_lifecycle=_LENDING_LIFECYCLE,
            wallet_delta=WalletDeltaLane.EVENT_REPLAY,
        ),
        _record(
            "REPAY",
            Primitive.LENDING,
            AccountingCategory.LENDING,
            position_type=PositionKind.LENDING_DEBT,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_LENDING_LIFECYCLE,
            wallet_delta=WalletDeltaLane.EVENT_REPLAY,
        ),
        _record(
            "DELEVERAGE",
            Primitive.LENDING,
            AccountingCategory.LENDING,
            position_type=PositionKind.LENDING_DEBT,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_LENDING_LIFECYCLE,
            wallet_delta=WalletDeltaLane.EVENT_REPLAY,
        ),
        _record(
            "PERP_OPEN",
            Primitive.PERP,
            AccountingCategory.PERP,
            position_type=PositionKind.PERP,
            event_kind=EventKind.OPEN,
            required_lifecycle=_PERP_LIFECYCLE,
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        _record(
            "PERP_CLOSE",
            Primitive.PERP,
            AccountingCategory.PERP,
            position_type=PositionKind.PERP,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_PERP_LIFECYCLE,
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        # Cancelling an unfilled order refunds collateral but closes no position.
        # PERP accounting would fabricate an unmatched close; the snapshot and ledger
        # capture the refund without a phantom position or PnL event.
        _record(
            "PERP_CANCEL_ORDER",
            Primitive.PERP,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.LEDGER_PROJECTION,
        ),
        # Hyperliquid free-margin withdrawal bridges USDC from HyperCore to HyperEVM.
        # It moves owned cash but closes no position, so PERP accounting would create
        # an unmatched close. Snapshot and ledger capture the credit; the venue fee is
        # a measured balance deduction, never a synthesized PnL row.
        _record(
            "PERP_WITHDRAW",
            Primitive.PERP,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.LEDGER_PROJECTION,
        ),
        # Keeper settlement shares the PERP version stream but owns no position
        # lifecycle; the submission event already does. The keeper pays gas and no
        # keeper transaction-ledger row exists, so projection would invent gas and
        # capital flow. AccountingWriter writes it directly, making PERP metadata-only.
        _record(
            "PERP_SETTLEMENT",
            Primitive.PERP,
            AccountingCategory.PERP,
            position_type=None,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        _record(
            "VAULT_DEPOSIT",
            Primitive.VAULT,
            AccountingCategory.VAULT,
            position_type=PositionKind.VAULT,
            event_kind=EventKind.OPEN,
            required_lifecycle=_VAULT_LIFECYCLE,
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        _record(
            "VAULT_REDEEM",
            Primitive.VAULT,
            AccountingCategory.VAULT,
            position_type=PositionKind.VAULT,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_VAULT_LIFECYCLE,
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        _record(
            "VAULT_REALLOCATE",
            Primitive.VAULT,
            AccountingCategory.VAULT,
            position_type=PositionKind.VAULT,
            event_kind=EventKind.ADJUST,
            required_lifecycle=_VAULT_LIFECYCLE,
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        _record(
            "VAULT_MANAGE",
            Primitive.VAULT,
            AccountingCategory.VAULT,
            position_type=PositionKind.VAULT,
            event_kind=EventKind.ADJUST,
            required_lifecycle=_VAULT_LIFECYCLE,
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        # Lagoon operator settlements are payload-only because they occur before
        # ``decide()``. They move depositor capital, not a strategy position, so
        # EventKind.NONE prevents a position reference. The dedicated category and
        # primitive route the handler and isolate its version stream.
        _record(
            "SETTLE_DEPOSIT",
            Primitive.SETTLEMENT,
            AccountingCategory.SETTLEMENT,
            position_type=None,
            event_kind=EventKind.NONE,
            required_lifecycle=_SETTLEMENT_LIFECYCLE,
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        _record(
            "SETTLE_REDEEM",
            Primitive.SETTLEMENT,
            AccountingCategory.SETTLEMENT,
            position_type=None,
            event_kind=EventKind.NONE,
            required_lifecycle=_SETTLEMENT_LIFECYCLE,
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        _record(
            "STAKE",
            Primitive.STAKING,
            AccountingCategory.NO_ACCOUNTING,
            position_type=PositionKind.STAKING,
            event_kind=EventKind.OPEN,
            required_lifecycle=_STAKING_LIFECYCLE,
            wallet_delta=WalletDeltaLane.LEDGER_PROJECTION,
        ),
        _record(
            "UNSTAKE",
            Primitive.STAKING,
            AccountingCategory.NO_ACCOUNTING,
            position_type=PositionKind.STAKING,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_STAKING_LIFECYCLE,
            wallet_delta=WalletDeltaLane.LEDGER_PROJECTION,
        ),
        # A bridge is a transfer-out/in pair with a settlement gap, not a
        # position or accounting-free operation.
        _record(
            "BRIDGE",
            Primitive.BRIDGE,
            AccountingCategory.TRANSFER,
            position_type=None,
            event_kind=EventKind.TRANSFER,
            is_async=True,
            lifecycle_phase=LifecyclePhase.REQUEST,
            # The ledger source amount is an intent fallback; receipt-measured
            # ``amount_sent`` exists only in bridge_data/extracted_data_json. Folding
            # the requested amount risks over-sweeping, so poison the token footprint
            # until a measured source amount reaches the ledger or event payload.
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        # TRANSFER is payload-only so the writer can resolve TransferAccountingEvent
        # and stamp the bridge version stream without failing strict lookup.
        _record(
            "TRANSFER",
            Primitive.BRIDGE,
            AccountingCategory.TRANSFER,
            position_type=None,
            event_kind=EventKind.TRANSFER,
            is_async=True,
            lifecycle_phase=LifecyclePhase.REQUEST,
            # The event payload has the same unmeasured source amount as BRIDGE.
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        # Polymarket BUY/SELL fills are off-chain. Measured cost and proceeds live
        # only in extracted_data_json while ledger token/amount columns are empty;
        # projection therefore emits nothing. The event payload's aggregate usd_delta
        # cannot reconstruct wallet legs, so keep them unmeasured. REDEEM has replay.
        _record(
            "PREDICTION_BUY",
            Primitive.PREDICTION,
            AccountingCategory.PREDICTION,
            position_type=None,
            event_kind=EventKind.OPEN,
            required_lifecycle=_PREDICTION_LIFECYCLE,
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        _record(
            "PREDICTION_SELL",
            Primitive.PREDICTION,
            AccountingCategory.PREDICTION,
            position_type=None,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_PREDICTION_LIFECYCLE,
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        _record(
            "PREDICTION_REDEEM",
            Primitive.PREDICTION,
            AccountingCategory.PREDICTION,
            position_type=None,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_PREDICTION_LIFECYCLE,
            wallet_delta=WalletDeltaLane.EVENT_REPLAY,
        ),
        _record(
            "FLASH_LOAN",
            Primitive.FLASH_LOAN,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.LEDGER_PROJECTION,
        ),
        # These accounting event types are payload-only, not IntentType members.
        # The writer resolves every emitted event type to stamp its primitive version;
        # these rows keep legitimate handler output from failing strict lookup.
        _record(
            "PT_BUY",
            Primitive.SWAP,
            AccountingCategory.SWAP,
            position_type=PositionKind.PENDLE_PT,
            event_kind=EventKind.OPEN,
            wallet_delta=WalletDeltaLane.EVENT_REPLAY,
        ),
        _record(
            "PT_SELL",
            Primitive.SWAP,
            AccountingCategory.SWAP,
            position_type=PositionKind.PENDLE_PT,
            event_kind=EventKind.CLOSE,
            wallet_delta=WalletDeltaLane.EVENT_REPLAY,
        ),
        _record(
            "PT_REDEEM",
            Primitive.SWAP,
            AccountingCategory.SWAP,
            position_type=PositionKind.PENDLE_PT,
            event_kind=EventKind.CLOSE,
            wallet_delta=WalletDeltaLane.EVENT_REPLAY,
        ),
        _record(
            "PENDLE_LP_OPEN",
            Primitive.LP,
            AccountingCategory.LP,
            position_type=PositionKind.LP,
            event_kind=EventKind.OPEN,
            required_lifecycle=_LP_LIFECYCLE,
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        _record(
            "PENDLE_LP_CLOSE",
            Primitive.LP,
            AccountingCategory.LP,
            position_type=PositionKind.LP,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_LP_LIFECYCLE,
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        # Observation only; LP_OPEN, LP_CLOSE, and LP_COLLECT_FEES own wallet movement.
        _record(
            "LP_SNAPSHOT",
            Primitive.LP,
            AccountingCategory.LP,
            position_type=PositionKind.LP,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.NONE,
        ),
        # Reserved event type with no emitted payload shape. UNMEASURED preserves
        # visible refusal rather than guessing how a hypothetical money path folds.
        _record(
            "LP_REBALANCE",
            Primitive.LP,
            AccountingCategory.LP,
            position_type=PositionKind.LP,
            event_kind=EventKind.ADJUST,
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        _record(
            "PERP_INCREASE",
            Primitive.PERP,
            AccountingCategory.PERP,
            position_type=PositionKind.PERP,
            event_kind=EventKind.ADJUST,
            required_lifecycle=_PERP_LIFECYCLE,
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        _record(
            "PERP_DECREASE",
            Primitive.PERP,
            AccountingCategory.PERP,
            position_type=PositionKind.PERP,
            event_kind=EventKind.ADJUST,
            required_lifecycle=_PERP_LIFECYCLE,
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        _record(
            "PERP_LIQUIDATE",
            Primitive.PERP,
            AccountingCategory.PERP,
            position_type=PositionKind.PERP,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_PERP_LIFECYCLE,
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        _record(
            "VAULT_HARVEST",
            Primitive.VAULT,
            AccountingCategory.VAULT,
            position_type=PositionKind.VAULT,
            event_kind=EventKind.COLLECT,
            required_lifecycle=_VAULT_LIFECYCLE,
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        # Observation only; the vault lifecycle events own wallet movement.
        _record(
            "VAULT_SNAPSHOT",
            Primitive.VAULT,
            AccountingCategory.VAULT,
            position_type=PositionKind.VAULT,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.NONE,
        ),
        # Aggregate close marker with no token legs; WITHDRAW and REPAY own movement.
        # NONE avoids poisoning tokens reconstructed by those measured events.
        _record(
            "CLOSE",
            Primitive.LENDING,
            AccountingCategory.LENDING,
            position_type=None,
            event_kind=EventKind.CLOSE,
            wallet_delta=WalletDeltaLane.NONE,
        ),
        # Health-factor observation between lending legs; no token moves.
        _record(
            "LIQUIDATION_RISK_UPDATE",
            Primitive.LENDING,
            AccountingCategory.LENDING,
            position_type=None,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.NONE,
        ),
        _record(
            "PREDICTION_OPEN",
            Primitive.PREDICTION,
            AccountingCategory.PREDICTION,
            position_type=None,
            event_kind=EventKind.OPEN,
            required_lifecycle=_PREDICTION_LIFECYCLE,
            wallet_delta=WalletDeltaLane.EVENT_REPLAY,
        ),
        _record(
            "PREDICTION_INCREASE",
            Primitive.PREDICTION,
            AccountingCategory.PREDICTION,
            position_type=None,
            event_kind=EventKind.ADJUST,
            required_lifecycle=_PREDICTION_LIFECYCLE,
            wallet_delta=WalletDeltaLane.EVENT_REPLAY,
        ),
        _record(
            "PREDICTION_REDUCE",
            Primitive.PREDICTION,
            AccountingCategory.PREDICTION,
            position_type=None,
            event_kind=EventKind.ADJUST,
            required_lifecycle=_PREDICTION_LIFECYCLE,
            wallet_delta=WalletDeltaLane.EVENT_REPLAY,
        ),
        _record(
            "PREDICTION_CLOSE",
            Primitive.PREDICTION,
            AccountingCategory.PREDICTION,
            position_type=None,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_PREDICTION_LIFECYCLE,
            wallet_delta=WalletDeltaLane.EVENT_REPLAY,
        ),
        # Compile-blocked placeholders preserve IntentType parity. CDP and
        # liquidation use isolated version streams instead of contaminating LENDING.
        # No handlers or position buckets exist, hence NO_ACCOUNTING and no position.
        _record(
            "LIQUIDATE",
            Primitive.LIQUIDATION,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.LEDGER_PROJECTION,
        ),
        _record(
            "OPEN_CDP",
            Primitive.CDP,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.LEDGER_PROJECTION,
        ),
        _record(
            "MINT_STABLE",
            Primitive.CDP,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.LEDGER_PROJECTION,
        ),
        _record(
            "REPAY_STABLE",
            Primitive.CDP,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.LEDGER_PROJECTION,
        ),
        _record(
            "CLOSE_CDP",
            Primitive.CDP,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.LEDGER_PROJECTION,
        ),
        _record(
            "HOLD",
            Primitive.UTILITY,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.LEDGER_PROJECTION,
        ),
        _record(
            "ENSURE_BALANCE",
            Primitive.UTILITY,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.LEDGER_PROJECTION,
        ),
        _record(
            "WRAP_NATIVE",
            Primitive.UTILITY,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.LEDGER_PROJECTION,
        ),
        _record(
            "UNWRAP_NATIVE",
            Primitive.UTILITY,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.LEDGER_PROJECTION,
        ),
    ]
)


class UnknownIntentTypeError(KeyError):
    """Raised when an intent string is not present in :data:`TAXONOMY`."""

    def __init__(self, intent_type: str) -> None:
        super().__init__(intent_type)
        self.intent_type = intent_type

    def __str__(self) -> str:
        return f"Unknown intent type: {self.intent_type!r}"


def record_for(intent_type: str) -> PrimitiveRecord:
    """Return the :class:`PrimitiveRecord` for ``intent_type``.

    The lookup resolves :data:`ALIASES` and is case-insensitive on the input
    (the canonical form is upper-case). Raises :class:`UnknownIntentTypeError`
    if no row is present — callers that want a fallback should catch the
    error explicitly rather than relying on a silent default.
    """
    key = _resolve_alias(intent_type)
    try:
        return TAXONOMY[key]
    except KeyError as e:
        raise UnknownIntentTypeError(intent_type) from e


def primitive_for(intent_type: str, protocol: str = "") -> Primitive:
    """Return the :class:`Primitive` for ``intent_type``, protocol-overridden.

    VIB-4477. The plain :func:`record_for` lookup maps every LP event_type to
    :attr:`Primitive.LP` because the AccountingCategory dispatcher (which is
    the consumer of :func:`record_for`) does not need to distinguish V3 from
    V4 — both route through ``lp_handler``. The version-stamping sites
    (``writer.augment_accounting_payload`` and the Accountant Test's G13
    per-primitive bucket collector) DO need that distinction so V3's
    ``primitive_version`` stream cannot retroactively re-baseline when V4's
    contract advances (and vice-versa).

    The override is currently scoped to Uniswap V4 (``protocol`` contains
    ``"uniswap_v4"``): the V4 contract is the only LP venue with a separate
    primitive slot in
    :data:`almanak.framework.accounting.payload_schemas.PRIMITIVE_VERSIONS`
    today. Other LP venues continue to resolve to :attr:`Primitive.LP`.

    Falls back to :attr:`Primitive.UTILITY` for unknown intent strings —
    same fallback as the augment chokepoint's non-live branch so callers do
    not see a KeyError they cannot resolve. Live callers should use
    :func:`record_for` first when they need a hard fail on unknown event
    types.
    """
    key = _resolve_alias(intent_type)
    record = TAXONOMY.get(key)
    if record is None:
        return Primitive.UTILITY
    if record.primitive is Primitive.LP and "uniswap_v4" in protocol.lower():
        return Primitive.LP_V4
    return record.primitive


def classify(
    intent_type: str,
    protocol: str = "",
    token_out: str = "",
) -> AccountingCategory:
    """Map an intent string to its :class:`AccountingCategory`.

    Mirrors the routing rules in :mod:`almanak.framework.accounting.classifier`
    so the two stay observationally identical. Returns the generic category for
    every protocol; connector-specific accounting (e.g. Pendle's LP / PT
    mechanics) is routed to the owning connector's treatment by
    ``AccountingProcessor._dispatch`` (stage-1, via ``AccountingTreatmentRegistry``)
    BEFORE ``classify`` is consulted (VIB-4931), so this function no longer
    special-cases any protocol.

    Args:
        intent_type: Canonical intent string (e.g. ``"LP_OPEN"``). Aliases
            are resolved.
        protocol: Optional protocol string. Retained for signature stability and
            forward compatibility; no longer used for routing.
        token_out: Optional output token symbol. Retained for signature
            stability; no longer used for routing.

    Returns:
        The accounting category for the intent. Unknown intents resolve to
        :attr:`AccountingCategory.NO_ACCOUNTING` (matching the pre-VIB-4161
        classifier behaviour — T2 raises instead).
    """
    key = _resolve_alias(intent_type)
    record = TAXONOMY.get(key)
    if record is None:
        return AccountingCategory.NO_ACCOUNTING

    return record.accounting_category


def position_type_for(intent_type: str) -> PositionKind | None:
    """Return the :class:`PositionKind` for ``intent_type``, or ``None``.

    Returns ``None`` for intents that do not create or modify a tracked
    position (SWAP, BRIDGE, HOLD, ENSURE_BALANCE, …) AND for intents that
    are not present in the taxonomy. Callers that want fail-fast behaviour
    should use :func:`record_for` and inspect ``record.position_type``.
    """
    key = _resolve_alias(intent_type)
    record = TAXONOMY.get(key)
    if record is None:
        return None
    return record.position_type


# Generic position labels are shared across venues. Protocol-name aliases belong
# to connectors and resolve through PrimitiveRegistry instead.
_GENERIC_LABEL_PRIMITIVES: dict[str, Primitive] = {
    "LP": Primitive.LP,
    "LENDING": Primitive.LENDING,
    "SUPPLY": Primitive.LENDING,
    "BORROW": Primitive.LENDING,
    "PERP": Primitive.PERP,
    "VAULT": Primitive.VAULT,
    "ERC4626": Primitive.VAULT,
    "STAKE": Primitive.STAKING,
    "STAKING": Primitive.STAKING,
    "STAKED": Primitive.STAKING,
    "PREDICTION": Primitive.PREDICTION,
    # Pendle PT is a swap primitive; PENDLE_PT is only the position-axis label.
    "PENDLE_PT": Primitive.SWAP,
    # CEX holdings and plain balances have no protocol state machine; teardown
    # unwinds them through swap or withdraw flows.
    "CEX": Primitive.UTILITY,
    "TOKEN": Primitive.UTILITY,
    "BALANCE": Primitive.UTILITY,
}


def materializer_primitive_for(position_type_str: str) -> Primitive | None:
    """Map a position-type string (teardown-side or protocol alias) to a top-level primitive.

    T2 (VIB-4162) consolidated the if-ladder previously hard-coded in
    :func:`almanak.framework.accounting.position_state._classify_position`.
    The protocol→primitive half of that ladder is now resolved through the
    strategy-side :class:`~almanak.connectors._strategy_base.primitive_registry.PrimitiveRegistry`
    (per ``docs/internal/blueprints/22-connector-self-containment.md``): each
    connector OWNS its ``Primitive`` + the position-type alias strings it
    answers to, and this function iterates the registry instead of branching
    on a hard-coded dispatch ladder.

    Recognises the two label families that historically reached the
    materializer:

    * ``teardown.models.PositionType`` values and other generic taxonomy
      labels (``LP`` / ``SUPPLY`` / ``BORROW`` / ``PERP`` / ``VAULT`` /
      ``STAKE`` / ``PREDICTION`` / ``CEX`` / ``TOKEN`` / ``BALANCE``). These
      have no single connector owner and resolve via
      :data:`_GENERIC_LABEL_PRIMITIVES`.
    * Protocol-name strings used by older callers (``UNISWAP_V3`` /
      ``AAVE_V3`` / ``GMX_V2`` etc.). These resolve via the connector-owned
      :class:`PrimitiveRegistry`.

    Every ``teardown.models.PositionType`` value resolves to a non-None
    primitive (``CEX`` and ``TOKEN`` collapse to ``Primitive.UTILITY``
    because they have no protocol-side state machine — they are bookkeeping
    legs the teardown system unwinds via plain swap/withdraw flows). The
    materializer caller in ``accounting.position_state._classify_position``
    only knows what to do with LP / LENDING / PERP and treats every other
    primitive as "skip" — that's the current materializer scope, not a
    statement about teardown coverage.

    Equivalence guarantee: the (generic table + connector registry) result is
    identical to the previous hard-coded ladder for every input string the
    ladder handled — pinned by the characterization test in
    ``tests/unit/primitives/test_materializer_primitive_equivalence.py``.

    VIB-4477: V4 *protocol-name alias* strings (``"UNI_V4"`` / ``"UNISWAP_V4"``,
    owned by the ``uniswap_v4`` connector) resolve to ``Primitive.LP_V4`` (a
    parallel version stream). NOTE the bare ``Primitive`` **enum-value** label
    ``"LP_V4"`` is NOT one of those aliases and resolves to ``None`` here —
    this function answers the generic-label and protocol-alias vocabularies,
    not the enum-value strings. Callers that may see a raw ``"LP_V4"``
    position-type label must recognise it directly (see
    ``accounting.accountant_test._is_track_c_eligible_position``, VIB-4483). The
    materializer's caller in ``accounting.position_state._classify_position``
    collapses ``Primitive.LP_V4`` back to the ``"LP"`` materializer bucket — the
    materializer code is V3/V4-shared because the LP position state machine is
    the same. The primitive split only matters at the version-stamping sites.

    VIB-4248: a CDP connector (Maker, Liquity, crvUSD, Lybra, Prisma, Aave
    GHO, …) declares ``Primitive.CDP`` in its own ``primitive.py`` when it
    lands; the materialiser then resolves CDP labels through the registry
    rather than silently misclassifying them back into ``LENDING``. The
    ``Primitive.CDP`` slot already exists in ``MATCHING_POLICY_VERSIONS`` /
    ``PRIMITIVE_VERSIONS`` — shipping the connector's ``primitive.py`` is the
    only step missing.
    """
    s = position_type_str.upper().strip()
    # Generic taxonomy labels take precedence over connector-owned protocol aliases.
    primitive = _GENERIC_LABEL_PRIMITIVES.get(s)
    if primitive is not None:
        return primitive
    primitive = PrimitiveRegistry.primitive_for_label(s)
    if primitive is not None:
        return primitive
    # The caller skips unknown labels; warn so the loss is visible.
    logger.warning(
        "materializer_primitive_for: unknown position_type_str=%r (normalized=%r); "
        "returning None — caller will treat as no-primitive. Declare the "
        "primitive on the owning connector's primitive.py (resolved via "
        "PrimitiveRegistry) or add a generic label in "
        "almanak/framework/primitives/taxonomy.py if this is a real primitive.",
        position_type_str,
        s,
    )
    return None


def is_async(intent_type: str) -> bool:
    """Return ``True`` if the intent has a non-atomic settlement gap.

    Unknown intents return ``False`` — the safe default is "atomic / no
    pending state". T2 fail-fasts on unknown intents instead.
    """
    key = _resolve_alias(intent_type)
    record = TAXONOMY.get(key)
    if record is None:
        return False
    return record.is_async
