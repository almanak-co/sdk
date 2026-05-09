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

from almanak.framework.primitives.types import (
    AccountingCategory,
    EventKind,
    LifecyclePhase,
    PositionKind,
    Primitive,
    PrimitiveRecord,
)

logger = logging.getLogger(__name__)

ALIASES: dict[str, str] = {
    # Ghost name from accounting/classifier.py:24 (pre-VIB-4161). The intent
    # was never declared in IntentType but the classifier still accepted it.
    # Resolving here keeps any caller that still passes the legacy spelling
    # working until T2 deletes the classifier-side acceptance.
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
    is_async: bool = False,
    lifecycle_phase: LifecyclePhase = LifecyclePhase.ATOMIC,
    required_lifecycle: tuple[str, ...] = (),
) -> tuple[str, PrimitiveRecord]:
    """Construct a (key, record) pair for the TAXONOMY table."""
    return intent_type, PrimitiveRecord(
        intent_type=intent_type,
        primitive=primitive,
        accounting_category=accounting_category,
        position_type=position_type,
        event_kind=event_kind,
        is_async=is_async,
        lifecycle_phase=lifecycle_phase,
        required_lifecycle=required_lifecycle,
    )


# Canonical lifecycles — kept as module-level constants so tests can assert
# that fixture lifecycles match the declared expectation without re-declaring
# them out-of-band.
_LP_LIFECYCLE: tuple[str, ...] = ("LP_OPEN", "LP_CLOSE")
_LP_LIFECYCLE_WITH_FEES: tuple[str, ...] = ("LP_OPEN", "LP_COLLECT_FEES", "LP_CLOSE")
_PERP_LIFECYCLE: tuple[str, ...] = ("PERP_OPEN", "PERP_CLOSE")
_LENDING_LIFECYCLE: tuple[str, ...] = ("SUPPLY", "BORROW", "REPAY", "WITHDRAW")
_VAULT_LIFECYCLE: tuple[str, ...] = ("VAULT_DEPOSIT", "VAULT_REDEEM")
_STAKING_LIFECYCLE: tuple[str, ...] = ("STAKE", "UNSTAKE")
_PREDICTION_LIFECYCLE: tuple[str, ...] = (
    "PREDICTION_BUY",
    "PREDICTION_SELL",
    "PREDICTION_REDEEM",
)


TAXONOMY: dict[str, PrimitiveRecord] = dict(
    [
        # ──────────────────────────────────────────────────────────────────
        # Swap
        # ──────────────────────────────────────────────────────────────────
        _record(
            "SWAP",
            Primitive.SWAP,
            AccountingCategory.SWAP,
            position_type=None,
            event_kind=EventKind.NONE,
        ),
        # ──────────────────────────────────────────────────────────────────
        # LP
        # ──────────────────────────────────────────────────────────────────
        _record(
            "LP_OPEN",
            Primitive.LP,
            AccountingCategory.LP,
            position_type=PositionKind.LP,
            event_kind=EventKind.OPEN,
            required_lifecycle=_LP_LIFECYCLE,
        ),
        _record(
            "LP_CLOSE",
            Primitive.LP,
            AccountingCategory.LP,
            position_type=PositionKind.LP,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_LP_LIFECYCLE,
        ),
        _record(
            "LP_COLLECT_FEES",
            Primitive.LP,
            AccountingCategory.LP,
            position_type=PositionKind.LP,
            event_kind=EventKind.COLLECT,
            required_lifecycle=_LP_LIFECYCLE_WITH_FEES,
        ),
        # ──────────────────────────────────────────────────────────────────
        # Lending
        # ──────────────────────────────────────────────────────────────────
        _record(
            "SUPPLY",
            Primitive.LENDING,
            AccountingCategory.LENDING,
            position_type=PositionKind.LENDING_COLLATERAL,
            event_kind=EventKind.OPEN,
            required_lifecycle=_LENDING_LIFECYCLE,
        ),
        _record(
            "WITHDRAW",
            Primitive.LENDING,
            AccountingCategory.LENDING,
            position_type=PositionKind.LENDING_COLLATERAL,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_LENDING_LIFECYCLE,
        ),
        _record(
            "BORROW",
            Primitive.LENDING,
            AccountingCategory.LENDING,
            position_type=PositionKind.LENDING_DEBT,
            event_kind=EventKind.OPEN,
            required_lifecycle=_LENDING_LIFECYCLE,
        ),
        _record(
            "REPAY",
            Primitive.LENDING,
            AccountingCategory.LENDING,
            position_type=PositionKind.LENDING_DEBT,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_LENDING_LIFECYCLE,
        ),
        _record(
            "DELEVERAGE",
            Primitive.LENDING,
            AccountingCategory.LENDING,
            position_type=PositionKind.LENDING_DEBT,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_LENDING_LIFECYCLE,
        ),
        # ──────────────────────────────────────────────────────────────────
        # Perp
        # ──────────────────────────────────────────────────────────────────
        _record(
            "PERP_OPEN",
            Primitive.PERP,
            AccountingCategory.PERP,
            position_type=PositionKind.PERP,
            event_kind=EventKind.OPEN,
            required_lifecycle=_PERP_LIFECYCLE,
        ),
        _record(
            "PERP_CLOSE",
            Primitive.PERP,
            AccountingCategory.PERP,
            position_type=PositionKind.PERP,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_PERP_LIFECYCLE,
        ),
        # ──────────────────────────────────────────────────────────────────
        # Vault (ERC-4626)
        # ──────────────────────────────────────────────────────────────────
        _record(
            "VAULT_DEPOSIT",
            Primitive.VAULT,
            AccountingCategory.VAULT,
            position_type=PositionKind.VAULT,
            event_kind=EventKind.OPEN,
            required_lifecycle=_VAULT_LIFECYCLE,
        ),
        _record(
            "VAULT_REDEEM",
            Primitive.VAULT,
            AccountingCategory.VAULT,
            position_type=PositionKind.VAULT,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_VAULT_LIFECYCLE,
        ),
        _record(
            "VAULT_REALLOCATE",
            Primitive.VAULT,
            AccountingCategory.VAULT,
            position_type=PositionKind.VAULT,
            event_kind=EventKind.ADJUST,
            required_lifecycle=_VAULT_LIFECYCLE,
        ),
        _record(
            "VAULT_MANAGE",
            Primitive.VAULT,
            AccountingCategory.VAULT,
            position_type=PositionKind.VAULT,
            event_kind=EventKind.ADJUST,
            required_lifecycle=_VAULT_LIFECYCLE,
        ),
        # ──────────────────────────────────────────────────────────────────
        # Staking
        # ──────────────────────────────────────────────────────────────────
        _record(
            "STAKE",
            Primitive.STAKING,
            AccountingCategory.NO_ACCOUNTING,
            position_type=PositionKind.STAKING,
            event_kind=EventKind.OPEN,
            required_lifecycle=_STAKING_LIFECYCLE,
        ),
        _record(
            "UNSTAKE",
            Primitive.STAKING,
            AccountingCategory.NO_ACCOUNTING,
            position_type=PositionKind.STAKING,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_STAKING_LIFECYCLE,
        ),
        # ──────────────────────────────────────────────────────────────────
        # Bridge / Transfer (VIB-4164, T4)
        #
        # T4 reclassifies BRIDGE from `NO_ACCOUNTING` to `TRANSFER`: a bridge
        # is a typed `transfer_out` on chain A and `transfer_in` on chain B
        # with a settlement gap, not "no accounting". The gateway whitelist
        # (`ALL_ACCOUNTING_EVENT_TYPES`) is widened atomically in this same
        # PR so the writer can persist the typed event the dispatcher now
        # routes to `transfer_handler`.
        # ──────────────────────────────────────────────────────────────────
        _record(
            "BRIDGE",
            Primitive.BRIDGE,
            AccountingCategory.TRANSFER,
            position_type=None,
            event_kind=EventKind.TRANSFER,
            is_async=True,
            lifecycle_phase=LifecyclePhase.REQUEST,
        ),
        # Payload-only event-type row (mirrors the VIB-4162 payload-only
        # rows for PT_BUY / PERP_INCREASE / etc.). The writer's augment
        # chokepoint at `accounting/writer.py:139` calls
        # `record_for(payload['event_type'])` with `event_type="TRANSFER"`
        # for every `TransferAccountingEvent`; without this row, every live
        # write would raise `UnknownIntentTypeError`. The `primitive` is
        # `Primitive.BRIDGE` so the augment step stamps
        # `MATCHING_POLICY_VERSIONS[Primitive.BRIDGE]`.
        _record(
            "TRANSFER",
            Primitive.BRIDGE,
            AccountingCategory.TRANSFER,
            position_type=None,
            event_kind=EventKind.TRANSFER,
            is_async=True,
            lifecycle_phase=LifecyclePhase.REQUEST,
        ),
        # ──────────────────────────────────────────────────────────────────
        # Prediction markets (Polymarket)
        # ──────────────────────────────────────────────────────────────────
        _record(
            "PREDICTION_BUY",
            Primitive.PREDICTION,
            AccountingCategory.PREDICTION,
            position_type=None,
            event_kind=EventKind.OPEN,
            required_lifecycle=_PREDICTION_LIFECYCLE,
        ),
        _record(
            "PREDICTION_SELL",
            Primitive.PREDICTION,
            AccountingCategory.PREDICTION,
            position_type=None,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_PREDICTION_LIFECYCLE,
        ),
        _record(
            "PREDICTION_REDEEM",
            Primitive.PREDICTION,
            AccountingCategory.PREDICTION,
            position_type=None,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_PREDICTION_LIFECYCLE,
        ),
        # ──────────────────────────────────────────────────────────────────
        # Flash loan
        # ──────────────────────────────────────────────────────────────────
        _record(
            "FLASH_LOAN",
            Primitive.FLASH_LOAN,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
        ),
        # ──────────────────────────────────────────────────────────────────
        # Payload-only event types — emitted by typed accounting models
        # (`accounting.models.*EventType`) but NOT declared in
        # `intents.vocabulary.IntentType`. The augment chokepoint
        # (`writer.augment_accounting_payload`) looks up the primitive
        # via `record_for(payload['event_type'])` and stamps the
        # per-primitive `matching_policy_version`. Without these rows,
        # live Pendle / PT / Prediction / extended-Perp / extended-Vault
        # writes raise `AccountingPersistenceError(cause=UnknownIntentTypeError)`
        # and halt the writer for legitimate handler output. These rows
        # are payload-side only — the dispatcher consumes IntentType
        # values, never these.
        # ──────────────────────────────────────────────────────────────────
        _record(
            "PT_BUY",
            Primitive.SWAP,
            AccountingCategory.PENDLE_PT,
            position_type=None,
            event_kind=EventKind.OPEN,
        ),
        _record(
            "PT_SELL",
            Primitive.SWAP,
            AccountingCategory.PENDLE_PT,
            position_type=None,
            event_kind=EventKind.CLOSE,
        ),
        _record(
            "PT_REDEEM",
            Primitive.SWAP,
            AccountingCategory.PENDLE_PT,
            position_type=None,
            event_kind=EventKind.CLOSE,
        ),
        _record(
            "PENDLE_LP_OPEN",
            Primitive.LP,
            AccountingCategory.PENDLE_LP,
            position_type=PositionKind.LP,
            event_kind=EventKind.OPEN,
            required_lifecycle=_LP_LIFECYCLE,
        ),
        _record(
            "PENDLE_LP_CLOSE",
            Primitive.LP,
            AccountingCategory.PENDLE_LP,
            position_type=PositionKind.LP,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_LP_LIFECYCLE,
        ),
        _record(
            "LP_SNAPSHOT",
            Primitive.LP,
            AccountingCategory.LP,
            position_type=PositionKind.LP,
            event_kind=EventKind.NONE,
        ),
        _record(
            "LP_REBALANCE",
            Primitive.LP,
            AccountingCategory.LP,
            position_type=PositionKind.LP,
            event_kind=EventKind.ADJUST,
        ),
        _record(
            "PERP_INCREASE",
            Primitive.PERP,
            AccountingCategory.PERP,
            position_type=PositionKind.PERP,
            event_kind=EventKind.ADJUST,
            required_lifecycle=_PERP_LIFECYCLE,
        ),
        _record(
            "PERP_DECREASE",
            Primitive.PERP,
            AccountingCategory.PERP,
            position_type=PositionKind.PERP,
            event_kind=EventKind.ADJUST,
            required_lifecycle=_PERP_LIFECYCLE,
        ),
        _record(
            "PERP_LIQUIDATE",
            Primitive.PERP,
            AccountingCategory.PERP,
            position_type=PositionKind.PERP,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_PERP_LIFECYCLE,
        ),
        _record(
            "VAULT_HARVEST",
            Primitive.VAULT,
            AccountingCategory.VAULT,
            position_type=PositionKind.VAULT,
            event_kind=EventKind.COLLECT,
            required_lifecycle=_VAULT_LIFECYCLE,
        ),
        _record(
            "VAULT_SNAPSHOT",
            Primitive.VAULT,
            AccountingCategory.VAULT,
            position_type=PositionKind.VAULT,
            event_kind=EventKind.NONE,
        ),
        _record(
            "CLOSE",
            Primitive.LENDING,
            AccountingCategory.LENDING,
            position_type=None,
            event_kind=EventKind.CLOSE,
        ),
        _record(
            "LIQUIDATION_RISK_UPDATE",
            Primitive.LENDING,
            AccountingCategory.LENDING,
            position_type=None,
            event_kind=EventKind.NONE,
        ),
        _record(
            "PREDICTION_OPEN",
            Primitive.PREDICTION,
            AccountingCategory.PREDICTION,
            position_type=None,
            event_kind=EventKind.OPEN,
            required_lifecycle=_PREDICTION_LIFECYCLE,
        ),
        _record(
            "PREDICTION_INCREASE",
            Primitive.PREDICTION,
            AccountingCategory.PREDICTION,
            position_type=None,
            event_kind=EventKind.ADJUST,
            required_lifecycle=_PREDICTION_LIFECYCLE,
        ),
        _record(
            "PREDICTION_REDUCE",
            Primitive.PREDICTION,
            AccountingCategory.PREDICTION,
            position_type=None,
            event_kind=EventKind.ADJUST,
            required_lifecycle=_PREDICTION_LIFECYCLE,
        ),
        _record(
            "PREDICTION_CLOSE",
            Primitive.PREDICTION,
            AccountingCategory.PREDICTION,
            position_type=None,
            event_kind=EventKind.CLOSE,
            required_lifecycle=_PREDICTION_LIFECYCLE,
        ),
        # ──────────────────────────────────────────────────────────────────
        # Utility intents (no position, no accounting row)
        # ──────────────────────────────────────────────────────────────────
        _record(
            "HOLD",
            Primitive.UTILITY,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
        ),
        _record(
            "ENSURE_BALANCE",
            Primitive.UTILITY,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
        ),
        _record(
            "WRAP_NATIVE",
            Primitive.UTILITY,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
        ),
        _record(
            "UNWRAP_NATIVE",
            Primitive.UTILITY,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
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


def classify(
    intent_type: str,
    protocol: str = "",
    token_out: str = "",
) -> AccountingCategory:
    """Map an intent string to its :class:`AccountingCategory`.

    Mirrors the routing rules in :mod:`almanak.framework.accounting.classifier`
    so the two stay observationally identical until T2 deletes the local
    classifier and re-points all consumers here. The two protocol-aware
    special cases preserved are:

    1. ``LP_OPEN`` / ``LP_CLOSE`` / ``LP_COLLECT_FEES`` on a Pendle protocol
       resolve to ``PENDLE_LP`` rather than ``LP``.
    2. ``SWAP`` on a Pendle protocol with a ``PT-`` token-out resolves to
       ``PENDLE_PT`` rather than ``SWAP``.

    Args:
        intent_type: Canonical intent string (e.g. ``"LP_OPEN"``). Aliases
            are resolved.
        protocol: Optional protocol string (e.g. ``"pendle_v2"``). Lower-cased
            before comparison.
        token_out: Optional output token symbol (e.g. ``"PT-stETH"``).

    Returns:
        The accounting category for the intent. Unknown intents resolve to
        :attr:`AccountingCategory.NO_ACCOUNTING` (matching the pre-VIB-4161
        classifier behaviour — T2 raises instead).
    """
    key = _resolve_alias(intent_type)
    record = TAXONOMY.get(key)
    if record is None:
        return AccountingCategory.NO_ACCOUNTING

    p = protocol.lower()
    if record.primitive is Primitive.LP and "pendle" in p:
        return AccountingCategory.PENDLE_LP
    if record.primitive is Primitive.SWAP and "pendle" in p and token_out.upper().startswith("PT-"):
        return AccountingCategory.PENDLE_PT
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


def materializer_primitive_for(position_type_str: str) -> Primitive | None:
    """Map a position-type string (teardown-side or protocol alias) to a top-level primitive.

    T2 (VIB-4162) — consolidates the if-ladder previously hard-coded in
    :func:`almanak.framework.accounting.position_state._classify_position`.
    Recognises the two label families that historically reached the
    materializer:

    * ``teardown.models.PositionType`` values (``LP`` / ``SUPPLY`` /
      ``BORROW`` / ``PERP`` / ``VAULT`` / ``STAKE`` / ``PREDICTION`` /
      ``CEX`` / ``TOKEN``).
    * Protocol-name strings used by older callers (``UNISWAP_V3`` /
      ``AAVE_V3`` / ``GMX_V2`` etc.) for backward compat.

    Every ``teardown.models.PositionType`` value resolves to a non-None
    primitive (``CEX`` and ``TOKEN`` collapse to ``Primitive.UTILITY``
    because they have no protocol-side state machine — they are bookkeeping
    legs the teardown system unwinds via plain swap/withdraw flows). The
    materializer caller in ``accounting.position_state._classify_position``
    only knows what to do with LP / LENDING / PERP and treats every other
    primitive as "skip" — that's the current materializer scope, not a
    statement about teardown coverage.
    """
    s = position_type_str.upper().strip()
    if s in {"LP", "UNI_V3", "UNISWAP_V3", "AERODROME", "AERODROME_LP", "TRADERJOE_LP"}:
        return Primitive.LP
    if s in {
        "LENDING",
        "SUPPLY",
        "BORROW",
        "AAVE_V3",
        "AAVE",
        "MORPHO",
        "MORPHO_BLUE",
        "COMPOUND_V3",
        "COMPOUND",
    }:
        return Primitive.LENDING
    if s in {"PERP", "GMX", "GMX_V2", "DRIFT", "HYPERLIQUID"}:
        return Primitive.PERP
    if s in {"VAULT", "ERC4626"}:
        return Primitive.VAULT
    if s in {"STAKE", "STAKING", "STAKED"}:
        return Primitive.STAKING
    if s in {"PREDICTION", "POLYMARKET"}:
        return Primitive.PREDICTION
    if s in {"CEX", "TOKEN", "BALANCE"}:
        # CEX holdings + plain token balances are bookkeeping legs the
        # teardown system unwinds via swap / withdraw — no protocol state
        # machine. Mapping to UTILITY documents the "no primitive of its
        # own" invariant while keeping the teardown-coverage test green.
        return Primitive.UTILITY
    # T05 (VIB-4190): unknown position-type strings are silently coerced to
    # None today, then the caller in accounting.position_state._classify_position
    # treats them as "skip". WARN here so the operator sees the unrecognized
    # string rather than the silent skip — primitives T2 (VIB-4162) deferred
    # this diagnostic to the position-registry epic.
    logger.warning(
        "materializer_primitive_for: unknown position_type_str=%r (normalized=%r); "
        "returning None — caller will treat as no-primitive. Add a mapping in "
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
