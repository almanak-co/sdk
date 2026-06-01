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
        # P0 placeholders (VIB-4165, T5 of VIB-4160) — locked design item #5.
        # Primitive split: VIB-4248.
        #
        # These five rows exist so ``record_for(...)`` returns a row for every
        # ``IntentType`` value (parity invariant). The CDP-family rows
        # (``OPEN_CDP``, ``MINT_STABLE``, ``REPAY_STABLE``, ``CLOSE_CDP``)
        # resolve to ``Primitive.CDP``; ``LIQUIDATE`` resolves to
        # ``Primitive.LIQUIDATION``. They do NOT resolve to ``Primitive.LENDING``
        # because the source PRD (VIB-4159 / 2026-05-08) explicitly required
        # the split: "without them, future code paths smuggle CDP through
        # BORROW/REPAY and pollute lending accounting before P1 lands."
        # Mapping these to LENDING here would have re-created at the data
        # layer the exact conflation the placeholders exist to prevent —
        # every CDP / liquidation event would consume LENDING's per-primitive
        # ``matching_policy_version`` slot, defeating the VIB-4166 (T6)
        # isolation contract.
        #
        # ``AccountingCategory.NO_ACCOUNTING`` and ``position_type=None`` because
        # no real handler / position bucket exists yet — that lands in P1
        # with the real connector.
        #
        # The compiler raises ``NotImplementedError`` for each — guarded by
        # ``_raise_if_placeholder_intent`` in
        # ``almanak/framework/intents/compiler.py`` and a parameterised test
        # in ``tests/unit/intents/test_placeholder_compilers.py`` (Hard
        # Ratification Condition #5). VIB-4248 leaves Gate A (PolicyEngine)
        # and Gate B (compiler) untouched: the corrected primitive only
        # matters when a P1 ticket removes one of these IntentTypes from
        # ``_PLACEHOLDER_INTENT_TYPES`` and starts emitting real events.
        # ──────────────────────────────────────────────────────────────────
        _record(
            "LIQUIDATE",
            Primitive.LIQUIDATION,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
        ),
        _record(
            "OPEN_CDP",
            Primitive.CDP,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
        ),
        _record(
            "MINT_STABLE",
            Primitive.CDP,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
        ),
        _record(
            "REPAY_STABLE",
            Primitive.CDP,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
        ),
        _record(
            "CLOSE_CDP",
            Primitive.CDP,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
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


# Generic (non-protocol) position-type labels — the taxonomy's own
# vocabulary, NOT connector folder names. These are shared across every venue
# (``LP`` is "some LP position", ``LENDING`` is "some money-market position",
# …) so they have no single connector owner and stay here rather than on a
# connector ``primitive.py``. Protocol-name labels (``AAVE_V3`` / ``UNI_V3`` /
# ``GMX_V2`` / …) are owned by their connector and resolved through
# :class:`PrimitiveRegistry` — see the W-series self-containment blueprint
# (``docs/internal/blueprints/22-connector-self-containment.md``).
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
    # CEX holdings + plain token balances are bookkeeping legs the teardown
    # system unwinds via swap / withdraw — no protocol state machine. Mapping
    # to UTILITY documents the "no primitive of its own" invariant while
    # keeping the teardown-coverage test green.
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

    VIB-4477: V4 position-type strings resolve to ``Primitive.LP_V4`` (a
    parallel version stream owned by the ``uniswap_v4`` connector). The
    materializer's caller in ``accounting.position_state._classify_position``
    collapses ``LP_V4`` back to the ``"LP"`` materializer bucket — the
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
    # Generic (non-protocol) labels take precedence: they are the taxonomy's
    # own vocabulary and a connector must never re-claim one (the registry
    # only owns protocol-name aliases, never these). Then fall through to the
    # connector-owned registry for protocol-name labels.
    primitive = _GENERIC_LABEL_PRIMITIVES.get(s)
    if primitive is not None:
        return primitive
    primitive = PrimitiveRegistry.primitive_for_label(s)
    if primitive is not None:
        return primitive
    # T05 (VIB-4190): unknown position-type strings are silently coerced to
    # None today, then the caller in accounting.position_state._classify_position
    # treats them as "skip". WARN here so the operator sees the unrecognized
    # string rather than the silent skip — primitives T2 (VIB-4162) deferred
    # this diagnostic to the position-registry epic.
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
