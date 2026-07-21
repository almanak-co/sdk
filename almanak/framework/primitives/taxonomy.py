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


# Canonical lifecycles — kept as module-level constants so tests can assert
# that fixture lifecycles match the declared expectation without re-declaring
# them out-of-band.
_LP_LIFECYCLE: tuple[str, ...] = ("LP_OPEN", "LP_CLOSE")
_LP_LIFECYCLE_WITH_FEES: tuple[str, ...] = ("LP_OPEN", "LP_COLLECT_FEES", "LP_CLOSE")
_PERP_LIFECYCLE: tuple[str, ...] = ("PERP_OPEN", "PERP_CLOSE")
_LENDING_LIFECYCLE: tuple[str, ...] = ("SUPPLY", "BORROW", "REPAY", "WITHDRAW")
_VAULT_LIFECYCLE: tuple[str, ...] = ("VAULT_DEPOSIT", "VAULT_REDEEM")
# VIB-5682: vault SETTLEMENT (Lagoon operator side) canonical lifecycle — the two
# capital-moving legs. The propose leg (updateNewTotalAssets → SETTLE_PROPOSE) is
# NO_ACCOUNTING and moves no capital, so it is NOT a lifecycle step. Distinct from
# _VAULT_LIFECYCLE (depositor-facing VAULT_DEPOSIT/VAULT_REDEEM); settlement is the
# operator side that issues/burns shares. Source of truth for the Accountant Test's
# ``settlement`` scorecard profile required_lifecycle.
_SETTLEMENT_LIFECYCLE: tuple[str, ...] = ("SETTLE_DEPOSIT", "SETTLE_REDEEM")
_STAKING_LIFECYCLE: tuple[str, ...] = ("STAKE", "UNSTAKE")
_PREDICTION_LIFECYCLE: tuple[str, ...] = (
    "PREDICTION_BUY",
    "PREDICTION_SELL",
    "PREDICTION_REDEEM",
)


# VIB-5865 — every row below declares a ``wallet_delta`` lane
# (:class:`WalletDeltaLane`). The declarations in THIS revision encode CURRENT
# truth, not aspiration, so no fold mechanics change for measured lanes:
#
#   * ``EVENT_REPLAY``      == exactly the taxonomy-backed keys of
#                              ``accounting.basis._REPLAY_DISPATCH``.
#   * ``LEDGER_PROJECTION`` == exactly the ``AccountingCategory.NO_ACCOUNTING``
#                              rows (the VIB-5416 / VIB-5471 measured-ledger
#                              lane). Kept an EXACT match so the generalized
#                              predicate ``basis._is_ledger_projected_row`` is
#                              behaviour-preserving BY CONSTRUCTION — pinned by
#                              a parity test against the old category check.
#                              This deliberately includes rows that move nothing
#                              (``HOLD`` / ``ENSURE_BALANCE``) and the
#                              compile-blocked CDP / ``LIQUIDATE`` placeholders:
#                              they write no token-legged ``transaction_ledger``
#                              row, so projecting them is a no-op, whereas
#                              demoting them to ``NONE`` would silently narrow a
#                              fund-safety lane. A later PR may demote them with
#                              its own evidence.
#   * ``UNMEASURED``        == every other wallet-moving row (LP, vault, perp,
#                              bridge/transfer, settlement, prediction BUY/SELL).
#                              These POISON their token footprint in the teardown
#                              clamp's tracked map (Empty ≠ Zero) so the strand is
#                              a visible degraded refusal, not silence. PR-2+ move
#                              rows out of this lane as real replay folds land.
#   * ``NONE``              == reviewed as moving no fungible wallet token; each
#                              such row carries a justification comment.
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
            wallet_delta=WalletDeltaLane.EVENT_REPLAY,
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
        # PERP_CANCEL_ORDER (VIB-5568) — cancel a pending (unfilled) perp order and
        # refund its committed collateral. Deliberately NOT AccountingCategory.PERP:
        # a cancel closes NO position (a stranded pending order never opened one —
        # the VIB-5116 orphaned-collateral case), so classifying it as a PERP CLOSE
        # would fabricate an unmatched close leg with no PERP_OPEN counterpart and
        # break perp lifecycle / lot-matching. It is a refund of committed-but-unspent
        # collateral: the wallet credit is captured by the portfolio balance snapshot,
        # and the cancel tx still gets a transaction_ledger row via the teardown commit
        # pipeline — so it is visible without a phantom position/PnL event. Modeled on
        # FLASH_LOAN (domain primitive + NO_ACCOUNTING + EventKind.NONE); position_type
        # None and NO required_lifecycle (order-management, not a position leg).
        _record(
            "PERP_CANCEL_ORDER",
            Primitive.PERP,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.LEDGER_PROJECTION,
        ),
        # PERP_WITHDRAW (VIB-5617) — withdraw free margin off the venue's off-chain
        # account back to L1 (Hyperliquid: a CoreWriter spotSend HyperCore→HyperEVM
        # USDC bridge). Deliberately NOT AccountingCategory.PERP: a withdraw closes
        # NO position — it is a cash movement (moving already-owned USD from the
        # off-chain ledger to the on-chain wallet), so classifying it as a PERP
        # CLOSE would fabricate an unmatched close leg with no PERP_OPEN counterpart
        # and break perp lifecycle / lot-matching. The credited wallet balance is
        # captured by the portfolio snapshot and the tx still gets a
        # transaction_ledger row via the commit pipeline — visible without a phantom
        # position/PnL event. The ~$1 HyperCore withdraw fee is a measured venue
        # deduction in the balance delta, never synthesised as a PnL row (Empty ≠
        # Zero). Modeled on PERP_CANCEL_ORDER / FLASH_LOAN (domain primitive +
        # NO_ACCOUNTING + EventKind.NONE); position_type None, no required_lifecycle.
        _record(
            "PERP_WITHDRAW",
            Primitive.PERP,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.LEDGER_PROJECTION,
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
        # ──────────────────────────────────────────────────────────────────
        # Vault SETTLEMENT (Lagoon ERC-7540 operator side) — VIB-5666
        #
        # Payload-only event types (NOT IntentType members — settlement is
        # lifecycle-owned and pre-``decide()``, never a first-class Intent verb;
        # design doc §Pillar-1). Emitted by ``settlement_handler`` when the
        # runner-owned settlement-commit pipeline routes a settleDeposit /
        # settleRedeem tx. ``event_kind=NONE`` because a settlement is a CAPITAL
        # event, not a position OPEN/CLOSE — the augment chokepoint must NOT
        # stamp a ``position_reference`` and no ``position_events`` row is emitted
        # (depositor capital is not a strategy position). ``AccountingCategory.
        # SETTLEMENT`` routes to the dedicated ``settlement_handler``;
        # ``Primitive.SETTLEMENT`` isolates the version streams. Present in
        # ``ALL_ACCOUNTING_EVENT_TYPES`` (via ``SettlementEventType``) so
        # ``test_taxonomy_has_no_extra_rows`` accepts these non-IntentType rows
        # and the augment chokepoint can resolve their per-primitive version.
        # ──────────────────────────────────────────────────────────────────
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
            # VIB-5865 PR-4: stays UNMEASURED — the source leg is NOT
            # receipt-measured on any surface the clamp can fold. The persisted
            # ledger ``amount_in`` (and the ``TransferAccountingEvent.asset`` /
            # ``amount`` derived from it) come from the INTENT via
            # ``ledger._extract_from_intent_fallback`` → ``BridgeIntent.token`` /
            # ``BridgeIntent.amount`` (no connector declares money-legs; bridges
            # emit no ``swap_amounts``). Folding an intent-requested amount is the
            # exact anti-pattern the LP FLOW-A trace disproved (requested 1.9006 vs
            # consumed 1.7087). The receipt-measured ``amount_sent`` exists only in
            # ``bridge_data`` / ``extracted_data_json`` (e.g.
            # ``stargate/receipt_parser.py``), never in ``amount_in`` and never in
            # the ``TransferAccountingEvent`` payload — so neither a LEDGER_PROJECTION
            # (empty-safe on ``amount_in``, but that value is a guess) nor an
            # EVENT_REPLAY handler can read a trustworthy source amount today.
            # UNMEASURED poisons the bridged token → VISIBLE degraded refusal
            # (the safe direction; live balance is the real over-sweep cap). A
            # measured fold requires first surfacing ``amount_sent`` into the
            # ledger / transfer-event payload (follow-up), then a ``_replay_transfer``.
            wallet_delta=WalletDeltaLane.UNMEASURED,
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
            # VIB-5865 PR-4: same as BRIDGE (this is the payload-only twin the
            # ``TransferAccountingEvent`` writer routes through) — the persisted
            # ``asset`` / ``amount`` are the intent-requested source values, not
            # receipt-measured, so it stays UNMEASURED. See the BRIDGE note above.
            wallet_delta=WalletDeltaLane.UNMEASURED,
        ),
        # ──────────────────────────────────────────────────────────────────
        # Prediction markets (Polymarket)
        # ──────────────────────────────────────────────────────────────────
        # VIB-5865 PR-4: PREDICTION_BUY / PREDICTION_SELL stay UNMEASURED.
        # LEDGER_PROJECTION is NOT viable and a lane flip alone wires in zero:
        #   1. These are OFF-CHAIN CLOB fills. The measured values (filled shares,
        #      ``cost_basis`` on BUY / ``proceeds`` on SELL) live in
        #      ``extracted_data_json`` (``polymarket/receipt_parser.py``
        #      SUPPORTED_EXTRACTIONS), NOT in the ledger ``token_in`` /
        #      ``token_out`` / ``amount_in`` / ``amount_out`` columns — those are
        #      EMPTY (``PredictionIntent`` has no from_token/token/amount, and
        #      ``amount_usd`` is deliberately excluded from the fallback,
        #      ``ledger.py`` VIB-5060). ``synthetic_wallet_movement_events`` reads
        #      exactly those empty columns and its has_in/has_out guard skips the
        #      row → projects nothing.
        #   2. Category is PREDICTION, not NO_ACCOUNTING, so declaring
        #      LEDGER_PROJECTION would break the PR-1 parity invariants
        #      (``test_ledger_projection_lane_equals_no_accounting_category`` +
        #      ``test_ledger_predicate_matches_old_category_predicate_on_every_row``).
        # A measured fold would need a ``_replay_prediction`` EVENT_REPLAY handler
        # sourcing the fill's cost_basis (USDC debit) / proceeds (USDC credit) —
        # today those live only in the ledger row's ``extracted_data_json`` (the
        # persisted PredictionAccountingEvent payload carries ``usd_delta``, not
        # the split legs), so the handler needs a payload-surfacing change first.
        # Its own design with real evidence (follow-up).
        # PREDICTION_REDEEM already declares EVENT_REPLAY, so the redemption USDC
        # return is already folded; only BUY-cost / SELL-proceeds remain the gap.
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
        # ──────────────────────────────────────────────────────────────────
        # Flash loan
        # ──────────────────────────────────────────────────────────────────
        _record(
            "FLASH_LOAN",
            Primitive.FLASH_LOAN,
            AccountingCategory.NO_ACCOUNTING,
            position_type=None,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.LEDGER_PROJECTION,
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
        # VIB-5865 wallet_delta=NONE: a snapshot is a periodic *observation* of an
        # already-open LP position (``event_kind=NONE``) — it moves no fungible
        # wallet token. The wallet-moving legs are LP_OPEN / LP_CLOSE /
        # LP_COLLECT_FEES, which carry their own (UNMEASURED) declarations.
        _record(
            "LP_SNAPSHOT",
            Primitive.LP,
            AccountingCategory.LP,
            position_type=PositionKind.LP,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.NONE,
        ),
        # VIB-5865 PR-2: stays UNMEASURED while LP_OPEN/LP_CLOSE/LP_COLLECT_FEES
        # move to EVENT_REPLAY. ``LPEventType.LP_REBALANCE`` is a RESERVED event
        # type — no handler emits it today (``observability/pnl_attributor.py``
        # §"The alternative model — explicit LP_REBALANCE lifecycle events — is
        # reserved"), so there is no payload shape to fold and no real row to
        # prove a fold against. Folding a hypothetical shape would be
        # unverifiable guesswork on a money path; the UNMEASURED declaration
        # keeps the safe visible-refusal behaviour until the lane actually ships.
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
        # VIB-5865 wallet_delta=NONE: observation-only, same rationale as
        # LP_SNAPSHOT. The vault's wallet-moving legs are VAULT_DEPOSIT /
        # VAULT_REDEEM / VAULT_HARVEST / VAULT_REALLOCATE / VAULT_MANAGE.
        _record(
            "VAULT_SNAPSHOT",
            Primitive.VAULT,
            AccountingCategory.VAULT,
            position_type=PositionKind.VAULT,
            event_kind=EventKind.NONE,
            wallet_delta=WalletDeltaLane.NONE,
        ),
        # VIB-5865 wallet_delta=NONE (reviewed judgment call — brief Q8). ``CLOSE``
        # is the lending *aggregate marker* consumed by
        # ``accounting/reporting/lending_report.py`` to flag a lending summary as
        # closed; it has no position_type and no token legs of its own. Every
        # fungible movement of the close is carried by the constituent WITHDRAW /
        # REPAY events, which are EVENT_REPLAY. Declaring it UNMEASURED would
        # poison the very tokens those measured legs just reconstructed — an
        # over-degradation with no fund-safety gain.
        _record(
            "CLOSE",
            Primitive.LENDING,
            AccountingCategory.LENDING,
            position_type=None,
            event_kind=EventKind.CLOSE,
            wallet_delta=WalletDeltaLane.NONE,
        ),
        # VIB-5865 wallet_delta=NONE: a health-factor/risk observation
        # (``event_kind=NONE``) emitted between lending legs — no token moves.
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
        # ──────────────────────────────────────────────────────────────────
        # Utility intents (no position, no accounting row)
        # ──────────────────────────────────────────────────────────────────
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

    # VIB-4931: Pendle's LP/PT events are routed to the connector treatment by
    # ``AccountingProcessor._dispatch`` (stage-1, via ``AccountingTreatmentRegistry``)
    # BEFORE ``classify`` is consulted, so the generic taxonomy no longer special-cases
    # Pendle here — it returns the generic category like every other protocol.
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
    # Pendle PT — the position lives on the SWAP primitive (a PT buy/sell IS a
    # swap; a redeem IS a withdraw treated as a swap-class disposal). The
    # PENDLE_PT PositionKind is the position-axis label, SWAP the primitive.
    "PENDLE_PT": Primitive.SWAP,
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
