"""Prediction-market category handler for AccountingProcessor (VIB-3707).

Builds PredictionAccountingEvents for PREDICTION_BUY / PREDICTION_SELL /
PREDICTION_REDEEM intents. Reads cost_basis / proceeds / payout from the
result-enricher fields landed in VIB-3706 / VIB-3708 (extracted_data_json).

Position state is keyed per (market_id, outcome) and tracked as a single
weighted-average aggregate in FIFOBasisStore — averaging up combines
size + basis directly, partial sells consume basis proportionally, and
SELL/REDEEM emit realized PnL. No live chain calls.

Confidence model:
- HIGH when extracted_data carries the spec keys for the intent.
- ESTIMATED when shares/USD fields are missing — the event still records
  the disposal so downstream pipelines surface a measurable gap.
- realized_pnl_usd=None on SELL/REDEEM with no prior recorded basis —
  the framework MUST NOT fabricate $0 basis from a missing record.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.accounting.ids import make_accounting_event_id
from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    PredictionAccountingEvent,
    PredictionEventType,
)

if TYPE_CHECKING:
    from almanak.framework.accounting.basis import FIFOBasisStore

logger = logging.getLogger(__name__)

_PREDICTION_INTENT_TYPES = frozenset({"PREDICTION_BUY", "PREDICTION_SELL", "PREDICTION_REDEEM"})


def handle_prediction(
    outbox_row: dict[str, Any],
    ledger_row: dict[str, Any],
    basis_store: FIFOBasisStore | None = None,
) -> PredictionAccountingEvent | None:
    """Build a PredictionAccountingEvent from an outbox + ledger row pair.

    Returns None for non-prediction intent types or when basis_store is None
    (the handler depends on the basis store for weighted-average aggregation
    — without it, PnL cannot be computed and writing a partial event would
    drift state on restart).

    All inputs come from the dicts — no live chain calls. Reads:
      - outbox_row: position_key, market_id, wallet_address
      - ledger_row: deployment_id, cycle_id, execution_mode,
                    chain, protocol, tx_hash, timestamp, gas_usd,
                    extracted_data_json
      - extracted_data:
          PREDICTION_BUY:    outcome_tokens_received, cost_basis, market_id
          PREDICTION_SELL:   outcome_tokens_sold,    proceeds,    market_id
          PREDICTION_REDEEM: redemption_amount,      payout,      market_id
    """
    from almanak.framework.observability.ledger import deserialize_extracted_data

    intent_type = (ledger_row.get("intent_type") or "").upper()
    if intent_type not in _PREDICTION_INTENT_TYPES:
        return None

    if basis_store is None:
        # Fail-soft: writing a no-PnL event without basis tracking would
        # silently drift on restart. Skip rather than corrupt state.
        logger.warning(
            "handle_prediction: basis_store is None for %s — skipping (no in-memory aggregate to maintain)",
            intent_type,
        )
        return None

    # ── Identity fields ──────────────────────────────────────────────────────
    deployment_id = ledger_row.get("deployment_id") or outbox_row.get("deployment_id") or ""
    cycle_id = ledger_row.get("cycle_id") or outbox_row.get("cycle_id") or ""
    execution_mode = ledger_row.get("execution_mode") or ""
    chain = ledger_row.get("chain") or ""
    protocol = (ledger_row.get("protocol") or "").lower()
    tx_hash = ledger_row.get("tx_hash") or ""
    ledger_entry_id = ledger_row.get("id") or ""
    wallet_address = outbox_row.get("wallet_address") or ""

    # ── Timestamp ────────────────────────────────────────────────────────────
    raw_ts = ledger_row.get("timestamp")
    try:
        ts_str = raw_ts.replace("Z", "+00:00") if isinstance(raw_ts, str) else None
        timestamp = datetime.fromisoformat(ts_str) if ts_str else datetime.now(UTC)
    except (ValueError, AttributeError):
        timestamp = datetime.now(UTC)

    # ── Extracted data (CLOB fill / CTF redemption fields) ───────────────────
    extracted = deserialize_extracted_data(ledger_row.get("extracted_data_json") or "")

    # market_id: outbox_row is the canonical source (set at execution time by
    # the runner from the intent). Fall back to extracted_data, then to the
    # last-but-one segment of position_key when present.
    market_id = (
        outbox_row.get("market_id")
        or _str_or_none(extracted.get("market_id"))
        or _market_from_position_key(outbox_row.get("position_key") or "")
        or ""
    )

    # outcome: parsed from the outbox position_key
    # (prediction:<protocol>:<chain>:<wallet>:<market_id>:<outcome>).
    # Fall back to extracted_data["outcome"] if the runner attached it
    # there. We never silently default to "YES" — a missing outcome is
    # an accounting gap and we surface it via unavailable_reason.
    outcome = _outcome_from_position_key(outbox_row.get("position_key") or "") or _str_or_none(extracted.get("outcome"))

    # ── Gas (only meaningful for REDEEM since BUY/SELL are off-chain CLOB) ──
    gas_usd: Decimal | None = None
    gas_usd_raw = ledger_row.get("gas_usd")
    if gas_usd_raw is not None and gas_usd_raw != "":
        gas_usd = _parse_decimal(gas_usd_raw)

    # ── Branch by intent type ────────────────────────────────────────────────
    if intent_type == "PREDICTION_BUY":
        return _handle_buy(
            outbox_row=outbox_row,
            extracted=extracted,
            basis_store=basis_store,
            deployment_id=deployment_id,
            cycle_id=cycle_id,
            execution_mode=execution_mode,
            chain=chain,
            protocol=protocol,
            tx_hash=tx_hash,
            ledger_entry_id=ledger_entry_id,
            wallet_address=wallet_address,
            timestamp=timestamp,
            market_id=market_id,
            outcome=outcome,
            gas_usd=gas_usd,
        )

    # PREDICTION_SELL / PREDICTION_REDEEM — both are disposals.
    return _handle_sell_or_redeem(
        intent_type=intent_type,
        outbox_row=outbox_row,
        extracted=extracted,
        basis_store=basis_store,
        deployment_id=deployment_id,
        cycle_id=cycle_id,
        execution_mode=execution_mode,
        chain=chain,
        protocol=protocol,
        tx_hash=tx_hash,
        ledger_entry_id=ledger_entry_id,
        wallet_address=wallet_address,
        timestamp=timestamp,
        market_id=market_id,
        outcome=outcome,
        gas_usd=gas_usd,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Branch helpers
# ──────────────────────────────────────────────────────────────────────────────


def _handle_buy(
    *,
    outbox_row: dict[str, Any],
    extracted: dict[str, Any],
    basis_store: FIFOBasisStore,
    deployment_id: str,
    cycle_id: str,
    execution_mode: str,
    chain: str,
    protocol: str,
    tx_hash: str,
    ledger_entry_id: str,
    wallet_address: str,
    timestamp: datetime,
    market_id: str,
    outcome: str | None,
    gas_usd: Decimal | None,
) -> PredictionAccountingEvent:
    shares = _parse_decimal(extracted.get("outcome_tokens_received"))
    cost_basis = _parse_decimal(extracted.get("cost_basis"))

    confidence, unavailable_reason = _confidence_for_fields(
        intent_type="PREDICTION_BUY",
        market_id=market_id,
        outcome=outcome,
        shares=shares,
        usd_value=cost_basis,
    )

    position_key = _build_position_key(
        outbox_row=outbox_row,
        protocol=protocol,
        chain=chain,
        wallet_address=wallet_address,
        market_id=market_id,
        outcome=outcome,
    )

    # CodeRabbit thread 3 fix: empty position_key means the outbox key was
    # missing AND the (market_id, outcome, wallet) trio is incomplete. Without
    # a key, every malformed event would collapse into the same empty-string
    # aggregate in the basis store — a future SELL/REDEEM keyed the same way
    # would book bogus realized PnL against that polluted bucket. Surface as
    # UNAVAILABLE and never touch the basis store.
    #
    # The position-key reason is the headline diagnostic (it's the root
    # accounting failure), so we always use it here — any field-level reason
    # from ``_confidence_for_fields`` is a downstream symptom of the same
    # outbox/runner gap.
    if not position_key:
        return _build_event(
            event_type=PredictionEventType.PREDICTION_OPEN,
            deployment_id=deployment_id,
            cycle_id=cycle_id,
            execution_mode=execution_mode,
            chain=chain,
            protocol=protocol,
            tx_hash=tx_hash,
            ledger_entry_id=ledger_entry_id,
            wallet_address=wallet_address,
            timestamp=timestamp,
            position_key="",
            market_id=market_id,
            outcome=outcome or "",
            intent_type="PREDICTION_BUY",
            shares_delta=shares or Decimal("0"),
            usd_delta=cost_basis or Decimal("0"),
            realized_pnl_usd=None,
            position_size_after=Decimal("0"),
            position_basis_after=Decimal("0"),
            gas_usd=gas_usd,
            confidence=AccountingConfidence.UNAVAILABLE,
            unavailable_reason="missing position_key / market_id / outcome on BUY",
        )

    # No usable shares/cost — emit a measurable-gap event so downstream
    # pipelines see the BUY happened. Aggregate is left untouched.
    if shares is None or shares <= 0 or cost_basis is None or cost_basis < 0:
        return _build_event(
            event_type=PredictionEventType.PREDICTION_OPEN,
            deployment_id=deployment_id,
            cycle_id=cycle_id,
            execution_mode=execution_mode,
            chain=chain,
            protocol=protocol,
            tx_hash=tx_hash,
            ledger_entry_id=ledger_entry_id,
            wallet_address=wallet_address,
            timestamp=timestamp,
            position_key=position_key,
            market_id=market_id,
            outcome=outcome or "",
            intent_type="PREDICTION_BUY",
            shares_delta=shares or Decimal("0"),
            usd_delta=cost_basis or Decimal("0"),
            realized_pnl_usd=None,
            position_size_after=Decimal("0"),
            position_basis_after=Decimal("0"),
            gas_usd=gas_usd,
            confidence=AccountingConfidence.UNAVAILABLE,
            unavailable_reason=unavailable_reason or "missing outcome_tokens_received / cost_basis on BUY",
        )

    # VIB-3710: fold gateway-side setup-tx gas + operator fee into the basis
    # row so realized PnL on a future SELL/REDEEM uses a fully-loaded cost.
    # Extracted_data populates these via ResultEnricher's
    # _extract_offchain_prediction_costs helper. Missing values default to
    # Decimal("0") (NOT None) — the handler must never silently convert a
    # missing measurement into a basis adjustment, but the basis-store API
    # already guards by clamping non-positive extras to 0, so we route the
    # parser's None / negative into the same guard explicitly.
    gas_cost_usd_extras = _parse_decimal(extracted.get("gas_cost_usd")) or Decimal("0")
    fee_pusd_extras = _parse_decimal(extracted.get("fee_pusd")) or Decimal("0")
    if gas_cost_usd_extras < 0:
        gas_cost_usd_extras = Decimal("0")
    if fee_pusd_extras < 0:
        fee_pusd_extras = Decimal("0")

    new_size, new_basis, is_open = basis_store.record_prediction_buy(
        deployment_id=deployment_id,
        position_key=position_key,
        shares=shares,
        cost_basis_usd=cost_basis,
        gas_cost_usd=gas_cost_usd_extras,
        fee_pusd=fee_pusd_extras,
    )
    event_type = PredictionEventType.PREDICTION_OPEN if is_open else PredictionEventType.PREDICTION_INCREASE

    return _build_event(
        event_type=event_type,
        deployment_id=deployment_id,
        cycle_id=cycle_id,
        execution_mode=execution_mode,
        chain=chain,
        protocol=protocol,
        tx_hash=tx_hash,
        ledger_entry_id=ledger_entry_id,
        wallet_address=wallet_address,
        timestamp=timestamp,
        position_key=position_key,
        market_id=market_id,
        outcome=outcome or "",
        intent_type="PREDICTION_BUY",
        shares_delta=shares,
        usd_delta=cost_basis,
        realized_pnl_usd=None,
        position_size_after=new_size,
        position_basis_after=new_basis,
        gas_usd=gas_usd,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )


def _handle_sell_or_redeem(
    *,
    intent_type: str,
    outbox_row: dict[str, Any],
    extracted: dict[str, Any],
    basis_store: FIFOBasisStore,
    deployment_id: str,
    cycle_id: str,
    execution_mode: str,
    chain: str,
    protocol: str,
    tx_hash: str,
    ledger_entry_id: str,
    wallet_address: str,
    timestamp: datetime,
    market_id: str,
    outcome: str | None,
    gas_usd: Decimal | None,
) -> PredictionAccountingEvent:
    if intent_type == "PREDICTION_SELL":
        shares = _parse_decimal(extracted.get("outcome_tokens_sold"))
        usd_value = _parse_decimal(extracted.get("proceeds"))
    else:  # PREDICTION_REDEEM
        shares = _parse_decimal(extracted.get("redemption_amount"))
        usd_value = _parse_decimal(extracted.get("payout"))

    # VIB-3710: SELL/REDEEM operator fee — subtracted from proceeds before
    # realized PnL is computed so the bookkeeping matches what the wallet
    # actually received (gross proceeds minus operator fee). The handler reads
    # ``fee_pusd`` from extracted_data the same way the BUY path does (the
    # result enricher writes it on the disposal fill when the operator
    # reported one). Missing values become 0; defensive clamp on negatives
    # mirrors the BUY-side guard. ``usd_value`` (the gross) is still preserved
    # on the event payload for audit traceability — only ``net_proceeds``
    # flows into ``match_prediction_sell``.
    #
    # CodeRabbit thread 4 fix: a negative gross ``usd_value`` from a malformed
    # enrichment payload would otherwise propagate into ``match_prediction_sell``
    # and book a synthetic loss against the live aggregate. The BUY branch
    # already rejects negative ``cost_basis``; SELL/REDEEM mirrors that
    # contract — negative gross proceeds become ``None`` here so the
    # missing-fields guard below short-circuits to UNAVAILABLE without
    # mutating the basis store.
    sell_fee = _parse_decimal(extracted.get("fee_pusd")) or Decimal("0")
    if sell_fee < 0:
        sell_fee = Decimal("0")
    net_proceeds: Decimal | None = None if usd_value is None or usd_value < 0 else usd_value - sell_fee

    confidence, unavailable_reason = _confidence_for_fields(
        intent_type=intent_type,
        market_id=market_id,
        outcome=outcome,
        shares=shares,
        usd_value=usd_value,
    )

    position_key = _build_position_key(
        outbox_row=outbox_row,
        protocol=protocol,
        chain=chain,
        wallet_address=wallet_address,
        market_id=market_id,
        outcome=outcome,
    )

    # CodeRabbit thread 3 fix: empty position_key on a disposal would otherwise
    # consume basis from the shared empty-key bucket — and any prior malformed
    # BUY (also collapsed there) would produce a fabricated PnL number. Surface
    # as UNAVAILABLE and never touch the basis store. The position-key reason
    # is the headline diagnostic (root cause); any field-level reason from
    # ``_confidence_for_fields`` is a downstream symptom of the same gap.
    if not position_key:
        event_type = (
            PredictionEventType.PREDICTION_REDEEM
            if intent_type == "PREDICTION_REDEEM"
            else PredictionEventType.PREDICTION_CLOSE
        )
        return _build_event(
            event_type=event_type,
            deployment_id=deployment_id,
            cycle_id=cycle_id,
            execution_mode=execution_mode,
            chain=chain,
            protocol=protocol,
            tx_hash=tx_hash,
            ledger_entry_id=ledger_entry_id,
            wallet_address=wallet_address,
            timestamp=timestamp,
            position_key="",
            market_id=market_id,
            outcome=outcome or "",
            intent_type=intent_type,
            shares_delta=shares or Decimal("0"),
            usd_delta=usd_value or Decimal("0"),
            realized_pnl_usd=None,
            position_size_after=Decimal("0"),
            position_basis_after=Decimal("0"),
            gas_usd=gas_usd,
            confidence=AccountingConfidence.UNAVAILABLE,
            unavailable_reason=f"missing position_key / market_id / outcome on {intent_type}",
        )

    # Look up prior basis. A missing row on SELL/REDEEM means the strategy
    # was deployed after the BUY (or the BUY accounting failed) — surface as
    # an explicit gap rather than fabricating $0 basis.
    prior = basis_store.get_prediction_position(
        deployment_id=deployment_id,
        position_key=position_key,
    )

    if prior is None:
        warning = (
            f"{intent_type} with no recorded basis (market_id={market_id}, outcome={outcome}) "
            "— strategy may have been deployed with existing position; PnL will be incomplete"
        )
        logger.warning(warning)
        # REDEEM always closes; SELL also reports as CLOSE here because we
        # have no aggregate to leave open.
        event_type = (
            PredictionEventType.PREDICTION_REDEEM
            if intent_type == "PREDICTION_REDEEM"
            else PredictionEventType.PREDICTION_CLOSE
        )
        return _build_event(
            event_type=event_type,
            deployment_id=deployment_id,
            cycle_id=cycle_id,
            execution_mode=execution_mode,
            chain=chain,
            protocol=protocol,
            tx_hash=tx_hash,
            ledger_entry_id=ledger_entry_id,
            wallet_address=wallet_address,
            timestamp=timestamp,
            position_key=position_key,
            market_id=market_id,
            outcome=outcome or "",
            intent_type=intent_type,
            shares_delta=shares or Decimal("0"),
            usd_delta=usd_value or Decimal("0"),
            realized_pnl_usd=None,
            position_size_after=Decimal("0"),
            position_basis_after=Decimal("0"),
            gas_usd=gas_usd,
            confidence=AccountingConfidence.UNAVAILABLE,
            unavailable_reason=(unavailable_reason or warning),
        )

    # Missing per-trade fields — record the disposal at UNAVAILABLE confidence
    # without mutating the aggregate. Future enrichment (manual or replay) can
    # adjust by writing a corrective event.
    #
    # CodeRabbit thread 4 fix: ``usd_value < 0`` falls into the same bucket as
    # ``usd_value is None``. A negative gross would have flowed into
    # ``match_prediction_sell`` and booked a synthetic loss against the
    # aggregate. Reject it here — gross proceeds/payout cannot be negative.
    if shares is None or shares <= 0 or usd_value is None or usd_value < 0:
        event_type = (
            PredictionEventType.PREDICTION_REDEEM
            if intent_type == "PREDICTION_REDEEM"
            else PredictionEventType.PREDICTION_REDUCE
        )
        prior_size, prior_basis = prior
        return _build_event(
            event_type=event_type,
            deployment_id=deployment_id,
            cycle_id=cycle_id,
            execution_mode=execution_mode,
            chain=chain,
            protocol=protocol,
            tx_hash=tx_hash,
            ledger_entry_id=ledger_entry_id,
            wallet_address=wallet_address,
            timestamp=timestamp,
            position_key=position_key,
            market_id=market_id,
            outcome=outcome or "",
            intent_type=intent_type,
            shares_delta=shares or Decimal("0"),
            usd_delta=usd_value or Decimal("0"),
            realized_pnl_usd=None,
            position_size_after=prior_size,
            position_basis_after=prior_basis,
            gas_usd=gas_usd,
            confidence=AccountingConfidence.UNAVAILABLE,
            unavailable_reason=(unavailable_reason or "missing/invalid shares / proceeds on disposal"),
        )

    # REDEEM always closes the position regardless of the parsed shares —
    # CTF redeem burns all winning tokens for the (market, outcome). We
    # still pass through shares as an audit trail but force the basis-store
    # consumption to the full prior size.
    if intent_type == "PREDICTION_REDEEM":
        prior_size, _prior_basis = prior
        consume_shares = prior_size
    else:
        consume_shares = shares

    # ``net_proceeds`` cannot be None here — the missing-shares/usd guard
    # above already returned UNAVAILABLE for that case. ``or Decimal("0")`` is
    # belt-and-suspenders for type narrowing.
    realized_pnl, new_size, new_basis, is_close = basis_store.match_prediction_sell(
        deployment_id=deployment_id,
        position_key=position_key,
        shares_sold=consume_shares,
        proceeds_usd=net_proceeds or Decimal("0"),
    )

    if intent_type == "PREDICTION_REDEEM":
        event_type = PredictionEventType.PREDICTION_REDEEM
    elif is_close:
        event_type = PredictionEventType.PREDICTION_CLOSE
    else:
        event_type = PredictionEventType.PREDICTION_REDUCE

    return _build_event(
        event_type=event_type,
        deployment_id=deployment_id,
        cycle_id=cycle_id,
        execution_mode=execution_mode,
        chain=chain,
        protocol=protocol,
        tx_hash=tx_hash,
        ledger_entry_id=ledger_entry_id,
        wallet_address=wallet_address,
        timestamp=timestamp,
        position_key=position_key,
        market_id=market_id,
        outcome=outcome or "",
        intent_type=intent_type,
        shares_delta=shares,
        usd_delta=usd_value,
        realized_pnl_usd=realized_pnl,
        position_size_after=new_size,
        position_basis_after=new_basis,
        gas_usd=gas_usd,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Builders / parsers
# ──────────────────────────────────────────────────────────────────────────────


def _build_event(
    *,
    event_type: PredictionEventType,
    deployment_id: str,
    cycle_id: str,
    execution_mode: str,
    chain: str,
    protocol: str,
    tx_hash: str,
    ledger_entry_id: str,
    wallet_address: str,
    timestamp: datetime,
    position_key: str,
    market_id: str,
    outcome: str,
    intent_type: str,
    shares_delta: Decimal,
    usd_delta: Decimal,
    realized_pnl_usd: Decimal | None,
    position_size_after: Decimal,
    position_basis_after: Decimal,
    gas_usd: Decimal | None,
    confidence: AccountingConfidence,
    unavailable_reason: str,
) -> PredictionAccountingEvent:
    _id_seed = tx_hash or ledger_entry_id or position_key
    identity = AccountingIdentity(
        id=make_accounting_event_id(deployment_id, cycle_id, intent_type, _id_seed, position_key),
        deployment_id=deployment_id,
        cycle_id=cycle_id,
        execution_mode=execution_mode,
        timestamp=timestamp,
        chain=chain,
        protocol=protocol,
        wallet_address=wallet_address,
        tx_hash=tx_hash,
        ledger_entry_id=ledger_entry_id,
    )
    return PredictionAccountingEvent(
        identity=identity,
        event_type=event_type,
        position_key=position_key,
        market_id=market_id,
        outcome=outcome,
        intent_type=intent_type,
        shares_delta=shares_delta,
        usd_delta=usd_delta,
        realized_pnl_usd=realized_pnl_usd,
        position_size_after=position_size_after,
        position_basis_after=position_basis_after,
        gas_usd=gas_usd,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )


def _build_position_key(
    *,
    outbox_row: dict[str, Any],
    protocol: str,
    chain: str,
    wallet_address: str,
    market_id: str,
    outcome: str | None,
) -> str:
    """Use the outbox-supplied position_key when present, else derive.

    The runner's _compute_outbox_position_key writes the canonical
    `prediction:<protocol>:<chain>:<wallet>:<market_id>:<outcome>` key.
    Falling back to derivation here keeps the handler usable in tests and
    in any pre-runner-wiring replay context.
    """
    pk = outbox_row.get("position_key") or ""
    if pk:
        return pk
    if not market_id or not outcome or not wallet_address:
        return ""
    from almanak.connectors._strategy_base.compiler_registry import CompilerRegistry

    chain_norm = (chain or "").lower().strip()
    wallet_norm = wallet_address.lower().strip()
    proto_norm = (protocol or CompilerRegistry.default_protocol("PREDICTION") or "").lower().strip()
    if not proto_norm:
        # Fail closed: never emit a malformed ``prediction::...`` key with an empty
        # protocol segment (mirrors the runner-side guard) so outbox/accounting
        # grouping stays consistent.
        return ""
    return f"prediction:{proto_norm}:{chain_norm}:{wallet_norm}:{market_id}:{outcome}"


def _outcome_from_position_key(position_key: str) -> str | None:
    """Last segment of a prediction position key is the outcome."""
    if not position_key or not position_key.startswith("prediction:"):
        return None
    parts = position_key.split(":")
    if len(parts) < 6:
        return None
    return parts[-1]


def _market_from_position_key(position_key: str) -> str | None:
    """Second-to-last segment is the market_id for prediction position keys."""
    if not position_key or not position_key.startswith("prediction:"):
        return None
    parts = position_key.split(":")
    if len(parts) < 6:
        return None
    return parts[-2]


def _confidence_for_fields(
    *,
    intent_type: str,
    market_id: str,
    outcome: str | None,
    shares: Decimal | None,
    usd_value: Decimal | None,
) -> tuple[AccountingConfidence, str]:
    missing: list[str] = []
    if not market_id:
        missing.append("market_id")
    if not outcome:
        missing.append("outcome")
    if shares is None or shares <= 0:
        missing.append("shares")
    if usd_value is None:
        missing.append("usd_value")
    if not missing:
        return AccountingConfidence.HIGH, ""
    return (
        AccountingConfidence.ESTIMATED,
        f"{intent_type} missing {', '.join(missing)} on extracted_data",
    )


def _parse_decimal(value: Any) -> Decimal | None:
    """Safely parse value to Decimal. Returns None on failure or non-finite result."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    try:
        parsed = Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None
    return parsed if parsed.is_finite() else None


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value)
    return s if s else None


# ──────────────────────────────────────────────────────────────────────────────
# Registry adapter (VIB-4163, T3)
# ──────────────────────────────────────────────────────────────────────────────

from almanak.framework.accounting.category_handlers import HandlerContext, register
from almanak.framework.primitives.types import AccountingCategory


@register(AccountingCategory.PREDICTION)
def _dispatch_prediction(ctx: HandlerContext) -> PredictionAccountingEvent | None:
    return handle_prediction(ctx.outbox_row, ctx.ledger_row, ctx.basis_store)
