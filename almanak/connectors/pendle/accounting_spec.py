"""Connector-owned accounting treatment for Pendle (VIB-4931).

Owns the Pendle-specific accounting the framework used to carry as protocol-named
``AccountingCategory`` members (``PENDLE_LP`` / ``PENDLE_PT``) and ``if "pendle" in
protocol`` branches. Published as :data:`ACCOUNTING_TREATMENT_SPEC` and resolved by
the strategy-side
:class:`~almanak.connectors._strategy_base.accounting_treatment_registry.AccountingTreatmentRegistry`,
so the framework dispatcher routes Pendle's LP and PT events here via a *generic*
category + an opaque ``treatment`` key — naming no protocol in the framework.

The treatments are pure: they read the already-persisted ledger/outbox rows (no
gateway egress, no live chain calls) — the same ``HandlerContext`` contract the
generic category handlers obey. The bodies below are relocated **verbatim** from
``framework/accounting/category_handlers/pendle_handler.py`` (``handle_pendle_lp`` /
``handle_pendle_pt`` + ``_get_field`` / ``_market_from_position_key``) and from
``framework/accounting/pendle_pt_accounting.py`` (``_parse_pt_maturity`` /
``compute_implied_apr_bps``); behaviour is pinned byte-for-byte by the relocated
``tests/unit/framework/accounting/test_pendle_handlers.py`` suite plus the
registry-wiring tests in ``tests/unit/connectors/pendle/test_accounting_spec.py``.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from almanak.connectors._strategy_base.accounting_treatment_base import (
    AccountingCategoryDecision,
    AccountingTreatmentSpec,
)
from almanak.framework.accounting.ids import make_accounting_event_id
from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    PendleAccountingEvent,
    PendleEventType,
)
from almanak.framework.primitives.types import AccountingCategory

if TYPE_CHECKING:
    from almanak.framework.accounting.basis import FIFOBasisStore, MatchResult
    from almanak.framework.accounting.category_handlers import HandlerContext

logger = logging.getLogger(__name__)

_SCALE_18 = Decimal(10**18)
_APR_BPS_CAP = 500_000  # 50 000 % — sentinel for near-maturity buys

_MONTH_MAP: dict[str, int] = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

# Intent types this connector claims for special accounting treatment: the Pendle
# LP family (the generic LP handler must not see them) and the PT-buy swap.
_PENDLE_LP_INTENTS = frozenset({"LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"})


# ──────────────────────────────────────────────────────────────────────────────
# Maturity / implied-APR helpers (relocated from pendle_pt_accounting.py)
# ──────────────────────────────────────────────────────────────────────────────


def _parse_pt_maturity(pt_symbol: str) -> datetime | None:
    """Parse the maturity date embedded in a Pendle PT symbol.

    Accepts formats like:
      PT-wstETH-25JUN2026   → datetime(2026, 6, 25, UTC)
      PT-sUSDe-29MAY2025    → datetime(2025, 5, 29, UTC)
      PT-SUSDAI-15OCT2026   → datetime(2026, 10, 15, UTC)

    Returns None when the symbol doesn't follow the pattern.
    """
    m = re.search(r"[-_](\d{1,2})([A-Z]{3})(\d{4})(?:$|[-_])", pt_symbol.upper())
    if not m:
        return None
    day_s, month_abbr, year_s = m.group(1), m.group(2), m.group(3)
    month = _MONTH_MAP.get(month_abbr)
    if month is None:
        return None
    try:
        return datetime(int(year_s), month, int(day_s), tzinfo=UTC)
    except ValueError:
        return None


def compute_implied_apr_bps(pt_price: Decimal, days_to_maturity: int) -> int | None:
    """Compute implied APR in basis-points from PT price and days to maturity.

    Formula: (1 - pt_price) / pt_price * (365 / days_to_maturity) * 10_000

    Returns None when days_to_maturity <= 0 (at/past maturity).
    Caps the result at _APR_BPS_CAP (500 000 bps) for near-maturity buys.
    """
    if days_to_maturity <= 0:
        return None
    try:
        discount = (Decimal("1") - pt_price) / pt_price
        annualised = discount * (Decimal("365") / Decimal(str(days_to_maturity)))
        bps = int((annualised * Decimal("10000")).to_integral_value())
        return min(bps, _APR_BPS_CAP)
    except (InvalidOperation, ZeroDivisionError):
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Row-field helpers (relocated from pendle_handler.py)
# ──────────────────────────────────────────────────────────────────────────────


def _get_field(obj: Any, field: str) -> Any:
    """Get a field from a dataclass or dict, returning None if absent."""
    if obj is None:
        return None
    if hasattr(obj, field):
        return getattr(obj, field)
    if isinstance(obj, dict):
        return obj.get(field)
    return None


def _market_from_position_key(position_key: str) -> str:
    """Extract market address from a pendle_lp position_key (last segment)."""
    if not position_key:
        return ""
    parts = position_key.split(":")
    return parts[-1] if len(parts) >= 4 else ""


def _pt_symbol_from_ledger(ledger_row: dict[str, Any]) -> str:
    """Return the PT symbol on a PT ledger row (``token_out`` on a buy,
    ``token_in`` on a sell/redeem) — the symbol that anchors the PT position
    identity. Empty when neither leg is a PT token.

    The PT symbol is the one identifier present on BOTH the ledger row (here)
    and the intent at position-event seed time, so a position_key derived from
    it is byte-identical to ``observability/position_events.py:_pendle_pt_event``
    (the resolved Pendle market address is never persisted on the ledger row).
    """
    token_out = (ledger_row.get("token_out") or "").strip()
    token_in = (ledger_row.get("token_in") or "").strip()
    if token_out.upper().startswith("PT-"):
        return token_out
    if token_in.upper().startswith("PT-"):
        return token_in
    return ""


# ──────────────────────────────────────────────────────────────────────────────
# Pendle LP treatment (relocated from pendle_handler.py)
# ──────────────────────────────────────────────────────────────────────────────


# crap-allowlist: VIB-4988 — verbatim relocation of a pre-existing cc=34 money-path
# handler; cc>30 is not coverage-fixable, and decomposition is tracked separately under
# the crap-refactor protocol (do not refactor it from this move PR).
def handle_pendle_lp(
    outbox_row: dict[str, Any],
    ledger_row: dict[str, Any],
) -> PendleAccountingEvent | None:
    """Build a PendleAccountingEvent(PENDLE_LP_OPEN|PENDLE_LP_CLOSE) from ledger row.

    SY and PT amounts are read from deserialized extracted_data_json.
    Position key is from the outbox_row (pre-computed by runner).
    """
    from almanak.framework.observability.ledger import deserialize_extracted_data

    intent_type_str = (ledger_row.get("intent_type") or "").upper()
    if intent_type_str not in ("LP_OPEN", "LP_CLOSE"):
        return None

    protocol = (ledger_row.get("protocol") or "").lower()
    if "pendle" not in protocol:
        return None

    event_type = PendleEventType.PENDLE_LP_OPEN if intent_type_str == "LP_OPEN" else PendleEventType.PENDLE_LP_CLOSE

    deployment_id = ledger_row.get("deployment_id") or outbox_row.get("deployment_id") or ""
    cycle_id = ledger_row.get("cycle_id") or outbox_row.get("cycle_id") or ""
    execution_mode = ledger_row.get("execution_mode") or ""
    chain = ledger_row.get("chain") or ""
    tx_hash = ledger_row.get("tx_hash") or ""
    ledger_entry_id = ledger_row.get("id") or ""
    wallet_address = outbox_row.get("wallet_address") or ""
    position_key = outbox_row.get("position_key") or ""

    raw_ts = ledger_row.get("timestamp")
    try:
        ts_str = raw_ts.replace("Z", "+00:00") if isinstance(raw_ts, str) else None
        timestamp = datetime.fromisoformat(ts_str) if ts_str else datetime.now(UTC)
    except (ValueError, AttributeError):
        timestamp = datetime.now(UTC)

    extracted = deserialize_extracted_data(ledger_row.get("extracted_data_json") or "")

    sy_amount_raw: int | None = None
    pt_amount_raw: int | None = None
    market_address = ""

    if intent_type_str == "LP_OPEN":
        lp_open = extracted.get("lp_open_data")
        if lp_open is not None:
            sy_amount_raw = _get_field(lp_open, "amount0")
            pt_amount_raw = _get_field(lp_open, "amount1")
        # Derive market_address from position_key or outbox
        market_address = outbox_row.get("market_id") or _market_from_position_key(position_key) or ""
    else:
        lp_close = extracted.get("lp_close_data")
        if lp_close is not None:
            sy_amount_raw = _get_field(lp_close, "amount0_collected")
            pt_amount_raw = _get_field(lp_close, "amount1_collected")
        market_address = outbox_row.get("market_id") or _market_from_position_key(position_key) or ""

    sy_amount = Decimal(str(sy_amount_raw)) / _SCALE_18 if sy_amount_raw is not None else None
    pt_amount = Decimal(str(pt_amount_raw)) / _SCALE_18 if pt_amount_raw is not None else None

    if not position_key and market_address and wallet_address:
        position_key = f"pendle_lp:{chain.lower()}:{wallet_address.lower()}:{market_address.lower()}"

    _id_seed = tx_hash or ledger_entry_id or position_key
    identity = AccountingIdentity(
        id=make_accounting_event_id(deployment_id, cycle_id, event_type.value, _id_seed, position_key),
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

    return PendleAccountingEvent(
        identity=identity,
        event_type=event_type,
        position_key=position_key,
        market_id=market_address,
        pt_token="",
        maturity_timestamp=None,
        pt_amount=pt_amount,
        sy_amount=sy_amount,
        pt_price=None,
        implied_apr_bps=None,
        days_to_maturity=None,
        realized_yield_usd=None,
        confidence=AccountingConfidence.ESTIMATED,
        unavailable_reason="SY/PT scaled by assumed 18-decimal precision; pt_token and USD price absent",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Pendle PT treatment (relocated from pendle_handler.py)
# ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _PTContext:
    """Identity/row fields shared by every PT builder (buy / sell / redeem).

    Extracted once by :func:`handle_pendle_pt` so each builder reads the same
    canonical view of the ledger + outbox rows (mirrors the inline field reads
    the pre-decomposition ``handle_pendle_pt`` performed verbatim).
    """

    deployment_id: str
    cycle_id: str
    execution_mode: str
    chain: str
    protocol: str
    tx_hash: str
    ledger_entry_id: str
    wallet_address: str
    position_key: str
    market_address: str
    now: datetime
    extracted: dict[str, Any]
    ledger_row: dict[str, Any]


def _pt_context(outbox_row: dict[str, Any], ledger_row: dict[str, Any]) -> _PTContext:
    """Read the shared identity/row fields a PT builder needs (verbatim extract)."""
    from almanak.framework.observability.ledger import deserialize_extracted_data

    protocol = (ledger_row.get("protocol") or "").lower()
    chain = ledger_row.get("chain") or ""
    wallet_address = outbox_row.get("wallet_address") or ""
    position_key = outbox_row.get("position_key") or ""
    market_address = outbox_row.get("market_id") or ""

    raw_ts = ledger_row.get("timestamp")
    try:
        ts_str = raw_ts.replace("Z", "+00:00") if isinstance(raw_ts, str) else None
        now = datetime.fromisoformat(ts_str) if ts_str else datetime.now(UTC)
    except (ValueError, AttributeError):
        now = datetime.now(UTC)

    # Identity: prefer an explicit position_key, else derive it from the
    # normalized PT symbol on the ledger row. The resolved Pendle market address
    # is NOT persisted on the ledger/outbox row (a SwapIntent carries no market;
    # the on-chain ACTION target is the Pendle router, not the market), so the
    # legacy market-derived key was empty in practice — which broke both the
    # position_events ↔ accounting_events join AND the FIFO (position_key,
    # pt_token) realized-yield match. The PT symbol is present on the row AND on
    # the intent at seed time, so keying on it is byte-identical to
    # observability/position_events.py:_pendle_pt_event.
    if not position_key and wallet_address:
        pt_symbol = _pt_symbol_from_ledger(ledger_row)
        if pt_symbol:
            position_key = f"pendle_pt:{chain.lower()}:{wallet_address.lower()}:{pt_symbol.strip().lower()}"
        elif market_address:
            position_key = f"pendle_pt:{chain.lower()}:{wallet_address.lower()}:{market_address}"

    return _PTContext(
        deployment_id=ledger_row.get("deployment_id") or outbox_row.get("deployment_id") or "",
        cycle_id=ledger_row.get("cycle_id") or outbox_row.get("cycle_id") or "",
        execution_mode=ledger_row.get("execution_mode") or "",
        chain=chain,
        protocol=protocol,
        tx_hash=ledger_row.get("tx_hash") or "",
        ledger_entry_id=ledger_row.get("id") or "",
        wallet_address=wallet_address,
        position_key=position_key,
        market_address=market_address,
        now=now,
        extracted=deserialize_extracted_data(ledger_row.get("extracted_data_json") or ""),
        ledger_row=ledger_row,
    )


def _pt_identity(ctx: _PTContext, event_type: str) -> AccountingIdentity:
    """Build the AccountingIdentity for a PT event (shared id-seed policy)."""
    _id_seed = ctx.tx_hash or ctx.ledger_entry_id or ctx.position_key
    return AccountingIdentity(
        id=make_accounting_event_id(ctx.deployment_id, ctx.cycle_id, event_type, _id_seed, ctx.position_key),
        deployment_id=ctx.deployment_id,
        cycle_id=ctx.cycle_id,
        execution_mode=ctx.execution_mode,
        timestamp=ctx.now,
        chain=ctx.chain,
        protocol=ctx.protocol,
        wallet_address=ctx.wallet_address,
        tx_hash=ctx.tx_hash,
        ledger_entry_id=ctx.ledger_entry_id,
    )


def _parse_swap_amounts(ctx: _PTContext) -> tuple[Decimal | None, Decimal | None]:
    """Return ``(amount_in, amount_out)`` raw-18 Decimals from ``swap_amounts``."""
    amount_in: Decimal | None = None
    amount_out: Decimal | None = None
    swap_amounts = ctx.extracted.get("swap_amounts")
    if swap_amounts is not None:
        raw_in = _get_field(swap_amounts, "amount_in")
        raw_out = _get_field(swap_amounts, "amount_out")
        if raw_in is not None:
            try:
                amount_in = Decimal(str(raw_in))
            except InvalidOperation:
                pass
        if raw_out is not None:
            try:
                amount_out = Decimal(str(raw_out))
            except InvalidOperation:
                pass
    return amount_in, amount_out


def _sy_price_from_ledger(ctx: _PTContext, base_token_symbol: str) -> Decimal | None:
    """Resolve the base/SY token USD price from ``price_inputs_json`` (R5).

    ``price_inputs_json`` is keyed by token SYMBOL. We key by the ACTUAL base
    token symbol (the swap's non-PT leg), never a synthetic "SY". Returns
    ``None`` (unmeasured) when the column is empty, unparseable, or has no entry
    for the symbol — the caller then degrades confidence and stores an
    SY-denominated yield (Empty ≠ Zero).
    """
    if not base_token_symbol:
        return None
    raw = ctx.ledger_row.get("price_inputs_json") or ""
    if not raw:
        return None
    try:
        prices = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(prices, dict):
        return None
    val = prices.get(base_token_symbol)
    if val is None:
        return None
    try:
        parsed = Decimal(str(val))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _realized_yield_from_match(
    match: MatchResult,
    sy_received_human: Decimal,
    sy_price: Decimal | None,
) -> tuple[Decimal | None, str, AccountingConfidence]:
    """Convert a FIFO ``MatchResult`` into ``(realized_yield_usd, reason, confidence)``.

    Implements the Empty≠Zero contract for PT realized yield (VIB-4988 C4):

    * No lot matched (``lot_matches`` empty) → ``None`` (unmeasured).
    * Matched & ``sy_price`` known → ``interest_or_yield * sy_price`` (USD).
      ``Decimal("0")`` is a genuine break-even, not "missing".
    * Matched & ``sy_price`` is ``None`` → store the SY-denominated value but
      flag ESTIMATED + an unavailable_reason (never silently treat SY as USD).
    * Partial match (``unmatched_amount > 0`` but a lot WAS consumed) → measured
      yield on the matched portion, with the unmatched qty noted in the reason.
    """
    if not match.lot_matches:
        return None, "no PT buy lot matched — realized yield unavailable", AccountingConfidence.ESTIMATED

    partial_note = ""
    confidence = AccountingConfidence.HIGH
    if match.unmatched_amount > 0:
        partial_note = f"; {match.unmatched_amount} PT unmatched (no prior buy lot) — yield on matched portion only"
        confidence = AccountingConfidence.ESTIMATED

    if sy_price is None:
        reason = "realized yield is SY-denominated (no USD price for base token)" + partial_note
        return match.interest_or_yield, reason, AccountingConfidence.ESTIMATED

    realized_usd = match.interest_or_yield * sy_price
    return realized_usd, partial_note.lstrip("; "), confidence


def _build_pt_buy(ctx: _PTContext, basis_store: FIFOBasisStore | None) -> PendleAccountingEvent:
    """Build a ``PT_BUY`` event.

    The event PAYLOAD stores ``pt_amount`` / ``sy_amount`` in **HUMAN** units —
    the ledger/FIFO measured-truth convention (Blueprint 27 §6.6), now uniform
    across PT_BUY / PT_SELL / PT_REDEEM so PEN6 PT-quantity conservation holds
    across BOTH a buy→sell and a buy→redeem round-trip. The replay reader
    (``basis.py:_replay_pt_buy``) reads these human values directly (no
    ``/ 1e18``), and the in-memory FIFO lot is recorded in the same human units,
    so a same-run match and a post-restart match agree (VIB-4988 R1).
    """
    pt_token_sym = ctx.ledger_row.get("token_out") or ""
    sy_raw, pt_raw = _parse_swap_amounts(ctx)
    # Convert raw-18 → human once, up front. ``pt_price`` is a unit-invariant
    # ratio (human/human == raw/raw), so the implied-APR math is unchanged.
    sy_amount = sy_raw / _SCALE_18 if sy_raw is not None else None
    pt_amount = pt_raw / _SCALE_18 if pt_raw is not None else None

    pt_price: Decimal | None = None
    if sy_amount and pt_amount and pt_amount > 0:
        try:
            pt_price = sy_amount / pt_amount
        except (InvalidOperation, ZeroDivisionError):
            pass

    maturity_ts = _parse_pt_maturity(pt_token_sym)
    days_to_maturity: int | None = None
    if maturity_ts is not None:
        days_to_maturity = (maturity_ts.date() - ctx.now.date()).days

    implied_apr_bps: int | None = None
    if pt_price is not None and days_to_maturity is not None:
        implied_apr_bps = compute_implied_apr_bps(pt_price, days_to_maturity)

    has_core_fields = pt_price is not None and pt_amount is not None
    has_apr = implied_apr_bps is not None
    if has_core_fields and has_apr:
        confidence = AccountingConfidence.HIGH
        unavailable_reason = ""
    elif has_core_fields and maturity_ts is not None:
        confidence = AccountingConfidence.ESTIMATED
        unavailable_reason = "PT matured — days_to_maturity <= 0, implied APR not applicable"
    elif has_core_fields:
        confidence = AccountingConfidence.ESTIMATED
        unavailable_reason = "maturity not parsed from PT symbol (implied APR unavailable)"
    else:
        confidence = AccountingConfidence.ESTIMATED
        unavailable_reason = "PT buy amounts unavailable from receipt"

    identity = _pt_identity(ctx, "PT_BUY")
    event = PendleAccountingEvent(
        identity=identity,
        event_type=PendleEventType.PT_BUY,
        position_key=ctx.position_key,
        market_id=ctx.market_address,
        pt_token=pt_token_sym,
        maturity_timestamp=maturity_ts,
        pt_amount=pt_amount,
        sy_amount=sy_amount,
        pt_price=pt_price,
        implied_apr_bps=implied_apr_bps,
        days_to_maturity=days_to_maturity,
        realized_yield_usd=None,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )

    # Record FIFO PT lot so PT_SELL / PT_REDEEM can compute realized yield on the
    # same run. The lot is HUMAN units — identical to the payload and to the
    # ``_replay_pt_buy`` reconstruction — so a same-run match and a post-restart
    # match agree (VIB-4988 R1).
    if basis_store is not None and pt_amount is not None and sy_amount is not None and pt_amount > 0:
        pt_token_key = pt_token_sym or "PT"
        basis_store.record_pt_buy(
            deployment_id=ctx.deployment_id,
            position_key=ctx.position_key,
            pt_token=pt_token_key,
            pt_amount=pt_amount,
            sy_cost=sy_amount,
            timestamp=ctx.now,
            lot_id=identity.id,
            source_ledger_entry_id=ctx.ledger_entry_id,
        )

    return event


def _build_pt_sell(ctx: _PTContext, basis_store: FIFOBasisStore | None) -> PendleAccountingEvent:
    """Build a ``PT_SELL`` event with FIFO-matched realized yield (VIB-4988).

    Detected by a ``PT-`` ``token_in``. ``swap_amounts`` carries raw-18
    ``amount_in`` (PT sold) and ``amount_out`` (base/SY received). The event
    payload stores **HUMAN** amounts (the uniform PT convention — see
    ``_build_pt_buy``; matched by ``_replay_pt_sell``); the FIFO match runs on the
    same human amounts (the lots were recorded human-side by ``_build_pt_buy`` /
    ``_replay_pt_buy``).
    """
    pt_token_sym = ctx.ledger_row.get("token_in") or ""
    base_token_sym = ctx.ledger_row.get("token_out") or ""
    pt_amount_raw, sy_amount_raw = _parse_swap_amounts(ctx)  # amount_in=PT, amount_out=base/SY
    pt_human = pt_amount_raw / _SCALE_18 if pt_amount_raw is not None else None
    sy_human = sy_amount_raw / _SCALE_18 if sy_amount_raw is not None else None

    identity = _pt_identity(ctx, "PT_SELL")

    realized_yield_usd: Decimal | None = None
    basis_lot_id: str | None = None
    confidence = AccountingConfidence.ESTIMATED
    unavailable_reason = "PT sell amounts unavailable from receipt"

    # Inline the None-guard (not a stored bool) so the type checker narrows
    # pt_human / sy_human to ``Decimal`` for the match call below.
    if pt_human is not None and sy_human is not None and pt_human > 0:
        sy_price = _sy_price_from_ledger(ctx, base_token_sym)
        if basis_store is not None:
            match = basis_store.match_pt_redeem(
                deployment_id=ctx.deployment_id,
                position_key=ctx.position_key,
                pt_token=pt_token_sym or "PT",
                pt_redeemed=pt_human,
                sy_received=sy_human,
            )
            realized_yield_usd, unavailable_reason, confidence = _realized_yield_from_match(match, sy_human, sy_price)
            if match.lot_matches:
                basis_lot_id = match.lot_matches[0].lot_id
        else:
            unavailable_reason = "no basis store — realized yield unavailable"

    return PendleAccountingEvent(
        identity=identity,
        event_type=PendleEventType.PT_SELL,
        position_key=ctx.position_key,
        market_id=ctx.market_address,
        pt_token=pt_token_sym,
        maturity_timestamp=_parse_pt_maturity(pt_token_sym),
        pt_amount=pt_human,
        sy_amount=sy_human,
        pt_price=None,
        implied_apr_bps=None,
        days_to_maturity=None,
        realized_yield_usd=realized_yield_usd,
        basis_lot_id=basis_lot_id,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )


def _redeem_amounts_from_legs(extracted: dict[str, Any]) -> tuple[Decimal | None, Decimal | None]:
    """Return ``(pt_human, sy_human)`` from the redeem's DECLARED money legs (PEN6).

    The Pendle parser's ``extract_primitive_money_legs`` declares a PT redeem's
    INPUT leg as the **PT token count** (basis-identical to the PT_BUY's PT
    ``amount_out``) and the OUTPUT leg as the **underlying received** — both in
    HUMAN units (``MeasuredMoney``). Sourcing the PT count from this INPUT leg,
    not ``redemption_amounts['py_redeemed']`` (which post-maturity is the
    SY-ASSET amount ≈ PT × SY-exchange-rate, NOT the PT count), keeps PT quantity
    conserved through the FIFO match so realized yield is computed against the
    right lot size (PEN6 / VIB-4988).

    Returns ``(None, None)`` when no declared legs are present or the value is
    not a ``PrimitiveMoneyLegs`` (Empty != Zero — the caller then falls back to
    ``redemption_amounts``). A leg whose amount is unmeasured yields ``None`` for
    that side (never zero, never an SY proxy).
    """
    if not isinstance(extracted, dict):
        return None, None
    legs = extracted.get("primitive_money_legs")
    if legs is None:
        return None, None
    # Deferred import: connector value types must never load at framework import
    # (framework → connector boundary; mirrors the ledger dispatcher's resolver).
    from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLegs

    if not isinstance(legs, PrimitiveMoneyLegs):
        return None, None
    pt_human: Decimal | None = None
    for leg in legs.input_legs:
        if (leg.token or "").strip().upper().startswith("PT-") and leg.amount.is_measured:
            pt_human = leg.amount.value
            break
    sy_human: Decimal | None = None
    output_legs = legs.output_legs
    if output_legs and output_legs[0].amount.is_measured:
        sy_human = output_legs[0].amount.value
    return pt_human, sy_human


def _build_pt_redeem(ctx: _PTContext, basis_store: FIFOBasisStore | None) -> PendleAccountingEvent:
    """Build a ``PT_REDEEM`` event with FIFO-matched realized yield (VIB-4988).

    Detected by a ``WITHDRAW`` intent. PEN6: the PT count and underlying received
    are sourced PREFERENTIALLY from the connector-DECLARED ``primitive_money_legs``
    (INPUT leg = PT token count, OUTPUT leg = underlying received; both human),
    which are basis-identical to the PT_BUY's PT ``amount_out``. Only when legs are
    absent do we fall back to ``redemption_amounts`` — a plain dict
    ``{"py_redeemed": raw, "sy_received": raw}`` (from
    ``PendleReceiptParser.extract_redemption_amounts``), converted raw→HUMAN. Both
    sources store HUMAN on the event (matching ``_replay_pt_redeem``, which —
    unlike PT_BUY/PT_SELL — does NOT divide by 1e18 on replay).

    Sourcing the PT count from legs (not ``redemption_amounts['py_redeemed']``,
    which post-maturity is the SY-ASSET amount ≈ PT × SY-exchange-rate) keeps PT
    quantity conserved through the FIFO ``match_pt_redeem`` so realized yield is
    computed against the right lot size (VIB-4988).

    The PT being redeemed is read from the ledger row's ``token_in`` (the PT
    symbol). R6: if ``token_in`` is empty / non-PT we fall back to the outbox
    position-key's market and degrade confidence rather than mismatch the FIFO
    key. The base/SY USD price comes from ``price_inputs_json`` keyed by the
    redeem's ``token_out`` symbol (the underlying the PT redeemed into).
    """
    pt_token_sym = ctx.ledger_row.get("token_in") or ""
    base_token_sym = ctx.ledger_row.get("token_out") or ""

    redemption = ctx.extracted.get("redemption_amounts")
    py_raw = _get_field(redemption, "py_redeemed")
    sy_raw = _get_field(redemption, "sy_received")

    # PEN6: prefer the DECLARED money legs (PT count on INPUT, underlying on
    # OUTPUT, both human) over redemption_amounts. Fall back to redemption_amounts
    # (raw → human) only when a leg is absent (Empty != Zero).
    legs_pt_human, legs_sy_human = _redeem_amounts_from_legs(ctx.extracted)

    pt_human: Decimal | None = legs_pt_human
    sy_human: Decimal | None = legs_sy_human
    if pt_human is None and py_raw is not None:
        try:
            pt_human = Decimal(str(py_raw)) / _SCALE_18
        except InvalidOperation:
            pt_human = None
    if sy_human is None and sy_raw is not None:
        try:
            sy_human = Decimal(str(sy_raw)) / _SCALE_18
        except InvalidOperation:
            sy_human = None

    identity = _pt_identity(ctx, "PT_REDEEM")

    realized_yield_usd: Decimal | None = None
    basis_lot_id: str | None = None
    confidence = AccountingConfidence.ESTIMATED
    unavailable_reason = "PT redeem amounts unavailable from receipt"

    # R6: degrade (do not mismatch the FIFO key) when token_in is not a PT symbol.
    # The degrade note is set only inside the match block below, so a missing-amounts
    # redeem keeps the "amounts unavailable" reason rather than this FIFO-key note.
    fifo_pt_key = pt_token_sym if pt_token_sym.upper().startswith("PT-") else (pt_token_sym or "PT")

    if pt_human is not None and sy_human is not None and pt_human > 0:
        sy_price = _sy_price_from_ledger(ctx, base_token_sym)
        if basis_store is not None:
            match = basis_store.match_pt_redeem(
                deployment_id=ctx.deployment_id,
                position_key=ctx.position_key,
                pt_token=fifo_pt_key,
                pt_redeemed=pt_human,
                sy_received=sy_human,
            )
            yield_usd, match_reason, match_conf = _realized_yield_from_match(match, sy_human, sy_price)
            realized_yield_usd = yield_usd
            if match.lot_matches:
                basis_lot_id = match.lot_matches[0].lot_id
            # Take the match note when token_in is a real PT- symbol; otherwise the
            # R6 degrade note (the match key may be unreliable).
            if pt_token_sym.upper().startswith("PT-"):
                unavailable_reason = match_reason
                confidence = match_conf
            else:
                unavailable_reason = "PT redeem token_in is not a PT- symbol — FIFO match may be unreliable"
                confidence = AccountingConfidence.ESTIMATED
        else:
            unavailable_reason = "no basis store — realized yield unavailable"

    return PendleAccountingEvent(
        identity=identity,
        event_type=PendleEventType.PT_REDEEM,
        position_key=ctx.position_key,
        market_id=ctx.market_address,
        pt_token=pt_token_sym,
        maturity_timestamp=_parse_pt_maturity(pt_token_sym),
        pt_amount=pt_human,
        sy_amount=sy_human,
        pt_price=None,
        implied_apr_bps=None,
        days_to_maturity=None,
        realized_yield_usd=realized_yield_usd,
        basis_lot_id=basis_lot_id,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )


def handle_pendle_pt(
    outbox_row: dict[str, Any],
    ledger_row: dict[str, Any],
    basis_store: FIFOBasisStore | None = None,
) -> PendleAccountingEvent | None:
    """Build a PendleAccountingEvent (PT_BUY / PT_SELL / PT_REDEEM) from a ledger row.

    Thin dispatcher (VIB-4988): routes by intent_type + PT leg side to one of three
    builders, each independently under the CRAP threshold (so the prior cc=50
    allowlist is dropped):

    * ``WITHDRAW`` → :func:`_build_pt_redeem` (PT → underlying at/after maturity).
    * ``SWAP`` with a ``PT-`` ``token_out`` → :func:`_build_pt_buy` (token → PT).
    * ``SWAP`` with a ``PT-`` ``token_in`` → :func:`_build_pt_sell` (PT → token).
    * anything else → ``None`` (a YT/SY pendle swap falls through to the generic
      SWAP path).

    When ``basis_store`` is provided, PT_BUY records a FIFO lot (HUMAN units, R1)
    and PT_SELL / PT_REDEEM FIFO-match it to attribute realized yield.

    Ordering note: the lot is recorded / matched inside ``_dispatch()`` BEFORE
    ``drain_one`` calls ``writer.write()``. This is safe because FIFOBasisStore is
    in-memory and is reconstructed from accounting_events on restart — a crash
    before the event is persisted loses both the event and the in-memory lot,
    which leaves the store consistent with accounting_events on the next startup.
    """
    intent_type_str = (ledger_row.get("intent_type") or "").upper()
    protocol = (ledger_row.get("protocol") or "").lower()
    if "pendle" not in protocol:
        return None

    token_out = ledger_row.get("token_out") or ""
    token_in = ledger_row.get("token_in") or ""

    if intent_type_str == "WITHDRAW":
        return _build_pt_redeem(_pt_context(outbox_row, ledger_row), basis_store)
    if intent_type_str == "SWAP" and token_out.upper().startswith("PT-"):
        return _build_pt_buy(_pt_context(outbox_row, ledger_row), basis_store)
    if intent_type_str == "SWAP" and token_in.upper().startswith("PT-"):
        return _build_pt_sell(_pt_context(outbox_row, ledger_row), basis_store)
    return None


# ──────────────────────────────────────────────────────────────────────────────
# HandlerContext adapters — the spec's treatments (reached via the registry)
# ──────────────────────────────────────────────────────────────────────────────


def treat_pendle_lp(ctx: HandlerContext) -> PendleAccountingEvent | None:
    """Treatment entry point: a Pendle LP open/close from a ``HandlerContext``."""
    return handle_pendle_lp(ctx.outbox_row, ctx.ledger_row)


def treat_pendle_pt(ctx: HandlerContext) -> PendleAccountingEvent | None:
    """Treatment entry point: a Pendle PT buy from a ``HandlerContext``."""
    return handle_pendle_pt(ctx.outbox_row, ctx.ledger_row, ctx.basis_store)


# ──────────────────────────────────────────────────────────────────────────────
# Categorization — replaces taxonomy.classify's pendle branches (generic category)
# ──────────────────────────────────────────────────────────────────────────────


def _categorize(
    intent_type: str,
    protocol: str,
    token_out: str,
    token_in: str = "",
) -> AccountingCategoryDecision | None:
    """Map a Pendle event to a *generic* AccountingCategory + opaque treatment key.

    Mirrors (and extends) the ``classify()`` branches this connector removes from
    the framework taxonomy:

    * Pendle LP (``LP_OPEN`` / ``LP_CLOSE`` / ``LP_COLLECT_FEES``) → generic
      ``AccountingCategory.LP`` with treatment ``"pendle_lp"`` (so the generic LP
      handler never sees a Pendle LP event — it is routed to the connector
      treatment, which builds a ``PENDLE_LP_OPEN`` / ``PENDLE_LP_CLOSE`` event for
      OPEN/CLOSE and ``None`` for COLLECT_FEES, exactly as today).
    * Pendle PT trade (``SWAP`` with a ``PT-`` ``token_out`` *or* a ``PT-``
      ``token_in``) → generic ``AccountingCategory.SWAP`` with treatment
      ``"pendle_pt"``. A ``PT-`` ``token_out`` is a PT *buy*; a ``PT-`` ``token_in``
      is a PT *sell* on the secondary market (VIB-4988). ``token_in`` is the
      decisive signal that distinguishes a sell from a YT/SY swap and is why the
      categorize signature carries it.
    * Pendle PT redeem (``WITHDRAW``) → generic ``AccountingCategory.SWAP`` with
      treatment ``"pendle_pt"`` (PT → underlying at/after maturity; VIB-4988).

    A Pendle ``SWAP`` whose *neither* leg is a ``PT-`` token (a YT/SY swap) is
    DECLINED (returns ``None``) so it books as a generic SWAP via the framework's
    stage-2 path — no special PT yield attribution applies.

    Returns ``None`` for every non-Pendle event (the registry then falls through
    to the generic category path).
    """
    if "pendle" not in protocol.lower():
        return None
    it = intent_type.upper()
    if it in _PENDLE_LP_INTENTS:
        return AccountingCategoryDecision(category=AccountingCategory.LP, treatment_key="pendle_lp")
    if it == "WITHDRAW":
        return AccountingCategoryDecision(category=AccountingCategory.SWAP, treatment_key="pendle_pt")
    if it == "SWAP" and (token_out.upper().startswith("PT-") or token_in.upper().startswith("PT-")):
        return AccountingCategoryDecision(category=AccountingCategory.SWAP, treatment_key="pendle_pt")
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Outbox position-key derivation (relocated from pendle_accounting.py)
# ──────────────────────────────────────────────────────────────────────────────


def _derive_pendle_position_key(chain: str, wallet: str, market_address: str) -> str:
    """Canonical position key for a Pendle LP position."""
    return f"pendle_lp:{chain.lower()}:{wallet.lower()}:{market_address.lower()}"


def _get_market_address(intent: Any) -> str:
    """Extract the Pendle market address from the intent pool field.

    LP_OPEN pool format is "TOKEN/0xmarket_address"; LP_CLOSE is bare "0xmarket_address".
    Parses out the address portion in both cases. Returns empty string when the market
    address cannot be resolved — callers should guard against empty values.
    """
    pool = getattr(intent, "pool", None)
    if not pool:
        return ""
    # Lowercase up front so the 0x prefix check is case-insensitive (an uppercase
    # "0X..." address is still valid hex) — Gemini review on #2598.
    pool_str = str(pool).strip().lower()
    if "/" in pool_str:
        pool_str = pool_str.split("/", 1)[1].strip()
    return pool_str if pool_str.startswith("0x") else ""


def _position_key(
    *,
    protocol: str,
    intent_type: str,
    chain: str,
    wallet: str,
    intent: Any,
) -> tuple[str, str] | None:
    """Derive the outbox ``(position_key, market_id)`` for a Pendle LP / PT event.

    Byte-identical to the Pendle branches the runner's
    ``_compute_outbox_position_key`` removes (VIB-4931): LP open/close key on
    ``pendle_lp:<chain>:<wallet>:<market>`` (market parsed from ``intent.pool``); PT
    swaps key on ``pendle_pt:<chain>:<wallet>:<market>`` (market = ``intent.pool``).
    Returns ``None`` for every event this connector does not own (the runner then
    uses its generic derivation).
    """
    if "pendle" not in protocol.lower():
        return None
    t = intent_type.upper()
    if t in {"LP_OPEN", "LP_CLOSE"}:
        market_address = _get_market_address(intent)
        position_key = _derive_pendle_position_key(chain, wallet, market_address) if market_address else ""
        return position_key, market_address
    # PT buy/sell (SWAP) keys on the normalized PT symbol — NOT the market
    # address: a Pendle ``SwapIntent`` carries no pool/market, and the resolved
    # market is never persisted on the ledger row, so a market-derived key was
    # empty in practice (which broke the position_events ↔ accounting_events join
    # AND the FIFO realized-yield match). The PT symbol (``to_token`` on a buy,
    # ``from_token`` on a sell) is present on the SwapIntent here AND on the
    # ledger row, making this byte-identical to
    # observability/position_events.py:_pendle_pt_event and to the
    # ``_pt_context`` fallback. ``market_id`` is surfaced best-effort.
    if t == "SWAP":
        from_token = (getattr(intent, "from_token", "") or "").strip()
        to_token = (getattr(intent, "to_token", "") or "").strip()
        if to_token.upper().startswith("PT-"):
            pt_symbol = to_token
        elif from_token.upper().startswith("PT-"):
            pt_symbol = from_token
        else:
            return None  # YT/SY swap — not a PT position action; runner uses generic SWAP key
        market_address = _get_market_address(intent) or (getattr(intent, "pool", None) or "").lower()
        position_key = f"pendle_pt:{chain.lower()}:{wallet.lower()}:{pt_symbol.strip().lower()}"
        return position_key, market_address
    if t == "WITHDRAW":
        # PT redeem at maturity. Pendle OWNS this event, but a ``WithdrawIntent``
        # names only the underlying ``token`` + YT ``market_id`` — it carries NO
        # from/to token — so the PT symbol is NOT resolvable at outbox time.
        # Return an owned-but-EMPTY key (a non-None tuple) so the runner does NOT
        # fall through to the generic lending branch; ``_pt_context`` then derives
        # the canonical ``pendle_pt:<chain>:<wallet>:<pt-symbol>`` key from the
        # ledger row's PT leg (stamped by the receipt parser, G-PT redeem-symbol),
        # which is the SAME identity the PT_BUY lot opened under.
        market_address = _get_market_address(intent) or (getattr(intent, "market_id", "") or "").lower()
        return "", market_address
    return None


#: Connector-published accounting treatment spec resolved by
#: ``AccountingTreatmentRegistry`` via ``CONNECTOR.accounting_treatment``.
ACCOUNTING_TREATMENT_SPEC = AccountingTreatmentSpec(
    categorize=_categorize,
    treatments={"pendle_lp": treat_pendle_lp, "pendle_pt": treat_pendle_pt},
    claims_event_types=frozenset({"LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES", "SWAP", "WITHDRAW"}),
    position_key=_position_key,
)
