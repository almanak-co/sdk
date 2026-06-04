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

import logging
import re
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
    from almanak.framework.accounting.basis import FIFOBasisStore
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


# crap-allowlist: VIB-4988 — verbatim relocation of a pre-existing cc=50 money-path
# handler; cc>30 is not coverage-fixable, and a multi-helper decomposition is tracked
# separately under the crap-refactor protocol (do not refactor it from this move PR).
def handle_pendle_pt(  # noqa: C901
    outbox_row: dict[str, Any],
    ledger_row: dict[str, Any],
    basis_store: FIFOBasisStore | None = None,
) -> PendleAccountingEvent | None:
    """Build a PendleAccountingEvent(PT_BUY) from a ledger row.

    Reads swap_amounts from deserialized extracted_data_json.
    token_out is from the ledger row's token_out column (PT symbol).

    When basis_store is provided, records a FIFO PT lot so PT_REDEEM can
    match the original cost basis.

    Ordering note: the lot is recorded inside _dispatch() BEFORE drain_one
    calls writer.write().  This is safe because FIFOBasisStore is in-memory
    and is reconstructed from accounting_events on restart — a crash before
    the event is persisted loses both the event and the in-memory lot, which
    leaves the store consistent with accounting_events on the next startup.
    """
    from almanak.framework.observability.ledger import deserialize_extracted_data

    intent_type_str = (ledger_row.get("intent_type") or "").upper()
    if intent_type_str != "SWAP":
        return None

    protocol = (ledger_row.get("protocol") or "").lower()
    if "pendle" not in protocol:
        return None

    pt_token_sym = ledger_row.get("token_out") or ""
    if not pt_token_sym.upper().startswith("PT-"):
        return None

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
        now = datetime.fromisoformat(ts_str) if ts_str else datetime.now(UTC)
    except (ValueError, AttributeError):
        now = datetime.now(UTC)

    extracted = deserialize_extracted_data(ledger_row.get("extracted_data_json") or "")

    # ── Amounts from swap_amounts ────────────────────────────────────────────
    sy_amount: Decimal | None = None
    pt_amount: Decimal | None = None
    swap_amounts = extracted.get("swap_amounts")
    if swap_amounts is not None:
        raw_in = _get_field(swap_amounts, "amount_in")
        raw_out = _get_field(swap_amounts, "amount_out")
        if raw_in is not None:
            try:
                sy_amount = Decimal(str(raw_in))
            except InvalidOperation:
                pass
        if raw_out is not None:
            try:
                pt_amount = Decimal(str(raw_out))
            except InvalidOperation:
                pass

    # ── PT price, maturity, implied APR ─────────────────────────────────────
    pt_price: Decimal | None = None
    if sy_amount and pt_amount and pt_amount > 0:
        try:
            pt_price = sy_amount / pt_amount
        except (InvalidOperation, ZeroDivisionError):
            pass

    maturity_ts = _parse_pt_maturity(pt_token_sym)
    days_to_maturity: int | None = None
    if maturity_ts is not None:
        days_to_maturity = (maturity_ts.date() - now.date()).days

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

    market_address = outbox_row.get("market_id") or ""
    if not position_key and market_address and wallet_address:
        position_key = f"pendle_pt:{chain.lower()}:{wallet_address.lower()}:{market_address}"

    _id_seed = tx_hash or ledger_entry_id or position_key
    identity = AccountingIdentity(
        id=make_accounting_event_id(deployment_id, cycle_id, "PT_BUY", _id_seed, position_key),
        deployment_id=deployment_id,
        cycle_id=cycle_id,
        execution_mode=execution_mode,
        timestamp=now,
        chain=chain,
        protocol=protocol,
        wallet_address=wallet_address,
        tx_hash=tx_hash,
        ledger_entry_id=ledger_entry_id,
    )

    event = PendleAccountingEvent(
        identity=identity,
        event_type=PendleEventType.PT_BUY,
        position_key=position_key,
        market_id=market_address,
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

    # Record FIFO PT lot so PT_REDEEM can compute realized yield on the same run.
    if basis_store is not None and pt_amount is not None and sy_amount is not None and pt_amount > 0:
        pt_token_key = pt_token_sym or "PT"
        basis_store.record_pt_buy(
            deployment_id=deployment_id,
            position_key=position_key,
            pt_token=pt_token_key,
            pt_amount=pt_amount,
            sy_cost=sy_amount,
            timestamp=now,
            lot_id=identity.id,
            source_ledger_entry_id=ledger_entry_id,
        )

    return event


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


def _categorize(intent_type: str, protocol: str, token_out: str) -> AccountingCategoryDecision | None:
    """Map a Pendle event to a *generic* AccountingCategory + opaque treatment key.

    Mirrors the two ``classify()`` branches this connector removes from the
    framework taxonomy:

    * Pendle LP (``LP_OPEN`` / ``LP_CLOSE`` / ``LP_COLLECT_FEES``) → generic
      ``AccountingCategory.LP`` with treatment ``"pendle_lp"`` (so the generic LP
      handler never sees a Pendle LP event — it is routed to the connector
      treatment, which builds a ``PENDLE_LP_OPEN`` / ``PENDLE_LP_CLOSE`` event for
      OPEN/CLOSE and ``None`` for COLLECT_FEES, exactly as today).
    * Pendle PT buy (``SWAP`` with a ``PT-`` ``token_out``) → generic
      ``AccountingCategory.SWAP`` with treatment ``"pendle_pt"``.

    Returns ``None`` for every non-Pendle event (the registry then falls through
    to the generic category path).
    """
    if "pendle" not in protocol.lower():
        return None
    it = intent_type.upper()
    if it in _PENDLE_LP_INTENTS:
        return AccountingCategoryDecision(category=AccountingCategory.LP, treatment_key="pendle_lp")
    if it == "SWAP" and token_out.upper().startswith("PT-"):
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
    if t == "SWAP":
        market_address = (getattr(intent, "pool", None) or "").lower()
        position_key = f"pendle_pt:{chain.lower()}:{wallet.lower()}:{market_address}" if market_address else ""
        return position_key, market_address
    return None


#: Connector-published accounting treatment spec resolved by
#: ``AccountingTreatmentRegistry`` (``_SPEC_LOADERS["pendle"]``).
ACCOUNTING_TREATMENT_SPEC = AccountingTreatmentSpec(
    categorize=_categorize,
    treatments={"pendle_lp": treat_pendle_lp, "pendle_pt": treat_pendle_pt},
    claims_event_types=frozenset({"LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES", "SWAP"}),
    position_key=_position_key,
)
