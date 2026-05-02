"""Per-layer typed-column FIELD_MAPs (AttemptNo17 §3 D2(a)).

Each accounting write layer (transaction_ledger, position_events,
portfolio_snapshots, accounting_events) declares a FIELD_MAP that names,
for every typed column the layer owns, where the source value lives:

- ``EXTRACTED`` — pull from ``extracted_data_json`` at a JSON path.
- ``PRE_STATE`` / ``POST_STATE`` — pull from the parsed pre/post state.
- ``PRICES`` — pull from ``price_inputs_json`` at a token symbol/address.
- ``DERIVED`` — call a pure derivation function over the row's inputs.
- ``CONST`` — a hardcoded value (e.g. schema_version).
- ``RECEIPT`` — direct from the receipt dataclass.

The writer at each layer iterates the map and calls
``populate_typed_columns(row, inputs, FIELD_MAP)``. Required-but-missing
entries are loud warnings + ``confidence=ESTIMATED`` — never silent NULL.

Adding a typed column without adding a FIELD_MAP entry fails the
Accountant Test contract (test fixture asserts ``set(row.typed_columns)
== set(FIELD_MAP)``).

This is the formal expression of the mapping rule we kept calling out as
"there is no enforced mapping" in AttemptNo17 §0.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

from almanak.framework.accounting.payload_schemas import (
    FORMULA_VERSION,
    MATCHING_POLICY_VERSION,
    SCHEMA_VERSION,
)

logger = logging.getLogger(__name__)


SourceKind = Literal["EXTRACTED", "PRE_STATE", "POST_STATE", "PRICES", "DERIVED", "CONST", "RECEIPT"]


@dataclass(frozen=True)
class FieldSource:
    """Declarative source for a typed column."""

    kind: SourceKind
    path: str | None = None  # JSON path (dot.notation) for EXTRACTED / PRE / POST / PRICES
    derive: Callable[..., Any] | None = None  # function for DERIVED
    value: Any = None  # for CONST
    required: bool = False  # always-required (e.g. schema_version)
    required_for: tuple[str, ...] = ()  # required for these intent_types only


@dataclass
class _PopulationResult:
    """What populate_typed_columns produces — never raises, always returns."""

    populated: dict[str, Any] = field(default_factory=dict)
    missing_required: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def has_missing(self) -> bool:
        return bool(self.missing_required)


def _path_get(d: dict[str, Any] | None, path: str | None) -> Any:
    if d is None or not path:
        return None
    cur: Any = d
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def populate_typed_columns(
    row: dict[str, Any],
    inputs: dict[str, Any],
    field_map: dict[str, FieldSource],
    *,
    intent_type: str = "",
    layer_name: str = "",
) -> _PopulationResult:
    """Apply ``field_map`` to ``inputs``, writing into ``row`` in place.

    ``inputs`` is a dict with the layer's available source dicts:

    - ``extracted``: dict — parsed from ``extracted_data_json``
    - ``pre_state``: dict — parsed from ``pre_state_json``
    - ``post_state``: dict — parsed from ``post_state_json``
    - ``prices``: dict — parsed from ``price_inputs_json``
    - ``receipt``: any — the receipt dataclass (for RECEIPT source)
    - ``derive_kwargs``: dict — kwargs forwarded to DERIVED functions

    Returns a ``_PopulationResult`` listing missing-but-required columns and
    notes. Missing columns are NOT written; the row keeps whatever default
    the dataclass had (typically ``None`` or ``""``) — the contract is
    "either populate it or report missing", never silently zero.
    """
    result = _PopulationResult()
    extracted = inputs.get("extracted") or {}
    pre_state = inputs.get("pre_state") or {}
    post_state = inputs.get("post_state") or {}
    prices = inputs.get("prices") or {}
    receipt = inputs.get("receipt")
    derive_kwargs = inputs.get("derive_kwargs") or {}

    for col, src in field_map.items():
        val: Any = None
        if src.kind == "CONST":
            val = src.value
        elif src.kind == "EXTRACTED":
            val = _path_get(extracted, src.path)
        elif src.kind == "PRE_STATE":
            val = _path_get(pre_state, src.path)
        elif src.kind == "POST_STATE":
            val = _path_get(post_state, src.path)
        elif src.kind == "PRICES":
            val = _path_get(prices, src.path)
        elif src.kind == "RECEIPT":
            val = getattr(receipt, src.path, None) if (receipt is not None and src.path) else None
        elif src.kind == "DERIVED" and src.derive is not None:
            try:
                val = src.derive(**derive_kwargs)
            except Exception as e:
                result.notes.append(f"{col}: DERIVED failed: {e}")
                val = None

        if val is not None and val != "":
            row[col] = val
            continue

        is_required_now = src.required or (intent_type and intent_type in src.required_for)
        if is_required_now:
            result.missing_required.append(col)
            logger.warning(
                "typed-column missing-required: layer=%s intent_type=%s col=%s source=%s path=%s",
                layer_name,
                intent_type,
                col,
                src.kind,
                src.path,
            )
    return result


# ─── Layer 1: transaction_ledger ──────────────────────────────────────────
#
# These are the columns the SQLite/Postgres DDL exposes. The dataclass is
# `LedgerEntry` in observability/ledger.py. Source paths reference the
# `extracted_data_json` shapes the connector receipt parsers produce
# (SwapAmounts, LPOpenData, LPCloseData, etc.) — see receipts.py for the
# canonical set.

LEDGER_FIELD_MAP: dict[str, FieldSource] = {
    # Always-required versioning
    "schema_version": FieldSource(kind="CONST", value=SCHEMA_VERSION, required=True),
    "formula_version": FieldSource(kind="CONST", value=FORMULA_VERSION, required=True),
    "matching_policy_version": FieldSource(kind="CONST", value=MATCHING_POLICY_VERSION, required=True),
    # Swap-specific
    "amount_in": FieldSource(kind="EXTRACTED", path="swap_amounts.amount_in_decimal", required_for=("SWAP",)),
    "amount_out": FieldSource(kind="EXTRACTED", path="swap_amounts.amount_out_decimal", required_for=("SWAP",)),
    "effective_price": FieldSource(kind="EXTRACTED", path="swap_amounts.effective_price", required_for=("SWAP",)),
    "slippage_bps": FieldSource(kind="EXTRACTED", path="swap_amounts.slippage_bps", required_for=("SWAP",)),
    # Gas USD comes through `compute_gas_usd` in the runner — the LedgerEntry
    # dataclass already carries gas_usd, this entry is the contract that says
    # "if it's empty after build_ledger_entry runs, that's a missing-required
    # warning for chains whose native token is in the oracle."
    "gas_usd": FieldSource(kind="DERIVED", required=True),
}


# ─── Layer 3: position_events ─────────────────────────────────────────────

POSITION_EVENT_FIELD_MAP: dict[str, FieldSource] = {
    "schema_version": FieldSource(kind="CONST", value=SCHEMA_VERSION, required=True),
    "formula_version": FieldSource(kind="CONST", value=FORMULA_VERSION, required=True),
    "matching_policy_version": FieldSource(kind="CONST", value=MATCHING_POLICY_VERSION, required=True),
    # LP
    "tick_lower": FieldSource(kind="EXTRACTED", path="lp_open_data.tick_lower", required_for=("LP_OPEN",)),
    "tick_upper": FieldSource(kind="EXTRACTED", path="lp_open_data.tick_upper", required_for=("LP_OPEN",)),
    "liquidity": FieldSource(kind="EXTRACTED", path="lp_open_data.liquidity", required_for=("LP_OPEN",)),
    "fees_token0": FieldSource(
        kind="EXTRACTED",
        path="lp_close_data.fees0",
        required_for=("LP_CLOSE",),
    ),
    "fees_token1": FieldSource(
        kind="EXTRACTED",
        path="lp_close_data.fees1",
        required_for=("LP_CLOSE",),
    ),
    # Perp
    "is_long": FieldSource(kind="EXTRACTED", path="perp_data.is_long", required_for=("PERP_OPEN", "PERP_CLOSE")),
    "leverage": FieldSource(kind="EXTRACTED", path="perp_data.leverage", required_for=("PERP_OPEN",)),
    "entry_price": FieldSource(kind="EXTRACTED", path="perp_data.entry_price", required_for=("PERP_OPEN",)),
    "mark_price": FieldSource(kind="POST_STATE", path="mark_price", required_for=("PERP_CLOSE",)),
    # Required for every event with a corresponding open/close pair —
    # the silent-Decimal(0) failure mode the May 1 audit exposed cannot recur.
    "unrealized_pnl": FieldSource(
        kind="DERIVED",
        required_for=("LP_OPEN", "LP_CLOSE", "PERP_OPEN", "PERP_CLOSE"),
    ),
}


# ─── Layer 5: accounting_events (frozen-payload contract) ────────────────
#
# Layer 5's typed columns are MINIMAL today (only versioning + confidence);
# everything else lives in payload_json. The frozen pydantic models in
# payload_schemas.py ARE the typed-column contract for this layer until
# Track B's metrics-database migration adds projection columns.
#
# `populate_accounting_event_payload(payload, event_type)` validates the
# payload against the frozen model and surfaces drift as an error.

ACCOUNTING_EVENT_FIELD_MAP: dict[str, FieldSource] = {
    "schema_version": FieldSource(kind="CONST", value=SCHEMA_VERSION, required=True),
    # confidence MUST be set by the writer — never default to HIGH silently.
    "confidence": FieldSource(kind="DERIVED", required=True),
}


def populate_accounting_event_payload(payload: dict[str, Any], event_type: str) -> tuple[dict[str, Any], list[str]]:
    """Validate a payload against the frozen pydantic model.

    Returns ``(validated_payload_dict, errors)``. Errors is a list of
    human-readable validation messages. When the event_type isn't in the v1
    surface, returns the payload unchanged with no errors.
    """
    from almanak.framework.accounting.payload_schemas import (
        is_v1_event_type,
        validate_payload,
    )

    if not is_v1_event_type(event_type):
        return payload, []
    try:
        model = validate_payload(event_type, payload)
        if model is None:
            return payload, []
        return model.model_dump(mode="json"), []
    except ValueError as e:
        return payload, [str(e)]


__all__ = [
    "ACCOUNTING_EVENT_FIELD_MAP",
    "FieldSource",
    "LEDGER_FIELD_MAP",
    "POSITION_EVENT_FIELD_MAP",
    "populate_accounting_event_payload",
    "populate_typed_columns",
]
