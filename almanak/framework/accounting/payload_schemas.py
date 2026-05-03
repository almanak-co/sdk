"""Frozen pydantic models for ``accounting_events.payload_json`` (AttemptNo17 §3 D2(b)).

Why frozen pydantic? `accounting_events.payload_json` has no projection
columns today (Track B) and the metrics-database team owns the schema.
Until that migration ships, the only safe way to read lending/perp typed
fields out of payload is to validate against a frozen model per
`event_type`. Drift in either direction (writer adds an unmodelled field,
reader expects one the writer dropped) fails loudly.

The Accountant Test reads `accounting_events.payload_json` ONLY through
these models. That is the "validated typed-payload reads" rail in
AttemptNo17 §1.1.

Versioning (AttemptNo17 §1.0a): every payload carries `schema_version`,
`formula_version`, and `matching_policy_version`. v1 = 1 across the board.
Bumping any of them triggers a separate Accountant Test score keyed by
that version triple — historical comparisons require re-running the test
under the new versions.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal

try:
    from pydantic import BaseModel, ConfigDict, model_validator
except ImportError:  # pragma: no cover — pydantic is a hard dep
    raise

ConfidenceLiteral = Literal["HIGH", "ESTIMATED", "STALE", "UNAVAILABLE"]

# Version constants (AttemptNo17 §1.0a). Bumping any of these requires re-
# running the Accountant Test against the new version triple.
SCHEMA_VERSION = 1
FORMULA_VERSION = 1
MATCHING_POLICY_VERSION = 1  # 1 = FIFO hardcoded


class _Base(BaseModel):
    """Shared config for all payload models.

    Frozen + ``extra="ignore"`` (not ``forbid``) because the typed event writers
    emit additional protocol-specific fields (e.g. ``LPAccountingEvent`` ships
    ``lp_token_amount``, ``fees0_collected``; ``LendingAccountingEvent`` ships
    ``collateral_value_*``, ``debt_value_*``, etc.) that are NOT in the v1 spec
    surface. Rejecting those would fail every real-run payload at the
    Accountant Test boundary (Codex P1, 2026-05-02).

    The "fail loudly on drift" intent is preserved at field level — required
    spec fields with no default still fail validation when missing/wrong-typed.
    Unknown extras are dropped during ``model_dump()`` so cells read a
    canonical shape regardless of which writer produced the row.
    """

    model_config = ConfigDict(frozen=True, extra="ignore", arbitrary_types_allowed=True)


class _Versioned(_Base):
    schema_version: int = SCHEMA_VERSION
    formula_version: int = FORMULA_VERSION
    matching_policy_version: int = MATCHING_POLICY_VERSION

    @model_validator(mode="after")
    def _enforce_confidence_exclusivity(self) -> _Versioned:
        """Reject ``confidence=HIGH`` with a non-empty ``unavailable_reason``.

        VIB-3886 (CONF). The May 2 LP_OPEN payload reported
        ``confidence=HIGH`` AND ``unavailable_reason="cost_basis_usd
        unavailable: ..."`` simultaneously — a contradiction that hid the
        upstream pricing failure under a misleading "data is fine" tag.
        SWAP-handler already degraded to ESTIMATED in the same scenario,
        so the fix is to make the contradiction unrepresentable. Models
        whose subclasses don't declare ``confidence`` (none today, but
        future v1 additions might) are silently ignored via ``getattr``.
        """
        confidence = getattr(self, "confidence", None)
        reason = getattr(self, "unavailable_reason", None)
        if confidence == "HIGH" and reason:
            raise ValueError(
                "confidence=HIGH is incompatible with a non-empty "
                f"unavailable_reason ({reason!r}). Set confidence=ESTIMATED "
                "(or STALE / UNAVAILABLE) when any USD field is missing."
            )
        return self


# ─── Lending ──────────────────────────────────────────────────────────────


class SupplyEventPayload(_Versioned):
    event_type: Literal["SUPPLY"] = "SUPPLY"
    protocol: str
    asset: str
    amount: Decimal
    amount_usd: Decimal | None = None
    supply_apr_pct: Decimal | None = None
    health_factor_after: Decimal | None = None
    confidence: ConfidenceLiteral
    unavailable_reason: str | None = None
    cost_basis_usd: Decimal | None = None
    position_key: str | None = None


class WithdrawEventPayload(_Versioned):
    event_type: Literal["WITHDRAW"] = "WITHDRAW"
    protocol: str
    asset: str
    amount: Decimal
    amount_usd: Decimal | None = None
    interest_accrued_usd: Decimal | None = None
    # Legacy field name preserved for grep compatibility — `_project_lending_aliases`
    # projects this onto `interest_accrued_usd` for L4 readers, but the legacy
    # name is still emitted by `LendingAccountingEvent.to_payload_json()` and
    # would otherwise trip the model's `extra="forbid"` policy.
    interest_delta_usd: Decimal | None = None
    realized_pnl_usd: Decimal | None = None
    health_factor_after: Decimal | None = None
    confidence: ConfidenceLiteral
    unavailable_reason: str | None = None
    position_key: str | None = None


class BorrowEventPayload(_Versioned):
    event_type: Literal["BORROW"] = "BORROW"
    protocol: str
    asset: str
    borrowed_amount: Decimal
    borrowed_amount_usd: Decimal | None = None
    borrow_apr_pct: Decimal | None = None
    health_factor_after: Decimal | None = None
    confidence: ConfidenceLiteral
    unavailable_reason: str | None = None
    position_key: str | None = None


class RepayEventPayload(_Versioned):
    event_type: Literal["REPAY", "DELEVERAGE"] = "REPAY"
    protocol: str
    asset: str
    amount: Decimal
    amount_usd: Decimal | None = None
    principal_repaid: Decimal | None = None  # L4: principal vs interest split
    interest_paid: Decimal | None = None
    principal_repaid_usd: Decimal | None = None
    interest_paid_usd: Decimal | None = None
    # Legacy field names preserved for grep compatibility —
    # `_project_lending_aliases` projects these onto `principal_repaid_usd` /
    # `interest_paid_usd` for L4 readers. The frozen model must still accept
    # the legacy names because `LendingAccountingEvent.to_payload_json()`
    # emits them and `extra="forbid"` would otherwise reject the row at the
    # writer's validate-then-persist boundary.
    principal_delta_usd: Decimal | None = None
    interest_delta_usd: Decimal | None = None
    health_factor_after: Decimal | None = None
    confidence: ConfidenceLiteral
    unavailable_reason: str | None = None
    position_key: str | None = None


# ─── LP ───────────────────────────────────────────────────────────────────


class LPOpenEventPayload(_Versioned):
    event_type: Literal["LP_OPEN"] = "LP_OPEN"
    protocol: str
    position_key: str
    pool_address: str
    token0: str
    token1: str
    amount0: Decimal
    amount1: Decimal
    amount0_usd: Decimal | None = None
    amount1_usd: Decimal | None = None
    cost_basis_usd: Decimal | None = None
    tick_lower: int | None = None
    tick_upper: int | None = None
    liquidity: int | None = None
    # VIB-3893 — current_tick and in_range stamped at OPEN so the Trade Tape
    # can render "in-range YES/NO" without re-querying the chain. Both nullable
    # because non-Uniswap-V3 LP venues may not expose a tick-bracket model.
    current_tick: int | None = None
    in_range: bool | None = None
    confidence: ConfidenceLiteral
    unavailable_reason: str | None = None


class LPCloseEventPayload(_Versioned):
    event_type: Literal["LP_CLOSE"] = "LP_CLOSE"
    protocol: str
    position_key: str
    pool_address: str
    token0: str
    token1: str
    amount0: Decimal
    amount1: Decimal
    amount0_usd: Decimal | None = None
    amount1_usd: Decimal | None = None
    fees0_collected: Decimal | None = None
    fees1_collected: Decimal | None = None
    fees_total_usd: Decimal | None = None
    realized_pnl_usd: Decimal | None = None
    il_usd: Decimal | None = None  # diagnostic only — see G6 / LP4 / LP5
    hodl_value_usd: Decimal | None = None
    confidence: ConfidenceLiteral
    unavailable_reason: str | None = None


# ─── Perp ─────────────────────────────────────────────────────────────────


class PerpOpenEventPayload(_Versioned):
    event_type: Literal["PERP_OPEN"] = "PERP_OPEN"
    protocol: str
    position_key: str
    market: str  # canonical market identifier (e.g. "ARB-USDC")
    is_long: bool
    size: Decimal
    leverage: Decimal | None = None
    entry_price: Decimal | None = None
    open_fee_usd: Decimal | None = None
    price_impact_usd: Decimal | None = None
    cost_basis_usd: Decimal | None = None
    confidence: ConfidenceLiteral
    unavailable_reason: str | None = None


class PerpCloseEventPayload(_Versioned):
    event_type: Literal["PERP_CLOSE"] = "PERP_CLOSE"
    protocol: str
    position_key: str
    market: str
    is_long: bool
    size: Decimal
    exit_price: Decimal | None = None
    close_fee_usd: Decimal | None = None
    price_impact_usd: Decimal | None = None
    funding_paid_usd: Decimal | None = None
    funding_received_usd: Decimal | None = None
    realized_pnl_usd: Decimal | None = None
    confidence: ConfidenceLiteral
    unavailable_reason: str | None = None


# ─── Swap ─────────────────────────────────────────────────────────────────


class SwapEventPayload(_Versioned):
    event_type: Literal["SWAP"] = "SWAP"
    protocol: str
    token_in: str
    token_out: str
    amount_in: Decimal
    amount_out: Decimal
    amount_in_usd: Decimal | None = None
    amount_out_usd: Decimal | None = None
    effective_price: Decimal | None = None
    slippage_bps: Decimal | None = None
    realized_pnl_usd: Decimal | None = None
    cost_basis_recorded: bool | None = None
    gas_usd: Decimal | None = None
    confidence: ConfidenceLiteral
    unavailable_reason: str | None = None
    swap_position_key: str | None = None


# ─── Validation registry ───────────────────────────────────────────────────


_PAYLOAD_MODELS: dict[str, type[_Versioned]] = {
    "SUPPLY": SupplyEventPayload,
    "WITHDRAW": WithdrawEventPayload,
    "BORROW": BorrowEventPayload,
    "REPAY": RepayEventPayload,
    # DELEVERAGE shares the REPAY payload shape — `_project_lending_aliases`
    # treats them identically and the spec fields (principal_repaid_usd /
    # interest_paid_usd) are produced by the same projection. Without this
    # entry, the v1 validation rail silently skips DELEVERAGE rows
    # (`is_v1_event_type("DELEVERAGE") == False`) and contract drift on
    # deleverage payloads would never surface.
    "DELEVERAGE": RepayEventPayload,
    "LP_OPEN": LPOpenEventPayload,
    "LP_CLOSE": LPCloseEventPayload,
    "PERP_OPEN": PerpOpenEventPayload,
    "PERP_CLOSE": PerpCloseEventPayload,
    "SWAP": SwapEventPayload,
}


def validate_payload(event_type: str, payload: dict[str, Any]) -> _Versioned | None:
    """Validate a raw `payload_json` dict against the frozen model.

    Returns None when the event_type is not in the v1 surface (e.g. PENDLE,
    POLYMARKET) — those primitives are tracked under v2 placeholder
    tickets per AttemptNo17 §8.5 and do NOT fail the Accountant Test;
    they're just out of v1 scope.

    Raises ``ValueError`` (wrapping pydantic's ValidationError) when the
    event_type IS in the v1 surface but the payload doesn't match — that's
    a contract drift the Accountant Test must surface.
    """
    model = _PAYLOAD_MODELS.get(event_type)
    if model is None:
        return None
    try:
        return model.model_validate(payload)
    except Exception as e:
        raise ValueError(f"payload schema mismatch for {event_type}: {e}") from e


def is_v1_event_type(event_type: str) -> bool:
    """True iff this event_type is part of the v1 surface."""
    return event_type in _PAYLOAD_MODELS
