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
`formula_version`, `matching_policy_version`, and `primitive_version`. v1 = 1
across the board. Bumping any of them triggers a separate Accountant Test
score keyed by that version tuple — historical comparisons require re-running
the test under the new versions.

Bump policy for `primitive_version` (VIB-4166, T6 of VIB-4160):
    Bump :data:`PRIMITIVE_VERSIONS[primitive]` ONLY when the primitive's
    semantics change — i.e. when the SET of fields the primitive emits, the
    LIFECYCLE STATES it tracks, or the FINANCIAL INVARIANTS it enforces
    change. Examples that warrant a bump:

      * LP_CLOSE starts emitting per-token fee accruals it didn't before.
      * BRIDGE adds destination-side observation (PENDING → SETTLED → ...).
      * LIQUIDATE emerges from placeholder to a real implemented primitive.
      * CDP gains leverage-normalisation that changes how `principal_*`
        columns are derived.

    Examples that DO NOT warrant a bump:

      * Classifier tweaks that re-route an existing event_type.
      * Receipt-parser fixes that recover a previously-empty field.
      * New event_type strings WITHIN an existing primitive (e.g. LP_REBALANCE
        added to LP — same fields, same lifecycle, just one more transition).
      * Fee structure changes from the protocol side that flow through the
        existing fields untouched.

    Procedure (mirrors :data:`MATCHING_POLICY_VERSIONS`): edit the
    per-primitive int below, regenerate ONLY the affected primitive's
    Accountant Test fixture, no global re-baselining of sibling primitives.
    The writer's per-primitive lookup at the augment chokepoint guarantees
    a bump in one primitive cannot contaminate another.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal

try:
    from pydantic import BaseModel, ConfigDict, Field, model_validator
except ImportError:  # pragma: no cover — pydantic is a hard dep
    raise

from almanak.framework.primitives.types import Primitive

ConfidenceLiteral = Literal["HIGH", "ESTIMATED", "STALE", "UNAVAILABLE"]

# Version constants (AttemptNo17 §1.0a). Bumping any of these requires re-
# running the Accountant Test against the new version triple.
SCHEMA_VERSION = 1
FORMULA_VERSION = 1
# v3 (VIB-3964): wallet-basis store also mints swap-key acquisition lots on
# BORROW / WITHDRAW (and consumes them on SUPPLY / REPAY) so the wallet basis
# pool mirrors actual on-chain wallet flow. Pre-v3 events left swap-of-borrow
# disposals with realized_pnl_usd=null, breaking G6 looping reconciliation.
# Kept in lock-step with ``almanak.framework.accounting.basis.MATCHING_POLICY_VERSION``
# (CodeRabbit 2026-05-04): the live writer stamps this onto persisted payloads
# and the basis store stamps the same value onto MatchResult, so a payload-vs-
# MatchResult version mismatch would silently misroute Accountant Test scoring.
#
# VIB-4162 (T2): kept for back-compat (live writer reads the global on the
# fallback path; legacy payloads on disk are stamped at this value). New
# code should consult :data:`MATCHING_POLICY_VERSIONS` so per-primitive
# advances do not require a global bump.
MATCHING_POLICY_VERSION = 3

# VIB-4162 (T2): per-primitive matching_policy_version map.
#
# Each primitive's lot-matching algorithm advances independently — bumping
# LP from v3 to v4 (e.g. on a new IL decomposition) MUST NOT force-restamp
# every Lending or Perp event. The ``writer.augment_accounting_payload``
# chokepoint reads ``record.primitive`` for the event_type and stamps the
# corresponding per-primitive version onto the payload. The Accountant
# Test's G13 cell (``_cell_g13_lot_matching``) verifies *per-primitive*
# uniqueness (one version per primitive bucket) instead of "one version
# across all events", so a Lending bump cannot regress LP scoring.
#
# Initial values (precursor parity):
#   LP            v3   (was global v3 pre-T2)
#   LENDING       v3   (was global v3 pre-T2)
#   PERP          v1   (the global v3 was lending-specific; perp was always v1)
#   CDP           v1   (split from LENDING in VIB-4248; greenfield primitive)
#   LIQUIDATION   v1   (split from LENDING in VIB-4248; greenfield primitive)
#   UTILITY/SWAP/VAULT/STAKING/BRIDGE/PREDICTION/FLASH_LOAN  v1
#
# Every Primitive value MUST appear here so a writer lookup never KeyError-s.
MATCHING_POLICY_VERSIONS: dict[Primitive, int] = {
    # VIB-4848: v4→v5. The LP close handler / attributor now branches
    # explicitly on a ``fee_separation_method`` taxonomy stamped on
    # ``LPCloseData`` and the close-event ``attribution_json`` sidecar
    # (SEPARATE / BUNDLED / UNKNOWN + EXACT / ESTIMATED / UNKNOWN
    # confidence). The position_events ``compute_impermanent_loss`` now
    # subtracts the SEPARATE/EXACT close fees from V_lp before IL — the
    # accounting_events lane has done this since VIB-4319; v5 closes the
    # symmetric gap on the attribution lane. Per-lifecycle ``attribute_lp``
    # also folds mid-life ``LP_COLLECT_FEES`` into ``net_pnl_usd``.
    # VIB-4275 (v3→v4) lot-matching invariant remains in effect: the LP
    # close→open resolver still filters same-``position_key`` candidates
    # by a per-position discriminator (NFT token id) and FAILS CLOSED to
    # ``None`` when it cannot uniquely identify the closing leg's open.
    # VIB-4264 (v5→v6): LP_CLOSE / LP_COLLECT_FEES wallet-basis distribution
    # is now VALUE-WEIGHTED across legs (leg_amount × close-time price) with an
    # exact Σ-residual, replacing the equal split that over-based the smaller
    # leg and inflated the closing SWAP's ``realized_pnl_usd_matched``. The
    # algorithm that seeds wallet-basis lots changed, so the per-primitive
    # lot-matching slot advances.
    Primitive.LP: 6,
    # VIB-4848: v1→v2. V4 is the canonical BUNDLED-fees protocol so the
    # taxonomy + IL-adjustment behaviour applies symmetrically; the
    # parallel slot keeps V4 fixtures from regressing on the V3 stream.
    # VIB-4477 originally landed V4 at v1 with the same FIFO matching as
    # Primitive.LP — the per-primitive slot exists so V4 can evolve
    # independently. T8 lands the same taxonomy on both primitives, so
    # both bump together.
    Primitive.LP_V4: 2,
    Primitive.LENDING: 3,
    Primitive.CDP: 1,
    Primitive.LIQUIDATION: 1,
    Primitive.PERP: 1,
    Primitive.UTILITY: 1,
    # VIB-4905 (v3→v4): partial-match SWAPs now surface a matched-portion
    # realized PnL alongside the unmatched amount/proceeds rather than
    # discarding the matched portion entirely when ``_unmatched > 0``.
    # The matching algorithm itself is unchanged — FIFO consumes lots
    # exactly as before; the bump tracks the contract change at the
    # writer / payload boundary (a partial-match payload now carries
    # ``realized_pnl_usd_matched`` + ``unmatched_amount_in`` +
    # ``unmatched_proceeds_usd``, where v3 emitted ``realized_pnl_usd=None``
    # and dropped the matched value on the floor).
    Primitive.SWAP: 4,
    Primitive.VAULT: 1,
    Primitive.STAKING: 1,
    Primitive.BRIDGE: 1,
    Primitive.PREDICTION: 1,
    Primitive.FLASH_LOAN: 1,
}

# VIB-4166 (T6): per-primitive primitive_version map.
#
# Stamped by ``writer.augment_accounting_payload`` onto every accounting-event
# payload alongside the existing version triple (see module docstring for the
# bump policy). Initial value is 1 for every primitive; bumping is per-primitive
# so a CDP semantics change cannot retroactively re-baseline LP, Lending or
# Perp scoring.
#
# Why a separate version from ``matching_policy_version``: the latter tracks
# changes to the lot-matching ALGORITHM (FIFO vs LIFO, wallet-basis vs
# position-basis, etc.) and is consumed by the Accountant Test G13 cell.
# ``primitive_version`` tracks changes to the primitive's BROADER CONTRACT
# (fields emitted, lifecycle states, financial invariants). The two can move
# independently — e.g. switching FIFO to LIFO inside LP bumps lot-matching
# without bumping the primitive contract; adding fee-collection events to LP
# bumps the primitive contract without changing lot matching.
#
# Every Primitive value MUST appear here so the writer lookup never KeyError-s.
PRIMITIVE_VERSION_DEFAULT = 1
PRIMITIVE_VERSIONS: dict[Primitive, int] = {
    Primitive.LP: PRIMITIVE_VERSION_DEFAULT,
    # VIB-4477: Uniswap V4 LP primitive contract — fresh v1 stream parallel to
    # Primitive.LP (V3 / Aerodrome / TraderJoe / Curve / etc. stays at v1 too,
    # but on its own dict slot so a future LP_V4 contract bump cannot
    # retro-baseline V3). See ``Primitive.LP_V4`` in
    # ``almanak.framework.primitives.types`` for the resolution contract.
    Primitive.LP_V4: PRIMITIVE_VERSION_DEFAULT,
    Primitive.LENDING: PRIMITIVE_VERSION_DEFAULT,
    Primitive.CDP: PRIMITIVE_VERSION_DEFAULT,
    Primitive.LIQUIDATION: PRIMITIVE_VERSION_DEFAULT,
    Primitive.PERP: PRIMITIVE_VERSION_DEFAULT,
    Primitive.UTILITY: PRIMITIVE_VERSION_DEFAULT,
    # VIB-4905 (v1→v2): SwapEventPayload contract extension — additive
    # three-field bundle for partial-match disposals
    # (``realized_pnl_usd_matched`` / ``unmatched_amount_in`` /
    # ``unmatched_proceeds_usd``).  Bump documents the new emitter contract
    # at the primitive level (separate from the matching-policy bump above
    # — see module docstring for the policy/contract split).
    Primitive.SWAP: 2,
    Primitive.VAULT: PRIMITIVE_VERSION_DEFAULT,
    Primitive.STAKING: PRIMITIVE_VERSION_DEFAULT,
    Primitive.BRIDGE: PRIMITIVE_VERSION_DEFAULT,
    Primitive.PREDICTION: PRIMITIVE_VERSION_DEFAULT,
    Primitive.FLASH_LOAN: PRIMITIVE_VERSION_DEFAULT,
}


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
    # VIB-4166 (T6): per-primitive primitive_version. Default to the global
    # initial of 1 so pre-T6 payloads on disk (which lack this field) round-
    # trip through the read rail without crashing — the augment chokepoint
    # writes the canonical per-primitive value at write time.
    primitive_version: int = PRIMITIVE_VERSION_DEFAULT

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
    # VIB-4539: amount is Decimal | None per AGENTS.md Empty ≠ Zero —
    # same rule the SwapEventPayload widening (VIB-4490 / PR #2338)
    # applies to the swap side. The Morpho receipt parser cannot always
    # resolve the assets amount when shares-mode withdraws are used or
    # when loan_token decimals are unresolved; ``None`` is a valid
    # measured-unmeasured state. Without this, the projection helper
    # at ``accountant_test.py:_project_payload_for_v1_validation`` cannot
    # forward ``amount_token=None`` through to the spec name and the row
    # FAILs Pydantic validation, blocking G6 / G13 / L1 / L4 / L6.
    #
    # Audit PR #2343 (CodeRabbit): use ``Field(...)`` — Pydantic v2's
    # "required key, no default" marker — so an absent ``amount`` field
    # raises ValidationError. Explicit ``None`` is still accepted. This
    # preserves Empty ≠ Zero discipline: a parser bug that drops the
    # field entirely (`""` shape) FAILs loud, while a measured-unmeasured
    # row with explicit ``None`` validates. The writer always emits
    # ``amount_token`` (``LendingAccountingEvent.to_payload_json``), and
    # the v1 projection helper aliases ``amount_token -> amount`` for
    # WITHDRAW regardless of value, so the key is always present in
    # validation input on the production path. We use ``Field(...)``
    # rather than the bare ``= ...`` shorthand because mypy doesn't
    # recognize ``EllipsisType`` as compatible with ``Decimal | None``;
    # ``Field(...)`` carries the same semantics with the correct type.
    amount: Decimal | None = Field(...)
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
    # VIB-4490: amount_in / amount_out are ``Decimal | None`` to honor the
    # framework-wide Empty ≠ Zero rule (AGENTS.md §Accounting). A receipt
    # parser that cannot resolve token decimals (or a teardown swap whose
    # output amount is genuinely unmeasured) emits ``None`` here and stamps
    # the reason on ``unavailable_reason``. The cell-level reconciliation
    # (G6 / L6 / G13) already null-checks before computing; rejecting None
    # at schema validation short-circuited the whole cell and surfaced as
    # a false-positive "data unusable" FAIL even when amount_in was measured
    # and the SWAP's USD basis could still be reconciled.
    #
    # Codex audit (PR #2338): the fields are required-but-nullable via
    # ``Field(...)`` rather than defaulted to ``None``. This matches the
    # WithdrawEventPayload precedent (line 260) and preserves the
    # Empty ≠ Zero discipline at validation:
    #   * Decimal value → measured amount.
    #   * Explicit None → measured-but-unavailable (reason MUST be set,
    #                     enforced by ``_enforce_unmeasured_reason`` below).
    #   * key omitted   → writer contract drift; FAIL loud.
    # ``Field(...)`` carries the same "required" semantic as the bare
    # ``= ...`` shorthand but mypy doesn't recognize ``EllipsisType`` as
    # compatible with ``Decimal | None``.
    amount_in: Decimal | None = Field(...)
    amount_out: Decimal | None = Field(...)
    amount_in_usd: Decimal | None = None
    amount_out_usd: Decimal | None = None
    effective_price: Decimal | None = None
    slippage_bps: Decimal | None = None
    realized_pnl_usd: Decimal | None = None
    # VIB-4905 (F1): matched-portion realized PnL.  Populated even on
    # partial-match disposals where ``realized_pnl_usd`` (legacy field) is
    # forced to ``None`` because ``_unmatched > 0``.  Computed as
    # ``matched_proceeds_usd - cost_basis_consumed`` where
    # ``matched_proceeds_usd`` is the pro-rated USD share of
    # ``amount_in_usd`` attributable to the matched leg.  ``None`` when no
    # prior basis exists for ``token_in`` (nothing was matched).
    realized_pnl_usd_matched: Decimal | None = None
    # VIB-4905 (F1): the portion of ``amount_in`` that could not be matched
    # against existing FIFO basis lots (caller spent more than the basis
    # store has recorded — e.g. tokens acquired before the accounting
    # system was deployed, or a shared-wallet residual the system never
    # saw).  ``None`` when matching was skipped entirely (no basis store,
    # unmeasured amounts).  ``Decimal("0")`` is a full match.
    unmatched_amount_in: Decimal | None = None
    # VIB-4905 (F1): the USD proceeds attributable to the unmatched portion
    # of ``amount_in``.  Pro-rated from ``amount_in_usd``.  ``None`` when
    # amount_in_usd is unavailable (price oracle missed).
    unmatched_proceeds_usd: Decimal | None = None
    cost_basis_recorded: bool | None = None
    gas_usd: Decimal | None = None
    confidence: ConfidenceLiteral
    unavailable_reason: str | None = None
    swap_position_key: str | None = None

    @model_validator(mode="after")
    def _enforce_unmeasured_reason(self) -> SwapEventPayload:
        """Require ``unavailable_reason`` when an amount is None.

        Gemini audit (PR #2338). Widening ``amount_in`` / ``amount_out`` to
        ``Decimal | None`` makes "measured-but-unmeasured" representable; this
        validator makes it auditable. The SWAP writer
        (``swap_handler._determine_confidence``) already populates
        ``unavailable_reason`` whenever amounts are unmeasured (the
        ``amounts_unmeasured`` branch composes a typed reason), so this rule
        is a structural safety net rather than a tightening of the existing
        path. Pairs with the inherited ``_enforce_confidence_exclusivity``
        which rejects ``confidence=HIGH`` alongside a non-empty reason —
        together they make "amount None" ↔ "confidence ≠ HIGH" ↔ "reason
        populated" a tri-invariant. Rejecting silent ``None`` here prevents
        a future writer regression from producing a payload that validates
        but tells auditors nothing about why the amount disappeared.
        """
        if (self.amount_in is None or self.amount_out is None) and not self.unavailable_reason:
            raise ValueError(
                "SwapEventPayload: unavailable_reason is required when "
                "amount_in or amount_out is None (Empty ≠ Zero audit trail)."
            )
        return self


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
