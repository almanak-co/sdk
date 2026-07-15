"""Data models for the Strategy Teardown System.

Defines the core types used throughout the teardown process:
- TeardownMode: Internal execution modes (SOFT/HARD)
- PositionType: Types of positions in close priority order
- PositionInfo: Individual position details
- TeardownPositionSummary: Complete position summary for teardown
- TeardownPreview: What user sees before confirming
- TeardownResult: Outcome of a teardown operation
- TeardownState: Persisted state for resumability
- EscalationLevel: Slippage escalation levels
- ApprovalRequest: Request for human approval at escalation points
"""

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

# Sentinel for "the operator expressed no target-token preference" (VIB-5727).
#
# Why a sentinel string and not None: `teardown_requests.target_token` is
# `TEXT NOT NULL` (state_manager.py), so None cannot be persisted; None also
# survives the to_dict/from_dict round-trip and then crashes the first
# `target_token.upper()` in the planner. A string keeps every existing reader
# total.
#
# Why not simply keep "USDC" as the default: then "the operator asked for USDC"
# and "nobody asked for anything" are indistinguishable, and the resolver
# cannot know whether it is allowed to substitute a chain-appropriate dollar
# (it must never override an explicit instruction — teardown's rule is to warn
# and skip rather than guess a trade).
#
# The value cannot collide with a real token symbol by construction. It is
# resolved to a concrete symbol at consolidation time, where the chain is
# known, and the resolved value is what gets reported and stamped back.
TARGET_TOKEN_CHAIN_DEFAULT = "__chain_default__"


class TeardownMode(StrEnum):
    """Internal execution mode (not exposed directly to users).

    User-facing names:
    - SOFT: "Graceful Shutdown"
    - HARD: "Safe Emergency Exit"
    """

    SOFT = "SOFT"  # Graceful: 15-30 minutes, minimize costs
    HARD = "HARD"  # Emergency: 1-3 minutes, prioritize speed


class TeardownPhase(StrEnum):
    """Phases of the teardown pipeline.

    Three-phase pipeline:
    - Phase 1: Position Closure (required)
    - Phase 2: Token Consolidation (optional, ON by default)
    - Phase 3: Chain Consolidation (optional, OFF by default)
    """

    CANCEL_WINDOW = "cancel_window"
    POSITION_CLOSURE = "position_closure"  # Phase 1: Required
    TOKEN_CONSOLIDATION = "token_consolidation"  # Phase 2: Optional
    CHAIN_CONSOLIDATION = "chain_consolidation"  # Phase 3: Optional


class TeardownAssetPolicy(StrEnum):
    """How to handle final asset allocation after closing positions.

    Determines what happens in Phase 2 (Token Consolidation):
    - TARGET_TOKEN: Swap all tokens to a target (default: USDC)
    - ENTRY_TOKEN: Swap back to original entry assets
    - KEEP_OUTPUTS: No terminal swaps, keep natural exit tokens

    Emergency mode automatically overrides to KEEP_OUTPUTS for safety.
    """

    TARGET_TOKEN = "target_token"  # Swap all to target (default: USDC)
    ENTRY_TOKEN = "entry_token"  # Return to original entry asset
    KEEP_OUTPUTS = "keep_outputs"  # No terminal swaps, keep native tokens


class PositionType(StrEnum):
    """Position types in close order priority.

    Critical for safety - always close in this order:
    1. PERP - Close perpetuals first (highest liquidation risk)
    2. BORROW - Repay borrows (frees collateral)
    3. SUPPLY - Withdraw collateral
    4. VAULT - Redeem ERC-4626 vault shares (must close before LP — vaults often wrap LPs)
    5. LP - Close LP positions
    6. STAKE - Unstake staked tokens
    7. PREDICTION - Close prediction market positions
    8. CEX - Withdraw from CEX
    9. TOKEN - Swap to target token last

    Note: STAKE, PREDICTION, CEX added for portfolio tracking.
    They have lower priority than core DeFi positions.
    """

    PERP = "PERP"
    BORROW = "BORROW"
    SUPPLY = "SUPPLY"
    VAULT = "VAULT"  # ERC-4626 vaults (MetaMorpho, Beefy, Yearn V3, ...)
    LP = "LP"
    STAKE = "STAKE"  # Staked tokens (Lido, Ethena)
    PREDICTION = "PREDICTION"  # Prediction market positions (Polymarket)
    CEX = "CEX"  # CEX holdings (Kraken)
    # TOKEN is a polymorphic catch-all for "I hold value here, surface it for
    # operator visibility / teardown". Two production shapes (VIB-4909):
    #   1. WALLET PSEUDO-POSITION — strategy reports a token that ALSO lives in
    #      wallet_balances (SWAP-class: WETH after a buy, Lido stETH if tracked,
    #      etc.). Set ``details["asset"]`` to the token symbol or
    #      ``details["address"]`` to its on-chain address so PortfolioValuer
    #      can detect the wallet-overlap and avoid double-counting in
    #      ``wallet_total_value_usd``.
    #   2. DEPLOYED HOLDING — strategy reports a deployed asset NOT in the
    #      tracked wallet (e.g. ``metamorpho_eth_yield`` vault shares; Pendle
    #      PT/YT held off the wallet's tracked-token set). Omit
    #      ``details["asset"]``/``details["address"]`` (or set them to a
    #      symbol/address that does NOT appear in wallet_balances) — the
    #      framework treats these as non-overlapping and adds them to
    #      ``wallet_total_value_usd`` like a real protocol position.
    # The overlap check is case-insensitive and matched against
    # ``TokenBalance.symbol`` / ``TokenBalance.address``. See
    # ``almanak.framework.valuation.portfolio_valuer._is_wallet_pseudo_position``.
    TOKEN = "TOKEN"

    @property
    def priority(self) -> int:
        """Return close order priority (lower = close first).

        VIB-4162 (T2): priority is teardown-protocol-specific (risk-ordered
        close); the underlying primitive mapping lives in
        :func:`almanak.framework.primitives.taxonomy.materializer_primitive_for`.
        A unit test
        (``tests/unit/teardown/test_position_type_taxonomy_coverage.py``)
        asserts every ``PositionType`` value has a corresponding
        ``Primitive`` so a new teardown PositionType cannot ship without a
        taxonomy row.
        """
        priorities = {
            PositionType.PERP: 1,
            PositionType.BORROW: 2,
            PositionType.SUPPLY: 3,
            PositionType.VAULT: 4,
            PositionType.LP: 5,
            PositionType.STAKE: 6,
            PositionType.PREDICTION: 7,
            PositionType.CEX: 8,
            PositionType.TOKEN: 9,
        }
        return priorities[self]


@dataclass
class PositionInfo:
    """A single position to be closed during teardown.

    Also used for monitoring — optional fields (entry_price, unrealized_pnl_usd, etc.)
    are populated by strategies that support position exposure reporting.
    """

    position_type: PositionType
    position_id: str
    chain: str
    protocol: str
    value_usd: Decimal

    # Risk info
    liquidation_risk: bool = False
    health_factor: Decimal | None = None

    # Protocol-specific details
    details: dict[str, Any] = field(default_factory=dict)

    # Optional monitoring fields (populated for position exposure reporting)
    entry_price: Decimal | None = None
    current_price: Decimal | None = None
    unrealized_pnl_usd: Decimal | None = None
    unrealized_pnl_pct: Decimal | None = None
    direction: str | None = None  # "LONG" / "SHORT" for perps
    size_usd: Decimal | None = None  # Notional size (perps)
    collateral_usd: Decimal | None = None  # Collateral value
    leverage: Decimal | None = None  # Current leverage

    def __post_init__(self) -> None:
        """Validate and normalize fields."""
        # ``details`` is typed ``dict`` (default_factory=dict), but nothing
        # stops a strategy's ``get_open_positions`` from handing us ``None``.
        # Coerce here so every consumer (valuation dedup, repricing, teardown)
        # can rely on a dict without per-call guards (VIB-4838).
        if self.details is None:
            self.details = {}
        if isinstance(self.value_usd, int | float | str):
            self.value_usd = Decimal(str(self.value_usd))
        if self.health_factor is not None and isinstance(self.health_factor, int | float | str):
            self.health_factor = Decimal(str(self.health_factor))
        # Normalize optional Decimal fields
        for attr in (
            "entry_price",
            "current_price",
            "unrealized_pnl_usd",
            "unrealized_pnl_pct",
            "size_usd",
            "collateral_usd",
            "leverage",
        ):
            value = getattr(self, attr)
            if value is not None and isinstance(value, int | float | str):
                setattr(self, attr, Decimal(str(value)))


def calculate_max_acceptable_loss(position_value_usd: Decimal) -> Decimal:
    """Position-size-aware loss cap.

    Larger positions get tighter relative caps because the absolute
    dollar amounts are more significant.

    Args:
        position_value_usd: Total value of the position in USD

    Returns:
        Maximum acceptable loss as a decimal percentage (e.g., 0.03 for 3%)
    """
    if position_value_usd < Decimal("50_000"):
        return Decimal("0.03")  # 3% for small positions (<$50K)
    elif position_value_usd < Decimal("200_000"):
        return Decimal("0.025")  # 2.5% for medium positions ($50K-$200K)
    elif position_value_usd < Decimal("500_000"):
        return Decimal("0.02")  # 2% for medium-large positions ($200K-$500K)
    elif position_value_usd < Decimal("2_000_000"):
        return Decimal("0.015")  # 1.5% for large positions ($500K-$2M)
    else:
        return Decimal("0.01")  # 1% for whale positions (>$2M)


@dataclass
class TeardownPositionSummary:
    """Complete summary of positions for teardown."""

    deployment_id: str
    timestamp: datetime
    positions: list[PositionInfo] = field(default_factory=list)

    # Computed fields (can be set explicitly or calculated)
    total_value_usd: Decimal = Decimal("0")
    has_liquidation_risk: bool = False

    def __post_init__(self) -> None:
        """Calculate derived fields if not set."""
        if isinstance(self.total_value_usd, int | float | str):
            self.total_value_usd = Decimal(str(self.total_value_usd))

        # Auto-calculate if positions exist but total not set
        if self.positions and self.total_value_usd == Decimal("0"):
            self.total_value_usd = sum((p.value_usd for p in self.positions), Decimal("0"))

        if self.positions and not self.has_liquidation_risk:
            self.has_liquidation_risk = any(p.liquidation_risk for p in self.positions)

    @property
    def max_loss_percent(self) -> Decimal:
        """Get position-aware max loss percentage."""
        return calculate_max_acceptable_loss(self.total_value_usd)

    @property
    def max_loss_usd(self) -> Decimal:
        """Get maximum acceptable loss in USD."""
        return self.total_value_usd * self.max_loss_percent

    @property
    def protected_minimum_usd(self) -> Decimal:
        """Get the protected minimum value (what user is guaranteed to keep)."""
        return self.total_value_usd - self.max_loss_usd

    @property
    def chains_involved(self) -> set[str]:
        """Get all chains with positions."""
        return {p.chain for p in self.positions}

    def positions_by_chain(self, chain: str) -> list[PositionInfo]:
        """Get positions for a specific chain."""
        return [p for p in self.positions if p.chain == chain]

    def positions_by_type(self, ptype: PositionType) -> list[PositionInfo]:
        """Get positions of a specific type."""
        return [p for p in self.positions if p.position_type == ptype]

    def positions_sorted_by_priority(self) -> list[PositionInfo]:
        """Get positions sorted by close order priority."""
        return sorted(self.positions, key=lambda p: p.position_type.priority)

    @classmethod
    def empty(cls, deployment_id: str) -> "TeardownPositionSummary":
        """Create an empty position summary (no open positions)."""
        return cls(deployment_id=deployment_id, timestamp=datetime.now(UTC), positions=[])


@dataclass
class TeardownPreview:
    """What the user sees before confirming teardown.

    This is the key UX element - it shows protections clearly.
    """

    deployment_id: str
    strategy_name: str
    mode: str  # "graceful" or "emergency" (user-facing)

    # Position info
    positions: list[dict[str, Any]]  # Simplified for API response
    current_value_usd: Decimal

    # Protection info (the key UX element)
    protected_minimum_usd: Decimal
    max_loss_percent: Decimal
    max_loss_usd: Decimal

    # Estimates
    estimated_return_min_usd: Decimal
    estimated_return_max_usd: Decimal
    estimated_duration_minutes: int

    # Steps (human readable)
    steps: list[str]  # ["Close perp position", "Swap to USDC", ...]

    # Warnings
    warnings: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Convert numeric fields to Decimal."""
        for attr in [
            "current_value_usd",
            "protected_minimum_usd",
            "max_loss_percent",
            "max_loss_usd",
            "estimated_return_min_usd",
            "estimated_return_max_usd",
        ]:
            value = getattr(self, attr)
            if isinstance(value, int | float | str):
                setattr(self, attr, Decimal(str(value)))


class VerificationStatus(StrEnum):
    """How a teardown's reported ``positions_closed`` was established (VIB-2932 / VIB-5472).

    ``positions_closed`` is a *count*; this enum records the *confidence* behind
    that count so an unverifiable closure is never silently presented as a
    chain-confirmed one. It is the qualitative companion to the quantitative
    closure counters on :class:`ClosureVerification` / :class:`TeardownResult`.

    Members (distinct categories, not an ordered scale):

    - ``CHAIN_VERIFIED`` — every pre-execution position had a registered on-chain
      post-condition (``TeardownPostCondition``) and all of them confirmed
      closure against the chain. The strongest signal: closure is proven, not
      assumed.
    - ``UNVERIFIED`` — closure is reported (``all_closed`` is True) but at least
      one position was counted *closed-by-execution* rather than chain-confirmed:
      either its protocol has no registered post-condition (e.g. Aave / Morpho /
      Compound lending today), or the verifier fell back to the in-memory
      ``get_open_positions()`` read (no pre-execution snapshot). Closure is
      plausible and nothing contradicts it, but the chain did not prove it — the
      operator must treat the count as optimistic. This is the explicit, visible
      surface for VIB-2932: an unverifiable closure is flagged, never hidden
      inside a ``positions_closed`` number that looks chain-confirmed.
    - ``FAILED`` — at least one pre-execution position still has residual on-chain
      liquidity / debt, a post-condition errored, or verification itself raised.
      Pairs with ``all_closed=False`` and a teardown ``success=False``.
    - ``NOT_RUN`` — verification did not run for this result (execution failed
      before the verify step, or a lane / early-exit path that never reaches the
      verifier, e.g. an empty / cancelled / paused teardown). The default on a
      freshly-built ``TeardownResult`` before the verify step stamps it.
    """

    CHAIN_VERIFIED = "chain_verified"
    UNVERIFIED = "unverified"
    FAILED = "failed"
    NOT_RUN = "not_run"


@dataclass(frozen=True)
class ClosureVerification:
    """Position-level result of post-teardown closure verification (VIB-5085).

    ``TeardownManager._verify_closure`` returns a bare ``bool`` (all-closed
    y/n) and is kept for back-compat. ``_verify_closure_detailed`` returns
    this richer record so lifecycle counters can report *positions* closed
    rather than *intents* landed — one position can be closed by several
    intents (REPAY + WITHDRAW + SWAP), so the two counts diverge.

    ``positions_closed`` counts the pre-execution positions that passed their
    on-chain post-condition; ``positions_total`` is the pre-execution position
    count. ``all_closed`` is tracked explicitly because the legacy in-memory
    fallback path (no pre-execution snapshot) can report all-closed with
    ``positions_total == 0`` — it is not derivable from the two counts.

    ``has_position_breakdown`` is True only when a real pre-execution position
    snapshot drove the verification (so ``positions_total`` / ``positions_closed``
    are trustworthy). It is False on the in-memory fallback path (empty snapshot)
    where ``positions_total == 0`` carries no information — callers must then NOT
    treat ``positions_closed == 0`` as authoritative and fall back to the intent
    signal instead (otherwise a balance-driven teardown that closed real positions
    but exposes no ``PositionInfo`` rows would persist ``positions_closed=0`` on
    success — the inverse of the VIB-5085 bug).

    ``verification_status`` (VIB-2932 / VIB-5472) records the *confidence* behind
    ``positions_closed`` — whether the closure was chain-confirmed, merely
    counted closed-by-execution (``UNVERIFIED``), or failed. See
    :class:`VerificationStatus`. It is derived alongside the counts so a caller
    can both count and qualify the closure without re-deriving it.
    """

    all_closed: bool
    positions_total: int = 0
    positions_closed: int = 0
    has_position_breakdown: bool = False
    verification_status: VerificationStatus = VerificationStatus.NOT_RUN


@dataclass
class TeardownResult:
    """Result of a completed teardown operation."""

    success: bool
    deployment_id: str
    mode: str

    # Timing
    started_at: datetime
    completed_at: datetime | None
    duration_seconds: float

    # Execution stats
    intents_total: int
    intents_succeeded: int
    intents_failed: int

    # Financial summary
    starting_value_usd: Decimal
    final_value_usd: Decimal
    total_costs_usd: Decimal  # gas + slippage

    # Final state
    final_balances: dict[str, Decimal]  # {token: amount}

    # If failed
    error: str | None = None
    recovery_options: list[str] = field(default_factory=list)

    # VIB-3773: accounting-degraded teardown — chain-side work succeeded but
    # one or more accounting writes failed and were recorded into the
    # deferred-write log. The teardown loop continued (degraded-but-continue
    # contract); operator must reconcile via the deferred log + outbox tail.
    accounting_degraded: bool = False
    accounting_degraded_count: int = 0

    # VIB-5011: token-consolidation (Phase 2) summary. Consolidation failure
    # after a successful closure keeps ``success=True`` (the closure already
    # removed on-chain risk); the partial state is carried here and folded
    # into ``result_json["consolidation"]`` via ``mark_completed``. There is
    # deliberately NO new TeardownStatus member — status-protocol consumers
    # keep seeing COMPLETED.
    consolidation_planned: int = 0
    consolidation_succeeded: int = 0
    consolidation_failed: int = 0
    consolidation_warnings: list[str] = field(default_factory=list)
    # The target the consolidation phase ACTUALLY used, after chain-aware
    # resolution (VIB-5727). The request row may carry the
    # ``TARGET_TOKEN_CHAIN_DEFAULT`` sentinel, so the request is not a truthful
    # source for "what happened" — this is. ``None`` means the phase never
    # resolved a target (skipped, or no usable dollar on the chain); Empty ≠
    # Zero, so it is not defaulted to a symbol that was never used.
    consolidation_target: str | None = None

    # VIB-5085: position-level closure counts. ``positions_closed`` reports the
    # number of *positions* verified closed by ``_verify_closure_detailed`` —
    # NOT the number of teardown *intents* that landed. ``has_position_breakdown``
    # is True only when post-execution verification actually ran and populated
    # these counts; lifecycle persistence falls back to the intent counts when
    # it is False (e.g. an execution failure before verification, or the
    # multi-chain / inline fallback lanes which have no verifier). The intent
    # signal survives on ``intents_total`` / ``intents_succeeded``.
    positions_total: int = 0
    positions_closed: int = 0
    has_position_breakdown: bool = False

    # VIB-2932 / VIB-5472: confidence behind ``positions_closed`` — whether the
    # closure was chain-confirmed (``CHAIN_VERIFIED``), counted closed-by-execution
    # without an on-chain post-condition proving it (``UNVERIFIED``), failed
    # (``FAILED``), or never verified (``NOT_RUN``, the pre-verify default). The
    # verify step stamps it from the ``ClosureVerification``; lifecycle persistence
    # carries it into ``result_json`` so an unverifiable closure is visible to the
    # operator and never masquerades as a chain-confirmed one. See
    # :class:`VerificationStatus`.
    verification_status: VerificationStatus = VerificationStatus.NOT_RUN

    # VIB-5140: block number of the last successful close-tx receipt in this
    # teardown. Threaded into the on-chain closure verifier so it pins its
    # ``QueryPositionLiquidity`` / ``QueryPositionTokensOwed`` reads to the
    # exact block the close landed at — a read replica that trails the writer
    # by a block then cannot return PRE-close state and false-negative the
    # verification. ``None`` (no receipt block available) falls back to the
    # legacy ``"latest"`` read.
    last_receipt_block: int | None = None

    def __post_init__(self) -> None:
        """Convert numeric fields to Decimal."""
        for attr in ["starting_value_usd", "final_value_usd", "total_costs_usd"]:
            value = getattr(self, attr)
            if isinstance(value, int | float | str):
                setattr(self, attr, Decimal(str(value)))

    @property
    def cost_percent(self) -> Decimal:
        """Get total costs as percentage of starting value."""
        if self.starting_value_usd == 0:
            return Decimal("0")
        return (self.total_costs_usd / self.starting_value_usd) * 100

    @property
    def all_succeeded(self) -> bool:
        """Check if all intents succeeded."""
        return self.intents_succeeded == self.intents_total


class TeardownStatus(StrEnum):
    """Status of a teardown operation."""

    PENDING = "pending"  # Created but not started
    CANCEL_WINDOW = "cancel_window"  # In 10-second cancel window
    EXECUTING = "executing"  # Actively executing intents
    PAUSED = "paused"  # Paused for approval or error
    COMPLETED = "completed"  # Successfully completed
    FAILED = "failed"  # Failed with error
    CANCELLED = "cancelled"  # Cancelled by user


@dataclass
class TeardownState:
    """Persisted state for resumable teardowns.

    This state survives system restarts, allowing interrupted
    teardowns to resume from the last checkpoint.
    """

    teardown_id: str
    deployment_id: str
    mode: TeardownMode
    status: TeardownStatus

    # Progress
    total_intents: int
    completed_intents: int
    current_intent_index: int

    # Timing
    started_at: datetime
    updated_at: datetime
    completed_at: datetime | None = None

    # For resumption
    pending_intents_json: str = ""  # Serialized intents
    intent_results: list[dict[str, Any]] = field(default_factory=list)

    # Cancel window
    cancel_window_until: datetime | None = None

    # Configuration snapshot (for resumption with same settings)
    config_json: str = ""

    # ALM-2766 / VIB-5174: operator consent to a full-wallet consolidation
    # sweep, set True ONLY for an operator-initiated (MANUAL) token-consolidation
    # phase. Persisted across crash-resume so the resumed consolidation tail does
    # NOT re-clamp a sweep the operator already consented to. It rides the
    # existing ``config_json`` snapshot column on the wire/disk (see
    # ``encode_consolidation_consent`` / ``decode_consolidation_consent``) rather
    # than a dedicated column — ``teardown_execution_state`` has a Postgres twin
    # owned by the external metrics-database repo, so a new typed column would
    # require external DDL and be dropped on a hosted save. config_json is an
    # existing opaque TEXT column that already round-trips through SQLite, the
    # gateway serialization, AND the Postgres adapter, so consent survives a
    # resume on BOTH backends with zero schema change.
    consolidation_consent: bool = False

    @property
    def progress_percent(self) -> int:
        """Get completion percentage."""
        if self.total_intents == 0:
            return 0
        return int((self.completed_intents / self.total_intents) * 100)

    @property
    def is_in_cancel_window(self) -> bool:
        """Check if still in cancel window."""
        if self.cancel_window_until is None:
            return False
        return datetime.now(UTC) < self.cancel_window_until

    @property
    def is_resumable(self) -> bool:
        """Check if this teardown can be resumed."""
        return self.status in (TeardownStatus.EXECUTING, TeardownStatus.PAUSED)


# VIB-5174: the consolidation-consent flag rides the existing ``config_json``
# snapshot column rather than a dedicated column. ``teardown_execution_state``
# has a Postgres twin whose DDL is owned by the external ``metrics-database``
# repo and a column-mapping adapter in the external ``platform-plugins`` package;
# a new typed column would require external DDL and would be DROPPED on a hosted
# save (the PG adapter never maps an unknown field), losing consent across a
# hosted resume. ``config_json`` is an existing opaque TEXT column that already
# round-trips through SQLite, the gateway serialization, and the Postgres
# adapter, so embedding consent there fixes resume on BOTH backends with zero
# schema change. The key is namespaced so it never collides with a
# ``TeardownConfig.to_dict()`` field, and ``config_json`` is a write-only debug
# snapshot (never parsed back as a TeardownConfig), so no consumer breaks.
_CONSOLIDATION_CONSENT_KEY = "__consolidation_consent__"


def encode_consolidation_consent(config_json: str, consent: bool) -> str:
    """Fold the consolidation-consent flag into a ``config_json`` snapshot.

    Idempotent and content-preserving for a JSON-object snapshot: parse, set or
    remove the reserved consent key, re-serialize. A non-object or corrupt
    snapshot is replaced with a fresh object carrying only the consent key —
    ``config_json`` is never read back as a ``TeardownConfig``, so no consumer
    breaks.
    """
    try:
        data = json.loads(config_json) if config_json else {}
    except (TypeError, ValueError):
        data = {}
    if not isinstance(data, dict):
        data = {}
    # Only an exact ``True`` writes the reserved key (symmetric with the
    # literal-``true`` decode); any other value clears it — the consent flag
    # gates a money-path clamp, so never persist a non-boolean grant.
    if consent is True:
        data[_CONSOLIDATION_CONSENT_KEY] = True
    else:
        data.pop(_CONSOLIDATION_CONSENT_KEY, None)
    return json.dumps(data)


def decode_consolidation_consent(config_json: str) -> bool:
    """Read the consolidation-consent flag out of a ``config_json`` snapshot.

    Tolerant of empty / non-object / corrupt snapshots (default ``False``). An
    old pre-VIB-5174 row predates the feature, so it correctly reads ``False``
    and a resumed consolidation re-clamps — the safe under-sweep direction.

    Consent requires the reserved key to be the LITERAL JSON boolean ``true`` —
    not merely truthy. This flag DISABLES the ALM-2766 swap-back clamp, so a
    malformed or externally-written snapshot (e.g. ``"false"``, ``"0"``, ``1``)
    must never be promoted to consent; only an exact ``true`` grants it.
    """
    try:
        data = json.loads(config_json) if config_json else {}
    except (TypeError, ValueError):
        return False
    if not isinstance(data, dict):
        return False
    return data.get(_CONSOLIDATION_CONSENT_KEY) is True


class EscalationLevel(StrEnum):
    """Slippage escalation levels.

    Each level represents an increasing slippage tolerance.
    Levels 1-2 auto-approve, levels 3+ require human approval.
    """

    LEVEL_1 = "level_1"  # 2% slippage, auto-approve, 3 retries
    LEVEL_2 = "level_2"  # 3% slippage, auto-approve, 2 retries
    LEVEL_3 = "level_3"  # 5% slippage, needs approval, 1 retry
    LEVEL_4 = "level_4"  # 8% slippage, needs explicit approval, 1 retry
    LEVEL_5 = "level_5"  # >8% slippage, manual intervention required


@dataclass
class EscalationConfig:
    """Configuration for a single escalation level."""

    level: EscalationLevel
    slippage: Decimal
    auto_approve: bool
    retries: int

    @classmethod
    def default_levels(cls) -> list["EscalationConfig"]:
        """Get default escalation level configurations."""
        return [
            cls(
                level=EscalationLevel.LEVEL_1,
                slippage=Decimal("0.02"),
                auto_approve=True,
                retries=3,
            ),
            cls(
                level=EscalationLevel.LEVEL_2,
                slippage=Decimal("0.03"),
                auto_approve=True,
                retries=2,
            ),
            cls(
                level=EscalationLevel.LEVEL_3,
                slippage=Decimal("0.05"),
                auto_approve=False,
                retries=1,
            ),
            cls(
                level=EscalationLevel.LEVEL_4,
                slippage=Decimal("0.08"),
                auto_approve=False,
                retries=1,
            ),
        ]


@dataclass
class ApprovalRequest:
    """Request for human approval at an escalation point.

    Sent when slippage would exceed auto-approved levels.
    """

    teardown_id: str
    deployment_id: str
    current_level: EscalationLevel
    current_slippage: Decimal
    estimated_loss_usd: Decimal
    position_value_usd: Decimal

    # Context for user decision
    reason: str  # Why approval is needed
    options: list[str]  # Available actions

    # Timestamps
    requested_at: datetime
    expires_at: datetime | None = None  # Auto-cancel if not responded

    def __post_init__(self) -> None:
        """Convert numeric fields to Decimal."""
        for attr in ["current_slippage", "estimated_loss_usd", "position_value_usd"]:
            value = getattr(self, attr)
            if isinstance(value, int | float | str):
                setattr(self, attr, Decimal(str(value)))

    @property
    def loss_percent(self) -> Decimal:
        """Get estimated loss as percentage."""
        if self.position_value_usd == 0:
            return Decimal("0")
        return (self.estimated_loss_usd / self.position_value_usd) * 100


@dataclass
class ApprovalResponse:
    """Response to an approval request."""

    approved: bool
    teardown_id: str
    approved_slippage: Decimal | None = None  # New max slippage if approved
    # "approve"/"continue" = accept current level; "wait_and_escalate" = pause
    # then advance to the next higher slippage level (current level is not
    # retried); "cancel" = abort the teardown.
    action: str = "continue"

    def __post_init__(self) -> None:
        """Convert numeric fields."""
        if self.approved_slippage is not None and isinstance(self.approved_slippage, int | float | str):
            self.approved_slippage = Decimal(str(self.approved_slippage))


@dataclass
class TeardownRequest:
    """State-based teardown request stored in SQLite/PostgreSQL.

    This is the signal mechanism for triggering teardowns from multiple sources:
    - CLI command
    - Config hot-reload
    - Dashboard UI
    - Risk guards (auto-protect)

    The strategy checks for this request each iteration via _check_teardown_request()
    and initiates teardown if found.
    """

    deployment_id: str
    mode: TeardownMode
    asset_policy: TeardownAssetPolicy = TeardownAssetPolicy.TARGET_TOKEN
    # NOT a token symbol by default (VIB-5727). The sentinel means "the
    # operator expressed no preference — resolve the chain's dollar at
    # consolidation time, where the chain is actually known". A literal
    # ``"USDC"`` here means the operator ASKED for USDC and is honoured as an
    # explicit instruction (never silently substituted), which is also what
    # every pre-VIB-5727 persisted row means — hence old rows keep their exact
    # behaviour. See ``TARGET_TOKEN_CHAIN_DEFAULT``.
    target_token: str = TARGET_TOKEN_CHAIN_DEFAULT

    # Request metadata
    reason: str | None = None
    requested_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    requested_by: str = "dashboard"  # "cli", "config", "dashboard", "risk_guard"

    # Processing state
    status: TeardownStatus = TeardownStatus.PENDING
    acknowledged_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None

    # Phase tracking
    current_phase: TeardownPhase | None = None

    # Progress tracking
    positions_total: int = 0
    positions_closed: int = 0
    positions_failed: int = 0

    # Failure detail (VIB-5778). Persisted in the ``error_message`` column by
    # ``mark_failed`` on every FAILED teardown. Carried on the dataclass so the
    # CLI can surface it at every FAILED render site instead of dropping it.
    error_message: str | None = None

    # Cancel window
    cancel_requested: bool = False
    cancel_deadline: datetime | None = None

    @property
    def is_active(self) -> bool:
        """Check if this request is still active (not completed/cancelled)."""
        return self.status not in (
            TeardownStatus.COMPLETED,
            TeardownStatus.CANCELLED,
            TeardownStatus.FAILED,
        )

    @property
    def counts_unmeasured(self) -> bool:
        """VIB-5778: True when a FAILED teardown's position counts are UNKNOWN, not zero.

        ``mark_started`` (``runner_teardown``) is the sole writer of
        ``positions_total`` and the only setter of ``started_at``; it runs only
        AFTER position enumeration succeeds and execution begins. A teardown that
        FAILED *before* that point — e.g. an exception raised inside
        ``generate_teardown_intents`` (the VIB-5778 field incident that stranded a
        live position) — has ``started_at IS NULL`` and its count columns are the
        schema 0-defaults, NOT measured zeros. Rendering those as ``0`` reports a
        success-shaped "0 positions failed" for a failure that closed nothing.

        UNKNOWN is not zero: callers render these counts as ``unknown``. A FAILED
        row that DID start (``started_at`` set) carries real measured counts —
        including a genuine measured ``0`` — and is never treated as unmeasured.

        This is a read-side inference only (no persisted shape change — the
        persisted tri-state UNKNOWN contract belongs to VIB-5792). Best-effort
        counts written to the DB by the failure path still round-trip via
        ``to_dict`` / ``--json``; this property governs only the human render.
        """
        return self.status == TeardownStatus.FAILED and self.started_at is None

    @property
    def can_cancel(self) -> bool:
        """Check if this request can be cancelled."""
        if not self.is_active:
            return False
        if self.cancel_deadline is None:
            return True
        return datetime.now(UTC) < self.cancel_deadline

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for storage."""
        return {
            "deployment_id": self.deployment_id,
            "mode": self.mode.value,
            "asset_policy": self.asset_policy.value,
            "target_token": self.target_token,
            "reason": self.reason,
            "requested_at": self.requested_at.isoformat(),
            "requested_by": self.requested_by,
            "status": self.status.value,
            "acknowledged_at": self.acknowledged_at.isoformat() if self.acknowledged_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "current_phase": self.current_phase.value if self.current_phase else None,
            "positions_total": self.positions_total,
            "positions_closed": self.positions_closed,
            "positions_failed": self.positions_failed,
            "error_message": self.error_message,
            "cancel_requested": self.cancel_requested,
            "cancel_deadline": self.cancel_deadline.isoformat() if self.cancel_deadline else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TeardownRequest":
        """Deserialize from dictionary."""
        return cls(
            deployment_id=data["deployment_id"],
            mode=TeardownMode(data["mode"]),
            asset_policy=TeardownAssetPolicy(data.get("asset_policy", "target_token")),
            # `or` (not `.get`'s default): a present-but-null / empty value has
            # to collapse to the sentinel too. `.get(k, default)` only
            # substitutes on a MISSING key, so an explicit JSON `null` used to
            # survive the round-trip as None and then crash the first
            # `target_token.upper()` in the planner (VIB-5727).
            target_token=data.get("target_token") or TARGET_TOKEN_CHAIN_DEFAULT,
            reason=data.get("reason"),
            requested_at=datetime.fromisoformat(data["requested_at"]),
            requested_by=data.get("requested_by", "dashboard"),
            status=TeardownStatus(data["status"]),
            acknowledged_at=datetime.fromisoformat(data["acknowledged_at"]) if data.get("acknowledged_at") else None,
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
            completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None,
            current_phase=TeardownPhase(data["current_phase"]) if data.get("current_phase") else None,
            positions_total=data.get("positions_total", 0),
            positions_closed=data.get("positions_closed", 0),
            positions_failed=data.get("positions_failed", 0),
            error_message=data.get("error_message"),
            cancel_requested=data.get("cancel_requested", False),
            cancel_deadline=datetime.fromisoformat(data["cancel_deadline"]) if data.get("cancel_deadline") else None,
        )


@dataclass
class TeardownProfile:
    """Strategy-specific teardown metadata for UX.

    Strategies can provide this profile to help the UI display
    better information about what the teardown will look like.
    """

    natural_exit_assets: list[str] = field(default_factory=list)  # e.g., ["WETH", "USDC"]
    original_entry_assets: list[str] = field(default_factory=list)  # e.g., ["USDC"]
    recommended_target: str = "USDC"
    conversion_complexity: str = "low"  # "low", "medium", "high"
    estimated_steps: int = 3
    chains_involved: list[str] = field(default_factory=list)
    has_perp_positions: bool = False
    has_lending_positions: bool = False
    has_lp_positions: bool = False
