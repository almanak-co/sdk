"""Intent vocabulary for expressing high-level trading actions.

This module defines the Intent class and its factory methods for creating
structured, serializable trading intents. Intents are the high-level
expression of what a strategy wants to do, which can then be compiled
into ActionBundles for execution.

Intent Types:
    - SWAP: Exchange one token for another
    - LP_OPEN: Open a liquidity position
    - LP_CLOSE: Close a liquidity position
    - BORROW: Borrow tokens from a lending protocol
    - REPAY: Repay borrowed tokens
    - BRIDGE: Bridge tokens between chains
    - HOLD: No action (wait)

Each intent type has its own dataclass with specific parameters, and the
Intent class provides factory methods for creating them ergonomically.
"""

import uuid
import warnings
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import Field, model_validator

from almanak.framework.models.base import (
    AlmanakImmutableModel,
    OptionalChainedAmount,
    OptionalSafeDecimal,
    SafeDecimal,
    default_intent_id,
    default_timestamp,
    validate_decimal_safe,
)

# PredictionExitConditions is bound at runtime so ``typing.get_type_hints``
# on ``Intent.prediction_buy`` resolves the annotation. The original eager
# import surfaced a circular cycle (services.prediction_monitor ->
# auto_redemption -> api.actions -> strategies.base -> ..intents); after the
# lazy framework/api/__init__ + the function-local ``api.timeline`` deferrals
# in services modules, that cycle is broken at the api layer and this import
# is cheap (~50 MB resident, no pandas / connectors / strategies pulled).
from almanak.framework.services.prediction_monitor import PredictionExitConditions

# BaseIntent (VIB-4192 / T06) — every concrete intent dataclass below
# inherits from this rather than `AlmanakImmutableModel` directly so that
# the reserved `registry_handle` field + strict TAXONOMY validator land on
# every intent class via single-point inheritance (AC #3 forbids
# per-primitive redeclaration).
from .base import BaseIntent, assert_registry_handle_known  # noqa: E402

# =============================================================================
# Exceptions (re-exported from intent_errors for backward compatibility)
# =============================================================================
from .intent_errors import (  # noqa: E402, F401
    BundledCollateralBorrowError,
    InvalidAmountError,
    InvalidChainError,
    InvalidCollateralForMarketError,
    InvalidProtocolParameterError,
    InvalidSequenceError,
    LpOpenZeroLiquidityError,
    ProtocolRequiredError,
)

# =============================================================================
# Enums
# =============================================================================


# =============================================================================
# Type Aliases
# =============================================================================

# Amount type that supports chained outputs from previous steps
# When amount="all", the intent will use the actual received amount from the
# previous step in a sequence (post-slippage, post-fees).
ChainedAmount = Decimal | Literal["all"]

# Interest rate mode type for lending protocols like Aave
# - 'variable': Interest rate fluctuates based on supply/demand
# Note: 'stable' rate was deprecated on Aave V3 and Spark (most assets disabled)
InterestRateMode = Literal["variable"]


def _supports_lp_close_exit_selectors(protocol: str | None) -> bool:
    """Return True when ``protocol`` declares the LP_CLOSE exit-selector capability.

    The pool-coin exit selectors on ``LPCloseIntent`` (``coin_index`` for a
    single-sided close, ``imbalanced_amounts`` for an exact-amounts close) are
    compiled only by connectors that declare ``lp_close_exit_selectors`` in
    their connector-owned ``capabilities.py`` (Curve today). Reading the flag
    from the capabilities registry keeps this guard free of protocol-name
    dispatch (``tests/static/test_protocol_chain_literal_ratchet.py``) and
    lets a future connector opt in from its own folder. Lazy import: the
    connector packages import this module (``IntentType``), so an eager
    registry import would create a cycle on cold boot.
    """
    if not protocol:
        return False
    from almanak.connectors._strategy_base.capabilities_registry import get_protocol_capabilities

    key = protocol.strip().lower().replace("-", "_").replace(" ", "_")
    return bool(get_protocol_capabilities(key).get("lp_close_exit_selectors", False))


def _lp_close_exit_selector_protocols() -> str:
    """Comma-joined protocol names declaring ``lp_close_exit_selectors``.

    Error-path helper for the two guard messages below; loads every connector
    capabilities module, which is acceptable on a path that ends in a raise.
    """
    from almanak.connectors._strategy_base.capabilities_registry import all_protocol_capabilities

    names = sorted(k for k, v in all_protocol_capabilities().items() if v.get("lp_close_exit_selectors"))
    return ", ".join(f"'{n}'" for n in names) or "none"


# =============================================================================
# Protocol Capabilities
# =============================================================================

# ``PROTOCOL_CAPABILITIES`` is a read-through aggregated view over every
# connector's ``capabilities.py`` module. The actual data lives next to each
# connector (see ``almanak/connectors/<protocol>/capabilities.py``)
# and is assembled by ``CapabilitiesRegistry``.
#
# The aggregator is resolved lazily via module-level ``__getattr__`` (PEP 562)
# rather than eagerly at import time because the connector packages import
# from this module (``IntentType``) -- eager aggregation would create an
# import cycle on cold boot.
#
# Identity contract: every access returns the same aggregated dict instance,
# and every value-dict is the connector module's own dict (not a copy). This
# matches the long-standing semantics of the previous hand-written table:
# tests that monkey-patch a single capability value (e.g.
# ``PROTOCOL_CAPABILITIES["aave_v3"]["interest_rate_modes"] = [...]``) see the
# change reflected in subsequent validator calls within the same process and
# can restore the original value in ``finally``.


def __getattr__(name: str) -> Any:
    """Lazy module-level attribute access (PEP 562) for ``PROTOCOL_CAPABILITIES``.

    Resolves the aggregated capability view on first read and caches it in
    this module's ``globals()`` so subsequent accesses skip ``__getattr__``
    entirely (PEP 562 only fires for missing attributes). Mirrors the cache
    pattern in ``almanak/framework/intents/__init__.py:__getattr__`` so both
    re-export sites have identical lookup cost.
    """
    if name == "PROTOCOL_CAPABILITIES":
        from almanak.connectors._strategy_base.capabilities_registry import (
            all_protocol_capabilities,
        )

        caps = all_protocol_capabilities()
        globals()["PROTOCOL_CAPABILITIES"] = caps
        return caps
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class IntentType(Enum):
    """Types of intents that strategies can express."""

    SWAP = "SWAP"
    LP_OPEN = "LP_OPEN"
    LP_CLOSE = "LP_CLOSE"
    BORROW = "BORROW"
    REPAY = "REPAY"
    SUPPLY = "SUPPLY"
    WITHDRAW = "WITHDRAW"
    PERP_OPEN = "PERP_OPEN"
    PERP_CLOSE = "PERP_CLOSE"
    # Cancel a pending (unfilled) perp order, recovering its committed collateral.
    # Not a position open/close — a refund of committed-but-unspent collateral
    # (the recovery half of VIB-5116; see PerpCancelIntent). NO_ACCOUNTING category.
    PERP_CANCEL_ORDER = "PERP_CANCEL_ORDER"
    BRIDGE = "BRIDGE"
    ENSURE_BALANCE = "ENSURE_BALANCE"
    FLASH_LOAN = "FLASH_LOAN"
    STAKE = "STAKE"
    UNSTAKE = "UNSTAKE"
    HOLD = "HOLD"
    # Prediction market intents
    PREDICTION_BUY = "PREDICTION_BUY"
    PREDICTION_SELL = "PREDICTION_SELL"
    PREDICTION_REDEEM = "PREDICTION_REDEEM"
    # Vault intents (MetaMorpho ERC-4626)
    VAULT_DEPOSIT = "VAULT_DEPOSIT"
    VAULT_REDEEM = "VAULT_REDEEM"
    VAULT_REALLOCATE = "VAULT_REALLOCATE"  # Phase 2
    VAULT_MANAGE = "VAULT_MANAGE"  # Phase 4
    # LP fee collection (without removing liquidity)
    LP_COLLECT_FEES = "LP_COLLECT_FEES"
    # Native token wrap/unwrap (ETH<->WETH, MATIC<->WMATIC, etc.)
    WRAP_NATIVE = "WRAP_NATIVE"
    UNWRAP_NATIVE = "UNWRAP_NATIVE"
    # Emergency deleverage — structurally a repay but carries risk-event context
    # (trigger_reason, observed_hf, target_hf) so dashboards and accounting can
    # distinguish forced unwinds from routine repays.
    DELEVERAGE = "DELEVERAGE"
    # ──────────────────────────────────────────────────────────────────────
    # P0 PLACEHOLDERS (VIB-4165 / VIB-4160 T5) — locked design item #5.
    #
    # These five enum values exist WITHOUT real connectors so future code paths
    # (LLM tool calls, strategy templates, the agent_tools PolicyEngine) cannot
    # silently smuggle CDP / liquidation / stablecoin-mint operations through
    # generic BORROW / REPAY / SUPPLY and pollute lending accounting before the
    # real connector ships in P1. The compiler MUST raise NotImplementedError on
    # each — see ``_raise_if_placeholder_intent`` in
    # ``almanak/framework/intents/compiler.py`` and
    # ``tests/unit/intents/test_placeholder_compilers.py`` (Hard Ratification
    # Condition #5).
    LIQUIDATE = "LIQUIDATE"
    OPEN_CDP = "OPEN_CDP"
    MINT_STABLE = "MINT_STABLE"
    REPAY_STABLE = "REPAY_STABLE"
    CLOSE_CDP = "CLOSE_CDP"


# =============================================================================
# Core Intent Data Classes (kept in vocabulary.py)
# =============================================================================


class SwapIntent(BaseIntent):
    """Intent to swap one token for another.

    Attributes:
        from_token: Symbol or address of the token to swap from
        to_token: Symbol or address of the token to swap to
        amount_usd: Amount to swap in USD terms (mutually exclusive with amount)
        amount: Amount to swap in token terms, or "all" to use output from previous step
        max_slippage: Maximum acceptable slippage (e.g., 0.005 = 0.5%)
        max_price_impact: Maximum acceptable price impact vs oracle price (e.g., 0.50 = 50%).
            If the on-chain quoter returns an amount deviating more than this from the oracle
            estimate, compilation fails. Defaults to None (uses compiler config default of 30%).
        protocol: Preferred protocol for the swap (e.g., "uniswap_v3", "enso")
        chain: Source chain for execution (defaults to strategy's primary chain)
        destination_chain: Destination chain for cross-chain swaps (None for same-chain)
        priority_fee_level: Solana priority fee level for Jupiter swaps.
            Valid values: "low", "medium", "high", "veryHigh". Defaults to "veryHigh".
        priority_fee_max_lamports: Maximum priority fee in lamports for Jupiter swaps.
            Defaults to 1_000_000 (0.001 SOL).
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Note:
        When amount="all", the swap will use the entire output from the previous
        step in a sequence. This is useful for chaining operations like:
        bridge -> swap -> supply. The actual amount is resolved at execution time.

        For cross-chain swaps, set destination_chain to the target chain name.
        Cross-chain swaps require protocol="enso" as Enso handles the bridging.

    Example:
        # Same-chain swap
        Intent.swap("USDC", "WETH", amount_usd=1000, chain="arbitrum")

        # Cross-chain swap: Base USDC -> Arbitrum WETH
        Intent.swap("USDC", "WETH", amount_usd=1000,
                    chain="base", destination_chain="arbitrum", protocol="enso")
    """

    from_token: str
    to_token: str
    amount_usd: OptionalSafeDecimal = None
    amount: OptionalChainedAmount = None
    max_slippage: SafeDecimal = Field(default=Decimal("0.005"))
    max_price_impact: OptionalSafeDecimal = Field(
        default=None,
        description="Maximum acceptable price impact vs oracle price (e.g., 0.50 = 50%). "
        "Compilation fails if quoter/oracle deviation exceeds this. "
        "Defaults to None (uses compiler config default of 30%).",
    )
    protocol: str | None = None
    chain: str | None = None
    destination_chain: str | None = None
    priority_fee_level: str | None = Field(
        default=None,
        description="Solana priority fee level for Jupiter swaps: 'low', 'medium', 'high', 'veryHigh'. "
        "Defaults to 'veryHigh' when None.",
    )
    priority_fee_max_lamports: int | None = Field(
        default=None,
        description="Maximum priority fee in lamports for Jupiter swaps. Defaults to 1_000_000 when None.",
    )
    swap_params: dict[str, Any] | None = Field(
        default=None,
        description="Optional connector-specific routing/escape-hatch parameters, mirroring the "
        "blessed ``protocol_params`` precedent on LP/stake intents (VIB-5548 / ALM-2889). "
        "Additive and optional: connectors that do not consume it ignore it entirely. "
        "Reachable keys today: Aerodrome — ``classic`` (bool: force Classic vs CL routing), "
        "``tick_spacing`` (positive int: pin a CL pool's tick spacing), ``stable`` (bool: "
        "Classic stable vs volatile pool type); Curve — ``pool`` (str address for "
        "disambiguation), ``oracle_guard_bps`` (int), ``strict_oracle_guard`` (bool). "
        "Only the shape is validated centrally; deeper per-key validation lives in each "
        "connector compiler.",
    )
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    # Centrally shape-validated ``swap_params`` keys (VIB-5548). Per-connector
    # compilers own deeper semantics; this map only rejects structurally
    # malformed values early so a typo'd escape hatch fails loudly at
    # construction rather than being silently ignored downstream.
    _SWAP_PARAMS_BOOL_KEYS: frozenset[str] = frozenset({"classic", "stable", "strict_oracle_guard"})
    _SWAP_PARAMS_POSITIVE_INT_KEYS: frozenset[str] = frozenset({"tick_spacing", "oracle_guard_bps"})

    @model_validator(mode="after")
    def validate_swap_intent(self) -> "SwapIntent":
        """Validate that either amount_usd or amount is provided."""
        if self.amount_usd is None and self.amount is None:
            raise ValueError("Either amount_usd or amount must be provided")
        if self.amount_usd is not None and self.amount is not None:
            raise ValueError("Only one of amount_usd or amount should be provided")
        if self.amount_usd is not None and self.amount_usd <= 0:
            raise ValueError("amount_usd must be positive")
        # Validate amount - either positive Decimal or "all"
        if self.amount is not None:
            if isinstance(self.amount, Decimal) and self.amount <= 0:
                raise ValueError("amount must be positive")
            elif not isinstance(self.amount, Decimal) and self.amount != "all":
                raise ValueError("amount must be a positive Decimal or 'all'")
        if self.max_slippage < 0 or self.max_slippage > 1:
            raise ValueError("max_slippage must be between 0 and 1")
        if self.max_price_impact is not None and (self.max_price_impact <= 0 or self.max_price_impact > 1):
            raise ValueError("max_price_impact must be between 0 (exclusive) and 1 (inclusive)")
        # Cross-chain swaps require an aggregator protocol (Enso or LiFi)
        if self.is_cross_chain and self.protocol and self.protocol.lower() not in ("enso", "lifi"):
            raise ValueError("Cross-chain swaps require protocol='enso' or protocol='lifi'")
        self._validate_swap_params()
        return self

    def _validate_swap_params(self) -> None:
        """Shape-only validation of the optional ``swap_params`` escape hatch.

        Only invoked when ``swap_params`` is not None. Mirrors the
        ``protocol_params`` precedent on :class:`LPOpenIntent`: the central
        validator rejects structurally malformed values (wrong container type,
        non-bool booleans, non-positive ints) so a typo fails loudly at
        construction; the *meaning* of each key stays owned by the connector
        compiler that reads it.
        """
        if self.swap_params is None:
            return
        if not isinstance(self.swap_params, dict):
            raise ValueError("swap_params must be a dict")
        for key in self._SWAP_PARAMS_BOOL_KEYS:
            if key in self.swap_params and not isinstance(self.swap_params[key], bool):
                raise ValueError(f"swap_params.{key} must be a bool, got {type(self.swap_params[key]).__name__}")
        for key in self._SWAP_PARAMS_POSITIVE_INT_KEYS:
            if key in self.swap_params:
                val = self.swap_params[key]
                # bool is a subclass of int; reject it explicitly so
                # swap_params={"tick_spacing": True} is not silently coerced.
                if isinstance(val, bool) or not isinstance(val, int) or val <= 0:
                    raise ValueError(f"swap_params.{key} must be a positive integer, got {val!r}")

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.amount == "all"

    @property
    def is_cross_chain(self) -> bool:
        """Check if this is a cross-chain swap."""
        return self.destination_chain is not None and self.chain is not None and self.destination_chain != self.chain

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.SWAP

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary.

        Backward compatible with existing serialization format.
        """
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        # Handle amount - preserve "all" as string (model_dump should do this)
        if self.amount == "all":
            data["amount"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "SwapIntent":
        """Deserialize a dictionary to a SwapIntent.

        Backward compatible with existing serialization format.
        """
        # Remove "type" field as it's not part of the model
        clean_data = {k: v for k, v in data.items() if k != "type"}

        # Handle created_at string -> datetime
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])

        return cls.model_validate(clean_data)


# =============================================================================
# RangeSpec — typed concentrated-liquidity range (VIB-5555 / FOUND-1)
# =============================================================================

# Protocols where the legacy ``range_lower``/``range_upper`` fields historically
# carried raw tick values (signed integers) rather than price bounds (always
# positive). Module-level so the backward-compat bridge below and the
# ``LPOpenIntent`` validator share one source of truth. Mirrored onto the class
# (``LPOpenIntent._TICK_BASED_LP_PROTOCOLS``) for callers / the backtest
# extractor that read it off an intent instance.
_TICK_BASED_LP_PROTOCOLS: frozenset[str] = frozenset({"aerodrome_slipstream"})

_RANGE_SPEC_TICK_DEPRECATION = (
    "Passing raw integer/negative ticks via range_lower/range_upper for "
    "'aerodrome_slipstream' is deprecated; pass a TickBand (or set the range via a "
    "RangeSpec). Legacy tick interpretation will be removed in a future release."
)


class PriceBand(AlmanakImmutableModel):
    """Concentrated-liquidity range expressed as a human-readable price band.

    This is the canonical, protocol-agnostic LP-range form (the default UX).
    ``lower``/``upper`` are prices in the same denomination as the legacy
    :attr:`LPOpenIntent.range_lower`/:attr:`~LPOpenIntent.range_upper` (token1
    per token0); every concentrated-liquidity connector converts the band to
    ticks internally. Use this to express "open a ±band around the current
    price" portably across Uniswap V3/V4, Aerodrome Slipstream, etc.
    """

    kind: Literal["price"] = "price"
    lower: SafeDecimal
    upper: SafeDecimal

    @model_validator(mode="after")
    def _validate_price_band(self) -> "PriceBand":
        if self.lower <= 0:
            raise ValueError("PriceBand.lower must be positive")
        if self.lower >= self.upper:
            raise ValueError("PriceBand.lower must be less than PriceBand.upper")
        return self


class TickBand(AlmanakImmutableModel):
    """Concentrated-liquidity range expressed as raw protocol ticks (escape hatch).

    ``lower``/``upper`` are signed integer ticks in the connector's native tick
    space (e.g. Uniswap V3 / Aerodrome Slipstream, where the current-price tick
    is frequently negative). Use this only when you deliberately want to bypass
    the price->tick conversion and address ticks directly.
    """

    kind: Literal["tick"] = "tick"
    lower: int
    upper: int

    @model_validator(mode="after")
    def _validate_tick_band(self) -> "TickBand":
        if self.lower >= self.upper:
            raise ValueError("TickBand.lower must be less than TickBand.upper")
        return self


# Discriminated union: ``kind`` selects the variant on (de)serialization.
RangeSpec = Annotated[PriceBand | TickBand, Field(discriminator="kind")]


def _range_spec_bounds(spec: Any) -> tuple[Decimal, Decimal]:
    """Return ``(lower, upper)`` as ``Decimal`` from a RangeSpec instance or dict.

    Accepts a :class:`PriceBand`/:class:`TickBand` instance or its serialized
    ``{"kind": ..., "lower": ..., "upper": ...}`` form (the deserialize path).
    """
    if isinstance(spec, PriceBand):
        return spec.lower, spec.upper
    if isinstance(spec, TickBand):
        return Decimal(spec.lower), Decimal(spec.upper)
    if isinstance(spec, dict):
        kind = spec.get("kind")
        try:
            lower = spec["lower"]
            upper = spec["upper"]
        except KeyError as exc:
            raise ValueError("range_spec must include 'kind', 'lower', and 'upper'") from exc
        if kind == "price":
            return validate_decimal_safe(lower), validate_decimal_safe(upper)
        if kind == "tick":
            return Decimal(int(lower)), Decimal(int(upper))
    raise ValueError(f"Unrecognized range_spec (expected PriceBand/TickBand): {spec!r}")


def _is_tick_shaped(lower: Decimal, upper: Decimal) -> bool:
    """Heuristic bridge (design §Migration Step 1).

    Integer-valued or non-positive bounds are treated as raw ticks; positive
    fractional bounds are treated as prices. Only consulted for legacy
    ``range_lower``/``range_upper`` on tick-based protocols (Slipstream) so the
    bridge never silently reinterprets a deployed intent.
    """
    if lower <= 0 or upper <= 0:
        return True
    return lower == lower.to_integral_value() and upper == upper.to_integral_value()


def _bridge_legacy_range(protocol: str, lower: Decimal, upper: Decimal) -> PriceBand | TickBand:
    """Map legacy ``range_lower``/``range_upper`` onto a typed :data:`RangeSpec`.

    Preserves on-chain semantics exactly: price-based protocols get a
    :class:`PriceBand`; a tick-based protocol (Slipstream) with tick-shaped
    bounds gets a :class:`TickBand` plus a :class:`DeprecationWarning`. Built via
    ``model_construct`` (unvalidated) so the :class:`LPOpenIntent` validator stays
    the single source of the legacy error messages.
    """
    if protocol in _TICK_BASED_LP_PROTOCOLS and _is_tick_shaped(lower, upper):
        # Ticks are integers. A non-integral tick-shaped bound (e.g. -1800.5)
        # would be silently truncated by int() below, so the synthesised TickBand
        # would no longer agree with the preserved legacy range_lower/range_upper
        # and a later serialize()->deserialize() would trip the conflict check and
        # fail to rehydrate. Reject fail-closed rather than truncate.
        if lower != lower.to_integral_value() or upper != upper.to_integral_value():
            raise ValueError(f"tick-based legacy bounds for '{protocol}' must be integer-valued (got {lower}, {upper})")
        warnings.warn(_RANGE_SPEC_TICK_DEPRECATION, DeprecationWarning, stacklevel=3)
        return TickBand.model_construct(kind="tick", lower=int(lower), upper=int(upper))
    return PriceBand.model_construct(kind="price", lower=lower, upper=upper)


class LPOpenIntent(BaseIntent):
    """Intent to open a liquidity position.

    Attributes:
        pool: Pool address or identifier
        amount0: Amount of token0 to provide
        amount1: Amount of token1 to provide
        range_spec: Canonical typed concentrated-liquidity range — a
            :class:`PriceBand` (prices, the default UX) or :class:`TickBand` (raw
            ticks, native escape hatch). When supplied it is the source of truth
            and the legacy ``range_lower``/``range_upper`` are derived from it.
            When omitted, a ``range_spec`` is synthesised from
            ``range_lower``/``range_upper`` via the backward-compat bridge so the
            field always round-trips. Connectors still read
            ``range_lower``/``range_upper`` (unchanged).
        range_lower: Lower price bound for concentrated liquidity (legacy field;
            kept for backward compatibility — see ``range_spec``)
        range_upper: Upper price bound for concentrated liquidity (legacy field;
            kept for backward compatibility — see ``range_spec``)
        protocol: LP protocol (e.g., "uniswap_v3", "camelot")
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        protocol_params: Optional protocol-specific parameters (e.g., {"bin_range": 10} for TraderJoe V2)
        coin_amounts: Optional pool-coin-aligned full allocation vector for multi-coin
            pools (e.g. Curve 3pool/4pool). When set, ``coin_amounts[i]`` is the amount to
            deposit for pool coin index ``i``, indexed exactly as the pool orders its
            ``coins``. This makes it possible to target non-leading coins — e.g. depositing
            only USDC.e (index 1) + USDT (index 2) into a Polygon 3pool without touching DAI
            (index 0), which the two-slot ``amount0``/``amount1`` mapping cannot express.
            Additive and optional: connectors that do not consume it (Uniswap V3/V4,
            Aerodrome, TraderJoe V2, Pendle, …) ignore it entirely and continue to read
            ``amount0``/``amount1``. When ``None`` (the default), behaviour is unchanged.
        max_slippage: Optional maximum acceptable slippage applied to the deposit's
            min-mint floor (e.g. ``0.005`` = 0.5%), in the same units as
            :attr:`SwapIntent.max_slippage`. Consumed by the Curve compiler to size the
            ``add_liquidity`` ``min_mint`` calldata. When ``None`` (the default), the
            connector falls back to its built-in default (Curve: 50 bps), so existing
            callers are byte-for-byte unchanged. Connectors that do not consume it ignore
            it entirely.
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created
    """

    pool: str
    amount0: SafeDecimal
    amount1: SafeDecimal
    range_lower: SafeDecimal
    range_upper: SafeDecimal
    range_spec: RangeSpec | None = None
    protocol: str = "uniswap_v3"
    chain: str | None = None
    protocol_params: dict[str, Any] | None = None
    coin_amounts: list[SafeDecimal] | None = None
    max_slippage: OptionalSafeDecimal = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    # Protocols where range_lower/range_upper carry raw tick values (integers that may be
    # negative) rather than price bounds (which are always positive).  The positivity
    # guard below is skipped for these protocols. Sourced from the module-level
    # constant so the backward-compat bridge and this class agree.
    _TICK_BASED_LP_PROTOCOLS: frozenset[str] = _TICK_BASED_LP_PROTOCOLS

    @model_validator(mode="before")
    @classmethod
    def _normalize_range_spec(cls, data: Any) -> Any:
        """Reconcile the typed ``range_spec`` with legacy ``range_lower``/``range_upper``.

        Backward-compat bridge (VIB-5555, design §Migration Step 1), **canonical
        path only** — when ``range_spec`` is supplied, derive the legacy
        ``range_lower``/``range_upper`` from it so connectors that still read
        those fields keep working unchanged. If a caller also supplies legacy
        bounds (e.g. a serialize round-trip), they must agree with the spec — a
        conflicting hand-built pair is rejected rather than silently resolved.

        The reverse direction (legacy bounds -> synthesised ``range_spec``) is
        handled in the after-validator: doing it here would route the
        ``model_construct``-bridged spec back through discriminated-union field
        validation, surfacing the variant's own error message instead of the
        legacy ``range_lower``/``range_upper`` messages callers depend on.
        """
        if not isinstance(data, dict):
            return data
        spec = data.get("range_spec")
        if spec is None:
            return data
        spec_lower, spec_upper = _range_spec_bounds(spec)
        if data.get("range_lower") is not None and validate_decimal_safe(data["range_lower"]) != spec_lower:
            raise ValueError("range_spec conflicts with range_lower")
        if data.get("range_upper") is not None and validate_decimal_safe(data["range_upper"]) != spec_upper:
            raise ValueError("range_spec conflicts with range_upper")
        data.setdefault("range_lower", spec_lower)
        data.setdefault("range_upper", spec_upper)
        return data

    @model_validator(mode="after")
    def validate_lp_open_intent(self) -> "LPOpenIntent":
        """Validate LP open parameters."""
        if self.amount0 < 0:
            raise ValueError("amount0 must be non-negative")
        if self.amount1 < 0:
            raise ValueError("amount1 must be non-negative")
        if self.amount0 == 0 and self.amount1 == 0 and self.coin_amounts is None:
            raise ValueError("At least one amount must be positive")
        if self.coin_amounts is not None:
            self._validate_coin_amounts()
        if self.max_slippage is not None and (self.max_slippage < 0 or self.max_slippage > 1):
            raise ValueError("max_slippage must be between 0 and 1")
        if self.range_lower >= self.range_upper:
            raise ValueError("range_lower must be less than range_upper")
        # Fail-closed gate (VIB-5555): a ``TickBand`` addresses raw protocol ticks
        # directly, so it is only meaningful on a protocol whose compiler consumes
        # ticks. On a price-based protocol the compilers read
        # ``range_lower``/``range_upper`` as PRICES (e.g. ``uniswap_v3`` computes
        # ``Decimal(1) / range_upper`` in its price->tick path), so a raw TickBand
        # would be silently misexecuted as a price AND would bypass the
        # price-positivity guard below. Reject until the compiler seam (VIB-5556)
        # wires ``range_spec.kind`` into the price->tick conversion; that ticket
        # relaxes this gate per protocol as each compiler learns to honor a
        # TickBand. Legacy callers never reach here with a TickBand — the bridge
        # only synthesises one for tick-based protocols, and after this check.
        if isinstance(self.range_spec, TickBand) and self.protocol not in self._TICK_BASED_LP_PROTOCOLS:
            raise ValueError(
                "TickBand range_spec is only valid for tick-based protocols "
                f"({sorted(self._TICK_BASED_LP_PROTOCOLS)}); protocol "
                f"'{self.protocol}' interprets range bounds as prices"
            )
        # Skip positivity check for raw-tick ranges: their values are Uniswap
        # V3-style ticks (integers) which are legitimately negative for pools
        # where the current price tick is below zero (e.g. WETH/USDC on Base).
        # A ``TickBand`` range_spec is authoritative; the legacy protocol set is
        # retained for the ``model_construct`` path where the bridge did not run.
        is_tick_range = isinstance(self.range_spec, TickBand) or self.protocol in self._TICK_BASED_LP_PROTOCOLS
        if not is_tick_range and self.range_lower <= 0:
            raise ValueError("range_lower must be positive")
        if self.protocol_params is not None:
            if not isinstance(self.protocol_params, dict):
                raise ValueError("protocol_params must be a dict")
            if "bin_range" in self.protocol_params:
                br = self.protocol_params["bin_range"]
                if isinstance(br, bool) or not isinstance(br, int) or br < 1 or br > 100:
                    raise ValueError(f"protocol_params.bin_range must be an integer between 1 and 100, got {br}")
        # Backward-compat bridge (VIB-5555): synthesise the canonical typed
        # range_spec from the now-validated legacy range_lower/range_upper so the
        # field always round-trips. Runs after the legacy checks above so their
        # error messages take precedence; ``object.__setattr__`` is used because
        # the model is frozen. A model_construct'd instance is stored directly
        # (not re-validated) so the legacy values pass through unchanged.
        if self.range_spec is None:
            object.__setattr__(
                self,
                "range_spec",
                _bridge_legacy_range(self.protocol, self.range_lower, self.range_upper),
            )
        return self

    def _validate_coin_amounts(self) -> None:
        """Validate the pool-coin-aligned ``coin_amounts`` allocation vector.

        Only invoked when ``coin_amounts`` is not None.
        """
        assert self.coin_amounts is not None  # narrowed by caller
        # coin_amounts is consumed only by the Curve compiler. Any other LP
        # protocol reads amount0/amount1 and would silently ignore it, opening a
        # 0-liquidity position. Fail loudly instead of silently mis-funding.
        if self.protocol != "curve":
            raise ValueError(
                "coin_amounts is only supported for the 'curve' protocol; "
                f"protocol '{self.protocol}' uses amount0/amount1"
            )
        # coin_amounts is the full pool-coin-aligned allocation vector, so
        # amount0/amount1 are unused for this path. Reject mixing the two mappings
        # rather than silently dropping amount0/amount1.
        if self.amount0 != 0 or self.amount1 != 0:
            raise ValueError(
                "Cannot provide both coin_amounts and amount0/amount1; coin_amounts is the full allocation vector"
            )
        if len(self.coin_amounts) == 0:
            raise ValueError("coin_amounts must not be empty when provided")
        if any(a < 0 for a in self.coin_amounts):
            raise ValueError("coin_amounts entries must be non-negative")
        if all(a == 0 for a in self.coin_amounts):
            raise ValueError("coin_amounts must have at least one positive entry")

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.LP_OPEN

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "LPOpenIntent":
        """Deserialize a dictionary to an LPOpenIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class LPCloseIntent(BaseIntent):
    """Intent to close a liquidity position.

    Attributes:
        position_id: Identifier of the position to close (e.g., NFT token ID).
            When ``amount="all"`` is set, ``position_id`` is resolved at
            execution time from the prior LP_OPEN's minted-LP wei (the runner
            writes the chained wei integer into this field as its string form).
            When ``amount is None`` (the default), the literal ``position_id``
            is used unchanged — byte-identical to historical behaviour.
        pool: Pool address (optional, for validation)
        collect_fees: Whether to collect accumulated fees
        protocol: LP protocol (e.g., "uniswap_v3", "camelot")
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        protocol_params: Optional protocol-specific parameters (e.g., V4 requires
            ``{"liquidity": <int>, "currency0": "<addr>", "currency1": "<addr>"}``
            from an on-chain position query)
        amount: Pure opt-in MARKER for WEI-denominated chaining (VIB-5346). The
            only accepted value is the literal ``"all"``; a numeric Decimal is
            rejected (close-all is the only meaningful chained semantic for a
            fungible LP position, and a numeric ``amount`` would be a second
            silent carrier). When ``"all"``, the runner resolves the prior
            LP_OPEN minted-LP wei into ``position_id``; the compiler still reads
            ``int(position_id)`` and never sees ``"all"``. Only fungible-LP
            connectors on the fail-closed allowlist
            (``_FUNGIBLE_LP_CHAINING_PROTOCOLS`` in
            ``almanak.framework.strategies.lp_position_tracker``; currently only
            Pendle) support this. The runner-level capability gate
            (``StrategyRunner._resolve_chained_amount_for_intent``) is the
            PRIMARY control: it REJECTS any non-allowlisted protocol
            (NFT token-ids, bin-ids, pool/wrapper identities, uncategorised) with
            a COMPILATION_FAILED result BEFORE the minted wei is resolved into
            ``position_id``. Per-connector LP_CLOSE compiler guards are
            defense-in-depth for the direct-compile path only.
        max_slippage: Optional maximum acceptable slippage applied to the
            withdrawal's min-amounts floor (e.g. ``0.005`` = 0.5%), in the same
            units as :attr:`SwapIntent.max_slippage`. Consumed by the Curve
            compiler to size the ``remove_liquidity`` ``min_amounts`` calldata.
            When ``None`` (the default), the connector falls back to its built-in
            default (Curve: 50 bps), so existing callers are byte-for-byte
            unchanged. Connectors that do not consume it ignore it entirely.
        coin_index: Optional single-sided exit selector (VIB-5437). When set to a
            non-negative pool-coin index, the close withdraws the ENTIRE position
            into that one coin via Curve's ``remove_liquidity_one_coin`` (min-out
            sized from the pool's on-chain ``calc_withdraw_one_coin``). When
            ``None`` (the default), the close is proportional
            (``remove_liquidity`` across all coins) — byte-for-byte unchanged for
            existing callers. Consumed only by the Curve compiler; other LP
            connectors ignore it.
        imbalanced_amounts: Optional imbalanced exit selector (VIB-5438). When set
            to a per-coin vector of EXACT amounts to withdraw (positional by
            pool-coin index, human units), the close uses Curve's
            ``remove_liquidity_imbalance``: the pool burns however much LP is needed
            to deliver those exact amounts, capped at a MAX-BURN ceiling the adapter
            sizes from the pool's on-chain ``calc_token_amount(amounts,
            is_deposit=False)`` (fail-closed; never an unbounded cap). Mutually
            exclusive with ``coin_index`` and with ``amount`` (the close-all chaining
            marker — an exact-amounts withdrawal is not a close-all). When ``None``
            (the default), the close is proportional — byte-for-byte unchanged.
            StableSwap-family only; consumed only by the Curve compiler.

            NOTE on slippage: on a LEGACY (non-NG) StableSwap pool,
            ``calc_token_amount(is_deposit=False)`` EXCLUDES the imbalance fee, so a
            ``max_slippage`` of exactly ``0`` makes the derived max-burn ceiling too
            tight and the on-chain ``remove_liquidity_imbalance`` ALWAYS reverts
            ("Slippage screwed you"). This is fail-closed — no funds are lost — but
            it wastes gas. Leave ``max_slippage`` unset (50 bps default) or give it
            enough headroom to cover the fee. StableSwap-NG pools fold the fee into
            the quote, so ``0`` is fine there.
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created
    """

    position_id: str
    pool: str | None = None
    collect_fees: bool = True
    protocol: str = "uniswap_v3"
    chain: str | None = None
    protocol_params: dict[str, Any] | None = None
    amount: OptionalChainedAmount = None
    max_slippage: OptionalSafeDecimal = None
    coin_index: int | None = None
    imbalanced_amounts: list[SafeDecimal] | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_lp_close_intent(self) -> "LPCloseIntent":
        """Validate the LP_CLOSE chaining marker.

        ``amount`` is a pure opt-in marker: the only accepted value is the
        literal ``"all"``. A numeric Decimal is rejected — close-all is the
        only meaningful chained semantic for a fungible LP position, and a
        numeric ``amount`` would be a second silent carrier alongside
        ``position_id``. When ``amount is None`` (the historical invariant),
        ``position_id`` must be a non-empty string.
        """
        if self.amount is not None:
            if self.amount != "all":
                raise ValueError(
                    "LPCloseIntent.amount must be the literal 'all' (or None); numeric amounts are not supported"
                )
        elif not isinstance(self.position_id, str) or not self.position_id:
            raise ValueError("position_id must be a non-empty string when amount is None")
        if self.max_slippage is not None and (self.max_slippage < 0 or self.max_slippage > 1):
            raise ValueError("max_slippage must be between 0 and 1")
        # coin_index opts into a single-sided close. Reject bool (a bool is an int
        # subclass in Python, but `True`/`False` are never a valid coin index) and
        # negatives. The connector validates the upper bound against the resolved
        # pool's coin count, where n_coins is known.
        if self.coin_index is not None:
            if isinstance(self.coin_index, bool) or self.coin_index < 0:
                raise ValueError("coin_index must be a non-negative integer when set")
            # coin_index, like imbalanced_amounts, is compiled ONLY by connectors
            # declaring the lp_close_exit_selectors capability; any other LP
            # connector would silently ignore it. Fail fast rather than let a
            # caller think a single-sided exit will happen on, e.g., the default
            # uniswap_v3 protocol (CodeRabbit — applied to both exit selectors
            # for a consistent contract).
            if not _supports_lp_close_exit_selectors(self.protocol):
                raise ValueError(
                    f"coin_index is not supported by protocol '{self.protocol}'. "
                    f"Protocols supporting a single-sided LP close: "
                    f"{_lp_close_exit_selector_protocols()}."
                )
        # imbalanced_amounts opts into an imbalanced close (exact per-coin amounts
        # OUT, capped by a derived max-burn). Mutually exclusive with the
        # single-sided coin_index path AND with the close-all `amount` chaining
        # marker (an exact-amounts withdrawal is not a close-all). Require a
        # non-empty, non-negative vector with at least one positive entry (an
        # all-zero withdrawal is a no-op). NaN/Infinity are already rejected by the
        # SafeDecimal field schema (Pydantic's finite-number constraint). The
        # connector validates the vector length against the resolved pool's coin
        # count, where n_coins is known.
        if self.imbalanced_amounts is not None:
            # imbalanced_amounts is compiled ONLY by connectors declaring the
            # lp_close_exit_selectors capability; every other LP connector would
            # silently ignore it. Fail fast rather than let a caller think an
            # exact-amounts withdrawal will happen on, e.g., the default
            # uniswap_v3 protocol (CodeRabbit). ``protocol`` is normalized
            # case-insensitively to match the capability-registry keys.
            if not _supports_lp_close_exit_selectors(self.protocol):
                raise ValueError(
                    f"imbalanced_amounts is not supported by protocol '{self.protocol}'. "
                    f"Protocols supporting an imbalanced LP close: "
                    f"{_lp_close_exit_selector_protocols()}."
                )
            if self.coin_index is not None:
                raise ValueError("imbalanced_amounts and coin_index are mutually exclusive")
            if self.amount is not None:
                raise ValueError("imbalanced_amounts and amount (close-all) are mutually exclusive")
            if len(self.imbalanced_amounts) == 0:
                raise ValueError("imbalanced_amounts must be a non-empty list when set")
            if any(a < 0 for a in self.imbalanced_amounts):
                raise ValueError("imbalanced_amounts must all be non-negative")
            if not any(a > 0 for a in self.imbalanced_amounts):
                raise ValueError("imbalanced_amounts must have at least one positive amount")
        return self

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.amount == "all"

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.LP_CLOSE

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        # Preserve the "all" marker as a string so it round-trips through
        # deserialize (mirrors SwapIntent.serialize).
        if self.amount == "all":
            data["amount"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "LPCloseIntent":
        """Deserialize a dictionary to an LPCloseIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class CollectFeesIntent(BaseIntent):
    """Intent to collect accumulated fees from an LP position without closing it.

    This is useful for fee harvesting and auto-compounding strategies that want
    to claim earned fees while keeping their liquidity position open.

    Supported protocols: ``traderjoe_v2``, ``uniswap_v4``,
    ``aerodrome_slipstream``, and every Uniswap-V3 fork
    (``uniswap_v3``, ``sushiswap_v3``, ``pancakeswap_v3``, ``agni_finance``;
    see ``connectors.protocol_aliases.UNISWAP_V3_FORKS``).

    Aerodrome Classic (``protocol="aerodrome"``, volatile/stable Solidly-fork
    pools) does NOT support standalone fee collection: trading fees auto-compound
    into pool reserves and are realized only on liquidity removal. Use
    ``LPCloseIntent(collect_fees=True)`` instead.

    Attributes:
        pool: Pool identifier (format: TOKEN_X/TOKEN_Y/BIN_STEP for TraderJoe V2,
            TOKEN_A/TOKEN_B/FEE for Uniswap V4, or
            TOKEN_A/TOKEN_B/TICK_SPACING for Aerodrome Slipstream)
        protocol: LP protocol (e.g., "traderjoe_v2", "uniswap_v4",
            "aerodrome_slipstream")
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        protocol_params: Optional protocol-specific parameters.
            For Uniswap V4: ``{"position_id": <int>, "currency0": "<addr>",
            "currency1": "<addr>"}``.
            For Aerodrome Slipstream: ``{"position_id": "<NFT tokenId>"}``.
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Example:
        # Collect fees from a TraderJoe V2 LP position
        intent = Intent.collect_fees(
            pool="WAVAX/USDC/20",
            protocol="traderjoe_v2",
        )
    """

    pool: str
    protocol: str
    chain: str | None = None
    protocol_params: dict[str, Any] | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_collect_fees_intent(self) -> "CollectFeesIntent":
        """Validate the collect fees intent."""
        if not self.pool:
            raise ValueError("pool is required for collect fees intent")
        return self

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.LP_COLLECT_FEES

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "CollectFeesIntent":
        """Deserialize a dictionary to a CollectFeesIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class HoldIntent(BaseIntent):
    """Intent to take no action (wait).

    This is useful when a strategy explicitly decides not to act,
    as opposed to returning None which might indicate an error.

    Attributes:
        reason: Optional reason for holding (for logging/debugging)
        reason_code: Optional structured reason code for alerting/filtering
            (e.g., "INSUFFICIENT_BALANCE", "RSI_NEUTRAL", "PRICE_BELOW_THRESHOLD")
        reason_details: Optional structured details for the hold reason
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created
    """

    reason: str | None = None
    reason_code: str | None = None
    reason_details: dict[str, Any] | None = None
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.HOLD

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "HoldIntent":
        """Deserialize a dictionary to a HoldIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


# =============================================================================
# Imported Intent Classes (from sub-modules)
# =============================================================================

# WARNING: Circular import boundary — DO NOT move these imports above this point.
# The sub-modules (lending_intents, advanced_intents, etc.) import IntentType,
# PROTOCOL_CAPABILITIES, InterestRateMode, ChainedAmount, and error classes from
# this module. All those symbols MUST be defined before these re-imports execute.
# Moving any of them below this block will cause ImportError at startup.

from .advanced_intents import (  # noqa: E402, F401
    FlashLoanCallbackIntent,
    FlashLoanIntent,
    StakeIntent,
    UnstakeIntent,
    UnwrapNativeIntent,
    VaultDepositIntent,
    VaultRedeemIntent,
    WrapNativeIntent,
)
from .lending_intents import (  # noqa: E402, F401
    BorrowIntent,
    DeleverageIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawIntent,
)
from .perp_intents import (  # noqa: E402, F401
    PerpCancelIntent,
    PerpCloseIntent,
    PerpOpenIntent,
)
from .prediction_intents import (  # noqa: E402, F401
    PredictionBuyIntent,
    PredictionOrderType,
    PredictionOutcome,
    PredictionRedeemIntent,
    PredictionSellIntent,
    PredictionShareAmount,
    PredictionTimeInForce,
)

# =============================================================================
# Union Type for All Intents
# =============================================================================

# Note: BridgeIntent is defined in .bridge module to avoid circular imports
# AnyIntent includes all base intents; BridgeIntent is handled dynamically
# in deserialize() and can be accessed via Intent.bridge() factory method
type AnyIntent = (
    SwapIntent
    | LPOpenIntent
    | LPCloseIntent
    | CollectFeesIntent
    | BorrowIntent
    | RepayIntent
    | DeleverageIntent
    | SupplyIntent
    | WithdrawIntent
    | PerpOpenIntent
    | PerpCloseIntent
    | PerpCancelIntent
    | FlashLoanIntent
    | StakeIntent
    | UnstakeIntent
    | HoldIntent
    | PredictionBuyIntent
    | PredictionSellIntent
    | PredictionRedeemIntent
    | VaultDepositIntent
    | VaultRedeemIntent
    | WrapNativeIntent
    | UnwrapNativeIntent
)


# =============================================================================
# Intent Sequence for Dependent Actions
# =============================================================================


@dataclass
class IntentSequence:
    """A sequence of intents that must execute in order (dependent actions).

    IntentSequence wraps a list of intents that have dependencies between them
    and must execute sequentially. This is used when the output of one intent
    feeds into the input of the next (e.g., swap output -> bridge input).

    Intents that are NOT in a sequence can execute in parallel if they are
    independent (e.g., two swaps on different chains).

    Attributes:
        intents: List of intents to execute in order
        sequence_id: Unique identifier for this sequence
        created_at: Timestamp when the sequence was created
        description: Optional description of the sequence purpose

    Example:
        # Create a sequence of dependent actions
        sequence = Intent.sequence([
            Intent.swap("USDC", "ETH", amount=Decimal("1000"), chain="base"),
            Intent.bridge(token="ETH", amount="all", from_chain="base", to_chain="arbitrum"),
            Intent.supply(protocol="aave_v3", token="WETH", amount="all", chain="arbitrum"),
        ])

        # Return from decide() - will execute sequentially
        return sequence
    """

    intents: list[AnyIntent]
    sequence_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    description: str | None = None

    def __post_init__(self) -> None:
        """Validate the sequence."""
        if not self.intents:
            raise InvalidSequenceError("IntentSequence must contain at least one intent")
        if len(self.intents) < 1:
            raise InvalidSequenceError("IntentSequence must contain at least one intent")

    def __len__(self) -> int:
        """Return the number of intents in the sequence."""
        return len(self.intents)

    def __iter__(self):
        """Iterate over intents in the sequence."""
        return iter(self.intents)

    def __getitem__(self, index: int) -> AnyIntent:
        """Get intent at index."""
        return self.intents[index]

    @property
    def first(self) -> AnyIntent:
        """Get the first intent in the sequence."""
        return self.intents[0]

    @property
    def last(self) -> AnyIntent:
        """Get the last intent in the sequence."""
        return self.intents[-1]

    def serialize(self) -> dict[str, Any]:
        """Serialize the sequence to a dictionary."""
        return {
            "type": "SEQUENCE",
            "sequence_id": self.sequence_id,
            "created_at": self.created_at.isoformat(),
            "description": self.description,
            "intents": [intent.serialize() for intent in self.intents],
        }

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "IntentSequence":
        """Deserialize a dictionary to an IntentSequence.

        Note: This requires the Intent.deserialize function to be available,
        which creates a circular dependency. The actual deserialization is
        done in the Intent class.
        """
        from .vocabulary import Intent  # Import here to avoid circular import

        intents = [Intent.deserialize(intent_data) for intent_data in data["intents"]]
        return cls(
            intents=intents,
            sequence_id=data.get("sequence_id", str(uuid.uuid4())),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.now(UTC),
            description=data.get("description"),
        )


# Type for decide() return value: single intent, sequence, or list of parallel intents.
#
# CAVEAT: list returns are for genuinely independent intents (different chains,
# different venues, different output tokens). For multiple positions sharing a
# wallet basis-pool, pool/market state, or position-registry semantic group
# (e.g. two LPs on the same pool, two SUPPLYs on the same Aave market), emit
# one Intent per iteration via a phase or slot machine that advances only when
# on_intent_executed observes a real position_id. See strategies/accounting/
# lp_dual/ and lp_triple/ for reference, and blueprint 04 §Multi-position
# dispatch for the contract.
type DecideResult = AnyIntent | IntentSequence | list[AnyIntent | IntentSequence] | None


# =============================================================================
# Intent Factory Class
# =============================================================================


class Intent:
    """Factory class for creating intents with a fluent API.

    This class provides static factory methods for creating intents,
    making strategy code more readable and ergonomic.

    Example:
        # Instead of:
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount_usd=Decimal("1000"))

        # You can write:
        intent = Intent.swap(from_token="USDC", to_token="ETH", amount_usd=Decimal("1000"))
    """

    @staticmethod
    def swap(
        from_token: str,
        to_token: str,
        amount_usd: Decimal | None = None,
        amount: ChainedAmount | None = None,
        max_slippage: Decimal = Decimal("0.005"),
        max_price_impact: Decimal | None = None,
        protocol: str | None = None,
        chain: str | None = None,
        destination_chain: str | None = None,
        registry_handle: str | None = None,
        swap_params: dict[str, Any] | None = None,
    ) -> SwapIntent:
        """Create a swap intent.

        Args:
            from_token: Symbol or address of the token to swap from
            to_token: Symbol or address of the token to swap to
            amount_usd: Amount to swap in USD terms
            amount: Amount to swap in token terms, or "all" to use previous step output
            max_slippage: Maximum acceptable slippage (default 0.5%)
            max_price_impact: Maximum acceptable price impact vs oracle price (e.g., 0.50 = 50%).
                Compilation fails if quoter/oracle deviation exceeds this.
                Defaults to None (uses compiler config default of 30%).
            protocol: Preferred protocol for the swap
            chain: Source chain for execution (defaults to strategy's primary chain)
            destination_chain: Destination chain for cross-chain swaps (None for same-chain)
            swap_params: Optional connector-specific routing/escape-hatch params
                (e.g. Aerodrome ``{"classic": True}`` / ``{"tick_spacing": 200}``;
                Curve ``{"pool": "0x..."}``). See :class:`SwapIntent.swap_params`.

        Returns:
            SwapIntent: The created swap intent

        Example:
            # Swap $1000 worth of USDC to ETH
            intent = Intent.swap("USDC", "ETH", amount_usd=Decimal("1000"))

            # Swap 0.5 ETH to USDC on Base
            intent = Intent.swap("ETH", "USDC", amount=Decimal("0.5"), chain="base")

            # Swap all ETH from previous step output
            intent = Intent.swap("ETH", "USDC", amount="all", chain="base")

            # Cross-chain swap: Base USDC -> Arbitrum WETH via Enso
            intent = Intent.swap("USDC", "WETH", amount_usd=Decimal("1000"),
                                 chain="base", destination_chain="arbitrum", protocol="enso")
        """
        return SwapIntent(
            from_token=from_token,
            to_token=to_token,
            amount_usd=amount_usd,
            amount=amount,
            max_slippage=max_slippage,
            max_price_impact=max_price_impact,
            protocol=protocol,
            chain=chain,
            destination_chain=destination_chain,
            registry_handle=registry_handle,
            swap_params=swap_params,
        )

    @staticmethod
    def lp_open(
        pool: str,
        amount0: Decimal = Decimal("0"),
        amount1: Decimal = Decimal("0"),
        range_lower: Decimal | None = None,
        range_upper: Decimal | None = None,
        range_spec: RangeSpec | None = None,
        protocol: str = "uniswap_v3",
        chain: str | None = None,
        protocol_params: dict[str, Any] | None = None,
        coin_amounts: list[Decimal] | None = None,
        max_slippage: Decimal | None = None,
        registry_handle: str | None = None,
    ) -> LPOpenIntent:
        """Create an LP open intent.

        Args:
            pool: Pool address or identifier
            amount0: Amount of token0 to provide (default 0; ignored when
                ``coin_amounts`` is supplied for a multi-coin pool)
            amount1: Amount of token1 to provide (default 0; ignored when
                ``coin_amounts`` is supplied for a multi-coin pool)
            range_lower: Lower price bound for concentrated liquidity (legacy).
                When neither ``range_spec`` nor an explicit bound is given it
                defaults to ``Decimal("1")`` so fungible-LP (Curve) callers need
                not pass dummy bounds. Mutually exclusive with ``range_spec``
                unless the two agree.
            range_upper: Upper price bound for concentrated liquidity (legacy).
                Defaults to ``Decimal("2")``; see ``range_lower``.
            range_spec: Canonical typed range — :class:`PriceBand` or
                :class:`TickBand`. When supplied, ``range_lower``/``range_upper``
                are derived from it (do not pass conflicting legacy bounds).
            protocol: LP protocol (default "uniswap_v3")
            chain: Target chain for execution (defaults to strategy's primary chain)
            protocol_params: Optional protocol-specific parameters (e.g., {"bin_range": 10} for TraderJoe V2)
            coin_amounts: Optional pool-coin-aligned full allocation vector for
                multi-coin pools (e.g. Curve 3pool/4pool). ``coin_amounts[i]`` is the
                amount for pool coin index ``i``. When supplied, the Curve compiler
                uses it directly instead of mapping ``amount0``/``amount1`` to indices
                0/1, so non-leading coins (index 2+) can be targeted. Connectors that
                do not consume it ignore it entirely.
            max_slippage: Optional maximum acceptable slippage on the deposit's min-mint
                floor (e.g. 0.005 = 0.5%). When None (the default), the connector uses its
                built-in default (Curve: 50 bps). Consumed only by the Curve compiler.

        Returns:
            LPOpenIntent: The created LP open intent

        Example:
            # Open an ETH/USDC LP position around the current price
            intent = Intent.lp_open(
                pool="0x8ad...",
                amount0=Decimal("1"),  # 1 ETH
                amount1=Decimal("2000"),  # 2000 USDC
                range_lower=Decimal("1800"),
                range_upper=Decimal("2200"),
            )

            # Open a Curve Polygon 3pool position in USDC.e (idx 1) + USDT (idx 2),
            # skipping DAI (idx 0):
            intent = Intent.lp_open(
                pool="USDC.e/USDT/DAI",
                coin_amounts=[Decimal("0"), Decimal("500"), Decimal("500")],
                protocol="curve",
                chain="polygon",
            )
        """
        # When no typed range_spec is supplied, fall back to the historical
        # legacy-bound defaults (1 / 2). When a range_spec IS supplied, leave
        # unsupplied bounds as None so the model derives them from the spec
        # (rather than tripping the conflict check against the old defaults).
        if range_spec is None:
            if range_lower is None:
                range_lower = Decimal("1")
            if range_upper is None:
                range_upper = Decimal("2")

        kwargs: dict[str, Any] = {
            "pool": pool,
            "amount0": amount0,
            "amount1": amount1,
            "range_spec": range_spec,
            "protocol": protocol,
            "chain": chain,
            "protocol_params": protocol_params,
            "coin_amounts": coin_amounts,
            "max_slippage": max_slippage,
            "registry_handle": registry_handle,
        }
        if range_lower is not None:
            kwargs["range_lower"] = range_lower
        if range_upper is not None:
            kwargs["range_upper"] = range_upper
        return LPOpenIntent(**kwargs)

    @staticmethod
    def lp_close(
        position_id: str,
        pool: str | None = None,
        collect_fees: bool = True,
        protocol: str = "uniswap_v3",
        chain: str | None = None,
        protocol_params: dict[str, Any] | None = None,
        amount: ChainedAmount | None = None,
        max_slippage: Decimal | None = None,
        coin_index: int | None = None,
        imbalanced_amounts: list[Decimal] | None = None,
        registry_handle: str | None = None,
    ) -> LPCloseIntent:
        """Create an LP close intent.

        Args:
            position_id: Identifier of the position to close. Ignored as a
                literal when ``amount="all"`` is set (the runner overwrites it
                with the prior LP_OPEN minted-LP wei); pass any placeholder.
            pool: Pool address (optional, for validation)
            collect_fees: Whether to collect accumulated fees (default True)
            protocol: LP protocol (default "uniswap_v3")
            chain: Target chain for execution (defaults to strategy's primary chain)
            protocol_params: Optional protocol-specific parameters (e.g., V4 requires
                liquidity, currency0, currency1 from an on-chain position query)
            amount: WEI-denominated chaining marker (VIB-5346). The only accepted
                value is the literal ``"all"``. When set, the runner resolves the
                prior LP_OPEN minted-LP wei into ``position_id`` at execution time.
                Only fungible-LP connectors on the fail-closed allowlist (e.g.
                Pendle) support it. The runner-level capability gate REJECTS any
                non-allowlisted protocol (NFT token-ids, bin-ids, pool/wrapper
                identities) with COMPILATION_FAILED BEFORE resolving the wei;
                per-connector compiler guards are defense-in-depth only.
            max_slippage: Optional maximum acceptable slippage on the withdrawal's
                min-amounts floor (e.g. 0.005 = 0.5%). When None (the default), the
                connector uses its built-in default (Curve: 50 bps). Consumed only by
                the Curve compiler.
            coin_index: Optional single-sided exit selector (Curve only, VIB-5437).
                A non-negative pool-coin index withdraws the whole position into
                that one coin via ``remove_liquidity_one_coin``; ``None`` (default)
                keeps the proportional all-coin close.
            imbalanced_amounts: Optional imbalanced exit (Curve StableSwap only,
                VIB-5438). A per-coin vector of EXACT amounts to withdraw (positional
                by pool-coin index) routed via ``remove_liquidity_imbalance`` with a
                fail-closed max-burn ceiling. Mutually exclusive with ``coin_index``;
                ``None`` (default) keeps the proportional all-coin close.

        Returns:
            LPCloseIntent: The created LP close intent

        Example:
            # Close an LP position and collect fees
            intent = Intent.lp_close(position_id="12345")

            # Close without collecting fees
            intent = Intent.lp_close(position_id="12345", collect_fees=False)

            # Chain a fungible-LP close off the prior LP_OPEN's minted liquidity
            # (Pendle): the runner resolves the minted wei into position_id.
            intent = Intent.lp_close(position_id="0", protocol="pendle", amount="all")
        """
        return LPCloseIntent(
            position_id=position_id,
            pool=pool,
            collect_fees=collect_fees,
            protocol=protocol,
            chain=chain,
            protocol_params=protocol_params,
            amount=amount,
            max_slippage=max_slippage,
            coin_index=coin_index,
            imbalanced_amounts=imbalanced_amounts,
            registry_handle=registry_handle,
        )

    @staticmethod
    def collect_fees(
        pool: str,
        *,
        protocol: str,
        chain: str | None = None,
        protocol_params: dict[str, Any] | None = None,
        registry_handle: str | None = None,
    ) -> CollectFeesIntent:
        """Create a collect fees intent to harvest LP fees without closing the position.

        Supported protocols:

        - ``traderjoe_v2``: pool format ``"TOKEN_X/TOKEN_Y/BIN_STEP"``.
        - ``uniswap_v4``: requires ``protocol_params={"position_id": <int>,
          "currency0": "<addr>", "currency1": "<addr>"}``.
        - ``aerodrome_slipstream``: concentrated-liquidity (NFT) positions on
          Base; requires ``protocol_params={"position_id": "<NFT tokenId>"}``.

        Aerodrome Classic (volatile/stable Solidly-fork pools) does NOT support
        standalone fee collection: trading fees auto-compound into the pool
        reserves and can only be realized by removing liquidity. Use
        ``Intent.lp_close(..., collect_fees=True)`` to harvest while exiting,
        or open a Slipstream CL position when in-position fee collection is
        required.

        Args:
            pool: Pool identifier (e.g., "WAVAX/USDC/20" for TraderJoe V2,
                "WETH/USDC/200" for Aerodrome Slipstream).
            protocol: LP protocol — required keyword-only argument.
                Supported literals (see compiler dispatch):
                ``"traderjoe_v2"``, ``"uniswap_v4"``,
                ``"aerodrome_slipstream"``, and the Uniswap V3 family
                (``"uniswap_v3"``, ``"sushiswap_v3"``, ``"pancakeswap_v3"``,
                ``"agni_finance"``). ``"aerodrome"`` (Classic V1/V2) does
                NOT support standalone fee collection — fees auto-compound
                into pool reserves; use ``Intent.lp_close(..., collect_fees=True)``
                instead. Previously defaulted to ``"traderjoe_v2"`` (the
                default was dropped to prevent silent mis-routing on
                multi-protocol strategies).
            chain: Target chain for execution (defaults to strategy's primary chain)
            protocol_params: Optional protocol-specific parameters. See per-protocol
                requirements above.

        Returns:
            CollectFeesIntent: The created collect fees intent

        Example:
            # TraderJoe V2 — symbolic pool identifier is sufficient
            intent = Intent.collect_fees(pool="WAVAX/USDC/20", protocol="traderjoe_v2")

            # Aerodrome Slipstream — NFT tokenId required
            intent = Intent.collect_fees(
                pool="WETH/USDC/200",
                protocol="aerodrome_slipstream",
                chain="base",
                protocol_params={"position_id": "12345"},
            )
        """
        return CollectFeesIntent(
            pool=pool,
            protocol=protocol,
            chain=chain,
            protocol_params=protocol_params,
            registry_handle=registry_handle,
        )

    @staticmethod
    def borrow(
        protocol: str,
        collateral_token: str,
        collateral_amount: ChainedAmount,
        borrow_token: str,
        borrow_amount: Decimal,
        interest_rate_mode: InterestRateMode | None = None,
        market_id: str | None = None,
        chain: str | None = None,
        registry_handle: str | None = None,
    ) -> BorrowIntent:
        """Create a borrow intent.

        Args:
            protocol: Lending protocol (e.g., "aave_v3", "morpho_blue")
            collateral_token: Token to use as collateral
            collateral_amount: Amount of collateral to supply, or "all" for previous step output
            borrow_token: Token to borrow
            borrow_amount: Amount to borrow
            interest_rate_mode: Interest rate mode for Aave ('variable' only, stable is deprecated).
                Only applies to protocols that support rate mode selection.
                For Aave V3, defaults to 'variable' if not specified.
            market_id: Market identifier for isolated lending protocols (e.g., Morpho Blue).
                Required for morpho/morpho_blue, ignored for aave_v3.
            chain: Target chain for execution (defaults to strategy's primary chain)

        Returns:
            BorrowIntent: The created borrow intent

        Example:
            # Supply ETH as collateral and borrow USDC on Arbitrum with variable rate
            intent = Intent.borrow(
                protocol="aave_v3",
                collateral_token="ETH",
                collateral_amount=Decimal("1"),
                borrow_token="USDC",
                borrow_amount=Decimal("1500"),
                interest_rate_mode="variable",
                chain="arbitrum",
            )

            # Borrow on Morpho Blue (requires market_id)
            intent = Intent.borrow(
                protocol="morpho_blue",
                collateral_token="wstETH",
                collateral_amount=Decimal("0"),  # Already supplied
                borrow_token="USDC",
                borrow_amount=Decimal("1500"),
                market_id="0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
                chain="ethereum",
            )
        """
        return BorrowIntent(
            protocol=protocol,
            collateral_token=collateral_token,
            collateral_amount=collateral_amount,
            borrow_token=borrow_token,
            borrow_amount=borrow_amount,
            interest_rate_mode=interest_rate_mode,
            market_id=market_id,
            chain=chain,
            registry_handle=registry_handle,
        )

    @staticmethod
    def repay(
        protocol: str,
        token: str,
        amount: ChainedAmount | None = None,
        repay_full: bool = False,
        interest_rate_mode: InterestRateMode | None = None,
        market_id: str | None = None,
        chain: str | None = None,
        registry_handle: str | None = None,
    ) -> RepayIntent:
        """Create a repay intent.

        Args:
            protocol: Lending protocol (e.g., "aave_v3", "morpho_blue")
            token: Token to repay
            amount: Amount to repay, or "all" to use previous step output.
                Defaults to Decimal("0") when repay_full=True (amount is ignored in that case).
                Required when repay_full=False.
            repay_full: If True, repay the full outstanding debt (sends MAX_UINT256 to protocol).
                When True, amount is ignored and may be omitted.
            interest_rate_mode: Interest rate mode for protocols that support it.
                Aave V3: 'variable' (default). Stable rate is deprecated. Must match
                the rate mode used when borrowing.
            market_id: Market identifier for isolated lending protocols (e.g., Morpho Blue).
                Required for morpho/morpho_blue, ignored for aave_v3.
            chain: Target chain for execution (defaults to strategy's primary chain)

        Returns:
            RepayIntent: The created repay intent

        Example:
            # Repay 500 USDC on Aave (variable rate)
            intent = Intent.repay(
                protocol="aave_v3",
                token="USDC",
                amount=Decimal("500"),
                interest_rate_mode="variable",
            )

            # Repay full debt on Aave (amount can be omitted when repay_full=True)
            intent = Intent.repay(
                protocol="aave_v3",
                token="USDC",
                repay_full=True,
            )

            # Repay full debt on Morpho Blue
            intent = Intent.repay(
                protocol="morpho_blue",
                token="USDC",
                repay_full=True,
                market_id="0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
            )
        """
        if amount is None:
            if repay_full:
                amount = Decimal("0")
            else:
                raise ValueError("amount is required when repay_full=False")
        return RepayIntent(
            protocol=protocol,
            token=token,
            amount=amount,
            repay_full=repay_full,
            interest_rate_mode=interest_rate_mode,
            market_id=market_id,
            chain=chain,
            registry_handle=registry_handle,
        )

    @staticmethod
    def deleverage(
        protocol: str,
        token: str,
        amount: ChainedAmount | None = None,
        repay_full: bool = False,
        interest_rate_mode: InterestRateMode | None = None,
        market_id: str | None = None,
        chain: str | None = None,
        trigger_reason: str = "",
        observed_hf: Decimal | None = None,
        target_hf: Decimal | None = None,
        registry_handle: str | None = None,
    ) -> "DeleverageIntent":
        """Create an emergency deleverage intent.

        Structurally identical to a repay at the protocol level, but carries
        risk-event context (trigger_reason, observed_hf, target_hf) so accounting
        and dashboards can distinguish forced unwinds from routine repays.

        Use this instead of ``Intent.repay()`` when the repay is triggered by a
        health-factor guard or emergency risk manager.

        Args:
            protocol: Lending protocol (e.g., "aave_v3", "morpho_blue")
            token: Token to repay
            amount: Amount to repay, or "all" to use previous step output.
                Defaults to Decimal("0") when repay_full=True (ignored in that case).
                Required when repay_full=False.
            repay_full: If True, repay the full outstanding debt (sends MAX_UINT256).
                When True, amount is ignored and may be omitted.
            interest_rate_mode: Interest rate mode for protocols that support it.
            market_id: Market identifier for isolated lending protocols (e.g., Morpho Blue).
            chain: Target chain for execution (defaults to strategy's primary chain)
            trigger_reason: Human-readable description of why the deleverage was triggered.
                (e.g., "HF 1.08 < emergency_threshold 1.2: full deleverage")
            observed_hf: Health factor observed at the time of triggering.
            target_hf: Desired health factor after the deleverage completes.

        Returns:
            DeleverageIntent: The created deleverage intent

        Example:
            # Full emergency deleverage on Aave when HF drops below threshold
            intent = Intent.deleverage(
                protocol="aave_v3",
                token="USDC",
                repay_full=True,
                trigger_reason="HF 1.08 below emergency threshold 1.2",
                observed_hf=Decimal("1.08"),
                target_hf=Decimal("2.0"),
            )
        """
        if amount is None:
            if repay_full:
                amount = Decimal("0")
            else:
                raise ValueError("amount is required when repay_full=False")
        return DeleverageIntent(
            protocol=protocol,
            token=token,
            amount=amount,
            repay_full=repay_full,
            interest_rate_mode=interest_rate_mode,
            market_id=market_id,
            chain=chain,
            trigger_reason=trigger_reason,
            observed_hf=observed_hf,
            target_hf=target_hf,
            registry_handle=registry_handle,
        )

    @staticmethod
    def supply(
        protocol: str,
        token: str,
        amount: ChainedAmount,
        use_as_collateral: bool = True,
        market_id: str | None = None,
        chain: str | None = None,
        registry_handle: str | None = None,
    ) -> SupplyIntent:
        """Create a supply intent.

        Args:
            protocol: Lending protocol (e.g., "aave_v3", "morpho_blue")
            token: Token to supply
            amount: Amount to supply, or "all" to use previous step output
            use_as_collateral: Whether to enable as collateral (default True)
            market_id: Market identifier for isolated lending protocols (e.g., Morpho Blue).
                Required for morpho/morpho_blue, ignored for aave_v3.
            chain: Target chain for execution (defaults to strategy's primary chain)

        Returns:
            SupplyIntent: The created supply intent

        Example:
            # Supply 1 ETH to Aave V3 on Arbitrum
            intent = Intent.supply(
                protocol="aave_v3",
                token="WETH",
                amount=Decimal("1"),
                chain="arbitrum",
            )

            # Supply wstETH to Morpho Blue market
            intent = Intent.supply(
                protocol="morpho_blue",
                token="wstETH",
                amount=Decimal("1"),
                market_id="0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
                chain="ethereum",
            )
        """
        return SupplyIntent(
            protocol=protocol,
            token=token,
            amount=amount,
            use_as_collateral=use_as_collateral,
            market_id=market_id,
            chain=chain,
            registry_handle=registry_handle,
        )

    @staticmethod
    def withdraw(
        protocol: str,
        token: str,
        amount: ChainedAmount,
        withdraw_all: bool = False,
        market_id: str | None = None,
        chain: str | None = None,
        *,
        is_collateral: bool = True,
        registry_handle: str | None = None,
    ) -> WithdrawIntent:
        """Create a withdraw intent.

        Args:
            protocol: Lending protocol (e.g., "aave_v3", "morpho_blue")
            token: Token to withdraw
            amount: Amount to withdraw, or "all" to use previous step output
            withdraw_all: If True, withdraw all available balance
            is_collateral: For Morpho Blue: True withdraws collateral, False withdraws
                loan token (e.g., USDC lent to earn interest). Default True.
                Other protocols ignore this field.
            market_id: Market identifier for isolated lending protocols (e.g., Morpho Blue).
                Required for morpho/morpho_blue, ignored for aave_v3.
            chain: Target chain for execution (defaults to strategy's primary chain)

        Returns:
            WithdrawIntent: The created withdraw intent

        Example:
            # Withdraw 0.5 ETH from Aave V3
            intent = Intent.withdraw(
                protocol="aave_v3",
                token="WETH",
                amount=Decimal("0.5"),
            )

            # Withdraw all collateral from Morpho Blue
            intent = Intent.withdraw(
                protocol="morpho_blue",
                token="wstETH",
                amount=Decimal("0"),  # Ignored when withdraw_all=True
                withdraw_all=True,
                market_id="0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
            )
        """
        return WithdrawIntent(
            protocol=protocol,
            token=token,
            amount=amount,
            withdraw_all=withdraw_all,
            is_collateral=is_collateral,
            market_id=market_id,
            chain=chain,
            registry_handle=registry_handle,
        )

    @staticmethod
    def perp_open(
        market: str,
        collateral_token: str,
        collateral_amount: ChainedAmount,
        size_usd: Decimal,
        is_long: bool = True,
        leverage: Decimal = Decimal("1"),
        max_slippage: Decimal = Decimal("0.01"),
        protocol: str = "gmx_v2",
        chain: str | None = None,
        registry_handle: str | None = None,
    ) -> PerpOpenIntent:
        """Create a perpetual position open intent.

        Args:
            market: Market identifier (e.g., "ETH/USD") or market address
            collateral_token: Token symbol or address for collateral
            collateral_amount: Amount of collateral in token terms, or "all" for previous step output
            size_usd: Position size in USD terms
            is_long: True for long, False for short (default True)
            leverage: Target leverage (default 1x)
            max_slippage: Maximum acceptable slippage (default 1%)
            protocol: Perpetuals protocol (default "gmx_v2")
            chain: Target chain for execution (defaults to strategy's primary chain)

        Returns:
            PerpOpenIntent: The created perp open intent

        Example:
            # Open a 5x long ETH position with 0.1 ETH collateral on Arbitrum
            intent = Intent.perp_open(
                market="ETH/USD",
                collateral_token="WETH",
                collateral_amount=Decimal("0.1"),
                size_usd=Decimal("1750"),  # ~5x at $3500 ETH
                is_long=True,
                leverage=Decimal("5"),
                chain="arbitrum",
            )

            # Use all collateral from previous step
            intent = Intent.perp_open(
                market="ETH/USD",
                collateral_token="WETH",
                collateral_amount="all",  # Use previous step output
                size_usd=Decimal("1750"),
                is_long=True,
                chain="arbitrum",
            )
        """
        return PerpOpenIntent(
            market=market,
            collateral_token=collateral_token,
            collateral_amount=collateral_amount,
            size_usd=size_usd,
            is_long=is_long,
            leverage=leverage,
            max_slippage=max_slippage,
            protocol=protocol,
            chain=chain,
            registry_handle=registry_handle,
        )

    @staticmethod
    def perp_close(
        market: str,
        collateral_token: str,
        is_long: bool,
        size_usd: Decimal | None = None,
        max_slippage: Decimal = Decimal("0.01"),
        protocol: str = "gmx_v2",
        chain: str | None = None,
        position_id: str | None = None,
        registry_handle: str | None = None,
    ) -> PerpCloseIntent:
        """Create a perpetual position close intent.

        Args:
            market: Market identifier (e.g., "ETH/USD") or market address
            collateral_token: Token symbol or address for collateral
            is_long: Position direction
            size_usd: Amount to close in USD (None = close full position)
            max_slippage: Maximum acceptable slippage (default 1%)
            protocol: Perpetuals protocol (default "gmx_v2")
            chain: Target chain for execution (defaults to strategy's primary chain)
            position_id: Optional venue-specific identifier (0x-prefixed hex). Required
                for venues that key positions on a ``bytes32`` (notably
                ``pancakeswap_perps``); ignored by venues that close by market+side
                (``gmx_v2``, ``hyperliquid``, ``drift``).

        Returns:
            PerpCloseIntent: The created perp close intent

        Example:
            # Close entire long ETH position on GMX V2 (no position_id needed)
            intent = Intent.perp_close(
                market="ETH/USD",
                collateral_token="WETH",
                is_long=True,
            )

            # Close a PancakeSwap Perps position by tradeHash
            intent = Intent.perp_close(
                market="BTC/USD",
                collateral_token="BNB",
                is_long=True,
                protocol="pancakeswap_perps",
                position_id="0xabcd...",  # bytes32 tradeHash from open receipt
            )
        """
        return PerpCloseIntent(
            market=market,
            collateral_token=collateral_token,
            is_long=is_long,
            size_usd=size_usd,
            max_slippage=max_slippage,
            protocol=protocol,
            chain=chain,
            position_id=position_id,
            registry_handle=registry_handle,
        )

    @staticmethod
    def perp_cancel_order(
        order_key: str,
        protocol: str = "gmx_v2",
        chain: str | None = None,
        registry_handle: str | None = None,
    ) -> PerpCancelIntent:
        """Create a perpetual pending-order cancel intent (recover committed collateral).

        Cancels a pending (unfilled) perp order, returning its committed collateral
        and unspent execution fee to the wallet. Not a position open/close — a
        refund of committed-but-unspent collateral (the recovery half of VIB-5116).

        Args:
            order_key: On-chain order key (``bytes32``: 0x-prefixed, exactly 66 chars)
                identifying the pending order to cancel. Obtained from the teardown
                residual-discovery read or the open receipt.
            protocol: Perpetuals protocol that owns the order (default "gmx_v2").
            chain: Target chain for execution (defaults to strategy's primary chain).

        Returns:
            PerpCancelIntent: The created perp cancel intent.

        Example:
            # Cancel a stranded GMX V2 pending order and recover its collateral
            intent = Intent.perp_cancel_order(
                order_key="0x1234...cdef",  # bytes32 from read_pending_orders
                protocol="gmx_v2",
                chain="arbitrum",
            )
        """
        return PerpCancelIntent(
            order_key=order_key,
            protocol=protocol,
            chain=chain,
            registry_handle=registry_handle,
        )

    @staticmethod
    def bridge(
        token: str,
        amount: Decimal | Literal["all"],
        from_chain: str,
        to_chain: str,
        max_slippage: Decimal = Decimal("0.005"),
        preferred_bridge: str | None = None,
        destination_address: str | None = None,
        registry_handle: str | None = None,
    ) -> Any:
        """Create a bridge intent for cross-chain asset transfer.

        Bridge intents represent cross-chain token transfers. They can be used
        standalone or as part of an IntentSequence for complex multi-step
        operations like swap -> bridge -> supply.

        When amount="all", the bridge will use the entire output from the
        previous step in a sequence. This is useful for chaining operations.

        Args:
            token: Token symbol to bridge (e.g., "ETH", "USDC", "WBTC")
            amount: Amount to bridge (Decimal) or "all" to use previous step's output
            from_chain: Source chain identifier (e.g., "base", "arbitrum")
            to_chain: Destination chain identifier (e.g., "arbitrum", "optimism")
            max_slippage: Maximum acceptable slippage (default 0.5%)
            preferred_bridge: Optional preferred bridge adapter name (e.g., "across", "stargate")
            destination_address: Optional recipient address on the destination chain.
                If None, the compiler resolves it from chain_wallets (multi-wallet mode)
                or uses the source wallet address (single-wallet mode).

        Returns:
            BridgeIntent: The created bridge intent

        Example:
            # Bridge 1000 USDC from Base to Arbitrum
            intent = Intent.bridge(
                token="USDC",
                amount=Decimal("1000"),
                from_chain="base",
                to_chain="arbitrum",
            )

            # Bridge all ETH from previous step (in a sequence)
            sequence = Intent.sequence([
                Intent.swap("USDC", "ETH", amount=Decimal("1000"), chain="base"),
                Intent.bridge(
                    token="ETH",
                    amount="all",  # Use output from swap
                    from_chain="base",
                    to_chain="arbitrum",
                ),
                Intent.supply(protocol="aave_v3", token="WETH", amount="all", chain="arbitrum"),
            ])

            # Bridge with preferred bridge
            intent = Intent.bridge(
                token="USDC",
                amount=Decimal("5000"),
                from_chain="arbitrum",
                to_chain="optimism",
                preferred_bridge="across",  # Prefer Across for fast finality
            )
        """
        # Import here to avoid circular import
        from .bridge import BridgeIntent

        return BridgeIntent(
            token=token,
            amount=amount,
            from_chain=from_chain,
            to_chain=to_chain,
            max_slippage=max_slippage,
            preferred_bridge=preferred_bridge,
            destination_address=destination_address,
            registry_handle=registry_handle,
        )

    @staticmethod
    def flash_loan(
        provider: Literal["aave", "balancer", "morpho", "auto"],
        token: str,
        amount: Decimal,
        callback_intents: list[FlashLoanCallbackIntent],
        chain: str | None = None,
        registry_handle: str | None = None,
    ) -> FlashLoanIntent:
        """Create a flash loan intent with callback operations.

        A flash loan allows borrowing assets without collateral, provided the
        borrowed amount plus fees is repaid within the same transaction.

        Args:
            provider: Flash loan provider ("aave", "balancer", or "auto")
                     - "aave": 0.09% fee, high liquidity
                     - "balancer": 0% fee, lower liquidity
                     - "auto": Automatically select based on availability and fees
            token: Token to borrow via flash loan
            amount: Amount to borrow
            callback_intents: List of intents to execute with borrowed funds.
                            Must return sufficient funds to repay loan + fees.
            chain: Target chain for execution (defaults to strategy's primary chain)

        Returns:
            FlashLoanIntent: The created flash loan intent

        Example:
            # Flash loan arbitrage: borrow USDC, swap through two DEXs
            intent = Intent.flash_loan(
                provider="aave",
                token="USDC",
                amount=Decimal("100000"),
                callback_intents=[
                    Intent.swap("USDC", "WETH", amount=Decimal("100000"), protocol="uniswap_v3"),
                    Intent.swap("WETH", "USDC", amount="all", protocol="curve"),
                ],
                chain="ethereum"
            )
        """
        return FlashLoanIntent(
            provider=provider,
            token=token,
            amount=amount,
            callback_intents=callback_intents,
            chain=chain,
            registry_handle=registry_handle,
        )

    @staticmethod
    def hold(
        reason: str | None = None,
        chain: str | None = None,
        reason_code: str | None = None,
        reason_details: dict[str, Any] | None = None,
        registry_handle: str | None = None,
    ) -> HoldIntent:
        """Create a hold intent (no action).

        Args:
            reason: Optional reason for holding (for logging/debugging)
            chain: Target chain for execution (defaults to strategy's primary chain)
            reason_code: Optional structured reason code for alerting/filtering
                (e.g., "INSUFFICIENT_BALANCE", "RSI_NEUTRAL")
            reason_details: Optional structured details for the hold reason

        Returns:
            HoldIntent: The created hold intent

        Example:
            # Hold with no reason
            intent = Intent.hold()

            # Hold with a reason for logging
            intent = Intent.hold(reason="RSI in neutral zone, waiting for signal")

            # Hold with structured reason for alerting
            intent = Intent.hold(
                reason="RSI neutral",
                reason_code="RSI_NEUTRAL",
                reason_details={"rsi": 52.3, "oversold": 30, "overbought": 70},
            )
        """
        return HoldIntent(
            reason=reason,
            chain=chain,
            reason_code=reason_code,
            reason_details=reason_details,
            registry_handle=registry_handle,
        )

    @staticmethod
    def stake(
        protocol: str,
        token_in: str,
        amount: ChainedAmount,
        receive_wrapped: bool = True,
        chain: str | None = None,
        registry_handle: str | None = None,
    ) -> StakeIntent:
        """Create a stake intent for liquid staking protocols.

        Args:
            protocol: Staking protocol (e.g., "lido", "ethena")
            token_in: Token to stake (e.g., "ETH" for Lido, "USDe" for Ethena)
            amount: Amount to stake, or "all" to use previous step output
            receive_wrapped: Whether to receive wrapped version (default True).
                For Lido: True = wstETH (non-rebasing), False = stETH (rebasing)
                For Ethena: Always receives sUSDe regardless of this flag
            chain: Target chain for execution (defaults to strategy's primary chain)

        Returns:
            StakeIntent: The created stake intent

        Example:
            # Stake 1 ETH with Lido on Ethereum, receive wstETH
            intent = Intent.stake(
                protocol="lido",
                token_in="ETH",
                amount=Decimal("1"),
                receive_wrapped=True,
                chain="ethereum",
            )

            # Stake USDe with Ethena
            intent = Intent.stake(
                protocol="ethena",
                token_in="USDe",
                amount=Decimal("10000"),
                chain="ethereum",
            )

            # Stake all ETH from previous step in a sequence
            intent = Intent.stake(
                protocol="lido",
                token_in="ETH",
                amount="all",
                chain="ethereum",
            )
        """
        return StakeIntent(
            protocol=protocol,
            token_in=token_in,
            amount=amount,
            receive_wrapped=receive_wrapped,
            chain=chain,
            registry_handle=registry_handle,
        )

    @staticmethod
    def unstake(
        protocol: str,
        token_in: str,
        amount: ChainedAmount,
        chain: str | None = None,
        protocol_params: "dict[str, Any] | None" = None,
        registry_handle: str | None = None,
    ) -> UnstakeIntent:
        """Create an unstake intent for withdrawing from liquid staking protocols.

        Args:
            protocol: Staking protocol (e.g., "lido", "ethena")
            token_in: Staked token to unstake (e.g., "wstETH" for Lido, "sUSDe" for Ethena)
            amount: Amount to unstake, or "all" to use previous step output
            chain: Target chain for execution (defaults to strategy's primary chain)
            protocol_params: Optional protocol-specific parameters (e.g., {"phase": "cooldown"} for Ethena)

        Returns:
            UnstakeIntent: The created unstake intent

        Example:
            # Unstake 1 wstETH with Lido on Ethereum
            intent = Intent.unstake(
                protocol="lido",
                token_in="wstETH",
                amount=Decimal("1"),
                chain="ethereum",
            )

            # Unstake sUSDe with Ethena (initiates cooldown)
            intent = Intent.unstake(
                protocol="ethena",
                token_in="sUSDe",
                amount=Decimal("10000"),
                chain="ethereum",
            )

            # Unstake all tokens from previous step in a sequence
            intent = Intent.unstake(
                protocol="lido",
                token_in="wstETH",
                amount="all",
                chain="ethereum",
            )
        """
        return UnstakeIntent(
            protocol=protocol,
            token_in=token_in,
            amount=amount,
            chain=chain,
            protocol_params=protocol_params,
            registry_handle=registry_handle,
        )

    @staticmethod
    def wrap(
        token: str,
        amount: ChainedAmount,
        chain: str | None = None,
        registry_handle: str | None = None,
    ) -> WrapNativeIntent:
        """Create a wrap native token intent (e.g. ETH -> WETH).

        Calls the wrapped token's ``deposit()`` function with ``msg.value``
        to convert native currency to its wrapped ERC-20 equivalent.

        Args:
            token: Wrapped token symbol (e.g., "WETH", "WMATIC", "WAVAX")
            amount: Amount of native token to wrap, or "all"
            chain: Target chain for execution

        Returns:
            WrapNativeIntent: The created wrap intent

        Example:
            intent = Intent.wrap(token="WETH", amount=Decimal("0.01"), chain="arbitrum")
        """
        return WrapNativeIntent(token=token, amount=amount, chain=chain, registry_handle=registry_handle)

    @staticmethod
    def unwrap(
        token: str,
        amount: ChainedAmount,
        chain: str | None = None,
        registry_handle: str | None = None,
    ) -> UnwrapNativeIntent:
        """Create an unwrap native token intent (e.g. WETH -> ETH).

        Calls the wrapped token's ``withdraw(uint256)`` function to convert
        wrapped native tokens back to the chain's native currency.

        Args:
            token: Wrapped token symbol to unwrap (e.g., "WETH", "WMATIC", "WAVAX")
            amount: Amount of wrapped token to unwrap, or "all"
            chain: Target chain for execution

        Returns:
            UnwrapNativeIntent: The created unwrap intent

        Example:
            # Unwrap 0.01 WETH to ETH on Arbitrum
            intent = Intent.unwrap(token="WETH", amount=Decimal("0.01"), chain="arbitrum")

            # Unwrap all WETH from previous step in a sequence
            intent = Intent.unwrap(token="WETH", amount="all", chain="arbitrum")
        """
        return UnwrapNativeIntent(token=token, amount=amount, chain=chain, registry_handle=registry_handle)

    @staticmethod
    def ensure_balance(
        token: str,
        min_amount: Decimal,
        target_chain: str,
        max_slippage: Decimal = Decimal("0.005"),
        preferred_bridge: str | None = None,
        registry_handle: str | None = None,
    ) -> Any:
        """Create an ensure_balance intent for automatic cross-chain balance management.

        EnsureBalanceIntent expresses the goal of having at least a certain amount
        of tokens on a specific chain. When resolved (via resolve() method), the
        system will automatically determine the appropriate action:

        1. If target chain has sufficient balance -> HoldIntent (no action)
        2. If another chain has sufficient balance -> BridgeIntent (transfer)
        3. If no single chain has enough -> InsufficientBalanceError

        This simplifies strategy development by abstracting away the complexity of
        cross-chain balance management.

        Args:
            token: Token symbol to ensure (e.g., "ETH", "USDC", "WBTC")
            min_amount: Minimum amount required on target chain
            target_chain: Chain where the balance is needed (e.g., "arbitrum", "base")
            max_slippage: Maximum acceptable slippage for bridging (default 0.5%)
            preferred_bridge: Optional preferred bridge adapter name for transfer

        Returns:
            EnsureBalanceIntent: The created ensure_balance intent

        Example:
            # Ensure at least 1000 USDC on Arbitrum before opening a position
            intent = Intent.ensure_balance(
                token="USDC",
                min_amount=Decimal("1000"),
                target_chain="arbitrum",
            )

            # Ensure at least 2 ETH on Base with custom slippage
            intent = Intent.ensure_balance(
                token="ETH",
                min_amount=Decimal("2"),
                target_chain="base",
                max_slippage=Decimal("0.01"),  # 1% max slippage
                preferred_bridge="across",  # Prefer Across bridge
            )

            # Using ensure_balance in a strategy
            def decide(self, market: MultiChainMarketSnapshot) -> DecideResult:
                # First ensure we have enough USDC on Arbitrum
                ensure_intent = Intent.ensure_balance(
                    token="USDC",
                    min_amount=Decimal("5000"),
                    target_chain="arbitrum",
                )

                # Resolve to concrete intent based on current balances
                target_balance = market.balance("USDC", chain="arbitrum").balance
                chain_balances = {
                    chain: market.balance("USDC", chain=chain).balance
                    for chain in market.chains
                    if chain != "arbitrum"
                }
                resolved_intent = ensure_intent.resolve(target_balance, chain_balances)

                # If resolved to HoldIntent, we can proceed with other actions
                # If resolved to BridgeIntent, execute the bridge first
                return resolved_intent
        """
        # Import here to avoid circular import
        from .ensure_balance import EnsureBalanceIntent

        return EnsureBalanceIntent(
            token=token,
            min_amount=min_amount,
            target_chain=target_chain,
            max_slippage=max_slippage,
            preferred_bridge=preferred_bridge,
            registry_handle=registry_handle,
        )

    @staticmethod
    def prediction_buy(
        market_id: str,
        outcome: Literal["YES", "NO"],
        amount_usd: Decimal | None = None,
        shares: Decimal | None = None,
        max_price: Decimal | None = None,
        order_type: Literal["market", "limit"] = "market",
        time_in_force: Literal["GTC", "IOC", "FOK"] = "GTC",
        expiration_hours: int | None = None,
        protocol: str | None = None,
        chain: str | None = None,
        exit_conditions: PredictionExitConditions | None = None,
        registry_handle: str | None = None,
    ) -> PredictionBuyIntent:
        """Create a prediction buy intent for purchasing outcome shares.

        Buy outcome tokens (YES or NO) on a prediction market like Polymarket.
        Prices represent implied probability (e.g., 0.65 = 65% chance).

        Args:
            market_id: Polymarket market ID or slug (e.g., "will-bitcoin-exceed-100000")
            outcome: Which outcome to buy ("YES" or "NO")
            amount_usd: USDC amount to spend (mutually exclusive with shares)
            shares: Number of shares to buy (mutually exclusive with amount_usd)
            max_price: Maximum price per share (0.01-0.99) for limit orders
            order_type: Order type ("market" or "limit", default "market")
            time_in_force: How long order remains active ("GTC", "IOC", "FOK")
            expiration_hours: Hours until order expires (None = no expiry)
            protocol: Protocol to use (None resolves to the connector default,
                currently "polymarket", at compile time)
            chain: Target chain (defaults to "polygon" for Polymarket)
            exit_conditions: Optional exit conditions for automatic position monitoring
                (stop-loss, take-profit, trailing stop, pre-resolution exit)

        Returns:
            PredictionBuyIntent: The created prediction buy intent

        Example:
            # Buy $100 worth of YES shares at market price
            intent = Intent.prediction_buy(
                market_id="will-bitcoin-exceed-100000",
                outcome="YES",
                amount_usd=Decimal("100"),
            )

            # Buy 50 YES shares with limit order at max price of $0.65
            intent = Intent.prediction_buy(
                market_id="will-bitcoin-exceed-100000",
                outcome="YES",
                shares=Decimal("50"),
                max_price=Decimal("0.65"),
                order_type="limit",
            )

            # Buy NO shares with IOC (immediate or cancel)
            intent = Intent.prediction_buy(
                market_id="will-bitcoin-exceed-100000",
                outcome="NO",
                amount_usd=Decimal("200"),
                time_in_force="IOC",
            )
        """
        return PredictionBuyIntent(
            market_id=market_id,
            outcome=outcome,
            amount_usd=amount_usd,
            shares=shares,
            max_price=max_price,
            order_type=order_type,
            time_in_force=time_in_force,
            expiration_hours=expiration_hours,
            protocol=protocol,
            chain=chain,
            exit_conditions=exit_conditions,
            registry_handle=registry_handle,
        )

    @staticmethod
    def prediction_sell(
        market_id: str,
        outcome: Literal["YES", "NO"],
        shares: Decimal | Literal["all"],
        min_price: Decimal | None = None,
        order_type: Literal["market", "limit"] = "market",
        time_in_force: Literal["GTC", "IOC", "FOK"] = "GTC",
        protocol: str | None = None,
        chain: str | None = None,
        registry_handle: str | None = None,
    ) -> PredictionSellIntent:
        """Create a prediction sell intent for selling outcome shares.

        Sell outcome tokens (YES or NO) on a prediction market like Polymarket.
        Use shares="all" to sell your entire position.

        Args:
            market_id: Polymarket market ID or slug
            outcome: Which outcome to sell ("YES" or "NO")
            shares: Number of shares to sell, or "all" to sell entire position
            min_price: Minimum price per share (0.01-0.99) for limit orders
            order_type: Order type ("market" or "limit", default "market")
            time_in_force: How long order remains active ("GTC", "IOC", "FOK")
            protocol: Protocol to use (None resolves to the connector default,
                currently "polymarket", at compile time)
            chain: Target chain (defaults to "polygon" for Polymarket)

        Returns:
            PredictionSellIntent: The created prediction sell intent

        Example:
            # Sell all YES shares at market price
            intent = Intent.prediction_sell(
                market_id="will-bitcoin-exceed-100000",
                outcome="YES",
                shares="all",
            )

            # Sell 25 NO shares with limit order at min $0.40
            intent = Intent.prediction_sell(
                market_id="will-bitcoin-exceed-100000",
                outcome="NO",
                shares=Decimal("25"),
                min_price=Decimal("0.40"),
                order_type="limit",
            )
        """
        return PredictionSellIntent(
            market_id=market_id,
            outcome=outcome,
            shares=shares,
            min_price=min_price,
            order_type=order_type,
            time_in_force=time_in_force,
            protocol=protocol,
            chain=chain,
            registry_handle=registry_handle,
        )

    @staticmethod
    def prediction_redeem(
        market_id: str,
        outcome: Literal["YES", "NO"] | None = None,
        shares: Decimal | Literal["all"] = "all",
        protocol: str | None = None,
        chain: str | None = None,
        registry_handle: str | None = None,
    ) -> PredictionRedeemIntent:
        """Create a prediction redeem intent for redeeming winning positions.

        Redeem winning outcome tokens after a market has resolved. Winning
        positions redeem for $1 per share in USDC.

        Args:
            market_id: Polymarket market ID or slug
            outcome: Which outcome to redeem ("YES", "NO", or None for both)
            shares: Number of shares to redeem, or "all" (default)
            protocol: Protocol to use (None resolves to the connector default,
                currently "polymarket", at compile time)
            chain: Target chain (defaults to "polygon" for Polymarket)

        Returns:
            PredictionRedeemIntent: The created prediction redeem intent

        Note:
            Redemption is only possible after the market has resolved.
            Losing positions are worthless and cannot be redeemed.

        Example:
            # Redeem all winning positions from a resolved market
            intent = Intent.prediction_redeem(
                market_id="will-bitcoin-exceed-100000",
            )

            # Redeem only YES shares (if YES won)
            intent = Intent.prediction_redeem(
                market_id="will-bitcoin-exceed-100000",
                outcome="YES",
            )

            # Redeem specific number of shares
            intent = Intent.prediction_redeem(
                market_id="will-bitcoin-exceed-100000",
                outcome="YES",
                shares=Decimal("50"),
            )
        """
        return PredictionRedeemIntent(
            market_id=market_id,
            outcome=outcome,
            shares=shares,
            protocol=protocol,
            chain=chain,
            registry_handle=registry_handle,
        )

    @staticmethod
    def vault_deposit(
        protocol: str,
        vault_address: str,
        amount: ChainedAmount,
        deposit_token: str | None = None,
        chain: str | None = None,
        registry_handle: str | None = None,
    ) -> VaultDepositIntent:
        """Create a vault deposit intent for MetaMorpho ERC-4626 vaults.

        Deposits underlying assets into a MetaMorpho vault in exchange for
        vault shares. The vault manages allocation across Morpho Blue markets.

        Args:
            protocol: Vault protocol (must be "metamorpho")
            vault_address: MetaMorpho vault contract address
            amount: Amount of underlying assets to deposit, or "all"
            deposit_token: Underlying token symbol (e.g. "USDC") for backtesting
            chain: Target chain (defaults to strategy's primary chain)

        Returns:
            VaultDepositIntent: The created vault deposit intent

        Example:
            # Deposit 1000 USDC into Steakhouse vault
            intent = Intent.vault_deposit(
                protocol="metamorpho",
                vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
                amount=Decimal("1000"),
                deposit_token="USDC",
                chain="ethereum",
            )
        """
        return VaultDepositIntent(
            protocol=protocol,
            vault_address=vault_address,
            amount=amount,
            deposit_token=deposit_token,
            chain=chain,
            registry_handle=registry_handle,
        )

    @staticmethod
    def vault_redeem(
        protocol: str,
        vault_address: str,
        shares: ChainedAmount,
        deposit_token: str | None = None,
        chain: str | None = None,
        registry_handle: str | None = None,
    ) -> VaultRedeemIntent:
        """Create a vault redeem intent for MetaMorpho ERC-4626 vaults.

        Redeems vault shares to receive underlying assets. No approval needed
        since the user is redeeming their own shares.

        Args:
            protocol: Vault protocol (must be "metamorpho")
            vault_address: MetaMorpho vault contract address
            shares: Number of shares to redeem, or "all" to redeem all
            deposit_token: Underlying token symbol (e.g. "USDC") for backtesting
            chain: Target chain (defaults to strategy's primary chain)

        Returns:
            VaultRedeemIntent: The created vault redeem intent

        Example:
            # Redeem all shares from Steakhouse vault
            intent = Intent.vault_redeem(
                protocol="metamorpho",
                vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
                shares="all",
                deposit_token="USDC",
                chain="ethereum",
            )
        """
        return VaultRedeemIntent(
            protocol=protocol,
            vault_address=vault_address,
            shares=shares,
            deposit_token=deposit_token,
            chain=chain,
            registry_handle=registry_handle,
        )

    @staticmethod
    def sequence(
        intents: list[AnyIntent],
        description: str | None = None,
    ) -> IntentSequence:
        """Create an intent sequence for dependent actions that must execute in order.

        Use this when you have a series of intents where each step depends on the
        previous step's output. For example:
        - Swap USDC -> ETH, then bridge ETH to another chain
        - Bridge tokens, then supply to lending protocol

        The intents in a sequence will always execute sequentially. If any step
        fails, subsequent steps will not execute.

        Args:
            intents: List of intents to execute in order
            description: Optional description of what this sequence accomplishes

        Returns:
            IntentSequence: The created intent sequence

        Raises:
            InvalidSequenceError: If the intents list is empty

        Example:
            # Create a sequence: swap -> bridge -> supply
            return Intent.sequence([
                Intent.swap("USDC", "ETH", amount=Decimal("1000"), chain="base"),
                Intent.supply(protocol="aave_v3", token="WETH", amount=Decimal("0.5"), chain="arbitrum"),
            ], description="Move funds from Base to Arbitrum and deposit")

            # In decide(), return multiple sequences for parallel execution
            return [
                Intent.sequence([swap1, supply1]),  # Execute as sequence
                Intent.sequence([swap2, supply2]),  # Execute in parallel with above
            ]
        """
        return IntentSequence(intents=intents, description=description)

    @staticmethod
    def serialize(intent: AnyIntent) -> dict[str, Any]:
        """Serialize any intent to a dictionary.

        Args:
            intent: The intent to serialize

        Returns:
            dict: The serialized intent
        """
        return intent.serialize()

    @staticmethod
    def deserialize(data: dict[str, Any]) -> Any:
        """Deserialize a dictionary to the appropriate intent type.

        Args:
            data: The serialized intent data

        Returns:
            The deserialized intent (AnyIntent or BridgeIntent)

        Raises:
            ValueError: If the intent type is unknown
        """
        intent_type = data.get("type")
        if intent_type is None:
            raise ValueError("Missing 'type' field in intent data")

        # Handle BridgeIntent separately to avoid circular import
        if intent_type == IntentType.BRIDGE.value:
            from .bridge import BridgeIntent

            return BridgeIntent.deserialize(data)

        # Handle EnsureBalanceIntent separately to avoid circular import
        if intent_type == IntentType.ENSURE_BALANCE.value:
            from .ensure_balance import EnsureBalanceIntent

            return EnsureBalanceIntent.deserialize(data)

        deserializers: dict[str, type[AnyIntent]] = {
            IntentType.SWAP.value: SwapIntent,
            IntentType.LP_OPEN.value: LPOpenIntent,
            IntentType.LP_CLOSE.value: LPCloseIntent,
            IntentType.LP_COLLECT_FEES.value: CollectFeesIntent,
            IntentType.BORROW.value: BorrowIntent,
            IntentType.REPAY.value: RepayIntent,
            IntentType.DELEVERAGE.value: DeleverageIntent,
            IntentType.SUPPLY.value: SupplyIntent,
            IntentType.WITHDRAW.value: WithdrawIntent,
            IntentType.PERP_OPEN.value: PerpOpenIntent,
            IntentType.PERP_CLOSE.value: PerpCloseIntent,
            IntentType.PERP_CANCEL_ORDER.value: PerpCancelIntent,
            IntentType.FLASH_LOAN.value: FlashLoanIntent,
            IntentType.STAKE.value: StakeIntent,
            IntentType.UNSTAKE.value: UnstakeIntent,
            IntentType.HOLD.value: HoldIntent,
            IntentType.PREDICTION_BUY.value: PredictionBuyIntent,
            IntentType.PREDICTION_SELL.value: PredictionSellIntent,
            IntentType.PREDICTION_REDEEM.value: PredictionRedeemIntent,
            IntentType.VAULT_DEPOSIT.value: VaultDepositIntent,
            IntentType.VAULT_REDEEM.value: VaultRedeemIntent,
            IntentType.WRAP_NATIVE.value: WrapNativeIntent,
            IntentType.UNWRAP_NATIVE.value: UnwrapNativeIntent,
        }

        deserializer = deserializers.get(intent_type)
        if deserializer is None:
            raise ValueError(f"Unknown intent type: {intent_type}")

        return deserializer.deserialize(data)

    @staticmethod
    def get_type(intent: AnyIntent) -> IntentType:
        """Get the type of an intent.

        Args:
            intent: The intent to get the type of

        Returns:
            IntentType: The type of the intent
        """
        return intent.intent_type

    @staticmethod
    def validate_chain(
        intent: AnyIntent,
        configured_chains: Sequence[str],
        default_chain: str | None = None,
    ) -> str:
        """Validate and resolve the chain for an intent.

        Validates that the intent's chain (if specified) is in the list of
        configured chains. If no chain is specified on the intent, returns
        the default chain.

        Args:
            intent: The intent to validate
            configured_chains: List of chains configured for the strategy
            default_chain: Default chain to use if intent has no chain specified.
                          If None, uses the first configured chain.

        Returns:
            str: The resolved chain name (lowercase)

        Raises:
            InvalidChainError: If the intent's chain is not in configured_chains
            ValueError: If no default chain can be determined

        Example:
            # Validate an intent against strategy's configured chains
            resolved_chain = Intent.validate_chain(
                intent,
                configured_chains=["arbitrum", "optimism"],
                default_chain="arbitrum",
            )
        """
        if not configured_chains:
            raise ValueError("No chains configured for strategy")

        # Normalize configured chains to lowercase
        normalized_chains = [c.lower() for c in configured_chains]

        # Get chain from intent (all intent types have chain attribute now)
        intent_chain = getattr(intent, "chain", None)

        if intent_chain is not None:
            # Validate the specified chain
            chain_lower = intent_chain.lower()
            if chain_lower not in normalized_chains:
                raise InvalidChainError(intent_chain, configured_chains)
            return chain_lower

        # No chain specified - use default
        if default_chain is not None:
            default_lower = default_chain.lower()
            if default_lower not in normalized_chains:
                raise InvalidChainError(default_chain, configured_chains)
            return default_lower

        # Fall back to first configured chain
        return normalized_chains[0]

    @staticmethod
    def get_chain(intent: AnyIntent) -> str | None:
        """Get the chain specified on an intent.

        Args:
            intent: The intent to get the chain from

        Returns:
            Optional[str]: The chain name if specified, None otherwise
        """
        return getattr(intent, "chain", None)

    @staticmethod
    def is_sequence(item: AnyIntent | IntentSequence) -> bool:
        """Check if an item is an IntentSequence.

        Args:
            item: Intent or IntentSequence to check

        Returns:
            bool: True if item is an IntentSequence
        """
        return isinstance(item, IntentSequence)

    @staticmethod
    def normalize_decide_result(
        result: DecideResult,
    ) -> list[AnyIntent | IntentSequence]:
        """Normalize a decide() result to a list of items to execute.

        This helper converts any valid decide() return value into a normalized
        list that the executor can process:
        - None -> empty list (no action)
        - Single intent -> list with one intent
        - IntentSequence -> list with one sequence
        - List -> returned as-is

        Args:
            result: The return value from decide()

        Returns:
            List of intents and/or sequences to execute.
            Items in the list can execute in parallel.
            Intents within a sequence execute sequentially.
        """
        if result is None:
            return []
        if isinstance(result, IntentSequence):
            return [result]
        if isinstance(result, list):
            return result
        # Single intent
        return [result]

    @staticmethod
    def count_intents(result: DecideResult) -> int:
        """Count the total number of intents in a decide() result.

        Args:
            result: The return value from decide()

        Returns:
            Total number of intents (counting all intents within sequences)
        """
        if result is None:
            return 0

        items = Intent.normalize_decide_result(result)
        total = 0
        for item in items:
            if isinstance(item, IntentSequence):
                total += len(item.intents)
            else:
                total += 1
        return total

    @staticmethod
    def _validate_registry_handles_for_emission(result: DecideResult) -> None:
        """Walk every intent in ``result`` and re-validate ``registry_handle``.

        VIB-4192 / T06 — defense-in-depth check at the documented decide-result
        emission chokepoint. The construction-side ``model_validator`` on
        :class:`BaseIntent` rejects bad handles when an intent is built
        normally, but Pydantic v2 exposes ``model_construct`` and
        ``model_copy(update=..., validate=False)`` as documented bypass
        paths that skip validators. Re-running :func:`record_for` on every
        emitted intent closes those paths at the framework boundary.

        Walks the entire result tree — ``None``, single intent,
        ``IntentSequence``, list of intents/sequences, list containing a
        nested ``IntentSequence`` — AND recurses into the
        ``callback_intents`` list carried by ``FlashLoanIntent``. A
        bypassed callback intent built via ``model_construct`` would
        otherwise reach the wire unchecked through
        ``FlashLoanIntent.serialize`` (CodeRabbit PR #2205 review surfaced
        this hole). The recursion is intentionally exhaustive across all
        nested-intent fields the framework is aware of.

        Raises:
            UnknownIntentTypeError: when any intent in the tree carries a
                non-None ``registry_handle`` and its resolved
                ``intent_type`` is missing from TAXONOMY (or is ``None``).
            ValueError / TypeError: when any handle violates the shape
                contract (empty, whitespace-only, non-string).
        """
        if result is None:
            return
        if isinstance(result, IntentSequence):
            for inner in result.intents:
                Intent._validate_registry_handles_for_emission(inner)
            return
        if isinstance(result, list):
            for item in result:
                Intent._validate_registry_handles_for_emission(item)
            return
        # Leaf intent — validate this intent's handle.
        assert_registry_handle_known(result)
        # FlashLoanIntent ships nested intents in `callback_intents`. Recurse
        # so a bypassed callback can't sneak past serialize_result via its
        # parent.
        callback_intents = getattr(result, "callback_intents", None)
        if callback_intents is not None:
            for callback in callback_intents:
                Intent._validate_registry_handles_for_emission(callback)

    @staticmethod
    def serialize_result(result: DecideResult) -> dict[str, Any] | None:
        """Serialize a decide() result to a dictionary.

        Args:
            result: The return value from decide()

        Returns:
            Serialized result, or None if result was None

        Raises:
            UnknownIntentTypeError: VIB-4192 — when any intent in the tree
                carries a ``registry_handle`` whose intent type is not in
                TAXONOMY. This is the emission-side strict guard
                complementing the construction-side ``model_validator`` on
                :class:`BaseIntent`. Together they close Pydantic v2's
                documented ``model_construct`` / ``model_copy(validate=False)``
                bypass paths at the framework boundary.
        """
        if result is None:
            return None

        # VIB-4192: emission-side strict re-validation of registry_handle
        # before producing the dict. This catches handles bypassed via
        # `model_construct` / `model_copy(update=..., validate=False)`.
        # Walks single / list / sequence / nested-list result shapes.
        Intent._validate_registry_handles_for_emission(result)

        if isinstance(result, IntentSequence):
            return result.serialize()

        if isinstance(result, list):
            return {
                "type": "PARALLEL",
                "items": [
                    item.serialize() if isinstance(item, IntentSequence) else Intent.serialize(item) for item in result
                ],
            }

        # Single intent
        return Intent.serialize(result)

    @staticmethod
    def deserialize_result(data: dict[str, Any] | None) -> DecideResult:
        """Deserialize a decide() result from a dictionary.

        Args:
            data: Serialized result data

        Returns:
            Deserialized DecideResult
        """
        if data is None:
            return None

        result_type = data.get("type")

        if result_type == "SEQUENCE":
            return IntentSequence.deserialize(data)

        if result_type == "PARALLEL":
            items: list[AnyIntent | IntentSequence] = []
            for item_data in data.get("items", []):
                if item_data.get("type") == "SEQUENCE":
                    items.append(IntentSequence.deserialize(item_data))
                else:
                    items.append(Intent.deserialize(item_data))
            return items

        # Single intent
        return Intent.deserialize(data)

    @staticmethod
    def has_chained_amount(intent: AnyIntent) -> bool:
        """Check if an intent uses a chained amount from a previous step.

        An intent has a chained amount when its amount field is set to "all",
        meaning it should use the actual received amount from the previous
        step in a sequence (post-slippage, post-fees).

        Args:
            intent: The intent to check

        Returns:
            True if the intent uses amount="all", False otherwise
        """
        return getattr(intent, "is_chained_amount", False)

    @staticmethod
    def validate_chained_amounts(sequence: IntentSequence) -> None:
        """Validate that chained amounts are used correctly in a sequence.

        Validates that:
        1. amount="all" is NOT used on the first step of a sequence
        2. The sequence has proper dependencies for amount resolution

        Args:
            sequence: The intent sequence to validate

        Raises:
            InvalidAmountError: If amount="all" is used on the first step
        """
        if not sequence.intents:
            return

        first_intent = sequence.intents[0]
        if Intent.has_chained_amount(first_intent):
            intent_type = first_intent.intent_type.value if hasattr(first_intent, "intent_type") else "Unknown"
            raise InvalidAmountError(
                intent_type=intent_type,
                reason="amount='all' cannot be used on the first step of a sequence because there is no previous step output to reference",
            )

    @staticmethod
    def get_amount_field(intent: AnyIntent) -> ChainedAmount | None:
        """Get the amount field value from an intent for chaining purposes.

        This returns the amount that flows to the next step in a sequence.
        Different intents output different amounts:
        - SwapIntent: amount (token output) or amount_usd
        - SupplyIntent: amount (what was supplied)
        - RepayIntent: amount (what was repaid)
        - WithdrawIntent: amount (what was withdrawn)
        - BorrowIntent: borrow_amount (NOT collateral_amount - this is what's borrowed)
        - PerpOpenIntent: collateral_amount (what was deposited)
        - BridgeIntent: amount (what was bridged)

        Args:
            intent: The intent to get the amount from

        Returns:
            The amount value (Decimal or "all"), or None if not applicable
        """
        # For BorrowIntent, the output is the borrow_amount (what was borrowed)
        # NOT the collateral_amount (which may be 0 if already supplied)
        if hasattr(intent, "borrow_amount"):
            borrow_amount = intent.borrow_amount
            if borrow_amount is not None:
                return borrow_amount
        # Check standard amount field first (prefer non-None value)
        if hasattr(intent, "amount"):
            amount = intent.amount
            if amount is not None:
                return amount
        # Check amount_usd as fallback (for SwapIntent using USD amounts)
        if hasattr(intent, "amount_usd"):
            amount_usd = intent.amount_usd
            if amount_usd is not None:
                return amount_usd
        # Check collateral_amount for perp intents
        if hasattr(intent, "collateral_amount"):
            return intent.collateral_amount
        return None

    @staticmethod
    def set_resolved_amount(intent: AnyIntent, resolved_amount: Decimal) -> AnyIntent:
        """Create a copy of an intent with the amount resolved from "all" to a concrete value.

        This is used at execution time to resolve amount="all" to the actual
        received amount from the previous step.

        Args:
            intent: The intent to update
            resolved_amount: The concrete amount to use. For most intents this is
                a human-unit token amount that lands on the ``amount`` /
                ``collateral_amount`` field. For LP_CLOSE ``amount="all"``
                (VIB-5346) the units contract widens: the caller passes an
                integer-valued Decimal (the prior LP_OPEN minted-LP **wei**) and
                it lands on ``position_id`` as a clean integer string (no
                exponent / decimal point) that ``int(position_id)`` parses.

        Returns:
            A new intent instance with the resolved amount

        Note:
            This creates a new intent instance; it does not mutate the original.
        """
        # Get the serialized form
        data = intent.serialize()

        # VIB-5346: LP_CLOSE WEI lane. When the LP_CLOSE chaining marker is set,
        # the resolved value is the prior LP_OPEN minted-LP wei (a fungible
        # amount), and it lands on ``position_id`` — NOT on a generic ``amount``
        # field. Clear the marker so the deserialized intent is a plain
        # literal-position_id close (the compiler reads int(position_id) and
        # never sees "all"). Guarded strictly on amount == "all" so literal
        # closes are untouched. This branch must come BEFORE the generic
        # ``amount`` rewrite below, otherwise the marker would be consumed there.
        if data.get("type") == "LP_CLOSE" and data.get("amount") == "all":
            # VIB-5346 defensive: a fungible LP-token amount is strictly positive
            # wei. A zero/negative would produce a bogus ``position_id`` (e.g.
            # "0" or "-1") that downstream ``int(position_id)`` would silently
            # accept. The live path passes non-negative minted-LP wei; reject
            # the degenerate case loudly rather than emit a poisoned identity.
            if resolved_amount <= 0:
                raise ValueError(
                    f"LP_CLOSE amount='all' resolved to a non-positive minted-LP wei "
                    f"value ({resolved_amount}); cannot form a valid position_id"
                )
            # Normalize to an integer string: int() strips any exponent/decimal
            # point so int(position_id) parses cleanly downstream.
            data["position_id"] = str(int(resolved_amount))
            data["amount"] = None
        # Update the appropriate amount field
        elif "amount" in data and data["amount"] == "all":
            data["amount"] = str(resolved_amount)
        elif "collateral_amount" in data and data["collateral_amount"] == "all":
            data["collateral_amount"] = str(resolved_amount)

        # Deserialize back to an intent
        return Intent.deserialize(data)
