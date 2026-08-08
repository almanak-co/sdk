"""Compiler data models — pure data classes used by IntentCompiler.

These are extracted from compiler.py for file-size management.
All symbols remain importable from ``almanak.framework.intents.compiler``.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from ..models.reproduction_bundle import ActionBundle


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class IntentCompilerConfig:
    """Configuration for IntentCompiler.

    Attributes:
        allow_placeholder_prices: If False (default), raises ValueError when no
            price_oracle is given. Set to True ONLY for unit tests.
            NEVER set to True in production - placeholder prices will cause
            incorrect slippage calculations and swap reverts.
        polymarket_config: Optional PolymarketConfig for prediction market intents.
            Required when compiling PredictionBuyIntent, PredictionSellIntent,
            or PredictionRedeemIntent on Polygon. If not provided when on Polygon,
            a warning is logged and prediction intents will fail to compile.
        swap_pool_selection_mode: Pool selection mode for V3-style swaps.
            - "auto" (default): Try all supported fee tiers and pick best quote when RPC is available.
            - "fixed": Use fixed_swap_fee_tier for deterministic execution.
        fixed_swap_fee_tier: Optional fixed fee tier used when swap_pool_selection_mode="fixed".
            Must be valid for the selected protocol.
        max_price_impact_pct: Maximum acceptable price impact as a fraction (0.0 to 1.0).
            If the on-chain quoter returns an amount deviating more than this from the oracle
            estimate, compilation fails with a clear error. Default: 0.10 (10%).
            Configurable at compiler construction; override per-swap via
            SwapIntent.max_price_impact (e.g. thin venues / Pendle YT).
        permission_discovery: If True, the compiler is being used for offline permission
            discovery. Enables fallbacks for RPC-dependent operations:
            - Uses synthetic LP balances when on-chain balance is 0 or unavailable
            This ensures LP_CLOSE compilation produces full transaction sets
            (approve + removeLiquidity) so the permission generator can extract
            the required target addresses and function selectors.
        offline_discovery: If True, ``_get_chain_rpc_url`` will NOT resolve an
            implicit transport (a managed Anvil fork, then a free public RPC)
            when no ``rpc_url`` was explicitly configured. An explicit
            ``rpc_url`` is still honoured.

            Set from ``PermissionHints.offline_discovery``, which a connector
            opts into once its compiler can produce complete calldata with no
            network reads. It exists because a manifest built from implicit
            live reads is a function of RPC weather, not of the registry:
            curve LP discovery on arbitrum was issuing 43 ``eth_call``s to a
            public RPC and producing 7/3/7 targets across three consecutive
            runs (VIB-6046 D5).

            Opt-IN rather than default-on: several connectors (gmx_v2, pendle,
            traderjoe_v2, uniswap_v4 hooks) currently depend on that implicit
            fallback and discover nothing without it. Flipping the default
            would turn their flakiness into a hard failure. They have the same
            nondeterminism, tracked separately — see the module note in
            ``almanak/connectors/curve/permission_hints.py``.

        gateway_internal_preflight: Set True ONLY when the compiler is
            constructed INSIDE the gateway process (see
            ``almanak/gateway/services/execution_service.py``). Compile-time
            safety pre-flights that read protocol risk parameters — Aave's
            frozen-reserve, borrowable and zero-LTV collateral checks — reach
            for the compiler's ``gateway_client``. A gateway-side compiler has
            none (it IS the gateway), so those pre-flights silently failed open
            on the one path the production runner actually uses: the runner
            compiles via ``execution.CompileIntent``, not in-process
            (``almanak/framework/runner/_inner_runner_helpers.py``). Measured
            consequence on Aave V3 Mantle after governance zeroed ``ltv`` — the
            gateway emitted ``approve + supply + setUserUseReserveAsCollateral``
            and the toggle leg reverted ``0x21e5c4ae UserHasAssetWithZeroLtv()``
            on-chain (VIB-6111).

            When True, those pre-flights may issue their reads through the
            framework ``eth_call`` service instead. This is NOT a strategy-
            container egress bypass: it is only ever set inside
            ``almanak/gateway/``, which IS the egress layer, and it stays False
            for every strategy-side and offline compile so those keep failing
            open exactly as before.

        managed_fork: Tri-state declaration of "this compile targets a managed
            Anvil fork" (ALM-3184). Swap compilers relax the oracle
            price-impact guard — the only independent cross-check that an
            on-chain quote has not been manipulated or drained — when this
            resolves True, because fork block state and live oracle prices are
            not time-aligned.

            Declaration only — there is no runtime detection. The production
            gateway compile path declares it from ``GatewaySettings.network``
            (``Network.ANVIL``); offline permission discovery declares
            ``False``; Anvil test harnesses declare ``True``. ``None`` means
            nobody declared, which resolves to production
            (``almanak.framework.execution.fork_signal``).

            It replaces the previous ``is_local_rpc(rpc_url)`` test, which
            returned True for **any** host on port 8545-8550 — so a production
            RPC proxy on ``:8545`` compiled mainnet swaps with the guard off.
            Absent/unknown now resolves to production, not to fork.
    """

    allow_placeholder_prices: bool = False
    #: ALM-3183. Optional explicit declaration of WHY this compiler may fabricate
    #: prices, as a ``PlaceholderPriceUse`` member (typed ``Any`` only to keep
    #: this module import-light). When set it wins over the inference in
    #: ``IntentCompiler._get_placeholder_prices``; a caller that enables
    #: ``allow_placeholder_prices`` on a non-test lane should set it so the
    #: production log names the real reason instead of defaulting to "unit_test".
    #: Making this MANDATORY whenever allow_placeholder_prices is True is the
    #: intended end state; it is optional for now because ~20 existing test
    #: construction sites would need updating in the same change.
    placeholder_price_use: Any = None
    polymarket_config: Any = None  # PolymarketConfig (typed Any to avoid a framework->connector import, VIB-4989)
    swap_pool_selection_mode: Literal["auto", "fixed"] = "auto"
    fixed_swap_fee_tier: int | None = None
    max_price_impact_pct: Decimal = Decimal("0.10")
    permission_discovery: bool = False
    offline_discovery: bool = False
    gateway_internal_preflight: bool = False
    managed_fork: bool | None = None

    def __post_init__(self) -> None:
        """Validate swap pool selection settings."""
        if self.swap_pool_selection_mode not in {"auto", "fixed"}:
            raise ValueError("swap_pool_selection_mode must be 'auto' or 'fixed'")
        if self.swap_pool_selection_mode == "fixed" and self.fixed_swap_fee_tier is None:
            raise ValueError("fixed_swap_fee_tier is required when swap_pool_selection_mode='fixed'")
        # Coerce float to Decimal to ensure guard always operates in Decimal space
        if not isinstance(self.max_price_impact_pct, Decimal):
            object.__setattr__(self, "max_price_impact_pct", Decimal(str(self.max_price_impact_pct)))
        if not Decimal("0") < self.max_price_impact_pct <= Decimal("1"):
            raise ValueError("max_price_impact_pct must be between 0 (exclusive) and 1 (inclusive)")


# =============================================================================
# Data Classes
# =============================================================================


class CompilationStatus(Enum):
    """Status of intent compilation."""

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"  # Some transactions built, some failed


@dataclass
class TransactionData:
    """Represents a single transaction in an ActionBundle.

    Attributes:
        to: Target contract address
        value: ETH value to send (in wei)
        data: Encoded calldata
        gas_estimate: Estimated gas for this transaction
        description: Human-readable description of what this TX does
        tx_type: Type of transaction (approve, swap, etc.)
    """

    to: str
    value: int
    data: str  # Hex-encoded calldata
    gas_estimate: int
    description: str
    tx_type: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "to": self.to,
            "value": str(self.value),
            "data": self.data,
            "gas_estimate": self.gas_estimate,
            "description": self.description,
            "tx_type": self.tx_type,
        }


@dataclass
class CompilationResult:
    """Result of compiling an intent to an ActionBundle.

    Attributes:
        status: Compilation status
        action_bundle: The compiled ActionBundle (if successful)
        transactions: List of transaction data
        total_gas_estimate: Sum of all gas estimates
        error: Error message (if failed)
        is_transient: Whether the failure is retryable orchestration-level I/O
        is_safety_refusal: Whether a ``FAILED`` status is a pre-execution
            SAFETY-GUARD refusal rather than an execution/compile fault
            (VIB-5746). Set by compile-time guards that refuse to build a
            transaction because acting would be unsafe — e.g. price impact above
            the configured max, or the on-chain quoter returned no amount so pool
            liquidity could not be verified. When True, ZERO transactions were
            built and the on-chain position is untouched: the guard did its job.
            The runner maps this to :class:`FailureKind.GUARD_REFUSED` so it does
            NOT count toward the circuit breaker's consecutive-failure trip
            thresholds (a correct refusal is a safety success, not a fault). Only
            meaningful when ``status is CompilationStatus.FAILED``.
        retry_after_seconds: Optional retry delay hinted by the failing backend
        warnings: List of warnings encountered during compilation
        intent_id: ID of the intent that was compiled
        compiled_at: Timestamp of compilation
    """

    status: CompilationStatus
    action_bundle: "ActionBundle | None" = None
    transactions: list[TransactionData] = field(default_factory=list)
    total_gas_estimate: int = 0
    error: str | None = None
    is_transient: bool = False
    is_safety_refusal: bool = False
    retry_after_seconds: float | None = None
    warnings: list[str] = field(default_factory=list)
    intent_id: str = ""
    compiled_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "status": self.status.value,
            "action_bundle": self.action_bundle.to_dict() if self.action_bundle else None,
            "transactions": [t.to_dict() for t in self.transactions],
            "total_gas_estimate": self.total_gas_estimate,
            "error": self.error,
            "is_transient": self.is_transient,
            "is_safety_refusal": self.is_safety_refusal,
            "retry_after_seconds": self.retry_after_seconds,
            "warnings": self.warnings,
            "intent_id": self.intent_id,
            "compiled_at": self.compiled_at.isoformat(),
        }


@dataclass
class TokenInfo:
    """Information about a token.

    Attributes:
        symbol: Token symbol (e.g., "USDC")
        address: Token contract address
        decimals: Token decimals
        is_native: Whether this is the native token (ETH, MATIC, etc.)
    """

    symbol: str
    address: str
    decimals: int = 18
    is_native: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "symbol": self.symbol,
            "address": self.address,
            "decimals": self.decimals,
            "is_native": self.is_native,
        }


@dataclass
class PriceInfo:
    """Price information for amount calculations.

    Attributes:
        token: Token symbol
        price_usd: Price in USD
        timestamp: When this price was fetched
    """

    token: str
    price_usd: Decimal
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
