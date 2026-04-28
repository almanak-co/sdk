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
    from ..connectors.polymarket.models import PolymarketConfig
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
            estimate, compilation fails with a clear error. Default: 0.30 (30%).
            Can be overridden per-intent via SwapIntent.max_price_impact.
        permission_discovery: If True, the compiler is being used for offline permission
            discovery. Enables fallbacks for RPC-dependent operations:
            - Uses synthetic LP balances when on-chain balance is 0 or unavailable
            This ensures LP_CLOSE compilation produces full transaction sets
            (approve + removeLiquidity) so the permission generator can extract
            the required target addresses and function selectors.
    """

    allow_placeholder_prices: bool = False
    polymarket_config: "PolymarketConfig | None" = None
    swap_pool_selection_mode: Literal["auto", "fixed"] = "auto"
    fixed_swap_fee_tier: int | None = None
    max_price_impact_pct: Decimal = Decimal("0.30")
    permission_discovery: bool = False

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
