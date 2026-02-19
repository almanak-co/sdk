"""Slippage guard for capping slippage and warning on large trades.

This module provides utilities for protecting against excessive slippage
and alerting users when trade sizes may cause significant price impact.

Key Features:
    - Configurable maximum slippage cap (default 10%)
    - Warnings when trade size exceeds safe percentage of pool liquidity
    - Detailed logging of trade information when warnings are triggered
    - Integration with AMM math for accurate price impact estimation
    - Historical liquidity depth support for accurate backtesting slippage
    - Confidence tracking for slippage calculations based on data source

Example:
    from almanak.framework.backtesting.pnl.fee_models.slippage_guard import (
        SlippageGuard,
        SlippageGuardConfig,
        SlippageWarning,
        HistoricalSlippageModel,
        SlippageModelConfig,
    )

    config = SlippageGuardConfig(
        max_slippage_pct=Decimal("0.10"),  # 10% max
        safe_liquidity_pct=Decimal("0.05"),  # Warn if trade > 5% of pool
    )
    guard = SlippageGuard(config)

    result = guard.check_trade(
        trade_amount_usd=Decimal("100000"),
        pool_liquidity_usd=Decimal("1000000"),
        estimated_slippage=Decimal("0.08"),
    )

    if result.warning:
        print(f"Warning: {result.warning.message}")

    # Use capped slippage
    effective_slippage = result.capped_slippage

    # Historical slippage model with liquidity depth
    from almanak.framework.backtesting.pnl.types import LiquidityResult, DataSourceInfo, DataConfidence
    from datetime import datetime, UTC

    model_config = SlippageModelConfig(use_twap_depth=True)
    model = HistoricalSlippageModel(config=model_config)

    liquidity = LiquidityResult(
        depth=Decimal("5000000"),
        source_info=DataSourceInfo(
            source="uniswap_v3_subgraph",
            confidence=DataConfidence.HIGH,
            timestamp=datetime.now(UTC),
        ),
    )

    slippage_result = model.calculate_slippage(
        trade_amount_usd=Decimal("50000"),
        historical_liquidity=liquidity,
        pool_type="v3",
    )
    print(f"Slippage: {slippage_result.slippage_pct}%, Confidence: {slippage_result.confidence}")
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..types import LiquidityResult

from ..types import DataConfidence, DataSourceInfo

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Default thresholds
DEFAULT_MAX_SLIPPAGE_PCT = Decimal("0.10")  # 10% max slippage
DEFAULT_SAFE_LIQUIDITY_PCT = Decimal("0.05")  # Warn if trade > 5% of pool
DEFAULT_HIGH_IMPACT_THRESHOLD = Decimal("0.01")  # 1% considered significant
DEFAULT_CRITICAL_IMPACT_THRESHOLD = Decimal("0.05")  # 5% considered critical

# Historical slippage model constants
DEFAULT_V3_CONCENTRATION_FACTOR = Decimal("3.0")  # Typical V3 concentration
DEFAULT_V2_FEE_BPS = 30  # 0.3% default for V2 pools
DEFAULT_V3_FEE_BPS = 3000  # 0.3% for V3 (in 1e6 scale)

# Data source identifiers for slippage calculations
SLIPPAGE_SOURCE_HISTORICAL = "historical_liquidity"
SLIPPAGE_SOURCE_CONSTANT_PRODUCT = "constant_product_fallback"
SLIPPAGE_SOURCE_TWAP = "twap_liquidity"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class SlippageGuardConfig:
    """Configuration for slippage guard.

    Attributes:
        max_slippage_pct: Maximum allowed slippage as decimal (0.10 = 10%). Default 10%.
        safe_liquidity_pct: Trade size threshold as % of liquidity for warnings. Default 5%.
        high_impact_threshold: Threshold for "high impact" warning. Default 1%.
        critical_impact_threshold: Threshold for "critical impact" warning. Default 5%.
        log_warnings: Whether to log warnings. Default True.
        emit_exceptions: Whether to raise exceptions for critical slippage. Default False.
    """

    max_slippage_pct: Decimal = DEFAULT_MAX_SLIPPAGE_PCT
    safe_liquidity_pct: Decimal = DEFAULT_SAFE_LIQUIDITY_PCT
    high_impact_threshold: Decimal = DEFAULT_HIGH_IMPACT_THRESHOLD
    critical_impact_threshold: Decimal = DEFAULT_CRITICAL_IMPACT_THRESHOLD
    log_warnings: bool = True
    emit_exceptions: bool = False

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if self.max_slippage_pct <= 0 or self.max_slippage_pct > 1:
            raise ValueError("max_slippage_pct must be between 0 and 1")
        if self.safe_liquidity_pct <= 0 or self.safe_liquidity_pct > 1:
            raise ValueError("safe_liquidity_pct must be between 0 and 1")
        if self.high_impact_threshold < 0:
            raise ValueError("high_impact_threshold must be non-negative")
        if self.critical_impact_threshold < 0:
            raise ValueError("critical_impact_threshold must be non-negative")
        if self.high_impact_threshold > self.critical_impact_threshold:
            raise ValueError("high_impact_threshold must be <= critical_impact_threshold")

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "max_slippage_pct": str(self.max_slippage_pct),
            "safe_liquidity_pct": str(self.safe_liquidity_pct),
            "high_impact_threshold": str(self.high_impact_threshold),
            "critical_impact_threshold": str(self.critical_impact_threshold),
            "log_warnings": self.log_warnings,
            "emit_exceptions": self.emit_exceptions,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SlippageGuardConfig:
        """Deserialize from dictionary."""
        return cls(
            max_slippage_pct=Decimal(data.get("max_slippage_pct", str(DEFAULT_MAX_SLIPPAGE_PCT))),
            safe_liquidity_pct=Decimal(data.get("safe_liquidity_pct", str(DEFAULT_SAFE_LIQUIDITY_PCT))),
            high_impact_threshold=Decimal(data.get("high_impact_threshold", str(DEFAULT_HIGH_IMPACT_THRESHOLD))),
            critical_impact_threshold=Decimal(
                data.get("critical_impact_threshold", str(DEFAULT_CRITICAL_IMPACT_THRESHOLD))
            ),
            log_warnings=data.get("log_warnings", True),
            emit_exceptions=data.get("emit_exceptions", False),
        )


@dataclass
class SlippageWarning:
    """Warning emitted when slippage thresholds are exceeded.

    Attributes:
        level: Warning level ("high" or "critical")
        message: Human-readable warning message
        trade_amount_usd: Trade size that triggered the warning
        pool_liquidity_usd: Pool liquidity at time of trade
        liquidity_ratio: Trade size as percentage of liquidity
        estimated_slippage: Estimated price impact
        capped_slippage: Slippage after applying cap
        was_capped: Whether slippage was capped
        details: Additional context for the warning
    """

    level: str
    message: str
    trade_amount_usd: Decimal
    pool_liquidity_usd: Decimal | None
    liquidity_ratio: Decimal | None
    estimated_slippage: Decimal
    capped_slippage: Decimal
    was_capped: bool
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "level": self.level,
            "message": self.message,
            "trade_amount_usd": str(self.trade_amount_usd),
            "pool_liquidity_usd": str(self.pool_liquidity_usd) if self.pool_liquidity_usd else None,
            "liquidity_ratio": str(self.liquidity_ratio) if self.liquidity_ratio else None,
            "estimated_slippage": str(self.estimated_slippage),
            "estimated_slippage_pct": f"{self.estimated_slippage * 100:.2f}%",
            "capped_slippage": str(self.capped_slippage),
            "capped_slippage_pct": f"{self.capped_slippage * 100:.2f}%",
            "was_capped": self.was_capped,
            "details": self.details,
        }


@dataclass
class SlippageCheckResult:
    """Result of a slippage check.

    Attributes:
        original_slippage: The estimated slippage before capping
        capped_slippage: The effective slippage after applying cap
        was_capped: Whether the slippage was capped
        warning: Warning if any thresholds were exceeded
        trade_amount_usd: The trade amount checked
        pool_liquidity_usd: The pool liquidity (if available)
        liquidity_ratio: Trade size as percentage of liquidity
    """

    original_slippage: Decimal
    capped_slippage: Decimal
    was_capped: bool
    warning: SlippageWarning | None
    trade_amount_usd: Decimal
    pool_liquidity_usd: Decimal | None = None
    liquidity_ratio: Decimal | None = None

    @property
    def has_warning(self) -> bool:
        """Check if the result has a warning."""
        return self.warning is not None

    @property
    def is_critical(self) -> bool:
        """Check if the warning level is critical."""
        return self.warning is not None and self.warning.level == "critical"

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "original_slippage": str(self.original_slippage),
            "original_slippage_pct": f"{self.original_slippage * 100:.2f}%",
            "capped_slippage": str(self.capped_slippage),
            "capped_slippage_pct": f"{self.capped_slippage * 100:.2f}%",
            "was_capped": self.was_capped,
            "has_warning": self.has_warning,
            "is_critical": self.is_critical,
            "warning": self.warning.to_dict() if self.warning else None,
            "trade_amount_usd": str(self.trade_amount_usd),
            "pool_liquidity_usd": str(self.pool_liquidity_usd) if self.pool_liquidity_usd else None,
            "liquidity_ratio": str(self.liquidity_ratio) if self.liquidity_ratio else None,
        }


# =============================================================================
# Exception Classes
# =============================================================================


class SlippageCapExceededError(Exception):
    """Raised when slippage exceeds the maximum allowed cap.

    This exception is only raised when SlippageGuardConfig.emit_exceptions is True.
    """

    def __init__(
        self,
        message: str,
        estimated_slippage: Decimal,
        max_slippage: Decimal,
        trade_amount_usd: Decimal,
    ) -> None:
        super().__init__(message)
        self.estimated_slippage = estimated_slippage
        self.max_slippage = max_slippage
        self.trade_amount_usd = trade_amount_usd


# =============================================================================
# Slippage Guard
# =============================================================================


@dataclass
class SlippageGuard:
    """Guard for capping slippage and warning on large trades.

    This class provides protection against excessive slippage by:
    1. Capping slippage at a configurable maximum
    2. Warning when trade size exceeds a safe percentage of pool liquidity
    3. Logging detailed trade information when warnings are triggered

    The guard can be configured to either:
    - Silently cap slippage and emit warnings (default)
    - Raise exceptions for critical slippage violations

    Attributes:
        config: Configuration for slippage thresholds and behavior

    Example:
        guard = SlippageGuard()

        # Basic check with just estimated slippage
        result = guard.check_trade(
            trade_amount_usd=Decimal("50000"),
            estimated_slippage=Decimal("0.03"),  # 3%
        )

        # Full check with pool liquidity
        result = guard.check_trade(
            trade_amount_usd=Decimal("100000"),
            pool_liquidity_usd=Decimal("1000000"),
            estimated_slippage=Decimal("0.08"),  # 8%
            token_in="USDC",
            token_out="ETH",
            protocol="uniswap_v3",
        )

        if result.was_capped:
            print(f"Slippage capped from {result.original_slippage} to {result.capped_slippage}")
    """

    config: SlippageGuardConfig = field(default_factory=SlippageGuardConfig)

    def check_trade(
        self,
        trade_amount_usd: Decimal,
        estimated_slippage: Decimal,
        pool_liquidity_usd: Decimal | None = None,
        **kwargs: Any,
    ) -> SlippageCheckResult:
        """Check a trade for slippage issues and apply caps.

        This method:
        1. Calculates the liquidity ratio (trade_size / pool_liquidity)
        2. Checks if trade exceeds safe liquidity percentage
        3. Checks if estimated slippage exceeds thresholds
        4. Caps slippage at the configured maximum
        5. Emits warnings as appropriate

        Args:
            trade_amount_usd: Trade size in USD
            estimated_slippage: Estimated price impact as decimal (0.05 = 5%)
            pool_liquidity_usd: Pool liquidity in USD (optional, for liquidity ratio)
            **kwargs: Additional context for warnings:
                - token_in: Input token symbol
                - token_out: Output token symbol
                - protocol: Protocol name
                - pool_address: Pool contract address
                - tick: Current tick (for V3 pools)

        Returns:
            SlippageCheckResult with capped slippage and any warnings

        Raises:
            SlippageCapExceededError: If config.emit_exceptions is True and
                slippage exceeds critical threshold
        """
        # Ensure trade_amount and slippage are positive
        trade_amount_usd = abs(trade_amount_usd)
        estimated_slippage = abs(estimated_slippage)

        # Calculate liquidity ratio
        liquidity_ratio: Decimal | None = None
        if pool_liquidity_usd is not None and pool_liquidity_usd > 0:
            liquidity_ratio = trade_amount_usd / pool_liquidity_usd

        # Apply slippage cap
        was_capped = estimated_slippage > self.config.max_slippage_pct
        capped_slippage = min(estimated_slippage, self.config.max_slippage_pct)

        # Determine warning level
        warning = self._determine_warning(
            trade_amount_usd=trade_amount_usd,
            pool_liquidity_usd=pool_liquidity_usd,
            liquidity_ratio=liquidity_ratio,
            estimated_slippage=estimated_slippage,
            capped_slippage=capped_slippage,
            was_capped=was_capped,
            **kwargs,
        )

        # Log warning if configured
        if warning and self.config.log_warnings:
            self._log_warning(warning, **kwargs)

        # Raise exception if configured and critical
        if warning and warning.level == "critical" and self.config.emit_exceptions:
            raise SlippageCapExceededError(
                message=warning.message,
                estimated_slippage=estimated_slippage,
                max_slippage=self.config.max_slippage_pct,
                trade_amount_usd=trade_amount_usd,
            )

        return SlippageCheckResult(
            original_slippage=estimated_slippage,
            capped_slippage=capped_slippage,
            was_capped=was_capped,
            warning=warning,
            trade_amount_usd=trade_amount_usd,
            pool_liquidity_usd=pool_liquidity_usd,
            liquidity_ratio=liquidity_ratio,
        )

    def _determine_warning(
        self,
        trade_amount_usd: Decimal,
        pool_liquidity_usd: Decimal | None,
        liquidity_ratio: Decimal | None,
        estimated_slippage: Decimal,
        capped_slippage: Decimal,
        was_capped: bool,
        **kwargs: Any,
    ) -> SlippageWarning | None:
        """Determine if a warning should be emitted and at what level.

        Warning conditions (in priority order):
        1. Slippage exceeds critical threshold -> "critical"
        2. Trade exceeds safe liquidity percentage -> "high" or "critical"
        3. Slippage exceeds high impact threshold -> "high"

        Args:
            trade_amount_usd: Trade size in USD
            pool_liquidity_usd: Pool liquidity in USD
            liquidity_ratio: Trade size as fraction of liquidity
            estimated_slippage: Estimated slippage before cap
            capped_slippage: Slippage after cap applied
            was_capped: Whether slippage was capped
            **kwargs: Additional context

        Returns:
            SlippageWarning if thresholds exceeded, None otherwise
        """
        warning_level: str | None = None
        warning_reasons: list[str] = []

        # Check critical slippage threshold
        if estimated_slippage >= self.config.critical_impact_threshold:
            warning_level = "critical"
            warning_reasons.append(
                f"slippage {estimated_slippage * 100:.2f}% exceeds critical threshold "
                f"{self.config.critical_impact_threshold * 100:.1f}%"
            )

        # Check liquidity ratio
        if liquidity_ratio is not None:
            if liquidity_ratio >= self.config.safe_liquidity_pct * 2:
                # Very large trade (>2x safe threshold) is critical
                warning_level = "critical"
                warning_reasons.append(
                    f"trade size {liquidity_ratio * 100:.1f}% of pool liquidity "
                    f"exceeds 2x safe limit ({self.config.safe_liquidity_pct * 200:.0f}%)"
                )
            elif liquidity_ratio >= self.config.safe_liquidity_pct:
                # Large trade (>safe threshold) is high warning
                if warning_level != "critical":
                    warning_level = "high"
                warning_reasons.append(
                    f"trade size {liquidity_ratio * 100:.1f}% of pool liquidity "
                    f"exceeds safe limit ({self.config.safe_liquidity_pct * 100:.0f}%)"
                )

        # Check high impact threshold
        if estimated_slippage >= self.config.high_impact_threshold:
            if warning_level != "critical":
                warning_level = "high"
            if not any("slippage" in r for r in warning_reasons):
                warning_reasons.append(
                    f"slippage {estimated_slippage * 100:.2f}% exceeds high impact threshold "
                    f"{self.config.high_impact_threshold * 100:.1f}%"
                )

        # If slippage was capped, add that to reasons
        if was_capped:
            warning_reasons.append(
                f"slippage capped from {estimated_slippage * 100:.2f}% to {capped_slippage * 100:.2f}%"
            )
            if warning_level is None:
                warning_level = "high"

        # No warning needed
        if warning_level is None:
            return None

        # Build warning message
        message = self._build_warning_message(
            trade_amount_usd=trade_amount_usd,
            pool_liquidity_usd=pool_liquidity_usd,
            warning_reasons=warning_reasons,
            **kwargs,
        )

        # Build details dict
        details: dict[str, Any] = {
            "reasons": warning_reasons,
        }
        for key in ("token_in", "token_out", "protocol", "pool_address", "tick"):
            if key in kwargs:
                details[key] = kwargs[key]

        return SlippageWarning(
            level=warning_level,
            message=message,
            trade_amount_usd=trade_amount_usd,
            pool_liquidity_usd=pool_liquidity_usd,
            liquidity_ratio=liquidity_ratio,
            estimated_slippage=estimated_slippage,
            capped_slippage=capped_slippage,
            was_capped=was_capped,
            details=details,
        )

    def _build_warning_message(
        self,
        trade_amount_usd: Decimal,
        pool_liquidity_usd: Decimal | None,
        warning_reasons: list[str],
        **kwargs: Any,
    ) -> str:
        """Build a human-readable warning message.

        Args:
            trade_amount_usd: Trade size in USD
            pool_liquidity_usd: Pool liquidity in USD
            warning_reasons: List of warning reasons
            **kwargs: Additional context (token_in, token_out, protocol)

        Returns:
            Formatted warning message
        """
        # Build trade description
        token_in = kwargs.get("token_in", "?")
        token_out = kwargs.get("token_out", "?")
        protocol = kwargs.get("protocol", "unknown")

        parts = [
            f"High slippage warning for {token_in}->{token_out} trade on {protocol}:",
            f"  Trade size: ${trade_amount_usd:,.2f}",
        ]

        if pool_liquidity_usd is not None:
            parts.append(f"  Pool liquidity: ${pool_liquidity_usd:,.2f}")

        for reason in warning_reasons:
            parts.append(f"  - {reason}")

        return "\n".join(parts)

    def _log_warning(self, warning: SlippageWarning, **kwargs: Any) -> None:
        """Log a slippage warning.

        Args:
            warning: The warning to log
            **kwargs: Additional context
        """
        log_level = logging.WARNING if warning.level == "high" else logging.ERROR
        logger.log(log_level, warning.message)

        # Log additional details at debug level
        logger.debug(
            "Slippage warning details: %s",
            {
                "level": warning.level,
                "trade_amount_usd": str(warning.trade_amount_usd),
                "pool_liquidity_usd": str(warning.pool_liquidity_usd),
                "liquidity_ratio": str(warning.liquidity_ratio),
                "estimated_slippage": str(warning.estimated_slippage),
                "capped_slippage": str(warning.capped_slippage),
                "was_capped": warning.was_capped,
                **kwargs,
            },
        )

    def cap_slippage(self, slippage: Decimal) -> Decimal:
        """Simply cap slippage at the maximum without full check.

        This is a convenience method for cases where you just need to
        apply the cap without the full warning logic.

        Args:
            slippage: Estimated slippage as decimal

        Returns:
            Capped slippage
        """
        return min(abs(slippage), self.config.max_slippage_pct)

    def is_safe_trade_size(
        self,
        trade_amount_usd: Decimal,
        pool_liquidity_usd: Decimal,
    ) -> bool:
        """Check if trade size is within safe limits.

        Args:
            trade_amount_usd: Trade size in USD
            pool_liquidity_usd: Pool liquidity in USD

        Returns:
            True if trade is within safe limits
        """
        if pool_liquidity_usd <= 0:
            return False
        ratio = trade_amount_usd / pool_liquidity_usd
        return ratio < self.config.safe_liquidity_pct

    def get_max_safe_trade_size(self, pool_liquidity_usd: Decimal) -> Decimal:
        """Calculate maximum trade size within safe limits.

        Args:
            pool_liquidity_usd: Pool liquidity in USD

        Returns:
            Maximum safe trade size in USD
        """
        return pool_liquidity_usd * self.config.safe_liquidity_pct

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "config": self.config.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SlippageGuard:
        """Deserialize from dictionary."""
        config_data = data.get("config", {})
        return cls(config=SlippageGuardConfig.from_dict(config_data))


# =============================================================================
# Historical Slippage Model with Liquidity Depth Support
# =============================================================================


@dataclass
class SlippageModelConfig:
    """Configuration for the historical slippage model.

    Controls how slippage is calculated using historical liquidity depth data.

    Attributes:
        use_twap_depth: Whether to use time-weighted average depth for slippage
            calculation. If False, uses point-in-time depth.
        v3_concentration_factor: Multiplier for V3 effective liquidity depth.
            Accounts for concentrated liquidity being more capital efficient.
            Default 3.0 means 3x effective depth compared to full-range.
        v2_fee_bps: Fee tier in basis points for V2 pools. Default 30 (0.3%).
        v3_fee_bps: Fee tier in V3 scale (1e6) for V3 pools. Default 3000 (0.3%).
        fallback_liquidity_usd: Default liquidity to use when historical data
            unavailable. Set to None to return zero slippage in fallback.
        max_slippage_pct: Maximum slippage cap. Default 10%.
    """

    use_twap_depth: bool = False
    v3_concentration_factor: Decimal = DEFAULT_V3_CONCENTRATION_FACTOR
    v2_fee_bps: int = DEFAULT_V2_FEE_BPS
    v3_fee_bps: int = DEFAULT_V3_FEE_BPS
    fallback_liquidity_usd: Decimal | None = Decimal("1000000")  # $1M default
    max_slippage_pct: Decimal = DEFAULT_MAX_SLIPPAGE_PCT

    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.v3_concentration_factor <= 0:
            raise ValueError("v3_concentration_factor must be positive")
        if self.v2_fee_bps < 0:
            raise ValueError("v2_fee_bps must be non-negative")
        if self.v3_fee_bps < 0:
            raise ValueError("v3_fee_bps must be non-negative")
        if self.fallback_liquidity_usd is not None and self.fallback_liquidity_usd < 0:
            raise ValueError("fallback_liquidity_usd must be non-negative")
        if self.max_slippage_pct <= 0 or self.max_slippage_pct > 1:
            raise ValueError("max_slippage_pct must be between 0 and 1")


@dataclass
class HistoricalSlippageResult:
    """Result of a historical slippage calculation.

    Contains the calculated slippage along with confidence level and
    source information for transparency in backtest results.

    Attributes:
        slippage: Calculated price impact as a decimal (0.01 = 1%).
        slippage_bps: Slippage in basis points.
        liquidity_usd: Liquidity depth used for calculation.
        confidence: Confidence level of the slippage calculation.
        data_source: Identifier for the data source used.
        pool_type: Type of pool calculation ("v2" or "v3").
        was_fallback: Whether fallback values were used.
        source_info: Full DataSourceInfo if available.
    """

    slippage: Decimal
    slippage_bps: int
    liquidity_usd: Decimal
    confidence: DataConfidence
    data_source: str
    pool_type: str
    was_fallback: bool
    source_info: DataSourceInfo | None = None

    @property
    def slippage_pct(self) -> Decimal:
        """Slippage as percentage (1.0 = 1%)."""
        return self.slippage * Decimal("100")

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "slippage": str(self.slippage),
            "slippage_pct": f"{self.slippage_pct:.4f}%",
            "slippage_bps": self.slippage_bps,
            "liquidity_usd": str(self.liquidity_usd),
            "confidence": self.confidence.value,
            "data_source": self.data_source,
            "pool_type": self.pool_type,
            "was_fallback": self.was_fallback,
            "source_info": {
                "source": self.source_info.source,
                "confidence": self.source_info.confidence.value,
                "timestamp": self.source_info.timestamp.isoformat(),
            }
            if self.source_info
            else None,
        }


class HistoricalSlippageModel:
    """Slippage model that uses historical liquidity depth for accurate calculations.

    This model integrates with the LiquidityDepthProvider to fetch historical
    liquidity data and calculate slippage based on actual pool depth at trade time.
    Falls back to constant product math when historical data is unavailable.

    Key Features:
        - Accept historical liquidity depth as input (LiquidityResult)
        - Calculate slippage based on actual liquidity at trade time
        - Support time-weighted average depth option via config
        - Fall back to constant product math when historical depth unavailable
        - Track confidence level in slippage result

    Attributes:
        config: Configuration for slippage calculation behavior.

    Example:
        from almanak.framework.backtesting.pnl.fee_models.slippage_guard import (
            HistoricalSlippageModel,
            SlippageModelConfig,
        )
        from almanak.framework.backtesting.pnl.types import (
            LiquidityResult, DataSourceInfo, DataConfidence
        )

        config = SlippageModelConfig(use_twap_depth=True)
        model = HistoricalSlippageModel(config=config)

        # With historical liquidity
        liquidity = LiquidityResult(
            depth=Decimal("5000000"),
            source_info=DataSourceInfo(
                source="uniswap_v3_subgraph",
                confidence=DataConfidence.HIGH,
                timestamp=datetime.now(UTC),
            ),
        )
        result = model.calculate_slippage(
            trade_amount_usd=Decimal("50000"),
            historical_liquidity=liquidity,
            pool_type="v3",
        )

        # Without historical liquidity (fallback)
        result = model.calculate_slippage(
            trade_amount_usd=Decimal("50000"),
            historical_liquidity=None,
            pool_type="v2",
        )
    """

    def __init__(self, config: SlippageModelConfig | None = None) -> None:
        """Initialize the historical slippage model.

        Args:
            config: Configuration for slippage calculation. Uses defaults if None.
        """
        self.config = config or SlippageModelConfig()

    def calculate_slippage(
        self,
        trade_amount_usd: Decimal,
        historical_liquidity: LiquidityResult | None = None,
        pool_type: str = "v3",
        concentration_factor: Decimal | None = None,
        fee_bps: int | None = None,
    ) -> HistoricalSlippageResult:
        """Calculate slippage based on historical liquidity depth.

        Routes to the appropriate AMM math (V2 constant product or V3 concentrated
        liquidity) based on pool_type. Uses historical liquidity depth when available,
        falling back to constant product math with default liquidity when not.

        Args:
            trade_amount_usd: Trade size in USD.
            historical_liquidity: Historical liquidity depth from LiquidityDepthProvider.
                If None, uses fallback_liquidity_usd from config.
            pool_type: Type of pool ("v2" or "v3"). Determines calculation method.
            concentration_factor: Override for V3 concentration factor.
                Uses config value if None.
            fee_bps: Override for pool fee. Uses config value based on pool_type if None.

        Returns:
            HistoricalSlippageResult with slippage, confidence, and source info.

        Example:
            result = model.calculate_slippage(
                trade_amount_usd=Decimal("100000"),
                historical_liquidity=liquidity_result,
                pool_type="v3",
            )
            if result.was_fallback:
                logger.warning("Using fallback slippage calculation")
        """
        # Normalize pool type and trade amount
        trade_amount_usd = abs(trade_amount_usd)
        pool_type_lower = pool_type.lower()
        is_v3 = pool_type_lower in ("v3", "uniswap_v3", "concentrated")

        # Determine liquidity and source info
        if historical_liquidity is not None and historical_liquidity.depth > 0:
            liquidity_usd = historical_liquidity.depth
            source_info = historical_liquidity.source_info
            # Guard against source_info being None
            if source_info is not None:
                confidence = source_info.confidence
                data_source = source_info.source
            else:
                confidence = DataConfidence.LOW
                data_source = SLIPPAGE_SOURCE_HISTORICAL
            was_fallback = False

            # Determine if this is TWAP data based on source name
            if "twap" in data_source.lower() or self.config.use_twap_depth:
                data_source = SLIPPAGE_SOURCE_TWAP
        else:
            # Fallback to constant product math with default liquidity
            liquidity_usd = self.config.fallback_liquidity_usd or Decimal("0")
            confidence = DataConfidence.LOW
            data_source = SLIPPAGE_SOURCE_CONSTANT_PRODUCT
            source_info = None
            was_fallback = True

            logger.warning(
                "Historical liquidity unavailable for slippage calculation, using fallback liquidity: $%s",
                liquidity_usd,
            )

        # Handle zero liquidity edge case
        if liquidity_usd <= 0:
            return HistoricalSlippageResult(
                slippage=Decimal("0"),
                slippage_bps=0,
                liquidity_usd=liquidity_usd,
                confidence=DataConfidence.LOW,
                data_source=data_source,
                pool_type="v3" if is_v3 else "v2",
                was_fallback=True,
                source_info=source_info,
            )

        # Calculate slippage based on pool type
        if is_v3:
            slippage = self._calculate_v3_slippage(
                trade_amount_usd=trade_amount_usd,
                liquidity_usd=liquidity_usd,
                concentration_factor=concentration_factor,
                fee_bps=fee_bps,
            )
        else:
            slippage = self._calculate_v2_slippage(
                trade_amount_usd=trade_amount_usd,
                liquidity_usd=liquidity_usd,
                fee_bps=fee_bps,
            )

        # Apply max slippage cap
        capped_slippage = min(slippage, self.config.max_slippage_pct)
        if capped_slippage < slippage:
            logger.debug(
                "Slippage capped from %s to %s",
                slippage,
                capped_slippage,
            )

        slippage_bps = int(capped_slippage * Decimal("10000"))

        return HistoricalSlippageResult(
            slippage=capped_slippage,
            slippage_bps=slippage_bps,
            liquidity_usd=liquidity_usd,
            confidence=confidence,
            data_source=data_source,
            pool_type="v3" if is_v3 else "v2",
            was_fallback=was_fallback,
            source_info=source_info,
        )

    def _calculate_v2_slippage(
        self,
        trade_amount_usd: Decimal,
        liquidity_usd: Decimal,
        fee_bps: int | None = None,
    ) -> Decimal:
        """Calculate V2 constant-product slippage.

        V2 price impact formula:
            price_impact = trade_amount / (reserve + trade_amount)

        For a balanced pool, reserve = TVL/2, so:
            price_impact = trade_amount / (TVL/2 + trade_amount)
                        = trade_amount / ((TVL + 2*trade_amount) / 2)
                        = 2 * trade_amount / (TVL + 2*trade_amount)

        Simplified for small trades relative to TVL:
            price_impact ≈ 2 * trade_amount / TVL

        Args:
            trade_amount_usd: Trade size in USD.
            liquidity_usd: Total pool liquidity (TVL) in USD.
            fee_bps: Pool fee in basis points (optional override).

        Returns:
            Price impact as decimal.
        """
        if liquidity_usd <= 0:
            return Decimal("0")

        # Assume 50/50 split, so reserve_in = TVL/2
        reserve_in_usd = liquidity_usd / Decimal("2")

        # price_impact = trade_amount / (reserve + trade_amount)
        price_impact = trade_amount_usd / (reserve_in_usd + trade_amount_usd)

        return price_impact

    def _calculate_v3_slippage(
        self,
        trade_amount_usd: Decimal,
        liquidity_usd: Decimal,
        concentration_factor: Decimal | None = None,
        fee_bps: int | None = None,
    ) -> Decimal:
        """Calculate V3 concentrated liquidity slippage.

        V3 maintains constant product in sqrt-space, so price impact scales
        with sqrt(amount/liquidity) rather than linearly.

        For concentrated liquidity with effective depth:
            effective_liquidity = liquidity * concentration_factor
            price_impact = sqrt(trade_amount / effective_liquidity)

        Args:
            trade_amount_usd: Trade size in USD.
            liquidity_usd: Active liquidity in current tick in USD.
            concentration_factor: Multiplier for effective depth (optional override).
            fee_bps: Pool fee in V3 scale (optional override, not used in impact calc).

        Returns:
            Price impact as decimal.
        """
        if liquidity_usd <= 0:
            return Decimal("0")

        # Use config concentration factor if not overridden
        conc_factor = concentration_factor or self.config.v3_concentration_factor

        # Effective liquidity with concentration factor
        effective_liquidity = liquidity_usd * conc_factor

        # V3 price impact scales with sqrt(amount/liquidity)
        ratio = trade_amount_usd / effective_liquidity

        # Calculate square root using Newton's method
        price_impact = self._decimal_sqrt(ratio)

        return price_impact

    @staticmethod
    def _decimal_sqrt(n: Decimal) -> Decimal:
        """Calculate square root of a Decimal using Newton's method.

        Args:
            n: Non-negative Decimal to find square root of.

        Returns:
            Square root as Decimal.
        """
        if n < 0:
            raise ValueError("Cannot calculate square root of negative number")
        if n == 0:
            return Decimal("0")

        # Newton's method for square root
        x = n
        two = Decimal("2")

        for _ in range(50):
            x_next = (x + n / x) / two
            if abs(x_next - x) < Decimal("1e-15"):
                break
            x = x_next

        return x

    def calculate_slippage_from_depth(
        self,
        trade_amount_usd: Decimal,
        liquidity_depth_usd: Decimal,
        pool_type: str = "v3",
        confidence: DataConfidence = DataConfidence.HIGH,
        data_source: str = SLIPPAGE_SOURCE_HISTORICAL,
    ) -> HistoricalSlippageResult:
        """Calculate slippage directly from liquidity depth value.

        Convenience method when you have the liquidity depth value directly
        without a LiquidityResult object.

        Args:
            trade_amount_usd: Trade size in USD.
            liquidity_depth_usd: Pool liquidity depth in USD.
            pool_type: Type of pool ("v2" or "v3").
            confidence: Confidence level to assign to result.
            data_source: Data source identifier.

        Returns:
            HistoricalSlippageResult with slippage and provided metadata.
        """
        from datetime import UTC, datetime

        # Import here to avoid circular dependency
        from ..types import LiquidityResult

        # Create a LiquidityResult from the raw values
        liquidity_result = LiquidityResult(
            depth=liquidity_depth_usd,
            source_info=DataSourceInfo(
                source=data_source,
                confidence=confidence,
                timestamp=datetime.now(UTC),
            ),
        )

        return self.calculate_slippage(
            trade_amount_usd=trade_amount_usd,
            historical_liquidity=liquidity_result,
            pool_type=pool_type,
        )


# =============================================================================
# Utility Functions
# =============================================================================


def check_trade_slippage(
    trade_amount_usd: Decimal,
    estimated_slippage: Decimal,
    pool_liquidity_usd: Decimal | None = None,
    max_slippage_pct: Decimal = DEFAULT_MAX_SLIPPAGE_PCT,
    safe_liquidity_pct: Decimal = DEFAULT_SAFE_LIQUIDITY_PCT,
    log_warnings: bool = True,
    **kwargs: Any,
) -> SlippageCheckResult:
    """Convenience function to check trade slippage with default configuration.

    This is a stateless version of SlippageGuard.check_trade() for one-off checks.

    Args:
        trade_amount_usd: Trade size in USD
        estimated_slippage: Estimated price impact as decimal (0.05 = 5%)
        pool_liquidity_usd: Pool liquidity in USD (optional)
        max_slippage_pct: Maximum allowed slippage (default 10%)
        safe_liquidity_pct: Safe trade size as % of liquidity (default 5%)
        log_warnings: Whether to log warnings (default True)
        **kwargs: Additional context for warnings

    Returns:
        SlippageCheckResult with capped slippage and any warnings

    Example:
        result = check_trade_slippage(
            trade_amount_usd=Decimal("50000"),
            estimated_slippage=Decimal("0.08"),
            pool_liquidity_usd=Decimal("500000"),
            token_in="ETH",
            token_out="USDC",
        )
    """
    config = SlippageGuardConfig(
        max_slippage_pct=max_slippage_pct,
        safe_liquidity_pct=safe_liquidity_pct,
        log_warnings=log_warnings,
    )
    guard = SlippageGuard(config=config)
    return guard.check_trade(
        trade_amount_usd=trade_amount_usd,
        estimated_slippage=estimated_slippage,
        pool_liquidity_usd=pool_liquidity_usd,
        **kwargs,
    )


def cap_slippage(
    slippage: Decimal,
    max_slippage_pct: Decimal = DEFAULT_MAX_SLIPPAGE_PCT,
) -> Decimal:
    """Convenience function to cap slippage at maximum.

    Args:
        slippage: Estimated slippage as decimal
        max_slippage_pct: Maximum allowed slippage (default 10%)

    Returns:
        Capped slippage
    """
    return min(abs(slippage), max_slippage_pct)


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Config and dataclasses
    "SlippageGuardConfig",
    "SlippageWarning",
    "SlippageCheckResult",
    # Main class
    "SlippageGuard",
    # Exception
    "SlippageCapExceededError",
    # Utility functions
    "check_trade_slippage",
    "cap_slippage",
    # Constants
    "DEFAULT_MAX_SLIPPAGE_PCT",
    "DEFAULT_SAFE_LIQUIDITY_PCT",
    "DEFAULT_HIGH_IMPACT_THRESHOLD",
    "DEFAULT_CRITICAL_IMPACT_THRESHOLD",
    # Historical slippage model
    "SlippageModelConfig",
    "HistoricalSlippageResult",
    "HistoricalSlippageModel",
    # Historical slippage model constants
    "DEFAULT_V3_CONCENTRATION_FACTOR",
    "DEFAULT_V2_FEE_BPS",
    "DEFAULT_V3_FEE_BPS",
    "SLIPPAGE_SOURCE_HISTORICAL",
    "SLIPPAGE_SOURCE_CONSTANT_PRODUCT",
    "SLIPPAGE_SOURCE_TWAP",
]
