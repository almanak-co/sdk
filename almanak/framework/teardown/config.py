"""Configuration for the Strategy Teardown System.

Provides TeardownConfig with sound defaults that operators can override.
All settings have reasonable defaults - configuration is optional, not required.

Two operation modes are supported:
- Manual Exit (human-in-the-loop): Escalates with approval checkpoints
- Auto-Protect Exit (no human): Uses pre-configured max slippage, pauses if exceeded

Three-phase pipeline:
- Phase 1: Position Closure (required)
- Phase 2: Token Consolidation (optional, ON by default)
- Phase 3: Chain Consolidation (optional, OFF by default)
"""

from dataclasses import dataclass, field
from decimal import Decimal

from almanak.framework.teardown.models import TeardownAssetPolicy


@dataclass
class TokenConsolidationConfig:
    """Phase 2 configuration: Token Consolidation.

    Controls whether and how tokens are consolidated after
    closing positions (Phase 1).

    Default: ON, consolidate to USDC.
    Emergency mode: Automatically disabled (KEEP_OUTPUTS).
    """

    enabled: bool = True  # ON by default
    target_token: str = "USDC"  # Default target
    keep_tokens: list[str] = field(default_factory=list)  # Don't swap these
    min_swap_value_usd: Decimal = field(default_factory=lambda: Decimal("1"))  # Dust threshold

    def __post_init__(self) -> None:
        """Normalize fields."""
        if isinstance(self.min_swap_value_usd, int | float | str):
            self.min_swap_value_usd = Decimal(str(self.min_swap_value_usd))

    def to_dict(self) -> dict:
        """Serialize to dictionary."""
        return {
            "enabled": self.enabled,
            "target_token": self.target_token,
            "keep_tokens": self.keep_tokens,
            "min_swap_value_usd": str(self.min_swap_value_usd),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "TokenConsolidationConfig":
        """Deserialize from dictionary."""
        return cls(
            enabled=data.get("enabled", True),
            target_token=data.get("target_token", "USDC"),
            keep_tokens=data.get("keep_tokens", []),
            min_swap_value_usd=Decimal(data.get("min_swap_value_usd", "1")),
        )


@dataclass
class ChainConsolidationConfig:
    """Phase 3 configuration: Chain Consolidation.

    Controls whether assets are bridged to a single chain
    after closing positions (Phase 1) and token consolidation (Phase 2).

    Default: OFF (bridges add risk and fees).
    Emergency mode: Always disabled for safety.
    """

    enabled: bool = False  # OFF by default - bridges add risk
    target_chain: str = "arbitrum"  # Default target chain
    max_bridge_fee_percent: Decimal = field(default_factory=lambda: Decimal("0.01"))  # 1% max
    min_bridge_amount_usd: Decimal = field(default_factory=lambda: Decimal("100"))  # Min to bridge

    def __post_init__(self) -> None:
        """Normalize fields."""
        if isinstance(self.max_bridge_fee_percent, int | float | str):
            self.max_bridge_fee_percent = Decimal(str(self.max_bridge_fee_percent))
        if isinstance(self.min_bridge_amount_usd, int | float | str):
            self.min_bridge_amount_usd = Decimal(str(self.min_bridge_amount_usd))

    def to_dict(self) -> dict:
        """Serialize to dictionary."""
        return {
            "enabled": self.enabled,
            "target_chain": self.target_chain,
            "max_bridge_fee_percent": str(self.max_bridge_fee_percent),
            "min_bridge_amount_usd": str(self.min_bridge_amount_usd),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ChainConsolidationConfig":
        """Deserialize from dictionary."""
        return cls(
            enabled=data.get("enabled", False),
            target_chain=data.get("target_chain", "arbitrum"),
            max_bridge_fee_percent=Decimal(data.get("max_bridge_fee_percent", "0.01")),
            min_bridge_amount_usd=Decimal(data.get("min_bridge_amount_usd", "100")),
        )


@dataclass
class TeardownConfig:
    """Per-strategy teardown configuration.

    All fields have sound defaults. Operators can override any setting.

    Configuration Philosophy:
    - All settings have safe, conservative defaults
    - Configuration is optional - the system works out of the box
    - Presets available for common use cases (conservative, aggressive)

    Three-Phase Pipeline:
    - Phase 1: Position Closure (required) - Close all DeFi positions
    - Phase 2: Token Consolidation (optional, ON by default) - Swap to target token
    - Phase 3: Chain Consolidation (optional, OFF by default) - Bridge to target chain
    """

    # === Asset Policy ===

    # How to handle final assets after closing positions
    asset_policy: TeardownAssetPolicy = TeardownAssetPolicy.TARGET_TOKEN

    # === Phase 2 & 3 Configuration ===

    # Token consolidation (Phase 2)
    token_consolidation: TokenConsolidationConfig = field(default_factory=TokenConsolidationConfig)

    # Chain consolidation (Phase 3)
    chain_consolidation: ChainConsolidationConfig = field(default_factory=ChainConsolidationConfig)

    # === Slippage Settings ===

    # Manual exit: starting slippage (escalates with approval)
    manual_initial_slippage: Decimal = field(
        default_factory=lambda: Decimal("0.02")  # 2% default start
    )

    # Manual exit: max before requiring approval
    manual_approval_threshold: Decimal = field(
        default_factory=lambda: Decimal("0.03")  # 3% - ask above this
    )

    # Auto-protect exit: max slippage (no human available)
    auto_max_slippage: Decimal = field(
        default_factory=lambda: Decimal("0.05")  # 5% default max for auto
    )

    # Absolute maximum (even with approval, never exceed)
    absolute_max_slippage: Decimal = field(
        default_factory=lambda: Decimal("0.10")  # 10% hard ceiling
    )

    # === Loss Cap Settings ===

    # Override position-aware cap (None = use default scaling)
    custom_max_loss_percent: Decimal | None = None

    # Absolute dollar cap (None = no absolute cap)
    max_loss_usd: Decimal | None = None  # e.g., $50,000 max

    # === Auto-Protect Settings ===

    # Enable auto-protect monitoring
    auto_protect_enabled: bool = True  # Default: ON

    # Health threshold for alerts
    alert_health_threshold: Decimal = field(
        default_factory=lambda: Decimal("1.3")  # Alert at 1.3
    )

    # Health threshold for auto-exit (if auto_exit_enabled)
    auto_exit_health_threshold: Decimal = field(
        default_factory=lambda: Decimal("1.1")  # Auto-exit at 1.1
    )

    # Enable automatic exit (vs alert-only)
    auto_exit_enabled: bool = False  # Default: OFF (alert only)

    # === Execution Settings ===

    # Target token for proceeds
    target_token: str = "USDC"

    # Gas price strategy: "normal", "fast", "aggressive"
    gas_strategy: str = "normal"

    # Cancel window duration (seconds)
    cancel_window_seconds: int = 10

    # === Advanced ===

    # Skip cancel window for auto-protect exits
    skip_cancel_window_for_auto: bool = True  # Default: Yes for auto

    # Bridge back to primary chain after multi-chain exit
    bridge_back_chain: str | None = None  # None = leave distributed

    # Retry configuration
    max_retries_per_intent: int = 3
    retry_delay_seconds: int = 5

    # State staleness threshold (seconds) - regenerate intents if older
    staleness_threshold_seconds: int = 300  # 5 minutes

    def __post_init__(self) -> None:
        """Validate and normalize configuration values."""
        # Convert any string/float values to Decimal
        decimal_fields = [
            "manual_initial_slippage",
            "manual_approval_threshold",
            "auto_max_slippage",
            "absolute_max_slippage",
            "alert_health_threshold",
            "auto_exit_health_threshold",
        ]

        for field_name in decimal_fields:
            value = getattr(self, field_name)
            if isinstance(value, int | float | str):
                setattr(self, field_name, Decimal(str(value)))

        # Handle optional Decimal fields
        if self.custom_max_loss_percent is not None:
            if isinstance(self.custom_max_loss_percent, int | float | str):
                self.custom_max_loss_percent = Decimal(str(self.custom_max_loss_percent))

        if self.max_loss_usd is not None:
            if isinstance(self.max_loss_usd, int | float | str):
                self.max_loss_usd = Decimal(str(self.max_loss_usd))

        # Validate slippage ordering
        self._validate_slippage_ordering()

    def _validate_slippage_ordering(self) -> None:
        """Ensure slippage thresholds are in correct order."""
        if self.manual_initial_slippage > self.manual_approval_threshold:
            raise ValueError(
                f"manual_initial_slippage ({self.manual_initial_slippage}) "
                f"cannot exceed manual_approval_threshold ({self.manual_approval_threshold})"
            )

        if self.manual_approval_threshold > self.auto_max_slippage:
            raise ValueError(
                f"manual_approval_threshold ({self.manual_approval_threshold}) "
                f"cannot exceed auto_max_slippage ({self.auto_max_slippage})"
            )

        if self.auto_max_slippage > self.absolute_max_slippage:
            raise ValueError(
                f"auto_max_slippage ({self.auto_max_slippage}) "
                f"cannot exceed absolute_max_slippage ({self.absolute_max_slippage})"
            )

    @classmethod
    def default(cls) -> "TeardownConfig":
        """Sound defaults for most users.

        Returns a configuration suitable for typical strategy operations
        with balanced safety and execution efficiency.
        """
        return cls()

    @classmethod
    def conservative(cls) -> "TeardownConfig":
        """Tighter caps for risk-averse operators.

        Use when:
        - Managing large positions (>$500K)
        - Operating in volatile market conditions
        - Prioritizing capital preservation over execution speed
        """
        return cls(
            manual_initial_slippage=Decimal("0.01"),  # 1% start
            manual_approval_threshold=Decimal("0.02"),  # 2% approval threshold
            auto_max_slippage=Decimal("0.03"),  # 3% auto max
            absolute_max_slippage=Decimal("0.05"),  # 5% hard ceiling
            alert_health_threshold=Decimal("1.5"),  # Earlier alerts
        )

    @classmethod
    def aggressive(cls) -> "TeardownConfig":
        """Looser caps for operators who prioritize execution.

        Use when:
        - Speed of execution is critical
        - Operating in illiquid markets
        - Willing to accept higher costs for certainty of exit
        """
        return cls(
            manual_initial_slippage=Decimal("0.03"),  # 3% start
            manual_approval_threshold=Decimal("0.05"),  # 5% approval threshold
            auto_max_slippage=Decimal("0.08"),  # 8% auto max
            auto_exit_enabled=True,  # Auto-exit enabled
            gas_strategy="fast",  # Pay more for faster gas
        )

    def get_slippage_for_mode(self, mode: str, is_auto: bool = False) -> Decimal:
        """Get the appropriate initial slippage for a teardown mode.

        Args:
            mode: "graceful" or "emergency"
            is_auto: Whether this is an auto-protect triggered exit

        Returns:
            Initial slippage to use
        """
        if is_auto:
            return self.auto_max_slippage

        if mode == "graceful":
            return Decimal("0.005")  # 0.5% for graceful
        else:
            return self.manual_initial_slippage  # 2% for emergency

    def get_max_slippage_for_mode(self, mode: str, is_auto: bool = False) -> Decimal:
        """Get the maximum slippage allowed for a teardown mode.

        Args:
            mode: "graceful" or "emergency"
            is_auto: Whether this is an auto-protect triggered exit

        Returns:
            Maximum slippage allowed
        """
        if is_auto:
            return self.auto_max_slippage

        # For manual modes, can escalate up to absolute max with approval
        return self.absolute_max_slippage

    def should_require_approval(self, slippage: Decimal, is_auto: bool = False) -> bool:
        """Check if the given slippage requires human approval.

        Args:
            slippage: The slippage level to check
            is_auto: Whether this is an auto-protect triggered exit

        Returns:
            True if human approval is required
        """
        if is_auto:
            # Auto mode never asks for approval - it pauses if exceeded
            return False

        return slippage > self.manual_approval_threshold

    def validate_slippage(self, slippage: Decimal) -> bool:
        """Validate that slippage is within absolute limits.

        Args:
            slippage: The slippage to validate

        Returns:
            True if slippage is acceptable
        """
        return Decimal("0") < slippage <= self.absolute_max_slippage

    def to_dict(self) -> dict:
        """Convert config to dictionary for serialization."""
        return {
            # Asset policy
            "asset_policy": self.asset_policy.value,
            # Phase 2 & 3
            "token_consolidation": self.token_consolidation.to_dict(),
            "chain_consolidation": self.chain_consolidation.to_dict(),
            # Slippage
            "manual_initial_slippage": str(self.manual_initial_slippage),
            "manual_approval_threshold": str(self.manual_approval_threshold),
            "auto_max_slippage": str(self.auto_max_slippage),
            "absolute_max_slippage": str(self.absolute_max_slippage),
            # Loss caps
            "custom_max_loss_percent": (str(self.custom_max_loss_percent) if self.custom_max_loss_percent else None),
            "max_loss_usd": str(self.max_loss_usd) if self.max_loss_usd else None,
            # Auto-protect
            "auto_protect_enabled": self.auto_protect_enabled,
            "alert_health_threshold": str(self.alert_health_threshold),
            "auto_exit_health_threshold": str(self.auto_exit_health_threshold),
            "auto_exit_enabled": self.auto_exit_enabled,
            # Execution
            "target_token": self.target_token,
            "gas_strategy": self.gas_strategy,
            "cancel_window_seconds": self.cancel_window_seconds,
            "skip_cancel_window_for_auto": self.skip_cancel_window_for_auto,
            "bridge_back_chain": self.bridge_back_chain,
            "max_retries_per_intent": self.max_retries_per_intent,
            "retry_delay_seconds": self.retry_delay_seconds,
            "staleness_threshold_seconds": self.staleness_threshold_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "TeardownConfig":
        """Create config from dictionary."""
        # Handle asset policy
        if "asset_policy" in data:
            data["asset_policy"] = TeardownAssetPolicy(data["asset_policy"])

        # Handle Phase 2 & 3 configs
        if "token_consolidation" in data and isinstance(data["token_consolidation"], dict):
            data["token_consolidation"] = TokenConsolidationConfig.from_dict(data["token_consolidation"])
        if "chain_consolidation" in data and isinstance(data["chain_consolidation"], dict):
            data["chain_consolidation"] = ChainConsolidationConfig.from_dict(data["chain_consolidation"])

        # Convert string Decimals back
        if "manual_initial_slippage" in data:
            data["manual_initial_slippage"] = Decimal(data["manual_initial_slippage"])
        if "manual_approval_threshold" in data:
            data["manual_approval_threshold"] = Decimal(data["manual_approval_threshold"])
        if "auto_max_slippage" in data:
            data["auto_max_slippage"] = Decimal(data["auto_max_slippage"])
        if "absolute_max_slippage" in data:
            data["absolute_max_slippage"] = Decimal(data["absolute_max_slippage"])
        if data.get("custom_max_loss_percent"):
            data["custom_max_loss_percent"] = Decimal(data["custom_max_loss_percent"])
        if data.get("max_loss_usd"):
            data["max_loss_usd"] = Decimal(data["max_loss_usd"])
        if "alert_health_threshold" in data:
            data["alert_health_threshold"] = Decimal(data["alert_health_threshold"])
        if "auto_exit_health_threshold" in data:
            data["auto_exit_health_threshold"] = Decimal(data["auto_exit_health_threshold"])

        return cls(**data)

    # === Additional Presets ===

    @classmethod
    def keep_native(cls) -> "TeardownConfig":
        """Keep native tokens, no consolidation swaps.

        Use when:
        - Want to keep natural exit tokens (e.g., WETH + USDC from LP)
        - Tax optimization (avoid swap taxable events)
        - Plan to manually manage returned assets
        """
        return cls(
            asset_policy=TeardownAssetPolicy.KEEP_OUTPUTS,
            token_consolidation=TokenConsolidationConfig(enabled=False),
            chain_consolidation=ChainConsolidationConfig(enabled=False),
        )

    @classmethod
    def full_consolidation(cls, target_chain: str = "arbitrum") -> "TeardownConfig":
        """Full consolidation to single token on single chain.

        Use when:
        - Want clean accounting (single balance on one chain)
        - Small amounts across many chains
        - Simplifying portfolio
        """
        return cls(
            asset_policy=TeardownAssetPolicy.TARGET_TOKEN,
            token_consolidation=TokenConsolidationConfig(enabled=True),
            chain_consolidation=ChainConsolidationConfig(
                enabled=True,
                target_chain=target_chain,
            ),
        )
