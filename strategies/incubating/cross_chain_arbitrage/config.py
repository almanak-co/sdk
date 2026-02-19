"""Configuration for Cross-Chain Arbitrage Strategy.

This config extends HotReloadableConfig to integrate with the Intent framework
for executing cross-chain arbitrage trades using bridge infrastructure.
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from almanak.framework.models.hot_reload_config import HotReloadableConfig

# Bridge fee estimates by provider (in basis points)
BRIDGE_FEES_BPS: dict[str, int] = {
    "across": 10,  # ~0.1% fee + gas
    "stargate": 15,  # ~0.15% fee
    "hop": 20,  # ~0.2% fee
    "cbridge": 25,  # ~0.25% fee
    "synapse": 30,  # ~0.3% fee
    "default": 50,  # Conservative default
}

# Bridge latency estimates in seconds
BRIDGE_LATENCY_SECONDS: dict[str, int] = {
    "across": 120,  # ~2 minutes (fast finality)
    "stargate": 600,  # ~10 minutes (LayerZero)
    "hop": 600,  # ~10 minutes
    "cbridge": 900,  # ~15 minutes
    "synapse": 600,  # ~10 minutes
    "default": 900,  # Conservative default ~15 min
}

# Supported chains for cross-chain arbitrage
SUPPORTED_CHAINS = ["ethereum", "arbitrum", "optimism", "base"]


@dataclass
class CrossChainArbConfig(HotReloadableConfig):
    """Configuration for Cross-Chain Arbitrage Strategy.

    Extends HotReloadableConfig with parameters for cross-chain arbitrage
    including spread thresholds, bridge preferences, and risk parameters.

    Key Parameters:
        min_spread_bps: Minimum price spread to trigger arbitrage
        bridge_provider: Preferred bridge for cross-chain transfers
        max_bridge_latency_seconds: Maximum acceptable bridge latency
        account_for_bridge_fees: Whether to subtract bridge fees from profit

    Risk Parameters:
        max_slippage_swap: Maximum slippage for swap operations
        max_slippage_bridge: Maximum slippage for bridge operations
        max_price_change_during_bridge: Max acceptable price change during bridge

    Position Sizing:
        trade_amount_usd: Amount to trade per arbitrage execution
        min_balance_usd: Minimum balance required on source chain

    Cooldown:
        cooldown_seconds: Time between arbitrage attempts
    """

    # Required fields (cold - cannot be hot-reloaded)
    strategy_id: str = ""
    wallet_address: str = ""

    # Multi-chain configuration (cold)
    primary_chain: str = "arbitrum"  # Primary chain for the strategy
    chains: list[str] = field(default_factory=lambda: ["arbitrum", "optimism", "base"])

    # Strategy control (hot-reloadable)
    pause_strategy: bool = False

    # Token configuration (cold)
    quote_token: str = "ETH"  # Token to arbitrage
    base_token: str = "USDC"  # Base token for trades

    # Arbitrage detection thresholds (hot-reloadable)
    min_spread_bps: int = 50  # 0.5% minimum spread required
    min_spread_after_fees_bps: int = 10  # 0.1% minimum profit after all fees

    # Bridge configuration (cold)
    bridge_provider: str | None = None  # None = auto-select
    max_bridge_latency_seconds: int = 900  # 15 minute max
    account_for_bridge_fees: bool = True
    account_for_bridge_latency: bool = True

    # Risk parameters (hot-reloadable)
    max_slippage_swap: Decimal = field(default_factory=lambda: Decimal("0.003"))  # 0.3%
    max_slippage_bridge: Decimal = field(default_factory=lambda: Decimal("0.005"))  # 0.5%
    max_price_change_during_bridge: Decimal = field(default_factory=lambda: Decimal("0.01"))  # 1% max price change

    # Position sizing (hot-reloadable)
    trade_amount_usd: Decimal = field(default_factory=lambda: Decimal("1000"))
    min_balance_usd: Decimal = field(default_factory=lambda: Decimal("100"))
    max_position_usd: Decimal = field(default_factory=lambda: Decimal("50000"))

    # Gas estimates per chain (hot-reloadable)
    estimated_swap_gas_usd: Decimal = field(default_factory=lambda: Decimal("5"))
    estimated_bridge_gas_usd: Decimal = field(default_factory=lambda: Decimal("10"))

    # Cooldown (hot-reloadable)
    cooldown_seconds: int = 60

    # Volatility thresholds (hot-reloadable)
    high_volatility_threshold: Decimal = field(default_factory=lambda: Decimal("0.02"))  # 2% hourly volatility
    pause_on_high_volatility: bool = True

    # Runtime state (not persisted)
    last_trade_timestamp: int | None = None
    last_opportunity_found: str | None = None
    total_profit_usd: Decimal = field(default_factory=lambda: Decimal("0"))
    total_trades: int = 0
    failed_trades: int = 0

    # Override hot-reloadable fields
    HOT_RELOADABLE_FIELDS: set[str] = field(
        default_factory=lambda: {
            "pause_strategy",
            "min_spread_bps",
            "min_spread_after_fees_bps",
            "max_slippage_swap",
            "max_slippage_bridge",
            "max_price_change_during_bridge",
            "trade_amount_usd",
            "min_balance_usd",
            "max_position_usd",
            "estimated_swap_gas_usd",
            "estimated_bridge_gas_usd",
            "cooldown_seconds",
            "high_volatility_threshold",
            "pause_on_high_volatility",
            # From HotReloadableConfig base
            "max_slippage",
            "trade_size_usd",
            "rebalance_threshold",
            "min_health_factor",
            "max_leverage",
            "daily_loss_limit_usd",
        },
        repr=False,
    )

    def get_bridge_fee_bps(self, bridge: str | None = None) -> int:
        """Get estimated bridge fee in basis points.

        Args:
            bridge: Bridge provider name. If None, uses configured provider.

        Returns:
            Estimated bridge fee in basis points
        """
        provider = bridge or self.bridge_provider or "default"
        return BRIDGE_FEES_BPS.get(provider, BRIDGE_FEES_BPS["default"])

    def get_bridge_latency_seconds(self, bridge: str | None = None) -> int:
        """Get estimated bridge latency in seconds.

        Args:
            bridge: Bridge provider name. If None, uses configured provider.

        Returns:
            Estimated bridge latency in seconds
        """
        provider = bridge or self.bridge_provider or "default"
        return BRIDGE_LATENCY_SECONDS.get(provider, BRIDGE_LATENCY_SECONDS["default"])

    def calculate_total_fees_bps(self, bridge: str | None = None) -> int:
        """Calculate total fees for a cross-chain arbitrage trade.

        Total fees include:
        - Bridge fee (0.1% - 0.5%)
        - Expected swap slippage (0.3% x2 for buy and sell)
        - Bridge slippage (0.5%)

        Args:
            bridge: Bridge provider name

        Returns:
            Total estimated fees in basis points
        """
        bridge_fee = self.get_bridge_fee_bps(bridge) if self.account_for_bridge_fees else 0
        swap_slippage = int(self.max_slippage_swap * 10000) * 2  # buy + sell
        bridge_slippage = int(self.max_slippage_bridge * 10000)

        return bridge_fee + swap_slippage + bridge_slippage

    def calculate_net_profit_bps(
        self,
        spread_bps: int,
        bridge: str | None = None,
    ) -> int:
        """Calculate net profit after all fees.

        Args:
            spread_bps: Raw price spread in basis points
            bridge: Bridge provider name

        Returns:
            Net profit in basis points (can be negative)
        """
        total_fees = self.calculate_total_fees_bps(bridge)
        return spread_bps - total_fees

    def is_profitable(
        self,
        spread_bps: int,
        bridge: str | None = None,
    ) -> bool:
        """Check if an opportunity is profitable after fees.

        Args:
            spread_bps: Raw price spread in basis points
            bridge: Bridge provider name

        Returns:
            True if opportunity meets minimum profit threshold
        """
        net_profit = self.calculate_net_profit_bps(spread_bps, bridge)
        return net_profit >= self.min_spread_after_fees_bps

    def estimate_profit_usd(
        self,
        spread_bps: int,
        amount_usd: Decimal,
        bridge: str | None = None,
    ) -> Decimal:
        """Estimate profit in USD for an arbitrage opportunity.

        Args:
            spread_bps: Raw price spread in basis points
            amount_usd: Trade amount in USD
            bridge: Bridge provider name

        Returns:
            Estimated profit in USD
        """
        net_profit_bps = self.calculate_net_profit_bps(spread_bps, bridge)

        # Calculate profit from spread
        profit_from_spread = amount_usd * Decimal(net_profit_bps) / Decimal("10000")

        # Subtract gas costs
        total_gas = (
            self.estimated_swap_gas_usd * 2  # buy + sell swaps
            + self.estimated_bridge_gas_usd
        )

        return profit_from_spread - total_gas

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary."""
        base = super().to_dict()
        base.update(
            {
                "strategy_id": self.strategy_id,
                "wallet_address": self.wallet_address,
                "primary_chain": self.primary_chain,
                "chains": self.chains,
                "pause_strategy": self.pause_strategy,
                "quote_token": self.quote_token,
                "base_token": self.base_token,
                "min_spread_bps": self.min_spread_bps,
                "min_spread_after_fees_bps": self.min_spread_after_fees_bps,
                "bridge_provider": self.bridge_provider,
                "max_bridge_latency_seconds": self.max_bridge_latency_seconds,
                "account_for_bridge_fees": self.account_for_bridge_fees,
                "account_for_bridge_latency": self.account_for_bridge_latency,
                "max_slippage_swap": str(self.max_slippage_swap),
                "max_slippage_bridge": str(self.max_slippage_bridge),
                "max_price_change_during_bridge": str(self.max_price_change_during_bridge),
                "trade_amount_usd": str(self.trade_amount_usd),
                "min_balance_usd": str(self.min_balance_usd),
                "max_position_usd": str(self.max_position_usd),
                "estimated_swap_gas_usd": str(self.estimated_swap_gas_usd),
                "estimated_bridge_gas_usd": str(self.estimated_bridge_gas_usd),
                "cooldown_seconds": self.cooldown_seconds,
                "high_volatility_threshold": str(self.high_volatility_threshold),
                "pause_on_high_volatility": self.pause_on_high_volatility,
                "last_trade_timestamp": self.last_trade_timestamp,
                "last_opportunity_found": self.last_opportunity_found,
                "total_profit_usd": str(self.total_profit_usd),
                "total_trades": self.total_trades,
                "failed_trades": self.failed_trades,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CrossChainArbConfig":
        """Create configuration from dictionary."""
        return cls(
            strategy_id=data.get("strategy_id", ""),
            wallet_address=data.get("wallet_address", ""),
            primary_chain=data.get("primary_chain", "arbitrum"),
            chains=data.get("chains", ["arbitrum", "optimism", "base"]),
            pause_strategy=data.get("pause_strategy", False),
            quote_token=data.get("quote_token", "ETH"),
            base_token=data.get("base_token", "USDC"),
            min_spread_bps=data.get("min_spread_bps", 50),
            min_spread_after_fees_bps=data.get("min_spread_after_fees_bps", 10),
            bridge_provider=data.get("bridge_provider"),
            max_bridge_latency_seconds=data.get("max_bridge_latency_seconds", 900),
            account_for_bridge_fees=data.get("account_for_bridge_fees", True),
            account_for_bridge_latency=data.get("account_for_bridge_latency", True),
            max_slippage_swap=Decimal(str(data.get("max_slippage_swap", "0.003"))),
            max_slippage_bridge=Decimal(str(data.get("max_slippage_bridge", "0.005"))),
            max_price_change_during_bridge=Decimal(str(data.get("max_price_change_during_bridge", "0.01"))),
            trade_amount_usd=Decimal(str(data.get("trade_amount_usd", "1000"))),
            min_balance_usd=Decimal(str(data.get("min_balance_usd", "100"))),
            max_position_usd=Decimal(str(data.get("max_position_usd", "50000"))),
            estimated_swap_gas_usd=Decimal(str(data.get("estimated_swap_gas_usd", "5"))),
            estimated_bridge_gas_usd=Decimal(str(data.get("estimated_bridge_gas_usd", "10"))),
            cooldown_seconds=data.get("cooldown_seconds", 60),
            high_volatility_threshold=Decimal(str(data.get("high_volatility_threshold", "0.02"))),
            pause_on_high_volatility=data.get("pause_on_high_volatility", True),
            max_slippage=Decimal(str(data.get("max_slippage", "0.005"))),
            trade_size_usd=Decimal(str(data.get("trade_size_usd", "1000"))),
            rebalance_threshold=Decimal(str(data.get("rebalance_threshold", "0.05"))),
            last_trade_timestamp=data.get("last_trade_timestamp"),
            last_opportunity_found=data.get("last_opportunity_found"),
            total_profit_usd=Decimal(str(data.get("total_profit_usd", "0"))),
            total_trades=data.get("total_trades", 0),
            failed_trades=data.get("failed_trades", 0),
        )


__all__ = [
    "CrossChainArbConfig",
    "BRIDGE_FEES_BPS",
    "BRIDGE_LATENCY_SECONDS",
    "SUPPORTED_CHAINS",
]
