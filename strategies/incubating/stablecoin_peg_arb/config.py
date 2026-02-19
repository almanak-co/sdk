"""Configuration for Stablecoin Peg Arbitrage Strategy.

This config extends HotReloadableConfig to integrate with the Intent framework
for executing stablecoin depeg arbitrage trades using Curve pools.
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from almanak.framework.models.hot_reload_config import HotReloadableConfig


@dataclass
class StablecoinPegArbConfig(HotReloadableConfig):
    """Configuration for Stablecoin Peg Arbitrage Strategy.

    Extends HotReloadableConfig with parameters for monitoring stablecoin prices
    and executing Curve swaps when depeg events occur.

    Attributes:
        strategy_id: Unique identifier for this strategy instance
        chain: Target blockchain network
        wallet_address: Wallet address for transactions
        stablecoins: List of stablecoins to monitor
        depeg_threshold_bps: Basis points deviation from peg to trigger trade
        min_profit_usd: Minimum profit in USD to execute trade
        max_position_usd: Maximum position size in USD
    """

    # Required fields (cold - cannot be hot-reloaded)
    strategy_id: str = ""
    chain: str = "ethereum"
    wallet_address: str = ""

    # Strategy control
    pause_strategy: bool = False

    # Stablecoin configuration (cold)
    stablecoins: list[str] = field(default_factory=lambda: ["USDC", "USDT", "DAI", "FRAX"])

    # Curve pool configuration (cold)
    # Default pools on Ethereum mainnet
    curve_pools: list[str] = field(
        default_factory=lambda: [
            "3pool",  # DAI/USDC/USDT
            "frax_usdc",  # FRAX/USDC
        ]
    )

    # Depeg detection thresholds (hot-reloadable)
    depeg_threshold_bps: int = 50  # 50 bps = 0.5% deviation triggers opportunity
    min_depeg_bps: int = 10  # Minimum 10 bps to consider a depeg
    max_depeg_bps: int = 500  # Maximum 5% depeg - beyond this too risky

    # Profit thresholds (hot-reloadable)
    min_profit_usd: Decimal = Decimal("5")  # Minimum $5 profit after gas
    min_profit_bps: int = 5  # Minimum 5 bps profit

    # Gas limits (hot-reloadable)
    max_gas_gwei: int = 100  # Maximum gas price in gwei
    estimated_gas_cost_usd: Decimal = Decimal("15")  # Estimated gas cost

    # Position sizing (hot-reloadable)
    min_trade_size_usd: Decimal = Decimal("1000")  # Minimum trade $1,000
    max_trade_size_usd: Decimal = Decimal("100000")  # Maximum trade $100,000
    default_trade_size_usd: Decimal = Decimal("10000")  # Default trade $10,000

    # Slippage protection (hot-reloadable)
    max_slippage_bps: int = 30  # Maximum 30 bps slippage (0.3%) - low for stables
    max_price_impact_bps: int = 50  # Maximum 50 bps price impact (0.5%)

    # Cooldown and timing (hot-reloadable)
    trade_cooldown_seconds: int = 60  # 1 minute between trades
    price_cache_seconds: int = 12  # Cache prices for 12 seconds
    opportunity_expiry_seconds: int = 30  # Opportunity valid for 30 seconds

    # Target price for restoration (assumed peg target)
    peg_target: Decimal = Decimal("1.00")

    # Current state (runtime - not persisted)
    last_trade_timestamp: int | None = None
    last_opportunity_found: str | None = None
    total_profit_usd: Decimal = Decimal("0")
    total_trades: int = 0

    # Override hot-reloadable fields
    HOT_RELOADABLE_FIELDS: set[str] = field(
        default_factory=lambda: {
            # From HotReloadableConfig
            "max_slippage",
            "trade_size_usd",
            "rebalance_threshold",
            "min_health_factor",
            "max_leverage",
            "daily_loss_limit_usd",
            # Strategy-specific
            "pause_strategy",
            "depeg_threshold_bps",
            "min_depeg_bps",
            "max_depeg_bps",
            "min_profit_usd",
            "min_profit_bps",
            "max_gas_gwei",
            "estimated_gas_cost_usd",
            "min_trade_size_usd",
            "max_trade_size_usd",
            "default_trade_size_usd",
            "max_slippage_bps",
            "max_price_impact_bps",
            "trade_cooldown_seconds",
            "price_cache_seconds",
            "opportunity_expiry_seconds",
            "peg_target",
        },
        repr=False,
    )

    # Extend valid ranges for strategy-specific fields
    _VALID_RANGES: dict[str, tuple[Decimal, Decimal]] = field(
        default_factory=lambda: {
            # From HotReloadableConfig
            "max_slippage": (Decimal("0.001"), Decimal("0.1")),
            "trade_size_usd": (Decimal("10"), Decimal("1000000")),
            "rebalance_threshold": (Decimal("0.01"), Decimal("0.5")),
            "min_health_factor": (Decimal("1.1"), Decimal("5")),
            "max_leverage": (Decimal("1"), Decimal("10")),
            "daily_loss_limit_usd": (Decimal("0"), Decimal("1000000")),
            # Strategy-specific
            "min_profit_usd": (Decimal("1"), Decimal("10000")),
            "estimated_gas_cost_usd": (Decimal("1"), Decimal("500")),
            "min_trade_size_usd": (Decimal("100"), Decimal("1000000")),
            "max_trade_size_usd": (Decimal("1000"), Decimal("10000000")),
            "default_trade_size_usd": (Decimal("100"), Decimal("1000000")),
            "peg_target": (Decimal("0.90"), Decimal("1.10")),
        },
        repr=False,
    )

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary."""
        base = super().to_dict()
        base.update(
            {
                "strategy_id": self.strategy_id,
                "chain": self.chain,
                "wallet_address": self.wallet_address,
                "pause_strategy": self.pause_strategy,
                "stablecoins": self.stablecoins,
                "curve_pools": self.curve_pools,
                "depeg_threshold_bps": self.depeg_threshold_bps,
                "min_depeg_bps": self.min_depeg_bps,
                "max_depeg_bps": self.max_depeg_bps,
                "min_profit_usd": str(self.min_profit_usd),
                "min_profit_bps": self.min_profit_bps,
                "max_gas_gwei": self.max_gas_gwei,
                "estimated_gas_cost_usd": str(self.estimated_gas_cost_usd),
                "min_trade_size_usd": str(self.min_trade_size_usd),
                "max_trade_size_usd": str(self.max_trade_size_usd),
                "default_trade_size_usd": str(self.default_trade_size_usd),
                "max_slippage_bps": self.max_slippage_bps,
                "max_price_impact_bps": self.max_price_impact_bps,
                "trade_cooldown_seconds": self.trade_cooldown_seconds,
                "price_cache_seconds": self.price_cache_seconds,
                "opportunity_expiry_seconds": self.opportunity_expiry_seconds,
                "peg_target": str(self.peg_target),
                "last_trade_timestamp": self.last_trade_timestamp,
                "last_opportunity_found": self.last_opportunity_found,
                "total_profit_usd": str(self.total_profit_usd),
                "total_trades": self.total_trades,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StablecoinPegArbConfig":
        """Create configuration from dictionary."""
        return cls(
            strategy_id=data.get("strategy_id", ""),
            chain=data.get("chain", "ethereum"),
            wallet_address=data.get("wallet_address", ""),
            pause_strategy=data.get("pause_strategy", False),
            stablecoins=data.get("stablecoins", ["USDC", "USDT", "DAI", "FRAX"]),
            curve_pools=data.get("curve_pools", ["3pool", "frax_usdc"]),
            depeg_threshold_bps=data.get("depeg_threshold_bps", 50),
            min_depeg_bps=data.get("min_depeg_bps", 10),
            max_depeg_bps=data.get("max_depeg_bps", 500),
            min_profit_usd=Decimal(str(data.get("min_profit_usd", "5"))),
            min_profit_bps=data.get("min_profit_bps", 5),
            max_gas_gwei=data.get("max_gas_gwei", 100),
            estimated_gas_cost_usd=Decimal(str(data.get("estimated_gas_cost_usd", "15"))),
            min_trade_size_usd=Decimal(str(data.get("min_trade_size_usd", "1000"))),
            max_trade_size_usd=Decimal(str(data.get("max_trade_size_usd", "100000"))),
            default_trade_size_usd=Decimal(str(data.get("default_trade_size_usd", "10000"))),
            max_slippage_bps=data.get("max_slippage_bps", 30),
            max_price_impact_bps=data.get("max_price_impact_bps", 50),
            trade_cooldown_seconds=data.get("trade_cooldown_seconds", 60),
            price_cache_seconds=data.get("price_cache_seconds", 12),
            opportunity_expiry_seconds=data.get("opportunity_expiry_seconds", 30),
            peg_target=Decimal(str(data.get("peg_target", "1.00"))),
            max_slippage=Decimal(str(data.get("max_slippage", "0.003"))),
            trade_size_usd=Decimal(str(data.get("trade_size_usd", "10000"))),
            rebalance_threshold=Decimal(str(data.get("rebalance_threshold", "0.05"))),
            last_trade_timestamp=data.get("last_trade_timestamp"),
            last_opportunity_found=data.get("last_opportunity_found"),
            total_profit_usd=Decimal(str(data.get("total_profit_usd", "0"))),
            total_trades=data.get("total_trades", 0),
        )

    def calculate_depeg_bps(self, price: Decimal) -> int:
        """Calculate the depeg in basis points from the target peg.

        Args:
            price: Current price of the stablecoin

        Returns:
            Depeg in basis points (absolute value)
        """
        deviation = abs(price - self.peg_target)
        return int(deviation / self.peg_target * Decimal("10000"))

    def is_depegged(self, price: Decimal) -> bool:
        """Check if a price represents a depeg event.

        Args:
            price: Current price of the stablecoin

        Returns:
            True if price deviates from peg by more than threshold
        """
        depeg_bps = self.calculate_depeg_bps(price)
        return self.min_depeg_bps <= depeg_bps <= self.max_depeg_bps

    def is_opportunity(self, price: Decimal) -> bool:
        """Check if a price represents a tradeable opportunity.

        Args:
            price: Current price of the stablecoin

        Returns:
            True if depeg exceeds opportunity threshold
        """
        depeg_bps = self.calculate_depeg_bps(price)
        return depeg_bps >= self.depeg_threshold_bps and depeg_bps <= self.max_depeg_bps

    def is_profitable(
        self,
        gross_profit_usd: Decimal,
        gross_profit_bps: int,
    ) -> bool:
        """Check if an opportunity meets profitability requirements.

        Args:
            gross_profit_usd: Gross profit in USD (before gas)
            gross_profit_bps: Gross profit in basis points

        Returns:
            True if opportunity is profitable after gas
        """
        # Check basis points threshold
        if gross_profit_bps < self.min_profit_bps:
            return False

        # Check USD profit after estimated gas
        net_profit_usd = gross_profit_usd - self.estimated_gas_cost_usd
        return net_profit_usd >= self.min_profit_usd


__all__ = ["StablecoinPegArbConfig"]
