"""Configuration for Flash Loan Triangular Arbitrage Strategy.

This config extends HotReloadableConfig to integrate with the Intent framework
for executing atomic triangular arbitrage trades using flash loans.
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from almanak.framework.models.hot_reload_config import HotReloadableConfig


@dataclass
class FlashTriangularArbConfig(HotReloadableConfig):
    """Configuration for Flash Loan Triangular Arbitrage Strategy.

    Extends HotReloadableConfig with parameters for triangular arbitrage
    including profit thresholds, token paths, and flash loan preferences.

    Attributes:
        strategy_id: Unique identifier for this strategy instance
        chain: Target blockchain network
        wallet_address: Wallet address for transactions
        tokens: List of tokens to consider for arbitrage paths
        min_profit_bps: Minimum profit threshold in basis points
        max_hops: Maximum number of hops in arbitrage path (3-4)
        flash_loan_provider: Preferred flash loan provider
    """

    # Required fields (cold - cannot be hot-reloaded)
    strategy_id: str = ""
    chain: str = "ethereum"
    wallet_address: str = ""

    # Strategy control
    pause_strategy: bool = False

    # Token configuration (cold)
    tokens: list[str] = field(default_factory=lambda: ["WETH", "USDC", "USDT", "DAI", "WBTC"])

    # DEX configuration (cold)
    dexs: list[str] = field(default_factory=lambda: ["uniswap_v3", "curve", "enso"])

    # Flash loan configuration (cold)
    flash_loan_provider: str = "auto"  # "aave", "balancer", or "auto"
    flash_loan_priority: str = "fee"  # "fee", "liquidity", "reliability", "gas"

    # Path configuration (cold)
    max_hops: int = 3  # Maximum hops: 3 or 4 (triangular or quadrilateral)
    min_hops: int = 3  # Minimum hops for valid path

    # Profit thresholds (hot-reloadable)
    min_profit_bps: int = 10  # Minimum 10 bps profit (0.1%)
    min_profit_usd: Decimal = Decimal("10")  # Minimum $10 profit after gas

    # Gas limits (hot-reloadable)
    max_gas_gwei: int = 100  # Maximum gas price in gwei
    max_gas_limit: int = 600000  # Maximum gas limit per trade (higher for multi-swap)
    estimated_gas_cost_usd: Decimal = Decimal("30")  # Estimated gas cost for 3 swaps

    # Trade sizing (hot-reloadable)
    min_trade_size_usd: Decimal = Decimal("1000")  # Minimum trade $1,000
    max_trade_size_usd: Decimal = Decimal("100000")  # Maximum trade $100,000
    default_trade_size_usd: Decimal = Decimal("10000")  # Default trade $10,000

    # Slippage protection (hot-reloadable)
    max_slippage_bps: int = 50  # Maximum 50 bps slippage per swap (0.5%)
    max_total_slippage_bps: int = 150  # Max cumulative slippage for all swaps
    max_price_impact_bps: int = 100  # Maximum 100 bps price impact per swap

    # Cooldown (hot-reloadable)
    trade_cooldown_seconds: int = 60  # 1 minute between trades
    opportunity_cache_seconds: int = 12  # Cache quotes for 12 seconds

    # Path finding (hot-reloadable)
    max_paths_to_evaluate: int = 20  # Maximum number of paths to evaluate

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
            "min_profit_bps",
            "min_profit_usd",
            "max_gas_gwei",
            "max_gas_limit",
            "estimated_gas_cost_usd",
            "min_trade_size_usd",
            "max_trade_size_usd",
            "default_trade_size_usd",
            "max_slippage_bps",
            "max_total_slippage_bps",
            "max_price_impact_bps",
            "trade_cooldown_seconds",
            "opportunity_cache_seconds",
            "max_paths_to_evaluate",
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
                "tokens": self.tokens,
                "dexs": self.dexs,
                "flash_loan_provider": self.flash_loan_provider,
                "flash_loan_priority": self.flash_loan_priority,
                "max_hops": self.max_hops,
                "min_hops": self.min_hops,
                "min_profit_bps": self.min_profit_bps,
                "min_profit_usd": str(self.min_profit_usd),
                "max_gas_gwei": self.max_gas_gwei,
                "max_gas_limit": self.max_gas_limit,
                "estimated_gas_cost_usd": str(self.estimated_gas_cost_usd),
                "min_trade_size_usd": str(self.min_trade_size_usd),
                "max_trade_size_usd": str(self.max_trade_size_usd),
                "default_trade_size_usd": str(self.default_trade_size_usd),
                "max_slippage_bps": self.max_slippage_bps,
                "max_total_slippage_bps": self.max_total_slippage_bps,
                "max_price_impact_bps": self.max_price_impact_bps,
                "trade_cooldown_seconds": self.trade_cooldown_seconds,
                "opportunity_cache_seconds": self.opportunity_cache_seconds,
                "max_paths_to_evaluate": self.max_paths_to_evaluate,
                "last_trade_timestamp": self.last_trade_timestamp,
                "last_opportunity_found": self.last_opportunity_found,
                "total_profit_usd": str(self.total_profit_usd),
                "total_trades": self.total_trades,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FlashTriangularArbConfig":
        """Create configuration from dictionary."""
        return cls(
            strategy_id=data.get("strategy_id", ""),
            chain=data.get("chain", "ethereum"),
            wallet_address=data.get("wallet_address", ""),
            pause_strategy=data.get("pause_strategy", False),
            tokens=data.get("tokens", ["WETH", "USDC", "USDT", "DAI", "WBTC"]),
            dexs=data.get("dexs", ["uniswap_v3", "curve", "enso"]),
            flash_loan_provider=data.get("flash_loan_provider", "auto"),
            flash_loan_priority=data.get("flash_loan_priority", "fee"),
            max_hops=data.get("max_hops", 3),
            min_hops=data.get("min_hops", 3),
            min_profit_bps=data.get("min_profit_bps", 10),
            min_profit_usd=Decimal(str(data.get("min_profit_usd", "10"))),
            max_gas_gwei=data.get("max_gas_gwei", 100),
            max_gas_limit=data.get("max_gas_limit", 600000),
            estimated_gas_cost_usd=Decimal(str(data.get("estimated_gas_cost_usd", "30"))),
            min_trade_size_usd=Decimal(str(data.get("min_trade_size_usd", "1000"))),
            max_trade_size_usd=Decimal(str(data.get("max_trade_size_usd", "100000"))),
            default_trade_size_usd=Decimal(str(data.get("default_trade_size_usd", "10000"))),
            max_slippage_bps=data.get("max_slippage_bps", 50),
            max_total_slippage_bps=data.get("max_total_slippage_bps", 150),
            max_price_impact_bps=data.get("max_price_impact_bps", 100),
            trade_cooldown_seconds=data.get("trade_cooldown_seconds", 60),
            opportunity_cache_seconds=data.get("opportunity_cache_seconds", 12),
            max_paths_to_evaluate=data.get("max_paths_to_evaluate", 20),
            max_slippage=Decimal(str(data.get("max_slippage", "0.005"))),
            trade_size_usd=Decimal(str(data.get("trade_size_usd", "10000"))),
            rebalance_threshold=Decimal(str(data.get("rebalance_threshold", "0.05"))),
            last_trade_timestamp=data.get("last_trade_timestamp"),
            last_opportunity_found=data.get("last_opportunity_found"),
            total_profit_usd=Decimal(str(data.get("total_profit_usd", "0"))),
            total_trades=data.get("total_trades", 0),
        )

    def calculate_min_output(self, amount_in: Decimal) -> Decimal:
        """Calculate minimum acceptable output for a trade.

        Args:
            amount_in: Input amount

        Returns:
            Minimum output amount after slippage
        """
        slippage_factor = Decimal(10000 - self.max_slippage_bps) / Decimal(10000)
        return amount_in * slippage_factor

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

    def get_estimated_gas_per_hop(self) -> Decimal:
        """Get estimated gas cost per hop for multi-hop trades.

        Returns:
            Estimated gas cost in USD per swap hop
        """
        # Estimate: base cost / typical hop count
        return self.estimated_gas_cost_usd / Decimal(self.max_hops)


__all__ = ["FlashTriangularArbConfig"]
