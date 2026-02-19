"""Configuration for LST Basis Trading Strategy.

This config extends HotReloadableConfig to integrate with the Intent framework
for executing LST basis trades (premium/discount arbitrage).
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from almanak.framework.models.hot_reload_config import HotReloadableConfig


@dataclass
class LSTBasisConfig(HotReloadableConfig):
    """Configuration for LST Basis Trading Strategy.

    Extends HotReloadableConfig with parameters for monitoring LST prices
    relative to ETH and executing swaps when premiums/discounts are detected.

    Attributes:
        strategy_id: Unique identifier for this strategy instance
        chain: Target blockchain network
        wallet_address: Wallet address for transactions
        lst_tokens: List of LST tokens to monitor
        min_spread_bps: Minimum spread to trigger trade
        trade_size_eth: Size of trades in ETH
    """

    # Required fields (cold - cannot be hot-reloaded)
    strategy_id: str = ""
    chain: str = "ethereum"
    wallet_address: str = ""

    # Strategy control
    pause_strategy: bool = False

    # LST token configuration (cold)
    lst_tokens: list[str] = field(default_factory=lambda: ["stETH", "rETH", "cbETH"])

    # Basis detection thresholds (hot-reloadable)
    min_spread_bps: int = 30  # 30 bps = 0.3% deviation triggers opportunity
    min_premium_bps: int = 10  # Minimum 10 bps to consider a premium
    max_spread_bps: int = 500  # Maximum 5% spread - beyond this too risky

    # Trade direction settings
    trade_premium: bool = True  # Sell LST when at premium (price > fair value)
    trade_discount: bool = True  # Buy LST when at discount (price < fair value)

    # Profit thresholds (hot-reloadable)
    min_profit_usd: Decimal = Decimal("10")  # Minimum $10 profit after gas
    min_profit_bps: int = 10  # Minimum 10 bps profit

    # Gas limits (hot-reloadable)
    max_gas_gwei: int = 100  # Maximum gas price in gwei
    estimated_gas_cost_usd: Decimal = Decimal("25")  # Estimated gas cost

    # Position sizing (hot-reloadable)
    min_trade_size_eth: Decimal = Decimal("0.1")  # Minimum trade 0.1 ETH
    max_trade_size_eth: Decimal = Decimal("100")  # Maximum trade 100 ETH
    default_trade_size_eth: Decimal = Decimal("1")  # Default trade 1 ETH

    # Slippage protection (hot-reloadable)
    max_slippage_bps: int = 50  # Maximum 50 bps slippage (0.5%)
    max_price_impact_bps: int = 100  # Maximum 100 bps price impact (1%)

    # Cooldown and timing (hot-reloadable)
    trade_cooldown_seconds: int = 120  # 2 minutes between trades
    price_cache_seconds: int = 12  # Cache prices for 12 seconds
    opportunity_expiry_seconds: int = 60  # Opportunity valid for 60 seconds

    # Protocol settings (cold)
    swap_protocol: str = "curve"  # Default swap protocol (curve, uniswap_v3)
    dexs: list[str] = field(default_factory=lambda: ["curve", "uniswap_v3"])

    # Current state (runtime - not persisted)
    last_trade_timestamp: int | None = None
    last_opportunity_found: str | None = None
    total_profit_usd: Decimal = Decimal("0")
    total_profit_eth: Decimal = Decimal("0")
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
            "min_spread_bps",
            "min_premium_bps",
            "max_spread_bps",
            "trade_premium",
            "trade_discount",
            "min_profit_usd",
            "min_profit_bps",
            "max_gas_gwei",
            "estimated_gas_cost_usd",
            "min_trade_size_eth",
            "max_trade_size_eth",
            "default_trade_size_eth",
            "max_slippage_bps",
            "max_price_impact_bps",
            "trade_cooldown_seconds",
            "price_cache_seconds",
            "opportunity_expiry_seconds",
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
            "min_trade_size_eth": (Decimal("0.01"), Decimal("1000")),
            "max_trade_size_eth": (Decimal("0.1"), Decimal("10000")),
            "default_trade_size_eth": (Decimal("0.01"), Decimal("1000")),
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
                "lst_tokens": self.lst_tokens,
                "min_spread_bps": self.min_spread_bps,
                "min_premium_bps": self.min_premium_bps,
                "max_spread_bps": self.max_spread_bps,
                "trade_premium": self.trade_premium,
                "trade_discount": self.trade_discount,
                "min_profit_usd": str(self.min_profit_usd),
                "min_profit_bps": self.min_profit_bps,
                "max_gas_gwei": self.max_gas_gwei,
                "estimated_gas_cost_usd": str(self.estimated_gas_cost_usd),
                "min_trade_size_eth": str(self.min_trade_size_eth),
                "max_trade_size_eth": str(self.max_trade_size_eth),
                "default_trade_size_eth": str(self.default_trade_size_eth),
                "max_slippage_bps": self.max_slippage_bps,
                "max_price_impact_bps": self.max_price_impact_bps,
                "trade_cooldown_seconds": self.trade_cooldown_seconds,
                "price_cache_seconds": self.price_cache_seconds,
                "opportunity_expiry_seconds": self.opportunity_expiry_seconds,
                "swap_protocol": self.swap_protocol,
                "dexs": self.dexs,
                "last_trade_timestamp": self.last_trade_timestamp,
                "last_opportunity_found": self.last_opportunity_found,
                "total_profit_usd": str(self.total_profit_usd),
                "total_profit_eth": str(self.total_profit_eth),
                "total_trades": self.total_trades,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LSTBasisConfig":
        """Create configuration from dictionary."""
        return cls(
            strategy_id=data.get("strategy_id", ""),
            chain=data.get("chain", "ethereum"),
            wallet_address=data.get("wallet_address", ""),
            pause_strategy=data.get("pause_strategy", False),
            lst_tokens=data.get("lst_tokens", ["stETH", "rETH", "cbETH"]),
            min_spread_bps=data.get("min_spread_bps", 30),
            min_premium_bps=data.get("min_premium_bps", 10),
            max_spread_bps=data.get("max_spread_bps", 500),
            trade_premium=data.get("trade_premium", True),
            trade_discount=data.get("trade_discount", True),
            min_profit_usd=Decimal(str(data.get("min_profit_usd", "10"))),
            min_profit_bps=data.get("min_profit_bps", 10),
            max_gas_gwei=data.get("max_gas_gwei", 100),
            estimated_gas_cost_usd=Decimal(str(data.get("estimated_gas_cost_usd", "25"))),
            min_trade_size_eth=Decimal(str(data.get("min_trade_size_eth", "0.1"))),
            max_trade_size_eth=Decimal(str(data.get("max_trade_size_eth", "100"))),
            default_trade_size_eth=Decimal(str(data.get("default_trade_size_eth", "1"))),
            max_slippage_bps=data.get("max_slippage_bps", 50),
            max_price_impact_bps=data.get("max_price_impact_bps", 100),
            trade_cooldown_seconds=data.get("trade_cooldown_seconds", 120),
            price_cache_seconds=data.get("price_cache_seconds", 12),
            opportunity_expiry_seconds=data.get("opportunity_expiry_seconds", 60),
            swap_protocol=data.get("swap_protocol", "curve"),
            dexs=data.get("dexs", ["curve", "uniswap_v3"]),
            max_slippage=Decimal(str(data.get("max_slippage", "0.005"))),
            trade_size_usd=Decimal(str(data.get("trade_size_usd", "2500"))),
            rebalance_threshold=Decimal(str(data.get("rebalance_threshold", "0.05"))),
            last_trade_timestamp=data.get("last_trade_timestamp"),
            last_opportunity_found=data.get("last_opportunity_found"),
            total_profit_usd=Decimal(str(data.get("total_profit_usd", "0"))),
            total_profit_eth=Decimal(str(data.get("total_profit_eth", "0"))),
            total_trades=data.get("total_trades", 0),
        )

    def calculate_spread_bps(self, market_price: Decimal, fair_value: Decimal) -> int:
        """Calculate the spread in basis points from fair value.

        Args:
            market_price: Current market price of LST vs ETH
            fair_value: Fair value based on staking rewards/exchange rate

        Returns:
            Spread in basis points (positive = premium, negative = discount)
        """
        if fair_value == Decimal("0"):
            return 0
        spread = (market_price - fair_value) / fair_value
        return int(spread * Decimal("10000"))

    def is_premium(self, spread_bps: int) -> bool:
        """Check if the spread represents a premium.

        Args:
            spread_bps: Spread in basis points

        Returns:
            True if LST is at a premium (trading above fair value)
        """
        return spread_bps >= self.min_premium_bps

    def is_discount(self, spread_bps: int) -> bool:
        """Check if the spread represents a discount.

        Args:
            spread_bps: Spread in basis points

        Returns:
            True if LST is at a discount (trading below fair value)
        """
        return spread_bps <= -self.min_premium_bps

    def is_opportunity(self, spread_bps: int) -> bool:
        """Check if the spread represents a tradeable opportunity.

        Args:
            spread_bps: Spread in basis points

        Returns:
            True if spread exceeds opportunity threshold
        """
        abs_spread = abs(spread_bps)
        if abs_spread < self.min_spread_bps:
            return False
        if abs_spread > self.max_spread_bps:
            return False
        # Check if we're configured to trade this direction
        if spread_bps > 0 and not self.trade_premium:
            return False
        if spread_bps < 0 and not self.trade_discount:
            return False
        return True

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


__all__ = ["LSTBasisConfig"]
