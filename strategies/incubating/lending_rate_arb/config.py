"""Configuration for Lending Rate Arbitrage Strategy.

This config extends HotReloadableConfig to integrate with the Intent framework
for lending rate arbitrage between DeFi protocols.
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from almanak.framework.models.hot_reload_config import HotReloadableConfig


@dataclass
class LendingRateArbConfig(HotReloadableConfig):
    """Configuration for Lending Rate Arbitrage Strategy.

    Extends HotReloadableConfig with lending arbitrage-specific parameters
    for cross-protocol rate optimization.

    Attributes:
        strategy_id: Unique identifier for this strategy instance
        chain: Target blockchain network
        wallet_address: Wallet address for transactions
        tokens: List of tokens to monitor for rate arbitrage
        protocols: List of protocols to compare rates across
        min_spread_bps: Minimum rate spread in basis points before rebalancing (default 50 = 0.5%)
        rebalance_threshold_usd: Minimum position size in USD to rebalance
        check_interval_seconds: How often to check for opportunities
        max_position_usd: Maximum position size per token
    """

    # Required fields (cold - cannot be hot-reloaded)
    strategy_id: str = ""
    chain: str = "ethereum"
    wallet_address: str = ""

    # Token configuration (cold)
    tokens: list[str] = field(default_factory=lambda: ["USDC", "USDT", "DAI", "WETH"])

    # Protocol configuration (cold)
    protocols: list[str] = field(default_factory=lambda: ["aave_v3", "morpho_blue", "compound_v3"])

    # Strategy control
    pause_strategy: bool = False

    # Arbitrage parameters (hot-reloadable)
    min_spread_bps: int = 50  # 0.5% minimum spread to act
    rebalance_threshold_usd: Decimal = Decimal("100")  # Minimum USD to rebalance
    check_interval_seconds: int = 60  # Check every minute
    max_position_usd: Decimal = Decimal("100000")  # Max $100k per token

    # Position tracking (runtime state)
    current_positions: dict[str, dict[str, Decimal]] = field(default_factory=dict)  # {token: {protocol: amount}}

    # Override hot-reloadable fields to include arb-specific params
    HOT_RELOADABLE_FIELDS: set[str] = field(
        default_factory=lambda: {
            # From HotReloadableConfig
            "max_slippage",
            "trade_size_usd",
            "rebalance_threshold",
            "min_health_factor",
            "max_leverage",
            "daily_loss_limit_usd",
            # Arbitrage-specific
            "min_spread_bps",
            "rebalance_threshold_usd",
            "check_interval_seconds",
            "max_position_usd",
            "pause_strategy",
        },
        repr=False,
    )

    # Extend valid ranges for arb-specific fields
    _VALID_RANGES: dict[str, tuple[Decimal, Decimal]] = field(
        default_factory=lambda: {
            # From HotReloadableConfig
            "max_slippage": (Decimal("0.001"), Decimal("0.1")),
            "trade_size_usd": (Decimal("10"), Decimal("1000000")),
            "rebalance_threshold": (Decimal("0.01"), Decimal("0.5")),
            "min_health_factor": (Decimal("1.1"), Decimal("5")),
            "max_leverage": (Decimal("1"), Decimal("10")),
            "daily_loss_limit_usd": (Decimal("0"), Decimal("1000000")),
            # Arbitrage-specific
            "rebalance_threshold_usd": (Decimal("10"), Decimal("1000000")),
            "max_position_usd": (Decimal("100"), Decimal("10000000")),
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
                "tokens": self.tokens,
                "protocols": self.protocols,
                "pause_strategy": self.pause_strategy,
                "min_spread_bps": self.min_spread_bps,
                "rebalance_threshold_usd": str(self.rebalance_threshold_usd),
                "check_interval_seconds": self.check_interval_seconds,
                "max_position_usd": str(self.max_position_usd),
                "current_positions": {
                    token: {proto: str(amt) for proto, amt in protos.items()}
                    for token, protos in self.current_positions.items()
                },
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LendingRateArbConfig":
        """Create configuration from dictionary."""
        # Parse current_positions
        current_positions = {}
        if "current_positions" in data:
            for token, protos in data["current_positions"].items():
                current_positions[token] = {proto: Decimal(str(amt)) for proto, amt in protos.items()}

        return cls(
            strategy_id=data.get("strategy_id", ""),
            chain=data.get("chain", "ethereum"),
            wallet_address=data.get("wallet_address", ""),
            tokens=data.get("tokens", ["USDC", "USDT", "DAI", "WETH"]),
            protocols=data.get("protocols", ["aave_v3", "morpho_blue", "compound_v3"]),
            pause_strategy=data.get("pause_strategy", False),
            min_spread_bps=data.get("min_spread_bps", 50),
            rebalance_threshold_usd=Decimal(str(data.get("rebalance_threshold_usd", "100"))),
            check_interval_seconds=data.get("check_interval_seconds", 60),
            max_position_usd=Decimal(str(data.get("max_position_usd", "100000"))),
            max_slippage=Decimal(str(data.get("max_slippage", "0.005"))),
            trade_size_usd=Decimal(str(data.get("trade_size_usd", "1000"))),
            rebalance_threshold=Decimal(str(data.get("rebalance_threshold", "0.05"))),
            current_positions=current_positions,
        )


__all__ = ["LendingRateArbConfig"]
