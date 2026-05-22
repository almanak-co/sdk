"""Configuration for Paper Trading.

This module defines the configuration dataclass for paper trading sessions,
which controls simulation parameters like chain, initial balances, tick intervals,
and Anvil fork settings.

Key Components:
    - PaperTraderConfig: Main configuration dataclass for paper trading

Examples:
    Basic configuration with minimal settings:

        from almanak.framework.backtesting.paper.config import PaperTraderConfig
        from decimal import Decimal

        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            deployment_id="my_strategy",
        )

    Custom configuration with initial balances and tick settings:

        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            deployment_id="momentum_v1",
            initial_eth=Decimal("20"),
            initial_tokens={"USDC": Decimal("50000"), "WETH": Decimal("5")},
            tick_interval_seconds=30,
            max_ticks=1000,
            reset_fork_every_tick=False,
        )

    Production-grade configuration with strict price validation:

        # When strict_price_mode=True, the paper trader will fail
        # if it cannot get prices from real data sources (Chainlink, TWAP,
        # or CoinGecko). This ensures accurate valuations for institutional use.
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY",
            deployment_id="production_strategy",
            initial_tokens={"USDC": Decimal("100000")},
            price_source="auto",  # Use Chainlink -> TWAP -> CoinGecko fallback
            strict_price_mode=True,  # Fail if no real price available
        )
"""

from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal

from almanak.framework.anvil.fork_manager import CHAIN_IDS


class ForkLifecycle(StrEnum):
    """Fork lifecycle mode for paper trading.

    Controls how the Anvil fork is managed between ticks.

    Values:
        ROLLING_RESET: Default. Reset fork to latest mainnet block each tick.
            Positions are destroyed and re-funded from the portfolio tracker.
            Best for execution validation (smoke testing TX flow).

        PERSISTENT: Keep fork alive across ticks. Positions survive.
            Time is advanced via evm_increaseTime + evm_mine between ticks.
            Protocol poke transactions trigger interest accrual.
            Best for yield-validation (measuring lending strategy PnL).
    """

    ROLLING_RESET = "rolling_reset"
    PERSISTENT = "persistent"


@dataclass
class PaperTraderConfig:
    """Configuration for a paper trading session.

    Controls all parameters of the paper trading session including chain,
    initial balances, tick intervals, and Anvil fork settings.

    Paper trading executes real transactions on a local Anvil fork,
    allowing strategies to be validated with actual DeFi protocol
    interactions before deployment with real capital.

    Attributes:
        chain: Blockchain to paper trade on (e.g., "arbitrum", "ethereum")
        rpc_url: Archive RPC URL to fork from (Alchemy, Infura, etc.)
        deployment_id: Identifier of the strategy being tested
        initial_eth: Initial ETH balance for the paper wallet (default: 10)
        initial_tokens: Dict of token symbol to amount for initial balances
        tick_interval_seconds: Time between trading ticks in seconds (default: 60)
        max_ticks: Maximum number of ticks to run, None = run indefinitely
        anvil_port: Port to run Anvil on (default: 8546)
        reset_fork_every_tick: Whether to reset fork to latest block each tick (default: True)
        startup_timeout_seconds: Timeout for Anvil startup (default: 30)
        auto_impersonate: Enable auto-impersonation for any address (default: True)
        block_time: Optional block time in seconds (default: None = instant)
        wallet_address: Optional paper wallet address (default: None = auto-generated)
        log_trades: Whether to log individual trades (default: True)
        log_level: Logging level for paper trader (default: "INFO")
        price_source: Price source to use ('coingecko', 'chainlink', 'twap', 'auto')

    Example:
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            deployment_id="momentum_v1",
            initial_eth=Decimal("10"),
            initial_tokens={"USDC": Decimal("10000")},
        )
        print(f"Chain: {config.chain} (ID: {config.chain_id})")
        print(f"Max duration: {config.max_duration_seconds}s")
    """

    # Required fields
    chain: str
    rpc_url: str
    deployment_id: str

    # Initial balances
    initial_eth: Decimal = Decimal("10")
    initial_tokens: dict[str, Decimal] = field(default_factory=dict)

    # Per-chain bootstrap requirements (VIB-2375)
    bootstrap: dict[str, dict[str, Decimal]] = field(default_factory=dict)
    """Per-chain token requirements for paper trading wallet funding.

    Structure: {chain: {token_symbol_or_address: amount}}

    Example::

        bootstrap={
            "arbitrum": {"USDC": Decimal("100"), "WETH": Decimal("1")},
            "ethereum": {"USDT": Decimal("50")},
        }

    When set, bootstrap[config.chain] is merged with initial_tokens to
    produce the full set of tokens to fund. initial_tokens takes precedence
    over bootstrap for the same token key (CLI flags override config).

    Populated from strategy config.json ``paper_trading.bootstrap`` key.
    The legacy ``anvil_funding`` flat dict is also supported as a fallback.
    """

    # Bootstrap validation (VIB-2377)
    strict_bootstrap: bool = False
    """Whether missing tokens should be a hard failure.

    When False (default): Missing tokens (zero balance) are logged as errors.
    When True: Missing tokens abort the session.
    """

    # Tick configuration
    tick_interval_seconds: int = 60
    max_ticks: int | None = None  # None = run indefinitely

    # Anvil fork configuration
    anvil_port: int = 8546
    reset_fork_every_tick: bool = True
    startup_timeout_seconds: float = 30.0
    auto_impersonate: bool = True
    block_time: int | None = None

    # Fork lifecycle (VIB-2631)
    fork_lifecycle: ForkLifecycle = ForkLifecycle.ROLLING_RESET
    """Fork lifecycle mode controlling how the fork is managed between ticks.

    ROLLING_RESET (default): Reset fork to latest mainnet block each tick.
        Current behavior. Best for execution validation.
    PERSISTENT: Keep fork alive across ticks with time advancement.
        Positions survive, yield accrues. Best for yield-validation.

    When set to PERSISTENT, reset_fork_every_tick is ignored.
    """

    # Yield-validation options (only apply when fork_lifecycle == PERSISTENT)
    yield_poker_enabled: bool = False
    """Enable YieldPoker to poke lending protocols for interest accrual."""

    use_rich_valuation: bool = False
    """Use _value_portfolio_rich() with live prices for equity calculation."""

    position_reconciler_enabled: bool = False
    """Enable PositionReconciler to detect on-chain vs tracked divergence."""

    oracle_divergence_threshold: Decimal = Decimal("0.05")
    """Maximum allowed divergence between live and on-fork prices (5% default).
    Paper trading halts with a clear error if this threshold is exceeded.
    Only applies when fork_lifecycle == PERSISTENT.
    """

    # Wallet configuration
    wallet_address: str | None = None

    # Logging configuration
    log_trades: bool = True
    log_level: str = "INFO"

    # Price source configuration
    price_source: Literal["coingecko", "chainlink", "twap", "auto"] = "auto"
    """Price source to use for portfolio valuation.

    Options:
        - 'coingecko': Use CoinGecko API for market prices.
            Best for: General tokens, off-chain price feeds, no RPC needed.
        - 'chainlink': Use Chainlink oracles for on-chain prices.
            Best for: Major tokens with Chainlink feeds, trustless pricing.
        - 'twap': Use time-weighted average price from DEX pools.
            Best for: On-chain pricing, newer tokens, DEX-native prices.
        - 'auto' (default): Automatic fallback chain - tries Chainlink first,
            falls back to TWAP, then CoinGecko if others fail.
    """

    # Data quality configuration
    strict_price_mode: bool = True
    """Whether to fail when price providers cannot return a price.

    When True (default): Raises ValueError if all price providers fail for a token.
    This is the institutional-grade setting that ensures all prices are from
    real data sources. Use this for production backtests where accuracy is critical.
    Error messages include the failed token and chain for debugging.

    When False: Falls back to hardcoded prices for common tokens
    (ETH=$3000, BTC=$60000, etc.) when all price providers fail. This allows
    backtests to complete but may produce inaccurate results. Only use this
    for development/testing where price accuracy is not critical.

    Note: This is the inverse of the deprecated allow_hardcoded_fallback field.
    If both are set, strict_price_mode takes precedence.

    Environment variable: Set ALMANAK_ALLOW_HARDCODED_PRICES=1 to override
    strict_price_mode=False for testing scenarios.
    """

    allow_hardcoded_fallback: bool | None = None
    """DEPRECATED: Use strict_price_mode instead.

    This field is kept for backward compatibility. If set, it will be converted
    to the equivalent strict_price_mode value (allow_hardcoded_fallback=False
    is equivalent to strict_price_mode=True).

    Will be removed in a future version.
    """

    # crap-allowlist: VIB-4722 mechanical deployment_id rename in existing high-CRAP function.
    def __post_init__(self) -> None:  # noqa: C901
        """Validate configuration after initialization."""
        # Chain validation
        chain_lower = self.chain.lower()
        if chain_lower not in CHAIN_IDS:
            valid_chains = ", ".join(sorted(CHAIN_IDS.keys()))
            raise ValueError(f"Unsupported chain '{self.chain}'. Valid chains: {valid_chains}")
        self.chain = chain_lower

        # RPC URL validation
        if not self.rpc_url:
            raise ValueError("rpc_url cannot be empty")
        if not self.rpc_url.startswith(("http://", "https://", "ws://", "wss://")):
            raise ValueError(f"rpc_url must be a valid URL, got: {self.rpc_url[:50]}...")

        # Deployment ID validation
        if not self.deployment_id:
            raise ValueError("deployment_id cannot be empty")

        # Initial ETH validation
        if self.initial_eth < Decimal("0"):
            raise ValueError("initial_eth cannot be negative")

        # Initial tokens validation
        for token, amount in self.initial_tokens.items():
            if amount < Decimal("0"):
                raise ValueError(f"initial_tokens[{token}] cannot be negative")

        # Bootstrap validation (VIB-2375)
        for chain_key, tokens in self.bootstrap.items():
            if not isinstance(tokens, dict):
                raise ValueError(f"bootstrap[{chain_key}] must be a dict, got {type(tokens).__name__}")
            for token, amount in tokens.items():
                if amount < Decimal("0"):
                    raise ValueError(f"bootstrap[{chain_key}][{token}] cannot be negative")

        # Tick interval validation
        if self.tick_interval_seconds <= 0:
            raise ValueError("tick_interval_seconds must be positive")

        # Max ticks validation
        if self.max_ticks is not None and self.max_ticks <= 0:
            raise ValueError("max_ticks must be positive if specified")

        # Anvil port validation
        if self.anvil_port <= 0 or self.anvil_port > 65535:
            raise ValueError(f"Invalid anvil_port: {self.anvil_port}")

        # Fork lifecycle validation (VIB-2631)
        if isinstance(self.fork_lifecycle, str):
            self.fork_lifecycle = ForkLifecycle(self.fork_lifecycle)
        # Sync reset_fork_every_tick with fork_lifecycle for backward compat
        if self.fork_lifecycle == ForkLifecycle.PERSISTENT:
            self.reset_fork_every_tick = False

        # Oracle divergence threshold validation
        if not (Decimal("0") <= self.oracle_divergence_threshold <= Decimal("1")):
            raise ValueError(
                f"oracle_divergence_threshold must be between 0 and 1, got {self.oracle_divergence_threshold}"
            )

        # Startup timeout validation
        if self.startup_timeout_seconds <= 0:
            raise ValueError("startup_timeout_seconds must be positive")

        # Wallet address validation (if provided)
        if self.wallet_address is not None:
            if not self.wallet_address.startswith("0x"):
                raise ValueError("wallet_address must start with '0x'")
            if len(self.wallet_address) != 42:
                raise ValueError("wallet_address must be 42 characters")

        # Log level validation
        valid_log_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if self.log_level.upper() not in valid_log_levels:
            raise ValueError(
                f"Invalid log_level '{self.log_level}'. Valid levels: {', '.join(sorted(valid_log_levels))}"
            )
        self.log_level = self.log_level.upper()

        # Price source validation
        valid_price_sources = {"coingecko", "chainlink", "twap", "auto"}
        if self.price_source not in valid_price_sources:
            raise ValueError(
                f"Invalid price_source '{self.price_source}'. Valid sources: {', '.join(sorted(valid_price_sources))}"
            )

        # Handle backward compatibility for allow_hardcoded_fallback -> strict_price_mode
        # allow_hardcoded_fallback=False is equivalent to strict_price_mode=True
        if self.allow_hardcoded_fallback is not None:
            import warnings

            warnings.warn(
                "allow_hardcoded_fallback is deprecated. Use strict_price_mode instead. "
                "allow_hardcoded_fallback=False is equivalent to strict_price_mode=True.",
                DeprecationWarning,
                stacklevel=3,
            )
            # If user explicitly set allow_hardcoded_fallback=True, they want relaxed mode
            # Override strict_price_mode to False (relaxed mode)
            if self.allow_hardcoded_fallback:
                self.strict_price_mode = False

    @property
    def chain_id(self) -> int:
        """Get the chain ID for the configured chain."""
        return CHAIN_IDS[self.chain]

    @property
    def max_duration_seconds(self) -> int | None:
        """Get the maximum duration in seconds, or None if indefinite."""
        if self.max_ticks is None:
            return None
        return self.max_ticks * self.tick_interval_seconds

    @property
    def max_duration_minutes(self) -> float | None:
        """Get the maximum duration in minutes, or None if indefinite."""
        if self.max_duration_seconds is None:
            return None
        return self.max_duration_seconds / 60

    @property
    def max_duration_hours(self) -> float | None:
        """Get the maximum duration in hours, or None if indefinite."""
        if self.max_duration_seconds is None:
            return None
        return self.max_duration_seconds / 3600

    @property
    def tick_interval_minutes(self) -> float:
        """Get the tick interval in minutes."""
        return self.tick_interval_seconds / 60

    @property
    def fork_rpc_url(self) -> str:
        """Get the local fork RPC URL."""
        return f"http://localhost:{self.anvil_port}"

    def get_initial_balances(self) -> dict[str, Decimal]:
        """Get all initial balances including ETH.

        Merges bootstrap[chain] (base) with initial_tokens (override).
        initial_tokens takes precedence for the same token key.
        Preserves original token key casing. Token symbols like "wstETH",
        "swETH", "USDbC" have mixed case that must be kept intact.
        ERC-20 addresses are passed through as-is (checksummed or lowercase).

        Returns:
            Dictionary of token key to initial balance amount
        """
        balances: dict[str, Decimal] = {"ETH": self.initial_eth}
        # Layer 1: bootstrap for the current chain (base defaults)
        bootstrap_tokens = self.bootstrap.get(self.chain, {})
        for token, amount in bootstrap_tokens.items():
            balances[token] = amount
        # Layer 2: initial_tokens override bootstrap (CLI flags > config)
        for token, amount in self.initial_tokens.items():
            balances[token] = amount
        return balances

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "chain": self.chain,
            "chain_id": self.chain_id,
            "rpc_url": self._mask_url(self.rpc_url),
            "deployment_id": self.deployment_id,
            "initial_eth": str(self.initial_eth),
            "initial_tokens": {k: str(v) for k, v in self.initial_tokens.items()},
            "bootstrap": {
                chain: {tok: str(amt) for tok, amt in tokens.items()} for chain, tokens in self.bootstrap.items()
            },
            "strict_bootstrap": self.strict_bootstrap,
            "tick_interval_seconds": self.tick_interval_seconds,
            "max_ticks": self.max_ticks,
            "anvil_port": self.anvil_port,
            "reset_fork_every_tick": self.reset_fork_every_tick,
            "startup_timeout_seconds": self.startup_timeout_seconds,
            "auto_impersonate": self.auto_impersonate,
            "block_time": self.block_time,
            "wallet_address": self.wallet_address,
            "log_trades": self.log_trades,
            "log_level": self.log_level,
            "price_source": self.price_source,
            "strict_price_mode": self.strict_price_mode,
            # Backward compat: serialize as inverse of strict_price_mode
            "allow_hardcoded_fallback": not self.strict_price_mode,
            # Fork lifecycle (VIB-2631)
            "fork_lifecycle": self.fork_lifecycle.value,
            "yield_poker_enabled": self.yield_poker_enabled,
            "use_rich_valuation": self.use_rich_valuation,
            "position_reconciler_enabled": self.position_reconciler_enabled,
            "oracle_divergence_threshold": str(self.oracle_divergence_threshold),
            # Computed properties
            "max_duration_seconds": self.max_duration_seconds,
            "fork_rpc_url": self.fork_rpc_url,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PaperTraderConfig":
        """Deserialize from dictionary.

        Args:
            data: Dictionary containing config fields

        Returns:
            PaperTraderConfig instance
        """
        # Parse Decimal fields
        initial_eth = (
            Decimal(data["initial_eth"])
            if isinstance(data.get("initial_eth"), str)
            else data.get("initial_eth", Decimal("10"))
        )

        # Parse initial_tokens dict
        initial_tokens: dict[str, Decimal] = {}
        raw_tokens = data.get("initial_tokens", {})
        if isinstance(raw_tokens, dict):
            for token, amount in raw_tokens.items():
                if isinstance(amount, str):
                    initial_tokens[token] = Decimal(amount)
                else:
                    initial_tokens[token] = Decimal(str(amount))

        # Parse bootstrap dict (VIB-2375)
        bootstrap: dict[str, dict[str, Decimal]] = {}
        raw_bootstrap = data.get("bootstrap", {})
        if isinstance(raw_bootstrap, dict):
            for chain_key, tokens in raw_bootstrap.items():
                if isinstance(tokens, dict):
                    bootstrap[chain_key] = {tok: Decimal(str(amt)) for tok, amt in tokens.items()}

        # Handle strict_price_mode with backward compatibility for allow_hardcoded_fallback
        # Priority: strict_price_mode > allow_hardcoded_fallback
        strict_price_mode = data.get("strict_price_mode", True)
        allow_hardcoded_fallback = data.get("allow_hardcoded_fallback")

        return cls(
            chain=data["chain"],
            rpc_url=data["rpc_url"],
            deployment_id=data["deployment_id"],
            initial_eth=initial_eth,
            initial_tokens=initial_tokens,
            bootstrap=bootstrap,
            strict_bootstrap=data.get("strict_bootstrap", False),
            tick_interval_seconds=data.get("tick_interval_seconds", 60),
            max_ticks=data.get("max_ticks"),
            anvil_port=data.get("anvil_port", 8546),
            reset_fork_every_tick=data.get("reset_fork_every_tick", True),
            startup_timeout_seconds=data.get("startup_timeout_seconds", 30.0),
            auto_impersonate=data.get("auto_impersonate", True),
            block_time=data.get("block_time"),
            wallet_address=data.get("wallet_address"),
            log_trades=data.get("log_trades", True),
            log_level=data.get("log_level", "INFO"),
            price_source=data.get("price_source", "auto"),
            strict_price_mode=strict_price_mode,
            allow_hardcoded_fallback=allow_hardcoded_fallback,
            # Fork lifecycle (VIB-2631)
            fork_lifecycle=ForkLifecycle(data["fork_lifecycle"])
            if "fork_lifecycle" in data
            else ForkLifecycle.ROLLING_RESET,
            yield_poker_enabled=data.get("yield_poker_enabled", False),
            use_rich_valuation=data.get("use_rich_valuation", False),
            position_reconciler_enabled=data.get("position_reconciler_enabled", False),
            oracle_divergence_threshold=Decimal(data["oracle_divergence_threshold"])
            if "oracle_divergence_threshold" in data
            else Decimal("0.05"),
        )

    @staticmethod
    def _mask_url(url: str) -> str:
        """Mask sensitive parts of URL for logging/serialization."""
        import re

        if not url:
            return url
        # Mask API keys in path or query
        masked = re.sub(
            r"(api[_-]?key|apikey|key|token)=([^&]+)",
            r"\1=***",
            url,
            flags=re.IGNORECASE,
        )
        # Mask API keys in URL path (common for Alchemy/Infura)
        masked = re.sub(r"/([a-zA-Z0-9_-]{20,})(/|$)", r"/***\2", masked)
        return masked

    def __repr__(self) -> str:
        """Return a human-readable representation."""
        max_ticks_str = str(self.max_ticks) if self.max_ticks else "∞"
        return (
            f"PaperTraderConfig("
            f"chain={self.chain}, "
            f"strategy={self.deployment_id}, "
            f"eth={self.initial_eth}, "
            f"tokens={len(self.initial_tokens)}, "
            f"interval={self.tick_interval_seconds}s, "
            f"max_ticks={max_ticks_str})"
        )


__all__ = ["ForkLifecycle", "PaperTraderConfig"]
