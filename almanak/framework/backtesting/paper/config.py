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
from decimal import Decimal, InvalidOperation
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


_RPC_URL_PREFIXES = ("http://", "https://", "ws://", "wss://")
_VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
_VALID_PRICE_SOURCES = {"coingecko", "chainlink", "twap", "auto"}


def _normalize_chain(chain: str) -> str:
    chain_lower = chain.lower()
    if chain_lower not in CHAIN_IDS:
        valid_chains = ", ".join(sorted(CHAIN_IDS.keys()))
        raise ValueError(f"Unsupported chain '{chain}'. Valid chains: {valid_chains}")
    return chain_lower


def _validate_rpc_url(rpc_url: str) -> None:
    if not rpc_url:
        raise ValueError("rpc_url cannot be empty")
    if not rpc_url.startswith(_RPC_URL_PREFIXES):
        raise ValueError(f"rpc_url must be a valid URL, got: {rpc_url[:50]}...")


def _validate_deployment_id(deployment_id: str) -> None:
    if not deployment_id:
        raise ValueError("deployment_id cannot be empty")


def _decimal_for_validation(name: str, value: Any) -> Decimal:
    if not isinstance(value, Decimal | int | float):
        raise ValueError(f"{name} must be a Decimal-compatible number")
    try:
        decimal_value = value if type(value) is Decimal else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{name} must be a Decimal-compatible number") from exc
    if not decimal_value.is_finite():
        raise ValueError(f"{name} must be finite")
    return decimal_value


def _validate_non_negative_decimal(name: str, value: Any, negative_message: str) -> None:
    if _decimal_for_validation(name, value) < Decimal("0"):
        raise ValueError(negative_message)


def _validate_fraction(name: str, value: Any) -> None:
    decimal_value = _decimal_for_validation(name, value)
    if not (Decimal("0") <= decimal_value <= Decimal("1")):
        raise ValueError(f"{name} must be between 0 and 1, got {value}")


def _validate_initial_balances(
    initial_eth: Any,
    initial_tokens: dict[str, Any],
    bootstrap: dict[str, Any],
) -> None:
    if not isinstance(initial_tokens, dict):
        raise ValueError(f"initial_tokens must be a dict, got {type(initial_tokens).__name__}")
    if not isinstance(bootstrap, dict):
        raise ValueError(f"bootstrap must be a dict, got {type(bootstrap).__name__}")

    _validate_non_negative_decimal("initial_eth", initial_eth, "initial_eth cannot be negative")

    for token, amount in initial_tokens.items():
        _validate_non_negative_decimal(
            f"initial_tokens[{token}]",
            amount,
            f"initial_tokens[{token}] cannot be negative",
        )

    for chain_key, tokens in bootstrap.items():
        if not isinstance(tokens, dict):
            raise ValueError(f"bootstrap[{chain_key}] must be a dict, got {type(tokens).__name__}")
        for token, amount in tokens.items():
            _validate_non_negative_decimal(
                f"bootstrap[{chain_key}][{token}]",
                amount,
                f"bootstrap[{chain_key}][{token}] cannot be negative",
            )


def _coerce_fork_lifecycle(fork_lifecycle: ForkLifecycle | str) -> ForkLifecycle:
    if isinstance(fork_lifecycle, str):
        return ForkLifecycle(fork_lifecycle)
    return fork_lifecycle


def _validate_tick_interval(tick_interval_seconds: int) -> None:
    if tick_interval_seconds <= 0:
        raise ValueError("tick_interval_seconds must be positive")


def _validate_max_ticks(max_ticks: int | None) -> None:
    if max_ticks is not None and max_ticks <= 0:
        raise ValueError("max_ticks must be positive if specified")


def _validate_anvil_port(anvil_port: int) -> None:
    if anvil_port <= 0 or anvil_port > 65535:
        raise ValueError(f"Invalid anvil_port: {anvil_port}")


def _validate_startup_timeout(startup_timeout_seconds: float) -> None:
    if startup_timeout_seconds <= 0:
        raise ValueError("startup_timeout_seconds must be positive")


def _validate_wallet_address(wallet_address: str | None) -> None:
    if wallet_address is None:
        return
    if not wallet_address.startswith("0x"):
        raise ValueError("wallet_address must start with '0x'")
    if len(wallet_address) != 42:
        raise ValueError("wallet_address must be 42 characters")


def _normalize_log_level(log_level: str) -> str:
    normalized = log_level.upper()
    if normalized not in _VALID_LOG_LEVELS:
        raise ValueError(f"Invalid log_level '{log_level}'. Valid levels: {', '.join(sorted(_VALID_LOG_LEVELS))}")
    return normalized


def _validate_price_source(price_source: str) -> None:
    if price_source not in _VALID_PRICE_SOURCES:
        raise ValueError(
            f"Invalid price_source '{price_source}'. Valid sources: {', '.join(sorted(_VALID_PRICE_SOURCES))}"
        )


def _apply_allow_hardcoded_fallback(strict_price_mode: bool, allow_hardcoded_fallback: bool | None) -> bool:
    if allow_hardcoded_fallback is None:
        return strict_price_mode

    import warnings

    warnings.warn(
        "allow_hardcoded_fallback is deprecated and is ignored; it no longer "
        "affects pricing behavior. Set strict_price_mode directly instead "
        "(strict_price_mode=False permits hardcoded fallbacks; the default "
        "strict_price_mode=True fails when no real price is available).",
        DeprecationWarning,
        stacklevel=3,
    )
    # The deprecated flag is inert: strict_price_mode is authoritative. We do
    # not let allow_hardcoded_fallback flip the mode, because a dataclass cannot
    # distinguish an explicit strict_price_mode=True from its default True, so
    # honoring the legacy flag would silently override an explicit strict choice.
    return strict_price_mode


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

    position_reconciler_enabled: bool = True
    """Enable the observe-only PositionReconciler divergence detector (VIB-2634).

    Default ON. The reconciler only runs on persistent forks
    (fork_lifecycle == PERSISTENT); when the fork resets every tick
    (ROLLING_RESET) reconciliation against a fresh fork is meaningless and
    the engine skips it with a DEBUG log. V1 is observe-only: divergence is
    logged at WARNING and surfaced in PaperTradingSummary.reconciliation,
    but nothing is auto-corrected and the tick loop never halts.
    """

    position_reconciler_tolerance_pct: Decimal = Decimal("0.01")
    """Relative divergence threshold for the PositionReconciler (default 1%).

    Divergence at or below this fraction is silent; above it, a WARNING is
    logged and the divergence is recorded in the session summary. The 1%
    default deliberately absorbs expected lending drift on persistent forks:
    aToken-style balances accrue lazily on fork time advance (~0.006%/day at
    typical rates per the VIB-2630 spike), so small on-chain-vs-tracked gaps
    between pokes are normal and must not spam warnings.
    """

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

    Note: This is the inverse of the deprecated allow_hardcoded_fallback field,
    which is now ignored. strict_price_mode is authoritative; set it directly.

    Environment variable: Set ALMANAK_ALLOW_HARDCODED_PRICES=1 to override
    strict_price_mode=False for testing scenarios.
    """

    allow_hardcoded_fallback: bool | None = None
    """DEPRECATED and IGNORED: use strict_price_mode instead.

    Retained only for backward-compatible construction; it no longer changes
    pricing behavior. strict_price_mode is authoritative (strict_price_mode=False
    permits hardcoded fallbacks; the default True fails when no real price is
    available). Passing any non-None value emits a DeprecationWarning.

    Will be removed in a future version.
    """

    # crap-allowlist: VIB-4722 mechanical deployment_id rename in existing high-CRAP function.
    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        # Chain validation
        self.chain = _normalize_chain(self.chain)

        # RPC URL validation
        _validate_rpc_url(self.rpc_url)

        # Deployment ID validation
        _validate_deployment_id(self.deployment_id)

        _validate_initial_balances(self.initial_eth, self.initial_tokens, self.bootstrap)

        # Tick interval validation
        _validate_tick_interval(self.tick_interval_seconds)

        # Max ticks validation
        _validate_max_ticks(self.max_ticks)

        # Anvil port validation
        _validate_anvil_port(self.anvil_port)

        # Fork lifecycle validation (VIB-2631)
        self.fork_lifecycle = _coerce_fork_lifecycle(self.fork_lifecycle)
        # Sync reset_fork_every_tick with fork_lifecycle for backward compat
        if self.fork_lifecycle == ForkLifecycle.PERSISTENT:
            self.reset_fork_every_tick = False

        # Oracle divergence threshold validation
        _validate_fraction("oracle_divergence_threshold", self.oracle_divergence_threshold)

        # Position reconciler tolerance validation (VIB-2634)
        _validate_fraction("position_reconciler_tolerance_pct", self.position_reconciler_tolerance_pct)

        # Startup timeout validation
        _validate_startup_timeout(self.startup_timeout_seconds)

        _validate_wallet_address(self.wallet_address)

        # Log level validation
        self.log_level = _normalize_log_level(self.log_level)

        # Price source validation
        _validate_price_source(self.price_source)

        self.strict_price_mode = _apply_allow_hardcoded_fallback(
            self.strict_price_mode,
            self.allow_hardcoded_fallback,
        )

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
            "position_reconciler_tolerance_pct": str(self.position_reconciler_tolerance_pct),
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
            position_reconciler_enabled=data.get("position_reconciler_enabled", True),
            position_reconciler_tolerance_pct=Decimal(data["position_reconciler_tolerance_pct"])
            if "position_reconciler_tolerance_pct" in data
            else Decimal("0.01"),
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
