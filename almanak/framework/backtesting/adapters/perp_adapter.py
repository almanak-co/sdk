"""Perp (Perpetual Futures) backtest adapter for leveraged positions.

This module provides the backtest adapter for perpetual futures strategies,
handling GMX, Hyperliquid, and similar perp protocol positions. It manages:

- Funding rate payment application
- Liquidation price calculation and monitoring
- Margin validation and tracking
- Position valuation with unrealized PnL

Key Features:
    - Configurable funding application frequency
    - Protocol-specific margin requirements
    - Liquidation warning and simulation
    - Accurate PnL tracking for long and short positions
    - Historical funding rate integration via BacktestDataConfig
    - Support for connector-declared historical funding providers

Example:
    from almanak.framework.backtesting.adapters.perp_adapter import (
        PerpBacktestAdapter,
        PerpBacktestConfig,
    )
    from almanak.framework.backtesting.config import BacktestDataConfig

    # Create config for perp backtesting with historical funding rates
    config = PerpBacktestConfig(
        strategy_type="perp",
        funding_application_frequency="hourly",
        liquidation_model_enabled=True,
    )
    data_config = BacktestDataConfig(
        use_historical_funding=True,
        funding_fallback_rate=Decimal("0.0001"),
    )

    # Get adapter instance with data config
    adapter = PerpBacktestAdapter(config, data_config=data_config)

    # Use in backtesting
    fill = adapter.execute_intent(intent, portfolio, market_state)
"""

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal, cast

from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry
from almanak.core.chains import DEFAULT_CHAIN, LEGACY_SERIALIZED_CHAIN
from almanak.framework.backtesting.adapters.base import (
    StrategyBacktestAdapter,
    StrategyBacktestConfig,
    register_adapter,
)
from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError
from almanak.framework.backtesting.models import LiquidationEvent
from almanak.framework.backtesting.pnl.calculators.funding import (
    FundingCalculator,
    FundingRateSource,
)
from almanak.framework.backtesting.pnl.calculators.liquidation import (
    LiquidationCalculator,
)
from almanak.framework.backtesting.pnl.calculators.margin import (
    MarginValidator,
)
from almanak.framework.backtesting.pnl.data_provider import TokenRef, token_ref_display, token_ref_provider_symbol
from almanak.framework.backtesting.pnl.portfolio import PositionType
from almanak.framework.backtesting.pnl.providers.base import BacktestProviderConfig, HistoricalFundingProvider
from almanak.framework.backtesting.pnl.providers.funding_rates import (
    DEFAULT_FUNDING_RATE,
    FundingRateProvider,
)

if TYPE_CHECKING:
    from almanak.framework.backtesting.config import BacktestDataConfig
    from almanak.framework.backtesting.pnl.data_provider import MarketState
    from almanak.framework.backtesting.pnl.portfolio import (
        SimulatedFill,
        SimulatedPortfolio,
        SimulatedPosition,
    )
    from almanak.framework.intents.vocabulary import Intent

logger = logging.getLogger(__name__)


# Wildcard chain key for injected funding providers (chain-agnostic seam).
_ANY_CHAIN = "*"


@dataclass
class PerpBacktestConfig(StrategyBacktestConfig):
    """Configuration for perp-specific backtesting.

    This config extends the base StrategyBacktestConfig with perp-specific
    options for controlling funding payments, liquidation model, and margin
    requirements.

    Attributes:
        strategy_type: Must be "perp" for perp adapter (inherited)
        fee_tracking_enabled: Whether to track trading fees (inherited)
        position_tracking_enabled: Whether to track positions in detail (inherited)
        reconcile_on_tick: Whether to reconcile position state each tick (inherited)
        extra_params: Additional parameters (inherited)
        funding_application_frequency: How often to apply funding payments:
            - "continuous": Apply funding on every update (most accurate)
            - "hourly": Apply funding every hour (standard for most perp protocols)
            - "8h": Apply funding every 8 hours (Binance-style)
        liquidation_model_enabled: Whether to simulate liquidations when margin
            is insufficient. When True, positions are force-closed if the current
            price crosses the liquidation price.
        initial_margin_ratio: Required initial margin ratio for opening positions.
            Default 0.1 (10%) = 10x max leverage.
        maintenance_margin_ratio: Maintenance margin for liquidation threshold.
            Default 0.05 (5%).
        default_funding_rate: Default hourly funding rate when not provided.
            Default 0.0001 (0.01% per hour).
        funding_rate_source: Source for funding rate data:
            - "fixed": Use default_funding_rate for all calculations
            - "historical": Use historical funding rates from data provider
            - "protocol": Use protocol-specific rates
        liquidation_warning_threshold: Distance from liquidation to emit warning.
            Default 0.10 (10%).
        liquidation_critical_threshold: Distance for critical warning.
            Default 0.05 (5%).
        protocol: Default protocol for margin/funding lookups (e.g., "gmx", "hyperliquid")

    Example:
        config = PerpBacktestConfig(
            strategy_type="perp",
            funding_application_frequency="hourly",
            liquidation_model_enabled=True,
            initial_margin_ratio=Decimal("0.05"),  # 20x max leverage
            default_funding_rate=Decimal("0.0001"),
        )
    """

    funding_application_frequency: Literal["continuous", "hourly", "8h"] = "hourly"
    """How often to apply funding payments."""

    liquidation_model_enabled: bool = True
    """Whether to simulate liquidations when margin insufficient."""

    initial_margin_ratio: Decimal = Decimal("0.1")
    """Required initial margin ratio (0.1 = 10% = 10x max leverage)."""

    maintenance_margin_ratio: Decimal = Decimal("0.05")
    """Maintenance margin for liquidation (0.05 = 5%)."""

    default_funding_rate: Decimal = Decimal("0.0001")
    """Default hourly funding rate (0.01% per hour)."""

    funding_rate_source: Literal["fixed", "historical", "protocol"] = "fixed"
    """Source for funding rate data."""

    liquidation_warning_threshold: Decimal = Decimal("0.10")
    """Distance from liquidation to emit warning (10%)."""

    liquidation_critical_threshold: Decimal = Decimal("0.05")
    """Distance for critical warning (5%)."""

    liquidation_penalty: Decimal = Decimal("0.05")
    """Liquidation penalty applied when position is liquidated (0.05 = 5%)."""

    protocol: str = "gmx"
    """Default protocol for margin/funding lookups."""

    chain: str = DEFAULT_CHAIN
    """Blockchain for funding rate lookups (arbitrum, avalanche)."""

    def __post_init__(self) -> None:
        """Validate perp-specific configuration.

        Raises:
            ValueError: If strategy_type is not "perp" or invalid parameters.
        """
        # Call parent validation
        super().__post_init__()

        # Validate strategy_type for perp
        if self.strategy_type.lower() != "perp":
            msg = f"PerpBacktestConfig requires strategy_type='perp', got '{self.strategy_type}'"
            raise ValueError(msg)

        # Validate funding_application_frequency
        valid_frequencies = {"continuous", "hourly", "8h"}
        if self.funding_application_frequency not in valid_frequencies:
            msg = f"funding_application_frequency must be one of {valid_frequencies}, got '{self.funding_application_frequency}'"
            raise ValueError(msg)

        # Validate funding_rate_source
        valid_sources = {"fixed", "historical", "protocol"}
        if self.funding_rate_source not in valid_sources:
            msg = f"funding_rate_source must be one of {valid_sources}, got '{self.funding_rate_source}'"
            raise ValueError(msg)

        # Validate margin ratios
        if self.initial_margin_ratio <= Decimal("0"):
            msg = f"initial_margin_ratio must be > 0, got {self.initial_margin_ratio}"
            raise ValueError(msg)
        if self.maintenance_margin_ratio <= Decimal("0"):
            msg = f"maintenance_margin_ratio must be > 0, got {self.maintenance_margin_ratio}"
            raise ValueError(msg)
        if self.maintenance_margin_ratio > self.initial_margin_ratio:
            msg = f"maintenance_margin_ratio ({self.maintenance_margin_ratio}) cannot exceed initial_margin_ratio ({self.initial_margin_ratio})"
            raise ValueError(msg)

    def to_dict(self) -> dict[str, Any]:
        """Serialize configuration to a dictionary.

        Returns:
            Dictionary representation of the configuration.
        """
        base = super().to_dict()
        base.update(
            {
                "funding_application_frequency": self.funding_application_frequency,
                "liquidation_model_enabled": self.liquidation_model_enabled,
                "initial_margin_ratio": str(self.initial_margin_ratio),
                "maintenance_margin_ratio": str(self.maintenance_margin_ratio),
                "default_funding_rate": str(self.default_funding_rate),
                "funding_rate_source": self.funding_rate_source,
                "liquidation_warning_threshold": str(self.liquidation_warning_threshold),
                "liquidation_critical_threshold": str(self.liquidation_critical_threshold),
                "liquidation_penalty": str(self.liquidation_penalty),
                "protocol": self.protocol,
                "chain": self.chain,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PerpBacktestConfig":
        """Create configuration from a dictionary.

        Args:
            data: Dictionary with configuration values.

        Returns:
            New PerpBacktestConfig instance.
        """
        return cls(
            strategy_type=data.get("strategy_type", "perp"),
            fee_tracking_enabled=data.get("fee_tracking_enabled", True),
            position_tracking_enabled=data.get("position_tracking_enabled", True),
            reconcile_on_tick=data.get("reconcile_on_tick", False),
            extra_params=data.get("extra_params", {}),
            strict_reproducibility=data.get("strict_reproducibility", False),
            funding_application_frequency=data.get("funding_application_frequency", "hourly"),
            liquidation_model_enabled=data.get("liquidation_model_enabled", True),
            initial_margin_ratio=Decimal(str(data.get("initial_margin_ratio", "0.1"))),
            maintenance_margin_ratio=Decimal(str(data.get("maintenance_margin_ratio", "0.05"))),
            default_funding_rate=Decimal(str(data.get("default_funding_rate", "0.0001"))),
            funding_rate_source=data.get("funding_rate_source", "fixed"),
            liquidation_warning_threshold=Decimal(str(data.get("liquidation_warning_threshold", "0.10"))),
            liquidation_critical_threshold=Decimal(str(data.get("liquidation_critical_threshold", "0.05"))),
            liquidation_penalty=Decimal(str(data.get("liquidation_penalty", "0.05"))),
            protocol=data.get("protocol", "gmx"),
            chain=data.get("chain", LEGACY_SERIALIZED_CHAIN),
        )


@dataclass(frozen=True)
class _FundingLookup:
    primary_token: str
    market: str
    timestamp: datetime | None


@dataclass(frozen=True)
class _PerpOpenParams:
    size_usd: Decimal
    collateral_amount: Any
    collateral_token: str
    leverage: Decimal
    is_long: bool
    protocol: str


@register_adapter(
    "perp",
    description="Adapter for perpetual futures strategies with funding, margin, and liquidation support",
    aliases=["perpetual", "futures", "gmx", "hyperliquid", "leverage"],
)
class PerpBacktestAdapter(StrategyBacktestAdapter):
    """Backtest adapter for Perp (Perpetual Futures) strategies.

    This adapter handles the simulation of perpetual futures positions during
    backtesting. It provides:

    - Funding rate payment application based on configurable frequency
    - Liquidation price calculation and monitoring
    - Margin validation for position opening and increases
    - Position valuation with unrealized PnL and accumulated funding
    - Historical funding rate integration via connector-declared providers

    The adapter can be used with or without explicit configuration.
    When used without config, it uses sensible defaults.

    When BacktestDataConfig is provided, the adapter uses connector-declared
    providers to fetch historical funding rates for supported venues.

    Attributes:
        config: Perp-specific configuration (optional)
        data_config: BacktestDataConfig for historical data provider settings (optional)

    Example:
        # With config and data_config
        from almanak.framework.backtesting.config import BacktestDataConfig

        config = PerpBacktestConfig(
            strategy_type="perp",
            liquidation_model_enabled=True,
        )
        data_config = BacktestDataConfig(
            use_historical_funding=True,
            funding_fallback_rate=Decimal("0.0001"),
        )
        adapter = PerpBacktestAdapter(config, data_config=data_config)

        # Without config (uses defaults)
        adapter = PerpBacktestAdapter()

        # In backtesting loop
        adapter.update_position(position, market_state, elapsed_seconds)

        # Value position
        value = adapter.value_position(position, market_state)

        # Check if rebalance needed (e.g., approaching liquidation)
        if adapter.should_rebalance(position, market_state):
            # Strategy should consider adjusting position
            pass
    """

    def __init__(
        self,
        config: PerpBacktestConfig | None = None,
        data_config: "BacktestDataConfig | None" = None,
        gmx_provider: HistoricalFundingProvider | None = None,
        hyperliquid_provider: HistoricalFundingProvider | None = None,
        injected_providers: Mapping[str, HistoricalFundingProvider] | None = None,
    ) -> None:
        """Initialize the perp backtest adapter.

        Args:
            config: Perp-specific configuration. If None, uses default
                PerpBacktestConfig with strategy_type="perp".
            data_config: BacktestDataConfig for controlling historical data provider
                behavior. When provided, overrides config.funding_rate_source behavior
                with data_config.use_historical_funding setting.
            gmx_provider: Optional legacy injection for the GMX-compatible provider.
                If None and historical funding is enabled, the registry creates one lazily.
            hyperliquid_provider: Optional legacy injection for the Hyperliquid provider.
                If None and historical funding is enabled, the registry creates one lazily.
            injected_providers: Optional provider mapping keyed by accepted protocol identifier.
        """
        self._config = config or PerpBacktestConfig(strategy_type="perp")
        self._data_config = data_config

        # Initialize calculators
        self._funding_calculator = FundingCalculator(
            funding_rate_source=(
                FundingRateSource.FIXED
                if self._config.funding_rate_source == "fixed"
                else FundingRateSource.HISTORICAL
                if self._config.funding_rate_source == "historical"
                else FundingRateSource.PROTOCOL
            ),
            default_funding_rate=self._config.default_funding_rate,
        )
        self._liquidation_calculator = LiquidationCalculator(
            default_maintenance_margin=self._config.maintenance_margin_ratio,
            warning_threshold=self._config.liquidation_warning_threshold,
            critical_threshold=self._config.liquidation_critical_threshold,
        )
        self._margin_validator = MarginValidator(
            default_initial_margin_ratio=self._config.initial_margin_ratio,
            default_maintenance_margin_ratio=self._config.maintenance_margin_ratio,
        )

        # Track last funding application time for frequency control
        self._last_funding_time: dict[str, datetime] = {}

        # Historical funding providers, keyed by connector registry canonical key.
        self._provider_cache: dict[tuple[str, str], HistoricalFundingProvider | None] = {}
        self._provider_tried: set[tuple[str, str]] = set()
        self._seed_injected_provider("gmx", gmx_provider)
        self._seed_injected_provider("hyperliquid", hyperliquid_provider)
        self._seed_injected_providers(injected_providers or {})

        # Cache for funding rate data to avoid repeated queries
        # Key: (protocol, market, timestamp_hour) -> (rate, confidence, source)
        self._funding_cache: dict[tuple[str, str, datetime], tuple[Decimal, str, str]] = {}

        # Legacy funding rate provider for backward compatibility
        self._funding_rate_provider: FundingRateProvider | None = None
        if self._config.funding_rate_source == "historical" and not self._use_historical_funding():
            # Only use legacy provider if data_config doesn't override
            self._funding_rate_provider = FundingRateProvider(
                chain=self._config.chain,
            )
            logger.info(
                "Initialized legacy FundingRateProvider for historical rates (chain=%s, protocol=%s)",
                self._config.chain,
                self._config.protocol,
            )

    def _seed_injected_provider(self, protocol: str, provider: HistoricalFundingProvider | None) -> None:
        """Seed one injected provider into the connector-keyed provider cache.

        ``protocol`` accepts an explicit ``"protocol:chain"`` scope (e.g.
        ``"gmx:arbitrum"``); a bare protocol falls back to the provider's
        public ``chain`` attribute (the HistoricalFundingProvider contract),
        and chainless providers serve as the wildcard fallback. An explicit
        scope that CONTRADICTS the provider's declared chain is rejected —
        serving one chain's data under another chain's key silently corrupts
        every funding number downstream. Scoping a chain-agnostic provider
        explicitly stays legal.
        """
        if provider is None:
            return
        protocol_key, scope_separator, explicit_chain = protocol.partition(":")
        if scope_separator and not explicit_chain.strip():
            # "gmx_v2:" is a malformed explicit scope, not a wildcard request —
            # silently broadening a typo to every supported chain is exactly
            # the cross-chain hazard the scoping syntax exists to prevent.
            logger.warning(
                "Rejecting injected funding provider %r: explicit chain scope is blank "
                "(use a bare protocol key for wildcard seeding)",
                protocol,
            )
            return
        canonical = FundingHistoryRegistry.canonical(protocol_key)
        if canonical is None:
            logger.debug("Ignoring injected funding provider for unknown protocol '%s'", protocol_key)
            return
        # Canonicalize BOTH sides before comparing/keying: "avax" and
        # "avalanche" are the same chain, and a raw-string mismatch here
        # rejects correct injections (the lookup then builds a wrong-chain
        # fallback under the alias key).
        public_chain = self._canonical_chain(provider.chain)
        if explicit_chain:
            chain_key = self._canonical_chain(explicit_chain) or explicit_chain.strip().lower()
            if public_chain is not None and public_chain != chain_key:
                logger.warning(
                    "Rejecting injected funding provider for %r: explicit scope %r contradicts "
                    "the provider's declared chain %r",
                    protocol_key,
                    chain_key,
                    public_chain,
                )
                return
        else:
            chain_key = public_chain if public_chain is not None else _ANY_CHAIN
        # Declared-chain contract: a connector that declares funding chains
        # serves ONLY those chains — an injection scoped (explicitly or via
        # its public chain) to an undeclared chain is rejected, never cached.
        # An EMPTY declared set is the explicit chain-agnostic contract
        # (Hyperliquid). The wildcard seam stays; lookups gate it below.
        declared = frozenset(
            resolved
            for resolved in (self._canonical_chain(c) for c in FundingHistoryRegistry.declared_chains(canonical) or ())
            if resolved is not None
        )
        if chain_key != _ANY_CHAIN and declared and chain_key not in declared:
            logger.warning(
                "Rejecting injected funding provider for %r: chain %r is not in the connector's "
                "declared funding chains %s",
                protocol_key,
                chain_key,
                sorted(declared),
            )
            return
        key = (canonical, chain_key)
        self._provider_cache[key] = provider
        self._provider_tried.add(key)

    def _seed_injected_providers(self, providers: Mapping[str, HistoricalFundingProvider | None]) -> None:
        """Seed the generic provider cache from test/operator injections."""
        for protocol, provider in providers.items():
            self._seed_injected_provider(protocol, provider)

    @property
    def adapter_name(self) -> str:
        """Return the unique name of this adapter.

        Returns:
            Strategy type identifier "perp"
        """
        return "perp"

    @property
    def config(self) -> PerpBacktestConfig:
        """Get the adapter configuration.

        Returns:
            Perp backtest configuration
        """
        return self._config

    def _use_historical_funding(self) -> bool:
        """Check if historical funding rate data should be used.

        Uses BacktestDataConfig.use_historical_funding if data_config provided,
        otherwise checks if config.funding_rate_source is 'historical'.

        Returns:
            True if historical funding rates should be fetched from provider APIs.
        """
        if self._data_config is not None:
            return self._data_config.use_historical_funding
        return self._config.funding_rate_source == "historical"

    def _get_funding_fallback_rate(self) -> Decimal:
        """Get the funding fallback rate to use.

        Uses BacktestDataConfig.funding_fallback_rate if data_config provided,
        otherwise falls back to PerpBacktestConfig.default_funding_rate.

        Returns:
            Funding rate for fallback when historical data unavailable.
        """
        if self._data_config is not None:
            return self._data_config.funding_fallback_rate
        return self._config.default_funding_rate

    def _is_strict_historical_mode(self) -> bool:
        """Check if strict historical mode is enabled.

        When strict mode is enabled, the adapter will raise HistoricalDataUnavailableError
        instead of using fallback values when historical data is unavailable.

        Returns:
            True if strict historical mode is enabled via BacktestDataConfig.
        """
        if self._data_config is not None:
            return self._data_config.strict_historical_mode
        return False

    async def prewarm_history(
        self,
        intent: Any,
        chain: str,
        start_time: datetime,
        end_time: datetime,
    ) -> None:
        """Pre-fetch the backtest window's hourly funding into ``_funding_cache``.

        Per-tick accrual fetches funding one hour at a time through a worker
        thread (~1.2s each — minutes of pure fetch latency on an hourly-tick
        backtest even with a healthy gateway). One whole-window fetch here (a
        legal await point, right after the PERP_OPEN fill) makes every
        subsequent tick a cache hit. Best-effort: on failure the per-tick
        semantics (fetch → fallback rate) are unchanged.
        """
        protocol = str(getattr(intent, "protocol", "") or "").lower()
        raw_market = getattr(intent, "market", None)
        if not protocol or not isinstance(raw_market, str) or not raw_market:
            return
        provider = self._get_provider_for_protocol(protocol, chain)
        if provider is None:
            return
        # Cache market key must match _funding_lookup's "<BASE>-USD". Both
        # sides normalize through the SAME token_ref_provider_symbol call
        # (which unwraps address-resolved wrapped natives) — key parity is
        # the contract; a divergent key is a guaranteed cache miss that
        # silently defeats the prewarm.
        base = raw_market.replace("/", "-").split("-")[0].strip().upper()
        base = token_ref_provider_symbol(base, chain, unwrap_wrapped_native=True).upper()
        market = f"{base}-USD"
        try:
            rates = await provider.get_funding_rates(market=market, start_date=start_time, end_date=end_time)
        except Exception as exc:  # noqa: BLE001 — best-effort prewarm
            logger.warning("Funding history prewarm failed for %s %s: %s", protocol, market, exc)
            return
        warmed = 0
        for rate in rates or []:
            info = rate.source_info
            if info.source == "fallback":
                # A degraded prewarm must not freeze fallback rates into the
                # cache — the per-tick lane can still fetch measured data.
                continue
            hour = self._normalize_timestamp_to_hour(info.timestamp)
            confidence = info.confidence.value if hasattr(info.confidence, "value") else str(info.confidence)
            self._funding_cache[(protocol, market, hour)] = (rate.rate, confidence, f"historical:{info.source}")
            warmed += 1
        logger.info("Prewarmed %d hours of funding history for %s %s", warmed, protocol, market)

    @staticmethod
    def _canonical_chain(value: Any) -> str | None:
        """Canonical ChainRegistry name for ``value``, or None when unset.

        Chain identity must never be raw-string compared: registered aliases
        ("avax") and canonical names ("avalanche") are the same chain, and a
        raw comparison both rejects correct injections and caches providers
        under alias keys that never match their canonical duplicates.
        Unregistered strings fall back to lowercase (fail-soft: an unknown
        chain still gets a stable key).
        """
        if not isinstance(value, str) or not value:
            return None
        from almanak.core.chains import ChainRegistry

        descriptor = ChainRegistry.try_resolve(value)
        return descriptor.name.lower() if descriptor is not None else value.strip().lower()

    def _provider_chain(self, chain: str | None) -> str:
        """Chain a funding provider should be built for (run chain, not default)."""
        return self._canonical_chain(chain) or self._canonical_chain(self._config.chain) or DEFAULT_CHAIN

    def _get_provider_for_protocol(self, protocol: str, chain: str | None = None) -> HistoricalFundingProvider | None:
        """Resolve a historical funding provider through connector declarations."""
        if not self._use_historical_funding():
            return None

        canonical = FundingHistoryRegistry.canonical(protocol)
        if canonical is None:
            logger.debug("No historical funding provider for protocol '%s', will use fallback", protocol)
            return None
        provider_chain = self._provider_chain(chain)
        key = (canonical, provider_chain)
        # Declared-chain contract, enforced on EVERY return path: a connector
        # that declares funding chains never serves an undeclared one — not
        # from the exact cache, not via the wildcard seam, and never by
        # constructing a provider whose factory would silently fall back to
        # its default chain. Empty declared set = chain-agnostic (Hyperliquid).
        # Canonicalize the DECLARED side too: chain identity is never
        # raw-string compared (the R5 lesson) — a connector declaring the
        # registered alias "avax" must match the canonical "avalanche" run
        # chain, not silently fall back on a correctly-aliased manifest.
        declared = frozenset(
            resolved
            for resolved in (self._canonical_chain(c) for c in FundingHistoryRegistry.declared_chains(canonical) or ())
            if resolved is not None
        )
        if declared and provider_chain not in declared:
            if key not in self._provider_tried:
                self._provider_tried.add(key)
                self._provider_cache[key] = None
                logger.warning(
                    "No funding history for %s on undeclared chain %r (declared: %s); using fallback rate.",
                    canonical,
                    provider_chain,
                    sorted(declared),
                )
            return None
        # Exact (protocol, chain) entries always win; the injected wildcard
        # only serves chains with no exact entry, so a chain-agnostic test
        # injection can never shadow a real chain-keyed provider.
        if key in self._provider_tried:
            return self._provider_cache.get(key)
        wildcard = (canonical, _ANY_CHAIN)
        if wildcard in self._provider_tried:
            return self._provider_cache.get(wildcard)

        self._provider_tried.add(key)
        provider_cls = cast(
            type[HistoricalFundingProvider] | None,
            FundingHistoryRegistry.backtest_provider(canonical),
        )
        if provider_cls is None:
            self._provider_cache[key] = None
            logger.debug("No historical funding provider declared for protocol '%s'", canonical)
            return None

        try:
            provider = provider_cls.for_backtest(
                BacktestProviderConfig(
                    funding_fallback_rate=self._get_funding_fallback_rate(),
                    chain=provider_chain,
                )
            )
            # Verify BEFORE caching: connector factories fall back to their
            # default chain for unsupported requests (GMX -> arbitrum), and
            # caching that mismatch serves another chain's funding history
            # for the rest of the run. For declared-chain protocols a MISSING
            # built chain is rejected too — a chain-scoped provider that does
            # not publish its chain can default internally to another chain
            # with no way to detect it. None stays legal only for explicitly
            # chain-agnostic venues (empty declared set, e.g. Hyperliquid).
            built_chain = self._canonical_chain(provider.chain)
            built_invalid = (
                (built_chain != provider_chain)
                if declared
                else (built_chain is not None and built_chain != provider_chain)
            )
            if built_invalid:
                logger.warning(
                    "Funding provider for %s was built for chain %r, not the requested %r; "
                    "using fallback rates instead of cross-chain history.",
                    canonical,
                    built_chain,
                    provider_chain,
                )
                self._provider_cache[key] = None
                return None
            self._provider_cache[key] = provider
            logger.debug(
                "Initialized historical funding provider for %s: chain=%s, fallback_rate=%s",
                canonical,
                provider_chain,
                self._get_funding_fallback_rate(),
            )
            return provider
        except Exception as e:
            logger.warning("Failed to initialize funding provider for %s: %s. Will use fallback rate.", canonical, e)
            self._provider_cache[key] = None
            return None

    def _normalize_timestamp_to_hour(self, timestamp: datetime) -> datetime:
        """Normalize timestamp to hourly boundary for caching.

        Args:
            timestamp: The timestamp to normalize

        Returns:
            Timestamp rounded down to the hour
        """
        return timestamp.replace(minute=0, second=0, microsecond=0)

    def execute_intent(
        self,
        intent: "Intent",
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> "SimulatedFill | None":
        """Simulate execution of a perp-related intent.

        This method handles PERP_OPEN and PERP_CLOSE intents. For PERP_OPEN,
        it validates margin requirements before allowing the position to be opened.

        For PERP_OPEN:
        - Validates that collateral meets initial margin requirements
        - Checks available capital in portfolio
        - Rejects if margin utilization would exceed max threshold
        - Calculates and sets the liquidation price on the position

        For PERP_CLOSE:
        - Validates the position exists and matches the intent
        - Calculates final PnL including funding

        Args:
            intent: The intent to execute (PerpOpenIntent, PerpCloseIntent)
            portfolio: Current portfolio state
            market_state: Current market prices and data

        Returns:
            SimulatedFill if margin validation fails (with success=False),
            or None to use default execution logic if validation passes.
        """
        from almanak.framework.intents.vocabulary import PerpCloseIntent, PerpOpenIntent

        # Handle PERP_OPEN intent with margin validation
        if isinstance(intent, PerpOpenIntent):
            return self._execute_perp_open(intent, portfolio, market_state)

        # Handle PERP_CLOSE intent
        if isinstance(intent, PerpCloseIntent):
            return self._execute_perp_close(intent, portfolio, market_state)

        # Not a perp intent, let default execution handle it
        return None

    def _execute_perp_open(
        self,
        intent: "Intent",
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> "SimulatedFill | None":
        """Execute a PERP_OPEN intent with margin validation.

        Args:
            intent: PerpOpenIntent to execute
            portfolio: Current portfolio state
            market_state: Current market prices and data

        Returns:
            SimulatedFill with success=False if margin validation fails,
            or None to proceed with default execution.
        """
        from almanak.framework.intents.vocabulary import PerpOpenIntent

        # Type narrowing for mypy
        if not isinstance(intent, PerpOpenIntent):
            return None

        params = self._perp_open_params(intent)

        if params.collateral_amount == "all":
            # Sizing has one owner: the shared resolver rejects perp
            # collateral "all" (no wallet-sizing lane yet) with a typed code.
            from almanak.framework.backtesting.models import IntentType
            from almanak.framework.backtesting.pnl.sizing import SizingRejection, resolve_all_sizing

            resolution = resolve_all_sizing(intent, IntentType.PERP_OPEN, portfolio, market_state)
            reason = resolution.detail if isinstance(resolution, SizingRejection) else "unsupported collateral sizing"
            return self._perp_margin_failure_fill(
                params,
                market_state,
                collateral_usd=Decimal("0"),
                required_margin_ratio=self._config.initial_margin_ratio,
                reason=reason,
                validation_type="sizing",
            )

        collateral_usd = self._perp_collateral_usd(params, market_state)
        required_margin_ratio = self._perp_required_margin_ratio(params.leverage)

        can_open, reason = self._margin_validator.can_open_position(
            position_size=params.size_usd,
            collateral=collateral_usd,
            # cash_usd alone is 0 for token-funded portfolios (platform
            # token_funding path) — stables held as tokens ARE the margin
            # capital, exactly as apply_fill debits them.
            available_capital=portfolio.cash_like_available(),
            current_margin_used=self._get_current_margin_used(portfolio),
            margin_ratio=required_margin_ratio,
        )

        if not can_open:
            return self._perp_margin_failure_fill(
                params,
                market_state,
                collateral_usd,
                required_margin_ratio,
                reason,
            )

        self._log_perp_open_success(intent, market_state, params, collateral_usd, required_margin_ratio)
        return None

    @staticmethod
    def _perp_open_params(intent: Any) -> _PerpOpenParams:
        return _PerpOpenParams(
            size_usd=Decimal(str(intent.size_usd)),
            collateral_amount=intent.collateral_amount,
            collateral_token=intent.collateral_token,
            leverage=Decimal(str(intent.leverage)),
            is_long=intent.is_long,
            protocol=intent.protocol,
        )

    def _perp_collateral_usd(
        self,
        params: _PerpOpenParams,
        market_state: "MarketState",
    ) -> Decimal:
        collateral_price = self._perp_collateral_price(params, market_state)
        return Decimal(str(params.collateral_amount)) * collateral_price

    def _perp_collateral_price(
        self,
        params: _PerpOpenParams,
        market_state: "MarketState",
    ) -> Decimal:
        try:
            collateral_price = market_state.get_price(params.collateral_token)
        except KeyError:
            collateral_price = None

        if collateral_price is not None and collateral_price > Decimal("0"):
            return collateral_price
        if self._config.strict_reproducibility:
            raise HistoricalDataUnavailableError(
                data_type="price",
                identifier=params.collateral_token,
                timestamp=getattr(market_state, "timestamp", datetime.now()),
                message=f"Price unavailable for perp collateral token {params.collateral_token}",
                chain=self._config.chain,
                protocol=params.protocol,
            )
        return Decimal("1")

    def _perp_required_margin_ratio(self, leverage: Decimal) -> Decimal:
        """The venue's initial-margin floor — the only economic constraint here.

        The intent's declared ``leverage`` is sizing metadata already encoded in
        ``(size_usd, collateral_amount)``. The original engine derived the
        required ratio as ``1/leverage`` (PR #143), which re-validates the
        user's own declaration against market-repriced collateral — rejecting
        every self-consistent intent by epsilon whenever the collateral stable
        isn't exactly $1. An over-leveraged intent is
        still rejected: its actual collateral/size ratio falls below the venue
        floor regardless of what it declared.
        """
        del leverage  # sizing metadata, not a constraint
        return self._config.initial_margin_ratio

    def _perp_margin_failure_fill(
        self,
        params: _PerpOpenParams,
        market_state: "MarketState",
        collateral_usd: Decimal,
        required_margin_ratio: Decimal,
        reason: str,
        validation_type: str = "margin",
    ) -> "SimulatedFill":
        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl.portfolio import SimulatedFill

        logger.warning(
            "%s validation failed for PERP_OPEN: size=%s, collateral=%s, reason=%s",
            validation_type.capitalize(),
            params.size_usd,
            collateral_usd,
            reason,
        )
        return SimulatedFill(
            timestamp=market_state.timestamp,
            intent_type=IntentType.PERP_OPEN,
            protocol=params.protocol,
            tokens=[params.collateral_token],
            executed_price=Decimal("0"),
            amount_usd=params.size_usd,
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={},
            tokens_out=self._perp_failed_tokens_out(),
            success=False,
            metadata={
                "failure_reason": reason,
                "validation_type": validation_type,
                "required_margin_ratio": str(required_margin_ratio),
                "collateral_usd": str(collateral_usd),
                "attempted_collateral_amount": str(params.collateral_amount),
                "attempted_collateral_token": params.collateral_token,
                "leverage": str(params.leverage),
            },
        )

    @staticmethod
    def _perp_failed_tokens_out() -> dict[TokenRef, Decimal]:
        return {}

    def _log_perp_open_success(
        self,
        intent: Any,
        market_state: "MarketState",
        params: _PerpOpenParams,
        collateral_usd: Decimal,
        required_margin_ratio: Decimal,
    ) -> None:
        try:
            market_token = intent.market.split("/")[0]  # e.g., "ETH/USD" -> "ETH"
            entry_price = market_state.get_price(market_token)
        except (KeyError, IndexError):
            entry_price = Decimal("0")

        if entry_price and entry_price > Decimal("0"):
            liq_price = self.get_liquidation_price(
                entry_price=entry_price,
                leverage=self._perp_effective_leverage(params.leverage, required_margin_ratio),
                is_long=params.is_long,
            )
            logger.info(
                "PERP_OPEN margin validated: size=%s, collateral=%s, leverage=%.1fx, entry=%.2f, liq_price=%.2f",
                params.size_usd,
                collateral_usd,
                float(self._perp_effective_leverage(params.leverage, required_margin_ratio)),
                float(entry_price),
                float(liq_price),
            )

    @staticmethod
    def _perp_effective_leverage(leverage: Decimal, required_margin_ratio: Decimal) -> Decimal:
        if leverage > Decimal("1"):
            return leverage
        return Decimal("1") / required_margin_ratio

    def _execute_perp_close(
        self,
        intent: "Intent",
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> "SimulatedFill | None":
        """Execute a PERP_CLOSE intent.

        Args:
            intent: PerpCloseIntent to execute
            portfolio: Current portfolio state
            market_state: Current market prices and data

        Returns:
            None to proceed with default execution (validation only).
        """
        from almanak.framework.intents.vocabulary import PerpCloseIntent

        # Type narrowing for mypy
        if not isinstance(intent, PerpCloseIntent):
            return None

        # Log the close intent details
        logger.debug(
            "PERP_CLOSE intent: market=%s, is_long=%s, size=%s",
            intent.market,
            intent.is_long,
            intent.size_usd,
        )

        # Let default execution handle the close
        # The update_position and value_position methods handle PnL calculation
        return None

    def _get_current_margin_used(self, portfolio: "SimulatedPortfolio") -> Decimal:
        """Calculate total margin currently used by perp positions.

        Args:
            portfolio: Current portfolio state

        Returns:
            Total collateral locked in perp positions
        """
        total_margin = Decimal("0")
        for position in portfolio.positions:
            if position.is_perp:
                total_margin += position.collateral_usd
        return total_margin

    def update_position(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        elapsed_seconds: float,
        timestamp: datetime | None = None,
    ) -> None:
        """Update perp position state based on time passage and price changes.

        This method handles perp-specific position updates:
        - Applies funding payments based on configured frequency
        - Updates liquidation price if parameters changed
        - Checks liquidation proximity and logs warnings
        - Updates unrealized PnL

        The funding application frequency is controlled by config:
        - "continuous": Apply funding on every call
        - "hourly": Apply funding once per hour
        - "8h": Apply funding every 8 hours

        Args:
            position: The perp position to update (modified in-place)
            market_state: Current market prices and data
            elapsed_seconds: Time elapsed since last update in seconds
            timestamp: Simulation timestamp for deterministic updates. If None,
                uses market_state.timestamp for reproducible backtests.

        Note:
            This method only updates perp positions (PERP_LONG or PERP_SHORT).
            Non-perp positions are ignored.

            For liquidation detection, call check_and_simulate_liquidation()
            separately. This method does not trigger liquidation, only emits
            proximity warnings.
        """
        if not self._should_update_perp_position(position, elapsed_seconds):
            return

        primary_token = self._perp_primary_token(position)
        funding_timestamp = self._resolve_perp_timestamp(position, market_state, timestamp, "funding")
        current_price = self._perp_token_price(
            position,
            market_state,
            primary_token,
            funding_timestamp,
            "perp position update",
        )
        funding_chain = str(getattr(market_state, "chain", self._config.chain))
        self._apply_funding_if_due(position, elapsed_seconds, funding_timestamp, funding_chain)

        self._liquidation_calculator.update_position_liquidation_price(position, self._config.maintenance_margin_ratio)
        self._check_perp_liquidation_proximity(position, current_price)

        position.last_updated = self._resolve_perp_timestamp(position, market_state, timestamp, "")
        self._log_perp_position_update(position, current_price)

    @staticmethod
    def _should_update_perp_position(position: "SimulatedPosition", elapsed_seconds: float) -> bool:
        if position.position_type not in (PositionType.PERP_LONG, PositionType.PERP_SHORT):
            return False
        if position.is_liquidated:
            return False
        return elapsed_seconds > 0

    @staticmethod
    def _perp_primary_token(position: "SimulatedPosition") -> TokenRef:
        return position.tokens[0] if position.tokens else "ETH"

    def _resolve_perp_timestamp(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        timestamp: datetime | None,
        purpose: str,
    ) -> datetime:
        if timestamp is not None:
            return timestamp
        if hasattr(market_state, "timestamp") and market_state.timestamp is not None:
            return market_state.timestamp
        if self._config.strict_reproducibility:
            purpose_suffix = f" {purpose}" if purpose else ""
            msg = (
                f"No simulation timestamp available for perp position {position.position_id}{purpose_suffix}. "
                "In strict reproducibility mode, timestamp must be provided. "
                "Either pass timestamp parameter or ensure market_state.timestamp is set."
            )
            raise ValueError(msg)
        warning_suffix = f" {purpose}" if purpose else ""
        logger.warning(
            "No simulation timestamp available for perp position %s%s, "
            "falling back to datetime.now(). This breaks backtest reproducibility.",
            position.position_id,
            warning_suffix,
        )
        return datetime.now()

    def _perp_token_price(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        primary_token: TokenRef,
        timestamp: datetime,
        context: str,
    ) -> Decimal:
        primary_label = token_ref_display(primary_token)
        try:
            current_price = market_state.get_price(primary_token)
        except KeyError:
            current_price = None

        if current_price is not None and current_price > 0:
            return current_price

        if self._config.strict_reproducibility:
            raise HistoricalDataUnavailableError(
                data_type="price",
                identifier=primary_label,
                timestamp=timestamp,
                message=f"Price unavailable for {primary_label} in {context}",
                chain=self._config.chain,
                protocol=position.protocol,
            )

        return position.entry_price

    def _check_perp_liquidation_proximity(self, position: "SimulatedPosition", current_price: Decimal) -> None:
        if not self._config.liquidation_model_enabled:
            return
        self._liquidation_calculator.check_liquidation_proximity(
            position,
            current_price,
            warning_threshold=self._config.liquidation_warning_threshold,
            critical_threshold=self._config.liquidation_critical_threshold,
            emit_warning=True,
        )

    @staticmethod
    def _log_perp_position_update(position: "SimulatedPosition", current_price: Decimal) -> None:
        logger.debug(
            "Perp position update: position=%s, type=%s, price=%.2f, funding=%.4f, liq_price=%s",
            position.position_id,
            position.position_type.value,
            float(current_price),
            float(position.accumulated_funding),
            str(position.liquidation_price) if position.liquidation_price else "N/A",
        )

    def _apply_funding_if_due(
        self,
        position: "SimulatedPosition",
        elapsed_seconds: float,
        timestamp: datetime | None = None,
        chain: str | None = None,
    ) -> None:
        """Apply funding payments if due based on configured frequency.

        Note: Current implementation applies cumulative funding for the elapsed
        period, which is appropriate for PnL backtesting where we care about
        total accumulated funding over the position lifetime. The frequency
        config controls the minimum interval between funding applications but
        the funding amount is always calculated for the full elapsed period.
        Granular per-tick funding calculation may be added in a future iteration.

        Args:
            position: The perp position (modified in-place)
            elapsed_seconds: Time elapsed since last update in seconds
            timestamp: Simulation timestamp for tracking. If None, uses datetime.now()
                but this is discouraged for reproducibility.
        """
        # Convert elapsed seconds to hours
        elapsed_hours = Decimal(str(elapsed_seconds)) / Decimal("3600")

        # Determine funding application threshold based on frequency
        if self._config.funding_application_frequency == "continuous":
            # Apply on every update
            min_hours = Decimal("0")
        elif self._config.funding_application_frequency == "8h":
            # Apply every 8 hours
            min_hours = Decimal("8")
        else:
            # Default: hourly
            min_hours = Decimal("1")

        # Track cumulative time for this position
        position_id = position.position_id
        if position_id not in self._last_funding_time:
            # First update - initialize and apply funding
            # Use simulation timestamp for reproducibility
            if timestamp is not None:
                current_time = timestamp
            else:
                if self._config.strict_reproducibility:
                    msg = (
                        f"No simulation timestamp for perp position {position_id} first funding. "
                        "In strict reproducibility mode, timestamp must be provided."
                    )
                    raise ValueError(msg)
                logger.warning(
                    "No simulation timestamp for perp position %s first funding, "
                    "falling back to datetime.now(). This breaks backtest reproducibility.",
                    position_id,
                )
                current_time = datetime.now()
            self._last_funding_time[position_id] = current_time
            # Apply funding for the elapsed time
            self._apply_funding_payment(position, elapsed_hours, timestamp, chain)
            return

        # For continuous mode, always apply
        if min_hours == Decimal("0"):
            self._apply_funding_payment(position, elapsed_hours, timestamp, chain)
            return

        # For hourly/8h mode, accumulate and apply when threshold reached
        # Note: In a real implementation, we'd track actual time since last funding
        # For simplicity, we apply proportionally based on elapsed time
        if elapsed_hours > Decimal("0"):
            self._apply_funding_payment(position, elapsed_hours, timestamp, chain)

    def _apply_funding_payment(
        self,
        position: "SimulatedPosition",
        time_hours: Decimal,
        timestamp: datetime | None = None,
        chain: str | None = None,
    ) -> None:
        """Apply funding payment to position.

        Args:
            position: The perp position (modified in-place)
            time_hours: Time period in hours
            timestamp: Simulation timestamp for historical rate lookups
        """
        if time_hours <= Decimal("0"):
            return

        # Get funding rate based on source
        funding_rate: Decimal
        rate_source: str
        confidence: str

        if self._use_historical_funding():
            # Query historical funding rate from new providers (GMX or Hyperliquid)
            funding_rate, confidence, rate_source = self._get_historical_funding_rate_v2(
                position=position,
                timestamp=timestamp,
                chain=chain,
            )
        elif self._config.funding_rate_source == "historical" and self._funding_rate_provider is not None:
            # Legacy provider path for backward compatibility
            funding_rate, rate_source = self._get_historical_funding_rate(
                position=position,
                timestamp=timestamp,
                chain=chain,
            )
            confidence = "medium"  # Legacy provider confidence
        elif self._config.funding_rate_source == "protocol":
            funding_rate = self._funding_calculator.get_funding_rate_for_protocol(position.protocol)
            rate_source = f"protocol:{position.protocol}"
            confidence = "medium"
        else:
            funding_rate = self._config.default_funding_rate
            rate_source = "fixed"
            confidence = "low"

        # Calculate funding payment
        result = self._funding_calculator.calculate_funding_payment(
            position=position,
            funding_rate=funding_rate,
            time_delta_hours=time_hours,
        )

        # Apply to position
        self._funding_calculator.apply_funding_to_position(position, result)

        # Update position funding confidence and data source
        position.funding_confidence = confidence
        position.funding_data_source = rate_source

        logger.debug(
            "Applied funding: position=%s, payment=%.4f, hours=%.2f, rate=%.6f, source=%s, confidence=%s",
            position.position_id,
            float(result.payment),
            float(time_hours),
            float(funding_rate),
            rate_source,
            confidence,
        )

    def _get_historical_funding_rate_v2(
        self,
        position: "SimulatedPosition",
        timestamp: datetime | None = None,
        chain: str | None = None,
    ) -> tuple[Decimal, str, str]:
        """Get historical funding rate from GMX or Hyperliquid providers.

        Routes to the appropriate provider based on the position's protocol.
        Returns funding_confidence and data_source for tracking.

        Args:
            position: The perp position
            timestamp: Timestamp to query funding rate for

        Returns:
            Tuple of (funding_rate, confidence, source_description)
            confidence is 'high' for API data, 'medium' for current rate, 'low' for fallback

        Raises:
            HistoricalDataUnavailableError: If strict_historical_mode is True and
                historical funding rate data cannot be fetched.
        """
        lookup = self._funding_lookup(position, timestamp, chain)
        if lookup.timestamp is None:
            return self._funding_no_timestamp_result(position, lookup)

        provider = self._get_provider_for_protocol(position.protocol, chain)
        if provider is None:
            return self._funding_provider_unavailable_result(position, lookup)

        cache_key = self._funding_cache_key(position, lookup)
        cached = self._cached_funding_rate(position, lookup, cache_key)
        if cached is not None:
            return cached

        try:
            rates = self._fetch_historical_funding_rates(provider, lookup)
            if not rates:
                return self._funding_no_data_result(position, lookup)
            return self._cache_latest_funding_rate(position, lookup, cache_key, rates)
        except HistoricalDataUnavailableError:
            raise
        except Exception as e:
            return self._funding_fetch_error_result(position, lookup, e)

    def _funding_lookup(
        self,
        position: "SimulatedPosition",
        timestamp: datetime | None,
        chain: str | None = None,
    ) -> _FundingLookup:
        primary_token = token_ref_provider_symbol(
            self._perp_primary_token(position),
            chain,
            unwrap_wrapped_native=True,
        )
        return _FundingLookup(
            primary_token=primary_token,
            market=f"{primary_token}-USD",
            timestamp=timestamp,
        )

    def _funding_no_timestamp_result(
        self,
        position: "SimulatedPosition",
        lookup: _FundingLookup,
    ) -> tuple[Decimal, str, str]:
        if self._is_strict_historical_mode():
            raise HistoricalDataUnavailableError(
                data_type="funding",
                identifier=lookup.market,
                timestamp=datetime.now(),
                message="No timestamp provided for historical funding rate lookup",
                chain=self._config.chain,
                protocol=position.protocol,
            )
        default_rate = self._get_funding_fallback_rate()
        logger.warning(
            "Using fallback funding rate for %s %s: %.6f (no timestamp provided)",
            position.protocol,
            lookup.primary_token,
            float(default_rate),
        )
        return default_rate, "low", "fallback:no_timestamp"

    def _funding_provider_unavailable_result(
        self,
        position: "SimulatedPosition",
        lookup: _FundingLookup,
    ) -> tuple[Decimal, str, str]:
        assert lookup.timestamp is not None
        if self._is_strict_historical_mode():
            raise HistoricalDataUnavailableError(
                data_type="funding",
                identifier=lookup.market,
                timestamp=lookup.timestamp,
                message=f"No funding rate provider available for protocol '{position.protocol}'",
                chain=self._config.chain,
                protocol=position.protocol,
            )
        default_rate = self._get_funding_fallback_rate()
        logger.warning(
            "Using fallback funding rate for %s %s: %.6f (no provider for protocol)",
            position.protocol,
            lookup.primary_token,
            float(default_rate),
        )
        return default_rate, "low", f"fallback:unsupported_protocol:{position.protocol}"

    def _funding_cache_key(self, position: "SimulatedPosition", lookup: _FundingLookup) -> tuple[str, str, datetime]:
        assert lookup.timestamp is not None
        return (position.protocol.lower(), lookup.market, self._normalize_timestamp_to_hour(lookup.timestamp))

    def _cached_funding_rate(
        self,
        position: "SimulatedPosition",
        lookup: _FundingLookup,
        cache_key: tuple[str, str, datetime],
    ) -> tuple[Decimal, str, str] | None:
        cached = self._funding_cache.get(cache_key)
        if cached is None:
            return None
        assert lookup.timestamp is not None
        logger.debug(
            "Using cached funding rate for %s %s at %s: %.6f",
            position.protocol,
            lookup.market,
            lookup.timestamp.isoformat(),
            float(cached[0]),
        )
        return cached

    def _fetch_historical_funding_rates(
        self,
        provider: Any,
        lookup: _FundingLookup,
    ) -> list[Any]:
        """Fetch the ``[T - 1h, T]`` funding window, bridging out of a running loop.

        ``update_position`` runs inside the engine's async iteration task, and
        a running event loop cannot be re-entered with ``asyncio.run`` on its
        own thread — so the query runs in a dedicated worker thread instead
        (the same bridge ``MarketSnapshot._run_async_bridged`` uses for the
        strategy-facing funding lane). The old behaviour skipped the fetch in
        that case (``fallback:async_context``), which silently broke the
        funding-lane coherence contract: decide() gated on measured history
        while the open position accrued the fallback rate (PR #3153 review).
        """
        assert lookup.timestamp is not None
        start_time = lookup.timestamp - timedelta(hours=1)
        if self._event_loop_is_running():
            return self._run_funding_query_in_thread(provider, lookup.market, start_time, lookup.timestamp)
        return asyncio.run(
            provider.get_funding_rates(
                market=lookup.market,
                start_date=start_time,
                end_date=lookup.timestamp,
            )
        )

    @staticmethod
    def _event_loop_is_running() -> bool:
        try:
            asyncio.get_running_loop()
            return True
        except RuntimeError:
            return False

    @staticmethod
    def _run_funding_query_in_thread(
        provider: Any,
        market: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[Any]:
        import concurrent.futures

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        try:
            future = executor.submit(
                asyncio.run,
                provider.get_funding_rates(
                    market=market,
                    start_date=start_time,
                    end_date=end_time,
                ),
            )
            return future.result(timeout=30)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    def _funding_no_data_result(
        self,
        position: "SimulatedPosition",
        lookup: _FundingLookup,
    ) -> tuple[Decimal, str, str]:
        assert lookup.timestamp is not None
        if self._is_strict_historical_mode():
            raise HistoricalDataUnavailableError(
                data_type="funding",
                identifier=lookup.market,
                timestamp=lookup.timestamp,
                message="No historical funding rate data returned from provider API",
                chain=self._config.chain,
                protocol=position.protocol,
            )
        default_rate = self._get_funding_fallback_rate()
        logger.warning(
            "No funding data returned for %s %s at %s, using fallback rate %.6f",
            position.protocol,
            lookup.market,
            lookup.timestamp.isoformat(),
            float(default_rate),
        )
        return default_rate, "low", "fallback:no_data"

    def _cache_latest_funding_rate(
        self,
        position: "SimulatedPosition",
        lookup: _FundingLookup,
        cache_key: tuple[str, str, datetime],
        rates: list[Any],
    ) -> tuple[Decimal, str, str]:
        assert lookup.timestamp is not None
        latest_rate = max(rates, key=lambda rate: rate.source_info.timestamp)
        confidence_enum = latest_rate.source_info.confidence
        confidence = confidence_enum.value if hasattr(confidence_enum, "value") else "medium"
        source = latest_rate.source_info.source
        result = (latest_rate.rate, confidence, f"historical:{source}")
        self._funding_cache[cache_key] = result

        logger.info(
            "Historical funding rate for %s %s at %s: %.6f (source=%s, confidence=%s)",
            position.protocol,
            lookup.market,
            lookup.timestamp.isoformat(),
            float(latest_rate.rate),
            source,
            confidence,
        )
        return result

    def _funding_fetch_error_result(
        self,
        position: "SimulatedPosition",
        lookup: _FundingLookup,
        error: Exception,
    ) -> tuple[Decimal, str, str]:
        assert lookup.timestamp is not None
        if self._is_strict_historical_mode():
            raise HistoricalDataUnavailableError(
                data_type="funding",
                identifier=lookup.market,
                timestamp=lookup.timestamp,
                message=f"Failed to fetch historical funding rate: {error}",
                chain=self._config.chain,
                protocol=position.protocol,
            ) from error
        default_rate = self._get_funding_fallback_rate()
        logger.warning(
            "Failed to fetch historical funding rate for %s %s at %s, using fallback rate %.6f: %s",
            position.protocol,
            lookup.market,
            lookup.timestamp.isoformat(),
            float(default_rate),
            str(error),
        )
        return default_rate, "low", "fallback:error"

    def _get_historical_funding_rate(
        self,
        position: "SimulatedPosition",
        timestamp: datetime | None = None,
        chain: str | None = None,
    ) -> tuple[Decimal, str]:
        """Get historical funding rate from legacy provider with fallback to default.

        This method is preserved for backward compatibility with existing code.
        New code should use _get_historical_funding_rate_v2 which also returns
        confidence level.

        Args:
            position: The perp position
            timestamp: Timestamp to query funding rate for

        Returns:
            Tuple of (funding_rate, source_description)
        """
        if self._funding_rate_provider is None or timestamp is None:
            # Fallback to default rate
            default_rate = (
                DEFAULT_FUNDING_RATE
                if FundingHistoryRegistry.has(position.protocol)
                else self._config.default_funding_rate
            )
            logger.debug(
                "Using default funding rate for %s %s: %.6f (no provider or timestamp)",
                position.protocol,
                position.tokens[0] if position.tokens else "UNKNOWN",
                float(default_rate),
            )
            return default_rate, "fallback:no_timestamp"

        # Build market identifier (e.g., "ETH-USD")
        primary_token = token_ref_provider_symbol(
            position.tokens[0] if position.tokens else "ETH",
            chain,
            unwrap_wrapped_native=True,
        )
        market = f"{primary_token}-USD"

        try:
            # Run async query synchronously
            # Check if we're in an async context
            try:
                asyncio.get_running_loop()
                # We're in an async context, use thread pool
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        asyncio.run,
                        self._funding_rate_provider.get_historical_funding_rate(
                            protocol=position.protocol,
                            market=market,
                            timestamp=timestamp,
                        ),
                    )
                    rate_data = future.result(timeout=30)
            except RuntimeError:
                # No running event loop, safe to use asyncio.run
                rate_data = asyncio.run(
                    self._funding_rate_provider.get_historical_funding_rate(
                        protocol=position.protocol,
                        market=market,
                        timestamp=timestamp,
                    )
                )

            logger.info(
                "Historical funding rate for %s %s at %s: %.6f (source=%s, annualized=%.2f%%)",
                position.protocol,
                market,
                timestamp.isoformat(),
                float(rate_data.rate),
                rate_data.source,
                float(rate_data.annualized_rate_pct),
            )
            return rate_data.rate, f"historical:{rate_data.source}"

        except Exception as e:
            # Fallback to default rate on any error
            default_rate = (
                DEFAULT_FUNDING_RATE
                if FundingHistoryRegistry.has(position.protocol)
                else self._config.default_funding_rate
            )
            logger.warning(
                "Failed to fetch historical funding rate for %s %s at %s, using default rate %.6f: %s",
                position.protocol,
                market,
                timestamp.isoformat() if timestamp else "N/A",
                float(default_rate),
                str(e),
            )
            return default_rate, "fallback:error"

    def value_position(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        timestamp: datetime | None = None,
    ) -> Decimal:
        """Calculate the current USD value of a perp position.

        This method computes perp position value as:
            value = collateral + unrealized_pnl + accumulated_funding

        Where unrealized PnL is:
            LONG: (current_price - entry_price) / entry_price * notional
            SHORT: (entry_price - current_price) / entry_price * notional

        The accumulated_funding can be positive (received) or negative (paid).

        Args:
            position: The perp position to value
            market_state: Current market prices and data
            timestamp: Simulation timestamp for deterministic valuation. If None,
                uses market_state.timestamp. Currently unused in perp valuation
                but accepted for interface consistency.

        Returns:
            Total position value in USD as a Decimal (collateral + PnL + funding)
        """
        # Note: timestamp parameter accepted for interface consistency
        # Perp valuation is based on current market prices, not time-dependent
        price_timestamp = self._perp_price_timestamp(position, market_state, timestamp)
        primary_token = self._perp_primary_token(position)
        current_price = self._perp_token_price(
            position,
            market_state,
            primary_token,
            price_timestamp,
            "perp position valuation",
        )

        # Get collateral (initial margin)
        collateral = position.collateral_usd

        # Calculate unrealized PnL
        unrealized_pnl = self._calculate_unrealized_pnl(position, current_price)

        # Get accumulated funding (can be positive or negative)
        accumulated_funding = position.accumulated_funding

        # Total value = collateral + unrealized PnL + funding
        total_value = collateral + unrealized_pnl + accumulated_funding

        logger.debug(
            "Perp position value: position=%s, collateral=%.2f, unrealized_pnl=%.2f, funding=%.2f, total=%.2f",
            position.position_id,
            float(collateral),
            float(unrealized_pnl),
            float(accumulated_funding),
            float(total_value),
        )

        return total_value

    @staticmethod
    def _perp_price_timestamp(
        position: "SimulatedPosition",
        market_state: "MarketState",
        timestamp: datetime | None,
    ) -> datetime:
        if timestamp is not None:
            return timestamp
        if hasattr(market_state, "timestamp") and market_state.timestamp is not None:
            return market_state.timestamp
        if position.last_updated is not None:
            return position.last_updated
        return position.entry_time

    def _calculate_unrealized_pnl(
        self,
        position: "SimulatedPosition",
        current_price: Decimal,
    ) -> Decimal:
        """Calculate unrealized PnL for a perp position.

        Args:
            position: The perp position
            current_price: Current market price

        Returns:
            Unrealized PnL in USD (positive = profit, negative = loss)
        """
        entry_price = position.entry_price
        notional = position.notional_usd

        if entry_price <= Decimal("0"):
            return Decimal("0")

        # Calculate price change percentage
        price_change_pct = (current_price - entry_price) / entry_price

        # Apply direction
        if position.position_type == PositionType.PERP_LONG:
            # Long profits when price increases
            unrealized_pnl = notional * price_change_pct
        else:
            # Short profits when price decreases
            unrealized_pnl = -notional * price_change_pct

        return unrealized_pnl

    def should_rebalance(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
    ) -> bool:
        """Determine if a perp position should be rebalanced.

        For perp positions, rebalancing is triggered when:
        - Position is approaching liquidation price (within warning threshold)
        - Margin utilization is too high

        The method checks liquidation proximity and returns True if the
        position is within the configured warning threshold of liquidation.

        Args:
            position: The perp position to check
            market_state: Current market prices and data

        Returns:
            True if the position should be rebalanced (reduce size or add margin),
            False otherwise

        Note:
            This method does NOT trigger on liquidation itself - only when
            approaching liquidation, giving the strategy a chance to adjust.
        """
        # Only process perp positions
        if position.position_type not in (PositionType.PERP_LONG, PositionType.PERP_SHORT):
            return False

        # Get primary token for price lookup
        primary_token = position.tokens[0] if position.tokens else "ETH"

        # Get current price
        try:
            current_price = market_state.get_price(primary_token)
        except KeyError:
            return False

        if current_price is None or current_price <= Decimal("0"):
            return False

        # Check if approaching liquidation
        warning = self._liquidation_calculator.check_liquidation_proximity(
            position,
            current_price,
            warning_threshold=self._config.liquidation_warning_threshold,
            critical_threshold=self._config.liquidation_critical_threshold,
            emit_warning=False,  # Don't log here, just check
        )

        if warning is not None:
            logger.info(
                "Perp position %s should rebalance: %s from liquidation (price=%.2f, liq=%.2f)",
                position.position_id,
                f"{warning.distance_pct * 100:.1f}%",
                float(current_price),
                float(warning.liquidation_price),
            )
            return True

        return False

    def validate_margin(
        self,
        position_size: Decimal,
        collateral: Decimal,
    ) -> tuple[bool, str]:
        """Validate margin requirements for a position.

        Args:
            position_size: Notional size of the position in USD
            collateral: Collateral amount in USD

        Returns:
            Tuple of (is_valid, message)
        """
        result = self._margin_validator.validate_margin(
            position_size=position_size,
            collateral=collateral,
            margin_ratio=self._config.initial_margin_ratio,
        )
        return result.is_valid, result.message

    def get_liquidation_price(
        self,
        entry_price: Decimal,
        leverage: Decimal,
        is_long: bool,
    ) -> Decimal:
        """Calculate liquidation price for given parameters.

        Args:
            entry_price: Entry price for the position
            leverage: Leverage multiplier
            is_long: True for long position, False for short

        Returns:
            Liquidation price
        """
        return self._liquidation_calculator.calculate_liquidation_price(
            entry_price=entry_price,
            leverage=leverage,
            maintenance_margin=self._config.maintenance_margin_ratio,
            is_long=is_long,
        )

    def check_and_simulate_liquidation(
        self,
        position: "SimulatedPosition",
        current_price: Decimal,
        timestamp: datetime,
    ) -> LiquidationEvent | None:
        """Check if a position should be liquidated and simulate the liquidation.

        This method checks if the current price has crossed the liquidation price
        for a perpetual position. If liquidation is triggered:
        - The position is marked as liquidated
        - A liquidation penalty is applied to the collateral
        - The remaining collateral is calculated
        - A LiquidationEvent is returned for recording

        Args:
            position: The perp position to check (modified in-place if liquidated)
            current_price: Current market price
            timestamp: Current simulation timestamp

        Returns:
            LiquidationEvent if liquidation occurred, None otherwise

        Note:
            This method only processes perp positions (PERP_LONG or PERP_SHORT).
            Non-perp positions or already liquidated positions are ignored.
        """
        # Only process perp positions
        if position.position_type not in (PositionType.PERP_LONG, PositionType.PERP_SHORT):
            return None

        # Skip if already liquidated
        if position.is_liquidated:
            return None

        # Skip if liquidation model is disabled
        if not self._config.liquidation_model_enabled:
            return None

        # Get or calculate liquidation price
        liq_price = position.liquidation_price
        if liq_price is None:
            liq_price = self.get_liquidation_price(
                entry_price=position.entry_price,
                leverage=position.leverage,
                is_long=position.position_type == PositionType.PERP_LONG,
            )
            position.liquidation_price = liq_price

        # Check if liquidation is triggered
        is_long = position.position_type == PositionType.PERP_LONG
        is_liquidated = False

        if is_long:
            # Long position: liquidated when price falls below liq_price
            is_liquidated = current_price <= liq_price
        else:
            # Short position: liquidated when price rises above liq_price
            is_liquidated = current_price >= liq_price

        if not is_liquidated:
            return None

        # Calculate loss at liquidation
        # For a long: loss = (entry - current) / entry * notional
        # For a short: loss = (current - entry) / entry * notional
        entry_price = position.entry_price
        notional = position.notional_usd

        if entry_price <= Decimal("0"):
            loss_usd = notional  # Full loss if no entry price
        else:
            if is_long:
                price_change_pct = (entry_price - current_price) / entry_price
            else:
                price_change_pct = (current_price - entry_price) / entry_price
            loss_usd = notional * price_change_pct

        # Apply liquidation penalty to remaining collateral
        remaining_collateral = position.collateral_usd - loss_usd
        penalty_amount = remaining_collateral * self._config.liquidation_penalty
        final_collateral = max(Decimal("0"), remaining_collateral - penalty_amount)

        # Calculate total loss (includes penalty)
        total_loss = position.collateral_usd - final_collateral + abs(position.accumulated_funding)

        # Mark position as liquidated
        position.is_liquidated = True
        position.collateral_usd = final_collateral
        position.last_updated = timestamp
        position.metadata["liquidation_timestamp"] = timestamp.isoformat()
        position.metadata["liquidation_price"] = str(current_price)
        position.metadata["liquidation_penalty"] = str(penalty_amount)
        position.metadata["remaining_collateral"] = str(final_collateral)

        # Create liquidation event
        event = LiquidationEvent(
            timestamp=timestamp,
            position_id=position.position_id,
            price=current_price,
            loss_usd=total_loss,
        )

        logger.warning(
            "LIQUIDATION: Position %s liquidated at price %.2f (liq_price=%.2f). "
            "Loss: $%.2f, Penalty: $%.2f, Remaining: $%.2f",
            position.position_id,
            float(current_price),
            float(liq_price),
            float(total_loss),
            float(penalty_amount),
            float(final_collateral),
        )

        return event

    def to_dict(self) -> dict[str, Any]:
        """Serialize the adapter configuration to a dictionary.

        Returns:
            Dictionary with adapter configuration
        """
        return {
            "adapter_name": self.adapter_name,
            "config": self._config.to_dict(),
        }


__all__ = [
    "PerpBacktestAdapter",
    "PerpBacktestConfig",
]
