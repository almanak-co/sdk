"""MEV (Maximal Extractable Value) cost simulation for realistic backtest execution.

This module simulates MEV extraction costs that occur in real DeFi execution,
particularly sandwich attacks on vulnerable trades. It models:

1. Sandwich attack probability based on trade size and token characteristics
2. Additional slippage for MEV-vulnerable trades
3. Variable inclusion delay based on gas price competitiveness

Key Features:
    - Configurable sandwich attack probability model
    - Token-specific vulnerability profiles (stablecoins vs volatile assets)
    - Size-dependent MEV extraction rates
    - Gas price-based inclusion delay simulation

Example:
    from almanak.framework.backtesting.pnl.mev_simulator import (
        MEVSimulator,
        MEVSimulatorConfig,
        MEVSimulationResult,
    )

    config = MEVSimulatorConfig(
        base_sandwich_probability=Decimal("0.05"),  # 5% base probability
        max_mev_extraction_rate=Decimal("0.02"),    # 2% max extraction
    )
    simulator = MEVSimulator(config)

    result = simulator.simulate_mev_cost(
        trade_amount_usd=Decimal("50000"),
        token_in="WETH",
        token_out="USDC",
        gas_price_gwei=Decimal("30"),
    )

    if result.is_sandwiched:
        print(f"MEV cost: ${result.mev_cost_usd}")
        print(f"Additional slippage: {result.additional_slippage_pct * 100:.2f}%")
"""

import logging
import random
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from almanak.framework.backtesting.models import IntentType

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Default configuration values
DEFAULT_BASE_SANDWICH_PROBABILITY = Decimal("0.05")  # 5% base probability
DEFAULT_MAX_SANDWICH_PROBABILITY = Decimal("0.80")  # 80% max probability
DEFAULT_MAX_MEV_EXTRACTION_RATE = Decimal("0.02")  # 2% max extraction rate
DEFAULT_MIN_TRADE_SIZE_FOR_MEV = Decimal("1000")  # $1,000 minimum for MEV
DEFAULT_LARGE_TRADE_THRESHOLD = Decimal("100000")  # $100,000 "large trade"
DEFAULT_BASE_INCLUSION_DELAY = 1  # 1 block base delay
DEFAULT_MAX_INCLUSION_DELAY = 5  # 5 blocks max delay
DEFAULT_HIGH_GAS_THRESHOLD = Decimal("50")  # 50 gwei is "high"
DEFAULT_LOW_GAS_THRESHOLD = Decimal("15")  # 15 gwei is "low"

# Token vulnerability classifications
# Volatile tokens are more attractive targets for MEV
HIGH_VOLATILITY_TOKENS = frozenset(
    {
        "WETH",
        "ETH",
        "WBTC",
        "BTC",
        "LINK",
        "UNI",
        "AAVE",
        "CRV",
        "SNX",
        "SUSHI",
        "MKR",
        "COMP",
        "YFI",
        "BAL",
        "GMX",
        "ARB",
        "OP",
        "MATIC",
        "SOL",
        "AVAX",
        "FTM",
        "ATOM",
        "DOT",
        "ADA",
        "XRP",
        "LTC",
        "DOGE",
        "PEPE",
        "SHIB",
        "APE",
        "BLUR",
    }
)

# Stablecoins have lower MEV risk (tighter spreads, less volatility)
STABLECOIN_TOKENS = frozenset(
    {
        "USDC",
        "USDT",
        "DAI",
        "FRAX",
        "LUSD",
        "BUSD",
        "TUSD",
        "USDP",
        "GUSD",
        "USDD",
        "CRVUSD",
        "GHO",
        "PYUSD",
        "EURC",
        "EURT",
    }
)

# LST/LSD tokens - moderate MEV risk
LST_TOKENS = frozenset(
    {
        "STETH",
        "WSTETH",
        "RETH",
        "CBETH",
        "SFRXETH",
        "FRXETH",
        "ANKR",
        "LIDO",
        "ROCKET",
    }
)


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class MEVSimulatorConfig:
    """Configuration for MEV simulation.

    Attributes:
        base_sandwich_probability: Base probability of sandwich attack (default 5%)
        max_sandwich_probability: Maximum sandwich probability for large trades (default 80%)
        max_mev_extraction_rate: Maximum MEV extraction as fraction of trade (default 2%)
        min_trade_size_for_mev: Minimum trade size in USD to consider for MEV (default $1,000)
        large_trade_threshold: Trade size in USD considered "large" (default $100,000)
        base_inclusion_delay: Base block delay for transaction inclusion (default 1)
        max_inclusion_delay: Maximum block delay for low gas transactions (default 5)
        high_gas_threshold_gwei: Gas price above which inclusion is fast (default 50)
        low_gas_threshold_gwei: Gas price below which inclusion is slow (default 15)
        random_seed: Optional seed for reproducible simulations (default None)
    """

    base_sandwich_probability: Decimal = DEFAULT_BASE_SANDWICH_PROBABILITY
    max_sandwich_probability: Decimal = DEFAULT_MAX_SANDWICH_PROBABILITY
    max_mev_extraction_rate: Decimal = DEFAULT_MAX_MEV_EXTRACTION_RATE
    min_trade_size_for_mev: Decimal = DEFAULT_MIN_TRADE_SIZE_FOR_MEV
    large_trade_threshold: Decimal = DEFAULT_LARGE_TRADE_THRESHOLD
    base_inclusion_delay: int = DEFAULT_BASE_INCLUSION_DELAY
    max_inclusion_delay: int = DEFAULT_MAX_INCLUSION_DELAY
    high_gas_threshold_gwei: Decimal = DEFAULT_HIGH_GAS_THRESHOLD
    low_gas_threshold_gwei: Decimal = DEFAULT_LOW_GAS_THRESHOLD
    random_seed: int | None = None

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if not (Decimal("0") <= self.base_sandwich_probability <= Decimal("1")):
            raise ValueError("base_sandwich_probability must be between 0 and 1")
        if not (Decimal("0") <= self.max_sandwich_probability <= Decimal("1")):
            raise ValueError("max_sandwich_probability must be between 0 and 1")
        if self.base_sandwich_probability > self.max_sandwich_probability:
            raise ValueError("base_sandwich_probability must be <= max_sandwich_probability")
        if not (Decimal("0") <= self.max_mev_extraction_rate <= Decimal("0.5")):
            raise ValueError("max_mev_extraction_rate must be between 0 and 0.5")
        if self.min_trade_size_for_mev < Decimal("0"):
            raise ValueError("min_trade_size_for_mev must be non-negative")
        if self.large_trade_threshold < self.min_trade_size_for_mev:
            raise ValueError("large_trade_threshold must be >= min_trade_size_for_mev")
        if self.base_inclusion_delay < 0:
            raise ValueError("base_inclusion_delay must be non-negative")
        if self.max_inclusion_delay < self.base_inclusion_delay:
            raise ValueError("max_inclusion_delay must be >= base_inclusion_delay")
        if self.low_gas_threshold_gwei < Decimal("0"):
            raise ValueError("low_gas_threshold_gwei must be non-negative")
        if self.high_gas_threshold_gwei < self.low_gas_threshold_gwei:
            raise ValueError("high_gas_threshold_gwei must be >= low_gas_threshold_gwei")

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "base_sandwich_probability": str(self.base_sandwich_probability),
            "max_sandwich_probability": str(self.max_sandwich_probability),
            "max_mev_extraction_rate": str(self.max_mev_extraction_rate),
            "min_trade_size_for_mev": str(self.min_trade_size_for_mev),
            "large_trade_threshold": str(self.large_trade_threshold),
            "base_inclusion_delay": self.base_inclusion_delay,
            "max_inclusion_delay": self.max_inclusion_delay,
            "high_gas_threshold_gwei": str(self.high_gas_threshold_gwei),
            "low_gas_threshold_gwei": str(self.low_gas_threshold_gwei),
            "random_seed": self.random_seed,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MEVSimulatorConfig":
        """Deserialize from dictionary."""
        return cls(
            base_sandwich_probability=Decimal(
                data.get("base_sandwich_probability", str(DEFAULT_BASE_SANDWICH_PROBABILITY))
            ),
            max_sandwich_probability=Decimal(
                data.get("max_sandwich_probability", str(DEFAULT_MAX_SANDWICH_PROBABILITY))
            ),
            max_mev_extraction_rate=Decimal(data.get("max_mev_extraction_rate", str(DEFAULT_MAX_MEV_EXTRACTION_RATE))),
            min_trade_size_for_mev=Decimal(data.get("min_trade_size_for_mev", str(DEFAULT_MIN_TRADE_SIZE_FOR_MEV))),
            large_trade_threshold=Decimal(data.get("large_trade_threshold", str(DEFAULT_LARGE_TRADE_THRESHOLD))),
            base_inclusion_delay=data.get("base_inclusion_delay", DEFAULT_BASE_INCLUSION_DELAY),
            max_inclusion_delay=data.get("max_inclusion_delay", DEFAULT_MAX_INCLUSION_DELAY),
            high_gas_threshold_gwei=Decimal(data.get("high_gas_threshold_gwei", str(DEFAULT_HIGH_GAS_THRESHOLD))),
            low_gas_threshold_gwei=Decimal(data.get("low_gas_threshold_gwei", str(DEFAULT_LOW_GAS_THRESHOLD))),
            random_seed=data.get("random_seed"),
        )


@dataclass
class MEVSimulationResult:
    """Result of MEV simulation for a trade.

    Attributes:
        is_sandwiched: Whether the trade was sandwiched by MEV bots
        mev_cost_usd: Total MEV extraction cost in USD
        additional_slippage_pct: Additional slippage caused by MEV (as decimal)
        inclusion_delay_blocks: Simulated block delay for transaction inclusion
        sandwich_probability: The calculated probability of being sandwiched
        token_vulnerability_factor: Vulnerability factor based on token pair (0-1)
        size_vulnerability_factor: Vulnerability factor based on trade size (0-1)
        details: Additional simulation details
    """

    is_sandwiched: bool
    mev_cost_usd: Decimal
    additional_slippage_pct: Decimal
    inclusion_delay_blocks: int
    sandwich_probability: Decimal
    token_vulnerability_factor: Decimal
    size_vulnerability_factor: Decimal
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "is_sandwiched": self.is_sandwiched,
            "mev_cost_usd": str(self.mev_cost_usd),
            "additional_slippage_pct": str(self.additional_slippage_pct),
            "additional_slippage_bps": float(self.additional_slippage_pct * Decimal("10000")),
            "inclusion_delay_blocks": self.inclusion_delay_blocks,
            "sandwich_probability": str(self.sandwich_probability),
            "sandwich_probability_pct": f"{self.sandwich_probability * 100:.2f}%",
            "token_vulnerability_factor": str(self.token_vulnerability_factor),
            "size_vulnerability_factor": str(self.size_vulnerability_factor),
            "details": self.details,
        }


# =============================================================================
# MEV Simulator
# =============================================================================


@dataclass
class MEVSimulator:
    """Simulator for MEV (Maximal Extractable Value) costs.

    This simulator models the costs that traders incur from MEV extraction,
    particularly sandwich attacks. It considers:

    1. **Trade Size**: Larger trades are more attractive targets for MEV bots
       because the profit potential is higher.

    2. **Token Characteristics**: Volatile tokens (ETH, WBTC) are more susceptible
       to sandwich attacks than stablecoins due to wider spreads and price volatility.

    3. **Gas Price**: Lower gas prices may result in longer inclusion delays,
       giving MEV bots more time to detect and front-run transactions.

    The simulation uses probabilistic modeling to determine:
    - Whether a trade gets sandwiched
    - How much MEV is extracted (as additional slippage)
    - How many blocks until the transaction is included

    Attributes:
        config: Configuration for MEV simulation parameters
        _rng: Random number generator (seeded if config.random_seed is set)

    Example:
        simulator = MEVSimulator()

        result = simulator.simulate_mev_cost(
            trade_amount_usd=Decimal("50000"),
            token_in="WETH",
            token_out="USDC",
            gas_price_gwei=Decimal("30"),
        )

        if result.is_sandwiched:
            print(f"Trade was sandwiched!")
            print(f"MEV cost: ${result.mev_cost_usd}")
    """

    config: MEVSimulatorConfig = field(default_factory=MEVSimulatorConfig)
    _rng: random.Random = field(default_factory=random.Random, repr=False)

    def __post_init__(self) -> None:
        """Initialize random number generator with seed if provided."""
        if self.config.random_seed is not None:
            self._rng = random.Random(self.config.random_seed)

    def simulate_mev_cost(
        self,
        trade_amount_usd: Decimal,
        token_in: str = "",
        token_out: str = "",
        gas_price_gwei: Decimal | None = None,
        intent_type: IntentType = IntentType.SWAP,
    ) -> MEVSimulationResult:
        """Simulate MEV costs for a trade.

        This method calculates:
        1. Token vulnerability factor based on the token pair
        2. Size vulnerability factor based on trade size
        3. Overall sandwich probability
        4. Whether the trade is sandwiched (probabilistic)
        5. MEV extraction cost if sandwiched
        6. Inclusion delay based on gas price

        Args:
            trade_amount_usd: Trade size in USD
            token_in: Input token symbol (e.g., "WETH")
            token_out: Output token symbol (e.g., "USDC")
            gas_price_gwei: Gas price in gwei (optional, affects inclusion delay)
            intent_type: Type of intent (only SWAP is MEV-vulnerable)

        Returns:
            MEVSimulationResult with simulation outcome
        """
        # Normalize token symbols
        token_in = token_in.upper() if token_in else ""
        token_out = token_out.upper() if token_out else ""

        # Check if intent type is MEV-vulnerable
        if not self._is_mev_vulnerable_intent(intent_type):
            return MEVSimulationResult(
                is_sandwiched=False,
                mev_cost_usd=Decimal("0"),
                additional_slippage_pct=Decimal("0"),
                inclusion_delay_blocks=self.config.base_inclusion_delay,
                sandwich_probability=Decimal("0"),
                token_vulnerability_factor=Decimal("0"),
                size_vulnerability_factor=Decimal("0"),
                details={"reason": f"Intent type {intent_type.value} is not MEV-vulnerable"},
            )

        # Check minimum trade size threshold
        if trade_amount_usd < self.config.min_trade_size_for_mev:
            return MEVSimulationResult(
                is_sandwiched=False,
                mev_cost_usd=Decimal("0"),
                additional_slippage_pct=Decimal("0"),
                inclusion_delay_blocks=self.config.base_inclusion_delay,
                sandwich_probability=Decimal("0"),
                token_vulnerability_factor=Decimal("0"),
                size_vulnerability_factor=Decimal("0"),
                details={
                    "reason": f"Trade size ${trade_amount_usd} below MEV threshold "
                    f"${self.config.min_trade_size_for_mev}"
                },
            )

        # Calculate vulnerability factors
        token_vulnerability = self._calculate_token_vulnerability(token_in, token_out)
        size_vulnerability = self._calculate_size_vulnerability(trade_amount_usd)

        # Calculate sandwich probability
        sandwich_probability = self._calculate_sandwich_probability(
            token_vulnerability=token_vulnerability,
            size_vulnerability=size_vulnerability,
        )

        # Simulate inclusion delay based on gas price
        inclusion_delay = self._calculate_inclusion_delay(gas_price_gwei)

        # Determine if trade is sandwiched (probabilistic)
        random_value = Decimal(str(self._rng.random()))
        is_sandwiched = random_value < sandwich_probability

        # Calculate MEV cost if sandwiched
        mev_cost_usd = Decimal("0")
        additional_slippage_pct = Decimal("0")

        if is_sandwiched:
            # MEV extraction rate scales with vulnerability factors
            extraction_rate = self._calculate_extraction_rate(
                token_vulnerability=token_vulnerability,
                size_vulnerability=size_vulnerability,
            )
            mev_cost_usd = trade_amount_usd * extraction_rate
            additional_slippage_pct = extraction_rate

            logger.debug(
                f"MEV simulation: Trade ${trade_amount_usd:.2f} {token_in}->{token_out} "
                f"sandwiched with {extraction_rate * 100:.2f}% extraction "
                f"(cost: ${mev_cost_usd:.2f})"
            )

        return MEVSimulationResult(
            is_sandwiched=is_sandwiched,
            mev_cost_usd=mev_cost_usd,
            additional_slippage_pct=additional_slippage_pct,
            inclusion_delay_blocks=inclusion_delay,
            sandwich_probability=sandwich_probability,
            token_vulnerability_factor=token_vulnerability,
            size_vulnerability_factor=size_vulnerability,
            details={
                "token_in": token_in,
                "token_out": token_out,
                "trade_amount_usd": str(trade_amount_usd),
                "gas_price_gwei": str(gas_price_gwei) if gas_price_gwei else None,
                "random_value": str(random_value),
            },
        )

    def _is_mev_vulnerable_intent(self, intent_type: IntentType) -> bool:
        """Check if an intent type is vulnerable to MEV extraction.

        Only swap-like operations that interact with DEX liquidity pools
        are vulnerable to sandwich attacks.

        Args:
            intent_type: The type of intent

        Returns:
            True if the intent is MEV-vulnerable
        """
        # Swaps are the primary MEV target
        # LP operations can be vulnerable but to a lesser extent
        return intent_type in (
            IntentType.SWAP,
            IntentType.LP_OPEN,
            IntentType.LP_CLOSE,
        )

    def _calculate_token_vulnerability(
        self,
        token_in: str,
        token_out: str,
    ) -> Decimal:
        """Calculate vulnerability factor based on token pair.

        Higher volatility tokens have higher vulnerability because:
        - Wider bid-ask spreads allow more MEV extraction
        - Price movements during sandwich are more profitable

        Stablecoin-to-stablecoin swaps have minimal vulnerability.

        Args:
            token_in: Input token symbol
            token_out: Output token symbol

        Returns:
            Vulnerability factor between 0 and 1
        """
        # Both stablecoins = very low vulnerability
        if token_in in STABLECOIN_TOKENS and token_out in STABLECOIN_TOKENS:
            return Decimal("0.1")

        # One stablecoin, one volatile = medium vulnerability
        if token_in in STABLECOIN_TOKENS or token_out in STABLECOIN_TOKENS:
            # Check if the non-stable is high volatility
            non_stable = token_out if token_in in STABLECOIN_TOKENS else token_in
            if non_stable in HIGH_VOLATILITY_TOKENS:
                return Decimal("0.7")
            if non_stable in LST_TOKENS:
                return Decimal("0.4")
            return Decimal("0.5")

        # Both LST tokens = low-medium vulnerability
        if token_in in LST_TOKENS and token_out in LST_TOKENS:
            return Decimal("0.3")

        # LST to volatile or vice versa = medium vulnerability
        if token_in in LST_TOKENS or token_out in LST_TOKENS:
            return Decimal("0.5")

        # Both high volatility = highest vulnerability
        if token_in in HIGH_VOLATILITY_TOKENS and token_out in HIGH_VOLATILITY_TOKENS:
            return Decimal("1.0")

        # One high volatility = high vulnerability
        if token_in in HIGH_VOLATILITY_TOKENS or token_out in HIGH_VOLATILITY_TOKENS:
            return Decimal("0.8")

        # Unknown tokens = assume medium-high vulnerability
        return Decimal("0.6")

    def _calculate_size_vulnerability(self, trade_amount_usd: Decimal) -> Decimal:
        """Calculate vulnerability factor based on trade size.

        Larger trades are more attractive to MEV bots because:
        - Higher absolute profit potential
        - More price impact to exploit

        Uses a logarithmic scale to model diminishing marginal vulnerability.

        Args:
            trade_amount_usd: Trade size in USD

        Returns:
            Vulnerability factor between 0 and 1
        """
        # Below threshold = no vulnerability
        if trade_amount_usd < self.config.min_trade_size_for_mev:
            return Decimal("0")

        # At or above large trade threshold = maximum vulnerability
        if trade_amount_usd >= self.config.large_trade_threshold:
            return Decimal("1.0")

        # Linear interpolation between min and large thresholds
        # Could use logarithmic scaling for more realism
        range_size = self.config.large_trade_threshold - self.config.min_trade_size_for_mev
        position_in_range = trade_amount_usd - self.config.min_trade_size_for_mev

        # Use square root for sublinear scaling (small trades get less attention)
        import math

        normalized = float(position_in_range / range_size)
        sqrt_normalized = Decimal(str(math.sqrt(normalized)))

        return sqrt_normalized

    def _calculate_sandwich_probability(
        self,
        token_vulnerability: Decimal,
        size_vulnerability: Decimal,
    ) -> Decimal:
        """Calculate the probability of being sandwiched.

        Combines token and size vulnerability factors to produce
        an overall sandwich probability between base and max.

        Args:
            token_vulnerability: Vulnerability from token pair (0-1)
            size_vulnerability: Vulnerability from trade size (0-1)

        Returns:
            Sandwich probability between base and max
        """
        # Combined vulnerability is the product (both factors must be present)
        # Use weighted combination: more weight on size since it's the profit driver
        combined_vulnerability = Decimal("0.3") * token_vulnerability + Decimal("0.7") * size_vulnerability

        # Scale to probability range
        probability_range = self.config.max_sandwich_probability - self.config.base_sandwich_probability
        probability = self.config.base_sandwich_probability + probability_range * combined_vulnerability

        return min(probability, self.config.max_sandwich_probability)

    def _calculate_extraction_rate(
        self,
        token_vulnerability: Decimal,
        size_vulnerability: Decimal,
    ) -> Decimal:
        """Calculate the MEV extraction rate for a sandwiched trade.

        The extraction rate represents the percentage of trade value
        captured by MEV bots through sandwich attacks.

        Higher vulnerability = more extraction possible.

        Args:
            token_vulnerability: Vulnerability from token pair (0-1)
            size_vulnerability: Vulnerability from trade size (0-1)

        Returns:
            Extraction rate as decimal (e.g., 0.01 = 1%)
        """
        # Base extraction scales with token vulnerability
        # (wider spreads on volatile tokens allow more extraction)
        base_extraction = self.config.max_mev_extraction_rate * token_vulnerability

        # Size factor adds more extraction for larger trades
        # (more room to move price)
        size_multiplier = Decimal("0.5") + Decimal("0.5") * size_vulnerability

        # Add some randomness to extraction (MEV bots compete)
        random_factor = Decimal(str(self._rng.uniform(0.5, 1.0)))

        extraction = base_extraction * size_multiplier * random_factor

        # Ensure we don't exceed max
        return min(extraction, self.config.max_mev_extraction_rate)

    def _calculate_inclusion_delay(
        self,
        gas_price_gwei: Decimal | None,
    ) -> int:
        """Calculate transaction inclusion delay based on gas price.

        Lower gas prices result in longer delays as transactions wait
        in the mempool, giving MEV bots more time to detect and front-run.

        Args:
            gas_price_gwei: Gas price in gwei (None = use base delay)

        Returns:
            Estimated block delay for transaction inclusion
        """
        if gas_price_gwei is None:
            return self.config.base_inclusion_delay

        # High gas = fast inclusion (base delay)
        if gas_price_gwei >= self.config.high_gas_threshold_gwei:
            return self.config.base_inclusion_delay

        # Low gas = slow inclusion (max delay)
        if gas_price_gwei <= self.config.low_gas_threshold_gwei:
            return self.config.max_inclusion_delay

        # Linear interpolation for middle range
        gas_range = self.config.high_gas_threshold_gwei - self.config.low_gas_threshold_gwei
        delay_range = self.config.max_inclusion_delay - self.config.base_inclusion_delay

        # Position in gas range (0 = low gas, 1 = high gas)
        gas_position = (gas_price_gwei - self.config.low_gas_threshold_gwei) / gas_range

        # Invert: low gas = high delay
        delay_position = Decimal("1") - gas_position

        additional_delay = int(delay_range * delay_position)
        return self.config.base_inclusion_delay + additional_delay

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "config": self.config.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MEVSimulator":
        """Deserialize from dictionary."""
        config_data = data.get("config", {})
        return cls(config=MEVSimulatorConfig.from_dict(config_data))


# =============================================================================
# Convenience Functions
# =============================================================================


def simulate_mev_cost(
    trade_amount_usd: Decimal,
    token_in: str = "",
    token_out: str = "",
    gas_price_gwei: Decimal | None = None,
    intent_type: IntentType = IntentType.SWAP,
    config: MEVSimulatorConfig | None = None,
) -> MEVSimulationResult:
    """Convenience function to simulate MEV cost with default configuration.

    This is a stateless version of MEVSimulator.simulate_mev_cost() for one-off simulations.

    Args:
        trade_amount_usd: Trade size in USD
        token_in: Input token symbol
        token_out: Output token symbol
        gas_price_gwei: Gas price in gwei (optional)
        intent_type: Type of intent
        config: Optional custom configuration (uses defaults if None)

    Returns:
        MEVSimulationResult with simulation outcome

    Example:
        result = simulate_mev_cost(
            trade_amount_usd=Decimal("50000"),
            token_in="WETH",
            token_out="USDC",
        )
    """
    if config is None:
        config = MEVSimulatorConfig()

    simulator = MEVSimulator(config=config)
    return simulator.simulate_mev_cost(
        trade_amount_usd=trade_amount_usd,
        token_in=token_in,
        token_out=token_out,
        gas_price_gwei=gas_price_gwei,
        intent_type=intent_type,
    )


def get_token_vulnerability(token_in: str, token_out: str) -> str:
    """Get a human-readable vulnerability classification for a token pair.

    Args:
        token_in: Input token symbol
        token_out: Output token symbol

    Returns:
        Vulnerability classification string: "low", "medium", "high", or "very_high"
    """
    simulator = MEVSimulator()
    vulnerability = simulator._calculate_token_vulnerability(token_in.upper(), token_out.upper())

    if vulnerability <= Decimal("0.25"):
        return "low"
    elif vulnerability <= Decimal("0.5"):
        return "medium"
    elif vulnerability <= Decimal("0.75"):
        return "high"
    else:
        return "very_high"


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Config and dataclasses
    "MEVSimulatorConfig",
    "MEVSimulationResult",
    # Main class
    "MEVSimulator",
    # Utility functions
    "simulate_mev_cost",
    "get_token_vulnerability",
    # Constants
    "HIGH_VOLATILITY_TOKENS",
    "STABLECOIN_TOKENS",
    "LST_TOKENS",
    "DEFAULT_BASE_SANDWICH_PROBABILITY",
    "DEFAULT_MAX_SANDWICH_PROBABILITY",
    "DEFAULT_MAX_MEV_EXTRACTION_RATE",
    "DEFAULT_MIN_TRADE_SIZE_FOR_MEV",
    "DEFAULT_LARGE_TRADE_THRESHOLD",
    "DEFAULT_BASE_INCLUSION_DELAY",
    "DEFAULT_MAX_INCLUSION_DELAY",
]
