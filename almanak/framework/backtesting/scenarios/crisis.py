"""Crisis scenario definitions for stress-testing strategies.

This module defines the CrisisScenario dataclass and pre-defined historical
crisis scenarios. These scenarios can be used to backtest strategies during
periods of extreme market stress.

Examples:
    >>> from almanak.framework.backtesting.scenarios import BLACK_THURSDAY
    >>> print(BLACK_THURSDAY.name)
    'black_thursday'
    >>> print(BLACK_THURSDAY.duration_days)
    7
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class CrisisScenario:
    """A historical crisis scenario for backtesting.

    This dataclass represents a period of significant market stress that
    can be used for stress-testing trading strategies.

    Attributes:
        name: Unique identifier for the scenario (lowercase, underscores)
        start_date: Beginning of the crisis period
        end_date: End of the crisis period
        description: Human-readable description of the crisis event

    Properties:
        duration_days: Number of days in the crisis period

    Example:
        >>> scenario = CrisisScenario(
        ...     name="custom_crisis",
        ...     start_date=datetime(2023, 3, 10),
        ...     end_date=datetime(2023, 3, 15),
        ...     description="SVB collapse",
        ... )
        >>> scenario.duration_days
        5
    """

    name: str
    start_date: datetime
    end_date: datetime
    description: str

    @property
    def duration_days(self) -> int:
        """Calculate the duration of the crisis in days."""
        delta = self.end_date - self.start_date
        return delta.days

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary.

        Returns:
            Dictionary with scenario data suitable for JSON serialization.
        """
        return {
            "name": self.name,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "description": self.description,
            "duration_days": self.duration_days,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CrisisScenario":
        """Deserialize from dictionary.

        Args:
            data: Dictionary with serialized CrisisScenario data

        Returns:
            CrisisScenario instance
        """
        return cls(
            name=data["name"],
            start_date=datetime.fromisoformat(data["start_date"]),
            end_date=datetime.fromisoformat(data["end_date"]),
            description=data["description"],
        )

    def __str__(self) -> str:
        """Human-readable string representation."""
        return (
            f"{self.name}: {self.start_date.strftime('%Y-%m-%d')} to "
            f"{self.end_date.strftime('%Y-%m-%d')} ({self.duration_days} days)"
        )


# =============================================================================
# Pre-defined Historical Crisis Scenarios
# =============================================================================

BLACK_THURSDAY = CrisisScenario(
    name="black_thursday",
    start_date=datetime(2020, 3, 12),
    end_date=datetime(2020, 3, 19),
    description=(
        "COVID-19 market crash ('Black Thursday'). On March 12, 2020, crypto markets "
        "experienced their largest single-day drop, with Bitcoin falling over 40% and "
        "Ethereum over 50%. This led to massive DeFi liquidations, with MakerDAO "
        "experiencing over $8M in undercollateralized loans. The crash was triggered "
        "by global pandemic fears and a liquidity crisis across all asset classes."
    ),
)
"""Black Thursday - March 2020 COVID crash.

Key characteristics:
- Bitcoin dropped from ~$8,000 to ~$3,800 (-52%) in 24 hours
- Ethereum dropped from ~$200 to ~$90 (-55%)
- DeFi protocols experienced massive liquidation cascades
- Gas prices spiked making liquidations economically unviable
- MakerDAO had $8.3M in undercollateralized debt from failed liquidations
"""

TERRA_COLLAPSE = CrisisScenario(
    name="terra_collapse",
    start_date=datetime(2022, 5, 7),
    end_date=datetime(2022, 5, 14),
    description=(
        "Terra/Luna collapse. The UST algorithmic stablecoin lost its dollar peg, "
        "triggering a death spiral that wiped out ~$60B in market value. LUNA "
        "crashed from ~$80 to near zero. This event caused widespread contagion "
        "across DeFi, affecting protocols like Anchor, and led to the failure of "
        "several crypto hedge funds including Three Arrows Capital."
    ),
)
"""Terra/Luna Collapse - May 2022 UST de-peg.

Key characteristics:
- UST de-pegged on May 9, falling from $1 to $0.30
- LUNA fell from ~$80 to effectively $0 within days
- $60B+ in market cap was wiped out
- Anchor Protocol yield collapsed from 20% to 0%
- Contagion spread to 3AC, Celsius, Voyager, BlockFi
"""

FTX_COLLAPSE = CrisisScenario(
    name="ftx_collapse",
    start_date=datetime(2022, 11, 6),
    end_date=datetime(2022, 11, 14),
    description=(
        "FTX exchange collapse. Following revelations about Alameda Research's "
        "balance sheet and a failed Binance acquisition, FTX halted withdrawals "
        "and filed for bankruptcy. Bitcoin fell from ~$21,000 to ~$15,500. "
        "The collapse exposed massive fraud and misappropriation of customer funds, "
        "leading to widespread loss of confidence in centralized exchanges."
    ),
)
"""FTX Collapse - November 2022 bankruptcy.

Key characteristics:
- Bitcoin dropped from ~$21,000 to ~$15,500 (-26%)
- FTT token collapsed from ~$22 to ~$1 (-95%)
- Solana dropped ~60% due to FTX/Alameda holdings
- $8B+ in customer funds were misappropriated
- Led to increased focus on proof-of-reserves and self-custody
"""


# =============================================================================
# Scenario Registry
# =============================================================================

PREDEFINED_SCENARIOS: dict[str, CrisisScenario] = {
    "black_thursday": BLACK_THURSDAY,
    "terra_collapse": TERRA_COLLAPSE,
    "ftx_collapse": FTX_COLLAPSE,
}
"""Registry of all predefined crisis scenarios by name."""


def get_scenario_by_name(name: str) -> CrisisScenario | None:
    """Look up a predefined scenario by name.

    Args:
        name: Scenario name (case-insensitive, underscores or hyphens)

    Returns:
        CrisisScenario if found, None otherwise

    Examples:
        >>> get_scenario_by_name("black_thursday")
        CrisisScenario(name='black_thursday', ...)
        >>> get_scenario_by_name("BLACK-THURSDAY")
        CrisisScenario(name='black_thursday', ...)
        >>> get_scenario_by_name("unknown")
        None
    """
    normalized = name.lower().replace("-", "_")
    return PREDEFINED_SCENARIOS.get(normalized)
