"""Interest calculator for lending positions.

This module provides tools for calculating interest payments for lending protocol
positions during backtesting. Interest calculations support both compound and
simple interest models.

Key Concepts:
    - APY (Annual Percentage Yield): The annualized return accounting for compounding
    - APR (Annual Percentage Rate): The annualized return without compounding
    - Simple Interest: Principal * Rate * Time
    - Compound Interest: Principal * (1 + Rate/n)^(n*t) - Principal

How Interest Works:
    - SUPPLY positions earn interest over time (positive accrual)
    - BORROW positions pay interest over time (negative accrual/debt growth)
    - Interest is calculated based on principal, rate, and time elapsed

Interest Rate Sources:
    - FIXED: Use default/protocol-specific APYs (fastest, good for quick testing)
    - HISTORICAL: Fetch actual historical APYs from subgraphs (most accurate)
    - PROTOCOL: Use protocol-specific default rates

Example:
    from almanak.framework.backtesting.pnl.calculators.interest import (
        InterestCalculator,
        InterestResult,
    )

    calculator = InterestCalculator()

    # Calculate compound interest for 30 days at 5% APY
    result = calculator.calculate_interest(
        principal=Decimal("10000"),
        apy=Decimal("0.05"),
        time_delta_days=Decimal("30"),
        compound=True,
    )
    print(f"Interest earned: ${result.interest}")

References:
    - Aave V3 Interest: https://docs.aave.com/developers/guides/rates-guide
    - Compound III: https://docs.compound.finance/interest-rates/
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.backtesting.pnl.providers.lending_apy import LendingAPYProvider

logger = logging.getLogger(__name__)


class InterestRateSource(StrEnum):
    """Source for interest rate data.

    Attributes:
        FIXED: Use a fixed APY (default, good for backtesting)
        HISTORICAL: Use historical APY from data provider
        PROTOCOL: Use protocol-specific rates (requires API access)
        VARIABLE: Use variable rates that change over time
    """

    FIXED = "fixed"
    HISTORICAL = "historical"
    PROTOCOL = "protocol"
    VARIABLE = "variable"


@dataclass
class InterestResult:
    """Result of an interest calculation.

    Attributes:
        interest: The interest amount accrued (always positive)
        principal: The principal amount used
        apy: The APY used for calculation
        time_days: The time period in days
        compound: Whether compound interest was used
        effective_rate: The effective rate applied over the period
    """

    interest: Decimal
    principal: Decimal
    apy: Decimal
    time_days: Decimal
    compound: bool
    effective_rate: Decimal

    @property
    def final_balance(self) -> Decimal:
        """Get final balance after interest."""
        return self.principal + self.interest


@dataclass
class InterestCalculator:
    """Calculator for interest on lending positions.

    This class provides the primary API for calculating interest during
    backtesting. It supports both compound and simple interest, and can
    use variable APY from different sources.

    Interest calculation formulas:
        Simple:   interest = principal * apy * (time_days / 365)
        Compound: interest = principal * ((1 + apy/n)^(n * time_days/365) - 1)

    For compound interest, n is the compounding frequency per year.
    The default is continuous compounding (n approaches infinity), which
    simplifies to: interest = principal * (e^(apy * time_days/365) - 1)

    Interest Rate Sources:
        - FIXED: Use default_supply_apy/default_borrow_apy or protocol_*_apys
        - HISTORICAL: Fetch actual historical APY from LendingAPYProvider
        - PROTOCOL: Same as FIXED but always uses protocol-specific rates

    Attributes:
        interest_rate_source: Source of APY data (default FIXED)
        default_supply_apy: Default APY for supply positions (default 3%)
        default_borrow_apy: Default APY for borrow positions (default 5%)
        compounding_periods_per_year: Number of compounding periods per year
            (365 for daily, 8760 for hourly, 0 for continuous)
        protocol_supply_apys: Protocol-specific default supply APYs
        protocol_borrow_apys: Protocol-specific default borrow APYs
        apy_provider: Optional LendingAPYProvider for historical APY lookups
        chain: Blockchain for historical APY lookups (default: ethereum)

    Example:
        calculator = InterestCalculator()

        # Calculate compound interest for 30 days
        result = calculator.calculate_interest(
            principal=Decimal("10000"),
            apy=Decimal("0.05"),
            time_delta=Decimal("30"),
            compound=True,
        )
        print(f"Interest: ${result.interest}")
        print(f"Final balance: ${result.final_balance}")

        # Use historical APY
        calculator_historical = InterestCalculator(
            interest_rate_source=InterestRateSource.HISTORICAL,
            chain="arbitrum",
        )
        apy = await calculator_historical.get_historical_supply_apy(
            protocol="aave_v3",
            market="USDC",
            timestamp=datetime(2024, 1, 15, tzinfo=UTC),
        )
    """

    interest_rate_source: InterestRateSource = InterestRateSource.FIXED
    default_supply_apy: Decimal = Decimal("0.03")  # 3% default supply APY
    default_borrow_apy: Decimal = Decimal("0.05")  # 5% default borrow APY
    compounding_periods_per_year: int = 365  # Daily compounding (0 for continuous)
    protocol_supply_apys: dict[str, Decimal] = field(default_factory=dict)
    protocol_borrow_apys: dict[str, Decimal] = field(default_factory=dict)
    chain: str = "ethereum"  # Chain for historical APY lookups
    _apy_provider: "LendingAPYProvider | None" = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """Initialize protocol-specific APYs and optional APY provider."""
        if not self.protocol_supply_apys:
            self.protocol_supply_apys = {
                "aave_v3": Decimal("0.03"),  # 3% supply
                "compound_v3": Decimal("0.025"),  # 2.5% supply
                "morpho": Decimal("0.035"),  # 3.5% supply
                "spark": Decimal("0.05"),  # 5% supply (DSR)
            }
        if not self.protocol_borrow_apys:
            self.protocol_borrow_apys = {
                "aave_v3": Decimal("0.05"),  # 5% borrow
                "compound_v3": Decimal("0.045"),  # 4.5% borrow
                "morpho": Decimal("0.04"),  # 4% borrow
                "spark": Decimal("0.055"),  # 5.5% borrow
            }

        # Initialize APY provider for historical lookups if source is HISTORICAL
        if self.interest_rate_source == InterestRateSource.HISTORICAL and self._apy_provider is None:
            self._init_apy_provider()

    def _init_apy_provider(self) -> None:
        """Initialize the LendingAPYProvider for historical lookups."""
        try:
            from almanak.framework.backtesting.pnl.providers.lending_apy import (
                LendingAPYProvider,
            )

            self._apy_provider = LendingAPYProvider(chain=self.chain)
            logger.debug(
                "Initialized LendingAPYProvider for historical APY lookups (chain=%s)",
                self.chain,
            )
        except ImportError:
            logger.warning("LendingAPYProvider not available, falling back to default APYs")
            self._apy_provider = None
        except Exception as e:
            logger.warning(
                "Failed to initialize LendingAPYProvider: %s, falling back to default APYs",
                e,
            )
            self._apy_provider = None

    async def get_historical_supply_apy(
        self,
        protocol: str,
        market: str,
        timestamp: datetime,
    ) -> Decimal:
        """Get historical supply APY for a market at a specific timestamp.

        When interest_rate_source is HISTORICAL, this method fetches the actual
        historical APY from subgraph data. Falls back to default APY if data is
        unavailable.

        Args:
            protocol: Lending protocol (aave_v3, compound_v3)
            market: Market/asset identifier (e.g., "USDC", "WETH")
            timestamp: The timestamp to query APY for

        Returns:
            Supply APY as decimal (0.03 = 3%)

        Example:
            apy = await calculator.get_historical_supply_apy(
                protocol="aave_v3",
                market="USDC",
                timestamp=datetime(2024, 1, 15, tzinfo=UTC),
            )
        """
        # If not using historical source, return default
        if self.interest_rate_source != InterestRateSource.HISTORICAL:
            apy = self.get_supply_apy_for_protocol(protocol)
            logger.debug(
                "Using default supply APY for %s %s: %.2f%% (source: %s)",
                protocol,
                market,
                float(apy * 100),
                self.interest_rate_source.value,
            )
            return apy

        # Try to fetch historical APY
        if self._apy_provider is not None:
            try:
                apy_data = await self._apy_provider.get_historical_apy(
                    protocol=protocol,
                    market=market,
                    timestamp=timestamp,
                )
                logger.info(
                    "Historical supply APY for %s %s at %s: %.2f%% (source: %s)",
                    protocol,
                    market,
                    timestamp.isoformat(),
                    float(apy_data.supply_apy_pct),
                    apy_data.source,
                )
                return apy_data.supply_apy
            except Exception as e:
                logger.warning(
                    "Failed to fetch historical supply APY for %s %s: %s, using default",
                    protocol,
                    market,
                    e,
                )

        # Fallback to default
        apy = self.get_supply_apy_for_protocol(protocol)
        logger.info(
            "Using default supply APY for %s %s: %.2f%% (source: fallback)",
            protocol,
            market,
            float(apy * 100),
        )
        return apy

    async def get_historical_borrow_apy(
        self,
        protocol: str,
        market: str,
        timestamp: datetime,
    ) -> Decimal:
        """Get historical borrow APY for a market at a specific timestamp.

        When interest_rate_source is HISTORICAL, this method fetches the actual
        historical APY from subgraph data. Falls back to default APY if data is
        unavailable.

        Args:
            protocol: Lending protocol (aave_v3, compound_v3)
            market: Market/asset identifier (e.g., "USDC", "WETH")
            timestamp: The timestamp to query APY for

        Returns:
            Borrow APY as decimal (0.05 = 5%)

        Example:
            apy = await calculator.get_historical_borrow_apy(
                protocol="aave_v3",
                market="USDC",
                timestamp=datetime(2024, 1, 15, tzinfo=UTC),
            )
        """
        # If not using historical source, return default
        if self.interest_rate_source != InterestRateSource.HISTORICAL:
            apy = self.get_borrow_apy_for_protocol(protocol)
            logger.debug(
                "Using default borrow APY for %s %s: %.2f%% (source: %s)",
                protocol,
                market,
                float(apy * 100),
                self.interest_rate_source.value,
            )
            return apy

        # Try to fetch historical APY
        if self._apy_provider is not None:
            try:
                apy_data = await self._apy_provider.get_historical_apy(
                    protocol=protocol,
                    market=market,
                    timestamp=timestamp,
                )
                logger.info(
                    "Historical borrow APY for %s %s at %s: %.2f%% (source: %s)",
                    protocol,
                    market,
                    timestamp.isoformat(),
                    float(apy_data.borrow_apy_pct),
                    apy_data.source,
                )
                return apy_data.borrow_apy
            except Exception as e:
                logger.warning(
                    "Failed to fetch historical borrow APY for %s %s: %s, using default",
                    protocol,
                    market,
                    e,
                )

        # Fallback to default
        apy = self.get_borrow_apy_for_protocol(protocol)
        logger.info(
            "Using default borrow APY for %s %s: %.2f%% (source: fallback)",
            protocol,
            market,
            float(apy * 100),
        )
        return apy

    def get_historical_supply_apy_sync(
        self,
        protocol: str,
        market: str,
        timestamp: datetime,
    ) -> Decimal:
        """Synchronous version of get_historical_supply_apy.

        This method attempts to run the async version in a new event loop.
        If that fails, it falls back to default APY.

        Args:
            protocol: Lending protocol
            market: Market/asset identifier
            timestamp: The timestamp to query

        Returns:
            Supply APY as decimal
        """
        import asyncio

        # If not using historical, just return default
        if self.interest_rate_source != InterestRateSource.HISTORICAL:
            return self.get_supply_apy_for_protocol(protocol)

        try:
            # Try to get the running loop
            try:
                asyncio.get_running_loop()
                # We're in an async context, can't run sync
                logger.debug(
                    "In async context, using default supply APY for %s %s",
                    protocol,
                    market,
                )
                return self.get_supply_apy_for_protocol(protocol)
            except RuntimeError:
                # No running loop, we can create one
                return asyncio.run(self.get_historical_supply_apy(protocol, market, timestamp))
        except Exception as e:
            logger.warning(
                "Failed to fetch historical supply APY sync for %s %s: %s, using default",
                protocol,
                market,
                e,
            )
            return self.get_supply_apy_for_protocol(protocol)

    def get_historical_borrow_apy_sync(
        self,
        protocol: str,
        market: str,
        timestamp: datetime,
    ) -> Decimal:
        """Synchronous version of get_historical_borrow_apy.

        This method attempts to run the async version in a new event loop.
        If that fails, it falls back to default APY.

        Args:
            protocol: Lending protocol
            market: Market/asset identifier
            timestamp: The timestamp to query

        Returns:
            Borrow APY as decimal
        """
        import asyncio

        # If not using historical, just return default
        if self.interest_rate_source != InterestRateSource.HISTORICAL:
            return self.get_borrow_apy_for_protocol(protocol)

        try:
            # Try to get the running loop
            try:
                asyncio.get_running_loop()
                # We're in an async context, can't run sync
                logger.debug(
                    "In async context, using default borrow APY for %s %s",
                    protocol,
                    market,
                )
                return self.get_borrow_apy_for_protocol(protocol)
            except RuntimeError:
                # No running loop, we can create one
                return asyncio.run(self.get_historical_borrow_apy(protocol, market, timestamp))
        except Exception as e:
            logger.warning(
                "Failed to fetch historical borrow APY sync for %s %s: %s, using default",
                protocol,
                market,
                e,
            )
            return self.get_borrow_apy_for_protocol(protocol)

    def calculate_interest(
        self,
        principal: Decimal,
        apy: Decimal,
        time_delta: Decimal | timedelta,
        compound: bool = True,
    ) -> InterestResult:
        """Calculate interest for a principal amount over a time period.

        This is the primary method for calculating interest. It supports both
        simple and compound interest calculations.

        For compound interest with daily compounding (default):
            interest = principal * ((1 + apy/365)^(365 * time_days/365) - 1)
                     = principal * ((1 + apy/365)^time_days - 1)

        For simple interest:
            interest = principal * apy * (time_days / 365)

        Args:
            principal: The principal amount (always positive)
            apy: The annual percentage yield (0.05 = 5%)
            time_delta: Time period in days (Decimal) or as timedelta
            compound: If True, use compound interest. If False, use simple interest.

        Returns:
            InterestResult with interest amount and metadata

        Example:
            # Compound interest for $10,000 at 5% APY over 30 days
            result = calculator.calculate_interest(
                principal=Decimal("10000"),
                apy=Decimal("0.05"),
                time_delta=Decimal("30"),
                compound=True,
            )
            # result.interest ≈ $41.10 (daily compounding)

            # Simple interest for same parameters
            result = calculator.calculate_interest(
                principal=Decimal("10000"),
                apy=Decimal("0.05"),
                time_delta=Decimal("30"),
                compound=False,
            )
            # result.interest ≈ $41.10 (very similar for short periods)
        """
        # Convert timedelta to days if needed
        if isinstance(time_delta, timedelta):
            days = Decimal(str(time_delta.total_seconds())) / Decimal("86400")
        else:
            days = time_delta

        # Handle edge cases
        if principal <= Decimal("0") or days <= Decimal("0"):
            return InterestResult(
                interest=Decimal("0"),
                principal=principal,
                apy=apy,
                time_days=days,
                compound=compound,
                effective_rate=Decimal("0"),
            )

        # Calculate interest based on method
        if compound:
            interest = self._calculate_compound_interest(principal, apy, days)
        else:
            interest = self._calculate_simple_interest(principal, apy, days)

        # Calculate effective rate
        effective_rate = interest / principal if principal > Decimal("0") else Decimal("0")

        return InterestResult(
            interest=interest,
            principal=principal,
            apy=apy,
            time_days=days,
            compound=compound,
            effective_rate=effective_rate,
        )

    def _calculate_simple_interest(self, principal: Decimal, apy: Decimal, days: Decimal) -> Decimal:
        """Calculate simple interest.

        Formula: interest = principal * apy * (days / 365)

        Args:
            principal: Principal amount
            apy: Annual percentage yield
            days: Time period in days

        Returns:
            Interest amount
        """
        year_fraction = days / Decimal("365")
        return principal * apy * year_fraction

    def _calculate_compound_interest(self, principal: Decimal, apy: Decimal, days: Decimal) -> Decimal:
        """Calculate compound interest.

        For n compounding periods per year:
            interest = principal * ((1 + apy/n)^(n * days/365) - 1)

        For continuous compounding (n=0):
            interest = principal * (e^(apy * days/365) - 1)

        Args:
            principal: Principal amount
            apy: Annual percentage yield
            days: Time period in days

        Returns:
            Interest amount
        """
        n = self.compounding_periods_per_year
        year_fraction = days / Decimal("365")

        if n == 0:
            # Continuous compounding: P * (e^(r*t) - 1)
            # Approximate e^x using Taylor series for small x
            exponent = apy * year_fraction
            # For typical APYs and time periods, use approximation
            # e^x ≈ 1 + x + x^2/2 + x^3/6 for small x
            if abs(exponent) < Decimal("0.1"):
                factor = Decimal("1") + exponent + (exponent**2) / Decimal("2") + (exponent**3) / Decimal("6")
            else:
                # For larger exponents, use more terms or fallback
                import math

                factor = Decimal(str(math.exp(float(exponent))))
            return principal * (factor - Decimal("1"))
        else:
            # Discrete compounding: P * ((1 + r/n)^(n*t) - 1)
            # Number of compounding periods in the time span
            periods = n * year_fraction
            period_rate = apy / Decimal(str(n))

            # Calculate (1 + r/n)^periods
            # For safety with Decimal, compute iteratively or use approximation
            base = Decimal("1") + period_rate

            # Use Python's built-in power for efficiency
            # This works well for reasonable period counts
            factor = base**periods

            return principal * (factor - Decimal("1"))

    def calculate_interest_variable_apy(
        self,
        principal: Decimal,
        apy_schedule: list[tuple[Decimal, Decimal]],
        compound: bool = True,
    ) -> InterestResult:
        """Calculate interest with variable APY over time periods.

        This method allows for different APY rates during different time periods,
        which is common in lending protocols where rates change based on utilization.

        Args:
            principal: The principal amount
            apy_schedule: List of (days, apy) tuples representing time periods and rates.
                          Each tuple specifies the duration in days and the APY for that period.
            compound: If True, use compound interest. If False, use simple interest.

        Returns:
            InterestResult with total interest and metadata

        Example:
            # Variable APY: 3% for first 10 days, then 5% for next 20 days
            result = calculator.calculate_interest_variable_apy(
                principal=Decimal("10000"),
                apy_schedule=[
                    (Decimal("10"), Decimal("0.03")),
                    (Decimal("20"), Decimal("0.05")),
                ],
            )
        """
        if not apy_schedule:
            return InterestResult(
                interest=Decimal("0"),
                principal=principal,
                apy=Decimal("0"),
                time_days=Decimal("0"),
                compound=compound,
                effective_rate=Decimal("0"),
            )

        total_interest = Decimal("0")
        total_days = Decimal("0")
        current_principal = principal

        for days, apy in apy_schedule:
            result = self.calculate_interest(
                principal=current_principal,
                apy=apy,
                time_delta=days,
                compound=compound,
            )
            total_interest += result.interest
            total_days += days

            if compound:
                # For compound interest, interest from previous period adds to principal
                current_principal += result.interest

        # Calculate weighted average APY
        weighted_apy = Decimal("0")
        if total_days > Decimal("0"):
            for days, apy in apy_schedule:
                weighted_apy += apy * (days / total_days)

        effective_rate = total_interest / principal if principal > Decimal("0") else Decimal("0")

        return InterestResult(
            interest=total_interest,
            principal=principal,
            apy=weighted_apy,
            time_days=total_days,
            compound=compound,
            effective_rate=effective_rate,
        )

    def get_supply_apy_for_protocol(self, protocol: str) -> Decimal:
        """Get the default supply APY for a specific protocol.

        Args:
            protocol: Protocol name (e.g., "aave_v3", "compound_v3")

        Returns:
            The protocol's default supply APY, or global default if not found
        """
        return self.protocol_supply_apys.get(protocol.lower(), self.default_supply_apy)

    def get_borrow_apy_for_protocol(self, protocol: str) -> Decimal:
        """Get the default borrow APY for a specific protocol.

        Args:
            protocol: Protocol name (e.g., "aave_v3", "compound_v3")

        Returns:
            The protocol's default borrow APY, or global default if not found
        """
        return self.protocol_borrow_apys.get(protocol.lower(), self.default_borrow_apy)

    def estimate_annual_interest(
        self,
        principal: Decimal,
        apy: Decimal,
        compound: bool = True,
    ) -> Decimal:
        """Estimate annual interest for quick projections.

        Useful for displaying expected annual earnings/costs.

        Args:
            principal: The principal amount
            apy: The annual percentage yield
            compound: Whether to use compound interest

        Returns:
            Estimated annual interest
        """
        result = self.calculate_interest(
            principal=principal,
            apy=apy,
            time_delta=Decimal("365"),
            compound=compound,
        )
        return result.interest

    def estimate_daily_interest(
        self,
        principal: Decimal,
        apy: Decimal,
        compound: bool = True,
    ) -> Decimal:
        """Estimate daily interest for quick projections.

        Useful for displaying expected daily earnings/costs.

        Args:
            principal: The principal amount
            apy: The annual percentage yield
            compound: Whether to use compound interest

        Returns:
            Estimated daily interest
        """
        result = self.calculate_interest(
            principal=principal,
            apy=apy,
            time_delta=Decimal("1"),
            compound=compound,
        )
        return result.interest

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "calculator_name": "interest",
            "interest_rate_source": self.interest_rate_source.value,
            "default_supply_apy": str(self.default_supply_apy),
            "default_borrow_apy": str(self.default_borrow_apy),
            "compounding_periods_per_year": self.compounding_periods_per_year,
            "protocol_supply_apys": {k: str(v) for k, v in self.protocol_supply_apys.items()},
            "protocol_borrow_apys": {k: str(v) for k, v in self.protocol_borrow_apys.items()},
            "chain": self.chain,
            "apy_provider_available": self._apy_provider is not None,
        }

    async def close(self) -> None:
        """Close the APY provider and release resources."""
        if self._apy_provider is not None:
            await self._apy_provider.close()
            self._apy_provider = None


__all__ = [
    "InterestCalculator",
    "InterestRateSource",
    "InterestResult",
]
