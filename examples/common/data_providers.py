"""Synthetic data providers for reproducible backtest examples.

These providers generate deterministic price data for backtesting demonstrations.
Using synthetic data ensures:
1. Reproducibility - same seed = same results
2. No API dependencies - works offline
3. Controlled market conditions - demonstrates specific scenarios
"""

import math
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from almanak.framework.backtesting.pnl.data_provider import (
    OHLCV,
    HistoricalDataConfig,
    MarketState,
)


@dataclass
class SyntheticDataProvider:
    """Base class for synthetic price data generation.

    Generates deterministic price series based on configurable patterns.
    All prices are seeded with the start_time for reproducibility.
    """

    start_time: datetime
    base_price: Decimal = Decimal("3000")
    num_hours: int = 720  # 30 days

    def __post_init__(self) -> None:
        self._prices: dict[int, Decimal] = {}
        self._generate_prices()

    def _generate_prices(self) -> None:
        """Override in subclasses to generate specific price patterns."""
        for i in range(self.num_hours):
            self._prices[i] = self.base_price

    def get_price_at_index(self, index: int) -> Decimal:
        """Get the price at a given hourly index."""
        return self._prices.get(index, self.base_price)

    async def get_price(self, token: str, timestamp: datetime) -> Decimal:
        """Get price for token at specific timestamp."""
        token = token.upper()
        if token in ("USDC", "USDT", "DAI"):
            return Decimal("1")

        delta = timestamp - self.start_time
        index = int(delta.total_seconds() / 3600)
        return self.get_price_at_index(max(0, min(index, self.num_hours - 1)))

    async def get_ohlcv(
        self,
        token: str,
        start: datetime,
        end: datetime,
        interval_seconds: int = 3600,
    ) -> list[OHLCV]:
        """Get OHLCV data for token."""
        result = []
        current = start
        while current <= end:
            price = await self.get_price(token, current)
            result.append(
                OHLCV(
                    timestamp=current,
                    open=price,
                    high=price * Decimal("1.002"),
                    low=price * Decimal("0.998"),
                    close=price,
                    volume=Decimal("1000000"),
                )
            )
            current += timedelta(seconds=interval_seconds)
        return result

    async def iterate(
        self, config: HistoricalDataConfig
    ) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Iterate through historical data with synthetic prices."""
        current = config.start_time
        index = 0
        interval_delta = timedelta(seconds=config.interval_seconds)

        while current <= config.end_time:
            prices = {}
            for token in config.tokens:
                token_upper = token.upper()
                if token_upper in ("USDC", "USDT", "DAI"):
                    prices[token_upper] = Decimal("1")
                elif token_upper in ("WETH", "ETH"):
                    prices[token_upper] = self.get_price_at_index(index)
                elif token_upper in ("WSTETH",):
                    # wstETH trades at ~1.15x ETH
                    prices[token_upper] = self.get_price_at_index(index) * Decimal("1.15")
                else:
                    prices[token_upper] = self.get_price_at_index(index)

            market_state = MarketState(
                timestamp=current,
                prices=prices,
                chain=config.chains[0] if config.chains else "arbitrum",
                block_number=15000000 + index * 100,
                gas_price_gwei=Decimal("30"),
            )
            yield current, market_state

            index += 1
            current += interval_delta

    @property
    def provider_name(self) -> str:
        return "synthetic_mock"

    @property
    def supported_tokens(self) -> list[str]:
        return ["WETH", "ETH", "USDC", "USDT", "DAI", "WSTETH"]

    @property
    def supported_chains(self) -> list[str]:
        return ["arbitrum", "ethereum", "base"]

    @property
    def min_timestamp(self) -> datetime | None:
        return self.start_time

    @property
    def max_timestamp(self) -> datetime | None:
        return self.start_time + timedelta(hours=self.num_hours)


class RSITriggerDataProvider(SyntheticDataProvider):
    """Data provider that generates price patterns to trigger RSI signals.

    Creates synthetic price data with deliberate oversold and overbought
    conditions to trigger RSI signals for visual verification.

    Price Phases (30 days):
    - Days 0-5: Decline to trigger oversold (RSI < 30)
    - Days 5-10: Recovery from oversold
    - Days 10-18: Rally to trigger overbought (RSI > 70)
    - Days 18-23: Decline from overbought
    - Days 23-30: Consolidation with small waves
    """

    def _generate_prices(self) -> None:
        """Generate synthetic price series with RSI-triggering patterns."""
        for i in range(self.num_hours):
            day = i // 24
            hour_in_day = i % 24

            if day < 5:
                # Steady decline: ~20% drop over 5 days -> triggers oversold
                decline_pct = -0.04 * (day + hour_in_day / 24)
                price_factor = Decimal(str(1.0 + decline_pct))
            elif day < 10:
                # Recovery from oversold: bounce back 15%
                day_in_phase = day - 5 + hour_in_day / 24
                recovery_pct = -0.20 + 0.03 * day_in_phase
                price_factor = Decimal(str(1.0 + recovery_pct))
            elif day < 18:
                # Strong rally: push to overbought (~25% gain from base)
                day_in_phase = day - 10 + hour_in_day / 24
                rally_pct = -0.05 + 0.0375 * day_in_phase
                price_factor = Decimal(str(1.0 + rally_pct))
            elif day < 23:
                # Decline from overbought
                day_in_phase = day - 18 + hour_in_day / 24
                decline_pct = 0.25 - 0.05 * day_in_phase
                price_factor = Decimal(str(1.0 + decline_pct))
            else:
                # Consolidation around base price with small waves
                day_in_phase = day - 23 + hour_in_day / 24
                wave = 0.02 * math.sin(day_in_phase * math.pi / 2)
                price_factor = Decimal(str(1.0 + wave))

            # Add small deterministic noise for realism
            noise = Decimal(str(0.001 * math.sin(i * 0.5)))
            self._prices[i] = self.base_price * (price_factor + noise)


class LPRangeDataProvider(SyntheticDataProvider):
    """Data provider that generates price movement across LP range boundaries.

    Creates synthetic price data that moves in and out of a defined LP range
    to demonstrate in-range vs out-of-range behavior for visual verification.

    Price Phases (30 days):
    - Days 0-5: Start in range, move towards upper bound
    - Days 5-10: Exit above range, stay out
    - Days 10-15: Re-enter range from above
    - Days 15-20: Move towards lower bound and exit below
    - Days 20-25: Stay below range
    - Days 25-30: Re-enter range from below
    """

    range_lower: Decimal = Decimal("2800")
    range_upper: Decimal = Decimal("3200")

    def __init__(
        self,
        start_time: datetime,
        base_price: Decimal = Decimal("3000"),
        range_lower: Decimal = Decimal("2800"),
        range_upper: Decimal = Decimal("3200"),
        num_hours: int = 720,
    ):
        self.range_lower = range_lower
        self.range_upper = range_upper
        super().__init__(start_time=start_time, base_price=base_price, num_hours=num_hours)

    def _generate_prices(self) -> None:
        """Generate synthetic price series that moves in and out of LP range."""
        range_mid = (self.range_lower + self.range_upper) / 2
        range_width = self.range_upper - self.range_lower

        for i in range(self.num_hours):
            day = i / 24  # Fractional day

            if day < 5:
                # Phase 1: Start at mid-range, move toward upper bound
                progress = day / 5
                target = range_mid + (range_width * Decimal("0.4") * Decimal(str(progress)))
                price = float(target)
            elif day < 10:
                # Phase 2: Exit above range, go to upper + 10%
                progress = (day - 5) / 5
                above_range = float(self.range_upper) * 1.15
                start = float(self.range_upper) + float(range_width) * 0.4 * 0.2
                price = start + (above_range - start) * progress
            elif day < 15:
                # Phase 3: Come back into range from above
                progress = (day - 10) / 5
                above_range = float(self.range_upper) * 1.15
                target = float(range_mid)
                price = above_range + (target - above_range) * progress
            elif day < 20:
                # Phase 4: Move toward lower bound and exit below
                progress = (day - 15) / 5
                start = float(range_mid)
                below_range = float(self.range_lower) * 0.90
                price = start + (below_range - start) * progress
            elif day < 25:
                # Phase 5: Stay below range with small wave pattern
                progress = (day - 20) / 5
                below_range = float(self.range_lower) * 0.88
                wave = 50 * math.sin(progress * math.pi * 4)
                price = below_range + wave
            else:
                # Phase 6: Re-enter range from below
                progress = (day - 25) / 5
                below_range = float(self.range_lower) * 0.88
                target = float(range_mid) + float(range_width) * 0.2
                price = below_range + (target - below_range) * progress

            # Add small deterministic noise for realism
            noise = 10 * math.sin(i * 0.3)
            self._prices[i] = Decimal(str(price + noise))


class LendingDataProvider(SyntheticDataProvider):
    """Data provider that generates price volatility for lending strategy testing.

    Creates synthetic price data with periods of decline to test health factor
    monitoring and deleverage triggers.

    Price Phases (30 days):
    - Days 0-7: Stable/slight increase (build leverage safely)
    - Days 7-12: Sharp decline (test HF monitoring)
    - Days 12-18: Recovery (HF improves)
    - Days 18-25: Another decline (test deleverage trigger)
    - Days 25-30: Final recovery
    """

    def _generate_prices(self) -> None:
        """Generate synthetic price series with volatility for HF testing."""
        for i in range(self.num_hours):
            day = i / 24  # Fractional day

            if day < 7:
                # Phase 1: Stable with slight increase (~5%)
                progress = day / 7
                price_factor = 1.0 + 0.05 * progress
            elif day < 12:
                # Phase 2: Sharp decline (~25%)
                progress = (day - 7) / 5
                price_factor = 1.05 - 0.25 * progress
            elif day < 18:
                # Phase 3: Recovery (~15%)
                progress = (day - 12) / 6
                price_factor = 0.80 + 0.15 * progress
            elif day < 25:
                # Phase 4: Another decline (~20%)
                progress = (day - 18) / 7
                price_factor = 0.95 - 0.20 * progress
            else:
                # Phase 5: Final recovery (~10%)
                progress = (day - 25) / 5
                price_factor = 0.75 + 0.10 * progress

            # Add small deterministic noise
            noise = Decimal(str(0.002 * math.sin(i * 0.7)))
            self._prices[i] = self.base_price * (Decimal(str(price_factor)) + noise)
