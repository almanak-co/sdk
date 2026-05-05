"""Shared mocks for the PnL backtester unit tests.

Lives outside the `test_*.py` collection set so other test modules can import
from it without coupling to a sibling test module's collection state. The
underscore prefix signals "private to this directory" and keeps pytest from
attempting to collect it.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from decimal import Decimal

from almanak.framework.backtesting.pnl.data_provider import (
    OHLCV,
    HistoricalDataConfig,
    MarketState,
)


class MockDataProvider:
    """Mock implementation of HistoricalDataProvider for testing."""

    def __init__(
        self,
        prices: dict[str, dict[datetime, Decimal]] | None = None,
        base_prices: dict[str, Decimal] | None = None,
        price_change_per_tick: Decimal = Decimal("0"),
    ):
        """Initialize mock data provider.

        Args:
            prices: Dict mapping token -> {timestamp -> price}
            base_prices: Dict of base prices to use for all timestamps
            price_change_per_tick: Amount to change price each tick
        """
        self._prices = prices or {}
        self._base_prices = base_prices or {
            "WETH": Decimal("3000"),
            "USDC": Decimal("1"),
        }
        self._price_change_per_tick = price_change_per_tick
        self._tick_count = 0

    async def get_price(self, token: str, timestamp: datetime) -> Decimal:
        """Get price for token at timestamp."""
        token = token.upper()
        if token in self._prices and timestamp in self._prices[token]:
            return self._prices[token][timestamp]
        if token in self._base_prices:
            return self._base_prices[token]
        raise ValueError(f"No price for {token}")

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
                    high=price * Decimal("1.01"),
                    low=price * Decimal("0.99"),
                    close=price,
                )
            )
            current += timedelta(seconds=interval_seconds)
        return result

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Iterate through historical data."""
        current = config.start_time
        while current <= config.end_time:
            prices = {}
            for token in config.tokens:
                token = token.upper()
                # Apply price change per tick
                base = self._base_prices.get(token, Decimal("1"))
                change = self._price_change_per_tick * self._tick_count
                prices[token] = base + change

            self._tick_count += 1

            market_state = MarketState(
                timestamp=current,
                prices=prices,
                chain=config.chains[0] if config.chains else "arbitrum",
            )
            yield current, market_state

            current += timedelta(seconds=config.interval_seconds)

    @property
    def provider_name(self) -> str:
        return "mock"

    @property
    def supported_tokens(self) -> list[str]:
        return list(self._base_prices.keys())

    @property
    def supported_chains(self) -> list[str]:
        return ["arbitrum"]

    @property
    def min_timestamp(self) -> datetime | None:
        return None

    @property
    def max_timestamp(self) -> datetime | None:
        return None
