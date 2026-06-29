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
    TokenRef,
    normalize_token_key,
    token_ref_display,
)


class MockDataProvider:
    """Mock implementation of HistoricalDataProvider for testing."""

    def __init__(
        self,
        prices: dict[TokenRef, dict[datetime, Decimal]] | None = None,
        base_prices: dict[TokenRef, Decimal] | None = None,
        price_change_per_tick: Decimal = Decimal("0"),
    ):
        """Initialize mock data provider.

        Args:
            prices: Dict mapping token -> {timestamp -> price}
            base_prices: Dict of base prices to use for all timestamps
            price_change_per_tick: Amount to change price each tick
        """
        self._prices = {self._key(token): values for token, values in (prices or {}).items()}
        default_base_prices: dict[TokenRef, Decimal] = {
            "WETH": Decimal("3000"),
            "USDC": Decimal("1"),
        }
        self._base_prices = {self._key(token): price for token, price in (base_prices or default_base_prices).items()}
        self._price_change_per_tick = price_change_per_tick
        self._tick_count = 0

    @staticmethod
    def _key(token: TokenRef) -> TokenRef:
        if isinstance(token, tuple):
            return normalize_token_key(token[0], token[1])
        return token.upper()

    async def get_price(self, token: TokenRef, timestamp: datetime) -> Decimal:
        """Get price for token at timestamp."""
        token_key = self._key(token)
        if token_key in self._prices and timestamp in self._prices[token_key]:
            return self._prices[token_key][timestamp]
        if token_key in self._base_prices:
            return self._base_prices[token_key]
        raise ValueError(f"No price for {token_ref_display(token)}")

    async def get_ohlcv(
        self,
        token: TokenRef,
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
                token_key = self._key(token)
                # Apply price change per tick
                base = self._base_prices.get(token_key, Decimal("1"))
                change = self._price_change_per_tick * self._tick_count
                prices[token_key] = base + change

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
        return [token_ref_display(token) for token in self._base_prices]

    @property
    def supported_chains(self) -> list[str]:
        return ["arbitrum"]

    @property
    def min_timestamp(self) -> datetime | None:
        return None

    @property
    def max_timestamp(self) -> datetime | None:
        return None
