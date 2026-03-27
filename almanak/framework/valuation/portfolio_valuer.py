"""Portfolio valuation orchestrator.

Produces PortfolioSnapshot by querying the gateway (via MarketSnapshot)
for wallet balances and token prices, optionally consuming
strategy.get_open_positions() for non-wallet positions (LP, lending, perps).

This is the single source of truth for portfolio valuation at runtime.
Strategies declare positions; the framework owns the math.
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from almanak.framework.portfolio.models import (
    PortfolioSnapshot,
    PositionValue,
    ValueConfidence,
)
from almanak.framework.valuation.spot_valuer import total_value, value_tokens

if TYPE_CHECKING:
    from almanak.framework.teardown.models import TeardownPositionSummary

logger = logging.getLogger(__name__)


@runtime_checkable
class MarketDataSource(Protocol):
    """Minimal interface for fetching prices and balances.

    Satisfied by both the strategy-facing MarketSnapshot and
    the data-layer MarketSnapshot.
    """

    def price(self, token: str, quote: str = "USD") -> Decimal: ...
    def balance(self, token: str) -> Any: ...


@runtime_checkable
class StrategyLike(Protocol):
    """Minimal strategy interface for PortfolioValuer."""

    @property
    def strategy_id(self) -> str: ...

    @property
    def chain(self) -> str: ...

    def _get_tracked_tokens(self) -> list[str]: ...


class PortfolioValuer:
    """Framework-owned portfolio valuation engine.

    Replaces strategy-level get_portfolio_snapshot() as the primary
    valuation path. Strategies still implement get_open_positions()
    for position discovery (LP, lending, perps), but the valuer
    owns the math and re-prices via gateway data.

    Usage:
        valuer = PortfolioValuer()
        snapshot = valuer.value(strategy, market)
    """

    def value(
        self,
        strategy: StrategyLike,
        market: MarketDataSource,
        iteration_number: int = 0,
    ) -> PortfolioSnapshot:
        """Produce a PortfolioSnapshot with real USD values.

        Never raises -- returns UNAVAILABLE confidence on total failure.
        This guarantees gap-free time series for PnL charts.

        Args:
            strategy: Strategy instance for position discovery and config
            market: MarketSnapshot for price/balance queries
            iteration_number: Current strategy iteration count

        Returns:
            PortfolioSnapshot with real values and appropriate ValueConfidence
        """
        now = datetime.now(UTC)
        strategy_id = ""
        chain = ""

        try:
            strategy_id = strategy.strategy_id
            chain = strategy.chain

            # Step 1: Discover tracked tokens from strategy config
            tracked_tokens = strategy._get_tracked_tokens()

            # Step 2: Fetch wallet balances and prices via gateway
            balances: dict[str, Decimal] = {}
            prices: dict[str, Decimal] = {}
            wallet_data_incomplete = False

            for token in tracked_tokens:
                try:
                    balance_result = market.balance(token)
                    # MarketSnapshot.balance() returns TokenBalance or Decimal
                    if hasattr(balance_result, "balance"):
                        bal = balance_result.balance
                    else:
                        bal = Decimal(str(balance_result))
                    if bal > 0:
                        balances[token] = bal
                except Exception:
                    wallet_data_incomplete = True
                    logger.debug("Could not fetch balance for %s", token)

                try:
                    price = market.price(token)
                    prices[token] = Decimal(str(price))
                except Exception:
                    if token in balances:
                        wallet_data_incomplete = True
                    logger.debug("Could not fetch price for %s", token)

            # Check for tokens with positive balance but missing/non-positive price
            for token in balances:
                token_price = prices.get(token)
                if token_price is None or token_price <= 0:
                    wallet_data_incomplete = True

            # Step 3: Apply spot valuation math (pure, deterministic)
            wallet_balances = value_tokens(balances, prices)
            wallet_value = total_value(wallet_balances)

            # Step 4: Get non-wallet positions (LP, lending, perps) if available
            positions, position_value, positions_unavailable = self._get_positions(strategy, prices)

            # Step 5: Determine confidence level
            has_any_value = bool(wallet_balances or positions)
            if not has_any_value and (positions_unavailable or wallet_data_incomplete):
                confidence = ValueConfidence.UNAVAILABLE
            elif positions_unavailable or wallet_data_incomplete:
                confidence = ValueConfidence.ESTIMATED
            else:
                confidence = ValueConfidence.HIGH

            return PortfolioSnapshot(
                timestamp=now,
                strategy_id=strategy_id,
                total_value_usd=wallet_value + position_value,
                available_cash_usd=wallet_value,
                value_confidence=confidence,
                positions=positions,
                wallet_balances=wallet_balances,
                chain=chain,
                iteration_number=iteration_number,
            )

        except Exception as e:
            # Failure contract: NEVER skip a snapshot. Persist with UNAVAILABLE.
            logger.warning("Portfolio valuation failed: %s", e)
            return PortfolioSnapshot(
                timestamp=now,
                strategy_id=strategy_id,
                total_value_usd=Decimal("0"),
                available_cash_usd=Decimal("0"),
                value_confidence=ValueConfidence.UNAVAILABLE,
                error=str(e),
                chain=chain,
                iteration_number=iteration_number,
            )

    def _get_positions(
        self,
        strategy: StrategyLike,
        prices: dict[str, Decimal],
    ) -> tuple[list[PositionValue], Decimal, bool]:
        """Extract non-wallet positions from strategy.get_open_positions().

        Treats strategy-reported value_usd as a hint. For Week 1 (spot-only MVP),
        we pass through the strategy's reported values. Week 2+ will re-price
        LP positions via gateway.

        Returns:
            (positions, total_position_value, positions_unavailable)
        """
        if not hasattr(strategy, "get_open_positions"):
            return [], Decimal("0"), False

        try:
            summary: TeardownPositionSummary = strategy.get_open_positions()
            if not summary or not summary.positions:
                return [], Decimal("0"), False

            positions: list[PositionValue] = []
            for p in summary.positions:
                positions.append(
                    PositionValue(
                        position_type=p.position_type,
                        protocol=p.protocol,
                        chain=p.chain,
                        value_usd=p.value_usd,
                        label=f"{p.protocol} {p.position_type.value}",
                        tokens=p.details.get("tokens", []),
                        details=p.details,
                    )
                )

            position_value = sum((p.value_usd for p in positions), Decimal("0"))
            return positions, position_value, False

        except Exception as e:
            logger.warning("Failed to get open positions: %s", e)
            return [], Decimal("0"), True
