"""Solana Perp Funding Rate Arbitrage Demo Strategy.

Trades SOL-PERP on Drift Protocol based on funding rates:
- When funding rate is very positive (longs pay shorts): go SHORT to earn funding
- When funding rate is very negative (shorts pay longs): go LONG to earn funding
- When funding rate is near zero: hold / close position

This is a simplified demo. A production version would also hedge the
directional exposure with a spot position (delta-neutral).

NOTE: This demo uses direct HTTP calls to the Drift Data API for funding rates.
In production, funding rate data should be served via the gateway MarketSnapshot
layer. See GATEWAY_VIOLATION comment in _get_funding_rate().

Usage:
    # Dry run:
    almanak strat run -d strategies/demo/solana_perp --once --dry-run

    # Real execution:
    almanak strat run -d strategies/demo/solana_perp --once

Environment:
    SOLANA_PRIVATE_KEY   Base58 Ed25519 keypair (required)
    SOLANA_RPC_URL       Solana RPC endpoint (optional)
"""

import logging
from decimal import Decimal

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="solana_perp_funding",
    version="0.1.0",
    description="Funding rate arb on Drift Protocol (demo)",
    supported_chains=["solana"],
    supported_protocols=["drift"],
    intent_types=["PERP_OPEN", "PERP_CLOSE"],
)
class SolanaPerpFundingStrategy(IntentStrategy):
    """Trade SOL-PERP funding rate on Drift.

    Strategy logic:
    1. Check current funding rate from Drift Data API
    2. If funding > threshold: go short (earn positive funding)
    3. If funding < -threshold: go long (earn negative funding)
    4. If funding near zero: hold or close existing position
    """

    def decide(self, market: MarketSnapshot) -> Intent:
        try:
            # Read config
            perp_market = self.config.get("market", "SOL-PERP")
            collateral_token = self.config.get("collateral_token", "USDC")
            collateral_amount = Decimal(str(self.config.get("collateral_amount", "100.0")))
            leverage = Decimal(str(self.config.get("leverage", "5.0")))
            threshold = Decimal(str(self.config.get("funding_rate_threshold", "0.01")))

            # Get funding rate from Drift Data API
            funding_rate = self._get_funding_rate(perp_market)
            logger.info(f"[{perp_market}] Funding rate: {funding_rate:.6f}, threshold: {threshold}")

            # Check if we have an existing position
            has_position = self.state.get("has_position", False)
            position_direction = self.state.get("position_direction", None)

            # Calculate size
            size_usd = collateral_amount * leverage

            if funding_rate > threshold:
                # Positive funding: longs pay shorts -> go short
                if has_position and position_direction == "short":
                    return Intent.hold(reason=f"Already short, funding={funding_rate:.6f}")

                if has_position and position_direction == "long":
                    # Close long first
                    logger.info(f"Closing long position before going short")
                    return Intent.perp_close(
                        market=perp_market,
                        collateral_token=collateral_token,
                        is_long=True,
                        protocol="drift",
                    )

                logger.info(f"Opening SHORT: funding={funding_rate:.6f} > {threshold}")
                return Intent.perp_open(
                    market=perp_market,
                    collateral_token=collateral_token,
                    collateral_amount=collateral_amount,
                    size_usd=size_usd,
                    is_long=False,
                    leverage=leverage,
                    protocol="drift",
                )

            elif funding_rate < -threshold:
                # Negative funding: shorts pay longs -> go long
                if has_position and position_direction == "long":
                    return Intent.hold(reason=f"Already long, funding={funding_rate:.6f}")

                if has_position and position_direction == "short":
                    logger.info(f"Closing short position before going long")
                    return Intent.perp_close(
                        market=perp_market,
                        collateral_token=collateral_token,
                        is_long=False,
                        protocol="drift",
                    )

                logger.info(f"Opening LONG: funding={funding_rate:.6f} < -{threshold}")
                return Intent.perp_open(
                    market=perp_market,
                    collateral_token=collateral_token,
                    collateral_amount=collateral_amount,
                    size_usd=size_usd,
                    is_long=True,
                    leverage=leverage,
                    protocol="drift",
                )

            else:
                # Funding near zero: close if we have a position
                if has_position:
                    is_long = position_direction == "long"
                    logger.info(f"Funding near zero ({funding_rate:.6f}), closing position")
                    return Intent.perp_close(
                        market=perp_market,
                        collateral_token=collateral_token,
                        is_long=is_long,
                        protocol="drift",
                    )

                return Intent.hold(reason=f"Funding rate near zero ({funding_rate:.6f})")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def on_intent_executed(self, intent, success: bool, result):
        """Track position state after execution."""
        if not success:
            return

        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return

        type_value = intent_type.value if hasattr(intent_type, "value") else str(intent_type)

        if type_value == "PERP_OPEN":
            self.state["has_position"] = True
            self.state["position_direction"] = "long" if intent.is_long else "short"
            logger.info(f"Opened {'LONG' if intent.is_long else 'SHORT'} position")
        elif type_value == "PERP_CLOSE":
            self.state["has_position"] = False
            self.state["position_direction"] = None
            logger.info("Closed position")

    def _get_funding_rate(self, market: str) -> Decimal:
        """Get current funding rate from Drift Data API.

        Returns the hourly funding rate as a decimal.
        Positive = longs pay shorts, Negative = shorts pay longs.
        """
        # GATEWAY_VIOLATION: Direct HTTP call to Drift Data API.
        # Funding rate data is not yet available via the gateway MarketSnapshot layer.
        # TODO: Add funding rate provider to gateway and use market.funding_rate() instead.
        try:
            from almanak.framework.connectors.drift import DriftDataClient, PERP_MARKET_SYMBOL_TO_INDEX

            client = DriftDataClient()
            market_index = PERP_MARKET_SYMBOL_TO_INDEX.get(market.upper(), 0)
            rates = client.get_funding_rates(market_index)
            if rates:
                return rates[0].funding_rate
        except Exception as e:
            logger.warning(f"Failed to fetch funding rate: {e}")

        return Decimal("0")

    # -- Teardown (required by framework) --

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self):
        from datetime import UTC, datetime
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self.state.get("has_position"):
            positions.append(
                PositionInfo(
                    position_type=PositionType.PERP,
                    position_id="drift_perp",
                    chain="solana",
                    protocol="drift",
                    value_usd=Decimal("0"),
                    details={
                        "market": self.config.get("market", "SOL-PERP"),
                        "direction": self.state.get("position_direction", "unknown"),
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None):
        if not self.state.get("has_position"):
            return []
        is_long = self.state.get("position_direction") == "long"
        return [
            Intent.perp_close(
                market=self.config.get("market", "SOL-PERP"),
                collateral_token=self.config.get("collateral_token", "USDC"),
                is_long=is_long,
                protocol="drift",
            )
        ]
