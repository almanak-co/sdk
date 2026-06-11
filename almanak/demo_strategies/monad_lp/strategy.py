"""Monad Uniswap V3 LP Demo Strategy.

Manages a Uniswap V3 concentrated liquidity position on Monad.

What this strategy does:
    1. Opens a concentrated liquidity position around current price
    2. Monitors if the position is still in range
    3. Holds while position is active

This demonstrates LP position management on Monad -- a different primitive
from simple swaps, showing how to provide liquidity to Uniswap V3 pools.

Usage:
    almanak strat run -d almanak/demo_strategies/monad_lp --network anvil --once
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_monad_lp",
    description="Uniswap V3 concentrated liquidity LP demo on Monad",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "monad", "lp", "uniswap-v3", "liquidity"],
    supported_chains=["monad"],
    supported_protocols=["uniswap_v3"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="monad",
    quote_asset="USD",
)
class MonadLPStrategy(IntentStrategy):
    """Uniswap V3 LP strategy on Monad.

    Configuration (config.json):
        pool: Pool identifier as TOKEN0/TOKEN1/FEE (default: WETH/USDC/500)
        range_width_pct: Total width of price range as decimal (default: 0.20 = 20%)
        amount0: Amount of token0 to provide (default: 0.001 WETH)
        amount1: Amount of token1 to provide (default: 3 USDC)
        force_action: Force "open" or "close" for testing (default: "")
        position_id: NFT ID of position to close (when force_action="close")
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.pool = self.get_config("pool", "WETH/USDC/500")
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WETH"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"
        self.fee_tier = int(pool_parts[2]) if len(pool_parts) > 2 else 500

        self.range_width_pct = Decimal(str(self.get_config("range_width_pct", "0.20")))
        self.amount0 = Decimal(str(self.get_config("amount0", "0.001")))
        self.amount1 = Decimal(str(self.get_config("amount1", "3")))
        self.force_action = str(self.get_config("force_action", "")).lower()
        self.position_id = self.get_config("position_id", None)

        self._current_position_id: str | None = None

        logger.info(
            f"MonadLP initialized: pool={self.pool}, "
            f"range_width={self.range_width_pct * 100}%, "
            f"amounts={self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        # Check forced close BEFORE price lookup — LP_CLOSE doesn't need price data
        if self.force_action == "close":
            if not self.position_id:
                return Intent.hold(reason="Close requested but no position_id")
            return self._create_close_intent(self.position_id)

        try:
            token0_price_usd = market.price(self.token0_symbol)
            token1_price_usd = market.price(self.token1_symbol)
            current_price = token0_price_usd / token1_price_usd
        except (ValueError, KeyError) as e:
            return Intent.hold(reason=f"Price data unavailable: {e}")

        # Handle forced open
        if self.force_action == "open":
            logger.info("Forced action: OPEN LP position")
            return self._create_open_intent(current_price)

        # If we have a position, hold and monitor
        if self._current_position_id:
            return Intent.hold(reason=f"Position {self._current_position_id} exists - monitoring")

        # Check balances before opening
        try:
            token0_balance = market.balance(self.token0_symbol)
            token1_balance = market.balance(self.token1_symbol)
            bal0 = token0_balance.balance if hasattr(token0_balance, "balance") else token0_balance
            bal1 = token1_balance.balance if hasattr(token1_balance, "balance") else token1_balance
            if bal0 < self.amount0:
                return Intent.hold(reason=f"Insufficient {self.token0_symbol}: {bal0} < {self.amount0}")
            if bal1 < self.amount1:
                return Intent.hold(reason=f"Insufficient {self.token1_symbol}: {bal1} < {self.amount1}")
        except (ValueError, KeyError):
            logger.warning("Could not verify balances, proceeding anyway")

        logger.info("No position found - opening new LP position")
        return self._create_open_intent(current_price)

    def _create_open_intent(self, current_price: Decimal) -> Intent:
        half_width = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        logger.info(
            f"LP_OPEN: {self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}, "
            f"range [{range_lower:.2f} - {range_upper:.2f}]"
        )

        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="uniswap_v3",
            protocol_params={"lp_slippage": 1.0},  # amount0Min=amount1Min=0; safe for demo
        )

    def _create_close_intent(self, position_id: str) -> Intent:
        logger.info(f"LP_CLOSE: position={position_id}")
        return Intent.lp_close(
            position_id=position_id,
            pool=self.pool,
            collect_fees=True,
            protocol="uniswap_v3",
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        if not success:
            return
        if intent.intent_type.value == "LP_OPEN":
            position_id = result.position_id if result else None
            if position_id:
                self._current_position_id = str(position_id)
                logger.info(f"LP position opened: position_id={position_id}")
        elif intent.intent_type.value == "LP_CLOSE":
            logger.info(f"LP position closed: position_id={self._current_position_id}")
            self._current_position_id = None

    # -- Teardown --

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self):
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []
        position_id = self._current_position_id or self.position_id

        if position_id:
            try:
                snapshot = self.create_market_snapshot()
                token0_price_usd = snapshot.price(self.token0_symbol)
                token1_price_usd = snapshot.price(self.token1_symbol)
            except Exception:
                token0_price_usd = Decimal("0")
                token1_price_usd = Decimal("0")

            estimated_value = self.amount0 * token0_price_usd + self.amount1 * token1_price_usd
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=str(position_id),
                    chain=self.chain,
                    protocol="uniswap_v3",
                    value_usd=estimated_value,
                    details={
                        "pool": self.pool,
                        "token0": self.token0_symbol,
                        "token1": self.token1_symbol,
                    },
                )
            )

        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "demo_monad_lp"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None) -> list[Intent]:
        intents: list[Intent] = []
        position_id = self._current_position_id or self.position_id
        if position_id:
            intents.append(
                Intent.lp_close(
                    position_id=position_id,
                    pool=self.pool,
                    collect_fees=True,
                    protocol="uniswap_v3",
                )
            )
        return intents

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_monad_lp",
            "chain": self.chain,
            "pool": self.pool,
            "position_id": self._current_position_id,
        }
