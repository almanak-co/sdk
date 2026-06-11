"""0G Chain LP Strategy — Uniswap V3 LP via JAINE DEX.

Manages a Uniswap V3 concentrated liquidity position on 0G Chain using JAINE DEX.

What this strategy does:
    1. Opens a concentrated liquidity position (W0G/st0G) around current price
    2. Monitors if the position is still in range
    3. Can close the position on demand (force_action="close")

The W0G/st0G pool is the primary liquidity pool on 0G Chain — it pairs the
wrapped native token (W0G) with Gimo Finance's liquid staking derivative (st0G).

Usage:
    almanak strat run -d almanak/demo_strategies/0g_lp --network anvil --once
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
    name="demo_0g_lp",
    description="Uniswap V3 concentrated liquidity LP on 0G Chain via JAINE DEX",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "0g", "zerog", "lp", "uniswap-v3", "jaine", "liquidity"],
    supported_chains=["zerog"],
    supported_protocols=["uniswap_v3"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="zerog",
    quote_asset="USD",
)
class ZeroGLPStrategy(IntentStrategy):
    """Uniswap V3 LP strategy on 0G Chain (JAINE DEX).

    Configuration (config.json):
        pool: Pool identifier as TOKEN0/TOKEN1/FEE (default: W0G/st0G/3000)
        range_width_pct: Total width of price range as decimal (default: 0.50 = 50%)
        amount0: Amount of token0 to provide (default: 10 W0G)
        amount1: Amount of token1 to provide (default: 10 st0G)
        force_action: Force "open" or "close" for testing (default: "")
        position_id: NFT ID of position to close (when force_action="close")
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.pool = self.get_config("pool", "W0G/st0G/3000")
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "W0G"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "st0G"
        self.fee_tier = int(pool_parts[2]) if len(pool_parts) > 2 else 3000

        self.range_width_pct = Decimal(str(self.get_config("range_width_pct", "0.50")))
        self.amount0 = Decimal(str(self.get_config("amount0", "10")))
        self.amount1 = Decimal(str(self.get_config("amount1", "10")))
        self.force_action = str(self.get_config("force_action", "")).lower()
        self.position_id = self.get_config("position_id", None)

        self._current_position_id: str | None = None

        logger.info(
            f"ZeroGLP initialized: pool={self.pool}, "
            f"range_width={self.range_width_pct * 100}%, "
            f"amounts={self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        # Check forced close BEFORE price lookup — LP_CLOSE doesn't need price data
        if self.force_action == "close":
            pid = self._current_position_id or self.position_id
            if not pid:
                return Intent.hold(reason="Close requested but no position_id")
            return self._create_close_intent(pid)

        # For LP_OPEN, we need to compute a price range.
        # The W0G/st0G pair should trade near 1:1 (st0G is a staking derivative of A0GI).
        # On-chain tick = -1885 -> price ~0.83 (st0G per W0G).
        # Use a default price if market data unavailable.
        current_price = Decimal("0.83")  # Default from on-chain observation
        try:
            token0_price_usd = market.price(self.token0_symbol)
            token1_price_usd = market.price(self.token1_symbol)
            if token1_price_usd > 0:
                current_price = token0_price_usd / token1_price_usd
        except (ValueError, KeyError, ZeroDivisionError) as e:
            logger.debug(f"Price data unavailable ({e}), using default price {current_price}")

        # Handle forced open
        if self.force_action == "open":
            logger.info("Forced action: OPEN LP position")
            return self._create_open_intent(current_price)

        # If we have a position, hold and monitor
        if self._current_position_id:
            return Intent.hold(reason=f"Position {self._current_position_id} exists - monitoring")

        logger.info("No position found - opening new LP position")
        return self._create_open_intent(current_price)

    def _create_open_intent(self, current_price: Decimal) -> Intent:
        half_width = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        logger.info(
            f"LP_OPEN: {self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}, "
            f"range [{range_lower:.4f} - {range_upper:.4f}]"
        )

        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="uniswap_v3",
            protocol_params={"lp_slippage": 1.0},
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
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=str(position_id),
                    chain=self.chain,
                    protocol="uniswap_v3",
                    value_usd=Decimal("0"),  # No reliable USD pricing for 0G tokens
                    details={
                        "pool": self.pool,
                        "token0": self.token0_symbol,
                        "token1": self.token1_symbol,
                    },
                )
            )

        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "demo_0g_lp"),
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
            "strategy": "demo_0g_lp",
            "chain": self.chain,
            "pool": self.pool,
            "position_id": self._current_position_id,
        }
