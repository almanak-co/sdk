"""TraderJoe V2 LP Lifecycle on Avalanche.

Full LP lifecycle testing LP_OPEN and LP_CLOSE on TraderJoe V2's Liquidity Book.

Phase 1 (force_action=open): OPEN — provide liquidity across discrete bins
Phase 2 (force_action=close): CLOSE — remove all liquidity and collect fees

TraderJoe V2 Liquidity Book uses discrete price bins with ERC1155-like fungible
LP tokens (not NFT positions like Uniswap V3). This means:
- Positions are identified by wallet + pool + bin IDs (no NFT token ID)
- LP_CLOSE queries on-chain for LP token balances per bin
- A single removeLiquidity call handles all bins

Coverage gaps filled:
- First dedicated LP_CLOSE lifecycle test on TraderJoe V2
- Validates bin-based LP close mechanics vs tick-based (Uniswap V3)
- Tests LP lifecycle on Liquidity Book model end-to-end

Kitchen Loop — VIB-195

Usage:
    # Phase 1: Open LP position
    almanak strat run -d strategies/demo/traderjoe_lp_lifecycle --network anvil --once

    # Phase 2: Close LP position (change force_action to "close" in config.json)
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)


@dataclass
class LPLifecycleConfig:
    """Configuration for TraderJoe V2 LP lifecycle strategy."""

    chain: str = "avalanche"
    network: str = "anvil"
    pool: str = "WAVAX/USDC/20"
    range_width_pct: Decimal = field(default_factory=lambda: Decimal("0.10"))
    amount_x: Decimal = field(default_factory=lambda: Decimal("0.001"))
    amount_y: Decimal = field(default_factory=lambda: Decimal("3"))
    force_action: str = "open"

    def __post_init__(self):
        if isinstance(self.range_width_pct, str):
            self.range_width_pct = Decimal(self.range_width_pct)
        if isinstance(self.amount_x, str):
            self.amount_x = Decimal(self.amount_x)
        if isinstance(self.amount_y, str):
            self.amount_y = Decimal(self.amount_y)


@almanak_strategy(
    name="demo_traderjoe_lp_lifecycle",
    description="TraderJoe V2 LP lifecycle (OPEN + CLOSE) on Avalanche — Liquidity Book",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "traderjoe", "lp", "lifecycle", "avalanche", "liquidity-book"],
    supported_chains=["avalanche"],
    supported_protocols=["traderjoe_v2"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="avalanche",
)
class TraderJoeLPLifecycleStrategy(IntentStrategy[LPLifecycleConfig]):
    """TraderJoe V2 LP lifecycle: OPEN then CLOSE.

    Config:
        force_action: "open" or "close" to control which phase executes
        pool: Pool identifier (e.g., "WAVAX/USDC/20")
        range_width_pct: Price range width as decimal (0.10 = 10%)
        amount_x: Amount of token X (e.g., WAVAX)
        amount_y: Amount of token Y (e.g., USDC)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.pool = self.config.pool
        pool_parts = self.pool.split("/")
        self.token_x = pool_parts[0] if len(pool_parts) > 0 else "WAVAX"
        self.token_y = pool_parts[1] if len(pool_parts) > 1 else "USDC"
        self.bin_step = int(pool_parts[2]) if len(pool_parts) > 2 else 20

        self.range_width_pct = self.config.range_width_pct
        self.amount_x = self.config.amount_x
        self.amount_y = self.config.amount_y
        self.force_action = str(self.config.force_action).lower()

        self._position_bin_ids: list[int] = []
        self._last_x_price: Decimal | None = None

        logger.info(
            f"TraderJoeLPLifecycle: force_action={self.force_action}, "
            f"pool={self.pool}, "
            f"amounts={self.amount_x} {self.token_x} + {self.amount_y} {self.token_y}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Execute OPEN or CLOSE based on force_action config."""
        try:
            x_price = market.price(self.token_x)
            y_price = market.price(self.token_y)
            current_price = x_price / y_price
            self._last_x_price = x_price
            logger.info(
                f"Prices: {self.token_x}=${x_price:.2f}, "
                f"{self.token_y}=${y_price:.4f}, "
                f"pair_price={current_price:.4f}"
            )
        except (ValueError, KeyError) as exc:
            logger.warning(f"Price fetch failed: {exc}")
            current_price = None

        if self.force_action == "open":
            return self._open(market, current_price)
        elif self.force_action == "close":
            return self._close()
        else:
            return Intent.hold(reason=f"Unknown force_action: {self.force_action}")

    def _open(self, market: MarketSnapshot, current_price: Decimal | None) -> Intent:
        """OPEN phase: provide liquidity across bins."""
        if current_price is None:
            return Intent.hold(reason="Cannot open LP without price data")

        try:
            x_bal = market.balance(self.token_x)
            y_bal = market.balance(self.token_y)
            if x_bal.balance < self.amount_x:
                return Intent.hold(
                    reason=f"Insufficient {self.token_x}: {x_bal.balance} < {self.amount_x}"
                )
            if y_bal.balance < self.amount_y:
                return Intent.hold(
                    reason=f"Insufficient {self.token_y}: {y_bal.balance} < {self.amount_y}"
                )
        except ValueError:
            logger.warning("Balance check unavailable, proceeding with OPEN")

        half_width = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        logger.info(
            f"LP_OPEN: {self.amount_x} {self.token_x} + {self.amount_y} {self.token_y}, "
            f"range [{range_lower:.4f} - {range_upper:.4f}]"
        )
        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount_x,
            amount1=self.amount_y,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="traderjoe_v2",
        )

    def _close(self) -> Intent:
        """CLOSE phase: remove all liquidity from the position."""
        logger.info(f"LP_CLOSE: pool={self.pool}")
        return Intent.lp_close(
            position_id=self.pool,
            pool=self.pool,
            collect_fees=True,
            protocol="traderjoe_v2",
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        if not success:
            logger.warning(f"{intent.intent_type.value} FAILED")
            return

        if intent.intent_type.value == "LP_OPEN":
            bin_ids = getattr(result, "bin_ids", None)
            if bin_ids:
                self._position_bin_ids = list(bin_ids)
                logger.info(f"LP_OPEN SUCCESS: bin_ids={bin_ids[:3]}...")
            else:
                logger.info("LP_OPEN SUCCESS (no bin_ids in result)")

        elif intent.intent_type.value == "LP_CLOSE":
            self._position_bin_ids = []
            logger.info("LP_CLOSE SUCCESS: position closed")

    def get_open_positions(self):
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []
        if self._position_bin_ids:
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=f"traderjoe-lp-lifecycle-{self.pool}",
                    chain=self.chain,
                    protocol="traderjoe_v2",
                    # Estimate: use amount_y as USD proxy (stablecoin) + amount_x at last known price
                    value_usd=self.amount_x * self._last_x_price + self.amount_y
                    if self._last_x_price
                    else self.amount_y,
                    details={
                        "pool": self.pool,
                        "bin_ids": self._position_bin_ids,
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", self.STRATEGY_NAME),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode=None, market=None) -> list[Intent]:
        if self._position_bin_ids:
            return [
                Intent.lp_close(
                    position_id=self.pool,
                    pool=self.pool,
                    collect_fees=True,
                    protocol="traderjoe_v2",
                )
            ]
        return []

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_traderjoe_lp_lifecycle",
            "chain": "avalanche",
            "pool": self.pool,
            "force_action": self.force_action,
            "position_bin_ids": self._position_bin_ids[:5] if self._position_bin_ids else [],
        }
