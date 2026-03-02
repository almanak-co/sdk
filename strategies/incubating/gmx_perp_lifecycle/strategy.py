"""GMX V2 Perpetual Futures Lifecycle Strategy.

Kitchen Loop iteration 27 -- first test of PerpOpenIntent and PerpCloseIntent.
Tests the GMX V2 connector end-to-end on Arbitrum with USDC collateral
(ERC-20 approval path, different from WETH native token path).

Lifecycle modes via force_action config:
  "open"      - Open a single long position (for isolated testing)
  "close"     - Close an existing position (for isolated testing)
  "lifecycle" - Open then close on subsequent iterations
  null/None   - Same as "lifecycle"
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="gmx_perp_lifecycle",
    description="GMX V2 perpetual futures lifecycle test (open + close)",
    version="1.0.0",
    author="Kitchen Loop",
    tags=["perpetuals", "gmx", "lifecycle", "test"],
    supported_chains=["arbitrum", "avalanche"],
    supported_protocols=["gmx_v2"],
    intent_types=["PERP_OPEN", "PERP_CLOSE", "HOLD"],
)
class GMXPerpLifecycleStrategy(IntentStrategy):
    """Lifecycle strategy for testing GMX V2 perp open/close on Anvil.

    Uses USDC collateral to exercise the ERC-20 approval + sendTokens path
    (distinct from WETH which uses sendWnt with msg.value).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        config = self.config if isinstance(self.config, dict) else {}
        if hasattr(self.config, "get") and not isinstance(self.config, dict):
            config = self.config

        self.market = config.get("market", "ETH/USD")
        self.collateral_token = config.get("collateral_token", "USDC")
        self.collateral_amount = Decimal(str(config.get("collateral_amount", "10")))
        self.leverage = Decimal(str(config.get("leverage", "2.0")))
        self.is_long = config.get("is_long", True)
        self.max_slippage_pct = Decimal(str(config.get("max_slippage_pct", "2.0")))
        self.force_action = config.get("force_action", None)

        # State machine: idle -> open -> closed
        self._loop_state = "idle"
        self._previous_stable_state = "idle"
        self._position_size_usd = Decimal("0")

        logger.info(
            f"GMXPerpLifecycle initialized: market={self.market}, "
            f"collateral={self.collateral_amount} {self.collateral_token}, "
            f"leverage={self.leverage}x, direction={'LONG' if self.is_long else 'SHORT'}, "
            f"force_action={self.force_action}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Main decision: open or close a perp position based on state."""
        try:
            index_token = self.market.split("/")[0]

            try:
                current_price = market.price(index_token)
                logger.info(f"{index_token} price: ${current_price:,.2f}")
            except (ValueError, Exception) as e:
                logger.warning(f"Price unavailable for {index_token}: {e}, using fallback")
                current_price = Decimal("3500") if index_token == "ETH" else Decimal("95000")

            # Force action mode (for isolated Anvil testing)
            if self.force_action == "open":
                logger.info("Force action: OPEN")
                return self._create_open_intent(current_price)

            if self.force_action == "close":
                logger.info("Force action: CLOSE")
                return self._create_close_intent()

            # Lifecycle mode (open -> close)
            if self._loop_state == "idle":
                logger.info("Lifecycle: opening position")
                self._previous_stable_state = "idle"
                self._loop_state = "opening"
                return self._create_open_intent(current_price)

            if self._loop_state == "open":
                logger.info("Lifecycle: closing position")
                self._previous_stable_state = "open"
                self._loop_state = "closing"
                return self._create_close_intent()

            if self._loop_state == "closed":
                logger.info("Lifecycle complete -- both open and close executed")
                return Intent.hold(reason="Lifecycle complete")

            # Transitional states (opening, closing) -- hold while waiting
            logger.info(f"In transitional state '{self._loop_state}', holding")
            return Intent.hold(reason=f"Waiting for {self._loop_state} to complete")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def _create_open_intent(self, current_price: Decimal) -> Intent:
        """Create PerpOpenIntent with USDC collateral."""
        # Position size = collateral_value_usd * leverage
        # For USDC: collateral_amount IS the USD value (1 USDC = $1)
        collateral_value_usd = self.collateral_amount * current_price if self.collateral_token != "USDC" else self.collateral_amount
        position_size_usd = collateral_value_usd * self.leverage

        max_slippage = self.max_slippage_pct / Decimal("100")

        self._position_size_usd = position_size_usd

        direction = "LONG" if self.is_long else "SHORT"
        logger.info(
            f"Opening {direction}: {self.collateral_amount} {self.collateral_token} "
            f"({format_usd(collateral_value_usd)}) -> {format_usd(position_size_usd)} position "
            f"@ {self.leverage}x leverage, slippage={self.max_slippage_pct}%"
        )

        return Intent.perp_open(
            market=self.market,
            collateral_token=self.collateral_token,
            collateral_amount=self.collateral_amount,
            size_usd=position_size_usd,
            is_long=self.is_long,
            leverage=self.leverage,
            max_slippage=max_slippage,
            protocol="gmx_v2",
        )

    def _create_close_intent(self) -> Intent:
        """Create PerpCloseIntent to close the full position."""
        max_slippage = self.max_slippage_pct / Decimal("100")

        # Use tracked size if available, otherwise close full position (size_usd=None)
        close_size = self._position_size_usd if self._position_size_usd > 0 else None

        direction = "LONG" if self.is_long else "SHORT"
        logger.info(
            f"Closing {direction}: {self.market}, size={format_usd(close_size) if close_size else 'FULL'}"
        )

        return Intent.perp_close(
            market=self.market,
            collateral_token=self.collateral_token,
            is_long=self.is_long,
            size_usd=close_size,
            max_slippage=max_slippage,
            protocol="gmx_v2",
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Advance state machine on successful execution."""
        intent_type = intent.intent_type.value if hasattr(intent, "intent_type") else str(intent)

        if success:
            logger.info(f"Intent {intent_type} executed successfully")
            if self._loop_state == "opening":
                self._loop_state = "open"
                logger.info("State: opening -> open")
            elif self._loop_state == "closing":
                self._loop_state = "closed"
                logger.info("State: closing -> closed")
        else:
            logger.warning(f"Intent {intent_type} failed, reverting to {self._previous_stable_state}")
            self._loop_state = self._previous_stable_state

    # --- Teardown ---

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self):
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._loop_state == "open" and self._position_size_usd > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.PERP,
                    position_id=f"gmx-{self.market}-{self.chain}",
                    chain=self.chain,
                    protocol="gmx_v2",
                    value_usd=self._position_size_usd,
                    details={
                        "market": self.market,
                        "is_long": self.is_long,
                        "leverage": str(self.leverage),
                        "collateral_token": self.collateral_token,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id="gmx_perp_lifecycle",
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None):
        from almanak.framework.teardown import TeardownMode

        intents = []
        if self._loop_state == "open":
            slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.01")
            intents.append(
                Intent.perp_close(
                    market=self.market,
                    collateral_token=self.collateral_token,
                    is_long=self.is_long,
                    size_usd=self._position_size_usd,
                    max_slippage=slippage,
                    protocol="gmx_v2",
                )
            )
        return intents
