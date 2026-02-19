"""Bollinger Band Breakout Perps - Volatility-Driven Leveraged Positions.

A quant-oriented strategy that uses Bollinger Band breakouts to open leveraged
perpetual futures positions on GMX V2.

THESIS:
-------
When price breaks above the upper Bollinger Band, it signals strong upward
momentum (breakout). When it drops below the lower band, it signals downward
momentum. We use these breakouts to enter leveraged perp positions in the
direction of the breakout.

Additionally, we monitor bandwidth (band squeeze) as a precursor to breakouts.
Narrow bands suggest low volatility and a potential breakout is coming.

SIGNALS:
--------
- Price > Upper Band (percent_b > 1.0): Open LONG perp
- Price < Lower Band (percent_b < 0.0): Open SHORT perp
- Price between bands: HOLD or close existing position
- Band squeeze (low bandwidth): Prepare for breakout

RISK MANAGEMENT:
----------------
- Uses moderate leverage (3x default)
- Closes positions when price reverts to middle band
- Configurable hold time as backstop
"""

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_bb_perps",
    description="Bollinger Band breakout perpetual futures on GMX V2",
    version="1.0.0",
    author="QuantUser",
    tags=["volatility", "bollinger", "perps", "gmx", "breakout"],
    supported_chains=["arbitrum", "avalanche"],
    supported_protocols=["gmx_v2"],
    intent_types=["PERP_OPEN", "PERP_CLOSE", "HOLD"],
)
class BBPerpsStrategy(IntentStrategy):
    """Bollinger Band breakout strategy for GMX V2 perpetual futures.

    Opens leveraged positions on breakouts from Bollinger Bands:
    - Long when price pushes above upper band
    - Short when price drops below lower band
    - Closes when price reverts toward middle band
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        config_dict = self.config if isinstance(self.config, dict) else {}
        if hasattr(self.config, "get"):
            config_dict = self.config

        # GMX position params
        self.market = config_dict.get("market", "ETH/USD")
        self.collateral_token = config_dict.get("collateral_token", "WETH")
        self.collateral_amount = Decimal(str(config_dict.get("collateral_amount", "0.1")))
        self.leverage = Decimal(str(config_dict.get("leverage", "3.0")))

        # Bollinger Band params
        self.bb_period = int(config_dict.get("bb_period", 20))
        self.bb_std_dev = float(config_dict.get("bb_std_dev", 2.0))

        # Risk params
        self.max_slippage_pct = float(config_dict.get("max_slippage_pct", 2.0))
        self.hold_minutes = int(config_dict.get("hold_minutes", 60))

        # Force action for testing
        self.force_action = config_dict.get("force_action", None)

        # Position state
        self._has_position = False
        self._is_long = True
        self._position_opened_at = None
        self._position_size_usd = Decimal("0")
        self._entry_percent_b = None
        self._trades_opened = 0
        self._trades_closed = 0

        logger.info(
            f"BBPerpsStrategy initialized: "
            f"market={self.market}, "
            f"BB({self.bb_period}, {self.bb_std_dev}), "
            f"leverage={self.leverage}x, "
            f"collateral={self.collateral_amount} {self.collateral_token}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Decide based on Bollinger Band breakout signals."""
        try:
            index_token = self.market.split("/")[0]

            # Get current price
            try:
                current_price = market.price(index_token)
            except ValueError:
                logger.warning(f"Price for {index_token} unavailable, holding")
                return Intent.hold(reason=f"Price data unavailable for {index_token}")

            # Handle forced actions
            if self.force_action:
                logger.info(f"Force action: {self.force_action}")
                if self.force_action == "long":
                    return self._create_open_intent(current_price, is_long=True)
                elif self.force_action == "short":
                    return self._create_open_intent(current_price, is_long=False)
                elif self.force_action == "close":
                    return self._create_close_intent()

            # Get Bollinger Bands
            try:
                bb = market.bollinger_bands(
                    index_token,
                    period=self.bb_period,
                    std_dev=self.bb_std_dev,
                )
                percent_b = bb.percent_b
                bandwidth = bb.bandwidth
            except (ValueError, AttributeError):
                logger.warning(f"Bollinger Bands unavailable for {index_token}, holding")
                return Intent.hold(reason="Bollinger Bands data unavailable")

            logger.debug(
                f"BB: upper={bb.upper_band:.2f}, mid={bb.middle_band:.2f}, "
                f"lower={bb.lower_band:.2f}, %B={percent_b:.3f}, "
                f"bandwidth={bandwidth:.4f}, has_pos={self._has_position}"
            )

            now = datetime.now(UTC)

            # If we have a position, check for exit conditions
            if self._has_position:
                # Time-based exit (backstop)
                if self._position_opened_at:
                    time_held = now - self._position_opened_at
                    if time_held >= timedelta(minutes=self.hold_minutes):
                        logger.info(f"Hold time exceeded ({time_held}) - closing position")
                        return self._create_close_intent()

                # Mean reversion exit: close when price returns to middle band
                # For longs: close if percent_b drops below 0.5 (below middle band)
                # For shorts: close if percent_b rises above 0.5 (above middle band)
                if self._is_long and percent_b < 0.5:
                    logger.info(
                        f"LONG EXIT: price reverted to middle band (%B={percent_b:.3f} < 0.5)"
                    )
                    return self._create_close_intent()
                elif not self._is_long and percent_b > 0.5:
                    logger.info(
                        f"SHORT EXIT: price reverted to middle band (%B={percent_b:.3f} > 0.5)"
                    )
                    return self._create_close_intent()

                direction = "LONG" if self._is_long else "SHORT"
                return Intent.hold(
                    reason=f"Holding {direction} position, %B={percent_b:.3f}"
                )

            # No position - check for breakout entry
            # Upper band breakout -> LONG
            if percent_b > 1.0:
                logger.info(
                    f"UPPER BAND BREAKOUT: %B={percent_b:.3f} > 1.0, "
                    f"bandwidth={bandwidth:.4f} | Opening LONG"
                )
                self._entry_percent_b = percent_b
                return self._create_open_intent(current_price, is_long=True)

            # Lower band breakout -> SHORT
            elif percent_b < 0.0:
                logger.info(
                    f"LOWER BAND BREAKOUT: %B={percent_b:.3f} < 0.0, "
                    f"bandwidth={bandwidth:.4f} | Opening SHORT"
                )
                self._entry_percent_b = percent_b
                return self._create_open_intent(current_price, is_long=False)

            # Band squeeze detection (informational)
            if bandwidth < 0.03:
                logger.debug(f"Band squeeze detected: bandwidth={bandwidth:.4f}")

            return Intent.hold(
                reason=f"No breakout: %B={percent_b:.3f} within bands, bandwidth={bandwidth:.4f}"
            )

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    def _create_open_intent(self, current_price: Decimal, is_long: bool) -> Intent:
        # NOTE: Assumes collateral token tracks index token price (e.g. WETH collateral for ETH/USD).
        # For stablecoin collateral, fetch the collateral token's price separately.
        collateral_value_usd = self.collateral_amount * current_price
        position_size_usd = collateral_value_usd * self.leverage
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")

        direction = "LONG" if is_long else "SHORT"
        logger.info(
            f"Opening {direction}: "
            f"{format_token_amount_human(self.collateral_amount, self.collateral_token)} "
            f"({format_usd(collateral_value_usd)}) -> {format_usd(position_size_usd)} "
            f"@ {self.leverage}x leverage"
        )

        self._has_position = True
        self._is_long = is_long
        self._position_opened_at = datetime.now(UTC)
        self._position_size_usd = position_size_usd
        self._trades_opened += 1

        return Intent.perp_open(
            market=self.market,
            collateral_token=self.collateral_token,
            collateral_amount=self.collateral_amount,
            size_usd=position_size_usd,
            is_long=is_long,
            leverage=self.leverage,
            max_slippage=max_slippage,
            protocol="gmx_v2",
        )

    def _create_close_intent(self) -> Intent:
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")

        direction = "LONG" if self._is_long else "SHORT"
        logger.info(
            f"Closing {direction} position: {self.market}, "
            f"size={format_usd(self._position_size_usd)}"
        )

        self._has_position = False
        self._position_opened_at = None
        self._trades_closed += 1

        return Intent.perp_close(
            market=self.market,
            collateral_token=self.collateral_token,
            is_long=self._is_long,
            size_usd=self._position_size_usd,
            max_slippage=max_slippage,
            protocol="gmx_v2",
        )

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_bb_perps",
            "chain": self.chain,
            "config": {
                "market": self.market,
                "bb_params": f"({self.bb_period}, {self.bb_std_dev})",
                "leverage": str(self.leverage),
                "collateral": f"{self.collateral_amount} {self.collateral_token}",
            },
            "state": {
                "has_position": self._has_position,
                "is_long": self._is_long,
                "position_size_usd": str(self._position_size_usd),
                "entry_percent_b": self._entry_percent_b,
                "trades_opened": self._trades_opened,
                "trades_closed": self._trades_closed,
            },
        }

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._has_position and self._position_size_usd > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.PERP,
                    position_id=f"bb-perp-{self.market}-{self.chain}",
                    chain=self.chain,
                    protocol="gmx_v2",
                    value_usd=self._position_size_usd,
                    details={
                        "market": self.market,
                        "is_long": self._is_long,
                        "leverage": str(self.leverage),
                        "entry_percent_b": self._entry_percent_b,
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_bb_perps"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        intents = []
        if self._has_position:
            slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.01")
            intents.append(
                Intent.perp_close(
                    market=self.market,
                    collateral_token=self.collateral_token,
                    is_long=self._is_long,
                    size_usd=self._position_size_usd,
                    max_slippage=slippage,
                    protocol="gmx_v2",
                )
            )
            intents.append(
                Intent.swap(
                    from_token=self.collateral_token,
                    to_token="USDC",
                    amount="all",
                    max_slippage=slippage,
                )
            )
        return intents

    def to_dict(self) -> dict[str, Any]:
        metadata = self.get_metadata()
        config_dict = self.config if isinstance(self.config, dict) else {}
        return {
            "strategy_name": self.__class__.STRATEGY_NAME,
            "chain": self.chain,
            "wallet_address": self.wallet_address,
            "config": config_dict,
            "config_version": self.get_current_config_version(),
            "current_intent": self._current_intent.serialize() if self._current_intent else None,
            "metadata": metadata.to_dict() if metadata else None,
        }
