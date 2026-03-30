"""Morpho Blue Collateral Rotator — Paper Trading Demo on Ethereum.

Kitchen Loop iteration 103 (VIB-1551).

This strategy validates the paper trading pipeline with a multi-step lending
rotation. It supplies wstETH as collateral to one of two Morpho Blue markets,
then rotates to the other when price momentum suggests a change is warranted.

Markets:
  - Market A: wstETH/USDC  (0xb323...) -- wstETH collateral, USDC loan token
  - Market B: wstETH/WETH  (0xc54d...) -- wstETH collateral, WETH loan token

Rotation signal: wstETH 7-day price momentum.
  - Rising strongly (>= +rotation_threshold_pct): prefer Market B (ETH-correlated)
  - Falling/stable (< +rotation_threshold_pct): prefer Market A (USDC-stable)

This exercises the paper trader's ability to handle:
  - SUPPLY → multi-tick HOLD → WITHDRAW → SUPPLY (rotation pattern)
  - PnL equity curve with collateral positions
  - JSON log persistence across ticks

Dedupe key: backtest:morpho_blue:ethereum:paper

USAGE:
------
    # Paper trade for 5 ticks (auto-rotates at tick 3 by default)
    almanak strat backtest paper start \\
        -s demo_morpho_blue_collateral_rotator_ethereum \\
        --chain ethereum \\
        --max-ticks 5 \\
        --tick-interval 30 \\
        --foreground

    # Run directly on Anvil (single iteration)
    almanak strat run -d strategies/demo/morpho_blue_collateral_rotator_ethereum \\
        --network anvil --once
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


# Morpho Blue markets on Ethereum
MARKET_WSTETH_USDC = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"
MARKET_WSTETH_WETH = "0xc54d7acf14de29e0e5527cabd7a576506870346a78a11a6762e2cca66322ec41"


@almanak_strategy(
    name="demo_morpho_blue_collateral_rotator_ethereum",
    description=(
        "Paper trading demo: supply wstETH collateral to the best Morpho Blue market "
        "(wstETH/USDC vs wstETH/WETH) and rotate based on wstETH price momentum. "
        "First paper trade to validate multi-step lending rotation on Ethereum."
    ),
    version="1.0.0",
    author="Kitchen Loop (VIB-1551)",
    tags=["demo", "paper-trading", "lending", "morpho-blue", "rotation", "ethereum", "backtesting"],
    supported_chains=["ethereum"],
    default_chain="ethereum",
    supported_protocols=["morpho_blue"],
    intent_types=["SUPPLY", "WITHDRAW", "HOLD"],
)
class MorphoBlueCollateralRotatorStrategy(IntentStrategy):
    """Morpho Blue collateral rotator for paper trading validation.

    Configuration (config.json):
        collateral_token: Token to supply as collateral (default: wstETH)
        collateral_amount: Amount to supply (default: 0.05)
        market_usdc_id: wstETH/USDC market ID
        market_weth_id: wstETH/WETH market ID
        rotation_threshold_bps: Min price change (bps) to trigger rotation (default: 30)
        cooldown_ticks: Min ticks between rotations (default: 3)
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.collateral_token: str = self.get_config("collateral_token", "wstETH")
        self.collateral_amount: Decimal = Decimal(str(self.get_config("collateral_amount", "0.05")))
        self.market_usdc_id: str = self.get_config("market_usdc_id", MARKET_WSTETH_USDC)
        self.market_weth_id: str = self.get_config("market_weth_id", MARKET_WSTETH_WETH)
        self.rotation_threshold: Decimal = Decimal(str(self.get_config("rotation_threshold_bps", 30))) / Decimal("10000")
        self.cooldown_ticks: int = int(self.get_config("cooldown_ticks", 3))

        # State machine: idle -> supplying -> supplied -> withdrawing -> withdrawn -> supplying -> ...
        self._state: str = "idle"
        self._prev_stable_state: str = "idle"
        self._current_market: str = "usdc"  # which market we're in: "usdc" or "weth"
        self._target_market: str = "usdc"
        self._ticks_since_rotation: int = 0
        self._entry_price: Decimal | None = None

        logger.info(
            "MorphoBlueCollateralRotatorStrategy initialized: "
            f"{self.collateral_amount} {self.collateral_token}, "
            f"threshold={self.rotation_threshold * 100:.2f}%, cooldown={self.cooldown_ticks} ticks"
        )

    def _best_market(self, collateral_price: Decimal) -> str:
        """Determine the preferred market based on wstETH price momentum.

        If wstETH has risen by >= rotation_threshold since entry, prefer the
        WETH market (correlated asset, higher LLTV). Otherwise prefer USDC
        (stable, safer benchmark).
        """
        if self._entry_price is None or self._entry_price == Decimal("0"):
            return "usdc"
        price_change = (collateral_price - self._entry_price) / self._entry_price
        return "weth" if price_change >= self.rotation_threshold else "usdc"

    def _market_id(self, market: str) -> str:
        return self.market_weth_id if market == "weth" else self.market_usdc_id

    def decide(self, market: MarketSnapshot) -> Intent:
        """Evaluate rotation opportunity and advance the state machine."""
        try:
            collateral_price = market.price(self.collateral_token)
        except (ValueError, KeyError) as e:
            logger.warning(f"Price unavailable for {self.collateral_token}: {e}")
            return Intent.hold(reason=f"Price data unavailable: {e}")

        self._ticks_since_rotation += 1
        logger.info(
            f"[state={self._state}, market={self._current_market}, "
            f"ticks_since_rotation={self._ticks_since_rotation}] "
            f"wstETH=${collateral_price:.2f}"
        )

        # --- IDLE: initial supply ---
        if self._state == "idle":
            self._entry_price = collateral_price
            self._target_market = self._best_market(collateral_price)
            return self._do_supply(self._target_market, collateral_price)

        # --- SUPPLIED: evaluate rotation ---
        if self._state == "supplied":
            best = self._best_market(collateral_price)
            cooldown_passed = self._ticks_since_rotation >= self.cooldown_ticks

            if best != self._current_market and cooldown_passed:
                logger.info(
                    f"ROTATION triggered: {self._current_market} -> {best} "
                    f"(price=${collateral_price:.2f}, ticks={self._ticks_since_rotation})"
                )
                self._target_market = best
                return self._do_withdraw(self._current_market, collateral_price)

            return Intent.hold(
                reason=(
                    f"Holding in {self._current_market} market "
                    f"(best={best}, cooldown_passed={cooldown_passed}, "
                    f"ticks={self._ticks_since_rotation})"
                )
            )

        # --- WITHDRAWN: complete rotation by supplying to new market ---
        if self._state == "withdrawn":
            return self._do_supply(self._target_market, collateral_price)

        # --- Transitional states: revert to last stable state ---
        if self._state in ("supplying", "withdrawing"):
            logger.warning(f"Stuck in transitional state '{self._state}', reverting to '{self._prev_stable_state}'")
            self._state = self._prev_stable_state

        return Intent.hold(reason=f"Recovering (state={self._state})")

    def _do_supply(self, market: str, price: Decimal) -> Intent:
        logger.info(
            f"SUPPLY {self.collateral_amount} {self.collateral_token} "
            f"-> {market} market (${price:.2f})"
        )
        self._entry_price = price
        self._prev_stable_state = self._state
        self._state = "supplying"
        return Intent.supply(
            protocol="morpho_blue",
            token=self.collateral_token,
            amount=self.collateral_amount,
            use_as_collateral=True,
            market_id=self._market_id(market),
            chain=self.chain,
        )

    def _do_withdraw(self, market: str, price: Decimal) -> Intent:
        logger.info(
            f"WITHDRAW {self.collateral_amount} {self.collateral_token} "
            f"from {market} market (${price:.2f})"
        )
        self._prev_stable_state = self._state
        self._state = "withdrawing"
        return Intent.withdraw(
            protocol="morpho_blue",
            token=self.collateral_token,
            amount=self.collateral_amount,
            withdraw_all=True,
            market_id=self._market_id(market),
            chain=self.chain,
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        if success:
            if self._state == "supplying":
                self._state = "supplied"
                self._current_market = self._target_market
                self._ticks_since_rotation = 0
                logger.info(
                    f"SUPPLY succeeded: now in {self._current_market} market "
                    f"(market_id={self._market_id(self._current_market)[:18]}...)"
                )
            elif self._state == "withdrawing":
                self._state = "withdrawn"
                logger.info(f"WITHDRAW succeeded: ready to supply to {self._target_market} market")
        else:
            logger.warning(f"Intent failed in state '{self._state}', reverting to '{self._prev_stable_state}'")
            self._state = self._prev_stable_state

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_morpho_blue_collateral_rotator_ethereum",
            "chain": self.chain,
            "state": self._state,
            "current_market": self._current_market,
            "ticks_since_rotation": self._ticks_since_rotation,
            "entry_price": str(self._entry_price) if self._entry_price else None,
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "prev_stable_state": self._prev_stable_state,
            "current_market": self._current_market,
            "target_market": self._target_market,
            "ticks_since_rotation": self._ticks_since_rotation,
            "entry_price": str(self._entry_price) if self._entry_price else None,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "state" in state:
            self._state = state["state"]
        if "prev_stable_state" in state:
            self._prev_stable_state = state["prev_stable_state"]
        if "current_market" in state:
            self._current_market = state["current_market"]
        if "target_market" in state:
            self._target_market = state["target_market"]
        if "ticks_since_rotation" in state:
            self._ticks_since_rotation = int(state["ticks_since_rotation"])
        entry_price = state.get("entry_price")
        if entry_price in (None, ""):
            self._entry_price = None
        else:
            self._entry_price = Decimal(str(entry_price))
        logger.info(f"Restored state: {self._state}, market={self._current_market}")

    # -------------------------------------------------------------------------
    # Teardown
    # -------------------------------------------------------------------------

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []
        if self._state == "supplied":
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"morpho-{self._current_market}-{self.collateral_token}",
                    chain=self.chain,
                    protocol="morpho_blue",
                    value_usd=self.collateral_amount * (self._entry_price or Decimal("0")),
                    details={
                        "market": self._current_market,
                        "market_id": self._market_id(self._current_market),
                        "asset": self.collateral_token,
                        "amount": str(self.collateral_amount),
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market: MarketSnapshot | None = None) -> list[Intent]:
        if self._state != "supplied":
            return []
        return [
            Intent.withdraw(
                protocol="morpho_blue",
                token=self.collateral_token,
                amount=self.collateral_amount,
                withdraw_all=True,
                market_id=self._market_id(self._current_market),
                chain=self.chain,
            )
        ]
