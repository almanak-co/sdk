"""
Mean Reversion Swap Strategy: edge_sweth_mean_rev

Captures the swETH premium over ETH by selling swETH for ETH via Uniswap V3.
This is a one-shot trade: once the swap executes, the position is complete.

Edge Signal: cfc82bb4-eb44-42b0-b085-84a7095a42a5
Type: HIGH_CONVICTION_SYNTHESIS (LST_DEPEG_RISK x2)
Thesis: swETH is trading at ~11.5% premium to its 1:1 ETH peg.
        The premium should mean-revert. Sell swETH for ETH to lock it in.

State machine:
  idle     -> check premium, swap if entry conditions met
  swapped  -> trade complete, hold ETH (strategy finished)

Chain: ethereum
Protocol: uniswap_v3
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

logger = logging.getLogger(__name__)

# swETH (Swell liquid staking token) address on Ethereum mainnet.
# Registered in the SDK token registry (almanak/framework/data/tokens/defaults.py).
# Source: Etherscan / Swell Network. VERIFY before deploying with real funds.
SWETH_ADDRESS = "0xf951E335afb289353dc249e82926178EaC7DEd78"

# States
STATE_IDLE = "idle"
STATE_SWAPPED = "swapped"


@almanak_strategy(
    name="edge_sweth_mean_rev",
    description="Mean reversion: sell swETH premium vs ETH peg via Uniswap V3",
    version="1.0.0",
    author="Edge Signal cfc82bb4",
    tags=["edge", "mean_reversion", "lst", "sweth"],
    supported_chains=["ethereum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
    default_chain="ethereum",
)
class EdgeSwethMeanRevStrategy(IntentStrategy):
    """Sell swETH to capture premium over ETH peg.

    swETH (Swell liquid staking token) is trading above its expected 1:1 peg
    with ETH. This strategy sells swETH for ETH when the premium exceeds
    a configurable threshold, locking in the arbitrage profit.

    This is a one-shot trade: once the swap executes, the strategy is done.
    The profit is the difference between swETH's market price and ETH's price,
    minus slippage and gas.

    Chain: ethereum
    Protocol: uniswap_v3

    Config Parameters:
    ------------------
    See config.json — key parameters:
      sell_token:         Token to sell (swETH address)
      buy_token:          Token to buy (WETH)
      sell_amount:        Amount of swETH to sell (in token terms)
      min_premium_pct:    Minimum premium % to trigger entry (default 5.0)
      stop_loss_pct:      Max loss tolerance as decimal (default -0.05)
      time_horizon_hours: Max time to wait for entry (default 72)
      max_slippage_bps:   Max slippage in basis points (default 50)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        # Token configuration
        # sell_token is swETH address (not in SDK registry)
        self.sell_token = get_config("sell_token", SWETH_ADDRESS)
        self.buy_token = get_config("buy_token", "WETH")

        # Trade parameters
        self.sell_amount = Decimal(str(get_config("sell_amount", "0.001")))
        self.max_slippage = Decimal(str(get_config("max_slippage_bps", 50))) / Decimal("10000")

        # Entry / exit thresholds
        self.min_premium_pct = Decimal(str(get_config("min_premium_pct", "5.0")))
        self.stop_loss_pct = Decimal(str(get_config("stop_loss_pct", "-0.05")))
        self.time_horizon_hours = int(get_config("time_horizon_hours", 72))

        # Signal metadata (informational)
        self.signal_id = get_config("signal_id", "cfc82bb4-eb44-42b0-b085-84a7095a42a5")

        # State machine (restored via load_persistent_state)
        self._state = STATE_IDLE
        self._entry_time: str | None = None  # ISO timestamp when strategy started
        self._swap_price_ratio: Decimal | None = None  # swETH/ETH ratio at swap time

        logger.info(
            "EdgeSwethMeanRevStrategy initialized: sell %s swETH -> %s, "
            "min_premium=%.1f%%, stop_loss=%.1f%%, horizon=%dh",
            self.sell_amount,
            self.buy_token,
            self.min_premium_pct,
            self.stop_loss_pct * 100,
            self.time_horizon_hours,
        )

    def decide(self, market: MarketSnapshot) -> Intent:
        """Make a trading decision based on swETH/ETH premium.

        State machine:
          idle     -> Check premium, swap if conditions met
          swapped  -> Trade complete, hold indefinitely
        """
        try:
            # Record when strategy first ran (for time horizon tracking)
            if self._entry_time is None:
                self._entry_time = datetime.now(UTC).isoformat()

            if self._state == STATE_SWAPPED:
                return Intent.hold(reason="Trade complete — swETH sold for ETH")

            # --- STATE: idle — evaluate entry conditions ---

            # Get prices to compute premium
            try:
                sweth_price = market.price(self.sell_token)
                eth_price = market.price(self.buy_token)
            except (ValueError, KeyError) as e:
                logger.warning("Price data unavailable: %s", e)
                return Intent.hold(reason=f"Price data unavailable: {e}")

            if eth_price <= 0:
                return Intent.hold(reason="ETH price is zero or negative")

            # Compute swETH/ETH ratio and premium
            # Premium > 0 means swETH is overpriced vs ETH (profitable to sell)
            price_ratio = sweth_price / eth_price
            premium_pct = (price_ratio - Decimal("1")) * Decimal("100")

            logger.info(
                "swETH/ETH ratio=%.6f, premium=%.2f%% (threshold=%.1f%%)",
                price_ratio,
                premium_pct,
                self.min_premium_pct,
            )

            # Check time horizon — if expired, hold and stop trying
            if self._entry_time:
                start = datetime.fromisoformat(self._entry_time)
                elapsed_hours = (datetime.now(UTC) - start).total_seconds() / 3600
                if elapsed_hours > self.time_horizon_hours:
                    logger.info(
                        "Time horizon expired (%.1fh > %dh) — no entry taken",
                        elapsed_hours,
                        self.time_horizon_hours,
                    )
                    return Intent.hold(
                        reason=f"Time horizon expired ({elapsed_hours:.1f}h) — no entry"
                    )

            # Check swETH balance — need enough to sell
            try:
                sweth_balance = market.balance(self.sell_token)
                if sweth_balance.balance < self.sell_amount:
                    logger.info(
                        "Insufficient swETH: have %.6f, need %.6f",
                        sweth_balance.balance,
                        self.sell_amount,
                    )
                    return Intent.hold(
                        reason=f"Insufficient swETH balance ({sweth_balance.balance:.6f} < {self.sell_amount})"
                    )
            except (ValueError, KeyError) as e:
                logger.warning("Cannot check swETH balance: %s", e)
                return Intent.hold(reason=f"Balance check failed: {e}")

            # Entry condition: premium must exceed threshold
            if premium_pct < self.min_premium_pct:
                logger.info(
                    "Premium %.2f%% below threshold %.1f%% — waiting",
                    premium_pct,
                    self.min_premium_pct,
                )
                return Intent.hold(
                    reason=f"Premium {premium_pct:.2f}% below entry threshold {self.min_premium_pct}%"
                )

            # Entry confirmed — execute swap
            logger.info(
                "ENTRY: Selling %.6f swETH for %s (premium=%.2f%%, ratio=%.6f)",
                self.sell_amount,
                self.buy_token,
                premium_pct,
                price_ratio,
            )

            return Intent.swap(
                from_token=self.sell_token,
                to_token=self.buy_token,
                amount=self.sell_amount,
                max_slippage=self.max_slippage,
                protocol="uniswap_v3",
            )

        except Exception as e:
            logger.exception("Error in decide(): %s", e)
            return Intent.hold(reason=f"Error: {e}")

    def on_intent_executed(self, intent, success: bool, result):
        """Track swap execution — transition to 'swapped' state."""
        if not success:
            logger.warning("Swap failed — staying in idle state")
            return

        intent_type = getattr(intent, "intent_type", None)
        if intent_type and intent_type.value == "SWAP":
            # Record the swap
            try:
                from_token = getattr(intent, "from_token", None)
                if from_token == self.sell_token:
                    self._state = STATE_SWAPPED
                    self._swap_price_ratio = None  # Could enrich from result if available
                    logger.info("swETH -> ETH swap executed — trade complete")
            except Exception as e:
                logger.warning("Error in on_intent_executed: %s", e)

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status for monitoring/dashboards."""
        return {
            "strategy": "edge_sweth_mean_rev",
            "signal_id": self.signal_id,
            "chain": self.chain,
            "state": self._state,
            "sell_token": self.sell_token,
            "buy_token": self.buy_token,
            "sell_amount": str(self.sell_amount),
            "min_premium_pct": str(self.min_premium_pct),
            "entry_time": self._entry_time,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else None,
        }

    # -------------------------------------------------------------------------
    # State persistence
    # -------------------------------------------------------------------------

    def get_persistent_state(self) -> dict[str, Any]:
        """Save state for crash recovery."""
        return {
            "state": self._state,
            "entry_time": self._entry_time,
            "swap_price_ratio": str(self._swap_price_ratio) if self._swap_price_ratio else None,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Restore state after restart."""
        if not state:
            return
        self._state = state.get("state", STATE_IDLE)
        self._entry_time = state.get("entry_time")
        ratio = state.get("swap_price_ratio")
        self._swap_price_ratio = Decimal(ratio) if ratio else None
        logger.info("Restored state: %s (entry_time=%s)", self._state, self._entry_time)

    # -------------------------------------------------------------------------
    # TEARDOWN (required)
    # After swap: we hold ETH/WETH — teardown swaps back if needed.
    # Before swap: we hold swETH — teardown sells it at whatever price.
    # -------------------------------------------------------------------------

    def get_open_positions(self):
        """Return all open positions for teardown preview."""
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions = []

        if self._state == STATE_IDLE:
            # Still holding swETH — it's a position that needs unwinding
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="edge_sweth_mean_rev_sell_token",
                    chain=self.chain,
                    protocol="uniswap_v3",
                    value_usd=Decimal("0"),  # Enriched by framework
                    details={
                        "asset": self.sell_token,
                        "description": "swETH awaiting sale",
                    },
                )
            )
        elif self._state == STATE_SWAPPED:
            # Holding ETH/WETH from the completed trade
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="edge_sweth_mean_rev_buy_token",
                    chain=self.chain,
                    protocol="uniswap_v3",
                    value_usd=Decimal("0"),  # Enriched by framework
                    details={
                        "asset": self.buy_token,
                        "description": "ETH received from swETH sale",
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "edge_sweth_mean_rev"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode=None, market=None) -> list[Intent]:
        """Generate intents to close all positions.

        - In idle state: sell swETH for WETH at whatever price
        - In swapped state: no action needed (already holding ETH)
        """
        from almanak.framework.teardown import TeardownMode

        intents: list[Intent] = []

        if self._state == STATE_IDLE:
            # Emergency: dump swETH at wider slippage
            max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.01")
            intents.append(
                Intent.swap(
                    from_token=self.sell_token,
                    to_token=self.buy_token,
                    amount="all",
                    max_slippage=max_slippage,
                    protocol="uniswap_v3",
                )
            )
        # In swapped state: we hold ETH — nothing to unwind

        return intents


if __name__ == "__main__":
    print("=" * 60)
    print("EdgeSwethMeanRevStrategy")
    print("=" * 60)
    print(f"Strategy Name: {EdgeSwethMeanRevStrategy.STRATEGY_NAME}")
    print(f"Supported Chains: {EdgeSwethMeanRevStrategy.SUPPORTED_CHAINS}")
    print(f"Supported Protocols: {EdgeSwethMeanRevStrategy.SUPPORTED_PROTOCOLS}")
    print(f"Intent Types: {EdgeSwethMeanRevStrategy.INTENT_TYPES}")
    print(f"\nswETH Address: {SWETH_ADDRESS}")
    print("\nTo run this strategy:")
    print("  almanak strat run --network anvil --once")
