"""
Mean Reversion Swap Strategy: edge_ankreth_mean_rev

Captures the ankrETH premium over ETH by selling ankrETH for ETH via Uniswap V3.
This is a one-shot trade: once the swap executes, the position is complete.

Edge Signal: 92e14fed-96ac-4b3c-9ae3-d4b4bde1203c
Type: HIGH_CONVICTION_SYNTHESIS (LST_DEPEG_RISK x2)
Alpha: 83/100, Regime: BEAR
Thesis: ankrETH (Ankr liquid staking token) is trading at 16.55% premium to its
        expected peg of 1.05 ETH. The premium should mean-revert. Swap ankrETH -> ETH
        to lock in the premium.

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
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

logger = logging.getLogger(__name__)

# ankrETH (Ankr liquid staking token) address on Ethereum mainnet.
# Registered in the SDK token registry (almanak/framework/data/tokens/defaults.py).
# Source: CoinGecko / Etherscan for Ankr Staked ETH. VERIFY before deploying with real funds.
ANKRETH_ADDRESS = "0xE95A203B1a91a908F9B9CE46459d101078c2c3cb"

# ankrETH fair-value peg: 1 ankrETH = 1.05 ETH (accrued staking yield)
ANKRETH_PEG_RATIO = Decimal("1.05")

# States
STATE_IDLE = "idle"
STATE_SWAPPED = "swapped"


@almanak_strategy(
    name="edge_ankreth_mean_rev",
    description="Mean reversion: sell ankrETH premium vs ETH peg via Uniswap V3",
    version="1.0.0",
    author="Edge Signal 92e14fed",
    tags=["edge", "mean_reversion", "lst", "ankreth"],
    supported_chains=["ethereum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
    default_chain="ethereum",
)
class EdgeAnkrethMeanRevStrategy(IntentStrategy):
    """Sell ankrETH to capture premium over ETH peg.

    ankrETH (Ankr liquid staking token) is trading above its expected peg
    of 1.05 ETH per ankrETH. This strategy sells ankrETH for ETH when the
    premium exceeds a configurable threshold, locking in the arbitrage profit.

    This is a one-shot trade: once the swap executes, the strategy is done.
    The profit is the difference between ankrETH's market price and its
    fair-value peg (1.05 ETH), minus slippage and gas.

    Chain: ethereum
    Protocol: uniswap_v3

    Config Parameters:
    ------------------
    See config.json — key parameters:
      sell_token:           ankrETH contract address
      sell_token_address:   Same as sell_token (explicit address)
      buy_token:            Token to buy (WETH)
      sell_amount:          Amount of ankrETH to sell (in token terms)
      peg_ratio:            Fair-value peg ratio (ankrETH/ETH, default 1.05)
      min_premium_pct:      Minimum premium % above peg to trigger entry (default 10.0)
      stop_loss_pct:        Max loss tolerance as decimal (default -0.075)
      time_horizon_hours:   Max time to wait for entry (default 72)
      max_slippage_bps:     Max slippage in basis points (default 50)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        # Token configuration
        # sell_token is ankrETH address (not in SDK registry)
        self.sell_token = get_config("sell_token", ANKRETH_ADDRESS)
        self.sell_token_address = get_config("sell_token_address", ANKRETH_ADDRESS)
        self.buy_token = get_config("buy_token", "WETH")

        # Trade parameters
        self.sell_amount = Decimal(str(get_config("sell_amount", "0.001")))
        self.max_slippage = Decimal(str(get_config("max_slippage_bps", 50))) / Decimal("10000")

        # Peg and entry / exit thresholds
        self.peg_ratio = Decimal(str(get_config("peg_ratio", str(ANKRETH_PEG_RATIO))))
        self.min_premium_pct = Decimal(str(get_config("min_premium_pct", "10.0")))
        self.stop_loss_pct = Decimal(str(get_config("stop_loss_pct", "-0.075")))
        self.time_horizon_hours = int(get_config("time_horizon_hours", 72))

        # Signal metadata (informational)
        self.signal_id = get_config("signal_id", "92e14fed-96ac-4b3c-9ae3-d4b4bde1203c")

        # State machine (restored via load_persistent_state)
        self._state = STATE_IDLE
        self._entry_time: str | None = None  # ISO timestamp when strategy started
        self._swap_price_ratio: Decimal | None = None  # ankrETH/ETH ratio at swap time

        logger.info(
            "EdgeAnkrethMeanRevStrategy initialized: sell %s ankrETH -> %s, "
            "peg=%.4f, min_premium=%.1f%%, stop_loss=%.1f%%, horizon=%dh",
            self.sell_amount,
            self.buy_token,
            self.peg_ratio,
            self.min_premium_pct,
            self.stop_loss_pct * 100,
            self.time_horizon_hours,
        )

    def decide(self, market: MarketSnapshot) -> Intent:
        """Make a trading decision based on ankrETH/ETH premium.

        State machine:
          idle     -> Check premium vs peg, swap if conditions met
          swapped  -> Trade complete, hold indefinitely

        Premium calculation:
          ankrETH fair value = peg_ratio * ETH price (default: 1.05 * ETH)
          market_ratio = ankrETH_price / ETH_price
          premium = (market_ratio - peg_ratio) / peg_ratio * 100

          If premium > min_premium_pct: SWAP ankrETH -> ETH
        """
        try:
            # Record when strategy first ran (for time horizon tracking)
            if self._entry_time is None:
                self._entry_time = datetime.now(UTC).isoformat()

            if self._state == STATE_SWAPPED:
                return Intent.hold(reason="Trade complete — ankrETH sold for ETH")

            # --- STATE: idle — evaluate entry conditions ---

            # Get prices to compute premium
            try:
                ankreth_price = market.price(self.sell_token)
                eth_price = market.price(self.buy_token)
            except (ValueError, KeyError) as e:
                logger.warning("Price data unavailable: %s", e)
                return Intent.hold(reason=f"Price data unavailable: {e}")

            if eth_price <= 0:
                return Intent.hold(reason="ETH price is zero or negative")

            # Compute ankrETH/ETH ratio and premium above peg
            # ankrETH peg = 1.05 ETH, so fair-value ratio = 1.05
            # Premium > 0 means ankrETH is overpriced vs its peg (profitable to sell)
            market_ratio = ankreth_price / eth_price
            premium_pct = (market_ratio - self.peg_ratio) / self.peg_ratio * Decimal("100")

            logger.info(
                "ankrETH/ETH ratio=%.6f (peg=%.4f), premium=%.2f%% (threshold=%.1f%%)",
                market_ratio,
                self.peg_ratio,
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

            # Check ankrETH balance — need enough to sell
            try:
                ankreth_balance = market.balance(self.sell_token)
                if ankreth_balance.balance < self.sell_amount:
                    logger.info(
                        "Insufficient ankrETH: have %.6f, need %.6f",
                        ankreth_balance.balance,
                        self.sell_amount,
                    )
                    return Intent.hold(
                        reason=f"Insufficient ankrETH balance ({ankreth_balance.balance:.6f} < {self.sell_amount})"
                    )
            except (ValueError, KeyError) as e:
                logger.warning("Cannot check ankrETH balance: %s", e)
                return Intent.hold(reason=f"Balance check failed: {e}")

            # Entry condition: premium above peg must exceed threshold
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
                "ENTRY: Selling %.6f ankrETH for %s (premium=%.2f%%, ratio=%.6f, peg=%.4f)",
                self.sell_amount,
                self.buy_token,
                premium_pct,
                market_ratio,
                self.peg_ratio,
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
            try:
                from_token = getattr(intent, "from_token", None)
                if from_token == self.sell_token:
                    self._state = STATE_SWAPPED
                    self._swap_price_ratio = None  # Could enrich from result if available
                    logger.info("ankrETH -> ETH swap executed — trade complete")
            except Exception as e:
                logger.warning("Error in on_intent_executed: %s", e)

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status for monitoring/dashboards."""
        return {
            "strategy": "edge_ankreth_mean_rev",
            "signal_id": self.signal_id,
            "chain": self.chain,
            "state": self._state,
            "sell_token": self.sell_token,
            "buy_token": self.buy_token,
            "sell_amount": str(self.sell_amount),
            "peg_ratio": str(self.peg_ratio),
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
    # Before swap: we hold ankrETH — teardown sells it at whatever price.
    # After swap: we hold ETH/WETH — nothing to unwind.
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
            # Still holding ankrETH — it's a position that needs unwinding
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="edge_ankreth_mean_rev_sell_token",
                    chain=self.chain,
                    protocol="uniswap_v3",
                    value_usd=Decimal("0"),  # Enriched by framework
                    details={
                        "asset": self.sell_token,
                        "description": "ankrETH awaiting sale",
                    },
                )
            )
        elif self._state == STATE_SWAPPED:
            # Holding ETH/WETH from the completed trade
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="edge_ankreth_mean_rev_buy_token",
                    chain=self.chain,
                    protocol="uniswap_v3",
                    value_usd=Decimal("0"),  # Enriched by framework
                    details={
                        "asset": self.buy_token,
                        "description": "ETH received from ankrETH sale",
                    },
                )
            )

        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "edge_ankreth_mean_rev"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode=None, market=None) -> list[Intent]:
        """Generate intents to close all positions.

        - In idle state: sell ankrETH for WETH at whatever price
        - In swapped state: no action needed (already holding ETH)
        """
        from almanak.framework.teardown import TeardownMode

        intents: list[Intent] = []

        if self._state == STATE_IDLE:
            # Emergency: dump ankrETH at wider slippage
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
    print("EdgeAnkrethMeanRevStrategy")
    print("=" * 60)
    print(f"Strategy Name: {EdgeAnkrethMeanRevStrategy.STRATEGY_NAME}")
    print(f"Supported Chains: {EdgeAnkrethMeanRevStrategy.SUPPORTED_CHAINS}")
    print(f"Supported Protocols: {EdgeAnkrethMeanRevStrategy.SUPPORTED_PROTOCOLS}")
    print(f"Intent Types: {EdgeAnkrethMeanRevStrategy.INTENT_TYPES}")
    print(f"\nankrETH Address: {ANKRETH_ADDRESS}")
    print(f"Peg Ratio: {ANKRETH_PEG_RATIO} ETH per ankrETH")
    print("\nTo run this strategy:")
    print("  almanak strat run --network anvil --once")
