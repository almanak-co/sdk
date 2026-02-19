"""
===============================================================================
Ethena Leverage Loop -- Amplified sUSDe Yield via Morpho Blue Recursive Borrowing
===============================================================================

This strategy amplifies Ethena sUSDe staking yield by recursively borrowing
against sUSDe on Morpho Blue.

HOW IT WORKS:
-------------
1. SETUP: Swap USDC -> USDe (Enso) -> Stake USDe -> sUSDe (Ethena)
2. LOOP (N times):
   a. Supply sUSDe as collateral on Morpho Blue (sUSDe/USDC market, 91.5% LLTV)
   b. Borrow USDC at target LTV (75% for safety)
   c. Swap borrowed USDC -> USDe via Enso
   d. Stake USDe -> sUSDe via Ethena
3. MONITOR: Track health factor, auto-deleverage if needed

YIELD MATH (example: 2 loops):
-------------------------------
sUSDe yield: ~10% | USDC borrow cost: ~6% | Net spread: ~4%/unit of leverage
At 2.3x leverage: 2.3 * 10% - 1.3 * 6% = ~15.2% net APY

RISKS:
------
- LIQUIDATION: sUSDe depeg or borrow rate spike can cause HF to drop below 1.0
- COOLDOWN: sUSDe has 7-day unstake cooldown -- cannot quickly deleverage via unstaking
- BORROW RATE: Morpho USDC rates can spike during high utilization
- SLIPPAGE: Each swap/stake incurs friction, reducing effective leverage
- GAS: Multiple Ethereum mainnet transactions per loop

MORPHO MARKET:
--------------
sUSDe/USDC: 0x85c7f4374f3a403b36d54cc284983b2b02bbd8581ee0f3c36494447b87d9fcab
LLTV: 91.5%

USAGE:
------
    # Full lifecycle on Anvil
    almanak strat run -d strategies/demo/ethena_leverage_loop --fresh --interval 15 --network anvil

    # Single step for debugging
    almanak strat run -d strategies/demo/ethena_leverage_loop --fresh --once --network anvil

===============================================================================
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

logger = logging.getLogger(__name__)

# Morpho Blue sUSDe/USDC market (Ethereum, 91.5% LLTV)
DEFAULT_MARKET_ID = "0x85c7f4374f3a403b36d54cc284983b2b02bbd8581ee0f3c36494447b87d9fcab"
DEFAULT_LLTV = Decimal("0.915")


@almanak_strategy(
    name="ethena_leverage_loop",
    description="Amplified sUSDe yield via recursive borrowing on Morpho Blue",
    version="1.0.0",
    author="Almanak",
    tags=["ethena", "morpho", "leverage", "looping", "yield", "susde", "usde"],
    supported_chains=["ethereum"],
    supported_protocols=["ethena", "morpho_blue", "enso"],
    intent_types=["SWAP", "STAKE", "SUPPLY", "BORROW", "REPAY", "WITHDRAW", "HOLD"],
)
class EthenaLeverageLoopStrategy(IntentStrategy):
    """Amplified sUSDe yield via Morpho Blue recursive borrowing.

    State machine phases:
        SETUP: USDC -> USDe -> sUSDe (initial conversion)
        LOOP: supply sUSDe -> borrow USDC -> swap to USDe -> stake to sUSDe (repeat)
        MONITOR: track health factor, deleverage if needed

    Each decide() call advances one step.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            if hasattr(self.config, "get"):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        # Market configuration
        self.market_id = get_config("market_id", DEFAULT_MARKET_ID)
        self.lltv = Decimal(str(get_config("lltv", str(DEFAULT_LLTV))))

        # Loop parameters
        self.target_loops = int(get_config("target_loops", 2))
        self.target_ltv = Decimal(str(get_config("target_ltv", "0.75")))
        self.min_health_factor = Decimal(str(get_config("min_health_factor", "1.5")))
        self.swap_slippage = Decimal(str(get_config("swap_slippage", "0.005")))
        self.min_usdc_amount = Decimal(str(get_config("min_usdc_amount", "100")))

        # State machine
        # Phases: idle -> setup_swap -> setup_stake
        #      -> loop_supply -> loop_borrow -> loop_swap -> loop_stake -> (repeat or complete)
        #      -> monitoring
        self._phase = "idle"
        self._current_loop = 0
        self._loops_completed = 0

        # Position tracking
        self._total_collateral_susde = Decimal("0")
        self._total_borrowed_usdc = Decimal("0")
        self._pending_amount = Decimal("0")  # amount flowing between steps

        # Health tracking
        self._current_health_factor = Decimal("0")

        logger.info(
            f"EthenaLeverageLoop initialized: market={self.market_id[:16]}..., "
            f"target_loops={self.target_loops}, target_ltv={self.target_ltv * 100}%"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Advance the state machine by one step.

        Returns the next intent to execute based on current phase.
        """
        try:
            # Get prices for calculations
            susde_price, usdc_price = self._get_prices(market)

            # State machine dispatch
            handler = {
                "idle": self._handle_idle,
                "setup_swap": self._handle_setup_swap,
                "setup_stake": self._handle_setup_stake,
                "loop_supply": self._handle_loop_supply,
                "loop_borrow": self._handle_loop_borrow,
                "loop_swap": self._handle_loop_swap,
                "loop_stake": self._handle_loop_stake,
                "complete": self._handle_complete,
                "monitoring": self._handle_monitoring,
            }.get(self._phase)

            if handler:
                return handler(market, susde_price, usdc_price)

            return Intent.hold(reason=f"Unknown phase: {self._phase}")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    # =========================================================================
    # STATE HANDLERS
    # =========================================================================

    def _handle_idle(self, market: MarketSnapshot, susde_price: Decimal, usdc_price: Decimal) -> Intent:
        """IDLE: check balance and start setup or loop."""
        # Check if we already have sUSDe (skip setup, go straight to looping)
        susde_balance = self._get_balance(market, "sUSDe")
        if susde_balance >= Decimal("10"):  # minimum viable sUSDe
            logger.info(f"Found {susde_balance} sUSDe, skipping setup -- starting loop phase")
            self._pending_amount = susde_balance
            self._transition("idle", "loop_supply")
            return self._create_supply_intent(susde_balance)

        # Check USDC balance for setup
        usdc_balance = self._get_balance(market, "USDC")
        if usdc_balance < self.min_usdc_amount:
            return Intent.hold(
                reason=f"Insufficient USDC: {usdc_balance} < {self.min_usdc_amount}"
            )

        # Start setup: swap USDC -> USDe
        logger.info(f"Starting setup with {usdc_balance} USDC")
        self._pending_amount = usdc_balance
        self._transition("idle", "setup_swap")
        return self._create_swap_usdc_to_usde(usdc_balance)

    def _handle_setup_swap(self, market: MarketSnapshot, susde_price: Decimal, usdc_price: Decimal) -> Intent:
        """SETUP_SWAP completed -> stake USDe."""
        usde_balance = self._get_balance(market, "USDe")
        if usde_balance <= Decimal("0"):
            return Intent.hold(reason="Waiting for USDe balance after swap")

        logger.info(f"Setup swap complete, staking {usde_balance} USDe")
        self._pending_amount = usde_balance
        self._transition("setup_swap", "setup_stake")
        return self._create_stake_intent(usde_balance)

    def _handle_setup_stake(self, market: MarketSnapshot, susde_price: Decimal, usdc_price: Decimal) -> Intent:
        """SETUP_STAKE completed -> start looping."""
        susde_balance = self._get_balance(market, "sUSDe")
        if susde_balance <= Decimal("0"):
            return Intent.hold(reason="Waiting for sUSDe balance after staking")

        logger.info(f"Setup complete with {susde_balance} sUSDe -- starting loop phase")
        self._pending_amount = susde_balance
        self._transition("setup_stake", "loop_supply")
        return self._create_supply_intent(susde_balance)

    def _handle_loop_supply(self, market: MarketSnapshot, susde_price: Decimal, usdc_price: Decimal) -> Intent:
        """LOOP_SUPPLY completed -> borrow USDC."""
        borrow_amount = self._calculate_borrow_amount(susde_price, usdc_price)
        if borrow_amount <= Decimal("0"):
            logger.warning("No borrowing capacity available")
            self._transition("loop_supply", "complete")
            return Intent.hold(reason="No borrowing capacity -- looping complete")

        logger.info(f"Loop {self._current_loop + 1}: Borrowing {borrow_amount} USDC")
        self._pending_amount = borrow_amount
        self._transition("loop_supply", "loop_borrow")
        return self._create_borrow_intent(borrow_amount)

    def _handle_loop_borrow(self, market: MarketSnapshot, susde_price: Decimal, usdc_price: Decimal) -> Intent:
        """LOOP_BORROW completed -> swap USDC to USDe."""
        usdc_balance = self._get_balance(market, "USDC")
        if usdc_balance <= Decimal("0"):
            return Intent.hold(reason="Waiting for USDC balance after borrow")

        logger.info(f"Loop {self._current_loop + 1}: Swapping {usdc_balance} USDC -> USDe")
        self._pending_amount = usdc_balance
        self._transition("loop_borrow", "loop_swap")
        return self._create_swap_usdc_to_usde(usdc_balance)

    def _handle_loop_swap(self, market: MarketSnapshot, susde_price: Decimal, usdc_price: Decimal) -> Intent:
        """LOOP_SWAP completed -> stake USDe to sUSDe."""
        usde_balance = self._get_balance(market, "USDe")
        if usde_balance <= Decimal("0"):
            return Intent.hold(reason="Waiting for USDe balance after swap")

        logger.info(f"Loop {self._current_loop + 1}: Staking {usde_balance} USDe -> sUSDe")
        self._pending_amount = usde_balance
        self._transition("loop_swap", "loop_stake")
        return self._create_stake_intent(usde_balance)

    def _handle_loop_stake(self, market: MarketSnapshot, susde_price: Decimal, usdc_price: Decimal) -> Intent:
        """LOOP_STAKE completed -> check if more loops or done."""
        self._loops_completed += 1
        self._current_loop += 1

        susde_balance = self._get_balance(market, "sUSDe")

        if self._current_loop < self.target_loops and susde_balance > Decimal("10"):
            # More loops -- supply the new sUSDe
            logger.info(
                f"Loop {self._loops_completed} complete. "
                f"Starting loop {self._current_loop + 1}/{self.target_loops}"
            )
            self._pending_amount = susde_balance
            self._transition("loop_stake", "loop_supply")
            return self._create_supply_intent(susde_balance)

        # All loops complete
        logger.info(f"All {self._loops_completed} loops complete. Entering monitoring phase.")
        self._transition("loop_stake", "monitoring")
        return Intent.hold(
            reason=f"Looping complete -- {self._loops_completed} loops, "
            f"total collateral: {self._total_collateral_susde} sUSDe, "
            f"total debt: {self._total_borrowed_usdc} USDC"
        )

    def _handle_complete(self, market: MarketSnapshot, susde_price: Decimal, usdc_price: Decimal) -> Intent:
        """COMPLETE: all loops done, transition to monitoring."""
        self._transition("complete", "monitoring")
        return self._handle_monitoring(market, susde_price, usdc_price)

    def _handle_monitoring(self, market: MarketSnapshot, susde_price: Decimal, usdc_price: Decimal) -> Intent:
        """MONITORING: track health factor, deleverage if needed."""
        if self._total_borrowed_usdc > Decimal("0") and susde_price > Decimal("0"):
            collateral_value = self._total_collateral_susde * susde_price
            borrow_value = self._total_borrowed_usdc * usdc_price
            self._current_health_factor = (collateral_value * self.lltv) / borrow_value

            if self._current_health_factor < self.min_health_factor:
                logger.error(
                    f"CRITICAL: Health factor {self._current_health_factor:.3f} < "
                    f"{self.min_health_factor} -- position at liquidation risk"
                )
                # Emit repay intent to deleverage: repay 25% of outstanding debt
                repay_amount = (self._total_borrowed_usdc * Decimal("0.25")).quantize(
                    Decimal("0.01"), rounding=ROUND_DOWN
                )
                if repay_amount > Decimal("0"):
                    return Intent.repay(
                        token="USDC",
                        amount=repay_amount,
                        protocol="morpho_blue",
                        market_id=self.market_id,
                    )

        leverage = Decimal("1")
        if self._total_collateral_susde > Decimal("0") and self._total_borrowed_usdc > Decimal("0"):
            equity = (self._total_collateral_susde * susde_price) - (self._total_borrowed_usdc * usdc_price)
            if equity > Decimal("0"):
                leverage = (self._total_collateral_susde * susde_price) / equity

        return Intent.hold(
            reason=f"Monitoring -- HF: {self._current_health_factor:.3f}, "
            f"Leverage: {leverage:.2f}x, "
            f"Collateral: {self._total_collateral_susde:.2f} sUSDe, "
            f"Debt: {self._total_borrowed_usdc:.2f} USDC"
        )

    # =========================================================================
    # INTENT CREATION
    # =========================================================================

    def _create_swap_usdc_to_usde(self, amount: Decimal) -> Intent:
        """Swap USDC -> USDe via Enso aggregator."""
        logger.info(f"SWAP: {format_token_amount_human(amount, 'USDC')} -> USDe via Enso")
        return Intent.swap(
            from_token="USDC",
            to_token="USDe",
            amount=amount,
            max_slippage=self.swap_slippage,
            protocol="enso",
            chain="ethereum",
        )

    def _create_stake_intent(self, amount: Decimal) -> Intent:
        """Stake USDe -> sUSDe via Ethena."""
        logger.info(f"STAKE: {format_token_amount_human(amount, 'USDe')} -> sUSDe")
        return Intent.stake(
            protocol="ethena",
            token_in="USDe",
            amount=amount,
            receive_wrapped=False,
            chain="ethereum",
        )

    def _create_supply_intent(self, amount: Decimal) -> Intent:
        """Supply sUSDe as collateral on Morpho Blue."""
        logger.info(f"SUPPLY: {format_token_amount_human(amount, 'sUSDe')} to Morpho Blue")
        return Intent.supply(
            protocol="morpho_blue",
            token="sUSDe",
            amount=amount,
            use_as_collateral=True,
            market_id=self.market_id,
            chain="ethereum",
        )

    def _create_borrow_intent(self, amount: Decimal) -> Intent:
        """Borrow USDC against sUSDe collateral on Morpho Blue."""
        logger.info(f"BORROW: {format_token_amount_human(amount, 'USDC')} from Morpho Blue")
        return Intent.borrow(
            protocol="morpho_blue",
            collateral_token="sUSDe",
            collateral_amount=Decimal("0"),  # already supplied
            borrow_token="USDC",
            borrow_amount=amount,
            market_id=self.market_id,
            chain="ethereum",
        )

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _get_prices(self, market: MarketSnapshot) -> tuple[Decimal, Decimal]:
        """Get sUSDe and USDC prices from market snapshot.

        Raises ValueError if prices are unavailable -- callers must handle this
        by returning Intent.hold() rather than proceeding with stale data.
        """
        try:
            susde_price = market.price("sUSDe")
        except (ValueError, KeyError) as e:
            raise ValueError(f"sUSDe price unavailable: {e}") from e

        try:
            usdc_price = market.price("USDC")
        except (ValueError, KeyError) as e:
            raise ValueError(f"USDC price unavailable: {e}") from e

        return susde_price, usdc_price

    def _get_balance(self, market: MarketSnapshot, token: str) -> Decimal:
        """Get token balance, returning 0 on error."""
        try:
            bal = market.balance(token)
            return bal.balance if hasattr(bal, "balance") else bal
        except (ValueError, KeyError, AttributeError):
            return Decimal("0")

    def _calculate_borrow_amount(self, susde_price: Decimal, usdc_price: Decimal) -> Decimal:
        """Calculate safe borrow amount based on collateral and target LTV."""
        collateral_value = self._total_collateral_susde * susde_price
        max_borrow_value = collateral_value * self.target_ltv
        existing_borrow_value = self._total_borrowed_usdc * usdc_price
        available = max_borrow_value - existing_borrow_value

        if available <= Decimal("0"):
            return Decimal("0")

        borrow_amount = (available / usdc_price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        return borrow_amount

    def _transition(self, old: str, new: str) -> None:
        """Transition state machine phase and emit timeline event."""
        self._phase = new
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description=f"Phase: {old.upper()} -> {new.upper()}",
                strategy_id=self.strategy_id,
                details={
                    "old_phase": old,
                    "new_phase": new,
                    "loop": self._current_loop,
                    "total_loops": self.target_loops,
                },
            )
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Update state tracking after each intent execution."""
        intent_type = intent.intent_type.value

        if not success:
            logger.warning(f"{intent_type} failed in phase {self._phase}")
            return

        if intent_type == "SWAP":
            if self._phase == "setup_swap":
                # Setup swap complete, will stake next
                pass
            elif self._phase == "loop_swap":
                # Loop swap complete, will stake next
                pass
            logger.info(f"Swap completed in phase {self._phase}")

        elif intent_type == "STAKE":
            logger.info(f"Stake completed in phase {self._phase}")

        elif intent_type in ("SUPPLY", "SUPPLY_COLLATERAL"):
            amount = Decimal("0")
            if hasattr(intent, "amount") and isinstance(intent.amount, Decimal):
                amount = intent.amount
            self._total_collateral_susde += amount
            logger.info(
                f"Supplied {amount} sUSDe -- total collateral: {self._total_collateral_susde}"
            )
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Supplied {amount} sUSDe to Morpho Blue",
                    strategy_id=self.strategy_id,
                    details={
                        "action": "supply_collateral",
                        "token": "sUSDe",
                        "amount": str(amount),
                        "total_collateral": str(self._total_collateral_susde),
                    },
                )
            )

        elif intent_type == "BORROW":
            amount = Decimal("0")
            if hasattr(intent, "borrow_amount") and isinstance(intent.borrow_amount, Decimal):
                amount = intent.borrow_amount
            self._total_borrowed_usdc += amount
            logger.info(
                f"Borrowed {amount} USDC -- total debt: {self._total_borrowed_usdc}"
            )
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"Borrowed {amount} USDC from Morpho Blue",
                    strategy_id=self.strategy_id,
                    details={
                        "action": "borrow",
                        "token": "USDC",
                        "amount": str(amount),
                        "total_borrowed": str(self._total_borrowed_usdc),
                    },
                )
            )

    # =========================================================================
    # STATUS & PERSISTENCE
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "ethena_leverage_loop",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "market_id": self.market_id[:20] + "...",
                "target_loops": self.target_loops,
                "target_ltv": str(self.target_ltv),
                "min_health_factor": str(self.min_health_factor),
            },
            "state": {
                "phase": self._phase,
                "current_loop": self._current_loop,
                "loops_completed": self._loops_completed,
                "total_collateral_susde": str(self._total_collateral_susde),
                "total_borrowed_usdc": str(self._total_borrowed_usdc),
                "health_factor": str(self._current_health_factor),
            },
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "phase": self._phase,
            "current_loop": self._current_loop,
            "loops_completed": self._loops_completed,
            "total_collateral_susde": str(self._total_collateral_susde),
            "total_borrowed_usdc": str(self._total_borrowed_usdc),
            "pending_amount": str(self._pending_amount),
            "current_health_factor": str(self._current_health_factor),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "phase" in state:
            self._phase = state["phase"]
        if "current_loop" in state:
            self._current_loop = int(state["current_loop"])
        if "loops_completed" in state:
            self._loops_completed = int(state["loops_completed"])
        if "total_collateral_susde" in state:
            self._total_collateral_susde = Decimal(str(state["total_collateral_susde"]))
        if "total_borrowed_usdc" in state:
            self._total_borrowed_usdc = Decimal(str(state["total_borrowed_usdc"]))
        if "pending_amount" in state:
            self._pending_amount = Decimal(str(state["pending_amount"]))
        if "current_health_factor" in state:
            self._current_health_factor = Decimal(str(state["current_health_factor"]))
        logger.info(
            f"Restored state: phase={self._phase}, loop={self._current_loop}/{self.target_loops}, "
            f"HF={self._current_health_factor}"
        )

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":  # noqa: F821
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._total_collateral_susde > Decimal("0"):
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"morpho-susde-{self.market_id[:16]}",
                    chain=self.chain,
                    protocol="morpho_blue",
                    value_usd=self._total_collateral_susde * Decimal("1.05"),
                    details={
                        "market_id": self.market_id,
                        "asset": "sUSDe",
                        "amount": str(self._total_collateral_susde),
                    },
                )
            )
        if self._total_borrowed_usdc > Decimal("0"):
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"morpho-usdc-{self.market_id[:16]}",
                    chain=self.chain,
                    protocol="morpho_blue",
                    value_usd=self._total_borrowed_usdc,
                    health_factor=self._current_health_factor,
                    details={
                        "market_id": self.market_id,
                        "asset": "USDC",
                        "amount": str(self._total_borrowed_usdc),
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:  # noqa: F821
        """Unwind: repay debt -> withdraw collateral -> swap to USDC."""
        intents = []
        if self._total_borrowed_usdc > Decimal("0"):
            intents.append(
                Intent.repay(
                    protocol="morpho_blue",
                    token="USDC",
                    amount=Decimal("0"),
                    repay_full=True,
                    market_id=self.market_id,
                    chain="ethereum",
                )
            )
        if self._total_collateral_susde > Decimal("0"):
            intents.append(
                Intent.withdraw(
                    protocol="morpho_blue",
                    token="sUSDe",
                    amount=self._total_collateral_susde,
                    withdraw_all=True,
                    market_id=self.market_id,
                    chain="ethereum",
                )
            )
        return intents

    def on_teardown_started(self, mode: "TeardownMode") -> None:  # noqa: F821
        from almanak.framework.teardown import TeardownMode

        mode_name = "graceful" if mode == TeardownMode.SOFT else "emergency"
        logger.info(
            f"Teardown ({mode_name}): repaying {self._total_borrowed_usdc} USDC, "
            f"withdrawing {self._total_collateral_susde} sUSDe"
        )

    def on_teardown_completed(self, success: bool, recovered_usd: Decimal) -> None:
        if success:
            logger.info(f"Teardown completed. Recovered ${recovered_usd:,.2f}")
            self._phase = "idle"
            self._current_loop = 0
            self._loops_completed = 0
            self._total_collateral_susde = Decimal("0")
            self._total_borrowed_usdc = Decimal("0")
            self._pending_amount = Decimal("0")
            self._current_health_factor = Decimal("0")
        else:
            logger.error("Teardown failed -- manual intervention may be required")


if __name__ == "__main__":
    print("=" * 70)
    print("EthenaLeverageLoopStrategy -- Amplified sUSDe Yield")
    print("=" * 70)
    print(f"\nStrategy: {EthenaLeverageLoopStrategy.STRATEGY_NAME}")
    print(f"Chains: {EthenaLeverageLoopStrategy.SUPPORTED_CHAINS}")
    print(f"Protocols: {EthenaLeverageLoopStrategy.SUPPORTED_PROTOCOLS}")
    print(f"Intents: {EthenaLeverageLoopStrategy.INTENT_TYPES}")
