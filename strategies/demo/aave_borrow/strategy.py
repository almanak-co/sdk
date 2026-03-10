"""
===============================================================================
TUTORIAL: Aave V3 Borrow Strategy - Supply Collateral and Borrow
===============================================================================

This tutorial strategy demonstrates how to create a leveraged borrow position
using Aave V3's lending protocol.

WHAT THIS STRATEGY DOES:
------------------------
1. Supplies collateral (e.g., WETH) to Aave V3
2. Borrows against it (e.g., USDC) at a target LTV

This is a simple supply-and-borrow strategy, NOT a looping strategy.
A true looping strategy would swap the borrowed tokens back to collateral
and re-supply to amplify leverage - this demo keeps it simple.

RISKS:
------
- Liquidation: If collateral value drops below borrow value, you get liquidated
- Health Factor: Aave uses "health factor" - below 1.0 = liquidatable
- Interest: You pay interest on borrowed amounts
- Smart Contract Risk: Protocol bugs, exploits

HEALTH FACTOR EXPLAINED:
------------------------
Health Factor = (Collateral Value * Liquidation Threshold) / Borrow Value

- HF > 1.0: Safe
- HF = 1.0: Liquidatable
- HF < 1.0: Being liquidated

For safety, maintain HF > 1.5 (this strategy uses 2.0 minimum)

USAGE:
------
    # Run once
    python -m src.cli.run --strategy demo_aave_borrow --once

    # Run continuously
    python -m src.cli.run --strategy demo_aave_borrow

    # Test on Anvil
    python strategies/demo/aave_borrow/run_anvil.py

===============================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

# Timeline API for logging
from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event

# Intent is what your strategy returns - describes what action to take
from almanak.framework.intents import Intent

# HotReloadableConfig for proper config handling
# Core strategy framework imports
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

# Logging utilities for user-friendly output
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

# Logger for debugging
logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import PositionInfo, TeardownMode, TeardownPositionSummary


# =============================================================================
# STRATEGY METADATA
# =============================================================================


@almanak_strategy(
    # Unique identifier for CLI
    name="demo_aave_borrow",
    # Description
    description="Tutorial strategy - supply collateral and borrow on Aave V3",
    # Version
    version="1.0.0",
    # Author
    author="Almanak",
    # Tags
    tags=["demo", "tutorial", "lending", "borrow", "aave-v3"],
    # Supported chains (Aave V3 is deployed on these)
    supported_chains=["arbitrum", "ethereum", "base", "optimism", "polygon"],
    # Protocols used
    supported_protocols=["aave_v3"],
    # Intent types this strategy may emit
    # SUPPLY: Deposit tokens into Aave
    # BORROW: Borrow tokens from Aave
    # HOLD: No action
    intent_types=["SUPPLY", "BORROW", "HOLD"],
    default_chain="arbitrum",
)
class AaveBorrowStrategy(IntentStrategy):
    """
    Aave V3 borrow strategy for educational purposes.

    This strategy demonstrates:
    - How to supply collateral to Aave V3
    - How to borrow against collateral
    - How to calculate safe borrow amounts

    Configuration Parameters (from config.json):
    --------------------------------------------
    - collateral_token: Token to use as collateral (default: "WETH")
    - collateral_amount: Amount to supply (default: "0.1")
    - borrow_token: Token to borrow (default: "USDC")
    - ltv_target: Target LTV ratio (default: 0.5 = 50%)
    - min_health_factor: Minimum health factor to maintain (default: 2.0)
    - interest_rate_mode: "variable" or "stable" (default: "variable")

    Example Config:
    ---------------
    {
        "collateral_token": "WETH",
        "collateral_amount": "0.1",
        "borrow_token": "USDC",
        "ltv_target": 0.5,
        "min_health_factor": 2.0,
        "force_action": "supply",
        "interest_rate_mode": "variable"
    }
    """

    # =========================================================================
    # INITIALIZATION
    # =========================================================================

    def __init__(self, *args, **kwargs):
        """
        Initialize the borrow strategy.

        Extracts configuration and sets up internal state for tracking
        the borrow process.
        """
        super().__init__(*args, **kwargs)

        # =====================================================================
        # Extract configuration
        # =====================================================================

        # Collateral configuration
        self.collateral_token = self.get_config("collateral_token", "WETH")
        self.collateral_amount = Decimal(str(self.get_config("collateral_amount", "0.1")))

        # Borrow configuration
        self.borrow_token = self.get_config("borrow_token", "USDC")
        self.ltv_target = Decimal(str(self.get_config("ltv_target", "0.5")))  # 50% LTV

        # Risk parameters
        self.min_health_factor = Decimal(str(self.get_config("min_health_factor", "2.0")))

        # Interest rate mode: "variable" or "stable"
        self.interest_rate_mode = self.get_config("interest_rate_mode", "variable")

        # Force action for testing
        self.force_action = str(self.get_config("force_action", "")).lower()

        # Internal state tracking
        self._loop_state = "idle"  # idle -> supplying -> supplied -> borrowing -> complete
        self._previous_stable_state = "idle"  # Revert target on intent failure
        self._supplied_amount = Decimal("0")
        self._borrowed_amount = Decimal("0")

        logger.info(
            f"AaveBorrowStrategy initialized: "
            f"collateral={self.collateral_amount} {self.collateral_token}, "
            f"borrow_token={self.borrow_token}, "
            f"LTV target={self.ltv_target * 100}%, "
            f"min HF={self.min_health_factor}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Make a lending decision based on market conditions and current state.

        Decision Flow:
        1. If force_action is set, execute that action
        2. If not supplied yet, supply collateral
        3. If supplied but not borrowed, borrow
        4. If fully looped, hold

        Parameters:
            market: MarketSnapshot containing prices, balances, etc.

        Returns:
            Intent: SUPPLY, BORROW, SWAP, or HOLD
        """
        # =================================================================
        # STEP 1: Get current market prices
        # =================================================================

        try:
            collateral_price = market.price(self.collateral_token)
            borrow_price = market.price(self.borrow_token)
            logger.debug(
                f"Prices: {self.collateral_token}=${collateral_price:.2f}, {self.borrow_token}=${borrow_price:.2f}"
            )
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not get prices: {e}")
            # Use reasonable defaults for testing
            collateral_price = Decimal("3400")  # ETH price
            borrow_price = Decimal("1")  # USDC price

        # =================================================================
        # STEP 2: Handle forced actions (for testing)
        # =================================================================

        if self.force_action == "supply":
            logger.info("Forced action: SUPPLY collateral")
            return self._create_supply_intent()

        elif self.force_action == "borrow":
            logger.info("Forced action: BORROW against collateral")
            return self._create_borrow_intent(collateral_price, borrow_price)

        # =================================================================
        # STEP 3: Check balances
        # =================================================================

        try:
            collateral_balance = market.balance(self.collateral_token)
            logger.debug(f"Collateral balance: {collateral_balance} {self.collateral_token}")
        except (ValueError, KeyError):
            logger.warning("Could not verify balances")
            collateral_balance = self.collateral_amount  # Assume we have it

        # =================================================================
        # STEP 4: State machine logic
        # =================================================================

        # State: IDLE - need to supply collateral
        if self._loop_state == "idle":
            # Check we have enough collateral
            # collateral_balance is a TokenBalance object, extract the balance value
            balance_value = (
                collateral_balance.balance if hasattr(collateral_balance, "balance") else collateral_balance
            )
            if balance_value < self.collateral_amount:
                return Intent.hold(
                    reason=f"Insufficient {self.collateral_token}: {balance_value} < {self.collateral_amount}"
                )

            logger.info("State: IDLE -> Supplying collateral")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.STATE_CHANGE,
                    description="State: IDLE -> Supplying collateral",
                    strategy_id=self.strategy_id,
                    details={"old_state": "idle", "new_state": "supplying"},
                )
            )
            self._previous_stable_state = self._loop_state
            self._loop_state = "supplying"
            return self._create_supply_intent()

        # State: SUPPLIED - need to borrow
        elif self._loop_state == "supplied":
            logger.info("State: SUPPLIED -> Borrowing")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.STATE_CHANGE,
                    description="State: SUPPLIED -> Borrowing",
                    strategy_id=self.strategy_id,
                    details={"old_state": "supplied", "new_state": "borrowing"},
                )
            )
            self._previous_stable_state = self._loop_state
            self._loop_state = "borrowing"
            return self._create_borrow_intent(collateral_price, borrow_price)

        # State: COMPLETE - done borrowing
        elif self._loop_state == "complete":
            return Intent.hold(reason="Loop complete - position established")

        # Safety net: if we're in a transitional state (supplying, borrowing)
        # it means the previous intent failed. Revert to last stable state.
        else:
            if self._loop_state in ("supplying", "borrowing"):
                revert_to = self._previous_stable_state
                logger.warning(
                    f"Stuck in transitional state '{self._loop_state}' — reverting to '{revert_to}'"
                )
                self._loop_state = revert_to
            return Intent.hold(reason=f"Waiting for state transition (current: {self._loop_state})")

    # =========================================================================
    # INTENT CREATION HELPERS
    # =========================================================================

    def _create_supply_intent(self) -> Intent:
        """
        Create a SUPPLY intent to deposit collateral into Aave V3.

        Supplying is the first step:
        1. Your tokens go into the Aave pool
        2. You receive aTokens (interest-bearing)
        3. Your supply can be used as collateral for borrowing

        Returns:
            SupplyIntent ready for compilation
        """
        logger.info(
            f"📥 SUPPLY intent: {format_token_amount_human(self.collateral_amount, self.collateral_token)} to Aave V3"
        )

        return Intent.supply(
            protocol="aave_v3",
            token=self.collateral_token,
            amount=self.collateral_amount,
            use_as_collateral=True,  # Enable as collateral for borrowing
            chain=self.chain,
        )

    def _create_borrow_intent(self, collateral_price: Decimal, borrow_price: Decimal) -> Intent:
        """
        Create a BORROW intent to borrow against supplied collateral.

        Borrowing calculation:
        1. Calculate collateral value in USD
        2. Apply target LTV to get safe borrow amount
        3. Convert to borrow token units

        Parameters:
            collateral_price: Current price of collateral token
            borrow_price: Current price of borrow token

        Returns:
            BorrowIntent ready for compilation
        """
        # Calculate collateral value
        collateral_value = self.collateral_amount * collateral_price

        # Calculate safe borrow amount based on target LTV
        # LTV = Borrow Value / Collateral Value
        # Borrow Value = Collateral Value * LTV
        max_borrow_value = collateral_value * self.ltv_target

        # Convert to borrow token units
        borrow_amount = max_borrow_value / borrow_price

        # Round down for safety
        borrow_amount = borrow_amount.quantize(Decimal("0.01"))

        logger.info(
            f"📤 BORROW intent: Collateral={format_usd(collateral_value)}, "
            f"LTV={self.ltv_target * 100:.0f}%, "
            f"Borrow={format_token_amount_human(borrow_amount, self.borrow_token)}"
        )

        return Intent.borrow(
            protocol="aave_v3",
            collateral_token=self.collateral_token,
            collateral_amount=Decimal("0"),  # Already supplied
            borrow_token=self.borrow_token,
            borrow_amount=borrow_amount,
            interest_rate_mode=self.interest_rate_mode,
            chain=self.chain,
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """
        Called after an intent is executed.

        Updates internal state to track borrow progress.
        """
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SUPPLY":
                self._loop_state = "supplied"
                self._supplied_amount = self.collateral_amount
                logger.info(f"Supply successful - state: {self._loop_state}")
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Supplied {self.collateral_amount} {self.collateral_token}",
                        strategy_id=self.strategy_id,
                        details={
                            "action": "supply",
                            "amount": str(self.collateral_amount),
                            "token": self.collateral_token,
                        },
                    )
                )

            elif intent_type == "BORROW":
                self._loop_state = "complete"
                if hasattr(intent, "borrow_amount"):
                    self._borrowed_amount = Decimal(str(intent.borrow_amount))
                logger.info("Borrow successful - loop complete")
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Borrowed {self.borrow_token}",
                        strategy_id=self.strategy_id,
                        details={"action": "borrow", "token": self.borrow_token},
                    )
                )
            elif intent_type == "REPAY":
                self._borrowed_amount = Decimal("0")
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Repaid {self.borrow_token}",
                        strategy_id=self.strategy_id,
                        details={"action": "repay", "token": self.borrow_token},
                    )
                )
            elif intent_type == "WITHDRAW":
                self._supplied_amount = Decimal("0")
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Withdrew {self.collateral_token}",
                        strategy_id=self.strategy_id,
                        details={"action": "withdraw", "token": self.collateral_token},
                    )
                )

        else:
            # On failure, revert to previous stable state so decide() can retry
            revert_to = self._previous_stable_state
            logger.warning(
                f"{intent_type} failed in state '{self._loop_state}' — reverting to '{revert_to}'"
            )
            self._loop_state = revert_to

    # =========================================================================
    # STATUS REPORTING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "demo_aave_borrow",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "collateral_token": self.collateral_token,
                "collateral_amount": str(self.collateral_amount),
                "borrow_token": self.borrow_token,
                "ltv_target": str(self.ltv_target),
                "min_health_factor": str(self.min_health_factor),
            },
            "state": {
                "loop_state": self._loop_state,
                "supplied_amount": str(self._supplied_amount),
                "borrowed_amount": str(self._borrowed_amount),
            },
        }

    # =========================================================================
    # STATE PERSISTENCE
    # =========================================================================

    def get_persistent_state(self) -> dict[str, Any]:
        """Get state to persist for crash recovery.

        This allows the strategy to resume from where it left off.
        """
        return {
            "loop_state": self._loop_state,
            "previous_stable_state": self._previous_stable_state,
            "supplied_amount": str(self._supplied_amount),
            "borrowed_amount": str(self._borrowed_amount),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Load persisted state on startup.

        Called when resuming a strategy after crash/restart.
        """
        if "loop_state" in state:
            self._loop_state = state["loop_state"]
            logger.info(f"Restored loop_state: {self._loop_state}")
        if "previous_stable_state" in state:
            self._previous_stable_state = state["previous_stable_state"]

        if "supplied_amount" in state:
            self._supplied_amount = Decimal(str(state["supplied_amount"]))
            logger.info(f"Restored supplied_amount: {self._supplied_amount}")

        if "borrowed_amount" in state:
            self._borrowed_amount = Decimal(str(state["borrowed_amount"]))
            logger.info(f"Restored borrowed_amount: {self._borrowed_amount}")

    # =========================================================================
    # TEARDOWN INTERFACE
    # =========================================================================
    # These methods enable safe strategy teardown.
    # For Aave borrow, teardown order is: REPAY -> WITHDRAW

    def supports_teardown(self) -> bool:
        """This strategy supports the teardown system."""
        return True

    def _get_gateway_client(self) -> Any:
        """Get the gateway client for on-chain queries, if available.

        Returns the gateway client from the compiler (set by the runner),
        or None if no gateway connection is available.
        """
        compiler = getattr(self, "_compiler", None)
        if compiler is not None:
            client = getattr(compiler, "_gateway_client", None)
            if client is not None:
                return client
        return None

    def _query_aave_positions_via_gateway(self, gateway_client: Any) -> list["PositionInfo"] | None:
        """Query Aave V3 positions through the gateway's RPC service.

        Uses eth_call routed through the gateway to query the Aave V3
        PoolDataProvider contract. This avoids direct web3 usage and
        keeps all RPC calls centralized through the gateway.

        Returns:
            List of PositionInfo if successful, None if query fails.
        """
        import json

        from almanak.framework.backtesting.paper.position_queries import (
            AAVE_V3_POOL_DATA_PROVIDER,
            GET_USER_RESERVE_DATA_SELECTOR,
            _pad_address,
            _parse_aave_user_reserve_data,
        )
        from almanak.framework.data.tokens import get_token_resolver
        from almanak.framework.teardown import PositionInfo, PositionType
        from almanak.gateway.proto import gateway_pb2

        if self.chain not in AAVE_V3_POOL_DATA_PROVIDER:
            logger.debug(f"Chain {self.chain} not supported for Aave V3 position queries")
            return None

        data_provider = AAVE_V3_POOL_DATA_PROVIDER[self.chain]

        # Resolve token addresses via the canonical TokenResolver
        resolver = get_token_resolver()
        aave_symbols = list(dict.fromkeys([
            self.collateral_token,
            self.borrow_token,
            "WETH", "USDC", "USDC.e", "USDT", "DAI", "WBTC", "LINK", "ARB", "wstETH",
        ]))
        token_entries: list[tuple[str, str]] = []  # (address, symbol)
        resolved_symbols: set[str] = set()
        for symbol in aave_symbols:
            try:
                resolved = resolver.resolve(symbol, self.chain)
                token_entries.append((resolved.address, symbol))
                resolved_symbols.add(symbol)
            except Exception as e:  # noqa: BLE001
                logger.debug(f"Token {symbol} not resolvable on {self.chain}: {e}")
                continue

        missing_required = {self.collateral_token, self.borrow_token} - resolved_symbols
        if missing_required:
            logger.warning(
                "Failed to resolve required Aave tokens %s on %s; falling back to internal state.",
                ", ".join(sorted(missing_required)),
                self.chain,
            )
            return None

        onchain_positions: list[PositionInfo] = []
        had_failure = False

        for asset_address, _symbol in token_entries:
            calldata = GET_USER_RESERVE_DATA_SELECTOR + _pad_address(asset_address) + _pad_address(self.wallet_address)
            params = json.dumps([{"to": data_provider, "data": calldata}, "latest"])

            response = gateway_client.rpc.Call(
                gateway_pb2.RpcRequest(
                    chain=self.chain,
                    method="eth_call",
                    params=params,
                    id=f"aave-pos-{asset_address[:10]}",
                ),
                timeout=10.0,
            )

            if not response.success:
                logger.debug(f"eth_call failed for asset {asset_address}: {response.error}")
                had_failure = True
                continue

            result_hex = json.loads(response.result) if response.result else None
            if not result_hex or result_hex == "0x":
                continue

            result_bytes = bytes.fromhex(result_hex.replace("0x", ""))
            position = _parse_aave_user_reserve_data(result_bytes, asset_address, self.chain)

            if position is None or not position.is_active:
                continue

            if position.has_supply:
                supply_amount = Decimal(str(position.atoken_balance_decimal))
                if position.asset in {"WETH", "ETH", "wstETH"}:
                    supply_price = Decimal("3400")
                elif position.asset == "WBTC":
                    supply_price = Decimal("60000")
                else:
                    supply_price = Decimal("1")
                onchain_positions.append(
                    PositionInfo(
                        position_type=PositionType.SUPPLY,
                        position_id=f"aave-supply-{position.asset}-{self.chain}",
                        chain=self.chain,
                        protocol="aave_v3",
                        value_usd=supply_amount * supply_price,
                        details={"asset": position.asset, "amount": str(supply_amount)},
                    )
                )

            if position.has_debt:
                debt_amount = Decimal(str(position.total_debt_decimal))
                if position.asset in {"WETH", "ETH", "wstETH"}:
                    debt_price = Decimal("3400")
                elif position.asset == "WBTC":
                    debt_price = Decimal("60000")
                else:
                    debt_price = Decimal("1")
                onchain_positions.append(
                    PositionInfo(
                        position_type=PositionType.BORROW,
                        position_id=f"aave-borrow-{position.asset}-{self.chain}",
                        chain=self.chain,
                        protocol="aave_v3",
                        value_usd=debt_amount * debt_price,
                        details={
                            "asset": position.asset,
                            "amount": str(debt_amount),
                            "interest_rate_mode": self.interest_rate_mode,
                        },
                    )
                )

        if had_failure:
            logger.warning(
                "Gateway eth_call failed for at least one asset; falling back to internal state."
            )
            return None
        return onchain_positions

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get all open positions for teardown.

        Queries on-chain Aave positions via the gateway if available,
        otherwise falls back to internal state tracking.

        Returns:
            TeardownPositionSummary with supply and borrow positions
        """
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []

        # Try to query on-chain positions through the gateway (most accurate)
        gateway_client = self._get_gateway_client()
        if gateway_client is not None:
            try:
                onchain_positions = self._query_aave_positions_via_gateway(gateway_client)
                if onchain_positions is not None:
                    # On-chain query succeeded -- return its result even if empty
                    # (empty means wallet has no Aave positions, which is authoritative)
                    return TeardownPositionSummary(
                        strategy_id=self.STRATEGY_NAME,
                        timestamp=datetime.now(UTC),
                        positions=onchain_positions,
                    )
            except Exception as e:  # noqa: BLE001 (fallback is intentional for teardown)
                logger.warning(f"On-chain Aave position query via gateway failed: {e!r}. Falling back to internal state.")

        # Fallback to internal state tracking
        # Check for supplied collateral
        if self._supplied_amount > 0:
            # In production, would query on-chain value
            # For demo, estimate value based on supply amount
            supply_value = self._supplied_amount * Decimal("3400")  # Assume ETH price
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"aave-supply-{self.collateral_token}-{self.chain}",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=supply_value,
                    details={
                        "asset": self.collateral_token,
                        "amount": str(self._supplied_amount),
                    },
                )
            )

        # Check for borrowed amount
        if self._borrowed_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"aave-borrow-{self.borrow_token}-{self.chain}",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=self._borrowed_amount,  # USDC is ~$1
                    health_factor=Decimal("2.0"),  # Would query on-chain
                    details={
                        "asset": self.borrow_token,
                        "amount": str(self._borrowed_amount),
                        "interest_rate_mode": self.interest_rate_mode,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Generate intents to unwind the borrow position.

        Teardown order (CRITICAL for safety):
        1. REPAY: Repay borrowed amount first (frees collateral)
        2. WITHDRAW: Withdraw supplied collateral
        3. SWAP: Swap everything to USDC

        Args:
            mode: TeardownMode.SOFT (graceful) or TeardownMode.HARD (emergency)

        Returns:
            List of intents in correct execution order
        """

        intents = []

        # Step 1: Repay borrowed amount (if any)
        if self._borrowed_amount > 0:
            intents.append(
                Intent.repay(
                    token=self.borrow_token,
                    amount=self._borrowed_amount,
                    protocol="aave_v3",
                    repay_full=True,  # Repay full amount including interest
                )
            )

        # Step 2: Withdraw supplied collateral (if any)
        if self._supplied_amount > 0:
            intents.append(
                Intent.withdraw(
                    token=self.collateral_token,
                    amount=self._supplied_amount,
                    protocol="aave_v3",
                    withdraw_all=True,  # Withdraw everything
                )
            )

        # Step 3: Swap collateral to USDC
        # Use the supplied_amount since that's what we're withdrawing
        # Note: "all" only works in IntentSequences, not standalone intents
        if self._supplied_amount > 0:
            intents.append(
                Intent.swap(
                    from_token=self.collateral_token,
                    to_token="USDC",
                    amount=self._supplied_amount,
                )
            )

        return intents

    def on_teardown_started(self, mode: "TeardownMode") -> None:
        """Called when teardown starts."""
        from almanak.framework.teardown import TeardownMode

        mode_name = "graceful" if mode == TeardownMode.SOFT else "emergency"
        logger.info(f"Teardown started in {mode_name} mode for Aave Borrow strategy")
        logger.info(f"Will repay ${self._borrowed_amount} and withdraw {self._supplied_amount} {self.collateral_token}")

    def on_teardown_completed(self, success: bool, recovered_usd: Decimal) -> None:
        """Called when teardown completes."""
        if success:
            logger.info(f"Teardown completed. Recovered ${recovered_usd:,.2f}")
            # Reset state
            self._loop_state = "idle"
            self._supplied_amount = Decimal("0")
            self._borrowed_amount = Decimal("0")
        else:
            logger.error("Teardown failed - manual intervention may be required")


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("AaveBorrowStrategy - Demo Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {AaveBorrowStrategy.STRATEGY_NAME}")
    print(f"Version: {AaveBorrowStrategy.STRATEGY_METADATA.version}")
    print(f"Supported Chains: {AaveBorrowStrategy.SUPPORTED_CHAINS}")
    print(f"Supported Protocols: {AaveBorrowStrategy.SUPPORTED_PROTOCOLS}")
    print(f"Intent Types: {AaveBorrowStrategy.INTENT_TYPES}")
    print(f"\nDescription: {AaveBorrowStrategy.STRATEGY_METADATA.description}")
    print("\nTo run this strategy:")
    print("  python -m src.cli.run --strategy demo_aave_borrow --once")
    print("\nTo test on Anvil:")
    print("  python strategies/demo/aave_borrow/run_anvil.py")
