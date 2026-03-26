"""Morpho-Aave Cross-Protocol Yield Rotation Strategy.

Monitors effective supply yields across Morpho Blue and Aave V3, and rotates
capital (wstETH) to whichever protocol currently offers the better rate.
When the spread between the two protocols exceeds a configurable threshold,
executes a sequential withdraw -> supply rebalance via IntentSequence.

Note: This is a yield rotation strategy, not a risk-free arbitrage. The edge
comes from actively monitoring rate differentials and rotating capital, but
rates can change between decision and execution. Gas costs must be considered.

Key Features:
    - Cross-protocol yield comparison (Morpho Blue vs Aave V3)
    - IntentSequence for sequential rebalancing (withdraw -> supply)
    - Dynamic spread threshold accounting for estimated gas costs
    - Cooldown period to avoid excessive rebalancing
    - Circuit breaker after consecutive unprofitable rotations
    - State persistence for crash recovery
    - Manual APY overrides for testing
    - Full teardown support

Example:
    # Run on Anvil with forced protocol selection
    almanak strat run -d strategies/reference/morpho_aave_arb --network anvil --once

    # Force initial deployment to Morpho
    Set config: "force_protocol": "morpho"
"""

import logging
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_token_amount_human

logger = logging.getLogger(__name__)

# Protocol identifiers
MORPHO = "morpho_blue"
AAVE = "aave_v3"
WALLET = "wallet"


@almanak_strategy(
    name="morpho_aave_yield_rotation",
    description="Cross-protocol yield rotation between Morpho Blue and Aave V3",
    version="1.1.0",
    author="Almanak",
    tags=["reference", "lending", "yield-rotation", "morpho", "aave", "yield"],
    supported_chains=["ethereum"],
    supported_protocols=["morpho_blue", "aave_v3"],
    intent_types=["SUPPLY", "WITHDRAW", "HOLD"],
)
class MorphoAaveYieldRotationStrategy(IntentStrategy):
    """Cross-protocol yield rotation between Morpho Blue and Aave V3.

    This strategy demonstrates:
    - Cross-protocol capital rotation based on yield differentials
    - IntentSequence for sequential multi-step operations
    - Dynamic spread thresholds that account for gas costs
    - Circuit breaker for consecutive unprofitable rotations
    - State tracking across protocol boundaries
    - Configurable thresholds and cooldowns

    State Machine:
        IDLE -> DEPLOYED_MORPHO | DEPLOYED_AAVE
        DEPLOYED_MORPHO -> REBALANCING_TO_AAVE -> DEPLOYED_AAVE
        DEPLOYED_AAVE -> REBALANCING_TO_MORPHO -> DEPLOYED_MORPHO

    Running Notes:
        - Use ``--fresh`` flag on Anvil to clear stale state from previous runs.
        - Use ``force_protocol`` config to force initial deployment for testing.
        - Use ``morpho_apy_override`` / ``aave_apy_override`` to simulate rate changes.

    Example::

        # Deploy to best protocol on Anvil
        almanak strat run -d strategies/reference/morpho_aave_arb --fresh --once --network anvil

        # Run continuously to monitor and rebalance
        almanak strat run -d strategies/reference/morpho_aave_arb --fresh --interval 30 --network anvil
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            if hasattr(self.config, "get"):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        # Token to arbitrage
        self.token = get_config("token", "wstETH")

        # Morpho Blue market ID (wstETH/USDC on Ethereum by default)
        self.morpho_market_id = get_config(
            "morpho_market_id",
            "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
        )

        # Amount to deploy
        self.deploy_amount = Decimal(str(get_config("deploy_amount", "0.5")))

        # Threshold: minimum spread in basis points to trigger rebalance
        self.min_spread_bps = int(get_config("min_spread_bps", 50))

        # Cooldown between rebalances
        self.cooldown_seconds = int(get_config("cooldown_seconds", 3600))

        # Manual APY overrides (for testing)
        morpho_override = get_config("morpho_apy_override", None)
        aave_override = get_config("aave_apy_override", None)
        self.morpho_apy_override = Decimal(str(morpho_override)) if morpho_override is not None else None
        self.aave_apy_override = Decimal(str(aave_override)) if aave_override is not None else None

        # Force initial deployment to a specific protocol (for testing)
        self.force_protocol = str(get_config("force_protocol", "")).lower()

        # Gas cost estimation (in USD) for a withdraw+supply rotation on Ethereum
        self.estimated_gas_cost_usd = Decimal(str(get_config("estimated_gas_cost_usd", "15")))

        # Circuit breaker: pause after N consecutive unprofitable rotations
        self.max_consecutive_losses = int(get_config("max_consecutive_losses", 3))

        # Internal state
        self._active_protocol: str = WALLET  # Where capital currently lives
        self._deployed_amount = Decimal("0")
        self._last_rebalance_time: float = 0.0
        self._rebalance_count = 0
        self._pending_target: str = ""  # Target protocol during rebalance
        self._last_known_price = Decimal("0")  # Last price for teardown valuation
        self._consecutive_losses = 0  # Circuit breaker counter

        logger.info(
            f"MorphoAaveArbStrategy initialized: "
            f"token={self.token}, "
            f"deploy_amount={self.deploy_amount}, "
            f"min_spread={self.min_spread_bps}bps, "
            f"cooldown={self.cooldown_seconds}s"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Decide whether to deploy, rebalance, or hold.

        Decision flow:
        1. If capital is in wallet (IDLE) -> deploy to best protocol
        2. If deployed -> check if rebalancing is profitable
        3. If spread > threshold and cooldown expired -> rebalance
        4. Otherwise -> hold

        Args:
            market: Current market snapshot with prices and balances.

        Returns:
            Intent to execute or Hold.
        """
        # Cache latest price for teardown valuation
        try:
            self._last_known_price = market.price(self.token)
        except (ValueError, KeyError):
            pass

        # Get APY estimates for both protocols
        try:
            morpho_apy = self._get_morpho_apy(market)
            aave_apy = self._get_aave_apy(market)
        except ValueError as e:
            logger.warning(f"APY data unavailable: {e}")
            return Intent.hold(reason=f"APY data unavailable: {e}")

        logger.info(
            f"APY comparison: Morpho={morpho_apy:.2f}%, Aave={aave_apy:.2f}% | "
            f"Active: {self._active_protocol}, Amount: {self._deployed_amount}"
        )

        # Handle forced protocol (testing)
        if self.force_protocol and self._active_protocol == WALLET:
            if self.force_protocol in ("morpho", "morpho_blue"):
                return self._deploy_to_morpho()
            elif self.force_protocol in ("aave", "aave_v3"):
                return self._deploy_to_aave()

        # State: IDLE (capital in wallet)
        if self._active_protocol == WALLET:
            return self._handle_idle(market, morpho_apy, aave_apy)

        # State: DEPLOYED (capital in a protocol)
        return self._handle_deployed(market, morpho_apy, aave_apy)


    # =========================================================================
    # STATE HANDLERS
    # =========================================================================

    def _handle_idle(
        self, market: MarketSnapshot, morpho_apy: Decimal, aave_apy: Decimal
    ) -> Intent:
        """Handle IDLE state -- deploy capital to the best protocol."""
        # Check wallet balance
        try:
            balance = market.balance(self.token)
            available = balance.balance if hasattr(balance, "balance") else balance
            if available < self.deploy_amount:
                return Intent.hold(
                    reason=f"Insufficient {self.token}: {available} < {self.deploy_amount}"
                )
        except (ValueError, KeyError):
            return Intent.hold(reason=f"Cannot verify {self.token} balance -- skipping deployment")

        # Deploy to whichever has higher APY
        if morpho_apy >= aave_apy:
            logger.info(f"Deploying to Morpho Blue (APY: {morpho_apy:.2f}% vs Aave: {aave_apy:.2f}%)")
            return self._deploy_to_morpho()
        else:
            logger.info(f"Deploying to Aave V3 (APY: {aave_apy:.2f}% vs Morpho: {morpho_apy:.2f}%)")
            return self._deploy_to_aave()

    def _calculate_dynamic_threshold_bps(self) -> int:
        """Calculate minimum profitable spread accounting for gas costs.

        The static min_spread_bps is a floor. On top of that, we compute
        the annualized gas cost as a percentage of the position to ensure
        a rotation is actually profitable over the expected hold period.
        """
        if self._deployed_amount <= Decimal("0") or self._last_known_price <= Decimal("0"):
            return self.min_spread_bps

        position_usd = self._deployed_amount * self._last_known_price
        if position_usd <= Decimal("0"):
            return self.min_spread_bps

        # Gas cost as basis points of position (annualized assuming monthly rotation)
        gas_bps = int((self.estimated_gas_cost_usd / position_usd) * Decimal("10000") * Decimal("12"))
        dynamic_threshold = max(self.min_spread_bps, gas_bps + 20)  # 20bps safety margin

        if dynamic_threshold > self.min_spread_bps:
            logger.debug(
                f"Dynamic threshold: {dynamic_threshold}bps "
                f"(static: {self.min_spread_bps}bps, gas: {gas_bps}bps)"
            )

        return dynamic_threshold

    def _handle_deployed(
        self, market: MarketSnapshot, morpho_apy: Decimal, aave_apy: Decimal
    ) -> Intent:
        """Handle DEPLOYED state -- check if rebalancing is profitable."""
        # Circuit breaker check
        if self._consecutive_losses >= self.max_consecutive_losses:
            return Intent.hold(
                reason=f"Circuit breaker active: {self._consecutive_losses} consecutive "
                f"unprofitable rotations. Manual review recommended."
            )

        # Determine current and alternative protocol
        if self._active_protocol == MORPHO:
            current_apy = morpho_apy
            alt_apy = aave_apy
            alt_protocol = AAVE
        else:
            current_apy = aave_apy
            alt_apy = morpho_apy
            alt_protocol = MORPHO

        # Calculate spread
        spread_bps = int((alt_apy - current_apy) * Decimal("100"))

        if spread_bps <= 0:
            return Intent.hold(
                reason=f"Current protocol ({self._active_protocol}) has best rate: {current_apy:.2f}%"
            )

        # Use dynamic threshold that accounts for gas costs
        effective_threshold = self._calculate_dynamic_threshold_bps()
        if spread_bps < effective_threshold:
            return Intent.hold(
                reason=f"Spread {spread_bps}bps below threshold {effective_threshold}bps "
                f"(static: {self.min_spread_bps}bps + gas adjustment)"
            )

        # Check cooldown
        elapsed = time.time() - self._last_rebalance_time
        if elapsed < self.cooldown_seconds and self._last_rebalance_time > 0:
            remaining = int(self.cooldown_seconds - elapsed)
            return Intent.hold(
                reason=f"Cooldown active: {remaining}s remaining"
            )

        # Execute rebalance
        logger.info(
            f"Rebalancing: {self._active_protocol} ({current_apy:.2f}%) "
            f"-> {alt_protocol} ({alt_apy:.2f}%), spread={spread_bps}bps "
            f"(threshold: {effective_threshold}bps)"
        )

        return self._rebalance_to(alt_protocol)

    # =========================================================================
    # INTENT BUILDERS
    # =========================================================================

    def _deploy_to_morpho(self) -> Intent:
        """Create a supply intent for Morpho Blue."""
        logger.info(f"SUPPLY: {format_token_amount_human(self.deploy_amount, self.token)} to Morpho Blue")
        self._pending_target = MORPHO
        return Intent.supply(
            protocol="morpho_blue",
            token=self.token,
            amount=self.deploy_amount,
            use_as_collateral=True,
            market_id=self.morpho_market_id,
            chain=self.chain,
        )

    def _deploy_to_aave(self) -> Intent:
        """Create a supply intent for Aave V3."""
        logger.info(f"SUPPLY: {format_token_amount_human(self.deploy_amount, self.token)} to Aave V3")
        self._pending_target = AAVE
        return Intent.supply(
            protocol="aave_v3",
            token=self.token,
            amount=self.deploy_amount,
            use_as_collateral=True,
            chain=self.chain,
        )

    def _rebalance_to(self, target_protocol: str) -> Intent:
        """Create an IntentSequence to rebalance: withdraw current -> supply target.

        Uses Intent.sequence() with amount='all' chaining so the full withdrawn
        amount flows into the supply step.
        """
        self._pending_target = target_protocol

        # Build withdraw intent from current protocol
        if self._active_protocol == MORPHO:
            withdraw_intent = Intent.withdraw(
                protocol="morpho_blue",
                token=self.token,
                amount=self._deployed_amount,
                withdraw_all=True,
                market_id=self.morpho_market_id,
                chain=self.chain,
            )
        else:
            withdraw_intent = Intent.withdraw(
                protocol="aave_v3",
                token=self.token,
                amount=self._deployed_amount,
                withdraw_all=True,
                chain=self.chain,
            )

        # Build supply intent to target protocol
        if target_protocol == MORPHO:
            supply_intent = Intent.supply(
                protocol="morpho_blue",
                token=self.token,
                amount="all",
                use_as_collateral=True,
                market_id=self.morpho_market_id,
                chain=self.chain,
            )
        else:
            supply_intent = Intent.supply(
                protocol="aave_v3",
                token=self.token,
                amount="all",
                use_as_collateral=True,
                chain=self.chain,
            )

        return Intent.sequence(
            [withdraw_intent, supply_intent],
            description=f"Rebalance {self.token}: {self._active_protocol} -> {target_protocol}",
        )

    # =========================================================================
    # APY ESTIMATION
    # =========================================================================

    def _get_morpho_apy(self, market: MarketSnapshot) -> Decimal:
        """Get Morpho Blue effective APY.

        Uses manual override if set, otherwise estimates from market data.
        For Morpho Blue collateral, effective yield comes from the underlying
        asset (e.g., wstETH staking APR) rather than direct supply APY.
        """
        if self.morpho_apy_override is not None:
            return self.morpho_apy_override

        # Try to get from market indicators
        try:
            indicators = market.indicators
            if hasattr(indicators, "get_lending_rate"):
                rate = indicators.get_lending_rate("morpho_blue", self.token, "supply")
                if rate is not None:
                    return Decimal(str(rate))
        except Exception:
            pass

        # No data available -- raise so decide() catches and returns HOLD.
        # Never fall back to hardcoded APYs because they would silently
        # drive capital allocation decisions.
        raise ValueError(f"Morpho Blue APY unavailable for {self.token}")

    def _get_aave_apy(self, market: MarketSnapshot) -> Decimal:
        """Get Aave V3 supply APY.

        Uses manual override if set, otherwise estimates from market data.
        """
        if self.aave_apy_override is not None:
            return self.aave_apy_override

        # Try to get from market indicators
        try:
            indicators = market.indicators
            if hasattr(indicators, "get_lending_rate"):
                rate = indicators.get_lending_rate("aave_v3", self.token, "supply")
                if rate is not None:
                    return Decimal(str(rate))
        except Exception:
            pass

        # No data available -- raise so decide() catches and returns HOLD.
        raise ValueError(f"Aave V3 APY unavailable for {self.token}")

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track state after intent execution."""
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SUPPLY_COLLATERAL" or intent_type == "SUPPLY":
                self._active_protocol = self._pending_target or MORPHO
                # Track the actual amount supplied (from intent), not the config default
                if hasattr(intent, "amount") and isinstance(intent.amount, Decimal):
                    self._deployed_amount = intent.amount
                elif self._deployed_amount <= 0:
                    self._deployed_amount = self.deploy_amount
                self._last_rebalance_time = time.time()
                self._rebalance_count += 1
                # Reset circuit breaker on successful rotation
                self._consecutive_losses = 0
                self._pending_target = ""

                logger.info(
                    f"Supply successful -> active_protocol={self._active_protocol}, "
                    f"amount={self._deployed_amount}"
                )
                self._emit_event(
                    "supply",
                    f"Supplied {self.token} to {self._active_protocol}",
                    {"protocol": self._active_protocol, "amount": str(self._deployed_amount)},
                )

            elif intent_type == "WITHDRAW":
                # During rebalance, withdraw is step 1 -- don't update active_protocol yet
                # The subsequent supply will set the new active protocol
                logger.info(f"Withdraw successful from {self._active_protocol}")
                self._emit_event(
                    "withdraw",
                    f"Withdrew {self.token} from {self._active_protocol}",
                    {"protocol": self._active_protocol, "amount": str(self._deployed_amount)},
                )
                # Temporarily in wallet
                self._active_protocol = WALLET

        else:
            logger.warning(f"{intent_type} failed -- staying in current state")
            if self._pending_target:
                # A rebalance rotation failed -- count as unprofitable
                self._consecutive_losses += 1
                logger.warning(
                    f"Rotation failed ({self._consecutive_losses}/{self.max_consecutive_losses} "
                    f"consecutive losses before circuit breaker)"
                )
            self._pending_target = ""

    def _emit_event(self, action: str, description: str, details: dict) -> None:
        """Emit a timeline event."""
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.POSITION_MODIFIED,
                description=description,
                strategy_id=self.strategy_id,
                details={"action": action, "token": self.token, **details},
            )
        )

    # =========================================================================
    # STATUS & PERSISTENCE
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "morpho_aave_yield_rotation",
            "chain": self.chain,
            "token": self.token,
            "active_protocol": self._active_protocol,
            "deployed_amount": str(self._deployed_amount),
            "rebalance_count": self._rebalance_count,
            "last_rebalance": self._last_rebalance_time,
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "active_protocol": self._active_protocol,
            "deployed_amount": str(self._deployed_amount),
            "last_rebalance_time": self._last_rebalance_time,
            "rebalance_count": self._rebalance_count,
            "last_known_price": str(self._last_known_price),
            "consecutive_losses": self._consecutive_losses,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "active_protocol" in state:
            self._active_protocol = state["active_protocol"]
        if "deployed_amount" in state:
            self._deployed_amount = Decimal(str(state["deployed_amount"]))
        if "last_rebalance_time" in state:
            self._last_rebalance_time = float(state["last_rebalance_time"])
        if "rebalance_count" in state:
            self._rebalance_count = int(state["rebalance_count"])
        if "last_known_price" in state:
            self._last_known_price = Decimal(str(state["last_known_price"]))
        if "consecutive_losses" in state:
            self._consecutive_losses = int(state["consecutive_losses"])

        logger.info(
            f"Restored state: protocol={self._active_protocol}, "
            f"amount={self._deployed_amount}, rebalances={self._rebalance_count}"
        )

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def get_open_positions(self) -> "TeardownPositionSummary":  # noqa: F821
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._active_protocol != WALLET and self._deployed_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"arb-supply-{self._active_protocol}",
                    chain=self.chain,
                    protocol=self._active_protocol,
                    value_usd=self._deployed_amount * self._last_known_price,
                    details={
                        "token": self.token,
                        "amount": str(self._deployed_amount),
                        "protocol": self._active_protocol,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:  # noqa: F821
        intents = []
        if self._active_protocol == MORPHO and self._deployed_amount > 0:
            intents.append(
                Intent.withdraw(
                    protocol="morpho_blue",
                    token=self.token,
                    amount=self._deployed_amount,
                    withdraw_all=True,
                    market_id=self.morpho_market_id,
                    chain=self.chain,
                )
            )
        elif self._active_protocol == AAVE and self._deployed_amount > 0:
            intents.append(
                Intent.withdraw(
                    protocol="aave_v3",
                    token=self.token,
                    amount=self._deployed_amount,
                    withdraw_all=True,
                    chain=self.chain,
                )
            )
        return intents

    def on_teardown_started(self, mode: "TeardownMode") -> None:  # noqa: F821
        from almanak.framework.teardown import TeardownMode

        mode_name = "graceful" if mode == TeardownMode.SOFT else "emergency"
        logger.info(f"Teardown ({mode_name}): withdrawing {self._deployed_amount} {self.token} from {self._active_protocol}")

    def on_teardown_completed(self, success: bool, recovered_usd: Decimal) -> None:
        if success:
            logger.info(f"Teardown complete. Recovered ${recovered_usd:,.2f}")
            self._active_protocol = WALLET
            self._deployed_amount = Decimal("0")
        else:
            logger.error("Teardown failed -- manual intervention may be required")
