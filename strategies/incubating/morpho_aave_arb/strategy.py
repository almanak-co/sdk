"""Morpho-Aave Supply Rate Arbitrage Strategy.

Monitors effective supply yields across Morpho Blue and Aave V3, and moves
capital (wstETH) to whichever protocol currently offers the better rate.
When the spread between the two protocols exceeds a configurable threshold,
executes an atomic withdraw -> supply sequence via IntentSequence.

Key Features:
    - Cross-protocol yield comparison (Morpho Blue vs Aave V3)
    - IntentSequence for atomic rebalancing (withdraw -> supply)
    - Configurable minimum spread threshold (bps)
    - Cooldown period to avoid excessive rebalancing
    - State persistence for crash recovery
    - Manual APY overrides for testing
    - Full teardown support

Example:
    # Run on Anvil with forced protocol selection
    almanak strat run -d strategies/incubating/morpho_aave_arb --network anvil --once

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
    name="demo_morpho_aave_arb",
    description="Supply rate arbitrage between Morpho Blue and Aave V3",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "lending", "arbitrage", "morpho", "aave", "yield"],
    supported_chains=["ethereum"],
    supported_protocols=["morpho_blue", "aave_v3"],
    intent_types=["SUPPLY", "WITHDRAW", "HOLD"],
)
class MorphoAaveArbStrategy(IntentStrategy):
    """Supply rate arbitrage between Morpho Blue and Aave V3.

    This strategy demonstrates:
    - Cross-protocol capital rotation based on yield differentials
    - IntentSequence for atomic multi-step operations
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
        almanak strat run -d strategies/incubating/morpho_aave_arb --fresh --once --network anvil

        # Run continuously to monitor and rebalance
        almanak strat run -d strategies/incubating/morpho_aave_arb --fresh --interval 30 --network anvil
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

        # Internal state
        self._active_protocol: str = WALLET  # Where capital currently lives
        self._deployed_amount = Decimal("0")
        self._last_rebalance_time: float = 0.0
        self._rebalance_count = 0
        self._pending_target: str = ""  # Target protocol during rebalance
        self._last_known_price = Decimal("0")  # Last price for teardown valuation

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
        try:
            # Cache latest price for teardown valuation
            try:
                self._last_known_price = market.price(self.token)
            except (ValueError, KeyError):
                pass

            # Get APY estimates for both protocols
            morpho_apy = self._get_morpho_apy(market)
            aave_apy = self._get_aave_apy(market)

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

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

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
            logger.warning("Could not verify wallet balance, proceeding")

        # Deploy to whichever has higher APY
        if morpho_apy >= aave_apy:
            logger.info(f"Deploying to Morpho Blue (APY: {morpho_apy:.2f}% vs Aave: {aave_apy:.2f}%)")
            return self._deploy_to_morpho()
        else:
            logger.info(f"Deploying to Aave V3 (APY: {aave_apy:.2f}% vs Morpho: {morpho_apy:.2f}%)")
            return self._deploy_to_aave()

    def _handle_deployed(
        self, market: MarketSnapshot, morpho_apy: Decimal, aave_apy: Decimal
    ) -> Intent:
        """Handle DEPLOYED state -- check if rebalancing is profitable."""
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

        # Check threshold
        if spread_bps < self.min_spread_bps:
            return Intent.hold(
                reason=f"Spread {spread_bps}bps below threshold {self.min_spread_bps}bps"
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
            f"-> {alt_protocol} ({alt_apy:.2f}%), spread={spread_bps}bps"
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
            "strategy": "demo_morpho_aave_arb",
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

        logger.info(
            f"Restored state: protocol={self._active_protocol}, "
            f"amount={self._deployed_amount}, rebalances={self._rebalance_count}"
        )

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def supports_teardown(self) -> bool:
        return True

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
