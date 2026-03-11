"""MetaMorpho Ethereum Yield Strategy.

Deposits USDC into the Steakhouse USDC MetaMorpho vault on Ethereum for
optimized lending yield across Morpho Blue markets. The vault's curator
(Steakhouse Financial) manages market allocation -- this strategy handles
the deposit/redeem lifecycle with risk-aware position management.

Strategy Logic:
1. IDLE: Check wallet USDC balance against min_deposit_usd threshold.
   If sufficient, deposit up to max_vault_allocation_pct of total portfolio.
2. DEPOSITED: Monitor vault share price for yield tracking.
   Redeem if yield drops below yield_floor_apy_bps or on teardown.
3. REBALANCING: After redeem, re-evaluate and deposit again if conditions met.

This is a real strategy targeting a production vault:
  Steakhouse USDC (0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB)
  - Curated by Steakhouse Financial
  - Allocates across multiple Morpho Blue USDC lending markets
  - ~$500M+ TVL at time of writing
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


@almanak_strategy(
    name="metamorpho_eth_yield",
    description="MetaMorpho USDC yield on Ethereum via Steakhouse vault",
    version="1.0.0",
    author="Almanak",
    tags=["vault", "metamorpho", "erc4626", "yield", "lending", "ethereum"],
    supported_chains=["ethereum"],
    supported_protocols=["metamorpho"],
    intent_types=["VAULT_DEPOSIT", "VAULT_REDEEM", "HOLD"],
)
class MetaMorphoEthYield(IntentStrategy):
    """MetaMorpho yield strategy depositing USDC into Steakhouse vault.

    State Machine:
        idle -> depositing -> deposited -> redeeming -> idle
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.vault_address = self.get_config(
            "vault_address",
            "0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
        )
        self.deposit_token = self.get_config("deposit_token", "USDC")
        self.deposit_amount = Decimal(str(self.get_config("deposit_amount", "1000")))
        self.min_deposit_usd = Decimal(str(self.get_config("min_deposit_usd", "100")))
        self.max_vault_allocation_pct = int(self.get_config("max_vault_allocation_pct", 80))
        self.rebalance_threshold_bps = int(self.get_config("rebalance_threshold_bps", 200))
        self.yield_floor_apy_bps = int(self.get_config("yield_floor_apy_bps", 50))

        # State
        self._state = "idle"
        self._previous_stable_state = "idle"
        self._deposit_assets = Decimal("0")
        self._deposit_shares = Decimal("0")
        self._deposit_timestamp: datetime | None = None
        self._redeem_assets = Decimal("0")
        self._total_yield_earned = Decimal("0")
        self._epochs_completed = 0

        logger.info(
            "MetaMorphoEthYield initialized: vault=%s, deposit=%s %s, "
            "max_alloc=%d%%, yield_floor=%dbps",
            self.vault_address[:10],
            self.deposit_amount,
            self.deposit_token,
            self.max_vault_allocation_pct,
            self.yield_floor_apy_bps,
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Advance the vault yield state machine."""
        try:
            if self._state == "idle":
                return self._handle_idle(market)
            elif self._state == "deposited":
                return self._handle_deposited(market)
            elif self._state in ("depositing", "redeeming"):
                revert_to = self._previous_stable_state
                logger.warning(
                    "Stuck in transitional state '%s' -- reverting to '%s'",
                    self._state, revert_to,
                )
                self._state = revert_to
                return Intent.hold(reason=f"Reverted from stuck state to '{revert_to}'")
            else:
                return Intent.hold(reason=f"Unknown state: {self._state}")
        except Exception as e:
            logger.exception("Error in decide(): %s", e)
            return Intent.hold(reason=f"Error: {e}")

    def _handle_idle(self, market: MarketSnapshot) -> Intent:
        """Evaluate whether to deposit into the vault."""
        try:
            balance_info = market.balance(self.deposit_token)
            available = balance_info.balance
            available_usd = balance_info.balance_usd
        except (ValueError, KeyError) as e:
            logger.warning("Could not check %s balance: %s", self.deposit_token, e)
            return Intent.hold(reason=f"Balance unavailable: {e}")

        if available_usd < self.min_deposit_usd:
            return Intent.hold(
                reason=f"Insufficient {self.deposit_token}: ${available_usd:.2f} < ${self.min_deposit_usd}"
            )

        # Calculate deposit amount respecting max allocation
        max_deposit = available * Decimal(self.max_vault_allocation_pct) / Decimal("100")
        deposit_amount = min(self.deposit_amount, max_deposit)

        if deposit_amount < Decimal("1"):
            return Intent.hold(reason="Deposit amount too small after allocation cap")

        logger.info(
            "DEPOSIT: %s %s into Steakhouse vault (available: %s, alloc cap: %d%%)",
            deposit_amount, self.deposit_token, available, self.max_vault_allocation_pct,
        )

        self._previous_stable_state = self._state
        self._state = "depositing"
        self._deposit_timestamp = datetime.now(UTC)

        return Intent.vault_deposit(
            protocol="metamorpho",
            vault_address=self.vault_address,
            amount=deposit_amount,
            chain=self.chain,
        )

    def _handle_deposited(self, market: MarketSnapshot) -> Intent:
        """Monitor position and decide whether to hold or redeem."""
        # For now, hold the position -- yield accrues passively
        # In a more advanced version, we would:
        # 1. Track share price over time to compute realized APY
        # 2. Redeem if APY drops below yield_floor_apy_bps
        # 3. Rebalance across multiple vaults

        if self._deposit_shares > 0:
            logger.info(
                "HOLD: Vault position active (shares=%s, deposited=%s %s, epoch=%d)",
                self._deposit_shares, self._deposit_assets, self.deposit_token,
                self._epochs_completed,
            )

        return Intent.hold(
            reason=f"Vault position active, earning yield (epoch {self._epochs_completed})"
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Update state after intent execution."""
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "VAULT_DEPOSIT":
                self._state = "deposited"
                self._epochs_completed += 1
                if hasattr(result, "extracted_data") and result.extracted_data:
                    deposit_data = result.extracted_data.get("deposit_data")
                    if deposit_data:
                        self._deposit_assets = Decimal(str(deposit_data.get("assets", 0)))
                        self._deposit_shares = Decimal(str(deposit_data.get("shares", 0)))
                        logger.info(
                            "Deposit confirmed: assets=%s, shares=%s",
                            self._deposit_assets, self._deposit_shares,
                        )
                logger.info("VAULT_DEPOSIT successful -> state=deposited")

            elif intent_type == "VAULT_REDEEM":
                if hasattr(result, "extracted_data") and result.extracted_data:
                    redeem_data = result.extracted_data.get("redeem_data")
                    if redeem_data:
                        self._redeem_assets = Decimal(str(redeem_data.get("assets_received", 0)))
                        yield_earned = self._redeem_assets - self._deposit_assets
                        if yield_earned > 0:
                            self._total_yield_earned += yield_earned
                        logger.info(
                            "Redeem confirmed: received=%s, yield=%s",
                            self._redeem_assets, yield_earned,
                        )
                self._state = "idle"
                self._deposit_assets = Decimal("0")
                self._deposit_shares = Decimal("0")
                logger.info("VAULT_REDEEM successful -> state=idle")
        else:
            revert_to = self._previous_stable_state
            logger.warning(
                "%s failed in state '%s' -- reverting to '%s'",
                intent_type, self._state, revert_to,
            )
            self._state = revert_to

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "metamorpho_eth_yield",
            "chain": self.chain,
            "state": self._state,
            "vault_address": self.vault_address,
            "deposit_token": self.deposit_token,
            "deposit_assets": str(self._deposit_assets),
            "deposit_shares": str(self._deposit_shares),
            "total_yield_earned": str(self._total_yield_earned),
            "epochs_completed": self._epochs_completed,
        }

    def get_persistent_state(self) -> dict[str, Any]:
        """Persist state for crash recovery."""
        return {
            "state": self._state,
            "previous_stable_state": self._previous_stable_state,
            "deposit_assets": str(self._deposit_assets),
            "deposit_shares": str(self._deposit_shares),
            "deposit_timestamp": self._deposit_timestamp.isoformat() if self._deposit_timestamp else None,
            "redeem_assets": str(self._redeem_assets),
            "total_yield_earned": str(self._total_yield_earned),
            "epochs_completed": self._epochs_completed,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Restore persisted state on startup."""
        if "state" in state:
            self._state = state["state"]
        if "previous_stable_state" in state:
            self._previous_stable_state = state["previous_stable_state"]
        if "deposit_assets" in state:
            self._deposit_assets = Decimal(str(state["deposit_assets"]))
        if "deposit_shares" in state:
            self._deposit_shares = Decimal(str(state["deposit_shares"]))
        if state.get("deposit_timestamp"):
            self._deposit_timestamp = datetime.fromisoformat(state["deposit_timestamp"])
        if "redeem_assets" in state:
            self._redeem_assets = Decimal(str(state["redeem_assets"]))
        if "total_yield_earned" in state:
            self._total_yield_earned = Decimal(str(state["total_yield_earned"]))
        if "epochs_completed" in state:
            self._epochs_completed = int(state["epochs_completed"])
        logger.info("Restored state: %s (epoch %d)", self._state, self._epochs_completed)

    # -------------------------------------------------------------------------
    # Teardown
    # -------------------------------------------------------------------------

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self):
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._state in ("deposited", "redeeming") and self._deposit_shares > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id=f"metamorpho-eth-{self.vault_address[:16]}",
                    chain=self.chain,
                    protocol="metamorpho",
                    value_usd=self._deposit_assets,  # approximate
                    details={
                        "vault_address": self.vault_address,
                        "deposit_token": self.deposit_token,
                        "shares": str(self._deposit_shares),
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None):
        intents = []
        if self._state in ("deposited", "redeeming") and self._deposit_shares > 0:
            intents.append(
                Intent.vault_redeem(
                    protocol="metamorpho",
                    vault_address=self.vault_address,
                    shares="all",
                    chain=self.chain,
                )
            )
        return intents
