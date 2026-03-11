"""Lagoon ERC-7540 Vault-Wrapped Momentum Strategy (Template).

Demonstrates how ANY IntentStrategy becomes vault-wrapped by adding a `vault`
block to config.json. The framework handles valuation, settlement, and epoch
management transparently -- the strategy author writes ZERO vault code.

IMPORTANT: This strategy requires a deployed Lagoon vault. Before running:
1. Deploy a Lagoon vault using LagoonVaultDeployer (see blueprint 24)
2. Set vault_address and valuator_address in config.json
3. Assign the valuator role on-chain: vault.setValuationManager(safe_address)

Strategy Logic (Momentum):
  A simple 24-hour momentum strategy that rotates between WETH and USDC:
  - If WETH price increased > momentum_threshold_pct in last 24h -> buy WETH (ride momentum)
  - If WETH price decreased > momentum_threshold_pct in last 24h -> sell to USDC (de-risk)
  - Otherwise -> hold

  This is intentionally simple alpha logic to focus on the vault wrapping.
  The framework handles:
  - Periodic NAV valuation (calls default valuate() which sums token balances)
  - Propose new total_assets to the vault contract
  - Settle deposits/redemptions from vault depositors
  - Crash recovery if the process dies mid-settlement

How It Works (zero vault code in this file):
  1. StrategyRunner calls VaultLifecycleManager.pre_decide_hook() before decide()
  2. If settlement is due, VaultLifecycleManager runs the full propose->settle cycle
  3. Then decide() is called normally -- it knows nothing about the vault
  4. The vault block in config.json is the ONLY thing that enables vault wrapping
"""

import logging
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
    name="lagoon_vault_template",
    description="Template: Lagoon vault-wrapped momentum strategy on Base",
    version="1.0.0",
    author="Almanak",
    tags=["vault", "lagoon", "erc7540", "momentum", "template", "base"],
    supported_chains=["base"],
    supported_protocols=["aerodrome"],
    intent_types=["SWAP", "HOLD"],
)
class LagoonVaultMomentum(IntentStrategy):
    """Momentum strategy wrapped by Lagoon ERC-7540 vault via config.json.

    NOTE: This strategy has NO vault code. All vault lifecycle management
    (settlement, valuation, epoch tracking) is handled by the framework
    based on the `vault` block in config.json.

    The strategy only implements decide() -- pure alpha logic.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.trade_size_usd = Decimal(str(self.get_config("trade_size_usd", "500")))
        self.momentum_window = int(self.get_config("momentum_window", 24))
        self.momentum_threshold_pct = Decimal(str(self.get_config("momentum_threshold_pct", "2.0")))
        self.max_slippage_bps = int(self.get_config("max_slippage_bps", 100))
        self.base_token = self.get_config("base_token", "WETH")
        self.quote_token = self.get_config("quote_token", "USDC")

        # Track momentum state
        self._last_price: Decimal | None = None
        self._current_position = "neutral"  # "long", "short", "neutral"
        self._trades_executed = 0

        logger.info(
            "LagoonVaultMomentum initialized: pair=%s/%s, "
            "trade_size=$%s, momentum_window=%dh, threshold=%s%%",
            self.base_token, self.quote_token,
            self.trade_size_usd, self.momentum_window,
            self.momentum_threshold_pct,
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Pure momentum logic -- no vault awareness needed.

        The framework handles vault settlement before this method is called.
        This method only returns swap/hold intents for the alpha strategy.
        """
        try:
            current_price = market.price(self.base_token)

            # First iteration: establish baseline
            if self._last_price is None:
                self._last_price = current_price
                return Intent.hold(reason=f"Establishing price baseline: {self.base_token}=${current_price:.2f}")

            # Calculate momentum
            price_change_pct = ((current_price - self._last_price) / self._last_price) * Decimal("100")

            # Get balances
            try:
                quote_bal = market.balance(self.quote_token)
                base_bal = market.balance(self.base_token)
            except (ValueError, KeyError) as e:
                logger.warning("Balance unavailable: %s", e)
                return Intent.hold(reason=f"Balance unavailable: {e}")

            slippage = Decimal(str(self.max_slippage_bps)) / Decimal("10000")

            # Strong upward momentum -> buy base token
            if price_change_pct > self.momentum_threshold_pct:
                if quote_bal.balance_usd < self.trade_size_usd:
                    return Intent.hold(
                        reason=f"Momentum +{price_change_pct:.2f}% but insufficient {self.quote_token}"
                    )
                logger.info(
                    "BUY: %s momentum +%s%% > threshold %s%%",
                    self.base_token, price_change_pct, self.momentum_threshold_pct,
                )
                self._last_price = current_price
                self._current_position = "long"
                return Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=slippage,
                    protocol="aerodrome",
                )

            # Strong downward momentum -> sell to quote token
            if price_change_pct < -self.momentum_threshold_pct:
                min_sell = self.trade_size_usd / current_price
                if base_bal.balance < min_sell:
                    return Intent.hold(
                        reason=f"Momentum {price_change_pct:.2f}% but insufficient {self.base_token}"
                    )
                logger.info(
                    "SELL: %s momentum %s%% < -%s%%",
                    self.base_token, price_change_pct, self.momentum_threshold_pct,
                )
                self._last_price = current_price
                self._current_position = "short"
                return Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount_usd=self.trade_size_usd,
                    max_slippage=slippage,
                    protocol="aerodrome",
                )

            # No strong momentum -> hold
            self._last_price = current_price
            return Intent.hold(
                reason=f"Momentum {price_change_pct:+.2f}% within threshold "
                f"[{-self.momentum_threshold_pct}%, +{self.momentum_threshold_pct}%]"
            )

        except Exception as e:
            logger.exception("Error in decide(): %s", e)
            return Intent.hold(reason=f"Error: {e}")

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track trade execution."""
        if success and intent.intent_type.value == "SWAP":
            self._trades_executed += 1
            logger.info("Trade #%d executed successfully", self._trades_executed)

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "lagoon_vault_template",
            "chain": self.chain,
            "position": self._current_position,
            "trades_executed": self._trades_executed,
            "last_price": str(self._last_price) if self._last_price else None,
            "base_token": self.base_token,
            "quote_token": self.quote_token,
        }

    def get_persistent_state(self) -> dict[str, Any]:
        """Persist state for crash recovery."""
        return {
            "last_price": str(self._last_price) if self._last_price else None,
            "current_position": self._current_position,
            "trades_executed": self._trades_executed,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Restore persisted state on startup."""
        if state.get("last_price"):
            self._last_price = Decimal(str(state["last_price"]))
        if "current_position" in state:
            self._current_position = state["current_position"]
        if "trades_executed" in state:
            self._trades_executed = int(state["trades_executed"])
        logger.info(
            "Restored state: position=%s, trades=%d",
            self._current_position, self._trades_executed,
        )

    # -------------------------------------------------------------------------
    # Teardown
    # -------------------------------------------------------------------------

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self):
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._current_position == "long":
            try:
                # We hold base_token -- report it as an open position
                positions.append(
                    PositionInfo(
                        position_type=PositionType.TOKEN,
                        position_id=f"lagoon-momentum-{self.base_token}",
                        chain=self.chain,
                        protocol="aerodrome",
                        value_usd=self.trade_size_usd,  # approximate
                        details={
                            "token": self.base_token,
                            "position": self._current_position,
                        },
                    )
                )
            except Exception:
                pass

        from datetime import UTC, datetime as dt
        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=dt.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None):
        from almanak.framework.teardown import TeardownMode

        intents = []
        if self._current_position == "long":
            slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")
            intents.append(
                Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount="all",
                    max_slippage=slippage,
                    protocol="aerodrome",
                )
            )
        return intents
