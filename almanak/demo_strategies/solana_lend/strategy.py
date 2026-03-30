"""Solana Kamino Lending Demo Strategy.

The simplest possible Solana lending strategy: supplies a fixed amount of USDC
to Kamino Finance on every iteration. No indicators, no market data -- just a
deposit.

Usage:
    # Dry run (no real transaction):
    almanak strat run -d strategies/demo/solana_lend --once --dry-run

    # Real deposit on mainnet:
    almanak strat run -d strategies/demo/solana_lend --once

Environment:
    SOLANA_PRIVATE_KEY   Base58 Ed25519 keypair (required)
    SOLANA_RPC_URL       Solana RPC endpoint (optional, defaults to public mainnet)
"""

import logging
from decimal import Decimal

from almanak.framework.intents import Intent
from almanak.framework.intents.vocabulary import SupplyIntent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="solana_lend",
    version="0.1.0",
    description="Supply tokens to Kamino Finance on Solana (demo)",
    supported_chains=["solana"],
    default_chain="solana",
    supported_protocols=["kamino"],
    intent_types=["SUPPLY"],
)
class SolanaLendStrategy(IntentStrategy):
    """Supply tokens to Kamino Finance lending market."""

    def decide(self, market: MarketSnapshot) -> Intent:
        try:
            token = self.config.get("token", "USDC")
            amount = Decimal(str(self.config.get("amount", "1.0")))

            logger.info(f"Supplying {amount} {token} to Kamino Finance")

            return SupplyIntent(
                protocol="kamino",
                token=token,
                amount=amount,
            )
        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def on_intent_executed(self, intent, success: bool, result):
        """Track supply state for teardown withdrawal."""
        if not success:
            return
        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return
        type_value = intent_type.value if hasattr(intent_type, "value") else str(intent_type)
        if type_value == "SUPPLY":
            self.state["supplied_token"] = getattr(intent, "token", None)
            self.state["supplied_amount"] = str(getattr(intent, "amount", "0"))
            self.state["has_supply"] = True
            logger.info(f"Tracked supply: {self.state['supplied_amount']} {self.state['supplied_token']}")

    # -- Teardown (required by framework) --

    def get_open_positions(self):
        from datetime import UTC, datetime

        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self.state.get("has_supply"):
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id="kamino_supply",
                    chain="solana",
                    protocol="kamino",
                    value_usd=Decimal("0"),
                    details={
                        "token": self.state.get("supplied_token"),
                        "amount": self.state.get("supplied_amount"),
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None):
        if not self.state.get("has_supply"):
            return []
        token = self.state.get("supplied_token", "USDC")
        return [
            Intent.withdraw(
                protocol="kamino",
                token=token,
                amount="all",
                withdraw_all=True,
            )
        ]
