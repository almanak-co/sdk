"""Solana Forced-Swap Demo Strategy.

The simplest possible Solana strategy: swaps a fixed amount every iteration
via Jupiter aggregator. No indicators, no market data -- just a swap.

Usage:
    # Dry run (no real transaction):
    almanak strat run -d strategies/demo/solana_swap --once --dry-run

    # Real swap on mainnet:
    almanak strat run -d strategies/demo/solana_swap --once

Environment:
    SOLANA_PRIVATE_KEY   Base58 Ed25519 keypair (required)
    SOLANA_RPC_URL       Solana RPC endpoint (optional, defaults to public mainnet)
    JUPITER_API_KEY      Jupiter API key (optional, uses free tier if not set)
"""

import logging
from decimal import Decimal

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="solana_swap",
    version="0.1.0",
    description="Forced swap on Solana via Jupiter (demo)",
    supported_chains=["solana"],
    supported_protocols=["jupiter"],
    intent_types=["SWAP"],
)
class SolanaSwapStrategy(IntentStrategy):
    """Execute a single swap on Solana via Jupiter."""

    def decide(self, market: MarketSnapshot) -> Intent:
        try:
            from_token = self.config.get("from_token", "USDC")
            to_token = self.config.get("to_token", "SOL")
            amount = Decimal(str(self.config.get("amount", "1.0")))
            max_slippage = Decimal(str(self.config.get("max_slippage_pct", "1.0"))) / 100

            logger.info(f"Forcing swap: {amount} {from_token} -> {to_token}")

            return Intent.swap(
                from_token=from_token,
                to_token=to_token,
                amount=amount,
                max_slippage=max_slippage,
            )
        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def on_intent_executed(self, intent, success: bool, result):
        """Track swap state for teardown reversal."""
        if not success:
            return
        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return
        type_value = intent_type.value if hasattr(intent_type, "value") else str(intent_type)
        if type_value == "SWAP":
            self.state["swapped_from"] = getattr(intent, "from_token", None)
            self.state["swapped_to"] = getattr(intent, "to_token", None)
            self.state["has_swap"] = True
            logger.info(f"Tracked swap: {self.state['swapped_from']} -> {self.state['swapped_to']}")

    # -- Teardown (required by framework) --

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self):
        from datetime import UTC, datetime

        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self.state.get("has_swap"):
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="solana_swap_position",
                    chain="solana",
                    protocol="jupiter",
                    value_usd=Decimal("0"),
                    details={
                        "swapped_from": self.state.get("swapped_from"),
                        "swapped_to": self.state.get("swapped_to"),
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None):
        from almanak.framework.teardown import TeardownMode

        if not self.state.get("has_swap"):
            return []
        # Reverse the swap
        from_token = self.state.get("swapped_to")
        to_token = self.state.get("swapped_from")
        if not from_token or not to_token:
            return []
        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.01")
        return [
            Intent.swap(
                from_token=from_token,
                to_token=to_token,
                amount="all",
                max_slippage=max_slippage,
            )
        ]
