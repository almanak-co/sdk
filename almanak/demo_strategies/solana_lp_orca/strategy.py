"""Solana Orca Whirlpools LP Demo Strategy.

The simplest possible Orca LP strategy: opens a concentrated liquidity
position on Orca Whirlpools in the SOL/USDC pool with a wide price range.

Usage:
    # Dry run (no real transaction):
    almanak strat run -d strategies/demo/solana_lp_orca --once --dry-run

    # Real LP open on mainnet:
    almanak strat run -d strategies/demo/solana_lp_orca --once

Environment:
    SOLANA_PRIVATE_KEY   Base58 Ed25519 keypair (required)
    SOLANA_RPC_URL       Solana RPC endpoint (optional, defaults to public mainnet)
"""

import logging
from decimal import Decimal

from almanak.framework.intents import Intent
from almanak.framework.intents.vocabulary import LPOpenIntent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="solana_lp_orca",
    version="0.1.0",
    description="Open Orca Whirlpool LP position on Solana (demo)",
    supported_chains=["solana"],
    default_chain="solana",
    supported_protocols=["orca_whirlpools"],
    intent_types=["LP_OPEN"],
)
class OrcaLPStrategy(IntentStrategy):
    """Open a concentrated liquidity position on Orca Whirlpools."""

    def decide(self, market: MarketSnapshot) -> Intent:
        try:
            pool = self.config.get("pool", "HJPjoWUrhoZzkNfRpHuieeFk9AnbKnovy8po1NtRSqX2")
            amount_sol = Decimal(str(self.config.get("amount_sol", "0.001")))
            amount_usdc = Decimal(str(self.config.get("amount_usdc", "0.15")))
            range_lower = Decimal(str(self.config.get("range_lower", "80")))
            range_upper = Decimal(str(self.config.get("range_upper", "200")))

            logger.info(
                f"Opening Orca Whirlpool LP position: "
                f"{amount_sol} SOL + {amount_usdc} USDC, "
                f"range [{range_lower}, {range_upper}]"
            )

            return LPOpenIntent(
                protocol="orca_whirlpools",
                pool=pool,
                amount0=amount_sol,
                amount1=amount_usdc,
                range_lower=range_lower,
                range_upper=range_upper,
            )
        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def on_intent_executed(self, intent, success: bool, result):
        """Track LP position ID for teardown."""
        if not success:
            return
        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return
        type_value = intent_type.value if hasattr(intent_type, "value") else str(intent_type)
        if type_value == "LP_OPEN":
            position_id = result.position_id if result else None
            if position_id:
                self.state["position_id"] = str(position_id)
                self.state["pool"] = getattr(intent, "pool", self.config.get("pool", ""))
                logger.info(f"Tracked Orca LP position: {position_id}")

    # -- Teardown (required by framework) --

    def get_open_positions(self):
        from datetime import UTC, datetime

        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self.state.get("position_id"):
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=self.state["position_id"],
                    chain="solana",
                    protocol="orca_whirlpools",
                    value_usd=Decimal("0"),
                    details={"pool": self.state.get("pool", "")},
                )
            )
        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None):
        if not self.state.get("position_id"):
            return []
        return [
            Intent.lp_close(
                protocol="orca_whirlpools",
                position_id=self.state["position_id"],
                pool=self.state.get("pool", ""),
                collect_fees=True,
            )
        ]
