"""
===============================================================================
0G Gimo Stake Strategy — Stake A0GI for yield-bearing st0G
===============================================================================

This strategy demonstrates liquid staking on 0G Chain via Gimo Finance.

WHAT THIS STRATEGY DOES:
------------------------
1. Monitors A0GI balance in the wallet
2. When A0GI balance > min_stake_amount: Stakes A0GI with Gimo Finance
3. Receives st0G (yield-bearing liquid staking derivative)
4. Holds when already staked or insufficient balance

WHAT IS GIMO FINANCE?
---------------------
Gimo Finance is a liquid staking protocol on 0G Chain built on StaFi's EVM LSD
Stack. It provides:
- st0G: Liquid staking derivative of A0GI
- Monotonically increasing exchange rate (no slashing on 0G)
- 22-day unbonding period for withdrawals

RISKS:
------
- Smart Contract Risk: Gimo contracts are unverified on the block explorer
- Chain Risk: 0G Chain is a nascent L1 with ~$1.19M ecosystem TVL
- Liquidity Risk: Limited exit liquidity for st0G
- Unbonding Period: 22-day wait to unstake st0G

USAGE:
------
    almanak strat run -d almanak/demo_strategies/0g_gimo_stake --network anvil --once
===============================================================================
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.utils.log_formatters import format_token_amount_human

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_0g_gimo_stake",
    description="Stake A0GI with Gimo Finance on 0G Chain for yield-bearing st0G",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "staking", "gimo", "0g", "zerog", "liquid-staking"],
    supported_chains=["zerog"],
    supported_protocols=["gimo"],
    intent_types=["STAKE", "HOLD"],
    default_chain="zerog",
)
class ZeroGGimoStakeStrategy(IntentStrategy):
    """Gimo Finance liquid staking strategy on 0G Chain.

    Configuration Parameters (from config.json):
    - min_stake_amount: Minimum A0GI balance to trigger staking (default: "10")
    - force_action: Force "stake" for testing (default: "")
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.min_stake_amount = Decimal(str(self.get_config("min_stake_amount", "10")))
        self.force_action = str(self.get_config("force_action", "")).lower()

        self._staked = False
        self._staked_amount = Decimal("0")

        logger.info(f"ZeroGGimoStakeStrategy initialized: min_stake={self.min_stake_amount} A0GI")

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make a staking decision based on wallet balance.

        Decision Flow:
        1. If force_action is "stake", stake the min amount
        2. If already staked, hold
        3. If A0GI balance > min_stake_amount, stake
        4. Otherwise, hold
        """
        if self.force_action == "stake":
            logger.info("Forced action: STAKE A0GI")
            return self._create_stake_intent(self.min_stake_amount)

        if self._staked:
            return Intent.hold(reason=f"Already staked {self._staked_amount} A0GI -> st0G")

        a0gi_balance_value = Decimal("0")
        try:
            a0gi_balance = market.balance("A0GI")
            a0gi_balance_value = a0gi_balance.balance if hasattr(a0gi_balance, "balance") else a0gi_balance
            logger.debug(f"A0GI balance: {a0gi_balance_value}")
        except (ValueError, KeyError) as e:
            logger.debug(f"Could not get A0GI balance: {e}")

        # Reserve 2 A0GI for gas — A0GI is both the staking asset and the gas token
        stake_amount = a0gi_balance_value - Decimal("2")
        if stake_amount >= self.min_stake_amount:
            logger.info(f"A0GI balance ({a0gi_balance_value}) sufficient, staking {stake_amount} A0GI")
            return self._create_stake_intent(stake_amount)

        return Intent.hold(reason=f"Insufficient A0GI balance: {a0gi_balance_value} < {self.min_stake_amount}")

    def _create_stake_intent(self, amount: Decimal) -> Intent:
        """Create a STAKE intent to deposit A0GI with Gimo Finance."""
        logger.info(f"STAKE intent: {format_token_amount_human(amount, 'A0GI')} -> st0G")

        return Intent.stake(
            protocol="gimo",
            token_in="A0GI",
            amount=amount,
            receive_wrapped=False,
            chain="zerog",
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        intent_type = intent.intent_type.value

        if success and intent_type == "STAKE":
            self._staked = True
            if hasattr(intent, "amount"):
                self._staked_amount = intent.amount if isinstance(intent.amount, Decimal) else Decimal("0")
            logger.info(f"Staking successful: {self._staked_amount} A0GI -> st0G")

        elif not success:
            logger.warning(f"{intent_type} failed")

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_0g_gimo_stake",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "min_stake_amount": str(self.min_stake_amount),
            },
            "state": {
                "staked": self._staked,
                "staked_amount": str(self._staked_amount),
            },
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "staked": self._staked,
            "staked_amount": str(self._staked_amount),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "staked" in state:
            self._staked = state["staked"]
        if "staked_amount" in state:
            self._staked_amount = Decimal(str(state["staked_amount"]))

    def get_open_positions(self):
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        if not self._staked or self._staked_amount <= 0:
            return TeardownPositionSummary.empty(self.strategy_id or self.STRATEGY_NAME)

        return TeardownPositionSummary(
            strategy_id=self.strategy_id or self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=[
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="gimo_st0g_stake",
                    chain=self.chain,
                    protocol="gimo",
                    value_usd=Decimal("0"),  # No reliable USD pricing for st0G
                    details={"asset": "st0G", "staked_a0gi": str(self._staked_amount)},
                )
            ],
        )

    def generate_teardown_intents(self, mode=None, market=None):
        if not self._staked or self._staked_amount <= 0:
            return []

        return [
            Intent.unstake(
                protocol="gimo",
                token_in="st0G",
                amount=self._staked_amount,
                chain="zerog",
            )
        ]
