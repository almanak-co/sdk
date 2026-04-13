"""
===============================================================================
0G Swap Strategy — Swap A0GI via JAINE DEX (Uniswap V3 fork) on 0G Chain
===============================================================================

This strategy demonstrates token swapping on 0G Chain via JAINE DEX, which is
a Uniswap V3 fork with verified contracts.

WHAT THIS STRATEGY DOES:
------------------------
1. Wraps A0GI -> W0G (required for Uniswap V3 pool interaction)
2. Swaps W0G -> target token via JAINE DEX (Uniswap V3 router)
3. Holds when swap is complete

JAINE DEX:
----------
JAINE (Justified AI for Next-Gen Exchange) is a Uniswap V3 fork on 0G Chain.
Contracts are verified on the 0G block explorer:
- SwapRouter: 0x18cCa38E51c4C339A6BD6e174025f08360FEEf30
- Factory: 0x6F3945Ab27296D1D66D8EEB042ff1B4fb2E0CE70
- NonfungiblePositionManager: 0x5143ba6007C197b4cF66c20601b9dB97E0F98c6A

Since it's a standard Uniswap V3 fork, the SDK's existing uniswap_v3 connector
handles it natively — no custom connector needed.

USAGE:
------
    almanak strat run -d almanak/demo_strategies/0g_swap --network anvil --once
===============================================================================
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
    name="demo_0g_swap",
    description="Swap tokens on 0G Chain via JAINE DEX (Uniswap V3 fork)",
    version="2.0.0",
    author="Almanak",
    tags=["demo", "0g", "zerog", "swap", "jaine", "uniswap-v3"],
    supported_chains=["zerog"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
    default_chain="zerog",
)
class ZeroGSwapStrategy(IntentStrategy):
    """0G Chain swap strategy via JAINE DEX (Uniswap V3 fork).

    Configuration Parameters (from config.json):
    - swap_amount: Amount of A0GI to swap (default: "1.0")
    - target_token: Token to swap to (default: "W0G" — wrap only)
    - max_slippage_pct: Max slippage percentage (default: 1.0)
    - force_action: Force "swap" for testing (default: "")
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.swap_amount = Decimal(str(self.get_config("swap_amount", "1.0")))
        self.target_token = str(self.get_config("target_token", "W0G"))
        self.max_slippage_pct = float(self.get_config("max_slippage_pct", 1.0))
        self.force_action = str(self.get_config("force_action", "")).lower()

        self._swapped = False

        logger.info(
            f"ZeroGSwapStrategy initialized: swap_amount={self.swap_amount} A0GI -> {self.target_token}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Swap A0GI for target token via JAINE DEX.

        Decision Flow:
        1. If force_action is "swap", execute swap
        2. If not swapped yet, swap
        3. If done, hold
        """
        if self.force_action == "swap":
            return self._create_swap_intent()

        if not self._swapped:
            logger.info(f"Swapping {self.swap_amount} A0GI -> {self.target_token} via JAINE (Uniswap V3)")
            return self._create_swap_intent()

        return Intent.hold(reason=f"Swap complete: {self.swap_amount} A0GI -> {self.target_token}")

    def _create_swap_intent(self) -> Intent:
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")

        return Intent.swap(
            from_token="A0GI",
            to_token=self.target_token,
            amount=self.swap_amount,
            max_slippage=max_slippage,
            protocol="uniswap_v3",
            chain="zerog",
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        if success and intent.intent_type.value == "SWAP":
            self._swapped = True
            logger.info(f"Swap successful: {self.swap_amount} A0GI -> {self.target_token}")
        elif not success:
            logger.warning("Swap failed")

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_0g_swap",
            "chain": self.chain,
            "config": {
                "swap_amount": str(self.swap_amount),
                "target_token": self.target_token,
            },
            "state": {"swapped": self._swapped},
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {"swapped": self._swapped}

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "swapped" in state:
            self._swapped = state["swapped"]

    def get_open_positions(self):
        from almanak.framework.teardown import TeardownPositionSummary

        return TeardownPositionSummary.empty(self.strategy_id or self.STRATEGY_NAME)

    def generate_teardown_intents(self, mode=None, market=None):
        return []
