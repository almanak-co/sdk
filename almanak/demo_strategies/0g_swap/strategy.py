"""
===============================================================================
0G Wrap Strategy — Wrap native A0GI into W0G on 0G Chain
===============================================================================

Minimal demo exercising the WRAP_NATIVE intent on 0G Chain. Wraps a configured
amount of the native A0GI token into W0G and then holds.

WHAT THIS STRATEGY DOES:
------------------------
1. Wraps `wrap_amount` A0GI -> W0G via Intent.wrap() on chain "zerog"
2. Holds when the wrap is complete

Intent.wrap() resolves the wrapped-native symbol via the compiler's
`wrapped_symbols` map (zerog -> W0G). No custom connector is needed — the
framework's built-in WRAP_NATIVE compilation handles it.

USAGE:
------
    almanak strat run -d almanak/demo_strategies/0g_swap --network anvil --once
===============================================================================
"""

import logging
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_0g_swap",
    description="Wrap native A0GI into W0G on 0G Chain",
    version="2.0.0",
    author="Almanak",
    tags=["demo", "0g", "zerog", "wrap"],
    supported_chains=["zerog"],
    supported_protocols=[],
    intent_types=["WRAP_NATIVE", "HOLD"],
    default_chain="zerog",
    quote_asset="USD",
)
class ZeroGSwapStrategy(IntentStrategy):
    """0G Chain wrap strategy: A0GI -> W0G.

    Configuration Parameters (from config.json):
    - wrap_amount: Amount of A0GI to wrap to W0G (default: "5")
    - force_action: Force "wrap" (alias "swap" accepted for backward compat) (default: "")
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.wrap_amount = Decimal(str(self.get_config("wrap_amount", "5")))
        self.force_action = str(self.get_config("force_action", "")).lower()

        self._wrapped = False

        logger.info(
            f"ZeroGSwapStrategy initialized: wrap_amount={self.wrap_amount} A0GI -> W0G"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Wrap A0GI -> W0G on 0G Chain.

        Decision Flow:
        1. If force_action is "wrap" (or legacy alias "swap"), execute wrap
        2. If not wrapped yet, wrap
        3. If done, hold
        """
        if self.force_action in ("wrap", "swap"):
            return self._create_wrap_intent()

        if not self._wrapped:
            logger.info(f"Wrapping {self.wrap_amount} A0GI -> W0G")
            return self._create_wrap_intent()

        return Intent.hold(reason=f"Wrap complete: {self.wrap_amount} A0GI -> W0G")

    def _create_wrap_intent(self) -> Intent:
        return Intent.wrap(
            token="W0G",
            amount=self.wrap_amount,
            chain="zerog",
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        if success and intent.intent_type.value == "WRAP_NATIVE":
            self._wrapped = True
            logger.info(f"Wrap successful: {self.wrap_amount} A0GI -> W0G")
        elif not success:
            logger.warning("Wrap failed")

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_0g_swap",
            "chain": self.chain,
            "config": {
                "wrap_amount": str(self.wrap_amount),
            },
            "state": {"wrapped": self._wrapped},
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {"wrapped": self._wrapped}

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "wrapped" in state:
            self._wrapped = state["wrapped"]

    def get_open_positions(self):
        from almanak.framework.teardown import TeardownPositionSummary

        return TeardownPositionSummary.empty(self.deployment_id or self.STRATEGY_NAME)

    def generate_teardown_intents(self, mode=None, market=None):
        return []
