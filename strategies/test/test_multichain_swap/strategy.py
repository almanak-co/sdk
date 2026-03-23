"""Test multi-chain strategy: Bridge USDC Base->Arb, swap round-trip on Arb, bridge back.

4-step IntentSequence:
  1. Bridge USDC from Base to Arbitrum (via Across)
  2. Swap USDC -> WETH on Arbitrum
  3. Swap WETH -> USDC on Arbitrum
  4. Bridge USDC from Arbitrum back to Base (via Across)

Note: amount="all" chaining after bridge doesn't work (bridge receipt parser
doesn't extract output amounts), so steps 2-4 use explicit amounts.
"""

from decimal import Decimal

from almanak import IntentStrategy
from almanak.framework.intents import Intent
from almanak.framework.strategies.intent_strategy import MarketSnapshot, almanak_strategy


@almanak_strategy(
    name="test_multichain_swap",
    description="Test: bridge USDC Base->Arb, swap round-trip, bridge back",
    version="0.3.0",
    author="Almanak",
    tags=["test", "multi-chain", "bridge", "swap"],
    supported_chains=["base", "arbitrum"],
    supported_protocols=["uniswap_v3", "across"],
    intent_types=["SWAP", "BRIDGE", "HOLD"],
)
class TestMultiChainSwapStrategy(IntentStrategy):
    """Multi-chain: Bridge USDC Base->Arb, swap round-trip on Arb, bridge back."""

    def decide(self, market: MarketSnapshot):
        config = self.config if isinstance(self.config, dict) else self.config.__dict__
        bridge_amount = Decimal(str(config.get("swap_amount_usdc", "0.5")))
        force = config.get("force_action", "")

        if force != "execute":
            return Intent.hold(reason="No force_action=execute set")

        # Use explicit amounts since amount="all" doesn't chain after bridge
        swap_amount = bridge_amount - Decimal("0.01")  # Leave margin for bridge fee

        return Intent.sequence(
            [
                # Step 1: Bridge USDC from Base to Arbitrum
                Intent.bridge(
                    token="USDC",
                    amount=bridge_amount,
                    from_chain="base",
                    to_chain="arbitrum",
                    preferred_bridge="across",
                    max_slippage=Decimal("0.01"),
                ),
                # Step 2: Swap USDC -> WETH on Arbitrum
                Intent.swap(
                    from_token="USDC",
                    to_token="WETH",
                    amount=swap_amount,
                    protocol="uniswap_v3",
                    chain="arbitrum",
                    max_slippage=Decimal("0.01"),
                ),
                # Step 3: Swap WETH -> USDC on Arbitrum
                Intent.swap(
                    from_token="WETH",
                    to_token="USDC",
                    amount_usd=swap_amount,
                    protocol="uniswap_v3",
                    chain="arbitrum",
                    max_slippage=Decimal("0.01"),
                ),
                # Step 4: Bridge USDC from Arbitrum back to Base
                Intent.bridge(
                    token="USDC",
                    amount=swap_amount,
                    from_chain="arbitrum",
                    to_chain="base",
                    preferred_bridge="across",
                    max_slippage=Decimal("0.01"),
                ),
            ],
            description="Bridge USDC Base->Arb, swap USDC->WETH->USDC, bridge back",
        )

    # -- Teardown (required) --

    def supports_teardown(self) -> bool:
        return False

    def get_open_positions(self):
        from datetime import UTC, datetime

        from almanak.framework.teardown import TeardownPositionSummary

        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            positions=[],
        )

    def generate_teardown_intents(self, mode, market=None):
        return []
