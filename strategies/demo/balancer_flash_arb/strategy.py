"""Balancer Flash Loan Arbitrage Demo Strategy.

Exercises the Balancer flash loan connector on Arbitrum -- the first kitchenloop
test of this connector across 65 iterations. The Balancer connector in the SDK
is flash-loan only (not a DEX swap adapter), so this strategy tests flash loan
intent compilation with Enso swap callbacks.

WHAT THIS TESTS:
1. FlashLoanIntent compilation with provider="balancer"
2. Balancer Vault calldata generation (zero-fee flash loan)
3. Enso swap callbacks inside flash loan context
4. Fallback to simple Enso swap when flash loan isn't needed

BALANCER FLASH LOANS:
- Zero fees (unlike Aave's 0.09%)
- Borrowed via Balancer Vault (same address on all chains)
- Must repay borrowed amount in same transaction (no fee)
- Ideal for arbitrage where profit covers gas only
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_balancer_flash_arb",
    description="Demo: Balancer flash loan with Enso swap callbacks on Arbitrum",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "balancer", "flash-loan", "arbitrage", "enso"],
    supported_chains=["arbitrum"],
    supported_protocols=["balancer", "enso"],
    intent_types=["FLASH_LOAN", "SWAP", "HOLD"],
)
class BalancerFlashArbStrategy(IntentStrategy):
    """Demo strategy testing Balancer flash loan intent compilation.

    CONFIGURATION (from config.json):
        flash_loan_amount_usd: USD value to flash loan
        max_slippage_pct: Max slippage for swap callbacks
        base_token: Token to trade (e.g., "WETH")
        quote_token: Quote token (e.g., "USDC")
        force_action: Force "flash_loan" or "swap" for testing
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.flash_loan_amount_usd = Decimal(str(self.get_config("flash_loan_amount_usd", "1000")))
        self.max_slippage_pct = float(self.get_config("max_slippage_pct", 1.0))
        self.base_token = self.get_config("base_token", "WETH")
        self.quote_token = self.get_config("quote_token", "USDC")
        self.force_action = self.get_config("force_action", None)
        self._trades_executed = 0

        # flash_loan_amount_usd is passed as raw token units to Intent.flash_loan,
        # so quote_token must be a dollar-pegged stablecoin for the amount to make sense.
        _USD_TOKENS = {"USDC", "USDT", "DAI", "USDC.E", "USDBC"}
        if self.quote_token.upper() not in _USD_TOKENS:
            logger.warning(
                f"quote_token '{self.quote_token}' is not a known USD stablecoin. "
                f"flash_loan_amount_usd ({self.flash_loan_amount_usd}) will be used as raw token units."
            )

        logger.info(
            f"BalancerFlashArbStrategy initialized: "
            f"flash_loan={format_usd(self.flash_loan_amount_usd)}, "
            f"pair={self.base_token}/{self.quote_token}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Decide: emit flash loan intent or simple swap for testing.

        In force_action="flash_loan" mode, creates a Balancer flash loan
        that borrows USDC and swaps through Enso (round-trip arbitrage pattern).

        In force_action="swap" mode, creates a simple Enso swap as fallback.
        """
        if self.force_action == "flash_loan":
            logger.info("Force action: Balancer flash loan with Enso swap callbacks")
            return self._create_flash_loan_intent()
        elif self.force_action == "swap":
            logger.info("Force action: simple Enso swap (fallback)")
            return self._create_swap_intent()
        else:
            return Intent.hold(reason="No action forced -- set force_action in config.json")

    def _create_flash_loan_intent(self) -> Intent:
        """Create a Balancer flash loan intent with swap callbacks.

        Pattern: Borrow USDC via Balancer -> swap USDC->WETH via Enso ->
        swap WETH->USDC via Enso -> repay USDC (zero fee).

        This is a round-trip that should return approximately the same amount
        (minus swap fees/slippage). Real arbitrage would use different DEX
        routes for a profit.
        """
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")

        self._trades_executed += 1

        return Intent.flash_loan(
            provider="balancer",
            token=self.quote_token,
            amount=self.flash_loan_amount_usd,
            callback_intents=[
                Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount=self.flash_loan_amount_usd,
                    max_slippage=max_slippage,
                    protocol="enso",
                ),
                Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount="all",
                    max_slippage=max_slippage,
                    protocol="enso",
                ),
            ],
            chain="arbitrum",
        )

    def _create_swap_intent(self) -> Intent:
        """Create a simple Enso swap as a fallback test."""
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")
        self._trades_executed += 1
        return Intent.swap(
            from_token=self.quote_token,
            to_token=self.base_token,
            amount_usd=Decimal("3"),
            max_slippage=max_slippage,
            protocol="enso",
        )

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_balancer_flash_arb",
            "chain": self.chain,
            "trades_executed": self._trades_executed,
        }

    def to_dict(self) -> dict[str, Any]:
        metadata = self.get_metadata()
        config_dict = self.config if isinstance(self.config, dict) else {}
        return {
            "strategy_name": self.__class__.STRATEGY_NAME,
            "chain": self.chain,
            "wallet_address": self.wallet_address,
            "config": config_dict,
            "config_version": self.get_current_config_version(),
            "current_intent": self._current_intent.serialize() if self._current_intent else None,
            "metadata": metadata.to_dict() if metadata else None,
        }

    # Teardown support
    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        # Only report a position if a swap was actually executed (not just a flash loan round-trip)
        if self._trades_executed > 0 and self.force_action == "swap":
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="balancer_flash_arb_token_0",
                    chain=self.chain,
                    protocol="enso",
                    value_usd=Decimal("0"),
                    details={"asset": self.base_token},
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_balancer_flash_arb"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        # Only generate teardown intents if a swap was actually executed
        if self._trades_executed == 0 or self.force_action != "swap":
            return []

        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal(str(self.max_slippage_pct)) / Decimal("100")
        return [
            Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount="all",
                max_slippage=max_slippage,
                protocol="enso",
            )
        ]
