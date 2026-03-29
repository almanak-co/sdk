"""
===============================================================================
Uniswap V4 Swap Demo — BUY + SELL Lifecycle via UniversalRouter
===============================================================================

Demonstrates the Uniswap V4 swap path end-to-end:

1. First run: BUY — swap USDC -> WETH via V4 UniversalRouter (Permit2 flow)
2. Second run: SELL — swap WETH -> USDC via V4 UniversalRouter
3. Subsequent runs: alternate BUY/SELL

KEY V4 SWAP CONCEPTS:
- V4 uses a singleton PoolManager (all pools in one contract)
- Swaps route through the canonical UniversalRouter with Permit2 approvals
- Pool identified by PoolKey = (currency0, currency1, fee, tickSpacing, hooks)
- V4SwapExactInSingle command encoded in UniversalRouter.execute()
- Receipt events: PoolManager emits Swap(poolId, sender, amount0, amount1, ...)

USAGE:
    # Run on Anvil fork (auto-starts Anvil + gateway)
    almanak strat run -d strategies/demo/uniswap_v4_swap --network anvil --once

    # Run twice to see BUY then SELL
    almanak strat run -d strategies/demo/uniswap_v4_swap --network anvil --once
    almanak strat run -d strategies/demo/uniswap_v4_swap --network anvil --once

===============================================================================
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_usd

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_uniswap_v4_swap",
    description="V4 swap demo — BUY/SELL lifecycle via UniversalRouter with Permit2",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "v4", "swap", "uniswap", "permit2"],
    supported_chains=["ethereum", "arbitrum", "base", "optimism"],
    supported_protocols=["uniswap_v4"],
    intent_types=["SWAP", "HOLD"],
    default_chain="ethereum",
)
class UniswapV4SwapStrategy(IntentStrategy):
    """Uniswap V4 swap demo: alternates BUY and SELL each iteration.

    Configuration Parameters (from config.json):
        trade_size_usd: Amount to trade per signal (default: 3)
        max_slippage_bps: Maximum slippage in basis points (default: 200 = 2%)
        base_token: Token to buy/sell (default: WETH)
        quote_token: Stable token (default: USDC)
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.trade_size_usd = Decimal(str(self.get_config("trade_size_usd", "3")))
        self.max_slippage_bps = int(self.get_config("max_slippage_bps", 200))
        self.base_token: str = self.get_config("base_token", "WETH")
        self.quote_token: str = self.get_config("quote_token", "USDC")

        self._max_slippage = Decimal(self.max_slippage_bps) / Decimal(10000)

    def decide(self, market: MarketSnapshot) -> Intent:
        """Decide whether to BUY or SELL based on state.

        First call: BUY (swap quote -> base).
        After a BUY: SELL (swap base -> quote).
        After a SELL: BUY again.
        """
        # Determine action from persisted state
        last_action = self.state.get("last_action", "SELL")  # default triggers BUY first

        base_price = market.price(self.base_token)
        quote_balance = market.balance(self.quote_token)
        base_balance = market.balance(self.base_token)

        logger.info(
            "V4 swap decision | last=%s | %s=%s | %s=%s | %s=%s",
            last_action,
            self.base_token,
            format_usd(base_price),
            self.quote_token,
            format_usd(quote_balance.balance_usd),
            self.base_token,
            format_usd(base_balance.balance_usd),
        )

        if last_action == "SELL":
            # BUY: swap quote -> base
            if quote_balance.balance_usd < self.trade_size_usd:
                return Intent.hold(
                    reason=f"Insufficient {self.quote_token} for BUY "
                    f"(need {format_usd(self.trade_size_usd)}, have {format_usd(quote_balance.balance_usd)})"
                )

            logger.info("BUY: %s %s -> %s via Uniswap V4", format_usd(self.trade_size_usd), self.quote_token, self.base_token)

            return Intent.swap(
                from_token=self.quote_token,
                to_token=self.base_token,
                amount_usd=self.trade_size_usd,
                max_slippage=self._max_slippage,
                protocol="uniswap_v4",
            )

        else:
            # SELL: swap base -> quote
            if base_balance.balance_usd < self.trade_size_usd:
                return Intent.hold(
                    reason=f"Insufficient {self.base_token} for SELL "
                    f"(need {format_usd(self.trade_size_usd)}, have {format_usd(base_balance.balance_usd)})"
                )

            # Compute base token amount from USD (round down to avoid overspend)
            if base_price and base_price > 0:
                sell_amount = (self.trade_size_usd / base_price).quantize(Decimal("1E-18"), rounding=ROUND_DOWN)
            else:
                return Intent.hold(reason=f"No price available for {self.base_token}")

            logger.info("SELL: %s %s -> %s via Uniswap V4", sell_amount, self.base_token, self.quote_token)

            return Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount=sell_amount,
                max_slippage=self._max_slippage,
                protocol="uniswap_v4",
            )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Update state only after successful execution."""
        if not success:
            return
        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return
        from_token = getattr(intent, "from_token", None)
        if from_token == self.quote_token:
            self.state["last_action"] = "BUY"
            logger.info("BUY executed successfully")
        elif from_token == self.base_token:
            self.state["last_action"] = "SELL"
            logger.info("SELL executed successfully")

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Return open positions (base token holdings) for teardown preview."""
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        try:
            base_balance = self.market.balance(self.base_token) if hasattr(self, "market") and self.market else None
            if base_balance and base_balance.balance_usd > Decimal("0.01"):
                positions.append(
                    PositionInfo(
                        position_type=PositionType.TOKEN,
                        position_id=f"v4_swap_{self.base_token.lower()}",
                        chain=self.chain,
                        protocol="uniswap_v4",
                        value_usd=base_balance.balance_usd,
                        details={
                            "token": self.base_token,
                            "balance": str(base_balance.balance),
                        },
                    )
                )
        except Exception:
            logger.warning("Failed to query balance for teardown; reporting no positions", exc_info=True)

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_uniswap_v4_swap"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Generate intents to close all positions (swap base -> quote)."""
        from almanak.framework.teardown import TeardownMode

        intents: list[Intent] = []

        try:
            m = market or (self.market if hasattr(self, "market") else None)
            if m is None:
                return intents

            base_balance = m.balance(self.base_token)
            if base_balance and base_balance.balance > Decimal("0"):
                slippage = max(Decimal("0.03"), self._max_slippage) if mode == TeardownMode.HARD else self._max_slippage
                intents.append(
                    Intent.swap(
                        from_token=self.base_token,
                        to_token=self.quote_token,
                        amount=base_balance.balance,
                        max_slippage=slippage,
                        protocol="uniswap_v4",
                    )
                )
        except Exception:
            logger.warning("Failed to generate teardown intents", exc_info=True)

        return intents
