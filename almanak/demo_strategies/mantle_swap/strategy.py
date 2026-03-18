"""Mantle Basic Swap Demo Strategy.

The simplest possible swap strategy on Mantle: buy WMNT with USDT via
Agni Finance on every iteration (if funds allow).

What this strategy does:
    1. Checks USDT balance
    2. Swaps a fixed USD amount of USDT -> WMNT via Agni Finance
    3. Holds if insufficient balance

This is the "hello world" of Mantle strategies — no indicators, no signals,
just a clean on-chain swap showing the core execution flow.

Usage:
    almanak strat run -d mantle_swap --network anvil --once
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_mantle_swap",
    description="Basic swap demo on Mantle L2 — USDT -> WMNT via Agni Finance",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "mantle", "swap", "agni", "beginner"],
    supported_chains=["mantle"],
    supported_protocols=["agni"],
    intent_types=["SWAP", "HOLD"],
    default_chain="mantle",
)
class MantleSwapStrategy(IntentStrategy):
    """Basic swap demo on Mantle.

    Configuration (config.json):
        trade_size_usd: Amount per swap in USD (default: 5)
        from_token: Token to sell (default: USDT)
        to_token: Token to buy (default: WMNT)
        max_slippage_bps: Max slippage in basis points (default: 300)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.trade_size_usd = Decimal(str(self.get_config("trade_size_usd", "5")))
        self.from_token = self.get_config("from_token", "USDT")
        self.to_token = self.get_config("to_token", "WMNT")
        self.max_slippage_bps = int(self.get_config("max_slippage_bps", 300))

        logger.info(
            f"MantleSwap initialized: "
            f"swap ${self.trade_size_usd} {self.from_token} -> {self.to_token} "
            f"via Agni Finance on Mantle"
        )

    def decide(self, market: MarketSnapshot) -> Intent:
        max_slippage = Decimal(str(self.max_slippage_bps)) / Decimal("10000")

        try:
            balance = market.balance(self.from_token)
        except (ValueError, KeyError) as e:
            return Intent.hold(reason=f"Balance unavailable: {e}")

        if balance.balance_usd < self.trade_size_usd:
            return Intent.hold(
                reason=f"Low {self.from_token} balance: "
                f"${balance.balance_usd:.2f} < ${self.trade_size_usd}"
            )

        logger.info(
            f"SWAP ${self.trade_size_usd} {self.from_token} -> {self.to_token} via Agni"
        )

        return Intent.swap(
            from_token=self.from_token,
            to_token=self.to_token,
            amount_usd=self.trade_size_usd,
            max_slippage=max_slippage,
            protocol="agni",
        )

    # -- Teardown --

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self):
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_mantle_swap"),
            timestamp=datetime.now(UTC),
            positions=[
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="mantle_swap_wmnt",
                    chain=self.chain,
                    protocol="agni",
                    value_usd=self.trade_size_usd,
                    details={"asset": self.to_token},
                )
            ],
        )

    def generate_teardown_intents(self, mode, market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.01")
        return [
            Intent.swap(
                from_token=self.to_token,
                to_token=self.from_token,
                amount="all",
                max_slippage=max_slippage,
                protocol="agni",
            )
        ]

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_mantle_swap",
            "chain": self.chain,
            "pair": f"{self.from_token}/{self.to_token}",
            "trade_size_usd": str(self.trade_size_usd),
        }
