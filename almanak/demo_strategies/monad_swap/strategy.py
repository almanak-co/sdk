"""Monad Basic Swap Demo Strategy.

The simplest possible swap strategy on Monad: swap tokens via
Uniswap V3 on every iteration (if funds allow).

What this strategy does:
    1. Checks from_token balance
    2. Swaps a fixed USD amount via Uniswap V3
    3. Holds if insufficient balance

This is the "hello world" of Monad strategies -- no indicators, no signals,
just a clean on-chain swap showing the core execution flow.

Usage:
    almanak strat run -d almanak/demo_strategies/monad_swap --network anvil --once
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_monad_swap",
    description="Basic swap demo on Monad via Uniswap V3",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "monad", "swap", "uniswap-v3", "beginner"],
    supported_chains=["monad"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
    default_chain="monad",
)
class MonadSwapStrategy(IntentStrategy):
    """Basic swap demo on Monad.

    Configuration (config.json):
        trade_size_usd: Amount per swap in USD (default: 5)
        from_token: Token to sell (default: USDC)
        to_token: Token to buy (default: WETH)
        max_slippage_bps: Max slippage in basis points (default: 300)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.trade_size_usd = Decimal(str(self.get_config("trade_size_usd", "5")))
        self.from_token = self.get_config("from_token", "USDC")
        self.to_token = self.get_config("to_token", "WETH")
        self.max_slippage_bps = int(self.get_config("max_slippage_bps", 300))

        logger.info(
            f"MonadSwap initialized: "
            f"swap ${self.trade_size_usd} {self.from_token} -> {self.to_token} "
            f"via Uniswap V3 on Monad"
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
            f"SWAP ${self.trade_size_usd} {self.from_token} -> {self.to_token} via Uniswap V3"
        )

        return Intent.swap(
            from_token=self.from_token,
            to_token=self.to_token,
            amount_usd=self.trade_size_usd,
            max_slippage=max_slippage,
            protocol="uniswap_v3",
        )

    # -- Teardown --

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self):
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []

        try:
            market = self.create_market_snapshot()
            to_balance = market.balance(self.to_token)
            if to_balance.balance > 0:
                positions.append(
                    PositionInfo(
                        position_type=PositionType.TOKEN,
                        position_id=f"monad_swap_{self.to_token.lower()}",
                        chain=self.chain,
                        protocol="uniswap_v3",
                        value_usd=to_balance.balance_usd,
                        details={"asset": self.to_token, "balance": str(to_balance.balance)},
                    )
                )
        except Exception:
            logger.warning("Failed to query balance for teardown; reporting no positions")

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_monad_swap"),
            timestamp=datetime.now(UTC),
            positions=positions,
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
                protocol="uniswap_v3",
            )
        ]

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_monad_swap",
            "chain": self.chain,
            "pair": f"{self.from_token}/{self.to_token}",
            "trade_size_usd": str(self.trade_size_usd),
        }
