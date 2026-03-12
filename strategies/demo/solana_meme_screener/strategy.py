"""Solana Meme Coin Screener Demo Strategy.

Uses DexScreener to find trending Solana meme coins with strong momentum,
then swaps a small amount into the best candidate. Demonstrates how to
combine DexScreener screening data with Jupiter swaps.

Strategy logic:
    1. Screen DexScreener for Solana meme coins passing filters
    2. Score candidates by: buy pressure, volume, price momentum
    3. Pick the top-scoring token and swap into it
    4. On subsequent runs: check if position hits take-profit or stop-loss

Usage:
    # Dry run (logs what it would trade):
    almanak strat run -d strategies/demo/solana_meme_screener --once --dry-run

    # Real execution:
    almanak strat run -d strategies/demo/solana_meme_screener --once

Environment:
    SOLANA_PRIVATE_KEY   Base58 Ed25519 keypair (required)
    SOLANA_RPC_URL       Solana RPC endpoint (optional)
    JUPITER_API_KEY      Jupiter API key (optional, uses free tier if not set)
"""

import asyncio
import concurrent.futures
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.data.dexscreener import DexScreenerClient, DexPair
from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.teardown import TeardownPositionSummary

logger = logging.getLogger(__name__)


def _score_meme_candidate(pair: DexPair) -> float:
    """Score a meme coin candidate for trading potential.

    Factors:
        - Buy pressure (h1 buy ratio > 0.5 is bullish)
        - Volume momentum (h1 volume relative to h24 suggests acceleration)
        - Price momentum (h1 price change)
        - Liquidity depth (higher is safer)
    """
    score = 0.0

    # Buy pressure: 0-30 points
    buy_ratio = pair.txns.h1.buy_ratio
    score += max(0, (buy_ratio - 0.5)) * 60  # 0.6 ratio = 6pts, 0.7 = 12pts

    # Volume acceleration: 0-25 points
    if pair.volume.h24 > 0:
        h1_share = pair.volume.h1 / (pair.volume.h24 / 24)  # Expected hourly share = 1.0
        score += min(25, h1_share * 10)

    # Price momentum: 0-25 points
    h1_change = pair.price_change.h1
    if h1_change > 0:
        score += min(25, h1_change * 2.5)

    # Liquidity bonus: 0-20 points
    if pair.liquidity.usd >= 1_000_000:
        score += 20
    elif pair.liquidity.usd >= 500_000:
        score += 15
    elif pair.liquidity.usd >= 100_000:
        score += 10

    return score


@almanak_strategy(
    name="solana_meme_screener",
    version="0.1.0",
    description="DexScreener-powered meme coin momentum screener on Solana",
    supported_chains=["solana"],
    supported_protocols=["jupiter"],
    intent_types=["SWAP"],
)
class SolanaMemeScreenerStrategy(IntentStrategy):
    """Screen for trending Solana meme coins and trade the best one.

    Uses DexScreener API to find meme coins with strong buy pressure and
    momentum, then executes a swap via Jupiter. Tracks position state
    for take-profit / stop-loss on subsequent runs.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._position: dict | None = None

    def decide(self, market: MarketSnapshot) -> Intent:
        # decide() is called from within an async StrategyRunner, so
        # the event loop is already running. Use a thread to run our
        # async code without conflicting with the existing loop.
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, self._decide_async(market))
            return future.result(timeout=30)

    async def _decide_async(self, market: MarketSnapshot) -> Intent:
        """Async decision logic with DexScreener screening."""
        base_token = self.config.get("base_token", "USDC")
        swap_amount = Decimal(str(self.config.get("swap_amount", "1.0")))
        max_slippage = Decimal(str(self.config.get("max_slippage_pct", "2.0"))) / 100

        # Check if we have an open position to manage
        if self._position:
            return await self._manage_position(market, self._position, base_token, max_slippage)

        # Screen for new candidates
        return await self._screen_and_trade(market, base_token, swap_amount, max_slippage)

    async def _screen_and_trade(
        self,
        market: MarketSnapshot,
        base_token: str,
        swap_amount: Decimal,
        max_slippage: Decimal,
    ) -> Intent:
        """Screen DexScreener for the best meme coin candidate."""
        min_liquidity = float(self.config.get("min_liquidity_usd", 100_000))
        min_volume = float(self.config.get("min_volume_h24", 200_000))
        min_h1_change = float(self.config.get("min_h1_change_pct", 3.0))
        min_buy_ratio = float(self.config.get("min_buy_ratio", 0.55))
        min_age = float(self.config.get("min_age_hours", 2))
        max_age = float(self.config.get("max_age_hours", 168))

        async with DexScreenerClient(cache_ttl=15) as client:
            candidates = await client.get_solana_meme_candidates(
                min_liquidity_usd=min_liquidity,
                min_volume_h24=min_volume,
                min_age_hours=min_age,
                max_age_hours=max_age,
                limit=30,
            )

        if not candidates:
            logger.info("No meme coin candidates found passing filters")
            return Intent.hold(reason="No candidates pass DexScreener filters")

        # Apply additional momentum filters
        filtered = []
        for pair in candidates:
            if pair.price_change.h1 < min_h1_change:
                continue
            if pair.txns.h1.buy_ratio < min_buy_ratio:
                continue
            filtered.append(pair)

        if not filtered:
            logger.info(
                "Found %d candidates but none pass momentum filters (h1_change>=%.1f%%, buy_ratio>=%.2f)",
                len(candidates),
                min_h1_change,
                min_buy_ratio,
            )
            return Intent.hold(reason="No candidates with sufficient momentum")

        # Score and pick the best
        scored = [(pair, _score_meme_candidate(pair)) for pair in filtered]
        scored.sort(key=lambda x: x[1], reverse=True)
        best_pair, best_score = scored[0]

        target_symbol = best_pair.base_token.symbol
        target_address = best_pair.base_token.address
        logger.info(
            "Top meme candidate: %s (%s) score=%.1f, h1_change=%.1f%%, vol_h24=$%.0f, liq=$%.0f, buy_ratio=%.2f",
            target_symbol,
            target_address[:12] + "...",
            best_score,
            best_pair.price_change.h1,
            best_pair.volume.h24,
            best_pair.liquidity.usd,
            best_pair.txns.h1.buy_ratio,
        )

        # Record position entry for tracking
        self._position = {
            "token": target_symbol,
            "token_address": target_address,
            "entry_price_usd": best_pair.price_usd,
            "entry_time": datetime.now(UTC).isoformat(),
            "pair_address": best_pair.pair_address,
            "dex_id": best_pair.dex_id,
        }

        # Use mint address for meme coins (not in static token registry)
        return Intent.swap(
            from_token=base_token,
            to_token=target_address,
            amount=swap_amount,
            max_slippage=max_slippage,
        )

    async def _manage_position(
        self,
        market: MarketSnapshot,
        position: dict,
        base_token: str,
        max_slippage: Decimal,
    ) -> Intent:
        """Manage an existing meme coin position (take-profit / stop-loss)."""
        token = position["token"]
        token_address = position.get("token_address", "")
        entry_price = float(position.get("entry_price_usd", "0"))
        take_profit = float(self.config.get("take_profit_pct", 15.0))
        stop_loss = float(self.config.get("stop_loss_pct", -10.0))

        # Use mint address for swaps (meme coins not in static registry)
        swap_token = token_address or token

        if not entry_price:
            logger.warning("No entry price recorded, closing position")
            self._position = None
            return Intent.swap(from_token=swap_token, to_token=base_token, amount="all", max_slippage=max_slippage)

        # Fetch current price from DexScreener
        current_price = 0.0
        async with DexScreenerClient(cache_ttl=10) as client:
            if token_address:
                pairs = await client.get_token_pairs("solana", token_address)
            else:
                pairs = await client.search_pairs(token)
                pairs = [p for p in pairs if p.chain_id == "solana"]

            if pairs:
                best = max(pairs, key=lambda p: p.liquidity.usd)
                current_price = best.price_usd_float

        if current_price <= 0:
            logger.warning("Cannot fetch current price for %s, holding", token)
            return Intent.hold(reason=f"Cannot fetch price for {token}")

        pnl_pct = ((current_price - entry_price) / entry_price) * 100
        logger.info(
            "Position %s: entry=$%.6f, current=$%.6f, PnL=%.1f%%",
            token,
            entry_price,
            current_price,
            pnl_pct,
        )

        if pnl_pct >= take_profit:
            logger.info("TAKE PROFIT triggered at %.1f%% (target: %.1f%%)", pnl_pct, take_profit)
            self._position = None
            return Intent.swap(from_token=swap_token, to_token=base_token, amount="all", max_slippage=max_slippage)

        if pnl_pct <= stop_loss:
            logger.info("STOP LOSS triggered at %.1f%% (limit: %.1f%%)", pnl_pct, stop_loss)
            self._position = None
            return Intent.swap(from_token=swap_token, to_token=base_token, amount="all", max_slippage=max_slippage)

        return Intent.hold(reason=f"Position {token} PnL={pnl_pct:.1f}%, within bounds")

    # -- State persistence --

    def get_persistent_state(self) -> dict[str, Any]:
        return {"position": self._position}

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        self._position = state.get("position")

    # -- Teardown --

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self):
        from almanak.framework.teardown import PositionInfo, PositionType

        positions = []
        if self._position:
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id=f"meme_{self._position.get('token', 'unknown')}",
                    chain="solana",
                    protocol="jupiter",
                    value_usd=Decimal("0"),
                    details={
                        "token": self._position.get("token"),
                        "token_address": self._position.get("token_address"),
                        "entry_price_usd": self._position.get("entry_price_usd"),
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

        if not self._position:
            return []
        base_token = self.config.get("base_token", "USDC")
        # Use mint address for meme coins (not in static token registry)
        token_address = self._position.get("token_address", self._position.get("token", ""))
        # Higher slippage for meme coins due to thin liquidity
        max_slippage = Decimal("0.05") if mode == TeardownMode.HARD else Decimal("0.03")
        return [
            Intent.swap(
                from_token=token_address,
                to_token=base_token,
                amount="all",
                max_slippage=max_slippage,
            )
        ]
