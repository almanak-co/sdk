"""Solana Narrative Momentum Strategy — Meteora/Raydium LP on trending pools.

Screens DexScreener for high-volume Solana pools on Meteora DLMM or Raydium CLMM,
scores candidates by volume/TVL ratio and buy pressure, then opens a concentrated
LP position on the best pool. Exits when volume drops significantly, indicating
the momentum narrative has faded.

Strategy logic:
    1. Screen DexScreener for Solana pairs on Raydium/Meteora with high Vol/TVL
    2. Score candidates by: vol/tvl ratio, buy pressure, price momentum, liquidity
    3. Open LP position on the top-scoring pool (Meteora DLMM or Raydium CLMM)
    4. On subsequent runs: monitor volume — exit when 24h volume drops >50%

Usage:
    # Dry run:
    almanak strat run -d strategies/demo/solana_narrative_momentum --once --dry-run

    # Real execution:
    almanak strat run -d strategies/demo/solana_narrative_momentum --once

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

from almanak.framework.data.dexscreener import DexPair, DexScreenerClient
from almanak.framework.intents import Intent
from almanak.framework.intents.vocabulary import LPOpenIntent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.teardown import TeardownPositionSummary

logger = logging.getLogger(__name__)

# DexScreener dex_id values for supported LP protocols
SUPPORTED_DEX_IDS = {
    "meteora": {"meteora"},
    "raydium": {"raydium"},
}
DEX_TO_PROTOCOL = {
    "meteora": "meteora_dlmm",
    "raydium": "raydium_clmm",
}


def _score_pool_candidate(pair: DexPair, min_vol_tvl: float) -> float:
    """Score a pool candidate for LP momentum potential.

    Factors:
        - Vol/TVL ratio (primary signal — high ratio means fees are attractive)
        - Buy pressure (bullish bias reduces IL risk)
        - Price momentum (trending = more volume = more fees)
        - Liquidity depth (higher = safer, lower slippage on entry/exit)
    """
    score = 0.0

    # Vol/TVL ratio: 0-35 points (the core signal)
    if pair.liquidity.usd > 0 and min_vol_tvl > 0:
        vol_tvl = pair.volume.h24 / pair.liquidity.usd
        if vol_tvl >= min_vol_tvl:
            # Normalize: ratio of 10 = 20pts, 20 = 30pts, 30+ = 35pts
            score += min(35, 15 + (vol_tvl / min_vol_tvl) * 10)

    # Buy pressure: 0-25 points
    buy_ratio = pair.txns.h1.buy_ratio
    buy_pts = max(0, (buy_ratio - 0.5)) * 50  # 0.6 = 5pts, 0.7 = 10pts, 0.8 = 15pts
    score += min(25, buy_pts)

    # Price momentum (h1): 0-20 points
    h1_change = pair.price_change.h1
    if h1_change > 0:
        score += min(20, h1_change * 2)

    # Liquidity depth bonus: 0-20 points
    if pair.liquidity.usd >= 1_000_000:
        score += 20
    elif pair.liquidity.usd >= 500_000:
        score += 15
    elif pair.liquidity.usd >= 200_000:
        score += 10
    elif pair.liquidity.usd >= 100_000:
        score += 5

    return score


@almanak_strategy(
    name="solana_narrative_momentum",
    version="0.1.0",
    description="LP on trending Solana pools via Meteora/Raydium (narrative momentum)",
    supported_chains=["solana"],
    default_chain="solana",
    supported_protocols=["meteora_dlmm", "raydium_clmm", "jupiter"],
    intent_types=["LP_OPEN", "LP_CLOSE", "SWAP"],
)
class SolanaNarrativeMomentumStrategy(IntentStrategy):
    """Screen for trending Solana pools and provide LP on the best one.

    Combines DexScreener pool discovery with Meteora DLMM or Raydium CLMM
    LP positions. Targets pools with high volume/TVL ratio (fee revenue)
    and strong buy pressure (reduced IL risk).
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._position: dict | None = None
        self._entry_volume_h24: float = 0.0

    def decide(self, market: MarketSnapshot) -> Intent:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, self._decide_async(market))
            return future.result(timeout=30)

    async def _decide_async(self, market: MarketSnapshot) -> Intent:
        """Async decision logic with DexScreener screening."""
        if self._position:
            return await self._manage_position()
        return await self._screen_and_lp()

    async def _screen_and_lp(self) -> Intent:
        """Screen DexScreener for the best LP candidate pool."""
        min_liquidity = float(self.config.get("min_liquidity_usd", 100_000))
        min_volume = float(self.config.get("min_volume_h24", 1_000_000))
        min_vol_tvl = float(self.config.get("min_vol_tvl_ratio", 10.0))
        min_buy_ratio = float(self.config.get("min_buy_ratio", 0.52))
        min_h1_change = float(self.config.get("min_h1_change_pct", 1.0))
        min_age = float(self.config.get("min_age_hours", 2))
        max_age = float(self.config.get("max_age_hours", 168))
        preferred_dex = self.config.get("preferred_dex", "meteora")

        # Collect all supported dex_ids
        all_dex_ids: set[str] = set()
        for dex_set in SUPPORTED_DEX_IDS.values():
            all_dex_ids.update(dex_set)

        async with DexScreenerClient(cache_ttl=15) as client:
            raw_candidates = await client.get_solana_meme_candidates(
                min_liquidity_usd=min_liquidity,
                min_volume_h24=min_volume,
                min_age_hours=min_age,
                max_age_hours=max_age,
                limit=50,
            )

        if not raw_candidates:
            logger.info("No pool candidates found on DexScreener")
            return Intent.hold(reason="No Solana pool candidates pass DexScreener filters")

        # Filter to Raydium/Meteora pools with momentum
        filtered = []
        for pair in raw_candidates:
            if pair.dex_id not in all_dex_ids:
                continue
            if pair.liquidity.usd <= 0:
                continue
            vol_tvl = pair.volume.h24 / pair.liquidity.usd
            if vol_tvl < min_vol_tvl:
                continue
            if pair.price_change.h1 < min_h1_change:
                continue
            if pair.txns.h1.buy_ratio < min_buy_ratio:
                continue
            filtered.append(pair)

        if not filtered:
            logger.info(
                "Found %d raw candidates but none pass momentum filters "
                "(vol/tvl>=%.1f, h1_change>=%.1f%%, buy_ratio>=%.2f, dex in %s)",
                len(raw_candidates),
                min_vol_tvl,
                min_h1_change,
                min_buy_ratio,
                all_dex_ids,
            )
            return Intent.hold(reason="No pools with sufficient momentum on Raydium/Meteora")

        # Score and rank
        scored = [(pair, _score_pool_candidate(pair, min_vol_tvl)) for pair in filtered]
        scored.sort(key=lambda x: x[1], reverse=True)

        # Prefer the configured DEX if available, otherwise take best overall
        best_pair, best_score = scored[0]
        for pair, sc in scored:
            if pair.dex_id in SUPPORTED_DEX_IDS.get(preferred_dex, set()):
                best_pair, best_score = pair, sc
                break

        # Determine protocol from dex_id
        protocol = DEX_TO_PROTOCOL.get(best_pair.dex_id)
        if not protocol:
            logger.warning("Unsupported dex_id %s for LP, falling back to swap", best_pair.dex_id)
            return self._spot_buy_fallback(best_pair)

        vol_tvl = best_pair.volume.h24 / best_pair.liquidity.usd if best_pair.liquidity.usd > 0 else 0
        logger.info(
            "Top LP candidate: %s/%s on %s (score=%.1f, vol/tvl=%.1f, h1=+%.1f%%, liq=$%.0f, buy=%.2f)",
            best_pair.base_token.symbol,
            best_pair.quote_token.symbol,
            best_pair.dex_id,
            best_score,
            vol_tvl,
            best_pair.price_change.h1,
            best_pair.liquidity.usd,
            best_pair.txns.h1.buy_ratio,
        )

        # Build LP position
        return self._build_lp_intent(best_pair, protocol)

    def _build_lp_intent(self, pair: DexPair, protocol: str) -> Intent:
        """Build an LPOpenIntent for the selected pool."""
        lp_amount_base = Decimal(str(self.config.get("lp_amount_base", "0.5")))
        lp_amount_quote = Decimal(str(self.config.get("lp_amount_quote", "0.5")))
        range_width_pct = float(self.config.get("range_width_pct", 30.0))

        # Calculate price range using pool price (token A per token B), not USD price.
        # DexScreener's price_native is the pool price in quote token units, which is
        # what Raydium/Meteora interpret range_lower/range_upper as.
        try:
            current_price = float(pair.price_native)
        except (ValueError, TypeError):
            current_price = 0.0
        if current_price <= 0:
            logger.warning("Invalid price for %s, holding", pair.base_token.symbol)
            return Intent.hold(reason=f"Invalid price for {pair.base_token.symbol}")

        half_width = Decimal(str(range_width_pct / 100 / 2))
        price_dec = Decimal(str(current_price))
        range_lower = price_dec * (1 - half_width)
        range_upper = price_dec * (1 + half_width)

        # Ensure range_lower > 0
        if range_lower <= 0:
            range_lower = Decimal("0.000001")

        # Record entry state for position management
        self._position = {
            "pair_address": pair.pair_address,
            "dex_id": pair.dex_id,
            "protocol": protocol,
            "base_token": pair.base_token.symbol,
            "base_token_address": pair.base_token.address,
            "quote_token": pair.quote_token.symbol,
            "quote_token_address": pair.quote_token.address,
            "entry_price_usd": current_price,
            "entry_volume_h24": pair.volume.h24,
            "entry_time": datetime.now(UTC).isoformat(),
            "pool": pair.pair_address,
        }
        self._entry_volume_h24 = pair.volume.h24

        logger.info(
            "Opening %s LP: %s %s + %s %s, range [%.6f, %.6f]",
            protocol,
            lp_amount_base,
            pair.base_token.symbol,
            lp_amount_quote,
            pair.quote_token.symbol,
            range_lower,
            range_upper,
        )

        return LPOpenIntent(
            protocol=protocol,
            pool=pair.pair_address,
            amount0=lp_amount_base,
            amount1=lp_amount_quote,
            range_lower=range_lower,
            range_upper=range_upper,
        )

    def _spot_buy_fallback(self, pair: DexPair) -> Intent:
        """Fallback: spot buy the base token if LP protocol is unsupported."""
        base_token = self.config.get("base_token", "USDC")
        amount = Decimal(str(self.config.get("lp_amount_quote", "0.5")))
        max_slippage = Decimal(str(self.config.get("max_slippage_pct", 3.0))) / 100

        self._position = {
            "pair_address": pair.pair_address,
            "dex_id": pair.dex_id,
            "protocol": "jupiter",
            "base_token": pair.base_token.symbol,
            "base_token_address": pair.base_token.address,
            "quote_token": pair.quote_token.symbol,
            "quote_token_address": pair.quote_token.address,
            "entry_price_usd": pair.price_usd_float,
            "entry_volume_h24": pair.volume.h24,
            "entry_time": datetime.now(UTC).isoformat(),
            "position_type": "spot",
        }
        self._entry_volume_h24 = pair.volume.h24

        return Intent.swap(
            from_token=base_token,
            to_token=pair.base_token.address,
            amount=amount,
            max_slippage=max_slippage,
        )

    async def _manage_position(self) -> Intent:
        """Monitor volume and exit when momentum fades."""
        pair_address = self._position.get("pair_address", "")
        protocol = self._position.get("protocol", "")
        entry_vol = self._position.get("entry_volume_h24", 0)
        volume_drop_pct = float(self.config.get("volume_drop_exit_pct", 50.0))

        if not pair_address:
            logger.warning("No pair address in position state, holding")
            return Intent.hold(reason="Missing pair address")

        # Fetch current pair data from DexScreener
        current_vol = 0.0
        async with DexScreenerClient(cache_ttl=15) as client:
            pair = await client.get_pair("solana", pair_address)
            if pair:
                current_vol = pair.volume.h24

        if current_vol <= 0:
            logger.warning("Cannot fetch volume for pair %s, holding", pair_address)
            return Intent.hold(reason=f"Cannot fetch volume for {pair_address}")

        # Check volume drop
        if entry_vol > 0:
            vol_change_pct = ((current_vol - entry_vol) / entry_vol) * 100
        else:
            vol_change_pct = 0.0

        logger.info(
            "Position %s/%s: entry_vol=$%.0f, current_vol=$%.0f, change=%.1f%%",
            self._position.get("base_token", "?"),
            self._position.get("quote_token", "?"),
            entry_vol,
            current_vol,
            vol_change_pct,
        )

        if vol_change_pct <= -volume_drop_pct:
            logger.info(
                "VOLUME DROP EXIT: volume fell %.1f%% (threshold: -%.1f%%)",
                vol_change_pct,
                volume_drop_pct,
            )
            return self._build_exit_intent()

        return Intent.hold(
            reason=f"Volume change {vol_change_pct:+.1f}%, within bounds (exit at -{volume_drop_pct:.0f}%)"
        )

    def _build_exit_intent(self) -> Intent:
        """Build exit intent based on position type (LP or spot)."""
        protocol = self._position.get("protocol", "")
        position_type = self._position.get("position_type", "lp")
        base_token = self.config.get("base_token", "USDC")
        max_slippage = Decimal(str(self.config.get("max_slippage_pct", 3.0))) / 100

        if position_type == "spot":
            # Close spot position via swap back.
            # Keep _position until on_intent_executed confirms success.
            token_address = self._position.get("base_token_address", "")
            return Intent.swap(
                from_token=token_address,
                to_token=base_token,
                amount="all",
                max_slippage=max_slippage,
            )

        # Close LP position
        position_id = self.state.get("position_id", "")
        pool = self._position.get("pool", "")
        if not position_id:
            logger.warning("No position_id in state for LP close, swapping base token instead")
            token_address = self._position.get("base_token_address", "")
            return Intent.swap(
                from_token=token_address,
                to_token=base_token,
                amount="all",
                max_slippage=max_slippage,
            )

        return Intent.lp_close(
            protocol=protocol,
            position_id=position_id,
            pool=pool,
            collect_fees=True,
        )

    def on_intent_executed(self, intent, success: bool, result):
        """Track LP position on open, clear position on successful close."""
        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return
        type_value = intent_type.value if hasattr(intent_type, "value") else str(intent_type)
        if not success:
            if type_value == "LP_OPEN":
                self._position = None
                self._entry_volume_h24 = 0.0
                logger.warning("LP_OPEN failed, clearing optimistic position state")
            return
        if type_value == "LP_OPEN":
            position_id = result.position_id if result else None
            if position_id:
                self.state["position_id"] = str(position_id)
                self.state["pool"] = getattr(intent, "pool", "")
                self.state["protocol"] = getattr(intent, "protocol", "")
                logger.info("Tracked LP position: %s", position_id)
        elif type_value in ("LP_CLOSE", "SWAP") and self._position:
            # Position successfully closed — clear state
            logger.info("Position closed successfully, clearing state")
            self._position = None
            self._entry_volume_h24 = 0.0

    # -- State persistence --

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "position": self._position,
            "entry_volume_h24": self._entry_volume_h24,
            "position_id": self.state.get("position_id"),
            "pool": self.state.get("pool"),
            "protocol": self.state.get("protocol"),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        self._position = state.get("position")
        self._entry_volume_h24 = state.get("entry_volume_h24", 0.0)
        # Restore LP tracking fields into self.state so teardown/exit can find them
        if state.get("position_id"):
            self.state["position_id"] = state["position_id"]
        if state.get("pool"):
            self.state["pool"] = state["pool"]
        if state.get("protocol"):
            self.state["protocol"] = state["protocol"]

    # -- Teardown --

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self):
        from almanak.framework.teardown import PositionInfo, PositionType

        positions = []
        if self._position:
            pos_type = self._position.get("position_type", "lp")
            protocol = self._position.get("protocol", "unknown")
            if pos_type == "spot":
                positions.append(
                    PositionInfo(
                        position_type=PositionType.TOKEN,
                        position_id=f"spot_{self._position.get('base_token', 'unknown')}",
                        chain="solana",
                        protocol=protocol,
                        value_usd=Decimal("0"),
                        details={
                            "token": self._position.get("base_token"),
                            "token_address": self._position.get("base_token_address"),
                        },
                    )
                )
            else:
                position_id = self.state.get("position_id", "pending")
                positions.append(
                    PositionInfo(
                        position_type=PositionType.LP,
                        position_id=position_id,
                        chain="solana",
                        protocol=protocol,
                        value_usd=Decimal("0"),
                        details={
                            "pool": self._position.get("pool"),
                            "base_token": self._position.get("base_token"),
                            "quote_token": self._position.get("quote_token"),
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

        pos_type = self._position.get("position_type", "lp")
        protocol = self._position.get("protocol", "")
        base_token = self.config.get("base_token", "USDC")
        max_slippage = Decimal("0.05") if mode == TeardownMode.HARD else Decimal("0.03")

        intents = []

        if pos_type == "spot":
            token_address = self._position.get("base_token_address", "")
            intents.append(
                Intent.swap(
                    from_token=token_address,
                    to_token=base_token,
                    amount="all",
                    max_slippage=max_slippage,
                )
            )
        else:
            position_id = self.state.get("position_id", "")
            pool = self._position.get("pool", "")
            if position_id:
                intents.append(
                    Intent.lp_close(
                        protocol=protocol,
                        position_id=position_id,
                        pool=pool,
                        collect_fees=True,
                    )
                )
            else:
                logger.warning(
                    "Cannot generate LP teardown: position_id missing from state. "
                    "Manual intervention may be required for pool %s",
                    pool,
                )

        return intents
