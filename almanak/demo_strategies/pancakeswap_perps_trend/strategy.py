"""PancakeSwap Perps trend-following demo strategy (VIB-2875).

Trades BTC/USD perpetuals on PancakeSwap Perps (ApolloX on BSC, broker id 2).

Strategy logic:
    - Track a simple momentum signal: change in BTC price over the last tick.
    - If momentum > +threshold: go LONG (or hold existing long).
    - If momentum < -threshold: close the LONG position.
    - No SL/TP, market orders only (v1 scope limit — see design doc).

IMPORTANT — two-phase execution on ApolloX:
    Every open and close on this venue is a two-phase flow:
      1. User-signed call emits a MarketPendingTrade event (with tradeHash).
      2. An off-chain keeper (PRICE_FEEDER_ROLE holder) subsequently settles the
         pending trade at the oracle-quoted price, emitting OpenMarketTrade /
         CloseTradeSuccessful.
    Strategies MUST persist the tradeHash returned by step 1 so they can
    (a) poll step 2 status on subsequent ticks, and (b) pass it to
    build_close_transaction when closing.

PerpCloseIntent carries an optional ``position_id`` (bytes32 tradeHash) field
that ApolloX-style venues (including PancakeSwap Perps) require to close a
position. This demo emits PERP_OPEN and PERP_CLOSE through the intent
pipeline end-to-end (compile/execute/parser/enricher).

Usage:
    almanak strat run -d almanak/demo_strategies/pancakeswap_perps_trend --once
    almanak strat run -d almanak/demo_strategies/pancakeswap_perps_trend --network anvil --once
"""

from __future__ import annotations

import logging
from decimal import Decimal

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="pancakeswap_perps_trend",
    version="0.1.0",
    description="BTC/USD trend-following on PancakeSwap Perps (ApolloX / BSC, broker=2)",
    supported_chains=["bsc"],
    default_chain="bsc",
    supported_protocols=["pancakeswap_perps"],
    intent_types=["PERP_OPEN", "PERP_CLOSE", "HOLD"],
    quote_asset="USD",
)
class PancakeSwapPerpsTrendStrategy(IntentStrategy):
    """Simple momentum-driven BTC/USD trend follower on PancakeSwap Perps."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        # State tracks: last_price (for momentum calc), trade_hash (open position ID),
        # and a pending-settlement flag so we don't double-open while the keeper hasn't
        # filled yet. The framework persists this dict across ticks via get/load hooks.
        self.state: dict = {}

    # -----------------------------------------------------------------
    # State hooks
    # -----------------------------------------------------------------

    def get_persistent_state(self) -> dict:
        return dict(self.state)

    def load_persistent_state(self, state: dict) -> None:
        self.state = dict(state or {})

    # -----------------------------------------------------------------
    # Token tracking — override the default config-derived tracker.
    # Without this, the framework auto-detects 'market: BTC/USD' and tries to
    # pre-warm prices for 'BTC' and 'USD', neither of which is a registered
    # BSC token (BSC has BTCB — Binance-Peg BTC at 0x7130d2A1…; "BTC" and
    # "WBTC" still resolve via alias to the same record). The pre-warm then
    # times out per token at 30s, blocking decide() for >60s on every tick.
    # We track the actual on-chain margin + price-lookup symbols.
    # -----------------------------------------------------------------

    def _get_tracked_tokens(self) -> list[str]:
        margin = self.config.get("collateral_token", "BNB")
        # Native BNB price comes from BNB symbol; for ERC20 margin track that.
        margin_lookup = "BNB" if margin.upper() in ("BNB", "NATIVE") else margin
        price_symbol = self.config.get("price_symbol", "BTCB")
        # Deduplicate while preserving order
        seen = set()
        out = []
        for t in (price_symbol, margin_lookup):
            if t and t not in seen:
                seen.add(t)
                out.append(t)
        return out

    # -----------------------------------------------------------------
    # Decide
    # -----------------------------------------------------------------

    def decide(self, market: MarketSnapshot) -> Intent:
        # --- Config ---
        perp_market = self.config.get("market", "BTC/USD")
        collateral_token = self.config.get("collateral_token", "BNB")
        collateral_amount = Decimal(str(self.config.get("collateral_amount", "0.3")))
        size_usd = Decimal(str(self.config.get("size_usd", "500")))
        leverage = Decimal(str(self.config.get("leverage", "1.5")))
        max_slippage = Decimal(str(self.config.get("max_slippage", "0.01")))
        threshold_bps = int(self.config.get("momentum_threshold_bps", 50))
        # Symbol used for the gateway price lookup — must be a token registered
        # in the gateway's token registry for this chain. PCS Perps trades
        # BTC/USD as a synthetic perp; on BSC the registered base is BTCB
        # (Binance-Peg BTC at 0x7130d2A1…, 18 decimals). "BTC" and "WBTC"
        # remain accepted via aliases for legacy callers. Strategies can
        # override via the price_symbol config field.
        price_symbol = self.config.get("price_symbol", "BTCB")

        # --- Current price ---
        try:
            current_price = market.price(price_symbol)
        except Exception as e:
            logger.warning(f"Price for '{price_symbol}' unavailable: {e}")
            return Intent.hold(reason=f"No mark price for {price_symbol}")

        # --- Momentum signal ---
        last_price = self.state.get("last_price")
        self.state["last_price"] = str(current_price)

        # Optional E2E / paper-trading helper: force an open on first tick by
        # synthesizing a "previous price" 2x the threshold below current.
        # Not for production — exists so `almanak strat run --once` against a
        # fresh fork can exercise the open path without waiting two ticks.
        if last_price is None and self.config.get("force_open_on_first_tick", False):
            synth_prev = current_price * (Decimal(10_000 - 2 * threshold_bps) / Decimal(10_000))
            last_price = str(synth_prev)
            logger.info(
                f"force_open_on_first_tick=true — synthesizing prev_price={synth_prev} "
                f"(2x threshold below current {current_price}) to trigger an open"
            )

        if last_price is None:
            return Intent.hold(reason="First tick — seeding last_price, no signal yet")

        try:
            last_price_dec = Decimal(str(last_price))
            if last_price_dec <= 0:
                return Intent.hold(reason="Invalid cached last_price")
            momentum_bps = int((current_price - last_price_dec) * Decimal(10_000) / last_price_dec)
        except Exception as e:
            logger.warning(f"Momentum calc failed: {e}")
            return Intent.hold(reason="Momentum calc error")

        logger.info(
            f"[PCS Perps Trend] {perp_market} mark={current_price} "
            f"prev={last_price_dec} momentum={momentum_bps}bps threshold=±{threshold_bps}bps"
        )

        # --- Position state ---
        open_trade_hash = self.state.get("open_trade_hash")

        # --- Bullish signal -> open LONG if flat ---
        if (
            momentum_bps >= threshold_bps
            and not open_trade_hash
            and not self.state.get("pending_open", False)
        ):
            logger.info(f"Bullish momentum +{momentum_bps}bps >= +{threshold_bps} — opening LONG")
            # Mark that we expect a tradeHash to be persisted on next tick via on_intent_executed.
            self.state["pending_open"] = True
            return Intent.perp_open(
                market=perp_market,
                collateral_token=collateral_token,
                collateral_amount=collateral_amount,
                size_usd=size_usd,
                is_long=True,
                leverage=leverage,
                max_slippage=max_slippage,
                protocol="pancakeswap_perps",
            )

        # --- Bearish signal -> close the open LONG via intent pipeline ---
        if momentum_bps <= -threshold_bps and open_trade_hash and self.config.get("close_on_reversal", True):
            logger.info(
                f"Bearish momentum {momentum_bps}bps <= -{threshold_bps} — CLOSE signal. "
                f"Closing tradeHash={open_trade_hash[:18]}... via PerpCloseIntent"
            )
            # size_usd omitted — ApolloX closeTrade(bytes32) always closes 100% of the
            # position identified by position_id (the compiler rejects partial closes).
            return Intent.perp_close(
                market=perp_market,
                collateral_token=collateral_token,
                is_long=True,
                max_slippage=max_slippage,
                protocol="pancakeswap_perps",
                position_id=open_trade_hash,
            )

        # --- Default: no signal ---
        return Intent.hold(
            reason=f"momentum {momentum_bps}bps within threshold ±{threshold_bps}bps"
        )

    # -----------------------------------------------------------------
    # Post-execution enrichment: persist tradeHash so we can close later
    # -----------------------------------------------------------------

    def on_intent_executed(self, intent, success: bool, result) -> None:
        """Callback after each intent execution.

        On a successful PERP_OPEN, ResultEnricher populates result.position_id
        with the tradeHash extracted from MarketPendingTrade.topics[2]. We
        stash it in state so subsequent ticks know a position is pending /
        open on-chain.
        """
        if not success:
            # Clear pending flag so the next tick can retry
            self.state["pending_open"] = False
            return

        # On successful PERP_CLOSE, clear the open_trade_hash so the next
        # bullish signal can open a fresh position.
        intent_type = getattr(intent, "intent_type", None)
        if intent_type and str(intent_type).endswith("PERP_CLOSE"):
            logger.info(f"PERP_CLOSE succeeded — clearing open_trade_hash={self.state.get('open_trade_hash')}")
            self.state.pop("open_trade_hash", None)
            self.state["pending_open"] = False
            return

        # ResultEnricher.position_id only accepts int (NFT IDs) or 40-char hex
        # addresses; a bytes32 tradeHash (64 hex chars) is rejected by the
        # validator and falls through to result.extracted_data["position_id"].
        # Fall back to extracted_data so we capture the hash either way.
        position_id = getattr(result, "position_id", None)
        if position_id is None:
            extracted = getattr(result, "extracted_data", {}) or {}
            position_id = extracted.get("position_id")
        if position_id:
            self.state["open_trade_hash"] = str(position_id)
            self.state["pending_open"] = False
            logger.info(f"Persisted tradeHash={position_id} to state")

    # -----------------------------------------------------------------
    # Teardown — required by StrategyBase. For v1, reports positions from
    # local state; a future iteration can query TradingReaderFacet.getPositionsV2.
    # -----------------------------------------------------------------

    def get_open_positions(self):
        from datetime import UTC, datetime

        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self.state.get("open_trade_hash"):
            positions.append(
                PositionInfo(
                    position_type=PositionType.PERP,
                    position_id=self.state["open_trade_hash"],
                    chain="bsc",
                    protocol="pancakeswap_perps",
                    value_usd=Decimal(str(self.config.get("size_usd", "500"))),
                    details={
                        "market": self.config.get("market", "BTC/USD"),
                        "is_long": True,
                    },
                )
            )
        return TeardownPositionSummary(
            deployment_id=self.deployment_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None):
        """Emit ordered intents to close the open PCS Perps position.

        With PerpCloseIntent.position_id (added alongside the PCS Perps connector),
        we can emit the close through the intent pipeline directly. If no position
        is tracked in state, returns an empty list (nothing to tear down).
        """
        from almanak.framework.teardown import TeardownMode

        open_trade_hash = self.state.get("open_trade_hash")
        if not open_trade_hash:
            return []

        # HARD mode: more permissive slippage tolerance to increase settle odds
        # under stressed market conditions. SOFT mode: normal tolerance.
        slippage = (
            Decimal(str(self.config.get("teardown_hard_slippage", "0.03")))
            if mode == TeardownMode.HARD
            else Decimal(str(self.config.get("max_slippage", "0.01")))
        )

        return [
            # size_usd omitted — PCS Perps always fully closes the position referenced by
            # position_id (partial closes are rejected by the compiler).
            Intent.perp_close(
                market=self.config.get("market", "BTC/USD"),
                collateral_token=self.config.get("collateral_token", "BNB"),
                is_long=True,
                max_slippage=slippage,
                protocol="pancakeswap_perps",
                position_id=open_trade_hash,
            )
        ]
