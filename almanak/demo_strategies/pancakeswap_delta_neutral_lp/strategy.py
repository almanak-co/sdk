"""Delta-Neutral LP — PancakeSwap V3 LP + PancakeSwap Perps short hedge (BSC).

Single-venue family delta-neutral strategy on BNB Smart Chain:
  - LP leg:   PancakeSwap V3 concentrated liquidity in WBNB/USDT (or any
              token0=volatile pool the user configures).
  - Perp leg: PancakeSwap Perps (ApolloX) short on the LP's volatile side
              (default BNB/USD), sized to the LP's token0 USD notional.

As the pool price moves, the LP's token0 (WBNB) balance shifts. The
strategy rebalances the perp short to match the LP's current token0
notional whenever the price drifts beyond a configurable threshold.

Phase state machine (one intent per decide() tick)
--------------------------------------------------
    IDLE           -> LP_OPEN       -> LP_OPENED
    LP_OPENED      -> PERP_OPEN     -> HEDGED
    HEDGED         -> monitor       -> (drift > threshold) PERP_CLOSE (in HEDGED)
    PERP_CLOSE ok  -> REBALANCING_LP (via on_intent_executed)
    REBALANCING_LP -> LP_CLOSE      -> IDLE

Terminal fail-closed phases (operator intervention required, decide() holds):
    LP_OPEN_FAILED      — LP mint TX ok but no position_id captured (duplicate-mint risk)
    PERP_OPEN_FAILED    — Perp open TX ok but no bytes32 tradeHash captured (unclosable)
    RECOVERY_REQUIRED   — strategy would close LP while hedge cannot be identified
    UNHEDGING           — legacy transient phase; retries PERP_CLOSE, or
                          falls through to RECOVERY_REQUIRED if tradeHash is missing

This is the PancakeSwap-only counterpart to the Arbitrum
``delta_neutral_lp`` (Uniswap V3 + GMX V2) demo. It depends on the
PerpCloseIntent.position_id vocabulary extension so closes can flow
through the IntentCompiler instead of dropping out to direct-SDK calls.

v1 limitations
--------------
- Single chain (BSC), single venue family (PancakeSwap).
- Delta estimation uses a static config-based ``amount0`` approximation:
  the LP's current token0 balance is approximated by the configured open
  amount. Live on-chain Uniswap-V3-style math
  (NonfungiblePositionManager.positions + pool slot0) is a follow-up
  ticket. Within a moderate in-range move the approximation is on the
  same order as the rebalance threshold.
- ApolloX uses two-phase keeper-filled orders. The strategy persists the
  ``tradeHash`` from the open receipt and treats a successful submission
  as "position open" (the keeper settles within a few seconds on
  mainnet). Richer status polling is a follow-up.
- ``closeTrade(bytes32)`` is full-position only — the strategy closes and
  re-opens (rather than resizes) on each rebalance.

Usage
-----
    almanak strat run -d almanak/demo_strategies/pancakeswap_delta_neutral_lp --once
    almanak strat run -d almanak/demo_strategies/pancakeswap_delta_neutral_lp --network anvil --once
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

logger = logging.getLogger(__name__)

# --- Phases ---
IDLE = "IDLE"
LP_OPENED = "LP_OPENED"
HEDGED = "HEDGED"
# UNHEDGING: legacy transient phase. New code transitions HEDGED -> REBALANCING_LP
# directly in `on_intent_executed(PERP_CLOSE, success=True)` so a failed close can
# never deadlock the strategy. Kept for state backward-compatibility + recovery.
UNHEDGING = "UNHEDGING"
REBALANCING_LP = "REBALANCING_LP"

# --- Terminal fail-closed phases (operator intervention required) ---
# These phases are entered when the strategy detects a state invariant violation
# that cannot be safely auto-resolved. In all of them:
#   * decide() returns Intent.hold() with a clear operator-facing message
#   * get_open_positions() flags every known position with details["state"]="unknown"
#   * generate_teardown_intents() returns [] and logs a warning — automated teardown
#     could close the wrong position or leave an unknown position stranded.
# The operator is expected to query on-chain state (NonfungiblePositionManager,
# TradingReaderFacet.getPositionsV2) to reconcile, then manually clear / rewrite
# self.state before re-enabling the strategy.
PERP_OPEN_FAILED = "PERP_OPEN_FAILED"
"""PERP_OPEN succeeded on-chain but no bytes32 tradeHash was extracted. A live
short without a tradeHash cannot be closed via the PCS Perps compiler."""
LP_OPEN_FAILED = "LP_OPEN_FAILED"
"""LP_OPEN succeeded on-chain but no position_id was captured. Retrying would
risk minting a duplicate LP NFT; we stop and require operator verification."""
RECOVERY_REQUIRED = "RECOVERY_REQUIRED"
"""Strategy reached a path that would close the LP while the hedge can no longer
be identified (missing perp_trade_hash on a phase >= HEDGED). Closing the LP
in this state would leave the strategy with unhedged directional exposure via
an unknown-to-us perp position. Operator must locate and close the short first."""

_TERMINAL_PHASES = frozenset({PERP_OPEN_FAILED, LP_OPEN_FAILED, RECOVERY_REQUIRED})


@almanak_strategy(
    name="pancakeswap_delta_neutral_lp",
    version="0.1.0",
    description=(
        "PancakeSwap V3 LP with PancakeSwap Perps short hedge (delta-neutral) on BSC. "
        "Earns LP fees while hedging the volatile-side exposure via a short perp."
    ),
    author="Almanak",
    tags=[
        "demo",
        "lp",
        "perp",
        "delta-neutral",
        "pancakeswap-v3",
        "pancakeswap-perps",
        "bsc",
    ],
    supported_chains=["bsc"],
    default_chain="bsc",
    supported_protocols=["pancakeswap_v3", "pancakeswap_perps"],
    intent_types=["LP_OPEN", "LP_CLOSE", "PERP_OPEN", "PERP_CLOSE", "HOLD"],
)
class PancakeSwapDeltaNeutralLPStrategy(IntentStrategy):
    """PancakeSwap-only delta-neutral LP demo (BSC)."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.state: dict[str, Any] = {}

        # --- LP config ---
        pool = str(self.config.get("pool", "WBNB/USDT/2500"))
        parts = pool.split("/")
        self.pool = pool
        self.token0_symbol = parts[0] if len(parts) > 0 else "WBNB"
        self.token1_symbol = parts[1] if len(parts) > 1 else "USDT"
        self.fee_tier = int(parts[2]) if len(parts) > 2 else 2500

        self.range_width_pct = Decimal(str(self.config.get("range_width_pct", "0.20")))
        self.amount0 = Decimal(str(self.config.get("amount0", "0.01")))
        self.amount1 = Decimal(str(self.config.get("amount1", "5")))

        # --- Perp config ---
        # ApolloX trades synthetic perps keyed on canonical symbols ('BTC',
        # 'ETH', 'BNB'). The compiler maps these back to BSC-registered tokens
        # (BTCB for BTC — Binance-Peg BTC at 0x7130d2A1…, WETH for the bridged
        # ETH, WBNB for BNB) for the price-oracle lookup. ``perp_price_symbol``
        # overrides the gateway-side price symbol if the strategy needs it
        # (e.g., for tracked-token pre-warming in the runner). Default to the
        # LP's token0_symbol so the hedged asset matches the LP's volatile side.
        self.perp_market = str(self.config.get("perp_market", "BNB/USD"))
        self.perp_price_symbol = str(self.config.get("perp_price_symbol", self.token0_symbol))
        self.perp_collateral_token = str(self.config.get("perp_collateral_token", "BNB"))
        self.perp_collateral_amount = Decimal(str(self.config.get("perp_collateral_amount", "0.05")))
        # ApolloX enforces a minimum position notional (~ a few hundred USD on
        # BTC/ETH, lower on BNB). Default size is conservative; users with more
        # LP capital should scale this with amount0 * spot_price.
        self.perp_size_usd_floor = Decimal(str(self.config.get("perp_size_usd", "30")))
        self.perp_leverage = Decimal(str(self.config.get("perp_leverage", "1.5")))
        self.perp_max_slippage = Decimal(str(self.config.get("perp_max_slippage", "0.01")))

        # --- Rebalance ---
        self.delta_rebalance_threshold_pct = Decimal(str(self.config.get("delta_rebalance_threshold_pct", "0.05")))

        logger.info(
            "PancakeSwapDeltaNeutralLPStrategy initialized: pool=%s range=±%s%% "
            "amount0=%s %s amount1=%s %s perp=%s collateral=%s %s lev=%sx thresh=%s%%",
            self.pool,
            self.range_width_pct * 100 / 2,
            self.amount0,
            self.token0_symbol,
            self.amount1,
            self.token1_symbol,
            self.perp_market,
            self.perp_collateral_amount,
            self.perp_collateral_token,
            self.perp_leverage,
            self.delta_rebalance_threshold_pct * 100,
        )

    # -----------------------------------------------------------------
    # Token tracking — match pancakeswap_perps_trend's pattern. Without
    # this the framework auto-detects "BNB/USD" and tries to pre-warm
    # 'BNB' (registered) and 'USD' (NOT registered on BSC), which times
    # out per-token at 30s and stalls decide() for >60s.
    # -----------------------------------------------------------------

    def _get_tracked_tokens(self) -> list[str]:
        margin_lookup = "BNB" if self.perp_collateral_token.upper() in ("BNB", "NATIVE") else self.perp_collateral_token
        seen: set[str] = set()
        out: list[str] = []
        for t in (
            self.token0_symbol,
            self.token1_symbol,
            self.perp_price_symbol,
            margin_lookup,
        ):
            if t and t not in seen:
                seen.add(t)
                out.append(t)
        return out

    # -----------------------------------------------------------------
    # State persistence
    # -----------------------------------------------------------------

    def get_persistent_state(self) -> dict[str, Any]:
        return dict(self.state)

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        self.state = dict(state or {})

    # -----------------------------------------------------------------
    # Main decision loop
    # -----------------------------------------------------------------

    def decide(self, market: MarketSnapshot) -> Intent:
        phase = self.state.get("phase", IDLE)

        # --- Price discovery ---
        try:
            token0_price_usd = market.price(self.token0_symbol)
            token1_price_usd = market.price(self.token1_symbol)
        except Exception as e:  # noqa: BLE001
            logger.warning("Price unavailable: %s", e)
            return Intent.hold(reason=f"Price data unavailable: {e}")

        # Fail safely on any non-positive price — every downstream use of these values
        # (pool-range calculation, hedge-notional sizing, HEDGED drift check) assumes
        # strictly positive inputs; a stale/zero feed would otherwise open a position
        # against a dead price or produce a nonsense LP range.
        if token0_price_usd <= 0:
            return Intent.hold(reason=f"Invalid {self.token0_symbol} price ({token0_price_usd})")
        if token1_price_usd <= 0:
            return Intent.hold(reason=f"Invalid {self.token1_symbol} price ({token1_price_usd})")
        current_pool_price = token0_price_usd / token1_price_usd

        logger.info(
            "[pancakeswap_delta_neutral_lp] phase=%s %s_usd=%s %s_usd=%s pool_price=%s",
            phase,
            self.token0_symbol,
            token0_price_usd,
            self.token1_symbol,
            token1_price_usd,
            current_pool_price,
        )

        # --- Phase: IDLE — open LP ---
        if phase == IDLE:
            logger.info("Phase IDLE -> LP_OPEN")
            return self._build_lp_open_intent(current_pool_price)

        # --- Phase: LP_OPENED — open perp short sized to LP token0 notional ---
        if phase == LP_OPENED:
            # v1 approximation: hedge_size = amount0 * token0_price (see docstring).
            # ApolloX has a minimum notional; floor at perp_size_usd_floor.
            ideal_hedge = (self.amount0 * token0_price_usd).quantize(Decimal("0.01"))
            hedge_notional_usd = max(ideal_hedge, self.perp_size_usd_floor)
            if hedge_notional_usd <= 0:
                return Intent.hold(reason="Hedge notional is zero")
            logger.info(
                "Phase LP_OPENED -> PERP_OPEN short %s size_usd=%s (ideal=%s, floor=%s) collateral=%s %s lev=%sx",
                self.perp_market,
                hedge_notional_usd,
                ideal_hedge,
                self.perp_size_usd_floor,
                self.perp_collateral_amount,
                self.perp_collateral_token,
                self.perp_leverage,
            )
            self.state["hedge_size_usd"] = str(hedge_notional_usd)
            self.state["last_hedge_token0_price_usd_pending"] = str(token0_price_usd)
            return Intent.perp_open(
                market=self.perp_market,
                collateral_token=self.perp_collateral_token,
                collateral_amount=self.perp_collateral_amount,
                size_usd=hedge_notional_usd,
                is_long=False,
                leverage=self.perp_leverage,
                max_slippage=self.perp_max_slippage,
                protocol="pancakeswap_perps",
            )

        # --- Phase: HEDGED — monitor for delta drift ---
        if phase == HEDGED:
            last_hedge_price = Decimal(str(self.state.get("last_hedge_token0_price_usd", token0_price_usd)))
            if last_hedge_price <= 0:
                last_hedge_price = token0_price_usd
            drift_pct = abs(token0_price_usd - last_hedge_price) / last_hedge_price
            logger.info(
                "HEDGED monitor: %s px=%s last_hedge_px=%s drift=%s%% (thresh=%s%%)",
                self.token0_symbol,
                token0_price_usd,
                last_hedge_price,
                drift_pct * 100,
                self.delta_rebalance_threshold_pct * 100,
            )
            if drift_pct >= self.delta_rebalance_threshold_pct:
                if not self.state.get("perp_trade_hash"):
                    # State invariant violation: we're in HEDGED (which only
                    # persists after a successful PERP_OPEN that captured a
                    # tradeHash) but the tradeHash is gone. DO NOT close the
                    # LP here — that would leave a live short stranded with no
                    # way to reference it. Fail closed into RECOVERY_REQUIRED;
                    # operator must locate the short on ApolloX and close it
                    # manually before clearing state.
                    logger.error(
                        "HEDGED drift trigger but perp_trade_hash is missing — "
                        "entering RECOVERY_REQUIRED. Closing the LP in this state "
                        "would expose directional risk via an unknown short. "
                        "Operator must query TradingReaderFacet.getPositionsV2 for "
                        "the wallet and close the short manually before clearing state."
                    )
                    self.state["phase"] = RECOVERY_REQUIRED
                    return Intent.hold(
                        reason="RECOVERY_REQUIRED — hedge cannot be identified; operator must reconcile"
                    )
                logger.info("Delta drift %s%% >= threshold — emitting PERP_CLOSE (staying in HEDGED)", drift_pct * 100)
                # Do NOT pre-flip phase here. The phase transition happens in
                # on_intent_executed(success=True) so a failed PERP_CLOSE does not
                # deadlock us — the next tick simply re-enters this branch and
                # retries the close.
                return self._build_perp_close_intent()
            return Intent.hold(reason=f"Within delta threshold ({drift_pct * 100:.2f}%)")

        # --- Phase: UNHEDGING — legacy transient state, recover by retrying the close ---
        if phase == UNHEDGING:
            # Older strategy versions flipped to UNHEDGING *before* emitting PERP_CLOSE,
            # which deadlocked on a failed close. New code leaves phase at HEDGED until
            # the close succeeds, so UNHEDGING is only reachable from persisted state on
            # upgraded agents.
            if not self.state.get("perp_trade_hash"):
                # State invariant violation: PERP_CLOSE success pops perp_trade_hash
                # AND advances phase to REBALANCING_LP atomically. Seeing phase=UNHEDGING
                # with no trade_hash means something corrupted state (crash mid-callback,
                # manual edit, etc.). Do NOT guess that the close landed — advancing to
                # REBALANCING_LP would close the LP while a live short may still exist.
                logger.error(
                    "UNHEDGING but perp_trade_hash is missing — state invariant "
                    "violation. Entering RECOVERY_REQUIRED; operator must confirm "
                    "the short's status on ApolloX before proceeding."
                )
                self.state["phase"] = RECOVERY_REQUIRED
                return Intent.hold(
                    reason="RECOVERY_REQUIRED — UNHEDGING with no tradeHash; operator must reconcile"
                )
            logger.info("UNHEDGING legacy phase — re-emitting PERP_CLOSE to recover")
            return self._build_perp_close_intent()

        # --- Phase: REBALANCING_LP — close LP, then back to IDLE ---
        if phase == REBALANCING_LP:
            position_id = self.state.get("lp_position_id")
            if not position_id:
                logger.warning("REBALANCING_LP but no lp_position_id — resetting to IDLE")
                self.state["phase"] = IDLE
                return Intent.hold(reason="No LP position id; resetting")
            logger.info("Phase REBALANCING_LP -> LP_CLOSE position_id=%s", position_id)
            return Intent.lp_close(
                position_id=str(position_id),
                pool=self.pool,
                collect_fees=True,
                protocol="pancakeswap_v3",
            )

        # --- Terminal fail-closed phases (require operator intervention) ---
        # In each of these we have detected a state invariant violation where
        # *acting* (retry, close, etc.) could make things worse than *waiting*.
        # decide() holds; the operator must query on-chain state, reconcile, and
        # manually clear / rewrite self.state before the strategy resumes.
        if phase == PERP_OPEN_FAILED:
            return Intent.hold(
                reason=(
                    "PERP_OPEN_FAILED — open short without a captured tradeHash. "
                    "Operator must query TradingReaderFacet.getPositionsV2 and reconcile."
                )
            )
        if phase == LP_OPEN_FAILED:
            return Intent.hold(
                reason=(
                    "LP_OPEN_FAILED — mint TX succeeded but position_id was not captured; "
                    "auto-retry suppressed to avoid a duplicate mint. Operator must query "
                    "NonfungiblePositionManager and reconcile."
                )
            )
        if phase == RECOVERY_REQUIRED:
            return Intent.hold(
                reason=(
                    "RECOVERY_REQUIRED — strategy would close LP while hedge cannot be "
                    "identified. Operator must locate/close the short on ApolloX before "
                    "clearing state."
                )
            )

        # Unknown phase — reset
        logger.warning("Unknown phase=%s — resetting to IDLE", phase)
        self.state["phase"] = IDLE
        return Intent.hold(reason=f"Unknown phase {phase}; reset to IDLE")

    # -----------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------

    def _build_lp_open_intent(self, current_price: Decimal) -> Intent:
        half_width = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)
        logger.info(
            "LP_OPEN %s amount0=%s %s amount1=%s %s range=[%s, %s]",
            self.pool,
            self.amount0,
            self.token0_symbol,
            self.amount1,
            self.token1_symbol,
            range_lower,
            range_upper,
        )
        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="pancakeswap_v3",
        )

    def _build_perp_close_intent(self) -> Intent:
        trade_hash = self.state.get("perp_trade_hash")
        # Caller MUST guard for trade_hash presence — this helper only builds the intent.
        return Intent.perp_close(
            market=self.perp_market,
            collateral_token=self.perp_collateral_token,
            is_long=False,
            max_slippage=self.perp_max_slippage,
            protocol="pancakeswap_perps",
            position_id=str(trade_hash) if trade_hash else None,
        )

    # -----------------------------------------------------------------
    # Post-execution: advance phase + capture position IDs
    # -----------------------------------------------------------------

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        intent_type = getattr(getattr(intent, "intent_type", None), "value", None) or ""
        if not success:
            logger.warning(
                "Intent %s failed; staying in phase=%s",
                intent_type,
                self.state.get("phase"),
            )
            return

        if intent_type == "LP_OPEN":
            pos = getattr(result, "position_id", None)
            if pos is None:
                extracted = getattr(result, "extracted_data", {}) or {}
                pos = extracted.get("position_id")
            if pos is None:
                # Fail closed: we cannot manage (rebalance / close) an LP position we
                # cannot identify. Enter LP_OPEN_FAILED (terminal) instead of falling
                # back to IDLE — IDLE would auto-retry LP_OPEN on the next tick and
                # risk minting a duplicate LP NFT if the original mint did in fact
                # succeed on-chain. Operator must query the NonfungiblePositionManager
                # for the wallet to reconcile before clearing state.
                logger.error(
                    "LP_OPEN succeeded on-chain but no position_id was captured — "
                    "entering LP_OPEN_FAILED. Auto-retry is unsafe because the mint "
                    "TX may have succeeded; a retry would duplicate the position. "
                    "Operator must query NonfungiblePositionManager.tokenOfOwnerByIndex "
                    "for the wallet, record the NFT id into state['lp_position_id'], "
                    "and set state['phase']=LP_OPENED before re-enabling."
                )
                self.state["phase"] = LP_OPEN_FAILED
                return
            self.state["lp_position_id"] = str(pos)
            logger.info("Captured lp_position_id=%s", pos)
            self.state["phase"] = LP_OPENED
            return

        if intent_type == "PERP_OPEN":
            # PCS Perps: the receipt parser extracts the bytes32 tradeHash.
            # ResultEnricher places anything non-int / non-40-char-hex into
            # extracted_data["position_id"] (bytes32 = 64 hex chars fails the
            # 40-char-hex address check and falls through). Cover both.
            extracted = getattr(result, "extracted_data", {}) or {}
            trade_hash = extracted.get("position_id") or getattr(result, "position_id", None)
            if not trade_hash:
                # Fail closed: a live short whose tradeHash we never captured cannot be
                # closed via Intent.perp_close (the PCS Perps compiler requires a
                # bytes32 position_id) nor matched back to its on-chain state. Moving
                # to HEDGED would strand the position when the drift check eventually
                # tries to unhedge. Require operator intervention instead.
                logger.error(
                    "PERP_OPEN succeeded on-chain but no bytes32 tradeHash was extracted — "
                    "entering PERP_OPEN_FAILED; operator must reconcile the open short "
                    "via TradingReaderFacet.getPositionsV2 before this strategy can continue"
                )
                # Preserve the pending hedge-price marker so the operator has it.
                self.state["phase"] = PERP_OPEN_FAILED
                return
            self.state["perp_trade_hash"] = str(trade_hash)
            logger.info("Captured perp_trade_hash=%s", trade_hash)
            self.state["last_hedge_token0_price_usd"] = self.state.pop("last_hedge_token0_price_usd_pending", "0")
            self.state["phase"] = HEDGED
            return

        if intent_type == "PERP_CLOSE":
            self.state.pop("perp_trade_hash", None)
            # Direct HEDGED/UNHEDGING -> REBALANCING_LP on success. The decide() branch
            # no longer pre-flips to UNHEDGING before emitting the close, so this is
            # the ONLY path that advances past the hedge.
            self.state["phase"] = REBALANCING_LP
            logger.info("Perp closed; phase -> REBALANCING_LP")
            return

        if intent_type == "LP_CLOSE":
            self.state.pop("lp_position_id", None)
            self.state["phase"] = IDLE
            logger.info("LP closed; phase -> IDLE (will re-open next tick)")
            return

    # -----------------------------------------------------------------
    # Teardown
    # -----------------------------------------------------------------

    def get_open_positions(self) -> Any:
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        phase = self.state.get("phase", IDLE)
        in_terminal = phase in _TERMINAL_PHASES

        positions: list[PositionInfo] = []
        if self.state.get("lp_position_id"):
            lp_details: dict[str, Any] = {
                "pool": self.pool,
                "token0": self.token0_symbol,
                "token1": self.token1_symbol,
                "fee_tier": self.fee_tier,
            }
            if in_terminal:
                # Flag: the operator cannot trust this is the only / real position.
                lp_details["state"] = "unknown"
                lp_details["recovery_phase"] = phase
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=str(self.state["lp_position_id"]),
                    chain=self.chain,
                    protocol="pancakeswap_v3",
                    value_usd=Decimal(str(self.state.get("hedge_size_usd", "0"))) * Decimal("2"),
                    details=lp_details,
                )
            )
        if self.state.get("perp_trade_hash"):
            perp_details: dict[str, Any] = {
                "market": self.perp_market,
                "is_long": False,
                "collateral_token": self.perp_collateral_token,
            }
            if in_terminal:
                perp_details["state"] = "unknown"
                perp_details["recovery_phase"] = phase
            positions.append(
                PositionInfo(
                    position_type=PositionType.PERP,
                    position_id=str(self.state["perp_trade_hash"]),
                    chain=self.chain,
                    protocol="pancakeswap_perps",
                    value_usd=Decimal(str(self.state.get("hedge_size_usd", "0"))),
                    details=perp_details,
                )
            )
        # Emit a sentinel unknown-state entry for terminal phases where we KNOW a
        # position exists on-chain but we have no id for it (LP_OPEN_FAILED,
        # PERP_OPEN_FAILED, RECOVERY_REQUIRED with a lost hash). Operators reading
        # this summary need to see that *something* needs reconciliation even if
        # the id field is empty.
        if phase == LP_OPEN_FAILED and not self.state.get("lp_position_id"):
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id="unknown-lp",
                    chain=self.chain,
                    protocol="pancakeswap_v3",
                    value_usd=Decimal("0"),
                    details={
                        "state": "unknown",
                        "recovery_phase": phase,
                        "note": (
                            "LP mint TX succeeded but position_id was not captured. "
                            "Query NonfungiblePositionManager for the wallet to find the NFT id."
                        ),
                        "pool": self.pool,
                    },
                )
            )
        if phase == PERP_OPEN_FAILED and not self.state.get("perp_trade_hash"):
            positions.append(
                PositionInfo(
                    position_type=PositionType.PERP,
                    position_id="unknown-perp",
                    chain=self.chain,
                    protocol="pancakeswap_perps",
                    value_usd=Decimal("0"),
                    details={
                        "state": "unknown",
                        "recovery_phase": phase,
                        "note": (
                            "Perp open TX succeeded but bytes32 tradeHash was not captured. "
                            "Query TradingReaderFacet.getPositionsV2 for the wallet."
                        ),
                        "market": self.perp_market,
                    },
                )
            )
        return TeardownPositionSummary(
            deployment_id=self.deployment_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: Any, market: Any = None) -> list[Intent]:
        """Return ordered intents to close all positions (perp first, then LP).

        In terminal fail-closed phases (PERP_OPEN_FAILED, LP_OPEN_FAILED,
        RECOVERY_REQUIRED) we refuse to auto-teardown: by definition at least
        one position in these phases is either unknown-to-us or cannot be
        safely matched to an intent, and the wrong teardown sequence could
        close the LP while a live short persists or vice versa. The operator
        must reconcile on-chain state first.
        """
        phase = self.state.get("phase", IDLE)
        if phase in _TERMINAL_PHASES:
            logger.warning(
                "generate_teardown_intents suppressed: phase=%s is a fail-closed terminal "
                "state. Automated teardown could strand or duplicate a position. Operator "
                "must reconcile on-chain state (NonfungiblePositionManager and "
                "TradingReaderFacet.getPositionsV2) before invoking teardown.",
                phase,
            )
            return []

        # Close perp first (eliminate market risk), then LP.
        intents: list[Intent] = []
        if self.state.get("perp_trade_hash"):
            intents.append(self._build_perp_close_intent())
        if self.state.get("lp_position_id"):
            intents.append(
                Intent.lp_close(
                    position_id=str(self.state["lp_position_id"]),
                    pool=self.pool,
                    collect_fees=True,
                    protocol="pancakeswap_v3",
                )
            )
        return intents
