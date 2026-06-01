"""Aster Perps basic demo — open then close a single BNB/USD long (VIB-3055).

This is the minimum viable ``protocol="aster_perps"`` strategy: tick 1 opens a
3x BNB/USD long with native BNB margin; tick 2 closes it. No signal logic, no
indicators — it's a connector smoke test you can run against ``--network anvil``
to verify the Aster Diamond open/close lifecycle executes end-to-end on your
wallet.

Routes through the canonical ``aster_perps`` path (broker id = 0, raw Aster —
no PancakeSwap attribution). If you want PCS attribution instead, either
switch ``protocol="pancakeswap_perps"`` below or use the
``pancakeswap_perps_trend`` demo, which runs the same connector through the
compatibility shim.

Two-phase execution (important!):
    Every open and close on Aster is user-TX + off-chain-keeper settle.
      1. The user-signed ``openMarketTrade(BNB)`` call emits
         ``MarketPendingTrade(tradeHash)``.
      2. A PRICE_FEEDER_ROLE holder subsequently calls
         ``PriceFacadeFacet.requestPriceCallback`` to fill the trade at the
         oracle price, emitting ``OpenMarketTrade``.
    This demo persists the ``tradeHash`` across ticks via ``state`` so tick 2
    can close the position by ``position_id``. On a live BSC run the keeper
    fill happens automatically; on an Anvil fork you can simulate it with the
    helpers in ``tests/intents/bnb/conftest.py::pcs_perps_keeper_fulfill``.

Usage:
    # Local Anvil fork (auto-starts Anvil + gateway):
    almanak strat run -d almanak/demo_strategies/aster_perps_basic --network anvil --once
    almanak strat run -d almanak/demo_strategies/aster_perps_basic --network anvil --once  # second tick closes

    # Live BSC (requires ALCHEMY_API_KEY + ALMANAK_PRIVATE_KEY funded with BNB):
    almanak strat run -d almanak/demo_strategies/aster_perps_basic --once
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="aster_perps_basic",
    version="0.1.0",
    description="Minimal Aster Perps smoke test: open + close a BNB/USD long (BSC, broker=0 raw)",
    supported_chains=["bsc"],
    default_chain="bsc",
    supported_protocols=["aster_perps"],
    intent_types=["PERP_OPEN", "PERP_CLOSE", "HOLD"],
)
class AsterPerpsBasicStrategy(IntentStrategy):
    """Open-then-close BNB/USD long on Aster Perps via the canonical ``aster_perps`` route."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.state: dict = {}

    # -----------------------------------------------------------------
    # State hooks
    # -----------------------------------------------------------------

    def get_persistent_state(self) -> dict:
        return dict(self.state)

    def load_persistent_state(self, state: dict) -> None:
        self.state = dict(state or {})

    # -----------------------------------------------------------------
    # Token tracking — derive from the configured market + collateral so the
    # demo works for BTC/USD and ETH/USD (not just the default BNB/USD).
    # Default tracker would pre-warm the literal "USD" quote symbol which
    # has no price feed — skip it.
    # -----------------------------------------------------------------

    # Aster Perps is BSC-only; BTC on BSC resolves to BTCB (Binance-Peg BTC,
    # 0x7130d2A1…, 18 decimals). WBTC still works through the legacy alias
    # but the canonical registered symbol is BTCB — match it here so the
    # pre-warm hits the canonical entry directly.
    _WRAP_MAP = {"BNB": "WBNB", "ETH": "WETH", "BTC": "BTCB"}

    def _get_tracked_tokens(self) -> list[str]:
        market = str(self.config.get("market", "BNB/USD"))
        base_asset = market.split("/")[0].strip().upper() if "/" in market else market.strip().upper()
        collateral_token = str(self.config.get("collateral_token", "BNB")).strip().upper()
        tokens: list[str] = []
        for sym in (base_asset, self._WRAP_MAP.get(base_asset, ""), collateral_token,
                    self._WRAP_MAP.get(collateral_token, "")):
            if sym and sym != "USD" and sym not in tokens:
                tokens.append(sym)
        return tokens

    # -----------------------------------------------------------------
    # Decide
    # -----------------------------------------------------------------

    def decide(self, market: MarketSnapshot) -> Intent:
        market_symbol = self.config.get("market", "BNB/USD")
        collateral_token = self.config.get("collateral_token", "BNB")
        collateral_amount = Decimal(str(self.config.get("collateral_amount", "0.3")))
        size_usd = Decimal(str(self.config.get("size_usd", "500")))
        leverage = Decimal(str(self.config.get("leverage", "3")))
        max_slippage = Decimal(str(self.config.get("max_slippage", "0.01")))
        is_long = bool(self.config.get("is_long", True))

        open_trade_hash = self.state.get("open_trade_hash")
        pending_open = self.state.get("pending_open", False)
        closed = self.state.get("closed", False)

        # Tick 2+ with an open position: close it.
        if open_trade_hash and not closed:
            logger.info(
                f"[aster_perps_basic] closing position {open_trade_hash[:18]}... on {market_symbol}"
            )
            return Intent.perp_close(
                market=market_symbol,
                collateral_token=collateral_token,
                is_long=is_long,
                max_slippage=max_slippage,
                protocol="aster_perps",
                position_id=open_trade_hash,
            )

        # Once closed, the demo is done. Future ticks hold.
        if closed:
            return Intent.hold(reason="Demo lifecycle complete (opened then closed)")

        # Tick 1 (or retry): open the position.
        if not pending_open:
            logger.info(
                f"[aster_perps_basic] opening {'LONG' if is_long else 'SHORT'} {market_symbol}: "
                f"collateral={collateral_amount} {collateral_token}, size=${size_usd}, leverage={leverage}x"
            )
            self.state["pending_open"] = True
            return Intent.perp_open(
                market=market_symbol,
                collateral_token=collateral_token,
                collateral_amount=collateral_amount,
                size_usd=size_usd,
                is_long=is_long,
                leverage=leverage,
                max_slippage=max_slippage,
                protocol="aster_perps",
            )

        # Pending keeper settlement from a previous tick — wait.
        return Intent.hold(reason="Waiting for open-tradeHash to settle on-chain")

    # -----------------------------------------------------------------
    # Post-execution: persist the tradeHash on open, mark closed on close.
    # -----------------------------------------------------------------

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        if not success:
            self.state["pending_open"] = False
            return

        intent_type = str(getattr(intent, "intent_type", ""))
        if intent_type.endswith("PERP_CLOSE"):
            logger.info("[aster_perps_basic] PERP_CLOSE succeeded — demo lifecycle complete")
            self.state["closed"] = True
            self.state.pop("open_trade_hash", None)
            self.state["pending_open"] = False
            return

        # OPEN: stash the tradeHash for tick 2. ResultEnricher promotes the
        # 66-char bytes32 tradeHash to result.position_id (since the audit fix
        # that extended _attach_to_result to accept bytes32 alongside NFT ids
        # and 40-char addresses). Falling back to extracted_data is defensive
        # in case the parser path changes in the future.
        position_id = getattr(result, "position_id", None)
        if position_id is None:
            extracted = getattr(result, "extracted_data", {}) or {}
            position_id = extracted.get("position_id")
        if position_id:
            self.state["open_trade_hash"] = str(position_id)
            self.state["pending_open"] = False
            logger.info(f"[aster_perps_basic] persisted tradeHash={position_id}")

    # -----------------------------------------------------------------
    # Teardown
    # -----------------------------------------------------------------

    def get_open_positions(self) -> TeardownPositionSummary:
        from datetime import UTC, datetime

        from almanak.framework.teardown import PositionInfo, PositionType

        positions = []
        if self.state.get("open_trade_hash") and not self.state.get("closed"):
            # Demo simplification: value_usd reports the configured nominal size, not the live
            # on-chain position value. A production strategy MUST query the Aster Diamond
            # (TradingReaderFacet.getPositionByHashV2) to read current margin + notional and
            # apply the mark price — otherwise teardown caps may bias against losing positions.
            positions.append(
                PositionInfo(
                    position_type=PositionType.PERP,
                    position_id=self.state["open_trade_hash"],
                    chain="bsc",
                    protocol="aster_perps",
                    value_usd=Decimal(str(self.config.get("size_usd", "500"))),
                    details={
                        "market": self.config.get("market", "BNB/USD"),
                        "is_long": bool(self.config.get("is_long", True)),
                    },
                )
            )
        return TeardownPositionSummary(
            deployment_id=self.deployment_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(
        self, mode: TeardownMode, market: MarketSnapshot | None = None
    ) -> list[Intent]:
        open_trade_hash = self.state.get("open_trade_hash")
        if not open_trade_hash or self.state.get("closed"):
            return []

        slippage = (
            Decimal(str(self.config.get("teardown_hard_slippage", "0.03")))
            if mode == TeardownMode.HARD
            else Decimal(str(self.config.get("max_slippage", "0.01")))
        )
        return [
            Intent.perp_close(
                market=self.config.get("market", "BNB/USD"),
                collateral_token=self.config.get("collateral_token", "BNB"),
                is_long=bool(self.config.get("is_long", True)),
                max_slippage=slippage,
                protocol="aster_perps",
                position_id=open_trade_hash,
            )
        ]
