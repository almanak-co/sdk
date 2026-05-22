"""Arbitrum Perp Lifecycle Probe: bridge + swap + GMX V2 perpetual open/close.

Starts with USDC on Base, bridges to Arbitrum, swaps some USDC for ETH
(execution fees), opens a GMX V2 perpetual long position on ETH/USD with
USDC collateral, waits for keeper fill, closes the position, swaps ETH
back to USDC, then bridges back to Base.

GMX V2 is structurally incompatible with Anvil forks (keepers don't
execute orders on forks). This probe must run on mainnet only.

Design: each phase executes exactly once. No retries. Phase advances
unconditionally on the next tick after submission (except SWAP_IN which
waits for bridged USDC, and WAIT_FILL/WAIT_CLOSE which wait for keepers).

Phase flow:
  BRIDGE_IN  -> Bridge USDC Base->Arb
  SWAP_IN    -> Swap USDC->ETH on Arb (waits for bridge to land)
  OPEN       -> PERP_OPEN ETH/USD long via GMX V2 (USDC collateral)
  WAIT_FILL  -> Wait 1 tick for keeper to fill the open order
  CLOSE      -> PERP_CLOSE the position
  WAIT_CLOSE -> Wait 1 tick for keeper to fill the close order
  SWAP_OUT   -> Swap ETH->USDC on Arb
  BRIDGE_OUT -> Bridge all USDC Arb->Base
  DONE       -> Evaluate PASS/FAIL, log budget delta
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
    name="probe_arbitrum_perp_lifecycle",
    description="Bridge + swap + GMX V2 perp lifecycle probe on Arbitrum (mainnet only)",
    version="3.0.0",
    author="Almanak",
    tags=["probe", "nightly", "perps", "bridge", "swap", "arbitrum", "mainnet-only", "lifecycle"],
    supported_chains=["base", "arbitrum"],
    supported_protocols=["gmx_v2", "enso"],
    intent_types=["BRIDGE", "SWAP", "PERP_OPEN", "PERP_CLOSE"],
    default_chain="base",
)
class ArbitrumPerpLifecycleProbeStrategy(IntentStrategy):
    """Deterministic bridge + swap + GMX V2 perp lifecycle probe for Arbitrum.

    Each phase fires exactly once with no retries.
    """

    SUPPORTED_CHAINS = ["base", "arbitrum"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.target_chain = self.get_config("target_chain", "arbitrum")
        self.budget_usd = Decimal(str(self.get_config("budget_usd", "3")))
        self.relay_token = self.get_config("relay_token", "USDC")
        self.max_slippage = Decimal(str(self.get_config("max_slippage_pct", "1.0"))) / Decimal("100")
        self.swap_eth_usd = Decimal(str(self.get_config("swap_eth_usd", "1.50")))
        self.perp_market = self.get_config("perp_market", "ETH/USD")
        self.perp_collateral_token = self.get_config("perp_collateral_token", "USDC")
        self.perp_collateral_usd = Decimal(str(self.get_config("perp_collateral_usd", "1.50")))
        self.perp_leverage = Decimal(str(self.get_config("perp_leverage", "2")))
        self.perp_is_long = self.get_config("perp_is_long", True)
        self.perp_protocol = self.get_config("perp_protocol", "gmx_v2")

        # Phase is the NEXT action to take. Once an action fires, phase advances.
        self._phase = "BRIDGE_IN"
        self._open_success = False
        self._close_success = False
        self._leg_results: list[dict] = []
        self._start_usdc_balance: Decimal | None = None
        self._load_from_state()

        logger.info(
            f"ArbitrumPerpLifecycleProbe v3: ${self.budget_usd} budget, "
            f"phase={self._phase}, {self.perp_market} {self.perp_leverage}x "
            f"{'long' if self.perp_is_long else 'short'}, "
            f"swap ${self.swap_eth_usd}->ETH, collateral=${self.perp_collateral_usd} {self.perp_collateral_token}"
        )

    # -- State persistence -----------------------------------------------------

    def _load_from_state(self):
        state = self.get_persistent_state()
        if state:
            self._phase = state.get("phase", "BRIDGE_IN")
            self._open_success = state.get("open_success", False)
            self._close_success = state.get("close_success", False)
            self._start_usdc_balance = (
                Decimal(str(state["start_usdc_balance"])) if state.get("start_usdc_balance") else None
            )

    def get_persistent_state(self) -> dict[str, Any]:
        state = super().get_persistent_state() if hasattr(super(), "get_persistent_state") else {}
        state.update({
            "phase": self._phase,
            "open_success": self._open_success,
            "close_success": self._close_success,
            "start_usdc_balance": str(self._start_usdc_balance) if self._start_usdc_balance else None,
        })
        return state

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if hasattr(super(), "load_persistent_state"):
            super().load_persistent_state(state)
        self._phase = state.get("phase", "BRIDGE_IN")
        self._open_success = state.get("open_success", False)
        self._close_success = state.get("close_success", False)
        self._start_usdc_balance = (
            Decimal(str(state["start_usdc_balance"])) if state.get("start_usdc_balance") else None
        )

    # -- Helpers ---------------------------------------------------------------

    def _bal_usd(self, market: MarketSnapshot, token: str, chain: str) -> Decimal:
        try:
            b = market.balance(token, chain)
            return Decimal(str(b.balance_usd)) if b and b.balance_usd else Decimal("0")
        except Exception:
            return Decimal("0")

    def _bal_raw(self, market: MarketSnapshot, token: str, chain: str) -> Decimal:
        try:
            b = market.balance(token, chain)
            return Decimal(str(b.balance)) if b and b.balance else Decimal("0")
        except Exception:
            return Decimal("0")

    def _advance(self, next_phase: str) -> None:
        logger.info(f"Phase transition: {self._phase} -> {next_phase}")
        self._phase = next_phase

    # -- decide ----------------------------------------------------------------

    def decide(self, market: MarketSnapshot) -> Intent:
        if self._start_usdc_balance is None:
            b = market.balance(self.relay_token, "base")
            if b and b.balance_usd:
                self._start_usdc_balance = Decimal(str(b.balance_usd))
                logger.info(f"BUDGET: starting USDC = ${self._start_usdc_balance}")

        handler = {
            "BRIDGE_IN": self._do_bridge_in,
            "SWAP_IN": self._do_swap_in,
            "OPEN": self._do_open,
            "WAIT_FILL": self._do_wait_fill,
            "CLOSE": self._do_close,
            "WAIT_CLOSE": self._do_wait_close,
            "SWAP_OUT": self._do_swap_out,
            "BRIDGE_OUT": self._do_bridge_out,
            "DONE": self._do_done,
        }.get(self._phase)

        if handler:
            return handler(market)
        return Intent.hold(reason=f"Unknown phase: {self._phase}")

    # -- Phase handlers (each fires its intent, then sets phase for next tick) -

    def _do_bridge_in(self, market: MarketSnapshot) -> Intent:
        # Pre-flight balance check
        base_usdc = self._bal_usd(market, self.relay_token, "base")
        if base_usdc < self.budget_usd:
            logger.error(f"BRIDGE_IN: insufficient Base USDC (${base_usdc} < ${self.budget_usd})")
            self._advance("DONE")
            return Intent.hold(reason=f"PROBE_FAIL: Base USDC ${base_usdc} < ${self.budget_usd} minimum")

        base_eth = self._bal_raw(market, "ETH", "base")
        if base_eth < Decimal("0.003"):
            logger.error(f"BRIDGE_IN: insufficient Base ETH for gas ({base_eth} < 0.003)")
            self._advance("DONE")
            return Intent.hold(reason=f"PROBE_FAIL: Base ETH {base_eth} < 0.003 for bridge gas")

        logger.info(f"BRIDGE_IN: bridge ${self.budget_usd} USDC Base->{self.target_chain}")
        self._advance("SWAP_IN")
        return Intent.bridge(
            token=self.relay_token, amount=self.budget_usd,
            from_chain="base", to_chain=self.target_chain, max_slippage=self.max_slippage,
        )

    def _do_swap_in(self, market: MarketSnapshot) -> Intent:
        arb_usdc = self._bal_usd(market, self.relay_token, self.target_chain)
        if arb_usdc < self.swap_eth_usd:
            logger.info(f"SWAP_IN: waiting for USDC (${arb_usdc} < ${self.swap_eth_usd})")
            return Intent.hold(reason=f"Waiting for bridged USDC (${arb_usdc})")

        logger.info(f"SWAP_IN: swap ${self.swap_eth_usd} USDC->ETH via Enso")
        self._advance("OPEN")
        return Intent.swap(
            from_token="USDC", to_token="ETH", amount_usd=self.swap_eth_usd,
            max_slippage=self.max_slippage, protocol="enso", chain=self.target_chain,
        )

    def _do_open(self, market: MarketSnapshot) -> Intent:
        size_usd = self.perp_collateral_usd * self.perp_leverage
        logger.info(f"OPEN: PERP_OPEN {self.perp_market} ${self.perp_collateral_usd} x{self.perp_leverage} = ${size_usd}")
        self._advance("WAIT_FILL")
        return Intent.perp_open(
            market=self.perp_market, collateral_token=self.perp_collateral_token,
            collateral_amount=self.perp_collateral_usd, size_usd=size_usd,
            is_long=self.perp_is_long, leverage=self.perp_leverage,
            max_slippage=self.max_slippage, protocol=self.perp_protocol, chain=self.target_chain,
        )

    def _do_wait_fill(self, market: MarketSnapshot) -> Intent:
        logger.info("WAIT_FILL: 1-tick wait for keeper to fill open order")
        self._advance("CLOSE")
        return Intent.hold(reason="Waiting for keeper to fill open order")

    def _do_close(self, market: MarketSnapshot) -> Intent:
        size_usd = self.perp_collateral_usd * self.perp_leverage
        logger.info(f"CLOSE: PERP_CLOSE {self.perp_market} size=${size_usd}")
        self._advance("WAIT_CLOSE")
        return Intent.perp_close(
            market=self.perp_market, collateral_token=self.perp_collateral_token,
            is_long=self.perp_is_long, size_usd=size_usd,
            max_slippage=self.max_slippage, protocol=self.perp_protocol, chain=self.target_chain,
        )

    def _do_wait_close(self, market: MarketSnapshot) -> Intent:
        logger.info("WAIT_CLOSE: 1-tick wait for keeper to fill close order")
        self._advance("SWAP_OUT")
        return Intent.hold(reason="Waiting for keeper to fill close order")

    def _do_swap_out(self, market: MarketSnapshot) -> Intent:
        logger.info(f"SWAP_OUT: swap ${self.swap_eth_usd} ETH->USDC via Enso")
        self._advance("BRIDGE_OUT")
        return Intent.swap(
            from_token="ETH", to_token="USDC", amount_usd=self.swap_eth_usd,
            max_slippage=self.max_slippage, protocol="enso", chain=self.target_chain,
        )

    def _do_bridge_out(self, market: MarketSnapshot) -> Intent:
        logger.info(f"BRIDGE_OUT: bridge all USDC {self.target_chain}->Base")
        self._advance("DONE")
        return Intent.bridge(
            token=self.relay_token, amount="all",
            from_chain=self.target_chain, to_chain="base", max_slippage=self.max_slippage,
        )

    def _do_done(self, market: MarketSnapshot) -> Intent:
        end = self._bal_usd(market, self.relay_token, "base")
        if self._start_usdc_balance and end:
            loss = self._start_usdc_balance - end
            pct = (loss / self._start_usdc_balance) * Decimal("100")
            logger.info(f"BUDGET: started=${self._start_usdc_balance}, ended=${end}, loss=${loss} ({pct:.1f}%)")

        verdict = "PASS" if self._open_success and self._close_success else "FAIL"
        log = logger.info if verdict == "PASS" else logger.error
        log(f"PROBE RESULT: {verdict} - open={'PASS' if self._open_success else 'FAIL'}, close={'PASS' if self._close_success else 'FAIL'}")
        return Intent.hold(reason="Probe complete")

    # -- Callback (logging only) -----------------------------------------------

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        it = str(getattr(intent, "intent_type", "?"))
        proto = getattr(intent, "protocol", "?")
        if success:
            logger.info(f"  [cb] {proto} {it} OK")
            if "PERP_OPEN" in it:
                self._open_success = True
            elif "PERP_CLOSE" in it:
                self._close_success = True
        else:
            err = getattr(result, "error", "?") if result else "no result"
            logger.error(f"  [cb] {proto} {it} FAIL: {err}")
        self._leg_results.append({"success": success, "intent_type": it, "protocol": proto, "phase": self._phase})

    # -- Teardown --------------------------------------------------------------

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self):
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary
        positions = []
        if self._open_success and not self._close_success:
            positions.append(PositionInfo(
                position_type=PositionType.PERP, position_id=f"gmx_v2_{self.perp_market}",
                chain=self.target_chain, protocol=self.perp_protocol,
                value_usd=self.perp_collateral_usd,
                details={"market": self.perp_market, "is_long": self.perp_is_long},
            ))
        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "probe_arbitrum_perp_lifecycle"),
            timestamp=datetime.now(UTC), positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None):
        from almanak.framework.teardown import TeardownMode
        slippage = Decimal("0.03") if mode == TeardownMode.HARD else self.max_slippage
        size_usd = self.perp_collateral_usd * self.perp_leverage
        intents = []
        if self._open_success and not self._close_success:
            intents.append(Intent.perp_close(
                market=self.perp_market, collateral_token=self.perp_collateral_token,
                is_long=self.perp_is_long, size_usd=size_usd, max_slippage=slippage,
                protocol=self.perp_protocol, chain=self.target_chain,
            ))
        intents.append(Intent.swap(
            from_token="ETH", to_token="USDC", amount="all",
            max_slippage=slippage, protocol="enso", chain=self.target_chain,
        ))
        intents.append(Intent.bridge(
            token=self.relay_token, amount="all",
            from_chain=self.target_chain, to_chain="base", max_slippage=slippage,
        ))
        return intents
