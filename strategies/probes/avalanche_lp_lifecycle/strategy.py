"""Avalanche LP Lifecycle Probe: bridge + swap + TraderJoe V2 LP open/close.

Starts with USDC on Base, bridges to Avalanche, swaps some USDC for WAVAX,
opens a dual-sided TraderJoe V2 LP position (WAVAX + USDC), closes it,
swaps WAVAX back to USDC, then bridges back to Base.

Budget allocation ($2):
  $1 -> swap to WAVAX (LP token0)
  $1 -> LP as USDC (LP token1)

Design: fire-once phase machine. Each phase executes exactly once.

Phase flow:
  BRIDGE_IN  -> Bridge $2 USDC Base->Avalanche
  WAIT_BRIDGE -> Wait for bridged USDC to land
  SWAP_IN    -> Swap $1 USDC->WAVAX via Enso
  OPEN       -> LP_OPEN WAVAX/USDC on TraderJoe V2
  CLOSE      -> LP_CLOSE position
  SWAP_OUT   -> Swap $1 WAVAX->USDC via Enso
  BRIDGE_OUT -> Bridge $2 USDC Avalanche->Base
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
    name="probe_avalanche_lp_lifecycle",
    description="Bridge + swap + TraderJoe V2 LP lifecycle probe on Avalanche",
    version="3.0.0",
    author="Almanak",
    tags=["probe", "nightly", "lp", "bridge", "swap", "avalanche", "lifecycle"],
    supported_chains=["base", "avalanche"],
    supported_protocols=["traderjoe_v2", "enso"],
    intent_types=["BRIDGE", "SWAP", "LP_OPEN", "LP_CLOSE"],
    default_chain="base",
)
class AvalancheLPLifecycleProbeStrategy(IntentStrategy):
    """Deterministic bridge + swap + TraderJoe V2 LP lifecycle probe.

    Each phase fires exactly once with no retries.
    """

    SUPPORTED_CHAINS = ["base", "avalanche"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.target_chain = self.get_config("target_chain", "avalanche")
        self.budget_usd = Decimal(str(self.get_config("budget_usd", "2")))
        self.relay_token = self.get_config("relay_token", "USDC")
        self.lp_token0 = self.get_config("lp_token0", "WAVAX")
        self.lp_token1 = self.get_config("lp_token1", "USDC")
        self.pool = self.get_config("pool", "WAVAX/USDC/20")
        self.swap_wavax_usd = Decimal(str(self.get_config("swap_wavax_usd", "1")))
        self.lp_usdc = Decimal(str(self.get_config("lp_usdc", "1")))
        self.protocol = self.get_config("protocol", "traderjoe_v2")
        self.max_slippage = Decimal(str(self.get_config("max_slippage_pct", "1.0"))) / Decimal("100")

        self._phase = "BRIDGE_IN"
        self._position_id: str | None = None
        self._open_success = False
        self._close_success = False
        self._had_failures = False
        self._leg_results: list[dict] = []
        self._start_usdc_balance: Decimal | None = None
        self._load_from_state()

        logger.info(
            f"AvalancheLPLifecycleProbe v3: ${self.budget_usd} budget, "
            f"phase={self._phase}, pool={self.pool}, "
            f"swap ${self.swap_wavax_usd}->WAVAX, LP ${self.lp_usdc} USDC"
        )

    # -- State persistence -----------------------------------------------------

    def _load_from_state(self):
        state = self.get_persistent_state()
        if state:
            self._phase = state.get("phase", "BRIDGE_IN")
            self._position_id = state.get("position_id")
            self._open_success = state.get("open_success", False)
            self._close_success = state.get("close_success", False)
            self._start_usdc_balance = (
                Decimal(str(state["start_usdc_balance"])) if state.get("start_usdc_balance") else None
            )
            self._had_failures = state.get("had_failures", False)

    def get_persistent_state(self) -> dict[str, Any]:
        state = super().get_persistent_state() if hasattr(super(), "get_persistent_state") else {}
        state.update({
            "phase": self._phase,
            "position_id": self._position_id,
            "open_success": self._open_success,
            "close_success": self._close_success,
            "had_failures": self._had_failures,
            "start_usdc_balance": str(self._start_usdc_balance) if self._start_usdc_balance else None,
        })
        return state

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if hasattr(super(), "load_persistent_state"):
            super().load_persistent_state(state)
        self._phase = state.get("phase", "BRIDGE_IN")
        self._position_id = state.get("position_id")
        self._open_success = state.get("open_success", False)
        self._close_success = state.get("close_success", False)
        self._had_failures = state.get("had_failures", False)
        self._start_usdc_balance = (
            Decimal(str(state["start_usdc_balance"])) if state.get("start_usdc_balance") else None
        )

    # -- Helpers ---------------------------------------------------------------

    def _bal_usd(self, market: MarketSnapshot, token: str, chain: str) -> Decimal:
        try:
            b = market.balance(token, chain=chain)
            return Decimal(str(b.balance_usd)) if b and b.balance_usd else Decimal("0")
        except Exception:
            return Decimal("0")

    def _bal_raw(self, market: MarketSnapshot, token: str, chain: str) -> Decimal:
        try:
            b = market.balance(token, chain=chain)
            return Decimal(str(b.balance)) if b and b.balance else Decimal("0")
        except Exception:
            return Decimal("0")

    def _advance(self, next_phase: str) -> None:
        logger.info(f"Phase transition: {self._phase} -> {next_phase}")
        self._phase = next_phase

    # -- decide ----------------------------------------------------------------

    def decide(self, market: MarketSnapshot) -> Intent:
        if self._start_usdc_balance is None:
            b = market.balance(self.relay_token, chain="base")
            if b and b.balance_usd:
                self._start_usdc_balance = Decimal(str(b.balance_usd))
                logger.info(f"BUDGET: starting USDC = ${self._start_usdc_balance}")

        handler = {
            "BRIDGE_IN": self._do_bridge_in,
            "WAIT_BRIDGE": self._do_wait_bridge,
            "SWAP_IN": self._do_swap_in,
            "OPEN": self._do_open,
            "CLOSE": self._do_close,
            "SWAP_OUT": self._do_swap_out,
            "BRIDGE_OUT": self._do_bridge_out,
            "DONE": self._do_done,
        }.get(self._phase)

        if handler:
            return handler(market)
        return Intent.hold(reason=f"Unknown phase: {self._phase}")

    # -- Phase handlers --------------------------------------------------------

    def _do_bridge_in(self, market: MarketSnapshot) -> Intent:
        logger.info(f"BRIDGE_IN: bridge ${self.budget_usd} USDC Base->{self.target_chain}")
        self._advance("WAIT_BRIDGE")
        return Intent.bridge(
            token=self.relay_token, amount=self.budget_usd,
            from_chain="base", to_chain=self.target_chain, max_slippage=self.max_slippage,
        )

    def _do_wait_bridge(self, market: MarketSnapshot) -> Intent:
        avax_usdc = self._bal_usd(market, self.relay_token, self.target_chain)
        needed = self.swap_wavax_usd + self.lp_usdc  # $4 minimum
        if avax_usdc >= needed:
            logger.info(f"WAIT_BRIDGE: ${avax_usdc} USDC on {self.target_chain}. Advancing to SWAP_IN.")
            self._advance("SWAP_IN")
            return Intent.hold(reason="Bridge landed, advancing to SWAP_IN")

        logger.info(f"WAIT_BRIDGE: waiting for USDC (${avax_usdc} < ${needed})")
        return Intent.hold(reason=f"Waiting for bridged USDC (${avax_usdc})")

    def _do_swap_in(self, market: MarketSnapshot) -> Intent:
        logger.info(f"SWAP_IN: swap ${self.swap_wavax_usd} USDC->WAVAX via Enso")
        self._advance("OPEN")
        return Intent.swap(
            from_token="USDC", to_token="WAVAX", amount_usd=self.swap_wavax_usd,
            max_slippage=self.max_slippage, protocol="enso", chain=self.target_chain,
        )

    def _do_open(self, market: MarketSnapshot) -> Intent:
        # Cap WAVAX to what we expect from the swap ($swap_wavax_usd worth)
        # Use price to convert USD -> WAVAX amount
        wavax_raw = self._bal_raw(market, self.lp_token0, self.target_chain)
        if wavax_raw <= Decimal("0"):
            logger.error("OPEN: no WAVAX balance, skipping to CLOSE")
            self._advance("CLOSE")
            return Intent.hold(reason="No WAVAX for LP")

        try:
            wavax_price = market.price(self.lp_token0, chain=self.target_chain)
            if wavax_price and Decimal(str(wavax_price)) > 0:
                max_wavax = self.swap_wavax_usd / Decimal(str(wavax_price))
                if wavax_raw > max_wavax:
                    logger.info(f"OPEN: capping WAVAX from {wavax_raw} to {max_wavax} (${self.swap_wavax_usd} worth)")
                    wavax_raw = max_wavax
        except Exception as e:
            logger.warning(f"OPEN: could not cap WAVAX amount: {e}")

        # Price range: +/- 10% around current price (or fallback)
        try:
            token0_price = market.price(self.lp_token0, chain=self.target_chain)
            current_price = float(token0_price) if token0_price else None
        except Exception:
            current_price = None

        if current_price:
            range_lower = Decimal(str(current_price * 0.90))
            range_upper = Decimal(str(current_price * 1.10))
        else:
            range_lower = Decimal("18")
            range_upper = Decimal("28")

        logger.info(
            f"OPEN: LP_OPEN {self.pool} dual-sided: {wavax_raw} WAVAX + ${self.lp_usdc} USDC, "
            f"range [{range_lower:.2f}, {range_upper:.2f}]"
        )
        self._advance("CLOSE")
        return Intent.lp_open(
            pool=self.pool,
            amount0=wavax_raw,
            amount1=self.lp_usdc,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol=self.protocol,
            chain=self.target_chain,
            protocol_params={"id_slippage": 20},
        )

    def _do_close(self, market: MarketSnapshot) -> Intent:
        logger.info(f"CLOSE: LP_CLOSE position_id={self._position_id or 'auto'}")
        self._advance("SWAP_OUT")
        return Intent.lp_close(
            position_id=self._position_id or self.pool,
            pool=self.pool,
            collect_fees=True,
            protocol=self.protocol,
            chain=self.target_chain,
        )

    def _do_swap_out(self, market: MarketSnapshot) -> Intent:
        wavax_raw = self._bal_raw(market, self.lp_token0, self.target_chain)
        if wavax_raw <= Decimal("0.0001"):
            logger.info("SWAP_OUT: no WAVAX, skipping to BRIDGE_OUT")
            self._advance("BRIDGE_OUT")
            return Intent.hold(reason="No WAVAX to swap")

        logger.info(f"SWAP_OUT: swap {wavax_raw} WAVAX->USDC via Enso")
        self._advance("BRIDGE_OUT")
        return Intent.swap(
            from_token="WAVAX", to_token="USDC", amount=wavax_raw,
            max_slippage=self.max_slippage, protocol="enso", chain=self.target_chain,
        )

    def _do_bridge_out(self, market: MarketSnapshot) -> Intent:
        usdc_raw = self._bal_raw(market, self.relay_token, self.target_chain)
        # Bridge back whatever we have, leaving $0.01 buffer for rounding
        bridge_amount = usdc_raw - Decimal("0.01") if usdc_raw > Decimal("0.01") else usdc_raw
        logger.info(f"BRIDGE_OUT: bridge ${bridge_amount} USDC {self.target_chain}->Base")
        self._advance("DONE")
        return Intent.bridge(
            token=self.relay_token, amount=bridge_amount,
            from_chain=self.target_chain, to_chain="base", max_slippage=self.max_slippage,
        )

    def _do_done(self, market: MarketSnapshot) -> Intent:
        end = self._bal_usd(market, self.relay_token, "base")
        if self._start_usdc_balance and end:
            loss = self._start_usdc_balance - end
            pct = (loss / self._start_usdc_balance) * Decimal("100")
            logger.info(f"BUDGET: started=${self._start_usdc_balance}, ended=${end}, loss=${loss} ({pct:.1f}%)")

        verdict = "PASS" if (self._open_success and self._close_success and not self._had_failures) else "FAIL"
        log = logger.info if verdict == "PASS" else logger.error
        log(f"PROBE RESULT: {verdict} - open={'PASS' if self._open_success else 'FAIL'}, close={'PASS' if self._close_success else 'FAIL'}")
        return Intent.hold(reason="Probe complete")

    # -- Callback (logging + position_id capture) ------------------------------

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        it = str(getattr(intent, "intent_type", "?"))
        proto = getattr(intent, "protocol", "?")
        if success:
            logger.info(f"  [cb] {proto} {it} OK")
            if "LP_OPEN" in it:
                self._open_success = True
                if result:
                    pid = getattr(result, "position_id", None)
                    if pid:
                        self._position_id = str(pid)
                        logger.info(f"  [cb] captured position_id={self._position_id}")
            elif "LP_CLOSE" in it:
                self._close_success = True
        else:
            self._had_failures = True
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
                position_type=PositionType.LP, position_id=self._position_id or self.pool,
                chain=self.target_chain, protocol=self.protocol,
                value_usd=self.swap_wavax_usd + self.lp_usdc,
                details={"pool": self.pool},
            ))
        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "probe_avalanche_lp_lifecycle"),
            timestamp=datetime.now(UTC), positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None):
        from almanak.framework.teardown import TeardownMode
        slippage = Decimal("0.03") if mode == TeardownMode.HARD else self.max_slippage
        intents = []
        if self._open_success and not self._close_success:
            intents.append(Intent.lp_close(
                position_id=self._position_id or self.pool, pool=self.pool,
                collect_fees=True, protocol=self.protocol, chain=self.target_chain,
            ))
        intents.append(Intent.swap(
            from_token="WAVAX", to_token="USDC", amount="all",
            max_slippage=slippage, protocol="enso", chain=self.target_chain,
        ))
        intents.append(Intent.bridge(
            token=self.relay_token, amount="all",
            from_chain=self.target_chain, to_chain="base", max_slippage=slippage,
        ))
        return intents
