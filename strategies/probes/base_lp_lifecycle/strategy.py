"""LP Lifecycle Probe: closed-loop LP open/close test for Base chain.

Tests the full LP lifecycle on a single protocol:
  Phase 1 (OPEN):  Open LP position with WETH + USDC
  Phase 2 (CLOSE): Close LP position using extracted position_id
  Phase 3 (DONE):  Evaluate PASS/FAIL

Runs with --interval (needs 2 ticks minimum: open, then close).
The probe is closed-loop: wallet starts with WETH+USDC and ends
with WETH+USDC (minus slippage/fees from the LP mint/burn).
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
    name="probe_base_lp_lifecycle",
    description="Closed-loop LP open/close lifecycle probe for Base chain",
    version="1.0.0",
    author="Almanak",
    tags=["probe", "nightly", "lp", "base", "lifecycle"],
    supported_chains=["base"],
    supported_protocols=["uniswap_v3", "pancakeswap_v3", "aerodrome"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="base",
)
class LPLifecycleProbeStrategy(IntentStrategy):
    """Deterministic LP lifecycle probe for nightly on-chain validation."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Config
        self.pool = self.get_config("pool", "WETH/USDC/500")
        self.amount0 = Decimal(str(self.get_config("amount0", "0.0004")))
        self.amount1 = Decimal(str(self.get_config("amount1", "0.75")))
        self.range_width_pct = Decimal(str(self.get_config("range_width_pct", "0.50")))
        self.protocol = self.get_config("protocol", "uniswap_v3")
        self.max_slippage = Decimal(str(self.get_config("max_slippage_pct", "1.0"))) / Decimal("100")

        # Parse pool tokens
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WETH"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"

        # State machine
        self._position_id: str | None = None
        self._phase: str = "OPEN"
        self._open_success = False
        self._close_success = False
        self._load_from_state()

        logger.info(
            f"LPLifecycleProbe initialized: {self.protocol} on {self.chain}, "
            f"pool={self.pool}, amounts={self.amount0} {self.token0_symbol} + "
            f"{self.amount1} {self.token1_symbol}, phase={self._phase}"
        )

    # -------------------------------------------------------------------------
    # State persistence
    # -------------------------------------------------------------------------

    def _load_from_state(self):
        state = self.get_persistent_state()
        if state:
            self._phase = state.get("phase", "OPEN")
            raw_pid = state.get("position_id")
            self._position_id = str(raw_pid) if raw_pid is not None else None
            self._open_success = state.get("open_success", False)
            self._close_success = state.get("close_success", False)

    def get_persistent_state(self) -> dict[str, Any]:
        state = super().get_persistent_state() if hasattr(super(), "get_persistent_state") else {}
        state["phase"] = self._phase
        state["position_id"] = self._position_id
        state["open_success"] = self._open_success
        state["close_success"] = self._close_success
        return state

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if hasattr(super(), "load_persistent_state"):
            super().load_persistent_state(state)
        self._phase = state.get("phase", "OPEN")
        raw_pid = state.get("position_id")
        self._position_id = str(raw_pid) if raw_pid is not None else None
        self._open_success = state.get("open_success", False)
        self._close_success = state.get("close_success", False)

    # -------------------------------------------------------------------------
    # Decision logic
    # -------------------------------------------------------------------------

    def decide(self, market: MarketSnapshot) -> Intent | None:
        if self._phase == "OPEN":
            return self._do_open(market)
        elif self._phase == "CLOSE":
            return self._do_close()
        elif self._phase == "DONE":
            self._evaluate_result()
            return Intent.hold(reason="LP lifecycle probe complete")
        else:
            return Intent.hold(reason=f"Unknown phase: {self._phase}")

    def _do_open(self, market: MarketSnapshot) -> Intent:
        """Open LP position with configured amounts and a wide range."""
        # Get current price for range calculation
        try:
            token0_price = market.price(self.token0_symbol)
            token1_price = market.price(self.token1_symbol)
            current_price = token0_price / token1_price
        except (ValueError, KeyError):
            current_price = Decimal("2500")  # fallback

        half_width = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        logger.info(
            f"Phase OPEN: {self.amount0} {self.token0_symbol} + "
            f"{self.amount1} {self.token1_symbol} on {self.protocol}, "
            f"range [{range_lower:.0f} - {range_upper:.0f}]"
        )

        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol=self.protocol,
        )

    def _do_close(self) -> Intent:
        """Close LP position using stored position_id."""
        if not self._position_id:
            logger.error("Phase CLOSE but no position_id -- cannot close")
            self._phase = "DONE"
            return Intent.hold(reason="PROBE_FAIL: no position_id for close")

        logger.info(f"Phase CLOSE: closing position {self._position_id} on {self.protocol}")

        return Intent.lp_close(
            position_id=self._position_id,
            pool=self.pool,
            collect_fees=True,
            protocol=self.protocol,
        )

    # -------------------------------------------------------------------------
    # Execution callback
    # -------------------------------------------------------------------------

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        intent_type = getattr(intent, "intent_type", None)
        intent_type_value = intent_type.value if intent_type else "?"

        if intent_type_value == "LP_OPEN":
            if success:
                position_id = result.position_id if result else None
                if position_id:
                    self._position_id = str(position_id)
                    self._open_success = True
                    self._phase = "CLOSE"
                    logger.info(
                        f"  LP OPEN PASS: position_id={position_id} on {self.protocol}"
                    )
                else:
                    self._open_success = False
                    self._phase = "DONE"
                    logger.error(
                        f"  LP OPEN FAIL: executed but no position_id extracted ({self.protocol})"
                    )
            else:
                self._open_success = False
                self._phase = "DONE"
                error = getattr(result, "error", "unknown") if result else "no result"
                logger.error(f"  LP OPEN FAIL: {error}")

        elif intent_type_value == "LP_CLOSE":
            if success:
                self._close_success = True
                self._phase = "DONE"
                logger.info(
                    f"  LP CLOSE PASS: position {self._position_id} closed on {self.protocol}"
                )
            else:
                self._close_success = False
                self._phase = "DONE"
                error = getattr(result, "error", "unknown") if result else "no result"
                logger.error(f"  LP CLOSE FAIL: {error}")

    # -------------------------------------------------------------------------
    # Evaluation
    # -------------------------------------------------------------------------

    def _evaluate_result(self):
        """Log PASS/FAIL for the full lifecycle."""
        if self._open_success and self._close_success:
            logger.info(
                f"LP LIFECYCLE RESULT: PASS - "
                f"open + close succeeded on {self.protocol} "
                f"(position_id={self._position_id})"
            )
        else:
            open_status = "OK" if self._open_success else "FAIL"
            close_status = "OK" if self._close_success else "FAIL"
            logger.error(
                f"LP LIFECYCLE RESULT: FAIL - "
                f"open={open_status}, close={close_status} "
                f"on {self.protocol}"
            )

    # -------------------------------------------------------------------------
    # Teardown -- handles mid-lifecycle failures
    # -------------------------------------------------------------------------

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self):
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._position_id and self._open_success and not self._close_success:
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=str(self._position_id),
                    chain=self.chain,
                    protocol=self.protocol,
                    value_usd=self.amount1,
                    details={"pool": self.pool},
                )
            )
        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "probe_base_lp_lifecycle"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None):
        if not self._position_id or not self._open_success or self._close_success:
            return []
        return [
            Intent.lp_close(
                position_id=self._position_id,
                pool=self.pool,
                collect_fees=True,
                protocol=self.protocol,
            )
        ]
