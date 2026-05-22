"""Arbitrum Full Cycle Probe: bridge + swap relay + Aave V3 lending.

Starts with USDC on Base, bridges to Arbitrum, runs a DEX swap relay
through 3 protocols, exercises a full Aave V3 lending lifecycle
(supply WETH, borrow USDC, repay, withdraw), then bridges back to
Base USDC.

Phase flow:
  BRIDGE_IN  -> Bridge USDC Base->Arbitrum
  EXECUTE    -> Swap relay (3 DEXs) + Aave V3 supply/borrow/repay/withdraw
  BRIDGE_OUT -> Swap remaining to USDC, bridge Arbitrum->Base
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
    name="probe_arbitrum_full_cycle",
    description="Bridge + swap relay + Aave V3 lending probe on Arbitrum",
    version="1.0.0",
    author="Almanak",
    tags=["probe", "nightly", "swap", "lending", "bridge", "arbitrum", "liveness"],
    supported_chains=["base", "arbitrum"],
    supported_protocols=["uniswap_v3", "pancakeswap_v3", "aave_v3"],
    intent_types=["BRIDGE", "SWAP", "SUPPLY", "BORROW", "REPAY", "WITHDRAW"],
    default_chain="base",
)
class ArbitrumFullCycleProbeStrategy(IntentStrategy):
    """Deterministic bridge + swap + lending probe for Arbitrum."""

    SUPPORTED_CHAINS = ["base", "arbitrum"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.target_chain = self.get_config("target_chain", "arbitrum")
        self.budget_usd = Decimal(str(self.get_config("budget_usd", "2")))
        self.relay_token = self.get_config("relay_token", "USDC")
        self.intermediate_token = self.get_config("intermediate_token", "WETH")
        self.max_slippage = Decimal(str(self.get_config("max_slippage_pct", "1.0"))) / Decimal("100")
        self.swap_protocols: list[str] = self.get_config(
            "swap_protocols", ["uniswap_v3", "sushiswap_v3", "pancakeswap_v3"]
        )
        self.swap_amount_per_protocol = Decimal(str(self.get_config("swap_amount_per_protocol_usd", "0.25")))
        self.lending_protocol = self.get_config("lending_protocol", "aave_v3")
        self.lending_supply_amount = Decimal(str(self.get_config("lending_supply_amount", "0.0002")))
        self.lending_borrow_usd = Decimal(str(self.get_config("lending_borrow_usd", "0.15")))

        # State machine
        self._phase = "BRIDGE_IN"
        self._leg_results: list[dict] = []
        self._had_failures = False
        self._total_legs = 0
        self._completed_legs = 0
        self._start_usdc_balance: Decimal | None = None
        self._load_from_state()

        logger.info(
            f"ArbitrumFullCycleProbe initialized: ${self.budget_usd} budget, "
            f"phase={self._phase}, target={self.target_chain}"
        )

    # -------------------------------------------------------------------------
    # State persistence
    # -------------------------------------------------------------------------

    def _load_from_state(self):
        state = self.get_persistent_state()
        if state:
            self._phase = state.get("phase", "BRIDGE_IN")
            self._had_failures = state.get("had_failures", False)
            self._total_legs = state.get("total_legs", 0)
            self._completed_legs = state.get("completed_legs", 0)
            self._start_usdc_balance = (
                Decimal(str(state["start_usdc_balance"])) if state.get("start_usdc_balance") else None
            )

    def get_persistent_state(self) -> dict[str, Any]:
        state = super().get_persistent_state() if hasattr(super(), "get_persistent_state") else {}
        state["phase"] = self._phase
        state["had_failures"] = self._had_failures
        state["total_legs"] = self._total_legs
        state["completed_legs"] = self._completed_legs
        state["start_usdc_balance"] = str(self._start_usdc_balance) if self._start_usdc_balance else None
        return state

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if hasattr(super(), "load_persistent_state"):
            super().load_persistent_state(state)
        self._phase = state.get("phase", "BRIDGE_IN")
        self._had_failures = state.get("had_failures", False)
        self._total_legs = state.get("total_legs", 0)
        self._completed_legs = state.get("completed_legs", 0)
        self._start_usdc_balance = (
            Decimal(str(state["start_usdc_balance"])) if state.get("start_usdc_balance") else None
        )

    # -------------------------------------------------------------------------
    # Decision logic
    # -------------------------------------------------------------------------

    def decide(self, market: MarketSnapshot) -> Intent:
        """Phase-driven decision."""
        # Record starting balance on first tick
        if self._start_usdc_balance is None:
            usdc_balance = market.balance(self.relay_token, "base")
            if usdc_balance and usdc_balance.balance_usd:
                self._start_usdc_balance = Decimal(str(usdc_balance.balance_usd))
                logger.info(f"BUDGET: starting USDC balance = ${self._start_usdc_balance}")

        if self._phase == "BRIDGE_IN":
            # Auto-detect if bridge already landed (handles callback-miss after retries)
            try:
                target_balance = market.balance(self.relay_token, self.target_chain)
                if target_balance and target_balance.balance_usd and Decimal(str(target_balance.balance_usd)) > Decimal("1"):
                    logger.info(
                        f"BRIDGE_IN auto-advance: detected ${target_balance.balance_usd} USDC on "
                        f"{self.target_chain}, skipping to EXECUTE"
                    )
                    self._phase = "EXECUTE"
                    return self._do_execute_safely(market)
            except Exception as e:
                logger.warning(f"BRIDGE_IN auto-advance check failed: {e}")
            return self._do_bridge_in(market)
        elif self._phase == "EXECUTE":
            return self._do_execute_safely(market)
        elif self._phase == "BRIDGE_OUT":
            return self._do_bridge_out()
        elif self._phase == "DONE":
            return self._do_done(market)
        else:
            logger.error(f"Unknown phase: {self._phase}")
            return Intent.hold(reason=f"Unknown phase: {self._phase}")

    def _do_bridge_in(self, market: MarketSnapshot) -> Intent:
        """Bridge USDC from Base to Arbitrum."""
        # Pre-flight balance check
        base_usdc = market.balance(self.relay_token, "base")
        base_usdc_usd = Decimal(str(base_usdc.balance_usd)) if base_usdc and base_usdc.balance_usd else Decimal("0")
        if base_usdc_usd < self.budget_usd:
            logger.error(f"BRIDGE_IN: insufficient Base USDC (${base_usdc_usd} < ${self.budget_usd})")
            self._phase = "DONE"
            self._had_failures = True
            return Intent.hold(reason=f"PROBE_FAIL: Base USDC ${base_usdc_usd} < ${self.budget_usd} minimum")

        # Check Base ETH for gas
        base_eth = market.balance("ETH", "base")
        base_eth_raw = Decimal(str(base_eth.balance)) if base_eth and base_eth.balance else Decimal("0")
        if base_eth_raw < Decimal("0.0005"):
            logger.error(f"BRIDGE_IN: insufficient Base ETH for gas ({base_eth_raw} < 0.0005)")
            self._phase = "DONE"
            self._had_failures = True
            return Intent.hold(reason=f"PROBE_FAIL: Base ETH {base_eth_raw} < 0.0005 for bridge gas")

        logger.info(f"Phase BRIDGE_IN: bridging ${self.budget_usd} USDC Base -> {self.target_chain}")
        return Intent.bridge(
            token=self.relay_token,
            amount=self.budget_usd,
            from_chain="base",
            to_chain=self.target_chain,
            max_slippage=self.max_slippage,
        )

    def _do_execute_safely(self, market: MarketSnapshot) -> Intent:
        """Wrap _do_execute with failure handling so both EXECUTE entry points behave consistently."""
        try:
            return self._do_execute(market)
        except Exception as e:
            logger.error(f"EXECUTE failed: {e}, recovering via BRIDGE_OUT")
            self._had_failures = True
            self._phase = "BRIDGE_OUT"
            return Intent.hold(reason=f"PROBE_FAIL: execute setup failed ({e})")

    def _do_execute(self, market: MarketSnapshot) -> Intent:
        """Swap relay through 3 DEXs + Aave V3 lending lifecycle."""
        market.price(self.relay_token, self.target_chain)
        market.price(self.intermediate_token, self.target_chain)

        intents: list[Any] = []

        # Swap relay: round-trip through each DEX
        for protocol in self.swap_protocols:
            intents.append(
                Intent.swap(
                    from_token=self.relay_token,
                    to_token=self.intermediate_token,
                    amount_usd=self.swap_amount_per_protocol,
                    max_slippage=self.max_slippage,
                    protocol=protocol,
                    chain=self.target_chain,
                )
            )
            intents.append(
                Intent.swap(
                    from_token=self.intermediate_token,
                    to_token=self.relay_token,
                    amount="all",
                    max_slippage=self.max_slippage,
                    protocol=protocol,
                    chain=self.target_chain,
                )
            )

        # Aave V3 lending lifecycle: swap to WETH, supply, borrow USDC, repay, withdraw, swap back
        intents.append(
            Intent.swap(
                from_token=self.relay_token,
                to_token=self.intermediate_token,
                amount_usd=Decimal("1"),
                max_slippage=self.max_slippage,
                protocol="uniswap_v3",
                chain=self.target_chain,
            )
        )
        intents.append(
            Intent.supply(
                protocol=self.lending_protocol,
                token=self.intermediate_token,
                amount=self.lending_supply_amount,
                use_as_collateral=True,
                chain=self.target_chain,
            )
        )
        intents.append(
            Intent.borrow(
                protocol=self.lending_protocol,
                collateral_token=self.intermediate_token,
                collateral_amount=Decimal("0"),
                borrow_token=self.relay_token,
                borrow_amount=self.lending_borrow_usd,
                interest_rate_mode="variable",
                chain=self.target_chain,
            )
        )
        intents.append(
            Intent.repay(
                protocol=self.lending_protocol,
                token=self.relay_token,
                amount=self.lending_borrow_usd,
                repay_full=True,
                interest_rate_mode="variable",
                chain=self.target_chain,
            )
        )
        intents.append(
            Intent.withdraw(
                protocol=self.lending_protocol,
                token=self.intermediate_token,
                amount=self.lending_supply_amount,
                withdraw_all=True,
                chain=self.target_chain,
            )
        )
        # Swap WETH remainder back to USDC
        intents.append(
            Intent.swap(
                from_token=self.intermediate_token,
                to_token=self.relay_token,
                amount="all",
                max_slippage=self.max_slippage,
                protocol="uniswap_v3",
                chain=self.target_chain,
            )
        )

        self._total_legs = len(intents)
        logger.info(
            f"Phase EXECUTE: {len(self.swap_protocols)} DEX round-trips + "
            f"{self.lending_protocol} lifecycle = {self._total_legs} legs on {self.target_chain}"
        )

        return Intent.sequence(
            intents,
            description=f"Arbitrum full cycle: swaps + {self.lending_protocol} lending",
        )

    def _do_bridge_out(self) -> Intent:
        """Bridge USDC from Arbitrum back to Base."""
        logger.info(f"Phase BRIDGE_OUT: bridging USDC {self.target_chain} -> Base")
        return Intent.bridge(
            token=self.relay_token,
            amount="all",
            from_chain=self.target_chain,
            to_chain="base",
            max_slippage=self.max_slippage,
        )

    def _do_done(self, market: MarketSnapshot) -> Intent:
        """Evaluate and report."""
        usdc_balance = market.balance(self.relay_token, "base")
        end_balance = Decimal(str(usdc_balance.balance_usd)) if usdc_balance and usdc_balance.balance_usd else None

        if self._start_usdc_balance and end_balance:
            loss = self._start_usdc_balance - end_balance
            loss_pct = (loss / self._start_usdc_balance) * Decimal("100")
            logger.info(
                f"BUDGET: started=${self._start_usdc_balance}, ended=${end_balance}, "
                f"loss=${loss} ({loss_pct:.1f}%)"
            )

        failed = [leg for leg in self._leg_results if not leg.get("success")]
        if failed or self._had_failures:
            logger.error(f"PROBE RESULT: FAIL - {len(failed)} legs failed in this run (had_failures={self._had_failures})")
        else:
            logger.info(f"PROBE RESULT: PASS - all phases completed successfully")

        return Intent.hold(reason="Arbitrum full cycle probe complete")

    # -------------------------------------------------------------------------
    # Intent execution callback
    # -------------------------------------------------------------------------

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        """Track results and advance phases."""
        intent_type = str(getattr(intent, "intent_type", "UNKNOWN"))
        protocol = getattr(intent, "protocol", "unknown")

        leg_data: dict[str, Any] = {
            "success": success,
            "intent_type": intent_type,
            "protocol": protocol,
            "phase": self._phase,
        }

        if success:
            logger.info(f"  [{self._phase}] {protocol} {intent_type} PASS")
        else:
            self._had_failures = True
            error = getattr(result, "error", "unknown error") if result else "no result"
            leg_data["error"] = str(error)
            logger.error(f"  [{self._phase}] {protocol} {intent_type} FAIL: {error}")

        self._leg_results.append(leg_data)

        # Phase transitions
        if self._phase == "BRIDGE_IN":
            if success:
                self._phase = "EXECUTE"
                logger.info(f"Bridge to {self.target_chain} succeeded, advancing to EXECUTE")
            else:
                self._phase = "DONE"
                logger.error("Bridge in failed, skipping to DONE")
        elif self._phase == "EXECUTE":
            self._completed_legs += 1
            if not success:
                self._phase = "BRIDGE_OUT"
                logger.error("Execute leg failed, advancing to BRIDGE_OUT to recover funds")
            elif self._completed_legs == self._total_legs:
                self._phase = "BRIDGE_OUT"
                logger.info("Execute phase complete, advancing to BRIDGE_OUT")
        elif self._phase == "BRIDGE_OUT":
            self._phase = "DONE"
            if success:
                logger.info("Bridge back to Base succeeded, advancing to DONE")
            else:
                logger.error("Bridge out failed, advancing to DONE")

    # -------------------------------------------------------------------------
    # Teardown
    # -------------------------------------------------------------------------

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self):
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._phase in ("EXECUTE", "BRIDGE_OUT"):
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="arb_full_cycle_funds",
                    chain=self.target_chain,
                    protocol="uniswap_v3",
                    value_usd=self.budget_usd,
                    details={"asset": self.relay_token, "chain": self.target_chain},
                )
            )
        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "probe_arbitrum_full_cycle"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None):
        from almanak.framework.teardown import TeardownMode

        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else self.max_slippage
        intents = []
        # Swap any intermediate token back to USDC on target chain
        if self._phase in ("EXECUTE", "BRIDGE_OUT"):
            intents.append(
                Intent.swap(
                    from_token=self.intermediate_token,
                    to_token=self.relay_token,
                    amount="all",
                    max_slippage=max_slippage,
                    protocol="uniswap_v3",
                    chain=self.target_chain,
                )
            )
            intents.append(
                Intent.bridge(
                    token=self.relay_token,
                    amount="all",
                    from_chain=self.target_chain,
                    to_chain="base",
                    max_slippage=max_slippage,
                )
            )
        return intents
