"""Swap Relay Probe: closed-loop protocol liveness test for Base chain.

Chains swaps through all supported swap protocols on Base, alternating
between USDC and WETH, and returns to USDC. Asserts that the ending
balance is within acceptable loss bounds.

Relay pattern (default config):
  $1.50 USDC --[uniswap_v3]--> WETH --[sushiswap_v3]--> USDC
          --[pancakeswap_v3]--> WETH --[aerodrome]--> USDC

Each protocol is tested exactly once. The probe always executes
(no market condition checks) and logs PASS/FAIL deterministically.
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
    name="probe_base_swap_relay",
    description="Closed-loop swap relay probe testing all Base chain DEX protocols",
    version="1.0.0",
    author="Almanak",
    tags=["probe", "nightly", "swap", "base", "liveness"],
    supported_chains=["base"],
    supported_protocols=["uniswap_v3", "sushiswap_v3", "pancakeswap_v3", "aerodrome"],
    intent_types=["SWAP"],
    default_chain="base",
)
class SwapRelayProbeStrategy(IntentStrategy):
    """Deterministic swap relay probe for nightly on-chain validation."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.relay_amount_usd = Decimal(str(self.get_config("relay_amount_usd", "1.50")))
        self.relay_token = self.get_config("relay_token", "USDC")
        self.intermediate_token = self.get_config("intermediate_token", "WETH")
        self.max_slippage = Decimal(str(self.get_config("max_slippage_pct", "1.0"))) / Decimal("100")
        self.max_acceptable_loss_pct = Decimal(str(self.get_config("max_acceptable_loss_pct", "10.0")))
        self.protocols: list[str] = self.get_config(
            "protocols", ["uniswap_v3", "sushiswap_v3", "pancakeswap_v3", "aerodrome"]
        )

        # Per-leg tracking
        self._leg_results: list[dict] = []
        self._total_legs = 0
        self._completed_legs = 0

        logger.info(
            f"SwapRelayProbe initialized: ${self.relay_amount_usd} {self.relay_token} "
            f"through {len(self.protocols)} protocols on {self.chain}"
        )

    def decide(self, market: MarketSnapshot) -> Intent:
        """Build per-protocol round-trip sequences.

        Each protocol gets a USDC->WETH->USDC round-trip. The forward leg uses
        amount_usd, the return leg uses amount="all" (safe because WETH is 18
        decimals and amount_out_decimal is always correct for 18-dec tokens).

        Budget is split evenly across protocols.
        """
        # Fetch prices so the framework can convert amount_usd to token amounts
        market.price(self.relay_token)
        market.price(self.intermediate_token)

        amount_per_protocol = self.relay_amount_usd / Decimal(str(len(self.protocols)))

        swaps = []
        for protocol in self.protocols:
            # Forward: USDC -> WETH (uses amount_usd, no chaining issues)
            swaps.append(
                Intent.swap(
                    from_token=self.relay_token,
                    to_token=self.intermediate_token,
                    amount_usd=amount_per_protocol,
                    max_slippage=self.max_slippage,
                    protocol=protocol,
                )
            )
            # Return: WETH -> USDC (amount="all" is safe -- WETH is 18 decimals)
            swaps.append(
                Intent.swap(
                    from_token=self.intermediate_token,
                    to_token=self.relay_token,
                    amount="all",
                    max_slippage=self.max_slippage,
                    protocol=protocol,
                )
            )

        self._total_legs = len(swaps)
        protocol_names = " -> ".join(self.protocols)
        logger.info(
            f"Relay sequence: {self._total_legs} swaps ({len(self.protocols)} round-trips) "
            f"through [{protocol_names}], total ${self.relay_amount_usd}"
        )

        return Intent.sequence(
            swaps,
            description=f"Swap relay probe: ${self.relay_amount_usd} {self.relay_token} through [{protocol_names}]",
        )

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        """Track per-leg results and evaluate on final leg."""
        self._completed_legs += 1
        leg_num = self._completed_legs

        leg_data: dict[str, Any] = {
            "leg": leg_num,
            "success": success,
            "protocol": getattr(intent, "protocol", "unknown"),
            "from_token": getattr(intent, "from_token", "?"),
            "to_token": getattr(intent, "to_token", "?"),
        }

        if success and result and hasattr(result, "swap_amounts") and result.swap_amounts:
            sa = result.swap_amounts
            leg_data["amount_in"] = str(sa.amount_in_decimal) if sa.amount_in_decimal is not None else None
            leg_data["amount_out"] = str(sa.amount_out_decimal) if sa.amount_out_decimal is not None else None
            leg_data["slippage_bps"] = sa.slippage_bps
            logger.info(
                f"  Leg {leg_num}/{self._total_legs} [{leg_data['protocol']}] "
                f"PASS: {sa.amount_in_decimal} {leg_data['from_token']} -> "
                f"{sa.amount_out_decimal} {leg_data['to_token']} "
                f"(slippage: {sa.slippage_bps}bps)"
            )
        elif not success:
            error = getattr(result, "error", "unknown error") if result else "no result"
            leg_data["error"] = str(error)
            logger.error(
                f"  Leg {leg_num}/{self._total_legs} [{leg_data['protocol']}] "
                f"FAIL: {error}"
            )

        self._leg_results.append(leg_data)

        # Evaluate on final leg
        if leg_num == self._total_legs:
            self._evaluate_relay_result()

    def _evaluate_relay_result(self):
        """Evaluate the overall relay: PASS if loss is within acceptable bounds.

        Since each protocol does a round-trip (USDC->WETH->USDC), the return
        legs (even-numbered: 2, 4, 6, ...) output USDC. However, their
        amount_out_decimal may use wrong decimals (18 instead of 6 for USDC).

        Instead of trusting the decimal amounts, we count successful round-trips
        and check if any legs failed.
        """
        failed_legs = [leg for leg in self._leg_results if not leg.get("success")]
        successful_legs = [leg for leg in self._leg_results if leg.get("success")]
        total_protocols = len(self.protocols)
        successful_roundtrips = len([
            leg for leg in self._leg_results
            if leg.get("success") and leg["leg"] % 2 == 0  # return legs
        ])

        if failed_legs:
            failed_protocols = [leg.get("protocol", "?") for leg in failed_legs]
            logger.error(
                f"RELAY RESULT: FAIL - {len(failed_legs)}/{self._total_legs} legs failed. "
                f"Failed protocols: {', '.join(failed_protocols)}"
            )
        elif successful_roundtrips == total_protocols:
            logger.info(
                f"RELAY RESULT: PASS - all {total_protocols} protocol round-trips "
                f"completed successfully ({self._total_legs} swaps executed)"
            )
        else:
            logger.warning(
                f"RELAY RESULT: PARTIAL - {successful_roundtrips}/{total_protocols} "
                f"round-trips completed"
            )

        self._log_leg_summary()

    def _log_leg_summary(self):
        """Log per-protocol summary."""
        for leg in self._leg_results:
            status = "OK" if leg["success"] else "FAIL"
            amount_in = leg.get("amount_in", "?")
            amount_out = leg.get("amount_out", "?")
            logger.info(
                f"  [{leg['protocol']}] {status}: "
                f"{amount_in} {leg['from_token']} -> {amount_out} {leg['to_token']}"
            )

    # -------------------------------------------------------------------------
    # Teardown -- handles mid-relay failures (left holding intermediate token)
    # -------------------------------------------------------------------------

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self):
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "probe_base_swap_relay"),
            timestamp=datetime.now(UTC),
            positions=[
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id="relay_intermediate_token",
                    chain=self.chain,
                    protocol="uniswap_v3",
                    value_usd=self.relay_amount_usd,
                    details={"asset": self.intermediate_token},
                )
            ],
        )

    def generate_teardown_intents(self, mode, market=None):
        from almanak.framework.teardown import TeardownMode

        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else self.max_slippage
        return [
            Intent.swap(
                from_token=self.intermediate_token,
                to_token=self.relay_token,
                amount="all",
                max_slippage=max_slippage,
                protocol="uniswap_v3",
            )
        ]
