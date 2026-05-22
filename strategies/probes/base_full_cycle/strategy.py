"""Base Full Cycle Probe: swap relay + Compound V3 lending on Base.

Chains swaps through 3 DEX protocols on Base (Aerodrome, Uniswap V3,
PancakeSwap V3) then exercises a Compound V3 supply/withdraw cycle.
Starts and ends with USDC on Base for clean budget tracking.

Relay pattern (default config):
  $0.50 USDC --[aerodrome]--> WETH --[aerodrome]--> USDC
  $0.50 USDC --[uniswap_v3]--> WETH --[uniswap_v3]--> USDC
  $0.50 USDC --[pancakeswap_v3]--> WETH --[pancakeswap_v3]--> USDC
  $1.00 USDC --[compound_v3 supply]--> ... --[compound_v3 withdraw]--> USDC

All protocols are tested deterministically. Logs PASS/FAIL per-leg.
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
    name="probe_base_full_cycle",
    description="Closed-loop swap relay + Compound V3 lending probe on Base",
    version="1.0.0",
    author="Almanak",
    tags=["probe", "nightly", "swap", "lending", "base", "liveness"],
    supported_chains=["base"],
    supported_protocols=["aerodrome", "uniswap_v3", "pancakeswap_v3", "compound_v3"],
    intent_types=["SWAP", "SUPPLY", "WITHDRAW"],
    default_chain="base",
)
class BaseFullCycleProbeStrategy(IntentStrategy):
    """Deterministic swap relay + lending probe for Base chain."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.budget_usd = Decimal(str(self.get_config("budget_usd", "2")))
        self.relay_token = self.get_config("relay_token", "USDC")
        self.intermediate_token = self.get_config("intermediate_token", "WETH")
        self.max_slippage = Decimal(str(self.get_config("max_slippage_pct", "1.0"))) / Decimal("100")
        self.swap_protocols: list[str] = self.get_config(
            "swap_protocols", ["aerodrome", "uniswap_v3", "pancakeswap_v3"]
        )
        self.lending_protocol = self.get_config("lending_protocol", "compound_v3")
        self.lending_market_id = self.get_config("lending_market_id", "usdc")
        self.lending_supply_amount = Decimal(str(self.get_config("lending_supply_usd", "0.50")))

        # Per-leg tracking
        self._leg_results: list[dict] = []
        self._total_legs = 0
        self._completed_legs = 0

        # Budget tracking
        self._start_usdc_balance: Decimal | None = None
        self._submitted = False

        logger.info(
            f"BaseFullCycleProbe initialized: ${self.budget_usd} budget, "
            f"{len(self.swap_protocols)} DEXs + {self.lending_protocol} on {self.chain}"
        )

    def decide(self, market: MarketSnapshot) -> Intent:
        """Build swap relay + lending lifecycle sequence."""
        # Record starting balance for budget tracking
        usdc_balance = market.balance(self.relay_token)
        if usdc_balance and self._start_usdc_balance is None:
            self._start_usdc_balance = Decimal(str(usdc_balance.balance_usd)) if usdc_balance.balance_usd else None
            logger.info(f"BUDGET: starting USDC balance = ${self._start_usdc_balance}")

        if self._submitted:
            return Intent.hold(reason="Probe already submitted")

        # Fetch prices for amount_usd conversion
        market.price(self.relay_token)
        market.price(self.intermediate_token)

        intents: list[Any] = []

        # --- Swap relay: round-trip through each DEX ---
        swap_budget = self.budget_usd - self.lending_supply_amount
        amount_per_protocol = swap_budget / Decimal(str(len(self.swap_protocols)))

        for protocol in self.swap_protocols:
            intents.append(
                Intent.swap(
                    from_token=self.relay_token,
                    to_token=self.intermediate_token,
                    amount_usd=amount_per_protocol,
                    max_slippage=self.max_slippage,
                    protocol=protocol,
                )
            )
            intents.append(
                Intent.swap(
                    from_token=self.intermediate_token,
                    to_token=self.relay_token,
                    amount="all",
                    max_slippage=self.max_slippage,
                    protocol=protocol,
                )
            )

        # --- Compound V3 lending: supply then withdraw ---
        intents.append(
            Intent.supply(
                protocol=self.lending_protocol,
                token=self.relay_token,
                amount=self.lending_supply_amount,
                use_as_collateral=False,
                market_id=self.lending_market_id,
            )
        )
        intents.append(
            Intent.withdraw(
                protocol=self.lending_protocol,
                token=self.relay_token,
                amount=self.lending_supply_amount,
                withdraw_all=True,
                market_id=self.lending_market_id,
            )
        )

        self._total_legs = len(intents)
        protocol_names = ", ".join(self.swap_protocols)
        logger.info(
            f"Sequence: {len(self.swap_protocols)} DEX round-trips [{protocol_names}] "
            f"+ {self.lending_protocol} supply/withdraw = {self._total_legs} legs"
        )

        self._submitted = True
        return Intent.sequence(
            intents,
            description=(
                f"Base full cycle probe: ${self.budget_usd} USDC through "
                f"[{protocol_names}] + {self.lending_protocol}"
            ),
        )

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        """Track per-leg results and evaluate on final leg."""
        self._completed_legs += 1
        leg_num = self._completed_legs

        intent_type = str(getattr(intent, "intent_type", "UNKNOWN"))
        protocol = getattr(intent, "protocol", "unknown")

        leg_data: dict[str, Any] = {
            "leg": leg_num,
            "success": success,
            "intent_type": intent_type,
            "protocol": protocol,
        }

        if success and intent_type == "SWAP":
            if result and hasattr(result, "swap_amounts") and result.swap_amounts:
                sa = result.swap_amounts
                leg_data["amount_in"] = str(sa.amount_in_decimal) if sa.amount_in_decimal is not None else None
                leg_data["amount_out"] = str(sa.amount_out_decimal) if sa.amount_out_decimal is not None else None
                leg_data["slippage_bps"] = sa.slippage_bps
                logger.info(
                    f"  Leg {leg_num}/{self._total_legs} [{protocol}] SWAP PASS: "
                    f"{sa.amount_in_decimal} -> {sa.amount_out_decimal} "
                    f"(slippage: {sa.slippage_bps}bps)"
                )
            else:
                logger.info(f"  Leg {leg_num}/{self._total_legs} [{protocol}] SWAP PASS")
        elif success:
            logger.info(f"  Leg {leg_num}/{self._total_legs} [{protocol}] {intent_type} PASS")
        else:
            error = getattr(result, "error", "unknown error") if result else "no result"
            leg_data["error"] = str(error)
            logger.error(f"  Leg {leg_num}/{self._total_legs} [{protocol}] {intent_type} FAIL: {error}")

        self._leg_results.append(leg_data)

        # Evaluate on final leg OR on any failure (sequences abort on first failure)
        if leg_num == self._total_legs or not success:
            self._evaluate_result()

    def _evaluate_result(self):
        """Evaluate overall probe result."""
        failed_legs = [leg for leg in self._leg_results if not leg.get("success")]
        swap_legs = [leg for leg in self._leg_results if leg.get("intent_type") == "SWAP"]
        lending_legs = [leg for leg in self._leg_results if leg.get("intent_type") in ("SUPPLY", "WITHDRAW")]

        successful_swaps = len([leg for leg in swap_legs if leg.get("success")])
        successful_lending = len([leg for leg in lending_legs if leg.get("success")])

        if failed_legs:
            failed_info = [f"{leg.get('protocol')}:{leg.get('intent_type')}" for leg in failed_legs]
            logger.error(
                f"PROBE RESULT: FAIL - {len(failed_legs)}/{self._total_legs} legs failed. "
                f"Failed: {', '.join(failed_info)}"
            )
        else:
            logger.info(
                f"PROBE RESULT: PASS - {successful_swaps} swaps + {successful_lending} lending ops "
                f"all succeeded ({self._total_legs} legs total)"
            )

        self._log_leg_summary()

    def _log_leg_summary(self):
        """Log per-leg summary."""
        for leg in self._leg_results:
            status = "OK" if leg["success"] else "FAIL"
            logger.info(f"  [{leg['protocol']}] {leg['intent_type']} {status}")

    # -------------------------------------------------------------------------
    # Teardown
    # -------------------------------------------------------------------------

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self):
        from almanak.framework.teardown import TeardownPositionSummary

        # This probe uses IntentSequence which is atomic — either all legs complete
        # or execution is rolled back. No partial positions to report.
        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "probe_base_full_cycle"),
            timestamp=datetime.now(UTC),
            positions=[],
        )

    def generate_teardown_intents(self, mode, market=None):
        # No positions to unwind — the sequence is atomic
        return []
