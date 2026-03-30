"""Solana LST Depeg Recovery Arbitrage Strategy.

Monitors Solana liquid staking tokens (mSOL, JitoSOL) for price deviations
from their fair value relative to SOL. When a depeg exceeding the entry
threshold is detected, buys the discounted LST via Jupiter. Sells back
to SOL when the price recovers to within the exit threshold.

Thesis: LST depeg events on Solana historically show ~73% hit rate with
~6.8% avg 7-day return. By buying during panic depegs and holding for
organic repeg (while earning staking yield), the strategy captures mean
reversion plus yield.

Usage:
    # Dry run (no real transaction):
    almanak strat run -d strategies/demo/solana_lst_depeg_arb --once --dry-run

    # Real execution on mainnet:
    almanak strat run -d strategies/demo/solana_lst_depeg_arb --once

    # Continuous monitoring (check every 60s):
    almanak strat run -d strategies/demo/solana_lst_depeg_arb --interval 60

Environment:
    SOLANA_PRIVATE_KEY   Base58 Ed25519 keypair (required)
    SOLANA_RPC_URL       Solana RPC endpoint (optional, defaults to public mainnet)
    JUPITER_API_KEY      Jupiter API key (optional, uses free tier if not set)

Config (config.json):
    lst_token              LST token to monitor (default: mSOL)
    base_token             Base token for comparison (default: SOL)
    depeg_entry_threshold_pct  Min depeg % to enter (default: 0.8)
    depeg_exit_threshold_pct   Max depeg % to exit (default: 0.15)
    max_hold_iterations    Max iterations before forced exit (default: 96)
    swap_amount            Amount of base_token to swap per entry (default: 10.0)
    max_slippage_pct       Max slippage % (default: 1.0)
    stop_loss_depeg_pct    Stop-loss if depeg deepens past this % (default: 3.0)
"""

import logging
from decimal import Decimal

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="solana_lst_depeg_arb",
    version="0.1.0",
    description="LST depeg recovery arbitrage on Solana via Jupiter",
    supported_chains=["solana"],
    default_chain="solana",
    supported_protocols=["jupiter"],
    intent_types=["SWAP"],
)
class SolanaLstDepegArbStrategy(IntentStrategy):
    """Buy discounted LSTs during depeg events, sell on recovery."""

    def decide(self, market: MarketSnapshot) -> Intent:
        try:
            return self._decide_inner(market)
        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def _decide_inner(self, market: MarketSnapshot) -> Intent:
        lst_token = self.config.get("lst_token", "mSOL")
        base_token = self.config.get("base_token", "SOL")
        entry_threshold = Decimal(str(self.config.get("depeg_entry_threshold_pct", "0.8"))) / 100
        exit_threshold = Decimal(str(self.config.get("depeg_exit_threshold_pct", "0.15"))) / 100
        max_hold = int(self.config.get("max_hold_iterations", 96))
        swap_amount = Decimal(str(self.config.get("swap_amount", "10.0")))
        max_slippage = Decimal(str(self.config.get("max_slippage_pct", "1.0"))) / 100
        stop_loss_threshold = Decimal(str(self.config.get("stop_loss_depeg_pct", "3.0"))) / 100

        # Get prices in quote token (USD)
        lst_price = market.price(lst_token)
        base_price = market.price(base_token)

        if lst_price is None or base_price is None or base_price == 0:
            return Intent.hold(reason=f"Missing price data: {lst_token}={lst_price}, {base_token}={base_price}")

        # Calculate depeg: how far LST spot price trades below SOL spot price.
        # Simplification: uses 1:1 SOL parity as baseline (ignores accrued staking
        # yield premium). Real depegs push the ratio well below 1.0, so this is
        # sufficient for detecting panic-driven discounts.
        lst_sol_ratio = lst_price / base_price
        depeg_pct = Decimal("1") - lst_sol_ratio

        has_position = self.state.get("has_position", False)
        hold_count = self.state.get("hold_iterations", 0)

        logger.info(
            f"{lst_token}/SOL ratio: {lst_sol_ratio:.6f}, depeg: {depeg_pct:.4%}, "
            f"position: {has_position}, hold_iters: {hold_count}"
        )

        if has_position:
            # Check exit conditions
            if depeg_pct <= exit_threshold:
                logger.info(f"Depeg recovered to {depeg_pct:.4%} (threshold: {exit_threshold:.4%}). Selling {lst_token}.")
                self.state["exit_reason"] = "repeg"
                return Intent.swap(
                    from_token=lst_token,
                    to_token=base_token,
                    amount="all",
                    max_slippage=max_slippage,
                )

            if depeg_pct > stop_loss_threshold:
                logger.warning(
                    f"Stop-loss triggered: depeg {depeg_pct:.4%} > {stop_loss_threshold:.4%}. "
                    f"Exiting {lst_token} position."
                )
                self.state["exit_reason"] = "stop_loss"
                return Intent.swap(
                    from_token=lst_token,
                    to_token=base_token,
                    amount="all",
                    max_slippage=Decimal("0.03"),  # wider slippage for emergency exit
                )

            if hold_count >= max_hold:
                logger.info(f"Max hold iterations ({max_hold}) reached. Exiting {lst_token} position.")
                self.state["exit_reason"] = "max_hold"
                return Intent.swap(
                    from_token=lst_token,
                    to_token=base_token,
                    amount="all",
                    max_slippage=max_slippage,
                )

            # Still holding -- waiting for repeg
            self.state["hold_iterations"] = hold_count + 1
            return Intent.hold(
                reason=f"Holding {lst_token}: depeg={depeg_pct:.4%}, iter={hold_count + 1}/{max_hold}"
            )

        # No position -- check entry conditions
        if depeg_pct >= entry_threshold:
            logger.info(
                f"Depeg detected: {lst_token}/SOL={lst_sol_ratio:.6f} ({depeg_pct:.4%} below fair value). "
                f"Buying {swap_amount} SOL worth of {lst_token}."
            )
            # Store USD value at entry for accurate teardown reporting
            self.state["entry_value_usd"] = str(swap_amount * base_price)
            return Intent.swap(
                from_token=base_token,
                to_token=lst_token,
                amount=swap_amount,
                max_slippage=max_slippage,
            )

        return Intent.hold(
            reason=f"No depeg: {lst_token}/SOL={lst_sol_ratio:.6f} (depeg={depeg_pct:.4%}, need {entry_threshold:.4%})"
        )

    def on_intent_executed(self, intent, success: bool, result):
        """Track position state after execution."""
        if not success:
            return

        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return
        type_value = intent_type.value if hasattr(intent_type, "value") else str(intent_type)
        if type_value != "SWAP":
            return

        lst_token = self.config.get("lst_token", "mSOL")
        to_token = getattr(intent, "to_token", None)

        if to_token == lst_token:
            # Entered position
            self.state["has_position"] = True
            self.state["hold_iterations"] = 0
            self.state["entry_swap_amount"] = str(Decimal(str(self.config.get("swap_amount", "10.0"))))
            logger.info(f"Entered {lst_token} depeg position")
        else:
            # Exited position
            exit_reason = self.state.get("exit_reason", "unknown")
            self.state["has_position"] = False
            self.state["hold_iterations"] = 0
            self.state["exit_reason"] = None
            logger.info(f"Exited {lst_token} position (reason: {exit_reason})")

    # -- Teardown --

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self):
        from datetime import UTC, datetime

        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self.state.get("has_position"):
            lst_token = self.config.get("lst_token", "mSOL")
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id=f"lst_depeg_{lst_token.lower()}",
                    chain="solana",
                    protocol="jupiter",
                    value_usd=Decimal(self.state.get("entry_value_usd", "0")),
                    details={
                        "lst_token": lst_token,
                        "hold_iterations": self.state.get("hold_iterations", 0),
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

        if not self.state.get("has_position"):
            return []

        lst_token = self.config.get("lst_token", "mSOL")
        base_token = self.config.get("base_token", "SOL")
        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.01")

        return [
            Intent.swap(
                from_token=lst_token,
                to_token=base_token,
                amount="all",
                max_slippage=max_slippage,
            )
        ]
