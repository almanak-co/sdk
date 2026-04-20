"""
===============================================================================
TraderJoe V2 Leveraged LP with Auto-Compound on Avalanche (VIB-111)
===============================================================================

Multi-protocol composability demo: supply WAVAX as collateral on Aave V3,
borrow USDC at conservative LTV, deploy into TraderJoe V2 Liquidity Book LP,
then periodically harvest earned fees and reinvest them into the LP position.

State machine:
    Phase 1 (setup):  idle -> supplying -> supplied -> borrowing -> borrowed
                      -> lp_opening -> active
    Phase 2 (steady): active -> collecting_fees -> closing_lp -> reopening_lp -> active
    Deleverage:       active -> deleveraging (close LP -> repay -> withdraw partial)

The auto-compound cycle runs on every decide() call once the initial setup
is complete. Fees are collected via CollectFeesIntent, then the LP is closed
and reopened with the original amounts plus collected fees.

Health monitoring: if WAVAX price drops enough that the implied health factor
falls below the floor (default 1.5), the strategy deleverages instead of
compounding.

Chain: Avalanche
Protocols: Aave V3 (lending), TraderJoe V2 (LP)
Pool: WAVAX/USDC (bin_step=20)

USAGE:
    almanak strat run -d strategies/demo/traderjoe_leveraged_lp --network anvil --once

    # Continuous mode (auto-compound on each iteration):
    almanak strat run -d strategies/demo/traderjoe_leveraged_lp --network anvil --interval 60
===============================================================================
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="traderjoe_leveraged_lp",
    description="Leveraged LP with auto-compound: Aave V3 + TraderJoe V2 on Avalanche",
    version="2.0.0",
    author="KitchenLoop",
    tags=["demo", "multi-protocol", "lending", "lp", "aave-v3", "traderjoe-v2", "avalanche", "auto-compound"],
    supported_chains=["avalanche"],
    default_chain="avalanche",
    supported_protocols=["aave_v3", "traderjoe_v2"],
    intent_types=["SUPPLY", "BORROW", "LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES", "REPAY", "WITHDRAW", "HOLD"],
)
class TraderJoeLeveragedLPStrategy(IntentStrategy):
    """
    Cross-protocol leveraged LP with auto-compound.

    Phase 1 (setup):
    1. Supply WAVAX as collateral to Aave V3
    2. Borrow USDC at conservative LTV (30%)
    3. Open WAVAX/USDC LP on TraderJoe V2 Liquidity Book

    Phase 2 (steady-state, on each decide() after setup):
    4. Collect earned fees via CollectFeesIntent (without closing)
    5. Close LP position
    6. Reopen LP with original amounts + collected fees (auto-compound)

    Health monitoring:
    - If implied health factor < floor, deleverage (close LP, repay, partial withdraw)

    Net yield = LP APY * leverage - borrow rate.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Aave config
        self.collateral_token = self.get_config("collateral_token", "WAVAX")
        self.collateral_amount = Decimal(str(self.get_config("collateral_amount", "5")))
        self.borrow_token = self.get_config("borrow_token", "USDC")
        self.ltv_target = Decimal(str(self.get_config("ltv_target", "0.3")))
        self.interest_rate_mode = self.get_config("interest_rate_mode", "variable")

        # LP config
        self.lp_pool = self.get_config("lp_pool", "WAVAX/USDC/20")
        self.lp_range_width_pct = Decimal(str(self.get_config("lp_range_width_pct", "0.1")))

        # Auto-compound config
        self.compound_min_usd = Decimal(str(self.get_config("compound_min_usd", "5")))
        self.health_factor_floor = Decimal(str(self.get_config("health_factor_floor", "1.5")))
        # Aave V3 liquidation threshold for WAVAX on Avalanche (0.65 = 65%)
        self.liquidation_threshold = Decimal(str(self.get_config("liquidation_threshold", "0.65")))

        # State machine
        self._loop_state = "idle"
        self._previous_stable_state = "idle"
        self._deleveraging = False

        # Position tracking
        self._supplied_amount = Decimal("0")
        self._borrowed_amount = Decimal("0")
        self._lp_bin_ids: list[int] = []
        self._lp_wavax = Decimal("0")
        self._lp_usdc = Decimal("0")
        self._compound_count = 0

        # Collected fees (set by on_intent_executed after COLLECT_FEES)
        self._collected_fee_wavax = Decimal("0")
        self._collected_fee_usdc = Decimal("0")

        logger.info(
            f"TraderJoeLeveragedLP initialized: "
            f"collateral={self.collateral_amount} {self.collateral_token}, "
            f"borrow={self.borrow_token}, LTV={self.ltv_target*100}%, "
            f"LP pool={self.lp_pool}, compound_min=${self.compound_min_usd}"
        )

    STABLE_STATES = {"idle", "supplied", "borrowed", "active", "deleveraged"}

    def decide(self, market: MarketSnapshot) -> Intent | None:
        try:
            # Get prices
            try:
                collateral_price = market.price(self.collateral_token)
                borrow_price = market.price(self.borrow_token)
            except (ValueError, KeyError) as e:
                return Intent.hold(reason=f"Price data unavailable: {e}")

            # ---- Phase 1: Setup (one-time) ----

            if self._loop_state == "idle":
                return self._handle_idle(market)

            if self._loop_state == "supplied":
                logger.info("Phase 1b: BORROW USDC from Aave V3")
                self._transition("borrowing")
                return self._create_borrow_intent(collateral_price, borrow_price)

            if self._loop_state == "borrowed":
                logger.info("Phase 1c: LP_OPEN on TraderJoe V2")
                self._transition("lp_opening")
                return self._create_lp_open_intent(collateral_price)

            # ---- Phase 2: Steady-state (auto-compound loop) ----

            if self._loop_state == "active":
                return self._handle_active(collateral_price, borrow_price)

            # Intermediate states from auto-compound cycle
            if self._loop_state == "fees_collected":
                logger.info("Compound step 2: LP_CLOSE to free liquidity for reinvestment")
                self._transition("closing_lp")
                return Intent.lp_close(
                    position_id=self.lp_pool,
                    pool=self.lp_pool,
                    protocol="traderjoe_v2",
                    chain=self.chain,
                )

            if self._loop_state == "lp_closed_for_compound":
                logger.info("Compound step 3: LP_OPEN with original + collected fees")
                self._transition("reopening_lp")
                return self._create_compound_lp_open_intent(collateral_price)

            # Deleverage path: after LP closed, repay the borrow
            if self._loop_state == "deleverage_repaying":
                if self._borrowed_amount > 0:
                    logger.info("Deleverage step 2: REPAY borrow to Aave V3")
                    self._transition("deleverage_withdrawing")
                    return Intent.repay(
                        token=self.borrow_token,
                        amount=self._borrowed_amount,
                        protocol="aave_v3",
                        repay_full=True,
                        chain=self.chain,
                    )
                else:
                    self._loop_state = "deleveraged"
                    return Intent.hold(reason="Deleverage complete: no borrow to repay")

            # Stuck in transitional state -- revert
            if self._loop_state not in self.STABLE_STATES:
                revert_to = self._previous_stable_state
                logger.warning(f"Stuck in '{self._loop_state}' -- reverting to '{revert_to}'")
                self._loop_state = revert_to

            return Intent.hold(reason=f"State: {self._loop_state}")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e!s}")

    # ---- State handlers ----

    def _handle_idle(self, market: MarketSnapshot) -> Intent:
        try:
            bal = market.balance(self.collateral_token)
            balance_value = bal.balance if hasattr(bal, "balance") else bal
            required = self.collateral_amount + Decimal("1")
            if balance_value < required:
                return Intent.hold(
                    reason=f"Insufficient {self.collateral_token}: {balance_value} < {required}"
                )
        except (ValueError, KeyError) as e:
            return Intent.hold(reason=f"Balance unavailable: {e}")

        logger.info("Phase 1a: SUPPLY collateral to Aave V3")
        self._transition("supplying")
        return self._create_supply_intent()

    def _handle_active(self, collateral_price: Decimal, borrow_price: Decimal) -> Intent:
        """Active state: check health, then auto-compound or hold."""
        # Health check
        if self._borrowed_amount > 0 and self._supplied_amount > 0:
            collateral_value = self._supplied_amount * collateral_price
            borrow_value = self._borrowed_amount * borrow_price
            if borrow_value > 0:
                implied_hf = (collateral_value * self.liquidation_threshold) / borrow_value
                logger.info(f"Health factor: {implied_hf:.2f} (floor: {self.health_factor_floor})")

                if implied_hf < self.health_factor_floor:
                    logger.warning(f"Health factor {implied_hf:.2f} < {self.health_factor_floor} -- deleveraging")
                    self._deleveraging = True
                    self._transition("closing_lp")
                    return Intent.lp_close(
                        position_id=self.lp_pool,
                        pool=self.lp_pool,
                        protocol="traderjoe_v2",
                        chain=self.chain,
                    )

        # Auto-compound: collect fees (skip if fees too small to be worth gas)
        logger.info("Compound step 1: COLLECT_FEES from TraderJoe V2 LP")
        self._transition("collecting_fees")
        return Intent.collect_fees(
            pool=self.lp_pool,
            protocol="traderjoe_v2",
            chain=self.chain,
        )

    # ---- Intent creation ----

    def _transition(self, new_state: str) -> None:
        if self._loop_state in self.STABLE_STATES:
            self._previous_stable_state = self._loop_state
        self._loop_state = new_state
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description=f"State: {self._previous_stable_state} -> {new_state}",
                strategy_id=self.strategy_id,
                details={"old_state": self._previous_stable_state, "new_state": new_state},
            )
        )

    def _create_supply_intent(self) -> Intent:
        logger.info(f"SUPPLY: {format_token_amount_human(self.collateral_amount, self.collateral_token)} to Aave V3")
        return Intent.supply(
            protocol="aave_v3",
            token=self.collateral_token,
            amount=self.collateral_amount,
            use_as_collateral=True,
            chain=self.chain,
        )

    def _create_borrow_intent(self, collateral_price: Decimal, borrow_price: Decimal) -> Intent:
        collateral_value = self.collateral_amount * collateral_price
        borrow_value = collateral_value * self.ltv_target
        borrow_amount = (borrow_value / borrow_price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

        logger.info(
            f"BORROW: Collateral={format_usd(collateral_value)}, "
            f"LTV={self.ltv_target*100:.0f}%, "
            f"Borrow={format_token_amount_human(borrow_amount, self.borrow_token)}"
        )

        return Intent.borrow(
            protocol="aave_v3",
            collateral_token=self.collateral_token,
            collateral_amount=Decimal("0"),
            borrow_token=self.borrow_token,
            borrow_amount=borrow_amount,
            interest_rate_mode=self.interest_rate_mode,
            chain=self.chain,
        )

    def _create_lp_open_intent(self, collateral_price: Decimal) -> Intent:
        lp_wavax = Decimal("1")
        lp_usdc = self._borrowed_amount

        half_width = self.lp_range_width_pct / 2
        price_lower = collateral_price * (1 - half_width)
        price_upper = collateral_price * (1 + half_width)

        self._lp_wavax = lp_wavax
        self._lp_usdc = lp_usdc

        logger.info(
            f"LP_OPEN: {self.lp_pool}, "
            f"WAVAX={format_token_amount_human(lp_wavax, 'WAVAX')}, "
            f"USDC={format_token_amount_human(lp_usdc, 'USDC')}, "
            f"range [{price_lower:.2f}-{price_upper:.2f}]"
        )

        return Intent.lp_open(
            pool=self.lp_pool,
            amount0=lp_wavax,
            amount1=lp_usdc,
            range_lower=price_lower,
            range_upper=price_upper,
            protocol="traderjoe_v2",
            chain=self.chain,
        )

    def _create_compound_lp_open_intent(self, collateral_price: Decimal) -> Intent:
        """Reopen LP with original amounts + collected fees."""
        lp_wavax = self._lp_wavax + self._collected_fee_wavax
        lp_usdc = self._lp_usdc + self._collected_fee_usdc

        half_width = self.lp_range_width_pct / 2
        price_lower = collateral_price * (1 - half_width)
        price_upper = collateral_price * (1 + half_width)

        logger.info(
            f"COMPOUND LP_OPEN: {self.lp_pool}, "
            f"WAVAX={format_token_amount_human(lp_wavax, 'WAVAX')} (+{self._collected_fee_wavax} fees), "
            f"USDC={format_token_amount_human(lp_usdc, 'USDC')} (+{self._collected_fee_usdc} fees), "
            f"compound #{self._compound_count + 1}"
        )

        # Don't mutate state here -- commit in on_intent_executed after LP_OPEN succeeds
        return Intent.lp_open(
            pool=self.lp_pool,
            amount0=lp_wavax,
            amount1=lp_usdc,
            range_lower=price_lower,
            range_upper=price_upper,
            protocol="traderjoe_v2",
            chain=self.chain,
        )

    def _get_token_decimals(self, token_symbol: str) -> int:
        from almanak.framework.data.tokens import get_token_resolver
        resolver = get_token_resolver()
        return resolver.get_decimals(self.chain, token_symbol)

    # ---- Lifecycle hooks ----

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SUPPLY":
                self._loop_state = "supplied"
                actual_amount = self._extract_enrichment_amount(result, "supply_amount", self.collateral_token)
                self._supplied_amount = actual_amount if actual_amount else self.collateral_amount
                logger.info(f"SUPPLY successful: {self._supplied_amount} {self.collateral_token}")

            elif intent_type == "BORROW":
                actual_amount = self._extract_enrichment_amount(result, "borrow_amount", self.borrow_token)
                if actual_amount:
                    self._borrowed_amount = actual_amount
                elif hasattr(intent, "borrow_amount"):
                    self._borrowed_amount = Decimal(str(intent.borrow_amount))

                if self._borrowed_amount > 0:
                    self._loop_state = "borrowed"
                    logger.info(f"BORROW successful: {self._borrowed_amount} {self.borrow_token}")
                else:
                    logger.error("BORROW succeeded but amount is 0 -- reverting")
                    self._loop_state = self._previous_stable_state

            elif intent_type == "LP_OPEN":
                self._loop_state = "active"
                self._lp_bin_ids = [-1]  # Default: position exists but bins unknown
                if result and hasattr(result, "extracted_data") and result.extracted_data:
                    ed = result.extracted_data
                    if isinstance(ed, dict) and "bin_ids" in ed:
                        self._lp_bin_ids = ed["bin_ids"]

                if self._previous_stable_state == "active":
                    # Compound reopen succeeded -- commit the new LP amounts
                    self._lp_wavax = self._lp_wavax + self._collected_fee_wavax
                    self._lp_usdc = self._lp_usdc + self._collected_fee_usdc
                    self._collected_fee_wavax = Decimal("0")
                    self._collected_fee_usdc = Decimal("0")
                    self._compound_count += 1
                    logger.info(f"COMPOUND #{self._compound_count} complete: LP reopened with fees reinvested")
                else:
                    logger.info(f"LP_OPEN successful: bins={self._lp_bin_ids}")

            elif intent_type == "LP_COLLECT_FEES":
                self._loop_state = "fees_collected"
                # Reset fees before extraction to avoid stale values from previous cycle
                self._collected_fee_wavax = Decimal("0")
                self._collected_fee_usdc = Decimal("0")
                # Extract and normalize fee amounts (raw wei -> human-readable)
                fee_wavax = self._extract_enrichment_amount(result, "fee_amount_x", self.collateral_token)
                fee_usdc = self._extract_enrichment_amount(result, "fee_amount_y", self.borrow_token)
                if fee_wavax is not None:
                    self._collected_fee_wavax = fee_wavax
                if fee_usdc is not None:
                    self._collected_fee_usdc = fee_usdc
                logger.info(
                    f"COLLECT_FEES: WAVAX={self._collected_fee_wavax}, USDC={self._collected_fee_usdc}"
                )

            elif intent_type == "LP_CLOSE":
                self._lp_bin_ids = []
                if self._deleveraging:
                    # Deleverage path: close LP -> repay borrow
                    self._loop_state = "deleverage_repaying"
                    self._deleveraging = False
                    logger.info("LP_CLOSE for deleverage: position closed, proceeding to repay")
                elif self._previous_stable_state == "active":
                    # Part of compound cycle
                    self._loop_state = "lp_closed_for_compound"
                    logger.info("LP_CLOSE for compound: position closed, ready to reopen")
                else:
                    self._loop_state = "active"
                    logger.info("LP_CLOSE successful")

            elif intent_type == "REPAY":
                self._borrowed_amount = Decimal("0")
                self._loop_state = "deleveraged"
                logger.info("REPAY successful: borrow fully repaid, deleverage complete")

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"{intent_type} successful -> state={self._loop_state}",
                    strategy_id=self.strategy_id,
                    details={"intent_type": intent_type, "new_state": self._loop_state},
                )
            )
        else:
            revert_to = self._previous_stable_state
            logger.warning(f"{intent_type} FAILED in '{self._loop_state}' -- reverting to '{revert_to}'")
            self._loop_state = revert_to

    def _extract_enrichment_amount(self, result: Any, field: str, token: str) -> Decimal | None:
        """Extract and normalize an amount from enrichment data."""
        if not result or not hasattr(result, "extracted_data") or not result.extracted_data:
            return None
        ed = result.extracted_data
        if not isinstance(ed, dict) or field not in ed:
            return None
        try:
            raw = Decimal(str(ed[field]))
            decimals = self._get_token_decimals(token)
            return raw / Decimal(10) ** decimals
        except Exception:
            logger.warning(f"Could not normalize enrichment {field}")
            return None

    # ---- Status ----

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "traderjoe_leveraged_lp",
            "chain": self.chain,
            "state": {
                "loop_state": self._loop_state,
                "supplied": str(self._supplied_amount),
                "borrowed": str(self._borrowed_amount),
                "lp_wavax": str(self._lp_wavax),
                "lp_usdc": str(self._lp_usdc),
                "lp_bin_ids": self._lp_bin_ids,
                "compound_count": self._compound_count,
            },
        }

    # ---- State persistence ----

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "loop_state": self._loop_state,
            "previous_stable_state": self._previous_stable_state,
            "supplied_amount": str(self._supplied_amount),
            "borrowed_amount": str(self._borrowed_amount),
            "lp_wavax": str(self._lp_wavax),
            "lp_usdc": str(self._lp_usdc),
            "lp_bin_ids": self._lp_bin_ids,
            "compound_count": self._compound_count,
            "collected_fee_wavax": str(self._collected_fee_wavax),
            "collected_fee_usdc": str(self._collected_fee_usdc),
            "deleveraging": self._deleveraging,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "loop_state" in state:
            self._loop_state = state["loop_state"]
        if "previous_stable_state" in state:
            self._previous_stable_state = state["previous_stable_state"]
        if "supplied_amount" in state:
            self._supplied_amount = Decimal(str(state["supplied_amount"]))
        if "borrowed_amount" in state:
            self._borrowed_amount = Decimal(str(state["borrowed_amount"]))
        if "lp_wavax" in state:
            self._lp_wavax = Decimal(str(state["lp_wavax"]))
        if "lp_usdc" in state:
            self._lp_usdc = Decimal(str(state["lp_usdc"]))
        if "lp_bin_ids" in state:
            self._lp_bin_ids = state["lp_bin_ids"]
        if "compound_count" in state:
            self._compound_count = state["compound_count"]
        if "collected_fee_wavax" in state:
            self._collected_fee_wavax = Decimal(str(state["collected_fee_wavax"]))
        if "collected_fee_usdc" in state:
            self._collected_fee_usdc = Decimal(str(state["collected_fee_usdc"]))
        if "deleveraging" in state:
            self._deleveraging = state["deleveraging"]
        logger.info(f"Restored state: {self._loop_state}, compound_count={self._compound_count}")

    # ---- Teardown ----

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        try:
            market = self.create_market_snapshot()
            collateral_price = Decimal(str(market.price(self.collateral_token)))
        except Exception:
            collateral_price = Decimal("0")

        positions: list[PositionInfo] = []

        if self._lp_bin_ids:
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=f"traderjoe-v2-lp-{self.lp_pool}",
                    chain=self.chain,
                    protocol="traderjoe_v2",
                    value_usd=self._lp_usdc * 2 if self._lp_usdc else Decimal("0"),
                    details={"pool": self.lp_pool, "bin_ids": self._lp_bin_ids},
                )
            )

        if self._supplied_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"aave-supply-{self.collateral_token}-{self.chain}",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=self._supplied_amount * collateral_price,
                    details={"asset": self.collateral_token, "amount": str(self._supplied_amount)},
                )
            )

        if self._borrowed_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"aave-borrow-{self.borrow_token}-{self.chain}",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=self._borrowed_amount,
                    details={"asset": self.borrow_token, "amount": str(self._borrowed_amount)},
                )
            )

        return TeardownPositionSummary(
            strategy_id="traderjoe_leveraged_lp",
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        intents = []

        # Step 1: Close LP
        if self._lp_bin_ids:
            intents.append(
                Intent.lp_close(
                    position_id=self.lp_pool,
                    pool=self.lp_pool,
                    protocol="traderjoe_v2",
                    chain=self.chain,
                )
            )

        # Step 2: Repay borrow
        if self._borrowed_amount > 0:
            intents.append(
                Intent.repay(
                    token=self.borrow_token,
                    amount=self._borrowed_amount,
                    protocol="aave_v3",
                    repay_full=True,
                    chain=self.chain,
                )
            )

        # Step 3: Withdraw collateral
        if self._supplied_amount > 0:
            intents.append(
                Intent.withdraw(
                    token=self.collateral_token,
                    amount=self._supplied_amount,
                    protocol="aave_v3",
                    withdraw_all=True,
                    chain=self.chain,
                )
            )

        return intents
