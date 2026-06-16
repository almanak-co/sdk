"""Curvance full lending lifecycle demo on Monad.

Exercises the Curvance connector end-to-end through the intent layer on the
isolated **WMON -> USDC** market:

    1. SUPPLY: approve + depositAsCollateral(WMON)
    2. BORROW: borrow(USDC) against the supplied collateral (collateral_amount=0)
    3. REPAY:  approve + repay(0)  (0 is Curvance's full-debt sentinel)
    4. WITHDRAW: redeemCollateral(shares, wallet, wallet)
    5. COMPLETE (hold)

The supply and borrow are emitted as two separate intents (not a bundled
``Intent.borrow(collateral_amount>0)``) so the accounting layer records a
distinct SUPPLY event and its supply cost-basis lot — the 1:1 ledger->event
invariant the framework enforces. See docs/internal/bundled-collateral-borrow-migration.md.

Protocol constraints to be aware of:

- ``MIN_LOAN_SIZE = $10 USD`` (public immutable, unbypassable) — borrow amount
  must exceed $10 of the debt asset at the oracle's price.
- ``MIN_HOLD_PERIOD = 20 minutes`` — blocks redeem AND repay for 20 min after
  any collateral deposit. Plan for one iteration per hold window.
- ``repay(0)`` is the full-debt sentinel; ``MAX_UINT256`` is **not** accepted.
  The caller must hold ``debtBalance(msg.sender)`` in the debt-underlying token
  at submission time (principal + accrued interest).

**Anvil fork limitation**: Curvance's oracle trips a CAUTION breakpoint on
Monad RPC forks (fork timestamp drifts past the Redstone adaptor freshness
window), which reverts BORROW / REPAY / WITHDRAW with
``MarketManager__InsufficientCollateral``. The SUPPLY step succeeds because it
does not consult the oracle in the same way. Full lifecycle verification is on
live Monad mainnet — see PR #1563 for mainnet TX evidence.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.utils.log_formatters import format_token_amount_human

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

IDLE = "idle"
SUPPLIED = "supplied"
BORROWED = "borrowed"
REPAID = "repaid"
COMPLETE = "complete"

SUPPLYING = "supplying"
BORROWING = "borrowing"
REPAYING = "repaying"
WITHDRAWING = "withdrawing"

STABLE_STATES = {IDLE, SUPPLIED, BORROWED, REPAID, COMPLETE}
TRANSITIONAL_STATES = {SUPPLYING, BORROWING, REPAYING, WITHDRAWING}

WMON_USDC_MARKET = "0xa6A2A92F126b79Ee0804845ee6B52899b4491093"


@almanak_strategy(
    name="monad_lending",
    description="Curvance full lending lifecycle on Monad: supply WMON -> borrow USDC -> repay -> redeem collateral",
    version="1.0.0",
    author="Kitchen Loop",
    tags=["demo", "lending", "curvance", "monad", "lifecycle"],
    supported_chains=["monad"],
    supported_protocols=["curvance"],
    intent_types=["SUPPLY", "BORROW", "REPAY", "WITHDRAW", "HOLD"],
    default_chain="monad",
    quote_asset="USD",
)
class MonadCurvanceLendingStrategy(IntentStrategy):
    """Curvance lending lifecycle on Monad's WMON -> USDC isolated market."""

    def supports_teardown(self) -> bool:
        return True

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.collateral_token = self.get_config("collateral_token", "WMON")
        self.collateral_amount = Decimal(str(self.get_config("collateral_amount", "30")))
        self.borrow_token = self.get_config("borrow_token", "USDC")
        self.ltv_target = Decimal(str(self.get_config("ltv_target", "0.25")))
        self.market_id = str(self.get_config("market_id", WMON_USDC_MARKET))

        borrow_override = self.get_config("borrow_amount_override", "")
        self.borrow_amount_override = Decimal(str(borrow_override)) if borrow_override else None

        self._loop_state = IDLE
        self._previous_stable_state = IDLE
        self._supplied_amount = Decimal("0")
        self._borrowed_amount = Decimal("0")

        logger.info(
            f"MonadCurvanceLending initialized: market_id={self.market_id}, "
            f"collateral={self.collateral_amount} {self.collateral_token}, "
            f"borrow {self.borrow_token} LTV={self.ltv_target * 100}%"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        try:
            if self._loop_state in TRANSITIONAL_STATES:
                stuck_state = self._loop_state
                revert_to = self._previous_stable_state
                logger.warning(f"Stuck in '{stuck_state}' -- reverting to '{revert_to}', holding this iteration")
                self._loop_state = revert_to
                return Intent.hold(reason=f"Recovered from stuck state '{stuck_state}', holding before retry")

            if self._loop_state == IDLE:
                # Step 1: SUPPLY the collateral as a standalone intent. The
                # accounting layer writes one event per intent, so the supply
                # must NOT be bundled into the borrow (a bundled
                # Intent.borrow(collateral_amount>0) drops the SUPPLY accounting
                # event + supply FIFO lot). Supplying first is also more robust
                # on Curvance: depositAsCollateral succeeds even inside the
                # post-deploy window that reverts BORROW/REPAY/WITHDRAW.
                logger.info(
                    f"Step 1: SUPPLY {self.collateral_amount} {self.collateral_token} as collateral on Curvance "
                    f"(market={self.market_id})"
                )
                self._transition(SUPPLYING)
                return Intent.supply(
                    protocol="curvance",
                    token=self.collateral_token,
                    amount=self.collateral_amount,
                    use_as_collateral=True,
                    market_id=self.market_id,
                    chain=self.chain,
                )

            if self._loop_state == SUPPLIED:
                if self.borrow_amount_override is not None:
                    borrow_amount = self.borrow_amount_override
                    logger.info(f"Using fixed borrow_amount_override: {borrow_amount} {self.borrow_token}")
                else:
                    try:
                        collateral_price = market.price(self.collateral_token)
                        borrow_price = market.price(self.borrow_token)
                    except (ValueError, KeyError) as e:
                        logger.warning(f"Price fetch failed: {e}")
                        return Intent.hold(reason=f"Price data unavailable: {e}")
                    collateral_value = self.collateral_amount * collateral_price
                    borrow_value = collateral_value * self.ltv_target
                    borrow_amount = (borrow_value / borrow_price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

                if borrow_amount <= 0:
                    return Intent.hold(reason="Computed borrow amount is zero")

                logger.info(
                    f"Step 2: BORROW {format_token_amount_human(borrow_amount, self.borrow_token)} "
                    f"against {self.collateral_amount} {self.collateral_token} on Curvance "
                    f"(market={self.market_id}, LTV={self.ltv_target * 100:.0f}%)"
                )
                self._transition(BORROWING)

                # collateral_amount=0: the collateral is already supplied by the
                # SUPPLY step above. collateral_token is retained as metadata.
                return Intent.borrow(
                    protocol="curvance",
                    collateral_token=self.collateral_token,
                    collateral_amount=Decimal("0"),
                    borrow_token=self.borrow_token,
                    borrow_amount=borrow_amount,
                    market_id=self.market_id,
                    chain=self.chain,
                )

            if self._loop_state == BORROWED:
                # repay(0) requires that we hold the FULL accrued debt in the
                # debt-underlying token at submission time. If the wallet
                # balance has dipped below the borrowed principal (e.g., the
                # debt accrued interest beyond what we hold), hold and let an
                # operator top up — otherwise we will revert and loop.
                wallet_debt_balance = self._wallet_balance(market, self.borrow_token)
                if wallet_debt_balance is not None and wallet_debt_balance < self._borrowed_amount:
                    return Intent.hold(
                        reason=(
                            f"Insufficient {self.borrow_token} for repay_full: "
                            f"wallet={wallet_debt_balance}, owed≈{self._borrowed_amount}. "
                            f"repay(0) pulls debtBalance(msg.sender) at submission time and reverts "
                            f"if the caller is short — top up before retrying."
                        )
                    )

                logger.info(
                    f"Step 2: REPAY full debt ({self._borrowed_amount} {self.borrow_token}) on Curvance "
                    f"via repay(0) sentinel"
                )
                self._transition(REPAYING)

                return Intent.repay(
                    protocol="curvance",
                    token=self.borrow_token,
                    amount=self._borrowed_amount,
                    repay_full=True,
                    market_id=self.market_id,
                    chain=self.chain,
                )

            if self._loop_state == REPAID:
                logger.info(
                    f"Step 3: WITHDRAW (redeemCollateral) {self._supplied_amount} {self.collateral_token} "
                    f"from Curvance"
                )
                self._transition(WITHDRAWING)

                return Intent.withdraw(
                    protocol="curvance",
                    token=self.collateral_token,
                    amount=self._supplied_amount,
                    withdraw_all=True,
                    market_id=self.market_id,
                    chain=self.chain,
                )

            if self._loop_state == COMPLETE:
                return Intent.hold(
                    reason="Curvance lending lifecycle complete: supply -> borrow -> repay -> redeem"
                )

            return Intent.hold(reason=f"Unknown state: {self._loop_state}")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e!s}")

    def _transition(self, new_state: str) -> None:
        old = self._loop_state
        if old in STABLE_STATES:
            self._previous_stable_state = old
        self._loop_state = new_state
        logger.info(f"State transition: {old} -> {new_state}")

    @staticmethod
    def _wallet_balance(market: MarketSnapshot, token: str) -> Decimal | None:
        """Return the wallet's human-units balance for ``token``, or ``None`` if unavailable."""
        try:
            bal = market.balance(token)
        except (KeyError, ValueError, AttributeError):
            return None
        if bal is None:
            return None
        # ``MarketSnapshot.balance()`` returns a TokenBalance dataclass with a ``balance``
        # field (human units). Be defensive in case it's a bare Decimal/float.
        amount = getattr(bal, "balance", bal)
        try:
            return Decimal(str(amount))
        except Exception:
            return None

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return
        intent_type_val = intent_type.value if hasattr(intent_type, "value") else str(intent_type)

        if success:
            if intent_type_val == "SUPPLY":
                self._loop_state = SUPPLIED
                self._previous_stable_state = SUPPLIED
                # Record what was actually supplied (the executed intent's amount),
                # not the config default — this is the collateral the WITHDRAW
                # step later redeems.
                self._supplied_amount = Decimal(str(getattr(intent, "amount", self.collateral_amount)))
                logger.info(
                    f"SUPPLY succeeded: supplied {self._supplied_amount} {self.collateral_token} as collateral on Curvance"
                )
                self._log_result_details("SUPPLY", result)

            elif intent_type_val == "BORROW":
                self._loop_state = BORROWED
                self._previous_stable_state = BORROWED
                if hasattr(intent, "borrow_amount"):
                    self._borrowed_amount = Decimal(str(intent.borrow_amount))
                logger.info(
                    f"BORROW succeeded: borrowed {self._borrowed_amount} {self.borrow_token} on Curvance "
                    f"(collateral {self._supplied_amount} {self.collateral_token} already supplied)"
                )
                self._log_result_details("BORROW", result)

            elif intent_type_val == "REPAY":
                self._loop_state = REPAID
                self._previous_stable_state = REPAID
                self._borrowed_amount = Decimal("0")
                logger.info("REPAY succeeded -- debt cleared via repay(0) sentinel, state -> repaid")
                self._log_result_details("REPAY", result)

            elif intent_type_val == "WITHDRAW":
                self._loop_state = COMPLETE
                self._previous_stable_state = COMPLETE
                self._supplied_amount = Decimal("0")
                logger.info("WITHDRAW succeeded -- collateral redeemed, lifecycle complete")
                self._log_result_details("WITHDRAW", result)
        else:
            revert_to = self._previous_stable_state
            logger.warning(f"{intent_type_val} FAILED in state '{self._loop_state}' -- reverting to '{revert_to}'")
            self._loop_state = revert_to

    def _log_result_details(self, intent_type: str, result: Any) -> None:
        if result is None:
            return
        if hasattr(result, "extracted_data") and result.extracted_data:
            logger.info(f"  {intent_type} extracted_data: {result.extracted_data}")
        if hasattr(result, "transaction_results"):
            tx_results = result.transaction_results
            if tx_results:
                for i, tx_result in enumerate(tx_results):
                    tx_hash = getattr(tx_result, "tx_hash", "N/A")
                    gas_used = getattr(tx_result, "gas_used", "N/A")
                    logger.info(f"  {intent_type} TX {i + 1}: hash={tx_hash}, gas={gas_used}")

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._loop_state,
            "previous_stable_state": self._previous_stable_state,
            "supplied_amount": str(self._supplied_amount),
            "borrowed_amount": str(self._borrowed_amount),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        self._loop_state = state.get("state", IDLE)
        self._previous_stable_state = state.get("previous_stable_state", IDLE)
        self._supplied_amount = Decimal(str(state.get("supplied_amount", "0")))
        self._borrowed_amount = Decimal(str(state.get("borrowed_amount", "0")))
        logger.info(f"Restored state: {self._loop_state}")

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "monad_lending",
            "chain": self.chain,
            "market_id": self.market_id,
            "state": self._loop_state,
            "supplied": f"{self._supplied_amount} {self.collateral_token}",
            "borrowed": f"{self._borrowed_amount} {self.borrow_token}",
        }

    def get_open_positions(self) -> TeardownPositionSummary:
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        try:
            market = self.create_market_snapshot()
            collateral_price = Decimal(str(market.price(self.collateral_token)))
            borrow_price = Decimal(str(market.price(self.borrow_token)))
        except Exception:
            logger.warning("Unable to fetch live prices for teardown valuation")
            collateral_price = Decimal("0")
            borrow_price = Decimal("0")

        positions: list[PositionInfo] = []

        if self._supplied_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"curvance-supply-{self.collateral_token}-monad",
                    chain=self.chain,
                    protocol="curvance",
                    value_usd=self._supplied_amount * collateral_price,
                    details={"asset": self.collateral_token, "market_id": self.market_id},
                )
            )

        if self._borrowed_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"curvance-borrow-{self.borrow_token}-monad",
                    chain=self.chain,
                    protocol="curvance",
                    value_usd=self._borrowed_amount * borrow_price,
                    details={"asset": self.borrow_token, "market_id": self.market_id},
                )
            )

        return TeardownPositionSummary(
            deployment_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: TeardownMode, market=None) -> list[Intent]:
        intents: list[Intent] = []
        repay_skipped = False

        if self._borrowed_amount > 0:
            # Same repay(0) wallet-balance constraint as decide(): if the
            # operator does not currently hold ≥ borrowed amount of the debt
            # token, the repay will revert. Skip the repay leg and surface a
            # HOLD-style log instead — operator must top up and re-tear down.
            wallet_debt_balance = (
                self._wallet_balance(market, self.borrow_token) if market is not None else None
            )
            if wallet_debt_balance is not None and wallet_debt_balance < self._borrowed_amount:
                logger.warning(
                    f"Teardown REPAY skipped: wallet has {wallet_debt_balance} {self.borrow_token} "
                    f"but owe ≈ {self._borrowed_amount}; top up before retrying teardown. "
                    f"Suppressing collateral WITHDRAW because Curvance rejects redeems with debt outstanding."
                )
                repay_skipped = True
            else:
                intents.append(
                    Intent.repay(
                        protocol="curvance",
                        token=self.borrow_token,
                        amount=self._borrowed_amount,
                        repay_full=True,
                        market_id=self.market_id,
                        chain=self.chain,
                    )
                )

        if self._supplied_amount > 0 and not repay_skipped:
            intents.append(
                Intent.withdraw(
                    protocol="curvance",
                    token=self.collateral_token,
                    amount=self._supplied_amount,
                    withdraw_all=True,
                    market_id=self.market_id,
                    chain=self.chain,
                )
            )

        return intents
