"""Aave V3.6 Supply/Borrow Carry on X-Layer.

Demonstrates Aave V3.6 lending on X-Layer:

1. Supply USDT0 (USD₮0) as collateral on Aave V3.6
2. Borrow USDG (Gravity USD) against it (50% LTV target)
3. Hold borrowed USDG — demonstrates supply + borrow balance changes

Aave V3.6 X-Layer collateral-eligible reserves (LTV > 0):
  - USDT0 (USD₮0 0x779Ded...): LTV=70%, borrowingEnabled=true
  - xETH (0xE7B000...): LTV=70%, borrowingEnabled=true (limited on-chain liquidity)
  - xBTC (0xb7C000...): LTV=70%, borrowingEnabled=true

Note: WOKB has LTV=0 on X-Layer Aave and cannot be used as collateral.
Note: xETH has very limited pool liquidity (~0.008 xETH). Use USDG as borrow token for demos.
Governance: Proposal #460 (https://app.aave.com/governance/v3/proposal/?proposalId=460)

Usage:
    almanak strat run -d almanak/demo_strategies/xlayer_aave_carry --network anvil --once
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="demo_xlayer_aave_carry",
    description="Aave V3.6 supply/borrow carry on X-Layer — supply USDT0 collateral, borrow USDG",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "xlayer", "lending", "aave-v3", "carry"],
    supported_chains=["xlayer"],
    supported_protocols=["aave_v3", "uniswap_v3"],
    intent_types=["SUPPLY", "BORROW", "REPAY", "WITHDRAW", "SWAP", "HOLD"],
    default_chain="xlayer",
    quote_asset="USD",
)
class XLayerAaveCarryStrategy(IntentStrategy):
    """Aave V3.6 supply+borrow carry on X-Layer.

    State machine:
        idle -> supplying -> supplied -> borrowing -> borrowed -> complete

    Configuration (config.json):
        supply_token: Collateral token with LTV>0 (default: USDT0)
        borrow_token: Token to borrow (default: USDG)
        initial_supply_amount: Supply amount in token units (default: 100.0 USDT0)
        ltv_target: Target LTV (default: 0.5 = 50%)
        min_health_factor: Safety floor (default: 1.5)
        interest_rate_mode: Must be "variable" (stable rate deprecated in Aave V3, ignored if set) (default: variable)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.supply_token = self.get_config("supply_token", "USDT0")
        self.borrow_token = self.get_config("borrow_token", "USDG")
        self.initial_supply_amount = Decimal(str(self.get_config("initial_supply_amount", "100.0")))
        self.ltv_target = Decimal(str(self.get_config("ltv_target", "0.5")))
        self.min_health_factor = Decimal(str(self.get_config("min_health_factor", "1.5")))
        self.interest_rate_mode = self.get_config("interest_rate_mode", "variable")

        # State tracking
        self._state = "idle"
        self._previous_stable_state = "idle"
        self._total_supplied = Decimal("0")
        self._total_borrowed = Decimal("0")
        self._supply_price_usd = Decimal("1")
        self._borrow_price_usd = Decimal("1")

        logger.info(
            f"XLayerAaveCarry: supply={self.initial_supply_amount} {self.supply_token}, "
            f"borrow={self.borrow_token}, LTV={self.ltv_target * 100:.0f}%"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Execute the next step in the state machine."""
        if self._state == "complete":
            return Intent.hold(
                reason=f"Carry complete: supplied={self._total_supplied} {self.supply_token}, "
                f"borrowed={self._total_borrowed:.6f} {self.borrow_token}"
            )

        # Get prices
        try:
            supply_price = Decimal(str(market.price(self.supply_token)))
            borrow_price = Decimal(str(market.price(self.borrow_token)))
            self._supply_price_usd = supply_price
            self._borrow_price_usd = borrow_price
        except (ValueError, KeyError) as e:
            return Intent.hold(reason=f"Price data unavailable: {e}")

        if supply_price <= 0 or borrow_price <= 0:
            return Intent.hold(
                reason=f"Invalid prices: {self.supply_token}=${supply_price}, {self.borrow_token}=${borrow_price}"
            )

        # Step 1: Supply collateral
        if self._state == "idle":
            supply_amount = self.initial_supply_amount
            try:
                balance = market.balance(self.supply_token)
                available = balance.balance if hasattr(balance, "balance") else balance
                if available < supply_amount:
                    supply_amount = available
                if supply_amount <= 0:
                    return Intent.hold(reason=f"No {self.supply_token} available to supply")
            except (ValueError, KeyError):
                return Intent.hold(reason=f"Cannot verify {self.supply_token} balance, waiting")

            self._previous_stable_state = self._state
            self._state = "supplying"

            logger.info(f"SUPPLY {supply_amount} {self.supply_token} as collateral")
            return Intent.supply(
                protocol="aave_v3",
                token=self.supply_token,
                amount=supply_amount,
                use_as_collateral=True,
                chain=self.chain,
            )

        # Step 2: Borrow against collateral
        if self._state == "supplied":
            collateral_value_usd = self._total_supplied * supply_price
            borrow_value_usd = collateral_value_usd * self.ltv_target
            borrow_amount = (borrow_value_usd / borrow_price).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)

            if borrow_amount <= 0:
                self._state = "complete"
                return Intent.hold(reason="Borrow amount too small")

            self._previous_stable_state = self._state
            self._state = "borrowing"

            logger.info(
                f"BORROW {borrow_amount} {self.borrow_token} "
                f"(LTV {self.ltv_target * 100:.0f}% on ${collateral_value_usd:.2f} collateral)"
            )
            return Intent.borrow(
                protocol="aave_v3",
                collateral_token=self.supply_token,
                collateral_amount=Decimal("0"),
                borrow_token=self.borrow_token,
                borrow_amount=borrow_amount,
                interest_rate_mode=self.interest_rate_mode,
                chain=self.chain,
            )

        # Safety: hold in transitional states until on_intent_executed callback
        if self._state in ("supplying", "borrowing"):
            return Intent.hold(reason=f"Waiting for {self._state} completion")

        return Intent.hold(reason=f"Waiting (state={self._state})")

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SUPPLY":
                self._state = "supplied"
                supply_amt = Decimal(str(getattr(intent, "amount", self.initial_supply_amount)))
                self._total_supplied += supply_amt
                logger.info(f"Supply OK. Total supplied: {self._total_supplied} {self.supply_token}")

            elif intent_type == "BORROW":
                self._state = "complete"
                borrow_amount = Decimal(str(intent.borrow_amount)) if hasattr(intent, "borrow_amount") else Decimal("0")
                self._total_borrowed += borrow_amount
                logger.info(
                    f"Borrow OK. Borrowed: {self._total_borrowed:.6f} {self.borrow_token}. "
                    f"Carry complete."
                )

            elif intent_type == "REPAY":
                self._total_borrowed = Decimal("0")

            elif intent_type == "WITHDRAW":
                if getattr(intent, "withdraw_all", False):
                    self._total_supplied = Decimal("0")
                else:
                    withdrawn = Decimal(str(getattr(intent, "amount", Decimal("0")) or Decimal("0")))
                    self._total_supplied = max(Decimal("0"), self._total_supplied - withdrawn)
        else:
            revert_to = self._previous_stable_state
            logger.warning(f"{intent_type} failed, reverting to '{revert_to}'")
            self._state = revert_to

    # -- Status & Persistence --

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_xlayer_aave_carry",
            "chain": self.chain,
            "state": self._state,
            "total_supplied": str(self._total_supplied),
            "total_borrowed": str(self._total_borrowed),
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "previous_stable_state": self._previous_stable_state,
            "total_supplied": str(self._total_supplied),
            "total_borrowed": str(self._total_borrowed),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "state" in state:
            self._state = state["state"]
        if "previous_stable_state" in state:
            self._previous_stable_state = state["previous_stable_state"]
        if "total_supplied" in state:
            self._total_supplied = Decimal(str(state["total_supplied"]))
        if "total_borrowed" in state:
            self._total_borrowed = Decimal(str(state["total_borrowed"]))

    # -- Teardown --

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list["PositionInfo"] = []

        if self._total_supplied > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"aave-supply-{self.supply_token}-xlayer",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=self._total_supplied * self._supply_price_usd,
                    details={"asset": self.supply_token, "amount": str(self._total_supplied)},
                )
            )

        if self._total_borrowed > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"aave-borrow-{self.borrow_token}-xlayer",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=self._total_borrowed * self._borrow_price_usd,
                    details={"asset": self.borrow_token, "amount": str(self._total_borrowed)},
                )
            )

        return TeardownPositionSummary(
            deployment_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        intents: list[Intent] = []
        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.01")

        if self._total_borrowed > 0:
            # Interest accrual gap: at teardown, Aave debt = principal + accrued interest,
            # but the wallet only holds the original borrowed principal.  Repaying only the
            # principal leaves residual dust debt that blocks a full collateral withdrawal.
            #
            # Fix: withdraw a small collateral buffer from Aave, swap it to the borrow token
            # on Uniswap V3, then repay.  The compiler's repay_full path uses the wallet
            # balance as the repay amount; Aave caps it to the actual debt if we overshoot.
            #
            # Buffer sizing: 0.5% of borrowed value in supply_token units.
            # Sized in USD first (0.5% of borrow value), then converted to supply_token
            # units via cached prices so it works for any collateral (USDT0, xETH, xBTC).
            # For stablecoin pairs (default USDT0/USDG) this equals 0.5% of borrow amount.
            borrow_price = self._borrow_price_usd if self._borrow_price_usd > 0 else Decimal("1")
            supply_price = self._supply_price_usd if self._supply_price_usd > 0 else Decimal("1")
            interest_buffer = (self._total_borrowed * Decimal("0.005") * borrow_price) / supply_price

            # Step 1: Partial collateral withdraw to fund the interest buffer swap
            intents.append(
                Intent.withdraw(
                    token=self.supply_token,
                    amount=interest_buffer,
                    protocol="aave_v3",
                    chain=self.chain,
                )
            )

            # Step 2: Swap withdrawn collateral → borrow token (covers the interest gap)
            intents.append(
                Intent.swap(
                    from_token=self.supply_token,
                    to_token=self.borrow_token,
                    amount=interest_buffer,
                    max_slippage=max_slippage,
                    chain=self.chain,
                )
            )

            # Step 3: Repay full debt (wallet now holds principal + buffer; Aave caps to actual debt)
            intents.append(
                Intent.repay(
                    token=self.borrow_token,
                    protocol="aave_v3",
                    repay_full=True,
                    chain=self.chain,
                )
            )

        if self._total_supplied > 0:
            # Step 4: Withdraw all remaining collateral (debt fully repaid above)
            intents.append(
                Intent.withdraw(
                    token=self.supply_token,
                    amount="all",
                    protocol="aave_v3",
                    withdraw_all=True,
                    chain=self.chain,
                )
            )

        return intents
