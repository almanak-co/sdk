"""
===============================================================================
TUTORIAL: Fluid Vault Borrow Strategy — NFT-CDP positions via operate()
===============================================================================

This tutorial strategy demonstrates Fluid's vault (NFT-CDP) lending surface
(protocol key ``fluid_vault``, VIB-5031) on Arbitrum vault id 1
(native-ETH collateral -> USDC debt).

WHAT THIS STRATEGY DOES:
------------------------
1. Opens a position ATOMICALLY: one ``BorrowIntent`` with both a collateral
   amount and a borrow amount compiles to a SINGLE on-chain call —
   ``operate(nftId=0, +collateral, +debt, wallet)`` — which mints the
   position NFT and draws the debt in one transaction.
2. Optionally repays (``force_action="repay"``) or adds collateral
   (``force_action="supply"``).

HOW FLUID VAULTS DIFFER FROM AAVE/MORPHO:
-----------------------------------------
- The position is an ERC-721 NFT minted by the VaultFactory; every
  lifecycle action goes through ONE signed-delta entrypoint per vault:
  ``operate(nftId, colDelta, debtDelta, to)``.
- ``market_id`` is the VAULT ADDRESS (isolated markets, Morpho-style) and
  is REQUIRED on every fluid_vault intent.
- Vault id 1 takes RAW native ETH as collateral (msg.value — no WETH wrap,
  no approve on the collateral leg).
- Full repays use the protocol's type(int256).min sentinel
  (``repay_full=True``): the vault resolves the exact debt at execution
  time, so accrued interest can never cause an over-repay revert.

RISKS:
------
- Liquidation: vault id 1 liquidates at 92% LTV (collateral factor 87%).
- Interest: you pay interest on borrowed USDC.

USAGE:
------
    # Test on Anvil (starts a fork, funds the wallet, runs via CLI):
    python almanak/demo_strategies/fluid_borrow/run_anvil.py

===============================================================================
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.teardown import TeardownPositionSummary
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

logger = logging.getLogger(__name__)

# Arbitrum Fluid vault id 1 (ETH -> USDC, type-1) — the pinned, on-chain
# verified market (docs/internal/qa/fluid-vault-verification-2026-06-12.md).
DEFAULT_VAULT = "0xeAbBfca72F8a8bf14C4ac59e69ECB2eB69F0811C"


@almanak_strategy(
    name="demo_fluid_borrow",
    description="Tutorial strategy — open a Fluid vault NFT-CDP (ETH collateral, USDC debt) atomically",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "tutorial", "lending", "borrow", "fluid", "nft-cdp"],
    supported_chains=["arbitrum"],
    supported_protocols=["fluid_vault"],
    intent_types=["SUPPLY", "BORROW", "REPAY", "HOLD"],
    default_chain="arbitrum",
    quote_asset="USD",
)
class FluidBorrowStrategy(IntentStrategy):
    """Fluid vault borrow strategy for educational purposes.

    Configuration Parameters (config.json):
    ---------------------------------------
    - market_id: The type-1 vault address (default: arbitrum vault id 1)
    - collateral_token: Vault collateral symbol (default: "ETH")
    - collateral_amount: Collateral to supply on open (default: "0.2")
    - borrow_token: Vault debt symbol (default: "USDC")
    - ltv_target: Target loan-to-value on open (default: 0.3 — conservative
      against the vault's 87% collateral factor)
    - repay_amount: Partial repay size for force_action="repay" (default: "50")
    - force_action: "open" | "supply" | "repay" | "" (state machine)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.market_id = str(self.get_config("market_id", DEFAULT_VAULT))
        self.collateral_token = self.get_config("collateral_token", "ETH")
        self.collateral_amount = Decimal(str(self.get_config("collateral_amount", "0.2")))
        self.borrow_token = self.get_config("borrow_token", "USDC")
        self.ltv_target = Decimal(str(self.get_config("ltv_target", "0.3")))
        self.repay_amount = Decimal(str(self.get_config("repay_amount", "50")))
        self.force_action = str(self.get_config("force_action", "")).lower()

        # idle -> opening -> complete
        self._loop_state = "idle"

        logger.info(
            "FluidBorrowStrategy initialized: vault=%s, collateral=%s %s, LTV target=%s%%",
            self.market_id,
            self.collateral_amount,
            self.collateral_token,
            self.ltv_target * 100,
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Open the NFT-CDP atomically (or run the forced action)."""
        if self.force_action in ("supply", "repay"):
            # One-shot exactly like the forced open below: without the state
            # machine, a continuous run re-emits the same SUPPLY/REPAY on
            # every decide() after the first one succeeds.
            if self._loop_state != "idle":
                return Intent.hold(reason=f"Forced {self.force_action} already dispatched (state: {self._loop_state})")
            self._loop_state = "opening"
            if self.force_action == "supply":
                logger.info("Forced action: SUPPLY collateral into the vault")
                return self._create_supply_intent()
            logger.info("Forced action: REPAY %s %s", self.repay_amount, self.borrow_token)
            return self._create_repay_intent()

        try:
            collateral_price = market.price(self.collateral_token)
            borrow_price = market.price(self.borrow_token)
        except (ValueError, KeyError) as exc:
            logger.warning("Could not get prices: %s", exc)
            return Intent.hold(reason=f"Price data unavailable: {exc}")

        if self.force_action == "open":
            # Route the forced open through the same one-shot state machine
            # as the organic path: without this, a continuous run re-borrows
            # against the SAME nftId (more collateral + more debt) on every
            # decide() after the first open succeeds.
            if self._loop_state != "idle":
                return Intent.hold(reason=f"Forced open already dispatched (state: {self._loop_state})")
            logger.info("Forced action: ATOMIC OPEN (collateral + borrow in one operate())")
            intent = self._create_open_intent(collateral_price, borrow_price)
            # Only consume the one-shot when a real open was emitted — a
            # price-guard HOLD must stay retryable on the next cycle.
            if intent.intent_type.value == "BORROW":
                self._loop_state = "opening"
            return intent

        if self._loop_state == "idle":
            intent = self._create_open_intent(collateral_price, borrow_price)
            # Mirror the forced-open path: only consume the one-shot when a
            # real open was emitted — a price-guard HOLD must stay retryable
            # on the next cycle (a failed BORROW still re-arms via
            # on_intent_executed).
            if intent.intent_type.value == "BORROW":
                self._loop_state = "opening"
            return intent

        if self._loop_state == "complete":
            return Intent.hold(reason="Vault position established")

        # "opening" = an intent is in flight. Do NOT re-arm here: decide()
        # can be polled again before on_intent_executed() delivers the
        # verdict, and a premature idle reset would double-fire the open.
        # Failure re-arms via on_intent_executed (success -> complete).
        return Intent.hold(reason=f"Waiting for execution callback (state: {self._loop_state})")

    # =========================================================================
    # INTENT CREATION
    # =========================================================================

    def _create_open_intent(self, collateral_price: Decimal, borrow_price: Decimal) -> Intent:
        """ONE BorrowIntent = ONE on-chain operate(): mint + supply + borrow."""
        if collateral_price <= 0 or borrow_price <= 0:
            # A zero/negative oracle answer is unmeasured data, not a price —
            # sizing debt with it would divide by zero or borrow garbage.
            return Intent.hold(reason=f"Invalid price data (collateral={collateral_price}, borrow={borrow_price})")
        collateral_value = self.collateral_amount * collateral_price
        borrow_amount = (collateral_value * self.ltv_target / borrow_price).quantize(Decimal("0.01"))

        logger.info(
            "ATOMIC OPEN intent: collateral=%s (%s), LTV=%s%%, borrow=%s",
            format_token_amount_human(self.collateral_amount, self.collateral_token),
            format_usd(collateral_value),
            self.ltv_target * 100,
            format_token_amount_human(borrow_amount, self.borrow_token),
        )
        return Intent.borrow(
            protocol="fluid_vault",
            collateral_token=self.collateral_token,
            collateral_amount=self.collateral_amount,
            borrow_token=self.borrow_token,
            borrow_amount=borrow_amount,
            market_id=self.market_id,  # the vault address — REQUIRED for fluid_vault
            chain=self.chain,
        )

    def _create_supply_intent(self) -> Intent:
        """Add collateral to the position (mints when none exists yet)."""
        return Intent.supply(
            protocol="fluid_vault",
            token=self.collateral_token,
            amount=self.collateral_amount,
            market_id=self.market_id,
            chain=self.chain,
        )

    def _create_repay_intent(self) -> Intent:
        """Partial repay. The compiler refuses any amount above the live debt
        (the protocol's over-repay revert 31015 is unreachable from a compile
        that passed) — use repay_full=True for full closes instead."""
        return Intent.repay(
            protocol="fluid_vault",
            token=self.borrow_token,
            amount=self.repay_amount,
            market_id=self.market_id,
            chain=self.chain,
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track the dispatched lifecycle intent across cycles.

        BORROW = the (forced or organic) open; SUPPLY/REPAY = the forced
        one-shot actions. Success completes the loop; failure re-arms the
        state machine so the next cycle retries.
        """
        if intent.intent_type.value in ("BORROW", "SUPPLY", "REPAY"):
            if success:
                self._loop_state = "complete"
                logger.info("%s executed — loop complete", intent.intent_type.value)
            else:
                self._loop_state = "idle"
                logger.warning("%s failed — will retry next cycle", intent.intent_type.value)

    # =========================================================================
    # TEARDOWN (tutorial stub — no teardown logic in this demo)
    # =========================================================================

    def get_open_positions(self) -> TeardownPositionSummary:
        """Return empty position summary — this tutorial demo does not track teardown state."""
        return TeardownPositionSummary(
            deployment_id=getattr(self, "deployment_id", "demo_fluid_borrow"),
            timestamp=datetime.now(UTC),
            positions=[],
        )

    def generate_teardown_intents(self, mode: Any, market: Any = None) -> list[Intent]:
        """Return empty teardown list — teardown is not implemented in this tutorial demo."""
        return []
