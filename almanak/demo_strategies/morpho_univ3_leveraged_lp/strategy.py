"""Morpho Blue + Uniswap V3 Leveraged LP Strategy on Ethereum.

Composes Morpho Blue lending with Uniswap V3 concentrated liquidity:
1. Supply wstETH as collateral on Morpho Blue
2. Borrow USDC against the collateral
3. Open a concentrated WETH/USDC LP position on Uniswap V3

The borrowed USDC, combined with existing WETH, provides liquidity in a
concentrated range around current price. This is a T2 (lending + LP)
composition -- the first on Ethereum mainnet.

Teardown order:
1. Close LP position (LP_CLOSE)
2. Repay USDC borrow (REPAY)
3. Withdraw wstETH collateral (WITHDRAW)
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="demo_morpho_univ3_leveraged_lp",
    description="Morpho Blue + Uniswap V3 leveraged LP on Ethereum",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "morpho", "uniswap-v3", "lending", "lp", "leverage", "ethereum"],
    supported_chains=["ethereum"],
    supported_protocols=["morpho_blue", "uniswap_v3"],
    intent_types=["SUPPLY", "BORROW", "SWAP", "LP_OPEN", "LP_CLOSE", "REPAY", "WITHDRAW", "HOLD"],
    default_chain="ethereum",
)
class MorphoUniV3LeveragedLPStrategy(IntentStrategy):
    """Leveraged LP: borrow against Morpho Blue collateral, LP on Uniswap V3."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Morpho Blue config
        self.market_id = self.get_config(
            "market_id",
            "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
        )
        self.collateral_token = self.get_config("collateral_token", "wstETH")
        self.borrow_token = self.get_config("borrow_token", "USDC")
        self.collateral_amount = Decimal(self.get_config("collateral_amount", "0.014"))
        self.target_ltv = Decimal(self.get_config("target_ltv", "0.50"))
        self.min_health_factor = Decimal(self.get_config("min_health_factor", "1.5"))

        # LP config
        self.lp_pool = self.get_config("lp_pool", "WETH/USDC/500")
        self.lp_range_width_pct = Decimal(self.get_config("lp_range_width_pct", "0.20"))
        self.swap_slippage = Decimal(self.get_config("swap_slippage", "0.005"))

        # State machine: idle -> supplying -> borrowing -> opening_lp -> active -> holding
        self._state = "idle"
        self._collateral_supplied = Decimal("0")
        self._borrowed_amount = Decimal("0")
        self._lp_position_id: int | None = None

        # force_action for testing (supply, borrow, lp_open, or empty)
        self._force_action = self.get_config("force_action", "")

        logger.info(
            f"MorphoUniV3LeveragedLP initialized: "
            f"collateral={self.collateral_amount} {self.collateral_token}, "
            f"pool={self.lp_pool}, target_ltv={self.target_ltv}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """State machine: supply -> borrow -> open LP."""
        # Force action override for testing
        if self._force_action:
            return self._handle_forced_action(market)

        try:
            if self._state == "idle":
                return self._start_supply()
            elif self._state == "supplying":
                return Intent.hold(reason="Waiting for supply confirmation")
            elif self._state == "supplied":
                return self._start_borrow(market)
            elif self._state == "borrowing":
                return Intent.hold(reason="Waiting for borrow confirmation")
            elif self._state == "borrowed":
                return self._open_lp(market)
            elif self._state == "opening_lp":
                return Intent.hold(reason="Waiting for LP open confirmation")
            elif self._state == "active":
                return Intent.hold(reason="Position active, monitoring")
            else:
                return Intent.hold(reason=f"Unknown state: {self._state}")
        except (ValueError, KeyError) as e:
            return Intent.hold(reason=f"Price data unavailable: {e}")

    def _handle_forced_action(self, market: MarketSnapshot) -> Intent | None:
        action = str(self._force_action).lower().strip()
        self._force_action = ""  # Clear after use

        # Normalize boolean true to default action (supply starts the cycle)
        if action in ("true", "1"):
            action = "supply"

        if action == "supply":
            return self._start_supply()
        elif action == "borrow":
            return self._start_borrow(market)
        elif action in ("lp_open", "open"):
            return self._open_lp(market)
        else:
            logger.warning(f"Unknown force_action: {action}")
            return Intent.hold(reason=f"Unknown force_action: {action}")

    def _start_supply(self) -> Intent:
        """Supply collateral to Morpho Blue."""
        self._state = "supplying"
        logger.info(f"Supplying {self.collateral_amount} {self.collateral_token} to Morpho Blue")
        return Intent.supply(
            protocol="morpho_blue",
            token=self.collateral_token,
            amount=self.collateral_amount,
            market_id=self.market_id,
            chain=self.chain,
        )

    def _start_borrow(self, market: MarketSnapshot) -> Intent:
        """Borrow against collateral."""
        collateral_price = market.price(self.collateral_token)
        borrow_price = market.price(self.borrow_token)
        collateral_value_usd = self.collateral_amount * collateral_price
        borrow_value_usd = collateral_value_usd * self.target_ltv

        # Cap borrow to respect min_health_factor (HF = collateral_value / borrow_value)
        max_borrow_value = collateral_value_usd / self.min_health_factor
        if borrow_value_usd > max_borrow_value:
            logger.warning(
                f"target_ltv={self.target_ltv} would breach min_health_factor={self.min_health_factor}, "
                f"capping borrow from ${borrow_value_usd:.2f} to ${max_borrow_value:.2f}"
            )
            borrow_value_usd = max_borrow_value

        # Divide by borrow token price to handle stablecoin depeg scenarios
        borrow_amount = (borrow_value_usd / borrow_price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

        self._state = "borrowing"
        logger.info(
            f"Borrowing {borrow_amount} {self.borrow_token} "
            f"(LTV={self.target_ltv}, collateral=${collateral_value_usd:,.2f}, "
            f"borrow_price=${borrow_price})"
        )
        return Intent.borrow(
            protocol="morpho_blue",
            collateral_token=self.collateral_token,
            collateral_amount=Decimal("0"),  # Already supplied
            borrow_token=self.borrow_token,
            borrow_amount=borrow_amount,
            market_id=self.market_id,
            chain=self.chain,
        )

    def _open_lp(self, market: MarketSnapshot) -> Intent:
        """Open concentrated LP position with borrowed USDC and existing WETH."""
        weth_price = market.price("WETH")
        half_width = self.lp_range_width_pct / 2
        range_lower = weth_price * (Decimal("1") - half_width)
        range_upper = weth_price * (Decimal("1") + half_width)

        # Use a small WETH amount alongside borrowed USDC
        # The USDC side is the borrowed amount; WETH side from wallet balance
        weth_amount = Decimal("0.005")
        if self._borrowed_amount <= 0:
            logger.error("Cannot open LP: no borrowed amount recorded")
            return Intent.hold(reason="Cannot open LP without borrow amount")
        usdc_amount = self._borrowed_amount

        self._state = "opening_lp"
        logger.info(
            f"Opening LP: {weth_amount} WETH + {usdc_amount} USDC, "
            f"range [{range_lower:,.2f} - {range_upper:,.2f}]"
        )
        return Intent.lp_open(
            pool=self.lp_pool,
            amount0=weth_amount,
            amount1=usdc_amount,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="uniswap_v3",
            chain=self.chain,
        )

    def on_intent_executed(self, intent, success: bool, result) -> None:
        """Handle intent execution results and advance state machine."""
        if not success:
            logger.error(f"Intent failed in state {self._state}: {getattr(result, 'error', 'unknown')}")
            # Revert to previous stable state
            if self._state == "supplying":
                self._state = "idle"
            elif self._state == "borrowing":
                self._state = "supplied"
            elif self._state == "opening_lp":
                self._state = "borrowed"
            return

        # Teardown unwind intents clear the corresponding cached position state.
        # Without this, get_open_positions() keeps reporting "positions still open"
        # after a successful teardown because LP_CLOSE/REPAY/WITHDRAW never reset
        # the counters. VIB-3738.
        intent_type = getattr(intent, "intent_type", None)
        intent_type_val = (
            intent_type.value if hasattr(intent_type, "value") else str(intent_type) if intent_type else ""
        )
        if intent_type_val == "LP_CLOSE":
            logger.info(f"LP_CLOSE confirmed: clearing cached LP position #{self._lp_position_id}")
            self._lp_position_id = None
            return
        if intent_type_val == "REPAY":
            logger.info(f"REPAY confirmed: clearing cached borrow {self._borrowed_amount} {self.borrow_token}")
            self._borrowed_amount = Decimal("0")
            return
        if intent_type_val == "WITHDRAW":
            logger.info(
                f"WITHDRAW confirmed: clearing cached collateral "
                f"{self._collateral_supplied} {self.collateral_token}"
            )
            self._collateral_supplied = Decimal("0")
            return

        if self._state == "supplying":
            self._collateral_supplied = self.collateral_amount
            self._state = "supplied"
            logger.info(f"Supply confirmed. Collateral: {self._collateral_supplied} {self.collateral_token}")

        elif self._state == "borrowing":
            # Extract borrow amount from result or intent
            intent_amount = getattr(intent, "borrow_amount", None)
            if intent_amount:
                self._borrowed_amount = Decimal(str(intent_amount))
            self._state = "borrowed"
            logger.info(f"Borrow confirmed: {self._borrowed_amount} {self.borrow_token}")

        elif self._state == "opening_lp":
            # Extract position ID from enriched result
            position_id = getattr(result, "position_id", None)
            if position_id:
                self._lp_position_id = int(position_id)
                logger.info(f"LP position opened: NFT #{self._lp_position_id}")
                logger.info("Strategy fully deployed: Morpho collateral + Uniswap V3 LP active")
            else:
                # LP succeeded on-chain but enrichment failed -- move to active to
                # prevent opening a duplicate LP. Teardown will skip LP close since
                # position_id is unknown; operator must close it manually.
                logger.error(
                    "LP_OPEN succeeded but no position_id in result -- "
                    "moving to active to prevent duplicate LP opens. "
                    "Manual intervention required to close the untracked LP."
                )
            self._state = "active"

    # =========================================================================
    # STATE PERSISTENCE
    # =========================================================================

    def get_persistent_state(self) -> dict:
        """Serialize strategy state for persistence across restarts."""
        return {
            "state": self._state,
            "collateral_supplied": str(self._collateral_supplied),
            "borrowed_amount": str(self._borrowed_amount),
            "lp_position_id": self._lp_position_id,
        }

    def load_persistent_state(self, state: dict) -> None:
        """Restore strategy state from persisted data."""
        if "state" in state:
            self._state = state["state"]
        if "collateral_supplied" in state:
            self._collateral_supplied = Decimal(str(state["collateral_supplied"]))
        if "borrowed_amount" in state:
            self._borrowed_amount = Decimal(str(state["borrowed_amount"]))
        if "lp_position_id" in state:
            self._lp_position_id = state["lp_position_id"]
        logger.info(f"Restored state: {self._state}, lp_position={self._lp_position_id}")

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def get_open_positions(self):
        """Return all open positions for teardown.

        Cached counters (`_lp_position_id`, `_collateral_supplied`,
        `_borrowed_amount`) are cleared by `on_intent_executed` when the
        corresponding teardown unwind intents (LP_CLOSE/REPAY/WITHDRAW) succeed,
        so a second call after a clean teardown returns no positions.

        Note: a strategy restart with stale persisted state would still report
        cached positions until the next on-chain truth check; production
        strategies should additionally probe Morpho `position(market_id, user)`
        and the Uniswap V3 NFT manager `positions(tokenId).liquidity`. Tracked
        in the on-chain accounting umbrella (VIB-3738 / report Bug L+G3).
        """
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        # Estimate value_usd from cached amounts and live prices
        try:
            market = self.create_market_snapshot()
            collateral_price = Decimal(str(market.price(self.collateral_token)))
        except Exception:
            collateral_price = Decimal("0")

        positions = []

        if self._lp_position_id is not None:
            # Approximate LP value from borrowed USDC (both sides contribute)
            lp_value = self._borrowed_amount * 2 if self._borrowed_amount > 0 else Decimal("0")
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=f"univ3_lp_{self._lp_position_id}",
                    chain=self.chain,
                    protocol="uniswap_v3",
                    value_usd=lp_value,
                    details={"nft_id": self._lp_position_id, "pool": self.lp_pool},
                )
            )

        if self._collateral_supplied > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"morpho_collateral_{self.market_id[:10]}",
                    chain=self.chain,
                    protocol="morpho_blue",
                    value_usd=self._collateral_supplied * collateral_price,
                    details={
                        "market_id": self.market_id,
                        "collateral": str(self._collateral_supplied),
                        "borrowed": str(self._borrowed_amount),
                    },
                )
            )

        if self._borrowed_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"morpho_borrow_{self.market_id[:10]}",
                    chain=self.chain,
                    protocol="morpho_blue",
                    value_usd=self._borrowed_amount,
                    details={
                        "market_id": self.market_id,
                        "borrowed": str(self._borrowed_amount),
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None) -> list[Intent]:
        """Generate intents to unwind: close LP -> swap proceeds -> repay -> withdraw.

        Teardown order is critical for safety:
        1. Close LP to recover WETH + USDC
        2. Swap WETH proceeds to USDC (so REPAY has enough USDC even after swap fees)
        3. Repay borrowed USDC (frees collateral)
        4. Withdraw wstETH collateral

        The proceeds-swap is gated on actual wallet WETH balance rather than
        `_lp_position_id`. After a partial-failure retry where LP_CLOSE
        succeeded but a downstream intent failed, `_lp_position_id` is already
        cleared but the wallet still holds the unconverted WETH; gating on the
        on-chain balance keeps the proceeds-swap on the retry path. (Codex P2 /
        Claude #7).
        """
        from almanak.framework.teardown import TeardownMode

        intents = []
        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else self.swap_slippage

        # Step 1: Close LP position
        if self._lp_position_id is not None:
            intents.append(
                Intent.lp_close(
                    pool=self.lp_pool,
                    position_id=str(self._lp_position_id),
                    protocol="uniswap_v3",
                    chain=self.chain,
                )
            )

        # Step 2: Swap WETH from LP proceeds to USDC for repayment.
        # Gate on either (a) an LP still tracked, or (b) wallet already holds WETH
        # (retry case after LP closed but later intent failed). amount="all"
        # is safe because each strategy has an isolated 1:1 wallet (gateway
        # architecture), so no unrelated WETH exists in this wallet.
        wallet_has_weth = self._lp_position_id is not None
        if not wallet_has_weth:
            # If the runner didn't pass a market snapshot, build one ourselves.
            # Without this fallback, the retry path (where _lp_position_id has
            # already been cleared by a successful LP_CLOSE) would skip the
            # WETH→USDC swap entirely and leave the borrow stuck (CodeRabbit
            # PR #1964 / Gemini feedback).
            snapshot = market
            if snapshot is None:
                try:
                    snapshot = self.create_market_snapshot()
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        f"Unable to build market snapshot for teardown retry: {exc!r}. "
                        "Conservatively assuming proceeds may exist and emitting swap."
                    )
                    wallet_has_weth = True

            if snapshot is not None and not wallet_has_weth:
                try:
                    weth_balance = snapshot.balance("WETH")
                    amount = (
                        weth_balance.balance
                        if hasattr(weth_balance, "balance")
                        else Decimal(str(weth_balance))
                    )
                    # 1e-9 WETH ≈ a few wei of dust — safely below any real LP
                    # proceeds but above rounding noise.
                    wallet_has_weth = Decimal(str(amount)) > Decimal("0.000000001")
                except (ValueError, KeyError, ConnectionError, TimeoutError) as exc:
                    logger.warning(
                        f"Unable to query on-chain WETH balance for teardown retry: {exc!r}. "
                        "Conservatively assuming proceeds may exist and emitting swap."
                    )
                    wallet_has_weth = True

        if wallet_has_weth:
            intents.append(
                Intent.swap(
                    from_token="WETH",
                    to_token=self.borrow_token,
                    amount="all",
                    max_slippage=max_slippage,
                    chain=self.chain,
                )
            )

        # Step 3: Repay all borrowed amount (repay_full=True uses shares-based
        # repay in the Morpho adapter, covering accrued interest automatically)
        if self._borrowed_amount > 0:
            intents.append(
                Intent.repay(
                    protocol="morpho_blue",
                    token=self.borrow_token,
                    amount=self._borrowed_amount,
                    repay_full=True,
                    market_id=self.market_id,
                    chain=self.chain,
                )
            )

        # Step 4: Withdraw all collateral
        if self._collateral_supplied > 0:
            intents.append(
                Intent.withdraw(
                    protocol="morpho_blue",
                    token=self.collateral_token,
                    amount=self._collateral_supplied,
                    withdraw_all=True,
                    market_id=self.market_id,
                    chain=self.chain,
                )
            )

        return intents
