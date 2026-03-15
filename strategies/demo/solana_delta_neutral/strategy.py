"""Solana Delta-Neutral Yield Farm.

Earns yield from three sources simultaneously while staying market-neutral:

  1. **LP fees** — Concentrated liquidity on Raydium CLMM (SOL/USDC)
  2. **Funding payments** — Short SOL-PERP on Drift hedges the SOL exposure
     from the LP and earns funding when rates are positive
  3. **Lending yield** — Idle USDC parked in Kamino earns supply APY

The strategy is delta-neutral: the SOL exposure from the LP position is offset
by the Drift short, so the PnL comes from fees/funding/yield, not price moves.

State machine phases (one intent per decide() call):
  IDLE          -> Supply USDC to Kamino              -> LENDING
  LENDING       -> Open LP on Raydium                 -> LP_OPEN
  LP_OPEN       -> Open short hedge on Drift          -> HEDGED
  HEDGED        -> Monitor; rebalance if out of range -> HEDGED / REBALANCING
  REBALANCING   -> Close old LP, reopen, adjust hedge -> HEDGED

Usage:
    almanak strat run -d strategies/demo/solana_delta_neutral --once --dry-run
    almanak strat run -d strategies/demo/solana_delta_neutral --interval 60

Environment:
    SOLANA_PRIVATE_KEY   Base58 Ed25519 keypair (required)
    SOLANA_RPC_URL       Solana RPC endpoint (optional)
"""

import logging
from decimal import Decimal

from almanak.framework.intents import Intent
from almanak.framework.intents.vocabulary import LPOpenIntent, SupplyIntent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)

# Strategy phases
IDLE = "IDLE"
LENDING = "LENDING"
LP_OPEN = "LP_OPEN"
HEDGED = "HEDGED"
REBALANCE_CLOSE_LP = "REBALANCE_CLOSE_LP"
REBALANCE_CLOSE_HEDGE = "REBALANCE_CLOSE_HEDGE"
REBALANCE_OPEN_LP = "REBALANCE_OPEN_LP"
REBALANCE_OPEN_HEDGE = "REBALANCE_OPEN_HEDGE"


@almanak_strategy(
    name="solana_delta_neutral",
    version="0.1.0",
    description="Delta-neutral yield farm: Raydium LP + Drift hedge + Kamino lending (demo)",
    supported_chains=["solana"],
    supported_protocols=["raydium_clmm", "drift", "kamino", "jupiter"],
    intent_types=["LP_OPEN", "LP_CLOSE", "PERP_OPEN", "PERP_CLOSE", "SUPPLY", "WITHDRAW", "SWAP"],
)
class SolanaDeltaNeutralStrategy(IntentStrategy):
    """Delta-neutral yield farm across Raydium, Drift, and Kamino.

    Each decide() call advances the state machine by one step. After ~3
    iterations the full position is established (lend -> LP -> hedge).
    Subsequent calls monitor and rebalance when price drifts out of range.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state: dict = {}

    def get_persistent_state(self) -> dict:
        return dict(self.state)

    def load_persistent_state(self, state: dict) -> None:
        self.state = dict(state or {})

    def decide(self, market: MarketSnapshot) -> Intent:
        phase = self.state.get("phase", IDLE)
        logger.info(f"Phase: {phase}")

        try:
            if phase == IDLE:
                return self._enter_lending()
            elif phase == LENDING:
                return self._open_lp()
            elif phase == LP_OPEN:
                return self._open_hedge()
            elif phase == HEDGED:
                return self._monitor_and_rebalance(market)
            elif phase == REBALANCE_CLOSE_LP:
                return self._rebalance_close_lp()
            elif phase == REBALANCE_CLOSE_HEDGE:
                return self._rebalance_close_hedge()
            elif phase == REBALANCE_OPEN_LP:
                return self._rebalance_open_lp(market)
            elif phase == REBALANCE_OPEN_HEDGE:
                return self._rebalance_open_hedge()
            else:
                return Intent.hold(reason=f"Unknown phase: {phase}")
        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    # -- Phase handlers --------------------------------------------------------

    def _enter_lending(self) -> Intent:
        """Phase IDLE -> LENDING: supply idle USDC to Kamino."""
        token = self.config.get("lending_token", "USDC")
        amount = Decimal(str(self.config.get("lending_amount", "5.0")))
        logger.info(f"Supplying {amount} {token} to Kamino")
        return SupplyIntent(protocol="kamino", token=token, amount=amount)

    def _open_lp(self) -> Intent:
        """Phase LENDING -> LP_OPEN: open concentrated LP on Raydium."""
        pool = self.config.get("lp_pool", "3ucNos4NbumPLZNWztqGHNFFgkHeRMBQAVemeeomsUxv")
        protocol = self.config.get("lp_protocol", "raydium_clmm")
        amount_sol = Decimal(str(self.config.get("lp_amount_sol", "0.005")))
        amount_usdc = Decimal(str(self.config.get("lp_amount_usdc", "0.75")))
        range_pct = Decimal(str(self.config.get("lp_range_pct", "10")))

        # Center range around current SOL price
        sol_price = self._get_sol_price()
        if sol_price is None:
            return Intent.hold(reason="Cannot determine SOL price — unsafe to open LP")
        range_lower = sol_price * (1 - range_pct / 100)
        range_upper = sol_price * (1 + range_pct / 100)

        logger.info(f"Opening LP: {amount_sol} SOL + {amount_usdc} USDC, range [{range_lower:.2f}, {range_upper:.2f}]")
        self.state["lp_range_lower"] = str(range_lower)
        self.state["lp_range_upper"] = str(range_upper)
        self.state["lp_entry_price"] = str(sol_price)

        return LPOpenIntent(
            protocol=protocol,
            pool=pool,
            amount0=amount_sol,
            amount1=amount_usdc,
            range_lower=range_lower,
            range_upper=range_upper,
        )

    def _open_hedge(self) -> Intent:
        """Phase LP_OPEN -> HEDGED: short SOL-PERP to hedge LP SOL exposure.

        Hedge size is derived from the LP's SOL exposure so the position
        stays delta-neutral.  collateral = size_usd / leverage.
        """
        market = self.config.get("perp_market", "SOL-PERP")
        leverage = Decimal(str(self.config.get("perp_leverage", "2.0")))

        # Derive hedge notional from LP SOL exposure
        lp_amount_sol = Decimal(str(self.config.get("lp_amount_sol", "0.005")))
        sol_price = self._get_sol_price()
        if sol_price is None:
            return Intent.hold(reason="Cannot determine SOL price — unsafe to open hedge")
        size_usd = lp_amount_sol * sol_price
        collateral = size_usd / leverage

        logger.info(f"Opening SHORT hedge: {market}, size=${size_usd:.2f} (LP SOL={lp_amount_sol}), leverage={leverage}x")

        return Intent.perp_open(
            market=market,
            collateral_token="USDC",
            collateral_amount=collateral,
            size_usd=size_usd,
            is_long=False,
            leverage=leverage,
            protocol="drift",
        )

    def _monitor_and_rebalance(self, market: MarketSnapshot) -> Intent:
        """Phase HEDGED: check if LP range needs rebalancing."""
        sol_price = self._get_sol_price()
        if sol_price is None:
            return Intent.hold(reason="Cannot determine SOL price — holding until price available")
        range_lower = Decimal(self.state.get("lp_range_lower", "0"))
        range_upper = Decimal(self.state.get("lp_range_upper", "0"))
        drift_pct = Decimal(str(self.config.get("rebalance_drift_pct", "8")))

        if range_lower == 0 or range_upper == 0:
            return Intent.hold(reason="No LP range in state, holding")

        range_mid = (range_lower + range_upper) / 2
        price_drift = abs(sol_price - range_mid) / range_mid * 100

        # Check funding rate — if very negative, warn (we're short)
        funding = self._get_funding_rate()
        funding_str = f"{funding:.6f}" if funding is not None else "unavailable"

        logger.info(
            f"SOL=${sol_price:.2f}, range=[{range_lower:.2f}, {range_upper:.2f}], "
            f"drift={price_drift:.1f}%, funding={funding_str}"
        )

        # Price moved out of range — start rebalance
        if sol_price < range_lower or sol_price > range_upper:
            logger.info("Price out of LP range, starting rebalance")
            self.state["phase"] = REBALANCE_CLOSE_LP
            return self._rebalance_close_lp()

        # Price drifting significantly — preemptive rebalance
        if price_drift > drift_pct:
            logger.info(f"Price drift {price_drift:.1f}% > {drift_pct}%, preemptive rebalance")
            self.state["phase"] = REBALANCE_CLOSE_LP
            return self._rebalance_close_lp()

        return Intent.hold(
            reason=(
                f"Delta-neutral position healthy. SOL=${sol_price:.2f}, drift={price_drift:.1f}%, funding={funding_str}"
            )
        )

    def _rebalance_close_lp(self) -> Intent:
        """Rebalance step 1: close old LP position."""
        position_id = self.state.get("lp_position_id", "")
        if not position_id:
            logger.warning("Cannot close LP: position_id missing from state")
            return Intent.hold(reason="LP position_id missing — cannot close")
        pool = self.state.get("lp_pool", self.config.get("lp_pool", ""))
        protocol = self.config.get("lp_protocol", "raydium_clmm")
        logger.info(f"Rebalance: closing LP position {position_id}")
        return Intent.lp_close(
            protocol=protocol,
            position_id=position_id,
            pool=pool,
            collect_fees=True,
        )

    def _rebalance_close_hedge(self) -> Intent:
        """Rebalance step 2: close old perp hedge."""
        market = self.config.get("perp_market", "SOL-PERP")
        logger.info(f"Rebalance: closing hedge on {market}")
        return Intent.perp_close(
            market=market,
            collateral_token="USDC",
            is_long=False,
            protocol="drift",
        )

    def _rebalance_open_lp(self, market: MarketSnapshot) -> Intent:
        """Rebalance step 3: open new LP at current price."""
        return self._open_lp()

    def _rebalance_open_hedge(self) -> Intent:
        """Rebalance step 4: reopen perp hedge."""
        return self._open_hedge()

    # -- Execution callbacks ---------------------------------------------------

    def on_intent_executed(self, intent, success: bool, result):
        """Advance state machine after each successful execution."""
        if not success:
            logger.warning(f"Intent failed, staying in phase {self.state.get('phase', IDLE)}")
            return

        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return
        type_value = intent_type.value if hasattr(intent_type, "value") else str(intent_type)
        phase = self.state.get("phase", IDLE)

        if type_value == "SUPPLY" and phase == IDLE:
            self.state["phase"] = LENDING
            self.state["has_lending"] = True
            logger.info("Phase -> LENDING (Kamino supply done)")

        elif type_value == "LP_OPEN" and phase in (LENDING, REBALANCE_OPEN_LP):
            position_id = result.position_id if result else None
            if not position_id:
                logger.warning(f"LP_OPEN succeeded but position_id missing — skipping state advance (phase={phase})")
                return
            self.state["lp_position_id"] = str(position_id)
            self.state["lp_pool"] = getattr(intent, "pool", "")
            next_phase = LP_OPEN if phase == LENDING else REBALANCE_OPEN_HEDGE
            self.state["phase"] = next_phase
            self.state["has_lp"] = True
            logger.info(f"Phase -> {next_phase} (LP opened, id={position_id})")

        elif type_value == "PERP_OPEN" and phase in (LP_OPEN, REBALANCE_OPEN_HEDGE):
            self.state["phase"] = HEDGED
            self.state["has_hedge"] = True
            logger.info("Phase -> HEDGED (delta-neutral position established)")

        elif type_value == "LP_CLOSE" and phase == REBALANCE_CLOSE_LP:
            self.state["phase"] = REBALANCE_CLOSE_HEDGE
            self.state["has_lp"] = False
            self.state["lp_position_id"] = None
            logger.info("Phase -> REBALANCE_CLOSE_HEDGE (LP closed)")

        elif type_value == "PERP_CLOSE" and phase == REBALANCE_CLOSE_HEDGE:
            self.state["phase"] = REBALANCE_OPEN_LP
            self.state["has_hedge"] = False
            logger.info("Phase -> REBALANCE_OPEN_LP (hedge closed)")

    # -- Data helpers ----------------------------------------------------------

    def _get_sol_price(self) -> Decimal | None:
        """Get SOL price from Drift oracle data.

        Returns None if price cannot be determined — callers must handle
        this by holding rather than proceeding with a stale price.
        """
        # GATEWAY_VIOLATION: Direct HTTP call to Drift Data API.
        # SOL price should come from market.price("SOL") via gateway.
        # TODO: Use MarketSnapshot price provider once gateway supports Solana prices.
        try:
            from almanak.framework.connectors.drift import PERP_MARKET_SYMBOL_TO_INDEX, DriftDataClient

            client = DriftDataClient()
            market_index = PERP_MARKET_SYMBOL_TO_INDEX.get("SOL-PERP", 0)
            price = client.get_oracle_price(market_index)
            if price is not None:
                return price
        except Exception as e:
            logger.warning(f"Failed to fetch SOL price from Drift: {e}")
        logger.warning("SOL price unavailable — cannot proceed safely")
        return None

    def _get_funding_rate(self) -> Decimal | None:
        """Get SOL-PERP funding rate from Drift.

        Returns None if funding rate cannot be determined — callers must
        distinguish "unknown" from "zero" to avoid acting on fabricated data.
        """
        # GATEWAY_VIOLATION: Direct HTTP call to Drift Data API.
        # TODO: Add funding rate provider to gateway MarketSnapshot.
        try:
            from almanak.framework.connectors.drift import PERP_MARKET_SYMBOL_TO_INDEX, DriftDataClient

            market = self.config.get("perp_market", "SOL-PERP")
            client = DriftDataClient()
            market_index = PERP_MARKET_SYMBOL_TO_INDEX.get(market.upper(), 0)
            rates = client.get_funding_rates(market_index)
            if rates:
                return rates[0].funding_rate
        except Exception as e:
            logger.warning(f"Failed to fetch funding rate: {e}")
        return None

    # -- Teardown (required by framework) --------------------------------------

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self):
        from datetime import UTC, datetime

        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []

        if self.state.get("has_hedge"):
            positions.append(
                PositionInfo(
                    position_type=PositionType.PERP,
                    position_id="drift_hedge",
                    chain="solana",
                    protocol="drift",
                    value_usd=Decimal("0"),
                    details={
                        "market": self.config.get("perp_market", "SOL-PERP"),
                        "direction": "short",
                    },
                )
            )

        if self.state.get("has_lp"):
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=self.state.get("lp_position_id", "raydium_lp"),
                    chain="solana",
                    protocol=self.config.get("lp_protocol", "raydium_clmm"),
                    value_usd=Decimal("0"),
                    details={"pool": self.state.get("lp_pool", "")},
                )
            )

        if self.state.get("has_lending"):
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id="kamino_supply",
                    chain="solana",
                    protocol="kamino",
                    value_usd=Decimal("0"),
                    details={"token": self.config.get("lending_token", "USDC")},
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode, market=None):
        """Unwind in reverse order: hedge -> lending -> LP -> swap remaining SOL."""
        from almanak.framework.teardown import TeardownMode

        intents = []
        hard = mode == TeardownMode.HARD

        # 1. Close perp hedge first (removes directional risk)
        if self.state.get("has_hedge"):
            intents.append(
                Intent.perp_close(
                    market=self.config.get("perp_market", "SOL-PERP"),
                    collateral_token="USDC",
                    is_long=False,
                    protocol="drift",
                )
            )

        # 2. Withdraw from Kamino (SUPPLY)
        if self.state.get("has_lending"):
            intents.append(
                Intent.withdraw(
                    protocol="kamino",
                    token=self.config.get("lending_token", "USDC"),
                    amount="all",
                    withdraw_all=True,
                )
            )

        # 3. Close LP position (LP)
        if self.state.get("has_lp"):
            intents.append(
                Intent.lp_close(
                    protocol=self.config.get("lp_protocol", "raydium_clmm"),
                    position_id=self.state.get("lp_position_id", ""),
                    pool=self.state.get("lp_pool", ""),
                    collect_fees=True,
                )
            )

        # 4. Swap any remaining SOL back to USDC (TOKEN)
        intents.append(
            Intent.swap(
                from_token="SOL",
                to_token="USDC",
                amount="all",
                max_slippage=Decimal("0.03") if hard else Decimal("0.01"),
            )
        )

        return intents
