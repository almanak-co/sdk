"""Aave OKB Supply-Borrow Loop on X-Layer.

Full lifecycle implementation of the spec at
``playground/xlayer/aave-uniswap/specs.md`` (ID cmnt8k6v2005saopppn2hx9bx).

Thesis
------
Lending protocols on new chains often offer implicit yield on native tokens.
Borrowing OKB against stable collateral is a capital-efficient way to gain
exposure to X-Layer's ecosystem yield opportunities, assuming the yield from
deploying OKB exceeds its borrow cost.

Mechanism
---------
1. Supply stable collateral to Aave V3.6 on X-Layer
2. Borrow a borrowable Aave reserve at 60% LTV (health factor target >= 1.6)
3. If the borrowed asset is neither LP leg, swap it 100% into ``lp_token1``
   (the stable side of the LP, default USDT). Using ``lp_token1`` as the
   conversion target lets the subsequent split reuse the WOKB/USDT pool
   we are about to LP into, avoiding exotic multi-hop route lookups.
4. Split into both LP legs:
     - If we now hold ``lp_token1`` (USDT): swap half to ``lp_token0`` (WOKB)
     - If we now hold ``lp_token0`` (WOKB): swap half to ``lp_token1`` (USDT)
5. Deposit WOKB + USDT into the Uniswap V3 WOKB/USDT/3000 CLMM pool
   around the current pair price with a configurable range width

Risks
-----
* Liquidation: a large OKB/collateral relative price move at 60% LTV
  triggers liquidation. The strategy holds ~40% headroom by default
  (``min_health_factor`` = 1.6).
* Impermanent Loss: OKB moves outside the CLMM range. Mitigated by the
  ``lp_range_width_pct`` setting (default +/-10%).
* Rate spikes: if OKB borrow APR exceeds CLMM fee APR, net yield can go
  negative. The spec calls for an exit when the spread drops below 5% for
  24 consecutive hours; the runner/operator is expected to trigger teardown
  when that external signal fires.

Deviations from spec on X-Layer
-------------------------------
1. The spec calls for supplying **WETH** as collateral. On X-Layer's Aave V3.6
   deployment, the ETH-equivalent reserve is ``xETH`` (address
   ``0xE7B000003A45145decf8a28FC755aD5eC5EA025A``) and it has very limited
   pool liquidity on Anvil forks. Default ``supply_token`` is **USDT0** (LTV=70%).
   Override to ``xETH`` or ``xBTC`` in ``config.json`` if you have live-chain
   access with sufficient liquidity.

2. The spec calls for **borrowing OKB**. On X-Layer's Aave V3.6 deployment,
   ``WOKB`` is listed as a reserve but has ``borrowingEnabled=false`` (verified
   on-chain: BORROW reverts with ``BorrowingNotEnabled()`` selector 0x53587745).
   The default ``borrow_token`` is therefore **USDG** (Gravity USD, borrow-side
   enabled), and the strategy adds a debt-asset conversion step that swaps the
   borrowed USDG into WOKB before the 50/50 LP split. If Aave governance later
   enables WOKB borrowing on X-Layer, set ``borrow_token: "WOKB"`` in
   ``config.json`` and the conversion step is automatically skipped.

X-Layer Aave V3.6 reserves (verified on-chain, iter 165):
  - USDT0 (USD_T0 0x779Ded...): LTV=70%, borrowingEnabled=true
  - xETH (0xE7B000...): LTV=70%, borrowingEnabled=true (limited liquidity)
  - xBTC (0xb7C000...): LTV=70%, borrowingEnabled=true
  - WOKB: LTV=0, borrowingEnabled=false (not usable as collateral or debt)
  - GHO, USDG: borrow-side only (LTV=0, borrowingEnabled=true)

Usage
-----
::

    almanak strat run -d strategies/xlayer/aave_okb_clmm_loop --network anvil --once
"""

import logging
import math
import time
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="xlayer_aave_okb_clmm_loop",
    description=(
        "Aave V3.6 supply + OKB borrow on X-Layer, redeployed into a "
        "Uniswap V3 WOKB/USDT CLMM position (spec cmnt8k6v2005saopppn2hx9bx)"
    ),
    version="0.1.0",
    author="Almanak",
    tags=["xlayer", "lending", "aave-v3", "uniswap-v3", "clmm", "carry", "yield"],
    supported_chains=["xlayer"],
    supported_protocols=["aave_v3", "uniswap_v3"],
    intent_types=["SUPPLY", "BORROW", "SWAP", "LP_OPEN", "LP_CLOSE", "REPAY", "WITHDRAW", "HOLD"],
    default_chain="xlayer",
)
class XLayerAaveOkbClmmLoopStrategy(IntentStrategy):
    """Aave supply -> borrow -> Uniswap V3 CLMM yield loop on X-Layer.

    State machine::

        idle
          -> supplying -> supplied          (Supply collateral to Aave)
          -> borrowing -> borrowed          (Borrow debt asset against collateral)
          -> converting -> converted        (Swap all debt asset -> lp_token1,
                                             skipped if borrow_token is already
                                             one of the LP legs)
          -> splitting -> split_done        (Swap half of whichever LP leg we hold
                                             to the other leg, 50/50 value split)
          -> opening_lp -> running          (Open CLMM position; monitor price)

        Rebalance loop (from running, when price exits range):
          running -> closing_lp -> lp_closed
          -> splitting -> split_done -> opening_lp -> running

    Configuration (``config.json``)::

        supply_token              Collateral symbol (default "USDT0")
        borrow_token              Debt symbol (default "USDG"; WOKB not borrowable)
        initial_supply_amount     Supply amount in supply_token units (default 1000.0)
        ltv_target                Target loan-to-value (default 0.60 per spec)
        min_health_factor         Safety floor for documentation (default 1.6)
        interest_rate_mode        "variable" (Aave V3 stable mode deprecated)
        lp_pool                   Pool identifier (default "WOKB/USDT/3000")
        lp_range_width_pct        Static fallback range width (used until enough
                                  price history is available for dynamic sizing)
        max_slippage              Max slippage on entry/teardown/rebalance swaps

        # --- Adaptive rebalance config ---
        dynamic_range             Enable vol-adaptive range width (default true)
        range_vol_multiplier      range_width = multiplier * realized_daily_vol
                                  (default 6.0 -> ~18 days expected time-in-range)
        vol_lookback_periods      Price samples for rolling vol (default 200)
        confirmation_cycles       Consecutive cycles price must stay outside range
                                  before rebalance fires — filters spikes (default 10)
        rebalance_cooldown_s      Minimum seconds between rebalances (default 3600)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.supply_token: str = self.get_config("supply_token", "USDT0")
        self.borrow_token: str = self.get_config("borrow_token", "USDG")
        self.initial_supply_amount = Decimal(str(self.get_config("initial_supply_amount", "1000.0")))
        self.ltv_target = Decimal(str(self.get_config("ltv_target", "0.6")))
        self.min_health_factor = Decimal(str(self.get_config("min_health_factor", "1.6")))
        self.interest_rate_mode: str = self.get_config("interest_rate_mode", "variable")

        pool_str: str = self.get_config("lp_pool", "WOKB/USDT/3000")
        parts = pool_str.split("/")
        self.lp_pool = pool_str
        self.lp_token0 = parts[0] if len(parts) > 0 else "WOKB"
        self.lp_token1 = parts[1] if len(parts) > 1 else "USDT"
        self.lp_fee_tier = int(parts[2]) if len(parts) > 2 else 3000

        self.lp_range_width_pct = Decimal(str(self.get_config("lp_range_width_pct", "0.20")))
        self.max_slippage = Decimal(str(self.get_config("max_slippage", "0.01")))

        # Adaptive rebalance config
        self.dynamic_range_enabled: bool = bool(self.get_config("dynamic_range", True))
        self.range_vol_multiplier = Decimal(str(self.get_config("range_vol_multiplier", "6.0")))
        self.vol_lookback_periods: int = int(self.get_config("vol_lookback_periods", "200"))
        self.confirmation_cycles: int = int(self.get_config("confirmation_cycles", "10"))
        self.rebalance_cooldown_s: int = int(self.get_config("rebalance_cooldown_s", "3600"))

        # State tracking
        self._state: str = "idle"
        self._previous_stable_state: str = "idle"
        self._total_supplied: Decimal = Decimal("0")
        self._total_borrowed: Decimal = Decimal("0")
        self._lp_amount_token0: Decimal = Decimal("0")
        self._lp_amount_token1: Decimal = Decimal("0")
        self._lp_range_lower: Decimal | None = None
        self._lp_range_upper: Decimal | None = None
        self._position_id: str | None = None
        self._rebalance_count: int = 0

        # Adaptive rebalance state
        self._price_history: list[float] = []      # rolling window of pair prices
        self._range_exit_count: int = 0             # consecutive cycles outside range
        self._last_rebalance_ts: float = 0.0        # epoch seconds of last rebalance
        self._realized_vol: Decimal | None = None   # latest daily vol estimate
        self._active_range_width: Decimal | None = None  # current dynamic range width

        # Teardown queue (populated by force_teardown config flag)
        self._teardown_queue: list | None = None

        # Cached prices (last observed) for teardown USD estimates
        self._supply_price_usd: Decimal = Decimal("1")
        self._borrow_price_usd: Decimal = Decimal("1")
        self._lp_token0_price_usd: Decimal = Decimal("1")
        self._lp_token1_price_usd: Decimal = Decimal("1")

        logger.info(
            f"XLayerAaveOkbClmmLoop init: supply={self.initial_supply_amount} {self.supply_token}, "
            f"borrow={self.borrow_token}, LTV={self.ltv_target * 100:.0f}%, "
            f"lp_pool={self.lp_pool}, range={'dynamic' if self.dynamic_range_enabled else f'{self.lp_range_width_pct * 100:.0f}%'}, "
            f"confirmation={self.confirmation_cycles} cycles, cooldown={self.rebalance_cooldown_s}s"
        )

    # ------------------------------------------------------------------
    # decide() — state machine driver
    # ------------------------------------------------------------------

    def decide(self, market: MarketSnapshot) -> Intent | None:
        # Refresh cached prices every cycle so teardown has fresh USD quotes.
        try:
            self._supply_price_usd = Decimal(str(market.price(self.supply_token)))
            self._borrow_price_usd = Decimal(str(market.price(self.borrow_token)))
            self._lp_token0_price_usd = Decimal(str(market.price(self.lp_token0)))
            self._lp_token1_price_usd = Decimal(str(market.price(self.lp_token1)))
        except (ValueError, KeyError) as e:
            return Intent.hold(reason=f"Price data unavailable: {e}")

        if min(
            self._supply_price_usd,
            self._borrow_price_usd,
            self._lp_token0_price_usd,
            self._lp_token1_price_usd,
        ) <= 0:
            return Intent.hold(reason="One or more token prices are non-positive, waiting")

        # -- Force teardown: operator sets force_teardown=true in config.json
        #    to trigger a clean unwind of all positions.
        if self.get_config("force_teardown", False) and self._state == "running":
            from almanak.framework.teardown import TeardownMode

            if self._teardown_queue is None:
                self._teardown_queue = self.generate_teardown_intents(TeardownMode.SOFT)
                logger.info(f"TEARDOWN initiated: {len(self._teardown_queue)} intents queued")
            if self._teardown_queue:
                intent = self._teardown_queue.pop(0)
                logger.info(f"TEARDOWN step: {intent.intent_type.value} ({len(self._teardown_queue)} remaining)")
                return intent
            self._state = "torn_down"
            return Intent.hold(reason="Teardown complete — all positions closed")

        if self._state == "torn_down":
            return Intent.hold(reason="Strategy torn down — restart with force_teardown=false to re-enter")

        # -- Transitional states: wait for on_intent_executed callback
        if self._state in ("supplying", "borrowing", "converting", "splitting", "opening_lp", "closing_lp"):
            return Intent.hold(reason=f"Waiting for {self._state} completion")

        # Steady-state: LP open, monitor price and rebalance if out of range
        if self._state == "running":
            return self._monitor_and_maybe_rebalance()

        # After LP close (rebalance): re-split and re-open at new range
        if self._state == "lp_closed":
            return self._build_split_swap_intent(market)

        # Step 1: supply collateral
        if self._state == "idle":
            return self._build_supply_intent(market)

        # Step 2: borrow against the freshly-supplied collateral
        if self._state == "supplied":
            return self._build_borrow_intent()

        # Step 3a: convert debt asset -> one of the LP legs. We target lp_token1
        # (the stable side, e.g. USDT) so the subsequent split can reuse the
        # already-verified WOKB/USDT pool. Skipped if borrow_token is already
        # one of the LP legs.
        if self._state == "borrowed":
            borrow_upper = self.borrow_token.upper()
            if borrow_upper in (self.lp_token0.upper(), self.lp_token1.upper()):
                # Already holding one of the LP legs — no conversion needed.
                self._state = "converted"
            else:
                return self._build_debt_convert_intent()

        # Step 3b: split whichever LP leg we hold into a 50/50 value pair
        if self._state == "converted":
            return self._build_split_swap_intent(market)

        # Step 4: open the WOKB/USDT CLMM position
        if self._state == "split_done":
            return self._build_lp_open_intent(market)

        return Intent.hold(reason=f"Unhandled state={self._state}")

    # ------------------------------------------------------------------
    # Intent builders
    # ------------------------------------------------------------------

    def _build_supply_intent(self, market: MarketSnapshot) -> Intent:
        supply_amount = self.initial_supply_amount
        try:
            balance = market.balance(self.supply_token)
            available = balance.balance if hasattr(balance, "balance") else balance
            if available < supply_amount:
                logger.warning(
                    f"Requested supply {supply_amount} {self.supply_token} "
                    f"exceeds wallet balance {available}; capping."
                )
                supply_amount = Decimal(str(available))
            if supply_amount <= 0:
                return Intent.hold(reason=f"No {self.supply_token} available to supply")
        except (ValueError, KeyError):
            return Intent.hold(reason=f"Cannot verify {self.supply_token} balance, waiting")

        self._previous_stable_state = self._state
        self._state = "supplying"
        self._pending_supply_amount = supply_amount  # latched for callback

        logger.info(f"SUPPLY {supply_amount} {self.supply_token} as Aave collateral")
        return Intent.supply(
            protocol="aave_v3",
            token=self.supply_token,
            amount=supply_amount,
            use_as_collateral=True,
            chain=self.chain,
        )

    def _build_borrow_intent(self) -> Intent:
        collateral_value_usd = self._total_supplied * self._supply_price_usd
        borrow_value_usd = collateral_value_usd * self.ltv_target
        borrow_amount = (borrow_value_usd / self._borrow_price_usd).quantize(
            Decimal("0.000001"), rounding=ROUND_DOWN
        )

        if borrow_amount <= 0:
            logger.warning("Borrow amount rounds to zero — marking loop running without debt")
            self._state = "running"
            return Intent.hold(reason="Borrow amount too small, skipping CLMM leg")

        self._previous_stable_state = self._state
        self._state = "borrowing"
        self._pending_borrow_amount = borrow_amount  # latched for callback

        logger.info(
            f"BORROW {borrow_amount} {self.borrow_token} "
            f"(LTV {self.ltv_target * 100:.0f}% on ${collateral_value_usd:.2f} collateral, "
            f"HF target >= {self.min_health_factor})"
        )
        return Intent.borrow(
            protocol="aave_v3",
            collateral_token=self.supply_token,
            collateral_amount=Decimal("0"),  # already supplied in step 1
            borrow_token=self.borrow_token,
            borrow_amount=borrow_amount,
            interest_rate_mode=self.interest_rate_mode,
            chain=self.chain,
        )

    def _build_debt_convert_intent(self) -> Intent:
        # Swap ALL borrowed debt asset -> lp_token1 (USDT). We target the
        # "stable side" of the LP because the subsequent split step can then
        # reuse the already-verified WOKB/USDT pool (the same pool we are
        # LP'ing into) instead of asking the compiler to search for an exotic
        # route. Used when we cannot directly borrow either LP leg (WOKB is
        # not borrowable on X-Layer Aave V3.6).
        swap_amount = self._total_borrowed.quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
        if swap_amount <= 0:
            self._state = "running"
            return Intent.hold(reason="Debt conversion amount too small, skipping LP leg")

        self._previous_stable_state = self._state
        self._state = "converting"

        logger.info(
            f"SWAP {swap_amount} {self.borrow_token} -> {self.lp_token1} "
            f"(100% debt-asset conversion, slippage={self.max_slippage * 100:.2f}%)"
        )
        return Intent.swap(
            from_token=self.borrow_token,
            to_token=self.lp_token1,
            amount=swap_amount,
            max_slippage=self.max_slippage,
            chain=self.chain,
        )

    def _build_split_swap_intent(self, market: MarketSnapshot) -> Intent:
        # Split our current LP-leg holding 50/50 by USD value. Reads live wallet
        # balances for both legs and swaps half of whichever side we hold more
        # of (by USD value) into the other side. Works for both entry paths:
        #   - After conversion: wallet holds lp_token1 -> swap half -> lp_token0
        #   - borrow_token == lp_token0: wallet holds lp_token0 -> swap half -> lp_token1
        try:
            tok0_bal = market.balance(self.lp_token0)
            tok1_bal = market.balance(self.lp_token1)
            tok0_amount = Decimal(str(tok0_bal.balance if hasattr(tok0_bal, "balance") else tok0_bal))
            tok1_amount = Decimal(str(tok1_bal.balance if hasattr(tok1_bal, "balance") else tok1_bal))
        except (ValueError, KeyError) as e:
            return Intent.hold(reason=f"Cannot read LP-leg balances for split: {e}")

        tok0_value_usd = tok0_amount * self._lp_token0_price_usd
        tok1_value_usd = tok1_amount * self._lp_token1_price_usd

        # Whichever leg has more USD value is the "heavy" side; swap half to the other.
        if tok0_value_usd >= tok1_value_usd:
            from_tok, to_tok, heavy_amount = self.lp_token0, self.lp_token1, tok0_amount
        else:
            from_tok, to_tok, heavy_amount = self.lp_token1, self.lp_token0, tok1_amount

        swap_amount = (heavy_amount / Decimal("2")).quantize(
            Decimal("0.000001"), rounding=ROUND_DOWN
        )
        if swap_amount <= 0:
            self._state = "running"
            return Intent.hold(
                reason=f"Split swap amount too small ({from_tok}={heavy_amount}), skipping LP leg"
            )

        self._previous_stable_state = self._state
        self._state = "splitting"

        logger.info(
            f"SWAP {swap_amount} {from_tok} -> {to_tok} "
            f"(50% value split, slippage={self.max_slippage * 100:.2f}%)"
        )
        return Intent.swap(
            from_token=from_tok,
            to_token=to_tok,
            amount=swap_amount,
            max_slippage=self.max_slippage,
            chain=self.chain,
        )

    # ------------------------------------------------------------------
    # Adaptive rebalance engine
    # ------------------------------------------------------------------

    def _compute_realized_vol(self) -> Decimal | None:
        """Compute annualized daily volatility from the rolling price window.

        Uses log returns and assumes a fixed sample interval (the runner's
        ``--interval`` setting). Returns ``None`` if fewer than 3 data points.
        """
        if len(self._price_history) < 3:
            return None
        returns = []
        for i in range(1, len(self._price_history)):
            prev, curr = self._price_history[i - 1], self._price_history[i]
            if prev > 0 and curr > 0:
                returns.append(math.log(curr / prev))
        if len(returns) < 2:
            return None
        mean_r = sum(returns) / len(returns)
        variance = sum((r - mean_r) ** 2 for r in returns) / len(returns)
        vol_per_sample = math.sqrt(variance)
        # Scale to daily: assume 30s intervals -> 2880 samples/day.
        # TODO: if interval changes, this scaling should adapt.
        daily_vol = vol_per_sample * math.sqrt(2880)
        return Decimal(str(daily_vol))

    def _compute_dynamic_range_width(self) -> Decimal:
        """Compute LP range width from realized volatility.

        Formula: ``range_width = range_vol_multiplier * daily_vol``

        The multiplier controls expected time-in-range via random walk:
        ``E[days_in_range] ≈ (multiplier / 2)² * 2``

        With the default multiplier of 6.0: expected ~18 days in range.
        Higher multiplier = wider range = less frequent rebalances but
        lower fee concentration.

        Returns the static ``lp_range_width_pct`` when dynamic sizing is
        disabled or insufficient price history exists.
        """
        if not self.dynamic_range_enabled or self._realized_vol is None:
            return self.lp_range_width_pct

        dynamic_width = self.range_vol_multiplier * self._realized_vol

        # Clamp to [10%, 200%] — never tighter than ±5% and never wider
        # than full-range equivalent
        dynamic_width = max(Decimal("0.10"), min(Decimal("2.00"), dynamic_width))

        if self._active_range_width != dynamic_width:
            expected_days = float((self.range_vol_multiplier / 2) ** 2 * 2)
            logger.info(
                f"Dynamic range: vol={self._realized_vol:.4f}/day, "
                f"width={dynamic_width * 100:.1f}% "
                f"(multiplier={self.range_vol_multiplier}, "
                f"E[days_in_range]≈{expected_days:.0f})"
            )
        self._active_range_width = dynamic_width
        return dynamic_width

    def _monitor_and_maybe_rebalance(self) -> Intent:
        """Price monitoring with spike filtering and vol-adaptive logic.

        The rebalance decision uses three gates:

        1. **Range exit**: price must be outside the current LP range.
        2. **Confirmation**: price must stay outside for
           ``confirmation_cycles`` consecutive decide() calls. This filters
           transient spikes / wicks that would mean-revert on their own.
        3. **Cooldown**: at least ``rebalance_cooldown_s`` seconds must have
           elapsed since the last rebalance. Prevents rapid-fire rebalancing
           in choppy, directionless markets.

        If price re-enters the range at any point during the confirmation
        window, the counter resets — the spike was noise, not a breakout.
        """
        if self._lp_range_lower is None or self._lp_range_upper is None:
            return Intent.hold(reason="No range set — waiting")

        pair_price = self._lp_token0_price_usd / self._lp_token1_price_usd

        # --- Update rolling price history and vol estimate ---
        self._price_history.append(float(pair_price))
        if len(self._price_history) > self.vol_lookback_periods:
            self._price_history = self._price_history[-self.vol_lookback_periods:]
        self._realized_vol = self._compute_realized_vol()

        # --- Check if price is inside or outside the LP range ---
        in_range = self._lp_range_lower <= pair_price <= self._lp_range_upper

        if in_range:
            if self._range_exit_count > 0:
                logger.info(
                    f"Price re-entered range after {self._range_exit_count} exit cycle(s) "
                    f"— spike filtered, no rebalance"
                )
            self._range_exit_count = 0
            vol_str = f", vol={self._realized_vol:.4f}/d" if self._realized_vol else ""
            width_str = f", width={self._active_range_width * 100:.0f}%" if self._active_range_width else ""
            return Intent.hold(
                reason=(
                    f"Loop active: price={pair_price:.2f} in range "
                    f"[{self._lp_range_lower:.2f}-{self._lp_range_upper:.2f}], "
                    f"lp={self._position_id}, rebalances={self._rebalance_count}"
                    f"{vol_str}{width_str}"
                )
            )

        # --- Price is outside range — apply confirmation gate ---
        self._range_exit_count += 1
        direction = "below" if pair_price < self._lp_range_lower else "above"

        if self._range_exit_count < self.confirmation_cycles:
            return Intent.hold(
                reason=(
                    f"Price {pair_price:.2f} {direction} range — confirming breakout "
                    f"({self._range_exit_count}/{self.confirmation_cycles} cycles)"
                )
            )

        # --- Confirmation passed — apply cooldown gate ---
        now = time.time()
        if self._last_rebalance_ts > 0:
            elapsed = now - self._last_rebalance_ts
            if elapsed < self.rebalance_cooldown_s:
                remaining = int(self.rebalance_cooldown_s - elapsed)
                return Intent.hold(
                    reason=(
                        f"Breakout confirmed but cooldown active "
                        f"({remaining}s remaining, rebalance #{self._rebalance_count})"
                    )
                )

        # --- All gates passed — trigger rebalance ---
        vol_str = f", vol={self._realized_vol:.4f}/d" if self._realized_vol else ""
        logger.info(
            f"REBALANCE #{self._rebalance_count + 1}: price={pair_price:.4f} {direction} "
            f"range [{self._lp_range_lower:.4f}-{self._lp_range_upper:.4f}] "
            f"(confirmed {self._range_exit_count} cycles{vol_str})"
        )
        self._last_rebalance_ts = now
        self._range_exit_count = 0
        return self._build_lp_close_intent()

    def _build_lp_close_intent(self) -> Intent:
        if not self._position_id:
            self._state = "lp_closed"
            return Intent.hold(reason="No position_id tracked — cannot close, skipping to re-open")

        self._previous_stable_state = self._state
        self._state = "closing_lp"

        logger.info(f"LP_CLOSE: position_id={self._position_id}, pool={self.lp_pool}")
        return Intent.lp_close(
            position_id=self._position_id,
            pool=self.lp_pool,
            collect_fees=True,
            protocol="uniswap_v3",
            chain=self.chain,
        )

    def _build_lp_open_intent(self, market: MarketSnapshot) -> Intent:
        # Re-read wallet balances to get the exact post-swap amounts.
        try:
            tok0_bal = market.balance(self.lp_token0)
            tok1_bal = market.balance(self.lp_token1)
            amount0 = Decimal(str(tok0_bal.balance if hasattr(tok0_bal, "balance") else tok0_bal))
            amount1 = Decimal(str(tok1_bal.balance if hasattr(tok1_bal, "balance") else tok1_bal))
        except (ValueError, KeyError) as e:
            return Intent.hold(reason=f"Cannot read LP token balances: {e}")

        if amount0 <= 0 or amount1 <= 0:
            self._state = "running"
            return Intent.hold(
                reason=f"Zero balance for LP leg ({self.lp_token0}={amount0}, {self.lp_token1}={amount1})"
            )

        # Leave a small dust buffer (~1%) so rounding / gas can't push us over wallet balance.
        amount0 = (amount0 * Decimal("0.99")).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
        amount1 = (amount1 * Decimal("0.99")).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)

        # Build a symmetric range around the current pair price.
        # Use dynamic vol-scaled width when enabled, else static config.
        pair_price = self._lp_token0_price_usd / self._lp_token1_price_usd
        range_width = self._compute_dynamic_range_width()
        half_width = range_width / Decimal("2")
        range_lower = pair_price * (Decimal("1") - half_width)
        range_upper = pair_price * (Decimal("1") + half_width)

        self._lp_range_lower = range_lower
        self._lp_range_upper = range_upper
        self._lp_amount_token0 = amount0
        self._lp_amount_token1 = amount1

        self._previous_stable_state = self._state
        self._state = "opening_lp"

        logger.info(
            f"LP_OPEN {self.lp_pool}: {amount0} {self.lp_token0} + {amount1} {self.lp_token1}, "
            f"range=[{range_lower:.6f} - {range_upper:.6f}]"
        )
        return Intent.lp_open(
            pool=self.lp_pool,
            amount0=amount0,
            amount1=amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="uniswap_v3",
            chain=self.chain,
        )

    # ------------------------------------------------------------------
    # on_intent_executed callback — advance state machine
    # ------------------------------------------------------------------

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        intent_type = intent.intent_type.value

        if not success:
            revert_to = self._previous_stable_state
            logger.warning(f"{intent_type} failed, reverting state '{self._state}' -> '{revert_to}'")
            self._state = revert_to
            return

        if intent_type == "SUPPLY":
            latched = getattr(self, "_pending_supply_amount", None) or self.initial_supply_amount
            self._total_supplied += Decimal(str(latched))
            self._state = "supplied"
            logger.info(f"Supply OK. total_supplied={self._total_supplied} {self.supply_token}")

        elif intent_type == "BORROW":
            latched = getattr(self, "_pending_borrow_amount", None)
            borrow_amount = Decimal(str(latched)) if latched is not None else Decimal(
                str(getattr(intent, "borrow_amount", Decimal("0")))
            )
            self._total_borrowed += borrow_amount
            self._state = "borrowed"
            logger.info(f"Borrow OK. total_borrowed={self._total_borrowed} {self.borrow_token}")

        elif intent_type == "SWAP":
            # Advance entry-pipeline swap states. Teardown swaps land in `running`
            # or other states and are intentionally not advanced here.
            if self._state == "converting":
                self._state = "converted"
                logger.info("Debt-asset conversion OK. Ready to split for LP.")
            elif self._state == "splitting":
                self._state = "split_done"
                logger.info("Split swap OK. Ready to open LP position.")

        elif intent_type == "LP_OPEN":
            position_id = getattr(result, "position_id", None)
            if position_id:
                self._position_id = str(position_id)
            self._state = "running"
            logger.info(f"LP_OPEN OK. position_id={self._position_id}. Loop entry complete.")

        elif intent_type == "LP_CLOSE":
            old_pos = self._position_id
            self._position_id = None
            self._lp_amount_token0 = Decimal("0")
            self._lp_amount_token1 = Decimal("0")
            if self._state == "closing_lp":
                self._rebalance_count += 1
                self._state = "lp_closed"
                logger.info(
                    f"LP_CLOSE OK (position {old_pos}). "
                    f"Rebalance #{self._rebalance_count} — ready to re-split."
                )

        elif intent_type == "REPAY":
            self._total_borrowed = Decimal("0")

        elif intent_type == "WITHDRAW":
            if getattr(intent, "withdraw_all", False):
                self._total_supplied = Decimal("0")
            else:
                withdrawn = Decimal(str(getattr(intent, "amount", Decimal("0")) or Decimal("0")))
                self._total_supplied = max(Decimal("0"), self._total_supplied - withdrawn)

    # ------------------------------------------------------------------
    # Status / persistence
    # ------------------------------------------------------------------

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "xlayer_aave_okb_clmm_loop",
            "chain": self.chain,
            "state": self._state,
            "total_supplied": str(self._total_supplied),
            "total_borrowed": str(self._total_borrowed),
            "position_id": self._position_id,
            "lp_pool": self.lp_pool,
            "lp_range": (
                f"[{self._lp_range_lower} - {self._lp_range_upper}]"
                if self._lp_range_lower is not None
                else "none"
            ),
            "rebalance_count": self._rebalance_count,
            "realized_vol": str(self._realized_vol) if self._realized_vol else "pending",
            "active_range_width": f"{self._active_range_width * 100:.1f}%" if self._active_range_width else "static",
            "range_exit_count": self._range_exit_count,
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "previous_stable_state": self._previous_stable_state,
            "total_supplied": str(self._total_supplied),
            "total_borrowed": str(self._total_borrowed),
            "lp_amount_token0": str(self._lp_amount_token0),
            "lp_amount_token1": str(self._lp_amount_token1),
            "lp_range_lower": str(self._lp_range_lower) if self._lp_range_lower is not None else None,
            "lp_range_upper": str(self._lp_range_upper) if self._lp_range_upper is not None else None,
            "position_id": self._position_id,
            "rebalance_count": self._rebalance_count,
            "range_exit_count": self._range_exit_count,
            "last_rebalance_ts": str(self._last_rebalance_ts),
            "realized_vol": str(self._realized_vol) if self._realized_vol is not None else None,
            "active_range_width": str(self._active_range_width) if self._active_range_width is not None else None,
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
        if "lp_amount_token0" in state:
            self._lp_amount_token0 = Decimal(str(state["lp_amount_token0"]))
        if "lp_amount_token1" in state:
            self._lp_amount_token1 = Decimal(str(state["lp_amount_token1"]))
        if state.get("lp_range_lower"):
            self._lp_range_lower = Decimal(str(state["lp_range_lower"]))
        if state.get("lp_range_upper"):
            self._lp_range_upper = Decimal(str(state["lp_range_upper"]))
        if "position_id" in state:
            self._position_id = state.get("position_id")
        if "rebalance_count" in state:
            self._rebalance_count = int(state["rebalance_count"])
        if "range_exit_count" in state:
            self._range_exit_count = int(state["range_exit_count"])
        if "last_rebalance_ts" in state:
            self._last_rebalance_ts = float(state["last_rebalance_ts"])
        if state.get("realized_vol"):
            self._realized_vol = Decimal(str(state["realized_vol"]))
        if state.get("active_range_width"):
            self._active_range_width = Decimal(str(state["active_range_width"]))

    # ------------------------------------------------------------------
    # Teardown
    # ------------------------------------------------------------------

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []

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

        if self._position_id is not None:
            lp_value_usd = (
                self._lp_amount_token0 * self._lp_token0_price_usd
                + self._lp_amount_token1 * self._lp_token1_price_usd
            )
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=f"xlayer-lp-{self._position_id}",
                    chain=self.chain,
                    protocol="uniswap_v3",
                    value_usd=lp_value_usd,
                    details={
                        "pool": self.lp_pool,
                        "range_lower": str(self._lp_range_lower),
                        "range_upper": str(self._lp_range_upper),
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Unwind the loop in strict reverse order.

        1. Close the Uniswap V3 LP position (collects lp_token0 + lp_token1 + fees)
        2. Swap any lp_token0 and lp_token1 that isn't already the borrow_token
           back to the borrow_token so we can repay the debt
        3. Withdraw a small collateral buffer and swap it to borrow_token too,
           covering the accrued-interest gap so repay_full can close the debt
        4. Repay the debt in full
        5. Withdraw all remaining supply_token collateral
        """
        from almanak.framework.teardown import TeardownMode

        intents: list[Intent] = []
        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else self.max_slippage
        borrow_upper = self.borrow_token.upper()

        # Step 1: close LP (if open)
        if self._position_id is not None:
            intents.append(
                Intent.lp_close(
                    position_id=self._position_id,
                    pool=self.lp_pool,
                    collect_fees=True,
                    protocol="uniswap_v3",
                    chain=self.chain,
                )
            )

            # Step 2: consolidate both LP legs into the borrow_token for repay.
            # Route via lp_token1 (the stable side, USDT) to avoid missing
            # direct pools (e.g., WOKB/USDG has no pool on xlayer).
            #   lp_token0 (WOKB) -> lp_token1 (USDT) -> borrow_token (USDG)
            if self.lp_token0.upper() != self.lp_token1.upper():
                intents.append(
                    Intent.swap(
                        from_token=self.lp_token0,
                        to_token=self.lp_token1,
                        amount="all",
                        max_slippage=max_slippage,
                        chain=self.chain,
                    )
                )
            # Now all value is in lp_token1 (USDT). Swap to borrow_token if different.
            if self.lp_token1.upper() != borrow_upper:
                intents.append(
                    Intent.swap(
                        from_token=self.lp_token1,
                        to_token=self.borrow_token,
                        amount="all",
                        max_slippage=max_slippage,
                        chain=self.chain,
                    )
                )

        if self._total_borrowed > 0:
            # Step 3: Aave debt accrues interest; principal alone cannot fully repay.
            # Withdraw a small collateral buffer (0.5% of borrow value in supply_token
            # units using cached prices), swap it to the borrow token, then repay_full.
            # The compiler caps repay_full to actual debt so over-shooting is safe.
            borrow_price = self._borrow_price_usd if self._borrow_price_usd > 0 else Decimal("1")
            supply_price = self._supply_price_usd if self._supply_price_usd > 0 else Decimal("1")
            interest_buffer = (self._total_borrowed * Decimal("0.005") * borrow_price) / supply_price

            intents.append(
                Intent.withdraw(
                    token=self.supply_token,
                    amount=interest_buffer,
                    protocol="aave_v3",
                    chain=self.chain,
                )
            )
            intents.append(
                Intent.swap(
                    from_token=self.supply_token,
                    to_token=self.borrow_token,
                    amount=interest_buffer,
                    max_slippage=max_slippage,
                    chain=self.chain,
                )
            )

            # Step 4: repay the full WOKB debt
            intents.append(
                Intent.repay(
                    token=self.borrow_token,
                    protocol="aave_v3",
                    repay_full=True,
                    chain=self.chain,
                )
            )

        # Step 5: withdraw all remaining collateral
        if self._total_supplied > 0:
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
