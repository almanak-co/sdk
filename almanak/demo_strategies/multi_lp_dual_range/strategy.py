"""Multi-position dual-range Uniswap V3 LP demo.

Reference template for the multi-position dispatch pattern: two LP positions
(narrow + wide ranges) on the same Uniswap V3 pool, opened **one Intent per
``decide()`` iteration** via an explicit phase machine.

This is the discoverable companion to ``blueprints/04-strategy-layer.md``
§Multi-position dispatch. The accounting-fixture sibling
``strategies/accounting/lp_dual/`` exercises the same pattern with extra
audit-test scaffolding; this demo strips that down to the dispatch shape
itself.

Lifecycle:

1. ``PHASE_INIT`` — emit ``LP_OPEN`` for the narrow leg.
2. ``PHASE_LP1_OPEN`` — emit ``LP_OPEN`` for the wide leg.
3. ``PHASE_BOTH_OPEN`` — ``HOLD`` until operator-initiated teardown.
4. Teardown — ``LP_CLOSE`` per still-open leg.

Why one Intent per iteration (and not a ``list[Intent]`` return):

- The phase only advances when ``on_intent_executed`` observes a real
  ``position_id`` on the receipt. A mint that lands on-chain but reports no
  NFT id holds the phase put, so the next iteration retries instead of
  silently stranding the slot.
- Leg #2's amounts are computed from a fresh ``market.balance(...)`` at the
  moment its open is built. Hardcoded per-leg amounts would desync against
  any real-world mint slippage / dust.
- Each leg carries a stable per-position ``registry_handle`` (``leg_narrow``
  / ``leg_wide``) so the position-registry auto-mode collision guard
  (``ix_registry_auto_mode``) doesn't reject the second open. The same
  handle survives open → close.
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

logger = logging.getLogger(__name__)


# Phase machine — exhaustive, advances only on successful LP_OPEN with a real
# position_id. See ``on_intent_executed``.
PHASE_INIT = "init"
PHASE_LP1_OPEN = "lp1_open"
PHASE_BOTH_OPEN = "both_open"
# Sticky error state. Entered when LP_OPEN reports ``success=True`` but the
# receipt has no ``position_id`` — the mint may have landed on-chain but is
# now untracked. Re-emitting the same LP_OPEN would mint a SECOND position
# while the first stays orphaned (stranding more capital, not less). The
# only safe response is to halt the strategy and surface the issue so an
# operator can reconcile on-chain state and run teardown. Once entered,
# ``decide()`` only returns HOLD; teardown still drains any tracked legs.
PHASE_BLOCKED = "blocked_no_position_id"

# Stable per-position registry_handle values. Action-scoped suffixes
# (``leg_narrow:open`` / ``leg_narrow:close``) would technically work but
# break the registry contract for open → close → rebalance lifecycles.
HANDLE_NARROW = "leg_narrow"
HANDLE_WIDE = "leg_wide"


@dataclass
class MultiLPDualRangeConfig:
    """Configuration for the dual-range LP demo.

    Attributes:
        pool: Pool identifier ``TOKEN0/TOKEN1/FEE`` (e.g. ``WETH/USDC/500``).
        narrow_range_width_pct: Total width of the narrow leg's price range
            (0.10 = ±5% from current price).
        wide_range_width_pct: Total width of the wide leg's price range
            (0.40 = ±20%).
        lp_capital_split_pct: Fraction of available token0/token1 the narrow
            leg consumes. The wide leg takes 0.99 of what remains (the 1%
            safety margin absorbs gas / dust / slippage drift between the
            balance read and tx submission).
    """

    pool: str = "WETH/USDC/500"
    narrow_range_width_pct: Decimal = Decimal("0.10")
    wide_range_width_pct: Decimal = Decimal("0.40")
    lp_capital_split_pct: Decimal = Decimal("0.50")

    def to_dict(self) -> dict[str, Any]:
        return {
            "pool": self.pool,
            "narrow_range_width_pct": str(self.narrow_range_width_pct),
            "wide_range_width_pct": str(self.wide_range_width_pct),
            "lp_capital_split_pct": str(self.lp_capital_split_pct),
        }

    def update(self, **kwargs: Any) -> Any:
        @dataclass
        class UpdateResult:
            success: bool = True
            updated_fields: list = field(default_factory=list)

        updated: list[str] = []
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
                updated.append(k)
        return UpdateResult(success=True, updated_fields=updated)


@almanak_strategy(
    name="demo_multi_lp_dual_range",
    description=(
        "Reference template for multi-position LP dispatch — opens narrow + "
        "wide Uniswap V3 positions on the same pool, one Intent per iteration "
        "via a phase machine."
    ),
    version="1.0.0",
    author="Almanak",
    tags=["demo", "tutorial", "lp", "multi-position", "uniswap-v3", "arbitrum"],
    # ``supported_chains`` is intentionally narrowed to "arbitrum" — the only
    # chain whose ``anvil_funding`` (config.json) has been validated end-to-end.
    # The dispatch pattern itself is chain-agnostic; widen this list once the
    # demo has a real on-chain run on the additional chain.
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="arbitrum",
)
class MultiLPDualRangeStrategy(IntentStrategy[MultiLPDualRangeConfig]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.pool = self.config.pool
        pool_parts = self.pool.split("/")
        if len(pool_parts) != 3:
            # Fail loud rather than silently default the fee tier — a mis-typed
            # 2-part pool string in config.json would otherwise land on a
            # 5 bps pool, which is money-critical config drift.
            raise ValueError(
                f"pool must be in TOKEN0/TOKEN1/FEE format (e.g. 'WETH/USDC/500'); "
                f"got {self.pool!r}"
            )
        self.token0_symbol = pool_parts[0]
        self.token1_symbol = pool_parts[1]
        self.fee_tier = int(pool_parts[2])

        self.narrow_range_width_pct = Decimal(str(self.config.narrow_range_width_pct))
        self.wide_range_width_pct = Decimal(str(self.config.wide_range_width_pct))
        self.lp_capital_split_pct = Decimal(str(self.config.lp_capital_split_pct))

        # Phase / position state — persisted via get_persistent_state /
        # load_persistent_state below so the runner restores it across
        # restarts. Range bounds are tracked per leg so the LP dashboard's
        # multi-position panel can render them (it consumes
        # ``session_state["positions"]`` as a list of dicts keyed off
        # ``registry_handle``; see ``lp_dashboard.py:642-688``).
        self._phase: str = PHASE_INIT
        self._position_id_narrow: str | None = None
        self._position_id_wide: str | None = None
        self._range_lower_narrow: Decimal | None = None
        self._range_upper_narrow: Decimal | None = None
        self._range_lower_wide: Decimal | None = None
        self._range_upper_wide: Decimal | None = None

        logger.info(
            "MultiLPDualRangeStrategy initialized: "
            f"pool={self.pool}, narrow_width={self.narrow_range_width_pct * 100}%, "
            f"wide_width={self.wide_range_width_pct * 100}%, "
            f"capital_split={self.lp_capital_split_pct * 100}%, phase={self._phase}"
        )

    # ---------------------------------------------------------------- decide

    def decide(self, market: MarketSnapshot) -> Intent | None:
        if self._phase == PHASE_BLOCKED:
            # Sticky error — never emit another open from here. Teardown
            # (separate dispatch lane) can still drain tracked legs.
            return Intent.hold(
                reason=(
                    "LP_OPEN succeeded without a position_id on the receipt — "
                    "manual reconciliation required. Operator must inspect "
                    "on-chain state for orphan positions and run teardown."
                )
            )
        if self._phase == PHASE_INIT:
            return self._build_lp_open(market, leg="narrow")
        if self._phase == PHASE_LP1_OPEN:
            return self._build_lp_open(market, leg="wide")
        if self._phase == PHASE_BOTH_OPEN:
            return Intent.hold(reason="Both LP legs open — awaiting teardown signal")
        return Intent.hold(reason=f"Unknown phase {self._phase!r}")

    # -------------------------------------------------------- intent builders

    def _build_lp_open(self, market: MarketSnapshot, *, leg: str) -> Intent:
        token0_balance = self._read_balance(market, self.token0_symbol)
        token1_balance = self._read_balance(market, self.token1_symbol)

        token0_price = Decimal(str(market.price(self.token0_symbol)))
        token1_price = Decimal(str(market.price(self.token1_symbol)))
        # Defensive guard on both sides of the ratio. A zero/negative price on
        # either token would crash on DivisionByZero (token1) or silently
        # produce a degenerate LP range with both bounds at 0 (token0).
        # Surface the upstream oracle failure loudly so the runner retries on
        # the next snapshot.
        if token0_price <= 0:
            raise ValueError(
                f"Invalid base price for {self.token0_symbol}: {token0_price}. "
                "Cannot compute LP range; skipping iteration."
            )
        if token1_price <= 0:
            raise ValueError(
                f"Invalid quote price for {self.token1_symbol}: {token1_price}. "
                "Cannot compute LP range; skipping iteration."
            )
        current_price = token0_price / token1_price

        if leg == "narrow":
            half = self.narrow_range_width_pct / Decimal("2")
            commit_pct = self.lp_capital_split_pct
            handle = HANDLE_NARROW
        else:
            half = self.wide_range_width_pct / Decimal("2")
            # The wide leg takes 0.99 of what's left — the 1% safety margin
            # absorbs gas / dust / slippage drift between balance read and tx
            # submission.
            commit_pct = Decimal("0.99")
            handle = HANDLE_WIDE

        range_lower = current_price * (Decimal("1") - half)
        range_upper = current_price * (Decimal("1") + half)
        amount0 = token0_balance * commit_pct
        amount1 = token1_balance * commit_pct

        logger.info(
            f"Phase {self._phase}: LP_OPEN {leg} leg — "
            f"{format_token_amount_human(amount0, self.token0_symbol)} + "
            f"{format_token_amount_human(amount1, self.token1_symbol)}, "
            f"range [{format_usd(range_lower)} - {format_usd(range_upper)}] "
            f"(half={half}, commit_pct={commit_pct}, handle={handle!r})"
        )
        return Intent.lp_open(
            pool=self.pool,
            amount0=amount0,
            amount1=amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="uniswap_v3",
            registry_handle=handle,
        )

    @staticmethod
    def _read_balance(market: MarketSnapshot, symbol: str) -> Decimal:
        result = market.balance(symbol)
        raw = result.balance if hasattr(result, "balance") else result
        return Decimal(str(raw))

    # ------------------------------------------------------------- callbacks

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        if not success:
            # Phase stays put on failure → next iteration retries the same
            # leg with a fresh market snapshot. This is the partial-success
            # guard the list-return shape can't provide.
            logger.warning(f"Intent {intent.intent_type.value} failed in phase {self._phase}")
            return

        intent_type = intent.intent_type.value

        if intent_type == "LP_OPEN":
            position_id = getattr(result, "position_id", None) if result is not None else None
            if not position_id:
                # An LP mint that landed on-chain but returned no position_id
                # is a sticky error state. Holding the phase open would cause
                # next iteration's decide() to re-emit the same LP_OPEN —
                # opening a SECOND position on-chain while the first stays
                # orphaned (stranding MORE capital, not less). Transition to
                # PHASE_BLOCKED so decide() returns HOLD until an operator
                # reconciles on-chain state.
                logger.error(
                    f"LP_OPEN succeeded in phase {self._phase!r} but receipt had no "
                    f"position_id. Transitioning to PHASE_BLOCKED — re-emitting "
                    f"LP_OPEN would duplicate the position. Operator must inspect "
                    f"on-chain state and run teardown to recover."
                )
                self._phase = PHASE_BLOCKED
                return

            prev_phase = self._phase
            # Capture range bounds from the just-executed intent so the LP
            # dashboard's multi-position panel can render them.
            intent_range_lower = getattr(intent, "range_lower", None)
            intent_range_upper = getattr(intent, "range_upper", None)
            if self._phase == PHASE_INIT:
                self._position_id_narrow = str(position_id)
                self._range_lower_narrow = intent_range_lower
                self._range_upper_narrow = intent_range_upper
                self._phase = PHASE_LP1_OPEN
            elif self._phase == PHASE_LP1_OPEN:
                self._position_id_wide = str(position_id)
                self._range_lower_wide = intent_range_lower
                self._range_upper_wide = intent_range_upper
                self._phase = PHASE_BOTH_OPEN
            else:
                logger.warning(
                    f"LP_OPEN succeeded in unexpected phase {self._phase!r}; "
                    "ignoring to avoid overwriting a populated slot."
                )
                return
            logger.info(
                f"Phase transition: {prev_phase} -> {self._phase}, "
                f"narrow={self._position_id_narrow}, wide={self._position_id_wide}"
            )
            return

        if intent_type == "LP_CLOSE":
            # Key off intent.position_id (not slot order) so out-of-order
            # closes don't desync the phase. Teardown closes flow through
            # here.
            pos_id = str(getattr(intent, "position_id", "") or "")
            if pos_id and pos_id == self._position_id_narrow:
                self._position_id_narrow = None
                self._range_lower_narrow = None
                self._range_upper_narrow = None
            elif pos_id and pos_id == self._position_id_wide:
                self._position_id_wide = None
                self._range_lower_wide = None
                self._range_upper_wide = None
            else:
                logger.warning(
                    f"LP_CLOSE for unrecognized position_id={pos_id!r} "
                    f"(narrow={self._position_id_narrow}, wide={self._position_id_wide})"
                )
            logger.info(
                f"LP_CLOSE executed for position_id={pos_id}. "
                f"Remaining: narrow={self._position_id_narrow}, wide={self._position_id_wide}"
            )

    # ------------------------------------------------------ persistent state

    def get_persistent_state(self) -> dict[str, Any]:
        state = super().get_persistent_state() if hasattr(super(), "get_persistent_state") else {}
        state["_phase"] = self._phase
        state["position_id_narrow"] = self._position_id_narrow or ""
        state["position_id_wide"] = self._position_id_wide or ""
        state["range_lower_narrow"] = str(self._range_lower_narrow) if self._range_lower_narrow is not None else ""
        state["range_upper_narrow"] = str(self._range_upper_narrow) if self._range_upper_narrow is not None else ""
        state["range_lower_wide"] = str(self._range_lower_wide) if self._range_lower_wide is not None else ""
        state["range_upper_wide"] = str(self._range_upper_wide) if self._range_upper_wide is not None else ""
        # Multi-position dashboard panel reads `positions` as a list of dicts
        # keyed off `registry_handle` (lp_dashboard.py:642-688). Each entry
        # overlays per-leg fields on top of strategy-wide session state, so
        # the operator sees per-leg range expanders rather than a single
        # collapsed "Position Status: N/A" panel.
        state["positions"] = [
            {
                "registry_handle": handle,
                "position_id": pid,
                "range_lower": str(lo) if lo is not None else None,
                "range_upper": str(hi) if hi is not None else None,
            }
            for handle, pid, lo, hi in (
                (HANDLE_NARROW, self._position_id_narrow, self._range_lower_narrow, self._range_upper_narrow),
                (HANDLE_WIDE, self._position_id_wide, self._range_lower_wide, self._range_upper_wide),
            )
            if pid
        ]
        return state

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if hasattr(super(), "load_persistent_state"):
            super().load_persistent_state(state)
        self._phase = state.get("_phase", PHASE_INIT)
        raw_narrow = state.get("position_id_narrow")
        raw_wide = state.get("position_id_wide")
        self._position_id_narrow = raw_narrow if raw_narrow else None
        self._position_id_wide = raw_wide if raw_wide else None

        def _restore_decimal(key: str) -> Decimal | None:
            raw = state.get(key)
            return Decimal(raw) if raw else None

        self._range_lower_narrow = _restore_decimal("range_lower_narrow")
        self._range_upper_narrow = _restore_decimal("range_upper_narrow")
        self._range_lower_wide = _restore_decimal("range_lower_wide")
        self._range_upper_wide = _restore_decimal("range_upper_wide")

    # ----------------------------------------------------------- status / UX

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_multi_lp_dual_range",
            "chain": self.chain,
            "wallet": (self.wallet_address[:10] + "...") if self.wallet_address else "N/A",
            "config": self.config.to_dict(),
            "state": {
                "phase": self._phase,
                "position_id_narrow": self._position_id_narrow,
                "position_id_wide": self._position_id_wide,
            },
        }

    # -------------------------------------------------------------- teardown

    def get_open_positions(self) -> "TeardownPositionSummary":  # type: ignore[name-defined]
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []
        for leg_name, pid in (
            ("narrow", self._position_id_narrow),
            ("wide", self._position_id_wide),
        ):
            if not pid:
                continue
            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=str(pid),
                    chain=self.chain,
                    protocol="uniswap_v3",
                    value_usd=Decimal("0"),
                    details={
                        "pool": self.pool,
                        "fee_tier": self.fee_tier,
                        "token0": self.token0_symbol,
                        "token1": self.token1_symbol,
                        "leg": leg_name,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_multi_lp_dual_range"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:  # type: ignore[name-defined]
        intents: list[Intent] = []
        for pid in (self._position_id_narrow, self._position_id_wide):
            if not pid:
                continue
            intents.append(
                Intent.lp_close(
                    position_id=pid,
                    pool=self.pool,
                    collect_fees=True,
                    protocol="uniswap_v3",
                )
            )
        return intents

    def on_teardown_started(self, mode: "TeardownMode") -> None:  # type: ignore[name-defined]
        from almanak.framework.teardown import TeardownMode

        mode_name = "Graceful" if mode == TeardownMode.SOFT else "Emergency"
        logger.info(
            f"[TEARDOWN] {mode_name} starting MultiLPDualRange. "
            f"narrow={self._position_id_narrow}, wide={self._position_id_wide}, "
            f"phase={self._phase}"
        )

    def on_teardown_completed(self, success: bool, recovered_usd: Decimal) -> None:
        if success:
            logger.info(f"[TEARDOWN] MultiLPDualRange completed. Recovered: ${recovered_usd:,.4f}")
            self._position_id_narrow = None
            self._position_id_wide = None
        else:
            logger.warning(f"[TEARDOWN] MultiLPDualRange failed. Partial recovery: ${recovered_usd:,.4f}")
