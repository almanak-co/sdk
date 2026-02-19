"""Whale Follower Strategy -- follow top DeFi market makers on Arbitrum.

Monitors two high-activity wallets (Wintermute, Jump Trading) and mirrors
their swap trades with proportional sizing (0.1% of leader trade) and
tight daily risk caps.

Thesis: Large market makers have informational edges on token flows and
momentum. By tailing their swaps at micro-scale with tight filters
(min $1k, allowlisted tokens only), we capture directional signal
while limiting adverse selection.

Usage:
    almanak strat run -d strategies/demo/whale_follower --network anvil --once
    almanak strat run -d strategies/demo/whale_follower --once --dry-run
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.services.copy_sizer import CopySizer, CopySizingConfig
from almanak.framework.services.copy_trading_models import CopySignal, CopyTradingConfig
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)


def _cfg(config: Any, key: str, default: Any) -> Any:
    """Read a value from dict-or-object config."""
    if isinstance(config, dict):
        return config.get(key, default)
    return getattr(config, key, default)


@almanak_strategy(
    name="whale_follower",
    description="Follows DeFi whales on Arbitrum and mirrors swap trades with proportional sizing",
    version="0.1.0",
    author="Senior DeFi Quant",
    tags=["copy-trading", "whale-following", "swap", "arbitrum"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3", "pancakeswap_v3", "sushiswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class WhaleFollowerStrategy(IntentStrategy):
    """Mirrors whale swaps with proportional sizing and multi-leader weighting.

    Key differences from the basic copy_trader demo:
      - Multi-leader with per-leader weights (Wintermute 1.0x, Jump 0.5x)
      - Proportion-of-leader sizing (0.1% of leader trade)
      - Tighter signal age filter (3 min vs 5 min)
      - Explicit token allowlist to avoid illiquid / ruggable tokens
      - Per-signal metrics tracking for post-hoc analysis
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        ct_config = _cfg(self.config, "copy_trading", {})
        self._ct_config = CopyTradingConfig.from_config(ct_config)
        self._filters = ct_config.get("filters", {})
        self._allowed_tokens = set(
            t.upper() for t in self._filters.get("tokens", [])
        )

        sizing_dict = ct_config.get("sizing", {})
        risk_dict = ct_config.get("risk", {})
        self._max_slippage = Decimal(str(risk_dict.get("max_slippage", "0.005")))
        self._sizer = CopySizer(config=CopySizingConfig.from_config(sizing_dict, risk_dict))

        self._dry_run = bool(_cfg(self.config, "dry_run", False))

        # Metrics
        self._signals_seen = 0
        self._signals_copied = 0
        self._signals_skipped = 0
        self._skip_reasons: dict[str, int] = {}
        self._last_signal_id: str | None = None

        leaders_summary = ", ".join(
            f"{l.get('label', l['address'][:10])}(w={l.get('weight', 1.0)})"
            for l in self._ct_config.leaders
        )
        logger.info(
            "WhaleFollowerStrategy initialized: "
            f"leaders=[{leaders_summary}], "
            f"sizing={self._sizer.config.mode.value}, "
            f"max_trade=${self._sizer.config.max_trade_usd}, "
            f"max_slippage={self._max_slippage}, "
            f"dry_run={self._dry_run}"
        )

    # --------------------------------------------------------------------- #
    # decide()
    # --------------------------------------------------------------------- #

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Process pending leader signals and emit a SwapIntent or HoldIntent.

        Iterates all pending signals, consuming any that are blocked or
        filtered, and returns an intent for the first viable one.
        """
        try:
            signals: list[CopySignal] = market.wallet_activity(
                action_types=self._filters.get("action_types"),
                protocols=self._filters.get("protocols"),
                min_usd_value=self._filters.get("min_usd_value"),
            )

            if not signals:
                return Intent.hold(reason="No new leader activity")

            provider = getattr(self, "_wallet_activity_provider", None)

            for signal in signals:
                self._signals_seen += 1
                intent = self._process_signal(signal, provider)
                if intent is not None:
                    return intent

            return Intent.hold(reason="No actionable signals after filtering")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def _process_signal(self, signal: CopySignal, provider: Any) -> Intent | None:
        """Evaluate a single signal. Returns SwapIntent if viable, else None."""

        # -- Gate 1: only swaps with two tokens --------------------------
        if signal.action_type != "SWAP" or len(signal.tokens) < 2:
            self._skip("not_swap_or_missing_tokens", signal, provider)
            return None

        from_token = signal.tokens[0]
        to_token = signal.tokens[1]

        # -- Gate 2: token allowlist -------------------------------------
        if self._allowed_tokens:
            if from_token.upper() not in self._allowed_tokens or to_token.upper() not in self._allowed_tokens:
                self._skip("token_not_allowlisted", signal, provider)
                return None

        # -- Gate 3: sizer pre-check -------------------------------------
        skip_reason = self._sizer.get_skip_reason(signal)
        if skip_reason:
            self._skip(skip_reason, signal, provider)
            return None

        # -- Gate 4: compute size with leader weight ---------------------
        leader_weight = self._ct_config.get_leader_weight(signal.leader_address)
        size = self._sizer.compute_size(signal, leader_weight=leader_weight)
        if size is None:
            self._skip("size_below_min", signal, provider)
            return None

        # -- Gate 5: daily cap -------------------------------------------
        if not self._sizer.check_daily_cap(size):
            return Intent.hold(reason="Daily notional cap reached")

        # -- Gate 6: position cap ----------------------------------------
        if not self._sizer.check_position_cap():
            return Intent.hold(reason="Position cap reached")

        # -- All gates passed: emit intent or log dry-run ----------------
        size = size.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

        leader_label = self._leader_label(signal.leader_address)

        if self._dry_run:
            logger.info(
                "[DRY-RUN] Would copy: %s -> %s, $%s, protocol=%s, leader=%s",
                from_token, to_token, size, signal.protocol, leader_label,
            )
            self._signals_copied += 1
            self._consume(provider, signal.event_id)
            return None  # dry-run: consume but don't emit intent

        logger.info(
            "Copying whale trade: %s -> %s, $%s (leader=%s, protocol=%s)",
            from_token, to_token, size, leader_label, signal.protocol,
        )
        self._signals_copied += 1
        self._last_signal_id = signal.event_id

        return Intent.swap(
            from_token=from_token,
            to_token=to_token,
            amount_usd=size,
            max_slippage=self._max_slippage,
            protocol=signal.protocol,
            chain=signal.chain,
        )

    # --------------------------------------------------------------------- #
    # Callbacks
    # --------------------------------------------------------------------- #

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Record execution result in sizer and consume the signal."""
        signal_id = self._last_signal_id
        if success and hasattr(intent, "amount_usd") and intent.amount_usd is not None:
            self._sizer.record_execution(intent.amount_usd)
            self._sizer.record_close()  # swaps are atomic
            if signal_id:
                self._consume(getattr(self, "_wallet_activity_provider", None), signal_id)
                self._last_signal_id = None
            logger.info(
                "Whale copy executed: %s -> %s, $%s",
                intent.from_token, intent.to_token, intent.amount_usd,
            )
        elif not success:
            logger.warning("Whale copy failed: %s", result)

    # --------------------------------------------------------------------- #
    # Helpers
    # --------------------------------------------------------------------- #

    def _skip(self, reason: str, signal: CopySignal, provider: Any) -> None:
        """Record a skipped signal and consume it from the queue."""
        self._signals_skipped += 1
        self._skip_reasons[reason] = self._skip_reasons.get(reason, 0) + 1
        logger.debug("Skipped %s: %s", signal.event_id, reason)
        self._consume(provider, signal.event_id)

    @staticmethod
    def _consume(provider: Any, event_id: str) -> None:
        if provider is not None:
            try:
                provider.consume_signals([event_id])
            except Exception:
                pass

    def _leader_label(self, address: str) -> str:
        """Resolve a leader address to its configured label."""
        for leader in self._ct_config.leaders:
            if leader.get("address", "").lower() == address.lower():
                return leader.get("label", address[:10])
        return address[:10]

    # --------------------------------------------------------------------- #
    # Status
    # --------------------------------------------------------------------- #

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "whale_follower",
            "chain": self.chain,
            "dry_run": self._dry_run,
            "sizing_mode": self._sizer.config.mode.value,
            "daily_notional": str(self._sizer._daily_notional),
            "open_positions": self._sizer._open_positions,
            "signals_seen": self._signals_seen,
            "signals_copied": self._signals_copied,
            "signals_skipped": self._signals_skipped,
            "skip_reasons": dict(self._skip_reasons),
            "leaders": [
                {"label": l.get("label", "?"), "weight": l.get("weight", 1.0)}
                for l in self._ct_config.leaders
            ],
        }
