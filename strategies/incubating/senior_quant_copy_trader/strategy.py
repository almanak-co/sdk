"""Senior Quant Copy Trader strategy.

Monitors selected leader wallets and mirrors qualifying SWAP flow with:
- weighted multi-leader sizing
- strict token/protocol allowlists
- capped daily notional and per-trade exposure
"""

from decimal import ROUND_DOWN, Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.services.copy_sizer import CopySizer, CopySizingConfig
from almanak.framework.services.copy_trading_models import CopySignal, CopyTradingConfig
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy


def _cfg(config: Any, key: str, default: Any) -> Any:
    """Read config value from dict-or-object."""
    if isinstance(config, dict):
        return config.get(key, default)
    return getattr(config, key, default)


@almanak_strategy(
    name="senior_quant_copy_trader",
    description="Weighted multi-leader copy trading strategy with strict risk filters",
    version="0.1.0",
    author="Senior DeFi Quant",
    tags=["copy-trading", "quant", "arbitrum", "swap"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3", "sushiswap_v3", "pancakeswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class SeniorQuantCopyTraderStrategy(IntentStrategy):
    """Copy selected leader swaps under conservative sizing and risk controls."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        ct_raw = _cfg(self.config, "copy_trading", {})
        self._ct_config = CopyTradingConfig.from_config(ct_raw)
        self._filters = ct_raw.get("filters", {})
        self._allowlisted_tokens = {t.upper() for t in self._filters.get("tokens", [])}

        sizing_dict = ct_raw.get("sizing", {})
        risk_dict = ct_raw.get("risk", {})
        self._max_slippage = Decimal(str(risk_dict.get("max_slippage", "0.005")))
        self._sizer = CopySizer(config=CopySizingConfig.from_config(sizing_dict, risk_dict))

        self._dry_run = bool(_cfg(self.config, "dry_run", False))
        self._signals_seen = 0
        self._signals_skipped = 0
        self._signals_copied = 0
        self._skip_reasons: dict[str, int] = {}
        self._last_signal_id: str | None = None

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Consume pending copy signals and return the first viable swap intent."""
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

        return Intent.hold(reason="No actionable signals")

    def _process_signal(self, signal: CopySignal, provider: Any) -> Intent | None:
        if signal.action_type != "SWAP" or len(signal.tokens) < 2:
            self._skip("not_swap_or_missing_tokens", signal, provider)
            return None

        token_in = signal.tokens[0]
        token_out = signal.tokens[1]
        if self._allowlisted_tokens and (
            token_in.upper() not in self._allowlisted_tokens or token_out.upper() not in self._allowlisted_tokens
        ):
            self._skip("token_not_allowlisted", signal, provider)
            return None

        skip_reason = self._sizer.get_skip_reason(signal)
        if skip_reason:
            self._skip(skip_reason, signal, provider)
            return None

        leader_weight = self._ct_config.get_leader_weight(signal.leader_address)
        size = self._sizer.compute_size(signal, leader_weight=leader_weight)
        if size is None:
            self._skip("size_below_min", signal, provider)
            return None

        if not self._sizer.check_daily_cap(size):
            self._consume(provider, signal.event_id)
            return Intent.hold(reason="Daily notional cap reached")
        if not self._sizer.check_position_cap():
            self._consume(provider, signal.event_id)
            return Intent.hold(reason="Position cap reached")

        size = size.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        if size <= 0:
            self._skip("size_below_min_after_rounding", signal, provider)
            return None
        if self._dry_run:
            self._signals_copied += 1
            self._consume(provider, signal.event_id)
            return None

        self._signals_copied += 1
        self._last_signal_id = signal.event_id
        return Intent.swap(
            from_token=token_in,
            to_token=token_out,
            amount_usd=size,
            max_slippage=self._max_slippage,
            protocol=signal.protocol,
            chain=signal.chain,
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Update sizing trackers and consume processed signal."""
        signal_id = self._last_signal_id
        if success and hasattr(intent, "amount_usd") and intent.amount_usd is not None:
            self._sizer.record_execution(intent.amount_usd)
            self._sizer.record_close()
            if signal_id:
                self._consume(getattr(self, "_wallet_activity_provider", None), signal_id)
                self._last_signal_id = None

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "senior_quant_copy_trader",
            "chain": self.chain,
            "dry_run": self._dry_run,
            "signals_seen": self._signals_seen,
            "signals_copied": self._signals_copied,
            "signals_skipped": self._signals_skipped,
            "skip_reasons": dict(self._skip_reasons),
            "sizing_mode": self._sizer.config.mode.value,
            "daily_notional": str(self._sizer._daily_notional),
            "open_positions": self._sizer._open_positions,
        }

    def _skip(self, reason: str, signal: CopySignal, provider: Any) -> None:
        self._signals_skipped += 1
        self._skip_reasons[reason] = self._skip_reasons.get(reason, 0) + 1
        self._consume(provider, signal.event_id)

    @staticmethod
    def _consume(provider: Any, event_id: str) -> None:
        if provider is not None:
            try:
                provider.consume_signals([event_id])
            except Exception:
                pass
