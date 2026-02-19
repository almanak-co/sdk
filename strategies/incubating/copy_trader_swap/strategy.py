"""Copy Trader Swap Strategy - Reference implementation for copy trading.

Monitors leader wallets on-chain and replicates their swap trades using
configurable sizing modes and risk caps. Single-chain, swaps-only.

How it works:
    1. WalletMonitor polls for leader transactions via gateway RPC
    2. CopySignalEngine decodes receipts into CopySignals
    3. Strategy reads signals via market.wallet_activity()
    4. CopySizer applies sizing mode and risk caps
    5. Strategy emits SwapIntent or HoldIntent

Usage:
    # Replace the leader address in config.json with a real address
    almanak strat run -d strategies/incubating/copy_trader_swap --network anvil --once
"""

import logging
from decimal import ROUND_DOWN, Decimal
from typing import Any

from almanak.framework.intents import Intent
from almanak.framework.services.copy_sizer import CopySizer, CopySizingConfig
from almanak.framework.services.copy_trading_models import CopySignal
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)


def _get_config(config: Any, key: str, default: Any) -> Any:
    """Get a config value from dict or object attributes."""
    if isinstance(config, dict):
        return config.get(key, default)
    return getattr(config, key, default)


@almanak_strategy(
    name="copy_trader_swap_demo",
    description="Monitors a leader wallet and copies swap trades",
    version="0.1.0",
    author="Almanak",
    tags=["copy-trading", "swap", "incubating"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3"],
    intent_types=["SWAP", "HOLD"],
)
class CopyTraderSwapStrategy(IntentStrategy):
    """Copy trading strategy that replicates leader swaps with risk caps."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        ct_config = _get_config(self.config, "copy_trading", {})
        self._filters = ct_config.get("filters", {})

        sizing_dict = ct_config.get("sizing", {})
        risk_dict = ct_config.get("risk", {})
        self._max_slippage = Decimal(str(risk_dict.get("max_slippage", "0.01")))
        self._sizer = CopySizer(config=CopySizingConfig.from_config(sizing_dict, risk_dict))
        self._last_acted_signal_id: str | None = None

        logger.info(
            f"CopyTraderSwapStrategy initialized: "
            f"sizing={self._sizer.config.mode.value}, "
            f"max_trade=${self._sizer.config.max_trade_usd}, "
            f"max_slippage={self._max_slippage}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Check for leader signals and emit SwapIntent or HoldIntent.

        Iterates through pending signals, consuming (discarding) any that
        are blocked by sizer checks, and executes the first viable one.
        This prevents a blocked head signal from starving the queue.
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
                if signal.action_type != "SWAP" or len(signal.tokens) < 2:
                    self._consume_signal(provider, signal.event_id)
                    continue

                skip_reason = self._sizer.get_skip_reason(signal)
                if skip_reason:
                    logger.info(f"Skipping signal {signal.event_id}: {skip_reason}")
                    self._consume_signal(provider, signal.event_id)
                    continue

                size = self._sizer.compute_size(signal)
                if size is None:
                    self._consume_signal(provider, signal.event_id)
                    continue

                if not self._sizer.check_daily_cap(size):
                    return Intent.hold(reason="Daily notional cap reached")

                if not self._sizer.check_position_cap():
                    return Intent.hold(reason="Position cap reached")

                from_token = signal.tokens[0]
                to_token = signal.tokens[1]

                logger.info(
                    f"Copying swap: {from_token} -> {to_token}, "
                    f"size=${size}, protocol={signal.protocol}, "
                    f"leader={signal.leader_address[:10]}..."
                )

                self._last_acted_signal_id = signal.event_id

                # Round down to avoid overspending
                size = size.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

                return Intent.swap(
                    from_token=from_token,
                    to_token=to_token,
                    amount_usd=size,
                    max_slippage=self._max_slippage,
                    protocol=signal.protocol,
                )

            return Intent.hold(reason="No actionable signals")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Log copy execution results, update sizer, and consume the signal."""
        signal_id = getattr(self, "_last_acted_signal_id", None)
        if success and hasattr(intent, "amount_usd") and intent.amount_usd is not None:
            self._sizer.record_execution(intent.amount_usd)
            self._sizer.record_close()  # Swaps are atomic -- no open position to track
            if signal_id:
                self._consume_signal(getattr(self, "_wallet_activity_provider", None), signal_id)
                self._last_acted_signal_id = None
            logger.info(
                f"Copy trade executed: {intent.from_token} -> {intent.to_token}, "
                f"size=${intent.amount_usd}"
            )
        elif not success:
            logger.warning(f"Copy trade failed: {result}")

    @staticmethod
    def _consume_signal(provider: Any, event_id: str) -> None:
        """Consume a signal from the activity provider (best-effort)."""
        if provider is not None:
            try:
                provider.consume_signals([event_id])
            except Exception:
                pass

    def get_status(self) -> dict[str, Any]:
        """Return current strategy status for monitoring."""
        return {
            "strategy": "copy_trader_swap_demo",
            "chain": self.chain,
            "sizing_mode": self._sizer.config.mode.value,
            "daily_notional": str(self._sizer._daily_notional),
            "open_positions": self._sizer._open_positions,
        }
