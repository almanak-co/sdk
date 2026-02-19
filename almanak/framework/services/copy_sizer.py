"""Copy trade sizing with mode selection and risk cap enforcement.

Translates leader trade sizes into follower trade sizes using configurable
sizing modes (fixed USD, proportion of leader, or proportion of equity),
with daily notional and position caps.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.services.copy_trading_models import CopySignal, SizingMode

logger = logging.getLogger(__name__)


@dataclass
class CopySizingConfig:
    """Configuration for copy trade sizing and risk caps."""

    mode: SizingMode = SizingMode.FIXED_USD
    fixed_usd: Decimal = Decimal("100")
    percentage_of_leader: Decimal = Decimal("0.1")
    percentage_of_equity: Decimal = Decimal("0.02")
    max_trade_usd: Decimal = Decimal("1000")
    min_trade_usd: Decimal = Decimal("10")
    max_daily_notional_usd: Decimal = Decimal("10000")
    max_open_positions: int = 10

    @classmethod
    def from_config(cls, sizing_dict: dict, risk_dict: dict) -> "CopySizingConfig":
        """Build from the sizing and risk blocks of config.json."""
        mode = SizingMode(sizing_dict.get("mode", "fixed_usd"))
        return cls(
            mode=mode,
            fixed_usd=Decimal(str(sizing_dict.get("fixed_usd", 100))),
            percentage_of_leader=Decimal(str(sizing_dict.get("percentage_of_leader", 0.1))),
            percentage_of_equity=Decimal(str(sizing_dict.get("percentage_of_equity", 0.02))),
            max_trade_usd=Decimal(str(risk_dict.get("max_trade_usd", 1000))),
            min_trade_usd=Decimal(str(risk_dict.get("min_trade_usd", 10))),
            max_daily_notional_usd=Decimal(str(risk_dict.get("max_daily_notional_usd", 10000))),
            max_open_positions=int(risk_dict.get("max_open_positions", 10)),
        )


@dataclass
class CopySizer:
    """Computes follower trade sizes from leader signals with cap enforcement."""

    config: CopySizingConfig
    portfolio_value_fn: Callable[[], Decimal] | None = None
    _daily_notional: Decimal = field(default=Decimal("0"), init=False)
    _daily_date: str = field(default="", init=False)
    _open_positions: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        self._daily_date = datetime.now(UTC).strftime("%Y-%m-%d")

    def compute_size(self, signal: CopySignal, leader_weight: Decimal | None = None) -> Decimal | None:
        """Compute the USD trade size for a signal, or None if blocked by caps.

        Returns None if the computed size falls below min_trade_usd or if
        proportional mode is selected but required data is unavailable.
        """
        if self.config.mode == SizingMode.FIXED_USD:
            size = self.config.fixed_usd
        elif self.config.mode == SizingMode.PROPORTION_OF_LEADER:
            if not signal.amounts_usd:
                logger.warning(
                    "proportion_of_leader sizing requires USD amounts but signal %s has no price data; skipping.",
                    signal.event_id,
                )
                return None
            # max() avoids double-counting swap legs (e.g. USDC:$5k + WETH:$5k = $5k, not $10k)
            leader_usd = max((abs(v) for v in signal.amounts_usd.values()), default=Decimal("0"))
            size = leader_usd * self.config.percentage_of_leader
        elif self.config.mode == SizingMode.PROPORTION_OF_EQUITY:
            if self.portfolio_value_fn is None:
                logger.warning("proportion_of_equity sizing requires portfolio_value_fn but none provided; skipping.")
                return None
            try:
                portfolio_value = self.portfolio_value_fn()
            except Exception:
                logger.exception("Failed to get portfolio value for equity sizing")
                return None
            if portfolio_value <= 0:
                return None
            size = portfolio_value * self.config.percentage_of_equity
        else:
            size = self.config.fixed_usd

        # Apply leader weight multiplier if provided
        if leader_weight is not None:
            size = size * leader_weight

        size = min(size, self.config.max_trade_usd)

        if size < self.config.min_trade_usd:
            return None

        return size

    def compute_target_notional(self, signal: CopySignal, leader_weight: Decimal | None = None) -> Decimal | None:
        """Alias for compute_size() with explicit copy-notional semantics."""
        return self.compute_size(signal, leader_weight=leader_weight)

    def compute_action_scale(
        self,
        signal: CopySignal,
        leader_notional_usd: Decimal,
        leader_weight: Decimal | None = None,
    ) -> Decimal | None:
        """Compute multiplicative scale for non-swap action payload amounts.

        Returns:
            Decimal scale factor, or None if sizing is blocked.
        """
        target = self.compute_target_notional(signal, leader_weight=leader_weight)
        if target is None:
            return None
        if leader_notional_usd <= 0:
            return Decimal("1")
        return target / leader_notional_usd

    def check_daily_cap(self, proposed_usd: Decimal) -> bool:
        """Return True if adding proposed_usd stays under the daily cap."""
        self._maybe_reset_daily()
        return (self._daily_notional + proposed_usd) <= self.config.max_daily_notional_usd

    def check_position_cap(self) -> bool:
        """Return True if there is room for another open position."""
        return self._open_positions < self.config.max_open_positions

    def record_execution(self, usd_amount: Decimal) -> None:
        """Record an executed trade: add to daily notional, increment positions."""
        self._maybe_reset_daily()
        self._daily_notional += usd_amount
        self._open_positions += 1

    def record_close(self) -> None:
        """Record a position close: decrement open positions."""
        if self._open_positions > 0:
            self._open_positions -= 1

    def get_skip_reason(self, signal: CopySignal) -> str | None:
        """Return the reason a signal would be blocked, or None if OK."""
        if self.config.mode == SizingMode.PROPORTION_OF_LEADER and not signal.amounts_usd:
            return "no_usd_amounts_for_proportional_sizing"

        if self.config.mode == SizingMode.PROPORTION_OF_EQUITY and self.portfolio_value_fn is None:
            return "no_portfolio_value_fn_for_equity_sizing"

        size = self.compute_size(signal)
        if size is None:
            return "below_min_usd"

        if not self.check_daily_cap(size):
            return "daily_cap_reached"

        if not self.check_position_cap():
            return "position_cap_reached"

        return None

    def _maybe_reset_daily(self) -> None:
        """Reset daily counters if the date has changed."""
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        if today != self._daily_date:
            self._daily_notional = Decimal("0")
            self._daily_date = today
