"""Immutable per-run context for the PnL backtest engine.

One object answers "which chain, which window, how strict" for a run.
Constructed once at initialization from the two config planes; threaded
instead of re-derived so partial reads cannot disagree.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.backtesting.config import BacktestDataConfig
    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig

__all__ = ["BacktestRunContext", "FidelityPolicy"]


@dataclass(frozen=True, slots=True)
class FidelityPolicy:
    """The run's single strictness answer.

    ``strict`` is the OR of every plane that used to carry its own flag
    (``strict_reproducibility``, ``institutional_mode``,
    ``strict_historical_mode``) so one plane defaulting to False can no
    longer silently soften a strict run.
    """

    strict: bool
    allow_degraded_data: bool
    allow_hardcoded_fallback: bool
    require_symbol_mapping: bool
    min_data_coverage: Decimal


@dataclass(frozen=True, slots=True)
class BacktestRunContext:
    chain: str
    start_time: datetime
    end_time: datetime
    interval_seconds: int
    fidelity: FidelityPolicy
    gas_funding_usd: Decimal | None

    @classmethod
    def from_configs(
        cls,
        config: PnLBacktestConfig,
        data_config: BacktestDataConfig | None = None,
    ) -> BacktestRunContext:
        strict = bool(
            config.strict_reproducibility
            or config.institutional_mode
            or (data_config is not None and data_config.strict_historical_mode)
        )
        return cls(
            chain=str(config.chain).lower(),
            start_time=config.start_time,
            end_time=config.end_time,
            interval_seconds=config.interval_seconds,
            fidelity=FidelityPolicy(
                strict=strict,
                allow_degraded_data=config.allow_degraded_data,
                allow_hardcoded_fallback=config.allow_hardcoded_fallback,
                require_symbol_mapping=config.require_symbol_mapping,
                min_data_coverage=config.min_data_coverage,
            ),
            gas_funding_usd=config.gas_funding_usd,
        )

    def chain_for(self, *candidates: Any) -> str:
        """The chain governing an intent/position/market-state, else the run default.

        Accepts objects with a ``chain`` attribute or plain strings; the first
        non-empty declared chain wins.
        """
        for candidate in candidates:
            declared = candidate if isinstance(candidate, str) else getattr(candidate, "chain", None)
            if declared:
                return str(declared).lower()
        return self.chain
