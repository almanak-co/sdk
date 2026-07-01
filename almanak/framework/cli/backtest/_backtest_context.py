"""Dataclass contexts shared by backtest CLI commands.

These contexts bundle the validated configuration used by `pnl_backtest` and
`sweep_backtest` so that extracted helpers can accept a single typed argument
rather than long positional argument lists. Phase 5B of the CLI CC reduction
plan.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ...backtesting import PnLBacktestConfig


@dataclass
class PnLBacktestContext:
    """Validated context for a single `pnl_backtest` invocation.

    Populated by `_validate_and_build_context` once CLI args have been parsed
    and (optionally) reconciled with a `--from-result` load. The orchestrating
    command holds exactly one of these for the lifetime of the run.
    """

    strategy: str
    pnl_config: PnLBacktestConfig
    token_list: list[str]
    output_path: Path | None
    loaded_from_result: bool
    # Original CLI args preserved for downstream phases that still need them
    # (e.g. warm-cache uses `start`/`end`/`interval`; benchmark uses `start`/`end`).
    start: datetime | None
    end: datetime | None
    interval: int


@dataclass
class SweepBacktestContext:
    """Validated context for a single `sweep_backtest` invocation.

    Wired in 5B.3; defined here so `run_helpers.py` and future sweep helpers
    can import from a stable location without churning imports.
    """

    strategy: str
    chain: str
    token_list: list[str]
    interval: int
    output_path: Path | None
    multi_period_mode: bool
    # Populated in later phases; defaults keep the dataclass instantiable
    # without forcing 5B.3 work into this PR.
    backtest_periods: list = field(default_factory=list)
    sweep_params: list = field(default_factory=list)
    combinations: list = field(default_factory=list)
