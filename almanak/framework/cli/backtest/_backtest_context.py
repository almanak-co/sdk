"""Validated context shared by the PnL backtest CLI helpers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ...backtesting import PnLBacktestConfig
    from ...backtesting.pnl.data_provider import TokenRef


@dataclass
class PnLBacktestContext:
    """Validated context for a single `pnl_backtest` invocation.

    Populated by `_validate_and_build_context` once CLI args have been parsed
    and (optionally) reconciled with a `--from-result` load. The orchestrating
    command holds exactly one of these for the lifetime of the run.
    """

    strategy: str
    pnl_config: PnLBacktestConfig
    token_list: Sequence[TokenRef]
    output_path: Path | None
    loaded_from_result: bool
    # Original CLI args preserved for downstream phases that still need them
    # (e.g. warm-cache uses `start`/`end`/`interval`; benchmark uses `start`/`end`).
    start: datetime | None
    end: datetime | None
    interval: int
