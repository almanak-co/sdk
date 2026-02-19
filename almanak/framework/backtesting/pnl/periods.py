"""Named period presets for multi-period backtesting.

Provides built-in period definitions (quarterly, monthly) and a loader
for custom period JSON files so that ``sweep`` and ``optimize`` can
evaluate parameter sets across multiple time windows in a single command.

Usage:
    from almanak.framework.backtesting.pnl.periods import resolve_periods, PERIOD_PRESETS

    periods = resolve_periods("2024-quarterly")
    for name, start, end in periods:
        print(f"{name}: {start} -> {end}")

    # Custom JSON file
    periods = resolve_periods("path/to/periods.json")
"""

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BacktestPeriod:
    """A named time window for backtesting."""

    name: str
    start: datetime
    end: datetime


# =============================================================================
# Built-in Presets
# =============================================================================

_Q = [
    (1, 1, 3, 31),
    (4, 1, 6, 30),
    (7, 1, 9, 30),
    (10, 1, 12, 31),
]


def _quarterly(year: int) -> list[BacktestPeriod]:
    return [
        BacktestPeriod(
            name=f"{year} Q{i + 1}",
            start=datetime(year, sm, sd),
            end=datetime(year, em, ed, 23, 59, 59),
        )
        for i, (sm, sd, em, ed) in enumerate(_Q)
    ]


def _monthly(year: int) -> list[BacktestPeriod]:
    import calendar

    periods = []
    for m in range(1, 13):
        last_day = calendar.monthrange(year, m)[1]
        periods.append(
            BacktestPeriod(
                name=f"{year}-{m:02d}",
                start=datetime(year, m, 1),
                end=datetime(year, m, last_day, 23, 59, 59),
            )
        )
    return periods


def _rolling_6m() -> list[BacktestPeriod]:
    """Six rolling 6-month windows ending at the most recent quarter boundary."""
    import calendar
    from datetime import timedelta

    now = datetime.now()

    # Compute current quarter's end date
    q_start_month = ((now.month - 1) // 3) * 3 + 1
    end_month = q_start_month + 2
    end_year = now.year
    end_day = calendar.monthrange(end_year, end_month)[1]
    latest_end = datetime(end_year, end_month, end_day, 23, 59, 59)

    # If the current quarter hasn't ended yet, step back to the previous quarter
    if latest_end > now:
        end_month -= 3
        if end_month <= 0:
            end_month += 12
            end_year -= 1
        end_day = calendar.monthrange(end_year, end_month)[1]
        latest_end = datetime(end_year, end_month, end_day, 23, 59, 59)

    periods = []
    for i in range(6):
        window_end = latest_end - timedelta(days=180 * i)
        window_start = (window_end - timedelta(days=180)).replace(hour=0, minute=0, second=0)
        periods.append(
            BacktestPeriod(
                name=f"6M-{i + 1} ({window_start.strftime('%Y-%m-%d')} to {window_end.strftime('%Y-%m-%d')})",
                start=window_start,
                end=window_end,
            )
        )
    periods.reverse()
    return periods


PERIOD_PRESETS: dict[str, list[BacktestPeriod]] = {
    "2023-quarterly": _quarterly(2023),
    "2024-quarterly": _quarterly(2024),
    "2025-quarterly": _quarterly(2025),
    "2023-monthly": _monthly(2023),
    "2024-monthly": _monthly(2024),
    "2025-monthly": _monthly(2025),
}

# Dynamic presets computed at call time
_DYNAMIC_PRESETS: dict[str, Callable[[], list[BacktestPeriod]]] = {
    "rolling-6m": _rolling_6m,
}


# =============================================================================
# Public API
# =============================================================================


def list_presets() -> list[str]:
    """Return sorted list of available preset names."""
    return sorted(list(PERIOD_PRESETS.keys()) + list(_DYNAMIC_PRESETS.keys()))


def resolve_periods(spec: str) -> list[BacktestPeriod]:
    """Resolve a period specification to a list of BacktestPeriod objects.

    Args:
        spec: Either a preset name (e.g., "2024-quarterly"), or a path to
              a JSON file with custom period definitions.

    Returns:
        List of BacktestPeriod objects.

    Raises:
        ValueError: If spec is not a valid preset or file path.
        json.JSONDecodeError: If JSON file is malformed.
    """
    # Check static presets
    if spec in PERIOD_PRESETS:
        return PERIOD_PRESETS[spec]

    # Check dynamic presets
    if spec in _DYNAMIC_PRESETS:
        return _DYNAMIC_PRESETS[spec]()

    # Try as file path
    path = Path(spec)
    if path.exists() and path.suffix == ".json":
        return _load_periods_from_json(path)

    available = ", ".join(list_presets())
    raise ValueError(
        f"Unknown period spec: '{spec}'. Available presets: {available}. Or provide a path to a JSON file."
    )


def _load_periods_from_json(path: Path) -> list[BacktestPeriod]:
    """Load periods from a JSON file.

    Expected format:
        [
            {"name": "Bull Run", "start": "2024-01-01", "end": "2024-03-31"},
            {"name": "Bear Market", "start": "2024-04-01", "end": "2024-06-30"}
        ]
    """
    data = json.loads(path.read_text())
    if not isinstance(data, list):
        raise ValueError(f"Period JSON must be a list, got {type(data).__name__}")

    periods = []
    for i, entry in enumerate(data):
        if not isinstance(entry, dict):
            raise ValueError(f"Period entry {i} must be a dict, got {type(entry).__name__}")

        for key in ("name", "start", "end"):
            if key not in entry:
                raise ValueError(f"Period entry {i} missing required key: '{key}'")

        start = datetime.strptime(entry["start"], "%Y-%m-%d")
        end = datetime.strptime(entry["end"], "%Y-%m-%d").replace(hour=23, minute=59, second=59)

        if start >= end:
            raise ValueError(f"Period '{entry['name']}': start ({entry['start']}) must be before end ({entry['end']})")

        periods.append(BacktestPeriod(name=entry["name"], start=start, end=end))

    if not periods:
        raise ValueError("Period JSON file is empty")

    logger.info("Loaded %d custom periods from %s", len(periods), path)
    return periods
