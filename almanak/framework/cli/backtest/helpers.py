"""Shared utilities, helper functions, and data classes for backtest CLI commands.

This module contains helpers used by multiple backtest subcommands, including
strategy instantiation, date parsing, result formatting, sweep data classes,
and paper trading session management.
"""

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import click

from almanak.core.chains._helpers import blocks_per_day_map

from ...backtesting import (
    BacktestResult,
    PaperTraderConfig,
)
from ...strategies import IntentStrategy, list_strategies
from .._strategy_config import coerce_strategy_config

# =============================================================================
# Configuration
# =============================================================================

# Approximate blocks per day per chain, derived from
# ``ChainDescriptor.rpc.block_time_seconds`` (round(86400 / block_time)).
# Read-only back-compat view: re-exported via ``backtest/__init__.py``, so the
# module-level name is preserved (VIB-4851).
BLOCKS_PER_DAY: Mapping[str, int] = blocks_per_day_map()

# Default recent block numbers for testing (when --end-block not specified)
# These are placeholder values - in production, would query chain for current block.
# Intentionally NOT descriptor-derived: there is no ChainDescriptor field for a
# representative recent block, so these literals stay here (VIB-4851 defers this).
DEFAULT_END_BLOCKS: dict[str, int] = {
    "ethereum": 19000000,
    "arbitrum": 170000000,
    "optimism": 115000000,
    "polygon": 52000000,
    "base": 10000000,
    "avalanche": 40000000,
}


# =============================================================================
# Helper Functions
# =============================================================================

# Placeholder wallet address for backtest strategy instantiation.
# Backtests don't execute real transactions so the address is irrelevant.
_BACKTEST_WALLET = "0x" + "0" * 40


def _create_backtest_strategy(
    strategy_class: Any,
    config: dict[str, Any],
    chain: str,
) -> Any:
    """Instantiate a strategy for backtesting.

    IntentStrategy subclasses are typed against a per-strategy config
    dataclass (``IntentStrategy[ConfigT]``) and read ``self.config.<field>``
    in ``__init__``, so the raw config dict is coerced through the same
    path the runner uses (``_strategy_config.coerce_strategy_config``)
    before construction. Construction errors propagate -- a strategy that
    cannot be built must fail the backtest loudly, not fall through to a
    wrong signature.

    Other classes (test doubles, duck-typed strategies) keep the legacy
    signature ladder: (config, chain, wallet), then (config,), then ().

    Args:
        strategy_class: The strategy class to instantiate.
        config: Strategy configuration dict.
        chain: Target chain (e.g. "arbitrum").

    Returns:
        An instantiated strategy object.
    """
    if isinstance(strategy_class, type) and issubclass(strategy_class, IntentStrategy):
        config_instance = coerce_strategy_config(strategy_class, config)
        return strategy_class(config_instance, chain, _BACKTEST_WALLET)

    # 1. Try IntentStrategy-shaped signature: (config, chain, wallet_address)
    try:
        return strategy_class(config, chain, _BACKTEST_WALLET)
    except TypeError:
        pass

    # 2. Try simple signature: (config,)
    try:
        return strategy_class(config)
    except TypeError:
        pass

    # 3. Fall back to no-arg constructor (mock strategies)
    return strategy_class()


def days_to_blocks(days: int, chain: str) -> int:
    """Convert days to approximate block count for a chain.

    Args:
        days: Number of days
        chain: Target blockchain

    Returns:
        Approximate number of blocks
    """
    blocks_per_day = BLOCKS_PER_DAY.get(chain, 7200)
    return days * blocks_per_day


def get_block_range(
    days: int,
    chain: str,
    end_block: int | None = None,
) -> tuple[int, int]:
    """Calculate start and end blocks for a backtest.

    Args:
        days: Number of days to backtest
        chain: Target blockchain
        end_block: Optional end block (defaults to recent block)

    Returns:
        Tuple of (start_block, end_block)
    """
    if end_block is None:
        end_block = DEFAULT_END_BLOCKS.get(chain, 19000000)

    blocks_needed = days_to_blocks(days, chain)
    start_block = max(0, end_block - blocks_needed)

    return start_block, end_block


def calculate_block_step(days: int, chain: str) -> int:
    """Calculate an appropriate block step for the backtest duration.

    Aims for roughly 100-1000 iterations for reasonable performance.

    Args:
        days: Number of days
        chain: Target blockchain

    Returns:
        Block step size
    """
    total_blocks = days_to_blocks(days, chain)

    # Target ~500 iterations
    target_iterations = 500
    step = max(1, total_blocks // target_iterations)

    # Round to nice numbers for readability
    if step >= 10000:
        step = (step // 10000) * 10000
    elif step >= 1000:
        step = (step // 1000) * 1000
    elif step >= 100:
        step = (step // 100) * 100
    elif step >= 10:
        step = (step // 10) * 10

    return step


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted string (e.g., "2m 30s")
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.0f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def print_results_summary(result: Any) -> None:
    """Print a formatted results summary to stdout.

    Args:
        result: Backtest result to display (legacy or new format)
    """
    click.echo(result.summary())


def print_detailed_results(result: Any) -> None:
    """Print detailed results including trade list (for legacy block-based backtest).

    Args:
        result: Backtest result to display (legacy format with block_number)
    """
    print_results_summary(result)

    if result.trades:
        click.echo()
        click.echo("-" * 60)
        click.echo("TRADE HISTORY")
        click.echo("-" * 60)

        for i, trade in enumerate(result.trades, 1):
            pnl_usd = getattr(trade, "pnl_usd", Decimal("0"))
            gas_cost_usd = getattr(trade, "gas_cost_usd", Decimal("0"))
            block_number = getattr(trade, "block_number", "N/A")
            intent_type_value = (
                trade.intent_type.value if hasattr(trade.intent_type, "value") else str(trade.intent_type)
            )
            pnl_sign = "+" if pnl_usd >= 0 else ""
            click.echo(
                f"{i:3}. Block {block_number}: "
                f"{intent_type_value:10} "
                f"{pnl_sign}${pnl_usd:,.2f} "
                f"(gas: ${gas_cost_usd:,.2f})"
            )

        click.echo("-" * 60)


def write_json_output(result: Any, output_path: Path) -> None:
    """Write full backtest results to a JSON file.

    Args:
        result: Backtest result (legacy or new format)
        output_path: Path to output file
    """
    output_data: dict[str, Any] = result.to_dict()
    output_data["_meta"] = {
        "generated_at": datetime.now(UTC).isoformat(),
        "generator": "almanak backtest",
    }

    with open(output_path, "w") as f:
        json.dump(output_data, f, indent=2, default=str)

    click.echo(f"Results written to: {output_path}")


@dataclass
class BacktestContext:
    """Context for backtest execution."""

    strategy_name: str
    days: int
    chain: str
    start_block: int
    end_block: int
    block_step: int
    initial_balance: Decimal
    gas_price: Decimal
    archive_rpc: str | None
    output_path: Path | None
    verbose: bool


def load_strategy_config(strategy_name: str, chain: str) -> dict[str, Any]:
    """Load default configuration for a strategy.

    This attempts to load config from a JSON file or creates
    a minimal default configuration.

    Args:
        strategy_name: Name of the strategy
        chain: Target chain

    Returns:
        Configuration dictionary
    """
    # Strip demo_/incubating_ prefix to get the directory name
    # e.g. "demo_uniswap_rsi" -> "uniswap_rsi"
    dir_name = strategy_name
    if dir_name.startswith("demo_"):
        dir_name = dir_name[len("demo_") :]
    elif dir_name.startswith("incubating_"):
        dir_name = dir_name[len("incubating_") :]

    # Look for config file in standard locations.
    # Strategy-name-specific paths are checked first so that invoking
    # `backtest pnl -s <other>` from inside strategy A's directory still finds
    # <other>'s config file instead of A's generic `./config.json`.
    config_paths = [
        Path(f"configs/{strategy_name}.json"),
        Path(f"configs/{strategy_name}_{chain}.json"),
        Path(f"src/strategies/{strategy_name}/config.json"),
        # Demo and incubating strategy directories
        Path(f"almanak/demo_strategies/{dir_name}/config.json"),
        Path(f"almanak/demo_strategies/{strategy_name}/config.json"),
        Path(f"strategies/incubating/{dir_name}/config.json"),
        Path(f"strategies/incubating/{strategy_name}/config.json"),
    ]
    # VIB-2917: `./config.json` is only consulted when cwd also contains
    # `./strategy.py` (i.e. we're clearly inside a strategy directory). This
    # matches the cwd auto-discovery flow while avoiding two footguns:
    # picking up an unrelated `config.json` in a random directory, and
    # overriding the requested strategy's own `configs/<name>.json` when the
    # user backtests a different strategy from inside another's folder.
    if Path("strategy.py").exists():
        config_paths.append(Path("config.json"))

    for path in config_paths:
        if path.exists():
            with open(path) as f:
                config: dict[str, Any] = json.load(f)
                click.echo(f"Loaded config from: {path}")
                return config

    # Return default minimal config
    return {
        "deployment_id": f"backtest-{strategy_name}-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "wallet_address": "0x" + "0" * 40,  # Placeholder
    }


# Alias to avoid conflict with --list-strategies option
list_strategies_fn = list_strategies


def parse_date(ctx: Any, param: Any, value: str | None) -> datetime | None:
    """Parse date string to datetime object."""
    if value is None:
        return None
    try:
        # Try common formats
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(value, fmt).replace(tzinfo=UTC)
            except ValueError:
                continue
        raise click.BadParameter(f"Cannot parse date '{value}'. Use YYYY-MM-DD format.")
    except Exception as e:
        raise click.BadParameter(str(e)) from e


# =============================================================================
# Sweep Data Classes & Helpers
# =============================================================================


@dataclass
class SweepParameter:
    """A parameter with multiple values to sweep."""

    name: str
    values: list[str]


@dataclass
class SweepResult:
    """Result from a single parameter combination backtest."""

    params: dict[str, str]
    result: BacktestResult
    sharpe_ratio: Decimal
    total_return_pct: Decimal
    max_drawdown_pct: Decimal
    win_rate: Decimal
    total_trades: int
    period_name: str = ""  # populated when using multi-period sweeps


@dataclass
class AggregatedParamResult:
    """Aggregated results for one parameter set across multiple periods."""

    params: dict[str, str]
    per_period: list[SweepResult]
    avg_sharpe: float
    avg_return_pct: float
    avg_max_dd_pct: float
    avg_trades: float
    cumulative_pnl: float
    sharpe_std: float  # lower = more robust across periods


def parse_param_string(param_str: str) -> SweepParameter:
    """Parse a parameter string in format 'name:val1,val2,val3'.

    Args:
        param_str: Parameter string to parse

    Returns:
        SweepParameter with name and values

    Raises:
        click.BadParameter: If format is invalid
    """
    if ":" not in param_str:
        raise click.BadParameter(f"Invalid parameter format: '{param_str}'. Expected 'name:val1,val2,val3'")

    name, values_str = param_str.split(":", 1)
    name = name.strip()

    if not name:
        raise click.BadParameter("Parameter name cannot be empty")

    values = [v.strip() for v in values_str.split(",") if v.strip()]

    if not values:
        raise click.BadParameter(f"Parameter '{name}' has no values")

    return SweepParameter(name=name, values=values)


def generate_combinations(params: list[SweepParameter]) -> list[dict[str, str]]:
    """Generate all combinations of parameter values (cartesian product).

    Args:
        params: List of parameters with their values

    Returns:
        List of dictionaries, each representing one parameter combination
    """
    if not params:
        return [{}]

    # Use itertools.product for cartesian product
    import itertools

    names = [p.name for p in params]
    value_lists = [p.values for p in params]

    combinations = []
    for values in itertools.product(*value_lists):
        combo = dict(zip(names, values, strict=False))
        combinations.append(combo)

    return combinations


# =============================================================================
# Paper Trading Session Management
# =============================================================================


PAPER_STATE_DIR = Path.home() / ".almanak" / "paper_sessions"


def get_paper_state_file(deployment_id: str) -> Path:
    """Get the state file path for a paper trading session.

    Args:
        deployment_id: Deployment identifier

    Returns:
        Path to the session state file
    """
    PAPER_STATE_DIR.mkdir(parents=True, exist_ok=True)
    return PAPER_STATE_DIR / f"{deployment_id}.json"


def save_paper_session_state(
    deployment_id: str,
    pid: int,
    config: PaperTraderConfig,
    start_time: datetime,
) -> None:
    """Save paper trading session state to disk.

    Args:
        deployment_id: Deployment identifier
        pid: Process ID of the background session
        config: PaperTraderConfig used for the session
        start_time: Session start time
    """
    state_file = get_paper_state_file(deployment_id)
    state = {
        "deployment_id": deployment_id,
        "pid": pid,
        "config": config.to_dict(),
        "start_time": start_time.isoformat(),
        "status": "running",
    }
    with open(state_file, "w") as f:
        json.dump(state, f, indent=2)


def load_paper_session_state(deployment_id: str) -> dict[str, Any] | None:
    """Load paper trading session state from disk.

    Args:
        deployment_id: Deployment identifier

    Returns:
        Session state dictionary or None if not found
    """
    state_file = get_paper_state_file(deployment_id)
    if not state_file.exists():
        return None
    try:
        with open(state_file) as f:
            state: dict[str, Any] = json.load(f)
            return state
    except Exception:
        return None


def update_paper_session_status(deployment_id: str, status: str, summary: dict[str, Any] | None = None) -> None:
    """Update paper trading session status.

    Args:
        deployment_id: Deployment identifier
        status: New status (running, stopped, completed, error)
        summary: Optional session summary data
    """
    state_file = get_paper_state_file(deployment_id)
    if not state_file.exists():
        return
    try:
        with open(state_file) as f:
            state = json.load(f)
        state["status"] = status
        state["last_updated"] = datetime.now(UTC).isoformat()
        if summary:
            state["summary"] = summary
        with open(state_file, "w") as f:
            json.dump(state, f, indent=2)
    except Exception:
        pass


def delete_paper_session_state(deployment_id: str) -> None:
    """Delete paper trading session state file.

    Args:
        deployment_id: Deployment identifier
    """
    state_file = get_paper_state_file(deployment_id)
    if state_file.exists():
        state_file.unlink()


def is_process_running(pid: int) -> bool:
    """Check if a process is still running.

    Args:
        pid: Process ID

    Returns:
        True if process is running
    """
    try:
        os.kill(pid, 0)  # Signal 0 doesn't kill, just checks
        return True
    except OSError:
        return False


def list_paper_sessions() -> list[dict[str, Any]]:
    """List all paper trading sessions.

    Returns:
        List of session state dictionaries
    """
    if not PAPER_STATE_DIR.exists():
        return []

    sessions = []
    for state_file in PAPER_STATE_DIR.glob("*.json"):
        try:
            with open(state_file) as f:
                state = json.load(f)
                # Check if process is still running
                pid = state.get("pid")
                if pid and not is_process_running(pid):
                    state["status"] = "stopped (process not found)"
                sessions.append(state)
        except Exception:
            pass

    return sessions


def _parse_duration(duration_str: str) -> int | None:
    """Parse a human-readable duration string into seconds.

    Supports: '30s', '5m', '1h', '2h30m', '90m', '1h30m15s'.

    Returns:
        Total seconds, or None if the string is unparseable.
    """
    duration_str = duration_str.strip().lower()
    if not duration_str:
        return None

    # Try pure integer (treat as seconds)
    if duration_str.isdigit():
        total = int(duration_str)
        return total if total > 0 else None

    pattern = re.compile(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$")
    match = pattern.match(duration_str)
    if not match or not any(match.groups()):
        return None

    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    total = hours * 3600 + minutes * 60 + seconds
    return total if total > 0 else None
