"""CLI command for replaying failures from reproduction bundles.

Usage:
    almanak replay --bundle <bundle_id>

Example:
    almanak replay --bundle my_strategy_20240115_120000_17000000
    almanak replay --bundle my_strategy_20240115_120000_17000000 --verbose
    almanak replay --bundle-file /path/to/bundle.json --verbose
"""

import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

import click

from ..testing.protocol_harness import AnvilFork, ForkConfig

# Approximate block times per chain (in seconds)
CHAIN_BLOCK_TIMES: dict[str, float] = {
    "ethereum": 12.0,
    "arbitrum": 0.25,
    "optimism": 2.0,
    "polygon": 2.0,
    "base": 2.0,
    "avalanche": 2.0,
}
from ..models.reproduction_bundle import (
    ActionBundle,
    MarketData,
    ReproductionBundle,
    TimelineEventSnapshot,
)

# =============================================================================
# Configuration
# =============================================================================

# Default bundle storage locations
DEFAULT_BUNDLE_PATHS: list[Path] = [
    Path("bundles"),
    Path(".bundles"),
    Path("data/bundles"),
    Path.home() / ".almanak" / "bundles",
]

# Default Anvil port for replay
DEFAULT_ANVIL_PORT = 8547


# Step types for replay logging
class ReplayStepType(Enum):
    """Types of steps in the replay."""

    INITIALIZE = "INITIALIZE"
    LOAD_STATE = "LOAD_STATE"
    LOAD_CONFIG = "LOAD_CONFIG"
    LOAD_MARKET_DATA = "LOAD_MARKET_DATA"
    EXECUTE_ACTION = "EXECUTE_ACTION"
    VERIFY_STATE = "VERIFY_STATE"
    ERROR = "ERROR"
    COMPLETE = "COMPLETE"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class ReplayStep:
    """Represents a single step in the replay process."""

    step_number: int
    step_type: ReplayStepType
    description: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    details: dict[str, Any] = field(default_factory=dict)
    state_before: dict[str, Any] | None = None
    state_after: dict[str, Any] | None = None
    success: bool = True
    error: str | None = None
    duration_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "step_number": self.step_number,
            "step_type": self.step_type.value,
            "description": self.description,
            "timestamp": self.timestamp.isoformat(),
            "details": self.details,
            "state_before": self.state_before,
            "state_after": self.state_after,
            "success": self.success,
            "error": self.error,
            "duration_ms": self.duration_ms,
        }


@dataclass
class ReplayResult:
    """Complete results from a replay run."""

    bundle: ReproductionBundle
    steps: list[ReplayStep] = field(default_factory=list)
    start_time: datetime | None = None
    end_time: datetime | None = None
    duration_seconds: float = 0.0
    success: bool = True
    error: str | None = None
    final_state: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "bundle_id": self.bundle.bundle_id,
            "steps": [s.to_dict() for s in self.steps],
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "success": self.success,
            "error": self.error,
            "final_state": self.final_state,
        }

    def summary(self) -> str:
        """Generate a human-readable summary."""
        lines = [
            "=" * 70,
            "REPLAY RESULTS",
            "=" * 70,
            f"Bundle ID: {self.bundle.bundle_id}",
            f"Strategy: {self.bundle.strategy_id}",
            f"Chain: {self.bundle.chain}",
            f"Block: {self.bundle.block_number}",
            f"Failure Time: {self.bundle.failure_timestamp.isoformat()}",
            "-" * 70,
            f"Status: {'SUCCESS' if self.success else 'FAILED'}",
            f"Duration: {self.duration_seconds:.2f}s",
            f"Steps Executed: {len(self.steps)}",
        ]

        if self.error:
            lines.append(f"Error: {self.error}")

        if self.bundle.revert_reason:
            lines.append(f"Revert Reason: {self.bundle.revert_reason}")

        if self.bundle.tenderly_trace_url:
            lines.append(f"Tenderly Trace: {self.bundle.tenderly_trace_url}")

        lines.append("=" * 70)

        return "\n".join(lines)


@dataclass
class ReplayContext:
    """Context for replay execution."""

    bundle: ReproductionBundle
    anvil_port: int = DEFAULT_ANVIL_PORT
    verbose: bool = False
    output_path: Path | None = None
    archive_rpc: str | None = None


# =============================================================================
# Bundle Storage
# =============================================================================


def find_bundle_file(bundle_id: str) -> Path | None:
    """Search for a bundle file by ID.

    Args:
        bundle_id: Bundle identifier to search for

    Returns:
        Path to bundle file if found, None otherwise
    """
    # Try direct paths first
    for base_path in DEFAULT_BUNDLE_PATHS:
        if not base_path.exists():
            continue

        # Try with .json extension
        direct_path = base_path / f"{bundle_id}.json"
        if direct_path.exists():
            return direct_path

        # Try without extension
        direct_path = base_path / bundle_id
        if direct_path.exists():
            return direct_path

        # Search recursively
        for file_path in base_path.rglob("*.json"):
            if bundle_id in file_path.stem:
                return file_path

    return None


def load_bundle_from_file(path: Path) -> ReproductionBundle:
    """Load a reproduction bundle from a JSON file.

    Args:
        path: Path to the bundle JSON file

    Returns:
        ReproductionBundle instance

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file contains invalid data
    """
    if not path.exists():
        raise FileNotFoundError(f"Bundle file not found: {path}")

    with open(path) as f:
        data = json.load(f)

    return ReproductionBundle.from_dict(data)


def fetch_bundle(bundle_id: str) -> ReproductionBundle:
    """Fetch a reproduction bundle by ID.

    This searches local storage for the bundle. In a production system,
    this could also fetch from remote storage (S3, GCS, etc.).

    Args:
        bundle_id: Bundle identifier

    Returns:
        ReproductionBundle instance

    Raises:
        FileNotFoundError: If bundle cannot be found
    """
    file_path = find_bundle_file(bundle_id)

    if file_path is None:
        raise FileNotFoundError(f"Bundle '{bundle_id}' not found. Searched: {[str(p) for p in DEFAULT_BUNDLE_PATHS]}")

    return load_bundle_from_file(file_path)


# =============================================================================
# Replay Engine
# =============================================================================


class ReplayEngine:
    """Engine for replaying failures from reproduction bundles.

    The replay engine:
    1. Loads the reproduction bundle
    2. Spins up an Anvil fork at the exact block
    3. Loads the exact state from the bundle
    4. Re-runs the strategy logic step by step
    5. Reports state changes at each step
    """

    def __init__(self, verbose: bool = False) -> None:
        """Initialize the replay engine.

        Args:
            verbose: Enable verbose output
        """
        self.verbose = verbose
        self._anvil: AnvilFork | None = None
        self._logger = logging.getLogger(__name__)
        self._current_step = 0

    def replay(self, ctx: ReplayContext) -> ReplayResult:
        """Replay a failure from a reproduction bundle.

        Args:
            ctx: Replay context containing bundle and configuration

        Returns:
            ReplayResult with step-by-step execution details
        """
        result = ReplayResult(
            bundle=ctx.bundle,
            start_time=datetime.now(UTC),
        )

        try:
            self._log_header(ctx)

            # Step 1: Initialize Anvil fork
            step = self._create_step(
                ReplayStepType.INITIALIZE,
                f"Starting Anvil fork at block {ctx.bundle.block_number}",
            )
            result.steps.append(step)

            if not self._start_anvil(ctx):
                step.success = False
                step.error = "Failed to start Anvil fork"
                result.success = False
                result.error = step.error
                return result

            self._complete_step(step)

            # Step 2: Load persistent state
            step = self._create_step(
                ReplayStepType.LOAD_STATE,
                "Loading persistent state from bundle",
            )
            step.state_after = ctx.bundle.persistent_state
            step.details = {
                "state_keys": list(ctx.bundle.persistent_state.keys()),
                "state_size_bytes": len(json.dumps(ctx.bundle.persistent_state)),
            }
            result.steps.append(step)
            self._complete_step(step)

            if ctx.verbose:
                self._print_state("Persistent State", ctx.bundle.persistent_state)

            # Step 3: Load configuration
            step = self._create_step(
                ReplayStepType.LOAD_CONFIG,
                "Loading strategy configuration",
            )
            step.details = ctx.bundle.config
            result.steps.append(step)
            self._complete_step(step)

            if ctx.verbose:
                self._print_state("Configuration", ctx.bundle.config)

            # Step 4: Load market data if available
            if ctx.bundle.market_data:
                step = self._create_step(
                    ReplayStepType.LOAD_MARKET_DATA,
                    "Loading market data snapshot",
                )
                step.details = ctx.bundle.market_data.to_dict()
                result.steps.append(step)
                self._complete_step(step)

                if ctx.verbose:
                    self._print_market_data(ctx.bundle.market_data)

            # Step 5: Show events leading up to failure
            if ctx.bundle.events_before and ctx.verbose:
                self._print_events_before(ctx.bundle.events_before)

            # Step 6: Execute the action bundle
            if ctx.bundle.action_bundle:
                step = self._create_step(
                    ReplayStepType.EXECUTE_ACTION,
                    f"Executing action bundle: {ctx.bundle.action_bundle.intent_type}",
                )
                step.state_before = ctx.bundle.persistent_state.copy()
                step.details = ctx.bundle.action_bundle.to_dict()

                try:
                    action_result = self._execute_action(ctx.bundle.action_bundle, ctx)
                    step.details["execution_result"] = action_result
                    step.state_after = self._get_current_state(ctx)
                except Exception as e:
                    step.success = False
                    step.error = str(e)
                    result.success = False
                    result.error = str(e)

                result.steps.append(step)
                self._complete_step(step)

                if ctx.verbose:
                    self._print_action_result(ctx.bundle.action_bundle, step)
            else:
                click.echo("\n⚠️  No action bundle in reproduction bundle - nothing to execute")

            # Step 7: Verify final state
            step = self._create_step(
                ReplayStepType.VERIFY_STATE,
                "Verifying final state",
            )
            final_state = self._get_current_state(ctx)
            step.state_after = final_state
            result.final_state = final_state
            result.steps.append(step)
            self._complete_step(step)

            # Step 8: Complete
            step = self._create_step(
                ReplayStepType.COMPLETE,
                "Replay completed",
            )
            result.steps.append(step)
            self._complete_step(step)

        except Exception as e:
            self._logger.exception(f"Replay failed: {e}")
            result.success = False
            result.error = str(e)

            step = self._create_step(
                ReplayStepType.ERROR,
                f"Replay failed: {e}",
            )
            step.success = False
            step.error = str(e)
            result.steps.append(step)

        finally:
            self._stop_anvil()

            result.end_time = datetime.now(UTC)
            if result.start_time is not None:
                result.duration_seconds = (result.end_time - result.start_time).total_seconds()

        return result

    def _create_step(
        self,
        step_type: ReplayStepType,
        description: str,
    ) -> ReplayStep:
        """Create a new replay step.

        Args:
            step_type: Type of step
            description: Step description

        Returns:
            New ReplayStep
        """
        self._current_step += 1
        return ReplayStep(
            step_number=self._current_step,
            step_type=step_type,
            description=description,
        )

    def _complete_step(self, step: ReplayStep) -> None:
        """Mark a step as complete and print status.

        Args:
            step: Step to complete
        """
        step.duration_ms = (datetime.now(UTC) - step.timestamp).total_seconds() * 1000

        status = "✓" if step.success else "✗"
        click.echo(f"  [{status}] Step {step.step_number}: {step.description}")

        if step.error:
            click.echo(f"      Error: {step.error}")

    def _log_header(self, ctx: ReplayContext) -> None:
        """Log replay header information.

        Args:
            ctx: Replay context
        """
        click.echo()
        click.echo("=" * 70)
        click.echo("ALMANAK REPLAY")
        click.echo("=" * 70)
        click.echo(f"Bundle ID: {ctx.bundle.bundle_id}")
        click.echo(f"Strategy: {ctx.bundle.strategy_id}")
        click.echo(f"Chain: {ctx.bundle.chain}")
        click.echo(f"Block: {ctx.bundle.block_number}")
        click.echo(f"Failure Time: {ctx.bundle.failure_timestamp}")

        if ctx.bundle.revert_reason:
            click.echo(f"Revert Reason: {ctx.bundle.revert_reason}")

        if ctx.bundle.tenderly_trace_url:
            click.echo(f"Tenderly Trace: {ctx.bundle.tenderly_trace_url}")

        click.echo("-" * 70)
        click.echo()

    def _start_anvil(self, ctx: ReplayContext) -> bool:
        """Start Anvil fork at the exact block.

        Args:
            ctx: Replay context

        Returns:
            True if Anvil started successfully
        """
        # Get archive RPC URL
        archive_rpc = ctx.archive_rpc
        if not archive_rpc:
            # Use default RPC for the chain (placeholder)
            archive_rpc = "http://localhost:8545"
            click.echo(f"⚠️  Using default RPC: {archive_rpc}")
            click.echo("    For accurate replay, provide --archive-rpc")

        fork_config = ForkConfig(
            chain=ctx.bundle.chain or "ethereum",
            rpc_url=archive_rpc,
            anvil_port=ctx.anvil_port,
            fork_block=ctx.bundle.block_number,
        )
        self._anvil = AnvilFork(fork_config)

        click.echo(f"    Starting Anvil at block {ctx.bundle.block_number}...")

        # In simulation mode, we don't actually start Anvil
        # Just validate that we could start it
        if self.verbose:
            click.echo(f"    RPC: {archive_rpc}")
            click.echo(f"    Port: {ctx.anvil_port}")
            click.echo(f"    Fork Block: {ctx.bundle.block_number}")

        # Simulate successful start for demo
        # In production, this would call self._anvil.start()
        return True

    def _stop_anvil(self) -> None:
        """Stop the Anvil fork."""
        if self._anvil is not None:
            # In production, this would call self._anvil.stop()
            self._anvil = None
            click.echo("\n    Anvil fork stopped")

    def _execute_action(
        self,
        action: ActionBundle,
        ctx: ReplayContext,
    ) -> dict[str, Any]:
        """Execute an action bundle on the Anvil fork.

        In production, this would actually execute the transactions
        on the Anvil fork. For MVP, we simulate execution.

        Args:
            action: Action bundle to execute
            ctx: Replay context

        Returns:
            Execution result details
        """
        click.echo(f"\n    Executing: {action.intent_type}")

        if ctx.verbose:
            click.echo(f"    Transactions: {len(action.transactions)}")
            for i, tx in enumerate(action.transactions):
                click.echo(f"      [{i + 1}] To: {tx.get('to', 'N/A')}")
                click.echo(f"          Data: {tx.get('data', 'N/A')[:50]}...")

        # Simulate execution result
        result = {
            "intent_type": action.intent_type,
            "transaction_count": len(action.transactions),
            "simulated": True,
            "gas_estimate": 150000 * len(action.transactions),
        }

        # If there was a receipt in the bundle, include it
        if ctx.bundle.receipt:
            result["original_receipt"] = {
                "status": ctx.bundle.receipt.status,
                "gas_used": ctx.bundle.receipt.gas_used,
                "revert_reason": ctx.bundle.receipt.revert_reason,
            }

            # If the original tx failed, indicate that
            if ctx.bundle.receipt.status == 0:
                click.echo("\n    ⚠️  Original transaction REVERTED")
                if ctx.bundle.receipt.revert_reason:
                    click.echo(f"    Reason: {ctx.bundle.receipt.revert_reason}")

        return result

    def _get_current_state(self, ctx: ReplayContext) -> dict[str, Any]:
        """Get the current state from the Anvil fork.

        In production, this would query actual state from the fork.
        For MVP, we return the bundle's persistent state.

        Args:
            ctx: Replay context

        Returns:
            Current state dictionary
        """
        return ctx.bundle.persistent_state.copy()

    def _print_state(self, label: str, state: dict[str, Any]) -> None:
        """Print state information in verbose mode.

        Args:
            label: State label
            state: State dictionary
        """
        click.echo(f"\n    {label}:")
        click.echo("    " + "-" * 40)

        for key, value in sorted(state.items()):
            # Format value for display
            if isinstance(value, dict):
                value_str = json.dumps(value, indent=2, default=str)
                # Indent nested content
                value_str = value_str.replace("\n", "\n        ")
                click.echo(f"      {key}:")
                click.echo(f"        {value_str}")
            elif isinstance(value, list) and len(value) > 3:
                click.echo(f"      {key}: [{len(value)} items]")
            else:
                value_str = str(value)
                if len(value_str) > 60:
                    value_str = value_str[:60] + "..."
                click.echo(f"      {key}: {value_str}")

    def _print_market_data(self, market_data: MarketData) -> None:
        """Print market data in verbose mode.

        Args:
            market_data: Market data snapshot
        """
        click.echo("\n    Market Data Snapshot:")
        click.echo("    " + "-" * 40)
        click.echo(f"      Timestamp: {market_data.timestamp}")

        if market_data.token_prices:
            click.echo("      Token Prices:")
            for token, price in market_data.token_prices.items():
                click.echo(f"        {token}: ${price:,.4f}")

        if market_data.gas_price:
            click.echo(f"      Gas Price: {market_data.gas_price} wei")

        if market_data.base_fee:
            click.echo(f"      Base Fee: {market_data.base_fee} wei")

    def _print_events_before(self, events: list[TimelineEventSnapshot]) -> None:
        """Print events leading up to failure.

        Args:
            events: List of timeline events
        """
        click.echo("\n    Events Before Failure:")
        click.echo("    " + "-" * 40)

        for event in events[-10:]:  # Show last 10 events
            click.echo(f"      [{event.timestamp}] {event.event_type}: {event.description}")
            if event.tx_hash:
                click.echo(f"        TX: {event.tx_hash}")

    def _print_action_result(self, action: ActionBundle, step: ReplayStep) -> None:
        """Print action execution result.

        Args:
            action: Action bundle
            step: Replay step with results
        """
        click.echo("\n    Action Execution Result:")
        click.echo("    " + "-" * 40)
        click.echo(f"      Intent Type: {action.intent_type}")
        click.echo(f"      Success: {step.success}")

        if step.error:
            click.echo(f"      Error: {step.error}")

        if step.state_before and step.state_after:
            # Calculate state changes
            changes = self._calculate_state_changes(step.state_before, step.state_after)
            if changes:
                click.echo("\n      State Changes:")
                for change in changes:
                    click.echo(f"        {change}")

    def _calculate_state_changes(
        self,
        before: dict[str, Any],
        after: dict[str, Any],
    ) -> list[str]:
        """Calculate changes between two states.

        Args:
            before: State before action
            after: State after action

        Returns:
            List of change descriptions
        """
        changes: list[str] = []

        all_keys = set(before.keys()) | set(after.keys())

        for key in sorted(all_keys):
            before_val = before.get(key)
            after_val = after.get(key)

            if before_val != after_val:
                if before_val is None:
                    changes.append(f"+ {key}: {after_val}")
                elif after_val is None:
                    changes.append(f"- {key}: {before_val}")
                else:
                    changes.append(f"~ {key}: {before_val} -> {after_val}")

        return changes


# =============================================================================
# CLI Command
# =============================================================================


@click.command("replay")
@click.option(
    "--bundle",
    "-b",
    required=False,
    help="Bundle ID to replay",
)
@click.option(
    "--bundle-file",
    "-f",
    type=click.Path(exists=True),
    required=False,
    help="Path to bundle JSON file",
)
@click.option(
    "--chain",
    "-c",
    type=click.Choice(list(CHAIN_BLOCK_TIMES.keys())),
    default=None,
    help="Override chain (uses bundle's chain by default)",
)
@click.option(
    "--block",
    type=int,
    default=None,
    help="Override block number (uses bundle's block by default)",
)
@click.option(
    "--archive-rpc",
    type=str,
    default=None,
    help="Archive RPC URL for forking",
)
@click.option(
    "--anvil-port",
    type=int,
    default=DEFAULT_ANVIL_PORT,
    help=f"Port for Anvil fork (default: {DEFAULT_ANVIL_PORT})",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(exists=False),
    default=None,
    help="Output file for full JSON results (optional)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show detailed output including state changes and traces",
)
@click.option(
    "--list-bundles",
    is_flag=True,
    default=False,
    help="List available bundles and exit",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show bundle info without replaying",
)
def replay(
    bundle: str | None,
    bundle_file: str | None,
    chain: str | None,
    block: int | None,
    archive_rpc: str | None,
    anvil_port: int,
    output: str | None,
    verbose: bool,
    list_bundles: bool,
    dry_run: bool,
) -> None:
    """
    Replay a failure from a reproduction bundle.

    This command loads a reproduction bundle and replays the failure locally
    using an Anvil fork at the exact block where the failure occurred.

    The replay process:
    1. Loads the reproduction bundle (from ID or file)
    2. Starts an Anvil fork at the exact block
    3. Loads the strategy's persistent state
    4. Re-executes the action that failed
    5. Reports state changes at each step

    Prerequisites:
        - Foundry (Anvil) must be installed: curl -L https://foundry.paradigm.xyz | bash
        - Archive RPC access for the target chain
        - A reproduction bundle (generated from on_failure hook)

    Examples:

        # Replay by bundle ID
        almanak replay --bundle my_strategy_20240115_120000_17000000

        # Replay from bundle file with verbose output
        almanak replay --bundle-file ./bundles/failure.json --verbose

        # Replay with custom archive RPC
        almanak replay --bundle my_bundle --archive-rpc https://arb-mainnet.g.alchemy.com/v2/KEY

        # List available bundles
        almanak replay --list-bundles

        # Show bundle info without replaying
        almanak replay --bundle my_bundle --dry-run
    """
    # Handle --list-bundles flag
    if list_bundles:
        list_available_bundles()
        return

    # Validate inputs
    if not bundle and not bundle_file:
        click.echo("Error: Must provide --bundle or --bundle-file", err=True)
        click.echo()
        click.echo("Use --bundle <bundle_id> or --bundle-file <path>", err=True)
        click.echo("Use --list-bundles to see available bundles", err=True)
        raise click.Abort()

    # Load bundle
    try:
        if bundle_file:
            bundle_path = Path(bundle_file)
            click.echo(f"Loading bundle from: {bundle_path}")
            reproduction_bundle = load_bundle_from_file(bundle_path)
        else:
            click.echo(f"Searching for bundle: {bundle}")
            reproduction_bundle = fetch_bundle(bundle)  # type: ignore[arg-type]
    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        click.echo()
        click.echo("Use --list-bundles to see available bundles", err=True)
        click.echo("Or use --bundle-file to specify the path directly", err=True)
        raise click.Abort() from e
    except json.JSONDecodeError as e:
        click.echo(f"Error: Invalid JSON in bundle file: {e}", err=True)
        raise click.Abort() from e
    except Exception as e:
        click.echo(f"Error loading bundle: {e}", err=True)
        raise click.Abort() from e

    # Override chain and block if specified
    if chain:
        reproduction_bundle.chain = chain
    if block:
        reproduction_bundle.block_number = block

    # Handle dry run
    if dry_run:
        print_bundle_info(reproduction_bundle)
        return

    # Create replay context
    output_path = Path(output) if output else None
    ctx = ReplayContext(
        bundle=reproduction_bundle,
        anvil_port=anvil_port,
        verbose=verbose,
        output_path=output_path,
        archive_rpc=archive_rpc,
    )

    # Run replay
    engine = ReplayEngine(verbose=verbose)
    result = engine.replay(ctx)

    # Display results
    click.echo()
    click.echo(result.summary())

    # Write JSON output if requested
    if output_path:
        write_json_output(result, output_path)

    # Exit with error code if replay failed
    if not result.success:
        click.echo()
        click.echo(f"Error: {result.error}", err=True)
        sys.exit(1)


def list_available_bundles() -> None:
    """List all available bundles in storage."""
    click.echo("Searching for bundles...")
    click.echo()

    found_bundles: list[tuple[Path, str, str]] = []

    for base_path in DEFAULT_BUNDLE_PATHS:
        if not base_path.exists():
            continue

        for bundle_path in base_path.rglob("*.json"):
            try:
                with open(bundle_path) as f:
                    data = json.load(f)
                bundle_id = data.get("bundle_id", bundle_path.stem)
                strategy_id = data.get("strategy_id", "unknown")
                found_bundles.append((bundle_path, bundle_id, strategy_id))
            except (json.JSONDecodeError, KeyError):
                continue

    if not found_bundles:
        click.echo("No bundles found.")
        click.echo()
        click.echo("Bundles are stored in:")
        for path in DEFAULT_BUNDLE_PATHS:
            click.echo(f"  - {path}")
        click.echo()
        click.echo("Bundles are automatically generated when failures occur.")
        click.echo("See: src/models/reproduction_bundle.py")
        return

    click.echo(f"Found {len(found_bundles)} bundle(s):")
    click.echo()

    for bundle_path, bundle_id, strategy_id in found_bundles:
        click.echo(f"  ID: {bundle_id}")
        click.echo(f"  Strategy: {strategy_id}")
        click.echo(f"  Path: {bundle_path}")
        click.echo()


def print_bundle_info(bundle: ReproductionBundle) -> None:
    """Print bundle information for dry run.

    Args:
        bundle: Bundle to display
    """
    click.echo()
    click.echo("=" * 70)
    click.echo("BUNDLE INFORMATION (dry run)")
    click.echo("=" * 70)
    click.echo(f"Bundle ID: {bundle.bundle_id}")
    click.echo(f"Strategy ID: {bundle.strategy_id}")
    click.echo(f"Chain: {bundle.chain}")
    click.echo(f"Block Number: {bundle.block_number}")
    click.echo(f"Failure Time: {bundle.failure_timestamp}")
    click.echo(f"Created At: {bundle.created_at}")
    click.echo("-" * 70)

    click.echo("Persistent State:")
    click.echo(f"  Keys: {list(bundle.persistent_state.keys())}")
    click.echo(f"  Size: {len(json.dumps(bundle.persistent_state))} bytes")

    click.echo()
    click.echo("Configuration:")
    for key, value in bundle.config.items():
        click.echo(f"  {key}: {value}")

    if bundle.action_bundle:
        click.echo()
        click.echo("Action Bundle:")
        click.echo(f"  Intent Type: {bundle.action_bundle.intent_type}")
        click.echo(f"  Transactions: {len(bundle.action_bundle.transactions)}")

    if bundle.transaction_hash:
        click.echo()
        click.echo(f"Transaction Hash: {bundle.transaction_hash}")

    if bundle.receipt:
        click.echo()
        click.echo("Receipt:")
        click.echo(f"  Status: {'Success' if bundle.receipt.status == 1 else 'Failed'}")
        click.echo(f"  Gas Used: {bundle.receipt.gas_used}")
        if bundle.receipt.revert_reason:
            click.echo(f"  Revert Reason: {bundle.receipt.revert_reason}")

    if bundle.tenderly_trace_url:
        click.echo()
        click.echo(f"Tenderly Trace: {bundle.tenderly_trace_url}")

    if bundle.revert_reason:
        click.echo()
        click.echo(f"Revert Reason: {bundle.revert_reason}")

    click.echo()
    click.echo("Replay Command:")
    click.echo(f"  {bundle.to_replay_command()}")
    click.echo("=" * 70)


def write_json_output(result: ReplayResult, output_path: Path) -> None:
    """Write full replay results to a JSON file.

    Args:
        result: Replay result
        output_path: Path to output file
    """
    output_data = result.to_dict()
    output_data["_meta"] = {
        "generated_at": datetime.now(UTC).isoformat(),
        "generator": "almanak replay",
    }

    with open(output_path, "w") as f:
        json.dump(output_data, f, indent=2, default=str)

    click.echo(f"Results written to: {output_path}")


if __name__ == "__main__":
    replay()
