"""``almanak strat run`` -- run-mode orchestration (once, test-lifecycle, continuous).

Split from run_helpers.py; import via the run_helpers facade externally.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any, NoReturn

import click

from almanak.config.cli_runtime import almanak_chain_from_env, anvil_port_for_chain

from ._run_components import _build_components, _build_runtime_config
from ._run_context import ComponentBundle, RuntimeBootstrap, StrategyBootstrap
from ._run_dashboard import _handle_standalone_dashboard
from ._run_gateway import _build_cleanup_fn
from ._run_setup import (
    _detect_state_resume,
    _discover_and_load_config,
    _DryRunVaultEarlyExit,
    _handle_list_all,
    _load_strategy_class,
    _require_strategy_deployment_id,
    _resolve_identity,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(
    "almanak.framework.cli.run_helpers"
)  # pinned: tests + operator filters key on the historical module path


# ---------------------------------------------------------------------------
# Strategy-run orchestration
# ---------------------------------------------------------------------------


def _maybe_handle_run_early_exit(
    *,
    list_all: bool,
    gateway_client: Any,
    working_dir: str,
    dashboard: bool,
    dashboard_port: int,
    gateway_host: str,
    gateway_port: int,
    auth_token: str | None,
    dashboard_mode: str = "command-center",
) -> bool:
    """Handle early-return `run()` branches before strategy bootstrap."""
    if _handle_list_all(list_all, gateway_client):
        return True

    return _handle_standalone_dashboard(
        working_dir=working_dir,
        dashboard=dashboard,
        dashboard_port=dashboard_port,
        gateway_host=gateway_host,
        gateway_port=gateway_port,
        auth_token=auth_token,
        dashboard_mode=dashboard_mode,
    )


def _maybe_start_dashboard_process(
    *,
    dashboard: bool,
    dashboard_port: int,
    gateway_host: str,
    gateway_port: int,
    auth_token: str | None,
    mode: str = "command-center",
    deployment_id: str | None = None,
    strategy_working_dir: str | None = None,
    strategy_config: dict[str, Any] | None = None,
) -> Any:
    """Start the dashboard sidecar when requested, registering cleanup.

    See ``_start_dashboard_background`` for the meaning of ``mode``,
    ``deployment_id``, ``strategy_working_dir``, and ``strategy_config``
    — they're forwarded verbatim and only consulted when
    ``mode == "hosted-parity"``.
    """
    import atexit

    if not dashboard:
        return None

    from almanak.framework.cli import run_helpers as _rh  # local import: avoids module-level cycle

    dashboard_process = _rh._start_dashboard_background(  # via facade: tests monkeypatch this attribute on run_helpers
        port=dashboard_port,
        gateway_host=gateway_host,
        gateway_port=gateway_port,
        auth_token=auth_token,
        mode=mode,
        deployment_id=deployment_id,
        strategy_working_dir=strategy_working_dir,
        strategy_config=strategy_config,
    )
    if dashboard_process is not None:
        atexit.register(
            _rh._stop_dashboard, dashboard_process
        )  # via facade: tests monkeypatch this attribute on run_helpers
    return dashboard_process


def _load_strategy_bootstrap(
    *,
    working_dir: str,
    config_file: str | None,
    copy_mode: str | None,
    copy_shadow: bool,
    copy_replay_file: str | None,
    copy_strict: bool,
    dry_run: bool,
    early_strategy_class: Any,
) -> StrategyBootstrap:
    """Load strategy class, config, and resolved chain metadata for `run()`."""
    from .run import get_strategy_chains, get_strategy_protocols

    strategy_class = _load_strategy_class(working_dir, early_strategy_class)
    strategy_name = strategy_class.__name__
    click.echo(f"Loaded strategy: {strategy_name}")

    strategy_chains = get_strategy_chains(strategy_class)
    strategy_protocols = get_strategy_protocols(strategy_class)

    (
        strategy_config,
        multi_chain,
        effective_dry_run,
        resolved_config_file,
        normalized_copy_mode,
    ) = _discover_and_load_config(
        working_dir=working_dir,
        config_file=config_file,
        strategy_class=strategy_class,
        copy_mode=copy_mode,
        copy_shadow=copy_shadow,
        copy_replay_file=copy_replay_file,
        copy_strict=copy_strict,
        dry_run=dry_run,
    )
    strategy_chains = _refine_strategy_chains(
        strategy_chains=strategy_chains,
        strategy_config=strategy_config,
        multi_chain=multi_chain,
    )
    config_display_name = _normalize_strategy_display_name(raw_name=strategy_config.get("deployment_id", strategy_name))
    strategy_config["strategy_display_name"] = config_display_name

    return StrategyBootstrap(
        strategy_class=strategy_class,
        strategy_name=strategy_name,
        strategy_config=strategy_config,
        multi_chain=multi_chain,
        config_file=resolved_config_file,
        normalized_copy_mode=normalized_copy_mode,
        strategy_chains=strategy_chains,
        strategy_protocols=strategy_protocols,
        config_display_name=config_display_name,
        effective_dry_run=effective_dry_run,
    )


def _refine_strategy_chains(
    *,
    strategy_chains: list[str],
    strategy_config: dict[str, Any],
    multi_chain: bool,
) -> list[str]:
    """Use config-specified chains when a multi-chain strategy provides them."""
    if not multi_chain:
        return strategy_chains

    config_chains = strategy_config.get("chains", [])
    if isinstance(config_chains, list) and len(config_chains) > 1:
        return config_chains
    return strategy_chains


def _normalize_strategy_display_name(*, raw_name: Any) -> str:
    """Strip any persisted deployment-id suffix from the display name."""
    name = "" if raw_name is None else str(raw_name)
    return name.split(":", 1)[0]


def _maybe_echo_chain_override(
    *,
    env_chain: str | None,
    config_chain: str | None,
    config_chain_norm: str,
    config_chain_raw: Any,
) -> None:
    """Emit the env-over-config chain banner only when the env actually won."""
    if env_chain != config_chain:
        return
    if env_chain == config_chain_norm:
        return
    click.echo(f"Chain override: ALMANAK_CHAIN={env_chain} (config.json: {config_chain_raw or 'unset'})")


def _resolve_config_chain_with_echo(
    *,
    strategy_class: Any,
    strategy_config: dict[str, Any],
    multi_chain: bool,
) -> str | None:
    """Resolve the effective chain context and preserve override echoing."""
    from .run import resolve_strategy_chain

    env_chain = almanak_chain_from_env()
    config_chain = resolve_strategy_chain(
        strategy_class,
        strategy_config,
        env_chain=env_chain,
        multi_chain=multi_chain,
    )
    config_chain_raw = strategy_config.get("chain")
    config_chain_norm = config_chain_raw.strip().lower() if isinstance(config_chain_raw, str) else ""
    if env_chain:
        _maybe_echo_chain_override(
            env_chain=env_chain,
            config_chain=config_chain,
            config_chain_norm=config_chain_norm,
            config_chain_raw=config_chain_raw,
        )

    return config_chain


def _echo_anvil_network_banner(*, config_chain: str | None) -> None:
    """Echo the local fork endpoint when `run()` targets Anvil."""
    anvil_port = anvil_port_for_chain(config_chain or "arbitrum") or 8545
    click.echo(f"Network: ANVIL (local fork at http://127.0.0.1:{anvil_port})")


def _resolve_network_with_echo(*, network: str | None, config_chain: str | None) -> str:
    """Resolve the effective network and preserve the Anvil banner."""
    resolved_network = "mainnet"
    if network:
        resolved_network = network
    if resolved_network == "anvil":
        _echo_anvil_network_banner(config_chain=config_chain)
    return resolved_network


def _prepare_runtime_bootstrap(
    *,
    strategy_bootstrap: StrategyBootstrap,
    no_gateway: bool,
    network: str | None,
    gateway_client: Any,
    gateway_network: str,
    fresh: bool,
) -> RuntimeBootstrap:
    """Resolve runtime config and stable identity for `run()`."""
    config_chain = _resolve_config_chain_with_echo(
        strategy_class=strategy_bootstrap.strategy_class,
        strategy_config=strategy_bootstrap.strategy_config,
        multi_chain=strategy_bootstrap.multi_chain,
    )
    resolved_network = _resolve_network_with_echo(
        network=network,
        config_chain=config_chain,
    )
    runtime_config, chain_wallets = _build_runtime_config(
        no_gateway=no_gateway,
        multi_chain=strategy_bootstrap.multi_chain,
        resolved_network=resolved_network,
        config_chain=config_chain,
        strategy_chains=strategy_bootstrap.strategy_chains,
        strategy_protocols=strategy_bootstrap.strategy_protocols,
        gateway_client=gateway_client,
        strategy_config=strategy_bootstrap.strategy_config,
    )
    identity_info = _resolve_identity(
        strategy_config=strategy_bootstrap.strategy_config,
        fresh=fresh,
        multi_chain=strategy_bootstrap.multi_chain,
        strategy_chains=strategy_bootstrap.strategy_chains,
        config_display_name=strategy_bootstrap.config_display_name,
        gateway_network=gateway_network,
    )
    return RuntimeBootstrap(
        config_chain=config_chain,
        resolved_network=resolved_network,
        runtime_config=runtime_config,
        chain_wallets=chain_wallets,
        deployment_id=strategy_bootstrap.strategy_config["deployment_id"],
        run_id=identity_info.run_id,
    )


def _load_resume_state(
    *,
    deployment_id: str,
) -> tuple[bool, dict[str, Any] | None]:
    """Load local SQLite resume metadata when the deployment mode is local."""
    from almanak.framework.deployment import is_local
    from almanak.framework.local_paths import local_db_path as _local_db_path

    if not is_local():
        return False, None

    state_db_path = _local_db_path()
    resume_info = _detect_state_resume(state_db_path, deployment_id)
    if not resume_info.is_resume:
        return False, None

    return True, {"version": resume_info.version, "keys": resume_info.state_keys}


def _echo_strategy_runtime_summary(
    *,
    strategy_class: Any,
    multi_chain: bool,
    strategy_chains: list[str],
) -> None:
    """Emit the final strategy-class summary banner."""
    click.echo(f"Strategy class loaded: {strategy_class.__name__}")
    if multi_chain:
        click.echo(f"  Multi-chain: Yes ({len(strategy_chains)} chains)")


def _cleanup_after_dry_run_vault_exit(
    *,
    gateway_client: Any,
    managed_gateway: Any,
    keep_anvil: bool,
    components: Any,
    dashboard_process: Any,
) -> NoReturn:
    """Unwind resources for the intentional dry-run vault early-exit path."""
    early_cleanup = _build_cleanup_fn(
        gateway_client=gateway_client,
        managed_gateway=managed_gateway,
        keep_anvil=keep_anvil,
        components=components,
    )
    try:
        asyncio.run(early_cleanup())
    except Exception:  # pragma: no cover - cleanup best-effort
        logger.exception("Cleanup failed during dry-run vault early exit")
    from almanak.framework.cli import run_helpers as _rh  # local import: avoids module-level cycle

    _rh._stop_dashboard(dashboard_process)  # via facade: tests monkeypatch this attribute on run_helpers
    sys.exit(0)


def _build_components_or_exit(
    *,
    strategy_instance: Any,
    strategy_config: dict[str, Any],
    runtime_config: Any,
    strategy_chains: list[str],
    multi_chain: bool,
    resolved_network: str,
    gateway_client: Any,
    chain_wallets: Any,
    interval: int,
    effective_dry_run: bool,
    deployment_id: str,
    normalized_copy_mode: str | None,
    copy_replay_file: str | None,
    copy_shadow: bool,
    copy_strict: bool,
    config_chain: str | None,
    managed_gateway: Any,
    keep_anvil: bool,
    dashboard_process: Any,
) -> Any:
    """Build run-time components, preserving the dry-run vault early-exit path."""
    try:
        return _build_components(
            strategy_instance=strategy_instance,
            strategy_config=strategy_config,
            runtime_config=runtime_config,
            strategy_chains=strategy_chains,
            multi_chain=multi_chain,
            resolved_network=resolved_network,
            gateway_client=gateway_client,
            chain_wallets=chain_wallets,
            interval=interval,
            effective_dry_run=effective_dry_run,
            deployment_id=deployment_id,
            normalized_copy_mode=normalized_copy_mode,
            copy_replay_file=copy_replay_file,
            copy_shadow=copy_shadow,
            copy_strict=copy_strict,
            config_chain=config_chain,
        )
    except _DryRunVaultEarlyExit as early:
        partial_components = early.components or ComponentBundle()
        _cleanup_after_dry_run_vault_exit(
            gateway_client=gateway_client,
            managed_gateway=managed_gateway,
            keep_anvil=keep_anvil,
            components=partial_components,
            dashboard_process=dashboard_process,
        )


def _execute_run_mode(
    *,
    test_actions: list[str] | None,
    once: bool,
    teardown_after: bool,
    test_json: bool,
    runner: Any,
    strategy_instance: Any,
    state_manager: Any,
    cleanup_fn: Any,
    interval: int,
    max_iterations: int | None,
    reset_fork: bool,
    managed_gateway: Any,
    test_inject: Any | None = None,
) -> int:
    """Dispatch to the lifecycle, once, or continuous execution lane."""
    if test_actions is not None:
        return _run_test_lifecycle(
            runner=runner,
            strategy_instance=strategy_instance,
            state_manager=state_manager,
            cleanup_fn=cleanup_fn,
            actions=test_actions,
            teardown=teardown_after,
            json_output=test_json,
            inject=test_inject,
        )

    if once:
        return _run_once(
            runner=runner,
            strategy_instance=strategy_instance,
            state_manager=state_manager,
            cleanup_fn=cleanup_fn,
            teardown_after=teardown_after,
        )

    return _run_continuous(
        runner=runner,
        strategy_instance=strategy_instance,
        cleanup_fn=cleanup_fn,
        interval=interval,
        max_iterations=max_iterations,
        reset_fork=reset_fork,
        managed_gateway=managed_gateway,
    )


# ---------------------------------------------------------------------------
# Single-iteration execution
# ---------------------------------------------------------------------------


def _run_once(  # noqa: C901
    *,
    runner: Any,
    strategy_instance: Any,
    state_manager: Any,
    cleanup_fn: Callable[[], Coroutine[Any, Any, None]],
    teardown_after: bool,
) -> int:
    """Execute a single strategy iteration (and optional teardown) and return exit code.

    Synchronous wrapper that mirrors the ``if once:`` block in ``run()``.
    Owns the outer ``asyncio.run(run_once_with_cleanup())`` call plus the
    exit-code resolution and the top-level error/except handling. Keeping
    this sync preserves the original ``asyncio.run`` boundary and lets
    ``KeyboardInterrupt`` semantics remain identical to the inlined code.

    Behavior-preserving:

        * Restores persisted strategy state and copy-trading cursor
          (inside the async wrapper).
        * Runs a single iteration, captures portfolio snapshot, emits summary.
        * If ``teardown_after`` is True, runs a second iteration after
          registering a TeardownRequest.
        * Persists copy-trading cursor and flushes pending saves.
        * Always runs gateway-integration teardown and ``cleanup_fn`` in
          ``finally``.

    Returns:
        Exit code: ``0`` on success, ``1`` on iteration or teardown failure
        (or unhandled exception).
    """
    import asyncio

    # Lazy-import so tests that monkeypatch these modules observe the fakes.
    from ..runner import IterationStatus

    # Runtime-local reference for format_iteration_result (lives in run.py to
    # avoid moving unrelated code; deferred import breaks the cycle).
    from .run import format_iteration_result

    click.echo()
    click.echo("Running single iteration...")
    click.echo()

    async def run_once_with_cleanup() -> tuple[Any, Any]:
        """Run single iteration, optional teardown, and cleanup resources."""
        # Guarded layout ensures cleanup_fn() always runs, even if
        # setup_gateway_integration or teardown_gateway_integration raise.
        # Mirrors the copy_replay_file safety fix (always-run cleanup).
        gateway_integration_ready = False
        try:
            runner.setup_gateway_integration(strategy_instance)
            gateway_integration_ready = True
            # Restore persisted strategy state (e.g. position_id after restart)
            if hasattr(strategy_instance, "load_state_async"):
                if await strategy_instance.load_state_async():
                    click.secho("  Strategy state restored from persistence", fg="yellow")
                else:
                    click.echo("  No previous state found (fresh start)")

            # Restore copy trading cursor state (mirrors run_loop pattern)
            activity_provider = getattr(strategy_instance, "_wallet_activity_provider", None)
            if activity_provider is not None:
                try:
                    ct_state = await state_manager.load_state(strategy_instance.deployment_id)
                    if ct_state is not None and "copy_trading_state" in ct_state.state:
                        activity_provider.set_state(ct_state.state["copy_trading_state"])
                except Exception as e:
                    logger.warning(f"Failed to restore copy trading state: {e}")

            # VIB-3944: rebuild lending FIFO lots from durable accounting_events.
            # The continuous run_loop entry point does this in initialize_run_loop
            # but --once / --teardown-after bypass that path. Without rebuild,
            # Earlier teardown flows could land a REPAY with no matching BORROW lot and the
            # writer cannot emit interest_delta_usd → L4 Accountant Test fails.
            # Run AFTER setup_gateway_integration so the gRPC channel is up.
            from ..runner._run_loop_helpers import (
                hydrate_recent_open_events_cache,
                reconstruct_lending_basis_store,
            )

            reconstruct_lending_basis_store(
                runner,
                strategy_instance,
                _require_strategy_deployment_id(
                    strategy_instance,
                    operation="reconstruct_lending_basis_store",
                ),
            )

            # VIB-4086 — same cross-process restart hole for the
            # position_events recent-open cache. Without hydration, the
            # ``--once --teardown-after`` can close a position
            # opened in a prior process with no in-memory bracket /
            # tokens to carry forward, landing the CLOSE row with empty
            # token0/token1/value_usd (the LP6 ship gate this PR closes).
            await hydrate_recent_open_events_cache(runner, strategy_instance)

            # VIB-3762: route --once snapshot persistence through the
            # mode-aware wrapper so accounting failures surface the same
            # way as continuous-mode failures (live -> ACCOUNTING_FAILED,
            # paper/dry-run -> ERROR log + continue). Direct calls to
            # ``_capture_portfolio_snapshot`` were the bypass that hid
            # April 29's silent accounting failures.
            import time as _time

            from ..runner._run_loop_helpers import capture_snapshot_with_accounting

            iteration_start_monotonic = _time.monotonic()
            result = await runner.run_iteration(strategy_instance)
            result = await capture_snapshot_with_accounting(
                runner=runner,
                strategy=strategy_instance,
                deployment_id=_require_strategy_deployment_id(
                    strategy_instance,
                    operation="capture_snapshot_with_accounting",
                ),
                result=result,
                iteration_start_monotonic=iteration_start_monotonic,
            )

            # Emit structured iteration summary for JSONL log analysis
            runner._emit_iteration_summary(result, chain=getattr(strategy_instance, "chain", None))

            # --- teardown-after: signal + second iteration ---
            teardown_result = None
            if teardown_after:
                click.echo()
                click.echo("Teardown requested -- closing positions...")

                from almanak.framework.teardown import get_teardown_state_manager
                from almanak.framework.teardown.models import TeardownMode, TeardownRequest

                deployment_id = _require_strategy_deployment_id(
                    strategy_instance,
                    operation="teardown_after",
                )
                manager = get_teardown_state_manager()
                manager.create_request(
                    TeardownRequest(
                        deployment_id=deployment_id,
                        mode=TeardownMode.SOFT,
                        reason="--teardown-after flag (CI cleanup)",
                        requested_by="cli",
                    )
                )

                teardown_result = await runner.run_iteration(strategy_instance)
                runner._emit_iteration_summary(teardown_result, chain=getattr(strategy_instance, "chain", None))
                click.echo(format_iteration_result(teardown_result))

            # Persist copy trading cursor state
            if activity_provider is not None:
                try:
                    ct_state = await state_manager.load_state(strategy_instance.deployment_id)
                    if ct_state is None:
                        from almanak.framework.state.state_manager import StateData

                        ct_state = StateData(
                            deployment_id=strategy_instance.deployment_id,
                            version=0,
                            state={},
                        )
                    ct_state.state["copy_trading_state"] = activity_provider.get_state()
                    await state_manager.save_state(ct_state, expected_version=ct_state.version)
                except Exception as e:
                    logger.warning(f"Failed to persist copy trading state: {e}")

            # Flush any pending state saves before cleanup
            # (run_loop does this automatically, but run_iteration doesn't)
            if hasattr(strategy_instance, "flush_pending_saves"):
                try:
                    await strategy_instance.flush_pending_saves()
                except Exception as e:
                    logger.warning(f"Error flushing pending saves: {e}")
            return result, teardown_result
        finally:
            # Nested try/finally guarantees cleanup_fn() runs even if
            # teardown_gateway_integration raises. The ready-guard avoids
            # calling teardown when setup itself failed (pairing invariant).
            try:
                if gateway_integration_ready:
                    runner.teardown_gateway_integration(strategy_instance.deployment_id)
            finally:
                await cleanup_fn()

    try:
        result, teardown_result = asyncio.run(run_once_with_cleanup())
        click.echo(format_iteration_result(result))

        # Determine exit code: main iteration + optional teardown
        if teardown_result is not None:
            # With --teardown-after: both iteration and teardown must succeed
            teardown_ok = teardown_result.status == IterationStatus.TEARDOWN
            if result.success and teardown_ok:
                click.echo()
                click.echo("Iteration and teardown completed successfully.")
                return 0
            click.echo()
            if not result.success:
                click.echo(f"Iteration failed: {result.error}")
            if not teardown_ok:
                click.echo(f"Teardown failed: {teardown_result.error or teardown_result.status.value}")
            return 1
        if result.success:
            click.echo()
            click.echo("Iteration completed successfully.")
            return 0
        click.echo()
        click.echo(f"Iteration failed: {result.error}")
        return 1

    except Exception as e:
        click.echo(f"Error running iteration: {e}", err=True)
        logger.exception("Iteration failed")
        return 1


# ---------------------------------------------------------------------------
# Test lifecycle — drive force_action sequence + optional teardown
# ---------------------------------------------------------------------------


def _run_test_lifecycle(  # noqa: C901
    *,
    runner: Any,
    strategy_instance: Any,
    state_manager: Any,
    cleanup_fn: Callable[[], Coroutine[Any, Any, None]],
    actions: list[str],
    teardown: bool,
    json_output: bool,
    inject: Any | None = None,
) -> int:
    """Execute a force-action lifecycle test.

    Drives each value in ``actions`` as a single ``--once`` iteration with
    ``strategy_instance.force_action`` mutated between iterations, optionally
    followed by a teardown iteration. State (position id, on-chain side
    effects, runner cycle counter) flows through naturally because all
    iterations share one strategy instance.

    When ``inject`` (a :class:`ScenarioOverrides`) is supplied, a post-build
    snapshot hook seeds the synthetic market conditions into every iteration's
    ``MarketSnapshot`` (VIB-5529), so condition-triggered ``decide()`` branches
    run instead of being force-action short-circuited.

    Stops on the first failed iteration (fail-fast). Always runs cleanup.

    Returns:
        Exit code: ``0`` if every iteration (and the teardown, if requested)
        passed, ``1`` otherwise.
    """
    import asyncio
    import logging as _logging

    from ..runner import IterationStatus
    from ..runner.runner_models import IterationResult

    class _BufferingHandler(_logging.Handler):
        """Captures WARN+ERROR log records into a ring buffer for later inspection.

        Used to attach framework-side diagnostics (REVERT DIAGNOSTIC blocks,
        non-retryable error notices, gas warnings, etc.) to failed steps in
        the JSON output. Successful steps don't carry this data — it would
        bloat the response.
        """

        def __init__(self, max_records: int = 200) -> None:
            super().__init__(level=_logging.WARNING)
            # Each record carries a monotonic id so per-step slicing stays correct
            # even after the ring buffer has dropped older entries (id != list index).
            self.records: list[tuple[int, str]] = []
            self.next_id = 0
            self.max_records = max_records

        def emit(self, record: _logging.LogRecord) -> None:
            try:
                msg = self.format(record)
            except Exception:
                msg = record.getMessage()
            if len(self.records) >= self.max_records:
                self.records.pop(0)
            self.records.append((self.next_id, msg))
            self.next_id += 1

        def slice_since(self, cursor: int) -> list[str]:
            """Return all records emitted since the given cursor (monotonic id)."""
            return [msg for idx, msg in self.records if idx >= cursor]

    log_buffer = _BufferingHandler(max_records=200)
    log_buffer.setFormatter(_logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    _logging.getLogger().addHandler(log_buffer)

    # Owned at the outer scope so the exception handler below can salvage
    # whatever steps completed before run_iteration() raised.
    action_results: list[dict] = []
    teardown_result_dict: dict | None = None

    async def run_lifecycle_with_cleanup() -> tuple[list[dict], dict | None]:  # noqa: C901
        nonlocal action_results, teardown_result_dict
        gateway_integration_ready = False
        try:
            runner.setup_gateway_integration(strategy_instance)
            gateway_integration_ready = True

            # VIB-5529: register the synthetic market-condition injector so each
            # iteration's MarketSnapshot is seeded before decide() runs. Done
            # after gateway integration so the snapshot the runner builds carries
            # live providers; the injected overrides win because the snapshot's
            # read caches are consulted before any provider call.
            if inject is not None:
                from ._scenario import apply_scenario

                def _override_hook(market: Any) -> None:
                    applied = apply_scenario(market, inject)
                    if applied and not json_output:
                        click.echo(f"  injected: {', '.join(applied)}")

                runner._snapshot_override_hook = _override_hook

            # Mirror _run_once state hooks: restore persisted strategy state and
            # copy-trading cursor before any iteration, so test runs see the same
            # startup conditions production does. load_state_async failures must
            # propagate (fatal in _run_once); copy-trading restore is best-effort
            # (also matches _run_once).
            if hasattr(strategy_instance, "load_state_async"):
                await strategy_instance.load_state_async()
            activity_provider = getattr(strategy_instance, "_wallet_activity_provider", None)
            if activity_provider is not None:
                try:
                    ct_state = await state_manager.load_state(strategy_instance.deployment_id)
                    if ct_state is not None and "copy_trading_state" in ct_state.state:
                        activity_provider.set_state(ct_state.state["copy_trading_state"])
                except Exception as e:
                    logger.warning(f"Failed to restore copy trading state: {e}")

            # VIB-3944: same cross-process FIFO rebuild as _run_once. The
            # test-lifecycle path drives multiple force_action iterations + an
            # optional teardown in a single CLI invocation, but the in-memory
            # FIFO store is empty if a previous CLI process opened the borrow.
            from ..runner._run_loop_helpers import (
                hydrate_recent_open_events_cache,
                reconstruct_lending_basis_store,
            )

            reconstruct_lending_basis_store(
                runner,
                strategy_instance,
                _require_strategy_deployment_id(
                    strategy_instance,
                    operation="reconstruct_lending_basis_store",
                ),
            )

            # VIB-4086 — symmetric cache hydration for the test-lifecycle
            # path. See `_run_once` above for the full rationale.
            await hydrate_recent_open_events_cache(runner, strategy_instance)

            # Single predicate: an action passes iff status is SUCCESS or HOLD.
            # Used identically by per-step failure_logs, fail-fast, and the final
            # summary so the three never disagree.
            deployment_id = _require_strategy_deployment_id(
                strategy_instance,
                operation="strat_test_lifecycle",
            )
            action_pass_statuses = (IterationStatus.SUCCESS.value, IterationStatus.HOLD.value)
            for action in actions:
                strategy_instance.force_action = action
                if not json_output:
                    click.echo(f"\n→ force_action={action!r}")
                logs_before = log_buffer.next_id
                try:
                    iteration_start_monotonic = time.monotonic()
                    result = await runner.run_iteration(strategy_instance)
                    # Capture portfolio snapshot per iteration through the
                    # canonical helper so the live-mode ``ACCOUNTING_FAILED``
                    # escalation contract (VIB-3762) is honoured. Direct
                    # calls to ``_capture_portfolio_snapshot`` here were the
                    # April 29 silent-failure shape: a live ledger-write
                    # exception in the snapshot path was swallowed by the
                    # surrounding ``except Exception`` and the loop carried
                    # on with a half-persisted iteration.
                    from almanak.framework.runner._run_loop_helpers import (
                        capture_snapshot_with_accounting,
                    )

                    result = await capture_snapshot_with_accounting(
                        runner=runner,
                        strategy=strategy_instance,
                        deployment_id=deployment_id,
                        result=result,
                        iteration_start_monotonic=iteration_start_monotonic,
                    )
                    runner._emit_iteration_summary(result, chain=getattr(strategy_instance, "chain", None))
                except Exception as exc:
                    # Record the raise as a synthetic failed step and break — but DO NOT
                    # propagate; the teardown block below must still run so positions
                    # opened by prior successful actions get unwound. Use IterationResult
                    # so the step shape matches normal steps from result.to_dict().
                    logger.exception("run_iteration raised for action %r", action)
                    synthetic = IterationResult(
                        status=IterationStatus.STRATEGY_ERROR,
                        error=f"run_iteration raised: {exc!r}",
                        deployment_id=deployment_id,
                    )
                    action_results.append(
                        {
                            "action": action,
                            **synthetic.to_dict(),
                            "failure_logs": log_buffer.slice_since(logs_before),
                        }
                    )
                    if not json_output:
                        click.echo(f"  raised: {exc!r}", err=True)
                    break
                entry = {"action": action, **result.to_dict()}
                action_passed = result.status.value in action_pass_statuses
                if not action_passed:
                    entry["failure_logs"] = log_buffer.slice_since(logs_before)
                action_results.append(entry)
                if not action_passed:
                    if not json_output:
                        click.echo(f"  failed: {result.error or result.status.value}", err=True)
                    break  # fail-fast

            # Always run teardown when requested — even if an earlier action
            # failed, we still want to clean up any positions opened by prior
            # successful actions in this run. Teardown is a no-op for
            # strategies whose generate_teardown_intents returns [] when no
            # position is open.
            if teardown:
                from almanak.framework.teardown import get_teardown_state_manager
                from almanak.framework.teardown.models import TeardownMode, TeardownRequest

                strategy_instance.force_action = ""
                deployment_id = _require_strategy_deployment_id(
                    strategy_instance,
                    operation="strat_test_teardown",
                )
                if not json_output:
                    click.echo("\n→ teardown")
                # Capture log cursor BEFORE create_request so a state-manager
                # failure here (locked DB / schema mismatch) is also surfaced as
                # a synthetic teardown step instead of escaping to the outer handler.
                logs_before = log_buffer.next_id
                try:
                    get_teardown_state_manager().create_request(
                        TeardownRequest(
                            deployment_id=deployment_id,
                            mode=TeardownMode.SOFT,
                            reason="strat test --teardown",
                            requested_by="cli",
                        )
                    )
                    td_iteration_start = time.monotonic()
                    td_result = await runner.run_iteration(strategy_instance)
                    # Same accounting-snapshot wrapper as the force-action
                    # iteration above — without this, a live teardown's
                    # ledger-write failure during the post-iteration snapshot
                    # would be swallowed (the canonical April 29 silent-
                    # failure shape).
                    from almanak.framework.runner._run_loop_helpers import (
                        capture_snapshot_with_accounting,
                    )

                    td_result = await capture_snapshot_with_accounting(
                        runner=runner,
                        strategy=strategy_instance,
                        deployment_id=deployment_id,
                        result=td_result,
                        iteration_start_monotonic=td_iteration_start,
                    )
                    runner._emit_iteration_summary(td_result, chain=getattr(strategy_instance, "chain", None))
                except Exception as exc:
                    # Materialize a failed teardown step instead of letting the
                    # exception escape — symmetric with the action loop above so
                    # JSON consumers see the failure_logs and a teardown step entry.
                    # Use IterationResult so the step shape matches normal steps.
                    logger.exception("teardown raised (create_request or run_iteration)")
                    synthetic = IterationResult(
                        status=IterationStatus.STRATEGY_ERROR,
                        error=f"run_iteration raised: {exc!r}",
                        deployment_id=deployment_id,
                    )
                    teardown_result_dict = {
                        "action": "teardown",
                        **synthetic.to_dict(),
                        "failure_logs": log_buffer.slice_since(logs_before),
                    }
                    if not json_output:
                        click.echo(f"  teardown raised: {exc!r}", err=True)
                else:
                    teardown_result_dict = {"action": "teardown", **td_result.to_dict()}
                    teardown_passed = td_result.status.value == IterationStatus.TEARDOWN.value
                    if not teardown_passed:
                        teardown_result_dict["failure_logs"] = log_buffer.slice_since(logs_before)
                        if not json_output:
                            click.echo(
                                f"  teardown failed: {td_result.error or td_result.status.value}",
                                err=True,
                            )

            # Persist copy trading cursor state (mirrors _run_once).
            if activity_provider is not None:
                try:
                    ct_state = await state_manager.load_state(strategy_instance.deployment_id)
                    if ct_state is None:
                        from almanak.framework.state.state_manager import StateData

                        ct_state = StateData(
                            deployment_id=strategy_instance.deployment_id,
                            version=0,
                            state={},
                        )
                    ct_state.state["copy_trading_state"] = activity_provider.get_state()
                    await state_manager.save_state(ct_state, expected_version=ct_state.version)
                except Exception as e:
                    logger.warning(f"Failed to persist copy trading state: {e}")

            if hasattr(strategy_instance, "flush_pending_saves"):
                try:
                    await strategy_instance.flush_pending_saves()
                except Exception as e:
                    logger.warning(f"Error flushing pending saves: {e}")

            return action_results, teardown_result_dict
        finally:
            try:
                if gateway_integration_ready:
                    runner.teardown_gateway_integration(strategy_instance.deployment_id)
            finally:
                await cleanup_fn()

    try:
        action_results, teardown_result_dict = asyncio.run(run_lifecycle_with_cleanup())
    except Exception as e:
        logger.exception("Test lifecycle failed")
        if json_output:
            partial_steps: list[dict] = list(action_results)
            if teardown_result_dict is not None:
                partial_steps.append(teardown_result_dict)
            # Reflect the real per-step pass state so summary doesn't contradict steps —
            # the exception itself (e.g. flush_pending_saves / cleanup) may have fired
            # AFTER all action and teardown iterations already passed.
            partial_actions_ok = all(
                step["status"] in (IterationStatus.SUCCESS.value, IterationStatus.HOLD.value) for step in action_results
            )
            if not teardown:
                partial_teardown_ok: bool | None = None
            elif teardown_result_dict is None:
                partial_teardown_ok = False
            else:
                partial_teardown_ok = teardown_result_dict["status"] == IterationStatus.TEARDOWN.value
            click.echo(
                json.dumps(
                    {
                        "summary": {
                            "all_passed": False,  # exception always means run failed overall
                            "skipped": False,
                            "skip_reason": None,
                            "steps_run": len(partial_steps),
                            "actions_passed": partial_actions_ok,
                            "teardown_passed": partial_teardown_ok,
                            "error": str(e),
                        },
                        "steps": partial_steps,
                    },
                    default=str,  # mirror success path — preserve datetime/Decimal/etc.
                )
            )
        else:
            click.echo(f"Error running test lifecycle: {e}", err=True)
        return 1
    finally:
        _logging.getLogger().removeHandler(log_buffer)

    # teardown_passed is None ("not applicable") when --teardown wasn't requested,
    # True when teardown ran and reached IterationStatus.TEARDOWN, False otherwise.
    # Same convention as the exception path, so JSON consumers see one shape.
    teardown_ok: bool | None
    if not teardown:
        teardown_ok = None
    elif teardown_result_dict is None:
        teardown_ok = False  # asked for but never executed (logic error)
    else:
        teardown_ok = teardown_result_dict["status"] == IterationStatus.TEARDOWN.value
    # all([]) is True — teardown-only runs (no actions) correctly identity to True here
    # and rely on teardown_ok for the final verdict.
    actions_ok = all(r["status"] in (IterationStatus.SUCCESS.value, IterationStatus.HOLD.value) for r in action_results)
    # Treat teardown_ok=None (not applicable) as a non-blocker for all_passed.
    all_passed = actions_ok and (teardown_ok is None or teardown_ok)

    if json_output:
        steps: list[dict] = list(action_results)
        if teardown_result_dict is not None:
            steps.append(teardown_result_dict)
        payload = {
            "deployment_id": _require_strategy_deployment_id(
                strategy_instance,
                operation="strat_test_json_output",
            ),
            "summary": {
                "all_passed": all_passed,
                "steps_run": len(steps),
                "actions_passed": actions_ok,
                "teardown_passed": teardown_ok,
            },
            "steps": steps,
        }
        click.echo(json.dumps(payload, indent=2, default=str))
    else:
        click.echo()
        if all_passed:
            click.echo("Test lifecycle passed.")
        else:
            click.echo("Test lifecycle failed.")

    return 0 if all_passed else 1


# ---------------------------------------------------------------------------
# Continuous execution
# ---------------------------------------------------------------------------


def _run_continuous(
    *,
    runner: Any,
    strategy_instance: Any,
    cleanup_fn: Callable[[], Coroutine[Any, Any, None]],
    interval: int,
    max_iterations: int | None,
    reset_fork: bool,
    managed_gateway: Any,
) -> int:
    """Execute the continuous run loop and return exit code.

    Synchronous wrapper that mirrors the ``else:`` block in ``run()``. Owns
    the outer ``asyncio.run(run_loop_with_cleanup())`` call, the
    ``KeyboardInterrupt`` fresh-loop cleanup (``asyncio.run(cleanup_fn())``),
    and the exit-code resolution. Keeping this sync preserves the original
    boundary: ``KeyboardInterrupt`` is raised from ``asyncio.run`` into the
    enclosing try/except, not into the coroutine itself.

    Behavior-preserving:

        * Registers runner signal handlers.
        * Wires an ``on_iteration`` echo callback.
        * Builds a ``pre_iteration`` callback if ``reset_fork`` is set and a
          managed gateway owns forks (raises ``CriticalCallbackError`` on
          reset failure).
        * Restores persisted strategy state inside the loop wrapper.
        * Runs ``runner.run_loop`` with the wired callbacks.
        * On ``KeyboardInterrupt`` requests shutdown and runs cleanup in a
          fresh event loop (matches original behavior).

    Returns:
        Exit code: ``2`` on signal-triggered stop, ``1`` on
        max-iterations-all-failed or unhandled exception, ``0`` otherwise.
    """
    import asyncio

    from ..runner.strategy_runner import CriticalCallbackError

    # Runtime-local reference for format_iteration_result.
    from .run import format_iteration_result

    if sys.stdout.isatty():
        click.echo()
        click.echo("Starting continuous execution...")
        click.echo("Press Ctrl+C to stop gracefully.")
        click.echo()

    # Set up signal handlers for graceful shutdown
    runner.setup_signal_handlers()

    def on_iteration(result: Any) -> None:
        """Callback for each iteration."""
        timestamp = result.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        click.echo(f"[{timestamp}] {format_iteration_result(result)}")

    # Build pre-iteration callback for --reset-fork
    pre_iteration_cb: Callable[[], None] | None = None
    if reset_fork and managed_gateway is not None:

        def pre_iteration_cb() -> None:
            click.echo("Resetting Anvil fork to latest block...")
            ok = managed_gateway.reset_anvil_forks()
            if ok:
                click.echo("Fork reset complete.")
            else:
                raise CriticalCallbackError(
                    "Anvil fork reset failed. Cannot continue with stale fork state. "
                    "Remove --reset-fork to run without fork resets."
                )

    async def run_loop_with_cleanup() -> None:
        """Run loop and cleanup resources."""
        try:
            # Restore persisted strategy state (e.g. position_id after restart)
            if hasattr(strategy_instance, "load_state_async"):
                if await strategy_instance.load_state_async():
                    click.secho("  Strategy state restored from persistence", fg="yellow")
                else:
                    click.echo("  No previous state found (fresh start)")

            await runner.run_loop(
                strategy=strategy_instance,
                interval_seconds=interval,
                iteration_callback=on_iteration,
                pre_iteration_callback=pre_iteration_cb,
                max_iterations=max_iterations,
            )
        finally:
            await cleanup_fn()

    try:
        asyncio.run(run_loop_with_cleanup())
        click.echo()

        # Exit 2 when stopped by signal (SIGTERM/SIGINT) so K8s sees a
        # pod failure and retries.  Check this first so it takes
        # precedence over the max-iterations branch.
        if runner._signal_received:
            click.echo("Runner stopped by signal.")
            return 2

        # Return a failure exit code when max_iterations is set and every
        # single iteration failed (no successful iterations at all).
        if max_iterations and runner._successful_iterations == 0 and runner._total_iterations > 0:
            click.echo(f"Runner completed {runner._total_iterations} iterations with 0 successes.")
            return 1

        click.echo("Runner stopped gracefully.")
        return 0

    except KeyboardInterrupt:
        click.echo()
        click.echo("Shutdown requested. Stopping...")
        runner.request_shutdown()
        # Run cleanup in a new event loop since the previous one was interrupted
        asyncio.run(cleanup_fn())
        return 0

    except Exception as e:
        click.echo(f"Error in run loop: {e}", err=True)
        logger.exception("Run loop failed")
        return 1
