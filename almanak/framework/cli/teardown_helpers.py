"""Pure helpers extracted from ``teardown.execute_teardown``.

The execute_teardown click command grew to ~880 LOC + CC=89 — well past the
``ruff C901`` 15-line-complexity gate and the highest non-omit CRAP entry in
the repo as of 2026-05-05. This module hosts the stateless and near-stateless
extractions in service of thinning that command without changing behavior.

The extraction follows the Phase 4 ``cli/run_helpers.py`` pattern: business
logic moves out of the ``@click.command`` body so it can be unit-tested in
isolation; the click body becomes a thin orchestration adapter that strings
helpers together.

**Behavior contract**: every helper here is ported verbatim from
``execute_teardown``. No new error paths, no normalization tweaks, no
silent data-shape changes. The CLI tests under ``tests/unit/cli/`` and
``tests/unit/teardown/`` are the verification surface — they exercised
``execute_teardown`` end-to-end before the extraction and must keep
passing after.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click

from almanak.config import cli_runtime_config_from_env

if TYPE_CHECKING:
    from almanak.gateway.managed import ManagedGateway

    from ..gateway_client import GatewayClient
    from ..teardown.models import TeardownResult

logger = logging.getLogger(__name__)


# =============================================================================
# Phase 1: option validation
# =============================================================================


def validate_teardown_options(no_gateway: bool, network: str | None) -> None:
    """Fail fast on incompatible CLI option combinations.

    ``--network`` selects between ``mainnet`` and ``anvil`` for the
    *managed* gateway path; with ``--no-gateway`` we connect to an
    already-running gateway and the flag is meaningless. Surfacing the
    conflict here — before the strategy-folder resolver runs — keeps the
    error message specific instead of falling through to a noisier failure
    later in strategy loading.
    """
    if no_gateway and network is not None:
        raise click.ClickException(
            "--network only applies when the managed gateway is auto-started. Remove --network or remove --no-gateway."
        )


# =============================================================================
# Phase 2: config loading
# =============================================================================


def load_strategy_config_dict(
    working_path: Path,
    config_file: str | None,
) -> tuple[dict[str, Any], str | None]:
    """Load the strategy config dict, auto-discovering ``config.{json,yaml,yml}``
    when no explicit path is given.

    Returns ``(config_dict, resolved_config_file)`` so the caller can echo the
    discovered path. ``config_dict`` is ``{}`` when no config file exists —
    the same fallback ``execute_teardown`` had inline.
    """
    resolved = config_file
    if resolved is None:
        for name in ["config.json", "config.yaml", "config.yml"]:
            candidate = working_path / name
            if candidate.exists():
                resolved = str(candidate)
                break

    if resolved is None:
        return {}, None

    # Explicit encoding for cross-platform safety (CR nitpick PR #2093):
    # Windows defaults to a non-utf-8 locale codec, which would fail on
    # config files containing non-ASCII content. utf-8 is the de-facto
    # standard for json/yaml so pin it.
    with open(resolved, encoding="utf-8") as fh:
        if resolved.endswith((".yaml", ".yml")):
            import yaml

            config_dict = yaml.safe_load(fh) or {}
        else:
            config_dict = json.load(fh)

    return config_dict, resolved


# =============================================================================
# Phase 3: wallet resolution
# =============================================================================


def resolve_wallet_address(
    config_dict: dict[str, Any],
    env: dict[str, str] | None = None,
) -> str | None:
    """Resolve the wallet address from config or ``ALMANAK_PRIVATE_KEY``.

    Checks ``config.wallet_address`` first; falls back to deriving the
    address from the private key. Returns ``None`` when neither is set —
    the caller decides whether that's fatal (it is, at strategy
    instantiation time, but managed-gateway boot can still succeed without
    a wallet, just without Anvil pre-funding).

    ``env`` defaults to the live process env (read through the typed
    gateway-config service) so callers can inject a fixture in tests
    without monkeypatching the global environment.
    """
    if wallet_address := config_dict.get("wallet_address"):
        return wallet_address
    if env is not None:
        private_key = env.get("ALMANAK_PRIVATE_KEY", "")
    else:
        # No explicit env dict supplied — read the typed boundary value.
        # We narrow to ``gateway_config_from_env`` instead of ``load_config``
        # so an unrelated submodel validation error (e.g. a malformed
        # ``ANVIL_*_PORT``) cannot strand teardown — wallet derivation only
        # needs the gateway's private key (PR #2152 review). The typed
        # ``GatewayConfig.private_key`` is populated by the Phase 1
        # ``_apply_gateway_env_fallbacks`` ladder which honours
        # ``ALMANAK_PRIVATE_KEY``.
        from almanak.config.env import gateway_config_from_env

        private_key = gateway_config_from_env().private_key or ""
    if not private_key:
        return None
    from eth_account import Account

    return Account.from_key(private_key).address


# =============================================================================
# Phase 4: gateway setup
# =============================================================================


@dataclass
class GatewaySetupResult:
    """Bundle of artifacts produced by ``setup_gateway``.

    The click body needs all four downstream:
    - ``client`` — talks to the gateway over gRPC
    - ``managed_gateway`` — set only when we auto-started one; ``None`` for
      ``--no-gateway``. The cleanup path stops it.
    - ``gateway_port`` — may differ from the requested port when
      ``find_available_gateway_port`` had to bump it.
    - ``solana_anvil_needed`` — derived during managed-gateway boot from
      the strategy's chain dispatch; the Solana fork bring-up below
      consults it.
    """

    client: GatewayClient
    managed_gateway: ManagedGateway | None
    gateway_port: int
    solana_anvil_needed: bool


def setup_gateway(
    no_gateway: bool,
    gateway_host: str,
    gateway_port: int,
    network: str | None,
    chain: str | None,
    config_dict: dict[str, Any],
    wallet_address: str | None,
) -> GatewaySetupResult:
    """Connect to or auto-start a gateway sidecar for the teardown.

    Two paths:
    - ``no_gateway=True``: connect to an already-running gateway, fail if
      unavailable. Used when the operator wants a long-lived sidecar
      shared across runs.
    - ``no_gateway=False`` (default): auto-start a managed gateway with a
      session auth token, register the stop hook with ``atexit`` as a
      safety net, then connect the client.

    The managed path mirrors the ``strat run`` ``run_helpers.py`` logic
    (multi-chain ``config["chains"]`` preferred over scalar ``chain``;
    Solana detection via ``_resolve_anvil_chain_dispatch``; per-chain
    Anvil startup timeout via ``_anvil_timeout.py``) so behavior stays
    identical between the two CLI paths (VIB-3819, VIB-3877).
    """
    import atexit

    # Lazy imports to keep the top-level cost low and to mirror the
    # original execute_teardown lazy-import pattern (VIB-522).
    from almanak.config.env import gateway_config_from_env
    from almanak.gateway.managed import ManagedGateway, find_available_gateway_port

    from ..gateway_client import GatewayClient, GatewayClientConfig

    # Normalize "localhost" to "127.0.0.1" (gateway binds to 127.0.0.1).
    effective_host = "127.0.0.1" if gateway_host == "localhost" else gateway_host
    managed_gateway: ManagedGateway | None = None
    # solana_anvil_needed only gets a real value on the managed path; for
    # --no-gateway we leave it False so the Solana-fork bring-up below
    # short-circuits cleanly.
    solana_anvil_needed = False

    if no_gateway:
        click.echo(f"Connecting to existing gateway at {effective_host}:{gateway_port}...")
        gateway_config = GatewayClientConfig(host=effective_host, port=gateway_port)
        gateway_client = GatewayClient(gateway_config)
        gateway_client.connect()

        if not gateway_client.health_check():
            gateway_client.disconnect()
            click.echo()
            click.secho("ERROR: Gateway is not running or not healthy", fg="red", bold=True)
            click.echo()
            click.echo("The gateway sidecar is required for teardown operations.")
            click.echo("Start the gateway first with:")
            click.echo()
            click.echo("  almanak gateway")
            click.echo()
            raise click.ClickException(f"Gateway not available at {effective_host}:{gateway_port}")

        click.secho(f"Connected to existing gateway at {effective_host}:{gateway_port}", fg="green")
        return GatewaySetupResult(
            client=gateway_client,
            managed_gateway=None,
            gateway_port=gateway_port,
            solana_anvil_needed=solana_anvil_needed,
        )

    # Default: auto-start a managed gateway.
    try:
        gateway_port = find_available_gateway_port(effective_host, gateway_port)
    except RuntimeError as e:
        click.echo()
        click.secho(f"ERROR: {e}", fg="red", bold=True)
        click.echo()
        click.echo("Set a specific port with:")
        click.echo()
        click.echo("  almanak strat teardown execute --gateway-port <port>")
        click.echo()
        click.echo("Or connect to an existing gateway:")
        click.echo()
        click.echo("  almanak strat teardown execute --no-gateway --gateway-port <port>")
        click.echo()
        logger.error("Managed gateway failed to start", exc_info=True)
        raise click.ClickException(str(e)) from e

    # Random session token so the managed gateway is never running without
    # authentication on mainnet (matches run.py pattern).
    import uuid

    session_auth_token = uuid.uuid4().hex

    resolved_network = network or "mainnet"
    # Phase 1: route through the config service so the same env-fallback
    # ladders apply here as for the gateway subcommand and managed gateway.
    gateway_settings = gateway_config_from_env(
        grpc_host=effective_host,
        grpc_port=gateway_port,
        network=resolved_network,
        allow_insecure=resolved_network == "anvil",
        metrics_enabled=False,
        audit_enabled=False,
        chains=[chain] if chain else [],
        auth_token=session_auth_token,
    )

    # VIB-3819: when running against --network anvil, the gateway must boot
    # an Anvil fork for the strategy's chain (and pre-fund the wallet) — the
    # `strat run` path does this via run_helpers; teardown was missing it,
    # causing the balance provider to hit a dead RPC port (8548) and the
    # strategy's get_open_positions() to swallow the error and report "no
    # positions". VIB-3705's no-op branch then exits 0 while WETH (or any
    # held position) is silently stranded on-chain. Mirror the run-helpers
    # pattern: pass anvil_chains, wallet_address, anvil_funding so the fork
    # actually starts and is pre-funded for the close swap.
    #
    # Multi-chain teardown: prefer config["chains"] (a list, possibly with
    # multiple entries) over the scalar `chain`, so a strategy holding
    # positions on more than one chain has every fork started. Falls back
    # to [chain] for the common single-chain case. Mirrors the
    # run_helpers.py:882-897 derivation order so behavior between
    # `strat run` and `strat teardown` stays identical (Codex review,
    # multi-chain teardown gap).
    from .run_helpers import _normalize_anvil_funding, _resolve_anvil_chain_dispatch

    anvil_chains, solana_anvil_needed = _resolve_anvil_chain_dispatch(resolved_network, chain, config_dict)
    anvil_funding = (
        _normalize_anvil_funding(config_dict.get("anvil_funding", {})) if (anvil_chains or solana_anvil_needed) else {}
    )

    chain_summary_parts: list[str] = []
    if anvil_chains:
        chain_summary_parts.append(f"anvil chains: {', '.join(anvil_chains)}")
    if solana_anvil_needed:
        chain_summary_parts.append("solana fork: yes")
    if not chain_summary_parts and chain:
        chain_summary_parts.append(f"chain={chain}")
    chain_summary = ", ".join(chain_summary_parts) if chain_summary_parts else f"chain={chain}"
    click.echo(
        f"Starting managed gateway on {effective_host}:{gateway_port} (network={resolved_network}, {chain_summary})..."
    )
    managed_gateway = ManagedGateway(
        gateway_settings,
        anvil_chains=anvil_chains,
        wallet_address=wallet_address,
        anvil_funding=anvil_funding,
    )

    # Per-chain Anvil startup-timeout policy lives in _anvil_timeout.py
    # (VIB-3877) so the run + teardown CLI paths can never drift.
    from ._anvil_timeout import compute_anvil_startup_timeout

    startup_timeout = compute_anvil_startup_timeout(anvil_chains)

    try:
        managed_gateway.start(timeout=startup_timeout)
    except RuntimeError as e:
        logger.error("Managed gateway startup failed", exc_info=True)
        click.echo()
        click.secho(f"ERROR: Failed to start managed gateway: {e}", fg="red", bold=True)
        click.echo()
        raise click.ClickException("Managed gateway startup failed") from e

    # Register atexit handler as safety net for sys.exit() paths that skip cleanup
    atexit.register(managed_gateway.stop)

    click.secho(f"Managed gateway started on {effective_host}:{gateway_port}", fg="green")

    # Connect client to the managed gateway.
    gateway_config = GatewayClientConfig(host=effective_host, port=gateway_port, auth_token=session_auth_token)
    gateway_client = GatewayClient(gateway_config)
    gateway_client.connect()

    if not gateway_client.health_check():
        managed_gateway.stop()
        gateway_client.disconnect()
        raise click.ClickException(
            "Managed gateway started but health check failed. Check gateway logs above for details."
        )

    return GatewaySetupResult(
        client=gateway_client,
        managed_gateway=managed_gateway,
        gateway_port=gateway_port,
        solana_anvil_needed=solana_anvil_needed,
    )


# =============================================================================
# Phase 5: Solana fork bring-up
# =============================================================================


class SolanaForkHandle:
    """Owns a started ``SolanaForkManager`` and provides idempotent cleanup.

    Wraps the original ``solana_stopped`` nonlocal flag in a small object
    so the atexit safety-net and the ``finally`` block can both call
    ``.stop()`` without double-stopping (which on Python 3.12+ would
    re-enter a closed event loop).
    """

    def __init__(self, fork_mgr: Any) -> None:
        self._fork_mgr = fork_mgr
        self._stopped = False

    @property
    def fork_mgr(self) -> Any:
        return self._fork_mgr

    @property
    def stopped(self) -> bool:
        return self._stopped

    def stop(self, *, swallow: bool = True, echo_on_success: bool = False) -> None:
        """Stop the fork. Idempotent — second call is a no-op.

        ``swallow=True`` (default) is for the atexit safety-net path:
        any exception during stop is suppressed because the interpreter
        is exiting anyway. ``swallow=False`` is for the ``finally``
        cleanup path where we still want to log debug info on failure.
        """
        if self._stopped:
            return
        self._stopped = True
        # Local import: same reasoning as the original (avoid pinning a
        # stale event-loop reference).
        import asyncio as _aio

        try:
            _aio.run(self._fork_mgr.stop())
            if echo_on_success:
                click.echo("  Stopped solana-test-validator")
        except Exception as exc:
            if swallow:
                return
            logger.debug("Failed to stop solana-test-validator: %s", exc)


def setup_solana_fork(
    *,
    config_dict: dict[str, Any],
    wallet_address: str | None,
    managed_gateway: Any | None,
) -> SolanaForkHandle | None:
    """Spin up a local ``solana-test-validator`` and fund the wallet.

    VIB-3878: when teardown runs against ``--network anvil`` for a Solana
    strategy, start a local validator the same way ``strat run`` does
    (run_helpers.py:1837-1884). Without this, the Solana balance probe
    hits a dead RPC, ``get_open_positions()`` swallows the error, and
    the no-op exit branch silently leaves Solana positions stranded —
    same VIB-3819 failure mode the EVM gateway-fork wiring already
    plugged.

    Pre-clones any pool addresses the strategy advertises in config and
    the Orca pool accounts (vaults + tick arrays) discovered via
    ``_solana_setup.get_orca_pool_accounts``.

    Funds the wallet with 100 SOL + 10K USDC + 10K USDT (PR #N CR major:
    captures bool returns so silent funding failures surface as a
    yellow warning rather than as a confusing "insufficient balance"
    mid-teardown).

    Registers an atexit safety-net for Ctrl-C / sys.exit() paths that
    skip the ``finally`` cleanup. The handle's ``stop()`` is idempotent,
    so the atexit + finally pair never double-stop the validator.

    On startup failure, the caller's ``managed_gateway`` is stopped
    before raising — preserves the original behavior where a half-booted
    state leaves no orphan EVM gateway running.

    Returns ``None`` when the caller didn't request a Solana fork
    (i.e. ``solana_anvil_needed=False``); the caller decides via the
    upstream ``setup_gateway`` result.
    """
    import asyncio as _aio
    import atexit
    from decimal import Decimal as _Decimal

    from ..anvil.solana_fork_manager import SolanaForkManager
    from ._solana_setup import get_orca_pool_accounts

    cli_cfg = cli_runtime_config_from_env()
    solana_rpc_url = cli_cfg.solana_rpc_url
    extra_clone: list[str] = []
    for _key in ("pool_address", "pool_a_address", "pool_b_address"):
        _addr = config_dict.get(_key)
        if _addr and isinstance(_addr, str):
            extra_clone.append(_addr)
    _orca_accounts = get_orca_pool_accounts(config_dict)
    if _orca_accounts:
        click.echo(f"  Pre-cloning {len(_orca_accounts)} Orca pool accounts (vaults + tick arrays)")
        extra_clone.extend(_orca_accounts)
    if extra_clone:
        click.echo(f"  Cloning {len(extra_clone)} account(s) from mainnet")

    fork_mgr = SolanaForkManager(
        rpc_url=solana_rpc_url,
        validator_port=cli_cfg.solana_validator_port,
        clone_accounts=extra_clone,
    )
    click.echo("  Starting local solana-test-validator...")

    # Use asyncio.run() (not get_event_loop().run_until_complete()) so we
    # don't accidentally pin a stale loop reference. The finally + atexit
    # cleanup also use asyncio.run(), which fails on Python 3.12+ when a
    # loop is already running but works correctly outside any loop — the
    # exact post-asyncio.run() state the cleanup code runs in.
    try:
        started = _aio.run(fork_mgr.start())
    except Exception as exc:
        if managed_gateway is not None:
            managed_gateway.stop()
        raise click.ClickException(f"solana-test-validator failed to start: {exc}") from exc
    if not started:
        if managed_gateway is not None:
            managed_gateway.stop()
        raise click.ClickException(
            "Failed to start solana-test-validator. "
            "Ensure Solana CLI tools are installed: "
            'sh -c "$(curl -sSfL https://release.anza.xyz/stable/install)"'
        )
    click.echo(f"  solana-test-validator running at {fork_mgr.get_rpc_url()}")

    handle = SolanaForkHandle(fork_mgr)

    if wallet_address:
        # ``fund_wallet`` and ``fund_tokens`` return bool. Capture the
        # results — a silent funding failure (airdrop quota, validator
        # hiccup) would otherwise surface as a confusing "insufficient
        # balance" mid-teardown. CodeRabbit P_major.
        sol_funded = _aio.run(fork_mgr.fund_wallet(wallet_address, _Decimal("100")))
        tokens_funded = _aio.run(
            fork_mgr.fund_tokens(
                wallet_address,
                {"USDC": _Decimal("10000"), "USDT": _Decimal("10000")},
            )
        )
        if sol_funded and tokens_funded:
            click.echo("  Wallet funded with 100 SOL + 10K USDC + 10K USDT")
        else:
            click.secho(
                f"  Warning: Solana wallet funding incomplete (SOL={sol_funded}, "
                f"tokens={tokens_funded}). Teardown may fail with insufficient balance.",
                fg="yellow",
            )

    # atexit safety-net for Ctrl-C / sys.exit() paths that skip the
    # finally block. The handle's stop() is idempotent so the success-
    # path finally cleanup doesn't get followed by a second stop() at
    # interpreter shutdown (which would re-enter the now-closed loop on
    # Py 3.12+).
    atexit.register(lambda: handle.stop(swallow=True))

    return handle


# =============================================================================
# Phase 11: cleanup
# =============================================================================


def cleanup_teardown_resources(
    *,
    resolver: Any,
    gateway_client: Any,
    solana_handle: SolanaForkHandle | None,
    managed_gateway: Any | None,
) -> None:
    """Symmetric teardown of the resources brought up earlier in the CLI.

    Order matches the original execute_teardown ``finally`` block:
    1. Reset TokenResolver gateway channel
    2. Disconnect gateway client
    3. Stop Solana fork (idempotent — atexit may have already run)
    4. Stop managed gateway (atexit-registered too, but we stop here for
       deterministic cleanup ordering)
    """
    resolver.set_gateway_channel(None)
    gateway_client.disconnect()
    if solana_handle is not None:
        solana_handle.stop(swallow=False, echo_on_success=True)
    if managed_gateway is not None:
        managed_gateway.stop()


# =============================================================================
# Phase 6: strategy instantiation + state restoration
# =============================================================================


def instantiate_strategy_with_state(
    *,
    strategy_class: type,
    config_dict: dict[str, Any],
    chain: str | None,
    wallet_address: str | None,
    gateway_client: Any,
    inject_balance_provider,
    restore_strategy_state,
) -> Any:
    """Wire the gateway channel into TokenResolver, instantiate the strategy,
    inject the balance provider, restore persisted state.

    ``inject_balance_provider`` and ``restore_strategy_state`` are the two
    same-file helpers from ``teardown.py`` — passed in to avoid a circular
    import. They have signatures
    ``inject_balance_provider(strategy, gateway_client, chain, wallet_address)``
    and
    ``restore_strategy_state(strategy=..., strategy_class=..., config_dict=..., gateway_client=...)``.

    Hard-fails with ``ClickException`` when ``wallet_address`` is ``None``
    here (managed-gateway boot can survive a missing wallet — only Anvil
    pre-funding is skipped — but strategy instantiation requires it).
    """
    from ..data.tokens import get_token_resolver

    resolver = get_token_resolver()
    resolver.set_gateway_channel(gateway_client.channel)

    if not wallet_address:
        raise click.ClickException(
            "Could not determine wallet address. Set config.wallet_address or ALMANAK_PRIVATE_KEY."
        )

    # Deferred import to avoid heavy run.py import cascade (VIB-522).
    from .run import DictConfigWrapper

    config_obj = DictConfigWrapper(config_dict)

    try:
        strategy = strategy_class(
            config=config_obj,
            chain=chain,
            wallet_address=wallet_address,
        )
    except Exception as e:
        logger.error("Failed to instantiate strategy", exc_info=True)
        raise click.ClickException(f"Failed to instantiate strategy: {e}") from e

    inject_balance_provider(strategy, gateway_client, chain, wallet_address)
    restore_strategy_state(
        strategy=strategy,
        strategy_class=strategy_class,
        config_dict=config_dict,
        gateway_client=gateway_client,
    )
    return strategy


def get_resolver_for_cleanup() -> Any:
    """Return the same TokenResolver singleton ``instantiate_strategy_with_state``
    wires. Caller stashes this for the cleanup path (which calls
    ``resolver.set_gateway_channel(None)``)."""
    from ..data.tokens import get_token_resolver

    return get_token_resolver()


# =============================================================================
# Phase 7: position discovery
# =============================================================================


def discover_positions(
    *,
    strategy: Any,
    strategy_class: type,
    discover: bool,
    include_empty: bool,
    gateway_client: Any,
    chain: str | None,
    wallet_address: str | None,
) -> Any:
    """Get open positions via ``--discover`` (NPM scan via gateway) or via
    ``strategy.get_open_positions()`` (strategy's own tracking).

    ``--discover`` bypasses the strategy's local state and reads NPM
    contracts directly via the gateway, so orphaned positions (e.g.
    after a gateway restart lost the in-memory tracking) remain
    recoverable. Without ``--discover``, the strategy's own tracking is
    authoritative — it knows ``value_usd``, health factors, and non-LP
    positions the on-chain scan wouldn't surface.
    """
    if discover:
        from ..teardown.discovery import discover_lp_positions, to_teardown_summary

        click.echo("\nDiscovering LP positions on-chain...")

        async def _do_discover():
            # Mypy: chain/wallet are typed `str | None` here while the
            # underlying functions require `str`. The original inline code
            # passed without complaint because the variables were inferred
            # as Any (from `dict.get(...) or default(...)`); the explicit
            # annotation here is more precise and preserves the original
            # runtime behavior — when None flows in, the underlying call
            # raises just as it did pre-refactor. type: ignore retains
            # parity rather than adding a new ClickException early-fail.
            return await discover_lp_positions(
                client=gateway_client,
                chain=chain,  # type: ignore[arg-type]
                wallet=wallet_address,  # type: ignore[arg-type]
                include_zero_liquidity=include_empty,
            )

        try:
            import asyncio

            discovered = asyncio.run(_do_discover())
        except Exception as e:
            logger.error("On-chain discovery failed", exc_info=True)
            raise click.ClickException(f"On-chain discovery failed: {e}") from e

        deployment_id = (getattr(strategy, "deployment_id", "") or "").strip()
        if not deployment_id:
            raise click.ClickException("Teardown discovery requires a resolved deployment_id")
        positions = to_teardown_summary(
            deployment_id=deployment_id,
            chain=chain,  # type: ignore[arg-type]  # See note above on _do_discover
            positions=discovered,
        )
        click.echo(f"  Found {len(discovered)} on-chain LP position(s).")
        return positions

    try:
        return strategy.get_open_positions()
    except Exception as e:
        logger.error("Failed to get positions from strategy", exc_info=True)
        raise click.ClickException(f"Failed to get positions: {e}") from e


def print_no_op_if_empty_and_signal_return(
    *,
    positions: Any,
    strategy: Any,
    strategy_class: type,
    discover: bool,
    no_op_message_builder,
) -> bool:
    """When ``positions.positions`` is empty, print the canonical no-op
    success message + (when not ``--discover``) the discovery tip, and
    return ``True`` so the caller can ``return`` from the CLI.

    Returns ``False`` when there are positions to tear down — caller
    proceeds normally.

    VIB-3705: returning here yields exit 0 via Click's normal command-
    return semantics — swap-only / HOLD-state strategies (uniswap_v4_swap_*,
    fluid_swap_*, edge_yield_*_fluiddex, edge_yield_base_univ4) hit this
    branch whenever the wallet's balance for the strategy's quote/target
    token is 0. Treating that as exit 1 produced 5+ false failures in the
    April 28-29 QA batch.
    """
    if positions.positions:
        return False
    deployment_id_for_log = strategy.deployment_id
    no_op_msg = no_op_message_builder(deployment_id_for_log)
    click.echo()
    click.secho(no_op_msg, fg="green")
    logger.info(no_op_msg)
    if not discover:
        click.echo(
            "Tip: if positions were opened by a previous gateway instance, "
            "rerun with --discover to scan NPM contracts on-chain."
        )
    return True


# =============================================================================
# Phase 8a: position display
# =============================================================================


def display_position_summary(positions: Any) -> tuple[Decimal, int]:
    """Print the open-positions table to stdout and return
    ``(total_value, unknown_value_count)`` so the caller can drive the
    SafetyGuard warning.

    ``positions`` is duck-typed (a ``TeardownSummary``) — has a
    ``positions`` list of ``PositionInfo``-shaped objects with
    ``position_type``, ``protocol``, ``chain``, ``position_id``,
    ``value_usd``, ``health_factor``. Test doubles can supply
    ``SimpleNamespace`` instances.
    """
    click.echo(f"\nOpen Positions ({len(positions.positions)}):")
    total_value = Decimal("0")
    unknown_value_count = 0
    for i, pos in enumerate(positions.positions, 1):
        click.echo(f"  {i}. [{pos.position_type.value}] {pos.protocol} on {pos.chain}")
        click.echo(f"     Position ID: {pos.position_id}")
        # Some test doubles don't expose `details` — tolerate that while
        # still checking the flag on real PositionInfo instances.
        pos_details = getattr(pos, "details", None) or {}
        if pos_details.get("value_usd_unknown"):
            click.echo("     Value: unknown (discovered on-chain, not priced)")
            unknown_value_count += 1
        else:
            click.echo(f"     Value: ${pos.value_usd:,.2f}")
        if pos.health_factor:
            click.echo(f"     Health Factor: {pos.health_factor:.2f}")
        total_value += pos.value_usd

    click.echo(f"\nTotal Value: ${total_value:,.2f}")
    return total_value, unknown_value_count


def display_unknown_value_warning(unknown_value_count: int) -> None:
    """Loud warning when ``--discover`` couldn't price positions.

    SafetyGuard uses ``total_value_usd`` to pick the loss cap, and ``$0``
    maps to the *most permissive* 3% tier (calculate_max_acceptable_loss).
    A mispriced $1M LP would otherwise get the same cap as a $100
    position. (PR #1522 CodeRabbit major.)
    """
    if unknown_value_count <= 0:
        return
    click.echo()
    click.secho(
        f"WARNING: {unknown_value_count} position(s) discovered without USD pricing. "
        "Teardown safety caps will be computed as if total value = $0, which uses the "
        "MOST PERMISSIVE loss tier. Review the tick ranges above before executing.",
        fg="yellow",
        bold=True,
    )


# =============================================================================
# Phase 8b: market snapshot + price oracle
# =============================================================================


def build_market_and_oracle(strategy: Any) -> tuple[Any | None, Any | None]:
    """Create the market snapshot up-front so the preview intents match
    what will execute. Returns ``(market, price_oracle)``; either may be
    ``None``. Logs a warning and degrades silently on any exception —
    the downstream IntentCompiler honors ``allow_placeholder_prices``
    when ``price_oracle is None``.
    """
    market: Any | None = None
    price_oracle: Any | None = None
    try:
        if hasattr(strategy, "create_market_snapshot"):
            market = strategy.create_market_snapshot()
            if hasattr(market, "get_price_oracle_dict"):
                fetched = market.get_price_oracle_dict()
                price_oracle = fetched if fetched is not None else None
                if price_oracle:
                    click.echo(f"\n  Using real prices for {len(price_oracle)} tokens")
    except Exception as e:
        click.echo(f"\n  Warning: Could not get market prices ({e}), using placeholders")
    return market, price_oracle


# =============================================================================
# Phase 8c: teardown intent generation + display
# =============================================================================


def _apply_lending_unwind_guard_cli(intents: list[Any], market: Any, mode: Any = None) -> list[Any]:
    """CLI wrapper around the VIB-5139 / VIB-4466 lending fresh-state guard.

    Returns the guarded intent list and echoes drops / synthesis / degraded
    outcomes for the operator. A guard error never blocks teardown — it falls
    back to the original intents with a warning (teardown's first job is removing
    on-chain risk).
    """
    from ..teardown.lending_unwind_guard import sanitize_lending_teardown_intents

    try:
        guarded = sanitize_lending_teardown_intents(intents, market, mode=mode)
    except Exception as e:  # pragma: no cover - defensive; guard is pure
        click.echo(f"\n  Warning: lending fresh-state guard errored ({e}); using original intents")
        return intents

    for reason in guarded.dropped:
        click.echo(f"  Lending guard dropped intent — {reason}")
    for synth in guarded.synthesized_positions:
        click.echo(
            f"  Lending guard synthesised HF-safe unwind staircase for {synth} "
            "(wallet cannot fully repay live debt; naive withdraw-all would revert)"
        )
    if guarded.no_op_positions:
        click.echo(f"  Lending guard: positions already flat: {', '.join(guarded.no_op_positions)}")
    if guarded.degraded:
        click.echo(
            "  Lending guard degraded: a fresh exposure read was unmeasured — "
            "kept risk-reducing intents only, suppressed any unconfirmed withdraw_all (VIB-5139)"
        )
    return guarded.intents


def generate_teardown_intents_for_cli(
    *,
    strategy: Any,
    mode_str: str,
    market: Any | None,
    discover: bool,
    positions: Any,
) -> list[Any]:
    """Generate the list of teardown intents.

    Two paths:
    - ``discover=True``: synthesize ``LPCloseIntent`` directly from the
      NPM-discovered positions (the strategy has no record of them, so
      ``strategy.generate_teardown_intents()`` would return nothing).
      Graceful teardowns collect fees by default; emergency teardowns
      skip fee collection to minimise wall-clock time and gas.
    - ``discover=False``: delegate to ``strategy.generate_teardown_intents``.
      Tries the new signature with ``market=`` first, falls back to the
      legacy positional-only signature when the strategy is on the old
      API.

    Echoes the resulting steps to stdout (preserving the original
    "Teardown Steps (N): ..." block).
    """
    from ..teardown import PositionType, TeardownMode

    internal_mode = TeardownMode.SOFT if mode_str == "graceful" else TeardownMode.HARD
    if discover:
        from ..intents.vocabulary import LPCloseIntent

        collect_fees_default = internal_mode == TeardownMode.SOFT
        intents = [
            LPCloseIntent(
                position_id=pos.position_id,
                protocol=pos.protocol,
                chain=pos.chain,
                collect_fees=collect_fees_default,
            )
            for pos in positions.positions
            if pos.position_type == PositionType.LP
        ]
    else:
        try:
            try:
                intents = strategy.generate_teardown_intents(internal_mode, market=market)
            except TypeError as exc:
                if "market" in str(exc):
                    intents = strategy.generate_teardown_intents(internal_mode)
                else:
                    raise
        except Exception as e:
            logger.error("Failed to generate teardown intents", exc_info=True)
            raise click.ClickException(f"Failed to generate teardown intents: {e}") from e

    # VIB-5139: universal fresh-state guard for lending unwind. Drops stale
    # REPAY 0 / withdraw_all-when-flat / withdraw-before-repay using a FRESH
    # gateway-backed exposure read; degrades conservatively on an unmeasured
    # read (Empty ≠ Zero). LP_CLOSE / discover-path intents pass through. Pure
    # list transform — dispatch funnel + commit pairing unchanged.
    intents = _apply_lending_unwind_guard_cli(intents, market, internal_mode)

    click.echo(f"\nTeardown Steps ({len(intents)}):")
    for i, intent in enumerate(intents, 1):
        intent_type = getattr(intent, "intent_type", "UNKNOWN")
        if hasattr(intent_type, "value"):
            intent_type = intent_type.value
        click.echo(f"  {i}. {intent_type}")

    return intents


# =============================================================================
# Phase 9a: confirmation
# =============================================================================


def prompt_teardown_confirmation(force: bool) -> bool:
    """Ask for explicit confirmation unless ``--force`` was passed.

    Returns ``True`` when the user said yes (or ``force`` short-circuits the
    prompt), ``False`` when the user declined.
    """
    if force:
        return True
    click.echo("\n" + "=" * 60)
    click.echo("WARNING: This will close all positions listed above.")
    click.echo("=" * 60)
    if not click.confirm("Do you want to proceed?"):
        click.echo("Teardown cancelled.")
        return False
    return True


# =============================================================================
# Phase 9b: teardown machinery (orchestrator + compiler + state adapter)
# =============================================================================


@dataclass
class TeardownMachinery:
    """Bundle of pre-async-context teardown machinery.

    All four are constructed synchronously before ``run_teardown_with_brackets``
    is invoked. ``cli_teardown_id`` and ``teardown_cycle_id`` are pre-generated
    so the cycle-id stamped on the bracket snapshots matches the cycle-id
    ``_execute_intents`` derives for per-intent commits (VIB-3839 — without
    this alignment the audit query ``WHERE cycle_id = X`` would split rows
    across two ids).
    """

    orchestrator: Any
    compiler: Any
    state_adapter: Any
    cli_teardown_id: str
    teardown_cycle_id: str


def build_teardown_machinery(
    *,
    gateway_client: Any,
    chain: str | None,
    wallet_address: str | None,
    price_oracle: Any | None,
    no_accounting: bool,
) -> TeardownMachinery:
    """Construct the synchronous teardown machinery and emit the
    ``--no-accounting`` operator-warning if applicable.

    The async ``TeardownManager`` and the optional ``StrategyRunner``
    (for accounting wiring) are built later inside
    ``run_teardown_with_brackets`` because they require an async context
    (``state_manager.initialize()`` is async).

    Surfaces ``LocalPathError`` raised by ``TeardownStateAdapter()`` as a
    clean ``ClickException`` (canonical remediation hint already in the
    message) instead of a raw traceback.
    """
    import uuid as _uuid

    from almanak.framework.local_paths import LocalPathError
    from almanak.framework.teardown.state_manager import TeardownStateAdapter

    from ..execution.gateway_orchestrator import GatewayExecutionOrchestrator
    from ..intents.compiler import IntentCompiler, IntentCompilerConfig

    # Mypy: chain/wallet typed `str | None` here, but the orchestrator and
    # compiler require `str`. The original inline code passed without
    # complaint because the variables were inferred as Any (see the same
    # note on discover_positions._do_discover). type: ignore preserves
    # original runtime behavior — None values would crash here just as
    # they did pre-refactor.
    orchestrator = GatewayExecutionOrchestrator(
        client=gateway_client,
        chain=chain,  # type: ignore[arg-type]
        wallet_address=wallet_address,
    )

    # Create compiler with real prices if available.
    # gateway_client is mandatory: LP_CLOSE compilation queries on-chain state
    # (ERC20 LP balances for Aerodrome, position liquidity for Uniswap V3).
    # Without it every on-chain query returns None and compilation fails silently.
    compiler_config = IntentCompilerConfig(allow_placeholder_prices=price_oracle is None)
    compiler = IntentCompiler(
        chain=chain,  # type: ignore[arg-type]
        wallet_address=wallet_address,  # type: ignore[arg-type]
        rpc_url=None,  # Will use gateway
        price_oracle=price_oracle,
        config=compiler_config,
        gateway_client=gateway_client,
    )

    # VIB-3835: TeardownStateAdapter() resolves through the strict
    # strategy-scoped path resolver. Surface LocalPathError as a clean CLI
    # error rather than a raw traceback.
    try:
        state_adapter = TeardownStateAdapter()
    except LocalPathError as exc:
        raise click.ClickException(str(exc)) from exc

    # VIB-3839 cycle-id alignment.
    cli_teardown_id = f"td_{_uuid.uuid4().hex[:12]}"
    teardown_cycle_id = f"teardown-{cli_teardown_id}"

    if no_accounting:
        click.echo(
            click.style(
                "  --no-accounting: augmentation pipeline DISABLED — "
                "transaction_ledger / accounting_events / position_events / "
                "portfolio_snapshots / portfolio_metrics will NOT be updated, "
                "and the pre/post-teardown snapshot brackets are skipped",
                fg="yellow",
            )
        )

    return TeardownMachinery(
        orchestrator=orchestrator,
        compiler=compiler,
        state_adapter=state_adapter,
        cli_teardown_id=cli_teardown_id,
        teardown_cycle_id=teardown_cycle_id,
    )


# =============================================================================
# Phase 9c: async run_teardown with VIB-3839 cycle-id swap + pre/post brackets
# =============================================================================


async def run_teardown_with_brackets(
    *,
    machinery: TeardownMachinery,
    strategy: Any,
    mode_str: str,
    market: Any | None,
    discover: bool,
    positions: Any,
    intents: list[Any],
    no_accounting: bool,
    gateway_client: Any,
    price_oracle: Any | None,
    chain: str | None,
    wallet_address: str | None,
    build_cli_teardown_runner,
) -> TeardownResult:
    """Run the teardown end-to-end inside an async context.

    Encapsulates the load-bearing VIB-3773 / VIB-3839 contract:

    1. Build the async ``TeardownManager`` (which needs the optional
       ``StrategyRunner`` to host accounting writers; ``--no-accounting``
       skips this and signals "writers intentionally bypassed").
    2. Apply the cycle-id swap on **both** surfaces (P1-4):
       ``runner._last_cycle_id`` (read first by ``runner_state``) AND
       the ``observability.context`` ContextVar.
    3. Capture the VIB-3839 pre-bracket snapshot (degraded-but-continue —
       failures land in the deferred-write log but never abort the
       teardown).
    4. Execute via the manager.
    5. Capture the post-bracket snapshot with the same degraded-but-
       continue contract.
    6. Restore the cycle ids in ``finally`` so they never leak into
       subsequent iterations.

    ``build_cli_teardown_runner`` is passed in (rather than imported) to
    avoid a circular import on the parent module's same-file async
    helper.
    """
    from almanak.framework.teardown.runner_helpers import build_runner_helpers

    from ..teardown.teardown_manager import TeardownManager

    async def on_progress(pct: int, msg: str):
        click.echo(f"  [{pct}%] {msg}")

    # VIB-3839 + VIB-3892: build the minimal StrategyRunner inside the
    # async context (state_manager.initialize() is async). The runner
    # exists only to host the runner_helpers callables — it is never
    # started as an iteration loop. Failures here propagate as
    # ClickException — never silently fall back to a runner_helpers=None
    # bypass (audit B2: books and on-chain reality must not diverge
    # silently).
    runner = None
    runner_helpers_for_manager = None
    if not no_accounting:
        try:
            runner = await build_cli_teardown_runner(
                gateway_client=gateway_client,
                price_oracle=price_oracle,
                orchestrator=machinery.orchestrator,
                chain=chain,
                wallet_address=wallet_address,
            )
            runner_helpers_for_manager = build_runner_helpers(runner)
        except Exception as exc:
            logger.error(
                "Could not wire accounting pipeline for teardown: %s",
                exc,
                exc_info=True,
            )
            raise click.ClickException(
                f"Accounting pipeline wiring failed: {exc}. "
                "Teardown aborted to prevent silent books/on-chain divergence. "
                "Pass --no-accounting to proceed without writing accounting tables "
                "(operator opt-in for known-broken environments only)."
            ) from exc

    # Original click body had a `# type: ignore[arg-type]` on this kwarg
    # because the inline orchestrator was typed precisely as
    # `GatewayExecutionOrchestrator` while TeardownManager expects a
    # protocol-shaped duck. Inside the helper, machinery.orchestrator is
    # `Any` (dataclass field), so mypy doesn't need the ignore — and
    # complains if it's left in.
    # VIB-5011: the execute lane surfaces no asset-routing knobs (those live
    # on the request lane), so token consolidation is DISABLED here — the
    # wallet-scoped ``amount="all"`` consolidation sweep requires the
    # operator's explicit asset policy as consent (pr-auditor blocker).
    # ``teardown request -a target`` is the consolidating path.
    from ..teardown.config import TeardownConfig as _TC

    _execute_lane_config = _TC.default()
    _execute_lane_config.token_consolidation.enabled = False
    teardown_manager = TeardownManager(
        orchestrator=machinery.orchestrator,
        compiler=machinery.compiler,
        state_manager=machinery.state_adapter,
        runner_helpers=runner_helpers_for_manager,
        config=_execute_lane_config,
    )

    kwargs = {
        "strategy": strategy,
        "mode": mode_str,
        "on_progress": on_progress,
        "market": market,
        "teardown_id": machinery.cli_teardown_id,
    }
    if discover:
        kwargs["precomputed_positions"] = positions
        kwargs["precomputed_intents"] = intents

    # When --no-accounting is set, skip the cycle-id swap and pre/post
    # brackets entirely; the pipeline is intentionally bypassed so there
    # are no accounting writers to align cycle ids for.
    if no_accounting or runner is None:
        return await teardown_manager.execute(**kwargs)

    # VIB-3839: cycle-id swap on BOTH surfaces (P1-4 — runner_state.py
    # reads ``runner._last_cycle_id`` first, then falls back to the
    # contextvar). Mirrors ``execute_teardown_via_manager`` Phase 6.5.
    from ..observability.context import (
        clear_cycle_id,
        get_cycle_id,
        set_cycle_id,
    )

    saved_last_cycle_id = getattr(runner, "_last_cycle_id", "") or ""
    saved_ctx_cycle_id = get_cycle_id()
    runner._last_cycle_id = machinery.teardown_cycle_id
    set_cycle_id(machinery.teardown_cycle_id)

    # Hoist the local once so the type narrows for the rest of the block —
    # Mypy can't follow the ``has_snapshot`` property across the boundary,
    # but it can narrow ``capture_snapshot`` itself.
    capture_snapshot = teardown_manager.runner_helpers.capture_snapshot
    try:
        # VIB-3839 pre-bracket: degraded-but-continue (failures land in
        # the deferred-write log but never abort the teardown).
        if capture_snapshot is not None:
            pre_outcome = await capture_snapshot(
                strategy,
                teardown_cycle_id=machinery.teardown_cycle_id,
                pre_teardown=True,
            )
            if pre_outcome.accounting_degraded:
                logger.error(
                    "CLI pre-teardown snapshot accounting degraded for %s — %s",
                    strategy.deployment_id,
                    pre_outcome.degraded_reason or "unknown",
                )

        result = await teardown_manager.execute(**kwargs)

        # VIB-3839 post-bracket: same degraded-but-continue contract.
        if capture_snapshot is not None:
            post_outcome = await capture_snapshot(
                strategy,
                teardown_cycle_id=machinery.teardown_cycle_id,
                pre_teardown=False,
            )
            if post_outcome.accounting_degraded:
                logger.error(
                    "CLI post-teardown snapshot accounting degraded for %s — %s",
                    strategy.deployment_id,
                    post_outcome.degraded_reason or "unknown",
                )
    finally:
        runner._last_cycle_id = saved_last_cycle_id
        if saved_ctx_cycle_id is None:
            clear_cycle_id()
        else:
            set_cycle_id(saved_ctx_cycle_id)

    return result


# =============================================================================
# Phase 10a: result display
# =============================================================================


def display_teardown_result(
    result: TeardownResult,
    deployment_id: str,
    no_op_message_builder,
) -> None:
    """Print the post-execution summary. ``no_op_message_builder`` is the
    canonical no-op message factory (passed in to avoid an import cycle
    with the parent ``teardown`` module).

    VIB-3705: when ``TeardownManager`` returns ``intents_total=0`` because
    ``generate_teardown_intents()`` returned an empty list (Branch 2 of
    the "nothing to do" taxonomy), we surface the canonical no-op log so
    QA harnesses can distinguish it from an executed teardown.
    """
    click.echo("\n" + "=" * 60)
    if result.success and result.intents_total == 0:
        no_op_msg = no_op_message_builder(deployment_id)
        click.secho(no_op_msg, fg="green")
        logger.info(no_op_msg)
    elif result.success:
        click.echo(click.style("[SUCCESS] Teardown completed successfully!", fg="green"))
    else:
        click.echo(click.style(f"[FAILED] Teardown failed: {result.error}", fg="red"))

    click.echo(f"  Duration: {result.duration_seconds:.1f}s")
    click.echo(f"  Intents executed: {result.intents_succeeded}/{result.intents_total}")
    if result.intents_failed > 0:
        click.echo(f"  Intents failed: {result.intents_failed}")
    click.echo(f"  Starting value: ${result.starting_value_usd:,.2f}")
    click.echo(f"  Final value: ${result.final_value_usd:,.2f}")
    click.echo(f"  Total costs: ${result.total_costs_usd:,.2f}")
    click.echo("=" * 60)


# =============================================================================
# Phase 10b: VIB-3920 — teardown_requests lifecycle update
# =============================================================================


def update_teardown_requests_lifecycle(
    deployment_id: str,
    mode: str,
    result: TeardownResult,
    state_manager_provider,
) -> None:
    """Record the execute-lane teardown lifecycle in ``teardown_requests``.

    VIB-3920: pre-fix the ``execute`` lane bypassed the request table
    entirely (the table was only populated by the request-lane CLI /
    dashboard), making ``teardown_requests.status`` and
    ``positions_closed`` always ``NULL/0`` for direct-execute runs even
    when the on-chain teardown closed real positions. The dashboard's
    Teardown tab and §1.2 G5 ship gate read from this table, so the
    bypass produced phantom-stuck rows.

    ``state_manager_provider`` is a zero-arg callable that returns the
    teardown state manager — passed in to avoid an import cycle with the
    parent ``teardown`` module's ``_get_teardown_state_manager_or_die``.

    Failures are swallowed (logged at debug) so a bookkeeping miss never
    blocks the CLI exit code — the teardown itself already succeeded
    when this runs.
    """
    try:
        # Local alias-imports to avoid shadowing the outer-scope
        # ``TeardownMode`` reference in the parent module.
        from ..teardown.models import TeardownAssetPolicy as _TAP
        from ..teardown.models import TeardownMode as _TM
        from ..teardown.models import TeardownRequest as _TR
        from ..teardown.models import TeardownStatus as _TS

        tsm = state_manager_provider()
        existing = tsm.get_active_request(deployment_id)
        if existing is None:
            # Create-then-mark to keep the lifecycle queryable. Use the
            # asset_policy default; the execute lane doesn't surface
            # asset-routing knobs at the CLI level (those live on the
            # request lane), so the safe default mirrors the request-
            # lane DEFAULTS.
            existing = _TR(
                deployment_id=deployment_id,
                mode=_TM(mode),
                asset_policy=_TAP.TARGET_TOKEN,
                target_token="USDC",
                requested_by="cli-execute",
                reason="execute_teardown CLI invocation",
                positions_total=result.positions_total if result.has_position_breakdown else result.intents_total,
            )
            tsm.create_request(existing)
        # VIB-5085: ``positions_*`` columns must count positions, not intents.
        # ``execute()`` stamps the verified position breakdown onto the result;
        # when verification ran, use it. Otherwise fall back to the intent
        # counts (the pre-VIB-5085 behaviour) so the columns are never blank.
        if result.has_position_breakdown:
            existing.positions_total = max(existing.positions_total, result.positions_total)
            existing.positions_closed = result.positions_closed
            existing.positions_failed = max(result.positions_total - result.positions_closed, 0)
        else:
            existing.positions_total = max(existing.positions_total, result.intents_total)
            existing.positions_closed = result.intents_succeeded
            existing.positions_failed = result.intents_failed
        existing.completed_at = datetime.now(UTC)
        existing.status = _TS.COMPLETED if result.success else _TS.FAILED
        tsm.update_request(existing)
    except Exception:  # noqa: BLE001 — never block CLI exit on bookkeeping
        logger.debug(
            "failed to update teardown_requests post-execute",
            exc_info=True,
        )
