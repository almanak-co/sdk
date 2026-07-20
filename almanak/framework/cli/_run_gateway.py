"""``almanak strat run`` -- gateway bootstrap, chain resolution, wallet isolation, cleanup.

Split from run_helpers.py; import via the run_helpers facade externally.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click

from ._run_context import ComponentBundle
from ._run_setup import _anchor_strategy_folder_env, _runtime_private_key_override

if TYPE_CHECKING:
    from ..strategies.intent_strategy import IntentStrategy
    from ._network_resolution import ResolvedNetwork

logger = logging.getLogger(
    "almanak.framework.cli.run_helpers"
)  # pinned: tests + operator filters key on the historical module path


def _normalize_quick_chains(raw: Any) -> list[str]:
    """Normalize a quick-config ``chains`` value into a list of chain names.

    User-authored config files may set ``chains`` to a single string scalar
    (``chains: arbitrum``) or a list (``chains: [arbitrum, base]``). Anything
    else — an int, a dict, None — is treated as "no chains specified" rather
    than blindly coerced. In particular, ``list({"base": 1})`` yields
    ``["base"]``, which would silently use dict keys as chain names; we
    reject that case explicitly.

    Args:
        raw: The raw value read from the config's ``chains`` field.

    Returns:
        A list of chain names, or an empty list if ``raw`` is neither a
        string nor a list.
    """
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, list):
        return [str(chain) for chain in raw]
    return []


def _normalize_anvil_funding(raw: Any) -> dict[str, int | float | str]:
    """Normalize a quick-config ``anvil_funding`` value into a safe dict (VIB-3876).

    User-authored config files may set ``anvil_funding`` to a malformed value
    — a list (``anvil_funding: [WETH, USDC]``), a string, an int — all of
    which would propagate to ``ManagedGateway._anvil_funding`` and then crash
    inside ``_fund_anvil_wallets()`` on ``.items()``. Validate the shape
    here (a dict of ``{token_symbol: amount}``) and emit a warning + safe
    fallback for anything else, rather than letting the gateway boot fail
    with an opaque ``AttributeError`` mid-startup.

    Token amounts may be ints, floats, or strings (the gateway accepts strings
    for high-precision Decimal values like wstETH); other value types
    (booleans, dicts, lists) are dropped with a warning.

    Args:
        raw: The raw value read from the config's ``anvil_funding`` field.

    Returns:
        A ``{token_symbol: amount}`` dict suitable for ``ManagedGateway``,
        or an empty dict if ``raw`` is malformed.
    """
    if not isinstance(raw, dict):
        if raw not in (None, {}):
            logger.warning(
                "Ignoring malformed anvil_funding (%s); expected dict[str, int | float | str], got %r",
                type(raw).__name__,
                raw,
            )
        return {}
    cleaned: dict[str, int | float | str] = {}
    for key, value in raw.items():
        if not isinstance(key, str):
            logger.warning(
                "Ignoring anvil_funding entry with non-string key %r (value=%r)",
                key,
                value,
            )
            continue
        if isinstance(value, bool) or not isinstance(value, int | float | str):
            # bool is a subclass of int — reject it explicitly so True/False
            # don't silently get treated as 1/0 fund amounts.
            logger.warning(
                "Ignoring anvil_funding[%s] — expected int | float | str, got %s (value=%r)",
                key,
                type(value).__name__,
                value,
            )
            continue
        cleaned[key] = value
    return cleaned


# Chains that run on solana-test-validator, not Anvil. Filtered out of
# ``anvil_chains`` so ``ManagedGateway`` doesn't try to start a RollingForkManager
# for them; tracked separately via ``solana_anvil_needed`` so the caller can wire
# up ``SolanaForkManager`` (run_helpers.py:1837-1884 / cli/teardown.py VIB-3878).
NON_ANVIL_CHAINS: frozenset[str] = frozenset({"solana"})


def _resolve_anvil_chain_dispatch(
    network: str,
    primary_chain: str | None,
    config_dict: dict[str, Any],
) -> tuple[list[str], bool]:
    """Decide which forks the teardown CLI must start (VIB-3878).

    Returns ``(anvil_chains, solana_anvil_needed)``:

    - ``anvil_chains`` — EVM chains to pass to ``ManagedGateway(anvil_chains=...)``.
      Empty when ``network != "anvil"`` or when the strategy is Solana-only.
    - ``solana_anvil_needed`` — ``True`` when ``--network anvil`` and the
      strategy declares a Solana chain (single ``chain`` field or in ``chains``
      list). Caller is responsible for spinning up a ``SolanaForkManager``.

    The two flags are independent; multi-chain strategies (e.g. EVM + Solana)
    set both. Mirrors ``run_helpers.py:907-910`` so ``strat run`` and
    ``strat teardown`` agree on which forks to start.
    """
    if network != "anvil":
        return [], False

    chains_val = config_dict.get("chains")
    if isinstance(chains_val, str | list):
        all_chains = _normalize_quick_chains(chains_val)
    else:
        if chains_val is not None:
            # Malformed config (int, dict, None-equivalent): warn and fall
            # back to ``primary_chain`` rather than silently starting nothing.
            # CodeRabbit P2 — turning a recoverable typo into a "start nothing"
            # path is exactly the silent-failure VIB-3819 was fixing for.
            logger.warning(
                "Ignoring malformed config['chains'] (%s); falling back to primary chain. value=%r",
                type(chains_val).__name__,
                chains_val,
            )
        all_chains = [primary_chain] if primary_chain else []

    # Normalize once: strip whitespace + lowercase. Otherwise a config like
    # ``chains: [" solana "]`` would slip past the NON_ANVIL_CHAINS filter and
    # the solana_anvil_needed check, sending "solana" to ``ManagedGateway`` as
    # an Anvil fork target (which would fail). CodeRabbit P_minor.
    normalized_chains = [str(c).strip().lower() for c in all_chains]
    anvil_chains = [c for c in normalized_chains if c not in NON_ANVIL_CHAINS]
    solana_anvil_needed = "solana" in normalized_chains
    return anvil_chains, solana_anvil_needed


def _validate_no_gateway_flags(
    *, no_gateway: bool, anvil_ports: tuple[str, ...], wallet: str, keep_anvil: bool
) -> None:
    """Raise ClickException for `--no-gateway` flag combinations that need a managed gateway."""
    if not no_gateway:
        return
    if anvil_ports:
        raise click.ClickException("--anvil-port requires a managed gateway (remove --no-gateway).")
    if keep_anvil:
        raise click.ClickException("--keep-anvil requires a managed gateway (remove --no-gateway).")
    if wallet == "isolated":
        raise click.ClickException(
            "--wallet isolated requires a managed gateway (remove --no-gateway). "
            "The managed gateway auto-funds the derived wallet on Anvil."
        )


def _attach_external_gateway(
    *,
    effective_host: str,
    gateway_port: int,
    gateway_network: str,
    runtime_private_key: str | None,
) -> tuple[Any, Any, str, int, str, str | None, str | None, type[IntentStrategy[Any]] | None]:
    """Connect to an existing gateway (`--no-gateway` path) and return the standard tuple."""
    from ..gateway_client import GatewayClient, GatewayClientConfig

    click.echo(f"Connecting to existing gateway at {effective_host}:{gateway_port}...")
    # Read the typed gateway auth token with fallback to the legacy unprefixed
    # GATEWAY_AUTH_TOKEN. Both flow through the config service, but
    # we narrow to the gateway + cli factories rather than ``load_config()``
    # so an unrelated submodel validation error (e.g. malformed
    # ``ANVIL_*_PORT``) cannot block ``--no-gateway`` startup (PR #2152 review).
    from almanak.config import cli_runtime_config_from_env
    from almanak.config.env import gateway_config_from_env

    auth_token = gateway_config_from_env().auth_token or cli_runtime_config_from_env().legacy_gateway_auth_token
    gateway_config = GatewayClientConfig(host=effective_host, port=gateway_port, auth_token=auth_token)
    gateway_client = GatewayClient(gateway_config)
    gateway_client.connect()

    click.echo("Waiting for gateway to become ready...")
    if not gateway_client.wait_for_ready(timeout=60.0, interval=5.0):
        gateway_client.disconnect()
        click.echo()
        click.secho("ERROR: Gateway is not running or not healthy", fg="red", bold=True)
        click.echo()
        click.echo("The gateway sidecar is required for all strategy operations.")
        click.echo("Start the gateway first with:")
        click.echo()
        click.echo("  almanak gateway")
        click.echo()
        raise click.ClickException(f"Gateway not available at {effective_host}:{gateway_port}")

    click.secho(f"Connected to existing gateway at {effective_host}:{gateway_port}", fg="green")
    # No managed gateway here, so no isolated-wallet path runs; whatever
    # the caller plumbed in (or the contextvar) is already the right key
    # for `_build_runtime_config` to read via the ContextVar. Mirror it
    # explicitly so direct `_setup_gateway` callers (test paths) that
    # set ``runtime_private_key=…`` see it propagated downstream too.
    if runtime_private_key is not None:
        _runtime_private_key_override.set(runtime_private_key)
    return (gateway_client, None, effective_host, gateway_port, gateway_network, None, None, None)


def _find_available_gateway_port_or_raise(effective_host: str, gateway_port: int) -> int:
    """Wrap `find_available_gateway_port` with the user-facing error UX."""
    from almanak.gateway.managed import find_available_gateway_port

    try:
        return find_available_gateway_port(effective_host, gateway_port)
    except RuntimeError as e:
        click.echo()
        click.secho(f"ERROR: {e}", fg="red", bold=True)
        click.echo()
        click.echo("Set a specific port with:")
        click.echo()
        click.echo("  almanak strat run --gateway-port <port>")
        click.echo()
        click.echo("Or connect to an existing gateway:")
        click.echo()
        click.echo("  almanak strat run --no-gateway --gateway-port <port>")
        click.echo()
        raise click.ClickException(str(e)) from None


def _parse_anvil_port_overrides(anvil_ports: tuple[str, ...]) -> dict[str, int]:
    """Parse `--anvil-port CHAIN=PORT` entries, raising ClickException on malformed input."""
    parsed: dict[str, int] = {}
    for entry in anvil_ports:
        if "=" not in entry:
            raise click.ClickException(
                f"Invalid --anvil-port format: '{entry}'. Expected CHAIN=PORT (e.g., arbitrum=8545)"
            )
        chain_name, port_str = entry.split("=", 1)
        chain_name = chain_name.strip().lower()
        if not chain_name:
            raise click.ClickException(f"Invalid --anvil-port format: '{entry}'. Chain name cannot be empty.")
        try:
            port = int(port_str)
        except ValueError:
            raise click.ClickException(f"Invalid port in --anvil-port: '{port_str}'") from None
        if not (1 <= port <= 65535):
            raise click.ClickException(f"Invalid port in --anvil-port: '{port_str}'. Expected 1-65535.")
        if chain_name in parsed:
            raise click.ClickException(f"Duplicate --anvil-port for chain '{chain_name}'.")
        parsed[chain_name] = port
    return parsed


def _early_load_strategy_class(working_dir: str) -> type[IntentStrategy[Any]] | None:
    """Eager-load strategy.py so decorator metadata is available before gateway startup."""
    from .intent_debug import load_strategy_from_file

    strategy_file = Path(working_dir) / "strategy.py"
    if not strategy_file.exists():
        return None
    cls, err = load_strategy_from_file(strategy_file)
    if err:
        logger.debug(f"Early strategy load failed (will retry later): {err}")
    return cls


def _resolve_quick_config_path(working_dir: str, config_file: str | None) -> Path | None:
    """Pick the explicit `--config` path or auto-discover config.json/yaml/yml in `working_dir`."""
    if config_file:
        return Path(config_file)
    for name in ("config.json", "config.yaml", "config.yml"):
        candidate = Path(working_dir) / name
        if candidate.exists():
            return candidate
    return None


def _load_quick_config(working_dir: str, config_file: str | None) -> dict[str, Any] | None:
    """Load + schema-validate the strategy config for a pre-boot peek (or ``None``).

    Same shared validated parse the Anvil / mainnet chain probes use (#2101) —
    ``warn_unknown_keys=False`` so the typo warning still fires exactly once at
    the canonical load.

    The ``ALMANAK_STRATEGY_CONFIG`` env override IS applied (codex P1, VIB-5920):
    the canonical loader deep-merges it unconditionally, so a peek that read only
    the on-disk file would resolve the gateway network (and the Anvil
    chain/funding probes) from a DIFFERENT config than the runtime bootstrap —
    recreating the exact gateway-vs-runtime split brain this module exists to
    close (gateway on mainnet while the runtime believes it is on a fork).
    ``echo=False`` so the "Applied … env override" notice still prints exactly
    once, at the canonical load.
    """
    from .run import _apply_env_strategy_config_override, parse_strategy_config_file

    config_path = _resolve_quick_config_path(working_dir, config_file)
    if not config_path or not config_path.exists():
        return None
    config = parse_strategy_config_file(config_path, warn_unknown_keys=False)
    if config is None:
        return None
    return _apply_env_strategy_config_override(config, echo=False)


def _quick_config_loader(working_dir: str, config_file: str | None) -> Callable[[], dict[str, Any] | None]:
    """Return a memoized zero-arg loader so one gateway setup parses the config at most once."""
    cache: list[dict[str, Any] | None] = []

    def _load() -> dict[str, Any] | None:
        if not cache:
            cache.append(_load_quick_config(working_dir, config_file))
        return cache[0]

    return _load


def _resolve_gateway_network(
    *,
    network: str | None,
    anvil_ports: tuple[str, ...],
    no_gateway: bool,
    load_quick_config: Callable[[], dict[str, Any] | None],
) -> ResolvedNetwork:
    """Resolve the network for the whole `strat run` process (VIB-5920).

    This is the ONLY network resolution in the run lane: the runtime bootstrap
    (``_run_modes``) consumes the value produced here rather than re-resolving,
    because a second resolution can legitimately see a *different* config —
    ``_run_setup`` falls back to ``load_strategy_config(<ClassName>)``, which
    resolves a strategy directory the gateway probe never looked at.

    Returns the full :class:`ResolvedNetwork` (network **and** source): the
    source decides the gateway's auth posture (see ``operator_signalled``), so
    it must not be discarded here.

    The config is read lazily — only when neither the flag nor the
    ``--anvil-port`` inference already decided, and only in local mode. That
    keeps hosted boot byte-identical (hosted ignores the key and never invokes
    the loader). It does mean a **local** ``--no-gateway`` run now parses the
    config here where it previously did not; a malformed config therefore
    fails at gateway setup instead of a few steps later at the canonical load,
    with the same ``ClickException`` naming the file.
    """
    from ._network_resolution import resolve_network

    return resolve_network(
        flag_network=network,
        anvil_ports_present=bool(anvil_ports),
        no_gateway=no_gateway,
        config_loader=load_quick_config,
    )


def _echo_resolved_network(resolved: ResolvedNetwork) -> None:
    """Announce an *implicitly* resolved network before anything is started.

    Only fires when the operator did not type the network on this invocation
    (config-sourced or ``--anvil-port``-inferred). Printed here — before the
    gateway boots and before any Anvil fork starts — because this is the point
    of no return for the auth posture and the fork lifecycle; the runtime
    banner in ``_run_modes`` prints much later.
    """
    if resolved.source == "config":
        click.echo(f"Network: {resolved.network.upper()} (resolved from config.json 'network')")
    elif resolved.source == "anvil-ports":
        click.echo(f"Network: {resolved.network.upper()} (inferred from --anvil-port)")


def _chains_from_quick_config(quick_config: dict[str, Any]) -> list[str]:
    """Extract `chains` (preferred) or `chain` from a quick-config dict."""
    chains_val = quick_config.get("chains")
    if chains_val is not None:
        # Normalize via helper: wraps strings into single-element lists,
        # coerces lists to list[str], and ignores non-str/non-list values
        # (ints, dicts) rather than silently using dict keys as chains.
        return _normalize_quick_chains(chains_val)
    chain_val = quick_config.get("chain")
    # Treat non-string scalars as absent so a malformed `chain: 123` doesn't
    # blow up `c.lower()` downstream — the real config loader will surface a
    # proper error later.
    if isinstance(chain_val, str) and chain_val.strip():
        return [chain_val]
    return []


def _resolve_anvil_chains_and_funding(
    *,
    working_dir: str,
    config_file: str | None,
    early_strategy_class: type[IntentStrategy[Any]] | None,
    external_anvil_ports: dict[str, int],
    quick_config: dict[str, Any] | None = None,
) -> tuple[list[str], dict[str, float | int | str]]:
    """Resolve EVM chains needing Anvil forks (and their funding) for `--network anvil`.

    ``quick_config`` lets a caller that already parsed the config (VIB-5920's
    network resolution) hand it in so the file is parsed once per gateway
    setup; when omitted the config is loaded here as before.
    """
    # Import get_default_chain lazily from .run to avoid circular-import.
    from .run import get_default_chain

    anvil_chains: list[str] = []
    anvil_funding: dict[str, float | int | str] = {}

    # Parse + schema-validate ONCE through the shared loader (#2101). A
    # malformed config fails fast with a ``click.ClickException`` naming the
    # file + line, instead of being swallowed into the misleading "no chain
    # found" warning below while the real error surfaces only later in the
    # runner. ``warn_unknown_keys=False`` so the typo warning fires exactly
    # once, at the canonical load — not on this pre-boot peek. The
    # ``ALMANAK_STRATEGY_CONFIG`` env override IS applied inside
    # ``_load_quick_config`` (VIB-5920 codex P1) so the Anvil chain/funding
    # probe sees the same merged config the runtime will use.
    if quick_config is None:
        quick_config = _load_quick_config(working_dir, config_file)
    if quick_config is not None:
        anvil_chains = _chains_from_quick_config(quick_config)
        anvil_funding = _normalize_anvil_funding(quick_config.get("anvil_funding", {}))

    # Fall back to decorator metadata if config.json has no chain
    if not anvil_chains and early_strategy_class:
        decorator_chain = get_default_chain(early_strategy_class)
        if decorator_chain:
            anvil_chains = [decorator_chain]

    # Add externally-specified chains to anvil_chains if not already present
    for ext_chain in external_anvil_ports:
        if ext_chain not in anvil_chains:
            anvil_chains.append(ext_chain)

    # Solana uses solana-test-validator (not Anvil); filter it out so
    # ManagedGateway doesn't try to start a RollingForkManager for it.
    # The Solana fork is started separately below in the runner setup.
    NON_EVM_CHAINS = {"solana"}
    evm_anvil_chains = [c for c in anvil_chains if c.lower() not in NON_EVM_CHAINS]
    solana_anvil = any(c.lower() in NON_EVM_CHAINS for c in anvil_chains)

    if not evm_anvil_chains and not solana_anvil:
        click.echo(
            "Warning: --network anvil specified but no chain found in config or decorator. "
            "Gateway will start without Anvil forks."
        )

    return evm_anvil_chains, anvil_funding


def _resolve_gateway_chains_for_mainnet(
    *,
    working_dir: str,
    config_file: str | None,
    early_strategy_class: type[IntentStrategy[Any]] | None,
    quick_config: dict[str, Any] | None = None,
) -> list[str]:
    """Resolve the chain list passed to the gateway for non-anvil networks.

    ``quick_config`` lets the caller hand in an already-parsed config (see
    ``_resolve_anvil_chains_and_funding``).
    """
    from .run import get_default_chain

    chains: list[str] = []
    # Same shared validated parse as the Anvil probe (#2101): one parse,
    # fail-fast with a file-naming error rather than swallowing. Probe peek
    # only — no typo warning; the ``ALMANAK_STRATEGY_CONFIG`` env override IS
    # applied inside ``_load_quick_config`` (VIB-5920 codex P1).
    if quick_config is None:
        quick_config = _load_quick_config(working_dir, config_file)
    if quick_config is not None:
        chains = _chains_from_quick_config(quick_config)

    # Fall back to decorator metadata if config has no chain
    if not chains and early_strategy_class:
        decorator_chain = get_default_chain(early_strategy_class)
        if decorator_chain:
            chains = [decorator_chain]
    return chains


def _derive_isolated_wallet_or_none(
    *,
    wallet: str,
    gateway_network: str,
    working_dir: str,
    runtime_private_key: str | None = None,
) -> tuple[str | None, str | None]:
    """Derive an isolated wallet+key for `--wallet isolated`; otherwise return (None, None).

    Resolution order for the master key: caller-plumbed ``runtime_private_key``
    (or its contextvar fallback set by `almanak strat test`) > the typed
    ``GatewayConfig.private_key`` (populated from
    ``ALMANAK_PRIVATE_KEY`` via the ``_apply_gateway_env_fallbacks`` ladder).
    Honours the kwarg-first, no-env signing-key plumbing the rest of
    ``_setup_gateway`` documents (#2100).
    """
    if wallet != "isolated":
        return None, None
    if gateway_network != "anvil":
        raise click.ClickException("--wallet isolated is only supported with --network anvil")

    from almanak.gateway.managed import derive_isolated_wallet

    master_key = runtime_private_key
    if master_key is None:
        master_key = _runtime_private_key_override.get()
    if not master_key:
        # Read through the typed gateway config rather than directly
        # off ``os.environ`` so the config-boundary lint stays clean. The
        # ``_apply_gateway_env_fallbacks`` ladder still honours
        # ``ALMANAK_PRIVATE_KEY``.
        from almanak.config.env import gateway_config_from_env

        master_key = gateway_config_from_env().private_key or ""
    if not master_key:
        raise click.ClickException(
            "--wallet isolated requires a private key — pass `runtime_private_key=...` or set ALMANAK_PRIVATE_KEY"
        )
    # Use the strategy directory name as the derivation seed
    strategy_seed = Path(working_dir).resolve().name
    derived_key, isolated_wallet_address = derive_isolated_wallet(master_key, strategy_seed)
    # The derived key is plumbed via the runtime-config kwarg + gateway
    # private_key argument below — no os.environ mutation (#2100).
    click.echo(
        f"Wallet: isolated ({isolated_wallet_address[:10]}...{isolated_wallet_address[-4:]}) "
        f"[derived from strategy '{strategy_seed}']"
    )
    return isolated_wallet_address, derived_key


def _resolve_signing_key(*, isolated_wallet_private_key: str | None, runtime_private_key: str | None) -> str | None:
    """Resolve the effective signing key and publish it on `_runtime_private_key_override` (#2100).

    Order: derived isolated-wallet key > caller-plumbed `runtime_private_key` kwarg
    (or its contextvar fallback set by `almanak strat test`) > None for env fallback.
    GatewaySettings reads ALMANAK_GATEWAY_PRIVATE_KEY (not ALMANAK_PRIVATE_KEY) for its
    prefixed env source, so an explicit kwarg is the only way to feed it the unprefixed
    ALMANAK_PRIVATE_KEY equivalent the CLI just resolved. The resolved value is published
    on `_runtime_private_key_override` so the downstream `_build_runtime_config` reads
    the same key — no extra return-tuple slot needed (which would have flagged every
    line of the destructure inside `run` against the CRAP gate).
    """
    if runtime_private_key is None:
        runtime_private_key = _runtime_private_key_override.get()
    effective = isolated_wallet_private_key or runtime_private_key
    if effective is not None:
        _runtime_private_key_override.set(effective)
    return effective


def _build_gateway_settings(
    *,
    effective_host: str,
    gateway_port: int,
    gateway_network: str,
    gateway_chains: list[str],
    gateway_private_key: str | None,
    operator_signalled_network: bool = True,
) -> tuple[Any, str | None]:
    """Assemble `gateway_kwargs` and call `gateway_config_from_env`; returns (settings, session_token).

    ``operator_signalled_network`` (VIB-5920): the unauthenticated test-network
    posture below is a *local-dev convenience* that must stay tied to an
    explicit operator signal (``--network anvil`` / ``--anvil-port``). When the
    network came from a config file instead, the gateway still boots with a
    random session token and ``allow_insecure=False`` — a committed / copied
    ``"network": "anvil"`` must not be able to disarm a gateway that may hold
    the real ``ALMANAK_PRIVATE_KEY``. Costs the operator nothing: the CLI owns
    both ends and hands the token to its own client. Defaults to ``True`` so
    non-VIB-5920 callers keep the historical behaviour.
    """
    import uuid

    from almanak.config.env import gateway_config_from_env

    # Security: generate a random session token for the managed gateway so it
    # is never running without authentication, even on mainnet (VIB-520).
    # For operator-requested anvil/sepolia we still use allow_insecure for
    # convenience (VIB-5920 scopes that to an explicit CLI signal).
    is_test_network = gateway_network in ("anvil", "sepolia") and operator_signalled_network
    session_auth_token = None if is_test_network else uuid.uuid4().hex

    gateway_kwargs: dict[str, Any] = {
        "grpc_host": effective_host,
        "grpc_port": gateway_port,
        "network": gateway_network,
        "allow_insecure": is_test_network,
        "metrics_enabled": False,
        "audit_enabled": False,
        "chains": gateway_chains,
    }
    # On operator-signalled test networks, force auth_token=None as an explicit
    # kwarg so it wins over any ALMANAK_GATEWAY_AUTH_TOKEN loaded from .env.
    # Without this, the server attaches AuthInterceptor while the client
    # (allow_insecure=True) sends no token, producing UNAUTHENTICATED on every
    # gRPC call (VIB-3032).
    if is_test_network:
        gateway_kwargs["auth_token"] = None
    elif session_auth_token:
        gateway_kwargs["auth_token"] = session_auth_token
    if gateway_private_key:
        gateway_kwargs["private_key"] = gateway_private_key
    # Route through the config service so unprefixed ALMANAK_* and
    # Polymarket-ladder fallbacks resolve identically to gateway boot.
    return gateway_config_from_env(**gateway_kwargs), session_auth_token


def _start_managed_gateway_and_connect(
    *,
    gateway_settings: Any,
    anvil_chains: list[str],
    isolated_wallet_address: str | None,
    anvil_funding: dict[str, float | int | str],
    external_anvil_ports: dict[str, int],
    keep_anvil: bool,
    effective_host: str,
    gateway_port: int,
    gateway_network: str,
    session_auth_token: str | None,
) -> tuple[Any, Any]:
    """Start the managed gateway, register atexit cleanup, and connect a client. Returns (client, managed)."""
    import atexit

    from almanak.gateway.managed import ManagedGateway

    from ..gateway_client import GatewayClient, GatewayClientConfig
    from ._anvil_timeout import compute_anvil_startup_timeout

    if anvil_chains:
        click.echo(
            f"Starting managed gateway on {effective_host}:{gateway_port} "
            f"(network={gateway_network}, anvil chains: {', '.join(anvil_chains)})..."
        )
    else:
        click.echo(f"Starting managed gateway on {effective_host}:{gateway_port} (network={gateway_network})...")
    managed_gateway = ManagedGateway(
        gateway_settings,
        anvil_chains=anvil_chains,
        wallet_address=isolated_wallet_address,
        anvil_funding=anvil_funding,
        external_anvil_ports=external_anvil_ports,
        keep_anvil=keep_anvil,
    )
    # Per-chain Anvil startup-timeout policy lives in _anvil_timeout.py
    # (VIB-3877) so the run + teardown CLI paths can never drift. See that
    # module for the cold-cache-fork-vs-gRPC-race rationale this guards.
    startup_timeout = compute_anvil_startup_timeout(anvil_chains)
    try:
        managed_gateway.start(timeout=startup_timeout)
    except RuntimeError as e:
        click.echo()
        click.secho(f"ERROR: Failed to start managed gateway: {e}", fg="red", bold=True)
        click.echo()
        raise click.ClickException("Managed gateway startup failed") from e

    # Register atexit handler as safety net for sys.exit() paths that skip cleanup
    atexit.register(managed_gateway.stop)

    # VIB-4047: persist the session token to a 0600 file sibling to the
    # folder-scoped DB so a SEPARATELY launched ``almanak dashboard`` (second
    # terminal) can authenticate against this gateway's ephemeral token. The
    # embedded ``--dashboard`` subprocess already gets it via env; this closes
    # the standalone-dashboard gap without an operator env-var dance. No-op in
    # hosted mode. Cleared on shutdown so a dead gateway's token can't linger.
    if session_auth_token:
        from almanak.framework.local_paths import (
            clear_gateway_session_token,
            write_gateway_session_token,
        )

        write_gateway_session_token(session_auth_token)
        atexit.register(clear_gateway_session_token)

    click.secho(f"Managed gateway started on {effective_host}:{gateway_port}", fg="green")

    # Connect client to the managed gateway (use same session token)
    gateway_config = GatewayClientConfig(host=effective_host, port=gateway_port, auth_token=session_auth_token)
    gateway_client = GatewayClient(gateway_config)
    gateway_client.connect()

    click.echo("Waiting for gateway to become ready...")
    if not gateway_client.wait_for_ready(timeout=60.0, interval=5.0):
        managed_gateway.stop()
        gateway_client.disconnect()
        raise click.ClickException(
            "Managed gateway started but health check failed. Check gateway logs above for details."
        )
    return gateway_client, managed_gateway


def _setup_gateway(
    *,
    working_dir: str,
    config_file: str | None,
    network: str | None,
    gateway_host: str,
    gateway_port: int,
    no_gateway: bool,
    anvil_ports: tuple[str, ...],
    wallet: str,
    keep_anvil: bool,
    reset_fork: bool,
    once: bool,
    runtime_private_key: str | None = None,
) -> tuple[Any, Any, str, int, str, str | None, str | None, type[IntentStrategy[Any]] | None]:
    """Set up the gateway (managed auto-start or connect to external).

    Orchestrates the gateway bootstrap that used to live inline in `run()`:
        - `--wallet isolated` derivation (returns a per-strategy derived
          key alongside the wallet address; the caller plumbs it into
          `_build_runtime_config` via the `runtime_private_key` kwarg
          instead of mutating `ALMANAK_PRIVATE_KEY`), with `--network anvil` guard.
        - `--anvil-port` CSV parsing into `external_anvil_ports`.
        - Early-load of the strategy class so decorator metadata is
          available for Anvil chain detection (runs before the later
          `_load_strategy_class` call in `run()`).
        - Anvil chain resolution from config.json/decorator + external ports.
        - `ALMANAK_GATEWAY_AUTH_TOKEN` / `GATEWAY_AUTH_TOKEN` /
          `ALMANAK_GATEWAY_ALLOW_INSECURE` handling.
        - Random session auth token for non-test-network managed gateways
          (VIB-520).
        - `find_available_gateway_port` and `ManagedGateway.start()` with
          anvil-appropriate timeouts.
        - atexit registration of `managed_gateway.stop` at the same point
          in the flow as the original.
        - `GatewayClient.wait_for_ready()` health wait.

    Args:
        working_dir: strategy directory (searched for strategy.py and config).
        config_file: explicit `--config` path (or None for auto-discovery).
        network: `--network` CLI flag (or None for default).
        gateway_host: `--gateway-host` (normalized to 127.0.0.1 if "localhost").
        gateway_port: `--gateway-port`.
        no_gateway: `--no-gateway` (connect to existing gateway instead of
            auto-starting managed).
        anvil_ports: tuple of CHAIN=PORT strings from `--anvil-port`.
        wallet: `--wallet` ("default" or "isolated").
        keep_anvil: `--keep-anvil` (skip Anvil teardown on exit).
        reset_fork: `--reset-fork` (Anvil-only).
        once: `--once` (used to suppress `--reset-fork` note in single-run).
        runtime_private_key: Optional caller-plumbed private key. When the
            ``--wallet isolated`` path does not run, this value (if non-None)
            is forwarded to the managed gateway as its ``private_key`` kwarg
            so the gateway sees the same key the runtime config will receive
            via ``_build_runtime_config``. Used by ``almanak strat test`` to
            inject ``ANVIL_DEFAULT_PRIVATE_KEY`` without mutating
            ``os.environ`` (#2100). The isolated-wallet derived key, when
            present, takes precedence over this argument because the
            funded-wallet identity must match what the gateway signs as.

    Returns:
        (gateway_client, managed_gateway, effective_host, gateway_port,
         gateway_network, session_auth_token, isolated_wallet_address,
         early_strategy_class).

        managed_gateway is None when `--no-gateway` was used.
        isolated_wallet_address is None unless `--wallet isolated` derived one.
        The precedence-resolved signing key (isolated-wallet derived key >
        caller-plumbed ``runtime_private_key`` > None for env fallback) is
        propagated to ``_build_runtime_config`` via the
        ``_runtime_private_key_override`` ContextVar (#2100). Returning it in
        the tuple kept the older helper contract but tripped the CRAP gate on
        every line of the destructure inside ``run`` — the ContextVar keeps
        the same end-to-end semantics with zero footprint in `run`.
        early_strategy_class is None if strategy.py wasn't present or failed
        to load (retried later by `_load_strategy_class`).
    """
    # VIB-3761: anchor ``ALMANAK_STRATEGY_FOLDER`` before any
    # downstream helper resolves a local path. Done here (not in ``run()``)
    # because every gateway-setup downstream — ``local_db_path``, the
    # managed gateway's SQLite store, ``_resolve_identity`` — reads the env
    # var; pinning it inside this allowlisted helper keeps the `run` body
    # free of CRAP-gated diff churn.
    _anchor_strategy_folder_env(working_dir)

    # Normalize "localhost" to "127.0.0.1" (gateway binds to 127.0.0.1)
    effective_host = "127.0.0.1" if gateway_host == "localhost" else gateway_host

    # Resolve network mode early because later runner setup uses the same value
    # for both managed and pre-existing gateway flows. VIB-5920: routed through
    # the shared resolver so the config's ``network`` key is honoured (local
    # mode) and all three CLI sites agree on the answer.
    load_quick_config = _quick_config_loader(working_dir, config_file)
    resolved_network = _resolve_gateway_network(
        network=network,
        anvil_ports=anvil_ports,
        no_gateway=no_gateway,
        load_quick_config=load_quick_config,
    )
    gateway_network = resolved_network.network
    _echo_resolved_network(resolved_network)

    # --wallet isolated requires a managed gateway (derives wallet + funds Anvil fork)
    if wallet == "isolated" and no_gateway:
        raise click.ClickException(
            "--wallet isolated requires a managed gateway (incompatible with --no-gateway). "
            "Remove --no-gateway to let the CLI start its own gateway + Anvil fork."
        )

    if no_gateway:
        _validate_no_gateway_flags(no_gateway=no_gateway, anvil_ports=anvil_ports, wallet=wallet, keep_anvil=keep_anvil)
        return _attach_external_gateway(
            effective_host=effective_host,
            gateway_port=gateway_port,
            gateway_network=gateway_network,
            runtime_private_key=runtime_private_key,
        )

    # Default: auto-start a managed gateway
    gateway_port = _find_available_gateway_port_or_raise(effective_host, gateway_port)
    external_anvil_ports = _parse_anvil_port_overrides(anvil_ports)

    if keep_anvil and gateway_network != "anvil":
        click.echo(
            'Warning: --keep-anvil has no effect off an Anvil network (--network anvil, --anvil-port, or config "network": "anvil").'
        )

    # Early-load strategy class so decorator metadata is available for chain detection.
    # This must happen before gateway startup so Anvil forks target the correct chain.
    early_strategy_class = _early_load_strategy_class(working_dir)

    # Determine which chains need Anvil forks
    anvil_chains: list[str] = []
    anvil_funding: dict[str, float | int | str] = {}
    if gateway_network == "anvil":
        anvil_chains, anvil_funding = _resolve_anvil_chains_and_funding(
            working_dir=working_dir,
            config_file=config_file,
            early_strategy_class=early_strategy_class,
            external_anvil_ports=external_anvil_ports,
            quick_config=load_quick_config(),
        )

    # Wallet isolation: derive a unique wallet per strategy on Anvil
    isolated_wallet_address, isolated_wallet_private_key = _derive_isolated_wallet_or_none(
        wallet=wallet,
        gateway_network=gateway_network,
        working_dir=working_dir,
        runtime_private_key=runtime_private_key,
    )

    # Validate --reset-fork requires an Anvil network (flag, --anvil-port, or
    # the config's "network": "anvil" — VIB-5920)
    if reset_fork and gateway_network != "anvil":
        raise click.ClickException(
            "--reset-fork is only supported on an Anvil network "
            '(pass --network anvil, or set "network": "anvil" in the strategy config)'
        )
    if reset_fork and once:
        click.echo("Note: --reset-fork has no effect with --once (fork is already fresh at startup)")

    gateway_private_key = _resolve_signing_key(
        isolated_wallet_private_key=isolated_wallet_private_key, runtime_private_key=runtime_private_key
    )

    # Ensure gateway knows the strategy's chain for on-chain pricing.
    # For anvil mode, anvil_chains is already populated above.
    # For mainnet, read chain from config or decorator metadata so the MarketService
    # uses the correct Chainlink oracle chain instead of defaulting to arbitrum.
    gateway_chains = anvil_chains or _resolve_gateway_chains_for_mainnet(
        working_dir=working_dir,
        config_file=config_file,
        early_strategy_class=early_strategy_class,
        quick_config=load_quick_config(),
    )

    gateway_settings, session_auth_token = _build_gateway_settings(
        effective_host=effective_host,
        gateway_port=gateway_port,
        gateway_network=gateway_network,
        gateway_chains=gateway_chains,
        gateway_private_key=gateway_private_key,
        operator_signalled_network=resolved_network.operator_signalled,
    )

    gateway_client, managed_gateway = _start_managed_gateway_and_connect(
        gateway_settings=gateway_settings,
        anvil_chains=anvil_chains,
        isolated_wallet_address=isolated_wallet_address,
        anvil_funding=anvil_funding,
        external_anvil_ports=external_anvil_ports,
        keep_anvil=keep_anvil,
        effective_host=effective_host,
        gateway_port=gateway_port,
        gateway_network=gateway_network,
        session_auth_token=session_auth_token,
    )

    return (
        gateway_client,
        managed_gateway,
        effective_host,
        gateway_port,
        gateway_network,
        session_auth_token,
        isolated_wallet_address,
        early_strategy_class,
    )


# ---------------------------------------------------------------------------
# Cleanup helper
# ---------------------------------------------------------------------------


def _build_cleanup_fn(
    *,
    gateway_client: Any,
    managed_gateway: Any,
    keep_anvil: bool,
    components: ComponentBundle,
) -> Callable[[], Coroutine[Any, Any, None]]:
    """Build an async cleanup closure that tears down all run-time resources.

    Returns a zero-arg async function that the caller awaits in the
    single-run and loop finally blocks. Preserves the original None-checks
    and hasattr-close checks exactly so that callers with partial component
    initialization don't blow up during shutdown.

    Args:
        gateway_client: The connected `GatewayClient` (or None).
        managed_gateway: Optional `ManagedGateway` handle (None when
            `--no-gateway` was used).
        keep_anvil: Preserve the `--keep-anvil` behavior: if the managed
            gateway owns Anvil forks, print their ports/PIDs before
            ``managed_gateway.stop()``.
        components: `ComponentBundle` carrying `ohlcv_provider`, `price_oracle`
            and `solana_fork_mgr` (any may be None).

    Returns:
        A coroutine factory. Call as ``await cleanup_fn()``.
    """

    async def cleanup_resources() -> None:
        if components.ohlcv_provider is not None:
            await components.ohlcv_provider.close()
        if components.price_oracle is not None and hasattr(components.price_oracle, "close"):
            await components.price_oracle.close()
        if gateway_client is not None:
            gateway_client.disconnect()
        if components.solana_fork_mgr is not None:
            try:
                await components.solana_fork_mgr.stop()
                click.echo("  Stopped solana-test-validator")
            except Exception as e:
                logger.debug(f"Failed to stop solana-test-validator: {e}")
        if managed_gateway is not None:
            if keep_anvil and managed_gateway._anvil_managers:
                for chain, mgr in managed_gateway._anvil_managers.items():
                    port = mgr.anvil_port
                    pid = mgr._process.pid if mgr._process else "unknown"
                    click.echo(f"Anvil for {chain} still running on port {port} (PID {pid})")
            managed_gateway.stop()

    return cleanup_resources
