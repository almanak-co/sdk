"""Single source of truth for CLI network resolution (VIB-5920).

Before this module existed, three strategy-folder-scoped call sites each
re-implemented ``network or "mainnet"``:

* ``_run_gateway._setup_gateway`` — decides whether the *managed gateway*
  forks a chain (Anvil) or talks to a real mainnet RPC. The dangerous one:
  booting mainnet against a config written for an Anvil fork moves real money.
* ``_run_modes`` — feeds ``_build_runtime_config`` and the runtime components.
  (It no longer resolves: it *consumes* the gateway's answer. A second
  resolution could read a different config.json, because the runtime's config
  load can fall back to ``load_strategy_config(<ClassName>)`` →
  ``find_strategy_dir``.)
* ``teardown_helpers.setup_gateway`` — the teardown lane's gateway.

None of them read the strategy config's ``"network"`` key, even though the
``--network`` flag's own help text has always promised "Overrides config.json
'network' field". The key was decorative: ``almanak strat run -c
config-anvil.json`` (no flag) silently booted **mainnet**.

The three sites also disagreed with each other: ``run.py`` threaded the *raw*
``--network`` flag into the runtime bootstrap, so ``--anvil-port arbitrum=8545``
without ``--network`` produced ``gateway_network="anvil"`` but
``resolved_network="mainnet"``. Routing all three through :func:`resolve_network`
closes that split-brain.

Precedence (highest first):

1. ``--network`` flag (explicit operator intent).
2. ``--anvil-port`` inference — ``anvil_ports`` present and not ``--no-gateway``
   implies ``anvil`` (preserves the pre-existing ``_setup_gateway`` behaviour).
3. Strategy config ``"network"`` key — **local mode only**.
4. ``"mainnet"`` default.

Hosted mode (``ALMANAK_IS_HOSTED``) deliberately ignores the config key: the
hosted runtime launches every strategy with a bare ``almanak strat run
--no-gateway`` and the platform owns the network. 35 in-repo configs declare
``"network": "anvil"``; honouring them on hosted would be a breaking change
(and would fork production strategies onto a local Anvil that does not exist).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import click

logger = logging.getLogger(__name__)

#: Networks a strategy config may legally declare — mirrors the
#: ``click.Choice`` on the ``--network`` flag (``_run_options.py``).
VALID_CONFIG_NETWORKS: frozenset[str] = frozenset({"mainnet", "anvil"})

#: Where the resolved network came from. Call sites echo this so an operator
#: can see *why* they are on a given network.
NetworkSource = str  # one of: "flag" | "anvil-ports" | "config" | "default"


@dataclass(frozen=True)
class ResolvedNetwork:
    """The resolved network plus the precedence tier that produced it."""

    network: str
    source: NetworkSource

    @property
    def from_config(self) -> bool:
        """True when the strategy config's ``network`` key decided the value."""
        return self.source == "config"

    @property
    def operator_signalled(self) -> bool:
        """True when a human typed the intent on the command line.

        The managed gateway drops authentication (``allow_insecure=True``,
        ``auth_token=None``) on test networks for local-dev convenience. That
        posture is only safe when the operator *asked* for a fork on this
        invocation — ``--network anvil`` or ``--anvil-port``. A config file
        (which is copied between repos, committed, and shared) must never be
        able to silently disarm the gateway that may hold the real
        ``ALMANAK_PRIVATE_KEY``, so config-sourced ``anvil`` keeps auth on
        (the CLI mints a session token and hands it to its own client — zero
        operator cost).
        """
        return self.source in ("flag", "anvil-ports")


def _config_network_raw(strategy_config: Mapping[str, Any] | None) -> Any:
    """Return the raw ``network`` value from a strategy config (or ``None``)."""
    if not strategy_config:
        return None
    try:
        return strategy_config.get("network")
    except AttributeError:  # pragma: no cover - defensive: non-Mapping input
        return None


def _normalized_config_network(raw: Any) -> str | None:
    """Validate + normalize a config ``network`` value.

    Returns ``None`` when the key is absent or blank (treated as unset).
    Raises ``click.ClickException`` for a non-string or an unrecognized value —
    a typo like ``"anvi"`` must fail LOUD rather than silently booting mainnet
    against a config written for a fork.
    """
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise click.ClickException(
            f"Strategy config 'network' must be a string, got {type(raw).__name__} ({raw!r}). "
            f"Valid values: {', '.join(sorted(VALID_CONFIG_NETWORKS))}."
        )
    normalized = raw.strip().casefold()
    if not normalized:
        # Empty / whitespace-only is "unset", not an error.
        return None
    if normalized not in VALID_CONFIG_NETWORKS:
        raise click.ClickException(
            f"Strategy config 'network' has an unrecognized value {raw!r}. "
            f"Valid values: {', '.join(sorted(VALID_CONFIG_NETWORKS))}. "
            f"{_chain_confusion_hint(normalized)}"
            "Fix the config, or pass --network explicitly to override it."
        )
    return normalized


def _chain_confusion_hint(normalized: str) -> str:
    """Extra guidance when a ``network`` value is really a *chain* name.

    ``"network": "base"`` is the most likely authoring mistake: the two keys
    look interchangeable but answer different questions — ``network`` selects
    mainnet vs a local fork, ``chain`` / ``chains`` select where the strategy
    trades. Registry-derived so a newly added chain is covered automatically.
    """
    try:
        from .chain_resolution import cli_chain_choices

        known_chains = {c.casefold() for c in cli_chain_choices()}
    except Exception:  # pragma: no cover - registry import must never mask the real error
        return ""
    if normalized not in known_chains:
        return ""
    return (
        f"'network' selects mainnet vs a local fork, not where the strategy trades — "
        f"use 'chain' / 'chains' for {normalized!r}. "
    )


def _warn_hosted_config_network_ignored(raw: Any) -> None:
    """Tell the operator the hosted platform ignores a config ``network`` key.

    Only fires for a config that declares ``anvil`` — a hosted pod that
    *thought* it was on a fork is the misunderstanding worth surfacing. Never
    raises: hosted boot must not fail on a decorative key.
    """
    if not isinstance(raw, str) or raw.strip().casefold() != "anvil":
        return
    message = (
        "Strategy config declares network='anvil' but this is a HOSTED deployment — "
        "the config 'network' key is ignored; the platform controls the network."
    )
    logger.warning(message)
    click.echo(f"Note: {message}")


def resolve_network(
    *,
    flag_network: str | None,
    anvil_ports_present: bool = False,
    no_gateway: bool = False,
    strategy_config: Mapping[str, Any] | None = None,
    config_loader: Callable[[], Mapping[str, Any] | None] | None = None,
) -> ResolvedNetwork:
    """Resolve the effective network for a strategy-folder-scoped CLI command.

    Args:
        flag_network: the ``--network`` CLI flag value (``None`` when unset).
        anvil_ports_present: whether ``--anvil-port CHAIN=PORT`` was supplied.
        no_gateway: whether ``--no-gateway`` was supplied. Disables the
            ``--anvil-port`` inference (the managed-gateway-only shortcut).
        strategy_config: the loaded strategy config dict, when the call site
            has one. Pass ``None`` when no config is in scope.
        config_loader: alternative to ``strategy_config`` for call sites that
            do not have the config loaded yet — invoked **only** when the
            higher-precedence tiers did not already decide, so a call site
            that never consults the config never pays for a file read (and a
            malformed config cannot newly break a path that previously never
            parsed it).

    Returns:
        A :class:`ResolvedNetwork` carrying the network and its source tier.

    Raises:
        click.ClickException: the config declares a ``network`` value that is
            not a string, or is not one of :data:`VALID_CONFIG_NETWORKS`
            (local mode only — hosted ignores the key entirely).
    """
    from almanak.framework.deployment import is_local

    if flag_network:
        return ResolvedNetwork(network=flag_network.strip().casefold(), source="flag")

    if anvil_ports_present and not no_gateway:
        return ResolvedNetwork(network="anvil", source="anvil-ports")

    if not is_local():
        # Hosted never consults the config key, so never invoke the loader
        # either — a hosted boot must not newly parse (and newly fail on) a
        # config file it previously never read. Only an already-loaded config
        # is inspected, purely to surface the ignored-key notice.
        _warn_hosted_config_network_ignored(_config_network_raw(strategy_config))
        return ResolvedNetwork(network="mainnet", source="default")

    if strategy_config is None and config_loader is not None:
        strategy_config = config_loader()

    config_network = _normalized_config_network(_config_network_raw(strategy_config))
    if config_network:
        return ResolvedNetwork(network=config_network, source="config")

    return ResolvedNetwork(network="mainnet", source="default")
