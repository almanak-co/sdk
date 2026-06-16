"""Frozen re-export facade for `almanak strat run` wiring helpers.

This file is the **stable public surface** of the run-wiring package.  All
symbols that external callers (CLI entrypoints, tests, strategy demos) import
from ``almanak.framework.cli.run_helpers`` continue to resolve here unchanged.
Implementation has moved into five cohesive sub-modules:

- ``_run_setup.py``      -- logging, identity, config discovery, state management
- ``_run_gateway.py``    -- gateway bootstrap, chain/funding resolution, signing
- ``_run_components.py`` -- orchestrator/provider assembly, runtime config, copy-trading
- ``_run_dashboard.py``  -- dashboard subprocess lifecycle
- ``_run_modes.py``      -- execution modes (once / test-lifecycle / continuous)

``_instantiate_strategy`` and ``_intent_strategy_runtime`` remain in this file
because test suites patch ``run_helpers.inspect`` at module level; moving them
would silently break those patches.

Do not import from this facade inside the sub-modules -- import from the
sub-module that owns the symbol to avoid circular imports.  The facade is
import-only (no logic); keep it that way.
"""

from __future__ import annotations

import asyncio  # noqa: F401 — preserve pre-split module surface
import contextvars  # noqa: F401 — preserve pre-split module surface
import inspect  # noqa: F401 — tests monkeypatch run_helpers.inspect
import json  # noqa: F401 — preserve pre-split module surface
import logging
import sys
import time  # noqa: F401 — preserve pre-split module surface
from collections.abc import Callable, Coroutine  # noqa: F401 — preserve pre-split module surface
from pathlib import Path  # noqa: F401 — tests monkeypatch run_helpers.Path
from typing import TYPE_CHECKING, Any, NoReturn  # noqa: F401 — preserve pre-split module surface

import click

from almanak.config.cli_runtime import (  # noqa: F401 — preserve pre-split module surface
    almanak_chain_from_env,
    anvil_port_for_chain,
)

if TYPE_CHECKING:
    pass

# Preserve pre-split module surface: re-export _run_context dataclasses that
# were previously imported at the top of the flat run_helpers.py.
# LEGACY_COMPAT_DATA_REQUIREMENTS and StrategyDataRequirements live in
# _run_components; re-export here to preserve pre-split module surface.
from ._run_components import (  # noqa: F401
    LEGACY_COMPAT_DATA_REQUIREMENTS,
    StrategyDataRequirements,
)
from ._run_context import (  # noqa: F401
    ComponentBundle,
    IdentityInfo,
    ResumeInfo,
    RuntimeBootstrap,
    StrategyBootstrap,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Facade re-exports — dependency order: setup → gateway → components → dashboard → modes
# ---------------------------------------------------------------------------

# _run_setup: logging, identity, config discovery, state management
# _run_components: orchestrator/provider assembly, runtime config, copy-trading
from ._run_components import (  # noqa: F401
    _accept_anvil_default_wallet_or_exit,
    _apply_strategy_config_chain,
    _apply_strategy_config_wallet,
    _build_components,
    _build_orchestrator_and_providers,
    _build_runner,
    _build_runtime_config,
    _build_sidecar_runtime_config,
    _echo_local_env_help,
    _echo_multichain_env_help,
    _get_data_requirements,
    _init_copy_trading,
    _load_local_runtime_config,
    _load_multichain_runtime_config,
    _maybe_auto_deploy_vault,
    _reconciliation_confirmation_from_env,
    _reconciliation_enforcement_from_env,
    _register_chain_wallets,
    _resolve_effective_signing_key,
    _resolve_runtime_private_key_kwarg,
)

# _run_dashboard: dashboard subprocess lifecycle
from ._run_dashboard import (  # noqa: F401
    _build_dashboard_subprocess_env,
    _handle_standalone_dashboard,
    _open_dashboard_log,
    _start_dashboard_background,
    _stop_dashboard,
)

# _run_gateway: gateway bootstrap, chain/funding resolution, signing
from ._run_gateway import (  # noqa: F401
    NON_ANVIL_CHAINS,
    _attach_external_gateway,
    _build_cleanup_fn,
    _build_gateway_settings,
    _chains_from_quick_config,
    _derive_isolated_wallet_or_none,
    _early_load_strategy_class,
    _find_available_gateway_port_or_raise,
    _normalize_anvil_funding,
    _normalize_quick_chains,
    _parse_anvil_port_overrides,
    _resolve_anvil_chain_dispatch,
    _resolve_anvil_chains_and_funding,
    _resolve_gateway_chains_for_mainnet,
    _resolve_quick_config_path,
    _resolve_signing_key,
    _setup_gateway,
    _start_managed_gateway_and_connect,
    _validate_no_gateway_flags,
)

# _run_modes: execution modes (once / test-lifecycle / continuous)
from ._run_modes import (  # noqa: F401
    _build_components_or_exit,
    _cleanup_after_dry_run_vault_exit,
    _echo_anvil_network_banner,
    _echo_strategy_runtime_summary,
    _execute_run_mode,
    _load_resume_state,
    _load_strategy_bootstrap,
    _maybe_echo_chain_override,
    _maybe_handle_run_early_exit,
    _maybe_start_dashboard_process,
    _normalize_strategy_display_name,
    _prepare_runtime_bootstrap,
    _refine_strategy_chains,
    _resolve_config_chain_with_echo,
    _resolve_network_with_echo,
    _run_continuous,
    _run_once,
    _run_test_lifecycle,
)
from ._run_setup import (  # noqa: F401
    _FRESH_DEPLOYMENT_ID_TABLES,
    _anchor_strategy_folder_env,
    _configure_logging_and_validate,
    _detect_state_resume,
    _discover_and_load_config,
    _DryRunVaultEarlyExit,
    _fresh_clear_state,
    _handle_list_all,
    _load_strategy_class,
    _print_startup_banner,
    _require_strategy_deployment_id,
    _resolve_identity,
    _runtime_private_key_override,
    _wire_token_resolver,
)

# ---------------------------------------------------------------------------
# Strategy instantiation  (STAYS IN FACADE — tests patch run_helpers.inspect)
# ---------------------------------------------------------------------------


def _instantiate_strategy(  # noqa: C901
    *,
    strategy_class: type,
    strategy_config: dict[str, Any],
    runtime_config: Any,
    multi_chain: bool,
    strategy_chains: list[str],
    chain_wallets: dict[str, str],
) -> Any:
    """Instantiate the strategy class with the right config-type branch.

    Two call conventions are supported:

    1. ``IntentStrategy`` subclasses: discovered via ``issubclass`` check.
       The config dict is coerced into the strategy's declared config
       dataclass via ``_strategy_config.coerce_strategy_config`` (shared
       with ``strat backtest``), then the helper introspects ``__init__``
       to filter optional kwargs (``chains`` / ``chain_wallets``) so older
       strategies without ``**kwargs`` don't TypeError.
    2. Other classes (``StrategyBase`` subclasses, test doubles): try the
       config-dict convention first, fall back to the no-arg constructor
       on TypeError.

    Exits the process with status 1 on any failure (preserving the original
    top-level except block's behavior -- `click.echo(..., err=True)` +
    `sys.exit(1)`).

    Args:
        strategy_class: The loaded strategy class.
        strategy_config: Parsed strategy config dict (chain / wallet_address
            already injected).
        runtime_config: The runtime config object. Single-chain mode reads
            ``runtime_config.chain``; multi-chain reads the first entry of
            ``strategy_chains``.
        multi_chain: True when the strategy runs across multiple chains.
        strategy_chains: Chains the strategy is configured for. Only used
            when ``multi_chain`` is True.
        chain_wallets: Mapping of chain -> wallet address resolved from the
            gateway's WalletRegistry. Empty when unused.

    Returns:
        The constructed strategy instance.
    """
    from ._strategy_config import coerce_strategy_config

    IntentStrategyRuntime = _intent_strategy_runtime()

    try:
        if issubclass(strategy_class, IntentStrategyRuntime):
            # IntentStrategy requires specific parameters
            primary_chain = strategy_chains[0] if multi_chain else runtime_config.chain

            # Resolve the dataclass config type (or DictConfigWrapper fallback)
            # through the coercion path shared with `strat backtest`.
            config_instance = coerce_strategy_config(strategy_class, strategy_config)

            # Resolve wallet for strategy construction
            strat_wallet = runtime_config.execution_address
            if chain_wallets:
                strat_wallet = chain_wallets.get(primary_chain, strat_wallet)

            # Build kwargs, then filter to only those the strategy __init__ accepts.
            # This prevents TypeError for user strategies that don't accept **kwargs
            # or newer framework params like chains/chain_wallets.
            # Base kwargs are always safe (IntentStrategy.__init__ requires them).
            base_kwargs: dict[str, Any] = {
                "config": config_instance,
                "chain": primary_chain,
                "wallet_address": strat_wallet,
            }
            # Optional kwargs only included when non-None (multi-chain mode).
            optional_kwargs: dict[str, Any] = {}
            if chain_wallets:
                optional_kwargs["chains"] = list(chain_wallets.keys())
                optional_kwargs["chain_wallets"] = chain_wallets
            init_kwargs = {**base_kwargs, **optional_kwargs}
            try:
                sig = inspect.signature(strategy_class.__init__)
                params = sig.parameters
                # If __init__ accepts **kwargs, pass everything
                has_var_keyword = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
                if not has_var_keyword:
                    # Filter to only accepted parameter names
                    init_kwargs = {k: v for k, v in init_kwargs.items() if k in params}
            except (ValueError, TypeError) as exc:
                # Introspection failed — fall back to base kwargs only to avoid
                # injecting unexpected kwargs like 'chains' (VIB-1987).
                logger.debug("Strategy __init__ introspection failed, using base kwargs only: %s", exc)
                init_kwargs = base_kwargs

            strategy_instance = strategy_class(**init_kwargs)

            # Apply a per-deployment ``quote_asset`` override from config.json, if
            # present. Definition-only: resolved + frozen here at boot; the SDK does
            # not branch on it. The @almanak_strategy decorator's quote_asset is the
            # default when config.json omits it.
            _qa_override = strategy_config.get("quote_asset")
            if _qa_override is not None and hasattr(strategy_instance, "apply_quote_asset_override"):
                strategy_instance.apply_quote_asset_override(_qa_override)
        else:
            # Try dict config first, then no config
            cls_any: Any = strategy_class
            try:
                strategy_instance = cls_any(strategy_config)
            except TypeError:
                strategy_instance = cls_any()

        click.echo("Strategy instance created successfully")
        return strategy_instance

    except Exception as e:
        click.echo(f"Error creating strategy instance: {e}", err=True)
        sys.exit(1)


def _intent_strategy_runtime() -> type:
    """Deferred import of `IntentStrategy` to avoid circular-import risk.

    `run_helpers` is imported by `run`, so we can't eagerly import
    `..strategies.IntentStrategy` at module load (strategies pulls gateway
    modules that transitively hit `.run`). Doing the import at call time
    keeps the dependency one-way.
    """
    from ..strategies import IntentStrategy as _IntentStrategy

    return _IntentStrategy
