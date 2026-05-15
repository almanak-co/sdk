"""Helpers for demo ``run_anvil.py`` harnesses.

The demo Anvil harnesses under ``almanak/demo_strategies/*/run_anvil.py`` are
standalone operator scripts, not part of the framework boot surface that owns a
single preloaded ``load_config()`` object. They still must respect the config
boundary: dotenv ingest, RPC env ladders, Anvil override knobs, and subprocess
env overlays all route through ``almanak.config`` instead of reading
``os.environ`` directly in each script.

VIB-4425 uses this file as the single env-reading boundary for the final
Phase 3 demo backlog slice:

* repo-root ``.env`` loading for standalone demo scripts
* chain RPC lookup (chain-specific only or chain-specific + generic fallback)
* ``ANVIL_URL`` / ``ANVIL_<CHAIN>_PORT`` discovery for local forks
* ``ANVIL_FORK_BLOCK[_<CHAIN>]`` discovery for pinned CI forks
* subprocess environment overlays for ``uv run almanak strat run``
* the legacy ``ALCHEMY_API_KEY`` path used by the SushiSwap demo harness
"""

from __future__ import annotations

import os
from pathlib import Path

from almanak.config.backtest import backtest_config_from_env
from almanak.config.cli_runtime import DEFAULT_ANVIL_PORT, anvil_port_for_chain, chain_rpc_url_from_env
from almanak.config.cli_runtime import subprocess_env_with_overrides as _subprocess_env_with_overrides
from almanak.config.env import _load_dotenv_once
from almanak.config.gateway_runtime import chain_specific_rpc_url


def load_demo_dotenv(project_root: str | Path) -> None:
    """Load the repo-root ``.env`` once for a standalone demo harness."""
    _load_dotenv_once(str(Path(project_root) / ".env"))


def demo_chain_rpc_url(
    chain: str,
    *,
    allow_generic_fallback: bool = True,
    fallback: str | None = None,
) -> str | None:
    """Resolve the fork RPC URL for ``chain``.

    Args:
        chain: Chain name, case-insensitive.
        allow_generic_fallback: When True, preserve the legacy ladder
            ``ALMANAK_<CHAIN>_RPC_URL`` -> ``<CHAIN>_RPC_URL`` ->
            ``ALMANAK_RPC_URL`` -> ``RPC_URL``. When False, only the
            chain-specific entries are considered.
        fallback: Optional hard-coded fallback used by some demos (for example
            the public Base or Avalanche RPC).
    """
    if allow_generic_fallback:
        url, _ = chain_rpc_url_from_env(chain)
    else:
        url = chain_specific_rpc_url(chain)
    return url or fallback


def demo_anvil_port(chain: str, *, default: int = DEFAULT_ANVIL_PORT) -> int:
    """Resolve ``ANVIL_<CHAIN>_PORT`` or return ``default`` when unset."""
    return anvil_port_for_chain(chain) or default


def demo_anvil_url(
    chain: str,
    *,
    default_port: int = DEFAULT_ANVIL_PORT,
    allow_generic_override: bool = True,
) -> str:
    """Resolve the local Anvil URL for ``chain``.

    ``ANVIL_URL`` wins when explicitly set because some demo harnesses support
    attaching to an already-running node on a non-default port. Otherwise the
    URL is rebuilt from ``ANVIL_<CHAIN>_PORT`` (or ``default_port``).
    """
    if allow_generic_override:
        raw = os.environ.get("ANVIL_URL")
        if raw and raw.strip():
            return raw.strip()
    return f"http://127.0.0.1:{demo_anvil_port(chain, default=default_port)}"


def demo_fork_block(chain: str) -> str | None:
    """Resolve ``ANVIL_FORK_BLOCK_<CHAIN>`` or the generic fallback.

    A whitespace-only chain-specific value is treated as unset so that the
    generic ``ANVIL_FORK_BLOCK`` fallback still applies. The same rule
    matches ``demo_anvil_url`` above.
    """
    chain_raw = os.environ.get(f"ANVIL_FORK_BLOCK_{chain.upper()}")
    if chain_raw and chain_raw.strip():
        return chain_raw.strip()
    generic_raw = os.environ.get("ANVIL_FORK_BLOCK")
    if generic_raw and generic_raw.strip():
        return generic_raw.strip()
    return None


def demo_subprocess_env(
    *,
    chain: str,
    rpc_url: str,
    private_key: str,
    extra_overrides: dict[str, str] | None = None,
) -> dict[str, str]:
    """Return the parent env plus the canonical demo runner overrides."""
    chain_upper = chain.upper()
    overrides = {
        "ALMANAK_CHAIN": chain,
        "ALMANAK_RPC_URL": rpc_url,
        f"ALMANAK_{chain_upper}_RPC_URL": rpc_url,
        "ALMANAK_PRIVATE_KEY": private_key,
    }
    if extra_overrides:
        overrides.update(extra_overrides)
    return _subprocess_env_with_overrides(overrides)


def demo_alchemy_api_key() -> str | None:
    """Return the legacy ``ALCHEMY_API_KEY`` used by the SushiSwap demo."""
    return backtest_config_from_env().alchemy_api_key


__all__ = [
    "demo_alchemy_api_key",
    "demo_anvil_port",
    "demo_anvil_url",
    "demo_chain_rpc_url",
    "demo_fork_block",
    "demo_subprocess_env",
    "load_demo_dotenv",
]
