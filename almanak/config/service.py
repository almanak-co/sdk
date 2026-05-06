"""Single config-service entry point."""

from __future__ import annotations

from typing import Any

from almanak.config.env import _load_dotenv_once, gateway_config_from_env
from almanak.config.hosted import HostedConfig
from almanak.config.local import LocalConfig


def load_config(
    *,
    gateway_overrides: dict[str, Any] | None = None,
    dotenv_path: str | None = None,
) -> LocalConfig | HostedConfig:
    """Construct the typed configuration once at the Click main group.

    Phase 1: gateway boot reads env via this single boundary instead of via
    ``GatewaySettings._fallback_env_vars``. Pass explicit overrides through
    ``gateway_overrides=`` — they win over env per pydantic-settings priority.
    """
    # ``is_hosted`` deferred to call time so importing ``almanak.config`` from
    # the CLI bootstrap doesn't pull ``almanak.framework.deployment`` into
    # ``sys.modules`` (forbidden in the deployed strategy container — see
    # tests/framework/cli/test_imports_lean.py).
    from almanak.framework.deployment.mode import is_hosted

    _load_dotenv_once(dotenv_path)
    gateway = gateway_config_from_env(**(gateway_overrides or {}))
    if is_hosted():
        return HostedConfig(gateway=gateway)
    return LocalConfig(gateway=gateway)


__all__ = ["load_config"]
