"""Single config-service entry point."""

from __future__ import annotations

from typing import Any

from almanak.config.agent_tools import agent_tools_config_from_env
from almanak.config.backtest import backtest_config_from_env
from almanak.config.cli_runtime import cli_runtime_config_from_env
from almanak.config.connectors import connectors_config_from_env
from almanak.config.env import _load_dotenv_once, gateway_config_from_env
from almanak.config.framework import framework_config_from_env
from almanak.config.hosted import HostedConfig
from almanak.config.local import LocalConfig
from almanak.config.safe_signer import safe_signer_service_config_from_env
from almanak.config.simulation import simulation_config_from_env


def load_config(
    *,
    gateway_overrides: dict[str, Any] | None = None,
    dotenv_path: str | None = None,
) -> LocalConfig | HostedConfig:
    """Construct the typed configuration once at the relevant boot surface.

    Phase 1: gateway boot reads env via this single boundary instead of via
    ``GatewaySettings._fallback_env_vars``. Pass explicit overrides through
    ``gateway_overrides=`` — they win over env per pydantic-settings priority.

    Phase 5b: ``connectors`` is populated eagerly here so the connector
    layer always sees a fully-resolved typed config (the
    ``default_factory=connectors_config_from_env`` on the model would do
    the same lookup, but constructing the submodel explicitly here makes
    the boot-time read auditable and lets future overrides plug in via a
    ``connectors_overrides=`` kwarg without changing call sites).

    Phase 5c: ``backtest`` follows the same pattern. Constructing the
    submodel explicitly at the service boundary lets future overrides
    (e.g. ``backtest_overrides={"coingecko_api_key": "..."}`` from a
    test harness) plug in via the same kwargs shape as ``gateway`` and
    ``connectors``.

    Phase 5e: ``cli`` follows the same eager-construct-at-boot pattern.
    The CLI cluster's env reads (gateway-wallets discriminator, Safe-mode
    preflight inputs, Solana fork URL/port, Anvil per-chain ports,
    reconciliation / hardcoded-prices toggles, and the legacy unprefixed
    ``GATEWAY_AUTH_TOKEN`` fallback) all flow through
    ``cli_runtime_config_from_env``.

    Phase 5d / 6: ``simulation``, ``framework``, and ``agent_tools`` are
    also constructed eagerly here so boot-time consumers can consume typed
    slices from the single config object rather than reparsing env ad hoc.
    ``safe_signer`` follows the same pattern for Safe wallet mapping and
    Zodiac signer service settings.

    ``load_config()`` is framework-owned. Process entrypoints / wrappers call
    it once after loading the relevant dotenv source, then downstream code
    should consume injected slices from the returned config object.
    """
    # ``is_hosted`` deferred to call time so importing ``almanak.config`` from
    # the CLI bootstrap doesn't pull ``almanak.framework.deployment`` into
    # ``sys.modules`` (forbidden in the deployed strategy container — see
    # tests/framework/cli/test_imports_lean.py).
    from almanak.framework.deployment.mode import is_hosted

    _load_dotenv_once(dotenv_path)
    gateway = gateway_config_from_env(**(gateway_overrides or {}))
    connectors = connectors_config_from_env(dotenv_path=dotenv_path)
    backtest = backtest_config_from_env(dotenv_path=dotenv_path)
    simulation = simulation_config_from_env(dotenv_path=dotenv_path)
    safe_signer = safe_signer_service_config_from_env(dotenv_path=dotenv_path)
    cli = cli_runtime_config_from_env(dotenv_path=dotenv_path)
    framework = framework_config_from_env(dotenv_path=dotenv_path)
    agent_tools = agent_tools_config_from_env(dotenv_path=dotenv_path)
    if is_hosted():
        return HostedConfig(
            gateway=gateway,
            connectors=connectors,
            backtest=backtest,
            simulation=simulation,
            safe_signer=safe_signer,
            cli=cli,
            framework=framework,
            agent_tools=agent_tools,
        )
    return LocalConfig(
        gateway=gateway,
        connectors=connectors,
        backtest=backtest,
        simulation=simulation,
        safe_signer=safe_signer,
        cli=cli,
        framework=framework,
        agent_tools=agent_tools,
    )


__all__ = ["load_config"]
