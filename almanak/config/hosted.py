"""Hosted-mode configuration sibling.

Phase 0 skeleton: subclass of :class:`BaseConfig` with mode-specific fields
landing in later phases.

* Phase 1 — ``agent_id``, ``gateway_db_url``, ``gateway_auth_token`` (the
  hosted gateway boot surface).
* Phase 5b — ``connectors``: same submodel as ``LocalConfig`` so connectors
  consume one shape regardless of deployment mode. The hosted gateway
  carries its own copy of the credentials in ``GatewayConfig.*_api_key`` —
  the connector-side mirror reads the same env vars but feeds the
  gRPC-stub side of the connector layer (the strategy container in hosted
  mode never has direct outbound HTTPS; this field is read only by
  fallback paths under tests / paper-trading).
* Phase 5c — ``backtest``: same shape as ``LocalConfig.backtest`` so the
  backtesting layer (paper-trading, PnL providers) sees a uniform typed
  config across deployment modes. In hosted mode the strategy container
  has no direct egress, so most consumers route through the gateway —
  but the typed shape stays uniform.

See ``docs/internal/config-service-plan.md`` for the full migration order.
"""

from pydantic import Field

from almanak.config.backtest import BacktestConfig, backtest_config_from_env
from almanak.config.base import BaseConfig
from almanak.config.cli_runtime import CliRuntimeConfig, cli_runtime_config_from_env
from almanak.config.connectors import ConnectorsConfig, connectors_config_from_env


class HostedConfig(BaseConfig):
    """Hosted-mode config (gateway-managed, postgres-backed).

    Phase 0 skeleton — no fields beyond ``BaseConfig.gateway``. The hosted
    surface resolves ``agent_id`` via :func:`almanak.framework.deployment.mode.agent_id`
    and gateway-managed secrets land here in Phase 1.

    ``connectors`` mirrors :attr:`LocalConfig.connectors`; the default
    factory reads env at construction time so the field is available the
    moment a connector is instantiated.

    ``backtest`` mirrors :attr:`LocalConfig.backtest` — same eager
    factory pattern, same submodel shape.

    ``cli`` mirrors :attr:`LocalConfig.cli`. The hosted strategy
    container's CLI surface is mostly idle (the runtime entry point is
    the gateway sidecar's startup script, not ``almanak strat run``),
    but the same env keys are read by the few CLI-shaped helpers that
    do execute on hosted (e.g. ``almanak ax`` invoked from operator
    debug shells), so the field shape stays uniform.
    """

    connectors: ConnectorsConfig = Field(default_factory=connectors_config_from_env)
    backtest: BacktestConfig = Field(default_factory=backtest_config_from_env)
    cli: CliRuntimeConfig = Field(default_factory=cli_runtime_config_from_env)


__all__ = ["HostedConfig"]
