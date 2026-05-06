"""Shared config base — fields present in every deployment mode."""

from pydantic import BaseModel, ConfigDict

from almanak.gateway.core.settings import GatewaySettings

# Phase 0 alias. Phase 1 inverts ownership: GatewaySettings becomes a deprecated
# alias for GatewayConfig, _fallback_env_vars / polymarket resolvers move to
# almanak/config/env.py at the service boundary, and the env_prefix discipline is
# enforced consistently. The parity test in tests/unit/config/ guards the cutover.
GatewayConfig = GatewaySettings


class BaseConfig(BaseModel):
    """Fields present in every deployment mode.

    Phase 0 skeleton: only the gateway submodel is wired so the parity test in
    tests/unit/config/test_gateway_settings_parity.py has something to assert
    against. Other shared submodels (RuntimeConfig, RiskConfig, StrategyConfig)
    land in subsequent phases.
    """

    gateway: GatewayConfig

    model_config = ConfigDict(arbitrary_types_allowed=True)


__all__ = ["BaseConfig", "GatewayConfig"]
