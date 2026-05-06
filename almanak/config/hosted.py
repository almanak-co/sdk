"""Hosted-mode configuration sibling.

Phase 0 skeleton: empty subclass of :class:`BaseConfig`. Mode-specific fields
land in later phases:

* Phase 1 — ``agent_id``, ``gateway_db_url``, ``gateway_auth_token`` (the
  hosted gateway boot surface).

See ``docs/internal/config-service-plan.md`` for the full migration order.
"""

from almanak.config.base import BaseConfig


class HostedConfig(BaseConfig):
    """Hosted-mode config (gateway-managed, postgres-backed).

    Phase 0 skeleton — no fields beyond ``BaseConfig.gateway``. The hosted
    surface resolves ``agent_id`` via :func:`almanak.framework.deployment.mode.agent_id`
    and gateway-managed secrets land here in Phase 1.
    """


__all__ = ["HostedConfig"]
