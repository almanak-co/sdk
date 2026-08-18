"""Strategy-runner hooks for the Curve connector."""

from __future__ import annotations

import logging
from typing import Any, ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.runner_hook_registry import (
    RunnerCurvePoolMetaLookupCapability,
    RunnerHookConnector,
    RunnerPoolDescriptorCapability,
)

logger = logging.getLogger(__name__)


class CurveRunnerHookConnector(
    RunnerHookConnector,
    RunnerCurvePoolMetaLookupCapability,
    RunnerPoolDescriptorCapability,
):
    """Runner hooks for Curve uncurated-pool metadata lookup (VIB-5628)."""

    protocol: ClassVar[ProtocolName] = ProtocolName("curve")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def has_pool_descriptor_declarations(self, *, chain: str, config: Any) -> bool:
        """Return whether deployment config declares an exact Curve pool."""
        from almanak.connectors.curve.pool_binding import configured_pool_binding_declarations

        return bool(configured_pool_binding_declarations(chain=chain, config=config))

    def resolve_pool_descriptors(
        self,
        *,
        gateway_client: Any,
        chain: str,
        config: Any,
    ) -> tuple[Any, ...]:
        """Live-verify declared pools and return neutral execution descriptors."""
        from almanak.connectors.curve.pool_binding import (
            binding_pool_descriptor,
            resolve_configured_pool_bindings,
        )

        bindings = resolve_configured_pool_bindings(
            chain=chain,
            config=config,
            gateway_client=gateway_client,
        )
        return tuple(binding_pool_descriptor(binding) for binding in bindings)

    def build_curve_pool_meta_lookup(self, gateway_client: Any) -> Any | None:
        """Return the sync ``(pool_address, chain) -> CurvePoolMetadata | None`` lookup.

        Binds the Curve dynamic-pool resolver to the runner's gateway client so
        the receipt parser can label an uncurated pool's LP legs on a static
        exact pool metadata. Returns ``None`` (no live path) when no gateway
        client is configured, so the parser degrades to the legacy static-only
        path — Empty != Zero, never fabricates a leg.
        """
        if gateway_client is None:
            return None
        from almanak.connectors.curve.gateway_pool_meta_client import (
            make_sync_curve_pool_meta_lookup,
        )

        return make_sync_curve_pool_meta_lookup(gateway_client)


__all__ = ["CurveRunnerHookConnector"]
