"""Strategy-runner hooks for the Curve connector."""

from __future__ import annotations

import logging
from typing import Any, ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.runner_hook_registry import (
    RunnerCurvePoolMetaLookupCapability,
    RunnerHookConnector,
)

logger = logging.getLogger(__name__)


class CurveRunnerHookConnector(
    RunnerHookConnector,
    RunnerCurvePoolMetaLookupCapability,
):
    """Runner hooks for Curve uncurated-pool metadata lookup (VIB-5628)."""

    protocol: ClassVar[ProtocolName] = ProtocolName("curve")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def build_curve_pool_meta_lookup(self, gateway_client: Any) -> Any | None:
        """Return the sync ``(pool_address, chain) -> CurvePoolMetadata | None`` lookup.

        Binds the Curve dynamic-pool resolver to the runner's gateway client so
        the receipt parser can label an uncurated pool's LP legs on a static
        ``CURVE_POOLS`` miss. Returns ``None`` (no live path) when no gateway
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
