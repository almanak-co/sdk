"""Strategy-runner hooks for the Uniswap V4 connector."""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.runner_hook_registry import (
    RunnerHookConnector,
    RunnerPoolKeyLookupCapability,
)


class UniswapV4RunnerHookConnector(RunnerHookConnector, RunnerPoolKeyLookupCapability):
    """Runner hooks for Uniswap V4 PoolKey lookup injection."""

    protocol: ClassVar[ProtocolName] = ProtocolName("uniswap_v4")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def build_pool_key_lookup(self, gateway_client: Any) -> Any | None:
        """Return the sync PoolKey lookup bridge used by V4 receipt parsing."""
        from almanak.connectors.uniswap_v4.gateway_pool_key_client import (
            make_sync_pool_key_lookup,
        )

        return make_sync_pool_key_lookup(gateway_client)


__all__ = ["UniswapV4RunnerHookConnector"]
