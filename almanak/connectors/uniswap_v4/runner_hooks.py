"""Strategy-runner hooks for the Uniswap V4 connector."""

from __future__ import annotations

import logging
from typing import Any, ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.runner_hook_registry import (
    RunnerHookConnector,
    RunnerPoolKeyLookupCapability,
    RunnerV4PositionStateCapability,
)

logger = logging.getLogger(__name__)


class UniswapV4RunnerHookConnector(
    RunnerHookConnector,
    RunnerPoolKeyLookupCapability,
    RunnerV4PositionStateCapability,
):
    """Runner hooks for Uniswap V4 PoolKey lookup + live position-state reads."""

    protocol: ClassVar[ProtocolName] = ProtocolName("uniswap_v4")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def build_pool_key_lookup(self, gateway_client: Any) -> Any | None:
        """Return the sync PoolKey lookup bridge used by V4 receipt parsing."""
        from almanak.connectors.uniswap_v4.gateway_pool_key_client import (
            make_sync_pool_key_lookup,
        )

        return make_sync_pool_key_lookup(gateway_client)

    def build_v4_position_state_reader(self, gateway_client: Any) -> Any | None:
        """Return a ``(chain, token_id) → V4PositionState | None`` reader (VIB-5024).

        Resolves the connector-owned PositionManager + StateView addresses for the
        chain and routes the live on-chain read through the gateway
        ``QueryV4PositionState`` RPC. Returns ``None`` (no live state) for any
        unsupported chain / missing address / gateway failure, so the framework
        valuer falls back to its ESTIMATED OPEN-amount path — never a wrong HIGH.
        """
        if gateway_client is None:
            return None

        def _read(chain: str, token_id: int) -> Any | None:
            from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

            chain_key = (chain or "").lower()
            addresses = UNISWAP_V4.get(chain_key)
            if not addresses:
                return None
            position_manager = addresses.get("position_manager")
            state_view = addresses.get("state_view")
            if not position_manager or not state_view:
                # V4 not fully deployed on this chain (no StateView) → no live path.
                return None
            try:
                return gateway_client.query_v4_position_state(
                    chain=chain_key,
                    position_manager=position_manager,
                    state_view=state_view,
                    token_id=int(token_id),
                )
            except Exception:
                logger.debug(
                    "V4 live position-state read failed for token_id=%s on %s",
                    token_id,
                    chain_key,
                    exc_info=True,
                )
                return None

        return _read


__all__ = ["UniswapV4RunnerHookConnector"]
