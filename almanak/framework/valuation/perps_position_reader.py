"""On-chain perp-position reader via gateway RPC.

Queries a wallet's open perpetual positions for a venue so valuation and
position discovery can reprice them. *How* the read is performed — which
contract holds the position book, the function selector, the calldata layout,
and the return decoding — is **connector knowledge** resolved through the
strategy-side
:class:`~almanak.connectors._strategy_base.perps_read_registry.PerpsReadRegistry`.
This module names no connector and no protocol-specific contract kind: it owns
only the gateway-routed ``eth_call`` that executes a read plan and the dispatch
to the registry.

Uses the gateway's generic Call RPC — no proto changes needed. Same pattern as
``lending_position_reader.py``.

VIB-4930 (epic VIB-4851).
"""

import json
import logging

from almanak.connectors._strategy_base.perps_read_base import (
    PerpsPositionOnChain,
    PerpsPositionQuery,
    PerpsReadResult,
)
from almanak.connectors._strategy_base.perps_read_registry import PerpsReadRegistry

logger = logging.getLogger(__name__)

# Backward-compatibility re-exports of the strategy-side foundation symbols.
# ``PerpsPositionOnChain`` is the canonical decoded position and ``PerpsReadResult``
# the read outcome; both relocated into ``_strategy_base`` with the read seam, so
# they stay importable here for call sites and tests that depend on the surface.
__all__ = [
    "PerpsPositionOnChain",
    "PerpsPositionReader",
    "PerpsReadResult",
]


class PerpsPositionReader:
    """Reads open perpetual positions for a venue via gateway RPC.

    Resolves the read (target contract(s) + calldata + reducer) for a protocol
    through :class:`PerpsReadRegistry`, executes the gateway-routed ``eth_call``
    per planned call, and reduces the responses with the connector's pure
    reducer. The reader holds no protocol-specific knowledge of its own.
    """

    def __init__(self, gateway_client: object | None = None) -> None:
        self._gateway = gateway_client

    def read_positions(
        self,
        chain: str,
        wallet_address: str,
        protocol: str,
    ) -> PerpsReadResult:
        """Query all open perpetual positions for a wallet on ``protocol``.

        ``protocol`` is required (no default): the reader names no venue of its
        own — the caller always knows which venue it is reading (the valuation
        path passes ``position.protocol``; discovery iterates
        :meth:`PerpsReadRegistry.supported_protocols`). This keeps the framework
        perp read path free of any hardcoded venue slug.

        Args:
            chain: Chain identifier (e.g. "arbitrum").
            wallet_address: Wallet address to query.
            protocol: Perp protocol identifier (e.g. "gmx_v2"). Resolved through
                :class:`PerpsReadRegistry`; an unregistered venue / undeployed
                chain yields an ``ok=False`` (unmeasured) result.

        Returns:
            PerpsReadResult: ``ok=True`` with the active positions on a
            successful read; ``ok=False`` (no positions) when the gateway is
            absent, the venue/chain is unresolved, or the read failed. The
            Empty≠Zero seam — callers keep the strategy-reported value rather
            than fabricating a zero on ``ok=False``.
        """
        if self._gateway is None:
            return PerpsReadResult(positions=(), ok=False)

        query = PerpsPositionQuery(chain=chain, wallet_address=wallet_address)
        plan = PerpsReadRegistry.resolve_plan(protocol, query)
        if plan is None:
            logger.debug(
                "No perps read available for protocol %s on chain %s",
                protocol,
                chain,
            )
            return PerpsReadResult(positions=(), ok=False)

        results = [self._eth_call(chain, call.to, call.data) for call in plan.calls]
        return plan.reduce(plan.query, results)

    @staticmethod
    def from_gateway_client(gateway_client: object | None) -> "PerpsPositionReader":
        """Create a reader from a gateway client or DirectRpcAdapter.

        Thin wrapper kept for call-site compatibility: the reader stores whatever
        is passed (a real ``GatewayClient`` or a ``DirectRpcAdapter`` — both
        expose ``_rpc_stub.Call``), mirroring the lending reader.
        """
        return PerpsPositionReader(gateway_client)

    def _eth_call(self, chain: str, to: str, data: str) -> str | None:
        """Make an eth_call via gateway generic RPC."""
        try:
            from almanak.gateway.proto import gateway_pb2

            rpc_stub = getattr(self._gateway, "_rpc_stub", None)
            if rpc_stub is None:
                logger.debug("Gateway client not connected for perps position query")
                return None

            timeout = getattr(getattr(self._gateway, "config", None), "timeout", 10)

            params_json = json.dumps([{"to": to, "data": data}, "latest"])
            response = rpc_stub.Call(
                gateway_pb2.RpcRequest(
                    chain=chain,
                    method="eth_call",
                    params=params_json,
                ),
                timeout=timeout,
            )

            if not response.success:
                logger.debug("eth_call failed for perps position: %s", response.error)
                return None

            if response.result:
                return json.loads(response.result)
            return None
        except Exception:
            logger.debug("Failed to make eth_call for perps position", exc_info=True)
            return None
