"""On-chain lending-position reader via gateway RPC.

Queries a wallet's current supply (aToken balance) and debt for a single
reserve so valuation, position discovery, and ``amount="all"`` resolution can
reprice lending positions. *How* the read is performed — which contract holds
the per-user reserve data, the function selector, the calldata layout, and the
return decoding — is **connector knowledge** resolved through the strategy-side
:class:`~almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry`.
This module names no connector and no protocol-specific contract kind: it owns
only the gateway-routed ``eth_call`` that executes a read plan and the dispatch
to the registry.

Uses the gateway's generic Call RPC — no proto changes needed. Same pattern as
``lp_position_reader.py``.
"""

import json
import logging

from almanak.connectors._strategy_base.lending_read_base import (
    LendingPositionOnChain,
    decode_uint_hex,
    pad_address,
    parse_user_reserve_data_hex,
)
from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

logger = logging.getLogger(__name__)

# Backward-compatibility re-exports of the strategy-side foundation symbols.
# ``LendingPositionOnChain`` is the canonical decoded result; the underscore-
# prefixed ABI helpers preserve the import surface existing call sites and tests
# depend on after the Aave-fork read logic moved into ``_strategy_base``.
_pad_address = pad_address
_decode_uint_hex = decode_uint_hex
_parse_user_reserve_data_hex = parse_user_reserve_data_hex

__all__ = [
    "LendingPositionOnChain",
    "LendingPositionReader",
]


class LendingPositionReader:
    """Reads single-reserve lending positions via gateway RPC.

    Resolves the read (target contract + calldata + decoder) for a protocol
    through :class:`LendingReadRegistry`, executes the gateway-routed
    ``eth_call``, and decodes the response with the connector's decoder. The
    reader holds no protocol-specific knowledge of its own.
    """

    def __init__(self, gateway_client: object | None = None) -> None:
        self._gateway = gateway_client

    def read_position(
        self,
        chain: str,
        asset_address: str,
        wallet_address: str,
        protocol: str | None = None,
    ) -> LendingPositionOnChain | None:
        """Query a single asset's lending position for a wallet.

        Args:
            chain: Chain identifier (e.g., "arbitrum", "base").
            asset_address: Underlying asset contract address.
            wallet_address: User wallet address.
            protocol: Lending protocol identifier (e.g. "aave_v3", "spark").
                When ``None``, the registry's default lending
                protocol is used — preserving the legacy single-reserve read
                path for callers that do not track the protocol.

        Returns:
            LendingPositionOnChain with supply/debt data, or None on failure.
        """
        if self._gateway is None:
            return None

        resolved_protocol = LendingReadRegistry.default_protocol() if protocol is None else protocol
        plan = LendingReadRegistry.resolve(
            protocol=resolved_protocol,
            chain=chain,
            asset_address=asset_address,
            wallet_address=wallet_address,
        )
        if plan is None:
            logger.debug(
                "No lending read available for protocol %s on chain %s",
                resolved_protocol,
                chain,
            )
            return None

        result_hex = self._eth_call(chain, plan.target_address, plan.calldata)
        if not result_hex:
            return None

        return plan.parse_result(result_hex, asset_address)

    def read_positions(
        self,
        chain: str,
        asset_addresses: list[str],
        wallet_address: str,
        protocol: str | None = None,
    ) -> list[LendingPositionOnChain]:
        """Query multiple assets' lending positions for a wallet.

        Returns only active positions (non-zero supply or debt).

        Args:
            chain: Chain identifier.
            asset_addresses: List of underlying asset addresses to check.
            wallet_address: User wallet address.
            protocol: Lending protocol identifier; ``None`` uses the registry
                default (see :meth:`read_position`).

        Returns:
            List of active LendingPositionOnChain entries.
        """
        positions = []
        for asset in asset_addresses:
            pos = self.read_position(chain, asset, wallet_address, protocol=protocol)
            if pos is not None and pos.is_active:
                positions.append(pos)
        return positions

    def _eth_call(self, chain: str, to: str, data: str) -> str | None:
        """Make an eth_call via gateway generic RPC."""
        try:
            from almanak.gateway.proto import gateway_pb2

            rpc_stub = getattr(self._gateway, "_rpc_stub", None)
            if rpc_stub is None:
                logger.debug("Gateway client not connected for lending position query")
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
                logger.debug("eth_call failed for lending position: %s", response.error)
                return None

            if response.result:
                return json.loads(response.result)
            return None
        except Exception:
            logger.debug("Failed to make eth_call for lending position", exc_info=True)
            return None
