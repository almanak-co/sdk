"""Pendle principal-token market reader capability."""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.principal_token_market_reader_registry import (
    PrincipalTokenMarketReadCapability,
    PrincipalTokenMarketReadConnector,
    PrincipalTokenMarketReader,
)


class PendlePrincipalTokenMarketReadConnector(
    PrincipalTokenMarketReadConnector,
    PrincipalTokenMarketReadCapability,
):
    """Build Pendle on-chain readers for PT market valuation."""

    protocol: ClassVar[ProtocolName] = ProtocolName("pendle")
    kind: ClassVar[ProtocolKind] = ProtocolKind.YIELD_TRADING

    def build_reader(
        self,
        *,
        chain: str,
        gateway_client: Any | None = None,
        rpc_url: str | None = None,
        cache_ttl_seconds: float = 30.0,
    ) -> PrincipalTokenMarketReader:
        from almanak.connectors.pendle.on_chain_reader import PendleOnChainReader

        return PendleOnChainReader(
            chain=chain,
            gateway_client=gateway_client,
            rpc_url=rpc_url,
            cache_ttl_seconds=cache_ttl_seconds,
        )


__all__ = ["PendlePrincipalTokenMarketReadConnector"]
