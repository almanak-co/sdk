"""Swap quote provider for Curve."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.swap_quote_registry import (
    SwapQuoteCapability,
    SwapQuoteConnector,
    SwapQuoteRequest,
    SwapQuoteResult,
    SwapQuoteUnavailable,
)


class CurveSwapQuoteConnector(SwapQuoteConnector, SwapQuoteCapability):
    """Quote exact-input Curve swaps through pool get_dy methods."""

    protocol: ClassVar[ProtocolName] = ProtocolName("curve")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def quote_swap(self, ctx, request: SwapQuoteRequest) -> SwapQuoteResult:
        if request.protocol != "curve":
            raise SwapQuoteUnavailable(f"CurveSwapQuoteConnector cannot quote {request.protocol}")
        if not request.pool_address:
            raise SwapQuoteUnavailable("Curve quotes require request.pool_address")

        from almanak.connectors.curve.adapter import CurveAdapter, CurveConfig

        try:
            config = CurveConfig(
                chain=request.chain,
                wallet_address=getattr(ctx, "wallet_address", "0x0000000000000000000000000000000000000000"),
                rpc_url=getattr(ctx, "rpc_url", None),
                gateway_client=getattr(ctx, "gateway_client", None),
            )
            adapter = CurveAdapter(config, token_resolver=getattr(ctx, "token_resolver", None))
            amount_out = adapter.quote_swap_output(
                pool_address=request.pool_address,
                token_in=request.token_in,
                token_out=request.token_out,
                amount_in_wei=request.amount_in,
            )
        except Exception as exc:
            raise SwapQuoteUnavailable(f"Curve quote unavailable: {exc}") from exc

        return SwapQuoteResult(
            amount_out=amount_out,
            source="curve_pool_get_dy",
            metadata={"pool_address": request.pool_address},
        )


__all__ = ["CurveSwapQuoteConnector"]
