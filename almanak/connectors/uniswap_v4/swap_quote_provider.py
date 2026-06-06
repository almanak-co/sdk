"""Swap quote provider for Uniswap V4."""

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


class UniswapV4SwapQuoteConnector(SwapQuoteConnector, SwapQuoteCapability):
    """Quote exact-input V4 swaps through the V4 Quoter contract."""

    protocol: ClassVar[ProtocolName] = ProtocolName("uniswap_v4")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def quote_swap(self, ctx, request: SwapQuoteRequest) -> SwapQuoteResult:
        if request.protocol != "uniswap_v4":
            raise SwapQuoteUnavailable(f"UniswapV4SwapQuoteConnector cannot quote {request.protocol}")

        from almanak.connectors.uniswap_v4.sdk import UniswapV4SDK

        sdk = UniswapV4SDK(
            chain=request.chain,
            rpc_url=getattr(ctx, "rpc_url", None),
            gateway_client=getattr(ctx, "gateway_client", None),
        )
        try:
            quote = sdk.get_quote(
                token_in=request.token_in,
                token_out=request.token_out,
                amount_in=request.amount_in,
                fee_tier=request.fee_tier if request.fee_tier is not None else 3000,
                token_in_decimals=request.token_in_decimals if request.token_in_decimals is not None else 18,
                token_out_decimals=request.token_out_decimals if request.token_out_decimals is not None else 18,
            )
        except Exception as exc:
            raise SwapQuoteUnavailable(f"Uniswap V4 quote unavailable: {exc}") from exc

        return SwapQuoteResult(
            amount_out=quote.amount_out,
            gas_estimate=quote.gas_estimate,
            source="uniswap_v4_quoter",
            metadata={"fee_tier": quote.fee_tier},
        )


__all__ = ["UniswapV4SwapQuoteConnector"]
