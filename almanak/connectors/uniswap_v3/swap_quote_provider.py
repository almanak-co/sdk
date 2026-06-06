"""Swap quote provider for Uniswap V3-style routers."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.base.swap_adapter import DefaultSwapAdapter
from almanak.connectors._strategy_base.swap_quote_registry import (
    SwapQuoteCapability,
    SwapQuoteConnector,
    SwapQuoteRequest,
    SwapQuoteResult,
    SwapQuoteUnavailable,
)


class UniswapV3SwapQuoteConnector(SwapQuoteConnector, SwapQuoteCapability):
    """Quote exact-input V3 swaps through the protocol quoter."""

    protocol: ClassVar[ProtocolName] = ProtocolName("uniswap_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def quote_swap(self, ctx, request: SwapQuoteRequest) -> SwapQuoteResult:
        if request.protocol != "uniswap_v3":
            raise SwapQuoteUnavailable(f"UniswapV3SwapQuoteConnector cannot quote {request.protocol}")

        adapter = DefaultSwapAdapter(
            request.chain,
            request.protocol,
            pool_selection_mode="fixed"
            if request.fee_tier is not None
            else getattr(ctx, "swap_pool_selection_mode", "auto"),
            fixed_fee_tier=request.fee_tier,
            rpc_url=getattr(ctx, "rpc_url", None),
            gateway_client=getattr(ctx, "gateway_client", None),
            rpc_timeout=getattr(ctx, "rpc_timeout", 10.0),
        )
        try:
            selected_fee = adapter.select_fee_tier(request.token_in, request.token_out, request.amount_in)
        except Exception as exc:
            raise SwapQuoteUnavailable(f"Uniswap V3 quote unavailable: {exc}") from exc

        amount_out = adapter.get_quoted_amount_out()
        if amount_out is None:
            raise SwapQuoteUnavailable("Uniswap V3 quoter returned no amount")

        return SwapQuoteResult(
            amount_out=amount_out,
            source="uniswap_v3_quoter",
            metadata={
                "fee_tier": selected_fee,
                "fee_selection": adapter.last_fee_selection,
            },
        )


__all__ = ["UniswapV3SwapQuoteConnector"]
