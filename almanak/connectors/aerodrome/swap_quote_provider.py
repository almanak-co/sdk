"""Swap quote provider for Aerodrome and Velodrome."""

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


class AerodromeSwapQuoteConnector(SwapQuoteConnector, SwapQuoteCapability):
    """Quote exact-input Aerodrome swaps through pool/router state."""

    protocol: ClassVar[ProtocolName] = ProtocolName("aerodrome")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def quote_swap(self, ctx, request: SwapQuoteRequest) -> SwapQuoteResult:
        if request.protocol != "aerodrome":
            raise SwapQuoteUnavailable(f"AerodromeSwapQuoteConnector cannot quote {request.protocol}")

        from almanak.connectors.aerodrome.adapter import AerodromeAdapter, AerodromeConfig

        config = AerodromeConfig(
            chain=request.chain,
            wallet_address=getattr(ctx, "wallet_address", "0x0000000000000000000000000000000000000000"),
            price_provider={},
            allow_placeholder_prices=True,
            rpc_url=getattr(ctx, "rpc_url", None),
            gateway_client=getattr(ctx, "gateway_client", None),
        )
        adapter = AerodromeAdapter(config, token_resolver=getattr(ctx, "token_resolver", None))
        stable = bool(request.extra.get("stable", False))
        use_cl = (
            bool(request.extra["use_cl"]) if "use_cl" in request.extra else bool(adapter.addresses.get("cl_quoter"))
        )
        try:
            tick_spacing = int(request.extra.get("tick_spacing", 100))
            amount_out = adapter.quote_swap_output(
                token_in=request.token_in,
                token_out=request.token_out,
                amount_in_wei=request.amount_in,
                stable=stable,
                tick_spacing=tick_spacing,
                use_cl=use_cl,
                require_onchain=True,
            )
        except Exception as exc:
            raise SwapQuoteUnavailable(f"Aerodrome quote unavailable: {exc}") from exc

        return SwapQuoteResult(
            amount_out=amount_out,
            source="aerodrome_cl_quoter" if use_cl else "aerodrome_router_getAmountsOut",
            metadata={"stable": stable, "use_cl": use_cl, "tick_spacing": tick_spacing},
        )


__all__ = ["AerodromeSwapQuoteConnector"]
