"""Swap quote provider for Fluid DEX (DexReservesResolver-backed)."""

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


class FluidSwapQuoteConnector(SwapQuoteConnector, SwapQuoteCapability):
    """Quote exact-input Fluid swaps through the DexReservesResolver.

    Fluid is routerless: the per-pair pool is resolved on-chain first, then
    quoted via ``estimateSwapIn`` (quotes match execution to the wei —
    Phase-0 validation, VIB-5028).
    """

    protocol: ClassVar[ProtocolName] = ProtocolName("fluid")
    kind: ClassVar[ProtocolKind] = ProtocolKind.SWAP

    def quote_swap(self, ctx, request: SwapQuoteRequest) -> SwapQuoteResult:
        if request.protocol != "fluid":
            raise SwapQuoteUnavailable(f"FluidSwapQuoteConnector cannot quote {request.protocol}")

        from almanak.connectors.fluid.sdk import FluidMinAmountError, FluidSDK, FluidSDKError

        try:
            sdk = FluidSDK(
                chain=request.chain,
                rpc_url=getattr(ctx, "rpc_url", None),
                gateway_client=getattr(ctx, "gateway_client", None),
            )
        except FluidSDKError as exc:
            raise SwapQuoteUnavailable(f"Fluid quote unavailable: {exc}") from exc

        try:
            found = sdk.find_pool_for_pair(request.token_in, request.token_out)
        except FluidSDKError as exc:
            raise SwapQuoteUnavailable(f"Fluid pool enumeration failed: {exc}") from exc
        if found is None:
            raise SwapQuoteUnavailable(f"No Fluid pool for {request.token_in}->{request.token_out} on {request.chain}")
        pool_address, swap0to1 = found

        try:
            amount_out = sdk.get_swap_quote(pool_address, swap0to1, request.amount_in)
        except FluidMinAmountError as exc:
            raise SwapQuoteUnavailable(f"Fluid quote limit-gated (retryable): {exc}") from exc
        except FluidSDKError as exc:
            raise SwapQuoteUnavailable(f"Fluid quote failed: {exc}") from exc

        return SwapQuoteResult(
            amount_out=amount_out,
            source="fluid_dex_reserves_resolver",
            metadata={
                "pool": pool_address,
                "swap0to1": swap0to1,
            },
        )


__all__ = ["FluidSwapQuoteConnector"]
