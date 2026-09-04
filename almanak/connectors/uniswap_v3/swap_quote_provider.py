"""Swap quote provider for Uniswap V3-style routers."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.base.swap_adapter import DefaultSwapAdapter
from almanak.connectors._strategy_base.protocol_aliases import UNISWAP_V3_FORKS
from almanak.connectors._strategy_base.swap_quote_registry import (
    SLIPPAGE_REFERENCE_V3_SPOT,
    SwapQuoteCapability,
    SwapQuoteConnector,
    SwapQuoteRequest,
    SwapQuoteResult,
    SwapQuoteUnavailable,
)


class UniswapV3SwapQuoteConnector(SwapQuoteConnector, SwapQuoteCapability):
    """Quote exact-input V3 swaps through the protocol quoter."""

    protocol: ClassVar[ProtocolName] = ProtocolName("uniswap_v3")
    protocol_aliases: ClassVar[tuple[ProtocolName, ...]] = tuple(
        ProtocolName(protocol) for protocol in sorted(UNISWAP_V3_FORKS - {"uniswap_v3"})
    )
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def quote_swap(self, ctx, request: SwapQuoteRequest) -> SwapQuoteResult:
        if request.protocol not in UNISWAP_V3_FORKS:
            raise SwapQuoteUnavailable(f"UniswapV3SwapQuoteConnector cannot quote {request.protocol}")

        try:
            selected_fee = request.fee_tier
            if request.pool_address is not None and selected_fee is None:
                from almanak.connectors._strategy_base.rpc import eth_call_uint256
                from almanak.connectors._strategy_base.v3_pool_abi import V3_FEE_SELECTOR

                selected_fee = eth_call_uint256(
                    chain=request.chain,
                    to=request.pool_address,
                    data=V3_FEE_SELECTOR,
                    rpc_url=getattr(ctx, "rpc_url", None),
                    gateway_client=getattr(ctx, "gateway_client", None),
                    timeout=getattr(ctx, "rpc_timeout", 10.0),
                )
                if selected_fee is None:
                    raise SwapQuoteUnavailable(f"Cannot read fee tier from pool {request.pool_address}")

            if request.pool_address is not None:
                from almanak.connectors._strategy_base.v3_pool_validation import validate_v3_pool

                if selected_fee is None:
                    raise SwapQuoteUnavailable(f"Cannot identify fee tier for pool {request.pool_address}")
                validation = validate_v3_pool(
                    chain=request.chain,
                    protocol=request.protocol,
                    token_a=request.token_in,
                    token_b=request.token_out,
                    fee_tier=selected_fee,
                    rpc_url=getattr(ctx, "rpc_url", None),
                    gateway_client=getattr(ctx, "gateway_client", None),
                )
                if (
                    validation.exists is not True
                    or validation.pool_address is None
                    or validation.pool_address.lower() != request.pool_address.lower()
                ):
                    raise SwapQuoteUnavailable(
                        f"Requested pool {request.pool_address} does not match the {request.protocol} "
                        f"route for fee tier {selected_fee}"
                    )

            adapter = DefaultSwapAdapter(
                request.chain,
                request.protocol,
                pool_selection_mode="fixed"
                if selected_fee is not None
                else getattr(ctx, "swap_pool_selection_mode", "auto"),
                fixed_fee_tier=selected_fee,
                rpc_url=getattr(ctx, "rpc_url", None),
                gateway_client=getattr(ctx, "gateway_client", None),
                rpc_timeout=getattr(ctx, "rpc_timeout", 10.0),
            )
            selected_fee = adapter.select_fee_tier(request.token_in, request.token_out, request.amount_in)
            amount_out = adapter.get_quoted_amount_out()
            if amount_out is None:
                raise SwapQuoteUnavailable("Uniswap V3 quoter returned no amount")
        except SwapQuoteUnavailable:
            raise
        except Exception as exc:
            raise SwapQuoteUnavailable(f"Uniswap V3 quote unavailable: {exc}") from exc

        return SwapQuoteResult(
            amount_out=amount_out,
            source=f"{request.protocol}_quoter",
            venue_binding_hash=request.venue_binding_hash,
            metadata={
                "fee_tier": selected_fee,
                "pool_address": request.pool_address,
                "pool_key": selected_fee,
                "pool_key_kind": "fee_tier",
                "slippage_reference": SLIPPAGE_REFERENCE_V3_SPOT,
                "fee_selection": adapter.last_fee_selection,
            },
        )


__all__ = ["UniswapV3SwapQuoteConnector"]
