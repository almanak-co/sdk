"""Swap quote provider for Uniswap V4."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.swap_quote_registry import (
    SLIPPAGE_REFERENCE_UNSUPPORTED,
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

        from almanak.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config
        from almanak.connectors.uniswap_v4.gateway_pool_key_client import make_sync_pool_key_lookup
        from almanak.connectors.uniswap_v4.routing import resolve_swap_selection
        from almanak.connectors.uniswap_v4.sdk import UniswapV4SDK

        sdk = UniswapV4SDK(
            chain=request.chain,
            rpc_url=getattr(ctx, "rpc_url", None),
            gateway_client=getattr(ctx, "gateway_client", None),
        )
        try:
            client = getattr(ctx, "gateway_client", None)
            params = dict(request.extra)
            if request.pool_address is not None:
                if "pool_id" in params and params["pool_id"] != request.pool_address:
                    raise ValueError("Conflicting V4 pool IDs in quote request")
                params["pool_id"] = request.pool_address
            if request.fee_tier is not None:
                if "fee_tier" in params and params["fee_tier"] != request.fee_tier:
                    raise ValueError("Conflicting V4 fee fields in quote request")
                params["fee_tier"] = request.fee_tier
            lookup = make_sync_pool_key_lookup(client) if client is not None else None
            selection = resolve_swap_selection(
                params,
                token_in=request.token_in,
                token_out=request.token_out,
                default_fee=3000,
                lookup=(lambda pool_id: lookup(pool_id, request.chain)) if lookup is not None else None,
            )
            factory = getattr(ctx, "venue_verification_gateway_factory", None)
            adapter = UniswapV4Adapter(
                config=UniswapV4Config(chain=request.chain),
                gateway_client=client,
                venue_verification_gateway_factory=factory,
            )
            verified, _ = adapter._verify_swap_selection(selection)
            if request.venue_binding_hash is not None and request.venue_binding_hash != verified.binding.binding_hash:
                raise ValueError("V4 quote venue binding does not match the requested binding")
            quote = sdk.get_quote(
                token_in=request.token_in,
                token_out=request.token_out,
                amount_in=request.amount_in,
                fee_tier=request.fee_tier if request.fee_tier is not None else 3000,
                token_in_decimals=request.token_in_decimals if request.token_in_decimals is not None else 18,
                token_out_decimals=request.token_out_decimals if request.token_out_decimals is not None else 18,
                pool_key=selection.key,
                hook_data=selection.hook_data or b"",
                block_number=verified.evidence.block_number,
                quote_gateway=factory() if callable(factory) else None,
            )
        except Exception as exc:
            raise SwapQuoteUnavailable(f"Uniswap V4 quote unavailable: {exc}") from exc

        return SwapQuoteResult(
            amount_out=quote.amount_out,
            gas_estimate=quote.gas_estimate,
            source="uniswap_v4_quoter",
            venue_binding_hash=verified.binding.binding_hash,
            metadata={
                "fee_tier": quote.fee_tier,
                "pool_key": selection.key.to_wire(),
                "pool_id": selection.key.pool_id,
                "quote_block": verified.evidence.block_number,
                "quote_block_hash": verified.evidence.block_hash,
                "slippage_reference": SLIPPAGE_REFERENCE_UNSUPPORTED,
            },
        )


__all__ = ["UniswapV4SwapQuoteConnector"]
