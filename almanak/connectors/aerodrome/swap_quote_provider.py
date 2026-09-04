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
    """Quote exact-input Aerodrome swaps through pool/router state.

    A Slipstream (CL) quote goes through the quoter of the reviewed generation
    that owns the pool: an exact ``pool_address`` selects it by the pool's own
    ``factory()``; a symbolic pair + tick spacing is asked of every reviewed
    generation and must resolve to exactly one.
    """

    protocol: ClassVar[ProtocolName] = ProtocolName("aerodrome")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def quote_swap(self, ctx, request: SwapQuoteRequest) -> SwapQuoteResult:
        if request.protocol != "aerodrome":
            raise SwapQuoteUnavailable(f"AerodromeSwapQuoteConnector cannot quote {request.protocol}")

        from almanak.connectors.aerodrome.adapter import AerodromeAdapter, AerodromeConfig
        from almanak.connectors.aerodrome.addresses import slipstream_lp_deployments

        rpc_url = getattr(ctx, "rpc_url", None)
        gateway_client = getattr(ctx, "gateway_client", None)
        config = AerodromeConfig(
            chain=request.chain,
            wallet_address=getattr(ctx, "wallet_address", "0x0000000000000000000000000000000000000000"),
            price_provider={},
            allow_placeholder_prices=True,
            rpc_url=rpc_url,
            gateway_client=gateway_client,
        )
        adapter = AerodromeAdapter(config, token_resolver=getattr(ctx, "token_resolver", None))
        stable = bool(request.extra.get("stable", False))
        use_cl = (
            bool(request.extra["use_cl"])
            if "use_cl" in request.extra
            else any(deployment.swap_router for deployment in slipstream_lp_deployments(request.chain))
        )
        try:
            tick_spacing = int(request.extra.get("tick_spacing", 100))
            deployment = None
            if use_cl:
                deployment, tick_spacing = _resolve_cl_venue(
                    request, tick_spacing, rpc_url=rpc_url, gateway_client=gateway_client
                )
            amount_out = adapter.quote_swap_output(
                token_in=request.token_in,
                token_out=request.token_out,
                amount_in_wei=request.amount_in,
                stable=stable,
                tick_spacing=tick_spacing,
                use_cl=use_cl,
                require_onchain=True,
                deployment=deployment,
            )
        except Exception as exc:
            raise SwapQuoteUnavailable(f"Aerodrome quote unavailable: {exc}") from exc

        metadata = {"stable": stable, "use_cl": use_cl, "tick_spacing": tick_spacing}
        if deployment is not None:
            metadata["slipstream_deployment"] = deployment.generation
            metadata["quoter"] = deployment.quoter
        return SwapQuoteResult(
            amount_out=amount_out,
            source="aerodrome_cl_quoter" if use_cl else "aerodrome_router_getAmountsOut",
            metadata=metadata,
        )


def _resolve_cl_venue(request: SwapQuoteRequest, tick_spacing: int, *, rpc_url, gateway_client):
    """Return ``(deployment, tick_spacing)`` for the reviewed generation that owns the quoted pool."""
    from almanak.connectors.aerodrome.addresses import slipstream_deployment_for_factory
    from almanak.connectors.aerodrome.pool_validation import (
        read_slipstream_cl_pool_binding,
        resolve_slipstream_pool_key,
        validate_aerodrome_cl_pool,
    )

    if request.pool_address:
        pool = request.pool_address
        binding = read_slipstream_cl_pool_binding(pool, rpc_url, chain=request.chain, gateway_client=gateway_client)
        if binding is None:
            raise ValueError(f"pool {pool} does not answer the Slipstream pool ABI")
        if {binding.token0, binding.token1} != {request.token_in.lower(), request.token_out.lower()}:
            raise ValueError(
                f"pool {pool} holds {binding.token0}/{binding.token1}, not {request.token_in}/{request.token_out}"
            )
        deployment = slipstream_deployment_for_factory(request.chain, binding.factory)
        if deployment is None:
            raise ValueError(f"pool {pool} reports unreviewed Slipstream factory {binding.factory}")
        # The pool's self-reported tuple must round-trip through the reviewed
        # factory it claims; a lookalike that merely answers the ABI fails here.
        check = validate_aerodrome_cl_pool(
            request.chain,
            binding.token0,
            binding.token1,
            binding.tick_spacing,
            rpc_url,
            gateway_client=gateway_client,
            deployment=deployment,
        )
        if check.exists is not True or (check.pool_address or "").lower() != pool.lower():
            raise ValueError(
                f"pool {pool} is not the {deployment.generation} Slipstream factory's pool for "
                f"{binding.token0}/{binding.token1} tick_spacing {binding.tick_spacing} "
                f"(factory returned {check.pool_address or check.error or check.warning or 'nothing'})"
            )
        return deployment, binding.tick_spacing
    resolution = resolve_slipstream_pool_key(
        request.chain, request.token_in, request.token_out, tick_spacing, rpc_url, gateway_client
    )
    match = resolution.unique
    if match is None:
        outcome = resolution.validation_result()
        raise ValueError(outcome.error or outcome.warning or "no reviewed Slipstream generation owns the pool")
    return match.deployment, tick_spacing


__all__ = ["AerodromeSwapQuoteConnector"]
