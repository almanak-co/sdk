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

        from almanak.connectors.curve.adapter import CurveAdapter, CurveConfig

        try:
            config = CurveConfig(
                chain=request.chain,
                wallet_address=getattr(ctx, "wallet_address", "0x0000000000000000000000000000000000000000"),
                rpc_url=getattr(ctx, "rpc_url", None),
                gateway_client=getattr(ctx, "gateway_client", None),
            )
            adapter = CurveAdapter(config, token_resolver=getattr(ctx, "token_resolver", None))

            # ALM-2896: callers that only have a token pair (e.g. the framework
            # estimate_slippage AMM fallback) may omit pool_address. Resolve it
            # from the connector's own pool registry over the public adapter
            # surface — keeps Curve pool resolution inside the connector.
            pool_address = request.pool_address or self._resolve_pool_for_pair(
                adapter, request.token_in, request.token_out
            )
            if not pool_address:
                raise SwapQuoteUnavailable(
                    f"No Curve pool for {request.token_in}->{request.token_out} on {request.chain}"
                )

            amount_out = adapter.quote_swap_output(
                pool_address=pool_address,
                token_in=request.token_in,
                token_out=request.token_out,
                amount_in_wei=request.amount_in,
            )
        except SwapQuoteUnavailable:
            raise
        except Exception as exc:
            raise SwapQuoteUnavailable(f"Curve quote unavailable: {exc}") from exc

        return SwapQuoteResult(
            amount_out=amount_out,
            source="curve_pool_get_dy",
            metadata={"pool_address": pool_address},
        )

    @staticmethod
    def _resolve_pool_for_pair(adapter, token_in: str, token_out: str) -> str | None:
        """Find a Curve pool whose coins include both tokens (symbol or address).

        Reads only the public adapter surface (``pools`` / ``get_pool_info`` /
        ``PoolInfo.get_coin_index``) so it stays decoupled from internal pool
        representation.
        """
        for pool_data in adapter.pools.values():
            pool_address = pool_data.get("address")
            if not pool_address:
                continue
            # Pair resolution only needs the (static) coin set to test membership;
            # opt out of the per-pool network reconcile (VIB-5423) so scanning the
            # registry for a pair doesn't amplify into O(pools) chain reads.
            info = adapter.get_pool_info(pool_address, refresh=False)
            if info is None:
                continue
            try:
                info.get_coin_index(token_in)
                info.get_coin_index(token_out)
            except ValueError:
                continue
            return pool_address
        return None


__all__ = ["CurveSwapQuoteConnector"]
