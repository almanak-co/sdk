"""Gateway transport for the backtest CoinGecko lanes (ALM-2952).

The platform runner holds no CoinGecko key (#3300), so direct HTTP there is
free-tier and 429s on real prefetches. Routes historical price reads and
contract-address resolution through the gateway's pro-key CoinGecko RPCs.
Historical price reads retain ALM-2952's operator-side fallback semantics;
contract resolution fails closed whenever this transport is configured so a
hosted runner can never silently switch to keyless vendor HTTP (ALM-3153).
"""

from __future__ import annotations

import logging
import re
from typing import Any

from .gateway_transport import GatewayTransportBase, gateway_backtest_configured

logger = logging.getLogger(__name__)

_MARKET_CHART_RANGE = re.compile(r"^/coins/(?P<token_id>[^/]+)/market_chart/range$")
_HISTORY = re.compile(r"^/coins/(?P<token_id>[^/]+)/history$")
_CONTRACT = re.compile(r"^/coins/(?P<asset_platform>[^/]+)/contract/(?P<contract_address>[^/]+)$")

# Backtest-cluster gateway presence signal; CG-named alias kept for the
# benchmark/crisis importers.
gateway_coingecko_configured = gateway_backtest_configured


class CoinGeckoGatewayResolutionError(RuntimeError):
    """Gateway contract resolution failed; the response is not an honest miss."""


class CoinGeckoGatewayUnavailableError(CoinGeckoGatewayResolutionError):
    """Configured gateway cannot serve contract resolution; direct fallback is forbidden."""


class CoinGeckoGatewayProviderError(ValueError):
    """The gateway was reachable but could not serve the CoinGecko operation."""


class GatewayCoinGeckoTransport(GatewayTransportBase):
    """Serves CoinGecko REST-shaped requests over the gateway's CG RPCs.

    ``request()`` returns the REST JSON shape callers already parse, or
    ``None`` when an endpoint is unmapped. Historical endpoints also return
    ``None`` when the gateway is unreachable so operator-side callers retain
    their fallback. Contract resolution instead raises
    :class:`CoinGeckoGatewayUnavailableError` and fails closed.
    """

    lane_label = "CoinGecko price"

    async def request(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any] | None:
        contract_match = _CONTRACT.match(endpoint)
        if self._dead:
            if contract_match:
                raise self._contract_unavailable()
            return None
        match = _MARKET_CHART_RANGE.match(endpoint)
        if match:
            return await self._market_chart_range(match.group("token_id"), params)
        match = _HISTORY.match(endpoint)
        if match:
            return await self._history(match.group("token_id"), params)
        if contract_match:
            return await self._contract(
                contract_match.group("asset_platform"),
                contract_match.group("contract_address"),
            )
        return None

    async def market_chart_range(
        self, token_id: str, start_ts: int, end_ts: int, vs_currency: str = "usd"
    ) -> dict[str, Any] | None:
        """Semantic entry point for callers that don't speak REST paths."""
        return await self.request(
            f"/coins/{token_id}/market_chart/range",
            {"vs_currency": vs_currency, "from": str(start_ts), "to": str(end_ts)},
        )

    async def _market_chart_range(self, token_id: str, params: dict[str, Any]) -> dict[str, Any] | None:
        handles = await self._ensure()
        if handles is None:
            return None
        client, pb2 = handles
        request = pb2.CoinGeckoMarketChartRangeRequest(
            token_id=token_id,
            from_timestamp=int(params.get("from", 0)),
            to_timestamp=int(params.get("to", 0)),
            vs_currency=str(params.get("vs_currency", "usd")),
        )
        response, app_error = await self._call_unary(
            client.integration.CoinGeckoGetMarketChartRange, request, rpc_name="CoinGeckoGetMarketChartRange"
        )
        if app_error is not None:
            raise CoinGeckoGatewayProviderError(f"CoinGecko API error via gateway: {app_error}")
        if response is None:
            return None
        if not response.success:
            raise CoinGeckoGatewayProviderError(f"CoinGecko API error via gateway: {response.error or 'unknown error'}")
        self._announce_serving()
        # Values stay strings: consumers parse Decimal(str(x)), lossless.
        return {
            "prices": [[point.timestamp, point.value] for point in response.prices],
            "market_caps": [[point.timestamp, point.value] for point in response.market_caps],
            "total_volumes": [[point.timestamp, point.value] for point in response.total_volumes],
        }

    async def _history(self, token_id: str, params: dict[str, Any]) -> dict[str, Any] | None:
        handles = await self._ensure()
        if handles is None:
            return None
        client, pb2 = handles
        request = pb2.CoinGeckoHistoricalPriceRequest(
            token_id=token_id,
            date=str(params.get("date", "")),
        )
        response, app_error = await self._call_unary(
            client.integration.CoinGeckoGetHistoricalPrice, request, rpc_name="CoinGeckoGetHistoricalPrice"
        )
        if app_error is not None:
            raise CoinGeckoGatewayProviderError(f"CoinGecko API error via gateway: {app_error}")
        if response is None:
            return None
        if not response.success:
            raise CoinGeckoGatewayProviderError(f"CoinGecko API error via gateway: {response.error or 'unknown error'}")
        self._announce_serving()
        price_usd = response.price_usd
        if not price_usd or price_usd == "0":
            # Mirror the REST no-data shape so the caller raises its honest
            # ValueError instead of pricing at $0.
            return {}
        return {"market_data": {"current_price": {"usd": price_usd}}}

    async def _contract(self, asset_platform: str, contract_address: str) -> dict[str, Any]:
        handles = await self._ensure(allow_direct_fallback=False)
        if handles is None:
            raise self._contract_unavailable()
        client, pb2 = handles
        request = pb2.CoinGeckoResolveContractRequest(
            asset_platform=asset_platform,
            contract_address=contract_address,
        )
        response, app_error = await self._call_unary(
            client.integration.CoinGeckoResolveContract,
            request,
            rpc_name="CoinGeckoResolveContract",
            allow_direct_fallback=False,
        )
        if app_error is not None:
            raise CoinGeckoGatewayResolutionError(f"CoinGecko contract resolution error via gateway: {app_error}")
        if response is None:
            raise self._contract_unavailable()
        if not response.found:
            self._announce_contract_resolution(response.source or "gateway")
            return {}
        if not response.coin_id:
            raise CoinGeckoGatewayResolutionError(
                "CoinGecko contract resolution gateway returned found=true without a coin id"
            )
        self._announce_contract_resolution(response.source or "gateway")
        return {"id": response.coin_id}

    @staticmethod
    def _contract_unavailable() -> CoinGeckoGatewayUnavailableError:
        return CoinGeckoGatewayUnavailableError(
            "CoinGecko contract resolution unavailable via configured gateway; direct HTTP fallback is disabled"
        )

    @staticmethod
    def _announce_contract_resolution(source: str) -> None:
        logger.info(
            "CoinGecko contract-resolution lane served via gateway (transport=gateway_coingecko_contract source=%s)",
            source,
        )


_shared_transport: GatewayCoinGeckoTransport | None = None


def shared_gateway_transport() -> GatewayCoinGeckoTransport | None:
    """Process-wide transport for one-shot callers (benchmark series)."""
    global _shared_transport
    if not gateway_coingecko_configured():
        return None
    if _shared_transport is None:
        _shared_transport = GatewayCoinGeckoTransport()
    return _shared_transport
