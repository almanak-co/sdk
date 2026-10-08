"""Gateway-side connector binding for Aster Pro.

Registers the Aster Pro trading servicer and publishes Aster's funding rates on
the gateway's shared funding lanes (``GatewayFundingRateCapability`` for the
live rate, ``GatewayFundingHistoryCapability`` for settled history). Both lanes
read Aster's public endpoints at the same base URL the trading client uses and
report rates per hour; see ``funding_rates`` for the unit conversion.
"""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    FundingHistorySource,
    GatewayFundingHistoryCapability,
    GatewayFundingRateCapability,
    GatewayServicerCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors.aster_perps.markets import FUNDING_MARKETS as _FUNDING_MARKETS
from almanak.connectors.aster_perps.proto import aster_perps_pb2_grpc
from almanak.integrations._base.gateway.base import RateLimiter

from . import funding_rates
from .service import AsterPerpsServiceServicer

FUNDING_MARKETS = frozenset(_FUNDING_MARKETS)

# Every public funding request is throttled here, not per operation: a history
# read is an interval lookup plus up to 40 pages. Aster's request-weight limit is
# per IP and shared with this gateway's trading calls, so funding reads stay at
# a small fraction of it.
_FUNDING_REQUESTS_PER_MINUTE = 300
_FUNDING_REQUEST_BURST = 20


class AsterPerpsGatewayConnector(
    GatewayConnector,
    GatewayServicerCapability,
    GatewayFundingRateCapability,
    GatewayFundingHistoryCapability,
):
    """Registers the Aster Pro gRPC servicer and serves Aster funding rates."""

    protocol: ClassVar[ProtocolName] = ProtocolName("aster_perps")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def __init__(self) -> None:
        self._servicer: AsterPerpsServiceServicer | None = None
        self._request_limiter: RateLimiter | None = None

    def _throttle(self) -> RateLimiter:
        if self._request_limiter is None:
            self._request_limiter = RateLimiter(
                requests_per_minute=_FUNDING_REQUESTS_PER_MINUTE, bucket_size=_FUNDING_REQUEST_BURST
            )
        return self._request_limiter

    @property
    def servicer(self) -> AsterPerpsServiceServicer | None:
        return self._servicer

    def register_servicers(self, server: Any, settings: Any) -> None:
        self._servicer = AsterPerpsServiceServicer(settings)
        aster_perps_pb2_grpc.add_AsterPerpsServiceServicer_to_server(self._servicer, server)

    def venue(self) -> str:
        return funding_rates.VENUE

    async def fetch_funding_rate(
        self,
        servicer: Any,
        market: str,
        chain: str,
        market_address: str = "",
    ) -> Any:
        """Live Aster funding, per hour. Off-chain venue: ``chain`` and ``market_address`` carry no identity."""
        del chain
        if market_address.strip():
            raise funding_rates.AsterUnknownMarket("Aster Pro markets have no on-chain address; pass the market symbol")
        if market not in FUNDING_MARKETS:
            raise funding_rates.AsterUnknownMarket(f"Aster funding is not served for market {market!r}")
        return await funding_rates.fetch_live_funding_rate(
            await servicer._get_http_session(),
            funding_rates.base_url_from_settings(servicer.settings),
            market,
            self._throttle().acquire,
        )

    def funding_venue(self) -> str:
        return funding_rates.VENUE

    def funding_supported_markets(self) -> frozenset[str]:
        return FUNDING_MARKETS

    def funding_history_source(self, chain: str) -> FundingHistorySource:
        del chain
        return FundingHistorySource(key="aster_pro_fapi", scope="", requests_per_minute=60, burst_size=10)

    async def fetch_funding_history(
        self,
        servicer: Any,
        *,
        market: str,
        market_address: str,
        chain: str,
        start_ts: int,
        end_ts: int,
    ) -> Any:
        """Settled Aster funding in ``[start_ts, end_ts]``, per hour, ascending."""
        from almanak.gateway.services.rate_history_service import (
            RateHistoryInvalidRequest,
            RateHistoryRateLimited,
            RateHistoryUnavailable,
        )

        del chain
        if market_address.strip():
            raise RateHistoryInvalidRequest("Aster Pro markets have no on-chain address; pass the market symbol")
        if market not in FUNDING_MARKETS:
            raise RateHistoryInvalidRequest(f"Aster funding history is not served for market {market!r}")
        try:
            return await funding_rates.fetch_funding_history(
                await servicer._get_http_session(),
                funding_rates.base_url_from_settings(servicer.settings),
                market,
                start_ts=start_ts,
                end_ts=end_ts,
                throttle=self._throttle().acquire,
            )
        except funding_rates.AsterUnknownMarket as exc:
            raise RateHistoryInvalidRequest(str(exc)) from exc
        except funding_rates.AsterFundingRateLimited as exc:
            raise RateHistoryRateLimited(funding_rates.VENUE, str(exc), retry_after=exc.retry_after) from exc
        except funding_rates.AsterFundingUnavailable as exc:
            raise RateHistoryUnavailable(funding_rates.VENUE, str(exc)) from exc


__all__ = ["FUNDING_MARKETS", "AsterPerpsGatewayConnector"]
