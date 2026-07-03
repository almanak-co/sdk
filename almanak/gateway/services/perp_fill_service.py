"""PerpFillService implementation — per-fill economics + funding deltas (VIB-5595).

CoreWriter-style async-settlement perp venues (Hyperliquid first) settle orders
**off the EVM** on their own matching engine, so the submit receipt carries no
fill price, fee, realized PnL, or funding. That data lives on the venue's Info
API (``api.hyperliquid.xyz/info`` ``userFills`` / ``userFunding``). This service
is the gateway-side reader: the HTTP egress belongs in the sidecar, never in the
strategy container (AGENTS.md §"Gateway boundary").

Dispatch is registry-driven exactly like ``FundingRateService``: each perp
connector that reads fills declares :class:`GatewayPerpFillsCapability` on its
gateway provider, and the servicer resolves the provider by venue at construction
time (O(1) dispatch, no per-request registry walk). Adding a new async-settlement
perp venue is a pure connector registration — no edit to this file.

Empty ≠ Zero: a field the venue did not report is unmeasured (empty string on the
wire), never a fabricated ``0``. A wallet with no fills for the window returns an
empty list with ``success=True`` (a measured empty book), distinct from an RPC
failure which returns ``success=False``.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

import aiohttp
import grpc

from almanak.connectors._base.gateway_capabilities import (
    GatewayOrderStatusCapability,
    GatewayPerpFillsCapability,
)
from almanak.connectors._gateway_registry import GATEWAY_REGISTRY
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.utils.ssl_context import build_ssl_context

logger = logging.getLogger(__name__)


# =============================================================================
# Gateway-side data models (connector fetch methods return these; the servicer
# maps them to the proto envelope). Kept gateway-side so ``_base/`` stays free
# of gateway internals.
# =============================================================================


@dataclass(frozen=True)
class PerpFillData:
    """One executed fill.

    Every economics field is a string so Empty ≠ Zero survives the boundary: an
    empty string is "the venue did not report this field", NEVER ``"0"``. A
    measured zero is the literal ``"0"``. Amount fields are human units
    (decimal-as-string).
    """

    coin: str = ""
    px: str = ""
    sz: str = ""
    dir: str = ""
    fee: str = ""
    closed_pnl: str = ""
    oid: str = ""
    cloid: str = ""
    time_ms: int = 0
    crossed: bool = False
    fee_token: str = ""


@dataclass(frozen=True)
class PerpFundingData:
    """One funding settlement (delta)."""

    coin: str = ""
    usdc: str = ""
    funding_rate: str = ""
    time_ms: int = 0


@dataclass
class PerpFillResult:
    """Connector return for ``fetch_user_fills``.

    ``ok=False`` means the read itself failed (RPC / decode) — the framework must
    treat the fills as UNMEASURED (not an empty book). ``ok=True`` with an empty
    ``fills`` list is a measured empty book (no fills in the window).
    """

    fills: list[PerpFillData] = field(default_factory=list)
    ok: bool = True
    error: str = ""


@dataclass
class PerpFundingResult:
    """Connector return for ``fetch_user_funding`` (see :class:`PerpFillResult`)."""

    deltas: list[PerpFundingData] = field(default_factory=list)
    ok: bool = True
    error: str = ""


@dataclass(frozen=True)
class OrderStatusData:
    """Connector return for ``fetch_order_status`` — an already-parsed verdict.

    The connector runs its own pure ``orderStatus`` parser internally and returns
    this gateway-side typed result (symmetric with :class:`PerpFillResult` for the
    fills lane) so the gateway holds NO connector import — the venue status
    vocabulary stays connector-owned; only the neutral verdict crosses the
    boundary (blueprint 22 §"Both sides need it → define on the gateway side").

    Every field is a string so Empty ≠ Zero survives: ``filled_size`` /
    ``avg_fill_price`` are ``""`` when the venue did not report them (the parser
    left ``None``), NEVER a fabricated ``"0"``. ``status`` is ``str(FillStatus)``
    (e.g. ``"filled"`` / ``"rejected"`` / ``"resting"`` / ``"unmeasured"``); a
    malformed / unknown payload parses to ``"unmeasured"`` (never a fabricated
    fill or reject). A transport fault raises before this is built — the servicer
    maps that to ``success=False`` / UNAVAILABLE.
    """

    status: str = ""
    filled_size: str = ""
    avg_fill_price: str = ""
    detail: str = ""


def _parse_cloid(cloid_raw: str) -> tuple[int | None, str]:
    """Parse the wire ``cloid`` (decimal string of a uint128) to an int.

    Returns ``(cloid, "")`` on success, or ``(None, error)`` when the value is
    missing / non-numeric / out of the uint128 range. Empty ≠ Zero: a missing
    cloid is an error, not a silent 0 (which would query the wrong order).
    """
    if not cloid_raw:
        return None, "cloid is required"
    try:
        cloid = int(cloid_raw)
    except (TypeError, ValueError):
        return None, f"cloid must be a decimal uint128 string, got {cloid_raw!r}"
    if cloid <= 0 or cloid >= 2**128:
        return None, f"cloid out of uint128 range: {cloid_raw!r}"
    return cloid, ""


def _order_status_to_proto(data: OrderStatusData) -> gateway_pb2.OrderStatusResponse:
    """Map an already-parsed :class:`OrderStatusData` verdict to the proto.

    The connector ran its own pure parser (single source of truth for the venue
    status vocabulary) inside ``fetch_order_status`` and returned the neutral,
    gateway-side :class:`OrderStatusData` — so the gateway holds NO connector
    import. This is a pure field copy. Empty ≠ Zero: ``filled_size`` /
    ``avg_fill_price`` are already ``""`` when unmeasured (never ``"0"``). We
    stamp ``success=True`` because the READ itself was measured (the servicer
    surfaces a transport fault as ``success=False`` before reaching here).
    """
    return gateway_pb2.OrderStatusResponse(
        status=data.status,
        filled_size=data.filled_size,
        avg_fill_price=data.avg_fill_price,
        detail=data.detail,
        success=True,
    )


class PerpFillServiceServicer(gateway_pb2_grpc.PerpFillServiceServicer):
    """gRPC servicer for per-fill economics + funding deltas.

    Registry-driven dispatch: ``_fills_providers`` maps ``venue -> connector``,
    built once from the connectors advertising ``GatewayPerpFillsCapability``.
    """

    def __init__(self, settings: GatewaySettings) -> None:
        self.settings = settings
        self._http_session: aiohttp.ClientSession | None = None

        # venue (lowercase) -> capability provider. Resolved once so dispatch is
        # O(1). Duplicate venue ids across two connectors are a hard error (the
        # registry only guards unique ProtocolName, not unique fills_venue()).
        self._fills_providers: dict[str, GatewayPerpFillsCapability] = {}
        for connector in GATEWAY_REGISTRY.capability_providers(GatewayPerpFillsCapability):  # type: ignore[type-abstract]
            venue = connector.fills_venue().lower()
            existing = self._fills_providers.get(venue)
            if existing is not None and existing is not connector:
                raise RuntimeError(
                    f"Duplicate perp-fills provider for venue {venue!r}: "
                    f"{type(existing).__qualname__} vs {type(connector).__qualname__}"
                )
            self._fills_providers[venue] = connector

        # venue (lowercase) -> order-status provider (VIB-5616). A separate
        # capability from GatewayPerpFillsCapability: the reject-detection lane
        # reads a single order's status by cloid, not the whole fills book.
        self._order_status_providers: dict[str, GatewayOrderStatusCapability] = {}
        # Distinct loop variable (not ``connector``): reusing the name from the
        # fills loop above pins its ``GatewayPerpFillsCapability`` type and makes
        # mypy reject ``order_status_venue()`` / the dict assignment here.
        for os_connector in GATEWAY_REGISTRY.capability_providers(GatewayOrderStatusCapability):  # type: ignore[type-abstract]
            venue = os_connector.order_status_venue().lower()
            existing_os = self._order_status_providers.get(venue)
            if existing_os is not None and existing_os is not os_connector:
                raise RuntimeError(
                    f"Duplicate order-status provider for venue {venue!r}: "
                    f"{type(existing_os).__qualname__} vs {type(os_connector).__qualname__}"
                )
            self._order_status_providers[venue] = os_connector

        logger.debug(
            "Initialized PerpFillService (fills venues=%s, order-status venues=%s)",
            sorted(self._fills_providers.keys()),
            sorted(self._order_status_providers.keys()),
        )

    async def _get_http_session(self) -> aiohttp.ClientSession:
        """Get or create the shared HTTP session (bounded timeout)."""
        if self._http_session is None or self._http_session.closed:
            connector = aiohttp.TCPConnector(ssl=build_ssl_context())
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10.0),
                connector=connector,
            )
        return self._http_session

    # ------------------------------------------------------------------ RPCs
    def _resolve_request(
        self,
        venue_raw: str,
        wallet_address: str,
        context: grpc.aio.ServicerContext,
    ) -> tuple[GatewayPerpFillsCapability | None, str]:
        """Validate the common request preconditions shared by both RPCs.

        Returns ``(connector, "")`` on success, or ``(None, error)`` when the
        venue is unknown or the wallet address is missing. On the error path the
        gRPC ``context`` is already stamped with ``INVALID_ARGUMENT`` + details;
        the caller only has to build its own typed error envelope with the
        returned message. Extracting this keeps each RPC method's cyclomatic
        complexity low (the two guard branches live here, once) — the RPCs stay
        thin dispatchers, mirroring ``FundingRateService``'s helper-delegation
        shape.
        """
        venue = venue_raw.lower()
        connector = self._fills_providers.get(venue)
        if connector is None:
            msg = f"Unknown venue: {venue}"
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(msg)
            return None, msg
        if not wallet_address:
            msg = "wallet_address is required"
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(msg)
            return None, msg
        return connector, ""

    async def GetUserFills(
        self,
        request: gateway_pb2.UserFillsRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.UserFillsResponse:
        venue = request.venue.lower()
        connector, error = self._resolve_request(request.venue, request.wallet_address, context)
        if connector is None:
            return gateway_pb2.UserFillsResponse(success=False, error=error)

        start_time = time.time()
        try:
            result = await connector.fetch_user_fills(
                self,
                wallet_address=request.wallet_address,
                coin=request.coin,
                start_ts=request.start_time_ms,
            )
        except Exception as exc:  # noqa: BLE001 — surface as unavailable, do not crash the servicer
            logger.warning("GetUserFills failed for %s/%s: %s", venue, request.wallet_address, exc)
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(str(exc))
            return gateway_pb2.UserFillsResponse(success=False, error=str(exc))

        latency_ms = (time.time() - start_time) * 1000
        logger.debug("GetUserFills for %s completed in %.1fms (%d fills)", venue, latency_ms, len(result.fills))

        if not result.ok:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(result.error or "fills read failed")
            return gateway_pb2.UserFillsResponse(success=False, error=result.error or "fills read failed")

        return gateway_pb2.UserFillsResponse(
            fills=[self._fill_to_proto(f) for f in result.fills],
            success=True,
        )

    async def GetUserFunding(
        self,
        request: gateway_pb2.UserFundingRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.UserFundingResponse:
        venue = request.venue.lower()
        connector, error = self._resolve_request(request.venue, request.wallet_address, context)
        if connector is None:
            return gateway_pb2.UserFundingResponse(success=False, error=error)

        try:
            result = await connector.fetch_user_funding(
                self,
                wallet_address=request.wallet_address,
                coin=request.coin,
                start_ts=request.start_time_ms,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("GetUserFunding failed for %s/%s: %s", venue, request.wallet_address, exc)
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(str(exc))
            return gateway_pb2.UserFundingResponse(success=False, error=str(exc))

        if not result.ok:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(result.error or "funding read failed")
            return gateway_pb2.UserFundingResponse(success=False, error=result.error or "funding read failed")

        return gateway_pb2.UserFundingResponse(
            deltas=[self._funding_to_proto(d) for d in result.deltas],
            success=True,
        )

    async def GetOrderStatus(
        self,
        request: gateway_pb2.OrderStatusRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.OrderStatusResponse:
        """Status of a single submitted order by ``cloid`` (VIB-5616).

        Reject-detection lane for the fill-reconciliation pump: ``userFills``
        alone cannot tell "not filled yet" from "rejected". This reads the
        venue ``orderStatus`` (egress in the connector's gateway provider),
        runs the connector's pure parser, and returns the typed verdict.

        Empty ≠ Zero: an unknown venue / missing wallet / malformed cloid is
        INVALID_ARGUMENT; a transport / decode fault is UNAVAILABLE — both
        yield ``success=False`` so the caller stays PENDING, never fabricating
        a fill or reject.
        """
        venue = request.venue.lower()
        connector = self._order_status_providers.get(venue)
        if connector is None:
            msg = f"Unknown venue: {venue}"
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(msg)
            return gateway_pb2.OrderStatusResponse(success=False, error=msg)
        if not request.wallet_address:
            msg = "wallet_address is required"
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(msg)
            return gateway_pb2.OrderStatusResponse(success=False, error=msg)

        cloid, cloid_err = _parse_cloid(request.cloid)
        if cloid is None:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(cloid_err)
            return gateway_pb2.OrderStatusResponse(success=False, error=cloid_err)

        try:
            status_data = await connector.fetch_order_status(
                self,
                wallet_address=request.wallet_address,
                cloid=cloid,
                chain=request.chain,
            )
        except Exception as exc:  # noqa: BLE001 — surface as unmeasured, never fabricate a verdict
            logger.warning("GetOrderStatus failed for %s/%s: %s", venue, request.wallet_address, exc)
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(str(exc))
            return gateway_pb2.OrderStatusResponse(success=False, error=str(exc))

        return _order_status_to_proto(status_data)

    # --------------------------------------------------------------- helpers
    @staticmethod
    def _fill_to_proto(fill: PerpFillData) -> gateway_pb2.PerpFill:
        return gateway_pb2.PerpFill(
            coin=fill.coin,
            px=fill.px,
            sz=fill.sz,
            dir=fill.dir,
            fee=fill.fee,
            closed_pnl=fill.closed_pnl,
            oid=fill.oid,
            cloid=fill.cloid,
            time_ms=fill.time_ms,
            crossed=fill.crossed,
            fee_token=fill.fee_token,
        )

    @staticmethod
    def _funding_to_proto(delta: PerpFundingData) -> gateway_pb2.PerpFundingDelta:
        return gateway_pb2.PerpFundingDelta(
            coin=delta.coin,
            usdc=delta.usdc,
            funding_rate=delta.funding_rate,
            time_ms=delta.time_ms,
        )

    async def close(self) -> None:
        """Close the HTTP session (called by the gateway shutdown loop)."""
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
            self._http_session = None
        logger.info("PerpFillService closed")


__all__ = [
    "OrderStatusData",
    "PerpFillData",
    "PerpFillResult",
    "PerpFillServiceServicer",
    "PerpFundingData",
    "PerpFundingResult",
]
