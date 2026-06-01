"""RateHistoryService implementation (VIB-4859 / W7 of epic VIB-4851).

Gateway-side dispatcher for lending APY / perp funding / DEX TWAP / DEX
volume data. Replaces the per-protocol dispatch + direct ``httpx`` /
``aiohttp`` / ``Web3(HTTPProvider(...))`` egress that lived in
``almanak/framework/data/rates/{monitor,history}.py`` and
``almanak/framework/backtesting/pnl/providers/{lending_apy,twap,dex/*}.py``
— all of which violated the gateway-boundary rule
(``AGENTS.md`` §"Gateway boundary").

Four-RPC service, mirroring the four sibling capabilities declared in
``almanak.connectors._base.gateway_capabilities``:

* :class:`GatewayLendingRateHistoryCapability` →
  ``GetLendingRateCurrent`` / ``GetLendingRateHistory``.
* :class:`GatewayFundingHistoryCapability` → ``GetFundingRateHistory``
  (sibling of the live-only ``FundingRateService.GetFundingRate``).
* :class:`GatewayDexTwapCapability` → ``GetDexTwap`` /
  ``GetDexTwapSeries``.
* :class:`GatewayDexVolumeCapability` → ``GetDexVolumeHistory``.

Each RPC follows the dispatcher pattern set by ``FundingRateService``:

1. Resolve ``request.<key>`` → capability provider at servicer
   construction (O(1) lookup at request time).
2. Validator-first: empty / unknown keys -> ``INVALID_ARGUMENT``.
3. Delegate to ``capability.fetch_*(self, …)``. The connector body
   receives the servicer (its shared HTTP session, web3 cache,
   settings) so connector code stays free of gateway plumbing.
4. ``DataSourceUnavailable`` from the connector → ``success=False``
   envelope (NO silent zeros / NO default-rate fallback).
5. Other exceptions → ``INTERNAL`` (preserved trailing message).

Strategy-side framework code reads :mod:`almanak.framework.gateway_client`
which holds a ``_rate_history_stub`` and translates the wire envelope to
:class:`almanak.framework.data.interfaces.DataSourceUnavailable` on
``success=False`` or non-OK status (matches the
``PoolHistoryReader`` / ``PoolAnalyticsReader`` precedents).

Phase 1 (this file): foundation skeleton + Aave V3 + Uniswap V3 prototype
wiring. Other connectors come online in Step 3 (lending cluster, perp
cluster, DEX cluster) per the plan PR #2473 migration plan.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import aiohttp
import grpc

from almanak.connectors._base.gateway_capabilities import (
    GatewayDexLwapCapability,
    GatewayDexTwapCapability,
    GatewayDexVolumeCapability,
    GatewayFundingHistoryCapability,
    GatewayLendingRateHistoryCapability,
)
from almanak.connectors._gateway_registry import GATEWAY_REGISTRY
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.utils.ssl_context import build_ssl_context

if TYPE_CHECKING:
    from web3 import AsyncWeb3

logger = logging.getLogger(__name__)


# =============================================================================
# Internal point dataclasses (server-side; never crosses the wire directly)
# =============================================================================
#
# Connector implementations return these typed objects and the dispatcher
# converts to the proto envelope. Living server-side (not in
# ``connectors/_base/``) keeps the foundation module free of gateway-only
# imports and lets us evolve the validator / serialiser logic without
# touching the capability Protocols.


@dataclass(frozen=True)
class LendingRatePoint:
    """One lending-rate observation.

    ``supply_apy_pct`` / ``borrow_apy_pct`` are percentages (e.g.
    ``Decimal("5.25")`` = 5.25%). ``None`` means "unmeasured by this
    provider for this row" — wire encoding is the empty string per the
    proto ``Empty != Zero`` convention. ``utilization_pct`` is 0..100
    (NOT a 0..1 ratio).

    Connectors MUST raise :class:`DataSourceUnavailable` rather than
    returning a point with all-``None`` numeric fields — the "no data"
    case belongs in the failure envelope, not the success one.
    """

    timestamp: int
    supply_apy_pct: Decimal | None = None
    borrow_apy_pct: Decimal | None = None
    utilization_pct: Decimal | None = None


@dataclass(frozen=True)
class FundingRatePoint:
    """One perp funding-rate observation. ``rate_hourly`` is a decimal
    rate (e.g. ``Decimal("0.00001")`` for 0.001%/h).
    """

    timestamp: int
    rate_hourly: Decimal | None = None
    rate_annualized: Decimal | None = None


@dataclass(frozen=True)
class DexTwapPoint:
    """One DEX TWAP observation. ``price`` is quote/base in human units."""

    timestamp: int
    price: Decimal
    tick_observation_count: int = 0


@dataclass(frozen=True)
class DexVolumePoint:
    """One DEX trading-volume observation. ``volume_usd`` is USD."""

    timestamp: int
    volume_usd: Decimal | None = None


@dataclass(frozen=True)
class DexLwapPoint:
    """One DEX liquidity-weighted spot observation.

    ``price`` is quote/base in human units; ``pool_count`` is the number of
    pools that contributed to the weighted average.
    """

    timestamp: int
    price: Decimal
    pool_count: int = 0


# =============================================================================
# Connector-facing exceptions
# =============================================================================
#
# Connectors raise these (or any subclass) when upstream data is missing
# / rate-limited / malformed. The dispatcher translates to the
# ``success=False`` envelope so the framework reader can map to
# ``DataSourceUnavailable`` uniformly. Defining the exception here keeps
# the connector body free of strategy-side imports.


class RateHistoryUnavailable(Exception):
    """Connector signalled "no data" — translated to ``success=False``.

    Carries an optional ``source`` and ``reason`` so the dispatcher can
    record an informative envelope. Connectors SHOULD prefer a typed
    raise over returning empty lists / default zeros.
    """

    def __init__(self, source: str, reason: str, retry_after: float | None = None) -> None:
        self.source = source
        self.reason = reason
        self.retry_after = retry_after
        super().__init__(f"{source}: {reason}")


# =============================================================================
# Validator helpers (cheap input-shape checks)
# =============================================================================

# Client-facing message for unexpected INTERNAL errors. The full
# exception is logged server-side; clients never see raw ``str(exc)``
# (which can leak upstream URLs / credentials / internals across the
# gateway boundary). CodeRabbit PR-review feedback (PR #2474).
_INTERNAL_ERROR_DETAIL = "internal server error"


def _normalize_key(value: str) -> str:
    return value.strip().lower()


def _invalid_argument(context: grpc.aio.ServicerContext, message: str) -> None:
    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
    context.set_details(message)


def _validate_window(
    start_ts: int,
    end_ts: int,
    context: grpc.aio.ServicerContext,
) -> str | None:
    if start_ts <= 0:
        msg = f"start_ts must be > 0 (unix seconds), got {start_ts}"
        _invalid_argument(context, msg)
        return msg
    if end_ts <= 0:
        msg = f"end_ts must be > 0 (unix seconds), got {end_ts}"
        _invalid_argument(context, msg)
        return msg
    if start_ts >= end_ts:
        msg = f"start_ts must be < end_ts (got start_ts={start_ts}, end_ts={end_ts})"
        _invalid_argument(context, msg)
        return msg
    return None


def _validate_twap_identity(
    *,
    dex: str,
    chain: str,
    pool_address: str,
    context: grpc.aio.ServicerContext,
) -> str | None:
    """Validate the ``(dex, chain, pool_address)`` triple; return error msg or ``None``."""
    if not dex:
        _invalid_argument(context, "dex is required")
        return "dex is required"
    if not chain:
        _invalid_argument(context, "chain is required")
        return "chain is required"
    if not pool_address:
        _invalid_argument(context, "pool_address is required")
        return "pool_address is required"
    return None


def _validate_twap_window_secs(
    *,
    secs_ago_start: int,
    secs_ago_end: int,
    context: grpc.aio.ServicerContext,
) -> str | None:
    """Validate ``observe(secondsAgos)`` window; return error msg or ``None``."""
    if secs_ago_start < 0 or secs_ago_end < 0:
        msg = f"secs_ago_* must be non-negative (got start={secs_ago_start}, end={secs_ago_end})"
        _invalid_argument(context, msg)
        return msg
    if secs_ago_start <= secs_ago_end:
        msg = f"secs_ago_start must be > secs_ago_end (got start={secs_ago_start}, end={secs_ago_end})"
        _invalid_argument(context, msg)
        return msg
    return None


def _validate_positive_interval(
    interval_secs: int | None,
    context: grpc.aio.ServicerContext,
) -> str | None:
    """Return error msg if ``interval_secs`` is set and not strictly positive."""
    if interval_secs is None or interval_secs > 0:
        return None
    msg = f"interval_secs must be > 0, got {interval_secs}"
    _invalid_argument(context, msg)
    return msg


def _validate_side(side: str, context: grpc.aio.ServicerContext) -> str | None:
    """``side`` is the literal ``"supply"`` / ``"borrow"`` the strategy
    submits. Anything else is a programming bug at the caller.
    """
    if side not in ("supply", "borrow"):
        msg = f"side must be 'supply' or 'borrow', got {side!r}"
        _invalid_argument(context, msg)
        return msg
    return None


# =============================================================================
# Servicer
# =============================================================================


class RateHistoryServiceServicer(gateway_pb2_grpc.RateHistoryServiceServicer):
    """Implements ``RateHistoryService`` gRPC interface.

    Holds a shared aiohttp session + web3 cache (lazy) so connector
    implementations can reuse them rather than each opening its own.
    Strategy-container ``framework/data/rates/*`` becomes a thin gRPC
    client of this servicer; all HTTP / Web3 egress lives HERE.
    """

    def __init__(self, settings: GatewaySettings) -> None:
        self.settings = settings
        self._http_session: aiohttp.ClientSession | None = None
        self._web3_cache: dict[str, AsyncWeb3] = {}
        # Guards the lazy build of ``_http_session`` / ``_web3_cache``
        # entries. Concurrent gRPC requests can otherwise race the
        # check-and-create and orphan a ``ClientSession`` / ``AsyncWeb3``,
        # leaking sockets. CodeRabbit PR-review feedback (PR #2474).
        self._resource_init_lock = asyncio.Lock()

        # Resolve capability providers once at construction; same O(1)
        # dispatcher pattern as ``FundingRateService``. Each provider
        # map is keyed by the lowercase identifier the strategy submits
        # in the request — protocol slug for lending, venue id for perp
        # funding, dex_name for DEX TWAP + volume.
        self._lending_providers: dict[str, GatewayLendingRateHistoryCapability] = {}
        # mypy: passing a ``@runtime_checkable`` Protocol class to
        # ``capability_providers`` trips ``type-abstract``; this is the
        # intentional dispatcher contract. Each loop binds its own
        # ``*_conn`` rather than reusing ``connector`` so mypy can narrow
        # the type per loop body.
        for lending_conn in GATEWAY_REGISTRY.capability_providers(GatewayLendingRateHistoryCapability):  # type: ignore[type-abstract]
            # ``connector.protocol`` is declared on ``GatewayConnector``;
            # mypy narrows to the Protocol type which doesn't list it.
            key = str(lending_conn.protocol).lower()  # type: ignore[attr-defined]
            existing_lending = self._lending_providers.get(key)
            if existing_lending is not None and existing_lending is not lending_conn:
                raise RuntimeError(
                    f"Duplicate lending-rate provider for protocol {key!r}: "
                    f"{type(existing_lending).__qualname__} vs "
                    f"{type(lending_conn).__qualname__}"
                )
            self._lending_providers[key] = lending_conn

        self._funding_providers: dict[str, GatewayFundingHistoryCapability] = {}
        for funding_conn in GATEWAY_REGISTRY.capability_providers(GatewayFundingHistoryCapability):  # type: ignore[type-abstract]
            venue = funding_conn.funding_venue().lower()
            existing_funding = self._funding_providers.get(venue)
            if existing_funding is not None and existing_funding is not funding_conn:
                raise RuntimeError(
                    f"Duplicate funding-history provider for venue {venue!r}: "
                    f"{type(existing_funding).__qualname__} vs "
                    f"{type(funding_conn).__qualname__}"
                )
            self._funding_providers[venue] = funding_conn

        self._twap_providers: dict[str, GatewayDexTwapCapability] = {}
        for twap_conn in GATEWAY_REGISTRY.capability_providers(GatewayDexTwapCapability):  # type: ignore[type-abstract]
            # ``dex_name()`` is declared on ``GatewayDexTwapCapability`` so
            # the registry's structural Protocol check enforces it; a DEX
            # reuses the same identifier as its ``GatewayDexQuoteCapability``
            # so a strategy doesn't carry two names for the same DEX.
            key = str(twap_conn.dex_name()).lower()
            existing_twap = self._twap_providers.get(key)
            if existing_twap is not None and existing_twap is not twap_conn:
                raise RuntimeError(
                    f"Duplicate DEX TWAP provider for dex {key!r}: "
                    f"{type(existing_twap).__qualname__} vs "
                    f"{type(twap_conn).__qualname__}"
                )
            self._twap_providers[key] = twap_conn

        self._lwap_providers: dict[str, GatewayDexLwapCapability] = {}
        for lwap_conn in GATEWAY_REGISTRY.capability_providers(GatewayDexLwapCapability):  # type: ignore[type-abstract]
            key = str(lwap_conn.dex_name()).lower()
            existing_lwap = self._lwap_providers.get(key)
            if existing_lwap is not None and existing_lwap is not lwap_conn:
                raise RuntimeError(
                    f"Duplicate DEX LWAP provider for dex {key!r}: "
                    f"{type(existing_lwap).__qualname__} vs "
                    f"{type(lwap_conn).__qualname__}"
                )
            self._lwap_providers[key] = lwap_conn

        self._volume_providers: dict[str, GatewayDexVolumeCapability] = {}
        for volume_conn in GATEWAY_REGISTRY.capability_providers(GatewayDexVolumeCapability):  # type: ignore[type-abstract]
            key = str(volume_conn.dex_name()).lower()
            existing_volume = self._volume_providers.get(key)
            if existing_volume is not None and existing_volume is not volume_conn:
                raise RuntimeError(
                    f"Duplicate DEX volume provider for dex {key!r}: "
                    f"{type(existing_volume).__qualname__} vs "
                    f"{type(volume_conn).__qualname__}"
                )
            self._volume_providers[key] = volume_conn

        logger.debug(
            "Initialized RateHistoryService (lending=%s, funding=%s, twap=%s, lwap=%s, volume=%s)",
            sorted(self._lending_providers.keys()),
            sorted(self._funding_providers.keys()),
            sorted(self._twap_providers.keys()),
            sorted(self._lwap_providers.keys()),
            sorted(self._volume_providers.keys()),
        )

    async def _get_http_session(self) -> aiohttp.ClientSession:
        """Lazily build the shared HTTP session (shared with connector bodies)."""
        if self._http_session is None or self._http_session.closed:
            async with self._resource_init_lock:
                # Double-checked: another coroutine may have built it
                # while we awaited the lock.
                if self._http_session is None or self._http_session.closed:
                    connector = aiohttp.TCPConnector(ssl=build_ssl_context())
                    self._http_session = aiohttp.ClientSession(
                        timeout=aiohttp.ClientTimeout(total=15.0),
                        connector=connector,
                    )
        return self._http_session

    async def _get_web3(self, chain: str) -> AsyncWeb3:
        """Lazily build a per-chain ``AsyncWeb3`` instance.

        Raises ``ValueError`` when no RPC URL is configured for the chain;
        the caller maps that to a ``DataSourceUnavailable`` envelope.
        """
        if chain in self._web3_cache:
            return self._web3_cache[chain]

        async with self._resource_init_lock:
            # Double-checked: another coroutine may have populated the
            # cache for this chain while we awaited the lock.
            if chain in self._web3_cache:
                return self._web3_cache[chain]

            from web3 import AsyncHTTPProvider, AsyncWeb3

            from almanak.gateway.utils import get_rpc_url

            network = self.settings.network
            rpc_url = get_rpc_url(chain, network=network)
            web3 = AsyncWeb3(
                AsyncHTTPProvider(rpc_url, request_kwargs={"ssl": build_ssl_context()}),
            )
            self._web3_cache[chain] = web3
            return web3

    async def close(self) -> None:
        """Server-shutdown hook (called by the server lifecycle)."""
        if self._http_session is not None and not self._http_session.closed:
            await self._http_session.close()
            self._http_session = None

    # ---------------------------------------------------------------------
    # Wire-encoding helpers (server-side dataclass → proto)
    # ---------------------------------------------------------------------

    @staticmethod
    def _encode_decimal(value: Decimal | None) -> str:
        """Empty string == unmeasured (per proto "Empty != Zero" rule)."""
        if value is None:
            return ""
        return str(value)

    @classmethod
    def _encode_lending_point(cls, p: LendingRatePoint) -> gateway_pb2.LendingRatePoint:
        return gateway_pb2.LendingRatePoint(
            timestamp=p.timestamp,
            supply_apy_pct=cls._encode_decimal(p.supply_apy_pct),
            borrow_apy_pct=cls._encode_decimal(p.borrow_apy_pct),
            utilization_pct=cls._encode_decimal(p.utilization_pct),
        )

    @classmethod
    def _encode_funding_point(cls, p: FundingRatePoint) -> gateway_pb2.FundingRatePoint:
        return gateway_pb2.FundingRatePoint(
            timestamp=p.timestamp,
            rate_hourly=cls._encode_decimal(p.rate_hourly),
            rate_annualized=cls._encode_decimal(p.rate_annualized),
        )

    @classmethod
    def _encode_twap_point(cls, p: DexTwapPoint) -> gateway_pb2.DexTwapPoint:
        return gateway_pb2.DexTwapPoint(
            timestamp=p.timestamp,
            price=cls._encode_decimal(p.price),
            tick_observation_count=p.tick_observation_count,
        )

    @classmethod
    def _encode_volume_point(cls, p: DexVolumePoint) -> gateway_pb2.DexVolumePoint:
        return gateway_pb2.DexVolumePoint(
            timestamp=p.timestamp,
            volume_usd=cls._encode_decimal(p.volume_usd),
        )

    @classmethod
    def _encode_lwap_point(cls, p: DexLwapPoint) -> gateway_pb2.DexLwapPoint:
        return gateway_pb2.DexLwapPoint(
            timestamp=p.timestamp,
            price=cls._encode_decimal(p.price),
            pool_count=p.pool_count,
        )

    @dataclass(frozen=True)
    class _TwapDispatchPrep:
        """Validation + provider-resolution bundle for a TWAP RPC."""

        provider: Any | None
        error_response: Any | None

    def _prepare_twap_dispatch(
        self,
        *,
        dex: str,
        chain: str,
        pool_address: str,
        context: grpc.aio.ServicerContext,
        response_cls: type,
        window_secs: tuple[int, int] | None = None,
        ts_window: tuple[int, int] | None = None,
        interval_secs: int | None = None,
    ) -> RateHistoryServiceServicer._TwapDispatchPrep:
        """Validate the request shape + resolve the TWAP provider.

        Returns a ``_TwapDispatchPrep``: ``.provider`` is the resolved
        provider when validation passes, otherwise ``.error_response`` is
        a populated ``response_cls`` ready to return.

        Pass ``window_secs=(start, end)`` for the single-observation lane
        (``observe(secondsAgos)`` window check); pass ``ts_window`` +
        ``interval_secs`` for the series lane.
        """
        validation_err = self._validate_twap_dispatch_inputs(
            dex=dex,
            chain=chain,
            pool_address=pool_address,
            context=context,
            window_secs=window_secs,
            ts_window=ts_window,
            interval_secs=interval_secs,
        )
        if validation_err is not None:
            return self._TwapDispatchPrep(None, response_cls(success=False, error=validation_err))

        provider, err = self._resolve_twap_provider(dex, chain, context)
        if provider is None:
            return self._TwapDispatchPrep(None, response_cls(success=False, error=err or "no provider"))

        return self._TwapDispatchPrep(provider, None)

    def _validate_twap_optional_inputs(
        self,
        *,
        context: grpc.aio.ServicerContext,
        window_secs: tuple[int, int] | None,
        ts_window: tuple[int, int] | None,
        interval_secs: int | None,
    ) -> str | None:
        """Validate the optional TWAP window / ts-window / interval shapes."""
        if window_secs is not None:
            err = _validate_twap_window_secs(
                secs_ago_start=window_secs[0],
                secs_ago_end=window_secs[1],
                context=context,
            )
            if err is not None:
                return err
        if ts_window is not None:
            err = _validate_window(ts_window[0], ts_window[1], context)
            if err is not None:
                return err
        return _validate_positive_interval(interval_secs, context)

    def _validate_twap_dispatch_inputs(
        self,
        *,
        dex: str,
        chain: str,
        pool_address: str,
        context: grpc.aio.ServicerContext,
        window_secs: tuple[int, int] | None,
        ts_window: tuple[int, int] | None,
        interval_secs: int | None,
    ) -> str | None:
        """Run all input-shape checks for a TWAP dispatch; return msg or ``None``."""
        err = _validate_twap_identity(dex=dex, chain=chain, pool_address=pool_address, context=context)
        if err is not None:
            return err
        return self._validate_twap_optional_inputs(
            context=context,
            window_secs=window_secs,
            ts_window=ts_window,
            interval_secs=interval_secs,
        )

    def _prepare_volume_dispatch(
        self,
        *,
        dex: str,
        chain: str,
        pool_address: str,
        ts_window: tuple[int, int],
        interval_secs: int,
        context: grpc.aio.ServicerContext,
    ) -> RateHistoryServiceServicer._TwapDispatchPrep:
        """Validate the request + resolve the DEX volume provider.

        Returns a ``_TwapDispatchPrep`` (re-used from the TWAP lane —
        same envelope shape: provider XOR error_response).
        """
        validation_err = self._validate_twap_dispatch_inputs(
            dex=dex,
            chain=chain,
            pool_address=pool_address,
            context=context,
            window_secs=None,
            ts_window=ts_window,
            interval_secs=interval_secs,
        )
        if validation_err is not None:
            return self._TwapDispatchPrep(
                None,
                gateway_pb2.DexVolumeHistoryResponse(success=False, error=validation_err),
            )

        provider, err = self._resolve_volume_provider(dex, chain, context)
        if provider is None:
            return self._TwapDispatchPrep(
                None,
                gateway_pb2.DexVolumeHistoryResponse(success=False, error=err or "no provider"),
            )

        return self._TwapDispatchPrep(provider, None)

    def _resolve_volume_provider(
        self,
        dex: str,
        chain: str,
        context: grpc.aio.ServicerContext,
    ) -> tuple[Any | None, str | None]:
        """Look up the DEX volume provider for ``dex`` and confirm chain support."""
        provider = self._volume_providers.get(dex)
        if provider is None:
            msg = f"unsupported dex (volume): {dex!r} (known: {sorted(self._volume_providers.keys())})"
            _invalid_argument(context, msg)
            return None, msg
        if chain not in provider.volume_supported_chains():
            msg = (
                f"dex {dex!r} does not support volume on chain {chain!r} "
                f"(supports: {sorted(provider.volume_supported_chains())})"
            )
            _invalid_argument(context, msg)
            return None, msg
        return provider, None

    def _resolve_twap_provider(
        self,
        dex: str,
        chain: str,
        context: grpc.aio.ServicerContext,
    ) -> tuple[Any | None, str | None]:
        """Look up the TWAP provider for ``dex`` and confirm chain support.

        Returns ``(provider, None)`` on success, or ``(None, error_msg)``
        when the dex is unknown or the chain is unsupported. Sets the gRPC
        ``INVALID_ARGUMENT`` status code on failure to match the inline
        validators.
        """
        provider = self._twap_providers.get(dex)
        if provider is None:
            msg = f"unsupported dex: {dex!r} (known: {sorted(self._twap_providers.keys())})"
            _invalid_argument(context, msg)
            return None, msg
        if chain not in provider.twap_supported_chains():
            msg = (
                f"dex {dex!r} does not support TWAP on chain {chain!r} "
                f"(supports: {sorted(provider.twap_supported_chains())})"
            )
            _invalid_argument(context, msg)
            return None, msg
        return provider, None

    def _resolve_lwap_provider(
        self,
        dex: str,
        chain: str,
        context: grpc.aio.ServicerContext,
    ) -> tuple[Any | None, str | None]:
        """Look up the LWAP provider for ``dex`` and confirm chain support.

        Mirrors ``_resolve_twap_provider``: ``(provider, None)`` on success,
        ``(None, error_msg)`` + ``INVALID_ARGUMENT`` when the dex is unknown or
        the chain is unsupported.
        """
        provider = self._lwap_providers.get(dex)
        if provider is None:
            msg = f"unsupported dex (lwap): {dex!r} (known: {sorted(self._lwap_providers.keys())})"
            _invalid_argument(context, msg)
            return None, msg
        if chain not in provider.lwap_supported_chains():
            msg = (
                f"dex {dex!r} does not support LWAP on chain {chain!r} "
                f"(supports: {sorted(provider.lwap_supported_chains())})"
            )
            _invalid_argument(context, msg)
            return None, msg
        return provider, None

    def _handle_twap_internal_error(
        self,
        exc: Exception,
        context: grpc.aio.ServicerContext,
        rpc_name: str,
        dex: str,
        chain: str,
        pool_address: str,
        *,
        response_cls: type,
    ) -> Any:
        """Log + decorate an unexpected TWAP-RPC exception, returning a typed response.

        The full exception is logged server-side; the client only sees a
        sanitized message so internal details (RPC URLs, credentials,
        stack traces) never cross the gateway boundary. CodeRabbit
        PR-review feedback (PR #2474); AGENTS.md §"Gateway is the security
        boundary".
        """
        logger.exception("%s failed for %s/%s/%s", rpc_name, dex, chain, pool_address)
        context.set_code(grpc.StatusCode.INTERNAL)
        context.set_details(_INTERNAL_ERROR_DETAIL)
        return response_cls(success=False, error=_INTERNAL_ERROR_DETAIL)

    # ---------------------------------------------------------------------
    # RPC: GetLendingRateCurrent
    # ---------------------------------------------------------------------

    async def GetLendingRateCurrent(
        self,
        request: gateway_pb2.GetLendingRateCurrentRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.LendingRatePointResponse:
        protocol = _normalize_key(request.protocol)
        chain = _normalize_key(request.chain)
        asset = request.asset_symbol.strip()
        side = _normalize_key(request.side)

        if not protocol:
            _invalid_argument(context, "protocol is required")
            return gateway_pb2.LendingRatePointResponse(success=False, error="protocol is required")
        if not chain:
            _invalid_argument(context, "chain is required")
            return gateway_pb2.LendingRatePointResponse(success=False, error="chain is required")
        if not asset:
            _invalid_argument(context, "asset_symbol is required")
            return gateway_pb2.LendingRatePointResponse(success=False, error="asset_symbol is required")
        if (msg := _validate_side(side, context)) is not None:
            return gateway_pb2.LendingRatePointResponse(success=False, error=msg)

        provider = self._lending_providers.get(protocol)
        if provider is None:
            msg = f"unsupported protocol: {protocol!r} (known: {sorted(self._lending_providers.keys())})"
            _invalid_argument(context, msg)
            return gateway_pb2.LendingRatePointResponse(success=False, error=msg)
        if chain not in provider.lending_supported_chains():
            msg = (
                f"protocol {protocol!r} does not support chain {chain!r} "
                f"(supports: {sorted(provider.lending_supported_chains())})"
            )
            _invalid_argument(context, msg)
            return gateway_pb2.LendingRatePointResponse(success=False, error=msg)

        start_time = time.time()
        try:
            point = await provider.fetch_lending_current(
                self,
                chain=chain,
                asset_symbol=asset,
                side=side,
            )
        except RateHistoryUnavailable as exc:
            logger.info(
                "GetLendingRateCurrent unavailable for %s/%s/%s/%s: %s",
                protocol,
                chain,
                asset,
                side,
                exc,
            )
            return gateway_pb2.LendingRatePointResponse(
                protocol=protocol,
                chain=chain,
                asset_symbol=asset,
                side=side,
                source=exc.source or "none",
                success=False,
                error=str(exc),
            )
        except Exception:
            logger.exception(
                "GetLendingRateCurrent failed for %s/%s/%s/%s",
                protocol,
                chain,
                asset,
                side,
            )
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(_INTERNAL_ERROR_DETAIL)
            return gateway_pb2.LendingRatePointResponse(success=False, error=_INTERNAL_ERROR_DETAIL)

        latency_ms = (time.time() - start_time) * 1000.0
        logger.debug(
            "GetLendingRateCurrent %s/%s/%s/%s served in %.2fms",
            protocol,
            chain,
            asset,
            side,
            latency_ms,
        )
        return gateway_pb2.LendingRatePointResponse(
            protocol=protocol,
            chain=chain,
            asset_symbol=asset,
            side=side,
            point=self._encode_lending_point(point),
            source="on_chain",
            is_live_data=True,
            success=True,
        )

    # ---------------------------------------------------------------------
    # RPC: GetLendingRateHistory
    # ---------------------------------------------------------------------

    async def GetLendingRateHistory(
        self,
        request: gateway_pb2.GetLendingRateHistoryRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.LendingRateHistoryResponse:
        protocol = _normalize_key(request.protocol)
        chain = _normalize_key(request.chain)
        asset = request.asset_symbol.strip()
        side = _normalize_key(request.side)

        if not protocol:
            _invalid_argument(context, "protocol is required")
            return gateway_pb2.LendingRateHistoryResponse(success=False, error="protocol is required")
        if not chain:
            _invalid_argument(context, "chain is required")
            return gateway_pb2.LendingRateHistoryResponse(success=False, error="chain is required")
        if not asset:
            _invalid_argument(context, "asset_symbol is required")
            return gateway_pb2.LendingRateHistoryResponse(success=False, error="asset_symbol is required")
        if (msg := _validate_side(side, context)) is not None:
            return gateway_pb2.LendingRateHistoryResponse(success=False, error=msg)
        if (msg := _validate_window(request.start_ts, request.end_ts, context)) is not None:
            return gateway_pb2.LendingRateHistoryResponse(success=False, error=msg)

        provider = self._lending_providers.get(protocol)
        if provider is None:
            msg = f"unsupported protocol: {protocol!r} (known: {sorted(self._lending_providers.keys())})"
            _invalid_argument(context, msg)
            return gateway_pb2.LendingRateHistoryResponse(success=False, error=msg)
        if chain not in provider.lending_supported_chains():
            msg = (
                f"protocol {protocol!r} does not support chain {chain!r} "
                f"(supports: {sorted(provider.lending_supported_chains())})"
            )
            _invalid_argument(context, msg)
            return gateway_pb2.LendingRateHistoryResponse(success=False, error=msg)

        try:
            points = await provider.fetch_lending_history(
                self,
                chain=chain,
                asset_symbol=asset,
                side=side,
                start_ts=request.start_ts,
                end_ts=request.end_ts,
            )
        except RateHistoryUnavailable as exc:
            logger.info(
                "GetLendingRateHistory unavailable for %s/%s/%s/%s: %s",
                protocol,
                chain,
                asset,
                side,
                exc,
            )
            return gateway_pb2.LendingRateHistoryResponse(
                protocol=protocol,
                chain=chain,
                asset_symbol=asset,
                side=side,
                source=exc.source or "none",
                success=False,
                error=str(exc),
            )
        except Exception:
            logger.exception(
                "GetLendingRateHistory failed for %s/%s/%s/%s",
                protocol,
                chain,
                asset,
                side,
            )
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(_INTERNAL_ERROR_DETAIL)
            return gateway_pb2.LendingRateHistoryResponse(success=False, error=_INTERNAL_ERROR_DETAIL)

        return gateway_pb2.LendingRateHistoryResponse(
            protocol=protocol,
            chain=chain,
            asset_symbol=asset,
            side=side,
            points=[self._encode_lending_point(p) for p in points],
            source="the_graph",
            success=True,
        )

    # ---------------------------------------------------------------------
    # RPC: GetFundingRateHistory
    # ---------------------------------------------------------------------

    async def GetFundingRateHistory(
        self,
        request: gateway_pb2.GetFundingRateHistoryRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.FundingRateHistoryResponse:
        venue = _normalize_key(request.venue)
        market = request.market.strip().upper()
        chain = _normalize_key(request.chain)

        if not venue:
            _invalid_argument(context, "venue is required")
            return gateway_pb2.FundingRateHistoryResponse(success=False, error="venue is required")
        if not market:
            _invalid_argument(context, "market is required")
            return gateway_pb2.FundingRateHistoryResponse(success=False, error="market is required")
        if (msg := _validate_window(request.start_ts, request.end_ts, context)) is not None:
            return gateway_pb2.FundingRateHistoryResponse(success=False, error=msg)

        provider = self._funding_providers.get(venue)
        if provider is None:
            msg = f"unsupported venue: {venue!r} (known: {sorted(self._funding_providers.keys())})"
            _invalid_argument(context, msg)
            return gateway_pb2.FundingRateHistoryResponse(success=False, error=msg)

        try:
            points = await provider.fetch_funding_history(
                self,
                market=market,
                chain=chain,
                start_ts=request.start_ts,
                end_ts=request.end_ts,
            )
        except RateHistoryUnavailable as exc:
            return gateway_pb2.FundingRateHistoryResponse(
                venue=venue,
                market=market,
                chain=chain,
                source=exc.source or "none",
                success=False,
                error=str(exc),
            )
        except Exception:
            logger.exception(
                "GetFundingRateHistory failed for %s/%s/%s",
                venue,
                market,
                chain,
            )
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(_INTERNAL_ERROR_DETAIL)
            return gateway_pb2.FundingRateHistoryResponse(success=False, error=_INTERNAL_ERROR_DETAIL)

        return gateway_pb2.FundingRateHistoryResponse(
            venue=venue,
            market=market,
            chain=chain,
            points=[self._encode_funding_point(p) for p in points],
            source=venue,
            success=True,
        )

    # ---------------------------------------------------------------------
    # RPC: GetDexTwap
    # ---------------------------------------------------------------------

    async def GetDexTwap(
        self,
        request: gateway_pb2.GetDexTwapRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.DexTwapPointResponse:
        dex = _normalize_key(request.dex)
        chain = _normalize_key(request.chain)
        pool_address = request.pool_address.strip()
        secs_ago_start = request.secs_ago_start
        secs_ago_end = request.secs_ago_end
        as_of_block = request.as_of_block if request.as_of_block > 0 else None

        prep = self._prepare_twap_dispatch(
            dex=dex,
            chain=chain,
            pool_address=pool_address,
            context=context,
            response_cls=gateway_pb2.DexTwapPointResponse,
            window_secs=(secs_ago_start, secs_ago_end),
        )
        if prep.error_response is not None:
            return prep.error_response
        assert prep.provider is not None  # narrowed by error_response check
        provider = prep.provider

        try:
            point = await provider.fetch_twap(
                self,
                chain=chain,
                pool_address=pool_address,
                secs_ago_start=secs_ago_start,
                secs_ago_end=secs_ago_end,
                as_of_block=as_of_block,
            )
        except RateHistoryUnavailable as exc:
            return gateway_pb2.DexTwapPointResponse(
                dex=dex,
                chain=chain,
                pool_address=pool_address,
                source=exc.source or "none",
                success=False,
                error=str(exc),
            )
        except Exception as exc:
            return self._handle_twap_internal_error(
                exc,
                context,
                "GetDexTwap",
                dex,
                chain,
                pool_address,
                response_cls=gateway_pb2.DexTwapPointResponse,
            )

        return gateway_pb2.DexTwapPointResponse(
            dex=dex,
            chain=chain,
            pool_address=pool_address,
            point=self._encode_twap_point(point),
            source="on_chain",
            success=True,
        )

    # ---------------------------------------------------------------------
    # RPC: GetDexLwap (VIB-4948 / L3 of ALM-2770)
    # ---------------------------------------------------------------------

    async def GetDexLwap(
        self,
        request: gateway_pb2.GetDexLwapRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.DexLwapPointResponse:
        dex = _normalize_key(request.dex)
        chain = _normalize_key(request.chain)
        # Dedupe (case-insensitive, order-preserving): a repeated pool address
        # would otherwise be read and weighted twice, letting a caller bias the
        # liquidity-weighted average toward one pool (CodeRabbit).
        pool_addresses: list[str] = []
        _seen_pools: set[str] = set()
        for a in request.pool_addresses:
            a = a.strip()
            if a and a.lower() not in _seen_pools:
                _seen_pools.add(a.lower())
                pool_addresses.append(a)
        min_liquidity = request.min_liquidity.strip()
        as_of_block = request.as_of_block if request.as_of_block > 0 else None
        base_token = request.base_token.strip()
        quote_token = request.quote_token.strip()

        # Validator-first (mirrors _validate_twap_identity, adapted for the
        # multi-pool shape — at least one pool address is required).
        if not dex:
            _invalid_argument(context, "dex is required")
            return gateway_pb2.DexLwapPointResponse(success=False, error="dex is required")
        if not chain:
            _invalid_argument(context, "chain is required")
            return gateway_pb2.DexLwapPointResponse(success=False, error="chain is required")
        if not pool_addresses:
            _invalid_argument(context, "pool_addresses is required (>= 1 address)")
            return gateway_pb2.DexLwapPointResponse(success=False, error="pool_addresses is required")

        provider, err = self._resolve_lwap_provider(dex, chain, context)
        if provider is None:
            return gateway_pb2.DexLwapPointResponse(
                dex=dex,
                chain=chain,
                pool_addresses=pool_addresses,
                success=False,
                error=err or "no provider",
            )

        try:
            point = await provider.fetch_lwap(
                self,
                chain=chain,
                pool_addresses=pool_addresses,
                min_liquidity=min_liquidity,
                as_of_block=as_of_block,
                base_token=base_token,
                quote_token=quote_token,
            )
        except RateHistoryUnavailable as exc:
            return gateway_pb2.DexLwapPointResponse(
                dex=dex,
                chain=chain,
                pool_addresses=pool_addresses,
                source=exc.source or "none",
                success=False,
                error=str(exc),
            )
        except Exception as exc:
            return self._handle_twap_internal_error(
                exc,
                context,
                "GetDexLwap",
                dex,
                chain,
                ",".join(pool_addresses),
                response_cls=gateway_pb2.DexLwapPointResponse,
            )

        return gateway_pb2.DexLwapPointResponse(
            dex=dex,
            chain=chain,
            pool_addresses=pool_addresses,
            point=self._encode_lwap_point(point),
            source="gateway_rpc",
            success=True,
        )

    # ---------------------------------------------------------------------
    # RPC: GetDexTwapSeries
    # ---------------------------------------------------------------------

    async def GetDexTwapSeries(
        self,
        request: gateway_pb2.GetDexTwapSeriesRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.DexTwapHistoryResponse:
        dex = _normalize_key(request.dex)
        chain = _normalize_key(request.chain)
        pool_address = request.pool_address.strip()

        prep = self._prepare_twap_dispatch(
            dex=dex,
            chain=chain,
            pool_address=pool_address,
            context=context,
            response_cls=gateway_pb2.DexTwapHistoryResponse,
            ts_window=(request.start_ts, request.end_ts),
            interval_secs=request.interval_secs,
        )
        if prep.error_response is not None:
            return prep.error_response
        assert prep.provider is not None  # narrowed by error_response check
        provider = prep.provider

        try:
            points = await provider.fetch_twap_series(
                self,
                chain=chain,
                pool_address=pool_address,
                start_ts=request.start_ts,
                end_ts=request.end_ts,
                interval_secs=request.interval_secs,
            )
        except RateHistoryUnavailable as exc:
            return gateway_pb2.DexTwapHistoryResponse(
                dex=dex,
                chain=chain,
                pool_address=pool_address,
                source=exc.source or "none",
                success=False,
                error=str(exc),
            )
        except Exception as exc:
            return self._handle_twap_internal_error(
                exc,
                context,
                "GetDexTwapSeries",
                dex,
                chain,
                pool_address,
                response_cls=gateway_pb2.DexTwapHistoryResponse,
            )

        return gateway_pb2.DexTwapHistoryResponse(
            dex=dex,
            chain=chain,
            pool_address=pool_address,
            points=[self._encode_twap_point(p) for p in points],
            source="on_chain",
            success=True,
        )

    # ---------------------------------------------------------------------
    # RPC: GetDexVolumeHistory
    # ---------------------------------------------------------------------

    async def GetDexVolumeHistory(
        self,
        request: gateway_pb2.GetDexVolumeHistoryRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.DexVolumeHistoryResponse:
        dex = _normalize_key(request.dex)
        chain = _normalize_key(request.chain)
        pool_address = request.pool_address.strip()

        prep = self._prepare_volume_dispatch(
            dex=dex,
            chain=chain,
            pool_address=pool_address,
            ts_window=(request.start_ts, request.end_ts),
            interval_secs=request.interval_secs,
            context=context,
        )
        if prep.error_response is not None:
            return prep.error_response
        assert prep.provider is not None  # narrowed by error_response check
        provider = prep.provider

        try:
            points = await provider.fetch_volume_history(
                self,
                chain=chain,
                pool_address=pool_address,
                start_ts=request.start_ts,
                end_ts=request.end_ts,
                interval_secs=request.interval_secs,
            )
        except RateHistoryUnavailable as exc:
            return gateway_pb2.DexVolumeHistoryResponse(
                dex=dex,
                chain=chain,
                pool_address=pool_address,
                source=exc.source or "none",
                success=False,
                error=str(exc),
            )
        except Exception as exc:
            return self._handle_twap_internal_error(
                exc,
                context,
                "GetDexVolumeHistory",
                dex,
                chain,
                pool_address,
                response_cls=gateway_pb2.DexVolumeHistoryResponse,
            )

        return gateway_pb2.DexVolumeHistoryResponse(
            dex=dex,
            chain=chain,
            pool_address=pool_address,
            points=[self._encode_volume_point(p) for p in points],
            source="the_graph",
            success=True,
        )


__all__ = [
    "DexTwapPoint",
    "DexVolumePoint",
    "FundingRatePoint",
    "LendingRatePoint",
    "RateHistoryServiceServicer",
    "RateHistoryUnavailable",
]
