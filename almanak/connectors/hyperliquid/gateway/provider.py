"""Gateway-side connector binding for Hyperliquid.

Phase 3 (VIB-4811) introduces capability-keyed dispatch at the gateway
boundary. Hyperliquid contributes:

* ``GatewayFundingRateCapability`` — venue identifier, per-market
  default funding rates, and the live REST fetch. Previously these
  lived as a venue branch in
  ``almanak.gateway.services.funding_rate_service``.

The live fetch delegates to the gateway servicer's existing
``_fetch_hyperliquid_rate(market)`` method so the venue-specific REST
client + Pydantic parser plumbing stays alongside the
``HyperliquidAssetContext`` / ``HyperliquidUniverseItem`` models, and
the existing unit tests for that method continue to pass.

W7 (VIB-4859) adds:

* ``GatewayFundingHistoryCapability`` — historical hourly funding-rate
  series via the Hyperliquid Info API (``POST /info`` with
  ``type=fundingHistory``). Migrates the
  ``_query_hyperliquid_funding`` body that used to live strategy-side
  in ``framework/data/rates/history.py`` (and opened its own aiohttp
  ``ClientSession``) and the duplicated egress in
  ``framework/backtesting/pnl/providers/perp/hyperliquid_funding.py``.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayFundingHistoryCapability,
    GatewayFundingRateCapability,
    GatewayOraclePriceCapability,
    GatewayOrderStatusCapability,
    GatewayPerpFillsCapability,
    OraclePriceQuery,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

# NOTE: the venue-specific connector modules (``.addresses`` / ``.markets`` /
# ``.sdk``) are imported LAZILY inside the oracle-capability methods below, NOT
# at module top level. This provider is instantiated eagerly at gateway registry
# bootstrap (``_gateway_registry._register_all``), and ``hyperliquid.sdk`` is a
# strategy-side heavy submodule the gateway sidecar must not eagerly load
# (guarded by tests/gateway/test_imports_lean.py). Deferring the import keeps the
# gateway boot import graph lean while still letting the provider — which lives
# under the connector package, so the gateway↔connector isolation ratchet does
# not apply here — expose the venue read through the capability.

logger = logging.getLogger(__name__)

# Default per-market hourly funding rates — fallback when the REST
# fetch fails / times out. Moved verbatim from
# ``funding_rate_service.DEFAULT_RATES["hyperliquid"]``.
_HYPERLIQUID_DEFAULT_RATES: dict[str, Decimal] = {
    "ETH-USD": Decimal("0.000015"),
    "BTC-USD": Decimal("0.000011"),
    "ARB-USD": Decimal("0.000018"),
    "LINK-USD": Decimal("0.000009"),
    "SOL-USD": Decimal("0.000022"),
}

# Historical fallback for unknown markets (matches the previous
# ``_get_default_rate`` second arg to ``.get``).
_UNKNOWN_MARKET_DEFAULT = Decimal("0.00001")

# W7: Hyperliquid Info API endpoint (POST /info).
_HYPERLIQUID_INFO_API = "https://api.hyperliquid.xyz/info"

# Mapping from market symbol → Hyperliquid coin code (used for the
# ``coin`` field on the ``fundingHistory`` request body). Moved verbatim
# from ``framework/data/rates/history.py:_HYPER_MARKET_TO_COIN``.
_HYPER_MARKET_TO_COIN: dict[str, str] = {
    "ETH-USD": "ETH",
    "BTC-USD": "BTC",
    "ARB-USD": "ARB",
    "LINK-USD": "LINK",
    "SOL-USD": "SOL",
    "DOGE-USD": "DOGE",
    "AVAX-USD": "AVAX",
    "OP-USD": "OP",
}

# Hours per year for annualisation.
_HOURS_PER_YEAR = 8760


def _hyperliquid_resolve_coin(market: str) -> str:
    """Resolve the Hyperliquid coin code for ``market``.

    Raises ``RateHistoryUnavailable`` if the market is not in the supported set.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    coin = _HYPER_MARKET_TO_COIN.get(market)
    if coin is None:
        raise RateHistoryUnavailable("hyperliquid", f"Unsupported market: {market!r}")
    return coin


async def _hyperliquid_read_funding_response(response: Any) -> Any:
    """Read a Hyperliquid Info-API response, raising on non-200 status.

    Split out of ``_hyperliquid_post_funding_history`` so the outer
    try/except plumbing stays decomposable.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    if response.status != 200:
        text = await response.text()
        raise RateHistoryUnavailable(
            "hyperliquid",
            f"HTTP {response.status}: {text[:200]}",
        )
    return await response.json()


async def _hyperliquid_post_funding_history(
    session: Any,
    *,
    coin: str,
    start_ts: int,
    end_ts: int,
    market: str,
) -> list[Any]:
    """POST ``fundingHistory`` to the Hyperliquid Info API and return the entry list.

    Normalises all non-200 / decode failure modes to ``RateHistoryUnavailable``.
    """
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    payload = {
        "type": "fundingHistory",
        "coin": coin,
        "startTime": start_ts * 1000,
        "endTime": end_ts * 1000,
    }

    try:
        async with session.post(
            _HYPERLIQUID_INFO_API,
            json=payload,
            headers={"Content-Type": "application/json"},
        ) as response:
            data = await _hyperliquid_read_funding_response(response)
    except RateHistoryUnavailable:
        raise
    except Exception as exc:
        raise RateHistoryUnavailable(
            "hyperliquid",
            f"fundingHistory request / decode failed: {exc}",
        ) from exc

    if not isinstance(data, list) or not data:
        raise RateHistoryUnavailable(
            "hyperliquid",
            f"No funding-rate data returned for market {market!r}",
        )
    return data


def _hyperliquid_parse_funding_timestamp(time_value: Any) -> datetime | None:
    """Parse a Hyperliquid ``time`` field into an aware ``datetime``.

    Accepts ISO-8601 strings (with or without trailing ``Z``) and
    epoch-ms numerics. Returns ``None`` for anything else (silent skip
    matches the original verbatim behaviour).
    """
    if isinstance(time_value, str) and time_value:
        timestamp = datetime.fromisoformat(time_value.replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)
        return timestamp
    if isinstance(time_value, int | float):
        return datetime.fromtimestamp(time_value / 1000, tz=UTC)
    return None


def _hyperliquid_build_funding_point(
    item: Any,
    *,
    start_dt: datetime,
    end_dt: datetime,
) -> Any:
    """Convert one Hyperliquid funding entry to ``FundingRatePoint``.

    Returns ``None`` when the entry is out of range or its ``time`` field
    is unparseable (silent skip preserved from history.py). Raises on
    decimal / parse failure so the caller can demote to a debug-log skip.
    """
    from almanak.gateway.services.rate_history_service import FundingRatePoint

    timestamp = _hyperliquid_parse_funding_timestamp(item.get("time", ""))
    if timestamp is None:
        return None
    if timestamp < start_dt or timestamp > end_dt:
        return None

    raw_rate = item.get("fundingRate")
    if raw_rate in (None, ""):
        # Unmeasured hour: skip, never manufacture a measured 0.
        return None
    # Strict parse: a malformed or non-finite rate must not become a
    # measured 0 — raise so the caller demotes to a skip.
    rate = Decimal(str(raw_rate))
    if not rate.is_finite():
        raise ValueError(f"fundingRate must be finite, got {raw_rate!r}")
    annualized = rate * Decimal(str(_HOURS_PER_YEAR))

    return FundingRatePoint(
        timestamp=int(timestamp.timestamp()),
        rate_hourly=rate,
        rate_annualized=annualized,
    )


def _hyperliquid_parse_funding_entries(
    data: list[Any],
    *,
    start_ts: int,
    end_ts: int,
) -> list[Any]:
    """Parse Hyperliquid ``fundingHistory`` entries into ``FundingRatePoint`` rows.

    Entries with malformed time / rate fields, or timestamps outside
    ``[start_ts, end_ts]``, are silently skipped — matches the verbatim
    history.py behaviour.
    """
    points: list[Any] = []
    start_dt = datetime.fromtimestamp(start_ts, tz=UTC)
    end_dt = datetime.fromtimestamp(end_ts, tz=UTC)
    for item in data:
        try:
            point = _hyperliquid_build_funding_point(item, start_dt=start_dt, end_dt=end_dt)
        except (ValueError, TypeError, InvalidOperation):
            logger.debug("Skipping malformed Hyperliquid funding entry: %s", item)
            continue
        if point is not None:
            points.append(point)
    return points


async def _hyperliquid_post_order_status(
    session: Any,
    *,
    wallet_address: str,
    cloid: int,
) -> dict[str, Any]:
    """POST ``orderStatus`` (by ``cloid``) to the Hyperliquid Info API (VIB-5597).

    Returns the raw response JSON — the connector-side parser
    (``fill_reconciliation.parse_order_status_response``) turns it into a fill
    verdict. Raises ``OrderStatusUnavailable`` on any transport/decode failure so
    the caller fail-closes to ``UNMEASURED`` (Empty ≠ Zero — never assume filled).

    The request body is built by the pure connector helper so the wire shape has
    a single source of truth (and its own unit tests).
    """
    from almanak.connectors.hyperliquid.fill_reconciliation import build_order_status_request

    payload = build_order_status_request(wallet_address, cloid)

    try:
        async with session.post(
            _HYPERLIQUID_INFO_API,
            json=payload,
            headers={"Content-Type": "application/json"},
        ) as response:
            if response.status != 200:
                text = await response.text()
                raise OrderStatusUnavailable(f"HTTP {response.status}: {text[:200]}")
            data = await response.json()
    except OrderStatusUnavailable:
        raise
    except Exception as exc:  # noqa: BLE001 — normalise all transport/decode faults
        raise OrderStatusUnavailable(f"orderStatus request / decode failed: {exc}") from exc

    if not isinstance(data, dict):
        raise OrderStatusUnavailable(f"orderStatus returned non-object payload: {type(data).__name__}")
    return data


class OrderStatusUnavailable(Exception):
    """The HyperCore ``orderStatus`` read could not be measured (VIB-5597).

    Empty ≠ Zero: raised on any transport / non-200 / decode failure so the
    caller treats the fill as UNMEASURED, never as filled or rejected.
    """


# =============================================================================
# VIB-5595 — userFills / userFunding parsing (pure, module-private)
# =============================================================================


def _hl_str(value: Any) -> str:
    """Coerce an Info-API field to a wire string, Empty ≠ Zero.

    ``None`` / absent → ``""`` (unmeasured); a present value (including ``0`` /
    ``"0"``) → its string form (measured). NEVER fabricates ``"0"`` for a missing
    field — the gateway wire and the accounting path both depend on this.
    """
    if value is None:
        return ""
    return str(value)


def _hl_int_ms(value: Any) -> int:
    """Coerce an Info-API epoch-ms field to int; 0 on absent/unparseable."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _parse_user_fill(item: Any) -> Any:
    """Convert one Hyperliquid ``userFills`` entry to ``PerpFillData``.

    Field names per the Hyperliquid Info API ``userFills`` response
    (``coin``, ``px``, ``sz``, ``dir``, ``closedPnl``, ``fee``, ``feeToken``,
    ``oid``, ``cloid``, ``time``, ``crossed``). Empty ≠ Zero: a field the venue
    omits is left as ``""`` (unmeasured), never coerced to ``"0"``.
    """
    from almanak.gateway.services.perp_fill_service import PerpFillData

    if not isinstance(item, dict):
        raise ValueError(f"userFills entry is not an object: {item!r}")

    return PerpFillData(
        coin=_hl_str(item.get("coin")),
        px=_hl_str(item.get("px")),
        sz=_hl_str(item.get("sz")),
        dir=_hl_str(item.get("dir")),
        fee=_hl_str(item.get("fee")),
        closed_pnl=_hl_str(item.get("closedPnl")),
        oid=_hl_str(item.get("oid")),
        cloid=_hl_str(item.get("cloid")),
        time_ms=_hl_int_ms(item.get("time")),
        crossed=bool(item.get("crossed", False)),
        fee_token=_hl_str(item.get("feeToken")),
    )


def _parse_user_funding(item: Any) -> Any:
    """Convert one Hyperliquid ``userFunding`` entry to ``PerpFundingData``.

    A ``userFunding`` row is ``{"time": <ms>, "delta": {"coin", "usdc",
    "fundingRate", ...}}``. Empty ≠ Zero on every economics field.
    """
    from almanak.gateway.services.perp_fill_service import PerpFundingData

    if not isinstance(item, dict):
        raise ValueError(f"userFunding entry is not an object: {item!r}")

    delta = item.get("delta")
    if not isinstance(delta, dict):
        delta = {}

    return PerpFundingData(
        coin=_hl_str(delta.get("coin")),
        usdc=_hl_str(delta.get("usdc")),
        funding_rate=_hl_str(delta.get("fundingRate")),
        time_ms=_hl_int_ms(item.get("time")),
    )


def _coin_matches(entry_coin: str, filter_coin: str) -> bool:
    """True when ``entry_coin`` matches the requested ``filter_coin``.

    Empty ``filter_coin`` matches everything. Comparison is case-insensitive on
    the bare base symbol (Hyperliquid ``userFills`` reports ``"BTC"``; a caller
    may pass ``"BTC"`` / ``"btc"``).
    """
    if not filter_coin:
        return True
    return entry_coin.strip().upper() == filter_coin.strip().upper()


async def _hl_post_info(session: Any, payload: dict[str, Any]) -> Any:
    """POST a request body to the Hyperliquid Info API, raising on non-200.

    Reuses the servicer's shared aiohttp session (bounded timeout). Raises on any
    non-200 status or decode failure so the caller can surface ``ok=False``.
    """
    async with session.post(
        _HYPERLIQUID_INFO_API,
        json=payload,
        headers={"Content-Type": "application/json"},
    ) as response:
        if response.status != 200:
            text = await response.text()
            raise RuntimeError(f"Hyperliquid Info API HTTP {response.status}: {text[:200]}")
        return await response.json()


class HyperliquidGatewayConnector(
    GatewayConnector,
    GatewayFundingRateCapability,
    GatewayFundingHistoryCapability,
    GatewayOraclePriceCapability,
    GatewayOrderStatusCapability,
    GatewayPerpFillsCapability,
):
    """Gateway-side connector for Hyperliquid perp venue."""

    protocol: ClassVar[ProtocolName] = ProtocolName("hyperliquid")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def venue(self) -> str:
        return "hyperliquid"

    def default_funding_rate(self, market: str) -> Decimal:
        return _HYPERLIQUID_DEFAULT_RATES.get(market, _UNKNOWN_MARKET_DEFAULT)

    async def fetch_funding_rate(
        self,
        servicer: Any,
        market: str,
        chain: str,
    ) -> Any:
        """Delegate to the servicer's existing REST fetch helper.

        ``chain`` is unused for Hyperliquid (the API is chain-agnostic)
        but the capability contract takes it for parity with on-chain
        venues like GMX V2.
        """
        return await servicer._fetch_hyperliquid_rate(market)

    # ---------------------------------------------------------------------
    # GatewayFundingHistoryCapability (VIB-4859 / W7)
    # ---------------------------------------------------------------------

    def funding_venue(self) -> str:
        """Venue identifier matching :meth:`venue` for the live capability."""
        return "hyperliquid"

    def funding_supported_markets(self) -> frozenset[str]:
        """Markets the Hyperliquid Info API serves on the historical lane."""
        return frozenset(_HYPER_MARKET_TO_COIN.keys())

    async def fetch_funding_history(
        self,
        servicer: Any,
        *,
        market: str,
        chain: str,
        start_ts: int,
        end_ts: int,
    ) -> Any:
        """Historical hourly funding via the Hyperliquid Info API.

        Migrated verbatim from
        ``framework/data/rates/history.py:_query_hyperliquid_funding`` +
        ``_parse_hyperliquid_funding_response``. ``servicer`` is the
        ``RateHistoryServiceServicer`` — we reuse its shared aiohttp
        session so the rate-limit budget is shared with other consumers.
        """
        from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

        coin = _hyperliquid_resolve_coin(market)
        session = await servicer._get_http_session()

        data = await _hyperliquid_post_funding_history(
            session,
            coin=coin,
            start_ts=start_ts,
            end_ts=end_ts,
            market=market,
        )

        points = _hyperliquid_parse_funding_entries(
            data,
            start_ts=start_ts,
            end_ts=end_ts,
        )

        if not points:
            raise RateHistoryUnavailable(
                "hyperliquid",
                f"All Hyperliquid funding entries fell outside [{start_ts}, {end_ts}]",
            )

        return points

    # ---------------------------------------------------------------------
    # GatewayOraclePriceCapability (VIB-5576)
    # ---------------------------------------------------------------------
    #
    # HyperCore's oracle price for a perp is read from the ``0x0807``
    # precompile. The gateway's ``HypercoreOraclePriceSource`` used to import
    # ``hyperliquid.addresses`` / ``.markets`` / ``.sdk`` directly, which the
    # gateway↔connector isolation ratchet forbids. These methods publish the
    # venue-specific read (precompile address, calldata encoding, decode +
    # fixed-point scale, symbol→asset resolution) so the gateway source can do
    # the eth_call without importing the connector. The gateway source keeps
    # ALL the RPC plumbing and Empty≠Zero miss semantics.

    def oracle_price_chain(self) -> str:
        """The chain whose oracle prices this capability serves (HyperEVM, 999)."""
        return "hyperevm"

    def resolve_oracle_query(self, symbol: str) -> OraclePriceQuery | None:
        """Resolve a perp symbol to the ``0x0807`` oracle read, or ``None`` on a miss.

        ``None`` (not an exception) for an unresolvable symbol so the gateway
        source maps it to a MISS and its aggregator falls through to spot
        sources. No network egress — resolution is from the connector's static
        market seed only.
        """
        # Lazy import: keeps the strategy-side heavy ``.sdk`` module out of the
        # gateway boot import graph (see module-level note + test_imports_lean).
        from almanak.connectors.hyperliquid.addresses import PRECOMPILE_ORACLE_PX
        from almanak.connectors.hyperliquid.markets import resolve_market
        from almanak.connectors.hyperliquid.sdk import encode_perp_query

        try:
            market = resolve_market(symbol)
        except ValueError:
            return None
        return OraclePriceQuery(
            symbol=market.symbol,
            to_address=PRECOMPILE_ORACLE_PX,
            calldata="0x" + encode_perp_query(market.asset_index).hex(),
            # Carry szDecimals so decode can apply the exact precompile scale
            # ``raw / 10**(PERP_PX_MAX_DECIMALS - szDecimals)``. Opaque to the gateway.
            context=market.sz_decimals,
        )

    def decode_oracle_price(self, query: OraclePriceQuery, raw_hex: str) -> Decimal | None:
        """Decode + scale the raw ``0x0807`` return into a human USD price.

        Wire → human: ``raw / 10**(PERP_PX_MAX_DECIMALS - szDecimals)`` — the
        EXACT scale the connector compiler uses. Returns ``None`` (Empty≠Zero)
        for an empty / undecodable / non-positive read; NEVER raises on a
        malformed payload (the gateway source relies on this to keep its
        aggregator crash-free on bad on-chain data).
        """
        # Lazy import: keeps the strategy-side heavy ``.sdk`` module out of the
        # gateway boot import graph (see module-level note + test_imports_lean).
        from almanak.connectors.hyperliquid.addresses import PERP_PX_MAX_DECIMALS
        from almanak.connectors.hyperliquid.sdk import decode_uint64

        try:
            wire = decode_uint64(raw_hex)
        except Exception:
            # Malformed / undecodable payload is a MISS, not a crash.
            return None
        if wire is None or wire <= 0:
            # Empty / zero read: unavailable is NOT a measured zero.
            return None
        sz_decimals = query.context
        price = Decimal(wire) / (Decimal(10) ** (PERP_PX_MAX_DECIMALS - sz_decimals))
        if price <= 0:
            return None
        return price

    # ---------------------------------------------------------------------
    # GatewayOrderStatusCapability (VIB-5597) — fill-vs-submission
    # ---------------------------------------------------------------------

    def order_status_venue(self) -> str:
        """Venue identifier matching :meth:`venue` for the order-status lane."""
        return "hyperliquid"

    async def fetch_order_status(
        self,
        servicer: Any,
        *,
        wallet_address: str,
        cloid: int,
        chain: str,  # noqa: ARG002 — API is chain-agnostic; parity with on-chain venues
    ) -> Any:
        """Live ``orderStatus``-by-``cloid`` via the Hyperliquid Info API.

        Confirms whether a specific CoreWriter submission (addressed by its
        deterministic ``cloid``) filled / partially filled / was rejected. The
        egress correctly lives here (gateway side); strategy/framework code reads
        through the gateway. Reuses the servicer's shared aiohttp session so the
        rate-limit budget is shared with the funding lanes.

        Parses the raw response with the connector's own pure parser
        (``fill_reconciliation.parse_order_status_response`` — single source of
        truth for the venue status vocabulary) and returns the neutral,
        gateway-side :class:`OrderStatusData` so the gateway holds NO connector
        import (symmetric with :meth:`fetch_user_fills` returning
        ``PerpFillResult``; blueprint 22 §connector self-containment). Empty ≠
        Zero: ``filled_size`` / ``avg_fill_price`` the venue did not report map
        to ``""`` (never ``"0"``). Raises ``OrderStatusUnavailable`` on any
        unmeasured read so the servicer fail-closes to ``success=False``.
        """
        from almanak.connectors.hyperliquid.fill_reconciliation import parse_order_status_response
        from almanak.gateway.services.perp_fill_service import OrderStatusData

        session = await servicer._get_http_session()
        raw = await _hyperliquid_post_order_status(
            session,
            wallet_address=wallet_address,
            cloid=cloid,
        )
        outcome = parse_order_status_response(raw)
        return OrderStatusData(
            status=str(outcome.status),
            filled_size="" if outcome.filled_size is None else str(outcome.filled_size),
            avg_fill_price="" if outcome.avg_fill_price is None else str(outcome.avg_fill_price),
            detail=outcome.detail,
        )

    # ---------------------------------------------------------------------
    # GatewayPerpFillsCapability (VIB-5595)
    # ---------------------------------------------------------------------

    def fills_venue(self) -> str:
        """Venue identifier matching :meth:`venue` / :meth:`funding_venue`."""
        return "hyperliquid"

    async def fetch_user_fills(
        self,
        servicer: Any,
        *,
        wallet_address: str,
        coin: str = "",
        start_ts: int = 0,
    ) -> Any:
        """Per-fill economics for ``wallet_address`` via the Hyperliquid Info API.

        ``servicer`` is the ``PerpFillServiceServicer`` — we reuse its shared
        aiohttp session (bounded timeout, shared rate-limit budget). Returns a
        ``PerpFillResult``; a malformed / non-200 response yields ``ok=False``
        (UNMEASURED — the accounting reader leaves fill economics ``None``), and a
        wallet with no fills yields ``ok=True`` with an empty list (measured empty
        book). ``start_ts`` is an epoch-**ms** lower bound (0 = unbounded).
        """
        from almanak.gateway.services.perp_fill_service import PerpFillData, PerpFillResult

        session = await servicer._get_http_session()
        payload: dict[str, Any] = {"type": "userFills", "user": wallet_address}
        try:
            data = await _hl_post_info(session, payload)
        except Exception as exc:  # noqa: BLE001 — surface as unmeasured, never fabricate fills
            logger.warning("Hyperliquid userFills failed for %s: %s", wallet_address, exc)
            return PerpFillResult(fills=[], ok=False, error=str(exc))

        if not isinstance(data, list):
            return PerpFillResult(fills=[], ok=False, error="userFills response was not a list")

        fills: list[PerpFillData] = []
        for item in data:
            try:
                fill = _parse_user_fill(item)
            except (ValueError, TypeError):
                logger.debug("Skipping malformed Hyperliquid fill: %s", item)
                continue
            if not _coin_matches(fill.coin, coin):
                continue
            if start_ts and fill.time_ms and fill.time_ms < start_ts:
                continue
            fills.append(fill)
        return PerpFillResult(fills=fills, ok=True)

    async def fetch_user_funding(
        self,
        servicer: Any,
        *,
        wallet_address: str,
        coin: str = "",
        start_ts: int = 0,
    ) -> Any:
        """Funding settlement deltas for ``wallet_address`` via the Info API.

        Semantics mirror :meth:`fetch_user_fills`. The Info API ``userFunding``
        request takes a ``startTime`` (epoch-ms) window; we pass ``start_ts`` when
        provided (0 → a wide default window so the caller still gets recent
        settlements). Returns a ``PerpFundingResult``.
        """
        from almanak.gateway.services.perp_fill_service import PerpFundingData, PerpFundingResult

        session = await servicer._get_http_session()
        payload: dict[str, Any] = {"type": "userFunding", "user": wallet_address}
        if start_ts:
            payload["startTime"] = start_ts
        try:
            data = await _hl_post_info(session, payload)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Hyperliquid userFunding failed for %s: %s", wallet_address, exc)
            return PerpFundingResult(deltas=[], ok=False, error=str(exc))

        if not isinstance(data, list):
            return PerpFundingResult(deltas=[], ok=False, error="userFunding response was not a list")

        deltas: list[PerpFundingData] = []
        for item in data:
            try:
                delta = _parse_user_funding(item)
            except (ValueError, TypeError):
                logger.debug("Skipping malformed Hyperliquid funding entry: %s", item)
                continue
            if not _coin_matches(delta.coin, coin):
                continue
            if start_ts and delta.time_ms and delta.time_ms < start_ts:
                continue
            deltas.append(delta)
        return PerpFundingResult(deltas=deltas, ok=True)


__all__ = ["HyperliquidGatewayConnector", "OrderStatusUnavailable"]
