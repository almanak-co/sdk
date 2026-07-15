"""Lending Rate Monitor Service.

This module provides a unified interface for fetching lending rates from
multiple DeFi protocols. It supports Aave V3, Morpho Blue, and Compound V3.

**VIB-4859 / W7**: This module is now a thin gRPC client of the gateway's
``RateHistoryService``. All HTTP / Web3 egress for lending rate queries
happens on the gateway side via :class:`GatewayLendingRateHistoryCapability`
implementations on the corresponding connectors. The strategy container
holds no protocol-specific dispatch and no outbound HTTP / Web3 clients.

The :class:`RateMonitor` public API + dataclasses
(:class:`LendingRate`, :class:`BestRateResult`, :class:`ProtocolRates`)
are preserved verbatim for back-compat. ``RateMonitor`` itself is marked
deprecated (use :meth:`MarketSnapshot.lending_rate` instead per VIB-4869);
the wrapper class will be removed once the caller-migration follow-up
ticket VIB-4869 lands.

Example:
    from almanak.framework.data.rates import RateMonitor, RateSide

    monitor = RateMonitor(chain="ethereum")

    # Get Aave USDC supply rate
    rate = await monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

    # Get best supply rate across all protocols
    best = await monitor.get_best_lending_rate("USDC", RateSide.SUPPLY)
"""

import asyncio
import logging
import time
import warnings
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from almanak.framework.data.interfaces import DataSourceUnavailable

logger = logging.getLogger(__name__)


def _monitor_get_connected_gateway_client() -> tuple[Any, Any]:
    """Return ``(client, gateway_pb2)`` with the client connected, or raise."""
    try:
        from almanak.framework.gateway_client import get_gateway_client
        from almanak.gateway.proto import gateway_pb2
    except ImportError as exc:
        raise DataSourceUnavailable(
            source="gateway",
            reason=f"Gateway client unavailable: {exc}",
        ) from exc

    client = get_gateway_client()
    if not client.is_connected:
        try:
            client.connect()
        except Exception as exc:
            raise DataSourceUnavailable(
                source="gateway",
                reason=f"Gateway connect failed: {exc}",
            ) from exc
    return client, gateway_pb2


def _build_lending_rate_from_point(
    response: Any,
    *,
    protocol: str,
    token: str,
    side: str,
    chain: str,
) -> Any:
    """Decode a ``GetLendingRateCurrentResponse.point`` into a ``LendingRate``.

    Imports ``LendingRate`` lazily to avoid a circular import (the dataclass
    is defined later in this module). Raises ``DataSourceUnavailable`` when
    the requested side carries no APY data (Empty != Zero).
    """
    point = response.point
    apy_str = point.supply_apy_pct if side == "supply" else point.borrow_apy_pct
    if not apy_str:
        raise DataSourceUnavailable(
            source=response.source,
            reason=f"Gateway returned no {side} APY for {protocol}/{token}",
        )
    apy_percent = Decimal(apy_str)
    utilization_percent: Decimal | None = None
    if point.utilization_pct:
        utilization_percent = Decimal(point.utilization_pct)

    # Re-derive ray from percent (gateway only ships percent).
    apy_ray = apy_percent * RAY / Decimal("100")

    return LendingRate(
        protocol=protocol,
        token=token,
        side=side,
        apy_ray=apy_ray,
        apy_percent=apy_percent,
        utilization_percent=utilization_percent,
        chain=chain,
        # Carry the market the gateway ACTUALLY read (VIB-5729). Callers that
        # discover a market from a rate (config omitting market_id) read
        # ``rate.market_id``; leaving it None would make an unscoped Morpho scan
        # unable to report which market it selected, even though the response
        # carries it. Empty echo -> None (unscoped venue / no claim made).
        market_id=(getattr(response, "market_id", "") or "").strip() or None,
    )


async def _monitor_call_lending_rate_current(
    client: Any,
    gateway_pb2: Any,
    *,
    protocol: str,
    chain: str,
    token: str,
    side: str,
    market_id: str | None = None,
) -> Any:
    """Issue ``GetLendingRateCurrent`` via ``asyncio.to_thread`` and return the response.

    Wraps transport + ``success=False`` failures as ``DataSourceUnavailable``.

    When ``market_id`` is supplied the response is only accepted if the gateway
    echoes back the SAME market (VIB-5729) — see :func:`_assert_market_scope_honoured`.
    """
    request = gateway_pb2.GetLendingRateCurrentRequest(
        protocol=protocol,
        chain=chain,
        asset_symbol=token,
        side=side,
        market_id=market_id or "",
    )
    try:
        response = await asyncio.to_thread(client.rate_history.GetLendingRateCurrent, request)
    except Exception as exc:
        raise DataSourceUnavailable(
            source="gateway",
            reason=f"GetLendingRateCurrent RPC failed: {exc}",
        ) from exc
    if not response.success:
        raise DataSourceUnavailable(
            source=response.source or "gateway",
            reason=response.error or "GetLendingRateCurrent returned success=false",
        )
    _assert_market_scope_honoured(response, requested=market_id, protocol=protocol, chain=chain, token=token)
    return response


def _assert_market_scope_honoured(
    response: Any,
    *,
    requested: str | None,
    protocol: str,
    chain: str,
    token: str,
) -> None:
    """Fail CLOSED unless the gateway proved it read the market we asked for.

    The rollout guard for market-scoped lending rates (VIB-5729). ``market_id``
    is an OPTIONAL proto3 request field, so a gateway older than the field —
    hosted runs the gateway as a sidecar that may lag the framework image —
    silently DROPS it and answers with the legacy best-across-markets rate.
    That rate is plausible and wrong: on robinhood the two USDG markets differ
    by ~27% relative, and recording one for a position in the other would be the
    exact fabrication market-scoping exists to prevent.

    Unknown fields are invisible to the client, so absence of support cannot be
    detected on the request side. Instead the server echoes the market its
    PROVIDER actually measured; an old gateway cannot set that field, and a
    provider that ignores the scoping does not set it either. So: no echo, or a
    different echo, ⇒ the scoping was not honoured ⇒ raise, which callers turn
    into an honest unmeasured ``None`` rather than a wrong number.
    """
    # Normalise first: a whitespace-only market_id is not a scoping claim, and
    # must take the same unscoped path as None rather than being compared against
    # an echo it can never match (gemini, PR #3287).
    wanted = (requested or "").strip()
    if not wanted:
        return  # unscoped read — nothing to prove
    echoed = (getattr(response, "market_id", "") or "").strip()
    if echoed.lower() == wanted.lower():
        return
    raise DataSourceUnavailable(
        source=response.source or "gateway",
        reason=(
            f"market-scoped lending rate for {protocol}/{chain}/{token} was not honoured: "
            f"requested market_id={requested!r} but the gateway echoed {echoed or '<none>'!r}. "
            "Treating as unmeasured — a gateway older than VIB-5729 ignores market scoping "
            "and would answer with another market's rate. Upgrade the gateway sidecar."
        ),
    )


# =============================================================================
# Constants
# =============================================================================


# Rate side (supply or borrow)
class RateSide(StrEnum):
    """Lending rate side."""

    SUPPLY = "supply"
    BORROW = "borrow"


def _supported_protocols() -> list[str]:
    """Lending venues with a declared gateway rate lane.

    Manifest-derived (``LendingReadDecl.rate_history_chains``, VIB-4851
    Phase D); the module-level ``SUPPORTED_PROTOCOLS`` / ``PROTOCOL_CHAINS``
    names stay importable via ``__getattr__`` below.
    """
    from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

    return list(LendingReadRegistry.rate_history_protocols())


def _protocols_for_chain(chain: str) -> list[str]:
    """Rate-lane venues declaring ``chain`` (legacy PROTOCOL_CHAINS rows)."""
    from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

    return list(LendingReadRegistry.rate_history_protocols_for_chain(chain))


def _protocol_chains() -> dict[str, list[str]]:
    """Legacy ``PROTOCOL_CHAINS`` view, derived per declared chain."""
    from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

    return {chain: _protocols_for_chain(chain) for chain in sorted(LendingReadRegistry.all_rate_history_chains())}


def __getattr__(name: str):  # noqa: ANN202 - PEP 562 lazy back-compat hook
    """Serve the legacy derived constants without import-time discovery."""
    if name == "SUPPORTED_PROTOCOLS":
        return _supported_protocols()
    if name == "PROTOCOL_CHAINS":
        return _protocol_chains()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# Common tokens supported by lending protocols
SUPPORTED_TOKENS: dict[str, list[str]] = {
    "ethereum": ["USDC", "USDT", "DAI", "WETH", "WBTC", "wstETH", "cbETH", "rETH"],
    "arbitrum": ["USDC", "USDC.e", "USDT", "DAI", "WETH", "WBTC", "ARB", "wstETH", "rETH"],
    "optimism": ["USDC", "USDC.e", "USDT", "DAI", "WETH", "wstETH", "OP", "rETH"],
    "polygon": ["USDC", "USDC.e", "USDT", "DAI", "WETH", "WBTC", "WMATIC", "wstETH"],
    "base": ["USDC", "WETH", "cbETH", "wstETH"],
    "avalanche": ["USDC", "USDT", "DAI.e", "WETH.e", "WBTC.e", "WAVAX", "sAVAX"],
}

# Default cache TTL in seconds (one block ~12s)
DEFAULT_CACHE_TTL_SECONDS = 12.0

# Ray unit for Aave (1e27)
RAY = Decimal("1000000000000000000000000000")

# Seconds per year for APY calculations
SECONDS_PER_YEAR = 365 * 24 * 60 * 60


# =============================================================================
# Exceptions
# =============================================================================


class RateMonitorError(Exception):
    """Base exception for rate monitor errors."""

    pass


class RateUnavailableError(RateMonitorError):
    """Raised when rate cannot be fetched."""

    def __init__(self, protocol: str, token: str, side: str, reason: str) -> None:
        self.protocol = protocol
        self.token = token
        self.side = side
        self.reason = reason
        super().__init__(f"Rate unavailable for {protocol}/{token}/{side}: {reason}")


class ProtocolNotSupportedError(RateMonitorError):
    """Raised when protocol is not supported on chain."""

    def __init__(self, protocol: str, chain: str) -> None:
        self.protocol = protocol
        self.chain = chain
        supported = _protocols_for_chain(chain)
        super().__init__(f"Protocol '{protocol}' not supported on {chain}. Supported protocols: {supported}")


class TokenNotSupportedError(RateMonitorError):
    """Raised when token is not supported by protocol.

    .. deprecated:: W7 / VIB-4859
        Prefer :class:`almanak.framework.data.interfaces.DataSourceUnavailable`
        for all "no data" paths. This exception remains for back-compat but
        new call sites should raise ``DataSourceUnavailable`` directly
        (matches the rest of the framework data layer).
    """

    def __init__(self, token: str, protocol: str, chain: str) -> None:
        self.token = token
        self.protocol = protocol
        self.chain = chain
        super().__init__(f"Token '{token}' not supported by {protocol} on {chain}")


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class LendingRate:
    """Lending rate data for a specific protocol/token/side.

    Attributes:
        protocol: Protocol identifier (aave_v3, morpho_blue, compound_v3)
        token: Token symbol
        side: Rate side (supply or borrow)
        apy_ray: APY in ray units (1e27) for precision
        apy_percent: APY as percentage (e.g., 5.25 for 5.25%)
        utilization_percent: Pool utilization as percentage
        timestamp: When the rate was fetched
        chain: Blockchain network
        market_id: Market identifier (for Morpho/Compound)
    """

    protocol: str
    token: str
    side: str
    apy_ray: Decimal
    apy_percent: Decimal
    utilization_percent: Decimal | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    chain: str = "ethereum"
    market_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "protocol": self.protocol,
            "token": self.token,
            "side": self.side,
            "apy_ray": str(self.apy_ray),
            "apy_percent": float(self.apy_percent),
            "utilization_percent": float(self.utilization_percent) if self.utilization_percent else None,
            "timestamp": self.timestamp.isoformat(),
            "chain": self.chain,
            "market_id": self.market_id,
        }


@dataclass
class LendingRateResult:
    """Result of a lending rate query.

    Attributes:
        success: Whether the query succeeded
        rate: The lending rate if successful
        error: Error message if failed
    """

    success: bool
    rate: LendingRate | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "rate": self.rate.to_dict() if self.rate else None,
            "error": self.error,
        }


@dataclass
class BestRateResult:
    """Result of a best rate query across protocols.

    Attributes:
        token: Token symbol
        side: Rate side
        best_rate: The best lending rate found
        all_rates: All rates from different protocols
        timestamp: When the comparison was made
    """

    token: str
    side: str
    best_rate: LendingRate | None
    all_rates: list[LendingRate]
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "token": self.token,
            "side": self.side,
            "best_rate": self.best_rate.to_dict() if self.best_rate else None,
            "all_rates": [r.to_dict() for r in self.all_rates],
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class ProtocolRates:
    """Rates for all tokens in a protocol.

    Attributes:
        protocol: Protocol identifier
        chain: Blockchain network
        rates: Dictionary mapping token -> side -> rate
        timestamp: When rates were fetched
    """

    protocol: str
    chain: str
    rates: dict[str, dict[str, LendingRate]]
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def get_rate(self, token: str, side: str) -> LendingRate | None:
        """Get rate for a token and side."""
        token_rates = self.rates.get(token)
        if token_rates:
            return token_rates.get(side)
        return None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "protocol": self.protocol,
            "chain": self.chain,
            "rates": {
                token: {side: rate.to_dict() for side, rate in sides.items()} for token, sides in self.rates.items()
            },
            "timestamp": self.timestamp.isoformat(),
        }


# =============================================================================
# Placeholder rates (preserved for tests / non-RPC environments per VIB-4859
# risk-mitigation §7.6). The gateway-side ``RateHistoryService`` lives behind
# a connector and only serves real data; when no gateway is reachable, the
# framework client falls back to these per-protocol defaults to keep the
# test surface (``tests/unit/data/rates/test_rate_monitor_onchain.py``) +
# the strategy authors' offline backtests working without a live RPC.
# =============================================================================


_AAVE_DEFAULT_SUPPLY: dict[str, Decimal] = {
    "USDC": Decimal("4.25"),
    "USDT": Decimal("3.85"),
    "DAI": Decimal("3.95"),
    "WETH": Decimal("2.15"),
    "WBTC": Decimal("0.45"),
    "wstETH": Decimal("0.05"),
    "cbETH": Decimal("0.08"),
    "rETH": Decimal("0.06"),
}
_AAVE_DEFAULT_BORROW: dict[str, Decimal] = {
    "USDC": Decimal("5.75"),
    "USDT": Decimal("5.25"),
    "DAI": Decimal("5.45"),
    "WETH": Decimal("3.85"),
    "WBTC": Decimal("1.25"),
    "wstETH": Decimal("0.85"),
    "cbETH": Decimal("1.05"),
    "rETH": Decimal("0.95"),
}
_MORPHO_DEFAULT_SUPPLY: dict[str, Decimal] = {
    "USDC": Decimal("5.15"),
    "USDT": Decimal("4.75"),
    "WETH": Decimal("2.85"),
    "wstETH": Decimal("0.12"),
    "cbETH": Decimal("0.15"),
}
_MORPHO_DEFAULT_BORROW: dict[str, Decimal] = {
    "USDC": Decimal("5.25"),
    "USDT": Decimal("4.85"),
    "WETH": Decimal("3.25"),
    "wstETH": Decimal("0.65"),
    "cbETH": Decimal("0.85"),
}
_COMPOUND_DEFAULT_SUPPLY: dict[str, Decimal] = {
    "USDC": Decimal("4.85"),
    "USDC.e": Decimal("4.85"),
    "USDT": Decimal("4.25"),
    "WETH": Decimal("2.35"),
}
_COMPOUND_DEFAULT_BORROW: dict[str, Decimal] = {
    "USDC": Decimal("6.15"),
    "USDC.e": Decimal("6.15"),
    "USDT": Decimal("5.75"),
    "WETH": Decimal("4.15"),
}


# Mirror of the gateway-side Compound V3 token → market mapping for
# placeholder-rate market-id reporting.
_COMPOUND_TOKEN_TO_MARKET: dict[str, str] = {
    "USDC": "usdc",
    "USDC.e": "usdc_bridged",
    "USDT": "usdt",
    "WETH": "weth",
    "wstETH": "wsteth",
    "USDS": "usds",
}


@dataclass(frozen=True)
class _PlaceholderProfile:
    """Per-protocol placeholder-rate profile.

    The (supply, borrow) tables and the default utilisation are stamped
    on this struct so the placeholder lookup is table-driven instead of
    a protocol-keyed ``if`` chain (preserves the W7 "no per-protocol
    dispatch in framework consumer files" invariant).
    """

    supply: dict[str, Decimal]
    borrow: dict[str, Decimal]
    utilization_pct: Decimal
    market_id_resolver: "Any | None" = None  # callable(token) -> str | None


def _aave_market_id(_token: str) -> str | None:
    return None


def _morpho_market_id(_token: str) -> str | None:
    return "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"


def _compound_market_id(token: str) -> str | None:
    return _COMPOUND_TOKEN_TO_MARKET.get(token)


_PLACEHOLDER_PROFILES: dict[str, _PlaceholderProfile] = {
    "aave_v3": _PlaceholderProfile(
        supply=_AAVE_DEFAULT_SUPPLY,
        borrow=_AAVE_DEFAULT_BORROW,
        utilization_pct=Decimal("72.5"),
        market_id_resolver=_aave_market_id,
    ),
    "morpho_blue": _PlaceholderProfile(
        supply=_MORPHO_DEFAULT_SUPPLY,
        borrow=_MORPHO_DEFAULT_BORROW,
        utilization_pct=Decimal("68.0"),
        market_id_resolver=_morpho_market_id,
    ),
    "compound_v3": _PlaceholderProfile(
        supply=_COMPOUND_DEFAULT_SUPPLY,
        borrow=_COMPOUND_DEFAULT_BORROW,
        utilization_pct=Decimal("75.0"),
        market_id_resolver=_compound_market_id,
    ),
}


def _manifest_default_placeholder(protocol: str, token: str, side: str, chain: str) -> "LendingRate":
    """Offline placeholder derived from the connector's manifest default APY.

    Rate-lane protocols that ship no curated per-token placeholder table above
    (e.g. spark — an Aave V3 fork that carries no ``AAVE_V3_TOKENS``-style
    catalogue and resolves symbols through the global ``TokenResolver``) fall
    back to their manifest-declared ``backtest_default_{supply,borrow}_apy``
    when the gateway is unreachable, instead of raising
    ``TokenNotSupportedError``. The manifest is the single source of truth for
    the sanctioned offline default (Empty != Zero: a venue that declares no
    default still fails loud rather than backtesting at a fabricated rate).

    Manifest APYs are decimal fractions (``"0.05"`` = 5%); the placeholder API
    speaks percentages, so scale by 100.
    """
    from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

    supply, borrow = LendingReadRegistry.backtest_default_apys(protocol)
    fraction = supply if side == "supply" else borrow
    if fraction is None:
        raise TokenNotSupportedError(token, protocol, chain)
    apy_percent = Decimal(fraction) * Decimal("100")
    return LendingRate(
        protocol=protocol,
        token=token,
        side=side,
        apy_ray=apy_percent * RAY / Decimal("100"),
        apy_percent=apy_percent,
        chain=chain,
    )


def _placeholder_rate(protocol: str, token: str, side: str, chain: str) -> LendingRate:
    """Return a placeholder LendingRate when the gateway is unreachable.

    Mirrors the pre-W7 ``_aave_v3_placeholder_rate`` /
    ``_compound_v3_placeholder_rate`` / ``_fetch_morpho_rate`` constants
    that lived inline in this module. Used by tests and offline-backtest
    callers that don't have a gateway running. Table-driven (no
    per-protocol ``if`` chain) per VIB-4859.
    """
    profile = _PLACEHOLDER_PROFILES.get(protocol)
    if profile is None:
        # No curated per-token table for this rate-lane venue (e.g. spark):
        # fall back to the manifest-declared default APY rather than crashing.
        return _manifest_default_placeholder(protocol, token, side, chain)

    table = profile.supply if side == "supply" else profile.borrow
    apy_percent = table.get(token)
    if apy_percent is None:
        raise TokenNotSupportedError(token, protocol, chain)

    market_id = profile.market_id_resolver(token) if profile.market_id_resolver else None
    return LendingRate(
        protocol=protocol,
        token=token,
        side=side,
        apy_ray=apy_percent * RAY / Decimal("100"),
        apy_percent=apy_percent,
        utilization_percent=profile.utilization_pct,
        chain=chain,
        market_id=market_id,
    )


# =============================================================================
# Rate Monitor (thin gRPC client of RateHistoryService — VIB-4859 / W7)
# =============================================================================


class RateMonitor:
    """Unified lending rate monitor for multiple DeFi protocols.

    .. deprecated:: VIB-4859 (W7)
        **Not a public strategy API.** Strategy code must use
        :meth:`almanak.framework.market.snapshot.MarketSnapshot.lending_rate`
        / :meth:`~almanak.framework.market.snapshot.MarketSnapshot.best_lending_rate`
        — the canonical strategy-side accessors. Constructing ``RateMonitor``
        directly from strategy code emits a :class:`DeprecationWarning`.

    Disposition (VIB-4869): the strategy / demo / doc callers have been
    migrated to ``MarketSnapshot.lending_rate(...)``, but the class is
    **retained as the framework-internal gateway client** that backs those
    accessors. ``MarketSnapshot`` and the strategy runner
    (``framework/cli/run_helpers.py``) construct it with ``_internal=True``
    to wire the gateway-backed rate source onto the snapshot; that path is
    silent (no deprecation warning) because it IS the canonical lane, not a
    legacy bypass. Deleting the class would require re-inlining its gateway
    gRPC client, caching, and cross-protocol aggregation into
    ``MarketSnapshot`` — a larger redesign outside the caller-migration
    scope.

    This class is now a thin wrapper around the gateway's
    ``RateHistoryService.GetLendingRateCurrent`` RPC. All HTTP / Web3 egress
    happens server-side in the gateway sidecar; the strategy container only
    speaks gRPC. The public API (``get_lending_rate``,
    ``get_best_lending_rate``, ``get_protocol_rates``) and dataclass shapes
    (``LendingRate``, ``BestRateResult``, ``ProtocolRates``) are preserved
    verbatim for back-compat.

    When the gateway is unreachable (e.g. offline test environments,
    backtests with no live RPC), the wrapper falls back to per-protocol
    placeholder rates so existing tests and offline replays continue to work.

    Attributes:
        chain: Blockchain network
        cache_ttl_seconds: How long to cache rates (default 12s)
        protocols: List of protocols to monitor

    Example:
        monitor = RateMonitor(chain="ethereum")

        # Get specific rate
        rate = await monitor.get_lending_rate("aave_v3", "USDC", RateSide.SUPPLY)

        # Get best rate
        best = await monitor.get_best_lending_rate("USDC", RateSide.SUPPLY)

        # Get all rates for a protocol
        rates = await monitor.get_protocol_rates("aave_v3")
    """

    def __init__(
        self,
        chain: str = "ethereum",
        cache_ttl_seconds: float = DEFAULT_CACHE_TTL_SECONDS,
        protocols: list[str] | None = None,
        rpc_url: str | None = None,
        *,
        _internal: bool = False,
    ) -> None:
        """Initialize the RateMonitor.

        Args:
            chain: Blockchain network (ethereum, arbitrum, etc.)
            cache_ttl_seconds: Cache TTL in seconds (default 12s = ~1 block)
            protocols: Protocols to monitor (default: all available on chain)
            rpc_url: Ignored. Kept for back-compat with pre-W7 callers
                that passed an RPC URL — all RPC egress now lives behind
                the gateway's ``RateHistoryService``.
            _internal: Framework-internal flag (keyword-only). When ``True``
                the deprecation warning is suppressed because the caller is
                the canonical ``MarketSnapshot.lending_rate`` lane (the
                snapshot or the strategy runner wiring it), not strategy code
                using the deprecated public surface. See the class docstring
                (VIB-4869 disposition). Strategy code MUST NOT pass this.
        """
        if not _internal:
            warnings.warn(
                "RateMonitor is not a public strategy API and is deprecated as "
                "of VIB-4859 (W7). Use "
                "almanak.framework.market.snapshot.MarketSnapshot.lending_rate() / "
                "best_lending_rate() instead (VIB-4869).",
                DeprecationWarning,
                stacklevel=2,
            )

        self._chain = chain
        self._cache_ttl_seconds = cache_ttl_seconds
        # Preserved for back-compat with pre-W7 callers — the gateway client
        # ignores it (RPC egress lives gateway-side now).
        self._rpc_url = rpc_url

        # Determine available protocols for this chain
        available = _protocols_for_chain(chain)
        if protocols:
            self._protocols = [p for p in protocols if p in available]
        else:
            self._protocols = available

        # Rate cache: protocol -> token -> side -> (rate, timestamp)
        self._cache: dict[str, dict[str, dict[str, tuple[LendingRate, float]]]] = {}

        # Mock rate providers (for testing without RPC/gateway)
        self._mock_rates: dict[str, dict[str, dict[str, Decimal]]] = {}

        logger.info(
            f"RateMonitor initialized for chain={chain}, protocols={self._protocols}, cache_ttl={cache_ttl_seconds}s"
        )

    @property
    def chain(self) -> str:
        """Get the chain."""
        return self._chain

    @property
    def protocols(self) -> list[str]:
        """Get monitored protocols."""
        return self._protocols.copy()

    def set_mock_rate(
        self,
        protocol: str,
        token: str,
        side: str,
        apy_percent: Decimal,
    ) -> None:
        """Set a mock rate for testing.

        Args:
            protocol: Protocol identifier
            token: Token symbol
            side: supply or borrow
            apy_percent: APY as percentage (e.g., 5.0 for 5%)
        """
        if protocol not in self._mock_rates:
            self._mock_rates[protocol] = {}
        if token not in self._mock_rates[protocol]:
            self._mock_rates[protocol][token] = {}
        self._mock_rates[protocol][token][side] = apy_percent

    def clear_mock_rates(self) -> None:
        """Clear all mock rates."""
        self._mock_rates.clear()

    @staticmethod
    def _cache_side_key(side: str, market_id: str | None) -> str:
        """Innermost cache key — ``side``, scoped by market when market-scoped.

        Two isolated markets can lend the SAME token at different rates
        (VIB-5729), so ``(protocol, token, side)`` is NOT a unique key for an
        isolated-market venue: a market-blind key would serve the first market's
        rate for the second. Unscoped reads keep the bare ``side`` key, so the
        Aave-family lane is byte-identical to before. A whitespace-only
        ``market_id`` is not a scoping claim and collapses to the unscoped key
        rather than minting a bogus ``"side|"`` slot (gemini, PR #3287).
        """
        scoped = (market_id or "").strip().lower()
        return f"{side}|{scoped}" if scoped else side

    def _get_cached_rate(
        self,
        protocol: str,
        token: str,
        side: str,
        market_id: str | None = None,
    ) -> LendingRate | None:
        """Get cached rate if still valid."""
        key = self._cache_side_key(side, market_id)
        try:
            cached = self._cache[protocol][token][key]
            rate, cache_time = cached
            age = time.time() - cache_time
            if age < self._cache_ttl_seconds:
                logger.debug(f"Cache hit for {protocol}/{token}/{key} (age: {age:.1f}s)")
                return rate
        except KeyError:
            pass
        return None

    def _set_cached_rate(
        self,
        protocol: str,
        token: str,
        side: str,
        rate: LendingRate,
        market_id: str | None = None,
    ) -> None:
        """Cache a rate."""
        if protocol not in self._cache:
            self._cache[protocol] = {}
        if token not in self._cache[protocol]:
            self._cache[protocol][token] = {}
        self._cache[protocol][token][self._cache_side_key(side, market_id)] = (rate, time.time())

    async def get_lending_rate(
        self,
        protocol: str,
        token: str,
        side: RateSide,
        market_id: str | None = None,
    ) -> LendingRate:
        """Get lending rate for a specific protocol/token/side.

        Args:
            protocol: Protocol identifier (aave_v3, morpho_blue, compound_v3)
            token: Token symbol (USDC, WETH, etc.)
            side: Rate side (SUPPLY or BORROW)
            market_id: Optional market scoping for isolated-market lenders
                (Morpho Blue). REQUIRED to get the rate of a specific market —
                without it an isolated-market venue answers with a best-across-
                markets selection, which is not any single position's rate
                (VIB-5729). A market-scoped call is treated as accounting-grade:
                it NEVER degrades to a placeholder (see below).

        Returns:
            LendingRate with APY data

        Raises:
            ProtocolNotSupportedError: If protocol not available on chain
            TokenNotSupportedError: If token not supported
            RateUnavailableError: If rate cannot be fetched — including when a
                requested ``market_id`` scoping was not honoured by the gateway.
        """
        side_str = side.value if isinstance(side, RateSide) else side

        # Validate protocol
        if protocol not in self._protocols:
            raise ProtocolNotSupportedError(protocol, self._chain)

        # Check cache first. The key carries market_id (VIB-5729): two isolated
        # markets can lend the SAME token at different rates, so a market-blind
        # key would serve one market's rate for the other.
        cached = self._get_cached_rate(protocol, token, side_str, market_id)
        if cached is not None:
            return cached

        # Check for mock rate
        if protocol in self._mock_rates:
            token_rates = self._mock_rates[protocol].get(token, {})
            if side_str in token_rates:
                apy_percent = token_rates[side_str]
                rate = LendingRate(
                    protocol=protocol,
                    token=token,
                    side=side_str,
                    apy_ray=apy_percent * RAY / Decimal("100"),
                    apy_percent=apy_percent,
                    chain=self._chain,
                )
                self._set_cached_rate(protocol, token, side_str, rate)
                return rate

        # Fetch from gateway via RateHistoryService.GetLendingRateCurrent.
        try:
            rate = await self._fetch_lending_rate_via_gateway(protocol, token, side_str, market_id)
        except (ProtocolNotSupportedError, TokenNotSupportedError):
            raise
        except DataSourceUnavailable as exc:
            if market_id:
                # A market-scoped read is accounting-grade: its answer is
                # persisted as a MEASURED rate. `_placeholder_rate` returns a
                # hardcoded constant, which for that purpose is a fabrication,
                # not a rate — and it cannot honour market scoping anyway (its
                # tables are keyed by token). Empty != Zero: fail loudly so the
                # caller records honest-unmeasured instead of a plausible
                # invention (VIB-5729).
                logger.warning(
                    "Market-scoped lending-rate lookup unavailable for %s/%s/%s market=%s on %s: %s; "
                    "NOT falling back to a placeholder (would fabricate a measured rate).",
                    protocol,
                    token,
                    side_str,
                    market_id,
                    self._chain,
                    exc,
                )
                raise RateUnavailableError(protocol, token, side_str, str(exc)) from exc
            # Gateway returned success=false (typed "no data" envelope) or
            # is unreachable. Fall back to the offline placeholder lane so
            # tests / offline backtests don't break. Production callers
            # should see the placeholder as a warning, not a real rate.
            logger.warning(
                "Gateway lending-rate lookup unavailable for %s/%s/%s on %s: %s; falling back to placeholder.",
                protocol,
                token,
                side_str,
                self._chain,
                exc,
            )
            rate = _placeholder_rate(protocol, token, side_str, self._chain)
        except Exception as e:
            logger.warning(f"Failed to fetch rate for {protocol}/{token}/{side_str}: {e}")
            raise RateUnavailableError(protocol, token, side_str, str(e)) from e

        self._set_cached_rate(protocol, token, side_str, rate, market_id)
        return rate

    async def _fetch_lending_rate_via_gateway(
        self,
        protocol: str,
        token: str,
        side: str,
        market_id: str | None = None,
    ) -> LendingRate:
        """Translate ``GetLendingRateCurrent`` RPC result to a ``LendingRate``.

        Raises :class:`DataSourceUnavailable` on any wire-level failure (the
        caller maps that to a placeholder-rate fallback for back-compat), and
        also when a requested ``market_id`` scoping was not honoured (VIB-5729).
        """
        client, gateway_pb2 = _monitor_get_connected_gateway_client()
        response = await _monitor_call_lending_rate_current(
            client,
            gateway_pb2,
            protocol=protocol,
            chain=self._chain,
            token=token,
            side=side,
            market_id=market_id,
        )
        return _build_lending_rate_from_point(
            response,
            protocol=protocol,
            token=token,
            side=side,
            chain=self._chain,
        )

    async def get_best_lending_rate(
        self,
        token: str,
        side: RateSide,
        protocols: list[str] | None = None,
    ) -> BestRateResult:
        """Get the best lending rate across protocols for a token.

        For supply rates, returns the highest rate.
        For borrow rates, returns the lowest rate.

        Args:
            token: Token symbol
            side: Rate side (SUPPLY or BORROW)
            protocols: Protocols to compare (default: all available)

        Returns:
            BestRateResult with best rate and all rates
        """
        side_str = side.value if isinstance(side, RateSide) else side
        target_protocols = protocols or self._protocols

        # Fetch rates from all protocols in parallel
        tasks = []
        for protocol in target_protocols:
            tasks.append(self._safe_get_rate(protocol, token, side_str))

        results = await asyncio.gather(*tasks)

        # Collect successful rates
        all_rates: list[LendingRate] = []
        for result in results:
            if result is not None:
                all_rates.append(result)

        # Find best rate
        best_rate: LendingRate | None = None
        if all_rates:
            if side_str == RateSide.SUPPLY.value:
                # For supply, higher APY is better
                best_rate = max(all_rates, key=lambda r: r.apy_percent)
            else:
                # For borrow, lower APY is better
                best_rate = min(all_rates, key=lambda r: r.apy_percent)

        return BestRateResult(
            token=token,
            side=side_str,
            best_rate=best_rate,
            all_rates=all_rates,
        )

    async def get_protocol_rates(
        self,
        protocol: str,
        tokens: list[str] | None = None,
    ) -> ProtocolRates:
        """Get all rates for a protocol.

        Args:
            protocol: Protocol identifier
            tokens: Tokens to fetch (default: common tokens for chain)

        Returns:
            ProtocolRates with all token rates
        """
        if protocol not in self._protocols:
            raise ProtocolNotSupportedError(protocol, self._chain)

        target_tokens = tokens or SUPPORTED_TOKENS.get(self._chain, [])

        rates: dict[str, dict[str, LendingRate]] = {}

        for token in target_tokens:
            token_rates: dict[str, LendingRate] = {}

            for side in [RateSide.SUPPLY, RateSide.BORROW]:
                rate = await self._safe_get_rate(protocol, token, side.value)
                if rate is not None:
                    token_rates[side.value] = rate

            if token_rates:
                rates[token] = token_rates

        return ProtocolRates(
            protocol=protocol,
            chain=self._chain,
            rates=rates,
        )

    async def _safe_get_rate(
        self,
        protocol: str,
        token: str,
        side: str,
    ) -> LendingRate | None:
        """Safely get a rate, returning None on error."""
        try:
            return await self.get_lending_rate(protocol, token, RateSide(side) if isinstance(side, str) else side)
        except (RateUnavailableError, ProtocolNotSupportedError, TokenNotSupportedError):
            return None
        except Exception as e:
            logger.debug(f"Failed to get rate for {protocol}/{token}/{side}: {e}")
            return None

    def clear_cache(self) -> None:
        """Clear all cached rates."""
        self._cache.clear()
        logger.debug("Rate cache cleared")

    def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        total_entries = sum(len(sides) for tokens in self._cache.values() for sides in tokens.values())
        return {
            "total_entries": total_entries,
            "protocols": list(self._cache.keys()),
            "ttl_seconds": self._cache_ttl_seconds,
        }


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Main service
    "RateMonitor",
    # Data classes
    "LendingRate",
    "LendingRateResult",
    "BestRateResult",
    "ProtocolRates",
    # Enums
    "RateSide",
    # Exceptions
    "RateMonitorError",
    "RateUnavailableError",
    "ProtocolNotSupportedError",
    "TokenNotSupportedError",
    # Constants
    "SUPPORTED_TOKENS",
    "DEFAULT_CACHE_TTL_SECONDS",
    "RAY",
    "SECONDS_PER_YEAR",
]
