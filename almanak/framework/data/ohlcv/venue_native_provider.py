"""Gateway-backed venue-native OHLCV provider (ALM-3148 / ALM-3152).

Consumes the ``GetPerpPriceCandles`` RPC that ALM-3149 shipped. That capability
already existed and was already correct; its only consumer was the backtesting
engine, so the live strategy path could not see it. This module is the wire.

Gateway boundary: this provider makes no HTTP call. It asks the gateway for a
market-scoped page; the connector resolves ``market -> market token -> index
token -> index symbol``, verifies the tuple on-chain, and owns the provider
egress.

Venue-agnostic by construction: the ``venue`` string is connector-declared and
travels in the request. Hyperliquid and Aster reach the live signal lane by
declaring ``perp_price_history`` on their manifest and implementing
``GatewayPerpPriceHistoryCapability`` — no change here.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from almanak.core.finality import DataFinality
from almanak.core.perp_markets import perp_market_pair_key
from almanak.framework.data.interfaces import (
    DataSourceUnavailable,
    OHLCVCandle,
)
from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta
from almanak.framework.data.ohlcv.venue_context import (
    VENUE_NATIVE_PROVIDER,
    OHLCVSourcePolicy,
    price_identity,
)
from almanak.framework.data.timeframes import OHLCVTimeframe, parse_ohlcv_timeframe

__all__ = ["VenueNativeOHLCVProvider"]

logger = logging.getLogger(__name__)


def _market_identity(label: str) -> str:
    """Reduce a venue market label to the identity two spellings must share.

    A venue may echo back a disambiguated full name (``ETH/USD [ETH-USDC]``) or
    a raw market address for the label the request carried, so string equality
    would reject correct answers. What must not differ is the *instrument*: the
    part before any collateral bracket, normalised.

    Normalisation is delegated to :func:`perp_market_pair_key` — the same
    function the perp connectors resolve markets through
    (``gmx_v2.market_identity.canonicalise_market`` wraps it). Hand-rolling it here
    normalised strictly less than the registry the request is resolved against:
    ``ETH_USD`` and ``ETH:USD`` are documented spellings of ``ETH/USD`` and were
    refused as a different market. A guard must not disagree with the resolver
    about what two names mean — on this lane that disagreement is terminal.
    """
    head = label.split("[", 1)[0].strip()
    return perp_market_pair_key(head) or head.upper()


def _market_labels_agree(served: str, requested: str) -> bool:
    """True when a served market label is the one that was requested.

    Exact on identity, tolerant only of the spellings the venue and the registry
    are both entitled to use for one market: a collateral bracket
    (``ETH/USD [ETH-USDC]``), any of the documented pair separators, and a bare
    connector alias. Prefix matching was tried and is wrong — it accepts
    ``BTC/USDT`` for a ``BTC/USD`` request, which is a different instrument.

    An address request (``0x…``) cannot be compared as a label, because the
    venue answers it with a human-readable name. Those are bound by
    :func:`_index_symbols_agree` on the index symbol instead, never waved
    through: an unchecked address request was the widest hole in the first
    version of this guard.
    """
    if requested.strip().lower().startswith("0x"):
        return True
    served_id = _market_identity(served)
    requested_id = _market_identity(requested)
    if served_id == requested_id:
        return True
    # A bare connector alias (``ETH``) names a market without naming its quote,
    # and `perp_market_pair_key` deliberately leaves it bare so the connector
    # keeps owning that resolution. The venue always answers from its catalogue
    # record, which is a full label, so requiring equality made every bare-alias
    # config a terminal refusal on the one lane that has no fallback to absorb
    # it. Tolerated in this direction only: the served side being *vaguer* than
    # the request would be the venue failing to confirm what was asked.
    if "/" not in requested_id and "/" in served_id:
        return served_id.split("/", 1)[0] == requested_id
    return False


def _index_symbols_agree(served_index: str, requested_base: str) -> bool:
    """True when the venue's index symbol is the asset that was asked about.

    This is the check that actually binds a page to the request. Everything
    else — market label, token addresses — can be internally consistent on a
    page that is about a different asset entirely; the index symbol is what the
    candles are *of*.

    Both sides are reduced to the asset they price, because the venue and the
    router genuinely disagree on spelling: GMX answers with the *token* symbol
    from its ``/tokens`` endpoint (``WETH``, ``WBTC``, and on Avalanche
    ``WETH.e`` / ``BTC.b``) while a strategy declares the asset (``ETH``,
    ``BTC``). Only reductions that provably preserve the price plane are
    applied.
    """
    served = (served_index or "").strip()
    requested = (requested_base or "").strip()
    if not served or not requested:
        return False
    return price_identity(served) == price_identity(requested)


class VenueNativeOHLCVProvider:
    """Serve a strategy's own venue index candles to the OHLCV router.

    Registers under the router name ``"venue_native"``.
    """

    def __init__(self, *, gateway_client: Any, policy: OHLCVSourcePolicy) -> None:
        self._gateway_client = gateway_client
        self._policy = policy

    # -- DataProvider protocol ------------------------------------------------

    @property
    def name(self) -> str:
        """Provider name matching the router's ``_PROVIDER_CHAINS`` key."""
        return VENUE_NATIVE_PROVIDER

    @property
    def data_class(self) -> DataClassification:
        """OHLCV data is informational (never execution-grade)."""
        return DataClassification.INFORMATIONAL

    def fetch(self, **kwargs: object) -> DataEnvelope:
        """Synchronous DataProvider entry point.

        Keyword Args:
            token: Canonical base symbol (str) — the router passes
                ``instrument.base``.
            chain: Canonical chain name (str).
            timeframe: Candle timeframe.
            limit: Maximum number of candles.

        Raises:
            DataSourceUnavailable: The policy does not claim this instrument,
                the gateway is unreachable, or the venue refused the request.
        """
        token = str(kwargs.get("token", ""))
        chain_raw = kwargs.get("chain")
        chain = str(chain_raw).strip().lower() if chain_raw else (self._policy.chain or "")
        timeframe = parse_ohlcv_timeframe(kwargs.get("timeframe", OHLCVTimeframe.ONE_HOUR))
        limit = int(kwargs.get("limit", 100))  # type: ignore[call-overload]

        market = self._policy.market_for(token, chain)
        if market is None or not self._policy.venue:
            # Not a routing failure to retry — this instrument simply is not
            # one the strategy trades on its venue. Say so precisely so the
            # router's composed error names the real cause.
            raise DataSourceUnavailable(
                source=self.name,
                reason=(
                    f"{token} is not a market this strategy trades on "
                    f"{self._policy.venue or 'its venue'}" + (f" ({chain})" if chain else "")
                ),
            )

        response = self._request_page(
            venue=self._policy.venue,
            chain=chain or (self._policy.chain or ""),
            market=market,
            base=token,
            timeframe=timeframe,
            limit=limit,
        )
        candles = self._decode_candles(response, timeframe)

        logger.debug(
            "ohlcv_venue_native venue=%s chain=%s market=%s index=%s timeframe=%s candles=%d",
            self._policy.venue,
            chain,
            response.market or market,
            response.index_symbol,
            timeframe,
            len(candles),
        )
        return DataEnvelope(
            value=candles,
            meta=DataMeta(
                source=self.name,
                observed_at=datetime.now(UTC),
                finality=DataFinality.OFF_CHAIN,
                staleness_ms=0,
                latency_ms=0,
                # Highest confidence for a venue-traded instrument: this is the
                # plane the position is marked and liquidated against. The
                # router's CEX-serving-DeFi downgrade is the opposite case and
                # does not apply.
                confidence=1.0,
                cache_hit=False,
            ),
            classification=DataClassification.INFORMATIONAL,
        )

    def health(self) -> dict[str, object]:
        """Report the policy this provider was built with."""
        return {
            "provider": self.name,
            "venue": self._policy.venue,
            "chain": self._policy.chain,
            "markets": dict(self._policy.markets),
        }

    # -- internals ------------------------------------------------------------

    def _request_page(
        self,
        *,
        venue: str,
        chain: str,
        market: str,
        base: str,
        timeframe: OHLCVTimeframe,
        limit: int,
    ) -> Any:
        from almanak.gateway.proto import gateway_pb2

        client = self._gateway_client
        if client is None:
            raise DataSourceUnavailable(source=self.name, reason="no gateway client configured", transport=True)
        if not getattr(client, "is_connected", False):
            connect = getattr(client, "connect", None)
            if callable(connect):
                connect()

        stub = getattr(getattr(client, "rate_history", None), "GetPerpPriceCandles", None)
        if stub is None:
            raise DataSourceUnavailable(
                source=self.name,
                reason="connected gateway does not expose GetPerpPriceCandles",
                transport=True,
            )

        request = gateway_pb2.GetPerpPriceCandlesRequest(
            venue=venue,
            chain=chain,
            market=market,
            timeframe=timeframe.value,
            # Exclusive upper bound. "Now" asks for the most recent page,
            # including the candle currently forming.
            before_ts=int(time.time()),
            limit=max(1, min(int(limit), 10_000)),
        )
        try:
            response = stub(request, timeout=getattr(getattr(client, "config", None), "timeout", 30.0))
        except Exception as exc:
            raise DataSourceUnavailable(
                source=self.name,
                reason=f"GetPerpPriceCandles RPC failed for {market} on {chain}: {exc}",
                transport=True,
            ) from exc

        if not response.success:
            raise DataSourceUnavailable(
                source=self.name,
                reason=response.error or f"{venue} returned no candles for {market} on {chain}",
            )
        # Market -> index verification travels with the page precisely so a
        # symbol series can be proven to belong to the requested market. A page
        # that lost it is not usable evidence about any market.
        if not (response.market_token and response.index_token and response.index_symbol):
            raise DataSourceUnavailable(
                source=self.name,
                reason=f"{venue} returned incomplete market provenance for {market} on {chain}",
            )
        # Provenance that is merely *present* proves nothing: it has to be the
        # provenance of the market we asked about. Without this, a page that is
        # internally consistent but belongs to a different market — a gateway
        # dispatch bug, a mis-associated response, a connector that resolved the
        # wrong catalogue entry — is accepted and served, and the strategy
        # decides on another instrument's prices with full confidence. The
        # comparison is deliberately loose about spelling (the venue may echo a
        # disambiguated full name or an address for the label we sent) and
        # strict about identity.
        served_market = str(response.market or "")
        if not served_market:
            # An empty label used to skip the comparison entirely, which turned
            # the guard off exactly when the page was least identifiable.
            raise DataSourceUnavailable(
                source=self.name,
                reason=f"{venue} returned no market label for {market} on {chain}; cannot bind the page to the request",
            )
        if not _market_labels_agree(served_market, market):
            raise DataSourceUnavailable(
                source=self.name,
                reason=(
                    f"{venue} answered for market {served_market!r} on a request for {market!r} "
                    f"({chain}); refusing a page that is not about the requested market"
                ),
            )
        # The check that actually binds the page to the request. A market label
        # and a pair of token addresses can be mutually consistent on a page
        # about a different asset; the index symbol is what the candles are OF.
        # It also covers the two cases a label comparison cannot: an address
        # request, which the venue answers with a name, and a strategy config
        # whose `base_token` names a different asset than its `market` (that
        # mis-binding is how a request for XRP could be served ETH candles and
        # labelled venue-native with confidence 1.0).
        if not _index_symbols_agree(str(response.index_symbol or ""), base):
            raise DataSourceUnavailable(
                source=self.name,
                reason=(
                    f"{venue} served index {response.index_symbol!r} for a {base!r} request "
                    f"on {market} ({chain}); refusing candles for a different asset"
                ),
            )
        if response.timeframe and response.timeframe != timeframe.value:
            raise DataSourceUnavailable(
                source=self.name,
                reason=(
                    f"{venue} served timeframe {response.timeframe!r} for a {timeframe.value!r} "
                    f"request on {market}; refusing to treat it as the requested plane"
                ),
            )
        return response

    def _decode_candles(self, response: Any, timeframe: OHLCVTimeframe) -> list[OHLCVCandle]:
        """Decode a page into ascending router-shaped candles.

        ``volume`` stays ``None``: a perp index/mark plane is not a trade tape,
        and it has no execution volume to report. ``None`` means *unmeasured*
        and must never be substituted with ``0`` — a zero would read as
        "measured, and there was none", which is a different and false claim
        (``Empty != Zero``).
        """
        candles: list[OHLCVCandle] = []
        for raw in response.candles:
            try:
                values = tuple(Decimal(value) for value in (raw.open, raw.high, raw.low, raw.close))
            except (InvalidOperation, ArithmeticError, ValueError) as exc:
                raise DataSourceUnavailable(
                    source=self.name,
                    reason=f"{self._policy.venue} returned a malformed candle value",
                ) from exc
            if raw.timestamp <= 0 or any(not value.is_finite() or value <= 0 for value in values):
                raise DataSourceUnavailable(
                    source=self.name,
                    reason=f"{self._policy.venue} returned an invalid candle observation",
                )
            candles.append(
                OHLCVCandle(
                    # Candle OPEN time, matching every other router provider and
                    # the staleness guard's start-time contract. (The backtest
                    # consumer shifts to close time instead, because a replay
                    # must not see a candle before it closed; a live indicator
                    # reading the forming candle is the normal contract here.)
                    timestamp=datetime.fromtimestamp(raw.timestamp, tz=UTC),
                    open=values[0],
                    high=values[1],
                    low=values[2],
                    close=values[3],
                    volume=None,
                )
            )

        if not candles:
            raise DataSourceUnavailable(
                source=self.name,
                reason=f"{self._policy.venue} returned no {timeframe.value} candles for {response.market}",
            )
        # The router treats the list as ascending (its disk cache slices
        # ``[-limit:]`` and its staleness guard reads the youngest from the
        # tail); the venue pages newest-first.
        candles.sort(key=lambda candle: candle.timestamp)
        return candles
