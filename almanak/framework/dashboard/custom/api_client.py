"""API client for custom dashboards.

This client is passed to user-written dashboard code (ui.py files).
It provides a controlled interface to strategy data - all access goes
through the gateway.

SECURITY: This client is the ONLY way custom dashboards can access data.
Custom dashboards cannot import the gateway client directly because:
1. In production, the dashboard container has no direct gateway access
2. The api_client is injected by the framework with proper auth
"""

import logging
import math
from datetime import datetime
from decimal import Decimal
from typing import Any

from ._token_decimals import token_decimals as _token_decimals

logger = logging.getLogger(__name__)


class _Sentinel:
    """Marker type for "value never resolved" distinct from ``None``."""


_UNSET: _Sentinel = _Sentinel()


def _liquidity_depth_to_rows(depth: Any) -> list[dict[str, Any]]:
    ticks = sorted(getattr(depth, "ticks", []) or [], key=lambda tick: tick.tick_index)
    current_tick = int(getattr(depth, "current_tick", 0) or 0)
    current_liquidity = int(getattr(depth, "total_liquidity", 0) or 0)
    tick_spacing = int(getattr(depth, "tick_spacing", 0) or 0)
    token0_decimals = int(getattr(depth, "token0_decimals", 18) or 18)
    token1_decimals = int(getattr(depth, "token1_decimals", 6) or 6)
    active_tick = (current_tick // tick_spacing) * tick_spacing if tick_spacing else current_tick
    liquidity_net_by_tick = {int(tick.tick_index): int(tick.liquidity_net) for tick in ticks}

    def _price_at_tick(tick_index: int) -> Decimal:
        return Decimal(str(math.pow(1.0001, tick_index))) * (Decimal(10) ** (token0_decimals - token1_decimals))

    def _row(tick_index: int, active_liquidity: int) -> dict[str, Any]:
        price0 = float(_price_at_tick(tick_index))
        return {
            "tick_idx": tick_index,
            "liquidity_active": max(active_liquidity, 0),
            "price0": price0,
            "price1": 1 / price0 if price0 else 0,
            "current_tick": current_tick,
        }

    if not tick_spacing:
        if current_liquidity:
            return [_row(current_tick, current_liquidity)]
        return []

    if not ticks:
        return [_row(active_tick, current_liquidity)] if current_liquidity else []

    min_tick = min(min(liquidity_net_by_tick), active_tick - (200 * tick_spacing))
    max_tick = max(max(liquidity_net_by_tick), active_tick + (200 * tick_spacing))
    min_tick = (min_tick // tick_spacing) * tick_spacing
    max_tick = (max_tick // tick_spacing) * tick_spacing

    rows_by_tick: dict[int, dict[str, Any]] = {active_tick: _row(active_tick, current_liquidity)}

    active = current_liquidity
    for tick_idx in range(active_tick + tick_spacing, max_tick + tick_spacing, tick_spacing):
        active += liquidity_net_by_tick.get(tick_idx, 0)
        rows_by_tick[tick_idx] = _row(tick_idx, active)

    # Walk downward. Row at ``tick_idx`` represents active liquidity in
    # range ``[tick_idx, tick_idx + tick_spacing)``. To move from the
    # current range into the next lower one, we cross the *upper* boundary
    # of the lower range going down, which subtracts ``liquidity_net`` at
    # that upper boundary tick — not at ``tick_idx`` itself.
    active = current_liquidity
    for tick_idx in range(active_tick - tick_spacing, min_tick - tick_spacing, -tick_spacing):
        upper_boundary = tick_idx + tick_spacing
        active -= liquidity_net_by_tick.get(upper_boundary, 0)
        rows_by_tick[tick_idx] = _row(tick_idx, active)

    return [rows_by_tick[tick] for tick in sorted(rows_by_tick)]


class DashboardAPIClient:
    """API client for custom dashboards.

    This is the interface provided to user-written dashboard code.
    All methods are read-only except for operator actions.

    Example usage in custom dashboard (ui.py):
        def render_custom_dashboard(
            deployment_id: str,
            strategy_config: dict,
            api_client: DashboardAPIClient,  # This client
            session_state: dict,
        ) -> None:
            # Get timeline events
            events = api_client.get_timeline(limit=10)

            # Get current state
            state = api_client.get_state()

            # Get price data
            eth_price = api_client.get_price("ETH", "USD")
    """

    def __init__(self, gateway_client: Any, deployment_id: str):
        """Initialize the API client.

        Args:
            gateway_client: The underlying GatewayDashboardClient
            deployment_id: The strategy this dashboard is for (for scoping)
        """
        self._client = gateway_client
        self._deployment_id = deployment_id
        # Cache of the strategy's configured chain, resolved lazily on first
        # chain-omitted price/balance call. Config is immutable for a given
        # strategy session, so there is no need to re-fetch on every chart
        # tick. Sentinel ``_UNSET`` distinguishes "never resolved" from
        # "resolved and empty".
        self._chain_cache: str | None | _Sentinel = _UNSET

    @property
    def deployment_id(self) -> str:
        """Get the deployment ID this client is scoped to."""
        return self._deployment_id

    # =========================================================================
    # Strategy Data (scoped to current strategy)
    # =========================================================================

    def get_state(self, fields: list[str] | None = None) -> dict[str, Any]:
        """Get current strategy state.

        Args:
            fields: Optional list of specific fields to return.
                   If None, returns full state.

        Returns:
            Strategy state as dictionary.
        """
        try:
            return self._client.get_strategy_state(self._deployment_id, fields)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to get strategy state: {e}")
            return {}

    def get_timeline(
        self,
        limit: int = 50,
        event_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get timeline events for this strategy.

        Args:
            limit: Maximum number of events to return
            event_type: Optional filter by event type

        Returns:
            List of timeline events as dictionaries.
        """
        try:
            events = self._client.get_timeline(
                self._deployment_id,
                limit=limit,
                event_type_filter=event_type,
            )
            return [self._event_to_dict(e) for e in events]
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to get timeline: {e}")
            return []

    def get_config(self) -> dict[str, Any]:
        """Get strategy configuration.

        Returns:
            Strategy configuration as dictionary.
        """
        try:
            return self._client.get_strategy_config(self._deployment_id)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to get strategy config: {e}")
            return {}

    def get_position(self) -> dict[str, Any]:
        """Get current position summary.

        Returns:
            Position data including balances, LP positions, etc.
        """
        try:
            details = self._client.get_strategy_details(self._deployment_id)
            return self._position_to_dict(details.position)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to get position: {e}")
            return {}

    def get_summary(self) -> dict[str, Any]:
        """Get strategy summary.

        Returns:
            Summary data including status, value, PnL, etc.
        """
        try:
            details = self._client.get_strategy_details(self._deployment_id)
            return self._summary_to_dict(details.summary)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to get summary: {e}")
            return {}

    def get_trade_tape(self, limit: int = 50, from_ts: datetime | None = None) -> Any:
        """Get the joined trade-tape view for this strategy.

        This is intentionally a thin scoped facade over
        ``GatewayDashboardClient.get_trade_tape``. Template dashboards consume
        the typed response directly because it preserves Decimal precision and
        timestamp types for chart markers.

        Args:
            limit: Maximum number of rows to return.
            from_ts: Optional window lower bound (VIB-5059 P2 / VIB-5114). When
                set, only trades at or after this timestamp are returned, so the
                chart's buy/sell markers follow the same selected range as the
                price candles and never float outside the plotted window. ``None``
                (the default) preserves today's newest-N behaviour byte-for-byte.

        Returns:
            ``TradeTapeResponse`` from the gateway client, or an empty response
            shape on failure.
        """
        try:
            return self._client.get_trade_tape(self._deployment_id, limit=limit, from_ts=from_ts)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to get trade tape: {e}")
            from almanak.framework.dashboard.gateway_client import TradeTapeResponse

            return TradeTapeResponse(rows=[], has_more=False)

    # =========================================================================
    # Market Data (via gateway)
    # =========================================================================

    def get_price(self, token: str, quote: str = "USD", chain: str | None = None) -> float | None:
        """Get current token price.

        Args:
            token: Token symbol (e.g., "ETH", "BTC") or contract address.
            quote: Quote currency (default "USD")
            chain: Chain name (e.g., "arbitrum", "base"). When omitted, falls
                back to the strategy config's ``default_chain``/``chain`` so
                the request carries the same chain context the strategy runs
                on. This is REQUIRED for address-based lookups on multi-chain
                gateways (VIB-3259) — without it the gateway rejects the
                request with gRPC ``INVALID_ARGUMENT``.

        Returns:
            Price as float, or None if unavailable.
        """
        try:
            # Access the underlying gateway client's market service
            from almanak.gateway.proto import gateway_pb2

            # Fall back to strategy config for chain context so dashboards
            # written against the previous 2-arg signature still work.
            # Strategy config chain is immutable for the session — cache it
            # on the instance so dashboards calling get_price on every chart
            # tick don't pay an extra gRPC round-trip each time.
            resolved_chain = chain
            if resolved_chain is None:
                if isinstance(self._chain_cache, _Sentinel):
                    try:
                        config = self.get_config()
                        self._chain_cache = config.get("default_chain") or config.get("chain") or None
                    except Exception as e:  # noqa: BLE001
                        logger.debug(f"Could not read chain from config: {e}")
                        self._chain_cache = None
                resolved_chain = self._chain_cache

            request = gateway_pb2.PriceRequest(
                token=token,
                quote=quote,
                chain=resolved_chain or "",
            )
            response = self._client._client.market.GetPrice(request)
            return float(response.price) if response.price else None
        except Exception as e:  # noqa: BLE001
            logger.debug(f"Failed to get price for {token}/{quote}: {e}")
            return None

    def get_pt_price(
        self,
        symbol: str,
        chain: str | None = None,
        quote: str = "USD",
        maturity_ts: int = 0,
    ) -> dict[str, Any]:
        """Get the composed Pendle PT/USD price via the gateway price authority.

        Typed accessor over the ``MarketService.GetPtPrice`` RPC (VIB-5309/5310)
        so a custom dashboard never re-derives a PT mark from a raw
        ``get_price`` ratio. PT/USD is composed gateway-side as
        ``pt_to_asset_rate × underlying/USD`` and stamped with confidence +
        staleness + maturity (design spine §1).

        **Empty != Zero**: ``price`` is ``None`` (never ``0.0``) unless
        ``availability == AVAILABLE``. A PT that is genuinely unpriceable
        (``UNMEASURED``), an old gateway (``UNSPECIFIED``), or a read that raised
        (``ERRORED``) all yield ``price=None`` — the dashboard shows an explicit
        "unmeasured" cell, never a fabricated number. Gate on ``availability``,
        not on string emptiness.

        Args:
            symbol: Canonical PT symbol — the identity / join / FIFO-match key
                (case-insensitive at the gateway).
            chain: Chain name. When omitted, falls back to the strategy config's
                ``default_chain`` / ``chain`` (mirrors :meth:`get_price`).
            quote: Quote currency (default ``USD``).
            maturity_ts: Optional maturity hint as a Unix timestamp in seconds;
                ``0`` lets the gateway resolve the active maturity.

        Returns:
            A dict with: ``symbol``, ``chain``, ``quote``, ``price``
            (``float | None`` — ``None`` when unmeasured), ``availability``
            (``"AVAILABLE"`` / ``"UNMEASURED"`` / ``"ERRORED"`` / ``"UNSPECIFIED"``),
            ``confidence`` (raw ``float`` 0..1), ``confidence_band``
            (``"HIGH"`` / ``"ESTIMATED"`` / ``"UNAVAILABLE"`` / ``"UNSPECIFIED"``),
            ``underlying_price`` (``float | None``), ``pt_to_asset_rate``
            (``float | None``), ``source``, ``stale`` (``bool``), ``maturity_ts``
            (``int``), ``days_to_maturity`` (``int``). Composition legs are
            ``None`` when the gateway left them blank (Empty != Zero). Returns the
            unmeasured shape (``price=None``, ``availability="UNSPECIFIED"``) on
            any RPC failure — never raises, never fabricates a number.
        """
        from almanak.gateway.proto import gateway_pb2

        resolved_chain = chain
        if resolved_chain is None:
            if isinstance(self._chain_cache, _Sentinel):
                try:
                    config = self.get_config()
                    self._chain_cache = config.get("default_chain") or config.get("chain") or None
                except Exception as e:  # noqa: BLE001
                    logger.debug(f"Could not read chain from config: {e}")
                    self._chain_cache = None
            resolved_chain = self._chain_cache

        # Empty != Zero unmeasured shape, returned on any failure path so the
        # dashboard renders an explicit "unmeasured" cell rather than crashing.
        unmeasured: dict[str, Any] = {
            "symbol": symbol,
            "chain": resolved_chain or "",
            "quote": quote,
            "price": None,
            "availability": "UNSPECIFIED",
            "confidence": 0.0,
            "confidence_band": "UNSPECIFIED",
            "underlying_price": None,
            "pt_to_asset_rate": None,
            "source": "",
            "stale": False,
            "maturity_ts": 0,
            "days_to_maturity": 0,
        }
        try:
            request = gateway_pb2.PtPriceRequest(
                symbol=symbol,
                chain=resolved_chain or "",
                quote=quote,
                maturity_ts=maturity_ts,
            )
            response = self._client._client.market.GetPtPrice(request)
        except Exception as e:  # noqa: BLE001
            logger.debug(f"Failed to get PT price for {symbol}/{quote}: {e}")
            return unmeasured

        # A gateway newer than this client can return an enum int we don't know.
        # ``EnumTypeWrapper.Name`` raises ValueError on an unknown value; fall back
        # to UNSPECIFIED rather than letting it escape (which would turn a
        # version-skew response into an uncaught exception instead of the
        # unmeasured shape). ``is_available`` below is an int-constant comparison
        # that never raises, so an unknown availability correctly yields price=None.
        try:
            availability = gateway_pb2.PtPriceAvailability.Name(response.availability)
        except ValueError:
            availability = "PT_PRICE_AVAILABILITY_UNSPECIFIED"
        try:
            band = gateway_pb2.PtPriceConfidenceBand.Name(response.confidence_band)
        except ValueError:
            band = "PT_PRICE_CONFIDENCE_BAND_UNSPECIFIED"
        # Strip the verbose proto-enum prefixes to a terse band the renderer can
        # show directly (AVAILABLE / UNMEASURED / ERRORED / HIGH / ESTIMATED / ...).
        availability_label = availability.removeprefix("PT_PRICE_AVAILABILITY_")
        band_label = band.removeprefix("PT_PRICE_CONFIDENCE_BAND_")

        is_available = response.availability == gateway_pb2.PT_PRICE_AVAILABILITY_AVAILABLE
        # Empty != Zero: only an AVAILABLE response carries a trustable price.
        # ``response.price`` is the proto-default empty string when unmeasured —
        # never coerce it to 0.
        price = float(response.price) if (is_available and response.price) else None

        return {
            "symbol": response.symbol or symbol,
            "chain": response.chain or (resolved_chain or ""),
            "quote": response.quote or quote,
            "price": price,
            "availability": availability_label,
            "confidence": float(response.confidence),
            "confidence_band": band_label,
            "underlying_price": float(response.underlying_price) if response.underlying_price else None,
            "pt_to_asset_rate": float(response.pt_to_asset_rate) if response.pt_to_asset_rate else None,
            "source": response.source or "",
            "stale": bool(response.stale),
            "maturity_ts": int(response.maturity_ts),
            "days_to_maturity": int(response.days_to_maturity),
        }

    def get_balance(self, token: str, chain: str | None = None) -> float | None:
        """Get token balance for strategy wallet.

        Args:
            token: Token symbol
            chain: Chain name (uses strategy's chain if not specified)

        Returns:
            Balance as float, or None if unavailable.
        """
        try:
            # Get wallet address from strategy config
            config = self.get_config()
            wallet = config.get("wallet_address", "")
            chain = chain or config.get("default_chain") or config.get("chain")
            if not chain:
                logger.debug("No chain specified and none found in config")
                return None

            from almanak.gateway.proto import gateway_pb2

            response = self._client._client.market.GetBalance(
                gateway_pb2.BalanceRequest(
                    token=token,
                    chain=chain,
                    wallet_address=wallet,
                )
            )
            return float(response.balance) if response.balance else None
        except Exception as e:  # noqa: BLE001
            logger.debug(f"Failed to get balance for {token}: {e}")
            return None

    def get_indicator(
        self,
        indicator_type: str,
        token: str,
        quote: str = "USD",
        params: dict[str, str] | None = None,
    ) -> float | None:
        """Get technical indicator value.

        Args:
            indicator_type: Indicator type (e.g., "RSI", "SMA")
            token: Token symbol
            quote: Quote currency
            params: Indicator parameters (e.g., {"period": "14"})

        Returns:
            Indicator value as float, or None if unavailable.
        """
        try:
            from almanak.gateway.proto import gateway_pb2

            response = self._client._client.market.GetIndicator(
                gateway_pb2.IndicatorRequest(
                    indicator_type=indicator_type,
                    token=token,
                    quote=quote,
                    params=params or {},
                )
            )
            return float(response.value) if response.value else None
        except Exception as e:  # noqa: BLE001
            logger.debug(f"Failed to get indicator {indicator_type}: {e}")
            return None

    def get_ohlcv(
        self,
        token: str,
        quote: str = "USD",
        timeframe: str = "1h",
        limit: int = 168,
        chain: str | None = None,
        pool_address: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch OHLCV candles via the shared OHLCV stack (VIB-4347).

        Routes through ``framework.data.ohlcv.create_ohlcv_stack`` — the same
        factory that wires the live runner's ``MarketSnapshot.ohlcv()`` and the
        indicator path's ``RoutingOHLCVProvider``. **Never** calls
        ``gateway_pb2.GeckoTerminalGetOHLCV`` directly: doing so would bypass
        the provider routing, CEX/DEX classification, disk cache, retry / typed
        errors, and provenance metadata that the router applies. See
        ``docs/internal/OHLCV-Data.md`` §2 for the full rationale.

        Args:
            token: Token symbol (e.g., ``"WETH"``). For DEX pool lookups, pass
                ``token0`` symbol — the gateway-side provider keys off
                ``pool_address`` when present.
            quote: Quote currency (default ``"USD"``).
            timeframe: Candle interval. One of ``1m``, ``5m``, ``15m``, ``1h``,
                ``4h``, ``1d``.
            limit: Number of candles to fetch. Default 168 (1 week at 1h).
            chain: Chain name. When omitted, falls back to the strategy
                config's ``default_chain`` / ``chain``. Mirrors
                :meth:`get_price` resolution semantics.
            pool_address: Optional pool address for DEX-pool lookups. Mandatory
                for DEX-only tokens; ignored for CEX-listed tokens (Binance is
                symbol-only).

        Returns:
            List of dicts with keys ``timestamp`` (ISO 8601), ``open``,
            ``high``, ``low``, ``close``, ``volume`` (all as strings to
            preserve full ``Decimal`` precision), plus the envelope's
            provenance fields when available: ``source`` (which provider
            answered), ``confidence`` (0.0 – 1.0), and ``cache_hit``.
            Returns ``[]`` on any failure — does **not** raise, does **not**
            substitute synthetic data.
        """
        try:
            from almanak.framework.data.ohlcv import create_ohlcv_stack

            resolved_chain = chain
            if resolved_chain is None:
                if isinstance(self._chain_cache, _Sentinel):
                    try:
                        config = self.get_config()
                        self._chain_cache = config.get("default_chain") or config.get("chain") or None
                    except Exception as e:  # noqa: BLE001
                        logger.debug(f"Could not read chain from config: {e}")
                        self._chain_cache = None
                resolved_chain = self._chain_cache

            if not resolved_chain:
                logger.debug("get_ohlcv: no chain specified and none found in config")
                return []

            stack = create_ohlcv_stack(
                gateway_client=self._client._client,
                chain=resolved_chain,
                pool_address=pool_address,
            )
            # ``RoutingOHLCVProvider.get_ohlcv`` is async; the dashboard
            # Streamlit context is synchronous. Use the underlying sync
            # ``OHLCVRouter`` directly so we don't pay the asyncio.to_thread
            # round-trip per Streamlit re-render. The router returns a
            # ``DataEnvelope`` so we can lift provenance into the output.
            envelope = stack.router.get_ohlcv(
                token,
                chain=resolved_chain,
                timeframe=timeframe,
                limit=limit,
                pool_address=pool_address,
                quote=quote,
            )
            candles = envelope.value or []
            meta = getattr(envelope, "meta", None)
            source = getattr(meta, "source", None)
            confidence = getattr(meta, "confidence", None)
            cache_hit = getattr(meta, "cache_hit", None)

            results: list[dict[str, Any]] = []
            for candle in candles:
                row: dict[str, Any] = {
                    "timestamp": candle.timestamp.isoformat()
                    if hasattr(candle.timestamp, "isoformat")
                    else str(candle.timestamp),
                    "open": str(candle.open),
                    "high": str(candle.high),
                    "low": str(candle.low),
                    "close": str(candle.close),
                    "volume": str(candle.volume) if candle.volume is not None else None,
                }
                # Stamp provenance only when present — never invent it. Dropping
                # provenance at the dashboard boundary would re-create part of
                # the hardcoded-provider problem this factory exists to prevent.
                if source is not None:
                    row["source"] = source
                if confidence is not None:
                    row["confidence"] = float(confidence)
                if cache_hit is not None:
                    row["cache_hit"] = bool(cache_hit)
                results.append(row)
            return results
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to fetch OHLCV for {token} on chain={chain}: {e}")
            return []

    def get_v3_pool_address(
        self,
        *,
        chain: str,
        protocol: str,
        token0_address: str,
        token1_address: str,
        fee_tier: int,
    ) -> str | None:
        """Resolve a V3-compatible pool address through the gateway."""
        try:
            from almanak.connectors._strategy_base.pool_validation_registry import PoolValidationRegistry

            gateway = self._client._client
            result = PoolValidationRegistry.validate(
                protocol,
                chain,
                token0_address,
                token1_address,
                {"fee_tier": fee_tier},
                rpc_url=None,
                gateway_client=gateway,
            )
            return result.pool_address if result.exists else None
        except Exception as e:  # noqa: BLE001
            logger.debug(f"Failed to resolve {protocol} pool address: {e}")
            return None

    def get_liquidity_distribution(
        self,
        *,
        pool_address: str,
        chain: str,
        fee_tier: int | None = None,
        token0: str = "WETH",
        token1: str = "USDC",
        tick_range_multiplier: int = 200,
    ) -> list[dict[str, Any]]:
        """Read concentrated-liquidity depth through gateway-routed eth_call."""
        try:
            from almanak.framework.data.pools.liquidity import LiquidityDepthReader

            gateway = self._client._client

            def _rpc_call(chain_name: str, to: str, calldata: str) -> bytes:
                raw = gateway.eth_call(chain=chain_name, to=to, data=calldata)
                if not raw or raw == "0x":
                    return b""
                return bytes.fromhex(raw.removeprefix("0x"))

            decimals0 = _token_decimals(token0)
            decimals1 = _token_decimals(token1)
            reader = LiquidityDepthReader(
                rpc_call=_rpc_call,
                tick_range_multiplier=tick_range_multiplier,
                source_name="gateway_rpc",
            )
            envelope = reader.read_liquidity_depth(
                pool_address=pool_address,
                chain=chain,
                token0_decimals=decimals0,
                token1_decimals=decimals1,
                fee_tier=fee_tier,
            )
            return _liquidity_depth_to_rows(envelope.value)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to fetch liquidity distribution for {pool_address}: {e}")
            return []

    def get_position_events(
        self,
        position_types: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch filtered position events for this strategy's deployment (VIB-4347).

        Backed by ``StateService.GetPositionEventsFiltered``. Returns a flat
        chronological list of position events scoped to this strategy's
        deployment. ``plot_positions_over_time`` consumes the per-position
        rollup produced by
        :func:`framework.dashboard.custom.position_event_adapter.position_events_to_position_data_dicts`,
        so dashboards that want the chart shape should call that adapter on
        the result.

        Args:
            position_types: Optional filter by ``position_type`` (e.g.
                ``["LP", "PERP"]``). Maps to the proto request's
                ``position_types`` (a repeated field).

                - ``None`` (default — no filter): expands to every known
                  :class:`PositionType` value, because the gateway treats
                  the empty list as the empty-set fast path
                  (``state_service.py`` §GetPositionEventsFiltered) and
                  the docstring contract says "no filter = all".
                - ``[]`` (explicit empty filter): passed through verbatim;
                  the gateway returns no rows. Use this when the caller
                  has computed a filter that turned out empty (e.g.
                  "no allowed types for this user") and wants the
                  zero-row result rather than the all-rows result —
                  conflating the two would silently broaden the answer
                  (CodeRabbit major on PR #2270).

        Returns:
            List of dicts (one per position event), shape per
            :func:`framework.dashboard.custom.position_event_adapter.position_event_to_dict`.
            Returns ``[]`` on any failure — does not raise.
        """
        try:
            from almanak.framework.observability.position_events import PositionType
            from almanak.gateway.proto import gateway_pb2

            from .position_event_adapter import position_event_to_dict

            # ``is None`` (not falsiness) so an explicit ``[]`` is honoured
            # as "empty filter → zero rows" and only ``None`` expands to
            # the full PositionType universe. Forwarded findings: CodeRabbit
            # major on PR #2270.
            if position_types is None:
                effective_types: list[str] = [pt.value for pt in PositionType]
            else:
                effective_types = list(position_types)
            response = self._client._client.state.GetPositionEventsFiltered(
                gateway_pb2.GetPositionEventsFilteredRequest(
                    deployment_id=self._deployment_id,
                    position_types=effective_types,
                )
            )
            if response.error:
                logger.warning(f"GetPositionEventsFiltered returned error: {response.error}")
                return []
            return [position_event_to_dict(e) for e in response.events]
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to fetch position events: {e}")
            return []

    def get_position_history(
        self,
        position_id: str,
    ) -> list[dict[str, Any]]:
        """Fetch the full lifecycle of a single position (VIB-4347).

        Backed by ``StateService.GetPositionHistory``. Returns chronological
        events (OPEN -> SNAPSHOT* -> CLOSE) for one position scoped to this
        strategy's deployment. Use this for drill-down detail views; use
        :meth:`get_position_events` for multi-position chart data.

        Args:
            position_id: The position UUID/identifier to retrieve history for.

        Returns:
            List of dicts (one per position event), shape per
            :func:`framework.dashboard.custom.position_event_adapter.position_event_to_dict`.
            Returns ``[]`` on any failure or missing arguments — does not raise.
        """
        if not position_id:
            logger.debug("get_position_history: position_id is required")
            return []
        try:
            from almanak.gateway.proto import gateway_pb2

            from .position_event_adapter import position_event_to_dict

            response = self._client._client.state.GetPositionHistory(
                gateway_pb2.GetPositionHistoryRequest(
                    deployment_id=self._deployment_id,
                    position_id=position_id,
                )
            )
            return [position_event_to_dict(e) for e in response.events]
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to fetch position history for {position_id}: {e}")
            return []

    # =========================================================================
    # Operator Actions (with audit)
    # =========================================================================

    def pause_strategy(self, reason: str) -> bool:
        """Pause the strategy.

        Args:
            reason: Reason for pausing (required for audit)

        Returns:
            True if successful, False otherwise.
        """
        if not reason:
            logger.warning("Cannot pause strategy: reason is required")
            return False

        try:
            return self._client.execute_action(
                self._deployment_id,
                action="PAUSE",
                reason=reason,
            )
        except Exception:
            logger.exception("Failed to pause strategy")
            return False

    def resume_strategy(self, reason: str = "Resumed from dashboard") -> bool:
        """Resume the strategy.

        Args:
            reason: Reason for resuming (optional, defaults to generic message)

        Returns:
            True if successful, False otherwise.
        """
        try:
            return self._client.execute_action(
                self._deployment_id,
                action="RESUME",
                reason=reason,
            )
        except Exception:
            logger.exception("Failed to resume strategy")
            return False

    # =========================================================================
    # Helper methods
    # =========================================================================

    def _event_to_dict(self, event: Any) -> dict[str, Any]:
        """Convert timeline event to dictionary."""
        return {
            "timestamp": event.timestamp.isoformat() if hasattr(event, "timestamp") and event.timestamp else None,
            "event_type": event.event_type if hasattr(event, "event_type") else str(type(event).__name__),
            "description": event.description if hasattr(event, "description") else "",
            "tx_hash": event.tx_hash if hasattr(event, "tx_hash") else None,
            "chain": event.chain if hasattr(event, "chain") else None,
            "details": event.details if hasattr(event, "details") and isinstance(event.details, dict) else {},
        }

    def _position_to_dict(self, position: Any) -> dict[str, Any]:
        """Convert position to dictionary."""
        if position is None:
            return {}

        result: dict[str, Any] = {
            "token_balances": [],
            "lp_positions": [],
            "total_lp_value_usd": "0",
            "health_factor": None,
            "leverage": None,
        }

        if hasattr(position, "token_balances") and position.token_balances:
            result["token_balances"] = [
                {
                    "symbol": b.symbol,
                    "balance": str(b.balance),
                    "value_usd": str(b.value_usd),
                }
                for b in position.token_balances
            ]

        if hasattr(position, "lp_positions") and position.lp_positions:
            result["lp_positions"] = [
                {
                    "pool": p.pool,
                    "token0": p.token0,
                    "token1": p.token1,
                    "liquidity_usd": str(p.liquidity_usd),
                    "in_range": p.in_range,
                }
                for p in position.lp_positions
            ]

        if hasattr(position, "total_lp_value_usd") and position.total_lp_value_usd:
            result["total_lp_value_usd"] = str(position.total_lp_value_usd)

        if hasattr(position, "health_factor") and position.health_factor is not None:
            result["health_factor"] = str(position.health_factor)

        if hasattr(position, "leverage") and position.leverage is not None:
            result["leverage"] = str(position.leverage)

        # Strategy-reported / valuer-synthesized positions (VIB-5317). Previously
        # dropped here, which made FIFO-derived held-PT inventory invisible to
        # custom dashboards even though the valuer stamps the PT display fields
        # (qty, days-to-maturity, pt_to_asset_rate, price confidence) into the
        # proto ``details`` map and the gateway surfaces them as
        # ``strategy_positions``. Surface them so a custom
        # ``render_custom_dashboard`` can render its own Open-Positions /
        # PT-inventory table, mirroring the generic operator detail page
        # (``pages/detail.py``). ``pt_inventory`` is the pre-filtered convenience
        # view of the held-PT rows.
        if hasattr(position, "strategy_positions") and position.strategy_positions:
            result["strategy_positions"] = [self._strategy_position_to_dict(sp) for sp in position.strategy_positions]
            result["pt_inventory"] = [d for d in result["strategy_positions"] if self._is_pt_inventory_row(d)]

        return result

    @staticmethod
    def _is_pt_inventory_row(sp_dict: dict[str, Any]) -> bool:
        """Detect a FIFO-derived held-PT inventory row by its data-shape marker.

        Keyed on ``details["source"] == "pt_inventory_lots"`` (the valuer's
        ``_PT_INVENTORY_SOURCE``) or ``protocol == "pt"`` — never on a
        connector-name string, so a future protocol rename never silently drops
        the row (VIB-4636 discipline, mirrors ``data_source._extract_pt_inventory``).
        """
        details = sp_dict.get("details") or {}
        return details.get("source") == "pt_inventory_lots" or sp_dict.get("protocol") == "pt"

    @staticmethod
    def _strategy_position_to_dict(sp: Any) -> dict[str, Any]:
        """Convert a ``StrategyPosition`` proto to a dict (VIB-5317).

        Empty != Zero: ``value_usd`` / ``unrealized_pnl_usd`` are passed through
        verbatim (no ``or "0"`` coercion). An unmeasured PT mark is flagged in
        ``details`` (``mark_unmeasured == "true"``) and the gateway leaves the USD
        strings at the proto-default empty string — the renderer keys "—" on the
        flag, NOT on a zero/empty value, so a measured zero is never confused with
        unmeasured. We preserve the raw proto strings so that distinction survives
        to the custom dashboard.
        """
        raw_details = getattr(sp, "details", None) or {}
        details = {str(k): str(v) for k, v in raw_details.items()}
        # Empty != Zero: the gateway-client dataclass carries ``value_usd`` /
        # ``unrealized_pnl_usd`` as ``Decimal | None``. A measured ``Decimal("0")``
        # is falsy, so ``... or ""`` would collapse a real $0 into the unmeasured
        # sentinel — exactly the conflation this PR exists to prevent. Map only the
        # genuinely-unmeasured sentinels (``None`` for the dataclass, ``""`` for a
        # raw proto) to "", and stringify every measured value (including "0").
        value_usd = getattr(sp, "value_usd", None)
        unrealized_pnl_usd = getattr(sp, "unrealized_pnl_usd", None)
        return {
            "position_type": getattr(sp, "position_type", "") or "",
            "position_id": getattr(sp, "position_id", "") or "",
            "chain": getattr(sp, "chain", "") or "",
            "protocol": getattr(sp, "protocol", "") or "",
            "value_usd": "" if value_usd is None or value_usd == "" else str(value_usd),
            "unrealized_pnl_usd": ""
            if unrealized_pnl_usd is None or unrealized_pnl_usd == ""
            else str(unrealized_pnl_usd),
            "details": details,
        }

    def _summary_to_dict(self, summary: Any) -> dict[str, Any]:
        """Convert summary to dictionary."""
        if summary is None:
            return {}

        return {
            "deployment_id": summary.deployment_id if hasattr(summary, "deployment_id") else self._deployment_id,
            "name": summary.name if hasattr(summary, "name") else "",
            "status": summary.status if hasattr(summary, "status") else "UNKNOWN",
            "chain": summary.chain if hasattr(summary, "chain") else "",
            "protocol": summary.protocol if hasattr(summary, "protocol") else "",
            "total_value_usd": str(summary.total_value_usd) if hasattr(summary, "total_value_usd") else "0",
            "pnl_24h_usd": str(summary.pnl_24h_usd) if hasattr(summary, "pnl_24h_usd") else "0",
            "attention_required": summary.attention_required if hasattr(summary, "attention_required") else False,
            "attention_reason": summary.attention_reason if hasattr(summary, "attention_reason") else "",
        }


def create_api_client(gateway_client: Any, deployment_id: str) -> DashboardAPIClient:
    """Create API client for custom dashboard.

    This is the factory function used by the renderer to create
    a gateway-backed API client for custom dashboards.

    Args:
        gateway_client: The GatewayDashboardClient instance
        deployment_id: Strategy this dashboard is for

    Returns:
        DashboardAPIClient for use in custom dashboard
    """
    return DashboardAPIClient(gateway_client, deployment_id)
