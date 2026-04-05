"""IntegrationService implementation - third-party data sources.

This service exposes platform integrations (Binance, CoinGecko, TheGraph, Zerion)
to strategy containers via gRPC. All API keys and rate limiting are handled
in the gateway.
"""

import json
import logging
import time
from datetime import UTC, datetime

import grpc

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.integrations.base import BaseIntegration
from almanak.gateway.integrations.binance import BinanceIntegration
from almanak.gateway.integrations.coingecko import CoinGeckoIntegration
from almanak.gateway.integrations.models import WalletPortfolioSnapshot
from almanak.gateway.integrations.portfolio_chain import PortfolioProviderChain, build_portfolio_chain
from almanak.gateway.integrations.thegraph import TheGraphIntegration
from almanak.gateway.integrations.zerion import ZerionIntegration
from almanak.gateway.metrics import record_integration_latency, record_integration_request
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.validation import (
    ValidationError,
    validate_address_for_chain,
    validate_chain,
    validate_graphql_query,
    validate_symbol,
    validate_token_id,
)

logger = logging.getLogger(__name__)


class IntegrationServiceServicer(gateway_pb2_grpc.IntegrationServiceServicer):
    """Implements IntegrationService gRPC interface.

    Provides access to third-party data sources:
    - Binance: Ticker, klines, order book
    - CoinGecko: Prices, markets
    - TheGraph: Subgraph queries
    """

    def __init__(self, settings: GatewaySettings):
        """Initialize IntegrationService.

        Args:
            settings: Gateway settings with API keys
        """
        self.settings = settings
        self._binance: BinanceIntegration | None = None
        self._coingecko: CoinGeckoIntegration | None = None
        self._thegraph: TheGraphIntegration | None = None
        self._zerion: ZerionIntegration | None = None
        self._portfolio_chain: PortfolioProviderChain | None = None
        self._initialized = False

    async def _ensure_initialized(self) -> None:
        """Lazy initialization of integrations."""
        if self._initialized:
            return

        # Initialize Binance (public API, no key required)
        self._binance = BinanceIntegration()

        # Initialize CoinGecko (uses API key from settings if available)
        self._coingecko = CoinGeckoIntegration(
            api_key=self.settings.coingecko_api_key,
        )

        # Initialize TheGraph
        self._thegraph = TheGraphIntegration()

        # Build multi-provider portfolio chain
        self._portfolio_chain = build_portfolio_chain(
            portfolio_providers_csv=self.settings.portfolio_providers,
            portfolio_api_key=self.settings.portfolio_api_key,
            portfolio_api_provider=self.settings.portfolio_api_provider,
            portfolio_api_cache_ttl=self.settings.portfolio_api_cache_ttl,
        )

        # Reuse chain's zerion instance if available; otherwise create standalone
        if self._portfolio_chain:
            self._zerion = self._portfolio_chain.get_provider("zerion")  # type: ignore[assignment]
        if self._zerion is None:
            self._zerion = ZerionIntegration(
                api_key=self.settings.portfolio_api_key,
                cache_ttl=self.settings.portfolio_api_cache_ttl,
            )

        self._initialized = True
        chain_names = [p.name for p in self._portfolio_chain.providers] if self._portfolio_chain else []
        logger.debug(
            "IntegrationService initialized with Binance, CoinGecko, TheGraph, portfolio chain=%s",
            chain_names or "not configured",
        )

    # =========================================================================
    # Binance endpoints
    # =========================================================================

    async def BinanceGetTicker(
        self,
        request: gateway_pb2.BinanceTickerRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.BinanceTickerResponse:
        """Get Binance 24h ticker data.

        Args:
            request: Ticker request with symbol
            context: gRPC context

        Returns:
            BinanceTickerResponse with price and statistics
        """
        await self._ensure_initialized()

        # Validate symbol format
        try:
            symbol = validate_symbol(request.symbol)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.BinanceTickerResponse()

        try:
            assert self._binance is not None
            start_time = time.time()
            ticker = await self._binance.get_ticker(symbol)
            latency = time.time() - start_time

            record_integration_request("binance", "get_ticker")
            record_integration_latency("binance", "get_ticker", latency)

            return gateway_pb2.BinanceTickerResponse(
                symbol=ticker.get("symbol", ""),
                price=str(ticker.get("lastPrice", "")),
                price_change=str(ticker.get("priceChange", "")),
                price_change_percent=str(ticker.get("priceChangePercent", "")),
                high_24h=str(ticker.get("highPrice", "")),
                low_24h=str(ticker.get("lowPrice", "")),
                volume_24h=str(ticker.get("volume", "")),
                quote_volume_24h=str(ticker.get("quoteVolume", "")),
                timestamp=int(ticker.get("closeTime", 0)),
            )

        except Exception as e:
            logger.exception("BinanceGetTicker failed for %s", request.symbol)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.BinanceTickerResponse()

    async def BinanceGetKlines(
        self,
        request: gateway_pb2.BinanceKlinesRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.BinanceKlinesResponse:
        """Get Binance kline/candlestick data.

        Args:
            request: Klines request with symbol, interval, limit
            context: gRPC context

        Returns:
            BinanceKlinesResponse with list of klines
        """
        await self._ensure_initialized()

        # Validate symbol format
        try:
            symbol = validate_symbol(request.symbol)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.BinanceKlinesResponse()

        try:
            assert self._binance is not None
            start_time_metric = time.time()
            klines = await self._binance.get_klines(
                symbol=symbol,
                interval=request.interval or "1h",
                limit=request.limit or 100,
                start_time=request.start_time if request.start_time else None,
                end_time=request.end_time if request.end_time else None,
            )
            latency = time.time() - start_time_metric

            record_integration_request("binance", "get_klines")
            record_integration_latency("binance", "get_klines", latency)

            kline_messages = []
            for k in klines:
                kline_messages.append(
                    gateway_pb2.BinanceKline(
                        open_time=int(k.get("open_time", 0)),
                        open=str(k.get("open", "")),
                        high=str(k.get("high", "")),
                        low=str(k.get("low", "")),
                        close=str(k.get("close", "")),
                        volume=str(k.get("volume", "")),
                        close_time=int(k.get("close_time", 0)),
                        quote_volume=str(k.get("quote_volume", "")),
                        trades=int(k.get("trades", 0)),
                    )
                )

            return gateway_pb2.BinanceKlinesResponse(klines=kline_messages)

        except ValueError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.BinanceKlinesResponse()
        except Exception as e:
            logger.exception("BinanceGetKlines failed for %s", request.symbol)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.BinanceKlinesResponse()

    async def BinanceGetOrderBook(
        self,
        request: gateway_pb2.BinanceOrderBookRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.BinanceOrderBookResponse:
        """Get Binance order book depth.

        Args:
            request: Order book request with symbol and limit
            context: gRPC context

        Returns:
            BinanceOrderBookResponse with bids and asks
        """
        await self._ensure_initialized()

        # Validate symbol format
        try:
            symbol = validate_symbol(request.symbol)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.BinanceOrderBookResponse()

        try:
            assert self._binance is not None
            start_time_metric = time.time()
            order_book = await self._binance.get_order_book(
                symbol=symbol,
                limit=request.limit or 100,
            )
            latency = time.time() - start_time_metric

            record_integration_request("binance", "get_order_book")
            record_integration_latency("binance", "get_order_book", latency)

            bids = [
                gateway_pb2.BinanceOrderBookEntry(
                    price=str(b.get("price", "")),
                    quantity=str(b.get("quantity", "")),
                )
                for b in order_book.get("bids", [])
            ]

            asks = [
                gateway_pb2.BinanceOrderBookEntry(
                    price=str(a.get("price", "")),
                    quantity=str(a.get("quantity", "")),
                )
                for a in order_book.get("asks", [])
            ]

            return gateway_pb2.BinanceOrderBookResponse(
                last_update_id=order_book.get("last_update_id", 0),
                bids=bids,
                asks=asks,
            )

        except Exception as e:
            logger.exception("BinanceGetOrderBook failed for %s", request.symbol)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.BinanceOrderBookResponse()

    # =========================================================================
    # CoinGecko endpoints
    # =========================================================================

    async def CoinGeckoGetPrice(
        self,
        request: gateway_pb2.CoinGeckoGetPriceRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.CoinGeckoGetPriceResponse:
        """Get CoinGecko price for a single token.

        Args:
            request: Price request with token_id and vs_currencies
            context: gRPC context

        Returns:
            CoinGeckoGetPriceResponse with prices map
        """
        await self._ensure_initialized()

        # Validate token_id format
        try:
            token_id = validate_token_id(request.token_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.CoinGeckoGetPriceResponse()

        try:
            assert self._coingecko is not None
            vs_currencies = list(request.vs_currencies) if request.vs_currencies else ["usd"]
            start_time_metric = time.time()
            prices = await self._coingecko.get_price(
                token_id=token_id,
                vs_currencies=vs_currencies,
            )
            latency = time.time() - start_time_metric

            record_integration_request("coingecko", "get_price")
            record_integration_latency("coingecko", "get_price", latency)

            return gateway_pb2.CoinGeckoGetPriceResponse(
                prices=prices,
                timestamp=int(time.time()),
            )

        except Exception as e:
            logger.exception("CoinGeckoGetPrice failed for %s", request.token_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.CoinGeckoGetPriceResponse()

    async def CoinGeckoGetPrices(
        self,
        request: gateway_pb2.CoinGeckoGetPricesRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.CoinGeckoGetPricesResponse:
        """Get CoinGecko prices for multiple tokens.

        Args:
            request: Prices request with token_ids and vs_currencies
            context: gRPC context

        Returns:
            CoinGeckoGetPricesResponse with token prices
        """
        await self._ensure_initialized()

        # Validate all token_ids
        validated_token_ids = []
        for tid in request.token_ids:
            try:
                validated_token_ids.append(validate_token_id(tid))
            except ValidationError as e:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(str(e))
                return gateway_pb2.CoinGeckoGetPricesResponse()

        try:
            assert self._coingecko is not None
            vs_currencies = list(request.vs_currencies) if request.vs_currencies else ["usd"]

            start_time_metric = time.time()
            prices = await self._coingecko.get_prices(
                token_ids=validated_token_ids,
                vs_currencies=vs_currencies,
            )
            latency = time.time() - start_time_metric

            record_integration_request("coingecko", "get_prices")
            record_integration_latency("coingecko", "get_prices", latency)

            token_prices = []
            for token_id, token_data in prices.items():
                token_prices.append(
                    gateway_pb2.CoinGeckoTokenPrice(
                        token_id=token_id,
                        prices=token_data,
                    )
                )

            return gateway_pb2.CoinGeckoGetPricesResponse(
                tokens=token_prices,
                timestamp=int(time.time()),
            )

        except Exception as e:
            logger.exception("CoinGeckoGetPrices failed")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.CoinGeckoGetPricesResponse()

    async def CoinGeckoGetMarkets(
        self,
        request: gateway_pb2.CoinGeckoGetMarketsRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.CoinGeckoGetMarketsResponse:
        """Get CoinGecko market data.

        Args:
            request: Markets request with filters and pagination
            context: gRPC context

        Returns:
            CoinGeckoGetMarketsResponse with market data
        """
        await self._ensure_initialized()

        try:
            assert self._coingecko is not None
            ids = list(request.ids) if request.ids else None
            start_time_metric = time.time()
            markets = await self._coingecko.get_markets(
                vs_currency=request.vs_currency or "usd",
                ids=ids,
                order=request.order or "market_cap_desc",
                per_page=request.per_page or 100,
                page=request.page or 1,
            )
            latency = time.time() - start_time_metric

            record_integration_request("coingecko", "get_markets")
            record_integration_latency("coingecko", "get_markets", latency)

            market_messages = []
            for m in markets:
                market_messages.append(
                    gateway_pb2.CoinGeckoMarket(
                        id=m.get("id", ""),
                        symbol=m.get("symbol", ""),
                        name=m.get("name", ""),
                        current_price=m.get("current_price", "0"),
                        market_cap=m.get("market_cap", "0"),
                        market_cap_rank=m.get("market_cap_rank", 0) or 0,
                        total_volume=m.get("total_volume", "0"),
                        high_24h=m.get("high_24h", "0"),
                        low_24h=m.get("low_24h", "0"),
                        price_change_24h=m.get("price_change_24h", "0"),
                        price_change_percentage_24h=m.get("price_change_percentage_24h", "0"),
                        last_updated=0,  # Could parse ISO timestamp if needed
                    )
                )

            return gateway_pb2.CoinGeckoGetMarketsResponse(markets=market_messages)

        except Exception as e:
            logger.exception("CoinGeckoGetMarkets failed")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.CoinGeckoGetMarketsResponse()

    async def CoinGeckoGetHistoricalPrice(
        self,
        request: gateway_pb2.CoinGeckoHistoricalPriceRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.CoinGeckoHistoricalPriceResponse:
        """Get CoinGecko historical price for a token at a specific date.

        Args:
            request: Historical price request with token_id and date
            context: gRPC context

        Returns:
            CoinGeckoHistoricalPriceResponse with price data
        """
        await self._ensure_initialized()

        # Validate token_id format
        try:
            token_id = validate_token_id(request.token_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.CoinGeckoHistoricalPriceResponse(success=False, error=str(e))

        # Parse and validate date format (CoinGecko expects dd-mm-yyyy)
        try:
            parsed_date = datetime.strptime(request.date, "%d-%m-%Y").replace(tzinfo=UTC)
            date_timestamp = int(parsed_date.timestamp())
        except ValueError:
            error_msg = f"Invalid date format: {request.date}. Expected dd-mm-yyyy (e.g., 30-12-2022)"
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(error_msg)
            return gateway_pb2.CoinGeckoHistoricalPriceResponse(success=False, error=error_msg)

        try:
            assert self._coingecko is not None
            start_time_metric = time.time()
            data = await self._coingecko.get_historical_price(
                token_id=token_id,
                date=request.date,
            )
            latency = time.time() - start_time_metric

            record_integration_request("coingecko", "get_historical_price")
            record_integration_latency("coingecko", "get_historical_price", latency)

            return gateway_pb2.CoinGeckoHistoricalPriceResponse(
                price_usd=data.get("price_usd", "0"),
                market_cap_usd=data.get("market_cap_usd", "0"),
                volume_usd=data.get("volume_usd", "0"),
                timestamp=date_timestamp,
                success=True,
            )

        except Exception as e:
            logger.exception("CoinGeckoGetHistoricalPrice failed for %s", request.token_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.CoinGeckoHistoricalPriceResponse(success=False, error=str(e))

    async def CoinGeckoGetMarketChartRange(
        self,
        request: gateway_pb2.CoinGeckoMarketChartRangeRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.CoinGeckoMarketChartRangeResponse:
        """Get CoinGecko market chart data for a token over a time range.

        Args:
            request: Market chart request with token_id, from/to timestamps
            context: gRPC context

        Returns:
            CoinGeckoMarketChartRangeResponse with price/volume data points
        """
        await self._ensure_initialized()

        # Validate token_id format
        try:
            token_id = validate_token_id(request.token_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.CoinGeckoMarketChartRangeResponse(success=False, error=str(e))

        # Validate timestamp range
        if request.from_timestamp < 0:
            error_msg = "from_timestamp must be non-negative"
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(error_msg)
            return gateway_pb2.CoinGeckoMarketChartRangeResponse(success=False, error=error_msg)

        if request.to_timestamp < 0:
            error_msg = "to_timestamp must be non-negative"
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(error_msg)
            return gateway_pb2.CoinGeckoMarketChartRangeResponse(success=False, error=error_msg)

        if request.from_timestamp > request.to_timestamp:
            error_msg = "from_timestamp must be less than or equal to to_timestamp"
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(error_msg)
            return gateway_pb2.CoinGeckoMarketChartRangeResponse(success=False, error=error_msg)

        try:
            assert self._coingecko is not None
            start_time_metric = time.time()
            data = await self._coingecko.get_market_chart_range(
                token_id=token_id,
                from_timestamp=request.from_timestamp,
                to_timestamp=request.to_timestamp,
                vs_currency=request.vs_currency or "usd",
            )
            latency = time.time() - start_time_metric

            record_integration_request("coingecko", "get_market_chart_range")
            record_integration_latency("coingecko", "get_market_chart_range", latency)

            # Convert prices list to proto messages
            prices = []
            for ts_ms, price in data.get("prices", []):
                prices.append(
                    gateway_pb2.CoinGeckoMarketChartDataPoint(
                        timestamp=int(ts_ms),
                        value=str(price),
                    )
                )

            market_caps = []
            for ts_ms, market_cap in data.get("market_caps", []):
                market_caps.append(
                    gateway_pb2.CoinGeckoMarketChartDataPoint(
                        timestamp=int(ts_ms),
                        value=str(market_cap),
                    )
                )

            total_volumes = []
            for ts_ms, volume in data.get("total_volumes", []):
                total_volumes.append(
                    gateway_pb2.CoinGeckoMarketChartDataPoint(
                        timestamp=int(ts_ms),
                        value=str(volume),
                    )
                )

            return gateway_pb2.CoinGeckoMarketChartRangeResponse(
                prices=prices,
                market_caps=market_caps,
                total_volumes=total_volumes,
                success=True,
            )

        except Exception as e:
            logger.exception("CoinGeckoGetMarketChartRange failed for %s", request.token_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.CoinGeckoMarketChartRangeResponse(success=False, error=str(e))

    # =========================================================================
    # TheGraph endpoints
    # =========================================================================

    async def TheGraphQuery(
        self,
        request: gateway_pb2.TheGraphQueryRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.TheGraphQueryResponse:
        """Execute a TheGraph subgraph query.

        Args:
            request: Query request with subgraph_id, query, variables
            context: gRPC context

        Returns:
            TheGraphQueryResponse with query result or errors
        """
        await self._ensure_initialized()

        # Validate GraphQL query
        try:
            query = validate_graphql_query(request.query)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.TheGraphQueryResponse(
                success=False,
                errors=json.dumps([{"message": str(e)}]),
            )

        try:
            # Parse variables if provided
            variables = None
            if request.variables:
                try:
                    variables = json.loads(request.variables)
                except json.JSONDecodeError:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details("Invalid variables JSON")
                    return gateway_pb2.TheGraphQueryResponse(success=False)

            assert self._thegraph is not None
            start_time_metric = time.time()
            result = await self._thegraph.query(
                subgraph_id=request.subgraph_id,
                query=query,
                variables=variables,
            )
            latency = time.time() - start_time_metric

            record_integration_request("thegraph", "query")
            record_integration_latency("thegraph", "query", latency)

            return gateway_pb2.TheGraphQueryResponse(
                data=json.dumps(result.get("data")) if result.get("data") else "",
                errors=json.dumps(result.get("errors")) if result.get("errors") else "",
                success=result.get("success", False),
            )

        except Exception as e:
            logger.exception("TheGraphQuery failed for %s", request.subgraph_id)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.TheGraphQueryResponse(
                success=False,
                errors=json.dumps([{"message": str(e)}]),
            )

    # =========================================================================
    # GeckoTerminal endpoints
    # =========================================================================

    async def GeckoTerminalGetOHLCV(
        self,
        request: gateway_pb2.GeckoTerminalOHLCVRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.GeckoTerminalOHLCVResponse:
        """Get DEX OHLCV data from GeckoTerminal.

        Proxies GeckoTerminal API requests from strategy containers that
        have no internet access. Mirrors the BinanceGetKlines pattern.

        Args:
            request: OHLCV request with token, chain, timeframe, limit
            context: gRPC context

        Returns:
            GeckoTerminalOHLCVResponse with list of candles
        """
        await self._ensure_initialized()

        # --- Input validation at the gRPC boundary ---
        if not request.token or not request.token.strip():
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("token is required and cannot be empty")
            return gateway_pb2.GeckoTerminalOHLCVResponse()

        if not request.chain or not request.chain.strip():
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("chain is required and cannot be empty")
            return gateway_pb2.GeckoTerminalOHLCVResponse()

        req_timeframe = request.timeframe or "1h"
        valid_timeframes = {"1m", "5m", "15m", "30m", "1h", "4h", "1d"}
        if req_timeframe not in valid_timeframes:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"Invalid timeframe: {req_timeframe}. Valid: {sorted(valid_timeframes)}")
            return gateway_pb2.GeckoTerminalOHLCVResponse()

        req_limit = request.limit or 100
        if req_limit < 1 or req_limit > 1000:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"limit must be between 1 and 1000, got {req_limit}")
            return gateway_pb2.GeckoTerminalOHLCVResponse()

        try:
            from almanak.framework.data.ohlcv.geckoterminal_provider import GeckoTerminalOHLCVProvider

            start_time_metric = time.monotonic()

            async with GeckoTerminalOHLCVProvider() as provider:
                candles = await provider.get_ohlcv(
                    token=request.token.strip(),
                    quote=request.quote or "USD",
                    timeframe=req_timeframe,
                    limit=req_limit,
                    chain=request.chain.strip(),
                    pool_address=request.pool_address or None,
                )

            latency = time.monotonic() - start_time_metric
            record_integration_request("geckoterminal", "get_ohlcv")
            record_integration_latency("geckoterminal", "get_ohlcv", latency)

            candle_messages = []
            for c in candles:
                candle_messages.append(
                    gateway_pb2.GeckoTerminalOHLCVCandle(
                        timestamp=int(c.timestamp.timestamp()),
                        open=str(c.open),
                        high=str(c.high),
                        low=str(c.low),
                        close=str(c.close),
                        volume=str(c.volume) if c.volume is not None else "",
                    )
                )

            return gateway_pb2.GeckoTerminalOHLCVResponse(candles=candle_messages)

        except ValueError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.GeckoTerminalOHLCVResponse()
        except Exception:
            logger.exception("GeckoTerminalGetOHLCV failed for %s on %s", request.token, request.chain)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("GeckoTerminal OHLCV request failed")
            return gateway_pb2.GeckoTerminalOHLCVResponse()

    # =========================================================================
    # Wallet portfolio endpoints
    # =========================================================================

    async def GetWalletPortfolio(
        self,
        request: gateway_pb2.WalletPortfolioRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.WalletPortfolioResponse:
        """Get external wallet portfolio summary for a chain."""
        await self._ensure_initialized()
        provider_pin, chain, wallet_address = self._validate_wallet_portfolio_request(request, context)
        if chain is None or wallet_address is None:
            return gateway_pb2.WalletPortfolioResponse(success=False)

        return await self._handle_portfolio_request(
            provider_pin, chain, wallet_address, "get_wallet_portfolio", context
        )

    async def GetWalletPositions(
        self,
        request: gateway_pb2.WalletPortfolioRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.WalletPortfolioResponse:
        """Get external wallet positions for a chain."""
        await self._ensure_initialized()
        provider_pin, chain, wallet_address = self._validate_wallet_portfolio_request(request, context)
        if chain is None or wallet_address is None:
            return gateway_pb2.WalletPortfolioResponse(success=False)

        return await self._handle_portfolio_request(
            provider_pin, chain, wallet_address, "get_wallet_positions", context
        )

    async def _handle_portfolio_request(
        self,
        provider_pin: str | None,
        chain: str,
        wallet_address: str,
        method_name: str,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.WalletPortfolioResponse:
        """Handle a portfolio request using either a pinned provider or the chain."""
        try:
            if provider_pin:
                # Caller explicitly requested a specific provider — bypass the chain
                integration = self._get_pinned_provider(provider_pin, context)
                if integration is None:
                    return gateway_pb2.WalletPortfolioResponse(success=False, provider=provider_pin, chain=chain)
                start_time_metric = time.monotonic()
                method = getattr(integration, method_name)
                snapshot = await method(wallet_address=wallet_address, chain=chain)
                latency = time.monotonic() - start_time_metric
                record_integration_request(provider_pin, method_name)
                record_integration_latency(provider_pin, method_name, latency)
            else:
                # No pin — use the provider chain with fallback
                if self._portfolio_chain is None:
                    context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                    context.set_details("No portfolio providers configured")
                    return gateway_pb2.WalletPortfolioResponse(success=False)
                start_time_metric = time.monotonic()
                chain_method = getattr(self._portfolio_chain, method_name)
                snapshot = await chain_method(wallet_address=wallet_address, chain=chain)
                latency = time.monotonic() - start_time_metric
                if snapshot is None:
                    logger.warning("All portfolio providers failed for %s on %s", wallet_address, chain)
                    return gateway_pb2.WalletPortfolioResponse(
                        success=False,
                        chain=chain,
                        wallet_address=wallet_address,
                        error="All portfolio providers failed",
                    )
                record_integration_request(snapshot.provider, method_name)
                record_integration_latency(snapshot.provider, method_name, latency)

            return self._wallet_snapshot_to_proto(snapshot, success=True)
        except Exception:
            logger.exception("%s failed for %s on %s", method_name, wallet_address, chain)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Wallet portfolio request failed")
            return gateway_pb2.WalletPortfolioResponse(
                success=False,
                provider=provider_pin or "",
                wallet_address=wallet_address,
                chain=chain,
                error="Wallet portfolio request failed",
            )

    def _validate_wallet_portfolio_request(
        self,
        request: gateway_pb2.WalletPortfolioRequest,
        context: grpc.aio.ServicerContext,
    ) -> tuple[str | None, str | None, str | None]:
        """Validate and extract fields. Returns (provider_pin_or_None, chain, wallet_address)."""
        # Empty provider = use chain; non-empty = pin to that provider
        provider_pin = (request.provider or "").strip().lower() or None

        try:
            chain = validate_chain(request.chain)
            wallet_address = validate_address_for_chain(request.wallet_address, chain, field="wallet_address")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return None, None, None

        return provider_pin, chain, wallet_address

    def _get_pinned_provider(
        self,
        provider_name: str,
        context: grpc.aio.ServicerContext,
    ) -> BaseIntegration | None:
        """Get a specific provider by name for pinned requests."""
        # Check the chain first (it has all registered providers)
        if self._portfolio_chain:
            provider = self._portfolio_chain.get_provider(provider_name)
            if provider:
                return provider

        # Legacy fallback: direct zerion reference
        if provider_name == "zerion" and self._zerion and self._zerion.is_configured:
            return self._zerion

        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
        context.set_details(f"Portfolio provider not available: {provider_name}")
        return None

    @staticmethod
    def _wallet_snapshot_to_proto(
        snapshot: WalletPortfolioSnapshot, success: bool
    ) -> gateway_pb2.WalletPortfolioResponse:
        positions = [
            gateway_pb2.WalletPortfolioPosition(
                position_id=position.position_id,
                protocol=position.protocol,
                label=position.label,
                position_type=position.position_type,
                value_usd=position.value_usd,
                pool_address=position.pool_address,
                token_symbols=position.token_symbols,
                raw_details_json=json.dumps(position.details, sort_keys=True, default=str),
            )
            for position in snapshot.positions
        ]
        return gateway_pb2.WalletPortfolioResponse(
            success=success,
            provider=snapshot.provider,
            wallet_address=snapshot.wallet_address,
            chain=snapshot.chain,
            total_value_usd=snapshot.total_value_usd,
            timestamp=int(snapshot.fetched_at.timestamp()),
            cache_hit=snapshot.cache_hit,
            positions=positions,
        )

    async def close(self) -> None:
        """Close all integration connections."""
        if self._binance:
            await self._binance.close()
        if self._coingecko:
            await self._coingecko.close()
        if self._thegraph:
            await self._thegraph.close()
        if self._portfolio_chain:
            await self._portfolio_chain.close()
        # Close the standalone zerion instance if it is NOT already managed
        # by the portfolio chain (which closes its own providers).
        if self._zerion and (
            not self._portfolio_chain or self._portfolio_chain.get_provider("zerion") is not self._zerion
        ):
            await self._zerion.close()
