"""MarketService implementation - provides market data to strategies.

This service provides price, balance, and indicator data to strategy containers
via gRPC. All external API calls (CoinGecko, Web3 RPC) are made here in the
gateway; strategy containers only see the results.
"""

import asyncio
import logging
import time
from typing import Any

import grpc

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.validation import (
    ValidationError,
    validate_address,
    validate_chain,
)

logger = logging.getLogger(__name__)


class MarketServiceServicer(gateway_pb2_grpc.MarketServiceServicer):
    """Implements MarketService gRPC interface.

    Provides market data access for strategy containers:
    - GetPrice: Token prices from aggregated sources
    - GetBalance: Token balances from on-chain
    - GetIndicator: Technical indicators (RSI, MACD, etc.)
    """

    def __init__(self, settings: GatewaySettings):
        """Initialize MarketService.

        Args:
            settings: Gateway settings with API keys and configuration.
        """
        self.settings = settings
        self._price_aggregator: Any = None
        self._balance_providers: dict[str, object] = {}
        self._initialized = False

    async def close(self) -> None:
        """Close resources held by MarketService (HTTP sessions, etc.)."""
        if self._price_aggregator is not None and hasattr(self._price_aggregator, "close"):
            await self._price_aggregator.close()
        for provider in self._balance_providers.values():
            if hasattr(provider, "close"):
                await provider.close()
        self._balance_providers.clear()

    async def _ensure_initialized(self) -> None:
        """Lazy initialization of data providers."""
        if self._initialized:
            return

        from almanak.gateway.data.price.aggregator import PriceAggregator
        from almanak.gateway.data.price.coingecko import CoinGeckoPriceSource
        from almanak.gateway.data.price.onchain import OnChainPriceSource

        # Determine primary chain for on-chain pricing.
        # IMPORTANT: Never default to a hardcoded chain -- that silently gives wrong
        # Chainlink oracle data for strategies running on a different chain (QA #4/#7/#8).
        chain = self.settings.chains[0] if self.settings.chains else None

        # Create price sources
        cg_source = CoinGeckoPriceSource(
            api_key=self.settings.coingecko_api_key if self.settings.coingecko_api_key is not None else "",
            cache_ttl=30,
        )

        has_cg_key = bool(self.settings.coingecko_api_key)

        if chain:
            onchain_source = OnChainPriceSource(chain=chain, network=self.settings.network)
            if has_cg_key:
                sources = [cg_source, onchain_source]
                logger.info("MarketService: CoinGecko (primary) + on-chain (fallback), chain=%s", chain)
            else:
                sources = [onchain_source, cg_source]
                logger.info("MarketService: on-chain (primary) + CoinGecko free tier (fallback), chain=%s", chain)
        else:
            # No chain configured -- on-chain pricing unavailable.
            # This can happen with standalone `almanak gateway` without --chains.
            sources = [cg_source]
            logger.warning(
                "MarketService: No chain configured -- on-chain (Chainlink) pricing DISABLED. "
                "Only CoinGecko is available. Pass --chains to the gateway or set ALMANAK_GATEWAY_CHAINS "
                "for accurate on-chain pricing."
            )

        self._price_aggregator = PriceAggregator(sources=sources)

        self._initialized = True

    async def _get_balance_provider(self, chain: str, wallet_address: str):
        """Get or create balance provider for a chain.

        Args:
            chain: Chain name (e.g., "arbitrum", "base")
            wallet_address: Wallet address to query

        Returns:
            Web3BalanceProvider for the specified chain
        """
        from almanak.gateway.data.balance import Web3BalanceProvider
        from almanak.gateway.utils import get_rpc_url

        cache_key = f"{chain}:{wallet_address}"
        if cache_key not in self._balance_providers:
            # Use network from settings (default: mainnet, can be set to anvil for testing)
            network = self.settings.network
            rpc_url = get_rpc_url(chain, network=network)
            self._balance_providers[cache_key] = Web3BalanceProvider(
                rpc_url=rpc_url,
                wallet_address=wallet_address,
                chain=chain,
            )

        return self._balance_providers[cache_key]

    async def GetPrice(
        self,
        request: gateway_pb2.PriceRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PriceResponse:
        """Get token price from aggregated sources.

        Args:
            request: Price request with token and quote currency
            context: gRPC context

        Returns:
            PriceResponse with price, timestamp, source, confidence
        """
        await self._ensure_initialized()

        token = request.token
        quote = request.quote or "USD"

        try:
            result = await self._price_aggregator.get_aggregated_price(token, quote)
            details = self._price_aggregator.get_last_details(token, quote)

            response = gateway_pb2.PriceResponse(
                price=str(result.price),
                timestamp=int(result.timestamp.timestamp()),
                source=result.source,
                confidence=result.confidence,
                stale=result.stale,
            )
            if details:
                response.sources_ok.extend(details.get("sources_ok", []))
                for k, v in details.get("sources_failed", {}).items():
                    response.sources_failed[k] = v
                response.outliers.extend(details.get("outliers", []))
            return response
        except Exception as e:
            logger.error(f"GetPrice failed for {token}/{quote}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.PriceResponse()

    async def GetBalance(
        self,
        request: gateway_pb2.BalanceRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.BalanceResponse:
        """Get token balance for wallet.

        Args:
            request: Balance request with token, chain, wallet_address
            context: gRPC context

        Returns:
            BalanceResponse with balance in human-readable units
        """
        await self._ensure_initialized()

        token = request.token

        # Validate chain
        try:
            chain = validate_chain(request.chain or "arbitrum")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.BalanceResponse()

        # Validate wallet address format
        try:
            wallet_address = validate_address(request.wallet_address, "wallet_address")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.BalanceResponse()

        try:
            provider = await self._get_balance_provider(chain, wallet_address)

            if token.upper() in ("ETH", "AVAX", "MATIC"):
                result = await provider.get_native_balance()
            else:
                result = await provider.get_balance(token)

            # Get USD value if available
            balance_usd = ""
            try:
                price_result = await self._price_aggregator.get_aggregated_price(token, "USD")
                balance_usd = str(result.balance * price_result.price)
            except Exception:
                pass  # USD conversion optional

            return gateway_pb2.BalanceResponse(
                balance=str(result.balance),
                balance_usd=balance_usd,
                address=result.address,
                decimals=result.decimals,
                raw_balance=str(result.raw_balance),
                timestamp=int(result.timestamp.timestamp()),
                stale=result.stale,
            )
        except Exception as e:
            logger.error(f"GetBalance failed for {token} on {chain}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.BalanceResponse()

    async def BatchGetBalances(
        self,
        request: gateway_pb2.BatchBalanceRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.BatchBalanceResponse:
        """Get balances for multiple tokens/chains in a single call.

        Executes individual balance queries concurrently. Partial success
        is allowed -- per-response errors are returned for failed queries.

        Args:
            request: Batch balance request with list of BalanceRequest
            context: gRPC context

        Returns:
            BatchBalanceResponse with per-request BalanceResponse
        """
        await self._ensure_initialized()

        async def _get_single_balance(req: gateway_pb2.BalanceRequest) -> gateway_pb2.BalanceResponse:
            """Get a single balance, returning error in response on failure."""
            try:
                chain = validate_chain(req.chain or "arbitrum")
            except ValidationError as e:
                return gateway_pb2.BalanceResponse(error=str(e))

            try:
                wallet_address = validate_address(req.wallet_address, "wallet_address")
            except ValidationError as e:
                return gateway_pb2.BalanceResponse(error=str(e))

            token = req.token
            try:
                provider = await self._get_balance_provider(chain, wallet_address)

                if token.upper() in ("ETH", "AVAX", "MATIC"):
                    result = await provider.get_native_balance()
                else:
                    result = await provider.get_balance(token)

                balance_usd = ""
                try:
                    price_result = await self._price_aggregator.get_aggregated_price(token, "USD")
                    balance_usd = str(result.balance * price_result.price)
                except Exception:
                    pass

                return gateway_pb2.BalanceResponse(
                    balance=str(result.balance),
                    balance_usd=balance_usd,
                    address=result.address,
                    decimals=result.decimals,
                    raw_balance=str(result.raw_balance),
                    timestamp=int(result.timestamp.timestamp()),
                    stale=result.stale,
                )
            except Exception as e:
                logger.warning(f"BatchGetBalances: failed for {token} on {chain}: {e}")
                return gateway_pb2.BalanceResponse(error=str(e))

        tasks = [_get_single_balance(req) for req in request.requests]
        responses = await asyncio.gather(*tasks)

        return gateway_pb2.BatchBalanceResponse(responses=list(responses))

    async def GetIndicator(
        self,
        request: gateway_pb2.IndicatorRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.IndicatorResponse:
        """Get technical indicator value.

        Args:
            request: Indicator request with type, token, params
            context: gRPC context

        Returns:
            IndicatorResponse with indicator value and metadata
        """
        indicator_type = request.indicator_type.upper()
        token = request.token
        params = dict(request.params)

        try:
            if indicator_type == "RSI":
                # RSI indicator
                from almanak.framework.data.indicators.rsi import CoinGeckoOHLCVProvider, RSICalculator

                period = int(params.get("period", "14"))
                timeframe = params.get("timeframe", "1h")

                async with CoinGeckoOHLCVProvider() as ohlcv_provider:
                    indicator = RSICalculator(ohlcv_provider=ohlcv_provider, default_period=period)
                    value = await indicator.calculate_rsi(token, period=period, timeframe=timeframe)

                return gateway_pb2.IndicatorResponse(
                    value=str(value),
                    metadata={"period": str(period), "timeframe": timeframe},
                    timestamp=int(time.time()),
                )
            else:
                context.set_code(grpc.StatusCode.UNIMPLEMENTED)
                context.set_details(f"Indicator type '{indicator_type}' not supported")
                return gateway_pb2.IndicatorResponse()

        except Exception as e:
            logger.error(f"GetIndicator failed for {indicator_type} on {token}: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return gateway_pb2.IndicatorResponse()
