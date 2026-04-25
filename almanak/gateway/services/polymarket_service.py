"""PolymarketService implementation - Polymarket CLOB API proxy.

This service provides secure access to Polymarket's CLOB API:
- L1 Authentication (EIP-712) for credential creation
- L2 Authentication (HMAC-SHA256) for trading operations
- All credentials held in gateway, keeping secrets secure

The service proxies calls to:
- CLOB API: Order management, orderbooks, prices
- Gamma API: Market metadata
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from urllib.parse import urlencode

import aiohttp
import grpc
from eth_account import Account
from eth_account.messages import encode_typed_data
from pydantic import SecretStr

from almanak.framework.connectors.polymarket import (
    CLOB_AUTH_DOMAIN,
    CLOB_AUTH_MESSAGE,
    CLOB_AUTH_TYPES,
    ApiCredentials,
    ClobClient,
    OrderFilters,
    PolymarketConfig,
    SignatureType,
)
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.utils.ssl_context import build_ssl_context

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

CLOB_BASE_URL = "https://clob.polymarket.com"
GAMMA_BASE_URL = "https://gamma-api.polymarket.com"


class PolymarketServiceServicer(gateway_pb2_grpc.PolymarketServiceServicer):
    """Implements PolymarketService gRPC interface.

    Provides secure proxy to Polymarket CLOB API with credentials held in gateway.
    """

    def __init__(self, settings: GatewaySettings):
        """Initialize PolymarketService.

        Args:
            settings: Gateway settings (contains Polymarket credentials)
        """
        self.settings = settings
        self._http_session: aiohttp.ClientSession | None = None
        self._credentials_lock = asyncio.Lock()

        self._private_key = settings.private_key or settings.polymarket_private_key
        self._wallet_address = self._resolve_signer_address()
        self._funder_address = self._resolve_funder_address()
        self._signature_type = (
            SignatureType.POLY_GNOSIS_SAFE
            if settings.safe_address and (settings.safe_mode or "").lower() in {"direct", "zodiac"}
            else SignatureType.EOA
        )
        self._api_key = settings.polymarket_api_key
        self._api_secret = settings.polymarket_secret
        self._api_passphrase = settings.polymarket_passphrase

        self._available = bool(self._private_key and self._wallet_address)
        self._credentials_available = bool(self._api_key and self._api_secret and self._api_passphrase)

        logger.debug(
            "PolymarketService initialized: available=%s, credentials=%s, signer=%s, funder=%s, signature_type=%s",
            self._available,
            self._credentials_available,
            self._wallet_address,
            self._funder_address,
            self._signature_type.name,
        )

    def _resolve_signer_address(self) -> str | None:
        if self.settings.eoa_address:
            return self.settings.eoa_address
        if self.settings.private_key:
            return Account.from_key(self.settings.private_key).address
        if self.settings.polymarket_private_key:
            return Account.from_key(self.settings.polymarket_private_key).address
        return None

    def _resolve_funder_address(self) -> str | None:
        if self.settings.polymarket_wallet_address:
            return self.settings.polymarket_wallet_address
        if self.settings.safe_address:
            return self.settings.safe_address
        return self._wallet_address

    def _build_client(self) -> ClobClient:
        if not self._available or not self._wallet_address or not self._private_key:
            raise ValueError("Polymarket signing identity is not configured in the gateway")

        api_credentials = None
        if self._credentials_available and self._api_key and self._api_secret and self._api_passphrase:
            api_credentials = ApiCredentials(
                api_key=self._api_key,
                secret=SecretStr(self._api_secret),
                passphrase=SecretStr(self._api_passphrase),
            )

        config = PolymarketConfig(
            wallet_address=self._wallet_address,
            private_key=SecretStr(self._private_key),
            signature_type=self._signature_type,
            funder_address=self._funder_address if self._funder_address != self._wallet_address else None,
            api_credentials=api_credentials,
        )
        return ClobClient(config)

    async def _build_authenticated_client(self) -> ClobClient:
        """Build a CLOB client with stable gateway-owned API credentials.

        Polymarket API keys are wallet-scoped but some authenticated endpoints
        are sensitive to which API key created the order. Re-deriving a fresh
        key for each RPC can make a just-created order unreadable via
        ``GetOrder`` even though ``CreateAndPostOrder`` succeeded. Resolve or
        derive once, cache on the service, and reuse the same credentials for
        subsequent authenticated calls.
        """
        if not self._credentials_available:
            ok = await self._ensure_credentials()
            if not ok:
                raise ValueError("Polymarket API credentials could not be derived in gateway")
        return self._build_client()

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._http_session is None or self._http_session.closed:
            connector = aiohttp.TCPConnector(ssl=build_ssl_context())
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30.0),
                connector=connector,
            )
        return self._http_session

    async def close(self) -> None:
        """Close HTTP session."""
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
            self._http_session = None

    # =========================================================================
    # L1 Authentication (EIP-712)
    # =========================================================================

    def _build_l1_headers(self, nonce: int = 0) -> dict[str, str]:
        """Build L1 authentication headers using EIP-712 signing."""
        timestamp = str(int(time.time()))

        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                ],
                **CLOB_AUTH_TYPES,
            },
            "primaryType": "ClobAuth",
            "domain": CLOB_AUTH_DOMAIN,
            "message": {
                "address": self._wallet_address,
                "timestamp": timestamp,
                "nonce": nonce,
                "message": CLOB_AUTH_MESSAGE,
            },
        }

        signable = encode_typed_data(full_message=typed_data)
        signed = Account.sign_message(signable, self._private_key)

        return {
            "POLY_ADDRESS": self._wallet_address,
            "POLY_SIGNATURE": signed.signature.hex(),
            "POLY_TIMESTAMP": timestamp,
            "POLY_NONCE": str(nonce),
        }

    async def _ensure_credentials(self) -> bool:
        """Ensure we have API credentials, creating if needed."""
        if self._credentials_available:
            return True

        if not self._available:
            return False

        async with self._credentials_lock:
            # Re-check inside lock in case another coroutine just derived them.
            if self._credentials_available:
                return True
            return await self._derive_or_create_credentials()

    async def _derive_or_create_credentials(self) -> bool:
        """Inner credential derivation/creation (must be called while holding _credentials_lock)."""
        # Try to derive existing credentials
        try:
            session = await self._get_session()
            headers = self._build_l1_headers()

            async with session.get(f"{CLOB_BASE_URL}/auth/derive-api-key", headers=headers) as response:
                if response.status == 200:
                    try:
                        data = await response.json()
                    except (aiohttp.ContentTypeError, json.JSONDecodeError, ValueError) as e:
                        response_text = await response.text()
                        logger.warning(
                            "Failed to parse derive credentials response: %s, body: %s", e, response_text[:200]
                        )
                    else:
                        self._api_key = data.get("apiKey")
                        self._api_secret = data.get("secret")
                        self._api_passphrase = data.get("passphrase")
                        self._credentials_available = True
                        logger.info("Derived existing API credentials")
                        return True
        except (TimeoutError, aiohttp.ClientError) as e:
            logger.warning("Failed to derive credentials: %s", e)

        # Create new credentials
        try:
            session = await self._get_session()
            headers = self._build_l1_headers()

            async with session.post(f"{CLOB_BASE_URL}/auth/api-key", headers=headers) as response:
                if response.status == 200:
                    try:
                        data = await response.json()
                    except (aiohttp.ContentTypeError, json.JSONDecodeError, ValueError) as e:
                        response_text = await response.text()
                        logger.warning(
                            "Failed to parse create credentials response: %s, body: %s", e, response_text[:200]
                        )
                    else:
                        self._api_key = data.get("apiKey")
                        self._api_secret = data.get("secret")
                        self._api_passphrase = data.get("passphrase")
                        self._credentials_available = True
                        logger.info("Created new API credentials")
                        return True
                else:
                    error_body = (await response.text())[:200]
                    logger.error("Failed to create credentials: HTTP %s, body: %s", response.status, error_body)
        except (TimeoutError, aiohttp.ClientError):
            logger.exception("Failed to create credentials")

        return False

    # =========================================================================
    # L2 Authentication (HMAC-SHA256)
    # =========================================================================

    def _build_l2_signature(self, method: str, path: str, timestamp: str, body: str = "") -> str:
        """Build HMAC-SHA256 signature for L2 authentication.

        Raises:
            ValueError: If api_secret is not valid base64
        """
        message = f"{timestamp}{method}{path}{body}"
        try:
            secret_bytes = base64.b64decode(self._api_secret)  # type: ignore[arg-type]
        except Exception as e:
            err_msg = f"Invalid Polymarket API secret: not valid base64 - {e}"
            raise ValueError(err_msg) from e
        signature = hmac.new(
            secret_bytes,
            message.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(signature).decode("utf-8")

    def _build_l2_headers(self, method: str, path: str, body: str = "") -> dict[str, str]:
        """Build L2 authentication headers."""
        missing = []
        if not self._wallet_address:
            missing.append("wallet_address")
        if not self._api_key:
            missing.append("api_key")
        if not self._api_secret:
            missing.append("api_secret")
        if not self._api_passphrase:
            missing.append("api_passphrase")
        if missing:
            raise ValueError(f"Polymarket L2 credentials missing: {', '.join(missing)}")

        timestamp = str(int(time.time()))
        signature = self._build_l2_signature(method, path, timestamp, body)

        return {
            "POLY_ADDRESS": str(self._wallet_address),
            "POLY_SIGNATURE": signature,
            "POLY_TIMESTAMP": timestamp,
            "POLY_API_KEY": str(self._api_key),
            "POLY_PASSPHRASE": str(self._api_passphrase),
        }

    # =========================================================================
    # HTTP Helpers
    # =========================================================================

    async def _request(
        self,
        method: str,
        base_url: str,
        endpoint: str,
        params: dict | None = None,
        json_body: dict | None = None,
        authenticated: bool = False,
    ) -> tuple[bool, dict | None, str | None]:
        """Make HTTP request.

        Returns:
            Tuple of (success, data, error)
        """
        session = await self._get_session()
        url = f"{base_url}{endpoint}"

        headers = {"Content-Type": "application/json"}

        path = endpoint
        if params:
            path = f"{path}?{urlencode(params)}"

        body = ""
        if json_body:
            body = json.dumps(json_body, separators=(",", ":"))

        if authenticated:
            if not await self._ensure_credentials():
                return False, None, "Polymarket credentials not configured"
            try:
                auth_headers = self._build_l2_headers(method, path, body)
            except ValueError as e:
                return False, None, str(e)
            headers.update(auth_headers)

        try:
            async with session.request(
                method=method,
                url=url,
                params=params,
                data=body if json_body else None,
                headers=headers,
            ) as response:
                if response.status == 200:
                    try:
                        data = await response.json()
                    except (aiohttp.ContentTypeError, json.JSONDecodeError, ValueError) as e:
                        return False, None, f"JSON parse error: {e}"
                    return True, data, None
                else:
                    error_text = await response.text()
                    return False, None, f"HTTP {response.status}: {error_text[:500]}"
        except (TimeoutError, aiohttp.ClientError) as e:
            return False, None, str(e)

    # =========================================================================
    # Market Data RPCs
    # =========================================================================

    @staticmethod
    def _market_response_from_gamma(data: dict) -> gateway_pb2.PolymarketMarketResponse:
        outcomes_raw = data.get("outcomes")
        outcome_prices_raw = data.get("outcomePrices")
        token_ids_raw = data.get("clobTokenIds")
        tags_raw = data.get("tags")

        try:
            outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else list(outcomes_raw or [])
        except (TypeError, ValueError):
            outcomes = []
        try:
            outcome_prices = (
                [str(value) for value in json.loads(outcome_prices_raw)]
                if isinstance(outcome_prices_raw, str)
                else [str(value) for value in (outcome_prices_raw or [])]
            )
        except (TypeError, ValueError):
            outcome_prices = []
        try:
            token_ids = (
                [str(value) for value in json.loads(token_ids_raw)]
                if isinstance(token_ids_raw, str)
                else [str(value) for value in (token_ids_raw or [])]
            )
        except (TypeError, ValueError):
            token_ids = []
        try:
            tags = json.loads(tags_raw) if isinstance(tags_raw, str) else list(tags_raw or [])
        except (TypeError, ValueError):
            tags = []

        return gateway_pb2.PolymarketMarketResponse(
            condition_id=data.get("conditionId", ""),
            question_id=data.get("questionID", data.get("questionId", "")),
            tokens=token_ids,
            active=data.get("active", False),
            closed=data.get("closed", False),
            accepting_orders=data.get("acceptingOrders", data.get("active", False)),
            minimum_order_size=str(data.get("orderMinSize", "5")),
            minimum_tick_size=str(data.get("orderPriceMinTickSize", "0.01")),
            success=True,
            market_id=str(data.get("id", "")),
            question=data.get("question", ""),
            slug=data.get("slug", ""),
            outcomes=outcomes,
            outcome_prices=outcome_prices,
            clob_token_ids=token_ids,
            volume=str(data.get("volume", "0")),
            volume_24hr=str(data.get("volume24hr", "0")),
            liquidity=str(data.get("liquidity", "0")),
            end_date=data.get("endDate", ""),
            enable_order_book=data.get("enableOrderBook", False),
            maker_base_fee_bps=str(data.get("makerBaseFee", "0")),
            taker_base_fee_bps=str(data.get("takerBaseFee", "0")),
            best_bid=str(data.get("bestBid", "")),
            best_ask=str(data.get("bestAsk", "")),
            last_trade_price=str(data.get("lastTradePrice", "")),
            event_id=str(data.get("eventId", "")),
            event_slug=data.get("eventSlug", ""),
            group_slug=data.get("groupItemSlug", data.get("group_slug", "")),
            tags=[str(tag) for tag in tags],
            raw_json=json.dumps(data, separators=(",", ":")),
        )

    async def GetMarket(
        self,
        request: gateway_pb2.PolymarketGetMarketRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketMarketResponse:
        """Get market by slug, market ID, or condition ID."""
        if request.slug:
            success, data, error = await self._request(
                "GET",
                GAMMA_BASE_URL,
                "/markets",
                params={"slug": request.slug, "limit": "1"},
            )
            if not success:
                return gateway_pb2.PolymarketMarketResponse(success=False, error=error or "Market not found")
            items: list[dict] = data if isinstance(data, list) else []
            if not items:
                return gateway_pb2.PolymarketMarketResponse(success=False, error="Market not found")
            return self._market_response_from_gamma(items[0])

        success, data, error = await self._request(
            "GET",
            GAMMA_BASE_URL,
            f"/markets/{request.condition_id}",
        )
        if success and isinstance(data, dict):
            return self._market_response_from_gamma(data)

        success, data, error = await self._request(
            "GET",
            GAMMA_BASE_URL,
            "/markets",
            params={"condition_ids": request.condition_id, "limit": "1"},
        )
        if not success:
            return gateway_pb2.PolymarketMarketResponse(success=False, error=error or "Market not found")
        items = data if isinstance(data, list) else []
        if not items:
            return gateway_pb2.PolymarketMarketResponse(success=False, error="Market not found")
        return self._market_response_from_gamma(items[0])

    async def GetMarkets(
        self,
        request: gateway_pb2.PolymarketGetMarketsRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketMarketsResponse:
        """Get list of markets from the Gamma API."""
        if request.next_cursor:
            return gateway_pb2.PolymarketMarketsResponse(
                success=False,
                error="Cursor pagination is not yet supported by GetMarkets",
            )
        params: dict[str, str] = {}
        if request.filters_json:
            try:
                raw_filters = json.loads(request.filters_json)
            except json.JSONDecodeError:
                return gateway_pb2.PolymarketMarketsResponse(success=False, error="Invalid filters_json")
            for key, value in raw_filters.items():
                if value is None:
                    continue
                if isinstance(value, list):
                    params[key] = ",".join(str(item) for item in value)
                elif isinstance(value, bool):
                    params[key] = str(value).lower()
                else:
                    params[key] = str(value)

        success, data, error = await self._request("GET", GAMMA_BASE_URL, "/markets", params=params or None)

        if not success:
            return gateway_pb2.PolymarketMarketsResponse(success=False, error=error or "")
        items: list[dict] = data if isinstance(data, list) else []
        markets = [self._market_response_from_gamma(item) for item in items]

        return gateway_pb2.PolymarketMarketsResponse(
            markets=markets,
            next_cursor="",
            success=True,
        )

    async def GetSimplifiedMarkets(
        self,
        request: gateway_pb2.PolymarketGetSimplifiedMarketsRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketSimplifiedMarketsResponse:
        """Get simplified market list."""
        params = {}
        if request.next_cursor:
            params["next_cursor"] = request.next_cursor

        success, data, error = await self._request("GET", CLOB_BASE_URL, "/simplified-markets", params=params)

        if not success:
            return gateway_pb2.PolymarketSimplifiedMarketsResponse(success=False, error=error or "")

        # Guard against data being None (e.g., JSON null response)
        if data is None:
            items = []
        elif isinstance(data, list):
            items = data
        else:
            items = data.get("data", [])

        markets = []
        for item in items:
            markets.append(
                gateway_pb2.PolymarketSimplifiedMarket(
                    condition_id=item.get("condition_id", ""),
                    tokens=[str(t) for t in item.get("tokens", [])],
                    min_incentive_size=str(item.get("min_incentive_size", "0")),
                    max_incentive_spread=str(item.get("max_incentive_spread", "0")),
                    active=item.get("active", False),
                    closed=item.get("closed", False),
                )
            )

        next_cursor = ""
        if isinstance(data, dict):
            next_cursor = data.get("next_cursor", "")

        return gateway_pb2.PolymarketSimplifiedMarketsResponse(
            markets=markets,
            next_cursor=next_cursor,
            success=True,
        )

    # =========================================================================
    # Order Book RPCs
    # =========================================================================

    async def GetOrderBook(
        self,
        request: gateway_pb2.PolymarketOrderBookRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketOrderBookResponse:
        """Get order book for a token."""
        success, data, error = await self._request(
            "GET",
            CLOB_BASE_URL,
            "/book",
            params={"token_id": request.token_id},
        )

        if not success or not data:
            return gateway_pb2.PolymarketOrderBookResponse(success=False, error=error or "Order book not found")

        bids = [
            gateway_pb2.PolymarketOrderBookLevel(price=str(b.get("price", "0")), size=str(b.get("size", "0")))
            for b in data.get("bids", [])
        ]
        asks = [
            gateway_pb2.PolymarketOrderBookLevel(price=str(a.get("price", "0")), size=str(a.get("size", "0")))
            for a in data.get("asks", [])
        ]

        return gateway_pb2.PolymarketOrderBookResponse(
            market=data.get("market", ""),
            asset_id=data.get("asset_id", ""),
            hash=data.get("hash", ""),
            timestamp=data.get("timestamp", 0),
            bids=bids,
            asks=asks,
            success=True,
        )

    async def GetMidpoint(
        self,
        request: gateway_pb2.PolymarketMidpointRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketMidpointResponse:
        """Get midpoint price for a token."""
        success, data, error = await self._request(
            "GET",
            CLOB_BASE_URL,
            "/midpoint",
            params={"token_id": request.token_id},
        )

        if not success or not data:
            return gateway_pb2.PolymarketMidpointResponse(success=False, error=error or "Midpoint not found")

        return gateway_pb2.PolymarketMidpointResponse(
            midpoint=str(data.get("mid", "0")),
            success=True,
        )

    async def GetPrice(
        self,
        request: gateway_pb2.PolymarketPriceRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketPriceResponse:
        """Get price for a token."""
        success, data, error = await self._request(
            "GET",
            CLOB_BASE_URL,
            "/price",
            params={"token_id": request.token_id, "side": request.side},
        )

        if not success or not data:
            return gateway_pb2.PolymarketPriceResponse(success=False, error=error or "Price not found")

        return gateway_pb2.PolymarketPriceResponse(
            price=str(data.get("price", "0")),
            success=True,
        )

    async def GetSpread(
        self,
        request: gateway_pb2.PolymarketSpreadRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketSpreadResponse:
        """Get spread for a token."""
        success, data, error = await self._request(
            "GET",
            CLOB_BASE_URL,
            "/spread",
            params={"token_id": request.token_id},
        )

        if not success or not data:
            return gateway_pb2.PolymarketSpreadResponse(success=False, error=error or "Spread not found")

        return gateway_pb2.PolymarketSpreadResponse(
            spread=str(data.get("spread", "0")),
            success=True,
        )

    async def GetTickSize(
        self,
        request: gateway_pb2.PolymarketTickSizeRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketTickSizeResponse:
        """Get tick size for a token."""
        success, data, error = await self._request(
            "GET",
            CLOB_BASE_URL,
            "/tick-size",
            params={"token_id": request.token_id},
        )

        if not success or not data:
            return gateway_pb2.PolymarketTickSizeResponse(success=False, error=error or "Tick size not found")

        return gateway_pb2.PolymarketTickSizeResponse(
            tick_size=str(data.get("minimum_tick_size", "0.01")),
            success=True,
        )

    # =========================================================================
    # Order Management RPCs
    # =========================================================================

    async def CreateAndPostOrder(
        self,
        request: gateway_pb2.PolymarketCreateOrderRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketOrderResponse:
        """Create and post a limit order via the gateway-owned signer."""
        if not self._available:
            return gateway_pb2.PolymarketOrderResponse(
                success=False,
                error="Polymarket signer not configured in gateway",
            )

        try:
            price = Decimal(request.price)
            size = Decimal(request.size)
            side = request.side.upper()
            if side not in ("BUY", "SELL"):
                return gateway_pb2.PolymarketOrderResponse(
                    success=False,
                    error=f"Invalid side '{request.side}': must be 'BUY' or 'SELL'",
                )
            client = await self._build_authenticated_client()
            try:
                response = await asyncio.to_thread(
                    client.create_and_post_order,
                    token_id=request.token_id,
                    price=price,
                    size=size,
                    side=side,
                    time_in_force=request.time_in_force or "GTC",
                    expiration=request.expiration if request.expiration > 0 else 0,
                    fee_rate_bps=request.fee_rate_bps or "0",
                )
            finally:
                client.close()
            return gateway_pb2.PolymarketOrderResponse(
                order_id=response.order_id,
                status=response.status.value,
                size_matched=str(response.filled_size),
                price=str(response.price),
                size=str(response.size),
                avg_fill_price=str(response.avg_fill_price) if response.avg_fill_price is not None else "",
                created_at=response.created_at.isoformat() if response.created_at else "",
                success=True,
            )
        except (InvalidOperation, ValueError) as e:
            return gateway_pb2.PolymarketOrderResponse(success=False, error=str(e))
        except Exception as e:
            logger.exception("Failed to create order through gateway Polymarket client")
            return gateway_pb2.PolymarketOrderResponse(success=False, error=str(e))

    async def CreateAndPostMarketOrder(
        self,
        request: gateway_pb2.PolymarketMarketOrderRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketOrderResponse:
        """Create and post a market order."""
        if not self._available:
            return gateway_pb2.PolymarketOrderResponse(success=False, error="Polymarket not configured")

        # Validate side explicitly
        side = request.side.upper() if request.side else ""
        if side not in ("BUY", "SELL"):
            return gateway_pb2.PolymarketOrderResponse(
                success=False, error=f"Invalid side: must be BUY or SELL, got '{request.side}'"
            )

        # Parse and validate amount
        try:
            amount = Decimal(request.amount)
        except InvalidOperation:
            return gateway_pb2.PolymarketOrderResponse(
                success=False, error=f"Invalid amount format: '{request.amount}'"
            )
        if amount <= 0:
            return gateway_pb2.PolymarketOrderResponse(success=False, error="Amount must be positive")

        # For market orders, we need to get the current price first
        price_side = "BUY" if side == "BUY" else "SELL"

        price_success, price_data, price_error = await self._request(
            "GET",
            CLOB_BASE_URL,
            "/price",
            params={"token_id": request.token_id, "side": price_side},
        )

        if not price_success or not price_data:
            return gateway_pb2.PolymarketOrderResponse(success=False, error=price_error or "Could not get price")

        # Parse price from API response
        try:
            price = Decimal(str(price_data.get("price", "0")))
        except InvalidOperation:
            return gateway_pb2.PolymarketOrderResponse(success=False, error="Invalid price format from API")

        # Validate price is positive before using it for calculations
        if price <= 0:
            return gateway_pb2.PolymarketOrderResponse(success=False, error="Invalid price: price must be positive")

        # Validate worst_price if provided
        if request.worst_price:
            try:
                worst = Decimal(request.worst_price)
            except InvalidOperation:
                return gateway_pb2.PolymarketOrderResponse(
                    success=False, error=f"Invalid worst_price format: '{request.worst_price}'"
                )
            if side == "BUY" and price > worst:
                return gateway_pb2.PolymarketOrderResponse(success=False, error="Price exceeds worst price")
            if side == "SELL" and price < worst:
                return gateway_pb2.PolymarketOrderResponse(success=False, error="Price below worst price")

        # For market orders, request.amount semantics differ by side:
        # - BUY: amount is in USDC, need to convert to token size
        # - SELL: amount is in tokens (size)
        if side == "BUY":
            # Convert USDC amount to token size by dividing by price
            # Round down to avoid overspending
            token_size = (amount / price).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
            size_str = str(token_size)
        else:
            # SELL: amount is already in tokens (use parsed Decimal for consistency)
            size_str = str(amount)

        # Create the order with the current market price
        create_request = gateway_pb2.PolymarketCreateOrderRequest(
            token_id=request.token_id,
            price=str(price),
            size=size_str,
            side=side,
            fee_rate_bps=request.fee_rate_bps,
            expiration=request.expiration,
            nonce=request.nonce,
            time_in_force="FOK",  # Market orders use Fill-or-Kill
        )

        return await self.CreateAndPostOrder(create_request, context)

    async def CancelOrder(
        self,
        request: gateway_pb2.PolymarketCancelOrderRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketCancelResponse:
        """Cancel a single order."""
        try:
            client = await self._build_authenticated_client()
            try:
                await asyncio.to_thread(client.cancel_order, request.order_id)
            finally:
                client.close()
            return gateway_pb2.PolymarketCancelResponse(canceled=[request.order_id], not_canceled=[], success=True)
        except Exception as e:
            return gateway_pb2.PolymarketCancelResponse(
                canceled=[],
                not_canceled=[request.order_id],
                success=False,
                error=str(e),
            )

    async def CancelOrders(
        self,
        request: gateway_pb2.PolymarketCancelOrdersRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketCancelResponse:
        """Cancel multiple orders."""
        canceled: list[str] = []
        not_canceled: list[str] = []
        client = await self._build_authenticated_client()
        try:
            for order_id in request.order_ids:
                try:
                    await asyncio.to_thread(client.cancel_order, order_id)
                    canceled.append(order_id)
                except Exception:
                    not_canceled.append(order_id)
        finally:
            client.close()
        return gateway_pb2.PolymarketCancelResponse(
            canceled=canceled, not_canceled=not_canceled, success=not not_canceled
        )

    async def CancelAll(
        self,
        request: gateway_pb2.PolymarketCancelAllRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketCancelResponse:
        """Cancel all orders, optionally scoped to market_id and/or asset_id."""
        client = await self._build_authenticated_client()
        try:
            open_orders = await asyncio.to_thread(
                client.get_open_orders, OrderFilters(market=request.market_id or None)
            )
            # Apply asset_id filter client-side (OpenOrder.market stores the token/asset id).
            if request.asset_id:
                open_orders = [o for o in open_orders if o.market == request.asset_id]
            order_ids = [order.order_id for order in open_orders]
            if order_ids:
                await asyncio.to_thread(client.cancel_orders, order_ids)
            return gateway_pb2.PolymarketCancelResponse(canceled=order_ids, not_canceled=[], success=True)
        except Exception as e:
            return gateway_pb2.PolymarketCancelResponse(success=False, error=str(e))
        finally:
            client.close()

    # =========================================================================
    # Position and Trade RPCs
    # =========================================================================

    async def GetPositions(
        self,
        _request: gateway_pb2.PolymarketGetPositionsRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketPositionsResponse:
        """Get positions for the wallet."""
        try:
            client = await self._build_authenticated_client()
            try:
                data = await asyncio.to_thread(client.get_positions)
            finally:
                client.close()
        except Exception as e:
            return gateway_pb2.PolymarketPositionsResponse(success=False, error=str(e))

        positions = [
            gateway_pb2.PolymarketPosition(
                asset=p.token_id,
                condition_id=p.condition_id,
                size=str(p.size),
                avg_price=str(p.avg_price),
                realized_pnl=str(p.realized_pnl),
                cur_price=str(p.current_price),
                market_id=p.market_id,
                token_id=p.token_id,
                outcome=p.outcome,
                market_question=p.market_question,
            )
            for p in data
        ]
        return gateway_pb2.PolymarketPositionsResponse(positions=positions, success=True)

    async def GetOpenOrders(
        self,
        request: gateway_pb2.PolymarketGetOpenOrdersRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketOpenOrdersResponse:
        """Get open orders."""
        try:
            client = await self._build_authenticated_client()
            try:
                data = await asyncio.to_thread(
                    client.get_open_orders,
                    OrderFilters(market=request.market_id or None),
                )
            finally:
                client.close()
        except Exception as e:
            return gateway_pb2.PolymarketOpenOrdersResponse(success=False, error=str(e))

        orders = [
            gateway_pb2.PolymarketOpenOrder(
                order_id=o.order_id,
                market=o.market,
                side=o.side,
                price=str(o.price),
                original_size=str(o.size),
                size_matched=str(o.filled_size),
                expiration=str(o.expiration or ""),
                created_at=o.created_at.isoformat() if o.created_at else "",
            )
            for o in data
        ]
        return gateway_pb2.PolymarketOpenOrdersResponse(orders=orders, success=True)

    async def GetTradesHistory(
        self,
        request: gateway_pb2.PolymarketGetTradesRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketTradesResponse:
        """Get trade history."""
        params = {}
        if request.market_id:
            params["market"] = request.market_id
        if request.asset_id:
            params["asset_id"] = request.asset_id
        if request.limit > 0:
            params["limit"] = str(request.limit)
        if request.before:
            params["before"] = request.before
        if request.after:
            params["after"] = request.after

        success, data, error = await self._request(
            "GET",
            CLOB_BASE_URL,
            "/trades",
            params=params if params else None,
            authenticated=True,
        )

        if not success:
            return gateway_pb2.PolymarketTradesResponse(success=False, error=error or "")

        trades = []
        trade_list = data if isinstance(data, list) else data.get("data", []) if data else []
        for t in trade_list:
            trades.append(
                gateway_pb2.PolymarketTrade(
                    trade_id=t.get("id", t.get("trade_id", "")),
                    market=t.get("market", ""),
                    asset_id=t.get("asset_id", ""),
                    side=t.get("side", ""),
                    price=str(t.get("price", "0")),
                    size=str(t.get("size", "0")),
                    fee_rate_bps=str(t.get("fee_rate_bps", "0")),
                    status=t.get("status", ""),
                    match_time=t.get("match_time", ""),
                    transaction_hash=t.get("transaction_hash", ""),
                    bucket_index=str(t.get("bucket_index", "")),
                )
            )

        next_cursor = ""
        if isinstance(data, dict):
            next_cursor = data.get("next_cursor", "")

        return gateway_pb2.PolymarketTradesResponse(trades=trades, next_cursor=next_cursor, success=True)

    async def GetOrder(
        self,
        request: gateway_pb2.PolymarketGetOrderRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketOrderInfoResponse:
        """Get a specific order by ID."""
        try:
            client = await self._build_authenticated_client()
            try:
                data = await asyncio.to_thread(client.get_order, request.order_id)
            finally:
                client.close()
        except Exception as e:
            return gateway_pb2.PolymarketOrderInfoResponse(success=False, error=str(e))
        if data is None:
            return gateway_pb2.PolymarketOrderInfoResponse(success=False, error="Order not found")
        return gateway_pb2.PolymarketOrderInfoResponse(
            order_id=data.order_id,
            market=data.market,
            side=data.side,
            price=str(data.price),
            original_size=str(data.size),
            size_matched=str(data.filled_size),
            expiration=str(data.expiration or ""),
            created_at=data.created_at.isoformat() if data.created_at else "",
            success=True,
        )

    # =========================================================================
    # Balance RPCs
    # =========================================================================

    async def GetBalanceAllowance(
        self,
        request: gateway_pb2.PolymarketBalanceAllowanceRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketBalanceAllowanceResponse:
        """Get balance and allowance."""
        params = {"asset_type": request.asset_type or "COLLATERAL"}
        if request.token_id:
            params["token_id"] = request.token_id

        success, data, error = await self._request(
            "GET",
            CLOB_BASE_URL,
            "/balance-allowance",
            params=params,
            authenticated=True,
        )

        if not success or not data:
            return gateway_pb2.PolymarketBalanceAllowanceResponse(success=False, error=error or "Could not get balance")

        return gateway_pb2.PolymarketBalanceAllowanceResponse(
            balance=str(data.get("balance", "0")),
            allowance=str(data.get("allowance", "0")),
            success=True,
        )
