"""PolymarketService implementation - Polymarket CLOB API proxy.

This service provides secure access to Polymarket's CLOB API:
- L1 Authentication (EIP-712) for credential creation
- L2 Authentication (HMAC-SHA256) for trading operations
- All credentials held in gateway, keeping secrets secure

The service proxies calls to:
- CLOB API: Order management, orderbooks, prices
- Gamma API: Market metadata
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from urllib.parse import urlencode

import aiohttp
import grpc
from eth_account import Account
from eth_account.messages import encode_typed_data

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

CLOB_BASE_URL = "https://clob.polymarket.com"
GAMMA_BASE_URL = "https://gamma-api.polymarket.com"

# EIP-712 domain and types for L1 auth
CLOB_AUTH_DOMAIN = {
    "name": "ClobAuthDomain",
    "version": "1",
    "chainId": 137,
}

CLOB_AUTH_TYPES = {
    "ClobAuth": [
        {"name": "address", "type": "address"},
        {"name": "timestamp", "type": "string"},
        {"name": "nonce", "type": "uint256"},
        {"name": "message", "type": "string"},
    ],
}

CLOB_AUTH_MESSAGE = "This message attests that I control the given wallet"

# Contract addresses (Polygon Mainnet)
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
POLYGON_CHAIN_ID = 137

# Order signing types
ORDER_TYPES = {
    "Order": [
        {"name": "salt", "type": "uint256"},
        {"name": "maker", "type": "address"},
        {"name": "signer", "type": "address"},
        {"name": "taker", "type": "address"},
        {"name": "tokenId", "type": "uint256"},
        {"name": "makerAmount", "type": "uint256"},
        {"name": "takerAmount", "type": "uint256"},
        {"name": "expiration", "type": "uint256"},
        {"name": "nonce", "type": "uint256"},
        {"name": "feeRateBps", "type": "uint256"},
        {"name": "side", "type": "uint8"},
        {"name": "signatureType", "type": "uint8"},
    ],
}


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

        # Load credentials from settings or environment
        self._private_key = getattr(settings, "polymarket_private_key", None) or os.environ.get(
            "POLYMARKET_PRIVATE_KEY"
        )
        self._wallet_address = getattr(settings, "polymarket_wallet_address", None) or os.environ.get(
            "POLYMARKET_WALLET_ADDRESS"
        )
        self._api_key = getattr(settings, "polymarket_api_key", None) or os.environ.get("POLYMARKET_API_KEY")
        self._api_secret = getattr(settings, "polymarket_secret", None) or os.environ.get("POLYMARKET_SECRET")
        self._api_passphrase = getattr(settings, "polymarket_passphrase", None) or os.environ.get(
            "POLYMARKET_PASSPHRASE"
        )

        # Derive wallet address from private key if not provided
        if self._private_key and not self._wallet_address:
            account = Account.from_key(self._private_key)
            self._wallet_address = account.address

        # Check if service is available
        self._available = bool(self._private_key and self._wallet_address)
        self._credentials_available = bool(self._api_key and self._api_secret and self._api_passphrase)

        logger.debug(
            "PolymarketService initialized: available=%s, credentials=%s",
            self._available,
            self._credentials_available,
        )

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30.0))
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
                    logger.error("Failed to create credentials: %s", await response.text())
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

    async def GetMarket(
        self,
        request: gateway_pb2.PolymarketGetMarketRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketMarketResponse:
        """Get market by condition ID."""
        success, data, error = await self._request(
            "GET",
            CLOB_BASE_URL,
            f"/markets/{request.condition_id}",
        )

        if not success or not data:
            return gateway_pb2.PolymarketMarketResponse(success=False, error=error or "Market not found")

        return gateway_pb2.PolymarketMarketResponse(
            condition_id=data.get("condition_id", ""),
            question_id=data.get("question_id", ""),
            tokens=[str(t) for t in data.get("tokens", [])],
            rewards_daily_rate=str(data.get("rewards", {}).get("daily_rate", "0")),
            rewards_min_size=str(data.get("rewards", {}).get("min_size", "0")),
            rewards_max_spread=str(data.get("rewards", {}).get("max_spread", "0")),
            active=data.get("active", False),
            closed=data.get("closed", False),
            accepting_orders=data.get("accepting_orders", False),
            accepting_order_timestamp=data.get("accepting_order_timestamp", ""),
            minimum_order_size=str(data.get("minimum_order_size", "0")),
            minimum_tick_size=str(data.get("minimum_tick_size", "0.01")),
            success=True,
        )

    async def GetMarkets(
        self,
        request: gateway_pb2.PolymarketGetMarketsRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketMarketsResponse:
        """Get list of markets."""
        params = {}
        if request.next_cursor:
            params["next_cursor"] = request.next_cursor

        success, data, error = await self._request("GET", CLOB_BASE_URL, "/markets", params=params)

        if not success:
            return gateway_pb2.PolymarketMarketsResponse(success=False, error=error or "")

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
                gateway_pb2.PolymarketMarketResponse(
                    condition_id=item.get("condition_id", ""),
                    question_id=item.get("question_id", ""),
                    tokens=[str(t) for t in item.get("tokens", [])],
                    active=item.get("active", False),
                    closed=item.get("closed", False),
                    accepting_orders=item.get("accepting_orders", False),
                    minimum_order_size=str(item.get("minimum_order_size", "0")),
                    minimum_tick_size=str(item.get("minimum_tick_size", "0.01")),
                    success=True,
                )
            )

        next_cursor = ""
        if isinstance(data, dict):
            next_cursor = data.get("next_cursor", "")

        return gateway_pb2.PolymarketMarketsResponse(
            markets=markets,
            next_cursor=next_cursor,
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

    def _generate_salt(self) -> int:
        """Generate a random salt for order uniqueness."""
        return int(secrets.token_hex(32), 16)

    def _to_token_units(self, amount: Decimal) -> int:
        """Convert decimal amount to token units (6 decimals)."""
        scaled = amount * Decimal("1000000")
        return int(scaled.quantize(Decimal("1"), rounding=ROUND_DOWN))

    def _sign_order(self, order_data: dict, is_neg_risk: bool = False) -> str:
        """Sign an order using EIP-712."""
        exchange = NEG_RISK_EXCHANGE if is_neg_risk else CTF_EXCHANGE

        domain = {
            "name": "Polymarket CTF Exchange",
            "version": "1",
            "chainId": POLYGON_CHAIN_ID,
            "verifyingContract": exchange,
        }

        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                **ORDER_TYPES,
            },
            "primaryType": "Order",
            "domain": domain,
            "message": order_data,
        }

        signable = encode_typed_data(full_message=typed_data)
        signed = Account.sign_message(signable, self._private_key)
        return signed.signature.hex()

    async def CreateAndPostOrder(
        self,
        request: gateway_pb2.PolymarketCreateOrderRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketOrderResponse:
        """Create and post a limit order."""
        if not self._available:
            return gateway_pb2.PolymarketOrderResponse(success=False, error="Polymarket not configured")

        # Validate side
        side_upper = request.side.upper() if request.side else ""
        if side_upper not in ("BUY", "SELL"):
            return gateway_pb2.PolymarketOrderResponse(
                success=False, error=f"Invalid side: must be BUY or SELL, got '{request.side}'"
            )

        # Parse and validate price
        try:
            price = Decimal(request.price)
        except InvalidOperation:
            return gateway_pb2.PolymarketOrderResponse(success=False, error=f"Invalid price format: '{request.price}'")
        if price <= 0:
            return gateway_pb2.PolymarketOrderResponse(success=False, error="Price must be positive")

        # Parse and validate size
        try:
            size = Decimal(request.size)
        except InvalidOperation:
            return gateway_pb2.PolymarketOrderResponse(success=False, error=f"Invalid size format: '{request.size}'")
        if size <= 0:
            return gateway_pb2.PolymarketOrderResponse(success=False, error="Size must be positive")

        try:
            side = 0 if side_upper == "BUY" else 1

            # Calculate amounts per Polymarket CLOB API semantics:
            # - makerAmount = what the maker (order creator) gives/spends
            # - takerAmount = what the maker receives
            if side == 0:  # BUY: maker gives USDC, receives tokens
                maker_amount = self._to_token_units(size * price)  # USDC to spend
                taker_amount = self._to_token_units(size)  # tokens to receive
            else:  # SELL: maker gives tokens, receives USDC
                maker_amount = self._to_token_units(size)  # tokens to sell
                taker_amount = self._to_token_units(size * price)  # USDC to receive

            # Build order
            salt = self._generate_salt()
            nonce = int(request.nonce) if request.nonce else 0
            expiration = request.expiration if request.expiration > 0 else 0
            fee_rate_bps = int(request.fee_rate_bps) if request.fee_rate_bps else 0

            order_data = {
                "salt": salt,
                "maker": self._wallet_address,
                "signer": self._wallet_address,
                "taker": "0x0000000000000000000000000000000000000000",
                "tokenId": int(request.token_id),
                "makerAmount": maker_amount,
                "takerAmount": taker_amount,
                "expiration": expiration,
                "nonce": nonce,
                "feeRateBps": fee_rate_bps,
                "side": side,
                "signatureType": 0,  # EOA
            }

            signature = self._sign_order(order_data)

            # Post order
            order_payload = {
                "order": {
                    "salt": str(salt),
                    "maker": self._wallet_address,
                    "signer": self._wallet_address,
                    "taker": "0x0000000000000000000000000000000000000000",
                    "tokenId": request.token_id,
                    "makerAmount": str(maker_amount),
                    "takerAmount": str(taker_amount),
                    "expiration": str(expiration),
                    "nonce": str(nonce),
                    "feeRateBps": str(fee_rate_bps),
                    "side": "BUY" if side == 0 else "SELL",
                    "signatureType": 0,
                    "signature": signature,
                },
                "owner": self._wallet_address,
                "orderType": request.time_in_force or "GTC",
            }

            success, data, error = await self._request(
                "POST",
                CLOB_BASE_URL,
                "/order",
                json_body=order_payload,
                authenticated=True,
            )

            if not success:
                return gateway_pb2.PolymarketOrderResponse(success=False, error=error or "")

            if data is None:
                data = {}
            return gateway_pb2.PolymarketOrderResponse(
                order_id=data.get("orderID", data.get("orderId", "")),
                status=data.get("status", ""),
                size_matched=str(data.get("sizeMatched", "0")),
                transact_time=data.get("transactTime", ""),
                success=True,
            )

        except (TimeoutError, aiohttp.ClientError, ValueError, InvalidOperation) as e:
            logger.exception("Failed to create order")
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
        success, _data, error = await self._request(
            "DELETE",
            CLOB_BASE_URL,
            "/order",
            params={"id": request.order_id},
            authenticated=True,
        )

        if not success:
            return gateway_pb2.PolymarketCancelResponse(
                canceled=[],
                not_canceled=[request.order_id],
                success=False,
                error=error or "",
            )

        return gateway_pb2.PolymarketCancelResponse(
            canceled=[request.order_id],
            not_canceled=[],
            success=True,
        )

    async def CancelOrders(
        self,
        request: gateway_pb2.PolymarketCancelOrdersRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketCancelResponse:
        """Cancel multiple orders."""
        canceled = []
        not_canceled = []

        for order_id in request.order_ids:
            success, _, _ = await self._request(
                "DELETE",
                CLOB_BASE_URL,
                "/order",
                params={"id": order_id},
                authenticated=True,
            )
            if success:
                canceled.append(order_id)
            else:
                not_canceled.append(order_id)

        return gateway_pb2.PolymarketCancelResponse(
            canceled=canceled,
            not_canceled=not_canceled,
            success=len(not_canceled) == 0,
        )

    async def CancelAll(
        self,
        request: gateway_pb2.PolymarketCancelAllRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketCancelResponse:
        """Cancel all orders."""
        params = {}
        if request.market_id:
            params["market"] = request.market_id
        if request.asset_id:
            params["asset_id"] = request.asset_id

        success, data, error = await self._request(
            "DELETE",
            CLOB_BASE_URL,
            "/cancel-all",
            params=params if params else None,
            authenticated=True,
        )

        if not success:
            return gateway_pb2.PolymarketCancelResponse(success=False, error=error or "")

        return gateway_pb2.PolymarketCancelResponse(
            canceled=data.get("canceled", []) if data else [],
            not_canceled=data.get("not_canceled", []) if data else [],
            success=True,
        )

    # =========================================================================
    # Position and Trade RPCs
    # =========================================================================

    async def GetPositions(
        self,
        _request: gateway_pb2.PolymarketGetPositionsRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketPositionsResponse:
        """Get positions for the wallet."""
        success, data, error = await self._request(
            "GET",
            CLOB_BASE_URL,
            "/positions",
            authenticated=True,
        )

        if not success:
            return gateway_pb2.PolymarketPositionsResponse(success=False, error=error or "")

        positions = []
        items_list: list[dict] = (
            data if isinstance(data, list) else data.get("data", []) if isinstance(data, dict) else []
        )
        for p in items_list:
            positions.append(
                gateway_pb2.PolymarketPosition(
                    asset=p.get("asset", ""),
                    condition_id=p.get("conditionId", ""),
                    size=str(p.get("size", "0")),
                    avg_price=str(p.get("avgPrice", "0")),
                    realized_pnl=str(p.get("realizedPnl", "0")),
                    cur_price=str(p.get("curPrice", "0")),
                )
            )

        return gateway_pb2.PolymarketPositionsResponse(positions=positions, success=True)

    async def GetOpenOrders(
        self,
        request: gateway_pb2.PolymarketGetOpenOrdersRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketOpenOrdersResponse:
        """Get open orders."""
        params = {}
        if request.market_id:
            params["market"] = request.market_id
        if request.asset_id:
            params["asset_id"] = request.asset_id

        success, data, error = await self._request(
            "GET",
            CLOB_BASE_URL,
            "/orders",
            params=params if params else None,
            authenticated=True,
        )

        if not success:
            return gateway_pb2.PolymarketOpenOrdersResponse(success=False, error=error or "")

        orders = []
        orders_list: list[dict] = (
            data if isinstance(data, list) else data.get("data", []) if isinstance(data, dict) else []
        )
        for o in orders_list:
            orders.append(
                gateway_pb2.PolymarketOpenOrder(
                    order_id=o.get("id", o.get("order_id", "")),
                    market=o.get("market", ""),
                    asset_id=o.get("asset_id", ""),
                    side=o.get("side", ""),
                    price=str(o.get("price", "0")),
                    original_size=str(o.get("original_size", "0")),
                    size_matched=str(o.get("size_matched", "0")),
                    outcome=o.get("outcome", ""),
                    status=o.get("status", ""),
                    expiration=str(o.get("expiration", "")),
                    created_at=o.get("created_at", ""),
                )
            )

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
        success, data, error = await self._request(
            "GET",
            CLOB_BASE_URL,
            f"/order/{request.order_id}",
            authenticated=True,
        )

        if not success or not data:
            return gateway_pb2.PolymarketOrderInfoResponse(success=False, error=error or "Order not found")

        associate_trades = data.get("associate_trades", [])
        trades_json = json.dumps(associate_trades) if associate_trades else ""

        return gateway_pb2.PolymarketOrderInfoResponse(
            order_id=data.get("id", data.get("order_id", "")),
            market=data.get("market", ""),
            asset_id=data.get("asset_id", ""),
            side=data.get("side", ""),
            price=str(data.get("price", "0")),
            original_size=str(data.get("original_size", "0")),
            size_matched=str(data.get("size_matched", "0")),
            status=data.get("status", ""),
            outcome=data.get("outcome", ""),
            owner=data.get("owner", ""),
            expiration=str(data.get("expiration", "")),
            created_at=data.get("created_at", ""),
            associate_trades_json=trades_json,
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
