"""EnsoService implementation - Enso Finance routing API proxy.

This service provides secure access to the Enso Finance API:
- Route finding across multiple DEXs
- Quote generation for swaps
- Bundle transactions for complex DeFi operations
- Token approval transactions

API key is held in gateway, keeping credentials secure.
"""

import json
import logging
import os
from typing import Any

import aiohttp
import grpc
from pydantic import BaseModel, Field

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc

logger = logging.getLogger(__name__)


# =============================================================================
# Pydantic Models for API Response Validation
# =============================================================================


class EnsoTransactionData(BaseModel):
    """Transaction data from Enso API responses."""

    to: str = ""
    data: str = ""
    value: int | str = 0
    gas: int | str = 0


class EnsoRouteResponse(BaseModel):
    """Validated response for Enso route API."""

    tx: EnsoTransactionData = Field(default_factory=EnsoTransactionData)
    amountOut: list[int | str] | int | str = Field(default_factory=list)
    priceImpact: int | None = None
    gas: int | str = 0
    bridgeFee: int | str = 0
    estimatedTime: int | None = None
    route: list[Any] = Field(default_factory=list)


class EnsoQuoteResponse(BaseModel):
    """Validated response for Enso quote API."""

    amountOut: list[int | str] | int | str = Field(default_factory=list)
    priceImpact: int | None = None
    gas: int | str = 0


class EnsoApprovalResponse(BaseModel):
    """Validated response for Enso approval API."""

    tx: EnsoTransactionData = Field(default_factory=EnsoTransactionData)
    # Some responses return tx data at top level instead of nested
    to: str = ""
    data: str = ""
    gas: int | str = 0


class EnsoBundleResponse(BaseModel):
    """Validated response for Enso bundle API."""

    tx: EnsoTransactionData = Field(default_factory=EnsoTransactionData)
    # Some responses return tx data at top level
    to: str = ""
    data: str = ""
    value: int | str = 0
    gas: int | str = 0


def _normalize_amount_out(value: list[int | str] | int | str | None) -> str:
    """Normalize Enso amountOut values to a consistent string output."""
    if value is None:
        return "0"
    if isinstance(value, list):
        if not value:
            return "0"
        return str(value[0])
    return str(value)


# =============================================================================
# Constants
# =============================================================================

ENSO_BASE_URL = "https://api.enso.finance"

# Chain ID mapping
CHAIN_MAPPING = {
    "ethereum": 1,
    "optimism": 10,
    "bsc": 56,
    "gnosis": 100,
    "polygon": 137,
    "zksync": 324,
    "base": 8453,
    "arbitrum": 42161,
    "avalanche": 43114,
    "sonic": 146,
    "linea": 59144,
    "sepolia": 11155111,
}


class EnsoServiceServicer(gateway_pb2_grpc.EnsoServiceServicer):
    """Implements EnsoService gRPC interface.

    Provides secure proxy to Enso Finance API with API key held in gateway.
    """

    def __init__(self, settings: GatewaySettings):
        """Initialize EnsoService.

        Args:
            settings: Gateway settings (contains Enso API key)
        """
        self.settings = settings
        self._http_session: aiohttp.ClientSession | None = None

        # Load API key from settings or environment
        self._api_key = getattr(settings, "enso_api_key", None) or os.environ.get("ENSO_API_KEY")

        self._available = bool(self._api_key)

        logger.info("EnsoService initialized: available=%s", self._available)

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

    def _get_chain_id(self, chain: str) -> int | None:
        """Get chain ID from chain name."""
        return CHAIN_MAPPING.get(chain.lower())

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: dict | None = None,
        json_body: list | dict | None = None,
    ) -> tuple[bool, dict | list | None, str | None]:
        """Make HTTP request to Enso API.

        Returns:
            Tuple of (success, data, error)
        """
        if not self._available:
            return False, None, "Enso API key not configured"

        session = await self._get_session()
        url = f"{ENSO_BASE_URL}{endpoint}"

        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        try:
            async with session.request(
                method=method,
                url=url,
                params=params,
                json=json_body,
                headers=headers,
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return True, data, None
                else:
                    error_text = await response.text()
                    return False, None, f"HTTP {response.status}: {error_text[:500]}"
        except (TimeoutError, aiohttp.ClientError) as e:
            logger.warning("Enso API request failed: %s %s - %s", method, url, e)
            return False, None, str(e)
        except Exception:
            logger.exception("Unexpected Enso API error: %s %s", method, url)
            return False, None, "Unexpected Enso API error"

    # =========================================================================
    # Route RPC
    # =========================================================================

    async def GetRoute(
        self,
        request: gateway_pb2.EnsoRouteRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.EnsoRouteResponse:
        """Get swap route across multiple DEXs."""
        chain_id = self._get_chain_id(request.chain)
        if chain_id is None:
            return gateway_pb2.EnsoRouteResponse(
                success=False,
                error=f"Unsupported chain: {request.chain}",
            )

        params = {
            "fromAddress": request.from_address,
            "tokenIn": [request.token_in],
            "tokenOut": [request.token_out],
            "amountIn": [request.amount_in],
            "chainId": chain_id,
            "slippage": str(request.slippage_bps) if request.slippage_bps >= 0 else "50",
            "disableRFQs": "false",
        }

        if request.routing_strategy:
            params["routingStrategy"] = request.routing_strategy

        if request.receiver:
            params["receiver"] = request.receiver

        # Cross-chain parameters
        is_cross_chain = request.destination_chain_id > 0 and request.destination_chain_id != chain_id
        if is_cross_chain:
            params["destinationChainId"] = request.destination_chain_id
            params["receiver"] = request.receiver or request.from_address
            params["refundReceiver"] = request.refund_receiver or request.from_address

        success, data, error = await self._request("GET", "/api/v1/shortcuts/route", params=params)

        if not success or not data:
            return gateway_pb2.EnsoRouteResponse(success=False, error=error or "Failed to get route")

        # Validate response with Pydantic
        try:
            validated = EnsoRouteResponse.model_validate(data)
        except Exception as e:
            logger.warning("Invalid Enso route response: %s", e)
            return gateway_pb2.EnsoRouteResponse(success=False, error=f"Invalid API response: {e}")

        amount_out = _normalize_amount_out(validated.amountOut)

        price_impact = validated.priceImpact or 0

        # Check price impact threshold
        if request.max_price_impact_bps > 0 and price_impact > request.max_price_impact_bps:
            return gateway_pb2.EnsoRouteResponse(
                success=False,
                error=f"Price impact {price_impact}bp exceeds threshold {request.max_price_impact_bps}bp",
            )

        return gateway_pb2.EnsoRouteResponse(
            success=True,
            to=validated.tx.to,
            data=validated.tx.data,
            value=str(validated.tx.value),
            gas=str(validated.tx.gas),
            amount_out=amount_out,
            price_impact=price_impact,
            gas_estimate=str(validated.gas),
            bridge_fee=str(validated.bridgeFee),
            estimated_time=validated.estimatedTime or 0,
            is_cross_chain=is_cross_chain,
            route_json=json.dumps(validated.route),
        )

    # =========================================================================
    # Quote RPC
    # =========================================================================

    async def GetQuote(
        self,
        request: gateway_pb2.EnsoQuoteRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.EnsoQuoteResponse:
        """Get quote for swap without building transaction."""
        chain_id = self._get_chain_id(request.chain)
        if chain_id is None:
            return gateway_pb2.EnsoQuoteResponse(
                success=False,
                error=f"Unsupported chain: {request.chain}",
            )

        params = {
            "fromAddress": request.from_address,
            "tokenIn": [request.token_in],
            "tokenOut": [request.token_out],
            "amountIn": [request.amount_in],
            "chainId": chain_id,
        }

        if request.routing_strategy:
            params["routingStrategy"] = request.routing_strategy

        if request.destination_chain_id > 0:
            params["destinationChainId"] = request.destination_chain_id

        success, data, error = await self._request("GET", "/api/v1/shortcuts/quote", params=params)

        if not success or not data:
            return gateway_pb2.EnsoQuoteResponse(success=False, error=error or "Failed to get quote")

        # Validate response with Pydantic
        try:
            validated = EnsoQuoteResponse.model_validate(data)
        except Exception as e:
            logger.warning("Invalid Enso quote response: %s", e)
            return gateway_pb2.EnsoQuoteResponse(success=False, error=f"Invalid API response: {e}")

        amount_out = _normalize_amount_out(validated.amountOut)

        return gateway_pb2.EnsoQuoteResponse(
            success=True,
            amount_out=amount_out,
            price_impact=validated.priceImpact or 0,
            gas_estimate=str(validated.gas),
        )

    # =========================================================================
    # Approval RPC
    # =========================================================================

    async def GetApproval(
        self,
        request: gateway_pb2.EnsoApprovalRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.EnsoApprovalResponse:
        """Get approval transaction data."""
        chain_id = self._get_chain_id(request.chain)
        if chain_id is None:
            return gateway_pb2.EnsoApprovalResponse(
                success=False,
                error=f"Unsupported chain: {request.chain}",
            )

        # Use max uint256 for unlimited if amount not specified
        amount = request.amount if request.amount else str(2**256 - 1)

        params = {
            "chainId": chain_id,
            "fromAddress": request.from_address,
            "tokenAddress": request.token_address,
            "amount": amount,
        }

        if request.routing_strategy:
            params["routingStrategy"] = request.routing_strategy

        success, data, error = await self._request("GET", "/api/v1/wallet/approve", params=params)

        if not success or not data:
            return gateway_pb2.EnsoApprovalResponse(success=False, error=error or "Failed to get approval")

        # Validate response with Pydantic
        try:
            validated = EnsoApprovalResponse.model_validate(data)
        except Exception as e:
            logger.warning("Invalid Enso approval response: %s", e)
            return gateway_pb2.EnsoApprovalResponse(success=False, error=f"Invalid API response: {e}")

        # Extract tx data from nested tx or top-level fields
        to_addr = validated.tx.to or validated.to
        tx_data = validated.tx.data or validated.data
        gas = validated.tx.gas or validated.gas

        return gateway_pb2.EnsoApprovalResponse(
            success=True,
            to=to_addr,
            data=tx_data,
            gas=str(gas),
        )

    # =========================================================================
    # Bundle RPC
    # =========================================================================

    async def GetBundle(
        self,
        request: gateway_pb2.EnsoBundleRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.EnsoBundleResponse:
        """Get bundle transaction for multiple DeFi operations."""
        chain_id = self._get_chain_id(request.chain)
        if chain_id is None:
            return gateway_pb2.EnsoBundleResponse(
                success=False,
                error=f"Unsupported chain: {request.chain}",
            )

        params = {
            "chainId": chain_id,
            "fromAddress": request.from_address,
        }

        if request.routing_strategy:
            params["routingStrategy"] = request.routing_strategy

        if request.skip_quote:
            params["skipQuote"] = True

        # Convert actions to API format
        actions = []
        for action in request.actions:
            action_data = {
                "protocol": action.protocol,
                "action": action.action,
                "args": dict(action.args),
            }
            actions.append(action_data)

        success, data, error = await self._request(
            "POST",
            "/api/v1/shortcuts/bundle",
            params=params,
            json_body=actions,
        )

        if not success or not data:
            return gateway_pb2.EnsoBundleResponse(success=False, error=error or "Failed to get bundle")

        # Validate response with Pydantic
        try:
            validated = EnsoBundleResponse.model_validate(data)
        except Exception as e:
            logger.warning("Invalid Enso bundle response: %s", e)
            return gateway_pb2.EnsoBundleResponse(success=False, error=f"Invalid API response: {e}")

        # Extract tx data from nested tx or top-level fields
        to_addr = validated.tx.to or validated.to
        tx_data = validated.tx.data or validated.data
        value = validated.tx.value or validated.value
        gas = validated.tx.gas or validated.gas

        return gateway_pb2.EnsoBundleResponse(
            success=True,
            to=to_addr,
            data=tx_data,
            value=str(value),
            gas=str(gas),
            bundle_json=json.dumps(data),
        )
