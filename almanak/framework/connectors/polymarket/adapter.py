"""Polymarket Adapter for Intent Compilation.

This adapter compiles prediction market intents (PredictionBuyIntent,
PredictionSellIntent, PredictionRedeemIntent) into executable ActionBundles.

The adapter bridges high-level trading intents to the CLOB API and CTF SDK:
- Buy/Sell intents -> CLOB orders (signed and submitted off-chain)
- Redeem intents -> CTF transactions (on-chain redemption)

Example:
    from almanak.framework.connectors.polymarket import PolymarketAdapter, PolymarketConfig
    from almanak.framework.intents import Intent

    config = PolymarketConfig.from_env()
    adapter = PolymarketAdapter(config)

    # Compile a buy intent
    intent = Intent.prediction_buy(
        market_id="will-bitcoin-exceed-100k",
        outcome="YES",
        amount_usd=Decimal("100"),
    )
    bundle = adapter.compile_intent(intent, market_snapshot)
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from ...data.market_snapshot import MarketSnapshot
from ...intents.vocabulary import (
    IntentType,
    PredictionBuyIntent,
    PredictionRedeemIntent,
    PredictionSellIntent,
)
from ...models.reproduction_bundle import ActionBundle
from .clob_client import ClobClient
from .ctf_sdk import BINARY_PARTITION, INDEX_SET_NO, INDEX_SET_YES, CtfSDK
from .exceptions import (
    PolymarketMarketNotFoundError,
    PolymarketMarketNotResolvedError,
)
from .models import (
    GammaMarket,
    LimitOrderParams,
    MarketOrderParams,
    OrderType,
    PolymarketConfig,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Token decimals (USDC on Polygon has 6 decimals)
USDC_DECIMALS = 6

# Gas estimates for Polymarket operations
POLYMARKET_GAS_ESTIMATES = {
    "clob_order": 0,  # CLOB orders are off-chain
    "approve_usdc": 50_000,
    "approve_ctf": 50_000,
    "redeem_positions": 200_000,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class OrderResult:
    """Result of building and signing an order.

    Attributes:
        success: Whether the order was built successfully
        order_id: Order ID if submitted
        signed_order_payload: Signed order payload for submission
        error: Error message if failed
    """

    success: bool
    order_id: str | None = None
    signed_order_payload: dict[str, Any] | None = None
    error: str | None = None
    token_id: str | None = None
    side: str | None = None
    price: Decimal | None = None
    size: Decimal | None = None


@dataclass
class RedeemResult:
    """Result of building a redeem transaction.

    Attributes:
        success: Whether the transaction was built successfully
        transactions: List of transaction data dicts
        error: Error message if failed
    """

    success: bool
    transactions: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None
    condition_id: str | None = None
    outcome: str | None = None
    amount: Decimal | None = None


# =============================================================================
# Polymarket Adapter
# =============================================================================


class PolymarketAdapter:
    """Adapter for compiling prediction market intents to ActionBundles.

    This adapter integrates with the Almanak Strategy Framework to compile
    high-level prediction market intents into executable orders and transactions.

    The compilation process:
    1. Resolve market_id to a GammaMarket (supports ID or slug)
    2. Determine token_id based on outcome (YES or NO)
    3. Build order/transaction based on intent type
    4. Return ActionBundle for execution

    Attributes:
        config: Polymarket configuration
        clob: CLOB API client for order management
        ctf: CTF SDK for on-chain operations
        web3: Optional Web3 instance for on-chain operations

    Example:
        >>> config = PolymarketConfig.from_env()
        >>> adapter = PolymarketAdapter(config)
        >>>
        >>> intent = Intent.prediction_buy(
        ...     market_id="btc-100k",
        ...     outcome="YES",
        ...     amount_usd=Decimal("100"),
        ... )
        >>> bundle = adapter.compile_intent(intent, market_snapshot)
    """

    def __init__(
        self,
        config: PolymarketConfig,
        web3: Any | None = None,
    ) -> None:
        """Initialize the Polymarket adapter.

        Args:
            config: Polymarket configuration with wallet and credentials
            web3: Optional Web3 instance for on-chain operations.
                  Required for redeem intents.
        """
        self.config = config
        self.web3 = web3
        self.clob = ClobClient(config)
        self.ctf = CtfSDK()

        # Cache for resolved markets
        self._market_cache: dict[str, GammaMarket] = {}

        logger.info(
            "PolymarketAdapter initialized",
            extra={
                "wallet": config.wallet_address,
                "has_web3": web3 is not None,
            },
        )

    def close(self) -> None:
        """Close adapter and release resources."""
        self.clob.close()

    def __enter__(self) -> "PolymarketAdapter":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # =========================================================================
    # Intent Compilation
    # =========================================================================

    def compile_intent(
        self,
        intent: PredictionBuyIntent | PredictionSellIntent | PredictionRedeemIntent,
        market_snapshot: MarketSnapshot | None = None,
    ) -> ActionBundle:
        """Compile a prediction market intent to an ActionBundle.

        This is the main entry point for intent compilation. It dispatches
        to the appropriate handler based on intent type.

        Args:
            intent: The prediction intent to compile
            market_snapshot: Optional market snapshot for additional context

        Returns:
            ActionBundle containing the compiled orders/transactions

        Raises:
            ValueError: If intent type is not supported
        """
        if isinstance(intent, PredictionBuyIntent):
            return self._compile_buy_intent(intent, market_snapshot)
        elif isinstance(intent, PredictionSellIntent):
            return self._compile_sell_intent(intent, market_snapshot)
        elif isinstance(intent, PredictionRedeemIntent):
            return self._compile_redeem_intent(intent, market_snapshot)
        else:
            raise ValueError(f"Unsupported intent type: {type(intent)}")

    def _compile_buy_intent(
        self,
        intent: PredictionBuyIntent,
        market_snapshot: MarketSnapshot | None,
    ) -> ActionBundle:
        """Compile a PredictionBuyIntent to an ActionBundle.

        For buy intents:
        - Resolve market_id to token_id (YES or NO)
        - Calculate order size (from amount_usd or shares)
        - Build and sign the order
        - Return ActionBundle with order submission action

        Args:
            intent: The buy intent to compile
            market_snapshot: Optional market context

        Returns:
            ActionBundle with CLOB order action
        """
        try:
            # Resolve market to get token IDs
            market = self._resolve_market(intent.market_id)
            token_id = self._get_token_id(market, intent.outcome)

            # Determine order parameters
            if intent.order_type == "limit" and intent.max_price is not None:
                # Limit order with specified price
                price = intent.max_price
                size = self._calculate_size(intent, price)

                params = LimitOrderParams(
                    token_id=token_id,
                    side="BUY",
                    price=price,
                    size=size,
                    expiration=self._calculate_expiration(intent.expiration_hours),
                )
                # Pass market metadata for market-specific minimum validation
                signed_order = self.clob.create_and_sign_limit_order(params, market=market)
                order_type = self._map_time_in_force(intent.time_in_force)

            else:
                # Market order - use aggressive price
                # For BUY, use 0.99 (max) to ensure fill
                price = intent.max_price or Decimal("0.99")
                size = self._calculate_size(intent, price)

                market_params = MarketOrderParams(
                    token_id=token_id,
                    side="BUY",
                    amount=size * price,  # USDC amount
                    worst_price=price,
                )
                # Pass market metadata for market-specific minimum validation
                signed_order = self.clob.create_and_sign_market_order(market_params, market=market)
                order_type = OrderType.IOC  # Market orders use IOC

            # Build order payload
            order_payload = signed_order.to_api_payload()
            order_payload["orderType"] = order_type.value

            logger.info(
                "Compiled buy intent",
                extra={
                    "market_id": intent.market_id,
                    "outcome": intent.outcome,
                    "size": str(size),
                    "price": str(price),
                    "order_type": order_type.value,
                },
            )

            return ActionBundle(
                intent_type=IntentType.PREDICTION_BUY.value,
                transactions=[],  # CLOB orders are off-chain
                metadata={
                    "intent_id": intent.intent_id,
                    "market_id": market.id,
                    "market_question": market.question,
                    "token_id": token_id,
                    "outcome": intent.outcome,
                    "side": "BUY",
                    "price": str(price),
                    "size": str(size),
                    "order_type": order_type.value,
                    "order_payload": order_payload,
                    "protocol": "polymarket",
                    "chain": "polygon",
                },
            )

        except Exception as e:
            logger.exception("Failed to compile buy intent", extra={"error": str(e)})
            return ActionBundle(
                intent_type=IntentType.PREDICTION_BUY.value,
                transactions=[],
                metadata={
                    "error": str(e),
                    "intent_id": intent.intent_id,
                },
            )

    def _compile_sell_intent(
        self,
        intent: PredictionSellIntent,
        market_snapshot: MarketSnapshot | None,
    ) -> ActionBundle:
        """Compile a PredictionSellIntent to an ActionBundle.

        For sell intents:
        - Resolve market_id to token_id (YES or NO)
        - Resolve shares ("all" -> query current position)
        - Build and sign the order
        - Return ActionBundle with order submission action

        Args:
            intent: The sell intent to compile
            market_snapshot: Optional market context

        Returns:
            ActionBundle with CLOB order action
        """
        try:
            # Resolve market to get token IDs
            market = self._resolve_market(intent.market_id)
            token_id = self._get_token_id(market, intent.outcome)

            # Resolve shares amount
            if intent.shares == "all":
                # Query current position size
                positions = self.clob.get_positions()
                position = next(
                    (p for p in positions if p.token_id == token_id),
                    None,
                )
                if position is None or position.size <= 0:
                    return ActionBundle(
                        intent_type=IntentType.PREDICTION_SELL.value,
                        transactions=[],
                        metadata={
                            "error": "No position to sell",
                            "intent_id": intent.intent_id,
                        },
                    )
                size = position.size
            else:
                if not isinstance(intent.shares, Decimal):
                    raise TypeError(f"Expected Decimal for shares, got {type(intent.shares).__name__}")
                size = intent.shares

            # Determine order parameters
            if intent.order_type == "limit" and intent.min_price is not None:
                # Limit order with specified minimum price
                price = intent.min_price

                params = LimitOrderParams(
                    token_id=token_id,
                    side="SELL",
                    price=price,
                    size=size,
                )
                # Pass market metadata for market-specific minimum validation
                signed_order = self.clob.create_and_sign_limit_order(params, market=market)
                order_type = self._map_time_in_force(intent.time_in_force)

            else:
                # Market order - use aggressive price
                # For SELL, use 0.01 (min) to ensure fill
                price = intent.min_price or Decimal("0.01")

                market_params = MarketOrderParams(
                    token_id=token_id,
                    side="SELL",
                    amount=size,  # Shares to sell
                    worst_price=price,
                )
                # Pass market metadata for market-specific minimum validation
                signed_order = self.clob.create_and_sign_market_order(market_params, market=market)
                order_type = OrderType.IOC  # Market orders use IOC

            # Build order payload
            order_payload = signed_order.to_api_payload()
            order_payload["orderType"] = order_type.value

            logger.info(
                "Compiled sell intent",
                extra={
                    "market_id": intent.market_id,
                    "outcome": intent.outcome,
                    "size": str(size),
                    "price": str(price),
                    "order_type": order_type.value,
                },
            )

            return ActionBundle(
                intent_type=IntentType.PREDICTION_SELL.value,
                transactions=[],  # CLOB orders are off-chain
                metadata={
                    "intent_id": intent.intent_id,
                    "market_id": market.id,
                    "market_question": market.question,
                    "token_id": token_id,
                    "outcome": intent.outcome,
                    "side": "SELL",
                    "price": str(price),
                    "size": str(size),
                    "order_type": order_type.value,
                    "order_payload": order_payload,
                    "protocol": "polymarket",
                    "chain": "polygon",
                },
            )

        except Exception as e:
            logger.exception("Failed to compile sell intent", extra={"error": str(e)})
            return ActionBundle(
                intent_type=IntentType.PREDICTION_SELL.value,
                transactions=[],
                metadata={
                    "error": str(e),
                    "intent_id": intent.intent_id,
                },
            )

    def _compile_redeem_intent(
        self,
        intent: PredictionRedeemIntent,
        market_snapshot: MarketSnapshot | None,
    ) -> ActionBundle:
        """Compile a PredictionRedeemIntent to an ActionBundle.

        For redeem intents:
        - Resolve market_id to condition_id
        - Check if market is resolved
        - Build CTF redeem transaction
        - Return ActionBundle with on-chain transaction

        Args:
            intent: The redeem intent to compile
            market_snapshot: Optional market context

        Returns:
            ActionBundle with CTF redeem transaction

        Raises:
            PolymarketMarketNotResolvedError: If market is not resolved
        """
        try:
            # Resolve market to get condition ID
            market = self._resolve_market(intent.market_id)

            # Check if we have Web3 for on-chain operations
            if self.web3 is None:
                return ActionBundle(
                    intent_type=IntentType.PREDICTION_REDEEM.value,
                    transactions=[],
                    metadata={
                        "error": "Web3 instance required for redemption. Initialize adapter with web3 parameter.",
                        "intent_id": intent.intent_id,
                    },
                )

            # Check if market is resolved
            resolution = self.ctf.get_condition_resolution(market.condition_id, self.web3)
            if not resolution.is_resolved:
                raise PolymarketMarketNotResolvedError(f"Market '{market.question}' is not yet resolved")

            # Determine which outcomes to redeem
            if intent.outcome is not None:
                # Specific outcome
                index_sets = [INDEX_SET_YES if intent.outcome == "YES" else INDEX_SET_NO]
            else:
                # Both outcomes (will only redeem winning side)
                index_sets = BINARY_PARTITION

            # Build redeem transaction
            tx_data = self.ctf.build_redeem_tx(
                condition_id=market.condition_id,
                index_sets=index_sets,
                sender=self.config.wallet_address,
            )

            transaction = {
                "to": tx_data.to,
                "data": tx_data.data,
                "value": tx_data.value,
                "gas_estimate": tx_data.gas_estimate,
                "description": tx_data.description,
                "tx_type": "redeem",
            }

            logger.info(
                "Compiled redeem intent",
                extra={
                    "market_id": intent.market_id,
                    "condition_id": market.condition_id,
                    "outcome": intent.outcome,
                    "winning_outcome": resolution.winning_outcome,
                },
            )

            return ActionBundle(
                intent_type=IntentType.PREDICTION_REDEEM.value,
                transactions=[transaction],
                metadata={
                    "intent_id": intent.intent_id,
                    "market_id": market.id,
                    "market_question": market.question,
                    "condition_id": market.condition_id,
                    "outcome": intent.outcome,
                    "winning_outcome": "YES" if resolution.winning_outcome == 0 else "NO",
                    "protocol": "polymarket",
                    "chain": "polygon",
                },
            )

        except PolymarketMarketNotResolvedError:
            raise
        except Exception as e:
            logger.exception("Failed to compile redeem intent", extra={"error": str(e)})
            return ActionBundle(
                intent_type=IntentType.PREDICTION_REDEEM.value,
                transactions=[],
                metadata={
                    "error": str(e),
                    "intent_id": intent.intent_id,
                },
            )

    # =========================================================================
    # Market Resolution
    # =========================================================================

    def _resolve_market(self, market_id: str) -> GammaMarket:
        """Resolve market_id to a GammaMarket.

        Supports both:
        - Market ID (numeric string)
        - Market slug (URL-friendly string)

        Args:
            market_id: Market identifier (ID or slug)

        Returns:
            GammaMarket object

        Raises:
            PolymarketMarketNotFoundError: If market not found
        """
        # Check cache first
        if market_id in self._market_cache:
            return self._market_cache[market_id]

        # Try by ID first (if it looks like an ID - numeric or alphanumeric)
        market = None
        try:
            market = self.clob.get_market(market_id)
        except Exception:
            pass

        # If not found, try by slug
        if market is None:
            market = self.clob.get_market_by_slug(market_id)

        if market is None:
            raise PolymarketMarketNotFoundError(f"Market not found: {market_id}")

        # Cache the result
        self._market_cache[market_id] = market
        self._market_cache[market.id] = market
        if market.slug:
            self._market_cache[market.slug] = market

        return market

    def _get_token_id(self, market: GammaMarket, outcome: str) -> str:
        """Get the CLOB token ID for a specific outcome.

        Args:
            market: GammaMarket with token IDs
            outcome: "YES" or "NO"

        Returns:
            Token ID string

        Raises:
            ValueError: If outcome is invalid or tokens not found
        """
        if outcome == "YES":
            if market.yes_token_id is None:
                raise ValueError(f"Market {market.id} has no YES token ID")
            return market.yes_token_id
        elif outcome == "NO":
            if market.no_token_id is None:
                raise ValueError(f"Market {market.id} has no NO token ID")
            return market.no_token_id
        else:
            raise ValueError(f"Invalid outcome: {outcome}. Must be 'YES' or 'NO'")

    # =========================================================================
    # Order Helpers
    # =========================================================================

    def _calculate_size(
        self,
        intent: PredictionBuyIntent,
        price: Decimal,
    ) -> Decimal:
        """Calculate order size from intent parameters.

        Args:
            intent: The buy intent
            price: Price per share

        Returns:
            Number of shares to buy
        """
        if intent.shares is not None:
            return intent.shares
        elif intent.amount_usd is not None:
            # size = amount_usd / price
            return intent.amount_usd / price
        else:
            raise ValueError("Either shares or amount_usd must be specified")

    def _calculate_expiration(self, expiration_hours: int | None) -> int:
        """Calculate expiration timestamp.

        Args:
            expiration_hours: Hours until expiration, or None for no expiry

        Returns:
            Unix timestamp or 0 for no expiry
        """
        if expiration_hours is None:
            return 0
        return int(datetime.now(UTC).timestamp()) + (expiration_hours * 3600)

    def _map_time_in_force(self, tif: str) -> OrderType:
        """Map time-in-force string to OrderType enum.

        Args:
            tif: Time-in-force string ("GTC", "IOC", "FOK")

        Returns:
            OrderType enum value
        """
        mapping = {
            "GTC": OrderType.GTC,
            "IOC": OrderType.IOC,
            "FOK": OrderType.FOK,
        }
        return mapping.get(tif, OrderType.GTC)


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "PolymarketAdapter",
    "OrderResult",
    "RedeemResult",
    "POLYMARKET_GAS_ESTIMATES",
]
