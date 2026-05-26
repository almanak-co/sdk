"""Polymarket Adapter for Intent Compilation.

This adapter compiles prediction market intents (PredictionBuyIntent,
PredictionSellIntent, PredictionRedeemIntent) into executable ActionBundles.

The adapter bridges high-level trading intents to the gateway-backed Polymarket
client and CTF SDK:
- Buy/Sell intents -> CLOB orders (submitted through the gateway)
- Redeem intents -> CTF transactions (on-chain redemption)

Example:
    from almanak.connectors.polymarket import GatewayPolymarketClient, PolymarketAdapter
    from almanak.framework.gateway_client import GatewayClient
    from almanak.framework.intents import Intent

    gateway_client = GatewayClient(...)
    gateway_client.connect()
    adapter = PolymarketAdapter(
        GatewayPolymarketClient(gateway_client),
        wallet_address="0x1234...",
    )

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
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any

from almanak.framework.intents.vocabulary import (
    IntentType,
    PredictionBuyIntent,
    PredictionRedeemIntent,
    PredictionSellIntent,
)

# MarketSnapshot is bound at runtime so ``typing.get_type_hints`` on this
# adapter's public methods resolves the annotation. Pandas was the original
# reason this import was deferred; ``data.market_snapshot`` now imports
# pandas only inside the methods that build DataFrames, so importing the
# class itself is cheap.
from almanak.framework.market import MarketSnapshot
from almanak.framework.models.reproduction_bundle import ActionBundle

from .ctf_sdk import BINARY_PARTITION, INDEX_SET_NO, INDEX_SET_YES, CtfSDK
from .exceptions import (
    PolymarketInvalidPrecisionError,
    PolymarketInvalidTickSizeError,
    PolymarketMarketNotFoundError,
    PolymarketMarketNotResolvedError,
    PolymarketMinimumOrderError,
)
from .models import (
    GammaMarket,
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

# CLOB pre-flight precision cap. The adapter validates user-supplied PRICES
# against this BEFORE snapping (VIB-3140) so `--dry-run` matches the live CLOB.
# Tick sizes top out at 0.0001 (4 decimals); any price with more fractional
# digits is meaningless on the CLOB wire and almost certainly came from a
# Python-float-to-Decimal conversion (e.g. ``Decimal(0.7)`` → 17 decimals).
# Share SIZE precision is intentionally NOT checked here: the SDK's internal
# share-quantization (``_build_amounts_at_price``) floors shares to the 2-dec
# grid, and typical sizing patterns (``amount_usd / price``) legitimately
# produce many-decimal values that the CLOB accepts.
CLOB_MAX_PRICE_DECIMALS = 4


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class OrderResult:
    """Result of building and signing an order.

    Attributes:
        success: Whether the order was built successfully
        order_id: Order ID if submitted
        error: Error message if failed
    """

    success: bool
    order_id: str | None = None
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
        client: Gateway-backed Polymarket client for order management
        clob: Compatibility alias for the client
        ctf: CTF SDK for on-chain operations
        web3: Optional Web3 instance for on-chain operations

    Example:
        >>> adapter = PolymarketAdapter(pm_client, wallet_address="0x1234...")
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
        client: Any,
        wallet_address: str | None = None,
        web3: Any | None = None,
    ) -> None:
        """Initialize the Polymarket adapter.

        Args:
            client: Gateway-backed Polymarket client
            wallet_address: Wallet address used for on-chain redemption
            web3: Optional Web3 instance for on-chain operations.
                  Required for redeem intents.
        """
        if isinstance(client, PolymarketConfig):
            raise ValueError(
                "PolymarketAdapter requires a gateway-backed Polymarket client. "
                "PolymarketConfig is not accepted here because it would bypass the gateway boundary."
            )
        if wallet_address is None:
            raise ValueError("wallet_address is required when initializing PolymarketAdapter with a client")
        self.client = client
        self.wallet_address = wallet_address
        # Compatibility alias for older tests/callers that still reference
        # ``adapter.clob`` even though the adapter now accepts any Polymarket
        # client implementation (gateway-backed or direct).
        self.clob = self.client
        self.web3 = web3
        self.ctf = CtfSDK()

        # Cache for resolved markets
        self._market_cache: dict[str, GammaMarket] = {}

        logger.info(
            "PolymarketAdapter initialized",
            extra={
                "wallet": self.wallet_address,
                "has_web3": web3 is not None,
            },
        )

    def close(self) -> None:
        """Close adapter and release resources."""
        close = getattr(self.client, "close", None)
        if callable(close):
            close()

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

            # Order routing: prefer LIMIT whenever the strategy gave us a price
            # to anchor to. Market BUYs at the 0.99 default reserve a full
            # ~size*$0.99 nominal against the wallet's USDC allowance, which
            # the CLOB rejects on cheap markets ($1 budget vs $80 nominal). A
            # tick-aligned limit at max_price has nominal = size*max_price and
            # avoids the footgun entirely. See Portfolio Manager incident
            # 2026-04-19 (market 556063).
            if intent.max_price is not None:
                # Compute size from the user-supplied max_price (preserve sizing
                # intent), then snap the submission price to the tick grid. If we
                # used the snapped price for sizing, an off-tick max_price would
                # silently inflate the share count: e.g. amount_usd=$1 with
                # max_price=0.0135 on a 0.01-tick market would snap price → 0.01
                # and sizing → 100 shares (vs. the user-intended ~74).
                size = self._calculate_size(intent, intent.max_price)
                # Run the live-CLOB pre-flight checks on the USER-supplied price
                # (pre-snap) so `--dry-run` fails with the same error text the
                # CLOB would emit in production instead of silently correcting
                # the price (VIB-3140). Tick / precision / $1-floor are all
                # covered here.
                self._validate_clob_preflight(
                    side="BUY",
                    price=intent.max_price,
                    size=size,
                    market=market,
                )
                price = self._round_price_to_tick(intent.max_price, "BUY", market=market)
                # If the strategy declared order_type='market' (the default), it
                # expected fail-fast immediacy. Routing to LIMIT for safety must
                # NOT silently downgrade that to a long-lived GTC order resting
                # on the book — force IOC so the order either fills or cancels.
                if intent.order_type == "market":
                    order_type = OrderType.IOC
                else:
                    order_type = self._map_time_in_force(intent.time_in_force)
                # V2 GTD: when expiration_hours is set, force GTD time-in-force
                # so the API matcher honors the off-chain expiration (V2 has no
                # on-chain expiration field in the signed Order struct).
                if intent.expiration_hours is not None and intent.expiration_hours > 0:
                    order_type = OrderType.GTD

            else:
                # No max_price → reject. The legacy fallback was an aggressive
                # market sweep at 0.99 which reserved ~size*$0.99 of USDC
                # allowance even though the actual fill landed at the order
                # book's best ask. On cheap markets ($1 budget vs $80 nominal
                # reserve) the CLOB rejected the order anyway, so the silent
                # "warn and submit" path was a footgun: misleading wallet
                # accounting AND a guaranteed CLOB rejection. Force the
                # strategy author to anchor the buy with a tick-aligned
                # max_price (PM Exp 14 / VIB-3131).
                raise ValueError(
                    "PredictionBuyIntent.max_price is required: a buy without "
                    "an explicit anchor price would reserve ~size * $0.99 of "
                    "USDC allowance for what may fill at a much lower price. "
                    "Set intent.max_price (e.g. best_ask + a slippage buffer, "
                    "snapped to the market's tick grid) so the order routes "
                    "through the LIMIT path."
                )

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

            # VIB-3710: thread the native (MATIC) USD price through the bundle
            # so the result enricher can convert wrap/approval gas costs from
            # wei → USD without needing a fresh price oracle round-trip. Use
            # ``MarketSnapshot.price`` when available; absent / failing
            # snapshot leaves the field unset and the enricher records
            # gas_cost_native only with a structured warning.
            native_price_str = self._matic_price_str(market_snapshot)

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
                    "order_request": {
                        "token_id": token_id,
                        "side": "BUY",
                        "price": str(price),
                        "size": str(size),
                        "time_in_force": order_type.value,
                        "expiration": self._calculate_expiration(intent.expiration_hours),
                    },
                    "protocol": "polymarket",
                    "chain": "polygon",
                    "native_token_price_usd": native_price_str,
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
            # Validate min_price BEFORE any market/position lookup so the new
            # mandatory-anchor rule (PM Exp 14 / VIB-3131) fails deterministically.
            # Otherwise an "all"-shares intent against an empty wallet would
            # short-circuit to "No position to sell" and the missing-anchor bug
            # would only surface once a position existed (CodeRabbit catch).
            if intent.min_price is None:
                raise ValueError(
                    "PredictionSellIntent.min_price is required: a sell "
                    "without an explicit floor would fill at any price down "
                    "to the 0.01 tick. Set intent.min_price (e.g. best_bid "
                    "minus a slippage buffer, snapped to the market's tick "
                    "grid) so the order routes through the LIMIT path."
                )

            # Resolve market to get token IDs
            market = self._resolve_market(intent.market_id)
            token_id = self._get_token_id(market, intent.outcome)

            # Resolve shares amount
            if intent.shares == "all":
                # Query current position size
                positions = self.client.get_positions()
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

            # min_price guaranteed non-None by the upfront check; route to LIMIT.
            # Market SELLs at the 0.01 default underprice the position and risk
            # filling at the floor — always go through the LIMIT path.
            # Run the live-CLOB pre-flight checks on the USER-supplied price
            # (pre-snap) so `--dry-run` fails with the same error text the
            # CLOB would emit in production instead of silently correcting
            # the price (VIB-3140). Tick size and decimal precision apply to
            # SELL orders too; the $1 USD floor is BUY-only and skipped here.
            self._validate_clob_preflight(
                side="SELL",
                price=intent.min_price,
                size=size,
                market=market,
            )
            price = self._round_price_to_tick(intent.min_price, "SELL", market=market)
            # Mirror the BUY routing: when the strategy declared 'market' but
            # we elevated to LIMIT for safety, force IOC so we don't leave a
            # long-lived GTC order resting on the book.
            if intent.order_type == "market":
                order_type = OrderType.IOC
            else:
                order_type = self._map_time_in_force(intent.time_in_force)

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

            # VIB-3710: surface MATIC USD price for parity with the BUY path —
            # SELL orders almost never trigger setup_txs (allowances already
            # applied from the first BUY) but the field stays consistent for
            # the enricher's wei → USD conversion in the rare case they do.
            native_price_str = self._matic_price_str(market_snapshot)

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
                    "order_request": {
                        "token_id": token_id,
                        "side": "SELL",
                        "price": str(price),
                        "size": str(size),
                        "time_in_force": order_type.value,
                        "expiration": 0,
                    },
                    "protocol": "polymarket",
                    "chain": "polygon",
                    "native_token_price_usd": native_price_str,
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
                sender=self.wallet_address,
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
            market = self.client.get_market(market_id)
        except Exception:
            pass

        # If not found, try by slug
        if market is None:
            market = self.client.get_market_by_slug(market_id)

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
            expiration_hours: Hours until expiration. ``None`` or ``<= 0`` means
                "no expiry"; the GTD gate at the call site uses the same
                ``> 0`` predicate, so non-positive values must round-trip to
                ``0`` here or non-GTD orders would carry a stale, already-
                expired timestamp.

        Returns:
            Unix timestamp or 0 for no expiry
        """
        if expiration_hours is None or expiration_hours <= 0:
            return 0
        return int(datetime.now(UTC).timestamp()) + (expiration_hours * 3600)

    @staticmethod
    def _matic_price_str(market_snapshot: MarketSnapshot | None) -> str:
        """Resolve the MATIC USD price from the snapshot, returning a string.

        VIB-3710: the result enricher uses this to convert wrap-tx +
        approval-tx gas costs from wei → USD. Returns ``""`` (empty) when
        the snapshot is absent OR the lookup fails — the enricher then
        records ``gas_cost_native`` only and emits a structured warning so
        the missing USD value is visible in iteration logs rather than
        silently lost.

        Note: the gateway-boundary rule applies. ``MarketSnapshot.price``
        already routes through the gateway's ``MarketService`` — there is
        no direct external HTTP call here.
        """
        if market_snapshot is None:
            return ""
        try:
            price = market_snapshot.price("MATIC")
        except Exception:  # noqa: BLE001 — snapshot lookups have many failure modes; degrade gracefully
            return ""
        if price is None or price <= 0:
            return ""
        return str(price)

    # =========================================================================
    # CLOB Pre-Flight Validation (VIB-3140)
    # =========================================================================

    def _validate_clob_preflight(
        self,
        *,
        side: str,
        price: Decimal,
        size: Decimal,
        market: GammaMarket,
    ) -> None:
        """Run all live-CLOB pre-flight validations at compile / dry-run time.

        The strategy runner's ``--dry-run`` mode stops before any network
        submission — so any validation that happens only on the live CLOB
        (off-tick prices, sub-$1 BUY, excess decimals) silently passes in
        dry-run and then blows up in production. This method closes that gap
        by running the CLOB's pre-flight checks against the *user-supplied*
        price (i.e. BEFORE :meth:`ClobClient.round_price_to_tick` snaps it).

        The error messages include the exact strings the live CLOB emits so
        strategy authors can grep for them and ``non_retryable`` classifiers
        (VIB-3141) can pattern-match the same text.

        Checks (all run in order, first failure raises):
        1. **Tick size** — ``price`` must be an integer multiple of
           ``market.order_price_min_tick_size``. Live CLOB:
           ``breaks minimum tick size rule: <tick_size>``.
        2. **Price precision** — ``price`` must have at most 4 decimals
           (CLOB caps at 0.0001 tick). Guards against Python-float ->
           Decimal bugs that inflate ``as_integer_ratio`` denominators.
        3. **Minimum order value** — for BUY, ``size * price`` must be
           ``>= $1``. Live CLOB:
           ``invalid amount for a marketable BUY order ($X), min size: $1``.

        Post-sign validations in :meth:`ClobClient.build_limit_order` and
        :meth:`ClobClient.build_market_order` remain in place as defence in
        depth — re-checking snapped amounts catches cases where flooring
        drops below the per-market shares minimum.

        Args:
            side: Order side ("BUY" or "SELL"). Only BUY is subject to the
                $1 USD floor; SELL pays in shares.
            price: User-supplied price (``intent.max_price`` for BUY,
                ``intent.min_price`` for SELL). Validated pre-snap so
                off-tick prices fail instead of being silently corrected.
            size: Number of shares being bought/sold.
            market: Resolved ``GammaMarket`` providing tick / size metadata.

        Raises:
            PolymarketInvalidTickSizeError: Price is not a tick multiple.
            PolymarketInvalidPrecisionError: Price or size has too many
                decimals for the CLOB precision caps.
            PolymarketMinimumOrderError: BUY order value (size * price) is
                below the $1 USD floor.
        """
        self._validate_tick_size(price, market=market)

        # 2. Price precision — Decimal.as_tuple().exponent is negative for
        #    fractional values; -5 means 5 decimal places. Normalize() strips
        #    trailing zeros so e.g. Decimal("0.1200") registers as 2 decimals.
        #    Positive exponents (e.g. Decimal("1E+2")) have 0 fractional
        #    digits and are fine; we only reject when fractional-digit count
        #    exceeds the CLOB's 4-decimal cap.
        price_exp = price.normalize().as_tuple().exponent
        if isinstance(price_exp, int) and price_exp < 0 and -price_exp > CLOB_MAX_PRICE_DECIMALS:
            raise PolymarketInvalidPrecisionError(
                field="price",
                value=str(price),
                max_decimals=CLOB_MAX_PRICE_DECIMALS,
            )

        # 3. Minimum share size — market.order_min_size floor (applies to both
        #    sides). Previously enforced inside ClobClient.build_limit_order();
        #    with the gateway-backed path the build step is deferred to the
        #    gateway, so the check must happen here to keep dry-run parity.
        if market and market.order_min_size and size < market.order_min_size:
            raise PolymarketMinimumOrderError(
                size=str(size),
                minimum=str(market.order_min_size),
            )

        # 4. Minimum order value — $1 USD floor for BUY only. Delegates to
        #    the ClobClient so we emit the same error text that the post-sign
        #    path does.
        if side == "BUY":
            self._validate_order_value_usd(size * price)

    def _validate_order_value_usd(self, value_usd: Decimal) -> None:
        if value_usd < Decimal("1"):
            raise PolymarketMinimumOrderError(size=f"${value_usd}", minimum="$1")

    def _validate_tick_size(self, price: Decimal, market: GammaMarket | None = None) -> None:
        effective_tick = market.order_price_min_tick_size if market else Decimal("0.01")
        if effective_tick <= 0:
            return
        remainder = price % effective_tick
        if remainder != 0:
            ticks = price / effective_tick
            nearest_ticks = round(ticks)
            nearest_valid = nearest_ticks * effective_tick
            raise PolymarketInvalidTickSizeError(
                price=str(price),
                tick_size=str(effective_tick),
                nearest_valid=str(nearest_valid),
            )

    def _round_price_to_tick(self, price: Decimal, side: str, market: GammaMarket | None = None) -> Decimal:
        effective_tick = market.order_price_min_tick_size if market else Decimal("0.01")
        if effective_tick <= 0:
            return price
        ticks = price / effective_tick
        if side == "BUY":
            rounded_ticks = ticks.quantize(Decimal("1"), rounding=ROUND_FLOOR)
        else:
            rounded_ticks = ticks.quantize(Decimal("1"), rounding=ROUND_CEILING)
        rounded_price = rounded_ticks * effective_tick
        return max(Decimal("0.01"), min(Decimal("0.99"), rounded_price))

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
