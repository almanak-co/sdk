"""Polymarket connector exceptions.

Custom exception classes for Polymarket API errors, authentication failures,
and order-related issues.
"""


class PolymarketError(Exception):
    """Base exception for all Polymarket errors."""

    pass


class PolymarketAPIError(PolymarketError):
    """Error from Polymarket API call."""

    def __init__(self, message: str, status_code: int | None = None, errors: list[str] | None = None):
        self.status_code = status_code
        self.errors = errors or []
        super().__init__(message)


class PolymarketAuthenticationError(PolymarketError):
    """Authentication failed with Polymarket."""

    pass


class PolymarketCredentialsError(PolymarketError):
    """API credentials are invalid or missing."""

    pass


class PolymarketRateLimitError(PolymarketError):
    """Rate limit exceeded."""

    def __init__(self, message: str = "Rate limit exceeded", retry_after: int | None = None):
        self.retry_after = retry_after
        super().__init__(message)


class PolymarketOrderError(PolymarketError):
    """Error related to order operations."""

    pass


class PolymarketOrderNotFoundError(PolymarketOrderError):
    """Order not found."""

    def __init__(self, order_id: str):
        self.order_id = order_id
        super().__init__(f"Order not found: {order_id}")


class PolymarketInsufficientBalanceError(PolymarketOrderError):
    """Insufficient balance for operation."""

    def __init__(self, asset: str, required: str, available: str):
        self.asset = asset
        self.required = required
        self.available = available
        super().__init__(f"Insufficient {asset} balance: required {required}, available {available}")


class PolymarketInvalidPriceError(PolymarketOrderError):
    """Invalid order price."""

    def __init__(self, price: str, min_price: str = "0.01", max_price: str = "0.99"):
        self.price = price
        self.min_price = min_price
        self.max_price = max_price
        super().__init__(f"Invalid price {price}: must be between {min_price} and {max_price}")


class PolymarketMinimumOrderError(PolymarketOrderError):
    """Order size below minimum.

    Two failure modes share this class, distinguished by the ``$`` prefix on
    ``size`` / ``minimum``:
    - Share-count floor failure: ``size="5"``, ``minimum="10"`` (market's
      ``order_min_size``).
    - USD-value floor failure: ``size="$0.30"``, ``minimum="$1"`` — Polymarket
      CLOB rejects BUY orders with makerAmount < $1. The live CLOB message is
      ``invalid amount for a marketable BUY order ($X), min size: $1``; we
      embed that exact string in the exception message so strategy authors
      can grep for it (VIB-3140, coordinated with VIB-3141's non_retryable
      classification).
    """

    def __init__(self, size: str, minimum: str):
        self.size = size
        self.minimum = minimum
        # Emit the exact live-CLOB error text for the USD-floor case so
        # strategy authors can grep for it (VIB-3140).
        if minimum.startswith("$"):
            # Live CLOB: `invalid amount for a marketable BUY order ($X), min size: $1`
            super().__init__(f"invalid amount for a marketable BUY order ({size}), min size: {minimum}")
        else:
            super().__init__(f"Order size {size} below minimum {minimum}")


class PolymarketInvalidTickSizeError(PolymarketOrderError):
    """Price does not conform to market tick size.

    Polymarket markets have specific tick sizes (e.g., 0.01, 0.001) that
    prices must be multiples of. The live CLOB rejects off-tick prices
    with ``order {order_hash} breaks minimum tick size rule: {tick_size}``;
    we embed that exact string in the exception message so strategy authors
    can grep for it (VIB-3140, coordinated with VIB-3141's non_retryable
    classification).
    """

    def __init__(self, price: str, tick_size: str, nearest_valid: str | None = None):
        self.price = price
        self.tick_size = tick_size
        self.nearest_valid = nearest_valid
        # Emit the exact live-CLOB error text so strategy authors can grep
        # for it (VIB-3140). The CLOB prefixes with an order hash; we use
        # the user-supplied price instead for dry-run readability, but the
        # trailing `breaks minimum tick size rule: <tick_size>` is verbatim.
        hint = f", nearest valid={nearest_valid}" if nearest_valid else ""
        super().__init__(f"order (price={price}) breaks minimum tick size rule: {tick_size}{hint}")


class PolymarketInvalidPrecisionError(PolymarketOrderError):
    """Order price or size has too many decimals for the CLOB to accept.

    Polymarket's CLOB caps price precision at 4 decimals (tick sizes top out
    at 0.0001) and size precision at 2 decimals (shares quantized to 0.01).
    Submitting extra decimals causes the server to round down silently or
    reject with an INVALID_ORDER error. We surface this at compile time so
    strategy authors cannot accidentally over-specify precision (VIB-3140).
    """

    def __init__(self, field: str, value: str, max_decimals: int):
        self.field = field
        self.value = value
        self.max_decimals = max_decimals
        super().__init__(f"INVALID_ORDER: {field} {value} has too many decimals (max {max_decimals} decimals allowed)")


class PolymarketMarketError(PolymarketError):
    """Error related to market operations."""

    pass


class PolymarketMarketNotFoundError(PolymarketMarketError):
    """Market not found."""

    def __init__(self, market_id: str):
        self.market_id = market_id
        super().__init__(f"Market not found: {market_id}")


class PolymarketMarketClosedError(PolymarketMarketError):
    """Market is closed for trading."""

    def __init__(self, market_id: str):
        self.market_id = market_id
        super().__init__(f"Market is closed: {market_id}")


class PolymarketMarketNotResolvedError(PolymarketMarketError):
    """Market is not yet resolved."""

    def __init__(self, market_id: str):
        self.market_id = market_id
        super().__init__(f"Market is not resolved: {market_id}")


class PolymarketRedemptionError(PolymarketError):
    """Error during position redemption."""

    pass


class PolymarketSignatureError(PolymarketError):
    """Error creating or verifying signature."""

    pass


__all__ = [
    "PolymarketError",
    "PolymarketAPIError",
    "PolymarketAuthenticationError",
    "PolymarketCredentialsError",
    "PolymarketRateLimitError",
    "PolymarketOrderError",
    "PolymarketOrderNotFoundError",
    "PolymarketInsufficientBalanceError",
    "PolymarketInvalidPriceError",
    "PolymarketInvalidTickSizeError",
    "PolymarketInvalidPrecisionError",
    "PolymarketMinimumOrderError",
    "PolymarketMarketError",
    "PolymarketMarketNotFoundError",
    "PolymarketMarketClosedError",
    "PolymarketMarketNotResolvedError",
    "PolymarketRedemptionError",
    "PolymarketSignatureError",
]
