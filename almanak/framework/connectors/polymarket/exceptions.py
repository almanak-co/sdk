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
    """Order size below minimum."""

    def __init__(self, size: str, minimum: str):
        self.size = size
        self.minimum = minimum
        super().__init__(f"Order size {size} below minimum {minimum}")


class PolymarketInvalidTickSizeError(PolymarketOrderError):
    """Price does not conform to market tick size.

    Polymarket markets have specific tick sizes (e.g., 0.01, 0.001) that
    prices must be multiples of. This error is raised when a price cannot
    be rounded to a valid tick.
    """

    def __init__(self, price: str, tick_size: str, nearest_valid: str | None = None):
        self.price = price
        self.tick_size = tick_size
        self.nearest_valid = nearest_valid
        if nearest_valid:
            super().__init__(f"Price {price} is not a valid tick: tick_size={tick_size}, nearest valid={nearest_valid}")
        else:
            super().__init__(f"Price {price} is not a valid tick: tick_size={tick_size}")


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
    "PolymarketMinimumOrderError",
    "PolymarketMarketError",
    "PolymarketMarketNotFoundError",
    "PolymarketMarketClosedError",
    "PolymarketMarketNotResolvedError",
    "PolymarketRedemptionError",
    "PolymarketSignatureError",
]
