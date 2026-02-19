"""Binance SDK wrapper for gateway-backed market data.

This module provides a clean Python API for accessing Binance market data
through the gateway. All API calls are proxied through the gateway, which
handles rate limiting and keeps API keys secure.

Example:
    from almanak.framework.integrations import binance

    # Get 24h ticker
    ticker = binance.get_ticker("BTCUSDT")
    print(f"BTC price: {ticker.price}")

    # Get OHLCV data
    klines = binance.get_klines("ETHUSDT", interval="1h", limit=100)
    for k in klines:
        print(f"{k.timestamp}: close={k.close}")

    # Get order book
    order_book = binance.get_order_book("BTCUSDT", limit=20)
    print(f"Best bid: {order_book.bids[0].price}")
"""

from dataclasses import dataclass
from datetime import UTC, datetime

from almanak.framework.gateway_client import get_gateway_client
from almanak.gateway.proto import gateway_pb2


@dataclass
class Ticker:
    """Binance 24h ticker data."""

    symbol: str
    price: str
    price_change: str
    price_change_percent: str
    high_24h: str
    low_24h: str
    volume_24h: str
    quote_volume_24h: str
    timestamp: datetime


@dataclass
class Kline:
    """Binance kline/candlestick data."""

    open_time: datetime
    open: str
    high: str
    low: str
    close: str
    volume: str
    close_time: datetime
    quote_volume: str
    trades: int


@dataclass
class OrderBookEntry:
    """Order book entry (bid or ask)."""

    price: str
    quantity: str


@dataclass
class OrderBook:
    """Binance order book."""

    last_update_id: int
    bids: list[OrderBookEntry]
    asks: list[OrderBookEntry]


def get_ticker(symbol: str) -> Ticker:
    """Get 24h ticker for a trading pair.

    Args:
        symbol: Trading pair (e.g., "BTCUSDT", "ETHUSDT")

    Returns:
        Ticker with price and 24h statistics

    Raises:
        RuntimeError: If gateway not connected
        Exception: On API errors
    """
    client = get_gateway_client()
    if not client.is_connected:
        raise RuntimeError("Gateway client not connected. Call connect() first.")

    request = gateway_pb2.BinanceTickerRequest(symbol=symbol.upper())
    response = client.integration.BinanceGetTicker(request)

    return Ticker(
        symbol=response.symbol,
        price=response.price,
        price_change=response.price_change,
        price_change_percent=response.price_change_percent,
        high_24h=response.high_24h,
        low_24h=response.low_24h,
        volume_24h=response.volume_24h,
        quote_volume_24h=response.quote_volume_24h,
        timestamp=datetime.fromtimestamp(response.timestamp / 1000, tz=UTC)
        if response.timestamp
        else datetime.now(UTC),
    )


def get_klines(
    symbol: str,
    interval: str = "1h",
    limit: int = 100,
    start_time: int | None = None,
    end_time: int | None = None,
) -> list[Kline]:
    """Get kline/candlestick data.

    Args:
        symbol: Trading pair (e.g., "BTCUSDT")
        interval: Kline interval (1m, 5m, 15m, 1h, 4h, 1d, etc.)
        limit: Number of klines (max 1000)
        start_time: Start time in milliseconds (optional)
        end_time: End time in milliseconds (optional)

    Returns:
        List of Kline objects

    Raises:
        RuntimeError: If gateway not connected
        ValueError: On invalid parameters
        Exception: On API errors
    """
    client = get_gateway_client()
    if not client.is_connected:
        raise RuntimeError("Gateway client not connected. Call connect() first.")

    request = gateway_pb2.BinanceKlinesRequest(
        symbol=symbol.upper(),
        interval=interval,
        limit=limit,
        start_time=start_time or 0,
        end_time=end_time or 0,
    )
    response = client.integration.BinanceGetKlines(request)

    klines = []
    for k in response.klines:
        klines.append(
            Kline(
                open_time=datetime.fromtimestamp(k.open_time / 1000, tz=UTC),
                open=k.open,
                high=k.high,
                low=k.low,
                close=k.close,
                volume=k.volume,
                close_time=datetime.fromtimestamp(k.close_time / 1000, tz=UTC),
                quote_volume=k.quote_volume,
                trades=k.trades,
            )
        )

    return klines


def get_order_book(symbol: str, limit: int = 100) -> OrderBook:
    """Get order book depth.

    Args:
        symbol: Trading pair (e.g., "BTCUSDT")
        limit: Depth limit (5, 10, 20, 50, 100, 500, 1000)

    Returns:
        OrderBook with bids and asks

    Raises:
        RuntimeError: If gateway not connected
        Exception: On API errors
    """
    client = get_gateway_client()
    if not client.is_connected:
        raise RuntimeError("Gateway client not connected. Call connect() first.")

    request = gateway_pb2.BinanceOrderBookRequest(
        symbol=symbol.upper(),
        limit=limit,
    )
    response = client.integration.BinanceGetOrderBook(request)

    bids = [OrderBookEntry(price=b.price, quantity=b.quantity) for b in response.bids]
    asks = [OrderBookEntry(price=a.price, quantity=a.quantity) for a in response.asks]

    return OrderBook(
        last_update_id=response.last_update_id,
        bids=bids,
        asks=asks,
    )
