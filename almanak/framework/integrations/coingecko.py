"""CoinGecko SDK wrapper for gateway-backed price data.

This module provides a clean Python API for accessing CoinGecko price and
market data through the gateway. All API calls are proxied through the
gateway, which handles rate limiting and API key management.

Example:
    from almanak.framework.integrations import coingecko

    # Get single token price
    prices = coingecko.get_price("ethereum", vs_currencies=["usd", "eur"])
    print(f"ETH in USD: {prices['usd']}")

    # Get multiple token prices
    prices = coingecko.get_prices(["ethereum", "bitcoin"], vs_currencies=["usd"])
    print(f"ETH: {prices['ethereum']['usd']}, BTC: {prices['bitcoin']['usd']}")

    # Get market data
    markets = coingecko.get_markets(vs_currency="usd", per_page=10)
    for m in markets:
        print(f"{m.name}: ${m.current_price}")
"""

from dataclasses import dataclass
from datetime import datetime

from almanak.framework.gateway_client import get_gateway_client
from almanak.gateway.proto import gateway_pb2


@dataclass
class Market:
    """CoinGecko market data for a token."""

    id: str
    symbol: str
    name: str
    current_price: str
    market_cap: str
    market_cap_rank: int
    total_volume: str
    high_24h: str
    low_24h: str
    price_change_24h: str
    price_change_percentage_24h: str
    last_updated: datetime | None


def get_price(
    token_id: str,
    vs_currencies: list[str] | None = None,
) -> dict[str, str]:
    """Get price for a single token.

    Args:
        token_id: CoinGecko token ID (e.g., "ethereum", "bitcoin")
        vs_currencies: Quote currencies (e.g., ["usd", "eur"]). Defaults to ["usd"]

    Returns:
        Dictionary mapping currency to price as string

    Raises:
        RuntimeError: If gateway not connected
        Exception: On API errors

    Example:
        prices = get_price("ethereum", vs_currencies=["usd", "eur"])
        # Returns: {"usd": "2500.50", "eur": "2300.25"}
    """
    client = get_gateway_client()
    if not client.is_connected:
        raise RuntimeError("Gateway client not connected. Call connect() first.")

    vs_currencies = vs_currencies or ["usd"]

    request = gateway_pb2.CoinGeckoGetPriceRequest(
        token_id=token_id.lower(),
        vs_currencies=vs_currencies,
    )
    response = client.integration.CoinGeckoGetPrice(request)

    return dict(response.prices)


def get_prices(
    token_ids: list[str],
    vs_currencies: list[str] | None = None,
) -> dict[str, dict[str, str]]:
    """Get prices for multiple tokens.

    Args:
        token_ids: List of CoinGecko token IDs
        vs_currencies: Quote currencies. Defaults to ["usd"]

    Returns:
        Dictionary mapping token_id to {currency: price}

    Raises:
        RuntimeError: If gateway not connected
        Exception: On API errors

    Example:
        prices = get_prices(["ethereum", "bitcoin"], vs_currencies=["usd"])
        # Returns: {"ethereum": {"usd": "2500.50"}, "bitcoin": {"usd": "45000.00"}}
    """
    client = get_gateway_client()
    if not client.is_connected:
        raise RuntimeError("Gateway client not connected. Call connect() first.")

    vs_currencies = vs_currencies or ["usd"]

    request = gateway_pb2.CoinGeckoGetPricesRequest(
        token_ids=[t.lower() for t in token_ids],
        vs_currencies=vs_currencies,
    )
    response = client.integration.CoinGeckoGetPrices(request)

    result = {}
    for token in response.tokens:
        result[token.token_id] = dict(token.prices)

    return result


def get_markets(
    vs_currency: str = "usd",
    ids: list[str] | None = None,
    order: str = "market_cap_desc",
    per_page: int = 100,
    page: int = 1,
) -> list[Market]:
    """Get market data with rankings.

    Args:
        vs_currency: Quote currency (e.g., "usd")
        ids: Optional list of token IDs to filter
        order: Sort order (market_cap_desc, volume_desc, etc.)
        per_page: Results per page (max 250)
        page: Page number

    Returns:
        List of Market objects

    Raises:
        RuntimeError: If gateway not connected
        Exception: On API errors

    Example:
        markets = get_markets(vs_currency="usd", per_page=10)
        for m in markets:
            print(f"{m.name}: ${m.current_price} (rank {m.market_cap_rank})")
    """
    client = get_gateway_client()
    if not client.is_connected:
        raise RuntimeError("Gateway client not connected. Call connect() first.")

    request = gateway_pb2.CoinGeckoGetMarketsRequest(
        vs_currency=vs_currency.lower(),
        ids=[i.lower() for i in ids] if ids else [],
        order=order,
        per_page=per_page,
        page=page,
    )
    response = client.integration.CoinGeckoGetMarkets(request)

    markets = []
    for m in response.markets:
        markets.append(
            Market(
                id=m.id,
                symbol=m.symbol,
                name=m.name,
                current_price=m.current_price,
                market_cap=m.market_cap,
                market_cap_rank=m.market_cap_rank,
                total_volume=m.total_volume,
                high_24h=m.high_24h,
                low_24h=m.low_24h,
                price_change_24h=m.price_change_24h,
                price_change_percentage_24h=m.price_change_percentage_24h,
                last_updated=None,  # Could parse if timestamp provided
            )
        )

    return markets
