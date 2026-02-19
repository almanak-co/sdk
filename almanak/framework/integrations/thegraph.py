"""TheGraph SDK wrapper for gateway-backed subgraph queries.

This module provides a clean Python API for querying TheGraph subgraphs
through the gateway. Queries are proxied through the gateway, which handles
rate limiting and restricts access to allowlisted subgraphs.

Example:
    from almanak.framework.integrations import thegraph

    # Query Uniswap V3 pools
    result = thegraph.query(
        subgraph_id="uniswap-v3-arbitrum",
        query='''
        {
            pools(first: 10, orderBy: totalValueLockedUSD, orderDirection: desc) {
                id
                token0 { symbol }
                token1 { symbol }
                totalValueLockedUSD
            }
        }
        ''',
    )

    if result.success:
        for pool in result.data["pools"]:
            print(f"{pool['token0']['symbol']}/{pool['token1']['symbol']}")

    # Query with variables
    result = thegraph.query(
        subgraph_id="aave-v3-arbitrum",
        query="query($first: Int!) { reserves(first: $first) { symbol } }",
        variables={"first": 5},
    )
"""

import json
from dataclasses import dataclass
from typing import Any

from almanak.framework.gateway_client import get_gateway_client
from almanak.gateway.proto import gateway_pb2


@dataclass
class QueryResult:
    """Result of a TheGraph query."""

    data: dict[str, Any] | None
    errors: list[dict[str, Any]]
    success: bool

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from the data dictionary.

        Args:
            key: Key to get from data
            default: Default value if key not found

        Returns:
            Value or default
        """
        if self.data is None:
            return default
        return self.data.get(key, default)


# Commonly used subgraph aliases
SUBGRAPH_ALIASES = {
    # Uniswap V3
    "uniswap-v3-ethereum": "uniswap-v3-ethereum",
    "uniswap-v3-arbitrum": "uniswap-v3-arbitrum",
    "uniswap-v3-optimism": "uniswap-v3-optimism",
    "uniswap-v3-polygon": "uniswap-v3-polygon",
    "uniswap-v3-base": "uniswap-v3-base",
    # Aave V3
    "aave-v3-ethereum": "aave-v3-ethereum",
    "aave-v3-arbitrum": "aave-v3-arbitrum",
    "aave-v3-optimism": "aave-v3-optimism",
    "aave-v3-polygon": "aave-v3-polygon",
    # Curve
    "curve-ethereum": "curve-ethereum",
    "curve-arbitrum": "curve-arbitrum",
    # Balancer
    "balancer-v2-ethereum": "balancer-v2-ethereum",
    "balancer-v2-arbitrum": "balancer-v2-arbitrum",
}


def query(
    subgraph_id: str,
    query: str,
    variables: dict[str, Any] | None = None,
) -> QueryResult:
    """Execute a GraphQL query on a subgraph.

    Args:
        subgraph_id: Subgraph ID or name (see SUBGRAPH_ALIASES for common names)
        query: GraphQL query string
        variables: Optional query variables

    Returns:
        QueryResult with data, errors, and success flag

    Raises:
        RuntimeError: If gateway not connected
        Exception: On network errors

    Example:
        result = query(
            subgraph_id="uniswap-v3-arbitrum",
            query='''
            {
                pools(first: 5) {
                    id
                    token0 { symbol }
                    token1 { symbol }
                }
            }
            ''',
        )
        if result.success:
            pools = result.get("pools", [])
    """
    client = get_gateway_client()
    if not client.is_connected:
        raise RuntimeError("Gateway client not connected. Call connect() first.")

    # Serialize variables to JSON if provided
    variables_json = json.dumps(variables) if variables else ""

    request = gateway_pb2.TheGraphQueryRequest(
        subgraph_id=subgraph_id,
        query=query,
        variables=variables_json,
    )
    response = client.integration.TheGraphQuery(request)

    # Parse data and errors from JSON
    data = json.loads(response.data) if response.data else None
    errors = json.loads(response.errors) if response.errors else []

    return QueryResult(
        data=data,
        errors=errors,
        success=response.success,
    )


def list_subgraphs() -> list[str]:
    """List available subgraph aliases.

    Returns:
        List of subgraph names that can be used with query()

    Example:
        for name in list_subgraphs():
            print(name)
    """
    return list(SUBGRAPH_ALIASES.keys())


# Convenience functions for common queries


def get_uniswap_pools(
    chain: str = "arbitrum",
    first: int = 10,
    order_by: str = "totalValueLockedUSD",
) -> list[dict[str, Any]]:
    """Get top Uniswap V3 pools.

    Args:
        chain: Chain name (arbitrum, ethereum, optimism, polygon, base)
        first: Number of pools to return
        order_by: Field to order by (totalValueLockedUSD, volumeUSD, etc.)

    Returns:
        List of pool dictionaries

    Example:
        pools = get_uniswap_pools(chain="arbitrum", first=10)
        for pool in pools:
            print(f"{pool['token0']['symbol']}/{pool['token1']['symbol']}")
    """
    subgraph_id = f"uniswap-v3-{chain}"

    result = query(
        subgraph_id=subgraph_id,
        query=f"""
        {{
            pools(
                first: {first},
                orderBy: {order_by},
                orderDirection: desc
            ) {{
                id
                token0 {{ id symbol name }}
                token1 {{ id symbol name }}
                feeTier
                liquidity
                sqrtPrice
                tick
                totalValueLockedUSD
                volumeUSD
            }}
        }}
        """,
    )

    if result.success:
        return result.get("pools", [])
    return []


def get_aave_reserves(
    chain: str = "arbitrum",
    first: int = 20,
) -> list[dict[str, Any]]:
    """Get Aave V3 reserves/markets.

    Args:
        chain: Chain name (arbitrum, ethereum, optimism, polygon)
        first: Number of reserves to return

    Returns:
        List of reserve dictionaries

    Example:
        reserves = get_aave_reserves(chain="arbitrum")
        for r in reserves:
            print(f"{r['symbol']}: supply APY {r['supplyAPY']}")
    """
    subgraph_id = f"aave-v3-{chain}"

    result = query(
        subgraph_id=subgraph_id,
        query=f"""
        {{
            reserves(first: {first}) {{
                id
                symbol
                name
                decimals
                totalLiquidity
                totalCurrentVariableDebt
                totalScaledVariableDebt
                liquidityRate
                variableBorrowRate
                stableBorrowRate
                availableLiquidity
                utilizationRate
                price {{
                    priceInEth
                }}
            }}
        }}
        """,
    )

    if result.success:
        return result.get("reserves", [])
    return []
