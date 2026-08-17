# GMX v2 Connector

This module provides an adapter for interacting with the GMX v2 perpetuals protocol on Arbitrum and Avalanche.

## Overview

GMX v2 is a decentralized perpetual exchange supporting:
- Long and short positions with leverage
- Multiple collateral types
- Market and limit orders
- Stop-loss orders
- Position management (increase/decrease)

## Installation

The GMX v2 connector is part of the Almanak Strategy Framework. No additional installation is required.

## Quick Start

```python
from almanak.connectors.gmx_v2 import GMXv2Adapter, GMXv2Config
from decimal import Decimal

# Obtain this with:
# almanak ax -c arbitrum perp-market gmx_v2 ETH/USD
market_token = "<verified GMX market-token address>"

# Initialize the adapter
config = GMXv2Config(
    chain="arbitrum",
    wallet_address="0x...",
)
adapter = GMXv2Adapter(config)

# Open a long position
result = adapter.open_position(
    market=market_token,
    collateral_token="USDC",
    collateral_amount=Decimal("1000"),  # $1000 USDC collateral
    size_delta_usd=Decimal("5000"),      # $5000 position size (5x leverage)
    is_long=True,
)

if result.success:
    print(f"Order created: {result.order_key}")

# Check position
position = adapter.get_position(
    market=market_token,
    collateral_token="USDC",
    is_long=True,
)

if position:
    print(f"Position size: ${position.size_in_usd}")
    print(f"Unrealized PnL: ${position.unrealized_pnl}")

# Close the position
close_result = adapter.close_position(
    market=market_token,
    collateral_token="USDC",
    is_long=True,
    size_delta_usd=position.size_in_usd,
)
```

## Configuration

### GMXv2Config

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `chain` | str | Required | Target chain ("arbitrum" or "avalanche") |
| `wallet_address` | str | Required | Wallet address for transactions |
| `execution_fee` | int | Auto | Execution fee in native token wei |
| `referral_code` | bytes | 0x00...00 | Referral code for fee discounts |

## Markets

The connector has no hand-maintained supported-market list. On Arbitrum and
Avalanche, live compilation resolves a market-token address or exact venue
label through GMX's current catalogue, then accepts it only when
`Reader.getMarket` returns the same market/index/long/short address tuple.
Collateral symbols and decimals come from that verified record as well.

Use `almanak ax -c <chain> perp-market gmx_v2 <market>` to inspect a live market.
An unavailable gateway is inconclusive, not evidence that GMX does not support
the market.

The intent compiler accepts exact venue labels because it owns gateway
discovery. A directly constructed adapter has no discovery transport: in a
fresh process it requires the verified market-token address shown above. It may
reuse a label only after the compiler has verified and remembered exactly one
matching market in that process.

## API Reference

### Position Management

#### `open_position()`
Open a new position or increase an existing one.

```python
result = adapter.open_position(
    market=market_token,         # Verified market-token address
    collateral_token="USDC",    # Collateral token symbol or address
    collateral_amount=Decimal("1000"),
    size_delta_usd=Decimal("5000"),
    is_long=True,
    acceptable_price=None,      # Optional: max price for longs, min for shorts
    trigger_price=None,         # Optional: creates limit order if provided
)
```

#### `close_position()`
Close a position or decrease its size.

```python
result = adapter.close_position(
    market=market_token,
    collateral_token="USDC",
    is_long=True,
    size_delta_usd=Decimal("5000"),  # None = close entire position
    receive_token=None,               # Token to receive (defaults to collateral)
    acceptable_price=None,
    trigger_price=None,
)
```

#### `increase_position()`
Add to an existing position.

```python
result = adapter.increase_position(
    market=market_token,
    collateral_token="USDC",
    is_long=True,
    collateral_delta=Decimal("500"),
    size_delta_usd=Decimal("2500"),
)
```

#### `decrease_position()`
Partially close a position.

```python
result = adapter.decrease_position(
    market=market_token,
    collateral_token="USDC",
    is_long=True,
    size_delta_usd=Decimal("2500"),
)
```

### Order Management

#### `cancel_order()`
Cancel a pending order.

```python
result = adapter.cancel_order(order_key)
```

#### `get_order()`
Get order details.

```python
order = adapter.get_order(order_key)
```

### Data Classes

#### GMXv2Position
```python
@dataclass
class GMXv2Position:
    position_key: str
    market: str
    collateral_token: str
    size_in_usd: Decimal        # Position size in USD
    size_in_tokens: Decimal     # Position size in index tokens
    collateral_amount: Decimal  # Collateral amount
    entry_price: Decimal        # Average entry price
    is_long: bool               # Position direction
    unrealized_pnl: Decimal     # Unrealized PnL
    leverage: Decimal           # Current leverage
    liquidation_price: Decimal  # Liquidation price
```

#### GMXv2Order
```python
@dataclass
class GMXv2Order:
    order_key: str
    market: str
    order_type: GMXv2OrderType  # MARKET_INCREASE, LIMIT_INCREASE, etc.
    is_long: bool
    size_delta_usd: Decimal
    trigger_price: Optional[Decimal]
    acceptable_price: Optional[Decimal]
```

## Receipt Parsing

Parse transaction receipts to extract GMX v2 events:

```python
from src.connectors.gmx_v2 import GMXv2ReceiptParser, GMXv2EventType

parser = GMXv2ReceiptParser()

# Parse a transaction receipt
result = parser.parse_receipt(receipt)

if result.success:
    for event in result.events:
        if event.event_type == GMXv2EventType.POSITION_INCREASE:
            print(f"Position increased: {event.data}")
        elif event.event_type == GMXv2EventType.ORDER_EXECUTED:
            print(f"Order executed: {event.data}")
```

### Supported Events
- ORDER_CREATED
- ORDER_EXECUTED
- ORDER_CANCELLED
- ORDER_FROZEN
- POSITION_INCREASE
- POSITION_DECREASE
- DEPOSIT_CREATED
- DEPOSIT_EXECUTED
- WITHDRAWAL_CREATED
- WITHDRAWAL_EXECUTED

## Error Handling

All methods return result objects with success status and error messages:

```python
result = adapter.open_position(...)

if not result.success:
    print(f"Error: {result.error}")
else:
    print(f"Success: {result.order_key}")
```

## Gas Estimates

The adapter provides gas estimates for all operations:

| Operation | Gas Estimate |
|-----------|--------------|
| Create increase order | 800,000 |
| Create decrease order | 600,000 |
| Cancel order | 200,000 |
| Claim funding fees | 300,000 |

## Testing

Run the test suite:

```bash
pytest src/connectors/gmx_v2/tests/ -v
```

## Contract Addresses (Updated Oct 2025)

### Arbitrum
- Exchange Router: `0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41`
- Data Store: `0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8`
- Reader: `0xf60becbba223EEA9495Da3f606753867eC10d139`

### Avalanche
- Exchange Router: `0x8f550E53DFe96C055D5Bdb267c21F268fCAF63B2`
- Data Store: `0x2F0b22339414ADeD7D5F06f9D604c7fF5b2fe3f6`
- Reader: `0x2eFEE1950ededC65De687b40Fd30a7B5f4544aBd`
- SyntheticsReader: `0x62Cb8740E6986B29dC671B2EB596676f60590A5B`

## Resources

- [GMX v2 Documentation](https://docs.gmx.io/)
- [GMX v2 Contracts](https://github.com/gmx-io/gmx-synthetics)
- [GMX v2 Interface](https://app.gmx.io/)
