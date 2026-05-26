# Hyperliquid Connector

This module provides an adapter for interacting with the Hyperliquid perpetual futures exchange.

## Overview

Hyperliquid is a decentralized perpetual futures exchange built on its own L1 blockchain, offering:

- **High Performance**: Sub-second order placement and execution
- **Deep Liquidity**: Substantial market depth across major assets
- **Low Fees**: Competitive fee structure with maker rebates
- **Cross/Isolated Margin**: Flexible margin modes per position
- **Up to 50x Leverage**: High leverage options for qualified traders

## Supported Networks

| Network | API URL | Chain ID |
|---------|---------|----------|
| Mainnet | https://api.hyperliquid.xyz | 1337 |
| Testnet | https://api.hyperliquid-testnet.xyz | 421614 |

## Installation

The connector is part of the Almanak Strategy Framework. No additional installation required.

## Quick Start

```python
from decimal import Decimal
from src.connectors.hyperliquid import (
    HyperliquidAdapter,
    HyperliquidConfig,
    HyperliquidOrderType,
    HyperliquidTimeInForce,
)

# Create configuration
config = HyperliquidConfig(
    network="mainnet",
    wallet_address="0xYourWalletAddress",
    private_key="0xYourPrivateKey",  # For signing orders
)

# Initialize adapter
adapter = HyperliquidAdapter(config)

# Place a limit buy order
result = adapter.place_order(
    asset="ETH",
    is_buy=True,
    size=Decimal("0.1"),
    price=Decimal("2000"),
)

if result.success:
    print(f"Order placed: {result.order_id}")
else:
    print(f"Order failed: {result.error}")
```

## Configuration

### HyperliquidConfig

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `network` | str | Yes | - | `"mainnet"` or `"testnet"` |
| `wallet_address` | str | Yes | - | Ethereum address for the account |
| `private_key` | str | No | None | Private key for signing (can use external signer) |
| `default_slippage_bps` | int | No | 50 | Default slippage tolerance (0.5%) |
| `vault_address` | str | No | None | Optional vault address for vault trading |
| `agent_address` | str | No | None | Optional agent address for delegated trading |

## Order Management

### Placing Orders

```python
# Limit order
result = adapter.place_order(
    asset="ETH",
    is_buy=True,
    size=Decimal("0.1"),
    price=Decimal("2000"),
    order_type=HyperliquidOrderType.LIMIT,
    time_in_force=HyperliquidTimeInForce.GTC,
)

# Market order (uses slippage tolerance)
result = adapter.place_order(
    asset="ETH",
    is_buy=True,
    size=Decimal("0.1"),
    price=Decimal("2000"),  # Reference price for slippage
    order_type=HyperliquidOrderType.MARKET,
    slippage_bps=100,  # 1% slippage
)

# Post-only order
result = adapter.place_order(
    asset="ETH",
    is_buy=True,
    size=Decimal("0.1"),
    price=Decimal("1950"),
    time_in_force=HyperliquidTimeInForce.ALO,  # Add liquidity only
)

# Reduce-only order
result = adapter.place_order(
    asset="ETH",
    is_buy=False,
    size=Decimal("0.1"),
    price=Decimal("2100"),
    reduce_only=True,
)
```

### Order Types

| Type | Description |
|------|-------------|
| `LIMIT` | Standard limit order |
| `MARKET` | Market order (implemented as aggressive limit with slippage) |

### Time in Force

| TIF | Description |
|-----|-------------|
| `GTC` | Good til cancelled (default) |
| `IOC` | Immediate or cancel |
| `ALO` | Add liquidity only (post-only) |

### Canceling Orders

```python
# Cancel by order ID
result = adapter.cancel_order(order_id="order_123")

# Cancel by client ID
result = adapter.cancel_order(client_id="my_cloid")

# Cancel all orders
result = adapter.cancel_all_orders()

# Cancel all orders for specific asset
result = adapter.cancel_all_orders(asset="ETH")
```

### Querying Orders

```python
# Get specific order
order = adapter.get_order("order_123")

# Get all open orders
orders = adapter.get_open_orders()

# Get open orders for specific asset
eth_orders = adapter.get_open_orders(asset="ETH")
```

## Position Management

### Querying Positions

```python
# Get position for specific asset
position = adapter.get_position("ETH")

if position:
    print(f"Size: {position.size}")
    print(f"Entry: {position.entry_price}")
    print(f"PnL: {position.unrealized_pnl}")
    print(f"Side: {position.side.value}")

# Get all positions
positions = adapter.get_all_positions()
```

### Position Properties

| Property | Description |
|----------|-------------|
| `size` | Position size (positive = long, negative = short) |
| `entry_price` | Average entry price |
| `mark_price` | Current mark price |
| `liquidation_price` | Estimated liquidation price |
| `unrealized_pnl` | Unrealized profit/loss |
| `leverage` | Current leverage |
| `side` | Position side (LONG/SHORT/NONE) |
| `notional_value` | abs(size) * mark_price |

### Leverage Management

```python
# Set leverage for an asset
adapter.set_leverage("ETH", 10)  # 10x leverage

# Get current leverage
leverage = adapter.get_leverage("ETH")  # Default: 1
```

## Message Signing

Hyperliquid requires cryptographic signatures for all write operations. The connector supports two signing modes:

### Built-in EIP-712 Signer

When you provide a private key in the config, the adapter uses the built-in EIP-712 signer:

```python
config = HyperliquidConfig(
    network="mainnet",
    wallet_address="0x...",
    private_key="0x...",  # Built-in signer will be used
)
adapter = HyperliquidAdapter(config)
```

### External Signer

For hardware wallets, custodians, or other external signing solutions:

```python
from src.connectors.hyperliquid import ExternalSigner

def my_sign_function(action: dict, nonce: int, is_l1: bool) -> str:
    # Your signing logic here
    # is_l1=True for mainnet L1 actions
    # is_l1=False for testnet L2 actions
    return signature

signer = ExternalSigner(my_sign_function)

config = HyperliquidConfig(
    network="mainnet",
    wallet_address="0x...",
)
adapter = HyperliquidAdapter(config, signer=signer)
```

### L1 vs L2 Signing

| Environment | Signing Scheme | Chain ID |
|-------------|---------------|----------|
| Mainnet | L1 (EIP-712 Agent) | 1337 |
| Testnet | L2 (Simplified) | 421614 |

## Supported Assets

The connector supports 30+ assets including:

- Major pairs: BTC, ETH, SOL, ARB, DOGE
- DeFi tokens: LINK, AAVE, MKR, CRV, LDO
- L1/L2 tokens: AVAX, NEAR, ATOM, APT, SUI
- Meme tokens: PEPE, WIF, BLUR, ORDI

See `HYPERLIQUID_ASSETS` for the complete list with asset indices.

## Error Handling

```python
result = adapter.place_order(...)

if result.success:
    print(f"Success: {result.order_id}")
else:
    print(f"Error: {result.error}")
```

Common errors:
- "Unknown asset" - Asset not supported
- "Order size must be positive" - Invalid order size
- "Order not found" - Cancel target doesn't exist

## Testing

Run the test suite:

```bash
cd /path/to/stack-v2
python -m pytest src/connectors/hyperliquid/tests/ -v
```

## API Reference

### Classes

- `HyperliquidAdapter` - Main adapter class
- `HyperliquidConfig` - Configuration dataclass
- `HyperliquidPosition` - Position dataclass
- `HyperliquidOrder` - Order dataclass
- `OrderResult` - Order operation result
- `CancelResult` - Cancel operation result
- `EIP712Signer` - Built-in EIP-712 message signer
- `ExternalSigner` - Wrapper for external signing functions

### Enums

- `HyperliquidNetwork` - mainnet/testnet
- `HyperliquidOrderType` - LIMIT/MARKET
- `HyperliquidOrderSide` - BUY/SELL
- `HyperliquidOrderStatus` - OPEN/FILLED/CANCELLED/etc.
- `HyperliquidPositionSide` - LONG/SHORT/NONE
- `HyperliquidTimeInForce` - GTC/IOC/ALO
- `HyperliquidMarginMode` - CROSS/ISOLATED

### Constants

- `HYPERLIQUID_API_URLS` - REST API endpoints
- `HYPERLIQUID_WS_URLS` - WebSocket endpoints
- `HYPERLIQUID_CHAIN_IDS` - Chain IDs per network
- `HYPERLIQUID_ASSETS` - Asset name to index mapping

## Resources

- [Hyperliquid Documentation](https://hyperliquid.gitbook.io/hyperliquid-docs)
- [Hyperliquid API Reference](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api)
- [EIP-712 Specification](https://eips.ethereum.org/EIPS/eip-712)
