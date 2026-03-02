# GMX V2

Connector for GMX V2 perpetual futures on Arbitrum and Avalanche.

## Overview

GMX V2 is a decentralized perpetuals exchange that supports leveraged long and short positions
on major crypto assets. The Almanak SDK integrates GMX V2 through the intent system, supporting
`PERP_OPEN` and `PERP_CLOSE` operations.

## Market Format

GMX V2 markets use a **slash separator**: `"BTC/USD"`, `"ETH/USD"`, `"LINK/USD"`.

```python
Intent.perp_open(
    market="ETH/USD",       # Slash separator, not dash
    collateral_token="USDC",
    collateral_amount=Decimal("1000"),
    size_usd=Decimal("5000"),
    is_long=True,
    leverage=Decimal("5"),
    protocol="gmx_v2",
)
```

## Supported Operations

| Intent | Description |
|--------|-------------|
| `Intent.perp_open()` | Open a leveraged long or short position |
| `Intent.perp_close()` | Close an existing position (full or partial) |

## Keeper Execution Model

GMX V2 uses a **two-step execution model**:

1. **Order creation**: Your transaction submits an order to the GMX exchange router. This is the
   transaction the SDK signs and submits.
2. **Keeper execution**: A GMX keeper bot picks up the order and executes the actual position
   change in a separate transaction.

**Important implications for strategies:**

- `on_intent_executed(success=True)` fires when the order creation TX confirms (step 1),
  **not** when the keeper executes the position (step 2).
- There is a delay (typically a few seconds) between order creation and keeper execution.
- `get_all_positions()` may not reflect the new position immediately after `on_intent_executed`
  fires. Poll position state before relying on it.

## Minimum Position Size

GMX V2 enforces a minimum position size of approximately **$11 net of fees**. Orders below this
threshold are silently rejected by the keeper with no on-chain error. The order creation TX will
still succeed, but the keeper will not execute the position.

## Collateral Tokens

Collateral tokens vary by chain:

| Chain | Supported Collateral |
|-------|---------------------|
| Arbitrum | USDC, USDT |
| Avalanche | USDC, USDT |

Collateral token approvals are handled automatically by the intent compiler.

## Known Limitations

- **Keeper delay**: Position state is not immediately available after order creation. Allow a
  few seconds before querying positions.
- **Silent rejections**: Orders below the minimum size are rejected without an on-chain error.
  Verify position creation by checking position state after the keeper delay.
- **Price impact**: Large positions relative to pool open interest may experience significant
  price impact. GMX V2 uses a price impact model that charges more for positions that increase
  imbalance.

## API Reference

::: almanak.framework.connectors.gmx_v2
    options:
      show_root_heading: true
      members_order: source
