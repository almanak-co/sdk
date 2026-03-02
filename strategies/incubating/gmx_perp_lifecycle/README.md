# GMX V2 Perp Lifecycle Strategy

Kitchen Loop iteration 27 -- first test of PerpOpenIntent/PerpCloseIntent.

## What it does

Opens and closes a leveraged ETH/USD position on GMX V2 using USDC as collateral.
Tests the complete perp lifecycle: ERC-20 approval -> create order -> execution.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| market | ETH/USD | GMX market to trade |
| collateral_token | USDC | Collateral token (tests ERC-20 path) |
| collateral_amount | 10 | USDC amount per position |
| leverage | 2.0 | Position leverage multiplier |
| is_long | true | Long (true) or short (false) |
| max_slippage_pct | 2.0 | Max slippage percentage |
| force_action | open | "open", "close", "lifecycle", or null |

## Running

```bash
# Test perp open on Anvil
almanak strat run -d strategies/incubating/gmx_perp_lifecycle --network anvil --once

# Test lifecycle (open then close) -- needs --iterations 2 or manual state
# Currently use force_action="open" then force_action="close"
```
