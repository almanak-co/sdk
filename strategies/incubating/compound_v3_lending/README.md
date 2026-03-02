# Compound V3 Lending Lifecycle

Kitchen Loop iteration 28 strategy. First test of the Compound V3 connector
through the intent system.

## What It Does

Tests two Compound V3 (Comet) paths on Ethereum:

1. **Supply** (`force_action=supply`): Lend USDC to the USDC Comet market to earn interest
2. **Borrow** (`force_action=borrow`): Supply WETH as collateral, borrow USDC

## Compound V3 Model

Unlike Aave V3 (unified pool), Compound V3 uses isolated Comet markets:
- Each market has one base asset (e.g., USDC)
- Supply the base asset = lend and earn interest
- Supply collateral + borrow base asset = leveraged position
- Collateral is NOT rehypothecated

## Running

```bash
# Test supply (lend USDC)
almanak strat run -d strategies/incubating/compound_v3_lending --network anvil --once

# Test borrow (supply WETH, borrow USDC) -- edit config.json force_action to "borrow"
```

## Config

| Parameter | Default | Description |
|-----------|---------|-------------|
| supply_token | USDC | Token to lend |
| supply_amount | 1000 | Amount to lend |
| collateral_token | WETH | Collateral for borrowing |
| collateral_amount | 0.5 | Collateral amount |
| borrow_token | USDC | Token to borrow |
| ltv_target | 0.4 | Target LTV (40%) |
| market | usdc | Compound V3 market ID |
| force_action | supply | "supply" or "borrow" |
