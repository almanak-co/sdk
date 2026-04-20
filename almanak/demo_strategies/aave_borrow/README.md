# Aave V3 Borrow Strategy (Demo)

A tutorial strategy demonstrating how to supply collateral and borrow on Aave V3.

## What This Strategy Does

1. **Supply collateral** (e.g., WETH) to Aave V3
2. **Borrow** against it (e.g., USDC) at a target LTV

This is a simple supply-and-borrow strategy. For a full looping strategy that swaps borrowed tokens back to collateral and re-supplies, see the `leverage_loop_cross_chain` example.

## Key Concepts

### Loan-to-Value (LTV)

LTV = Borrow Value / Collateral Value

- Higher LTV = more leverage, more risk
- Max LTV varies by asset (typically 70-85% for major assets)
- This strategy uses 50% LTV for safety

### Health Factor

Health Factor = (Collateral Value x Liquidation Threshold) / Borrow Value

- **HF > 1.0**: Safe
- **HF = 1.0**: Liquidatable
- **HF < 1.0**: Being liquidated

This strategy maintains minimum HF of 2.0 for safety margin.

### Interest Rate Modes

Aave V3 offers two borrowing modes:
- **Variable**: Rate fluctuates with market conditions (usually cheaper)
- **Stable**: Fixed rate at a premium (predictable costs)

## Quick Start

### Test on Anvil (Recommended)

```bash
# Prerequisites: Foundry installed, RPC URL in .env

# Run with default settings
python strategies/demo/aave_borrow/run_anvil.py
```

> **Tip: Funding the Anvil Wallet**
>
> If using Claude Code, ask it to fund your wallet with the required tokens:
> ```
> "cast send 0.1 WETH to Anvil wallet on Arbitrum"
> ```
> Claude Code will use `anvil_setStorageAt` to set token balances for testing.

### Run with CLI

```bash
# Set required environment variables
export ALMANAK_CHAIN=arbitrum
export ALMANAK_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY
export ALMANAK_PRIVATE_KEY=0x...

# Run once
almanak strat run --once

# Run continuously
almanak strat run
```

## Configuration

Edit `config.json` to customize:

```json
{
    "collateral_token": "WETH",        // Token to supply as collateral
    "collateral_amount": "0.002",      // Amount to supply
    "borrow_token": "USDC",            // Token to borrow
    "ltv_target": 0.5,                 // Target LTV (50%)
    "min_health_factor": 2.0,          // Minimum HF to maintain
    "interest_rate_mode": "variable"   // only "variable" (stable deprecated)
}
```

## How It Works

### State Machine

The strategy uses a simple state machine:

1. **IDLE** -> Supply collateral
2. **SUPPLIED** -> Borrow against collateral
3. **COMPLETE** -> Hold (position established)

State is persisted, so the strategy can resume after crash/restart.

### 1. Supply Collateral

```python
def _create_supply_intent(self):
    return Intent.supply(
        protocol="aave_v3",
        token="WETH",
        amount=Decimal("0.002"),
        use_as_collateral=True,
        chain="arbitrum",
    )
```

### 2. Borrow Against Collateral

```python
def _create_borrow_intent(self, collateral_price, borrow_price):
    collateral_value = self.collateral_amount * collateral_price
    max_borrow_value = collateral_value * self.ltv_target
    borrow_amount = max_borrow_value / borrow_price

    return Intent.borrow(
        protocol="aave_v3",
        collateral_token="WETH",
        collateral_amount=Decimal("0"),  # Already supplied
        borrow_token="USDC",
        borrow_amount=borrow_amount,
        interest_rate_mode="variable",
        chain="arbitrum",
    )
```

## Risk Management

### Liquidation Risk

If collateral value drops below borrowed amount (adjusted for liquidation threshold), your position can be liquidated.

**Mitigation:**
- Use conservative LTV (this strategy uses 50%)
- Monitor health factor (maintain > 1.5)
- Set up alerts for price drops

### Interest Rate Risk

Variable rates can spike during high-demand periods.

**Mitigation:**
- Use stable rates for predictability
- Monitor borrow APY

## File Structure

```
strategies/demo/aave_borrow/
├── __init__.py      # Package exports
├── strategy.py      # Main strategy logic (with tutorial comments)
├── config.json      # Default configuration
├── run_anvil.py     # Test script using Anvil
└── README.md        # This file
```

## Intent Types Used

- **SUPPLY**: Deposit tokens into Aave lending pool
- **BORROW**: Borrow tokens against deposited collateral
- **HOLD**: No action needed

## Limitations

This is a **demo strategy** for educational purposes:

- No automatic position monitoring
- No de-leveraging logic
- No health factor monitoring
- Real strategies need comprehensive risk management

## References

- [Aave V3 Documentation](https://docs.aave.com/developers/v/2.0/)
- [Aave Risk Framework](https://docs.aave.com/risk/)
- [Understanding Health Factor](https://docs.aave.com/faq/borrowing#what-is-the-health-factor)
