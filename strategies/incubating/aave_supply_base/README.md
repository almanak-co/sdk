# Aave V3 Supply on Base

Kitchen Loop iteration 26 strategy (VIB-320).

## What It Does

Supplies USDC to Aave V3 on Base. Monitors supply APY and withdraws if the rate drops below a configurable floor. Re-enters when APY recovers above a re-entry threshold.

## Why It Exists

Validates that the Aave V3 connector is chain-portable (previously only tested on Arbitrum). Also the first test of a rate-triggered WithdrawIntent on Aave.

## Running

```bash
# Single iteration on Anvil fork
almanak strat run -d strategies/incubating/aave_supply_base --network anvil --once

# With forced supply action
# (config.json has force_action: "supply" by default)
```

## Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `supply_token` | USDC | Token to supply |
| `supply_amount` | 100 | Amount to supply |
| `min_apy` | 0.01 | Withdraw threshold (1%) |
| `re_entry_apy` | 0.03 | Re-entry threshold (3%) |
| `force_action` | supply | Force a specific action for testing |
