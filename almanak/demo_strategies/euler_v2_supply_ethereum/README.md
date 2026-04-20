# Euler V2 Supply Lifecycle on Ethereum

Euler V2 supply/withdraw lifecycle on Ethereum mainnet using the eUSDC-2 vault.

## What it does

1. **SUPPLY**: Deposit USDC into the Euler V2 eUSDC-2 vault (ERC-4626 deposit)
2. **WITHDRAW**: Withdraw USDC from the vault
3. **HOLD**: Lifecycle complete

## Run

```bash
almanak strat run -d almanak/demo_strategies/euler_v2_supply_ethereum --network anvil --once
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `supply_token` | USDC | Token to supply |
| `supply_amount` | 1000 | Amount to supply |

## Protocol Details

- **Protocol**: Euler V2 (ERC-4626 vaults + EVC)
- **Chain**: Ethereum (chain_id=1)
- **Vault**: eUSDC-2 (0x797DD80692c3b2dAdabCe8e30C07fDE5307D48a9)
