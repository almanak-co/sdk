# Vault Strategies

Production DeFi strategies with vault integration. Each strategy either wraps
its alpha logic in a Lagoon ERC-7540 vault (config-driven) or interacts directly
with ERC-4626 vaults (MetaMorpho).

## Strategies

| Strategy | Chain | Vault | Maturity |
|----------|-------|-------|----------|
| `metamorpho_eth_yield` | Ethereum | Steakhouse USDC (`0xBEEF...`) | Design Gate |
| `metamorpho_base_yield` | Base | Moonwell Flagship USDC (`0xc1256Ae5...`) | Candidate |
| `lagoon_vault_template` | Base | Lagoon ERC-7540 (config-driven) | Template |

## Maturity Model

**Design Gate** (current): Strategy implements `decide()`, teardown methods,
passes unit tests, and is documented. Can be run on Anvil fork.

**Deployment Gate** (follow-on): Anvil-validated with real vault interaction,
4-layer intent tests pass (VAULT_DEPOSIT, VAULT_REDEEM), stress-tested.
Gets "Anvil-Validated" badge.

## How Vault Wrapping Works

### Pattern A: Lagoon ERC-7540 (config-driven, zero code)

Add a `vault` block to `config.json`. The framework handles settlement
(propose -> settle deposit -> settle redeem) transparently:

```json
{
  "vault": {
    "vault_address": "0x...",
    "valuator_address": "0x...",
    "underlying_token": "USDC",
    "settlement_interval_minutes": 60
  }
}
```

The strategy writes zero vault code -- just `decide()` as usual.

### Pattern B: MetaMorpho ERC-4626 (direct interaction)

Use `Intent.vault_deposit()` and `Intent.vault_redeem()` to interact with
ERC-4626 vaults directly. The MetaMorpho connector handles the RPC calls.

## Running on Anvil

```bash
# MetaMorpho Ethereum (forks Ethereum mainnet)
almanak strat run -d strategies/vaults/metamorpho_eth_yield --network anvil --once

# MetaMorpho Base (forks Base mainnet)
almanak strat run -d strategies/vaults/metamorpho_base_yield --network anvil --once

# Lagoon template (auto-deploys vault on Anvil -- zero manual setup)
almanak strat run -d strategies/vaults/lagoon_vault_template --network anvil --once

# Dry-run mode (skips auto-deploy, prints warning)
almanak strat run -d strategies/vaults/lagoon_vault_template --network anvil --once --dry-run
```

### Lagoon Auto-Deploy on Anvil

When running the Lagoon vault template on Anvil, the framework detects the
placeholder vault address (`0x_DEPLOY_LAGOON_VAULT_FIRST`) and automatically:

1. Deploys a new Lagoon vault via the factory contract
2. Approves the underlying token for vault operations
3. Patches the runtime config with the deployed vault address

This only triggers on `--network anvil` with a placeholder address. Mainnet
requires a pre-deployed vault address in `config.json`.

## Chain Support

- **Ethereum**: MetaMorpho supported
- **Base**: MetaMorpho supported
- **Arbitrum**: Not currently supported by MetaMorpho connector
  (`SUPPORTED_CHAINS = {"ethereum", "base"}` in `morpho_vault/sdk.py`).
  Deferred to future iteration.
