# Velodrome LP Strategy (Optimism)

Kitchen Loop Iteration 24 (VIB-314).

## Purpose

Tests portability of the Aerodrome/Solidly connector from Base to Optimism.
Velodrome V2 is the canonical Solidly fork on Optimism -- same contract
interfaces as Aerodrome on Base, different deployment addresses.

## Key Questions

1. Does `protocol="aerodrome"` work on Optimism (chain-agnostic)?
2. Are Velodrome contract addresses registered in the framework?
3. Does the full LP lifecycle (LP_OPEN + LP_CLOSE) succeed on Optimism?

## Configuration

- **Chain**: Optimism
- **Pool**: WETH/USDC volatile
- **Protocol**: aerodrome (Solidly fork)
- **Mode**: lifecycle (open -> close -> hold)

## Run

```bash
almanak strat run -d strategies/incubating/velodrome_lp_optimism --network anvil --once
```

## Velodrome V2 Addresses (Optimism)

| Contract | Address |
|----------|---------|
| Router   | `0xa062aE8A9c5e11aaA026fc2670B0D65cCc8B2858` |
| Factory  | `0xF1046053aa5682b4F9a81b5481394DA16BE5FF5a` |
| Voter    | `0x41C914ee0c7E1A5edCD0295623e6dC557B5aBf3C` |
