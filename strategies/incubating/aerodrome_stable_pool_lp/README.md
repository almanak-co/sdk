# Aerodrome Stable Pool LP Strategy

Iteration 23 — Kitchen Loop Phase 1

Opens a USDC/DAI liquidity position in Aerodrome's **stable pool** on Base.

## What This Tests

This is the first kitchenloop strategy to exercise the `pool_type="stable"` code path
in the Aerodrome connector. All prior aerodrome tests used volatile pools (WETH/USDC, 0.3% fee).

Stable pools differ fundamentally:
- Invariant: `x^3*y + x*y^3 = k` (vs `x*y = k` for volatile)
- Fee tier: 0.05% (vs 0.3%)
- Deposit ratio: must be near 1:1 (both tokens are USD-pegged)

## Pool

- **Chain**: Base
- **Protocol**: Aerodrome Finance
- **Pair**: USDC / DAI
- **Pool type**: Stable (`pool_type="stable"`)
- **Pool string**: `USDC/DAI/stable`

## Running

```bash
# Full lifecycle (open + close) — requires 2 iterations
almanak strat run -d strategies/incubating/aerodrome_stable_pool_lp --network anvil --interval 15

# Open only
almanak strat run -d strategies/incubating/aerodrome_stable_pool_lp --network anvil --once
```

## Config Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `pool` | `USDC/DAI` | Token pair |
| `stable` | `true` | Use stable pool invariant |
| `amount0` | `100` | USDC to deposit |
| `amount1` | `100` | DAI to deposit |
| `force_action` | `lifecycle` | `open`, `close`, or `lifecycle` |
| `depeg_threshold` | `0.005` | Close if peg drifts > 0.5% |
