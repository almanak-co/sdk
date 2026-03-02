# Uniswap V3 LP on Polygon

**Iteration 22 - KitchenLoop strategy**

First kitchenloop strategy on Polygon (chain_id=137). Opens a concentrated Uniswap V3 LP position (WETH/USDC, 0.05% fee tier) centered on the current market price.

## Purpose

Validates the entire Polygon chain path end-to-end:
- Anvil fork setup (chain_id=137)
- MATIC/WETH/USDC wallet funding
- Token resolver (bridged WETH at 0x7ceB23fD..., native USDC at 0x3c499c...)
- Uniswap V3 LP compilation and execution
- Receipt parsing and position ID enrichment

## Chain Notes

On Polygon:
- Native gas token: MATIC
- Bridged WETH: `0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619`
- Native USDC (Circle): `0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359`
- Uniswap V3 contracts are identical to Arbitrum/Optimism

## Running

```bash
almanak strat run -d strategies/incubating/uniswap_lp_polygon --network anvil --once
```

## Config

| Parameter | Default | Description |
|-----------|---------|-------------|
| `pool` | `WETH/USDC/500` | Pool specification |
| `range_width_pct` | `0.20` | Price range width (±10% from current) |
| `amount0` | `0.001` | WETH amount to deposit |
| `amount1` | `3` | USDC amount to deposit |
| `force_action` | `open` | Force LP open for testing |
