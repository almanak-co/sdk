# Balancer Flash Arb (Demo)

Demo: Balancer flash loan with Enso swap callbacks on Arbitrum

## Chain

arbitrum

## Quick Start

```bash
almanak strat demo --name balancer_flash_arb
cd balancer_flash_arb
almanak strat run --network anvil --once
```

## Configuration

Edit `config.json` to adjust strategy parameters. The demo action path uses
Balancer flash-loan compilation with Enso swap callbacks; `teardown_protocol`
controls only the closing swap that exits the fallback WETH position during
teardown. It defaults to `uniswap_v3` so the teardown path does not depend on a
fresh Enso route.
