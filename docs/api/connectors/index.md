# Connectors

Protocol connectors provide adapters for interacting with DeFi protocols. Each connector includes an SDK (low-level interactions), an adapter (standard interface), and a receipt parser.

The matrix below is generated from ``almanak.connectors._strategy_base.registry.ConnectorRegistry``. To regenerate, run ``uv run python scripts/docs/generate_connector_matrix.py --apply``.

## Supported Protocols

<!-- generated:connector-matrix:begin -->
| Protocol | Type | Chains | Intent Types | Module |
|----------|------|--------|--------------|--------|
| [Aave V3](aave_v3.md) | Lending | Arbitrum, Avalanche, Base, BNB Chain, Ethereum, Mantle, Optimism, Polygon, X-Layer | BORROW, FLASH_LOAN, REPAY, SUPPLY, WITHDRAW | `aave_v3` |
| [Across](across.md) | Bridge | Arbitrum, Base, Ethereum, Optimism, Polygon | BRIDGE | `across` |
| [Aerodrome](aerodrome.md) | DEX | Base, Optimism | LP_CLOSE, LP_OPEN, SWAP | `aerodrome` |
| [Aster Perps](aster_perps.md) | Perp | BNB Chain | PERP_CLOSE, PERP_OPEN | `aster_perps` |
| Balancer V2 | Flash Loan | Arbitrum, Avalanche, Base, Ethereum, Optimism, Polygon | FLASH_LOAN | `balancer_v2` |
| [Benqi](benqi.md) | Lending | Avalanche | BORROW, REPAY, SUPPLY, WITHDRAW | `benqi` |
| [Camelot](camelot.md) | DEX | Arbitrum | SWAP | `camelot` |
| [Compound V3](compound_v3.md) | Lending | Arbitrum, Base, Ethereum, Optimism, Polygon | BORROW, REPAY, SUPPLY, WITHDRAW | `compound_v3` |
| [Curvance](curvance.md) | Lending | Monad | BORROW, REPAY, SUPPLY, WITHDRAW | `curvance` |
| [Curve](curve.md) | DEX | Arbitrum, Base, Ethereum, Optimism, Polygon | LP_CLOSE, LP_OPEN, SWAP | `curve` |
| [Drift](drift.md) | Perp | Solana | PERP_CLOSE, PERP_OPEN | `drift` |
| [Enso](enso.md) | DEX | Arbitrum, Avalanche, Base, BNB Chain, Ethereum, Optimism, Polygon | SWAP | `enso` |
| [Ethena](ethena.md) | Staking | Ethereum | STAKE, UNSTAKE | `ethena` |
| [Euler V2](euler_v2.md) | Lending | Avalanche, Ethereum | BORROW, REPAY, SUPPLY, WITHDRAW | `euler_v2` |
| [Fluid](fluid.md) | DEX | Arbitrum | LP_CLOSE, LP_OPEN, SWAP | `fluid` |
| [Gimo](gimo.md) | Staking | 0G | STAKE, UNSTAKE | `gimo` |
| [GMX V2](gmx_v2.md) | Perp | Arbitrum, Avalanche | PERP_CLOSE, PERP_OPEN | `gmx_v2` |
| [Jupiter](jupiter.md) | DEX | Solana | SWAP | `jupiter` |
| [Kamino](kamino.md) | Lending | Solana | BORROW, REPAY, SUPPLY, WITHDRAW | `kamino` |
| [Kraken](kraken.md) | DEX | N/A | SWAP | `kraken` |
| [Lagoon](lagoon.md) | Vault | Base, Ethereum | VAULT_DEPOSIT, VAULT_REDEEM | `lagoon` |
| [Lido](lido.md) | Staking | Ethereum | STAKE, UNSTAKE | `lido` |
| [LiFi](lifi.md) | DEX | Arbitrum, Avalanche, Base, BNB Chain, Ethereum, Optimism, Polygon | BRIDGE, SWAP | `lifi` |
| [Meteora](meteora.md) | DEX | Solana | LP_CLOSE, LP_OPEN | `meteora` |
| [Morpho Blue](morpho_blue.md) | Lending | Arbitrum, Base, Ethereum, Monad, Polygon | BORROW, FLASH_LOAN, REPAY, SUPPLY, WITHDRAW | `morpho_blue` |
| [Morpho Vault](morpho_vault.md) | Vault | Base, Ethereum | VAULT_DEPOSIT, VAULT_REDEEM | `morpho_vault` |
| [Orca](orca.md) | DEX | Solana | LP_CLOSE, LP_OPEN | `orca` |
| [PancakeSwap Perps](pancakeswap_perps.md) | Perp | BNB Chain | PERP_CLOSE, PERP_OPEN | `pancakeswap_perps` |
| [PancakeSwap V3](pancakeswap_v3.md) | DEX | Arbitrum, Base, BNB Chain, Ethereum | LP_CLOSE, LP_COLLECT_FEES, LP_OPEN, SWAP | `pancakeswap_v3` |
| [Pendle](pendle.md) | DEX | Arbitrum, Ethereum | LP_CLOSE, LP_OPEN, SWAP, WITHDRAW | `pendle` |
| [Polymarket](polymarket.md) | Prediction | Polygon | PREDICTION_BUY, PREDICTION_REDEEM, PREDICTION_SELL | `polymarket` |
| [Radiant V2](radiant_v2.md) | Lending | Ethereum | BORROW, REPAY, SUPPLY, WITHDRAW | `radiant_v2` |
| [Raydium](raydium.md) | DEX | Solana | LP_CLOSE, LP_OPEN | `raydium` |
| [Silo V2](silo_v2.md) | Lending | Avalanche | BORROW, REPAY, SUPPLY, WITHDRAW | `silo_v2` |
| [Spark](spark.md) | Lending | Ethereum | BORROW, REPAY, SUPPLY, WITHDRAW | `spark` |
| [Stargate](stargate.md) | Bridge | Arbitrum, Avalanche, Base, BNB Chain, Ethereum, Optimism, Polygon | BRIDGE | `stargate` |
| [SushiSwap V3](sushiswap_v3.md) | DEX | Arbitrum, Base, BNB Chain, Ethereum, Optimism, Polygon | LP_CLOSE, LP_COLLECT_FEES, LP_OPEN, SWAP | `sushiswap_v3` |
| [TraderJoe V2](traderjoe_v2.md) | DEX | Arbitrum, Avalanche, BNB Chain, Ethereum | LP_CLOSE, LP_COLLECT_FEES, LP_OPEN, SWAP | `traderjoe_v2` |
| [Uniswap V3](uniswap_v3.md) | DEX | Arbitrum, Avalanche, Base, BNB Chain, Ethereum, Monad, Optimism, Polygon | LP_CLOSE, LP_COLLECT_FEES, LP_OPEN, SWAP | `uniswap_v3` |
| [Uniswap V4](uniswap_v4.md) | DEX | Arbitrum, Base, Ethereum | LP_CLOSE, LP_COLLECT_FEES, LP_OPEN, SWAP | `uniswap_v4` |
<!-- generated:connector-matrix:end -->
