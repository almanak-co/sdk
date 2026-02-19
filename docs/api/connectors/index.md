# Connectors

Protocol connectors provide adapters for interacting with DeFi protocols. Each connector includes an SDK (low-level interactions), an adapter (standard interface), and a receipt parser.

## Supported Protocols

| Protocol | Type | Chains | Module |
|----------|------|--------|--------|
| [Uniswap V3](uniswap_v3.md) | DEX | Ethereum, Arbitrum, Optimism, Base, Polygon | `uniswap_v3` |
| [Aave V3](aave_v3.md) | Lending | Ethereum, Arbitrum, Optimism, Base, Avalanche | `aave_v3` |
| [Morpho Blue](morpho_blue.md) | Lending | Ethereum, Base | `morpho_blue` |
| [GMX V2](gmx_v2.md) | Perpetuals | Arbitrum, Avalanche | `gmx_v2` |
| [Aerodrome](aerodrome.md) | DEX | Base | `aerodrome` |
| [TraderJoe V2](traderjoe_v2.md) | DEX | Avalanche, Arbitrum | `traderjoe_v2` |
| [PancakeSwap V3](pancakeswap_v3.md) | DEX | BSC, Ethereum, Arbitrum | `pancakeswap_v3` |
| [SushiSwap V3](sushiswap_v3.md) | DEX | Ethereum, Arbitrum | `sushiswap_v3` |
| [Curve](curve.md) | DEX | Ethereum | `curve` |
| [Balancer](balancer.md) | DEX | Ethereum, Arbitrum | `balancer` |
| [Compound V3](compound_v3.md) | Lending | Ethereum, Arbitrum, Base | `compound_v3` |
| [Enso](enso.md) | Aggregator | Multi-chain | `enso` |
| [Polymarket](polymarket.md) | Prediction | Polygon | `polymarket` |
| [Hyperliquid](hyperliquid.md) | Perpetuals | HyperEVM | `hyperliquid` |
| [Lido](lido.md) | Liquid Staking | Ethereum | `lido` |
| [Ethena](ethena.md) | Yield | Ethereum | `ethena` |
| [Spark](spark.md) | Lending | Ethereum | `spark` |
| [Pendle](pendle.md) | Yield | Ethereum, Arbitrum | `pendle` |
| [Kraken](kraken.md) | CEX | N/A | `kraken` |
| [Bridges](bridges.md) | Bridge | Multi-chain | `bridges` |
| [Flash Loan](flash_loan.md) | Utility | Multi-chain | `flash_loan` |
| [Base Infrastructure](base.md) | Shared | N/A | `base` |
