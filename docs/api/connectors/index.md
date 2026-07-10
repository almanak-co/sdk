# Connectors

Protocol connectors provide adapters for interacting with DeFi protocols. Each generated connector page includes the registry-owned chain and intent support matrix plus its API reference.

## Supported Protocols

| Protocol | Type | Chains | Intent Types | Module |
|----------|------|--------|--------------|--------|
| [Aave V3](aave_v3.md) | Lending | [Arbitrum](../../chains/arbitrum.md), [Avalanche](../../chains/avalanche.md), [Base](../../chains/base.md), [BNB Chain](../../chains/bsc.md), [Ethereum](../../chains/ethereum.md), [Mantle](../../chains/mantle.md), [Optimism](../../chains/optimism.md), [Polygon](../../chains/polygon.md), [X-Layer](../../chains/xlayer.md) | ``BORROW``, ``FLASH_LOAN``, ``REPAY``, ``SUPPLY``, ``WITHDRAW`` | ``almanak.connectors.aave_v3`` |
| [Across](across.md) | Bridge | [Arbitrum](../../chains/arbitrum.md), [Base](../../chains/base.md), [Ethereum](../../chains/ethereum.md), [Linea](../../chains/linea.md), [Optimism](../../chains/optimism.md), [Polygon](../../chains/polygon.md) | ``BRIDGE`` | ``almanak.connectors.across`` |
| [Aerodrome](aerodrome.md) | DEX | [Base](../../chains/base.md), [Optimism](../../chains/optimism.md) | ``LP_CLOSE``, ``LP_OPEN``, ``SWAP`` | ``almanak.connectors.aerodrome`` |
| [Aster Perps](aster_perps.md) | Perp | [BNB Chain](../../chains/bsc.md) | ``PERP_CLOSE``, ``PERP_OPEN`` | ``almanak.connectors.aster_perps`` |
| [Balancer](balancer.md) | Flash Loan | [Arbitrum](../../chains/arbitrum.md), [Avalanche](../../chains/avalanche.md), [Base](../../chains/base.md), [Ethereum](../../chains/ethereum.md), [Optimism](../../chains/optimism.md), [Polygon](../../chains/polygon.md) | ``FLASH_LOAN`` | ``almanak.connectors.balancer_v2`` |
| [Beefy](beefy.md) | Vault | No strategy chains registered | No strategy intents registered | ``almanak.connectors.beefy`` |
| [BenQi](benqi.md) | Lending | [Avalanche](../../chains/avalanche.md) | ``BORROW``, ``REPAY``, ``SUPPLY``, ``WITHDRAW`` | ``almanak.connectors.benqi`` |
| [Camelot](camelot.md) | DEX | [Arbitrum](../../chains/arbitrum.md) | ``SWAP`` | ``almanak.connectors.camelot`` |
| [Compound V3](compound_v3.md) | Lending | [Arbitrum](../../chains/arbitrum.md), [Base](../../chains/base.md), [Ethereum](../../chains/ethereum.md), [Optimism](../../chains/optimism.md), [Polygon](../../chains/polygon.md) | ``BORROW``, ``REPAY``, ``SUPPLY``, ``WITHDRAW`` | ``almanak.connectors.compound_v3`` |
| [Curvance](curvance.md) | Lending | [Monad](../../chains/monad.md) | ``BORROW``, ``REPAY``, ``SUPPLY``, ``WITHDRAW`` | ``almanak.connectors.curvance`` |
| [Curve](curve.md) | DEX | [Arbitrum](../../chains/arbitrum.md), [Base](../../chains/base.md), [Ethereum](../../chains/ethereum.md), [Optimism](../../chains/optimism.md), [Polygon](../../chains/polygon.md) | ``LP_CLOSE``, ``LP_OPEN``, ``SWAP`` | ``almanak.connectors.curve`` |
| [Drift](drift.md) | Perp | [Solana](../../chains/solana.md) | ``PERP_CLOSE``, ``PERP_OPEN`` | ``almanak.connectors.drift`` |
| [Enso](enso.md) | DEX | [Arbitrum](../../chains/arbitrum.md), [Avalanche](../../chains/avalanche.md), [Base](../../chains/base.md), [BNB Chain](../../chains/bsc.md), [Ethereum](../../chains/ethereum.md), [Optimism](../../chains/optimism.md), [Polygon](../../chains/polygon.md) | ``SWAP`` | ``almanak.connectors.enso`` |
| [Ethena](ethena.md) | Staking | [Ethereum](../../chains/ethereum.md) | ``STAKE``, ``UNSTAKE`` | ``almanak.connectors.ethena`` |
| [Euler V2](euler_v2.md) | Lending | [Arbitrum](../../chains/arbitrum.md), [Avalanche](../../chains/avalanche.md), [Base](../../chains/base.md), [Ethereum](../../chains/ethereum.md) | ``BORROW``, ``REPAY``, ``SUPPLY``, ``WITHDRAW`` | ``almanak.connectors.euler_v2`` |
| [Fluid](fluid.md) | DEX | [Arbitrum](../../chains/arbitrum.md), [Base](../../chains/base.md), [Ethereum](../../chains/ethereum.md), [Polygon](../../chains/polygon.md) | ``SUPPLY``, ``SWAP``, ``WITHDRAW`` | ``almanak.connectors.fluid`` |
| [Fluid DEX LP](fluid_dex_lp.md) | DEX | [Arbitrum](../../chains/arbitrum.md) | ``LP_CLOSE``, ``LP_OPEN`` | ``almanak.connectors.fluid_dex_lp`` |
| [Fluid Vault](fluid_vault.md) | Lending | [Arbitrum](../../chains/arbitrum.md), [Base](../../chains/base.md) | ``BORROW``, ``REPAY``, ``SUPPLY``, ``WITHDRAW`` | ``almanak.connectors.fluid_vault`` |
| [Gimo](gimo.md) | Staking | [0G](../../chains/zerog.md) | ``STAKE``, ``UNSTAKE`` | ``almanak.connectors.gimo`` |
| [GMX V2](gmx_v2.md) | Perp | [Arbitrum](../../chains/arbitrum.md), [Avalanche](../../chains/avalanche.md) | ``PERP_CLOSE``, ``PERP_OPEN`` | ``almanak.connectors.gmx_v2`` |
| [Hyperliquid](hyperliquid.md) | Perp | [Hyperevm](../../chains/hyperevm.md) | ``PERP_CLOSE``, ``PERP_OPEN``, ``PERP_WITHDRAW`` | ``almanak.connectors.hyperliquid`` |
| [JoeLend](joelend.md) | Lending | No strategy chains registered | No strategy intents registered | ``almanak.connectors.joelend`` |
| [Jupiter](jupiter.md) | DEX | [Solana](../../chains/solana.md) | ``SWAP`` | ``almanak.connectors.jupiter`` |
| [Jupiter Lend](jupiter_lend.md) | Lending | No strategy chains registered | No strategy intents registered | ``almanak.connectors.jupiter_lend`` |
| [Kamino](kamino.md) | Lending | [Solana](../../chains/solana.md) | ``BORROW``, ``REPAY``, ``SUPPLY``, ``WITHDRAW`` | ``almanak.connectors.kamino`` |
| [Kraken](kraken.md) | DEX | N/A (off-chain) | ``SWAP`` | ``almanak.connectors.kraken`` |
| [Lagoon](lagoon.md) | Vault | [Base](../../chains/base.md), [Ethereum](../../chains/ethereum.md) | ``VAULT_DEPOSIT``, ``VAULT_REDEEM`` | ``almanak.connectors.lagoon`` |
| [Lido](lido.md) | Staking | [Ethereum](../../chains/ethereum.md) | ``STAKE``, ``UNSTAKE`` | ``almanak.connectors.lido`` |
| [LiFi](lifi.md) | DEX | [Arbitrum](../../chains/arbitrum.md), [Avalanche](../../chains/avalanche.md), [Base](../../chains/base.md), [BNB Chain](../../chains/bsc.md), [Ethereum](../../chains/ethereum.md), [Optimism](../../chains/optimism.md), [Polygon](../../chains/polygon.md) | ``BRIDGE``, ``SWAP`` | ``almanak.connectors.lifi`` |
| [Meteora](meteora.md) | DEX | [Solana](../../chains/solana.md) | ``LP_CLOSE``, ``LP_OPEN`` | ``almanak.connectors.meteora`` |
| [Morpho Blue](morpho_blue.md) | Lending | [Arbitrum](../../chains/arbitrum.md), [Base](../../chains/base.md), [Ethereum](../../chains/ethereum.md), [Monad](../../chains/monad.md), [Polygon](../../chains/polygon.md), [Robinhood](../../chains/robinhood.md) | ``BORROW``, ``FLASH_LOAN``, ``REPAY``, ``SUPPLY``, ``WITHDRAW`` | ``almanak.connectors.morpho_blue`` |
| [Morpho Vault](morpho_vault.md) | Vault | [Base](../../chains/base.md), [Ethereum](../../chains/ethereum.md) | ``VAULT_DEPOSIT``, ``VAULT_REDEEM`` | ``almanak.connectors.morpho_vault`` |
| [Orca](orca.md) | DEX | [Solana](../../chains/solana.md) | ``LP_CLOSE``, ``LP_OPEN`` | ``almanak.connectors.orca`` |
| [PancakeSwap Perps](pancakeswap_perps.md) | Perp | [BNB Chain](../../chains/bsc.md) | ``PERP_CLOSE``, ``PERP_OPEN`` | ``almanak.connectors.pancakeswap_perps`` |
| [PancakeSwap V3](pancakeswap_v3.md) | DEX | [Arbitrum](../../chains/arbitrum.md), [Base](../../chains/base.md), [BNB Chain](../../chains/bsc.md), [Ethereum](../../chains/ethereum.md) | ``LP_CLOSE``, ``LP_COLLECT_FEES``, ``LP_OPEN``, ``SWAP`` | ``almanak.connectors.pancakeswap_v3`` |
| [Pendle](pendle.md) | DEX | [Arbitrum](../../chains/arbitrum.md), [Ethereum](../../chains/ethereum.md) | ``LP_CLOSE``, ``LP_OPEN``, ``SWAP``, ``WITHDRAW`` | ``almanak.connectors.pendle`` |
| [Polymarket](polymarket.md) | Prediction | [Polygon](../../chains/polygon.md) | ``PREDICTION_BUY``, ``PREDICTION_REDEEM``, ``PREDICTION_SELL`` | ``almanak.connectors.polymarket`` |
| [Raydium](raydium.md) | DEX | [Solana](../../chains/solana.md) | ``LP_CLOSE``, ``LP_OPEN`` | ``almanak.connectors.raydium`` |
| [Silo V2](silo_v2.md) | Lending | [Avalanche](../../chains/avalanche.md) | ``BORROW``, ``REPAY``, ``SUPPLY``, ``WITHDRAW`` | ``almanak.connectors.silo_v2`` |
| [Spark](spark.md) | Lending | [Ethereum](../../chains/ethereum.md) | ``BORROW``, ``REPAY``, ``SUPPLY``, ``WITHDRAW`` | ``almanak.connectors.spark`` |
| [Stargate](stargate.md) | Bridge | [Arbitrum](../../chains/arbitrum.md), [Avalanche](../../chains/avalanche.md), [Base](../../chains/base.md), [BNB Chain](../../chains/bsc.md), [Ethereum](../../chains/ethereum.md), [Optimism](../../chains/optimism.md), [Polygon](../../chains/polygon.md) | ``BRIDGE`` | ``almanak.connectors.stargate`` |
| [SushiSwap V3](sushiswap_v3.md) | DEX | [Arbitrum](../../chains/arbitrum.md), [Base](../../chains/base.md), [BNB Chain](../../chains/bsc.md), [Ethereum](../../chains/ethereum.md), [Optimism](../../chains/optimism.md), [Polygon](../../chains/polygon.md) | ``LP_CLOSE``, ``LP_COLLECT_FEES``, ``LP_OPEN``, ``SWAP`` | ``almanak.connectors.sushiswap_v3`` |
| [TraderJoe V2](traderjoe_v2.md) | DEX | [Arbitrum](../../chains/arbitrum.md), [Avalanche](../../chains/avalanche.md), [BNB Chain](../../chains/bsc.md), [Ethereum](../../chains/ethereum.md) | ``LP_CLOSE``, ``LP_COLLECT_FEES``, ``LP_OPEN``, ``SWAP`` | ``almanak.connectors.traderjoe_v2`` |
| [Uniswap V3](uniswap_v3.md) | DEX | [Arbitrum](../../chains/arbitrum.md), [Avalanche](../../chains/avalanche.md), [Base](../../chains/base.md), [BNB Chain](../../chains/bsc.md), [Ethereum](../../chains/ethereum.md), [Monad](../../chains/monad.md), [Optimism](../../chains/optimism.md), [Polygon](../../chains/polygon.md), [Robinhood](../../chains/robinhood.md) | ``LP_CLOSE``, ``LP_COLLECT_FEES``, ``LP_OPEN``, ``SWAP`` | ``almanak.connectors.uniswap_v3`` |
| [Uniswap V4](uniswap_v4.md) | DEX | [Arbitrum](../../chains/arbitrum.md), [Avalanche](../../chains/avalanche.md), [Base](../../chains/base.md), [BNB Chain](../../chains/bsc.md), [Ethereum](../../chains/ethereum.md), [Optimism](../../chains/optimism.md), [Polygon](../../chains/polygon.md) | ``LP_CLOSE``, ``LP_COLLECT_FEES``, ``LP_OPEN``, ``SWAP`` | ``almanak.connectors.uniswap_v4`` |
| [Yearn](yearn.md) | Vault | No strategy chains registered | No strategy intents registered | ``almanak.connectors.yearn`` |
