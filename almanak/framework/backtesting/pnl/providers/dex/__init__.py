"""DEX-specific historical data providers.

This package contains volume providers for various decentralized exchanges (DEXs).
Each provider implements the HistoricalVolumeProvider interface and fetches
historical volume data from protocol-specific subgraphs.

Available Providers:
    - UniswapV3VolumeProvider: Uniswap V3 subgraph volume data
    - SushiSwapV3VolumeProvider: SushiSwap V3 subgraph volume data
    - PancakeSwapV3VolumeProvider: PancakeSwap V3 subgraph volume data
    - AerodromeVolumeProvider: Aerodrome subgraph volume data (Base chain)
    - TraderJoeV2VolumeProvider: TraderJoe V2 Liquidity Book volume data (Avalanche)
    - CurveVolumeProvider: Curve Finance Messari subgraph volume data (Ethereum, Optimism)
    - BalancerVolumeProvider: Balancer V2 subgraph volume data (Ethereum, Arbitrum, Polygon)

Example:
    from almanak.framework.backtesting.pnl.providers.dex import (
        UniswapV3VolumeProvider,
        SushiSwapV3VolumeProvider,
        PancakeSwapV3VolumeProvider,
        AerodromeVolumeProvider,
        TraderJoeV2VolumeProvider,
    )
    from almanak.core.enums import Chain
    from datetime import date

    # Uniswap V3 example
    uniswap_provider = UniswapV3VolumeProvider()
    volumes = await uniswap_provider.get_volume(
        pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
        chain=Chain.ARBITRUM,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )

    # SushiSwap V3 example
    sushi_provider = SushiSwapV3VolumeProvider()
    volumes = await sushi_provider.get_volume(
        pool_address="0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
        chain=Chain.ETHEREUM,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )

    # PancakeSwap V3 example
    pancake_provider = PancakeSwapV3VolumeProvider()
    volumes = await pancake_provider.get_volume(
        pool_address="0x92c63d0e701caae670c9415d91c474f686298f00",
        chain=Chain.BSC,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )

    # Aerodrome example (Base chain)
    aerodrome_provider = AerodromeVolumeProvider()
    volumes = await aerodrome_provider.get_volume(
        pool_address="0x6cDcb1C4A4D1C3C6d054b27AC5B77e89eAFb971d",
        chain=Chain.BASE,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )

    # TraderJoe V2 example (Avalanche - Liquidity Book)
    traderjoe_provider = TraderJoeV2VolumeProvider()
    volumes = await traderjoe_provider.get_volume(
        pool_address="0x7eC3717f70894F6d9BA0F8ff67a0115e4c919Cc2",
        chain=Chain.AVALANCHE,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )

    # Curve example (Ethereum - Messari schema)
    curve_provider = CurveVolumeProvider()
    volumes = await curve_provider.get_volume(
        pool_address="0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
        chain=Chain.ETHEREUM,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )

    # Balancer example (Ethereum - weighted pools)
    balancer_provider = BalancerVolumeProvider()
    volumes = await balancer_provider.get_volume(
        pool_address="0x5c6Ee304399DBdB9C8Ef030aB642B10820DB8F56",
        chain=Chain.ETHEREUM,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )
"""

from .aerodrome_volume import (
    AERODROME_SUBGRAPH_IDS,
    AerodromeVolumeProvider,
)
from .balancer_volume import (
    BALANCER_SUBGRAPH_IDS,
    BalancerVolumeProvider,
)
from .curve_volume import (
    CURVE_SUBGRAPH_IDS,
    CurveVolumeProvider,
)
from .pancakeswap_v3_volume import (
    PANCAKESWAP_V3_SUBGRAPH_IDS,
    PancakeSwapV3VolumeProvider,
)
from .sushiswap_v3_volume import (
    SUSHISWAP_V3_SUBGRAPH_IDS,
    SushiSwapV3VolumeProvider,
)
from .traderjoe_v2_volume import (
    TRADERJOE_V2_SUBGRAPH_IDS,
    TraderJoeV2VolumeProvider,
)
from .uniswap_v3_volume import (
    UNISWAP_V3_SUBGRAPH_IDS,
    UniswapV3VolumeProvider,
)

__all__ = [
    # Uniswap V3
    "UniswapV3VolumeProvider",
    "UNISWAP_V3_SUBGRAPH_IDS",
    # SushiSwap V3
    "SushiSwapV3VolumeProvider",
    "SUSHISWAP_V3_SUBGRAPH_IDS",
    # PancakeSwap V3
    "PancakeSwapV3VolumeProvider",
    "PANCAKESWAP_V3_SUBGRAPH_IDS",
    # Aerodrome
    "AerodromeVolumeProvider",
    "AERODROME_SUBGRAPH_IDS",
    # TraderJoe V2
    "TraderJoeV2VolumeProvider",
    "TRADERJOE_V2_SUBGRAPH_IDS",
    # Curve
    "CurveVolumeProvider",
    "CURVE_SUBGRAPH_IDS",
    # Balancer
    "BalancerVolumeProvider",
    "BALANCER_SUBGRAPH_IDS",
]
