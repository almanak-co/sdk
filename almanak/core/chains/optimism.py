"""Optimism (chain_id 10) — L2 (Optimistic rollup)."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import (
    ChainDescriptor,
    Explorer,
    GasProfile,
    NativeToken,
    RpcProfile,
    SimulationProfile,
    Timeouts,
)
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.OPTIMISM,
        name="optimism",
        chain_id=10,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="ETH",
            name="Ethereum",
            decimals=18,
            wrapped_address="0x4200000000000000000000000000000000000006",
            coingecko_id="ethereum",
            wrapped_symbol="WETH",
            wrapped_coingecko_id="weth",
        ),
        gas=GasProfile(
            buffer=1.5,
            simulation_buffer=0.5,
            price_cap_gwei=10,
            cost_cap_native=0.01,
            fallback_base_fee_gwei=0.001,
            fallback_priority_fee_gwei=0.001,
        ),
        timeouts=Timeouts(
            tx_confirmation=120,
            grpc_execute=300,
        ),
        rpc=RpcProfile(
            public_rpc="https://optimism-rpc.publicnode.com",
            alchemy_prefix="opt",
            anvil_port=8550,
            block_time_seconds=2.0,
            rate_limit_rpm=300,
        ),
        explorer=Explorer(
            api_url="https://api-optimistic.etherscan.io/api",
            api_key_env="OPTIMISTIC_ETHERSCAN_API_KEY",
            browse_url="https://optimistic.etherscan.io",
        ),
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS.
        tokens={
            "usdc": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
            "usdt": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
            "weth": "0x4200000000000000000000000000000000000006",
        },
        simulation=SimulationProfile(tenderly_supported=True, alchemy_network="opt-mainnet"),
        # VIB-4851 (B1): per-vendor external ids, transposed from the legacy
        # standalone vendor maps (CoinGecko / DexScreener / GeckoTerminal /
        # DeFiLlama / Zerion / Moralis / OKX). Values verbatim incl. case.
        external_ids={
            "tenderly": "optimism",
            "coingecko": "optimistic-ethereum",
            "dexscreener": "optimism",
            "geckoterminal": "optimism",
            "defillama": "optimism",
            "defillama_display": "Optimism",
            "zerion": "optimism",
            "moralis": "optimism",
            "okx": "10",
        },
        aliases=("op",),
    )
)
