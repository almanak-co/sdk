"""Base (chain_id 8453) — Coinbase L2 (OP Stack)."""

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
        enum=Chain.BASE,
        name="base",
        chain_id=8453,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="ETH",
            name="Ethereum",
            decimals=18,
            wrapped_address="0x4200000000000000000000000000000000000006",
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
            public_rpc="https://base-rpc.publicnode.com",
            alchemy_prefix="base",
            tenderly_subdomain="base",
            anvil_port=8548,
            block_time_seconds=2.0,
        ),
        explorer=Explorer(
            api_url="https://api.basescan.org/api",
            api_key_env="BASESCAN_API_KEY",
        ),
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS.
        tokens={
            "usdc": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "weth": "0x4200000000000000000000000000000000000006",
        },
        simulation=SimulationProfile(tenderly_supported=True, alchemy_network="base-mainnet"),
        # VIB-4851 (B1): per-vendor external ids, transposed from the legacy
        # standalone vendor maps (CoinGecko / DexScreener / GeckoTerminal /
        # DeFiLlama / Zerion / Moralis / OKX). Values verbatim incl. case.
        external_ids={
            "coingecko": "base",
            "dexscreener": "base",
            "geckoterminal": "base",
            "defillama": "base",
            "defillama_display": "Base",
            "zerion": "base",
            "moralis": "base",
            "okx": "8453",
        },
        aliases=(),
    )
)
