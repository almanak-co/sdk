"""Ethereum mainnet (chain_id 1) — L1.

Source values mirror the legacy scattered dicts as of VIB-4801. Do not
change numeric values here without an explicit owner sign-off; the
chain_id is the on-the-wire identifier owned by ``metrics-database``.
"""

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
        enum=Chain.ETHEREUM,
        name="ethereum",
        chain_id=1,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="ETH",
            name="Ethereum",
            decimals=18,
            wrapped_address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        ),
        gas=GasProfile(
            buffer=1.1,
            simulation_buffer=0.1,
            price_cap_gwei=300,
            cost_cap_native=0.1,
            # VIB-4857: chain half of CHAIN_GAS_OVERRIDES. Proxy tokens
            # like USDC need ~150k+ delegatecall gas, hence the buffer.
            operation_overrides={
                "swap_simple": 180000,
                "swap_multi_hop": 300000,
            },
            fallback_base_fee_gwei=20.0,
            fallback_priority_fee_gwei=2.0,
        ),
        timeouts=Timeouts(
            tx_confirmation=300,
            grpc_execute=600,
        ),
        rpc=RpcProfile(
            public_rpc="https://ethereum-rpc.publicnode.com",
            alchemy_prefix="eth",
            tenderly_subdomain="mainnet",
            anvil_port=8549,
            block_time_seconds=12.0,
        ),
        explorer=Explorer(
            api_url="https://api.etherscan.io/api",
            api_key_env="ETHERSCAN_API_KEY",
        ),
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS in
        # ``framework/intents/compiler_constants.py``. Lowercase symbol
        # keys, chain-canonical addresses.
        tokens={
            "usdc": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "usdt": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
            "weth": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "wbtc": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
            "dai": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        },
        simulation=SimulationProfile(tenderly_supported=True, alchemy_network="eth-mainnet"),
        # VIB-4851 (B1): per-vendor external ids, transposed from the legacy
        # standalone vendor maps (CoinGecko / DexScreener / GeckoTerminal /
        # DeFiLlama / Zerion / Moralis / OKX). Values verbatim incl. case.
        external_ids={
            "coingecko": "ethereum",
            "dexscreener": "ethereum",
            "geckoterminal": "eth",
            "defillama": "ethereum",
            "defillama_display": "Ethereum",
            "zerion": "ethereum",
            "moralis": "eth",
            "okx": "1",
        },
        aliases=("eth", "mainnet"),
    )
)
