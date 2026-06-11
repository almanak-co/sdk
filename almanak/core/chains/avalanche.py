"""Avalanche C-Chain (chain_id 43114)."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import (
    ChainDescriptor,
    ChainlinkFeeds,
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
        enum=Chain.AVALANCHE,
        name="avalanche",
        chain_id=43114,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="AVAX",
            name="Avalanche",
            decimals=18,
            wrapped_address="0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
            coingecko_id="avalanche-2",
            wrapped_symbol="WAVAX",
            wrapped_coingecko_id="avalanche-2",
        ),
        gas=GasProfile(
            buffer=1.1,
            simulation_buffer=0.1,
            price_cap_gwei=100,
            cost_cap_native=1.0,
            # VIB-4857: chain half of CHAIN_GAS_OVERRIDES. Avalanche native
            # USDC is a proxy and needs ~150k+ for swap calldata.
            operation_overrides={
                "swap_simple": 180000,
            },
            fallback_base_fee_gwei=25.0,
            fallback_priority_fee_gwei=1.0,
        ),
        timeouts=Timeouts(
            tx_confirmation=120,
            grpc_execute=300,
            # VIB-4857: Avalanche Anvil forks are slower than L2s. Mirrors
            # the legacy CHAIN_RECEIPT_TIMEOUTS entry in chain_executor.py.
            receipt_polling=180,
        ),
        rpc=RpcProfile(
            public_rpc="https://avalanche-c-chain-rpc.publicnode.com",
            alchemy_prefix="avax",
            tenderly_subdomain="avalanche",
            anvil_port=8547,
            poa=True,
            block_time_seconds=2.0,
            rate_limit_rpm=300,
            fork_requires_archive=True,
        ),
        explorer=Explorer(
            api_url="https://api.snowtrace.io/api",
            api_key_env="SNOWTRACE_API_KEY",
            browse_url="https://snowtrace.io",
        ),
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS.
        tokens={
            "usdc": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
            "usdt": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
            "wavax": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        },
        simulation=SimulationProfile(tenderly_supported=True),
        # VIB-4851 (B1): per-vendor external ids, transposed from the legacy
        # standalone vendor maps (CoinGecko / DexScreener / GeckoTerminal /
        # DeFiLlama / Zerion / Moralis / OKX). Values verbatim incl. case.
        external_ids={
            "tenderly": "avalanche",
            "coingecko": "avalanche",
            "dexscreener": "avalanche",
            "geckoterminal": "avax",
            "defillama": "avax",
            "defillama_display": "Avalanche",
            "zerion": "avalanche",
            "moralis": "avalanche",
            "okx": "43114",
        },
        # Chainlink aggregator addresses (VIB-4851 CS-5) — moved verbatim
        # from the legacy almanak/core/chainlink.py per-chain dicts.
        # Reference: https://docs.chain.link/data-feeds/price-feeds/addresses
        chainlink=ChainlinkFeeds(
            usd_feeds={
                "AVAX/USD": "0x0A77230d17318075983913bC2145DB16C7366156",
                "ETH/USD": "0x976B3D034E162d8bD72D6b9C989d545b839003b0",
                "BTC/USD": "0x2779D32d5166BAaa2B2b658333bA7e6Ec0C65743",
                "LINK/USD": "0x49cCd9Ca821efeAb2B98C60Dc60f518e765EdADc",
                "USDC/USD": "0xF096872672F44d6EBA71458D74fe67F9a77a23B9",
                "USDT/USD": "0xEBE676ee90Fe1112671f19b6B7459bC678B67e8a",
                "DAI/USD": "0x51D7180edA2260cc4F6e4EebB82FEF5c3c2B8300",
                "AAVE/USD": "0x3CA13391E9fb38a75330fb28f8cc2eB3D9ceceED",
                "JOE/USD": "0x02D35d3a8aC3e1626d3eE09A78Dd87286F5E8e3a",
                "WAVAX/USD": "0x0A77230d17318075983913bC2145DB16C7366156",
            },
        ),
        # Safe MultiSendCallOnly v1.4.1 — CREATE2, same address on every
        # chain Safe deploys to; presence here == deployment-verified
        # (legacy MULTISEND_ADDRESSES membership, VIB-4851 CS-5).
        contracts={"safe_multisend": "0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526"},
        aliases=("avax",),
    )
)
