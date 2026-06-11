"""Optimism (chain_id 10) — L2 (Optimistic rollup)."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import (
    AnvilProfile,
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
        # Chainlink aggregator addresses (VIB-4851 CS-5) — moved verbatim
        # from the legacy almanak/core/chainlink.py per-chain dicts.
        # Reference: https://docs.chain.link/data-feeds/price-feeds/addresses
        chainlink=ChainlinkFeeds(
            usd_feeds={
                "ETH/USD": "0x13e3Ee699D1909E989722E753853AE30b17e08c5",
                "BTC/USD": "0xD702DD976Fb76Fffc2D3963D037dfDae5b04E593",
                "LINK/USD": "0xCC232DcFAaE6354cE191bd574108c1Ad03F86CeA",
                "USDC/USD": "0x16a9FA2FDa030272Ce99B29CF780dFA30361E0f3",
                "USDT/USD": "0xECef79e109E997BCa29c1c0897EC9D7678e00BB1",
                "DAI/USD": "0x8dBa75e83DA73cc766A7e5a0ee71F656BAb470d6",
                "OP/USD": "0x0D276FC14719f9292D5C1eA2198673d1f4269246",
                "SNX/USD": "0x2FCF37343e916eAEd1f1DdaaF84458a359b53877",
                "AAVE/USD": "0x338ed6787f463394D24813b297401B9F05a8C9d1",
                "WSTETH/USD": "0x698B585CbC4407e2D54aa898B2600B53C68958f7",
            },
        ),
        # Safe MultiSendCallOnly v1.4.1 — CREATE2, same address on every
        # chain Safe deploys to; presence here == deployment-verified
        # (legacy MULTISEND_ADDRESSES membership, VIB-4851 CS-5).
        contracts={"safe_multisend": "0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526"},
        # Managed-Anvil fork-test funding facts (VIB-4851 CS-6) — moved
        # verbatim from framework/anvil/fork_manager.py (display-case keys).
        anvil=AnvilProfile(
            funding_tokens={
                "WETH": "0x4200000000000000000000000000000000000006",
                "USDC": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
                "USDC.e": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
                "USDT": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
                "DAI": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
                "OP": "0x4200000000000000000000000000000000000042",
            },
            balance_slots={
                "USDC": 9,
                "WETH": 3,
                "USDT": 0,
                "USDC.e": 0,
                "OP": 0,
            },
            wrapped_native_deposit=True,
        ),
        bridged_stablecoin_variants=("USDC.e",),
        aliases=("op",),
    )
)
