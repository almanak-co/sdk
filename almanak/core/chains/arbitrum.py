"""Arbitrum One (chain_id 42161) — L2 (Optimistic rollup)."""

from almanak.core.enums import Chain, ChainFamily

from ._contracts import safe_stack_contracts
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
        enum=Chain.ARBITRUM,
        name="arbitrum",
        chain_id=42161,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="ETH",
            name="Ethereum",
            decimals=18,
            wrapped_address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            coingecko_id="ethereum",
            wrapped_symbol="WETH",
            wrapped_coingecko_id="weth",
            slip44=60,  # SLIP-44 coin type for Ether (CAIP-19 native)
        ),
        gas=GasProfile(
            buffer=1.5,
            simulation_buffer=0.5,
            price_cap_gwei=10,
            cost_cap_native=0.01,
            fallback_base_fee_gwei=0.1,
            fallback_priority_fee_gwei=0.0,
            # Arbitrum ArbGasInfo precompile for L1 data-cost estimation (Plan 026).
            l1_fee_oracle_kind="arbitrum_nodeinterface",
            l1_fee_oracle_address="0x000000000000000000000000000000000000006C",
        ),
        timeouts=Timeouts(
            tx_confirmation=120,
            grpc_execute=300,
        ),
        rpc=RpcProfile(
            public_rpc="https://arbitrum-one-rpc.publicnode.com",
            alchemy_prefix="arb",
            tenderly_subdomain="arbitrum",
            anvil_port=8545,
            block_time_seconds=0.25,
            rate_limit_rpm=300,
        ),
        explorer=Explorer(
            api_url="https://api.arbiscan.io/api",
            api_key_env="ARBISCAN_API_KEY",
            browse_url="https://arbiscan.io",
        ),
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS in
        # ``framework/intents/compiler_constants.py``.
        tokens={
            "usdc": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  # Native USDC
            "usdc_bridged": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",  # USDC.e
            "usdt": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
            "weth": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            "wbtc": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
        },
        simulation=SimulationProfile(tenderly_supported=True, alchemy_network="arb-mainnet"),
        # VIB-4851 (B1): per-vendor external ids, transposed from the legacy
        # standalone vendor maps (CoinGecko / DexScreener / GeckoTerminal /
        # DeFiLlama / Zerion / Moralis / OKX). Values verbatim incl. case.
        external_ids={
            "tenderly": "arbitrum",
            "coingecko": "arbitrum-one",
            "dexscreener": "arbitrum",
            "geckoterminal": "arbitrum",
            "defillama": "arbitrum",
            "defillama_display": "Arbitrum",
            "zerion": "arbitrum",
            "moralis": "arbitrum",
            "okx": "42161",
        },
        # Chainlink aggregator addresses (VIB-4851 CS-5) — moved verbatim
        # from the legacy almanak/core/chainlink.py per-chain dicts.
        # Reference: https://docs.chain.link/data-feeds/price-feeds/addresses
        chainlink=ChainlinkFeeds(
            usd_feeds={
                "ETH/USD": "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",
                "BTC/USD": "0x6CE185860A4963106506C203335A2910F5E5E8CC",
                "LINK/USD": "0x86E53CF1B870786351Da77A57575e79CB55812CB",
                "USDC/USD": "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3",
                "USDT/USD": "0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7",
                "DAI/USD": "0xc5C8E77B397E531B8EC06BFb0048328B30E9eCfB",
                "ARB/USD": "0xb2A824043730FE05F3DA2efaFa1CBbe83fa548D6",
                "GMX/USD": "0xDB98056FecFff59D032aB628337A4887110df3dB",
                "UNI/USD": "0x9C917083fDb403ab5ADbEC26Ee294f6EcAda2720",
                "AAVE/USD": "0xaD1d5344AaDE45F43E596773Bcc4c423EAbdD034",
                "PENDLE/USD": "0x66853E19D73C0F9301fE099c324A1e9726953C89",
                "RDNT/USD": "0x20d0Fcab0ECFD078B036b6CAf1FaC69A6453b352",
                "MAGIC/USD": "0x47E55cCec6582838E173f252D08Afd8116c2202d",
                "WOO/USD": "0x5d5Ab15fb857De6FA209B6B41C7375F1C4BD9B90",
                "SOL/USD": "0x24ceA4b8ce57cdA5058b924B9B9987992450590c",
            },
            eth_denominated={
                "WSTETH/ETH": "0xb523AE262D20A936BC152e6023996e46FDC2A95D",
            },
        ),
        # Safe MultiSendCallOnly v1.4.1 — CREATE2, same address on every
        # chain Safe deploys to; presence here == deployment-verified
        # (legacy MULTISEND_ADDRESSES membership, VIB-4851 CS-5).
        contracts=safe_stack_contracts(enso_delegate_primary=True),
        # Managed-Anvil fork-test funding facts (VIB-4851 CS-6) — moved
        # verbatim from framework/anvil/fork_manager.py (display-case keys).
        anvil=AnvilProfile(
            funding_tokens={
                "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "USDC.e": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
                "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
                "DAI": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
                "WBTC": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
                "ARB": "0x912CE59144191C1204E64559FE8253a0e49E6548",
                "GMX": "0xfc5A1A6EB076a2C7aD06eD22C90d7E710E35ad0a",
                "wstETH": "0x5979D7b546E38E414F7E9822514be443A4800529",
            },
            balance_slots={
                "USDC": 9,
                "WETH": 51,
                "USDC.e": 51,
                "USDT": 51,
                "DAI": 2,
                "WBTC": 51,
                "ARB": 51,
                "GMX": 0,
                "wstETH": 1,
            },
            wrapped_native_deposit=True,
        ),
        bridged_stablecoin_variants=("USDC.e",),
        aliases=("arb",),
        color="#28a0f0",  # Plan 027: Arbitrum blue (from legacy CHAIN_COLORS)
        # Plan 027: default wallet-overview tokens (from legacy _CHAIN_DEFAULT_TOKENS)
        default_display_tokens=("ETH", "WETH", "USDC", "USDC.e", "USDT", "WBTC", "DAI", "ARB"),
    )
)
