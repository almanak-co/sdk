"""Ethereum mainnet (chain_id 1) — L1.

Source values mirror the legacy scattered dicts as of VIB-4801. Do not
change numeric values here without an explicit owner sign-off; the
chain_id is the on-the-wire identifier owned by ``metrics-database``.
"""

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
        enum=Chain.ETHEREUM,
        name="ethereum",
        chain_id=1,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="ETH",
            name="Ethereum",
            decimals=18,
            wrapped_address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            coingecko_id="ethereum",
            wrapped_symbol="WETH",
            wrapped_coingecko_id="weth",
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
            rate_limit_rpm=300,
            fork_requires_archive=True,
        ),
        explorer=Explorer(
            api_url="https://api.etherscan.io/api",
            api_key_env="ETHERSCAN_API_KEY",
            browse_url="https://etherscan.io",
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
            "tenderly": "mainnet",
            "coingecko": "ethereum",
            "dexscreener": "ethereum",
            "geckoterminal": "eth",
            "defillama": "ethereum",
            "defillama_display": "Ethereum",
            "zerion": "ethereum",
            "moralis": "eth",
            "okx": "1",
        },
        # Chainlink aggregator addresses (VIB-4851 CS-5) — moved verbatim
        # from the legacy almanak/core/chainlink.py per-chain dicts.
        # Reference: https://docs.chain.link/data-feeds/price-feeds/addresses
        chainlink=ChainlinkFeeds(
            usd_feeds={
                "ETH/USD": "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419",
                "BTC/USD": "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c",
                "LINK/USD": "0x2c1d072e956AFFC0D435Cb7AC38EF18d24d9127c",
                "USDC/USD": "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6",
                "USDT/USD": "0x3E7d1eAB13ad0104d2750B8863b489D65364e32D",
                "DAI/USD": "0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9",
                "AAVE/USD": "0x547a514d5e3769680Ce22B2361c10Ea13619e8a9",
                "UNI/USD": "0x553303d460EE0afB37EdFf9bE42922D8FF63220e",
                "CRV/USD": "0xcD627aa160A6fA45Eb793D19286F3879d5cdCe0a",
                "COMP/USD": "0xdBD020CAef83eFd542f4de03864E8c5D2d9bc6CA",
                "MKR/USD": "0xEC1D1b3b0443256Cc3860E24a46f108E699cF2b4",
                "SNX/USD": "0xDC3EA94CD0AC27d9A86C180091e7f78C683d3699",
                "MATIC/USD": "0x7bAC85A8a13A4BcD8abb3eB7d6b4d632c5a57676",
                "ARB/USD": "0x31697852a68433DBcC2FF612A4c1C919a0254678",
                "LDO/USD": "0x4e844125952d32acdF339be976C98FE6D1F5F8bE",
                "WSTETH/USD": "0x164b276057258D81941072Eb5f9D7F71C3Dd94b8",
                "CBETH/USD": "0xF017fcB346A1885194689bA23Eff2fE6fA5C483b",
                "RETH/USD": "0x536218f9E9Eb48863970252233c8F271f554C2d0",
                "SOL/USD": "0x4ffC43a60e009B551865A93d232E33Fce9f01507",
            },
            eth_denominated={
                "WSTETH/ETH": "0x86392dC19c0b719886221c78AB11eb8Cf5c52812",
            },
        ),
        # Safe MultiSendCallOnly v1.4.1 — CREATE2, same address on every
        # chain Safe deploys to; presence here == deployment-verified
        # (legacy MULTISEND_ADDRESSES membership, VIB-4851 CS-5).
        contracts={"safe_multisend": "0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526"},
        aliases=("eth", "mainnet"),
    )
)
