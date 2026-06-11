"""Polygon PoS (chain_id 137) — Ethereum sidechain."""

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
        enum=Chain.POLYGON,
        name="polygon",
        chain_id=137,
        family=ChainFamily.EVM,
        # Native symbol stays "MATIC" deliberately. Polygon renamed MATIC -> POL
        # (Sept 2024, 1:1), and the token resolver canonicalizes the native
        # sentinel to POL for token identity — but the gas/price/funding stack is
        # pinned to MATIC (the Chainlink MATIC/USD feed key, the gateway native
        # symbol derived from this descriptor, and every shipped Polygon config's
        # anvil_funding key). The two views are bridged: ``symbol`` stays MATIC
        # (gas/price/funding canonical) while ``accepted_symbols=("POL",)`` makes
        # both symbols route to the native-balance path (VIB-4851 A1, the
        # registry-derived replacement for NATIVE_SYMBOLS_BY_CHAIN["polygon"]).
        # Do NOT flip ``symbol`` to POL in isolation — see
        # tests/unit/core/test_polygon_native_symbol_parity.py for the contract.
        native=NativeToken(
            symbol="MATIC",
            name="Polygon",
            decimals=18,
            wrapped_address="0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
            accepted_symbols=("POL",),
            # POL id preferred over deprecated matic-network (VIB-3137)
            coingecko_id="polygon-ecosystem-token",
            wrapped_symbol="WMATIC",
            wrapped_coingecko_id="polygon-ecosystem-token",
        ),
        gas=GasProfile(
            buffer=1.2,
            simulation_buffer=0.2,
            # VIB-4879: bumped 500 → 1000. Mainnet snapshot 2026-05-27
            # observed Polygon live gas at ~284 gwei, leaving the previous
            # 500 cap with only 1.76× spike headroom. Polygon's PoS
            # economics make 5-10× short spikes routine during NFT mints
            # and busy DeFi periods. 1000 gwei = ~$0.013 per 150k-gas tx
            # at POL ~$0.087, well below cost_cap_native (50 MATIC) and
            # SANE_GWEI_CEILING (10_000).
            price_cap_gwei=1000,
            cost_cap_native=50.0,
            fallback_base_fee_gwei=30.0,
            fallback_priority_fee_gwei=30.0,
        ),
        timeouts=Timeouts(
            tx_confirmation=180,
            grpc_execute=360,
        ),
        rpc=RpcProfile(
            public_rpc="https://polygon-bor-rpc.publicnode.com",
            alchemy_prefix="polygon",
            anvil_port=8551,
            poa=True,
            block_time_seconds=2.0,
            rate_limit_rpm=300,
            fork_requires_archive=True,
        ),
        explorer=Explorer(
            api_url="https://api.polygonscan.com/api",
            api_key_env="POLYGONSCAN_API_KEY",
            browse_url="https://polygonscan.com",
        ),
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS.
        tokens={
            "usdc": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
            "usdt": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
            "weth": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
        },
        simulation=SimulationProfile(tenderly_supported=True),
        # VIB-4851 (B1): per-vendor external ids, transposed from the legacy
        # standalone vendor maps (CoinGecko / DexScreener / GeckoTerminal /
        # DeFiLlama / Zerion / Moralis / OKX). Values verbatim incl. case.
        external_ids={
            "tenderly": "polygon",
            "coingecko": "polygon-pos",
            "dexscreener": "polygon",
            "geckoterminal": "polygon_pos",
            "defillama": "polygon",
            "defillama_display": "Polygon",
            "zerion": "polygon",
            "moralis": "polygon",
            "okx": "137",
        },
        # Chainlink aggregator addresses (VIB-4851 CS-5) — moved verbatim
        # from the legacy almanak/core/chainlink.py per-chain dicts.
        # Reference: https://docs.chain.link/data-feeds/price-feeds/addresses
        chainlink=ChainlinkFeeds(
            usd_feeds={
                "ETH/USD": "0xF9680D99D6C9589e2a93a78A04A279e509205945",
                "BTC/USD": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
                "MATIC/USD": "0xAB594600376Ec9fD91F8e885dADF0CE036862dE0",
                "LINK/USD": "0xd9FFdb71EbE7496cC440152d43986Aae0AB76665",
                "USDC/USD": "0xfE4A8cc5b5B2366C1B58Bea3858e81843581b2F7",
                "USDT/USD": "0x0A6513e40db6EB1b165753AD52E80663aeA50545",
                "DAI/USD": "0x4746DeC9e833A82EC7C2C1356372CcF2cfcD2F3D",
                "AAVE/USD": "0x72484B12719E23115761D5DA1646945632979bB6",
                "UNI/USD": "0xdf0Fb4e4F928d2dCB76f438575fDD8682386e13C",
                "CRV/USD": "0x336584C8E6Dc19637A5b36206B1c79923111b405",
                "WSTETH/USD": "0x10f964234cae09cB6a9854B56FF7D4F38Cda5E6a",
            },
        ),
        # Safe MultiSendCallOnly v1.4.1 — CREATE2, same address on every
        # chain Safe deploys to; presence here == deployment-verified
        # (legacy MULTISEND_ADDRESSES membership, VIB-4851 CS-5).
        contracts={"safe_multisend": "0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526"},
        aliases=("matic",),
    )
)
