"""Polygon PoS (chain_id 137) — Ethereum sidechain."""

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
            "coingecko": "polygon-pos",
            "dexscreener": "polygon",
            "geckoterminal": "polygon_pos",
            "defillama": "polygon",
            "defillama_display": "Polygon",
            "zerion": "polygon",
            "moralis": "polygon",
            "okx": "137",
        },
        aliases=("matic",),
    )
)
