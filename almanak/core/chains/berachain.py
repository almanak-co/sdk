"""Berachain (chain_id 80094) — EVM-compatible L1."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import AnvilProfile, ChainDescriptor, Explorer, GasProfile, NativeToken, SimulationProfile, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.BERACHAIN,
        name="berachain",
        chain_id=80094,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="BERA",
            name="Berachain",
            decimals=18,
            wrapped_address="0x6969696969696969696969696969696969696969",
            # wrapper aliases native
            coingecko_id="berachain-bera",
            wrapped_symbol="WBERA",
            wrapped_coingecko_id="berachain-bera",
            slip44=8008,  # SLIP-44 "Berachain" — BERA (CAIP-19)
        ),
        gas=GasProfile(
            buffer=1.2,
            simulation_buffer=0.2,
            price_cap_gwei=50,
            cost_cap_native=10.0,
        ),
        explorer=Explorer(browse_url="https://berascan.com"),
        timeouts=Timeouts(
            tx_confirmation=120,
            grpc_execute=300,
        ),
        simulation=SimulationProfile(tenderly_supported=True),
        # VIB-4851 (B1): per-vendor external ids, transposed from the legacy
        # standalone vendor maps (CoinGecko / DexScreener / GeckoTerminal /
        # DeFiLlama / Zerion / Moralis / OKX). Values verbatim incl. case.
        external_ids={
            "coingecko": "berachain",
            "dexscreener": "berachain",
        },
        # Managed-Anvil fork-test funding facts (VIB-4851 CS-6) — moved
        # verbatim from framework/anvil/fork_manager.py (display-case keys).
        anvil=AnvilProfile(
            funding_tokens={
                "WBERA": "0x6969696969696969696969696969696969696969",
                "HONEY": "0xFCBD14DC51f0A4d49d5E53C2E0950e0bC26d0Dce",
                "USDC.e": "0x549943e04f40284185054145c6E4e9568C1D3241",
                "WETH": "0x2F6F07CDcf3588944Bf4C42aC74ff24bF56e7590",
                "WBTC": "0x0555E30da8f98308EdB960aa94C0Db47230d2B9c",
                "USDT0": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",
            },
        ),
        bridged_stablecoin_variants=("USDC.e",),
        aliases=("bera",),
    )
)
