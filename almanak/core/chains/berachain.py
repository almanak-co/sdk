"""Berachain (chain_id 80094) — EVM-compatible L1."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, Explorer, GasProfile, NativeToken, SimulationProfile, Timeouts
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
        aliases=("bera",),
    )
)
