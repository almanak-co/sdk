"""Blast (chain_id 81457) — L2."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, Explorer, GasProfile, NativeToken, SimulationProfile, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.BLAST,
        name="blast",
        chain_id=81457,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="ETH",
            name="Ethereum",
            decimals=18,
            wrapped_address="0x4300000000000000000000000000000000000004",
            coingecko_id="ethereum",
            wrapped_symbol="WETH",
            wrapped_coingecko_id="weth",
            slip44=60,  # SLIP-44 coin type for Ether (CAIP-19 native)
        ),
        gas=GasProfile(
            buffer=1.5,
            simulation_buffer=0.5,
            price_cap_gwei=10,
            cost_cap_native=None,
        ),
        explorer=Explorer(browse_url="https://blastscan.io"),
        timeouts=Timeouts(
            tx_confirmation=None,  # legacy: not in CHAIN_TX_TIMEOUTS
            grpc_execute=None,  # legacy: not in CHAIN_GRPC_EXECUTE_TIMEOUTS
        ),
        simulation=SimulationProfile(tenderly_supported=True),
        # VIB-4851 (B1): per-vendor external ids, transposed from the legacy
        # standalone vendor maps (CoinGecko / DexScreener / GeckoTerminal /
        # DeFiLlama / Zerion / Moralis / OKX). Values verbatim incl. case.
        external_ids={
            "coingecko": "blast",
            "dexscreener": "blast",
        },
        aliases=(),
    )
)
