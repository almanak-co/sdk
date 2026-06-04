"""Plasma (chain_id 9745) — EVM L1."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import (
    ChainDescriptor,
    GasProfile,
    NativeToken,
    RpcProfile,
    SimulationProfile,
    Timeouts,
)
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.PLASMA,
        name="plasma",
        chain_id=9745,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="XPL",
            name="Plasma",
            decimals=18,
            wrapped_address="0x6100E367285b01F48D07953803A2d8dCA5D19873",
        ),
        gas=GasProfile(
            buffer=1.1,
            simulation_buffer=0.1,
            price_cap_gwei=50,
            cost_cap_native=None,
        ),
        timeouts=Timeouts(
            tx_confirmation=120,
            grpc_execute=300,
        ),
        rpc=RpcProfile(
            public_rpc="https://rpc.plasma.to",
            alchemy_prefix="plasma",
            tenderly_subdomain="plasma",
            anvil_port=8554,
        ),
        simulation=SimulationProfile(tenderly_supported=True),
        # VIB-4851 (B1): per-vendor external ids, transposed from the legacy
        # standalone vendor maps (CoinGecko / DexScreener / GeckoTerminal /
        # DeFiLlama / Zerion / Moralis / OKX). Values verbatim incl. case.
        external_ids={
            "coingecko": "plasma",
            "dexscreener": "plasma",
            "zerion": "plasma",
        },
        aliases=(),
    )
)
