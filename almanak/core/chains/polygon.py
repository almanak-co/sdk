"""Polygon PoS (chain_id 137) — Ethereum sidechain."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, GasProfile, NativeToken, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.POLYGON,
        name="polygon",
        chain_id=137,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="MATIC",
            name="Polygon",
            decimals=18,
            wrapped_address="0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
        ),
        gas=GasProfile(
            buffer=1.2,
            simulation_buffer=0.2,
            price_cap_gwei=500,
            cost_cap_native=50.0,
        ),
        timeouts=Timeouts(
            tx_confirmation=180,
            grpc_execute=360,
        ),
        aliases=("matic",),
    )
)
