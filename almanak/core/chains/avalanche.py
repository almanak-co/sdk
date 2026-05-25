"""Avalanche C-Chain (chain_id 43114)."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, GasProfile, NativeToken, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.AVALANCHE,
        name="avalanche",
        chain_id=43114,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="AVAX",
            name="Avalanche",
            decimals=18,
            wrapped_address="0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        ),
        gas=GasProfile(
            buffer=1.1,
            simulation_buffer=0.1,
            price_cap_gwei=100,
            cost_cap_native=1.0,
        ),
        timeouts=Timeouts(
            tx_confirmation=120,
            grpc_execute=300,
        ),
        aliases=("avax",),
    )
)
