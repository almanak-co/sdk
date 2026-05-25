"""Mantle (chain_id 5000) — L2."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, GasProfile, NativeToken, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.MANTLE,
        name="mantle",
        chain_id=5000,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="MNT",
            name="Mantle",
            decimals=18,
            wrapped_address="0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8",
        ),
        gas=GasProfile(
            buffer=1.5,
            simulation_buffer=0.5,
            price_cap_gwei=10,
            cost_cap_native=50.0,
        ),
        timeouts=Timeouts(
            tx_confirmation=120,
            grpc_execute=300,
        ),
        aliases=(),
    )
)
