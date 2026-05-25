"""Monad (chain_id 143) — high-throughput EVM-compatible L1."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, GasProfile, NativeToken, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.MONAD,
        name="monad",
        chain_id=143,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="MON",
            name="Monad",
            decimals=18,
            wrapped_address="0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A",
        ),
        gas=GasProfile(
            buffer=1.1,
            simulation_buffer=0.1,
            price_cap_gwei=50,
            cost_cap_native=10.0,
        ),
        timeouts=Timeouts(
            tx_confirmation=60,
            grpc_execute=240,
        ),
        aliases=(),
    )
)
