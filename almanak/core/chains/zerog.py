"""0G Chain (chain_id 16661) — AI L1 (preview support)."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, GasProfile, NativeToken, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.ZEROG,
        name="zerog",
        chain_id=16661,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="A0GI",
            name="0G",
            decimals=18,
            wrapped_address="0x1Cd0690fF9a693f5EF2dD976660a8dAFc81A109c",
        ),
        gas=GasProfile(
            buffer=1.1,
            simulation_buffer=0.1,
            price_cap_gwei=50,
            cost_cap_native=10.0,
        ),
        timeouts=Timeouts(
            tx_confirmation=120,
            grpc_execute=300,
        ),
        aliases=("0g",),
    )
)
