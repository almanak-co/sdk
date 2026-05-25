"""Optimism (chain_id 10) — L2 (Optimistic rollup)."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, GasProfile, NativeToken, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.OPTIMISM,
        name="optimism",
        chain_id=10,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="ETH",
            name="Ethereum",
            decimals=18,
            wrapped_address="0x4200000000000000000000000000000000000006",
        ),
        gas=GasProfile(
            buffer=1.5,
            simulation_buffer=0.5,
            price_cap_gwei=10,
            cost_cap_native=0.01,
        ),
        timeouts=Timeouts(
            tx_confirmation=120,
            grpc_execute=300,
        ),
        aliases=("op",),
    )
)
