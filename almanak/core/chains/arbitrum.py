"""Arbitrum One (chain_id 42161) — L2 (Optimistic rollup)."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, GasProfile, NativeToken, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.ARBITRUM,
        name="arbitrum",
        chain_id=42161,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="ETH",
            name="Ethereum",
            decimals=18,
            wrapped_address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
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
        aliases=("arb",),
    )
)
