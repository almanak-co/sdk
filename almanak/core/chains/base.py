"""Base (chain_id 8453) — Coinbase L2 (OP Stack)."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, GasProfile, NativeToken, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.BASE,
        name="base",
        chain_id=8453,
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
        aliases=(),
    )
)
