"""Ethereum mainnet (chain_id 1) — L1.

Source values mirror the legacy scattered dicts as of VIB-4801. Do not
change numeric values here without an explicit owner sign-off; the
chain_id is the on-the-wire identifier owned by ``metrics-database``.
"""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, GasProfile, NativeToken, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.ETHEREUM,
        name="ethereum",
        chain_id=1,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="ETH",
            name="Ethereum",
            decimals=18,
            wrapped_address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        ),
        gas=GasProfile(
            buffer=1.1,
            simulation_buffer=0.1,
            price_cap_gwei=300,
            cost_cap_native=0.1,
        ),
        timeouts=Timeouts(
            tx_confirmation=300,
            grpc_execute=600,
        ),
        aliases=("eth", "mainnet"),
    )
)
