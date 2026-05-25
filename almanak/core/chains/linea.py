"""Linea (chain_id 59144) — zkEVM L2."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, GasProfile, NativeToken, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.LINEA,
        name="linea",
        chain_id=59144,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="ETH",
            name="Ethereum",
            decimals=18,
            wrapped_address="0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f",
        ),
        gas=GasProfile(
            buffer=1.5,
            simulation_buffer=0.3,
            price_cap_gwei=10,
            cost_cap_native=None,
        ),
        timeouts=Timeouts(
            tx_confirmation=None,  # legacy: not in CHAIN_TX_TIMEOUTS
            grpc_execute=None,  # legacy: not in CHAIN_GRPC_EXECUTE_TIMEOUTS
        ),
        aliases=(),
    )
)
