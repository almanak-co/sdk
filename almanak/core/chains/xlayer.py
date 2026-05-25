"""X Layer (chain_id 196) — zkEVM L2 (Polygon CDK, OKX)."""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, GasProfile, NativeToken, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.XLAYER,
        name="xlayer",
        chain_id=196,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="OKB",
            name="OKB",
            decimals=18,
            wrapped_address="0xe538905cf8410324e03A5A23C1c177a474D59b2b",
        ),
        gas=GasProfile(
            buffer=1.3,
            simulation_buffer=0.3,
            price_cap_gwei=10,
            cost_cap_native=1.0,
        ),
        timeouts=Timeouts(
            tx_confirmation=120,
            grpc_execute=300,
        ),
        aliases=(),
    )
)
