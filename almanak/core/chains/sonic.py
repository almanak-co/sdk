"""Sonic (chain_id 146) — EVM-compatible L1.

Several legacy dicts had no Sonic entry (CHAIN_GAS_BUFFERS,
CHAIN_TX_TIMEOUTS, CHAIN_GAS_COST_CAPS_NATIVE). For those, the framework
default applied at lookup time. The descriptor reuses those exact
framework defaults (1.2 buffer, None timeouts, None cost cap) so the
derived legacy view is byte-identical at the lookup boundary, even though
the dict now formally contains an entry.
"""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, GasProfile, NativeToken, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.SONIC,
        name="sonic",
        chain_id=146,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="S",
            name="Sonic",
            decimals=18,
            wrapped_address="0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38",
        ),
        gas=GasProfile(
            buffer=None,  # legacy: not in CHAIN_GAS_BUFFERS (falls back to DEFAULT_GAS_BUFFER)
            simulation_buffer=0.1,
            price_cap_gwei=100,
            cost_cap_native=None,
        ),
        timeouts=Timeouts(
            tx_confirmation=None,  # legacy: not in CHAIN_TX_TIMEOUTS
            grpc_execute=300,
        ),
        aliases=(),
    )
)
