"""BNB Smart Chain (chain_id 56).

The legacy ``CHAIN_TX_TIMEOUTS`` had no entry for BSC (it fell back to the
framework default 120s). The descriptor captures that by leaving
``tx_confirmation`` ``None`` — the orchestrator's ``.get(chain, DEFAULT)``
still picks up the framework default.
"""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, GasProfile, NativeToken, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.BSC,
        name="bsc",
        chain_id=56,
        family=ChainFamily.EVM,
        native=NativeToken(
            symbol="BNB",
            name="BNB",
            decimals=18,
            wrapped_address="0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
        ),
        gas=GasProfile(
            buffer=1.2,
            simulation_buffer=0.1,
            price_cap_gwei=20,
            cost_cap_native=0.05,
        ),
        timeouts=Timeouts(
            tx_confirmation=None,  # legacy: not in CHAIN_TX_TIMEOUTS
            grpc_execute=300,
        ),
        aliases=("bnb", "binance"),
    )
)
