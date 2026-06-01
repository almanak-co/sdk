"""Sonic (chain_id 146) — EVM-compatible L1.

Several legacy dicts had no Sonic entry (CHAIN_GAS_BUFFERS,
CHAIN_TX_TIMEOUTS, CHAIN_GAS_COST_CAPS_NATIVE). For those, the framework
default applied at lookup time. The descriptor reuses those exact
framework defaults (1.2 buffer, None timeouts, None cost cap) so the
derived legacy view is byte-identical at the lookup boundary, even though
the dict now formally contains an entry.
"""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, GasProfile, NativeToken, RpcProfile, Timeouts
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
            # VIB-4879: bumped 100 → 200. Mainnet snapshot 2026-05-27
            # observed Sonic live gas at ~55 gwei; previous 100-gwei cap
            # left only ~1.8× headroom for spikes. 200 gives ~3.6× while
            # staying well below SANE_GWEI_CEILING (10_000).
            price_cap_gwei=200,
            cost_cap_native=None,
        ),
        timeouts=Timeouts(
            tx_confirmation=None,  # legacy: not in CHAIN_TX_TIMEOUTS
            grpc_execute=300,
        ),
        rpc=RpcProfile(
            public_rpc="https://sonic-rpc.publicnode.com",
            alchemy_prefix="sonic",
            anvil_port=8553,
        ),
        # VIB-4872 (W6-followup): chain half of legacy CHAIN_TOKENS.
        tokens={
            "usdc": "0x29219dd400f2Bf60E5a23d13Be72B486D4038894",
            "weth": "0x50c42dEAcD8Fc9773493ED674b675bE577f2634b",
            "ws": "0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38",
        },
        aliases=(),
    )
)
