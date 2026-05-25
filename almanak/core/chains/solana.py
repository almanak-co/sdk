"""Solana — non-EVM chain.

The legacy ``CHAIN_IDS`` mapped Solana to ``0`` (sentinel for "non-EVM");
we keep that contract. EVM gas / timeout knobs do not apply, so the
``GasProfile`` is populated with no-op fallback values (None caps,
``buffer`` carrying the framework default 1.0 — Solana uses
compute-unit + priority-fee accounting, not gas multipliers).
"""

from almanak.core.enums import Chain, ChainFamily

from ._descriptor import ChainDescriptor, GasProfile, NativeToken, Timeouts
from ._registry import register_chain

DESCRIPTOR = register_chain(
    ChainDescriptor(
        enum=Chain.SOLANA,
        name="solana",
        chain_id=0,  # Non-EVM sentinel; matches legacy CHAIN_IDS
        family=ChainFamily.SOLANA,
        native=NativeToken(
            symbol="SOL",
            name="Solana",
            decimals=9,
            # Wrapped SOL mint (SPL token, base58)
            wrapped_address="So11111111111111111111111111111111111111112",
        ),
        gas=GasProfile(
            buffer=None,
            simulation_buffer=None,
            price_cap_gwei=None,
            cost_cap_native=None,
        ),
        timeouts=Timeouts(
            tx_confirmation=None,
            grpc_execute=None,
        ),
        aliases=("sol",),
    )
)
