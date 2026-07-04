"""Connector-owned pool reader spec for Aerodrome Slipstream."""

from __future__ import annotations

from almanak.connectors._strategy_base.pool_reader import PoolReaderSpec

from .addresses import AERODROME

_KNOWN_POOLS: dict[str, dict[tuple[str, str, int], str]] = {
    "base": {
        (
            "0x4200000000000000000000000000000000000006",
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            100,
        ): "0xb2cc224c1c9feE385f8ad6a55b4d94E92359DC59",
        (
            "0x4200000000000000000000000000000000000006",
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            200,
        ): "0x6cDcb1C4A4D1C3C6d054b27AC5B77e89eAFb971d",
    },
}

POOL_READER_SPEC = PoolReaderSpec(
    protocol="aerodrome",
    aliases=("aerodrome_slipstream",),
    factory_addresses={chain: addrs["cl_factory"] for chain, addrs in AERODROME.items() if "cl_factory" in addrs},
    known_pools=_KNOWN_POOLS,
    get_pool_selector="0x28af8d0b",
    # Slipstream keys pools by TICK SPACING, not Uniswap fee tier. Snapshot of
    # the Base CL factory's ``tickSpacings()`` (governance-extensible — keep
    # in sync if it grows).
    candidate_pool_keys=(1, 10, 50, 100, 200, 2000),
)

__all__ = ["POOL_READER_SPEC"]
