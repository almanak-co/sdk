"""Radiant V2 contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the Radiant V2 entries previously held in
``almanak.framework.intents.compiler_constants.LENDING_POOL_ADDRESSES``
and ``LENDING_POOL_DATA_PROVIDERS`` (VIB-4872 / epic VIB-4851).

Radiant V2 is an Aave V2 fork — same per-chain address-kind vocabulary
applies (``pool`` / ``pool_data_provider``). The Aave V2 / V3 lending
ABI is identical for the pre-flight ``getReserveConfigurationData``
selector + return layout, so Radiant V2's data provider plugs into the
same lending pre-flight code path the Aave V3 connector uses.

Only Ethereum is supported. The Arbitrum Radiant V2 LendingPool proxy
was reduced to a stub after the Oct 2024 attack and the framework
excludes ``radiant_v2 / arbitrum`` at every layer (see issues
#1842 / #1847 / #1889 and the previous comment in
``compiler_constants.LENDING_POOL_ADDRESSES``).

The contract-kind vocabulary is connector-private — callers outside
this folder should consume the registry, not guess key names.
"""

from __future__ import annotations

RADIANT_V2: dict[str, dict[str, str]] = {
    "ethereum": {
        "pool": "0xA950974f64aA33f27F6C5e017eEE93BF7588ED07",
        # AaveProtocolDataProvider equivalent; same selector + ABI as Aave V3.
        "pool_data_provider": "0x362f3BB63Cff83bd169aE1793979E9e537993813",
    },
}


__all__ = ["RADIANT_V2"]
