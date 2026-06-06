"""Spark contract addresses per chain.

Single source of truth for this connector's on-chain addresses. Replaces
the Spark entries previously held in
``almanak.framework.intents.compiler_constants.LENDING_POOL_ADDRESSES``
(VIB-4872 / epic VIB-4851), and supersedes ``SPARK_POOL_ADDRESSES`` /
``SPARK_POOL_DATA_PROVIDER_ADDRESSES`` / ``SPARK_ORACLE_ADDRESSES`` from
``adapter.py`` as the canonical reference (the adapter module re-exports
them for backward compatibility).

Spark is an Aave V3 fork — the same per-chain address-kind vocabulary
applies (``pool`` / ``pool_data_provider`` / ``oracle``). The contract-
kind vocabulary is connector-private — callers outside this folder
should consume the registry, not guess key names.
"""

from __future__ import annotations

SPARK: dict[str, dict[str, str]] = {
    "ethereum": {
        "pool": "0xC13e21B648A5Ee794902342038FF3aDAB66BE987",
        "pool_data_provider": "0xFc21d6d146E6086B8359705C8b28512a983db0cb",
        "oracle": "0x8105f69D9C41644c6A0803fDA7D03Aa70996cFD9",
    },
}

__all__ = ["SPARK"]
