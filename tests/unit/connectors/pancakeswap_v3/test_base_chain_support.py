"""Regression guard: PancakeSwap V3 must be usable on Base end-to-end.

VIB-740 / VIB-exp19 root cause: PancakeSwap V3 was declared supported on
``base`` in seven places in its own connector (``connector.py`` twice,
``addresses.py``, ``fee_model.py``, ``pool_reader.py``, the ``__init__.py``
docstring, and a Base subgraph id) — everywhere EXCEPT the one place that is
actually enforced at runtime: ``supported_chains.py``'s
``SUPPORTED_CHAINS_BY_PROTOCOL``. Every other declaration made "base" look
supported (including in ``almanak info matrix``, which reads
``strategy_chains``), while the runtime gate silently rejected it. This was a
regression: PancakeSwap V3 LP-on-Base last worked at Kitchen Loop iteration
70, so the gate was added later without "base".

Independent on-chain confirmation PancakeSwap V3 really is deployed on Base:
reading the real Base pool ``0x72AB388E2E2F6FaceF59E3C3FA2C4E29011c2D38``
returns ``factory() == 0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865`` — byte-
identical to ``PANCAKESWAP_V3["base"]["factory"]`` below.

This file pins "base" is present everywhere pancakeswap_v3 declares chain
coverage, so a partial fix (only patching one of the seven places) fails
loudly here instead of silently regressing again. The general drift class
(any connector's ``strategy_chains`` outrunning its own runtime gate) is
covered for ALL connectors by
``tests/unit/connectors/registry/test_supported_chains_manifest_gate.py``;
this file exists in addition because pancakeswap_v3/base is the specific,
previously-broken case worth pinning by name.
"""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.supported_chains_registry import supported_chains_for
from almanak.connectors.pancakeswap_v3.addresses import PANCAKESWAP_V3
from almanak.connectors.pancakeswap_v3.connector import CONNECTOR
from almanak.connectors.pancakeswap_v3.fee_model import BACKTEST_EXPORT_METADATA
from almanak.connectors.pancakeswap_v3.pool_reader import POOL_READER_SPEC

# The real, on-chain-verified PancakeSwap V3 factory address on Base (see
# module docstring for the pool read that confirmed it byte-for-byte).
_BASE_FACTORY = "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"


def test_base_is_in_the_runtime_enforced_gate() -> None:
    """The actual bug: ``supported_chains.py`` must list "base".

    This is what ``almanak.framework.execution.config.SUPPORTED_PROTOCOLS``
    (built from :func:`supported_chains_for`) enforces at strategy-build
    time — the ONE place all the others don't matter if this is wrong.
    """
    assert "base" in supported_chains_for("pancakeswap_v3")


def test_base_is_in_connector_manifest_strategy_chains() -> None:
    """``almanak info matrix`` (and the strategy layer) advertise "base"."""
    assert CONNECTOR.strategy_chains is not None
    assert "base" in CONNECTOR.strategy_chains


def test_base_is_in_the_registered_manifest() -> None:
    """The connector actually registers with the strategy-side registry, base included."""
    manifest = next(m for m in CONNECTOR_REGISTRY.all() if m.name == "pancakeswap_v3")
    assert manifest.strategy_chains is not None
    assert "base" in manifest.strategy_chains


def test_base_has_real_addresses_matching_the_on_chain_factory() -> None:
    """``addresses.py`` must carry the real Base factory, not a placeholder."""
    assert "base" in PANCAKESWAP_V3
    assert PANCAKESWAP_V3["base"]["factory"] == _BASE_FACTORY
    # Distinct SmartRouter per chain — Base is not a copy-paste of BSC/Ethereum/Arbitrum.
    assert PANCAKESWAP_V3["base"]["swap_router"] == "0x678Aa4bF4E210cf2166753e054d5b7c31cc7fa86"


def test_base_is_in_the_pool_reader_factory_chains() -> None:
    """Pool discovery on Base must resolve through the real factory address."""
    assert POOL_READER_SPEC.factory_addresses.get("base") == _BASE_FACTORY


def test_base_is_in_the_backtest_fee_model_supported_chains() -> None:
    """Backtest PnL fee modelling must cover Base."""
    assert "base" in BACKTEST_EXPORT_METADATA["supported_chains"]


def test_base_is_in_the_dex_volume_liquidity_subgraph_ids() -> None:
    """Base must have its own liquidity subgraph id (backtest volume/liquidity data)."""
    dex_volume = CONNECTOR.dex_volume
    assert dex_volume is not None
    assert "base" in dex_volume.chains
    assert "base" in dex_volume.liquidity_subgraph_ids
