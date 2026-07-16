"""Guard: connector manifests must not outrun their own runtime chain gate.

Every connector carries (up to) two independent, hand-authored declarations
of "which chains does this protocol run on":

1. ``Connector.strategy_chains`` on the connector manifest (``connector.py``)
   — the venue list the strategy layer advertises, and what
   ``almanak info matrix`` renders (via ``manifest.chains``, derived 1:1 from
   ``strategy_chains`` in ``_manifest_from_descriptor``).
2. ``SUPPORTED_CHAINS_BY_PROTOCOL`` in the connector's own
   ``supported_chains.py`` — the RUNTIME-ENFORCED gate, aggregated by
   :class:`~almanak.connectors._strategy_base.supported_chains_registry.SupportedChainsRegistry`
   into ``almanak.framework.execution.config.SUPPORTED_PROTOCOLS``.

These two lists are maintained by hand in different files and can drift.
When ``strategy_chains`` advertises a chain the gate doesn't list, the
matrix / docs / addresses / fee-model all say a chain is supported, a
strategy is built for it, and then the runtime gate rejects it — a
"declared but not reachable" trap (VIB-740: PancakeSwap V3 was declared
supported on ``base`` in seven places in its own connector, but
``supported_chains.py`` omitted "base", so every PancakeSwap-V3-on-Base
strategy was rejected at runtime).

This test asserts the invariant that prevents that class of bug: for every
connector that declares BOTH ``strategy_chains`` and a ``supported_chains``
ownership spec, every chain in ``strategy_chains`` must also appear in the
aggregated gate for that connector's protocol key(s). The gate is allowed to
list MORE chains than the manifest advertises (execution-side coverage can
outrun what the strategy layer has wired up) — only the reverse direction
(manifest promises something the gate rejects) is a bug.

Connectors with no ``supported_chains`` spec at all are out of scope here —
they have no enforced gate to drift from (nothing to compare against).
"""

from __future__ import annotations

import pytest

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.supported_chains_registry import SupportedChainsRegistry


def _gate_chains_for(keys: tuple[str, ...]) -> frozenset[str]:
    """Union the runtime-enforced chain set across every protocol key a connector owns."""
    chains: set[str] = set()
    for key in keys:
        resolved = SupportedChainsRegistry.get(key)
        if resolved:
            chains |= set(resolved)
    return frozenset(chains)


def _connectors_with_both_declarations() -> list:
    """Connectors declaring both ``strategy_chains`` and a ``supported_chains`` spec.

    A connector with ``strategy_chains=None`` is an off-chain venue (no chain
    list to check). A connector with no ``supported_chains`` spec has no
    enforced gate at all — nothing for the manifest to drift from.
    """
    return [
        connector
        for connector in CONNECTOR_REGISTRY.with_supported_chains()
        if connector.strategy_chains is not None
    ]


def test_every_connector_with_a_gate_has_at_least_one_strategy_chain() -> None:
    """Sanity check the fixture set isn't accidentally empty (would silently no-op the guard)."""
    assert len(_connectors_with_both_declarations()) > 0


@pytest.mark.parametrize(
    "connector",
    _connectors_with_both_declarations(),
    ids=lambda connector: connector.name,
)
def test_strategy_chains_is_subset_of_enforced_gate(connector) -> None:
    """``strategy_chains`` must not advertise a chain the runtime gate rejects.

    Failure here means: the connector manifest (and everything derived from
    it — ``almanak info matrix``, docs, address tables, fee models) claims a
    chain is supported, but ``SUPPORTED_CHAINS_BY_PROTOCOL`` in the
    connector's own ``supported_chains.py`` will reject a strategy on that
    chain at runtime. Fix by adding the missing chain(s) to
    ``supported_chains.py`` — but ONLY once you've confirmed the protocol is
    actually deployed there (real, non-placeholder addresses in the
    connector's ``addresses.py`` / real subgraph id / etc.). If the protocol
    is NOT really deployed on that chain, the manifest's ``strategy_chains``
    is the one that's wrong — trim it there instead.
    """
    gate_chains = _gate_chains_for(connector.supported_chains.keys)
    manifest_chains = frozenset(connector.strategy_chains)

    missing = manifest_chains - gate_chains
    assert not missing, (
        f"connector {connector.name!r} declares strategy_chains={sorted(manifest_chains)} "
        f"but its runtime-enforced gate (supported_chains.py via "
        f"keys={connector.supported_chains.keys}) only allows {sorted(gate_chains)}. "
        f"Chain(s) {sorted(missing)} would compile a strategy that gets rejected at runtime. "
        f"Add them to {connector.supported_chains.module}.SUPPORTED_CHAINS_BY_PROTOCOL if the "
        f"protocol really is deployed there, or trim strategy_chains on the connector manifest "
        f"if it isn't."
    )
