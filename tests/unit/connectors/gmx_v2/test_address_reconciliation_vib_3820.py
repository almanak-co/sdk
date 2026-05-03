"""Regression guards for VIB-3820 (QA-PostFixes April31 BUG-26).

Two GMX V2 contract registries existed in the codebase and silently drifted:

* ``almanak/core/contracts.py:GMX_V2[chain]`` — the **canonical** registry,
  hot-path-fed into ``gmx_v2/sdk.py`` via ``_build_chain_address_map``. The
  comment at construction declares this is verified against
  ``github.com/gmx-io/gmx-synthetics/deployments/<chain>`` and the live GMX
  REST markets endpoint.
* ``almanak/framework/connectors/gmx_v2/adapter.py:GMX_V2_ADDRESSES`` — a
  parallel registry used by the adapter for bookkeeping. Audit revealed the
  Avalanche ``order_vault`` had drifted (``0xee7d43517A62Fa0ac642E22Eb93A93f82D0d3dF6``)
  while ``core/contracts.py`` carried the real one
  (``0xD3D60D22d415aD43b7e64b510D86A30f19B1B12C``). The adapter field was
  unused on the SDK hot path (no execution-time consequence) but the drift
  masqueraded as official source — any operator reading adapter.py to verify
  on-chain state would have been misled.

This test pins both registries together so future drift fails CI before it
reaches Avalanche-perp users. The keys present in BOTH registries must
contain the same address per chain. ``core/contracts.py`` is canonical; if
they ever disagree the test fails identifying which key drifted, prompting a
manual reconciliation against the GMX deployments repo.
"""

from __future__ import annotations

import pytest

from almanak.core.contracts import GMX_V2
from almanak.framework.connectors.gmx_v2.adapter import GMX_V2_ADDRESSES

# Canonical-to-adapter key aliases for semantically equivalent fields whose
# names differ between the two registries. ``core/contracts.py`` uses the
# shorter ``reader`` key; the adapter calls the same on-chain contract
# ``synthetics_reader``. Without this alias the test was silently skipping the
# critical reader contract — adapter.py's bare ``reader`` field is a stale
# legacy address and is intentionally orphaned (no canonical analogue).
CANONICAL_TO_ADAPTER_KEY = {
    "reader": "synthetics_reader",
}


class TestGmxV2RegistryReconciliation:
    @pytest.mark.parametrize("chain", sorted(GMX_V2.keys()))
    def test_overlapping_keys_match(self, chain: str) -> None:
        """Every canonical key (with alias mapping) must match the adapter."""
        assert chain in GMX_V2_ADDRESSES, (
            f"adapter.py:GMX_V2_ADDRESSES is missing chain '{chain}' that "
            f"exists in core/contracts.py:GMX_V2"
        )

        canonical = GMX_V2[chain]
        adapter = GMX_V2_ADDRESSES[chain]

        for canonical_key, canonical_value in sorted(canonical.items()):
            adapter_key = CANONICAL_TO_ADAPTER_KEY.get(canonical_key, canonical_key)
            if adapter_key not in adapter:
                # Not all canonical keys exist on the adapter side (e.g. the
                # per-market singletons live only in core). Skip silently —
                # this guard catches address drift, not key inventory drift.
                continue
            assert canonical_value.lower() == adapter[adapter_key].lower(), (
                f"Registry drift on '{chain}.{canonical_key}' "
                f"(adapter key: '{adapter_key}'): "
                f"core/contracts.py says {canonical_value!r}, "
                f"adapter.py says {adapter[adapter_key]!r}. "
                f"core/contracts.py is canonical (verified against the GMX "
                f"deployments repo); update adapter.py to match."
            )

    def test_adapter_chains_subset_of_canonical(self) -> None:
        """An adapter chain absent from the canonical registry would be
        reconciled against nothing — surface that as an explicit failure
        rather than letting the parametrized test silently skip it."""
        extra = sorted(set(GMX_V2_ADDRESSES) - set(GMX_V2))
        assert not extra, (
            f"adapter.py:GMX_V2_ADDRESSES has chain(s) absent from "
            f"core/contracts.py:GMX_V2 canonical registry: {extra}. "
            f"Either add the chain to GMX_V2 (preferred — core is canonical) "
            f"or remove it from the adapter."
        )

    def test_avalanche_order_vault_is_canonical(self) -> None:
        """The specific drift VIB-3820 closed."""
        canonical = "0xD3D60D22d415aD43b7e64b510D86A30f19B1B12C"
        assert GMX_V2["avalanche"]["order_vault"] == canonical
        assert GMX_V2_ADDRESSES["avalanche"]["order_vault"] == canonical

    def test_arbitrum_order_vault_unchanged(self) -> None:
        """Sanity: Arbitrum side was already consistent — guard against accidental edits."""
        canonical = "0x31eF83a530Fde1B38EE9A18093A333D8Bbbc40D5"
        assert GMX_V2["arbitrum"]["order_vault"] == canonical
        assert GMX_V2_ADDRESSES["arbitrum"]["order_vault"] == canonical
