"""Smoke tests for every connector that publishes ``GatewayAddressCapability``.

W1 / VIB-4853 moves per-protocol contract addresses out of the central
``almanak/core/contracts.py`` into per-connector ``addresses.py`` modules,
surfaced to non-connector callers through ``GatewayAddressCapability``.
This test guards two basic contract properties on the live registry:

1. Every connector that implements the capability declares at least one
   chain (we don't accept "I publish the capability but support nothing"
   on production providers — only the test scaffolds in
   ``test_gateway_capability_runtime_checks.py`` do that).
2. ``address_supported_chains()`` and ``addresses_for(chain)`` are
   internally consistent: every chain in the supported-set returns a
   non-empty mapping, and unknown chains return an empty mapping.

If a future Phase-3 scaffold legitimately ships with zero chains
(Solana-native connector resolving accounts at runtime, etc.) it must
be added to ``_ZERO_CHAIN_EXEMPT`` with a one-line reason.
"""

from __future__ import annotations

from almanak.connectors._base.gateway_capabilities import GatewayAddressCapability
from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

# Connectors permitted to publish the capability with an empty
# ``address_supported_chains()``. Empty entries are illegal by default — Solana
# / off-chain-only connectors that resolve accounts at runtime are the only
# legitimate case and must be listed here explicitly.
_ZERO_CHAIN_EXEMPT: frozenset[str] = frozenset()


def test_address_capability_has_providers() -> None:
    """At least one connector must publish ``GatewayAddressCapability``.

    The capability is the W1 replacement for the centralised
    ``almanak/core/contracts.py``; if none of the connectors implement it,
    the deletion was incomplete.
    """
    providers = GATEWAY_REGISTRY.capability_providers(GatewayAddressCapability)
    assert len(providers) > 0, (
        "No connector implements GatewayAddressCapability — W1 migration is "
        "incomplete. Each connector with on-chain addresses must publish "
        "addresses_for() + address_supported_chains()."
    )


def test_every_provider_declares_supported_chains() -> None:
    """Production providers must declare at least one supported chain."""
    failures: list[str] = []
    for provider in GATEWAY_REGISTRY.capability_providers(GatewayAddressCapability):
        chains = provider.address_supported_chains()
        if not chains and provider.protocol not in _ZERO_CHAIN_EXEMPT:
            failures.append(
                f"  {provider.protocol}: address_supported_chains() is empty "
                f"(add to _ZERO_CHAIN_EXEMPT if intentional)"
            )
    assert not failures, "\n".join(failures)


def test_supported_chain_set_matches_addresses_for() -> None:
    """``address_supported_chains()`` and ``addresses_for()`` agree on chains.

    Every chain in the set must produce a non-empty mapping; the set must
    be exactly the set of chains for which a non-empty mapping is returned.
    """
    failures: list[str] = []
    for provider in GATEWAY_REGISTRY.capability_providers(GatewayAddressCapability):
        chains = provider.address_supported_chains()
        if not isinstance(chains, frozenset):
            failures.append(
                f"  {provider.protocol}.address_supported_chains() returned "
                f"{type(chains).__name__}, expected frozenset"
            )
            continue
        for chain in chains:
            mapping = provider.addresses_for(chain)
            if not mapping:
                failures.append(
                    f"  {provider.protocol}.addresses_for({chain!r}) returned "
                    f"empty mapping but chain is in address_supported_chains()"
                )
    assert not failures, "\n".join(failures)


def test_addresses_for_unknown_chain_returns_empty() -> None:
    """Unknown chains must return an empty mapping, not raise."""
    failures: list[str] = []
    for provider in GATEWAY_REGISTRY.capability_providers(GatewayAddressCapability):
        try:
            result = provider.addresses_for("this-chain-does-not-exist-12345")
        except Exception as exc:  # noqa: BLE001 — defensive test surface
            failures.append(
                f"  {provider.protocol}.addresses_for('this-chain-does-not-exist-12345') "
                f"raised {type(exc).__name__}: {exc}"
            )
            continue
        if result:
            failures.append(
                f"  {provider.protocol}.addresses_for('this-chain-does-not-exist-12345') "
                f"returned non-empty mapping: {dict(result)}"
            )
    assert not failures, "\n".join(failures)
