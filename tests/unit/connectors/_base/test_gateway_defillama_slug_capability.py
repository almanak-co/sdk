"""``GatewayDefillamaSlugCapability`` contract tests (VIB-4811 / Phase 3).

The pool-history dispatcher unions every registered connector's
``defillama_slug()`` + ``defillama_slug_aliases()`` into the live
``protocol -> slug`` dispatch dict (the pool-analytics service no longer
consumes DefiLlama slugs). Tests pin:

* ``isinstance(connector, GatewayDefillamaSlugCapability)`` is True iff
  the connector defines both ``defillama_slug`` and
  ``defillama_slug_aliases``.
* The registered ``uniswap_v3``, ``aerodrome``, ``aave_v3`` and
  ``compound_v3`` connectors contribute the expected slugs.
* The derived slug table matches the legacy hardcoded dispatch dict.
"""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayDefillamaSlugCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName


class _DefillamaImpl(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("llama_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def defillama_slug(self) -> str | None:
        return "demo-slug"

    def defillama_slug_aliases(self) -> dict[str, str]:
        return {"demo_v2": "demo-slug-v2"}


class _DefillamaNoneImpl(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("llama_none_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def defillama_slug(self) -> str | None:
        return None

    def defillama_slug_aliases(self) -> dict[str, str]:
        return {}


class _BareConnector(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("bare_llama_demo")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP


def test_defillama_slug_capability_runtime_isinstance() -> None:
    assert isinstance(_DefillamaImpl(), GatewayDefillamaSlugCapability)
    assert isinstance(_DefillamaNoneImpl(), GatewayDefillamaSlugCapability)
    assert not isinstance(_BareConnector(), GatewayDefillamaSlugCapability)


def test_defillama_slug_returns_canonical_and_aliases() -> None:
    inst = _DefillamaImpl()
    assert inst.defillama_slug() == "demo-slug"
    aliases = inst.defillama_slug_aliases()
    assert aliases == {"demo_v2": "demo-slug-v2"}


def test_defillama_slug_table_matches_legacy_dict() -> None:
    """The registry-derived slug table matches the Phase-2 hardcoded dict.

    The derivation now lives on the pool-history dispatcher — the
    pool-analytics service no longer consumes DefiLlama slugs (its
    structurally-dead matcher lane was deleted).
    """
    from almanak.gateway.data.pool_history.dispatcher import _defillama_slug_table

    expected = {
        "uniswap_v3": "uniswap-v3",
        "aerodrome": "aerodrome-v2",
        "aerodrome_slipstream": "aerodrome-slipstream",
        "pancakeswap_v3": "pancakeswap-amm-v3",
        "aave_v3": "aave-v3",
        "morpho": "morpho-blue",
        "compound_v3": "compound-v3",
    }
    assert _defillama_slug_table() == expected


def test_registered_connectors_advertise_defillama_slug() -> None:
    """The registered Phase-3/Phase-6 slug connectors expose the capability."""
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    providers = GATEWAY_REGISTRY.capability_providers(GatewayDefillamaSlugCapability)
    protocols = {str(p.protocol) for p in providers}
    # VIB-4811 (Phase 3) registered the first four.
    # VIB-4817 added pancakeswap_v3 + morpho_vault (publishing the ``morpho`` alias).
    assert {
        "uniswap_v3",
        "aerodrome",
        "aave_v3",
        "compound_v3",
        "pancakeswap_v3",
        "morpho_vault",
    }.issubset(protocols)


def test_pancakeswap_v3_provider_publishes_defillama_slug() -> None:
    """VIB-4817: pancakeswap_v3 provider publishes ``pancakeswap-amm-v3``."""
    from almanak.connectors.pancakeswap_v3.gateway.provider import (
        PancakeSwapV3GatewayConnector,
    )

    provider = PancakeSwapV3GatewayConnector()
    assert isinstance(provider, GatewayDefillamaSlugCapability)
    assert provider.defillama_slug() == "pancakeswap-amm-v3"
    assert provider.defillama_slug_aliases() == {}


def test_morpho_vault_provider_publishes_morpho_blue_alias() -> None:
    """VIB-4817: morpho_vault provider publishes the ``morpho`` -> ``morpho-blue`` alias."""
    from almanak.connectors.morpho_vault.gateway.provider import (
        MorphoVaultGatewayConnector,
    )

    provider = MorphoVaultGatewayConnector()
    assert isinstance(provider, GatewayDefillamaSlugCapability)
    # Vault connector itself has no canonical slug — the lending product
    # rides under the ``morpho`` alias.
    assert provider.defillama_slug() is None
    assert provider.defillama_slug_aliases() == {"morpho": "morpho-blue"}


def test_defillama_capability_does_not_imply_other_caps() -> None:
    from almanak.connectors._base.gateway_capabilities import (
        GatewayPoolHistoryCapability,
        GatewayServicerCapability,
    )

    inst: Any = _DefillamaImpl()
    assert not isinstance(inst, GatewayPoolHistoryCapability)
    assert not isinstance(inst, GatewayServicerCapability)
