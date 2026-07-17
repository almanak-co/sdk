"""Tests for ``Connector.external_ids`` and the ``vendor_protocol_map`` helper (plan 024).

Pins:
* Unknown-vendor ``ValueError`` fires at construction time.
* ``vendor_protocol_map("defillama")`` produces the exact 9-entry map from the
  nine connector manifests declared in plan 024.
* Cross-parity: every connector that declares ``external_ids["defillama"]`` AND
  implements ``GatewayDefillamaSlugCapability.defillama_slug()`` must agree.
* ``_OFF_PLATFORM_LLAMA_SLUGS`` keys are exactly {jito, marinade, sanctum, morpho}.
"""

from __future__ import annotations

import pytest

from almanak.connectors._connector_descriptor import (
    KNOWN_PROTOCOL_VENDORS,
    Connector,
    vendor_protocol_map,
)
from almanak.connectors._base.types import ProtocolKind


# ---------------------------------------------------------------------------
# KNOWN_PROTOCOL_VENDORS gate
# ---------------------------------------------------------------------------


def test_known_protocol_vendors_contains_defillama() -> None:
    """``KNOWN_PROTOCOL_VENDORS`` must contain at least ``"defillama"``."""
    assert "defillama" in KNOWN_PROTOCOL_VENDORS


def test_unknown_vendor_raises_at_construction() -> None:
    """Constructing a ``Connector`` with an unrecognised vendor key raises ``ValueError``."""
    with pytest.raises(ValueError, match="unknown vendor keys"):
        Connector(
            name="test_unknown_vendor",
            kind=ProtocolKind.LENDING,
            external_ids={"notavendor": "some-slug"},
        )


def test_known_vendor_accepted() -> None:
    """A valid ``defillama`` key is accepted and frozen."""
    c = Connector(name="test_valid_vendor", kind=ProtocolKind.LENDING, external_ids={"defillama": "test-slug"})
    assert c.external_ids is not None
    assert c.external_ids["defillama"] == "test-slug"


def test_external_ids_frozen_after_construction() -> None:
    """``external_ids`` is a read-only proxy; mutation raises ``TypeError``."""
    from types import MappingProxyType

    c = Connector(name="test_frozen", kind=ProtocolKind.LENDING, external_ids={"defillama": "test-slug"})
    assert isinstance(c.external_ids, MappingProxyType)
    with pytest.raises((TypeError, AttributeError)):
        c.external_ids["defillama"] = "mutated"  # type: ignore[index]


# ---------------------------------------------------------------------------
# vendor_protocol_map
# ---------------------------------------------------------------------------


def test_vendor_protocol_map_defillama_exact_nine_entries() -> None:
    """``vendor_protocol_map("defillama")`` returns exactly the 9 plan-024 entries."""
    result = vendor_protocol_map("defillama")
    expected = {
        "aave_v3": "aave-v3",
        "compound_v3": "compound-v3",
        "uniswap_v3": "uniswap-v3",
        "aerodrome": "aerodrome-v1",
        "lido": "lido",
        "pancakeswap_v3": "pancakeswap-amm-v3",
        "kamino": "kamino-lending",
        "raydium": "raydium",
        "fluid": "fluid-dex",
    }
    assert result == expected, f"Got {sorted(result.items())}"


def test_vendor_protocol_map_unknown_vendor_returns_empty() -> None:
    """An unknown vendor key returns an empty dict (no-fail semantics)."""
    assert vendor_protocol_map("no_such_vendor") == {}


def test_vendor_protocol_map_empty_string_returns_empty() -> None:
    """An empty vendor string returns an empty dict."""
    assert vendor_protocol_map("") == {}


def test_vendor_protocol_map_case_insensitive() -> None:
    """Vendor key lookup is case-insensitive."""
    lower = vendor_protocol_map("defillama")
    upper = vendor_protocol_map("DeFiLlama")
    assert lower == upper


# ---------------------------------------------------------------------------
# Cross-parity: Connector.external_ids["defillama"] vs gateway defillama_slug()
# ---------------------------------------------------------------------------


def test_external_ids_defillama_parity_with_gateway_capability() -> None:
    """Every connector declaring ``external_ids["defillama"]`` AND implementing
    ``GatewayDefillamaSlugCapability.defillama_slug()`` must return the same slug.

    Modelled on ``test_gateway_defillama_slug_capability.py:67-82`` (plan 024).
    """
    from almanak.connectors._connector_descriptor import CONNECTOR_REGISTRY
    from almanak.connectors._base.gateway_capabilities import GatewayDefillamaSlugCapability
    from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

    # Build a map from connector (protocol) name -> gateway defillama_slug()
    gateway_slug_map: dict[str, str | None] = {}
    for provider in GATEWAY_REGISTRY.capability_providers(GatewayDefillamaSlugCapability):
        slug = provider.defillama_slug()
        gateway_slug_map[str(provider.protocol)] = slug

    mismatches: list[str] = []
    for connector in CONNECTOR_REGISTRY.with_external_ids():
        assert connector.external_ids is not None
        declared_slug = connector.external_ids.get("defillama")
        if declared_slug is None:
            continue
        # Only check parity where the gateway connector also declares a slug
        gateway_slug = gateway_slug_map.get(connector.name)
        if gateway_slug is None:
            continue  # Gateway does not implement the capability for this connector
        if declared_slug != gateway_slug:
            mismatches.append(
                f"{connector.name!r}: manifest says {declared_slug!r}, "
                f"gateway defillama_slug() returns {gateway_slug!r}"
            )

    assert not mismatches, (
        "Connector.external_ids['defillama'] and gateway defillama_slug() disagree:\n"
        + "\n".join(f"  {m}" for m in mismatches)
    )


# ---------------------------------------------------------------------------
# _OFF_PLATFORM_LLAMA_SLUGS residual guard
# ---------------------------------------------------------------------------


def test_off_platform_llama_slugs_keys() -> None:
    """``_OFF_PLATFORM_LLAMA_SLUGS`` must contain exactly {jito, marinade, sanctum, morpho}."""
    from almanak.framework.data.yields.aggregator import _OFF_PLATFORM_LLAMA_SLUGS

    assert set(_OFF_PLATFORM_LLAMA_SLUGS.keys()) == {"jito", "marinade", "sanctum", "morpho"}
