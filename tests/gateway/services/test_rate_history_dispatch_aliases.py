"""Dispatch aliases + Aerodrome Slipstream TWAP registration.

Covers the gateway-side fixes from this session:
  - Aerodrome (Slipstream) now declares ``GatewayDexTwapCapability`` — the
    Slipstream pool exposes the Uniswap-V3 ``observe()`` oracle.
  - ``dex_aliases()`` lets the connector answer to the canonical
    ``aerodrome_slipstream`` slug (callers / the executor pass this), not only
    its legacy ``dex_name()`` of ``aerodrome`` — for BOTH TWAP and volume.
"""

from __future__ import annotations

import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.services.rate_history_service import (
    RateHistoryServiceServicer,
    _provider_dispatch_keys,
)


@pytest.fixture
def servicer() -> RateHistoryServiceServicer:
    return RateHistoryServiceServicer(GatewaySettings())


def test_aerodrome_registered_as_twap_provider(servicer):
    # Slipstream pools expose observe(); the connector now publishes TWAP.
    assert "aerodrome" in servicer._twap_providers
    # ...and is reachable under the canonical slug callers actually pass.
    assert "aerodrome_slipstream" in servicer._twap_providers
    # Both names route to the same connector instance.
    assert servicer._twap_providers["aerodrome"] is servicer._twap_providers["aerodrome_slipstream"]


def test_aerodrome_twap_supported_on_base(servicer):
    provider = servicer._twap_providers["aerodrome_slipstream"]
    assert "base" in provider.twap_supported_chains()


def test_aerodrome_volume_reachable_under_slipstream_alias(servicer):
    # The dispatch alias applies to every DEX capability, not just TWAP — the
    # existing volume provider now answers to aerodrome_slipstream too.
    assert "aerodrome_slipstream" in servicer._volume_providers
    assert servicer._volume_providers["aerodrome"] is servicer._volume_providers["aerodrome_slipstream"]


def test_sushiswap_v3_remains_twap_provider(servicer):
    # Unchanged here, but confirms the alias refactor didn't drop other dexes.
    assert "sushiswap_v3" in servicer._twap_providers


def test_provider_dispatch_keys_includes_dex_name_and_aliases():
    class _Conn:
        def dex_name(self) -> str:
            return "Aerodrome"

        def dex_aliases(self) -> tuple[str, ...]:
            return ("Aerodrome_Slipstream",)

    # Normalized (lowercased) and de-duplicated, dex_name first.
    assert _provider_dispatch_keys(_Conn()) == ("aerodrome", "aerodrome_slipstream")


def test_provider_dispatch_keys_without_aliases():
    class _Conn:
        def dex_name(self) -> str:
            return "uniswap_v3"

    assert _provider_dispatch_keys(_Conn()) == ("uniswap_v3",)
