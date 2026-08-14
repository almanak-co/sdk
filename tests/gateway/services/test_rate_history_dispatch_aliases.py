"""Dispatch aliases + Aerodrome Slipstream historical-data registration.

Covers the gateway-side fixes from this session:
  - Aerodrome Slipstream declares ``GatewayDexTwapCapability`` — the
    Slipstream pool exposes the Uniswap-V3 ``observe()`` oracle.
  - Classic and Slipstream have distinct historical-data dispatch keys, while
    the classic connector retains its volume alias for compatibility.
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
    # Classic Solidly pools do not expose observe(); only Slipstream publishes TWAP.
    assert "aerodrome" not in servicer._twap_providers
    assert "aerodrome_slipstream" in servicer._twap_providers


def test_aerodrome_twap_supported_on_base(servicer):
    provider = servicer._twap_providers["aerodrome_slipstream"]
    assert "base" in provider.twap_supported_chains()


def test_aerodrome_volume_reachable_under_slipstream_alias(servicer):
    # The dispatch alias applies to every DEX capability, not just TWAP — the
    # existing volume provider now answers to aerodrome_slipstream too.
    assert "aerodrome_slipstream" in servicer._volume_providers
    assert servicer._volume_providers["aerodrome"] is servicer._volume_providers["aerodrome_slipstream"]


def test_v3_forks_register_both_historical_facets(servicer):
    for protocol, chain in (
        ("sushiswap_v3", "ethereum"),
        ("pancakeswap_v3", "bsc"),
        ("agni_finance", "mantle"),
        ("uniswap_v3", "bsc"),
        ("aerodrome_slipstream", "base"),
    ):
        assert chain in servicer._pool_state_providers[protocol].pool_state_supported_chains()
        assert chain in servicer._twap_providers[protocol].twap_supported_chains()


def test_pancakeswap_v3_registered_for_bsc_pool_state_and_twap(servicer):
    assert "pancakeswap_v3" in servicer._pool_state_providers
    assert "bsc" in servicer._pool_state_providers["pancakeswap_v3"].pool_state_supported_chains()
    assert "pancakeswap_v3" in servicer._twap_providers
    assert "bsc" in servicer._twap_providers["pancakeswap_v3"].twap_supported_chains()


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
