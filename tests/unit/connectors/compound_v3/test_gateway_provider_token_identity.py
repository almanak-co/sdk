"""Token-identity coverage for the Compound V3 Comet market resolver.

The gateway lending-rate lane resolves ``asset_symbol`` → Comet address via
the exact-case ``_COMPOUND_V3_TOKEN_TO_MARKET`` map. Post symbol-deprecation,
strategies pass contract addresses — the resolver bridges those to the
canonical symbol (offline) and probes the map case-insensitively, while
symbol-form behaviour stays byte-identical (token-identity PR #3612).
"""

from __future__ import annotations

import pytest

from almanak.connectors.compound_v3.gateway.provider import _compound_v3_resolve_comet_address
from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

# Canonical USDC on Base — resolvable by the offline static registry.
USDC_BASE_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"


def test_symbol_form_resolves_comet_address() -> None:
    from almanak.connectors.compound_v3.adapter import COMPOUND_V3_COMET_ADDRESSES

    assert _compound_v3_resolve_comet_address("base", "USDC") == COMPOUND_V3_COMET_ADDRESSES["base"]["usdc"]


@pytest.mark.parametrize(
    "address_form",
    [
        USDC_BASE_ADDRESS,  # checksummed
        USDC_BASE_ADDRESS.lower(),
        "0X" + USDC_BASE_ADDRESS[2:].upper(),  # instrument-canonicalized casing
    ],
)
def test_address_form_resolves_same_comet_as_symbol(address_form: str) -> None:
    by_symbol = _compound_v3_resolve_comet_address("base", "USDC")
    assert _compound_v3_resolve_comet_address("base", address_form) == by_symbol


def test_unmapped_symbol_still_raises() -> None:
    with pytest.raises(RateHistoryUnavailable):
        _compound_v3_resolve_comet_address("base", "NOPE")


def test_unresolvable_address_still_raises() -> None:
    with pytest.raises(RateHistoryUnavailable):
        _compound_v3_resolve_comet_address("base", "0x" + "d" * 40)


def test_mapped_symbol_on_chain_without_market_raises() -> None:
    # wstETH is in the token→market map but not every chain carries its Comet.
    with pytest.raises(RateHistoryUnavailable):
        _compound_v3_resolve_comet_address("base", "wstETH")
