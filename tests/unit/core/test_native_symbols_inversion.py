"""Equivalence harness for the VIB-4851 A1 native-token-symbol inversion.

Three per-chain native-symbol matrices were folded onto ``ChainDescriptor.native``
and now derive from the registry via ``native_symbols_for`` /
``native_token_for_chain``:

* ``framework/intents/compiler.py::_CHAIN_NATIVE_SYMBOLS``
* ``gateway/services/market_service.py::NATIVE_SYMBOLS_BY_CHAIN``
* ``framework/data/balance/gateway_provider.py`` (the inline single-symbol map)

This test freezes each OLD map verbatim and asserts the registry-derived lookup
reproduces it — proving the *data* is preserved, not the design. It is the
reusable Class-A/B equivalence harness the chain-string inversion campaign relies
on (``docs/internal/plans/chain-string-inversion-campaign.md``).

Two of the maps were not faithful: ``NATIVE_SYMBOLS_BY_CHAIN`` carried three dead
*unregistered* chains (``scroll``/``zksync``/``fantom``, unreachable past
``validate_chain``) and a typo'd ``"x-layer"`` key (canonical is ``"xlayer"``),
and was missing ``zerog``. The market-service test reconciles those explicitly and
by name so the delta is documented, not silently filtered.
"""

from __future__ import annotations

from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import native_symbols_for
from almanak.core.enums import Chain
from almanak.framework.accounting.gas_pricing import native_token_for_chain

# --- the three OLD maps, frozen verbatim from origin/main (pre-A1) --------------

FROZEN_COMPILER_MAP: dict[str, frozenset[str]] = {
    "ethereum": frozenset({"ETH"}),
    "arbitrum": frozenset({"ETH"}),
    "optimism": frozenset({"ETH"}),
    "base": frozenset({"ETH"}),
    "blast": frozenset({"ETH"}),
    "linea": frozenset({"ETH"}),
    "polygon": frozenset({"MATIC", "POL"}),
    "avalanche": frozenset({"AVAX"}),
    "bsc": frozenset({"BNB"}),
    "sonic": frozenset({"S"}),
    "plasma": frozenset({"XPL"}),
    "mantle": frozenset({"MNT"}),
    "berachain": frozenset({"BERA"}),
    "monad": frozenset({"MON"}),
    "xlayer": frozenset({"OKB"}),
    "zerog": frozenset({"A0GI"}),
    "hyperevm": frozenset({"HYPE"}),
    "solana": frozenset({"SOL"}),
}

FROZEN_MARKET_MAP: dict[str, frozenset[str]] = {
    "ethereum": frozenset({"ETH"}),
    "arbitrum": frozenset({"ETH"}),
    "optimism": frozenset({"ETH"}),
    "base": frozenset({"ETH"}),
    "linea": frozenset({"ETH"}),
    "blast": frozenset({"ETH"}),
    "scroll": frozenset({"ETH"}),  # dead — no descriptor, unreachable past validate_chain
    "zksync": frozenset({"ETH"}),  # dead
    "polygon": frozenset({"MATIC", "POL"}),
    "avalanche": frozenset({"AVAX"}),
    "bsc": frozenset({"BNB"}),
    "sonic": frozenset({"S"}),
    "fantom": frozenset({"FTM"}),  # dead
    "mantle": frozenset({"MNT"}),
    "berachain": frozenset({"BERA"}),
    "monad": frozenset({"MON"}),
    "plasma": frozenset({"XPL"}),
    "x-layer": frozenset({"OKB"}),  # typo — canonical chain name is "xlayer"
    "solana": frozenset({"SOL"}),
}

FROZEN_GATEWAY_PROVIDER_MAP: dict[str, str] = {
    "ethereum": "ETH",
    "arbitrum": "ETH",
    "optimism": "ETH",
    "base": "ETH",
    "avalanche": "AVAX",
    "polygon": "MATIC",
}

_DEAD_UNREGISTERED = frozenset({"scroll", "zksync", "fantom"})


def _derived() -> dict[str, frozenset[str]]:
    return {d.name: native_symbols_for(d.name) for d in ChainRegistry.all()}


# --- compiler map: exact parity (all 18 keys are registered) --------------------


def test_compiler_map_matches_registry_exactly() -> None:
    # Catches polygon's multi-symbol case: without accepted_symbols=("POL",) the
    # derived polygon value would be {"MATIC"} != frozen {"MATIC","POL"}.
    assert _derived() == FROZEN_COMPILER_MAP


# --- gateway_provider single-symbol map: parity on its 6 keys -------------------


def test_gateway_provider_map_matches_registry() -> None:
    for chain, symbol in FROZEN_GATEWAY_PROVIDER_MAP.items():
        assert native_token_for_chain(chain) == symbol
    # Regression pins for chains the legacy 6-entry map omitted (it fell back to
    # "ETH" for these, requesting the wrong native balance). Catches any
    # reintroduction of that ETH fallback (VIB-4851 A1).
    assert native_token_for_chain("bsc") == "BNB"
    assert native_token_for_chain("mantle") == "MNT"
    assert native_token_for_chain("xlayer") == "OKB"
    assert native_token_for_chain("zerog") == "A0GI"


# --- market_service map: parity after documented reconciliation -----------------


def test_market_service_map_matches_registry_after_reconciliation() -> None:
    expected = {k: v for k, v in FROZEN_MARKET_MAP.items() if k not in _DEAD_UNREGISTERED}
    expected["xlayer"] = expected.pop("x-layer")  # typo -> canonical

    derived = _derived()
    # Every reconciled legacy entry is preserved byte-for-byte.
    for chain, symbols in expected.items():
        assert derived[chain] == symbols, f"{chain}: {derived.get(chain)} != {symbols}"
    # The derive additionally covers a registered chain the legacy map missed
    # (zerog) — a strict improvement, not a regression. Pin it so the delta is
    # explicit rather than a mystery diff.
    assert set(derived) - set(expected) == {"zerog", "hyperevm"}


def test_market_reconciliation_deltas_are_real() -> None:
    # Defends the reconciliation in the test above: the dropped keys are genuinely
    # unregistered (so unreachable past validate_chain) and the canonical exists.
    for dead in (*_DEAD_UNREGISTERED, "x-layer"):
        assert ChainRegistry.try_resolve(dead) is None, f"{dead!r} is unexpectedly registered"
    assert ChainRegistry.try_resolve("xlayer") is not None


# --- the polygon MATIC/POL bridge (the one data change A1 introduces) -----------


def test_polygon_accepted_symbols_bridge() -> None:
    # Balance-routing accepts both symbols...
    assert native_symbols_for("polygon") == frozenset({"MATIC", "POL"})
    # ...while the canonical gas/price/funding symbol stays MATIC.
    descriptor = ChainRegistry.get(Chain.POLYGON)
    assert descriptor.native.symbol == "MATIC"
    assert descriptor.native.accepted_symbols == ("POL",)


def test_unknown_chain_fails_closed() -> None:
    # The VIB-3137 contract: an unregistered chain yields an empty set so callers
    # fall through to the ERC-20 path instead of mis-routing to native.
    assert native_symbols_for("definitely-not-a-chain") == frozenset()
    assert native_symbols_for("") == frozenset()
