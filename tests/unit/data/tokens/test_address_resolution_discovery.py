"""An address that is unknown now must become nameable once the resolver discovers it on-chain."""

from types import SimpleNamespace

import pytest

import almanak.framework.data.tokens as tokens
from almanak.framework.data.tokens.address_resolution import _resolve_known, resolve_token_symbol

ADDRESS = "0x" + "12" * 20


@pytest.fixture
def resolver(monkeypatch):
    state = {"known": False, "calls": 0}

    class Resolver:
        def resolve(self, lookup, chain, *, log_errors=True, skip_gateway=False):
            state["calls"] += 1
            if not state["known"]:
                raise ValueError("Address not found in registry")
            return SimpleNamespace(symbol="cme")

    monkeypatch.setattr(tokens, "get_token_resolver", lambda *args, **kwargs: Resolver())
    _resolve_known.cache_clear()
    yield state
    _resolve_known.cache_clear()


def test_a_miss_before_discovery_does_not_hide_the_token_afterwards(resolver):
    assert resolve_token_symbol(ADDRESS, "robinhood") is None
    resolver["known"] = True
    assert resolve_token_symbol(ADDRESS, "robinhood") == "CME"


def test_a_resolved_symbol_is_still_memoized(resolver):
    resolver["known"] = True
    assert resolve_token_symbol(ADDRESS, "robinhood") == "CME"
    assert resolve_token_symbol(ADDRESS.upper().replace("0X", "0x"), "robinhood") == "CME"
    assert resolver["calls"] == 1
