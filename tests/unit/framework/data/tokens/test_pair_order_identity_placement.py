"""VIB-6471 — unit tests for ``place_token_pair_by_observed_identity``.

Sibling of ``test_pair_order_vib5983.py``, which covers the ADDRESS SORT in the
same module. The two functions answer the same question by opposite means, and
the difference is the whole point of this file:

* :func:`realign_token_pair_by_address` RECOVERS slot order by assuming the venue
  orders its coins by address. True for the V3 family and Solidly; FALSE for
  TraderJoe Liquidity Book (``tokenX``/``tokenY`` are fixed at pool creation) and
  for Curve (``coins(i)`` is pool-index order). Applied there it transposes the
  row — VIB-6383's ``$322,107,799,472.28`` on a ``$2.47`` position.
* :func:`place_token_pair_by_observed_identity` needs no assumption. When the
  receipt OBSERVED which token sits in a slot, that IS the answer: it matches the
  observed addresses against the label symbols' offline-resolved addresses and
  puts each symbol in its own slot. It never sorts, so it is family-agnostic.

``test_placement_beats_the_address_sort_on_a_non_address_sorted_pool`` and its
single-sided sibling are the load-bearing cases: they assert placement's answer
AND that the address sort disagrees with it, which is what proves the fix is
family-agnostic rather than accidentally agreeing with the sort.

Fail-closed contract: every shape whose placement cannot be PROVEN returns
``None``, never a guess. The caller decides the fallback.
"""

from __future__ import annotations

import pytest

from types import SimpleNamespace

from almanak.framework.data.tokens.pair_order import (
    place_token_pair_by_observed_identity,
    realign_token_pair_by_address,
)


def _addr(byte: str) -> str:
    return "0x" + byte * 20


# Ethereum-like WETH/USDC: USDC's address is the LOWER one, so the label order
# (WETH, USDC) is the inverse of the on-chain/address order.
USDC_ADDR = _addr("11")
WETH_ADDR = _addr("cc")

# Avalanche TraderJoe Liquidity Book WAVAX/USDT (the VIB-6383 shape): the pool's
# tokenX is WAVAX even though WAVAX's address is the HIGHER one. Address sorting
# this pair is exactly the transposition that produced the $228bn signature.
WAVAX_ADDR = _addr("cc")
USDT_ADDR = _addr("11")


def _patch_resolver(monkeypatch, book: dict[str, str]):
    """Fake symbol->address resolver that records every ``resolve()`` call's kwargs.

    Same idiom as ``test_pair_order_vib5983.py``: production must resolve OFFLINE
    (``skip_gateway=True``) because this runs on the accounting write path, which
    has no gateway egress. Tests assert that contract so a regression that drops
    the flag fails the suite.
    """
    calls: list[dict] = []

    class _Fake:
        # VIB-6628: accepts kwargs loosely rather than mirroring production's exact
        # acceptance surface. Conformance (does it accept every legal call?) is
        # enforced by test_resolver_double_conformance_vib6100.py; strictness (does
        # it reject illegal ones?) is tracked there. Tightening needs the surface
        # MEASURED first — a double stricter than production is a false-green
        # generator too, as the chain-alias case in #3472 showed.
        def resolve(self, token, chain, *, log_errors=True, skip_gateway=False):  # noqa: ANN001
            # ``chain`` recorded explicitly: it used to arrive via ``**kwargs``,
            # and once the double accepts it positionally (as production does)
            # the splat no longer captures it — leaving the contract assertion
            # below silently reading ``None`` for every call.
            calls.append({"key": token, "chain": chain, "log_errors": log_errors, "skip_gateway": skip_gateway})
            up = str(token).upper()
            if up in book:
                return SimpleNamespace(symbol=up, address=book[up], decimals=18)
            return None

    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.get_token_resolver",
        lambda: _Fake(),
    )
    return calls


def _assert_offline_contract(calls: list[dict], *, chain: str) -> None:
    assert len(calls) == 2, f"expected 2 resolve calls, got {len(calls)}: {calls}"
    for c in calls:
        assert c.get("skip_gateway") is True, f"skip_gateway not True: {c}"
        assert c.get("log_errors") is False, f"log_errors not False: {c}"
        assert c.get("chain") == chain, f"chain mismatch: {c}"


class TestBothCurrenciesObserved:
    def test_places_each_symbol_in_the_slot_its_address_names(self, monkeypatch):
        calls = _patch_resolver(monkeypatch, {"WETH": WETH_ADDR, "USDC": USDC_ADDR})
        placed = place_token_pair_by_observed_identity(
            "WETH", "USDC", "ethereum", WETH_ADDR, USDC_ADDR
        )
        assert placed == ("WETH", "USDC")
        _assert_offline_contract(calls, chain="ethereum")

    def test_inverts_when_the_labels_are_in_the_opposite_order(self, monkeypatch):
        """The label says (WETH, USDC); the receipt says slot 0 held USDC. The
        receipt wins — this is the placement, not a sort, and it must move the
        symbols rather than the amounts."""
        _patch_resolver(monkeypatch, {"WETH": WETH_ADDR, "USDC": USDC_ADDR})
        placed = place_token_pair_by_observed_identity(
            "WETH", "USDC", "ethereum", USDC_ADDR, WETH_ADDR
        )
        assert placed == ("USDC", "WETH")

    def test_observed_addresses_match_case_insensitively(self, monkeypatch):
        _patch_resolver(monkeypatch, {"WETH": WETH_ADDR, "USDC": USDC_ADDR})
        placed = place_token_pair_by_observed_identity(
            "WETH", "USDC", "ethereum", USDC_ADDR.upper(), f"  {WETH_ADDR}  "
        )
        assert placed == ("USDC", "WETH")


class TestSingleCurrencyObserved:
    """The VIB-6471 shape: a single-sided close observes ONE currency. The
    unobserved slot moved no money (the caller's ``identity_is_complete`` guard),
    so the leftover symbol cannot mis-scale anything."""

    def test_only_currency0_observed_takes_slot_zero(self, monkeypatch):
        _patch_resolver(monkeypatch, {"WETH": WETH_ADDR, "USDC": USDC_ADDR})
        assert place_token_pair_by_observed_identity(
            "WETH", "USDC", "ethereum", USDC_ADDR, None
        ) == ("USDC", "WETH")
        assert place_token_pair_by_observed_identity(
            "WETH", "USDC", "ethereum", WETH_ADDR, None
        ) == ("WETH", "USDC")

    def test_only_currency1_observed_takes_slot_one(self, monkeypatch):
        _patch_resolver(monkeypatch, {"WETH": WETH_ADDR, "USDC": USDC_ADDR})
        assert place_token_pair_by_observed_identity(
            "WETH", "USDC", "ethereum", None, WETH_ADDR
        ) == ("USDC", "WETH")
        assert place_token_pair_by_observed_identity(
            "WETH", "USDC", "ethereum", None, USDC_ADDR
        ) == ("WETH", "USDC")

    def test_empty_string_currency_counts_as_unobserved(self, monkeypatch):
        """``""`` is the parser-did-not-emit sentinel, not an address."""
        _patch_resolver(monkeypatch, {"WETH": WETH_ADDR, "USDC": USDC_ADDR})
        assert place_token_pair_by_observed_identity(
            "WETH", "USDC", "ethereum", "", USDC_ADDR
        ) == ("WETH", "USDC")


class TestFailsClosed:
    """Never a guess. Every unprovable shape returns ``None``."""

    def test_no_currency_observed(self, monkeypatch):
        calls = _patch_resolver(monkeypatch, {"WETH": WETH_ADDR, "USDC": USDC_ADDR})
        assert place_token_pair_by_observed_identity("WETH", "USDC", "ethereum", None, None) is None
        assert place_token_pair_by_observed_identity("WETH", "USDC", "ethereum", "", "") is None
        assert calls == [], "no observation must short-circuit BEFORE resolving"

    def test_observed_currency_matching_neither_label(self, monkeypatch):
        """The receipt and the label disagree about which pool this is. Guessing
        here is how a transposed row gets written with confidence."""
        _patch_resolver(monkeypatch, {"WETH": WETH_ADDR, "USDC": USDC_ADDR})
        stranger = _addr("99")
        assert (
            place_token_pair_by_observed_identity("WETH", "USDC", "ethereum", stranger, WETH_ADDR)
            is None
        )
        assert (
            place_token_pair_by_observed_identity("WETH", "USDC", "ethereum", None, stranger)
            is None
        )

    def test_resolver_raises(self, monkeypatch):
        class _Boom:
            def resolve(self, token, chain, *, log_errors=True, skip_gateway=False):
                raise RuntimeError("resolver down")

        monkeypatch.setattr(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            lambda: _Boom(),
        )
        assert (
            place_token_pair_by_observed_identity("WETH", "USDC", "ethereum", WETH_ADDR, USDC_ADDR)
            is None
        )

    def test_resolver_returns_none(self, monkeypatch):
        _patch_resolver(monkeypatch, {})  # neither symbol in the book
        assert (
            place_token_pair_by_observed_identity("WETH", "USDC", "ethereum", WETH_ADDR, USDC_ADDR)
            is None
        )

    def test_resolver_returns_entry_without_address(self, monkeypatch):
        class _NoAddress:
            def resolve(self, token, chain, *, log_errors=True, skip_gateway=False):  # noqa: ANN001, ARG002
                return SimpleNamespace(symbol=str(token).upper(), address=None, decimals=18)

        monkeypatch.setattr(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            lambda: _NoAddress(),
        )
        assert (
            place_token_pair_by_observed_identity("WETH", "USDC", "ethereum", WETH_ADDR, USDC_ADDR)
            is None
        )

    def test_both_labels_resolve_to_one_address(self, monkeypatch):
        """A degenerate book cannot distinguish the slots — unusable, not a coin flip."""
        _patch_resolver(monkeypatch, {"WETH": WETH_ADDR, "WETH9": WETH_ADDR})
        assert (
            place_token_pair_by_observed_identity("WETH", "WETH9", "ethereum", WETH_ADDR, None)
            is None
        )

    def test_both_slots_claim_the_same_symbol(self, monkeypatch):
        _patch_resolver(monkeypatch, {"WETH": WETH_ADDR, "USDC": USDC_ADDR})
        assert (
            place_token_pair_by_observed_identity("WETH", "USDC", "ethereum", WETH_ADDR, WETH_ADDR)
            is None
        )

    def test_empty_inputs(self, monkeypatch):
        calls = _patch_resolver(monkeypatch, {"WETH": WETH_ADDR, "USDC": USDC_ADDR})
        assert place_token_pair_by_observed_identity("", "USDC", "ethereum", USDC_ADDR, None) is None
        assert place_token_pair_by_observed_identity("WETH", "", "ethereum", WETH_ADDR, None) is None
        assert place_token_pair_by_observed_identity("WETH", "USDC", "", WETH_ADDR, None) is None
        assert calls == [], "unusable inputs must short-circuit BEFORE resolving"


class TestPlacementIsFamilyAgnostic:
    """THE LOAD-BEARING CASES.

    A pool whose slot order is the INVERSE of its address order. Placement gets it
    right; the address sort gets it wrong. Asserting only placement's answer would
    not distinguish the two — a placement implemented as a sort would pass. So each
    test also calls :func:`realign_token_pair_by_address` on the same inputs and
    asserts the two DISAGREE.
    """

    def test_placement_beats_the_address_sort_on_a_non_address_sorted_pool(self, monkeypatch):
        _patch_resolver(monkeypatch, {"WAVAX": WAVAX_ADDR, "USDT": USDT_ADDR})

        # Receipt observed: slot 0 (tokenX) is WAVAX, slot 1 (tokenY) is USDT.
        placed = place_token_pair_by_observed_identity(
            "WAVAX", "USDT", "avalanche", WAVAX_ADDR, USDT_ADDR
        )
        assert placed == ("WAVAX", "USDT")

        # The address sort claims the opposite, because WAVAX's address is higher.
        # On a Liquidity Book pool that is simply false, and it is the transposition
        # that produced VIB-6383's $322bn row.
        sorted_pair = realign_token_pair_by_address("WAVAX", "USDT", "avalanche")
        assert sorted_pair == ("USDT", "WAVAX")
        assert placed != sorted_pair, (
            "placement must not be agreeing with the address sort by accident — "
            "if these match, this test proves nothing about family-agnosticism"
        )

    def test_single_sided_lb_close_still_beats_the_address_sort(self, monkeypatch):
        """The exact VIB-6471 residual: a WAVAX-only withdrawal from an LB pool
        stamps ONE currency. Under the old presence gate both realignments
        no-opped and the address sort ran on an LB pair; placement resolves it
        from the single observation without ever sorting."""
        _patch_resolver(monkeypatch, {"WAVAX": WAVAX_ADDR, "USDT": USDT_ADDR})

        placed = place_token_pair_by_observed_identity(
            "WAVAX", "USDT", "avalanche", WAVAX_ADDR, None
        )
        assert placed == ("WAVAX", "USDT")
        assert placed != realign_token_pair_by_address("WAVAX", "USDT", "avalanche")
