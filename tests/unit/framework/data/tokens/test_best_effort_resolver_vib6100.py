"""VIB-6100 — a defect must never be laundered into "token unresolvable".

The pre-fix shape was a broad ``except Exception`` around
``resolver.resolve(...)`` in five accounting/observability helpers. It tolerated
three different things and reported all of them identically:

1. a genuinely unresolvable token (the only intended case),
2. a ``TypeError`` from a caller-or-double signature mismatch,
3. an ``AttributeError`` from a malformed resolver / token-info object.

(2) and (3) are defects. Reporting them as (1) destroyed the evidence — no
traceback, no WARNING — and made a test written against a mismatched double
pass while exercising the fallback branch it believed it was bypassing.

**The fix separates the two without adding a halt path.** An earlier revision of
this seam made (2) and (3) *propagate*; seven rounds of fault injection then
found seven ways an environmental fault reaches the seam wearing those same
exception types, each turning an accounting write into a halt with the trade
already on-chain (VIB-6167). So the seam is now **total** — every failure
returns ``None`` — and the distinction lives in the *evidence* instead: a
defect is ERROR + traceback + a report to the defect observer, a miss is DEBUG.

The observer is what closes the original defect, in CI rather than in
production: ``tests/conftest.py`` installs one that FAILS any test in which the
seam degraded a defect. Tests in this file drive defects deliberately, so they
carry ``@pytest.mark.expects_resolver_defect`` and assert on the report instead.

These tests pin that contract at the shared seam and at every call site that
routes through it.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.data.tokens.best_effort import (
    ResolverDefect,
    resolve_token_best_effort,
    resolve_token_decimals_best_effort,
    set_resolver_defect_observer,
)
from almanak.framework.data.tokens.exceptions import TokenNotFoundError, TokenResolutionError
from tests.support.token_resolver import FakeToken, FakeTokenResolver, SignatureStrictTokenResolver

USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"


@pytest.fixture
def resolver() -> FakeTokenResolver:
    return FakeTokenResolver(
        {
            "USDC": FakeToken(symbol="USDC", address=USDC, decimals=6, chain="arbitrum"),
            "WETH": FakeToken(symbol="WETH", address=WETH, decimals=18, chain="arbitrum"),
        }
    )


@pytest.fixture
def defects():
    """Collect what the seam reports to the defect observer.

    Shadows the autouse collector in ``tests/conftest.py`` for the duration of
    the test, so these tests can assert ON the report rather than being failed
    BY it. They still carry ``@pytest.mark.expects_resolver_defect`` so the
    intent is visible at the test, not only in this fixture.
    """
    collected: list[ResolverDefect] = []
    previous = set_resolver_defect_observer(collected.append)
    try:
        yield collected
    finally:
        set_resolver_defect_observer(previous)


@pytest.fixture
def patched(monkeypatch: pytest.MonkeyPatch):
    """Install a resolver double behind ``get_token_resolver``."""

    def _install(double: object) -> object:
        monkeypatch.setattr(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            lambda *a, **k: double,
        )
        return double

    return _install


# --------------------------------------------------------------------------
# The seam's own contract
# --------------------------------------------------------------------------


def test_resolves_a_known_token(patched, resolver) -> None:
    patched(resolver)
    token = resolve_token_best_effort(USDC, "arbitrum", context="test")
    assert token is not None
    assert token.symbol == "USDC"
    assert resolve_token_decimals_best_effort(USDC, "arbitrum", context="test") == 6


def test_unresolvable_token_is_none_not_an_exception(patched, resolver) -> None:
    patched(resolver)
    assert resolve_token_best_effort("0xdead", "arbitrum", context="test") is None
    assert resolve_token_decimals_best_effort("0xdead", "arbitrum", context="test") is None


def test_every_token_resolution_error_subclass_is_tolerated(patched) -> None:
    """The tolerance is the whole ``TokenResolutionError`` family, not one class."""

    class Raiser:
        def __init__(self, exc: Exception) -> None:
            self._exc = exc

        def resolve(self, token, chain, *, log_errors=True, skip_gateway=False):
            raise self._exc

    for exc in (
        TokenNotFoundError(token="X", chain="arbitrum"),
        TokenResolutionError(token="X", chain="arbitrum", reason="nope"),
    ):
        patched(Raiser(exc))
        assert resolve_token_best_effort("USDC", "arbitrum", context="test") is None


@pytest.mark.expects_resolver_defect
def test_a_signature_mismatch_is_reported_as_a_defect_not_a_miss(patched, resolver, defects, caplog) -> None:
    """THE regression, in its final form.

    The value returned is the SAME as a miss — that is deliberate, and is what
    keeps an environmental fault wearing ``TypeError`` from halting an accounting
    write. What must differ is the evidence: ERROR, a traceback, and a report to
    the observer that fails the test run. A silent ``None`` is the bug.
    """
    import logging

    class KeywordOnlyDouble:
        VIB_6100_NONCONFORMING = (
            "Deliberately mismatched: this double EXISTS to prove the seam reports a\n            signature defect rather than a resolver miss. Exempt from the conformance\n            gate, which is the thing it is a fixture for."
        )

        def resolve(self, token, *, chain=None, **kwargs):  # noqa: ARG002
            raise TypeError("resolve() takes 2 positional arguments but 3 were given")

    patched(KeywordOnlyDouble())
    with caplog.at_level(logging.ERROR):
        assert resolve_token_best_effort("USDC", "arbitrum", context="test") is None

    record = next(r for r in caplog.records if "looks DEFECTIVE" in r.message)
    assert record.levelno == logging.ERROR
    assert record.exc_info is not None, "a defect must carry the traceback"
    assert [d.exc_type for d in defects] == ["TypeError"]
    assert defects[0].context == "test"


@pytest.mark.expects_resolver_defect
def test_a_defect_raised_at_the_call_boundary_is_distinguished_from_one_raised_inside(
    patched, defects
) -> None:
    """``at_call_boundary`` separates "we called it wrong" from "it broke inside".

    This is the diagnosis VIB-6167 identified as the only sound way to tell those
    apart: a signature mismatch is raised AT the call, so the callee is never
    entered and the traceback has one frame. It is used for the log wording and
    the report only — never for control flow — so the caveats (decorated or
    proxied ``resolve``, ``raise ... from``) cost nothing here.
    """

    class MissingSkipGateway:
        # The real resolver accepts skip_gateway; this double does not, so the
        # TypeError is raised by the CALL, not by any code inside resolve().
        VIB_6100_NONCONFORMING = (
            "Deliberately narrower than production: proving the seam DETECTS a "
            "signature mismatch requires a double that has one. Exempt from the "
            "conformance gate in test_resolver_double_conformance_vib6100.py."
        )

        def resolve(self, token, chain, *, log_errors=True):  # noqa: ARG002
            raise AssertionError("never entered")

    class RaisesInside:
        def resolve(self, token, chain, *, log_errors=True, skip_gateway=False):  # noqa: ARG002
            raise TypeError("a fault deep inside the callee")

    patched(MissingSkipGateway())
    assert resolve_token_best_effort("USDC", "arbitrum", context="boundary") is None

    patched(RaisesInside())
    assert resolve_token_best_effort("USDC", "arbitrum", context="inside") is None

    by_context = {d.context: d for d in defects}
    assert by_context["boundary"].at_call_boundary is True, (
        "a signature mismatch must be recognised as a call-boundary defect"
    )
    assert by_context["inside"].at_call_boundary is False, (
        "a fault inside the callee must NOT be reported as a signature mismatch"
    )


@pytest.mark.expects_resolver_defect
def test_a_malformed_resolver_is_reported_as_a_defect_not_a_miss(patched, defects, caplog) -> None:
    import logging

    class Broken:
        def resolve(self, token, chain, *, log_errors=True, skip_gateway=False):
            raise AttributeError("'NoneType' object has no attribute 'startswith'")

    patched(Broken())
    with caplog.at_level(logging.ERROR):
        assert resolve_token_best_effort("USDC", "arbitrum", context="test") is None
    assert [d.exc_type for d in defects] == ["AttributeError"]


@pytest.mark.expects_resolver_defect
def test_the_seam_never_raises_whatever_the_resolver_does(patched) -> None:
    """The totality contract, stated as one test.

    Every caller now omits its own ``try``, so this is load-bearing for all five
    of them: if the seam can raise, an accounting write can halt with the trade
    already on-chain. ``BaseException`` subclasses that are not ``Exception``
    (``KeyboardInterrupt``, ``SystemExit``) are deliberately NOT caught.
    """
    for exc in (
        TypeError("signature"),
        AttributeError("shape"),
        NameError("typo"),
        RuntimeError("operational"),
        ValueError("weird"),
        MemoryError("environment"),
        RecursionError("deep"),
    ):

        class Raiser:
            def resolve(self, token, chain, *, log_errors=True, skip_gateway=False, _e=exc):  # noqa: ARG002
                raise _e

        patched(Raiser())
        assert resolve_token_best_effort("USDC", "arbitrum", context="totality") is None
        assert resolve_token_decimals_best_effort("USDC", "arbitrum", context="totality") is None


def test_chain_is_passed_by_keyword_so_a_keyword_only_double_still_works(patched, resolver) -> None:
    """Fix item 2: the seam pins ``chain=``, removing the positional/keyword split."""
    strict = SignatureStrictTokenResolver()
    strict.add("USDC", FakeToken(symbol="USDC", address=USDC, decimals=6, chain="arbitrum"))
    patched(strict)
    token = resolve_token_best_effort(USDC, "arbitrum", context="test")
    assert token is not None and token.symbol == "USDC"


def test_hot_path_flags_are_pinned_by_the_seam(patched, resolver) -> None:
    """``skip_gateway``/``log_errors`` are contract, not per-caller choice."""
    patched(resolver)
    resolve_token_best_effort(USDC, "arbitrum", context="test")
    (_, chain, kwargs) = resolver.calls[-1]
    assert chain == "arbitrum"
    assert kwargs == {"log_errors": False, "skip_gateway": True}


@pytest.mark.expects_resolver_defect
def test_negative_decimals_are_unmeasured_not_zero(patched) -> None:
    """Empty != Zero: a nonsense decimals value is ``None``, never ``0``.

    ``ResolvedToken`` validates decimals, so this state is defence-in-depth for
    a future producer rather than a shape production can reach today.

    The impossible token is built **locally and explicitly**, not with
    ``FakeToken``: the shared double now enforces production's ``0..77``
    validation, precisely so no other suite can be handed a shape production
    cannot construct. Reaching for a local ``SimpleNamespace`` here is the
    documentation that this state is unreachable — if ``FakeToken`` accepted
    it, that fact would be invisible.
    """

    class _MalformedProducer:
        """Yields a token whose ``decimals`` no real producer could emit."""

        def resolve(self, token, chain, *, log_errors=True, skip_gateway=False) -> SimpleNamespace:
            return SimpleNamespace(symbol="BAD", address="0x" + "ba" * 20, decimals=-1, chain=chain)

    patched(_MalformedProducer())
    assert resolve_token_decimals_best_effort("BAD", "arbitrum", context="test") is None


@pytest.mark.expects_resolver_defect
def test_a_non_string_token_is_loud_but_does_not_fail_the_write(patched, resolver, caplog) -> None:
    """A ``bytes``/``HexBytes`` token is an upstream DATA defect — tier 3, not tier 2.

    ``bytes`` slips every upstream guard (truthy, ``.strip()`` and ``.upper()``
    work, and a BLOB survives a TEXT-affinity SQLite round trip) and would hit
    ``"/" in token`` inside the resolver. Promoting that to a raise would newly
    fail an accounting write that previously degraded, with the trade already
    on-chain — trading one silent-evidence bug for a worse one. Raised by
    adversarial review of PR #3472.
    """
    import logging

    patched(resolver)
    with caplog.at_level(logging.ERROR):
        assert resolve_token_best_effort(b"0xabc", "arbitrum", context="test") is None
    assert any("non-string token" in r.message for r in caplog.records)
    # And it never reached the resolver.
    assert resolver.calls == []


@pytest.mark.expects_resolver_defect
def test_tier_3_keeps_the_evidence(patched, caplog) -> None:
    """The whole point of tier 3 is a traceback, not silence."""
    import logging

    class Flaky:
        def resolve(self, token, chain, *, log_errors=True, skip_gateway=False):
            raise RuntimeError("disk cache unreadable")

    patched(Flaky())
    with caplog.at_level(logging.WARNING):
        assert resolve_token_best_effort("USDC", "arbitrum", context="test") is None
    record = next(r for r in caplog.records if "operationally" in r.message)
    assert record.levelno == logging.WARNING
    assert record.exc_info is not None, "tier 3 must carry the traceback"


def test_tier_1_stays_quiet(patched, resolver, caplog) -> None:
    """An unresolvable token is ordinary — DEBUG, never WARNING/ERROR."""
    import logging

    patched(resolver)
    with caplog.at_level(logging.DEBUG):
        assert resolve_token_best_effort("0xdead", "arbitrum", context="test") is None
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


# --------------------------------------------------------------------------
# Call sites that route through the seam
# --------------------------------------------------------------------------


@pytest.mark.expects_resolver_defect
def test_ledger_lp_close_symbol_falls_back_on_a_miss_and_on_a_defect(patched, resolver, defects) -> None:
    from almanak.framework.observability.ledger import _resolve_lp_close_symbol

    patched(resolver)
    assert _resolve_lp_close_symbol(USDC, "arbitrum") == "USDC"
    assert _resolve_lp_close_symbol("0xdead", "arbitrum") == ""

    class Broken:
        VIB_6100_NONCONFORMING = (
            "Deliberately mismatched: this double EXISTS to prove the seam reports a\n            signature defect rather than a resolver miss. Exempt from the conformance\n            gate, which is the thing it is a fixture for."
        )

        def resolve(self, token, *, chain=None, **kwargs):  # noqa: ARG002
            raise TypeError("signature mismatch")

    patched(Broken())
    assert _resolve_lp_close_symbol(USDC, "arbitrum") == ""
    assert [d.exc_type for d in defects] == ["TypeError"]


@pytest.mark.expects_resolver_defect
def test_ledger_lp_amount_scaling_falls_back_on_a_miss_and_on_a_defect(patched, resolver, defects) -> None:
    from almanak.framework.observability.ledger import _lp_amount_to_human

    patched(resolver)
    assert _lp_amount_to_human(1_500_000, "USDC", "arbitrum") == "1.5"
    assert _lp_amount_to_human(1_500_000, "NOPE", "arbitrum") is None
    # Measured zero never needs decimals — and must stay "0", not None.
    assert _lp_amount_to_human(0, "NOPE", "arbitrum") == "0"

    class Broken:
        VIB_6100_NONCONFORMING = (
            "Deliberately mismatched: this double EXISTS to prove the seam reports a\n            signature defect rather than a resolver miss. Exempt from the conformance\n            gate, which is the thing it is a fixture for."
        )

        def resolve(self, token, *, chain=None, **kwargs):  # noqa: ARG002
            raise TypeError("signature mismatch")

    patched(Broken())
    assert _lp_amount_to_human(1_500_000, "USDC", "arbitrum") is None
    assert [d.exc_type for d in defects] == ["TypeError"]


@pytest.mark.expects_resolver_defect
def test_pair_order_keeps_input_order_on_a_miss_and_on_a_defect(patched, resolver, defects) -> None:
    from almanak.framework.data.tokens.pair_order import realign_token_pair_by_address

    patched(resolver)
    # USDC (0xaf88…) > WETH (0x82af…) numerically, so the pair swaps.
    assert realign_token_pair_by_address("USDC", "WETH", "arbitrum") == ("WETH", "USDC")
    # An unresolvable leg keeps the caller's order — fail-open preserved.
    assert realign_token_pair_by_address("USDC", "NOPE", "arbitrum") == ("USDC", "NOPE")

    class Broken:
        VIB_6100_NONCONFORMING = (
            "Deliberately mismatched: this double EXISTS to prove the seam reports a\n            signature defect rather than a resolver miss. Exempt from the conformance\n            gate, which is the thing it is a fixture for."
        )

        def resolve(self, token, *, chain=None, **kwargs):  # noqa: ARG002
            raise TypeError("signature mismatch")

    patched(Broken())
    assert realign_token_pair_by_address("USDC", "WETH", "arbitrum") == ("USDC", "WETH")
    assert {d.exc_type for d in defects} == {"TypeError"}


@pytest.mark.expects_resolver_defect
def test_lp_report_fee_scaling_drops_the_leg_on_a_miss_and_on_a_defect(patched, resolver, defects) -> None:
    from almanak.framework.accounting.reporting.lp_report import _scale_fee

    patched(resolver)
    assert _scale_fee(1_500_000, "USDC", "arbitrum") == Decimal("1.5")
    assert _scale_fee(1_500_000, "NOPE", "arbitrum") == Decimal("0")

    class Broken:
        VIB_6100_NONCONFORMING = (
            "Deliberately mismatched: this double EXISTS to prove the seam reports a\n            signature defect rather than a resolver miss. Exempt from the conformance\n            gate, which is the thing it is a fixture for."
        )

        def resolve(self, token, *, chain=None, **kwargs):  # noqa: ARG002
            raise TypeError("signature mismatch")

    patched(Broken())
    assert _scale_fee(1_500_000, "USDC", "arbitrum") == Decimal("0")
    assert [d.exc_type for d in defects] == ["TypeError"]


@pytest.mark.expects_resolver_defect
def test_pnl_attributor_scaling_skips_on_a_miss_and_on_a_defect(patched, resolver, defects) -> None:
    from almanak.framework.observability.pnl_attributor import _scale_raw_amount_to_human

    patched(resolver)
    assert _scale_raw_amount_to_human(Decimal(1_500_000), "USDC", "arbitrum") == Decimal("1.5")
    assert _scale_raw_amount_to_human(Decimal(1_500_000), "NOPE", "arbitrum") is None

    class Broken:
        VIB_6100_NONCONFORMING = (
            "Deliberately mismatched: this double EXISTS to prove the seam reports a\n            signature defect rather than a resolver miss. Exempt from the conformance\n            gate, which is the thing it is a fixture for."
        )

        def resolve(self, token, *, chain=None, **kwargs):  # noqa: ARG002
            raise TypeError("signature mismatch")

    patched(Broken())
    assert _scale_raw_amount_to_human(Decimal(1_500_000), "USDC", "arbitrum") is None
    assert [d.exc_type for d in defects] == ["TypeError"]


# --------------------------------------------------------------------------
# The shared double must not be looser than production (VIB-6100 review, #3472)
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("kwargs", "why"),
    [
        ({"symbol": "", "address": USDC, "decimals": 6, "chain": "arbitrum"}, "empty symbol"),
        ({"symbol": "USDC", "address": "", "decimals": 6, "chain": "arbitrum"}, "empty address"),
        ({"symbol": "USDC", "address": USDC, "decimals": -1, "chain": "arbitrum"}, "negative decimals"),
        ({"symbol": "USDC", "address": USDC, "decimals": 78, "chain": "arbitrum"}, "decimals > 77"),
        ({"symbol": "USDC", "address": USDC, "decimals": "6", "chain": "arbitrum"}, "decimals not an int"),
        ({"symbol": "USDC", "address": USDC, "decimals": 6, "chain": ""}, "empty chain"),
    ],
)
def test_the_shared_double_rejects_what_production_rejects(kwargs, why) -> None:
    """``FakeToken`` must refuse every shape ``ResolvedToken`` refuses.

    A double that yields a token production cannot construct is the VIB-6100
    failure mode relocated into the thing meant to prevent it. The concrete
    instance: ``address=""`` used to be allowed, which left
    ``realign_token_pair_by_address`` — and every other address-dependent branch
    — silently inert, so LP handler tests "covered" a path they never entered.
    """
    with pytest.raises(ValueError):
        FakeToken(**kwargs)


def test_the_shared_double_still_builds_a_legitimate_token() -> None:
    """Anti-vacuity control: the validation above must not reject everything."""
    token = FakeToken(symbol="USDC", address=USDC, decimals=6, chain="arbitrum")
    assert (token.symbol, token.address, token.decimals, token.chain) == ("USDC", USDC, 6, "arbitrum")


def test_the_shared_double_rejects_kwargs_the_real_resolver_rejects(resolver) -> None:
    """A call site passing an unknown kwarg must fail here, not only in production."""
    with pytest.raises(TypeError):
        resolver.resolve(USDC, chain="arbitrum", not_a_real_kwarg=True)


def test_the_shared_double_requires_chain(resolver) -> None:
    """Omitting ``chain`` must raise here exactly as it would in production."""
    with pytest.raises(TypeError):
        resolver.resolve(USDC)


# --------------------------------------------------------------------------
# The realignment branch the empty-address double left unexercised
# --------------------------------------------------------------------------


def test_pair_realignment_transposes_when_label_order_is_not_chain_order(patched) -> None:
    """The branch that carries the VIB-5851 / VIB-5983 phantom-basis risk.

    On arbitrum WETH (0x82af…) is the lower address, so a pool labelled
    ``(USDC, WETH)`` must be reported as ``(WETH, USDC)`` to match the chain
    order the receipt parser emitted ``amount0``/``amount1`` in. With the old
    empty-address double this comparison could never happen, so the transposing
    direction had no coverage at all — only the no-op direction did, and only by
    accident.
    """
    from almanak.framework.data.tokens.pair_order import realign_token_pair_by_address

    patched(
        FakeTokenResolver(
            {
                "USDC": FakeToken(symbol="USDC", address=USDC, decimals=6, chain="arbitrum"),
                "WETH": FakeToken(symbol="WETH", address=WETH, decimals=18, chain="arbitrum"),
            }
        )
    )
    assert int(WETH, 16) < int(USDC, 16), "fixture precondition: WETH must be the lower address"
    assert realign_token_pair_by_address("USDC", "WETH", "arbitrum") == ("WETH", "USDC")
    # Already in chain order -> unchanged.
    assert realign_token_pair_by_address("WETH", "USDC", "arbitrum") == ("WETH", "USDC")


def test_pair_realignment_keeps_input_order_when_a_symbol_is_unresolvable(patched) -> None:
    """Tier 1 stays fail-open: an unknown symbol must not transpose anything."""
    from almanak.framework.data.tokens.pair_order import realign_token_pair_by_address

    patched(FakeTokenResolver({"USDC": FakeToken(symbol="USDC", address=USDC, decimals=6, chain="arbitrum")}))
    assert realign_token_pair_by_address("USDC", "NOPE", "arbitrum") == ("USDC", "NOPE")


def test_the_shared_double_rejects_an_unknown_chain() -> None:
    """Production canonicalizes ``chain`` and raises on an unknown one.

    A typo'd chain must not construct here either — otherwise a test can seed a
    token on a chain that does not exist and still go green.
    """
    with pytest.raises(ValueError):
        FakeToken(symbol="USDC", address=USDC, decimals=6, chain="not-a-real-chain")


def test_the_shared_double_will_not_answer_a_different_chain(patched) -> None:
    """A cross-chain match is a false-green generator, not a convenience.

    The double used to answer an ``arbitrum`` request with an ethereum-seeded
    token. That is fatal for exactly the address-ORDER tests this double exists
    to make honest: USDC and WETH sort in OPPOSITE order on those two chains, so
    a cross-chain match silently reintroduces the impossible-pool bug the
    fixtures were just corrected for. Production resolves per chain and cannot
    do this. (VIB-6100 review of #3472.)
    """
    eth_usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    double = FakeTokenResolver({"USDC": FakeToken(symbol="USDC", address=eth_usdc, decimals=6, chain="ethereum")})
    patched(double)

    # Same chain resolves.
    assert resolve_token_best_effort("USDC", "ethereum", context="test") is not None
    # A different chain is a MISS (tier 1), not a silent cross-chain answer.
    assert resolve_token_best_effort("USDC", "arbitrum", context="test") is None


@pytest.mark.parametrize("alias", ["eth", "mainnet", "Ethereum", "ETHEREUM"])
def test_the_shared_double_accepts_the_chain_aliases_production_accepts(patched, alias) -> None:
    """A double STRICTER than production is a false-green generator too.

    The real resolver canonicalizes chain aliases, so it answers ``"eth"`` with
    the ethereum token. The double canonicalized only the token's own chain and
    compared against the raw request, so an alias raised ``TokenNotFoundError``
    -> tier 1 -> a quiet ``None``, and a test using an alias would go green while
    exercising the fallback it believed it bypassed. Same bug as the loose
    double, opposite direction. (VIB-6100 review of #3472.)
    """
    eth_usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    patched(FakeTokenResolver({"USDC": FakeToken(symbol="USDC", address=eth_usdc, decimals=6, chain="ethereum")}))
    assert resolve_token_best_effort("USDC", alias, context="test") is not None


def test_one_symbol_can_be_seeded_on_two_chains_and_each_resolves_to_its_own() -> None:
    """The double is keyed per chain, exactly as production resolves.

    The earlier flat key namespace rejected this outright. That was a real
    limitation dressed as a safety check: ``USDC`` on ethereum and ``USDC`` on
    arbitrum are different tokens that legitimately share a symbol, and the two
    sort in OPPOSITE order against WETH — which is precisely the situation the
    LP fixtures in this PR were corrected for. A double that cannot express it
    forces every such fixture onto one chain, or onto a second resolver.
    """
    eth_usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    arb_usdc = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
    r = FakeTokenResolver({"USDC": FakeToken(symbol="USDC", address=eth_usdc, decimals=6, chain="ethereum")})
    r.add("USDC", FakeToken(symbol="USDC", address=arb_usdc, decimals=6, chain="arbitrum"))

    assert r.resolve("USDC", "ethereum").address == eth_usdc
    assert r.resolve("USDC", "arbitrum").address == arb_usdc


def test_seeding_one_alias_twice_on_the_SAME_chain_is_loud_not_silent() -> None:
    """Two different tokens cannot share an alias on one chain.

    Keeping the last silently would turn the first into an inert miss — green
    while covering nothing, which is the whole VIB-6100 failure shape.
    """
    a = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    b = "0xdac17f958d2ee523a2206206994597c13d831ec7"
    r = FakeTokenResolver({"USDC": FakeToken(symbol="USDC", address=a, decimals=6, chain="ethereum")})
    with pytest.raises(ValueError, match="already seeded on chain"):
        r.add("USDC", FakeToken(symbol="USDT", address=b, decimals=6, chain="ethereum"))


def test_a_cross_chain_miss_names_the_chain_it_was_seeded_on() -> None:
    """The miss must be diagnosable, not just correct.

    A bare "no entry for this token" sends the reader looking for a missing
    fixture that is in fact right there on another chain.
    """
    eth_usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    r = FakeTokenResolver({"USDC": FakeToken(symbol="USDC", address=eth_usdc, decimals=6, chain="ethereum")})
    with pytest.raises(TokenNotFoundError, match="ethereum"):
        r.resolve("USDC", "arbitrum")


# --------------------------------------------------------------------------
# Review findings from Codex + Grok on PR #3472
# --------------------------------------------------------------------------


def test_the_lp_amounts_site_still_resolves_THROUGH_the_gateway(patched, monkeypatch) -> None:
    """The measured-to-unmeasured regression Codex found, pinned.

    ``lp_handler._resolve_lp_amounts`` resolved WITH the gateway before the seam
    existed. Routing it through a seam that pins ``skip_gateway=True`` silently
    turned a measured decimals into ``None`` for any token the gateway can
    discover but the static registry cannot — marking BOTH LP money columns
    unmeasured on a live accounting path.

    This asserts the flag actually reaches the resolver, so the regression cannot
    return by someone "tidying" the ``allow_gateway`` argument away.
    """
    seen: list[bool] = []

    class GatewayOnly:
        def resolve(self, token, chain, *, log_errors=True, skip_gateway=False):  # noqa: ARG002
            seen.append(skip_gateway)
            if skip_gateway:
                raise TokenNotFoundError(token=str(token), chain=str(chain), reason="registry miss")
            return FakeToken(symbol="TKN", address="0x" + "11" * 20, decimals=6, chain=chain)

    patched(GatewayOnly())

    # The seam's DEFAULT must still be gateway-free.
    assert resolve_token_decimals_best_effort("TKN", "arbitrum", context="default") is None
    assert seen[-1] is True

    # The opt-in must reach the resolver and recover the measurement.
    assert resolve_token_decimals_best_effort("TKN", "arbitrum", context="optin", allow_gateway=True) == 6
    assert seen[-1] is False


@pytest.mark.expects_resolver_defect
def test_a_result_without_symbol_is_caught_at_the_seam_not_in_the_caller(patched, defects) -> None:
    """Codex: the seam kept its own no-raise promise but handed callers a landmine.

    ``_v4_realign_token_pair`` reads ``ti0.symbol`` with no guard, on the strength
    of the seam's ``ResolvedToken | None`` contract. A resolver returning an
    object without ``symbol`` therefore raised ``AttributeError`` in the CALLER,
    on an accounting write path.
    """
    from types import SimpleNamespace

    class NoSymbol:
        def resolve(self, token, chain, *, log_errors=True, skip_gateway=False):  # noqa: ARG002
            return SimpleNamespace(address="0x" + "22" * 20, decimals=18, chain=chain)  # no `symbol`

    patched(NoSymbol())
    assert resolve_token_best_effort("TKN", "arbitrum", context="noshape") is None
    assert any("missing symbol" in d.exc_type for d in defects), defects

    # And the caller that dereferences `.symbol` no longer explodes.
    from almanak.framework.accounting.category_handlers.lp_handler import _v4_realign_token_pair

    lp_data = {"currency0": "0x" + "aa" * 20, "currency1": "0x" + "bb" * 20}
    assert _v4_realign_token_pair(lp_data, "arbitrum", "USDC", "WETH") == ("USDC", "WETH", False)


@pytest.mark.expects_resolver_defect
@pytest.mark.parametrize(
    "factory,needle",
    [
        (lambda: None, "returned None"),
        (
            lambda: __import__("types").SimpleNamespace(symbol="USDC", decimals=6, chain="arbitrum"),
            "missing address",
        ),
        (
            lambda: __import__("types").SimpleNamespace(
                symbol="USDC", address="0x" + "11" * 20, chain="arbitrum"
            ),
            "missing decimals",
        ),
        (
            lambda: __import__("types").SimpleNamespace(
                symbol="USDC", address="0x" + "11" * 20, decimals=6
            ),
            "missing chain",
        ),
        (
            lambda: __import__("types").SimpleNamespace(
                symbol="USDC", address="", decimals=6, chain="arbitrum"
            ),
            "empty or non-str address",
        ),
        (
            lambda: __import__("types").SimpleNamespace(
                symbol="", address="0x" + "11" * 20, decimals=6, chain="arbitrum"
            ),
            "empty or non-str symbol",
        ),
        (
            lambda: __import__("types").SimpleNamespace(
                symbol="USDC", address="0x" + "11" * 20, decimals=999, chain="arbitrum"
            ),
            "out-of-contract decimals",
        ),
        (
            lambda: __import__("types").SimpleNamespace(
                symbol="USDC",
                address="0x" + "11" * 20,
                decimals=6,
                chain="not-a-real-chain-xyz",
            ),
            "unknown chain",
        ),
    ],
    ids=[
        "returns-None",
        "missing-address",
        "missing-decimals",
        "missing-chain",
        "empty-address",
        "empty-symbol",
        "bad-decimals",
        "unknown-chain",
    ],
)
def test_incomplete_or_none_success_shapes_are_defects_not_quiet_misses(
    patched, defects, factory, needle
) -> None:
    """Codex (#3694): production resolve never returns None or a partial token.

    A double that returns either shape previously degraded to fallback with an
    empty defect list — the dynamic observer was silent and fallback assertions
    passed. Both must notify the observer.
    """

    class Partial:
        def resolve(self, token, chain, *, log_errors=True, skip_gateway=False):  # noqa: ARG002
            return factory()

    patched(Partial())
    assert resolve_token_best_effort("USDC", "arbitrum", context="partial") is None
    assert any(needle in d.exc_type for d in defects), (needle, [d.exc_type for d in defects])


def test_valid_fake_token_is_not_a_defect(patched, defects) -> None:
    """Control: a production-shaped FakeToken must remain a quiet success."""
    from tests.support.token_resolver import FakeToken, FakeTokenResolver

    resolver = FakeTokenResolver()
    resolver.add(
        "USDC",
        FakeToken(symbol="USDC", address=USDC, decimals=6, chain="arbitrum"),
    )
    patched(resolver)
    token = resolve_token_best_effort("USDC", "arbitrum", context="valid")
    assert token is not None
    assert token.symbol == "USDC"
    assert token.address == USDC
    assert token.decimals == 6
    assert defects == []


@pytest.mark.expects_resolver_defect
def test_an_operational_failure_also_reaches_the_ci_observer(patched, defects) -> None:
    """Codex/Grok: the CI guarantee had a hole for non-defect-shaped exceptions.

    A broken double raising ``RuntimeError`` degraded to ``None`` with
    ``observer_count=0``, so the autouse fixture saw nothing and a fallback
    assertion passed falsely — VIB-6100's failure mode wearing a different
    exception type. The static gate cannot see this: it reads signatures, not
    bodies.
    """

    class Flaky:
        def resolve(self, token, chain, *, log_errors=True, skip_gateway=False):  # noqa: ARG002
            raise RuntimeError("backend down")

    patched(Flaky())
    assert resolve_token_best_effort("USDC", "arbitrum", context="op") is None
    assert [d.exc_type for d in defects] == ["operational: RuntimeError"]


@pytest.mark.expects_resolver_defect
def test_a_construction_failure_also_reaches_the_ci_observer(patched, defects, monkeypatch) -> None:
    """Grok: a miswired resolver factory could false-green a suite silently."""

    def _boom(*_a, **_k):
        raise RuntimeError("factory misconfigured")

    monkeypatch.setattr("almanak.framework.data.tokens.resolver.get_token_resolver", _boom)
    assert resolve_token_best_effort("USDC", "arbitrum", context="ctor") is None
    assert [d.exc_type for d in defects] == ["resolver construction failed"]


@pytest.mark.expects_resolver_defect
def test_identity_placement_fails_closed_on_a_defect_and_reports_it(patched, resolver, defects) -> None:
    """Grok: the migrated high-blast sites lacked defect->fallback pins."""
    from almanak.framework.data.tokens.pair_order import place_token_pair_by_observed_identity

    patched(resolver)
    ok = place_token_pair_by_observed_identity("USDC", "WETH", "arbitrum", USDC, WETH)
    assert ok == ("USDC", "WETH")

    class Broken:
        VIB_6100_NONCONFORMING = "Deliberately mismatched: drives the defect path under test."

        def resolve(self, token, *, chain=None, **kwargs):  # noqa: ARG002
            raise TypeError("signature mismatch")

    patched(Broken())
    # Fails CLOSED (None), never a guess — and the defect is no longer silent.
    assert place_token_pair_by_observed_identity("USDC", "WETH", "arbitrum", USDC, WETH) is None
    assert {d.exc_type for d in defects} == {"TypeError"}
