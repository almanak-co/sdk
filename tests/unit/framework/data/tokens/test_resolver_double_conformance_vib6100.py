"""Every token-resolver double must accept the call production makes (VIB-6100).

This is the *static* half of the VIB-6100 guarantee: every resolver double
must accept the calls production makes. A follow-up production PR (VIB-6167)
adds the *dynamic* half — an autouse fixture that fails a test when the
best-effort seam degrades a defect at run time.

The two catch different things and neither subsumes the other:

* the dynamic fixture only fires on a code path a test actually reaches, so a
  double that is broken but never called stays invisible until someone adds a
  test;
* this check reads every double in the tree whether or not it is exercised, but
  it can only see the *signature* — it cannot know what the body returns.

The signature is where the original defect lived. ``observability/ledger.py``
called ``resolve(cur, chain, ...)`` positionally while ``data/tokens/pair_order.py``
called ``resolve(token0, chain=chain, ...)`` by keyword, so a double written with
``chain`` keyword-only worked at one site and raised ``TypeError`` at the other —
and the fail-open reported that ``TypeError`` as an ordinary resolver miss.

The contract enforced here is **substitutability**, not "accepts the call my own
caller happens to make today". A double stands in for ``TokenResolver``, so it
must accept every call the real one accepts::

    resolve(token, chain, *, log_errors=True, skip_gateway=False)

Scoping it to the current caller instead would be exactly the mistake that
produced VIB-6100: each double was individually fine against the one site its
author had in mind, and the trap sprang when a *second* site called the same
seam differently. A double narrower than production is a latent false green
waiting for the next caller — including the seam, which pins ``skip_gateway``.

Scope: classes defined under ``tests/`` that declare a ``resolve`` method, in a
module that either references ``get_token_resolver`` (i.e. installs a double) or
lives under ``tests/support/`` (i.e. IS shared double infrastructure). That
pairing is what makes this specific rather than a blanket rule about every method
named ``resolve`` in the repo — and the second clause is why the gate does not
exempt its own reference double, which it did until CodeRabbit caught it.

Parsed with ``ast``, never imported. Importing every test module to introspect it
would execute module-level fixtures and collection side effects, and would make
this check fail for reasons that have nothing to do with what it measures.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest

TESTS_ROOT = Path(__file__).resolve().parents[5] / "tests"

#: Shared double infrastructure. Modules here ARE doubles, so they qualify for
#: the scan even though they never reference ``get_token_resolver`` themselves.
SUPPORT_DIR = "support"

#: The calls a stand-in for ``TokenResolver`` must accept. BOTH shapes, because
#: substitutability is the contract — see the module docstring.
#:
#: Checking only the keyword form would be a gate that cannot catch its own
#: motivating defect. The seam pins ``chain=`` by keyword (fix item 2), so the
#: historical VIB-6100 double — ``resolve(self, token, *, chain, ...)`` — passes
#: the keyword call perfectly well. What made it a trap was the OTHER caller,
#: ``ledger.py``, passing ``chain`` positionally. A double that accepts one shape
#: and not the other is exactly the bug, and only the positional probe sees it.
#:
#: ``test_the_real_resolver_accepts_both_call_shapes`` pins these against
#: production so the check cannot enforce a stale contract.
CANONICAL_CALLS: tuple[tuple[tuple[object, ...], dict[str, object]], ...] = (
    # positional chain — observability/ledger.py's historical shape
    (("USDC", "ethereum"), {"log_errors": False, "skip_gateway": True}),
    # keyword chain — data/tokens/pair_order.py's shape, and what the seam pins
    (("USDC",), {"chain": "ethereum", "log_errors": False, "skip_gateway": True}),
)


def _signature_from_ast(node: ast.FunctionDef | ast.AsyncFunctionDef) -> inspect.Signature:
    """Build an ``inspect.Signature`` from a parsed ``def``, dropping ``self``.

    Defaults are represented by a sentinel: this check is about *arity and
    naming*, and evaluating a default expression would mean executing test-module
    code, which is exactly what parsing instead of importing avoids.
    """
    a = node.args
    params: list[inspect.Parameter] = []
    sentinel = object()

    posonly = list(a.posonlyargs)
    normal = list(a.args)
    # Drop the bound ``self``/``cls`` of the method definition.
    if posonly:
        posonly = posonly[1:]
    elif normal:
        normal = normal[1:]

    n_defaults = len(a.defaults)
    positional = posonly + normal
    first_defaulted = len(positional) - n_defaults

    for i, arg in enumerate(posonly):
        params.append(
            inspect.Parameter(
                arg.arg,
                inspect.Parameter.POSITIONAL_ONLY,
                default=sentinel if i >= first_defaulted else inspect.Parameter.empty,
            )
        )
    for j, arg in enumerate(normal):
        i = len(posonly) + j
        params.append(
            inspect.Parameter(
                arg.arg,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=sentinel if i >= first_defaulted else inspect.Parameter.empty,
            )
        )
    if a.vararg:
        params.append(inspect.Parameter(a.vararg.arg, inspect.Parameter.VAR_POSITIONAL))
    for arg, default in zip(a.kwonlyargs, a.kw_defaults, strict=True):
        params.append(
            inspect.Parameter(
                arg.arg,
                inspect.Parameter.KEYWORD_ONLY,
                default=inspect.Parameter.empty if default is None else sentinel,
            )
        )
    if a.kwarg:
        params.append(inspect.Parameter(a.kwarg.arg, inspect.Parameter.VAR_KEYWORD))
    return inspect.Signature(params)


#: A class may opt out by declaring ``VIB_6100_NONCONFORMING = "<reason>"`` in its
#: body. The only legitimate use is a double whose non-conformance IS the thing
#: under test — the seam's own suite needs one to prove the seam reports a
#: signature mismatch at all. It requires a written reason precisely so it cannot
#: become a quiet allowlist: a bare marker is not accepted (see
#: ``test_optouts_state_a_reason``).
OPT_OUT_ATTR = "VIB_6100_NONCONFORMING"


def _module_qualifies(path: Path, source: str) -> bool:
    """Does this module hold resolver doubles this gate should read?

    ONE predicate, used by every scan below. It was previously inlined in two
    places, and fixing only one of them left ``_iter_opt_outs`` still skipping
    ``tests/support/`` after ``_iter_resolver_doubles`` had been corrected — the
    classic "fixed one entrypoint of a two-entrypoint rule". Keep it single.

    A module qualifies if it INSTALLS a double (references ``get_token_resolver``)
    or if it IS shared double infrastructure (lives under ``tests/support/``).
    The second clause matters: ``tests/support/token_resolver.py`` never mentions
    ``get_token_resolver`` — callers monkeypatch it — so without it the gate
    silently exempts the very double it tells everyone to adopt.
    """
    return "get_token_resolver" in source or SUPPORT_DIR in path.parts


def _opt_out_reason(node: ast.ClassDef) -> str | None:
    """The declared reason this class is exempt, or ``None`` if it is not."""
    for item in node.body:
        targets = (
            item.targets
            if isinstance(item, ast.Assign)
            else [item.target]
            if isinstance(item, ast.AnnAssign)
            else []
        )
        if any(isinstance(t, ast.Name) and t.id == OPT_OUT_ATTR for t in targets):
            value = item.value
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                return value.value
            return ""  # declared but not a string reason — reported as invalid
    return None


def _iter_resolver_doubles(*, include_opted_out: bool = False) -> list[tuple[Path, str, inspect.Signature]]:
    """Every ``resolve`` method of a class in a module that installs a resolver."""
    found: list[tuple[Path, str, inspect.Signature]] = []
    for path in sorted(TESTS_ROOT.rglob("*.py")):
        try:
            source = path.read_text(encoding="utf-8")
        except OSError:  # pragma: no cover - unreadable file is not this test's business
            continue
        if not _module_qualifies(path, source):
            continue
        try:
            tree = ast.parse(source, filename=str(path))
        except SyntaxError:  # pragma: no cover - a broken test file fails elsewhere, loudly
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            if not include_opted_out and _opt_out_reason(node) is not None:
                continue
            for item in node.body:
                if isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef) and item.name == "resolve":
                    found.append((path, f"{node.name}.resolve", _signature_from_ast(item)))
    return found


def _iter_opt_outs() -> list[tuple[Path, str, str]]:
    """Every class declaring the opt-out, with its stated reason."""
    out: list[tuple[Path, str, str]] = []
    for path in sorted(TESTS_ROOT.rglob("*.py")):
        try:
            source = path.read_text(encoding="utf-8")
        except OSError:  # pragma: no cover
            continue
        if OPT_OUT_ATTR not in source or not _module_qualifies(path, source):
            continue
        try:
            tree = ast.parse(source, filename=str(path))
        except SyntaxError:  # pragma: no cover
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                reason = _opt_out_reason(node)
                if reason is not None:
                    out.append((path, node.name, reason))
    return out


def test_optouts_state_a_reason() -> None:
    """An opt-out without a written reason is an allowlist, which is not allowed.

    AGENTS.md is explicit that the response to a quality gate is to fix the code,
    not to widen the gate. The escape hatch exists only for a double whose
    non-conformance is the property under test, and that is always a sentence
    someone can write down.
    """
    for path, name, reason in _iter_opt_outs():
        assert reason.strip(), (
            f"{path.relative_to(TESTS_ROOT)}::{name} declares {OPT_OUT_ATTR} without a "
            "string reason. State why this double must be narrower than production, or "
            "fix the double."
        )


def test_the_scan_actually_finds_doubles() -> None:
    """Anti-vacuity control.

    Without this, a broken glob, a renamed directory, or a bad ``ast`` walk would
    make every assertion below hold over an empty set — the shape of vacuous pass
    this whole PR is about. If the tree genuinely stops hand-rolling doubles this
    will need updating, which is the correct time to think about it.
    """
    doubles = _iter_resolver_doubles()
    assert len(doubles) >= 5, (
        f"expected the tree to contain several resolver doubles, found {len(doubles)}. "
        f"Scanned {TESTS_ROOT}; if that path is wrong this check has been silently inert."
    )


def test_the_shared_double_itself_is_covered_by_this_gate() -> None:
    """The reference implementation must not be exempt from its own rule.

    Asserted by PATH, not by class name. An earlier check for the *name*
    ``FakeTokenResolver`` in the scan results passed while the canonical module
    was being skipped entirely — it was matching ``_FakeTokenResolver`` in an
    unrelated file. A gate that exempts its own reference double, and reports
    itself covered while doing so, is the VIB-6100 false-green class reproduced
    inside the fix for it.
    """
    shared = (TESTS_ROOT / "support" / "token_resolver.py").resolve()

    covered = {q for p, q, _ in _iter_resolver_doubles() if p.resolve() == shared}
    assert any("FakeTokenResolver" in n for n in covered), (
        f"FakeTokenResolver in {shared} is not scanned by this gate. It is the double "
        "every other suite is told to adopt; exempting it defeats the check."
    )

    # ``SignatureStrictTokenResolver`` is deliberately keyword-only — it is the
    # probe that proves a call site passes ``chain=``, so it must REJECT the
    # positional form production accepts. It therefore belongs in the opt-out
    # list, not the covered list, and must carry a written reason like any other.
    opted = {n for p, n, _ in _iter_opt_outs() if p.resolve() == shared}
    assert "SignatureStrictTokenResolver" in opted, (
        "SignatureStrictTokenResolver must declare the documented opt-out with a "
        f"reason; found opt-outs in that module: {opted or 'none'}"
    )


def test_the_real_resolver_accepts_both_call_shapes() -> None:
    """Pin ``CANONICAL_CALLS`` against production, so this check cannot go stale.

    Every assertion below measures doubles against ``CANONICAL_CALLS``. If the
    real resolver's signature changed and that constant did not, the whole check
    would keep passing while measuring the wrong contract — enforcing yesterday's
    shape on today's doubles. This is also what proves the positional form is a
    real call the production resolver accepts, rather than one this file invented.
    """
    from almanak.framework.data.tokens.resolver import TokenResolver

    signature = inspect.signature(TokenResolver.resolve)
    for args, kwargs in CANONICAL_CALLS:
        signature.bind(object(), *args, **kwargs)  # object() is self


@pytest.mark.parametrize(
    ("source", "why"),
    [
        (
            "class D:\n    def resolve(self, token, *, chain, log_errors=True, skip_gateway=False): ...\n",
            "THE VIB-6100 double: `chain` keyword-only. Worked at pair_order.py, "
            "raised TypeError at ledger.py's positional call.",
        ),
        (
            "class D:\n    def resolve(self, token, chain, log_errors=True): ...\n",
            "no `skip_gateway` — the flag the seam pins on the accounting hot path.",
        ),
        (
            "class D:\n    def resolve(self, symbol): ...\n",
            "no `chain` at all.",
        ),
    ],
)
def test_the_gate_rejects_the_shapes_it_exists_to_catch(source: str, why: str) -> None:
    """Liveness. A gate that cannot fail its own motivating defect proves nothing.

    Every other assertion in this file is of the form "nothing is broken", which
    holds just as well when the machinery is inert — a bad glob, a broken AST
    walk, a ``bind`` that never raises. These cases pin that the check actually
    discriminates, using the real historical shape from the ticket.
    """
    tree = ast.parse(source)
    fn = tree.body[0].body[0]  # type: ignore[attr-defined]
    signature = _signature_from_ast(fn)
    failures = 0
    for args, kwargs in CANONICAL_CALLS:
        try:
            signature.bind(*args, **kwargs)
        except TypeError:
            failures += 1
    assert failures > 0, f"the gate failed to reject a shape it must catch: {why}"


def test_the_gate_accepts_a_conforming_double() -> None:
    """The other half of the control: it must not reject everything."""
    tree = ast.parse(
        "class D:\n    def resolve(self, token, chain, *, log_errors=True, skip_gateway=False): ...\n"
    )
    fn = tree.body[0].body[0]  # type: ignore[attr-defined]
    signature = _signature_from_ast(fn)
    for args, kwargs in CANONICAL_CALLS:
        signature.bind(*args, **kwargs)


@pytest.mark.parametrize(
    ("path", "qualname", "signature"),
    [pytest.param(p, q, s, id=f"{p.relative_to(TESTS_ROOT)}::{q}") for p, q, s in _iter_resolver_doubles()],
)
def test_double_accepts_the_production_call(path: Path, qualname: str, signature: inspect.Signature) -> None:
    """A double that cannot take the seam's call is a false-green generator.

    Through the seam, the ``TypeError`` such a double raises is degraded to
    ``None`` — indistinguishable from "this token does not resolve". Every
    assertion in the owning test then runs against the fallback branch while
    claiming to test the resolved one.
    """
    for args, kwargs in CANONICAL_CALLS:
        try:
            signature.bind(*args, **kwargs)
        except TypeError as exc:
            shape = "chain POSITIONAL" if len(args) == 2 else "chain by KEYWORD"
            pytest.fail(
                f"{path.relative_to(TESTS_ROOT)}::{qualname} cannot accept a call the real "
                f"TokenResolver accepts ({shape}).\n"
                f"  production: resolve(token, chain, *, log_errors=True, skip_gateway=False)\n"
                f"  double is:  resolve{signature}\n"
                f"  bind failed: {exc}\n\n"
                "Through the seam this raises TypeError, which is degraded to None and reads "
                "as an ordinary resolver miss — so this suite's assertions may be passing "
                "against the fallback branch. Prefer "
                "tests.support.token_resolver.FakeTokenResolver, which mirrors production "
                "and cannot mismatch."
            )


@pytest.mark.parametrize(
    ("path", "qualname", "signature"),
    [pytest.param(p, q, s, id=f"{p.relative_to(TESTS_ROOT)}::{q}") for p, q, s in _iter_resolver_doubles()],
)
def test_double_requires_chain(path: Path, qualname: str, signature: inspect.Signature) -> None:
    """``chain`` must be required, exactly as production requires it.

    A double that defaults ``chain`` is looser than production, which is the same
    bug wearing a different hat: a call site that forgets ``chain`` is green here
    and ``TypeError`` in production. Found by adversarial review of PR #3472,
    inside the very double written to prevent this class.

    ``**kwargs`` does NOT exempt a double: ``def resolve(self, token, **kwargs)``
    still accepts ``resolve("USDC")`` with no chain (CodeRabbit / #3693). Production
    rejects unknown keywords too, so a VAR_KEYWORD double is also non-conformant
    unless opted out via ``VIB_6100_NONCONFORMING``.
    """
    assert not any(p.kind is inspect.Parameter.VAR_KEYWORD for p in signature.parameters.values()), (
        f"{path.relative_to(TESTS_ROOT)}::{qualname} accepts arbitrary keyword arguments "
        "(`**kwargs`). Production TokenResolver.resolve rejects unknown keywords; a double "
        "that swallows them is looser than production. Declare only `log_errors` and "
        "`skip_gateway`, or opt out with VIB_6100_NONCONFORMING if non-conformance is the "
        "property under test."
    )
    chain = signature.parameters.get("chain")
    assert chain is not None, (
        f"{path.relative_to(TESTS_ROOT)}::{qualname} does not declare an explicit `chain` "
        "parameter. Hiding it only in `**kwargs` (or omitting it) is not production-shaped."
    )
    assert chain.default is inspect.Parameter.empty, (
        f"{path.relative_to(TESTS_ROOT)}::{qualname} defaults `chain`, but production requires it. "
        "A double looser than production lets a call site that omits `chain` pass in tests and "
        "fail in production."
    )
