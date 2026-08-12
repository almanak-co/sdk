"""Best-effort token resolution for accounting / observability write paths.

Why this exists (VIB-6100)
==========================

Several accounting and observability helpers need a token's decimals or symbol
*without* being allowed to fail the write. Historically each of them hand-rolled
the same shape::

    try:
        resolved = get_token_resolver().resolve(tok, chain, log_errors=False, skip_gateway=True)
    except Exception:            # noqa: BLE001 — fail-open
        logger.debug(...)
        return None              # "this token did not resolve"

The *intent* — "a token we cannot resolve must not raise on the accounting write
path" — is correct. The *implementation* is far wider than the intent. ``except
Exception`` also swallows:

* a ``TypeError`` because the callee's signature does not match the call
  (including a test double that only accepts ``chain`` as a keyword while the
  caller passes it positionally — the observed VIB-6100 instance),
* an ``AttributeError`` from a malformed resolver or token-info object,
* anything else that is a **defect** rather than an unresolvable token,

and reports every one of them as the perfectly ordinary business outcome
"unresolved". That is a fail-open that destroys its own evidence: no traceback,
no WARNING, and — worse — a unit test written against a mismatched double
**passes while exercising the fallback branch it believes it is bypassing**.
Two tests in PR #3451 did exactly that.

The contract here: LOUD, but never fatal
========================================

**This seam is total. Every failure returns ``None``; it never raises
``Exception``.** Callers therefore need no ``try`` of their own, and the broad
``except Exception`` this replaces is deleted at each site rather than moved.

That totality is a deliberate reversal, and the reasoning is worth keeping
(VIB-6167). An earlier design of this seam added a middle tier that
**propagated** ``TypeError`` / ``AttributeError`` / ``NameError`` on the theory
that those types mean "*we* called it wrong". Seven rounds of fault injection
found seven ways an **environmental** fault reaches this seam wearing one of
those types — a type-drifted disk-cache row, a structurally corrupt cache file,
a failing metrics backend, CPython's process-global ``warnings.showwarning``
hook (replaceable by any third party in the process), a lazy import one frame
above every tier, the seam's own ``logger.error`` call, and the same construct
one frame lower. Each one turned an accounting write into a halt **with the
trade already on-chain**.

The discovery rate never converged, and the last two escapes were the seam's own
*evidence-keeping* branch — when the fix mechanism is subject to the defect
class, the design is wrong rather than incomplete. Enumerating producers cannot
win: nothing bounds the set of libraries sharing a Python process.

So the runtime no longer tries to win it. What survives is the half that was
always sound and never risky — **the evidence**:

1. :class:`~almanak.framework.data.tokens.exceptions.TokenResolutionError` and
   its subclasses (``TokenNotFoundError``, ``InvalidTokenAddressError``,
   ``SymbolTokenResolutionError``, ``AmbiguousTokenError``,
   ``TokenResolutionTimeoutError``) — **the token is not resolvable**. The
   business outcome the callers mean to fall back on. ``None``, DEBUG.
2. Everything else — **``None``, but never silently**. A defect-shaped failure
   (``TypeError`` / ``AttributeError`` / ``NameError``) is logged at ERROR with
   a traceback and reported to the defect observer below; anything else is
   logged at WARNING with a traceback. The pre-fix code's real sin was not the
   fallback — it was destroying the evidence. This keeps both.

The original defect — a broken test double reading as an unresolvable token —
was a **test-quality** problem that was never present in production. It is
closed where it lives, in CI, by two checks that use the observer below:

* ``tests/conftest.py::_fail_on_token_resolver_defects`` — an autouse fixture
  that FAILS any test in which the seam degraded a defect (dynamic);
* ``tests/unit/framework/data/tokens/test_resolver_double_conformance_vib6100.py``
  — every resolver double in the tree must accept the calls production makes,
  whether or not any test exercises it (static).

Together those close it with zero production risk, which the runtime tier never
could.

``chain`` is passed **by keyword**, always. The historical split — positional
at ``observability/ledger.py``, keyword at ``data/tokens/pair_order.py`` — is
precisely what let one shared test double work at one site and raise
``TypeError`` at the other. Routing every call site through this helper removes
the divergence at source.

``log_errors=False`` is fixed (the caller decides what to log). Gateway
discovery defaults OFF (``allow_gateway=False`` / ``skip_gateway=True``) so a
hot-path write never stalls on an RPC; a caller that must match pre-seam
behaviour where the static registry is incomplete may opt in with
``allow_gateway=True``. Loud resolver logs still want the resolver directly.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

# Imported at MODULE level, deliberately (VIB-6167, escape #5).
#
# A function-local ``from ... import`` is a deferred module-body execution: the
# first call runs the imported module's body one frame ABOVE this module's own
# handlers, so an import-time fault escapes every guard here and surfaces in
# whatever the caller happens to be — an accounting write with the trade already
# on-chain. The resolver's import subgraph is wide (chains, registry, models,
# cache, deprecation, transitively ``web3``), and a ``warnings.warn`` anywhere in
# it can raise through a third-party ``warnings.showwarning``.
#
# Importing at module load moves that fault to process start, where it is a boot
# failure — loud, attributable, and before any money moves. That is the correct
# place for it. Only the resolver CONSTRUCTION (registry build + disk-cache read)
# remains guarded below, because it is genuinely environmental and happens lazily.
#
# The MODULE is imported and ``get_token_resolver`` is looked up on it at CALL
# time, rather than binding the function object here. Binding it would capture
# whatever was installed at import time, silently defeating the
# ``monkeypatch.setattr("...tokens.resolver.get_token_resolver", ...)`` convention
# every suite in this repo uses — the double would be installed, the seam would
# go on calling the real resolver, and the test would pass for the wrong reason.
# That is the same false-green shape VIB-6100 is about, so it is not a detail.
from almanak.framework.data.tokens import resolver as _resolver_module
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import _canonical_chain_name

if TYPE_CHECKING:  # pragma: no cover - typing only
    from almanak.framework.data.tokens.resolver import ResolvedToken

logger = logging.getLogger(__name__)

#: Upper bound on a token's decimals, mirroring ``ResolvedToken.__post_init__``.
#: Kept in sync there; a value outside ``0..77`` is not a token.
_MAX_DECIMALS = 77

#: Exception types that usually mean "the call itself was wrong" rather than
#: "the token is unresolvable". Used ONLY to pick a log level and to report to
#: the defect observer — never for control flow. See the module docstring for
#: why keying *behaviour* on these types is unsound.
_DEFECT_SHAPED = (TypeError, AttributeError, NameError)


@dataclass(frozen=True)
class ResolverDefect:
    """A defect-shaped resolver failure, reported to the observer for CI.

    ``at_call_boundary`` is the sharp signal. A genuine signature mismatch is
    raised *at* the call — the callee is never entered, so the traceback has
    exactly one frame (``tb_next is None``). A fault deep inside the callee has
    more. Verified against both shapes; see
    ``tests/unit/framework/data/tokens/test_best_effort_resolver_vib6100.py``.

    This is a *diagnosis*, not a decision: production behaviour is identical
    either way (degrade to ``None``). VIB-6167 records real caveats — ``raise ...
    from ...``, C-implemented callees, and decorated or proxied ``resolve``
    methods can all move the frame count — and every one of them is harmless
    here precisely because nothing branches on it at runtime.
    """

    context: str
    token: str
    chain: str
    exc_type: str
    at_call_boundary: bool


#: Optional observer for defect-shaped failures. ``None`` in production — the
#: test harness installs a collector (see ``tests/conftest.py``) so that a broken
#: double FAILS THE TEST RUN instead of quietly reading as an unresolvable token.
#:
#: This is how VIB-6100 is actually closed: in CI, where the defect lives, rather
#: than by a production halt path. The production code path is byte-identical
#: with and without an observer installed.
_defect_observer: Callable[[ResolverDefect], None] | None = None


def set_resolver_defect_observer(
    observer: Callable[[ResolverDefect], None] | None,
) -> Callable[[ResolverDefect], None] | None:
    """Install ``observer`` for defect-shaped failures; return the previous one.

    Test-harness seam. Returns the previous observer so a fixture can restore it
    rather than clearing it, which keeps nested installs honest.
    """
    global _defect_observer  # noqa: PLW0603 — a process-wide harness seam by design
    previous = _defect_observer
    _defect_observer = observer
    return previous


def _safe_log(level: int, msg: str, *args: object, **kwargs: object) -> None:
    """Log without being able to raise into the caller.

    Every tolerance arm below logs, and logging CAN raise:
    ``logging.Handler.handle`` does not wrap ``emit`` in a try — a handler is
    expected to call ``self.handleError`` itself, and a third-party one that does
    not propagates straight out. ``logging.raiseExceptions = False`` does not
    help; it guards errors *inside* ``handleError``, not an ``emit`` that raises.

    Measured: with such a handler installed, a ``TypeError`` escaped the
    non-string-token arm — the ERROR-and-tolerate branch became a raise, on the
    accounting write path, which is the exact failure that arm exists to prevent.
    The most recursive instance of this class on this PR: the evidence-keeping is
    what broke the tolerance. Mirrors the same helper in ``cache.py`` (VIB-6168).

    Deliberately silent on failure — there is nothing left to report *with*, and
    a second logging attempt is the most likely thing to fail next.
    """
    try:
        logger.log(level, msg, *args, **kwargs)  # type: ignore[arg-type]
    except Exception:  # noqa: BLE001 — logging must never break the caller
        pass


def _notify_defect(defect: ResolverDefect) -> None:
    """Report to the observer, which must never be able to break the caller.

    The observer is test-installed and arbitrary, so it is exactly the kind of
    third-party code the module docstring says cannot be trusted not to raise.
    Guarding it here is the same lesson applied to this module's own addition —
    an observability hook must not be able to break what it observes.
    """
    observer = _defect_observer
    if observer is None:
        return
    try:
        observer(defect)
    except Exception:  # noqa: BLE001 — a CI hook must never fail a money path
        _safe_log(logging.ERROR, "resolver defect observer raised; ignoring", exc_info=True)


def _classify(exc: BaseException) -> bool:
    """True when ``exc`` was raised AT the call boundary (callee never entered)."""
    tb = exc.__traceback__
    return tb is not None and tb.tb_next is None


def _malformed_result_reason(resolved: object) -> str | None:
    """Return a defect reason if ``resolved`` is not a usable resolve success.

    Production ``TokenResolver.resolve`` is typed ``-> ResolvedToken`` and
    **raises** ``TokenNotFoundError`` / ``TokenResolutionError`` on miss — it
    never returns ``None`` and never returns a partial object. A double that
    returns ``None`` or an incomplete namespace therefore degrades to the
    fallback **without** a ``TokenResolutionError``, which is the VIB-6100
    false-green wearing a different costume: the dynamic observer used to see
    nothing and the test passed against the fallback (Codex review of the
    VIB-6100 split / #3694).

    Shape must match production ``ResolvedToken.__post_init__`` (not a weaker
    subset). Callers may still fail-open when this returns a reason; the point
    is that the degradation is **observable** via the defect observer.

    * ``symbol`` — non-empty ``str`` (production rejects ``""``)
    * ``address`` — non-empty ``str`` (pair_order / placement)
    * ``decimals`` — ``int`` in ``0..77``, not ``bool`` (amount scaling)
    * ``chain`` — non-empty ``str`` that canonicalizes (unknown chain raises
      in production; any non-empty string is not enough)
    """
    if resolved is None:
        return "resolver returned None (production raises TokenNotFoundError on miss)"

    missing = [name for name in ("symbol", "address", "decimals", "chain") if not hasattr(resolved, name)]
    if missing:
        return f"malformed resolver result: {type(resolved).__name__} missing {', '.join(missing)}"

    symbol = getattr(resolved, "symbol", None)
    if not isinstance(symbol, str) or not symbol.strip():
        return f"malformed resolver result: empty or non-str symbol ({symbol!r})"

    address = getattr(resolved, "address", None)
    if not isinstance(address, str) or not address.strip():
        return f"malformed resolver result: empty or non-str address ({address!r})"

    decimals = getattr(resolved, "decimals", None)
    if isinstance(decimals, bool) or not isinstance(decimals, int) or not (0 <= decimals <= _MAX_DECIMALS):
        return f"malformed resolver result: out-of-contract decimals ({decimals!r})"

    chain_val = getattr(resolved, "chain", None)
    if not isinstance(chain_val, str) or not chain_val.strip():
        return f"malformed resolver result: empty or non-str chain ({chain_val!r})"
    # Match ResolvedToken: unknown / uncanonicalizable chain is not a success.
    try:
        _canonical_chain_name(chain_val)
    except ValueError:
        return f"malformed resolver result: unknown chain ({chain_val!r})"

    return None


def _validated(resolved: object, *, token: str, chain: str, context: str) -> ResolvedToken | None:
    """Return ``resolved`` only if it is a complete, production-shaped success.

    The seam promises callers a usable token object or ``None``. Callers rely on
    it: ``lp_handler._v4_realign_token_pair`` reads ``.symbol`` unguarded;
    ``pair_order`` reads ``.address``; amount scaling reads ``.decimals``. A
    resolver (or double) returning ``None`` or a partial object without raising
    ``TokenResolutionError`` would previously pass through as a quiet miss —
    no defect observer, fallback assertions green.

    Legitimate business misses must raise ``TokenResolutionError`` (or a
    subclass); those are handled as DEBUG above this helper, not here.
    """
    reason = _malformed_result_reason(resolved)
    if reason is None:
        return resolved  # type: ignore[return-value]

    _safe_log(
        logging.ERROR,
        "token resolver returned a non-production success shape (%s): token=%s chain=%s "
        "type=%s reason=%s — reporting unresolved and notifying the defect observer. "
        "Production resolve raises on miss and returns a full ResolvedToken on success.",
        context,
        token,
        chain,
        type(resolved).__name__ if resolved is not None else "NoneType",
        reason,
    )
    _notify_defect(
        ResolverDefect(
            context=context,
            token=token,
            chain=str(chain),
            exc_type=reason,
            at_call_boundary=False,
        )
    )
    return None


def resolve_token_best_effort(
    token: str, chain: str, *, context: str, allow_gateway: bool = False
) -> ResolvedToken | None:
    """Resolve ``token`` on ``chain``, or ``None`` when it cannot be resolved.

    **Never raises ``Exception``.** Every failure mode returns ``None``; the
    difference between them is in the log, not in the control flow.

    Args:
        token: Token symbol, address, or CAIP-19 asset id. A non-``str`` is an
            upstream data defect: logged at ERROR and reported unresolved.
        chain: Chain name. Passed to the resolver **by keyword**.
        context: Short caller tag used in the log line (e.g. ``"ledger
            LP_CLOSE"``), so a resolver miss is attributable without a traceback.
        allow_gateway: Opt IN to gateway discovery for this call. Defaults to
            ``False`` — every caller is a best-effort read on an accounting write
            path and must not risk a gateway round-trip stall.

            One caller sets it: ``lp_handler._resolve_lp_amounts``, which
            resolved *with* the gateway before this seam existed. Pinning the
            flag there silently converted a MEASURED decimals into an unmeasured
            ``None`` for any token the gateway can discover but the static
            registry cannot — both LP money columns going unmeasured on a live
            path. Honest under Empty != Zero, but a real loss of measurement, and
            not a change this PR set out to make. Found by Codex review of #3472,
            after the PR body had wrongly claimed ``_v4_realign_token_pair`` was
            the only behavioural narrowing.

            This is a per-caller flag by evidence, not by preference: the "flags
            are contract, not choice" rule below assumed every caller wanted
            ``skip_gateway=True``, and that assumption was simply false for one
            of them.

    Returns:
        The ``ResolvedToken``; or ``None`` when the token was unresolvable
        (DEBUG), the call looked defective (ERROR + traceback + observer), or the
        resolver failed operationally (WARNING + traceback). Never returns a
        partially-populated placeholder — callers keep their own "unmeasured"
        handling (Empty != Zero).
    """
    if not isinstance(token, str):
        # A non-string token is an upstream DATA defect (a connector stamping
        # raw ``HexBytes``, a BLOB read back out of a TEXT column), and it is
        # what ``"/" in token`` inside the resolver would raise ``TypeError`` on.
        # Caught here so the log names the real cause rather than a confusing
        # membership-test traceback.
        _safe_log(
            logging.ERROR,
            "token resolver called with a non-string token (%s): type=%s chain=%s — "
            "treating as unresolved; fix the producer, this is not a resolvable token",
            context,
            type(token).__name__,
            chain,
        )
        _notify_defect(
            ResolverDefect(
                context=context,
                token=repr(token),
                chain=str(chain),
                exc_type=f"non-str token: {type(token).__name__}",
                at_call_boundary=True,
            )
        )
        return None

    # Resolver CONSTRUCTION is its own environment-fault domain: the singleton
    # builds the static registry and reads the shared disk cache on first use.
    # A fault there is an environment fault, not a resolution outcome.
    try:
        resolver = _resolver_module.get_token_resolver()
    except Exception:  # noqa: BLE001 — environment fault, not a resolution outcome
        _safe_log(
            logging.ERROR,
            "token resolver could not be constructed (%s): chain=%s — reporting %r "
            "unresolved. This is an environment fault, not an unresolvable token; the "
            "accounting write is deliberately NOT failed for it.",
            context,
            chain,
            token,
            exc_info=True,
        )
        # Reported to the observer too (Grok review of #3472). A miswired factory
        # is every bit as capable of false-greening a suite as a mismatched
        # double: the test installs a resolver, construction blows up, the seam
        # degrades, and the fallback assertion passes. Silent construction
        # failure was a hole in the CI guarantee.
        _notify_defect(
            ResolverDefect(
                context=context,
                token=token,
                chain=str(chain),
                exc_type="resolver construction failed",
                at_call_boundary=False,
            )
        )
        return None

    try:
        resolved = resolver.resolve(token, chain=chain, log_errors=False, skip_gateway=not allow_gateway)
        return _validated(resolved, token=token, chain=chain, context=context)
    except TokenResolutionError as exc:
        # The token is not resolvable. The expected, quiet outcome.
        _safe_log(
            logging.DEBUG,
            "token resolve miss (%s): token=%s chain=%s: %s: %s",
            context,
            token,
            chain,
            type(exc).__name__,
            exc,
        )
        return None
    except Exception as exc:  # noqa: BLE001 — total by contract; the tier is in the LOG
        if isinstance(exc, _DEFECT_SHAPED):
            at_boundary = _classify(exc)
            _safe_log(
                logging.ERROR,
                "token resolver call looks DEFECTIVE (%s): token=%s chain=%s %s — "
                "%s. Reported unresolved so the accounting write survives, but this is "
                "very likely a bug, not an unresolvable token; the evidence is in the "
                "traceback, not swallowed.",
                context,
                token,
                chain,
                type(exc).__name__,
                (
                    "raised AT the call boundary, so the resolver was never entered "
                    "(a signature mismatch — check the call, or the test double)"
                    if at_boundary
                    else "raised inside the resolver (a malformed object, or an "
                    "environmental fault wearing a defect-shaped type)"
                ),
                exc_info=True,
            )
            _notify_defect(
                ResolverDefect(
                    context=context,
                    token=token,
                    chain=str(chain),
                    exc_type=type(exc).__name__,
                    at_call_boundary=at_boundary,
                )
            )
        else:
            _safe_log(
                logging.WARNING,
                "token resolver failed operationally (%s): token=%s chain=%s — treating as "
                "unresolved; the evidence is in the traceback, not swallowed",
                context,
                token,
                chain,
                exc_info=True,
            )
            # Reported to the observer as well (Codex/Grok review of #3472).
            #
            # The CI guarantee used to fire only on defect-SHAPED types, which
            # left an obvious hole: a broken double raising ``RuntimeError`` or
            # ``ValueError`` degraded to ``None`` with observer_count=0, so the
            # autouse fixture saw nothing and the fallback assertion passed
            # falsely. That is the VIB-6100 failure mode with a different
            # exception type — and the static gate cannot see it, because it
            # checks signatures, not bodies.
            #
            # In PRODUCTION this changes nothing: no observer is installed and
            # the return value is identical. It only makes the test harness
            # louder, which is where this guarantee is supposed to live.
            _notify_defect(
                ResolverDefect(
                    context=context,
                    token=token,
                    chain=str(chain),
                    exc_type=f"operational: {type(exc).__name__}",
                    at_call_boundary=False,
                )
            )
        return None


def resolve_token_decimals_best_effort(
    token: str, chain: str, *, context: str, allow_gateway: bool = False
) -> int | None:
    """Decimals for ``token`` on ``chain``, or ``None`` when not resolvable.

    Same tolerance contract as :func:`resolve_token_best_effort` — never raises.
    ``None`` means *unmeasured*; callers must not substitute a guessed ``18``.
    """
    resolved = resolve_token_best_effort(token, chain, context=context, allow_gateway=allow_gateway)
    if resolved is None:
        return None
    # Re-read decimals only under a total guard: a descriptor that raised on
    # second access would otherwise break the no-raise contract (CodeRabbit #3694).
    # Shape/range was already validated in _malformed_result_reason.
    try:
        decimals = resolved.decimals
    except Exception as exc:  # noqa: BLE001 — total seam: never raise
        _safe_log(
            logging.ERROR,
            "token decimals attribute raised after validated resolve (%s): token=%s chain=%s "
            "type=%s err=%s — reporting unmeasured",
            context,
            token,
            chain,
            type(exc).__name__,
            exc,
            exc_info=True,
        )
        _notify_defect(
            ResolverDefect(
                context=context,
                token=token,
                chain=str(chain),
                exc_type=f"decimals-access: {type(exc).__name__}",
                at_call_boundary=False,
            )
        )
        return None

    # Validate the RANGE, not just the sign (VIB-6100 review of PR #3472).
    #
    # This previously checked only ``decimals < 0``, which let a malformed
    # token-info object through the top: ``decimals=999`` was accepted and the
    # caller scaled a raw amount by ``10**999``, and ``decimals=True`` was
    # accepted as 1 (``bool`` is an ``int`` subclass, and ``True < 0`` is False).
    # Production ``ResolvedToken`` rejects both — it validates ``0..77`` and is
    # the shape this seam claims to return — so anything outside that range is
    # not a token, whatever produced it.
    #
    # Reported UNMEASURED rather than raised, consistently with the rest of this
    # module: a malformed payload is a DATA defect, and failing an accounting
    # write for one is the louder, worse bug. ERROR so the producer is findable;
    # ``None`` so Empty != Zero holds downstream.
    if isinstance(decimals, bool) or not isinstance(decimals, int) or not (0 <= decimals <= _MAX_DECIMALS):
        # A MISSING ``decimals`` is reported too, not skipped (CodeRabbit, #3472).
        #
        # This arm previously stayed silent when ``decimals`` was ``None``, on the
        # reasoning that ``None`` means "nothing to report". That was wrong, and
        # wrong in this PR's own failure shape: a resolver that returns a token
        # object WITHOUT a ``decimals`` attribute is malformed — production's
        # ``ResolvedToken`` always carries one, validated to 0..77 — so the caller
        # was handed a silent "unmeasured" that is indistinguishable from an
        # ordinary miss. That is precisely the defect-read-as-miss laundering the
        # seam exists to end, surviving inside the seam.
        #
        # Still degrades rather than raises; only the evidence changes.
        _safe_log(
            logging.ERROR,
            "token resolver returned an out-of-contract decimals (%s): token=%s chain=%s "
            "decimals=%r type=%s — reporting unmeasured; ResolvedToken permits int 0..%d "
            "and always carries the attribute",
            context,
            token,
            chain,
            decimals,
            type(decimals).__name__,
            _MAX_DECIMALS,
        )
        _notify_defect(
            ResolverDefect(
                context=context,
                token=token,
                chain=str(chain),
                exc_type=(
                    "missing decimals attribute" if decimals is None else f"out-of-contract decimals: {decimals!r}"
                ),
                at_call_boundary=False,
            )
        )
        return None
    return decimals


__all__ = [
    "ResolverDefect",
    "resolve_token_best_effort",
    "resolve_token_decimals_best_effort",
    "set_resolver_defect_observer",
]
