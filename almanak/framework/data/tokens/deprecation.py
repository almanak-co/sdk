"""Deprecation policy for symbol-based token references.

Token symbols are metadata, not stable asset identity. Public SDK surfaces
accept them for the remainder of the 2.x line with a visible ``FutureWarning``
and reject them from SDK 3.0.0 onward. Chain-specific contract addresses and
CAIP-19 asset identifiers remain supported.
"""

from __future__ import annotations

import re
import sys
import warnings
from functools import lru_cache
from types import CodeType, FrameType

from almanak._version import __version__ as SDK_VERSION
from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily

from .exceptions import SymbolTokenResolutionError

SYMBOL_TOKEN_REMOVAL_VERSION = "3.0.0"
_SYMBOL_TOKEN_REMOVAL_RELEASE = (3, 0, 0)

# Case-insensitive 0x prefix: Instrument canonicalization uppercases tokens
# ("0X..." reaches this classifier via twap/lwap), and a valid address must
# never be reclassified as a deprecated *symbol* because of casing — that
# turns into a spurious warning today and a hard SymbolTokenResolutionError
# for a correctly-supplied address in 3.0.
_EVM_ADDRESS_PATTERN = re.compile(r"^0[xX][a-fA-F0-9]{40}$")
_SOLANA_ADDRESS_PATTERN = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
_RELEASE_PATTERN = re.compile(r"^\s*(\d+)\.(\d+)\.(\d+)")
_INTERNAL_MODULE_PREFIXES = ("almanak.", "pydantic.")
_MAX_CALLER_DEPTH_CACHE_SIZE = 4096
_CALLER_DEPTH_CACHE: dict[tuple[CodeType, CodeType | None], tuple[int, CodeType]] = {}


class SymbolTokenResolutionWarning(FutureWarning):
    """Warn that a symbol is being used where stable token identity is required."""


@lru_cache(maxsize=16)
def _release_tuple(version: str) -> tuple[int, int, int] | None:
    """Return the numeric release tuple from the SDK's generated version."""
    match = _RELEASE_PATTERN.match(version)
    if match is None:
        return None
    major, minor, patch = match.groups()
    return int(major), int(minor), int(patch)


def _is_solana_chain(chain: str | None) -> bool:
    if not chain:
        return False
    descriptor = ChainRegistry.try_resolve(chain)
    return descriptor is not None and descriptor.family is ChainFamily.SOLANA


@lru_cache(maxsize=4096)
def _is_address_based_token_reference(token: str, chain: str | None) -> bool:
    stripped = token.strip()
    if "/" in stripped:
        return True
    if _EVM_ADDRESS_PATTERN.fullmatch(stripped):
        return True

    prefix, separator, address = stripped.partition(":")
    if separator and prefix and address and ChainRegistry.try_resolve(prefix) is not None:
        return _is_address_based_token_reference(address, prefix)

    if chain is None or _is_solana_chain(chain):
        return bool(_SOLANA_ADDRESS_PATTERN.fullmatch(stripped))
    return False


def is_address_based_token_reference(token: str, chain: str | None = None) -> bool:
    """Return whether ``token`` carries address-based asset identity.

    Accepted identity forms are:

    - a raw EVM contract address;
    - a raw Solana mint when the chain is Solana or unspecified;
    - the snapshot's internal ``chain:address`` display form;
    - a CAIP-19 asset identifier.

    A malformed slash-containing value is left to the CAIP-19 parser to reject
    rather than being misreported as a deprecated token symbol.
    """
    if not isinstance(token, str):
        return False
    return _is_address_based_token_reference(token, chain)


def _is_internal_frame(frame: FrameType) -> bool:
    module_name = str(frame.f_globals.get("__name__", ""))
    return module_name.startswith(_INTERNAL_MODULE_PREFIXES)


def _is_infrastructure_frame(frame: FrameType) -> bool:
    """True for stdlib / dispatch-machinery frames that carry no caller identity.

    An SDK-internal symbol read dispatched through a thread pool (the runner's
    native-gas pre-fetch via ``asyncio.to_thread``) or through a CLI framework
    (``almanak strat new`` scaffolding under ``click``) bottoms out on frames
    like ``concurrent/futures/thread.py`` or ``click/core.py``. Those are not
    external callsites — attributing the warning there told users that
    *something* in their code used a deprecated symbol when nothing did
    (observed: ``MarketSnapshot received symbol-based token reference 'ETH'``
    at ``thread.py:59`` on a fully address-form strategy). Frames here are
    skipped like internal ones; a stack with no genuine external frame left
    is an internal origin and must not warn at all.
    """
    module_name = str(frame.f_globals.get("__name__", ""))
    if not module_name:
        return False
    top_level = module_name.partition(".")[0]
    if top_level in sys.stdlib_module_names:
        return True
    return module_name.startswith("click.")


# Sentinel filename meaning "internal origin — do not warn".
#
# Deliberately NOT cached by route: the same (start, parent) code pair is
# reachable from both internal-origin stacks (thread-pool dispatch) and
# genuine external callers, and unlike the depth cache below there is no
# frame identity left to re-validate a suppression against — a cached
# suppression would leak onto real user callsites. The walk below is the
# price of correctness on this path.
_INTERNAL_ORIGIN = "<sdk-internal>"


def _external_callsite() -> tuple[str, int, str]:
    """Return the external warning location without repeatedly walking the stack."""
    start = sys._getframe(2)
    parent = start.f_back
    route = (start.f_code, parent.f_code if parent is not None else None)
    cached_route = _CALLER_DEPTH_CACHE.get(route)

    if cached_route is not None:
        cached_depth, external_code = cached_route
        try:
            candidate = sys._getframe(cached_depth + 2)
        except ValueError:
            candidate = None
        if candidate is not None and candidate.f_code is external_code:
            return (
                candidate.f_code.co_filename,
                candidate.f_lineno,
                str(candidate.f_globals.get("__name__", "")),
            )

    frame: FrameType | None = start
    depth = 0
    while frame is not None and (_is_internal_frame(frame) or _is_infrastructure_frame(frame)):
        frame = frame.f_back
        depth += 1

    if frame is None:
        # Every frame was SDK-internal or dispatch machinery: internal origin.
        return (_INTERNAL_ORIGIN, 0, "")

    if len(_CALLER_DEPTH_CACHE) >= _MAX_CALLER_DEPTH_CACHE_SIZE:
        _CALLER_DEPTH_CACHE.clear()
    _CALLER_DEPTH_CACHE[route] = (depth, frame.f_code)
    return (
        frame.f_code.co_filename,
        frame.f_lineno,
        str(frame.f_globals.get("__name__", "")),
    )


@lru_cache(maxsize=4096)
def _apply_symbol_token_policy(
    token: str,
    chain: str | None,
    api: str,
    sdk_version: str,
    filename: str,
    lineno: int,
    module_name: str,
    _warning_context: int,
) -> None:
    """Apply the policy once per external callsite and warning context."""
    chain_label = chain or "the active chain"
    release = _release_tuple(sdk_version)
    if release is not None and release >= _SYMBOL_TOKEN_REMOVAL_RELEASE:
        raise SymbolTokenResolutionError(token=token, chain=chain_label, api=api)

    try:
        warnings.warn_explicit(
            (
                f"{api} received symbol-based token reference {token!r} on {chain_label}. "
                "Symbol-based token resolution is deprecated because it is unreliable. "
                "Use the chain-specific token contract address or a CAIP-19 asset identifier instead. "
                f"Symbol references will be rejected in Almanak SDK {SYMBOL_TOKEN_REMOVAL_VERSION} and later."
            ),
            SymbolTokenResolutionWarning,
            filename,
            lineno,
            module=module_name,
        )
    except SymbolTokenResolutionWarning:
        # A filter set to "error" makes ``warn_explicit`` raise the warning
        # CATEGORY itself. That is a deliberate user/CI escalation ("treat this
        # deprecation as fatal"), not a broken hook, and it MUST keep working —
        # ``simplefilter("error", SymbolTokenResolutionWarning)`` is asserted by
        # tests/unit/intent/test_symbol_token_deprecation.py and
        # tests/unit/data/tokens/test_symbol_resolution_deprecation.py.
        # Only this category is re-raised: a broken third-party showwarning that
        # raises some other Warning subclass is machinery failure and is
        # tolerated below (Codex review of #3472).
        raise
    except Exception:  # noqa: BLE001 — a notice must never fail the call it annotates
        # VIB-6100 review of PR #3472. ``warn_explicit`` reaches CPython's
        # ``_showwarnmsg``, which calls the PROCESS-GLOBAL, third-party-owned
        # ``warnings.showwarning`` hook. Anything in the process may replace it
        # — a test harness, a logging shim, a notebook frontend — and a bad
        # replacement raises straight out of this deprecation notice:
        #
        #   showwarning = None            -> TypeError
        #   showwarning of wrong arity    -> TypeError
        #   sys.stderr without .write     -> AttributeError
        #
        # (CPython guards only ``file is None`` and ``OSError``; a *replaced*
        # stderr is not covered.) Every bare-symbol resolve passes through here,
        # and all five accounting/observability call sites migrated by VIB-6100
        # pass symbols, so this is the common path rather than a corner of it.
        #
        # This guard is kept even though the best-effort seam is now total and
        # would degrade rather than halt (VIB-6167). Containment belongs to the
        # layer that owns the fault: a caller that is NOT the seam — a connector,
        # an adapter — gets no such tolerance, and would take the raise directly.
        # Relying on a downstream reader to be forgiving is not containment.
        #
        # This is the same rule as ``_try_record_metric``, one layer over:
        # observability must never break the thing it observes. A deprecation
        # notice is advisory by construction — losing it costs a log line, while
        # raising it costs a ledger row.
        #
        # NOT logged: this function is ``lru_cache``d per callsite, and the
        # obvious cause is a broken logging/warnings setup, so a logger call
        # here is the most likely thing to fail next. Silence is the correct
        # tolerance for an advisory notice; the SymbolTokenResolutionError path
        # above is unaffected and still raises.
        pass


def warn_or_reject_symbol_token_reference(
    token: str,
    chain: str | None,
    *,
    api: str,
) -> None:
    """Warn on 2.x or reject on 3.0.0+ when ``token`` is a bare symbol.

    The policy result is cached for each external callsite and active warnings
    context. Backtests can read the same token hundreds of thousands of times,
    while every actionable user callsite still receives its own warning.
    """
    if _is_address_based_token_reference(token, chain):
        return
    filename, lineno, module_name = _external_callsite()
    if filename == _INTERNAL_ORIGIN:
        # No genuine external caller in the stack (SDK-internal read reached
        # through thread-pool / CLI dispatch machinery). The deprecation
        # policy targets USER symbol references; warning here misattributes
        # the SDK's own internals to the user — suppress the WARNING only.
        # The 3.0 removal still applies: internal reads must be migrated
        # before the removal release, so on 3.0.0+ the hard rejection fires
        # regardless of origin (CodeRabbit review, PR #3612).
        release = _release_tuple(SDK_VERSION)
        if release is not None and release >= _SYMBOL_TOKEN_REMOVAL_RELEASE:
            raise SymbolTokenResolutionError(token=token, chain=chain or "the active chain", api=api)
        return
    _apply_symbol_token_policy(
        token,
        chain,
        api,
        SDK_VERSION,
        filename,
        lineno,
        module_name,
        id(warnings.filters),
    )


__all__ = [
    "SYMBOL_TOKEN_REMOVAL_VERSION",
    "SymbolTokenResolutionWarning",
    "is_address_based_token_reference",
    "warn_or_reject_symbol_token_reference",
]
