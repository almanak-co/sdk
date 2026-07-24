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

_EVM_ADDRESS_PATTERN = re.compile(r"^0x[a-fA-F0-9]{40}$")
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
    while frame is not None and _is_internal_frame(frame):
        frame = frame.f_back
        depth += 1

    if frame is None:
        return ("<unknown>", 1, "")

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
