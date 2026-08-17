"""Canonical token-decimals resolution for adapters and receipt parsers."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Protocol, cast

from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily

from .defaults import NATIVE_SENTINEL
from .exceptions import TokenNotFoundError, TokenResolutionError
from .metadata import TokenMeta
from .models import ResolvedToken, normalize_token_address_for_chain

DecimalsCache = MutableMapping[tuple[str, str], int | None]
DecimalsHint = TokenMeta | tuple[str, int] | int
DecimalsHints = Mapping[str, DecimalsHint]


class _Resolver(Protocol):
    def resolve(self, token: str, chain: str) -> ResolvedToken: ...


_RESOLVER_UNSET = object()


def _cache_key(token: str, chain: str) -> tuple[str, str]:
    return (chain, normalize_token_address_for_chain(token, chain))


def _hint_decimals(token: str, chain: str, hints: DecimalsHints | None) -> int | None:
    if not hints:
        return None
    normalized = normalize_token_address_for_chain(token, chain)
    for address, metadata in hints.items():
        if normalize_token_address_for_chain(address, chain) == normalized:
            if isinstance(metadata, int):
                hinted = metadata
            elif isinstance(metadata, tuple):
                hinted = metadata[1]
            else:
                hinted = metadata["decimals"]
            return hinted if not isinstance(hinted, bool) and 0 <= hinted <= 77 else None
    return None


def resolve_token_decimals(
    token: str,
    chain: str,
    *,
    cache: DecimalsCache | None = None,
    hints: DecimalsHints | None = None,
    resolver: _Resolver | None | object = _RESOLVER_UNSET,
) -> int:
    """Resolve decimals with one strict, chain-aware policy.

    Precedence is the EVM native sentinel invariant, an exact address hint,
    the caller cache, then ``TokenResolver``. Definitive not-found results may
    be negative-cached; transient resolution errors are never cached. Unknown
    tokens never receive an arbitrary decimals default.
    """
    descriptor = ChainRegistry.try_resolve(str(chain))
    if descriptor is None:
        raise TokenResolutionError(token=token, chain=str(chain), reason=f"Unknown chain '{chain}'")
    canonical_chain = descriptor.name
    key = _cache_key(token, canonical_chain)

    # The sentinel is an EVM protocol constant, not a token-list guess. This is
    # the only permitted fixed-decimals shortcut in the helper.
    if descriptor.family is ChainFamily.EVM and token.lower() == NATIVE_SENTINEL.lower():
        if cache is not None:
            cache[key] = 18
        return 18

    hinted = _hint_decimals(token, canonical_chain, hints)
    if hinted is not None:
        if cache is not None:
            cache[key] = hinted
        return hinted

    if cache is not None and key in cache:
        cached = cache[key]
        if cached is None:
            raise TokenNotFoundError(token=token, chain=canonical_chain, reason="Token decimals are negatively cached")
        return cached

    if resolver is _RESOLVER_UNSET:
        try:
            # Import the public binding lazily. Besides avoiding an import cycle,
            # this preserves the framework's established resolver-injection seam.
            from . import get_token_resolver

            resolver = get_token_resolver()
        except Exception as exc:
            raise TokenResolutionError(
                token=token,
                chain=canonical_chain,
                reason=f"Token resolver is unavailable: {exc}",
            ) from exc

    if resolver is None:
        raise AttributeError("Token resolver is not configured")
    typed_resolver = cast(_Resolver, resolver)

    try:
        decimals = typed_resolver.resolve(token, canonical_chain).decimals
    except TokenNotFoundError:
        if cache is not None:
            cache[key] = None
        raise

    if isinstance(decimals, bool) or not isinstance(decimals, int) or not 0 <= decimals <= 77:
        raise TokenResolutionError(
            token=token,
            chain=canonical_chain,
            reason=f"Token resolver returned invalid decimals: {decimals!r}",
        )

    if cache is not None:
        cache[key] = decimals
    return decimals


__all__ = ["DecimalsCache", "DecimalsHint", "DecimalsHints", "resolve_token_decimals"]
