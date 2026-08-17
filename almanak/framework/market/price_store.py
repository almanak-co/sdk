"""Typed, chain-aware price storage and the canonical price lookup seam.

``MarketSnapshot`` historically flattened prices into one string-keyed dict.
That made every consumer reconstruct token identity with its own precedence
rules, and it allowed non-USD quotes to leak into the USD compatibility view.

This module owns both sides of the replacement:

* :class:`PriceStore` keeps address-shaped assets under the same
  ``(chain, normalized_address)`` identity used by ``TokenRef.identity_key``
  and keeps quote currencies in a separate key dimension.
* :func:`lookup_price` is the one compatibility-aware lookup.  Its precedence
  is exact typed identity, chain-qualified compatibility key, legacy bare
  address, symbol alias, then an explicitly supplied peg.

The module is pure and performs no network I/O.  Offline address-to-symbol
resolution is best-effort and ambiguity always fails closed.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from almanak.core.chains import ChainRegistry
from almanak.framework.data.tokens.address_resolution import (
    looks_like_address,
    looks_like_case_sensitive_address,
    resolve_token_symbol,
)
from almanak.framework.data.tokens.models import TokenRef

PriceIdentity = tuple[str, str]
PriceMatch = Literal["identity", "chain_address", "legacy_address", "symbol", "peg"]


@dataclass(frozen=True, slots=True)
class StoredPrice[T]:
    """One quote-aware price record held by :class:`PriceStore`."""

    price: Decimal
    quote: str
    data: T
    chain: str
    identity_key: PriceIdentity | None = None
    symbol: str | None = None


@dataclass(frozen=True, slots=True)
class PriceLookupResult:
    """The value, raw record, and precedence branch selected by a lookup."""

    price: Decimal
    raw: Any
    key: Any
    match: PriceMatch

    @property
    def used_peg(self) -> bool:
        """Whether this result came from the explicit synthetic-peg branch."""
        return self.match == "peg"


def _normalized_quote(quote: str | None) -> str:
    return (quote or "USD").strip().upper() or "USD"


def _canonical_chain(chain: str | None) -> str:
    if not isinstance(chain, str):
        return ""
    stripped = chain.strip()
    if not stripped:
        return ""
    descriptor = ChainRegistry.try_resolve(stripped)
    return descriptor.name if descriptor is not None else stripped.lower()


def _identity_key(chain: str | None, address: str | None) -> PriceIdentity | None:
    if not chain or not address or not looks_like_address(address, chain):
        return None
    try:
        # Decimal metadata is deliberately absent at a price boundary.  Zero is
        # a valid placeholder because TokenRef equality/hash use identity_key
        # only; constructing TokenRef here guarantees byte-identical chain and
        # address normalization without growing a second identity algorithm.
        return TokenRef(chain=chain, address=address, decimals=0).identity_key
    except (TypeError, ValueError):
        return None


def _identity_from_token(token: str | TokenRef, chain: str | None) -> PriceIdentity | None:
    if isinstance(token, TokenRef):
        return token.identity_key
    if not isinstance(token, str):
        return None
    stripped = token.strip()
    prefix, separator, address = stripped.partition(":")
    if separator and prefix and address and looks_like_address(address, prefix):
        return _identity_key(prefix, address)
    return _identity_key(chain, stripped)


def _symbol_from_token(token: str | TokenRef, chain: str | None, explicit: str | None) -> str | None:
    if explicit:
        return explicit.strip().upper() or None
    if isinstance(token, TokenRef) and token.symbol:
        return token.symbol.strip().upper() or None
    identity = _identity_from_token(token, chain)
    if identity is not None:
        return resolve_token_symbol(identity[1], identity[0])
    if isinstance(token, str) and token.strip():
        return token.strip().upper()
    return None


class PriceStore[T]:
    """Quote-aware prices keyed by token identity, with legacy symbol aliases.

    Address-shaped inputs always land in ``_by_identity`` under
    ``TokenRef.identity_key``.  Symbol-only inputs are retained in
    ``_by_symbol`` as a compatibility alias because a symbol does not contain
    enough information to invent an address identity.
    """

    def __init__(self) -> None:
        self._by_identity: dict[tuple[PriceIdentity, str], StoredPrice[T]] = {}
        self._by_symbol: dict[tuple[str, str, str], StoredPrice[T]] = {}
        self._identity_aliases: dict[tuple[str, str, str], PriceIdentity | None] = {}

    def put(
        self,
        token: str | TokenRef,
        *,
        chain: str,
        quote: str,
        price: Decimal,
        data: T,
        symbol: str | None = None,
    ) -> StoredPrice[T]:
        """Insert or replace one record and return its canonical form."""
        normalized_quote = _normalized_quote(quote)
        identity = _identity_from_token(token, chain)
        canonical_symbol = _symbol_from_token(token, chain, symbol)
        canonical_chain = identity[0] if identity is not None else _canonical_chain(chain)
        record = StoredPrice(
            price=price,
            quote=normalized_quote,
            data=data,
            chain=canonical_chain,
            identity_key=identity,
            symbol=canonical_symbol,
        )
        if identity is None:
            if canonical_symbol is None:
                raise ValueError(f"Price token must carry an identity or symbol: {token!r}")
            self._by_symbol[(canonical_chain, canonical_symbol, normalized_quote)] = record
            return record

        self._by_identity[(identity, normalized_quote)] = record
        if canonical_symbol:
            alias_key = (identity[0], canonical_symbol, normalized_quote)
            existing = self._identity_aliases.get(alias_key)
            if existing is None and alias_key in self._identity_aliases:
                # Already known ambiguous; a later write must not revive it.
                return record
            if existing is not None and existing != identity:
                self._identity_aliases[alias_key] = None
            else:
                self._identity_aliases[alias_key] = identity
        return record

    def records(self, *, quote: str = "USD") -> tuple[StoredPrice[T], ...]:
        """Return all records for ``quote`` once, in deterministic insertion order."""
        normalized_quote = _normalized_quote(quote)
        identity_records = [record for (_identity, q), record in self._by_identity.items() if q == normalized_quote]
        symbol_records = [record for (_chain, _symbol, q), record in self._by_symbol.items() if q == normalized_quote]
        return (*identity_records, *symbol_records)

    def has_unambiguous_symbol_alias(self, record: StoredPrice[T]) -> bool:
        """Whether ``record.symbol`` names exactly this identity in its quote lane."""
        if record.identity_key is None or not record.symbol:
            return False
        alias_key = (record.chain, record.symbol, record.quote)
        return self._identity_aliases.get(alias_key) == record.identity_key

    def _lookup_record(
        self,
        *,
        chain: str | None,
        address: str | None,
        symbols: tuple[str, ...],
        quote: str,
    ) -> tuple[StoredPrice[T], Any, PriceMatch] | None:
        normalized_quote = _normalized_quote(quote)
        identity = _identity_key(chain, address)
        if identity is not None:
            record = self._by_identity.get((identity, normalized_quote))
            if record is not None:
                return record, identity, "identity"

        canonical_chain = identity[0] if identity is not None else _canonical_chain(chain)
        for symbol in symbols:
            upper = symbol.strip().upper()
            if not upper:
                continue
            if canonical_chain:
                direct = self._by_symbol.get((canonical_chain, upper, normalized_quote))
                if direct is not None:
                    return direct, upper, "symbol"
                alias_identity = self._identity_aliases.get((canonical_chain, upper, normalized_quote))
                if alias_identity is not None:
                    aliased = self._by_identity.get((alias_identity, normalized_quote))
                    if aliased is not None:
                        return aliased, alias_identity, "symbol"
                continue

            # A chain-less symbol lookup is safe only when every matching
            # chain names the same stored record.  Otherwise choosing the
            # first insertion would recreate the cross-chain collision bug.
            candidates: list[StoredPrice[T]] = []
            candidates.extend(
                record
                for (candidate_chain, candidate_symbol, candidate_quote), record in self._by_symbol.items()
                if candidate_symbol == upper and candidate_quote == normalized_quote and candidate_chain
            )
            for (candidate_chain, candidate_symbol, candidate_quote), alias_identity in self._identity_aliases.items():
                if candidate_symbol != upper or candidate_quote != normalized_quote or not candidate_chain:
                    continue
                if alias_identity is not None:
                    aliased = self._by_identity.get((alias_identity, normalized_quote))
                    if aliased is not None:
                        candidates.append(aliased)
            # Equal marks do not make two token identities interchangeable.
            # Requiring one candidate keeps a later price divergence from
            # changing which asset a chain-less symbol silently names.
            if len(candidates) == 1:
                return candidates[0], upper, "symbol"
        return None

    def __len__(self) -> int:
        return len(self._by_identity) + len(self._by_symbol)


def _coerce_price(raw: Any) -> Decimal | None:
    if isinstance(raw, StoredPrice):
        return raw.price
    candidate = raw
    if isinstance(raw, Mapping):
        candidate = raw.get("price_usd")
        if candidate is None:
            candidate = raw.get("price")
    elif hasattr(raw, "price"):
        candidate = raw.price
    if candidate is None:
        return None
    try:
        price = candidate if isinstance(candidate, Decimal) else Decimal(str(candidate))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return price if price.is_finite() else None


def _result(raw: Any, key: Any, match: PriceMatch) -> PriceLookupResult | None:
    price = _coerce_price(raw)
    return PriceLookupResult(price=price, raw=raw, key=key, match=match) if price is not None else None


def _mapping_identity_candidates(
    prices: Mapping[Any, Any],
    identity: PriceIdentity,
    *,
    include_bare: bool,
) -> Iterable[tuple[Any, Any]]:
    for key, raw in prices.items():
        if isinstance(key, tuple) and len(key) == 2:
            candidate = _identity_key(str(key[0]), str(key[1]))
            if candidate == identity:
                yield key, raw
            continue
        if not isinstance(key, str):
            continue
        stripped = key.strip()
        prefix, separator, address = stripped.partition(":")
        if separator and prefix and address:
            candidate = _identity_key(prefix, address)
            if candidate == identity:
                yield key, raw
            continue
        if include_bare and looks_like_address(stripped, identity[0]):
            candidate = _identity_key(identity[0], stripped)
            if candidate == identity:
                yield key, raw


def _unambiguous_result(matches: Iterable[tuple[Any, Any]], match: PriceMatch) -> PriceLookupResult | None:
    parsed = [(key, raw, price) for key, raw in matches if (price := _coerce_price(raw)) is not None]
    if len(parsed) != 1:
        return None
    key, raw, price = parsed[0]
    return PriceLookupResult(price=price, raw=raw, key=key, match=match)


def _direct_symbol_result(prices: Mapping[Any, Any], symbol: str) -> PriceLookupResult | None:
    exact = prices.get(symbol)
    if exact is not None and (found := _result(exact, symbol, "symbol")) is not None:
        return found
    upper = symbol.upper()
    for key, raw in prices.items():
        if isinstance(key, str) and key.strip().upper() == upper:
            if (found := _result(raw, key, "symbol")) is not None:
                return found
    return None


def _qualified_symbol_result(
    prices: Mapping[Any, Any],
    symbol: str,
    chain: str | None,
) -> PriceLookupResult | None:
    """Resolve an exported ``chain:SYMBOL`` without guessing across chains."""
    canonical_chain = _canonical_chain(chain)
    matches: list[tuple[Any, Any]] = []
    for key, raw in prices.items():
        if not isinstance(key, str):
            continue
        prefix, separator, candidate_symbol = key.strip().partition(":")
        if not separator or candidate_symbol.strip().upper() != symbol:
            continue
        if canonical_chain and _canonical_chain(prefix) != canonical_chain:
            continue
        matches.append((key, raw))
    return _unambiguous_result(matches, "symbol")


def _metadata_symbol_result(
    prices: Mapping[Any, Any],
    symbol: str,
    chain: str | None,
) -> PriceLookupResult | None:
    matches: list[tuple[Any, Any]] = []
    for key, raw in prices.items():
        if not isinstance(raw, Mapping) or str(raw.get("symbol") or "").strip().upper() != symbol:
            continue
        if chain:
            key_identity: PriceIdentity | None = None
            if isinstance(key, tuple) and len(key) == 2:
                key_identity = _identity_key(str(key[0]), str(key[1]))
            elif isinstance(key, str):
                prefix, separator, key_address = key.partition(":")
                if separator:
                    key_identity = _identity_key(prefix, key_address)
            if key_identity is not None and key_identity[0] != _canonical_chain(chain):
                continue
        matches.append((key, raw))
    return _unambiguous_result(matches, "symbol")


def _identity_symbol_result(
    prices: Mapping[Any, Any],
    symbol: str,
    chain: str | None,
) -> PriceLookupResult | None:
    matches: list[tuple[Any, Any]] = []
    for key, raw in prices.items():
        if not isinstance(key, str):
            continue
        prefix, separator, candidate_address = key.strip().partition(":")
        if not (separator and prefix and candidate_address):
            continue
        identity = _identity_key(prefix, candidate_address)
        if identity is None or (chain and identity[0] != _canonical_chain(chain)):
            continue
        if resolve_token_symbol(identity[1], identity[0]) == symbol:
            matches.append((key, raw))
    return _unambiguous_result(matches, "symbol")


def _mapping_symbol_result(
    prices: Mapping[Any, Any],
    symbols: tuple[str, ...],
    *,
    chain: str | None,
) -> PriceLookupResult | None:
    for symbol in symbols:
        upper = symbol.strip().upper()
        if not upper:
            continue
        found = (
            _qualified_symbol_result(prices, upper, chain)
            or _direct_symbol_result(prices, upper)
            or _metadata_symbol_result(prices, upper, chain)
            or _identity_symbol_result(prices, upper, chain)
        )
        if found is not None:
            return found
    return None


def _mapping_chainless_address_result(
    prices: Mapping[Any, Any],
    address: str,
) -> tuple[PriceLookupResult | None, bool]:
    matches: list[tuple[Any, Any, Decimal | None]] = []
    for key, raw in prices.items():
        if not isinstance(key, str):
            continue
        prefix, separator, candidate_address = key.strip().partition(":")
        if not (separator and prefix and candidate_address):
            continue
        candidate_identity = _identity_key(prefix, candidate_address)
        requested_identity = _identity_key(prefix, address)
        if candidate_identity is not None and candidate_identity == requested_identity:
            matches.append((key, raw, _coerce_price(raw)))
    # A bare address carries no chain identity.  More than one composite key
    # is ambiguous even when both happen to have the same mark today; choosing
    # either would make correctness depend on mapping insertion order again.
    if len(matches) > 1:
        return None, True
    if matches and matches[0][2] is not None:
        key, raw, price = matches[0]
        assert price is not None
        return PriceLookupResult(price=price, raw=raw, key=key, match="chain_address"), False
    return None, False


def _inferred_symbols_for_address(
    prices: Mapping[Any, Any],
    *,
    address: str,
    chain: str | None,
) -> tuple[str, ...]:
    inferred_chain = chain
    if inferred_chain is None:
        # Legacy ledger/snapshot maps sometimes pair symbol keys with a bare
        # address consumer.  A single composite-key chain supplies enough
        # context to bridge that address to its offline symbol.  Two or more
        # chains remain ambiguous even if only one registry happens to know the
        # address today; registry coverage must not decide token identity.
        mapped_chains: set[str] = set()
        for key in prices:
            if not isinstance(key, str):
                continue
            prefix, separator, candidate_address = key.strip().partition(":")
            if separator and _identity_key(prefix, candidate_address) is not None:
                mapped_chains.add(prefix.strip().lower())
        if len(mapped_chains) != 1:
            return ()
        inferred_chain = next(iter(mapped_chains))
    resolved = resolve_token_symbol(address, inferred_chain)
    return (resolved,) if resolved else ()


def _normalize_lookup_input(
    token: str | TokenRef | None,
    chain: str | None,
    address: str | None,
    symbol: str | None,
    *,
    infer_symbol_from_address: bool,
) -> tuple[str | None, str | None, str | None]:
    if token is None:
        return chain, address, symbol
    token_identity = _identity_from_token(token, chain)
    if token_identity is not None:
        chain, address = token_identity
        if symbol is None and infer_symbol_from_address:
            symbol = _symbol_from_token(token, chain, None)
        return chain, address, symbol
    if address is None and isinstance(token, str):
        # A chain-less compatibility reader can still match this against an
        # explicit ``chain:address`` key below.
        address = token.strip()
    # A chain-less EVM address is not a symbol alias.  Treating it as one would
    # make the case-insensitive symbol fallback accept a bare address without
    # the chain identity required by the lookup contract.
    if (
        symbol is None
        and isinstance(token, str)
        and not (
            chain is None and (looks_like_address(token.strip()) or looks_like_case_sensitive_address(token.strip()))
        )
    ):
        symbol = _symbol_from_token(token, chain, None)
    return chain, address, symbol


def _mapping_identity_result(
    prices: Mapping[Any, Any],
    identity: PriceIdentity,
) -> PriceLookupResult | None:
    # Tuple keys are the typed mapping form.
    typed = prices.get(identity)
    if typed is not None and (found := _result(typed, identity, "identity")) is not None:
        return found
    for key, raw in _mapping_identity_candidates(prices, identity, include_bare=False):
        if not isinstance(key, tuple) and (found := _result(raw, key, "chain_address")) is not None:
            return found
    for key, raw in _mapping_identity_candidates(prices, identity, include_bare=True):
        if isinstance(key, tuple) or (isinstance(key, str) and ":" in key):
            continue
        if (found := _result(raw, key, "legacy_address")) is not None:
            return found
    return None


def _mapping_lookup(
    prices: Mapping[Any, Any],
    *,
    chain: str | None,
    address: str | None,
    symbols: tuple[str, ...],
    infer_symbol_from_address: bool,
) -> tuple[PriceLookupResult | None, bool]:
    identity = _identity_key(chain, address)
    if identity is not None and (found := _mapping_identity_result(prices, identity)) is not None:
        return found, False
    if identity is None and address and not chain:
        found, ambiguous = _mapping_chainless_address_result(prices, address)
        if found is not None or ambiguous:
            return found, ambiguous
    # Address-to-symbol inference needs a chain.  Without one, inspecting the
    # other keys in the mapping and guessing their chain would reintroduce the
    # very cross-chain aliasing this seam is meant to remove.
    inferred = (
        _inferred_symbols_for_address(prices, address=address, chain=chain)
        if address and infer_symbol_from_address
        else ()
    )
    return _mapping_symbol_result(prices, (*symbols, *inferred), chain=chain), False


def lookup_price(
    prices: PriceStore[Any] | Mapping[Any, Any] | None,
    *,
    token: str | TokenRef | None = None,
    chain: str | None = None,
    address: str | None = None,
    symbol: str | None = None,
    aliases: Iterable[str] = (),
    quote: str = "USD",
    peg: Decimal | None = None,
    infer_symbol_from_address: bool = True,
) -> PriceLookupResult | None:
    """Resolve one price with the SDK-wide deterministic precedence contract.

    Precedence is:

    1. exact typed ``(chain, address)`` identity;
    2. legacy ``chain:address`` compatibility key;
    3. legacy bare address (accepted only with an explicit chain);
    4. explicit or safely inferred symbol aliases;
    5. an explicit caller-supplied peg.

    Generic mappings are the USD-only compatibility surface.  Non-USD quotes
    therefore resolve only from :class:`PriceStore`, never by relabelling a USD
    scalar under another quote.
    """
    chain, address, symbol = _normalize_lookup_input(
        token,
        chain,
        address,
        symbol,
        infer_symbol_from_address=infer_symbol_from_address,
    )

    normalized_quote = _normalized_quote(quote)
    primary_symbol = symbol.strip().upper() if isinstance(symbol, str) and symbol.strip() else None
    alias_symbols = sorted({alias.strip().upper() for alias in aliases if isinstance(alias, str) and alias.strip()})
    if primary_symbol is not None:
        alias_symbols = [alias for alias in alias_symbols if alias != primary_symbol]
    symbols = ((primary_symbol,) if primary_symbol is not None else ()) + tuple(alias_symbols)

    if isinstance(prices, PriceStore):
        store_match = prices._lookup_record(chain=chain, address=address, symbols=symbols, quote=normalized_quote)
        if store_match is not None:
            record, key, match = store_match
            return PriceLookupResult(price=record.price, raw=record.data, key=key, match=match)
    elif prices is not None and normalized_quote == "USD":
        mapping_match, ambiguous = _mapping_lookup(
            prices,
            chain=chain,
            address=address,
            symbols=symbols,
            infer_symbol_from_address=infer_symbol_from_address,
        )
        if mapping_match is not None:
            return mapping_match
        if ambiguous:
            return None

    if peg is not None:
        price = _coerce_price(peg)
        if price is not None:
            return PriceLookupResult(price=price, raw=peg, key=None, match="peg")
    return None


__all__ = [
    "PriceIdentity",
    "PriceLookupResult",
    "PriceStore",
    "StoredPrice",
    "lookup_price",
]
