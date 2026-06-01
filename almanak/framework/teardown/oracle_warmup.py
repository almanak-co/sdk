"""Teardown price-oracle warm + validate seam (VIB-4842).

A freshly-constructed ``MarketSnapshot`` has an empty ``_price_cache`` until
something calls ``.price(...)``. The teardown path reads
``market.get_price_oracle_dict()`` straight into the compiler (see
``teardown_manager.py`` Step 5.5), so an un-warmed oracle means the compiler
fails three layers down with a generic ``ValueError`` such as::

    Price for 'WETH' is missing in the price oracle.

This module closes that gap **at the teardown seam** rather than inside
``get_price_oracle_dict()`` — that getter stays pure (a lazy-fetch-on-miss
there would reintroduce the greedy-call problem the data-layer work is fixing).

The contract is a **pre-flight check** (VIB-4842, PRD May26 §T2b): it runs
*before* any closing intent has executed, so failing loud here cannot strand a
partially-unwound position. Once an intent has landed on-chain, teardown's
inverted failure semantics (``AGENTS.md`` §Teardown) take over and pricing
failures must never block the next risk-reducing intent — this module is never
called from that post-execution path.

Design notes:

- We warm only the **intent token set plus the native gas token**, never the
  whole wallet, to stay within the rate budget.
- Token extraction handles both decompiled ``Intent`` objects (``execute``
  path) and serialized intent dicts (``resume`` path, which stores
  ``pending_intents_json``).
- Validation mirrors the compiler's own resolution leniency
  (case-insensitive, wrapped<->native alias, known-stablecoin $1 fallback) so
  we only fail loud when a token is *genuinely* unpriceable — not when the
  compiler would have resolved it anyway.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from almanak.framework.intents.compiler import (
    _CHAIN_NATIVE_SYMBOLS,
    IntentCompiler,
)

logger = logging.getLogger(__name__)


# Attribute / dict-key names that carry a token *symbol* on an intent. LP
# intents (LP_OPEN) carry their pair inside a ``pool`` string (``TOKEN0/TOKEN1
# [/FEE]``) which is handled separately; LP_CLOSE intents carry only
# ``position_id`` / ``pool`` and the connector resolves the pair on-chain at
# compile time, so those tokens are not warmable from the intent alone.
_TOKEN_SYMBOL_FIELDS: tuple[str, ...] = (
    "from_token",
    "to_token",
    "token",
    "token_in",
    "collateral_token",
    "borrow_token",
    "deposit_token",
    "pt_token",
    "token_a",
    "token_b",
    "token_x",
    "token_y",
    "asset",
)


class TeardownPriceOracleError(RuntimeError):
    """Raised when the teardown price oracle cannot be made complete.

    This is a loud pre-flight failure (before any closing intent executes)
    naming the unpriceable token and the warming attempt, instead of a generic
    compiler ``ValueError`` surfaced three layers down at compile time.
    """


def _looks_like_symbol(value: Any) -> bool:
    """True for a plausible token *symbol* string (not an address)."""
    if not isinstance(value, str):
        return False
    candidate = value.strip()
    if not candidate:
        return False
    # Raw 0x addresses are resolved by the connector, not priced by symbol.
    if candidate.startswith("0x"):
        return False
    return True


def _symbols_from_pool_string(pool: Any) -> list[str]:
    """Extract token symbols from a ``TOKEN0/TOKEN1[/FEE]`` pool string.

    Matches the compiler's pool parsing (``_parse_pool_info``). Bare 0x
    addresses and address-based pool strings yield nothing — the connector
    resolves those at compile time.
    """
    if not isinstance(pool, str) or "/" not in pool:
        return []
    parts = [p.strip() for p in pool.split("/")]
    if len(parts) < 2:
        return []
    return [p for p in parts[:2] if _looks_like_symbol(p)]


def _intent_field(intent: Any, name: str) -> Any:
    """Read ``name`` from either an Intent object or a serialized dict."""
    if isinstance(intent, dict):
        return intent.get(name)
    return getattr(intent, name, None)


def extract_required_token_chains(intents: list[Any], fallback_chain: str | None) -> dict[str, str | None]:
    """Map every token symbol a teardown plan needs priced to its chain.

    Returns ``{SYMBOL: chain}`` where ``chain`` is the chain of the intent the
    symbol came from. Each token is warmed with *its own* intent's chain so a
    multi-chain teardown prices every token on the correct chain (VIB-4842
    Codex review P1) instead of pricing them all on one plan-wide chain.

    Handles both decompiled ``Intent`` objects (``execute`` path) and
    serialized intent dicts (``resume`` path). Includes the native gas token of
    **every** chain present in the plan — ledger gas pricing needs them, and a
    multi-chain plan has more than one native gas token to warm (VIB-4842 Codex
    review P2). The repro shows ``gas_native_status='price_missing'`` when a gas
    token is left un-warmed.

    Symbols are returned upper-cased to match ``get_price_oracle_dict()`` keys.
    When a token appears on multiple chains, the first-seen chain wins (the warm
    call only needs *a* valid chain to populate the cache; later validation is
    chain-agnostic).

    ``fallback_chain`` is used only for tokens on an intent that declares no
    chain, and to seed native-gas warming when no intent carries a chain.
    """
    token_chains: dict[str, str | None] = {}
    chains_in_plan: set[str] = set()

    def _record(symbol: str, chain: str | None) -> None:
        key = symbol.strip().upper()
        # First-seen chain wins; never overwrite a concrete chain with None.
        if key not in token_chains or (token_chains[key] is None and chain is not None):
            token_chains[key] = chain

    for intent in intents:
        raw_chain = _intent_field(intent, "chain")
        intent_chain = raw_chain.strip() if isinstance(raw_chain, str) and raw_chain.strip() else fallback_chain
        if isinstance(intent_chain, str) and intent_chain.strip():
            chains_in_plan.add(intent_chain.strip())

        for field in _TOKEN_SYMBOL_FIELDS:
            value = _intent_field(intent, field)
            if _looks_like_symbol(value):
                _record(value, intent_chain)
        for symbol in _symbols_from_pool_string(_intent_field(intent, "pool")):
            _record(symbol, intent_chain)

    # Native gas token(s) for EVERY chain in the plan — ledger gas pricing
    # needs each one. A multi-chain teardown that only warmed one chain's gas
    # token would leave the others ``price_missing`` (VIB-4842 Codex review P2).
    if not chains_in_plan and fallback_chain:
        chains_in_plan.add(fallback_chain)
    for plan_chain in chains_in_plan:
        for native in _CHAIN_NATIVE_SYMBOLS.get(plan_chain.lower(), frozenset()):
            _record(native, plan_chain)

    return token_chains


def extract_required_tokens(intents: list[Any], chain: str | None) -> set[str]:
    """Collect the token symbols a teardown plan needs priced.

    Thin wrapper over :func:`extract_required_token_chains` that drops the
    per-token chain mapping. Retained for callers / tests that only need the
    symbol set. Symbols are upper-cased to match ``get_price_oracle_dict()``.
    """
    return set(extract_required_token_chains(intents, chain).keys())


def _is_usable_price(value: Any) -> bool:
    """True for a real, positive numeric price (rejects None / 0 / non-numbers)."""
    if value is None or isinstance(value, bool):
        return False
    if not isinstance(value, int | float | Decimal):
        return False
    try:
        return value > 0
    except TypeError:
        return False


def _can_resolve_price(symbol: str, oracle: dict[str, Any]) -> bool:
    """Mirror the compiler's price-resolution leniency for validation.

    Returns ``True`` when ``symbol`` would resolve to a non-zero price in
    ``_require_token_price`` — directly, case-insensitively, via the
    wrapped<->native alias (BOTH directions), or via the known-stablecoin $1
    fallback.
    """
    symbol_upper = symbol.upper()

    # Direct / case-insensitive match with a non-zero price.
    for key, val in oracle.items():
        if key.upper() == symbol_upper and val is not None and val != 0:
            return True

    # Wrapped<->native alias, BOTH directions. Mirror the compiler's
    # bidirectional ``_expand_native_aliases_in_price_oracle``, which copies a
    # known price across a wrapped/native pair from whichever side is present
    # (``WETH`` -> ``ETH`` *and* ``ETH`` -> ``WETH``). A one-directional check
    # here false-positives a ``TeardownPriceOracleError`` for, e.g., a native
    # ``ETH`` requirement when the oracle only holds ``WETH`` — blocking a
    # risk-reducing teardown the compiler would have priced fine (VIB-4842).
    aliases: set[str] = set()
    native_alias = IntentCompiler._WRAPPED_TO_NATIVE.get(symbol_upper)
    if native_alias:
        aliases.add(native_alias.upper())
    aliases.update(
        wrapped.upper()
        for wrapped, native in IntentCompiler._WRAPPED_TO_NATIVE.items()
        if native.upper() == symbol_upper
    )
    for alias in aliases:
        for key, val in oracle.items():
            if key.upper() == alias and val is not None and val != 0:
                return True

    # Known stablecoins fall back to $1.00 in the compiler.
    if symbol_upper in IntentCompiler._get_known_stablecoins():
        return True

    return False


def warm_and_validate_oracle(
    market: Any,
    intents: list[Any],
    chain: str | None,
    *,
    raise_on_missing: bool = True,
) -> dict[str, Any] | None:
    """Warm the price oracle for a teardown plan, then validate completeness.

    Pre-flight check run *before* any closing intent executes:

    1. Extract the required token set from ``intents`` (+ native gas token).
    2. Synchronously call ``market.price(token)`` for each (the teardown setup
       path is not on the async strategy loop), populating ``_price_cache``.
    3. Fetch the oracle dict and validate every required token resolves; raise
       :class:`TeardownPriceOracleError` naming the first unpriceable token if
       not.

    Returns the warmed ``{symbol: price}`` oracle dict, or ``None`` when
    ``market`` cannot supply one (no warming possible — leave the legacy
    behaviour untouched).

    Args:
        raise_on_missing: When ``True`` (default), an incomplete oracle raises
            loud — correct for the genuine pre-flight (no closing intent has
            executed yet). When ``False``, warming still runs (to populate the
            cache for the remaining intents) but a still-missing token only logs
            and the warmed dict is returned anyway. This is the *resume-past-
            progress* path (VIB-4842 Codex review P1): some closing intents have
            already landed on-chain, so teardown's inverted-failure semantics
            forbid blocking the next risk-reducing intent.

    Raises:
        TeardownPriceOracleError: A required token is still missing after
            warming AND ``raise_on_missing`` is ``True``. Loud, named, and
            pre-execution by construction.
    """
    if market is None or not hasattr(market, "get_price_oracle_dict"):
        return None

    token_chains = extract_required_token_chains(intents, chain)
    required = set(token_chains.keys())
    if not required:
        # Nothing to warm (e.g. LP_CLOSE-only plan whose tokens resolve
        # on-chain at compile time). Return whatever the oracle already holds.
        fetched = market.get_price_oracle_dict()
        return fetched if fetched is not None else None

    can_price = hasattr(market, "price")
    warm_errors: dict[str, str] = {}
    # Tokens for which ``price()`` returned a usable (non-None, non-zero) value.
    # This is the authoritative "is priceable" signal — the oracle dict is a
    # secondary reflection that a real ``MarketSnapshot`` populates from the
    # same call, but which a token may legitimately resolve past via the
    # wrapped<->native alias even when its own key is absent.
    priced_ok: set[str] = set()
    for token in sorted(required):
        if not can_price:
            break
        # VIB-4842 Codex review P1: warm each token on ITS OWN intent's chain so
        # a multi-chain teardown prices every token on the correct chain. Pass
        # ``chain=`` only when known — ``chain=None`` lets MarketSnapshot apply
        # its single-chain default (and raise AmbiguousChainError on a genuinely
        # ambiguous multi-chain snapshot, which we surface as a warm error).
        token_chain = token_chains.get(token)
        try:
            value = market.price(token, chain=token_chain) if token_chain else market.price(token)
        except Exception as exc:  # noqa: BLE001 — validation re-checks below
            warm_errors[token] = str(exc)
            logger.warning(
                "Teardown oracle warm: price(%s, chain=%s) failed: %s",
                token,
                token_chain,
                exc,
            )
        else:
            if _is_usable_price(value):
                priced_ok.add(token.upper())

    fetched = market.get_price_oracle_dict()
    oracle: dict[str, Any] = fetched if fetched is not None else {}

    # A token is genuinely missing only when it neither returned a usable
    # ``price()`` value NOR resolves through the compiler's lenient lookup
    # (direct / case-insensitive / wrapped-native alias / stablecoin fallback).
    missing = [
        token for token in sorted(required) if token.upper() not in priced_ok and not _can_resolve_price(token, oracle)
    ]
    if missing:
        first = missing[0]
        sources = warm_errors.get(first, "market.price() returned no usable price")
        if raise_on_missing:
            raise TeardownPriceOracleError(
                f"Teardown pre-flight: price for '{first}' is missing from the oracle "
                f"after warming (required tokens: {', '.join(sorted(required))}; "
                f"all still-missing: {', '.join(missing)}). "
                f"Sources tried for '{first}': {sources}. "
                "Refusing to compile closing intents with an incomplete oracle."
            )
        # resume-past-progress: warm best-effort, never block the unwind.
        logger.warning(
            "Teardown oracle warm (resume past progress): %d token(s) still "
            "missing after warming (%s) — continuing anyway to avoid stranding "
            "a partially-unwound position. First missing '%s' sources: %s",
            len(missing),
            ", ".join(missing),
            first,
            sources,
        )

    return oracle


__all__ = [
    "TeardownPriceOracleError",
    "extract_required_token_chains",
    "extract_required_tokens",
    "warm_and_validate_oracle",
]
