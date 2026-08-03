"""Canonical perp market-symbol parsing — the single canonicalization seam.

Perp market identifiers arrive in several spellings depending on the venue and
the author: ``"ETH/USD"`` (the SDK's documented GMX examples), ``"ETH-USD"``
(venue funding tables), ``"SOL-PERP"`` (Drift), ``"ETH_USD"``/``"ETH:USD"``
(seen in duck-typed intents), and bare coins (``"ETH"``, Hyperliquid).

Historically each lane parsed its own form: the backtest engine split on a
separator list, the gateway funding service split on ``"-"`` only, and the
connector funding tables were keyed by the dash form — so ``"ETH/USD"`` priced
a hedge at a $1 fallback in one lane (campaign-50 s42) and missed every
funding table in another (s38) while ``"ETH-USD"`` worked end to end.

Every consumer now normalizes through these two functions:

- :func:`perp_market_base` — the base asset symbol, for pricing lanes.
- :func:`perp_market_pair_key` — a separator-normalized pair, for connector
  execution registries whose canonical form is ``BASE/QUOTE``.
- :func:`perp_market_funding_key` — the canonical ``"<BASE>-USD"`` venue form,
  for funding-rate tables, gateway requests, and cache keys.

This module is intentionally pure stdlib (no framework/gateway imports) so the
backtest engine, the gateway servicers, and connector gateway providers can all
import it without cycles and without violating the gateway lean-import ratchet.
"""

from __future__ import annotations

#: Separators seen in perp market identifiers: "ETH/USD" (GMX docs), "ETH-USD",
#: "SOL-PERP" (Drift), "ETH_USD", "ETH:USD"; bare symbols ("ETH", Hyperliquid)
#: have no separator. The FIRST separator present wins, matching the engine's
#: historical parse.
PERP_MARKET_SEPARATORS: tuple[str, ...] = ("/", "-", ":", "_")


def perp_market_base(market: object) -> str | None:
    """Parse the base asset symbol from a perp market identifier.

    ``"ETH/USD"`` / ``"ETH-USD"`` / ``"ETH_USD"`` / ``"ETH:USD"`` / ``"ETH"``
    → ``"ETH"``; ``"SOL-PERP"`` → ``"SOL"``.

    Returns None for non-strings, empty strings, and address-style
    identifiers (0x...), which cannot be mapped to a priceable symbol
    without chain data.
    """
    if not isinstance(market, str):
        return None
    candidate = market.strip()
    if not candidate or candidate.lower().startswith("0x"):
        return None
    for separator in PERP_MARKET_SEPARATORS:
        if separator in candidate:
            candidate = candidate.split(separator)[0].strip()
            break
    if not candidate:
        return None
    return candidate.upper()


def perp_market_funding_key(market: object) -> str | None:
    """Canonical ``"<BASE>-USD"`` venue form for funding-rate lookups.

    ``"ETH/USD"`` / ``"ETH-USD"`` / ``"ETH"`` → ``"ETH-USD"``. Returns None
    when no base symbol is resolvable (see :func:`perp_market_base`).
    """
    base = perp_market_base(market)
    if base is None:
        return None
    return f"{base}-USD"


#: Quote-position tokens that mark a market as venue-suffixed rather than a
#: BASE/QUOTE pair. Not protocol names — these are market-shape tokens, so this
#: stays venue-agnostic and clear of the chain/protocol coupling ratchet.
_VENUE_SUFFIX_QUOTES: frozenset[str] = frozenset({"PERP", "PERPETUAL", "SWAP"})


def _has_venue_suffix(candidate: str) -> bool:
    """True when the quote position is a venue suffix, not a quote currency."""
    for source_separator in PERP_MARKET_SEPARATORS:
        if source_separator in candidate:
            _, _, quote = candidate.partition(source_separator)
            return quote.strip().upper() in _VENUE_SUFFIX_QUOTES
    return False


def perp_market_pair_key(market: object, *, separator: str = "/") -> str | None:
    """Return an upper-case pair with a caller-selected separator.

    ``"ETH/USD"`` / ``"ETH-USD"`` / ``"ETH_USD"`` / ``"ETH:USD"`` all
    become ``"ETH/USD"`` with the default separator. Bare symbols stay bare
    so connector aliases such as GMX's ``"ETH"`` / ``"WETH"`` remain valid.

    Returns ``None`` for non-strings, empty strings, malformed pairs, and raw
    address identifiers. Address pass-through is connector-owned because
    changing its case would corrupt a checksum-form identifier.
    """
    if not isinstance(market, str):
        return None
    candidate = market.strip()
    if not candidate or candidate.lower().startswith("0x"):
        return None

    for source_separator in PERP_MARKET_SEPARATORS:
        if source_separator not in candidate:
            continue
        base, quote = candidate.split(source_separator, 1)
        base = base.strip()
        quote = quote.strip()
        # A separator in EITHER component means the input was never a clean
        # pair. Checking only the quote accepted ``"eth-usd/foo"`` as
        # ``"ETH-USD/FOO"`` — a malformed string promoted to canonical, which
        # then becomes a lot-matching key. Reject both sides (CodeRabbit, #3565).
        if (
            not base
            or not quote
            or any(item in base for item in PERP_MARKET_SEPARATORS)
            or any(item in quote for item in PERP_MARKET_SEPARATORS)
        ):
            return None
        return f"{base.upper()}{separator}{quote.upper()}"

    return candidate.upper()


def perp_market_identity_key(market: object) -> str:
    """Return the market segment of a perp identity key, separator-insensitive.

    Identity keys — the accounting ``position_key`` and the observability
    ``position_id`` — are FIFO lot-matching inputs, so one on-chain position
    must produce exactly one string. GMX V2's compiler accepts ``"ETH/USD"``,
    ``"ETH-USD"``, ``"ETH_USD"`` and ``"ETH:USD"`` for a single market
    (ALM-3094); without this seam each spelling mints a distinct key and a
    close is matched against the wrong lots — wrong cost basis, wrong realized
    PnL (VIB-6412).

    **This is an opaque identity string, not an execution parameter.** It must
    be stable and collision-free; it does not need to preserve a venue's
    spelling, and nothing reads a market back out of it to send to a venue.
    Collapsing separators cannot merge two genuinely different markets: no
    venue lists ``"ETH/USD"`` and ``"ETH-USD"`` as separate products, and forms
    that differ in the QUOTE (``"SOL-PERP"`` vs ``"SOL/USD"``) stay distinct
    because only the separator is normalised.

    Venue-agnostic by construction: the exemption below keys off the QUOTE
    token (``-PERP``), never off a protocol name, so no protocol literal enters
    ``almanak/core/`` and the chain/protocol coupling ratchet stays clean.

    An earlier revision collapsed venue-suffixed markets too and justified it
    with a sweep of all 206 SQLite DBs in the repo (GMX and Hyperliquid already
    slash-form, zero Drift rows). **That argument is retired**: the repo corpus
    is not production, and shipped Drift demos default ``market="SOL-PERP"``, so
    a hosted position opened under the hyphen form would have had its close
    keyed differently — an orphaned lot. The transform is now narrowed to the
    pair-spelling collision it exists for, and every persisted form is
    byte-identical without needing corpus evidence at all.

    Known limitation: ``SOL-PERP`` and ``SOL/PERP`` mint DIFFERENT keys. That is
    unchanged from before this seam existed, and Hyperliquid accepts both
    spellings, so it is a real (pre-existing) orphan risk — pinned by test and
    ticketed, not silently accepted.

    Changing this function's output is a lot-matching contract change: it
    re-keys history and needs a ``matching_policy_version`` bump.
    """
    raw = market.strip() if isinstance(market, str) else ""
    # Venue-suffixed markets (Drift/Hyperliquid ``"SOL-PERP"``) are NOT a second
    # spelling of anything: nothing writes ``"SOL/PERP"``, so there is no
    # collision to collapse and normalising them only risks orphaning a lot
    # opened under the hyphen form. The repo corpus shows zero such rows, but a
    # hosted deployment is not the repo corpus — so narrow the transform to the
    # pair-spelling collision this seam exists for rather than argue
    # reachability (Codex P1, #3565).
    if _has_venue_suffix(raw):
        return raw.upper()
    return perp_market_pair_key(raw) or raw


__all__ = [
    "PERP_MARKET_SEPARATORS",
    "perp_market_base",
    "perp_market_funding_key",
    "perp_market_identity_key",
    "perp_market_pair_key",
]
