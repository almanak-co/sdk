"""Canonical pool identity for fungible-LP closes (VIB-6162).

A fungible-LP close must be bounded to the strategy's own outstanding liquidity, which
means matching the ``position_events`` rows this deployment wrote against the identifier
the close intent carries. Those two sides are frequently written in different forms by
the *same* strategy, so a raw string comparison silently fails to match and the clamp
never engages:

* ``strategies/incubating/aerodrome_aave_carry_base`` reports
  ``aerodrome-lp-{pool}-{chain}`` in its teardown summary (``:416``) and closes with
  ``{pool}/volatile`` (``:267``).
* ``strategies/accounting/lp_curve`` reports ``curve_3pool_{lp_token}`` (``:241``) and
  closes with the bare ``{lp_token}`` (``:158``).
* Token order is not stable either: the Aerodrome stable pool's own ``symbol()`` is
  ``sAMM-DAI/USDC`` while the shipped strategy config names the same pool ``USDC/DAI``.

The fix is canonicalization, **not** address resolution. Resolving a symbolic pool spec
to a pool address would need a live chain read, and that read is exactly what broke the
first attempt at this ticket: ``market.balance()`` resolves through the token registry,
a Solidly-fork pool is not a registered token, and the lookup therefore raised
deterministically — the clamp fail-closed-skipped 100% of closes and stranded every
position. Canonicalizing both sides to a comparable key is a pure function, needs no
network, and cannot fail that way.

**Unresolvable returns ``None``, and ``None`` means refuse.** It never means "no limit
applies". A caller that treats an unresolved identifier as permission to close unbounded
reproduces the defect this module exists to prevent.
"""

from __future__ import annotations

import re

#: An EVM address, the form used for a pool (Aerodrome), an LP token (Curve) or a
#: wrapper (Fluid). Which contract it denotes is connector knowledge; that it is an
#: address, and therefore an identifier rather than a quantity, is not.
_ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")

#: A bare decimal. Curve accepts this as an *amount to withdraw*, not a name -- see
#: ``strategies/accounting/lp_curve:150-158``. Treating it as an identifier would let a
#: partial-withdrawal request be matched against position history and silently widened.
_DECIMAL_RE = re.compile(r"^\d+(\.\d+)?$")

#: Prefixes strategies attach when reporting a position in a teardown summary. Stripped
#: so the summary lane and the close lane canonicalize to the same key.
_KNOWN_PREFIXES: tuple[str, ...] = ("aerodrome-lp-", "aerodrome-stable-", "curve_3pool_", "curve_tricrypto2_")

#: Solidly pool-type markers. These are part of the pool's identity (a stable and a
#: volatile pool over the same pair are different pools) and must survive sorting, so
#: they are held aside while the token symbols are ordered.
_POOL_TYPE_MARKERS: frozenset[str] = frozenset({"stable", "volatile"})


def canonical_pool_key(position_id: object, pool: object = None, *, chain: object = None) -> str | None:
    """Return a stable key for a fungible-LP position, or ``None`` if unresolvable.

    ``position_id`` is whatever the close intent (or the stored position event) carries.
    ``pool`` is the intent's pool field, consulted only when ``position_id`` alone does
    not yield a key -- a strategy may pass an opaque id and a meaningful pool, or the
    reverse. ``chain`` is accepted so a caller can pass it through uniformly; it is not
    part of the key, because the framework already scopes its history query by
    deployment and a deployment does not span chains.

    Returns ``None`` for an empty value, and for a bare decimal, which is an amount
    rather than a name. ``None`` obliges the caller to refuse the close.
    """
    for candidate in (position_id, pool):
        key = _canonicalize_one(candidate)
        if key is not None:
            return key
    return None


def _canonicalize_one(value: object) -> str | None:
    """Canonicalize a single candidate identifier, or return ``None``."""
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None

    # An amount, not an identifier. Checked BEFORE prefix stripping so a numeric value
    # can never be reshaped into something that looks like a name.
    if _DECIMAL_RE.match(text):
        return None

    for prefix in _KNOWN_PREFIXES:
        if text.startswith(prefix):
            text = text[len(prefix) :]
            break

    # A trailing "-{chain}" suffix appears on summary-lane ids (``aerodrome-lp-X-base``).
    # Only stripped when what remains is still non-empty and not a bare address, so a
    # legitimate hyphenated pool name is not truncated.
    if _ADDRESS_RE.match(text.split("-")[0]) and "-" in text:
        text = text.split("-")[0]

    if _ADDRESS_RE.match(text):
        return text

    if "/" in text:
        return _canonicalize_symbolic(text)

    # A non-empty opaque token (e.g. a wrapper alias). Usable as a key precisely because
    # both sides go through this same function; it is stable, just not interpretable.
    return text or None


def _canonicalize_symbolic(text: str) -> str | None:
    """Canonicalize ``TOKEN0/TOKEN1[/pool_type]`` with a stable token order.

    Token order is not stable across the codebase -- the Aerodrome stable pool's
    ``symbol()`` says ``DAI/USDC`` where the strategy config says ``USDC/DAI`` -- so the
    symbols are sorted while any pool-type marker is held aside and re-appended. Without
    this, one ordering matches stored history and the other does not, and the clamp is
    inert for exactly half the callers while looking entirely healthy for the other half.
    """
    parts = [p.strip() for p in text.split("/") if p.strip()]
    if len(parts) < 2:
        return None
    markers = [p for p in parts if p in _POOL_TYPE_MARKERS]
    symbols = sorted(p for p in parts if p not in _POOL_TYPE_MARKERS)
    if not symbols:
        return None
    # A numeric tail is a fee tier (``WETH/USDC/100``). It is part of identity and is
    # sorted along with the symbols rather than held aside -- the resulting key is not
    # human-readable, but both sides pass through this same function, and stability is
    # what matching needs. Do not "fix" the readability by special-casing the tier
    # unless both producers are changed together.
    return "/".join(symbols + sorted(markers))


__all__ = ["canonical_pool_key"]
