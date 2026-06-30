"""Shared money-leg construction for lending connectors (VIB-5218).

Workstream B1 of the June-27 demo-E2E plan. Lending intents (SUPPLY / WITHDRAW /
BORROW / REPAY) execute on-chain but historically wrote **null** money legs:
their ledger row fell through to the legacy intent-attribute guesser
(``observability/ledger.py``), which left ``token_in`` empty. An empty
``token_in`` makes the lending accounting handler resolve ``asset = "UNKNOWN"``
→ ``amount_human = None`` → ``principal_delta_usd = None`` → **no FIFO supply
lot → ``deployed_capital_usd = 0``** and no cost basis (the books don't tie).

The fix is the same control-flow inversion the Lido / TraderJoe / Curve / Pendle
connectors already shipped: the connector **declares** its money legs as a typed
:class:`~almanak.connectors._strategy_base.primitive_money_leg.PrimitiveMoneyLegs`
via ``extract_primitive_money_legs``, and the US-009 ledger dispatcher
(``_extract_tokens_and_amounts``) prefers that typed fact over its guesser. A
lending action moves a single notional, so it declares **one PRINCIPAL leg** —
which the dispatcher projects onto ``token_in`` / ``amount_in`` (blueprint 27
§6.6).

This module is the shared seam the three lending connectors (euler_v2, spark,
morpho_blue) call so the leg-building discipline (token-symbol resolution +
Empty≠Zero human scaling) lives in **one** place rather than being re-derived per
connector. The per-connector work is reduced to *sourcing the token identity* for
its receipt shape (Spark: the ``reserve`` in the event; Euler: the vault→underlying
map; Morpho: the matching ERC-20 ``Transfer`` leg), then handing it here.

Empty ≠ Zero (blueprint 27 §10.10): amounts are :class:`MeasuredMoney`. A measured
on-chain amount with known decimals is ``measured``; a known token whose decimals
cannot be resolved is ``unmeasured`` (never a wrongly-scaled value); an unknown
token or unparseable amount yields ``None`` so the dispatcher falls back to its
legacy path unchanged — the fix only activates where it actually helps and never
regresses a row it cannot improve.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLegs

logger = logging.getLogger(__name__)


def resolve_symbol_decimals(token_address: str | None, chain: str | None) -> tuple[str, int | None]:
    """Resolve an ERC-20 address to its ``(symbol, decimals)`` for a money leg.

    Uses the static token resolver (``skip_gateway=True`` / ``log_errors=False``)
    — this runs on the accounting write hot path, so it must never risk a gateway
    round-trip stall (mirrors the TraderJoe V2 / ledger ``_lp_amount_to_human``
    discipline). A failure to resolve degrades to ``("", None)`` rather than
    raising on the accounting path.

    Returns:
        ``(symbol, decimals)`` — symbol is ``""`` when unresolved (Empty ≠ Zero:
        an unknown token is the empty string, never a fabricated one); decimals is
        ``None`` when unknown.
    """
    if not token_address or not chain:
        return "", None
    try:
        from almanak.framework.data.tokens.resolver import get_token_resolver

        resolver = get_token_resolver()
        info = resolver.resolve(token_address, chain, log_errors=False, skip_gateway=True)
        symbol = getattr(info, "symbol", "") or ""
        decimals = getattr(info, "decimals", None)
        return symbol, (decimals if isinstance(decimals, int) else None)
    except Exception as exc:  # noqa: BLE001 — fail to unresolved, never raise on the accounting path
        logger.debug("lending leg: token resolve failed for %s on %s: %s", token_address, chain, exc)
        return "", None


def lending_principal_legs(
    *,
    token_symbol: str,
    raw_amount: object,
    decimals: int | None,
) -> PrimitiveMoneyLegs | None:
    """Build the single-PRINCIPAL-leg declaration for a lending action.

    The lending family (SUPPLY / WITHDRAW / BORROW / REPAY) operates on one
    notional, so it declares exactly one PRINCIPAL leg — the dispatcher projects
    it onto ``token_in`` / ``amount_in`` (blueprint 27 §6.6). Token identity is the
    chain-truth ERC-20 **symbol** (not an address) so the lending handler's
    price-oracle lookup (``_amount_to_usd``, symbol-keyed) can value the principal
    into ``deployed_capital_usd``.

    Args:
        token_symbol: Resolved ERC-20 symbol of the principal asset. ``""`` →
            return ``None`` (an unknown token cannot help; let the dispatcher fall
            back to its legacy guesser rather than declare a token-less leg that
            re-creates the ``UNKNOWN`` bug via a different path).
        raw_amount: On-chain integer amount (raw token units). ``None`` / not an
            integer → return ``None`` (legacy fallback).
        decimals: Token decimals for human scaling. ``None`` → the leg amount is
            ``unmeasured`` (token known, amount not scalable) — never a wrongly
            scaled or fabricated value. The handler still scales the raw amount
            independently from ``extracted_data`` via the token resolver, so a
            known symbol alone is enough to populate cost basis.

    Returns:
        A :class:`PrimitiveMoneyLegs` with one PRINCIPAL leg, or ``None`` when the
        token symbol or amount is unknown.
    """
    from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLeg, PrimitiveMoneyLegs
    from almanak.framework.accounting.measured import MeasuredMoney

    if not token_symbol:
        return None

    if isinstance(raw_amount, bool) or not isinstance(raw_amount, int | str):
        # Only integral raw token units (or a string thereof) can be scaled.
        # ``bool`` is an ``int`` subclass but never a raw amount, and floats /
        # Decimals would be silently truncated by ``int()`` — both must fall
        # through to the dispatcher's legacy guesser rather than fabricate a
        # wrong amount (docstring contract: non-integer → ``None``).
        return None
    try:
        raw_int: int | None = int(raw_amount)
    except (TypeError, ValueError):
        raw_int = None
    if raw_int is None:
        return None

    if isinstance(decimals, int) and decimals >= 0:
        amount = MeasuredMoney.measured(Decimal(raw_int) / Decimal(10**decimals))
    else:
        # Token identity is known but its decimals are not — declare the token so
        # the ledger ``token_in`` is populated, with an UNMEASURED amount rather
        # than a wrongly-scaled one (Empty ≠ Zero).
        amount = MeasuredMoney.unmeasured()

    return PrimitiveMoneyLegs.of(PrimitiveMoneyLeg.principal(token_symbol, amount))


__all__ = ["lending_principal_legs", "resolve_symbol_decimals"]
