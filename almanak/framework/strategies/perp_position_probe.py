"""Three-valued venue-truth probe for a perps strategy's own position.

A perps strategy's ``get_open_positions()`` decides **whether** teardown emits a
close. Strategies have historically answered that question from their own cached
bookkeeping — a ``_position_side`` / ``_position_state`` / ``_cumulative_size_usd``
field written in ``on_intent_executed`` from the *requested* size. Cache and venue
diverge on paths this SDK already documents:

* an async keeper fill that reverts or is cancelled (ALM-3109, VIB-6160);
* a crash between the fill and the state save (VIB-6159);
* a close whose *submission* succeeded without settling (VIB-6497).

When they diverge the user loses access to their own position: teardown
enumerates from that answer, so a position the cache does not know about is never
closed, and a position the cache invented is closed twice or reported as a
phantom residual.

:meth:`~almanak.framework.market.snapshot.MarketSnapshot.perp_positions`
(ALM-3101) is a real venue read. This module turns it into the three-valued
answer a teardown decision actually needs:

===============  =====================================================
``OPEN``         the venue holds a matching position (measured, positive)
``FLAT``         the venue holds none (measured, negative)
``UNMEASURED``   the read did not run, or cannot support a negative claim
===============  =====================================================

**Empty ≠ Zero is the whole reason the third value exists.** Collapsing
``UNMEASURED`` into ``FLAT`` is the false certification recorded in VIB-6497 — a
teardown that closed nothing, reported success, and exited 0 over a live,
collateral-consuming position. Callers must read ``UNMEASURED`` as "keep whatever
the strategy already believed", never as "flat".

Asserting ``FLAT`` is therefore deliberately harder than asserting ``OPEN``:

* ``OPEN`` needs one positively-matched live position. A partial read still
  supports it — presence is never weakened by not having seen everything.
* ``FLAT`` needs the read to have run (``ok``), to be complete
  (``not truncated``), to have identified every position it returned, and to
  cover this strategy's market at all. A per-market venue that never planned a
  call for our market (Hyperliquid reads a seeded symbol set) returns an empty
  book that says nothing about us; treating that as flat would strand the
  position the probe exists to find.

Scope: the probe answers *whether the venue holds a position on this market*, and
what it is currently worth. It does not size a close — a full close carries
``size_usd=None`` and the compiler resolves the live size at compile time
(VIB-5465 / VIB-5950), the half of this problem that was already correct.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)

__all__ = [
    "PerpProbe",
    "PerpProbePosition",
    "PerpProbeState",
    "probe_perp_position",
]

# Only ``markets`` is inspected on the resolved plan — never executed, never sent
# anywhere — so the wallet is a structural placeholder. ``resolve_plan`` is pure
# (no egress): it resolves addresses and asks the connector to PLAN calls.
_PLACEHOLDER_WALLET = "0x" + "0" * 40

# Market-symbol separators, in the venue vocabularies that reach this seam:
# GMX "ETH/USD", an Aster-style "ETH-USD", Hyperliquid's bare "ETH".
_MARKET_SEPARATORS = ("/", "-", ":", "_")


class PerpProbeState(str, Enum):  # noqa: UP042 - preserve serialized enum compatibility
    """Measured venue state for one strategy's perp market."""

    OPEN = "OPEN"
    FLAT = "FLAT"
    UNMEASURED = "UNMEASURED"


@dataclass(frozen=True)
class PerpProbePosition:
    """One live venue position matched to the strategy's market.

    ``is_long`` comes from the VENUE, not from what the strategy requested — that
    is what lets a probe report a position whose side the cache never recorded.

    ``notional_usd`` is ``None`` when the size was read but could not be priced.
    ``None`` is not zero: a caller emitting a ``PositionInfo`` must pair a
    ``Decimal("0")`` placeholder with the ``value_usd_unknown`` /
    ``valuation_status`` detail markers rather than publish a measured $0.
    """

    is_long: bool
    market: str
    collateral_token: str
    notional_usd: Decimal | None


@dataclass(frozen=True)
class PerpProbe:
    """Outcome of one venue probe. ``reason`` is a short diagnostic for logs."""

    state: PerpProbeState
    positions: tuple[PerpProbePosition, ...] = ()
    reason: str = ""

    @property
    def is_open(self) -> bool:
        """The venue positively holds at least one matching position."""
        return self.state is PerpProbeState.OPEN

    @property
    def is_flat(self) -> bool:
        """The venue was measured and holds no matching position."""
        return self.state is PerpProbeState.FLAT

    @property
    def is_measured(self) -> bool:
        """The probe supports a claim in EITHER direction (``OPEN`` or ``FLAT``)."""
        return self.state is not PerpProbeState.UNMEASURED


def probe_perp_position(
    market: Any,
    *,
    protocol: str,
    chain: str,
    market_symbol: str,
    index_token_address: str | None = None,
    is_long: bool | None = None,
) -> PerpProbe:
    """Read the venue and classify this strategy's exposure on ``market_symbol``.

    Args:
        market: A ``MarketSnapshot`` (or ``None`` — an absent snapshot is
            ``UNMEASURED``, never flat).
        protocol: Connector slug (``"gmx_v2"``, ``"hyperliquid"``, ...).
        chain: Chain the strategy trades on.
        market_symbol: The strategy's own market key (``"ETH/USD"``, ``"ETH"``).
            Matched against the venue's market key directly and, failing that,
            through the connector's index-token metadata, so an address-keyed
            venue and a symbol-keyed venue resolve through the same call.
        index_token_address: Chain-specific token address used to mark the
            position. It is required for EVM venues. The one supported
            addressless venue uses the snapshot's provenance-checked,
            market-scoped perp mark and never a symbol token-price lookup.
        is_long: Restrict the match to one side. Leave ``None`` (the teardown
            default) to let the venue tell you the side — the cache's idea of the
            side is exactly what may be missing.

    Returns:
        A :class:`PerpProbe`. Never raises: a probe that cannot answer returns
        ``UNMEASURED``, because a raised exception in ``get_open_positions()``
        would fail the enumeration for every other position too.
    """
    if market is None:
        return PerpProbe(PerpProbeState.UNMEASURED, reason="no_market_snapshot")

    try:
        result = market.perp_positions(protocol, chain=chain)
    except Exception as exc:  # noqa: BLE001 — an unreadable venue is UNMEASURED, not flat
        logger.warning(
            "perp probe: venue read raised for %s on %s (%s) — treating as UNMEASURED, not flat",
            protocol,
            chain,
            exc,
        )
        return PerpProbe(PerpProbeState.UNMEASURED, reason=f"read_raised:{type(exc).__name__}")

    if not getattr(result, "ok", False):
        return PerpProbe(PerpProbeState.UNMEASURED, reason="read_unavailable")

    matched: list[PerpProbePosition] = []
    saw_unidentified = False
    for position in getattr(result, "positions", ()) or ():
        if not getattr(position, "is_active", False):
            continue
        verdict = _market_matches(position.market, market_symbol, protocol=protocol, chain=chain)
        if verdict is None:
            # A live position we could not name. It may be ours. Absence of a
            # match is therefore not evidence of absence.
            saw_unidentified = True
            continue
        if not verdict:
            continue
        if is_long is not None and bool(position.is_long) is not is_long:
            continue
        matched.append(
            PerpProbePosition(
                is_long=bool(position.is_long),
                market=str(position.market),
                collateral_token=str(position.collateral_token),
                notional_usd=_notional_usd(
                    market,
                    position,
                    protocol=protocol,
                    chain=chain,
                    index_token_address=index_token_address,
                ),
            )
        )

    if matched:
        # Positive evidence: a truncated or partially-identified read still proves
        # the position exists.
        return PerpProbe(PerpProbeState.OPEN, tuple(matched), reason="venue_position_found")

    # Everything below is a NEGATIVE claim, and each guard is a way the empty
    # match set fails to support one.
    if getattr(result, "truncated", False):
        return PerpProbe(PerpProbeState.UNMEASURED, reason="read_truncated")
    if saw_unidentified:
        return PerpProbe(PerpProbeState.UNMEASURED, reason="unidentified_venue_position")
    if not _read_covers_market(protocol=protocol, chain=chain, market_symbol=market_symbol):
        return PerpProbe(PerpProbeState.UNMEASURED, reason="market_outside_read_universe")
    return PerpProbe(PerpProbeState.FLAT, reason="venue_measured_flat")


def _index_symbol(market_symbol: str) -> str:
    """Base asset of a market key: ``"ETH/USD"`` → ``"ETH"``, ``"ETH"`` → ``"ETH"``."""
    symbol = (market_symbol or "").strip()
    for separator in _MARKET_SEPARATORS:
        symbol = symbol.split(separator, 1)[0]
    return symbol.upper()


def _market_matches(
    venue_market: str,
    market_symbol: str,
    *,
    protocol: str,
    chain: str,
) -> bool | None:
    """Is ``venue_market`` the strategy's market? ``None`` = could not be named.

    ``None`` is load-bearing: an unnameable live position must not be silently
    read as "not ours", because that turns an unknown into a negative.
    """
    venue_key = (venue_market or "").strip().lower()
    if not venue_key:
        return None
    if venue_key == (market_symbol or "").strip().lower():
        return True
    try:
        from almanak.connectors._strategy_base.perps_read_registry import PerpsReadRegistry

        meta = PerpsReadRegistry.market_metadata(protocol, venue_market, chain)
    except Exception:  # noqa: BLE001 — a broken connector must not assert a negative
        logger.debug("perp probe: market_metadata raised for %s/%s", protocol, venue_market, exc_info=True)
        return None
    if meta is None or not meta.index_token_symbol:
        return None
    return meta.index_token_symbol.strip().upper() == _index_symbol(market_symbol)


def _read_covers_market(*, protocol: str, chain: str, market_symbol: str) -> bool:
    """Can this venue's read see ``market_symbol`` at all?

    Per-market venues plan one call per market in a resolved universe
    (Hyperliquid's seeded symbol set). A market outside that universe is never
    read, so the returned book is silent about it and an empty result must not be
    reported as flat. Range-read venues (GMX returns the whole account book in
    one call) have an empty ``markets`` tuple and always cover it.

    Fails CLOSED: anything unresolvable returns ``False``, which downgrades a
    would-be ``FLAT`` to ``UNMEASURED``.
    """
    try:
        from almanak.connectors._strategy_base.perps_read_base import PerpsPositionQuery
        from almanak.connectors._strategy_base.perps_read_registry import PerpsReadRegistry

        plan = PerpsReadRegistry.resolve_plan(
            protocol,
            PerpsPositionQuery(chain=chain, wallet_address=_PLACEHOLDER_WALLET),
        )
    except Exception:  # noqa: BLE001 — unresolvable ⇒ cannot assert coverage
        logger.debug("perp probe: resolve_plan raised for %s on %s", protocol, chain, exc_info=True)
        return False
    if plan is None:
        return False
    planned_markets = tuple(getattr(plan.query, "markets", ()) or ())
    if not planned_markets:
        return True  # range read: the whole account book, so our market is in it
    wanted = _index_symbol(market_symbol)
    return any(_index_symbol(planned) == wanted for planned in planned_markets)


def _notional_usd(
    market: Any,
    position: Any,
    *,
    protocol: str,
    chain: str,
    index_token_address: str | None,
) -> Decimal | None:
    """Mark-value the position's notional, or ``None`` when it cannot be priced.

    ``size_in_tokens`` is raw in the venue's own index-token decimals, so the
    conversion goes through the connector's own metadata rather than a divisor
    guessed here (GMX carries 30-decimal USD, Hyperliquid 1e6, Aster 1e8 — a
    hardcoded divisor would be wrong on two of the three).

    ``None`` — not ``Decimal("0")`` — is the unmeasured answer. A fabricated zero
    is dropped as dust by ``_measure_open_positions_after_teardown`` (≤ $0.01) and
    buys the loosest slippage tier from the position-aware loss cap.
    """
    try:
        from almanak.connectors._strategy_base.perps_read_registry import PerpsReadRegistry

        meta = PerpsReadRegistry.market_metadata(protocol, position.market, chain)
        if meta is None or meta.index_token_decimals is None or meta.index_token_decimals < 0:
            return None
        tokens = Decimal(int(position.size_in_tokens)) / Decimal(10 ** int(meta.index_token_decimals))
        if tokens <= 0:
            return None
        if index_token_address is not None:
            price = Decimal(str(market.price(index_token_address, chain=chain)))
        elif not PerpsReadRegistry.requires_index_token_address(protocol):
            price = Decimal(str(market.perp_mark_price(protocol, position.market, chain=chain)))
        else:
            return None
    except Exception:  # noqa: BLE001 — unpriceable ⇒ unmeasured, never a measured $0
        logger.debug("perp probe: notional valuation failed for %s", protocol, exc_info=True)
        return None
    if price <= 0:
        return None
    notional = tokens * price
    return notional if notional > 0 else None
