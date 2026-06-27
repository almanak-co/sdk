"""Oracle-vs-pool peg-divergence cross-check (VIB-5426 / Curve audit P0-2).

A pegged-pool LP is canonically marked from the pool's own invariant
(``virtual_price`` for Curve StableSwap) times an assumed numeraire peg. That
invariant is **depeg-insensitive**: it only grows as the pool accrues fees, so a
pool whose coin has *lost its peg* keeps marking at par while real value bleeds —
exactly when risk controls must fire (audit P0-2, "the single highest real-money
risk"). The fix is to cross-check the pool's assumed peg against an INDEPENDENT
price oracle and degrade to UNAVAILABLE on divergence rather than mark at par.

This module is the **pool-agnostic** core of that check: a pure value function
over a list of independently-oracle-priced coin prices. It holds no Curve
knowledge, opens no sockets, and reads no oracle itself — the caller supplies the
already-priced coins (sourced gateway-side, never by this module). Any pegged
pool — Curve StableSwap, a Balancer stable pool, a future StableSwap-NG fork —
calls the same :func:`check_peg_divergence`. There is no per-pool branch here.

Divergence is each coin's relative distance from a single **reference peg**, so
the signal is stable regardless of how many coins the pool has (a 2-coin and a
3-coin pool with the same off-peg coin report the same divergence — a
deviation-from-median metric would not, because the median sits between two
coins and halves the signal):

* When the caller knows the **expected** numeraire (a USD-stable pool expects
  ``$1``), the reference is that expected peg, so ``max|pᵢ − expected| /
  expected`` catches both a single coin drifting (``USDT`` at ``$0.90``) and a
  *systemic* drift where every coin falls together (all at ``$0.90`` is invisible
  to a coin-vs-coin spread, but ``$0.90`` vs the expected ``$1`` is 1000 bps).
* When the expected numeraire is unknown (a stETH/ETH pool with no passed peg),
  the reference falls back to the **discovered** numeraire — the median of the
  coins' oracle prices — so coin-vs-consensus drift is still caught with zero
  ``$1`` special-casing.

``peg_usd`` always reports the *discovered* median (what the oracle says one
unit is worth) for diagnostics, independent of which reference the divergence
used.

Empty ≠ Zero: a missing / non-positive oracle price is *unmeasured*, reported
distinctly from a real depeg so an oracle outage is never mis-blamed as a peg
break — the caller degrades to UNAVAILABLE in both cases, but with the honest
reason.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from statistics import median

# A USD-pegged stablecoin more than 1% off the pool's own implied peg is a depeg
# signal, not oracle noise: healthy USDC/DAI/USDT oracle marks sit within a few
# tens of bps of each other, while the depegs that matter (USDC SVB ≈ 1200 bps,
# UST collapse) blow far past 1%. Conservative by design — too tight makes a
# healthy pool's NAV flap to UNAVAILABLE (itself a risk-control failure), so the
# default leaves headroom and is overridable per-intent / per-pool by the caller.
DEFAULT_DEPEG_THRESHOLD_BPS = 100

_BPS = Decimal(10_000)


@dataclass(frozen=True)
class PegCheck:
    """Result of a peg-divergence cross-check.

    ``ok`` is the only field a caller must branch on; the rest are diagnostics to
    stamp on the (degraded) valuation row so an operator can see WHY a position
    went UNAVAILABLE and by how much.
    """

    ok: bool
    # The DISCOVERED numeraire (median of the coins' oracle prices) — ``None``
    # only when no peg could be discovered (oracle miss). NOT a hardcoded $1.
    peg_usd: Decimal | None
    max_divergence_bps: int
    # ``None`` when ok; otherwise ``"depeg_divergence"`` (a real peg break) or
    # ``"oracle_unmeasured"`` (could not price — Empty ≠ Zero, distinct cause).
    reason: str | None


def check_peg_divergence(
    coin_prices_usd: list[Decimal | None],
    *,
    threshold_bps: int = DEFAULT_DEPEG_THRESHOLD_BPS,
    expected_peg_usd: Decimal | None = None,
) -> PegCheck:
    """Cross-check independently-oracle-priced pool coins for a peg break.

    Args:
        coin_prices_usd: one entry per pool coin — the coin's USD price from an
            INDEPENDENT oracle, or ``None`` when it could not be priced. A
            non-positive price (``<= 0``) is treated as ``None`` (Empty ≠ Zero):
            a real coin is never worth ``<= $0``, so it is an oracle miss.
        threshold_bps: divergence (in basis points) above which the pool is
            ruled depegged. Caller resolves precedence (intent > pool > default).
        expected_peg_usd: the numeraire the pool is *supposed* to track (``$1``
            for a USD-stable pool). When given, a systemic drift of the whole
            pool off this peg is also caught. Omit (``None``) when the expected
            numeraire is unknown — only inter-coin divergence is then checked.

    Returns:
        A :class:`PegCheck`. ``ok=False`` with ``reason="oracle_unmeasured"`` if
        any coin is unpriced; ``ok=False`` with ``reason="depeg_divergence"`` if
        divergence exceeds the threshold; ``ok=True`` otherwise.
    """
    # Empty ≠ Zero: any unmeasured / non-positive coin makes the whole check
    # unmeasured. Distinct reason so an oracle outage is not reported as a depeg.
    if not coin_prices_usd or any(p is None or p <= 0 for p in coin_prices_usd):
        return PegCheck(ok=False, peg_usd=None, max_divergence_bps=0, reason="oracle_unmeasured")

    prices: list[Decimal] = [Decimal(p) for p in coin_prices_usd]  # type: ignore[arg-type]
    # The discovered numeraire (what the oracle says one unit is worth) — always
    # reported for diagnostics, regardless of the reference used below.
    discovered = Decimal(median(prices))
    if discovered <= 0:  # defensive — median of positive values is positive
        return PegCheck(ok=False, peg_usd=None, max_divergence_bps=0, reason="oracle_unmeasured")

    # Reference peg: the EXPECTED numeraire when the caller knows it (catches a
    # systemic all-coins drift directly — every coin at $0.90 is 1000 bps off the
    # expected $1), else the discovered median (coin-vs-consensus, for an unknown
    # numeraire). Single reference ⇒ size-stable divergence.
    reference = expected_peg_usd if (expected_peg_usd is not None and expected_peg_usd > 0) else discovered
    max_div_bps = int(max(abs(p - reference) / reference for p in prices) * _BPS)
    if max_div_bps > threshold_bps:
        return PegCheck(ok=False, peg_usd=discovered, max_divergence_bps=max_div_bps, reason="depeg_divergence")
    return PegCheck(ok=True, peg_usd=discovered, max_divergence_bps=max_div_bps, reason=None)
