"""Lending NAV helper (W1-1, VIB-4776).

Pure read-side aggregation of lending-related positions from a PortfolioSnapshot.
No writes. No persisted semantics are changed. This is additive reporting state.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from almanak.framework.portfolio.models import PortfolioSnapshot


@dataclass(frozen=True)
class LendingNAVSummary:
    """Aggregated lending NAV computed from a PortfolioSnapshot."""

    gross_supply_value_usd: Decimal  # Σ positive value_usd for SUPPLY positions
    gross_debt_value_usd: Decimal  # Σ abs(value_usd) for BORROW positions
    net_lending_value_usd: Decimal  # gross_supply - gross_debt
    supply_unrealized_pnl_usd: Decimal  # Σ unrealized_pnl_usd for SUPPLY positions where non-zero
    borrow_unrealized_pnl_usd: Decimal  # Σ unrealized_pnl_usd for BORROW positions where non-zero
    net_unrealized_carry_usd: Decimal  # supply_unrealized + borrow_unrealized (borrow already signed negative)
    supply_positions: int
    borrow_positions: int


_ZERO = Decimal("0")


def compute_lending_nav(snapshot: PortfolioSnapshot | None) -> LendingNAVSummary:
    """Compute lending NAV summary from a PortfolioSnapshot.

    Aggregates SUPPLY and BORROW positions, netting liabilities against
    collateral. Non-lending position types (LP, TOKEN, etc.) are ignored.

    Args:
        snapshot: The portfolio snapshot to aggregate, or None for an empty
            strategy that has not yet opened any positions.

    Returns:
        A frozen LendingNAVSummary. Returns all-zeros when snapshot is None or
        contains no lending positions.

    Notes:
        - ``value_usd`` for BORROW positions is signed negative by convention
          (on-chain variable debt token balance reported as a liability).
          ``gross_debt_value_usd`` stores ``abs(value_usd)`` so callers can
          express the net as ``gross_supply - gross_debt``.
        - ``unrealized_pnl_usd`` defaults to ``Decimal("0")`` on PositionValue,
          meaning "not measured / not populated" when zero.  The helper sums
          the field as-is; callers should not infer per-position carry from
          a zero value alone.
        - Unknown or raise-raising ``position_type`` values are silently
          skipped so legacy data shapes cannot crash the reporter.
    """
    if snapshot is None:
        return _zero_summary()

    from almanak.framework.teardown.models import PositionType

    gross_supply = _ZERO
    gross_debt = _ZERO
    supply_unrealized = _ZERO
    borrow_unrealized = _ZERO
    n_supply = 0
    n_borrow = 0

    for pos in snapshot.positions:
        try:
            ptype = pos.position_type
        except Exception:  # pragma: no cover
            continue

        try:
            if ptype == PositionType.SUPPLY:
                n_supply += 1
                # value_usd is always Decimal on PositionValue (non-optional).
                # Treat None defensively for legacy payloads that may have been
                # reconstructed outside the canonical from_dict path.
                v = pos.value_usd
                if v is not None:
                    gross_supply += v
                supply_unrealized += pos.unrealized_pnl_usd

            elif ptype == PositionType.BORROW:
                n_borrow += 1
                v = pos.value_usd
                if v is not None:
                    # BORROW value_usd is signed negative (liability); store abs.
                    gross_debt += abs(v)
                borrow_unrealized += pos.unrealized_pnl_usd

        except Exception:  # pragma: no cover
            # Tolerate unexpected shape on any individual position.
            continue

    net = gross_supply - gross_debt
    net_carry = supply_unrealized + borrow_unrealized

    return LendingNAVSummary(
        gross_supply_value_usd=gross_supply,
        gross_debt_value_usd=gross_debt,
        net_lending_value_usd=net,
        supply_unrealized_pnl_usd=supply_unrealized,
        borrow_unrealized_pnl_usd=borrow_unrealized,
        net_unrealized_carry_usd=net_carry,
        supply_positions=n_supply,
        borrow_positions=n_borrow,
    )


def _zero_summary() -> LendingNAVSummary:
    return LendingNAVSummary(
        gross_supply_value_usd=_ZERO,
        gross_debt_value_usd=_ZERO,
        net_lending_value_usd=_ZERO,
        supply_unrealized_pnl_usd=_ZERO,
        borrow_unrealized_pnl_usd=_ZERO,
        net_unrealized_carry_usd=_ZERO,
        supply_positions=0,
        borrow_positions=0,
    )
