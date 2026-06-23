"""PT inventory dashboard rendering hop (VIB-5317).

FIFO-derived held-PT inventory is NOT a position-registry position (VIB-4931
removed the PENDLE_PT PositionType); it only ever lives on
``snapshot.positions`` and reaches the proto as ``StrategyPosition`` rows
(tagged ``details["source"] == "pt_inventory_lots"``). These tests cover the
dashboard adapter hop that turns those proto rows into the
``PositionSummary.pt_inventory`` rows the Streamlit detail page renders.

Empty ≠ Zero (VIB-5316): an UNMEASURED PT must carry ``value_usd is None`` (the
renderer shows "—"), never a fabricated ``Decimal("0")``.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.dashboard.data_source import _extract_pt_inventory
from almanak.framework.dashboard.gateway_client import StrategyPosition


def _measured_pt() -> StrategyPosition:
    return StrategyPosition(
        position_type="TOKEN",
        position_id="PT-wstETH-26DEC2024",
        chain="arbitrum",
        protocol="pt",
        value_usd=Decimal("1050.50"),
        unrealized_pnl_usd=Decimal("50.50"),
        details={
            "source": "pt_inventory_lots",
            "pt_symbol": "PT-wstETH-26DEC2024",
            "quantity": "10.5",
            "days_to_maturity": "42",
            "sy_cost": "9.8",
            "price_confidence": "HIGH",
        },
    )


def _unmeasured_pt() -> StrategyPosition:
    # The gateway adapter maps a blank proto value_usd → None (Empty ≠ Zero).
    return StrategyPosition(
        position_type="TOKEN",
        position_id="PT-eETH-26JUN2025",
        chain="arbitrum",
        protocol="pt",
        value_usd=None,
        unrealized_pnl_usd=None,
        details={
            "source": "pt_inventory_lots",
            "pt_symbol": "PT-eETH-26JUN2025",
            "quantity": "3.0",
            "sy_cost": "2.9",
            "price_confidence": "UNAVAILABLE",
            "mark_unmeasured": "true",
        },
    )


def test_extract_measured_pt_inventory():
    rows = _extract_pt_inventory([_measured_pt()])

    assert len(rows) == 1
    row = rows[0]
    assert row.symbol == "PT-wstETH-26DEC2024"
    assert row.quantity == "10.5"
    assert row.value_usd == Decimal("1050.50")
    assert row.unrealized_pnl_usd == Decimal("50.50")
    assert row.days_to_maturity == "42"
    assert row.confidence == "HIGH"
    assert row.sy_cost == "9.8"
    assert row.unmeasured is False


def test_extract_unmeasured_pt_has_no_fabricated_usd():
    rows = _extract_pt_inventory([_unmeasured_pt()])

    assert len(rows) == 1
    row = rows[0]
    # CRITICAL: unmeasured mark stays None, never Decimal("0").
    assert row.value_usd is None
    assert row.unrealized_pnl_usd is None
    assert row.unmeasured is True
    # Qty + SY cost + UNAVAILABLE badge still surface.
    assert row.quantity == "3.0"
    assert row.sy_cost == "2.9"
    assert row.confidence == "UNAVAILABLE"


def test_non_pt_strategy_positions_are_ignored():
    perp = StrategyPosition(
        position_type="PERP",
        position_id="ETH-PERP",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("500"),
        details={},
    )
    rows = _extract_pt_inventory([perp, _measured_pt()])

    # Only the PT row is extracted.
    assert [r.symbol for r in rows] == ["PT-wstETH-26DEC2024"]


def test_empty_strategy_positions_yields_empty():
    assert _extract_pt_inventory([]) == []
    assert _extract_pt_inventory(None) == []


def test_protocol_pt_without_source_marker_still_extracted():
    """A row tagged ``protocol == 'pt'`` but missing the source marker is still PT.

    Defensive: the primary key is the ``pt_inventory_lots`` source marker, but a
    ``protocol == 'pt'`` row must not be silently dropped either.
    """
    pt = StrategyPosition(
        position_type="TOKEN",
        position_id="PT-foo",
        chain="arbitrum",
        protocol="pt",
        value_usd=Decimal("1"),
        details={"pt_symbol": "PT-foo", "quantity": "1"},
    )
    rows = _extract_pt_inventory([pt])
    assert [r.symbol for r in rows] == ["PT-foo"]


def _reported_pt_measured() -> StrategyPosition:
    """A REPORTED PT (``get_open_positions`` common case): ``protocol='pendle'``.

    The valuer's reprice path (``_reprice_principal_token_enriched``) now stamps
    the same ``source`` marker + PT display fields, and the gateway proto carries
    ``source`` forward — so it renders identically to a FIFO-derived held PT even
    though its ``protocol`` is ``pendle`` (not ``pt``). This is the case the first
    VIB-5317 impl was INERT for.
    """
    return StrategyPosition(
        position_type="SUPPLY",
        position_id="PT-wstETH",
        chain="arbitrum",
        protocol="pendle",
        value_usd=Decimal("2000.00"),
        unrealized_pnl_usd=Decimal("100.00"),
        details={
            "source": "pt_inventory_lots",
            "pt_symbol": "PT-wstETH",
            "quantity": "20.0",
            "days_to_maturity": "180",
            "price_confidence": "HIGH",
        },
    )


def test_reported_pendle_pt_is_extracted():
    """The reported PT (protocol='pendle' + source marker) reaches PT inventory."""
    rows = _extract_pt_inventory([_reported_pt_measured()])

    assert len(rows) == 1
    row = rows[0]
    assert row.symbol == "PT-wstETH"
    assert row.quantity == "20.0"
    assert row.value_usd == Decimal("2000.00")
    assert row.unrealized_pnl_usd == Decimal("100.00")
    assert row.days_to_maturity == "180"
    assert row.confidence == "HIGH"
    assert row.unmeasured is False


def test_reported_pendle_pt_unmeasured_has_no_fabricated_usd():
    """An unmeasured reported PT renders blank USD (Empty ≠ Zero), never $0."""
    reported = StrategyPosition(
        position_type="SUPPLY",
        position_id="PT-wstETH",
        chain="arbitrum",
        protocol="pendle",
        value_usd=None,
        unrealized_pnl_usd=None,
        details={
            "source": "pt_inventory_lots",
            "pt_symbol": "PT-wstETH",
            "quantity": "20.0",
            "price_confidence": "UNAVAILABLE",
            "mark_unmeasured": "true",
        },
    )
    rows = _extract_pt_inventory([reported])

    assert len(rows) == 1
    row = rows[0]
    assert row.value_usd is None
    assert row.unrealized_pnl_usd is None
    assert row.unmeasured is True
    assert row.quantity == "20.0"
    assert row.confidence == "UNAVAILABLE"


def test_non_pt_pendle_row_without_marker_is_not_extracted():
    """A ``protocol='pendle'`` row WITHOUT the source marker is NOT PT inventory.

    Only rows the valuer tagged as PT inventory (``source`` marker) — or a bare
    ``protocol == 'pt'`` row — surface here. A non-PT pendle position (e.g. a
    pendle LP that did not flow through the PT path) must be left to its own
    surface, never mis-rendered as PT inventory.
    """
    pendle_non_pt = StrategyPosition(
        position_type="LP",
        position_id="pendle-lp-1",
        chain="arbitrum",
        protocol="pendle",
        value_usd=Decimal("500"),
        details={"pool": "0xabc"},
    )
    rows = _extract_pt_inventory([pendle_non_pt])
    assert rows == []


def test_no_duplicate_when_reported_and_fifo_same_symbol():
    """Defensive: a reported + a FIFO row for the same symbol render only once.

    The valuer's ``_reported_pt_symbols`` dedup already guarantees both never
    reach the snapshot together; the first-wins guard here is a backstop against
    an upstream regression.
    """
    fifo = _measured_pt()  # PT-wstETH-26DEC2024
    reported = StrategyPosition(
        position_type="SUPPLY",
        position_id="PT-wstETH-26DEC2024",
        chain="arbitrum",
        protocol="pendle",
        value_usd=Decimal("9999"),
        details={
            "source": "pt_inventory_lots",
            "pt_symbol": "PT-wstETH-26DEC2024",
            "quantity": "10.5",
        },
    )
    rows = _extract_pt_inventory([fifo, reported])
    assert [r.symbol for r in rows] == ["PT-wstETH-26DEC2024"]
    # First-wins: the FIFO row (measured 1050.50) is kept.
    assert rows[0].value_usd == Decimal("1050.50")
