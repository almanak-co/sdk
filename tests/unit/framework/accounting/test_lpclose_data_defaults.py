"""VIB-4470 — ``LPCloseData.fees0`` / ``fees1`` default flip.

Pins the Empty ≠ Zero contract on ``LPCloseData``:

- Default ``fees0`` / ``fees1`` are ``None`` (unmeasured), not ``0``
  (measured zero). This prevents the prior silent-lie path where every
  parser that didn't separate fees from principal (V4, V3 fee-only burn
  paths, Aerodrome V1, Fluid, TraderJoe V2, Pendle, …) wrote
  ``fees0_collected = 0`` to the LP accounting event despite never having
  observed the fee amount on-chain.
- ``all_fees`` surfaces ``None`` slots as ``None`` rather than coercing
  to ``0``.
- ``to_dict`` serialises ``None`` fees to JSON ``null`` and a numeric
  zero (measured zero) to the string ``"0"``, so the two states remain
  distinguishable across the SQLite payload boundary.

Together these pin the parser-side contract used by
``lp_accounting._to_human`` (already None-safe) and the
``LPAccountingEvent`` JSON payload encoder (which writes ``null`` for
``None`` ``fees0_collected``).
"""

from __future__ import annotations

from almanak.framework.execution.extracted_data import LPCloseData


class TestLPCloseDataDefaults:
    def test_default_fees0_is_none_not_zero(self) -> None:
        data = LPCloseData(amount0_collected=0, amount1_collected=0, liquidity_removed=0)
        assert data.fees0 is None, "fees0 default must be None (unmeasured), not 0"
        assert data.fees1 is None, "fees1 default must be None (unmeasured), not 0"

    def test_default_fees_distinct_from_measured_zero(self) -> None:
        unmeasured = LPCloseData(amount0_collected=10, amount1_collected=20)
        measured_zero = LPCloseData(amount0_collected=10, amount1_collected=20, fees0=0, fees1=0)
        assert unmeasured.fees0 is None
        assert measured_zero.fees0 == 0
        # Empty ≠ Zero — the two states must remain distinguishable
        assert unmeasured.fees0 is not measured_zero.fees0

    def test_explicit_numeric_fees_round_trip(self) -> None:
        data = LPCloseData(
            amount0_collected=1_000_000,
            amount1_collected=2_000_000,
            fees0=42,
            fees1=99,
        )
        assert data.fees0 == 42
        assert data.fees1 == 99

    def test_all_fees_surfaces_none_unchanged(self) -> None:
        data = LPCloseData(amount0_collected=1, amount1_collected=2)
        assert data.all_fees == [None, None]

    def test_all_fees_mixed_with_additional(self) -> None:
        data = LPCloseData(
            amount0_collected=1,
            amount1_collected=2,
            fees0=10,
            fees1=20,
            additional_fees={2: 30, 3: 40},
        )
        assert data.all_fees == [10, 20, 30, 40]


class TestLPCloseDataToDict:
    def test_to_dict_default_fees_serialise_as_null(self) -> None:
        d = LPCloseData(amount0_collected=0, amount1_collected=0).to_dict()
        # Unmeasured fees → JSON null (NOT the string "0")
        assert d["fees0"] is None
        assert d["fees1"] is None

    def test_to_dict_measured_zero_serialises_as_string_zero(self) -> None:
        d = LPCloseData(
            amount0_collected=0,
            amount1_collected=0,
            fees0=0,
            fees1=0,
        ).to_dict()
        # Measured zero → "0" string (distinct from JSON null)
        assert d["fees0"] == "0"
        assert d["fees1"] == "0"

    def test_to_dict_numeric_fees_serialise_as_string(self) -> None:
        d = LPCloseData(
            amount0_collected=1,
            amount1_collected=2,
            fees0=42,
            fees1=99,
        ).to_dict()
        assert d["fees0"] == "42"
        assert d["fees1"] == "99"

    def test_to_dict_includes_required_fields(self) -> None:
        d = LPCloseData(amount0_collected=1, amount1_collected=2).to_dict()
        # Sanity-check the public shape so a future refactor doesn't drop
        # fields silently.
        for key in (
            "amount0_collected",
            "amount1_collected",
            "fees0",
            "fees1",
            "liquidity_removed",
            "current_tick",
            "pool_address",
            "source",
        ):
            assert key in d, f"to_dict missing required key {key!r}"
