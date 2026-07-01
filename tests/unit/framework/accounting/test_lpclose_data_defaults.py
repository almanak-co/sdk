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

from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData


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
            # VIB-4848 (T8) — fee separation taxonomy
            "fee_separation_method",
            "fee_confidence",
        ):
            assert key in d, f"to_dict missing required key {key!r}"


class TestLPCloseDataPrincipalNullability:
    """VIB-5117 — ``amount0_collected`` / ``amount1_collected`` nullability.

    The close PRINCIPAL legs are now ``int | None``. ``None`` = unmeasured (the
    canonical case is a Uniswap V4 native leg withdrawn as raw ETH with no
    Transfer); ``0`` = measured zero (an ERC-20 leg that truly withdrew nothing).
    The two must stay distinguishable across the SQLite payload boundary — a
    ``None`` native leg must not collapse to ``"0"`` and silently understate
    realized PnL by the full native principal.
    """

    def test_principal_legs_default_to_none(self) -> None:
        # No explicit amounts → unmeasured (None), NOT a fabricated zero.
        data = LPCloseData()
        assert data.amount0_collected is None
        assert data.amount1_collected is None

    def test_measured_zero_distinct_from_none(self) -> None:
        unmeasured = LPCloseData(amount0_collected=None, amount1_collected=5)
        measured_zero = LPCloseData(amount0_collected=0, amount1_collected=5)
        assert unmeasured.amount0_collected is None
        assert measured_zero.amount0_collected == 0
        # Empty ≠ Zero — the two states must remain distinguishable.
        assert unmeasured.amount0_collected is not measured_zero.amount0_collected

    def test_to_dict_none_principal_serialises_as_null(self) -> None:
        # Native leg unmeasured → JSON null (NOT the literal string "None").
        d = LPCloseData(amount0_collected=None, amount1_collected=170_000).to_dict()
        assert d["amount0_collected"] is None
        assert d["amount1_collected"] == "170000"

    def test_to_dict_measured_zero_principal_serialises_as_string_zero(self) -> None:
        # Measured ERC-20 zero → "0" string, distinct from JSON null.
        d = LPCloseData(amount0_collected=0, amount1_collected=170_000).to_dict()
        assert d["amount0_collected"] == "0"
        assert d["amount1_collected"] == "170000"

    def test_all_amounts_surfaces_none_unchanged(self) -> None:
        # ``all_amounts`` must surface a None slot as None, never coerce to 0.
        data = LPCloseData(amount0_collected=None, amount1_collected=170_000)
        assert data.all_amounts == [None, 170_000]


class TestLPCloseDataFeeSeparationTaxonomy:
    """VIB-4848 (T8) — ``fee_separation_method`` + ``fee_confidence``."""

    def test_default_inference_bundled_when_fees_unmeasured(self) -> None:
        # Both fees None ⇒ parser did not separate ⇒ BUNDLED. Confidence
        # stays UNKNOWN since no estimator wired yet.
        data = LPCloseData(amount0_collected=100, amount1_collected=200)
        assert data.fee_separation_method == "BUNDLED"
        assert data.fee_confidence == "UNKNOWN"

    def test_default_inference_separate_when_fees_measured(self) -> None:
        # Numeric (incl. zero) fee on at least one leg ⇒ parser DID
        # separate ⇒ SEPARATE/EXACT.
        data = LPCloseData(amount0_collected=100, amount1_collected=200, fees0=5, fees1=7)
        assert data.fee_separation_method == "SEPARATE"
        assert data.fee_confidence == "EXACT"

    def test_default_inference_separate_with_measured_zero(self) -> None:
        # Measured zero on a leg still counts as SEPARATE (Empty ≠ Zero).
        data = LPCloseData(amount0_collected=100, amount1_collected=200, fees0=0, fees1=0)
        assert data.fee_separation_method == "SEPARATE"
        assert data.fee_confidence == "EXACT"

    def test_default_inference_separate_with_one_leg_measured(self) -> None:
        # One leg measured, other unmeasured ⇒ still SEPARATE (we have
        # evidence the parser supports separation).
        data = LPCloseData(amount0_collected=100, amount1_collected=200, fees0=3)
        assert data.fee_separation_method == "SEPARATE"
        assert data.fee_confidence == "EXACT"

    def test_explicit_parser_value_wins_over_inference(self) -> None:
        # A parser that knows it bundles but happens to emit a numeric
        # fee (e.g. extracted from a different log) can still override.
        data = LPCloseData(
            amount0_collected=100,
            amount1_collected=200,
            fees0=10,
            fees1=20,
            fee_separation_method="BUNDLED",
            fee_confidence="ESTIMATED",
        )
        assert data.fee_separation_method == "BUNDLED"
        assert data.fee_confidence == "ESTIMATED"

    def test_to_dict_round_trips_taxonomy(self) -> None:
        d = LPCloseData(
            amount0_collected=100,
            amount1_collected=200,
            fees0=10,
            fees1=20,
        ).to_dict()
        assert d["fee_separation_method"] == "SEPARATE"
        assert d["fee_confidence"] == "EXACT"

    def test_to_dict_bundled_default(self) -> None:
        d = LPCloseData(amount0_collected=100, amount1_collected=200).to_dict()
        assert d["fee_separation_method"] == "BUNDLED"
        assert d["fee_confidence"] == "UNKNOWN"


class TestAllAmountsNumericOrdering:
    """VIB-5545 — ``all_amounts`` must sort ``additional_amounts`` numerically.

    After a JSON round-trip (``to_dict`` writes ``{str(k): ...}``, then a
    ``json.loads`` on the SQLite payload hands back string keys), the integer
    coin-index keys deserialize as STRINGS. A plain ``sorted()`` over string
    keys orders them lexicographically (``"10" < "2"``), which would misplace
    coin amounts at ≥10 coins. ``sorted(..., key=int)`` restores numeric order
    regardless of str/int key type.

    Latent at ≤8 coins (single-digit keys sort identically under both orders);
    these tests exercise the ≥10-coin regime with STRING keys where the two
    orderings diverge, and assert index 2 lands before index 10.
    """

    # A JSON round-trip yields string keys — reproduce that exact shape.
    # 12 coins ("0".."11"), value = index * 1000 so mis-ordering is visible.
    _STR_KEYED_ADDITIONAL = {str(i): i * 1000 for i in range(2, 12)}

    def test_lp_close_numeric_order_with_string_keys(self) -> None:
        data = LPCloseData(
            amount0_collected=0,
            amount1_collected=1000,
            additional_amounts=dict(self._STR_KEYED_ADDITIONAL),
        )
        amounts = data.all_amounts
        # amount0/amount1 occupy slots 0 and 1; additional coins 2..11 follow
        # in numeric index order.
        assert amounts == [i * 1000 for i in range(12)]
        # The load-bearing assertion: coin 2 must precede coin 10, which a
        # lexicographic ("10" < "2") sort would invert.
        assert amounts.index(2 * 1000) < amounts.index(10 * 1000)

    def test_lp_open_numeric_order_with_string_keys(self) -> None:
        data = LPOpenData(
            position_id=1,
            amount0=0,
            amount1=1000,
            additional_amounts=dict(self._STR_KEYED_ADDITIONAL),
        )
        amounts = data.all_amounts
        assert amounts == [i * 1000 for i in range(12)]
        assert amounts.index(2 * 1000) < amounts.index(10 * 1000)

    def test_lp_close_numeric_order_survives_actual_to_dict_round_trip(self) -> None:
        # End-to-end: build with int keys, serialise via to_dict, JSON
        # round-trip (which stringifies keys), rebuild, and confirm order.
        import json

        original = LPCloseData(
            amount0_collected=0,
            amount1_collected=1000,
            additional_amounts={i: i * 1000 for i in range(2, 12)},
        )
        payload = json.loads(json.dumps(original.to_dict()["additional_amounts"]))
        # payload keys are now strings, values are strings ("2000", ...)
        rehydrated = LPCloseData(
            amount0_collected=0,
            amount1_collected=1000,
            additional_amounts={k: int(v) for k, v in payload.items()},
        )
        assert rehydrated.all_amounts == [i * 1000 for i in range(12)]
