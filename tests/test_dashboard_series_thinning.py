from decimal import Decimal

from almanak.dashboard import build as b


def reading(i, amount):
    return (
        1_000 + i,
        b.index(
            f"r{i}",
            None if amount is None else Decimal(amount),
            "Reading.",
            evidence="author_derived",
        ),
    )


def amounts(points):
    return [p[1].amount for p in points]


def test_a_short_series_is_untouched():
    points = [reading(i, i) for i in range(150)]
    assert b.thin(points) == points


def test_a_long_series_keeps_spikes_gaps_and_the_latest_point_in_order():
    points = [reading(i, 10) for i in range(500)]
    points[123] = reading(123, 999)  # a spike
    points[321] = reading(321, -999)  # a dip
    points[400] = reading(400, None)  # a gap
    thinned = b.thin(points)
    assert len(thinned) <= b.MAX_SERIES_POINTS
    times = [t for t, _ in thinned]
    assert times == sorted(times)
    assert {1_000 + 123, 1_000 + 321, 1_000 + 400} <= set(times)  # the spike, the dip and the gap
    assert None in amounts(thinned)
    assert thinned[-1] == points[-1]


def test_series_thins_time_charts_and_leaves_price_move_axes_alone():
    long = [reading(i, i % 7) for i in range(500)]
    assert len(b.series("s", "S", long).points) <= b.MAX_SERIES_POINTS
    moves = [(i * 10, v) for i, (_, v) in enumerate(long[:90])]
    assert len(b.series("m", "M", moves, x_axis="price_move").points) == 90


def test_the_budget_holds_with_many_gaps_and_any_length():
    for n in (151, 300, 301, 500):
        points = [reading(i, None if i % 2 else i) for i in range(n)]
        thinned = b.thin(points)
        assert len(thinned) <= b.MAX_SERIES_POINTS
        assert thinned[-1] == points[-1]
        assert None in amounts(thinned)


def test_readings_compare_at_their_declared_precision():
    def scaled(i, amount, decimals):
        value = b.index(f"r{i}", Decimal(amount), "Reading.", evidence="author_derived")
        return (
            1_000 + i,
            value.model_copy(
                update={"amount": str(int(Decimal(amount) * 10**decimals)), "decimals": decimals}
            ),
        )

    # One bucket: 1 (0 places), 0.90 and 0.95 (2 places); the true low is 0.90.
    points = [scaled(0, "1", 0), scaled(1, "0.90", 2), scaled(2, "0.95", 2)] + [
        reading(i, 5) for i in range(3, 400)
    ]
    times = [t for t, _ in b.thin(points)]
    assert 1_001 in times


def test_large_readings_compare_exactly_past_decimal_precision():
    def raw(i, amount):
        value = b.index(f"r{i}", Decimal(1), "Reading.", evidence="author_derived")
        return (1_000 + i, value.model_copy(update={"amount": amount, "decimals": 0}))

    base = 10**40
    # Differ only beyond digit 28: the true low (index 1) must survive.
    points = [raw(0, str(base + 2)), raw(1, str(base + 1)), raw(2, str(base + 3))] + [
        raw(i, str(base * 2)) for i in range(3, 400)
    ]
    assert 1_001 in [t for t, _ in b.thin(points)]


def test_a_limit_below_four_is_refused():
    import pytest

    with pytest.raises(ValueError):
        b.thin([reading(i, i) for i in range(10)], limit=3)
