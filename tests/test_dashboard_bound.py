import pytest

from almanak.dashboard import (
    Column,
    Dashboard,
    Dataset,
    Group,
    LineChart,
    Move,
    Ref,
    Sample,
    Series,
    Table,
    Value,
)
from almanak.dashboard.bound import MAX_TABLE_ROWS, bounded
from almanak.dashboard.build import MAX_SERIES_POINTS
from almanak.dashboard.evaluate import MAX_BYTES, TOO_LARGE, _document, evaluate, failure
from almanak.strategy import StrategyDefinition
from almanak.worker_process import WorkerFailed


@pytest.fixture
def dashboard_state(recorded, reference_strategy):
    return recorded.model_copy(update={"strategy": reference_strategy})


def reading(i: int) -> Value:
    return Value(
        id=f"v{i}", amount=str(i % 97), unit="count", evidence="author_derived", reason="r"
    )


def long_series(n: int, cited_every: int = 0) -> Series:
    points = tuple(
        Sample(
            time=i,
            value=Ref(ref=f"fact{i}") if cited_every and i % cited_every == 0 else reading(i),
        )
        for i in range(n)
    )
    return Series(id="s", label="S", points=points)


def chart(*series: Series, **extra) -> LineChart:
    return LineChart(id="c", title="C", series=series, caption="test", **extra)


def test_long_series_are_thinned_in_order_keeping_the_latest():
    drawn = bounded(Dashboard(title="T", blocks=(chart(long_series(480, cited_every=5)),)))
    points = drawn.blocks[0].series[0].points
    assert len(points) <= MAX_SERIES_POINTS
    assert [p.time for p in points] == sorted(p.time for p in points)
    assert points[-1].time == 479


def test_a_chart_of_only_cited_facts_keeps_points_through_the_series():
    points = bounded(Dashboard(title="T", blocks=(chart(long_series(400, cited_every=1)),)))
    kept = points.blocks[0].series[0].points
    assert 1 < len(kept) <= MAX_SERIES_POINTS
    assert kept[0].time == 0 and kept[-1].time == 399


def test_short_series_and_price_move_lines_are_untouched():
    short = chart(long_series(40))
    moves = LineChart(
        id="m",
        title="M",
        x_axis="price_move",
        points=tuple(Move(move_bps=i, value=reading(i)) for i in range(300)),
        caption="test",
    )
    drawn = bounded(Dashboard(title="T", blocks=(short, moves)))
    assert drawn.blocks[0] == short
    assert drawn.blocks[1] == moves


def test_long_tables_keep_their_newest_rows_and_say_so():
    rows = tuple({"n": str(i)} for i in range(450))
    table = Table(
        id="t",
        title="T",
        data=Dataset(columns=(Column(key="n", label="N", kind="text"),), rows=rows, coverage="x"),
    )
    nested = Group(id="g", title="G", blocks=(table, chart(long_series(300))))
    drawn = bounded(Dashboard(title="T", blocks=(nested,)))
    kept, line = drawn.blocks[0].blocks
    assert len(kept.data.rows) == MAX_TABLE_ROWS
    assert kept.data.rows[-1] == {"n": "449"} and kept.data.truncated
    assert len(line.series[0].points) <= MAX_SERIES_POINTS


def draw(dashboard_state, source: str):
    strategy = StrategyDefinition(filename="strategy.py", source=source)
    return evaluate(dashboard_state.model_copy(update={"strategy": strategy}))


def test_a_hook_that_builds_its_own_long_series_still_draws(dashboard_state):
    source = """
from almanak.dashboard import Dashboard, LineChart, Sample, Series, Value

def dashboard(ctx):
    points = tuple(
        Sample(time=i, value=Value(id=f"p{i}", amount=str(i), unit="count",
                                   evidence="author_derived", reason="Reading."))
        for i in range(500)
    )
    line = Series(id="raw", label="Raw", points=points)
    chart = LineChart(id="c", title="C", series=(line,), caption="x")
    return Dashboard(title="Raw", blocks=(chart,))
"""
    result = draw(dashboard_state, source)
    assert result.status == "ready", result.diagnostic
    assert len(result.document.blocks[0].series[0].points) <= MAX_SERIES_POINTS


@pytest.mark.parametrize(
    ("source", "says"),
    [
        ("raise RuntimeError('private marker')", "raised RuntimeError"),
        ("def dashboard(ctx):\n    return {'title': 'wrong'}", "raised TypeError"),
        (
            "import os\nfrom almanak.dashboard import Dashboard, Text\n"
            "def dashboard(ctx):\n    os.write(1, b'x' * 2000000)\n"
            "    return Dashboard(title='T', blocks=(Text(id='t', text='ok'),))",
            "too large",
        ),
    ],
)
def test_failures_say_what_went_wrong(dashboard_state, source, says):
    result = draw(dashboard_state, source)
    assert result.status == "unavailable"
    assert says in result.diagnostic
    assert "private marker" not in result.model_dump_json()


def test_an_oversized_write_reports_what_was_written(dashboard_state):
    source = (
        "import os\nfrom almanak.dashboard import Dashboard, Text\n"
        "def dashboard(ctx):\n    os.write(1, b'x' * 2000000)\n"
        "    return Dashboard(title='T', blocks=(Text(id='t', text='ok'),))"
    )
    assert "(1025 KiB" in draw(dashboard_state, source).diagnostic


def test_a_spinning_hook_names_the_cpu_limit(dashboard_state, monkeypatch):
    # A generous wall budget, so the CPU limit is what stops it even on a loaded host.
    monkeypatch.setattr("almanak.dashboard.evaluate.WALL_SECONDS", 60)
    assert "CPU" in draw(dashboard_state, "def dashboard(ctx):\n    while True: pass").diagnostic


def test_a_hook_that_keeps_writing_past_the_limit_is_too_large(dashboard_state):
    source = (
        "import os\ndef dashboard(ctx):\n    os.write(1, b'x' * 2000000)\n    os.write(1, b'x')"
    )
    assert "too large" in draw(dashboard_state, source).diagnostic


def test_a_slow_hook_names_the_time_limit(dashboard_state, monkeypatch):
    monkeypatch.setattr("almanak.dashboard.evaluate.WALL_SECONDS", 0.3)
    result = draw(dashboard_state, "import time\ndef dashboard(ctx):\n    time.sleep(30)")
    assert "time limit" in result.diagnostic


def test_a_missing_sandbox_is_named(dashboard_state, monkeypatch):
    def unavailable():
        raise RuntimeError("missing sandbox")

    monkeypatch.setattr("almanak.worker_process.worker_command", unavailable)
    assert "No OS sandbox" in evaluate(dashboard_state).diagnostic


def test_an_oversized_document_reports_its_size():
    report = "noise\nalmanak-dashboard-failure: too_large 1530\n"
    message = failure(WorkerFailed("x", TOO_LARGE, report))
    assert "1530 KiB" in message and "1024 KiB" in message


def test_author_output_cannot_inject_text():
    report = "almanak-dashboard-failure: raised <script>alert(1)</script>\n"
    assert "<script>" not in failure(WorkerFailed("x", 3, report))


def test_the_document_is_bounded_and_refused_only_past_the_limit():
    drawn = Dashboard(title="T", blocks=(chart(long_series(480)),))
    document, size = _document(drawn, 0)
    assert document is not None and size == len(document.encode())
    assert Dashboard.model_validate_json(document).blocks[0].series[0].points[-1].time == 479
    assert _document(None, 0) == ("null", 4)
    refused, over = _document(drawn, MAX_BYTES)
    assert refused is None and over == size + MAX_BYTES
