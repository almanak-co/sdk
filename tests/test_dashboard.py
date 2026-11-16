"""Dashboard contracts over explicit captured evidence; no provider access."""

import json
import subprocess
from pathlib import Path

import pytest
from pydantic import ValidationError

from almanak.dashboard import (
    BarChart,
    Column,
    Dashboard,
    Dataset,
    Group,
    LineChart,
    Metric,
    Point,
    Ref,
    Sample,
    Text,
    Value,
)
from almanak.dashboard.evaluate import evaluate
from almanak.dashboard.models import Response, validate_document
from almanak.dashboard.preview import render_html, schema, write_private
from almanak.dashboard.projection import project
from almanak.state import Journal
from almanak.strategy import StrategyDefinition

ROOT = Path(__file__).parents[1]


@pytest.fixture
def dashboard_state(recorded, reference_strategy):
    return recorded.model_copy(update={"strategy": reference_strategy})


def test_reference_dashboard_uses_actual_hook_and_preserves_state(dashboard_state):
    before = dashboard_state.model_dump_json()
    first = evaluate(dashboard_state, captured_at=42)
    second = evaluate(dashboard_state, captured_at=42)
    assert first == second
    assert first.status == "ready"
    assert first.document.title == "Aave WETH positions"
    assert dashboard_state.model_dump_json() == before
    assert (
        first.snapshot.values[f"{dashboard_state.config.allocations[0].name}-observed"].amount
        == "0"
    )
    assert first.snapshot.strategy_hash == dashboard_state.strategy.sha256
    assert all("raw_transaction" not in key for key in first.model_dump())


def test_projection_missing_is_not_zero(tmp_path, dashboard_state, reference_strategy):
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(dashboard_state.config, reference_strategy)
    initial = journal.load()
    ctx, snapshot = project(initial, captured_at=42)
    assert ctx.values[f"{dashboard_state.config.allocations[0].name}-observed"].amount is None
    assert ctx.values[f"{dashboard_state.config.allocations[0].name}-supplied"].amount == "0"
    assert ctx.values[f"{dashboard_state.config.allocations[0].name}-configured"].decimals is None
    assert snapshot.latest_observation is None
    assert not snapshot.initialized
    assert evaluate(initial).status == "ready"


def test_open_position_delta_is_unavailable(dashboard_state):
    state = dashboard_state.model_copy(
        update={
            "mode": "running",
            "owned": (dashboard_state.owned[0].model_copy(update={"shares": 1}),),
        }
    )
    context, _ = project(state)
    assert context.values[f"{dashboard_state.config.allocations[0].name}-delta"].amount is None


def test_individually_closed_position_has_delta(dashboard_state):
    context, _ = project(dashboard_state.model_copy(update={"mode": "running"}))
    assert context.values[f"{dashboard_state.config.allocations[0].name}-delta"].amount is not None


def test_gas_uses_receipts_and_actual_payer(dashboard_state):
    ctx, _ = project(dashboard_state)
    binding = dashboard_state.config.accounts.bindings[0]
    gas = ctx.values[binding.name + "-gas"]
    assert int(gas.amount) == sum(o.gas_paid.amount for o in dashboard_state.outcomes)
    assert gas.scope.payer == binding.profile.submitter
    assert gas.decimals == 18
    assert len(ctx.datasets["fund_costs"].rows) == 1


def test_hook_has_no_host_file_or_network_access(dashboard_state, tmp_path):
    marker = tmp_path / "marker"
    source = f"""
from pathlib import Path
import socket
from almanak.dashboard import Dashboard, Text

def dashboard(ctx):
    failures = 0
    try:
        Path({str(marker)!r}).write_text('unexpected')
    except OSError:
        failures += 1
    try:
        socket.create_connection(('127.0.0.1', 1), timeout=0.1)
    except OSError:
        failures += 1
    return Dashboard(title='Isolation', blocks=(Text(id='result', text=str(failures)),))
"""
    state = dashboard_state.model_copy(
        update={"strategy": StrategyDefinition(filename="strategy.py", source=source)}
    )
    result = evaluate(state)
    assert result.status == "ready"
    assert result.document.blocks[0].text == "2"
    assert not marker.exists()


def test_author_print_does_not_corrupt_the_result(dashboard_state):
    source = """\
from almanak.dashboard import Dashboard, Text


def dashboard(ctx):
    print('debugging the hook')
    return Dashboard(title='Printed', blocks=(Text(id='result', text='ok'),))
"""
    state = dashboard_state.model_copy(
        update={"strategy": StrategyDefinition(filename="strategy.py", source=source)}
    )
    result = evaluate(state)
    assert result.status == "ready", result.diagnostic
    assert result.document.blocks[0].text == "ok"


@pytest.mark.parametrize(
    "source",
    [
        "raise RuntimeError('private marker must not be exposed')",
        "def dashboard(ctx):\n    return {'title': 'wrong return type'}",
        "dashboard = 42",
        "def dashboard(ctx):\n    while True: pass",
        "import os\ndef dashboard(ctx):\n    os.write(1, b'x' * 2000000)",
    ],
)
def test_failed_hooks_are_unavailable_and_redacted(dashboard_state, source):
    state = dashboard_state.model_copy(
        update={"strategy": StrategyDefinition(filename="strategy.py", source=source)}
    )
    result = evaluate(state)
    assert result.status == "unavailable"
    assert result.document is None
    assert "private marker" not in result.model_dump_json()
    assert result.snapshot.mode == state.mode


def test_missing_sandbox_refuses_dashboard(dashboard_state, monkeypatch):
    def unavailable():
        raise RuntimeError("missing sandbox")

    monkeypatch.setattr("almanak.worker_process.worker_command", unavailable)
    assert evaluate(dashboard_state).status == "unavailable"


@pytest.mark.parametrize("source", [None, "def decide(ctx): pass"])
def test_dashboard_is_optional(dashboard_state, source):
    strategy = StrategyDefinition(filename="strategy.py", source=source) if source else None
    assert evaluate(dashboard_state.model_copy(update={"strategy": strategy})).status == "absent"


def test_forged_fact_cannot_become_verified(dashboard_state):
    ctx, _ = project(dashboard_state)
    original = ctx.values[f"{dashboard_state.config.allocations[0].name}-supplied"]
    for changed in (
        original.model_copy(update={"amount": "999"}),
        original.model_copy(update={"id": "forged"}),
    ):
        document = Dashboard(
            title="Untrusted", blocks=(Metric(id="forged", label="Balance", value=changed),)
        )
        with pytest.raises(ValueError):
            validate_document(document, ctx.values)


def test_explicit_author_calculation_requires_known_inputs(dashboard_state):
    ctx, _ = project(dashboard_state)
    value = Value(
        id="custom-count",
        amount="1",
        unit="count",
        evidence="author_derived",
        reason="A custom explanation",
        inputs=("position-count",),
    )
    document = Dashboard(title="Custom", blocks=(Metric(id="custom", label="Count", value=value),))
    validate_document(document, ctx.values)
    bad = value.model_copy(update={"inputs": ("another-deployment",)})
    with pytest.raises(ValueError, match="unknown inputs"):
        validate_document(
            Dashboard(title="Custom", blocks=(Metric(id="x", label="X", value=bad),)), ctx.values
        )


@pytest.mark.parametrize(
    "change",
    [
        {"scope": None},
        {"asset": None},
        {"amount": None, "reason": ""},
        {"amount": "1.5"},
        {"unit": "shares", "decimals": 18},
        {"unit": "count"},
    ],
)
def test_invalid_financial_values_refused(dashboard_state, change):
    ctx, _ = project(dashboard_state)
    value = ctx.values[f"{dashboard_state.config.allocations[0].name}-supplied"]
    with pytest.raises(ValidationError):
        Value.model_validate({**value.model_dump(), **change})


def test_dataset_refuses_missing_columns_and_string_amounts():
    columns = (Column(key="amount", label="Amount", kind="value"),)
    for row in ({}, {"amount": "100"}):
        with pytest.raises(ValueError):
            Dataset(columns=columns, rows=(row,), coverage="test")


def test_unknown_references_and_incomparable_charts_refused(dashboard_state):
    ctx, _ = project(dashboard_state)
    from almanak.dashboard import Table

    table = Table(
        id="x",
        title="X",
        data=Dataset(
            columns=(Column(key="v", label="Value", kind="value"),),
            rows=({"v": Ref(ref="missing")},),
            coverage="test",
        ),
    )
    chart = BarChart(
        id="c",
        title="C",
        points=(
            Point(
                label="Shares",
                value=ctx.values[f"{dashboard_state.config.allocations[0].name}-shares"],
            ),
            Point(
                label="Assets",
                value=ctx.values[f"{dashboard_state.config.allocations[0].name}-supplied"],
            ),
        ),
        caption="test",
    )
    for block in (table, chart):
        with pytest.raises(ValueError):
            validate_document(Dashboard(title="Test", blocks=(block,)), ctx.values)


def test_chart_gaps_keep_nulls_and_exact_values(dashboard_state):
    ctx, snapshot = project(dashboard_state)
    missing = Value(
        id="gap", amount=None, unit="count", evidence="author_derived", reason="Not measured"
    )
    large = Value(
        id="large",
        amount=str(2**256 - 1),
        unit="count",
        evidence="author_derived",
        reason="Exact-count fixture",
    )
    chart = LineChart(
        id="line",
        title="Samples",
        points=(
            Sample(time=1, value=large),
            Sample(time=2, value=missing),
            Sample(time=3, value=large),
        ),
        caption="Offline fixture",
    )
    response = Response(
        snapshot=snapshot, status="ready", document=Dashboard(title="Test", blocks=(chart,))
    )
    assert response.document.blocks[0].points[1].value.amount is None
    assert str(2**256 - 1) in response.model_dump_json()
    with pytest.raises(ValueError):
        LineChart(id="bad", title="Bad", points=tuple(reversed(chart.points)), caption="test")


def test_duplicate_ids_are_rejected():
    text = Text(id="same", text="One")
    with pytest.raises(ValueError):
        Dashboard(title="Test", blocks=(text, Group(id="g", title="G", blocks=(text,))))


def test_export_cannot_inject_scripts(dashboard_state):
    response = evaluate(dashboard_state)
    doc = Dashboard(
        title="</script><script>alert(1)</script>",
        blocks=(Text(id="x", text="<img src=x onerror=alert(1)>"),),
    )
    response = response.model_copy(update={"document": doc})
    markup = render_html(response)
    assert "</script><script>alert(1)" not in markup
    assert "\\u003cimg src=x" in markup
    assert "connect-src &#x27;none&#x27;" in markup
    assert "'unsafe-inline'" not in markup


def test_private_export_refuses_overwrite(tmp_path):
    path = tmp_path / "preview.html"
    write_private(path, "original")
    assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        write_private(path, "replacement")
    assert path.read_text() == "original"


def test_shared_browser_validator_and_exact_formatting(dashboard_state, tmp_path):
    response = evaluate(dashboard_state)
    payload = tmp_path / "response.json"
    payload.write_text(response.model_dump_json())
    schema_file = tmp_path / "schema.json"
    schema_file.write_text(json.dumps(schema()))
    script = """
const fs = require('fs');
const assert = require('node:assert/strict');
require(process.argv[1]); require(process.argv[2]);
const response = JSON.parse(fs.readFileSync(process.argv[3]));
const schema = JSON.parse(fs.readFileSync(process.argv[4]));
AlmanakDashboardValidation.validate(response, schema);
const value = Object.values(response.snapshot.values).find(v => v.decimals === 18);
assert.equal(AlmanakDashboard.exact({...value, amount:'1234567890123456789'}),
  '1.234567890123456789');
assert.equal(AlmanakDashboard.exact({...value, amount:'0'}), '0');
assert.equal(AlmanakDashboard.exact({...value, amount:null}), 'Unavailable');
const altered = structuredClone(response); altered.protocol_version = 999;
assert.throws(() => AlmanakDashboardValidation.validate(altered, schema));
const forged = structuredClone(response);
const recorded = forged.document.blocks.flatMap(b => b.kind === 'group' ? b.blocks : [b])
  .filter(b => b.kind === 'metric' && b.value.evidence === 'recorded');
assert.ok(recorded.length > 0);
recorded[0].value.amount = '9000';
assert.throws(() => AlmanakDashboardValidation.validate(forged, schema));
"""
    result = subprocess.run(
        [
            "node",
            "-e",
            script,
            str(ROOT / "src/almanak/dashboard/validate.js"),
            str(ROOT / "src/almanak/dashboard/renderer.js"),
            str(payload),
            str(schema_file),
        ],
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr


def test_renderer_applies_only_known_host_theme_tokens():
    script = """
const assert = require('node:assert/strict');
globalThis.document = {createElement: () => ({append() {}})};
require(process.argv[1]); require(process.argv[2]);
const props = new Map();
const style = {setProperty: (k, v) => props.set(k, v), removeProperty: k => props.delete(k)};
const root = {ownerDocument: {documentElement: {style}}, replaceChildren() {}, append() {}};
assert.ok(AlmanakDashboard.themeTokens.includes('paper'));
assert.ok(AlmanakDashboard.themeTokens.includes('heading-font'));
AlmanakDashboard.mount(root, {}, {}, {theme: {
  paper: '#192729', 'heading-font': '"Bw Gradual", sans-serif', scheme: 'dark',
  position: 'fixed', ink: 7, accent: '', muted: 'x'.repeat(257),
}});
assert.deepEqual(Object.fromEntries(props), {
  '--paper': '#192729', '--heading-font': '"Bw Gradual", sans-serif', '--scheme': 'dark',
});
AlmanakDashboard.mount(root, {}, {}, {theme: {ink: '#fcfcfc'}});
assert.deepEqual(Object.fromEntries(props), {'--ink': '#fcfcfc'});
AlmanakDashboard.mount(root, {}, {});
assert.equal(props.size, 0);
"""
    result = subprocess.run(
        [
            "node",
            "-e",
            script,
            str(ROOT / "src/almanak/dashboard/validate.js"),
            str(ROOT / "src/almanak/dashboard/renderer.js"),
        ],
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr


def test_renderer_axis_decimals_keep_the_two_ends_apart():
    script = """
const assert = require('node:assert/strict');
globalThis.document = {createElement: () => ({append() {}})};
require(process.argv[1]); require(process.argv[2]);
const {axisDigits} = AlmanakDashboard;
const label = (v, d) =>
  v.toLocaleString('en-US', {minimumFractionDigits: d, maximumFractionDigits: d});
for (const [lo, hi] of [[0.0171, 0.0201], [0.000012, 0.0000131], [0.98, 1.02], [3100, 3100.4]]) {
  const d = axisDigits(lo, hi);
  assert.notEqual(label(lo, d), label(hi, d), lo + ' and ' + hi + ' read alike at ' + d);
}
assert.equal(axisDigits(1500, 3200), 0);
assert.equal(axisDigits(2, 9), 1);
assert.equal(axisDigits(0.4, 1.6), 2);
assert.equal(axisDigits(0.02, 0.02), 3);
assert.equal(axisDigits(0, 0), 2);
assert.notEqual(label(1e-12, axisDigits(1e-12, 2e-12)), label(2e-12, axisDigits(1e-12, 2e-12)));
"""
    result = subprocess.run(
        [
            "node",
            "-e",
            script,
            str(ROOT / "src/almanak/dashboard/validate.js"),
            str(ROOT / "src/almanak/dashboard/renderer.js"),
        ],
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr


def test_renderer_stylesheet_takes_every_colour_and_font_from_theme_tokens():
    css = (ROOT / "src/almanak/dashboard/renderer.css").read_text()
    root, rules = css.split("}", 1)
    assert "#" not in rules.replace("#block-", "")
    assert "ui-monospace" not in rules and "Inter" not in rules
    declarations = root.split("{", 1)[1].split(";")
    declared = {part.split(":", 1)[0][2:] for part in declarations if part.startswith("--")}
    renderer = (ROOT / "src/almanak/dashboard/renderer.js").read_text()
    tokens = renderer.split("const themeTokens = Object.freeze([", 1)[1].split("]", 1)[0]
    exposed = {token.strip().strip('"') for token in tokens.split(",") if token.strip()}
    assert declared == exposed - {"heading-weight"}


def test_cli_preview_and_json(dashboard_state, tmp_path, monkeypatch, capsys):
    from almanak.cli import main
    from almanak.state import Journal

    journal = Journal(tmp_path / "state.sqlite")
    journal.create(dashboard_state.config, dashboard_state.strategy)
    journal.update(lambda _: dashboard_state)
    for format in ("json", "html"):
        output = tmp_path / ("dashboard." + format)
        monkeypatch.setattr(
            "sys.argv",
            [
                "almanak",
                "report",
                "--run",
                str(tmp_path),
                "--output",
                str(output),
                "--format",
                format,
            ],
        )
        assert main() == 0
        assert json.loads(capsys.readouterr().out)["report"] == str(output.resolve())
        assert output.exists()


def test_worker_cannot_spawn_descendants(dashboard_state):
    source = """
import os
from almanak.dashboard import Dashboard, Text

def dashboard(ctx):
    try:
        child = os.fork()
    except OSError:
        result = 'refused'
    else:
        if child == 0:
            os._exit(0)
        os.waitpid(child, 0)
        result = 'spawned'
    return Dashboard(title='Process limit', blocks=(Text(id='result', text=result),))
"""
    strategy = StrategyDefinition(filename="strategy.py", source=source)
    result = evaluate(dashboard_state.model_copy(update={"strategy": strategy}))
    assert result.status == "ready"
    assert result.document.blocks[0].text == "refused"


def test_unknown_outcome_keeps_status_and_excludes_signed_bytes(dashboard_state):
    from tests.test_runtime_recovery import pending_for

    pending = pending_for(dashboard_state.config, dashboard_state.outcomes[1])
    signed = "0x" + "ab" * 128
    pending = pending.model_copy(update={"raw_transaction": signed})
    state = dashboard_state.model_copy(update={"mode": "running", "pending": pending})
    response = evaluate(state)
    assert response.status == "ready"
    assert response.snapshot.pending_hash == pending.tx_hash
    assert signed not in response.model_dump_json()
    assert "raw_transaction" not in response.model_dump_json()


def test_read_only_context_cannot_commit_memory(dashboard_state):
    source = """
from almanak.dashboard import Dashboard, Text

def dashboard(ctx):
    ctx.memory.values.clear()
    return Dashboard(title='View', blocks=(Text(id='result', text='done'),))
"""
    state = dashboard_state.model_copy(
        update={"strategy": StrategyDefinition(filename="strategy.py", source=source)}
    )
    previous = state.model_dump_json()
    assert evaluate(state).status == "ready"
    assert state.model_dump_json() == previous


def test_logical_second_chain_keeps_missing_data_and_fund_scope():
    from tests.network_support import initial_state

    state = initial_state()
    context, snapshot = project(state)
    for allocation in state.config.allocations:
        value = context.values[allocation.name + "-observed"]
        assert value.amount is None
        assert value.asset == allocation.reserve.asset
        assert value.scope.fund == state.config.target(allocation.name).fund
    assert len(snapshot.chains) == 2
    assert len(context.datasets["fund_costs"].rows) == 2
    assert all(
        int(context.values[b.name + "-gas"].amount) == 0 for b in state.config.accounts.bindings
    )


def test_wall_timeout_stops_sleeping_hook(dashboard_state, monkeypatch):
    source = "import time\ndef dashboard(ctx):\n    time.sleep(30)"
    strategy = StrategyDefinition(filename="strategy.py", source=source)
    monkeypatch.setattr("almanak.dashboard.evaluate.WALL_SECONDS", 0.3)
    assert (
        evaluate(dashboard_state.model_copy(update={"strategy": strategy})).status == "unavailable"
    )


def test_snapshot_fact_keys_cannot_disagree(dashboard_state):
    from almanak.dashboard.models import Snapshot

    _, snapshot = project(dashboard_state)
    raw = snapshot.model_dump()
    raw["values"]["different-key"] = raw["values"].pop("position-count")
    with pytest.raises(ValueError, match="identifiers"):
        Snapshot.model_validate(raw)
