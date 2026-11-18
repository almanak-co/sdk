"""Retained fork and archival evidence, without provider or signing authority."""

import gzip
import json
import sqlite3
from functools import cache
from pathlib import Path

from almanak.backtest.cli import provision, sdk_digest, write_json
from almanak.backtest.engine import Journal
from almanak.backtest.evm import EvmHistory
from almanak.backtest.history import ReadEvidence, ReadRequest, validate_read
from almanak.connectors.uniswap_v4.backtest import State, UniswapModel
from almanak.connectors.uniswap_v4.backtest_binding import Manifest, contract_bindings
from almanak.connectors.uniswap_v4.state import Checkpoint
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.strategy import StrategyDefinition
from tests.legacy_references import reviewed_source
from tests.retained_scheduling import checkpoint_input

ROOT = Path(__file__).parents[1]


AUTHOR_BUDGETS = frozenset({"maximum0", "maximum1"})


def scope_only(value):
    """A retained capture in the scope-only interface (blueprint 27).

    Captures predate it: their allocations and capabilities carry the author budget
    fields, and their permission partitions carry Roles quotas. Those are dropped as
    archived history; every other retained fact stays as captured. Liquidity intents and
    mint operations keep their own `maximum0`/`maximum1` deposit amounts: they carry an
    intent or operation `kind`, while an allocation's `kind` names its trading or
    liquidity shape.
    """
    if isinstance(value, list):
        return [scope_only(item) for item in value]
    if not isinstance(value, dict):
        return value
    retired = set(AUTHOR_BUDGETS) if value.get("kind") in (None, "trading", "liquidity") else set()
    if "grants" in value and "profile" in value:
        retired.add("quotas")
    return {name: scope_only(item) for name, item in value.items() if name not in retired}


def scope_only_json(raw):
    return json.dumps(scope_only(json.loads(raw)))


def live_state(name="lp"):
    raw = json.loads((ROOT / f"tests/fixtures/uniswap-v4-{name}-lifecycle.json").read_text())
    return Checkpoint.model_validate_json(json.dumps(checkpoint_input(scope_only(raw))))


def author(name):
    return StrategyDefinition.read(ROOT / f"strategies/uniswap_v4_{name}/strategy.py")


@cache
def historical_bundle(avalanche=False):
    filename = (
        "uniswap-v4-avalanche-history.json.gz"
        if avalanche
        else "uniswap-v4-history-intervals.json.gz"
    )
    return json.loads(gzip.decompress((ROOT / "tests/fixtures" / filename).read_bytes()))


def historical_run(name):
    raw = historical_bundle(name.startswith("avalanche_"))
    run = raw["runs"][name]
    evidence = tuple(
        ReadEvidence.model_validate_json(
            json.dumps(
                {
                    **{k: v for k, v in row.items() if k != "result_digest"},
                    "result": raw["results"][row["result_digest"]],
                }
            )
        )
        for row in run["evidence"]
    )
    return (
        Manifest.model_validate_json(json.dumps(scope_only(run["manifest"]))),
        State.model_validate_json(json.dumps(run["state"])),
        evidence,
    )


class CapturedHistory(EthereumReader):
    def __init__(self, binding, evidence):
        super().__init__(binding.chain, "http://unused.invalid")
        self.binding = binding
        self.records = {e.request.key: e for e in evidence}
        self.used = set()

    def _rpc(self, method, params, *, allow_none=False):
        request = ReadRequest(chain=self.chain, method=method, params=tuple(params))
        validate_read(
            request,
            self.binding.contracts,
            self.binding.logs,
            implementation_lookup=lambda pointer: self.records.get(pointer.key),
        )
        if request.key not in self.records:
            raise DataError("missing_capture", f"missing recorded input {request.key}")
        self.used.add(request.key)
        return self.records[request.key].result


def historical_model(name, *, current=True):
    manifest, expected, evidence = current_historical_run(name) if current else historical_run(name)
    binding = contract_bindings(manifest.settings, manifest.config)[0]
    history = EvmHistory(CapturedHistory(binding, evidence))
    model = UniswapModel(
        manifest.config,
        manifest.settings,
        author(name),
        {binding.chain: history},
        manifest.baselines,
        manifest.authority,
    )
    from tests.dx_replay import current_memory

    expected = expected.model_copy(update={"memory": current_memory(author(name), expected.memory)})
    return manifest, expected, model


def prepare_historical(directory, name):
    manifest, expected, evidence = current_historical_run(name)
    provision(directory, manifest.settings, manifest.config)
    from almanak.backtest.history import Capture

    Capture(directory / "observations.sqlite")
    with sqlite3.connect(directory / "observations.sqlite") as db:
        db.executemany(
            "INSERT INTO observations VALUES (?, ?)",
            ((e.request.key, e.model_dump_json()) for e in evidence),
        )
    # This is a new run of the current implementation against captured inputs.
    # The original capture's SDK digest and state remain untouched in the fixture.
    from almanak.connectors.composition import connector_for
    from almanak.connectors.uniswap_v4.backtest import capability
    from tests.dx_replay import current_memory

    connector = connector_for(manifest.config)
    current = manifest.model_copy(
        update={
            "sdk_digest": sdk_digest(),
            "strategy": author(name),
            "capabilities": tuple(
                capability(connector, item.chain) for item in manifest.capabilities
            ),
        }
    )
    expected = expected.model_copy(update={"memory": current_memory(author(name), expected.memory)})
    write_json(directory / "manifest.json", current)
    Journal(directory / "state.sqlite", State).create(current.initial)
    return current, expected


def current_historical_run(name):
    prefix = "calibrated" if name == "avalanche_trading" else "direct-close"
    path = ROOT / f"tests/fixtures/uniswap-v4-{prefix}-{name}-history.json.gz"
    if path.exists():
        return current_captured_historical_run(path, name)
    original, expected, evidence = historical_run(name)
    path = ROOT / f"tests/fixtures/uniswap-v4-dx-{name}-history.json.gz"
    if not path.exists():
        return original, expected, evidence
    raw = json.loads(gzip.decompress(path.read_bytes()))
    supplemental = tuple(
        ReadEvidence.model_validate_json(json.dumps(row)) for row in raw["evidence"]
    )
    manifest = Manifest.model_validate_json(json.dumps(scope_only(raw["manifest"])))
    expected = State.model_validate_json(json.dumps(raw["state"]))
    return manifest, expected, (*evidence, *supplemental)


def current_captured_historical_run(path, name):
    raw = json.loads(gzip.decompress(path.read_bytes()))
    manifest = Manifest.model_validate_json(json.dumps(scope_only(raw["manifest"])))
    definition = author(name)
    captured = manifest.strategy.source.replace("almanak_v3.", "almanak.")
    assert reviewed_source(captured) == definition.source, (
        "component replay permits only the SDK import namespace change, "
        "the reviewed optional-hold guard and the reviewed decision-reason edit"
    )
    # Replay current imports against captured inputs; the original remains historical evidence.
    manifest = manifest.model_copy(update={"strategy": definition, "sdk_digest": sdk_digest()})
    expected = State.model_validate_json(json.dumps(raw["state"]))
    evidence = tuple(ReadEvidence.model_validate_json(json.dumps(row)) for row in raw["evidence"])
    return manifest, expected, evidence
