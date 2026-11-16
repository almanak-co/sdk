"""Current sandboxed LP references reproduce captures after an exact helper extraction."""

import json

import httpx
import pytest

from almanak.backtest.engine import Journal, Simulator
from almanak.connectors.composition import connector_from_wire
from almanak.strategy import StrategyDefinition
from almanak.worker_session import preloaded_workers
from tests.qa.catalog import ROOT
from tests.qa.lp_refactor import archived_reference, assert_helper_extraction
from tests.qa.retained import current_value
from tests.qa.test_direct_lp_evidence import PREPARED
from tests.qa.test_pancakeswap_v3_direct import BASE as PANCAKE
from tests.qa.test_pancakeswap_v3_direct import NETWORKS
from tests.qa.test_slipstream import PREPARED as SLIPSTREAM
from tests.qa.test_uniswap_v3 import artifact

CASES = (
    *((package, prefix, prefix, reference) for package, prefix, reference in PREPARED),
    *(
        (PANCAKE, "prepared/" + name, "inputs/" + name, name)
        for name in (f"pancakeswap_v3_{chain}_lp" for chain in NETWORKS)
    ),
    (
        SLIPSTREAM,
        "pinned-aerodrome_slipstream_base_lp",
        "pinned-aerodrome_slipstream_base_lp",
        "aerodrome_slipstream_base_lp",
    ),
)


def test_refactor_matrix_covers_every_changed_reference():
    from tests.qa.lp_refactor import PACKAGE

    rows = json.loads((PACKAGE / "manifest.json").read_text())["files"]
    assert {row["path"] for row in rows} == {f"strategies/{c[3]}/strategy.py" for c in CASES}
    assert len(CASES) == len(rows) == 30


@pytest.mark.parametrize("package,prefix,inputs,reference", CASES, ids=lambda value: str(value))
def test_current_sandboxed_reference_reproduces_complete_prepared_state(
    tmp_path, monkeypatch, package, prefix, inputs, reference
):
    def forbidden(*args, **kwargs):
        raise AssertionError("refactor reproduction attempted external data access")

    monkeypatch.setattr(httpx.Client, "send", forbidden)
    raw = json.loads(artifact(package, prefix + "/run/manifest.json.gz"))
    binding = connector_from_wire(raw["config"]).backtest(raw["settings"]["market_model"])
    manifest = binding.manifest_type.model_validate_json(json.dumps(current_value(raw)))
    assert manifest.strategy == archived_reference(reference)
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    for name in ("dataset.json", "data.sqlite"):
        (dataset / name).write_bytes(artifact(package, inputs + "/dataset/" + name + ".gz"))
    expected_path = tmp_path / "expected.sqlite"
    expected_path.write_bytes(artifact(package, prefix + "/run/state.sqlite.gz"))
    expected = Journal(expected_path, type(manifest.initial)).load()
    current = StrategyDefinition.read(ROOT / "strategies" / reference / "strategy.py")
    book = binding.open_book(manifest.model_copy(update={"dataset": dataset}))
    try:
        driver = binding.driver(manifest.config, manifest.settings, current, book)
        journal = Journal(tmp_path / "current.sqlite", type(manifest.initial))
        journal.create(manifest.initial)
        with preloaded_workers():
            actual = Simulator(journal, driver).run()
        assert actual == expected
        assert binding.report(actual, driver)["closed"]
    finally:
        book.close()


@pytest.mark.parametrize(
    "old,new",
    (
        ("fraction_bps=5000", "fraction_bps=4000"),
        ("started is None", "started is not None"),
        ("return context.actions.close_liquidity", "return context.actions.exit"),
    ),
)
def test_source_equivalence_refuses_changed_economic_decisions(old, new):
    reference = "uniswap_v3_base_lp"
    before = archived_reference(reference)
    current = StrategyDefinition.read(ROOT / "strategies" / reference / "strategy.py")
    assert old in current.source
    with pytest.raises(AssertionError):
        assert_helper_extraction(before.source, current.source.replace(old, new))
