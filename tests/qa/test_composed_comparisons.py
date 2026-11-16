"""Every composed DEX component must retain independent comparison evidence."""

import json
from types import SimpleNamespace

import pytest

from almanak.connectors.identifiers import ConnectorId
from tests.qa import composed_comparisons as comparisons


@pytest.mark.parametrize(
    "connector,adapter",
    [
        ("uniswap_v3", "compare_v3"),
        ("uniswap_v4", "compare_v4"),
        ("curve", "compare_curve"),
        ("pancakeswap_v3", "compare_pancake"),
        ("aerodrome", "compare_aerodrome"),
        ("aerodrome_slipstream", "compare_slipstream"),
    ],
)
@pytest.mark.parametrize("equal", [True, False])
def test_composed_checks_each_dex_and_retains_failures(
    tmp_path, monkeypatch, connector, adapter, equal
):
    (tmp_path / "run").mkdir()
    (tmp_path / "run/manifest.json").write_text("{}")
    dataset = tmp_path / "dataset"
    component = object()
    closed = []
    book = SimpleNamespace(
        components={ConnectorId(connector): component}, close=lambda: closed.append(True)
    )
    monkeypatch.setattr(
        comparisons.Manifest, "model_validate_json", lambda _: SimpleNamespace(dataset=dataset)
    )
    monkeypatch.setattr(comparisons.ComposedEconomic, "open_book", lambda _: book)

    def compare(actual, path, env, records):
        assert actual is component and path == dataset / connector and env == "selected.env"
        records.append({"equal": equal, "connector": connector})

    monkeypatch.setattr(comparisons, adapter, compare)
    if equal:
        comparisons.compare_components(tmp_path, "selected.env")
    else:
        with pytest.raises(AssertionError):
            comparisons.compare_components(tmp_path, "selected.env")
    assert closed == [True]
    assert json.loads((tmp_path / f"contract-comparisons-{connector}.json").read_text()) == [
        {"equal": equal, "connector": connector}
    ]
