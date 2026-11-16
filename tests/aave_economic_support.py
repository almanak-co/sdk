"""Published Aave histories and actual reference states, with exact raw bytes."""

import gzip
import json
import lzma
from functools import cache

from almanak.backtest.cli import sdk_digest, write_json
from almanak.backtest.economic_state import EconomicState
from almanak.connectors.aave_v4.economic_binding import AaveEconomic, Manifest
from almanak.strategy import StrategyDefinition
from tests.lending_captures import current_capture
from tests.uniswap_support import ROOT


def strategy_name(name):
    return "aave_v4_weth_supply" if name == "ethereum-reference" else f"aave_v4_{name}_borrowing"


@cache
def captured(name):
    path = ROOT / f"tests/fixtures/aave-v4-{name}-economic.json.gz"
    raw = json.loads(gzip.decompress(path.read_bytes()))
    # The recorded manifest predates scope-only authority; read it in today's interface.
    return {**raw, "manifest": current_capture(raw["manifest"])}


def prepared(tmp_path, name="multichain"):
    raw = captured(name)
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    label = f"aave-v4-{name}-economic"
    (dataset / "data.sqlite").write_bytes(
        lzma.decompress((ROOT / f"tests/fixtures/{label}.sqlite.xz").read_bytes())
    )
    write_json(dataset / "dataset.json", raw["dataset_manifest"])
    manifest = Manifest.model_validate_json(json.dumps(raw["manifest"]))
    manifest = manifest.model_copy(
        update={
            "dataset": dataset,
            "sdk_digest": sdk_digest(),
            "strategy": StrategyDefinition.read(
                ROOT / "strategies" / strategy_name(name) / "strategy.py"
            ),
        }
    )
    expected = EconomicState.model_validate_json(json.dumps(raw["state"]))
    from almanak.memory_codec import freeze

    namespace = {}
    exec(compile(manifest.strategy.source, "strategy.py", "exec"), namespace)
    expected = expected.model_copy(
        update={"memory": freeze(namespace, namespace["State"](**expected.memory.values))}
    )
    model = AaveEconomic.driver(
        manifest.config, manifest.settings, manifest.strategy, AaveEconomic.open_book(manifest)
    )
    return manifest, expected, model


def provision_archive(tmp_path, name="multichain"):
    from almanak.backtest.economic_prepare import provision

    manifest, expected, model = prepared(tmp_path, name)
    model.book.close()
    directory = tmp_path / "acquisition"
    reserves = tuple(dict.fromkeys(a.reserve for a in manifest.config.positions))
    provision(directory, manifest.settings, reserves)
    source = ROOT / f"tests/fixtures/aave-v4-{name}-economic-archive.sqlite.xz"
    (directory / "observations.sqlite").write_bytes(lzma.decompress(source.read_bytes()))
    return directory, manifest, expected
