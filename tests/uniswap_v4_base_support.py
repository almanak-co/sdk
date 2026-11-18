"""Current Base reference replay over retained, independently acquired pool state."""

import gzip
import json
import lzma
from pathlib import Path

from almanak.backtest.cli import sdk_digest
from almanak.connectors.uniswap_v4.economic_binding import Manifest
from almanak.connectors.uniswap_v4.economic_book import PoolBook
from almanak.connectors.uniswap_v4.economic_model import EconomicModel
from almanak.connectors.uniswap_v4.economic_state import State
from almanak.strategy import StrategyDefinition
from tests.uniswap_support import scope_only

ROOT = Path(__file__).parents[1]


def prepared(directory, style="trading"):
    name = f"uniswap-v4-base-economic-{style}"
    raw = json.loads(gzip.decompress((ROOT / f"tests/fixtures/{name}.json.gz").read_bytes()))
    dataset = directory / "dataset"
    dataset.mkdir()
    (dataset / "data.sqlite").write_bytes(
        lzma.decompress((ROOT / f"tests/fixtures/{name}.sqlite.xz").read_bytes())
    )
    (dataset / "dataset.json").write_text(raw["dataset_manifest_bytes"])
    manifest = Manifest.model_validate_json(json.dumps(scope_only(raw["manifest"])))
    manifest = manifest.model_copy(
        update={
            "dataset": dataset,
            "sdk_digest": sdk_digest(),
            "strategy": StrategyDefinition.read(
                ROOT / f"strategies/uniswap_v4_base_{style}/strategy.py"
            ),
        }
    )
    book = PoolBook(dataset, manifest.config, manifest.settings, manifest.dataset_sha256)
    model = EconomicModel(manifest.config, manifest.settings, manifest.strategy, book)
    return manifest, State.model_validate_json(json.dumps(raw["state"])), model


def historical(style):
    from almanak.backtest.evm import EvmHistory
    from almanak.connectors.uniswap_v4.backtest import UniswapModel
    from almanak.connectors.uniswap_v4.backtest_binding import contract_bindings
    from tests.uniswap_support import CapturedHistory, current_captured_historical_run

    path = ROOT / f"tests/fixtures/uniswap-v4-base-{style}-history.json.gz"
    manifest, expected, evidence = current_captured_historical_run(path, "base_" + style)
    (binding,) = contract_bindings(manifest.settings, manifest.config)
    reader = CapturedHistory(binding, evidence)
    model = UniswapModel(
        manifest.config,
        manifest.settings,
        manifest.strategy,
        {binding.chain: EvmHistory(reader)},
        manifest.baselines,
        manifest.authority,
    )
    return manifest, expected, model, reader
