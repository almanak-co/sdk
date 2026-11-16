"""Fresh prepared pool evidence, kept separate from original historical captures."""

import gzip
import json
import lzma
from functools import cache

from almanak.backtest.cli import sdk_digest
from almanak.backtest.inputs import HarnessSelection
from almanak.connectors.uniswap_v4.economic_binding import Manifest
from almanak.connectors.uniswap_v4.economic_book import PoolBook
from almanak.connectors.uniswap_v4.economic_model import EconomicModel
from almanak.connectors.uniswap_v4.economic_state import State
from almanak.strategy import StrategyDefinition
from tests.uniswap_support import ROOT, scope_only


@cache
def captured(chain, kind):
    path = ROOT / f"tests/fixtures/uniswap-v4-{chain}-economic-{kind}.json.gz"
    return json.loads(gzip.decompress(path.read_bytes()))


def prepared(tmp_path, chain="ethereum", kind="trading"):
    raw = captured(chain, kind)
    name = f"uniswap-v4-{chain}-economic-{kind}"
    binary = ROOT / f"tests/fixtures/{name}.sqlite.xz"
    prefix = {
        "avalanche": "avalanche_",
        "bnb": "bnb_",
        "polygon": "polygon_",
        "robinhood": "robinhood_",
    }.get(chain, "")
    return prepared_capture(
        tmp_path,
        raw["manifest"],
        raw["state"],
        # Preserve the exact published manifest bytes, including indentation/newline.
        json.dumps(raw["dataset_manifest"], indent=2) + "\n",
        lzma.decompress(binary.read_bytes()),
        f"uniswap_v4_{prefix}{kind}",
    )


def prepared_capture(tmp_path, run_manifest, state, dataset_manifest, data, strategy_name):
    """Replay inputs for one economic capture: its run manifest, final state and dataset."""
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    (dataset / "data.sqlite").write_bytes(data)
    (dataset / "dataset.json").write_text(dataset_manifest)
    manifest = Manifest.model_validate_json(json.dumps(scope_only(run_manifest)))
    manifest = manifest.model_copy(
        update={
            "dataset": dataset,
            "sdk_digest": sdk_digest(),
            "strategy": StrategyDefinition.read(
                ROOT / "strategies" / strategy_name / "strategy.py"
            ),
        }
    )
    book = PoolBook(dataset, manifest.config, manifest.settings, manifest.dataset_sha256)
    model = EconomicModel(manifest.config, manifest.settings, manifest.strategy, book)
    expected = State.model_validate_json(json.dumps(state))
    from tests.dx_replay import current_memory

    expected = expected.model_copy(
        update={"memory": current_memory(manifest.strategy, expected.memory)}
    )
    return manifest, expected, model


@cache
def quoter_evidence(chain):
    path = ROOT / f"tests/fixtures/uniswap-v4-{chain}-economic-quoter.json.gz"
    return json.loads(gzip.decompress(path.read_bytes()))


def archive_selection(name, settings):
    """Typed private inputs for component replays of the archived author fixture."""
    from scripts.legacy_configuration.author import read_author
    from tests.legacy_references import harness_directory

    author = read_author(harness_directory(name) / "config.yaml")
    strategy = StrategyDefinition.read(ROOT / "strategies" / name / "strategy.py")
    return HarnessSelection(author, settings, strategy)


def provision_archive(tmp_path, chain="ethereum", kind="trading"):
    from almanak.connectors.uniswap_v4.economic_prepare import provision

    manifest, expected, model = prepared(tmp_path, chain, kind)
    model.book.close()
    directory = tmp_path / "acquisition"
    directory.mkdir()
    from scripts.legacy_configuration.author import read_author
    from scripts.legacy_configuration.backtest import read_backtest

    name = f"uniswap_v4_{'avalanche_' if chain == 'avalanche' else ''}{kind}"
    from tests.legacy_references import harness_directory

    root = harness_directory(name)
    author = read_author(root / "config.yaml")
    settings = read_backtest(root / "backtest-economic.yaml", author)
    from almanak.connectors.uniswap_v4.backtest import parameters
    from almanak.triggers import IntervalSelection

    interval = IntervalSelection(
        configured=author.decision_trigger,
        override_seconds=settings.schedule.decision_interval_override_seconds,
    )
    config = parameters(author, settings, interval)
    provision(directory, config, settings)
    manifest = manifest.model_copy(update={"config": config, "settings": settings})
    archive = ROOT / f"tests/fixtures/uniswap-v4-{chain}-economic-{kind}-archive.sqlite.xz"
    (directory / "observations.sqlite").write_bytes(lzma.decompress(archive.read_bytes()))
    return directory, manifest, expected


def assert_archive_payloads(actual, original):
    rows = actual.db.execute("SELECT body FROM inputs ORDER BY source,time").fetchall()
    old_rows = original.db.execute("SELECT body FROM inputs ORDER BY source,time").fetchall()
    baselines = set()
    old_baselines = set()
    for row, old_row in zip(rows, old_rows, strict=True):
        new, old = json.loads(row[0]), json.loads(old_row[0])
        # Only the simulation fund binding changes; archival pool facts stay exact.
        new_digest = new["payload"].pop("baseline_digest")
        old_digest = old["payload"].pop("baseline_digest")
        baselines.add(new_digest)
        old_baselines.add(old_digest)
        new["evidence"] = [e for e in new["evidence"] if e != new_digest]
        old["evidence"] = [e for e in old["evidence"] if e != old_digest]
        assert new == old
        current = json.loads(
            actual.db.execute("SELECT body FROM evidence WHERE digest=?", (new_digest,)).fetchone()[
                0
            ]
        )
        prior = json.loads(
            original.db.execute(
                "SELECT body FROM evidence WHERE digest=?", (old_digest,)
            ).fetchone()[0]
        )
        current["target"]["fund"].pop("address")
        prior["target"]["fund"].pop("address")
        assert current == prior
    assert {
        r for r in actual.db.execute("SELECT digest,body FROM evidence") if r[0] not in baselines
    } == {
        r
        for r in original.db.execute("SELECT digest,body FROM evidence")
        if r[0] not in old_baselines
    }
