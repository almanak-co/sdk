"""Replay real captured archive inputs through preparation without provider access."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.backtest.config import load_settings
from almanak.backtest.economic_book import EconomicBook
from almanak.backtest.economic_prepare import prepare, provision
from almanak.backtest.history import Capture, ReadEvidence
from almanak.backtest.inputs import HarnessSelection
from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.strategy import StrategyDefinition
from tests.legacy_references import without_retired_fields
from tests.lending_captures import current_config


@pytest.fixture
def acquisition(tmp_path):
    raw = json.loads(
        gzip.decompress(
            Path(__file__)
            .with_name("fixtures")
            .joinpath("aave-v4-economic-archive.json.gz")
            .read_bytes()
        )
    )
    # The capture predates the removal of `repayment_ceiling`; its reads never depended
    # on it, and its source no longer exists to rerun, so the stored config is mapped.
    raw["config"] = without_retired_fields(current_config(raw["config"]))
    # New proxy admission checks use these independently captured same-block reads.
    raw["evidence"].extend(
        json.loads(
            Path(__file__)
            .with_name("fixtures")
            .joinpath("aave-v4-economic-authority-reads.json")
            .read_text()
        )
    )
    config = tmp_path / "config.json"
    backtest_config = tmp_path / "backtest.json"
    backtest_config.write_text(json.dumps(raw["config"].pop("backtest")))
    settings = load_settings(backtest_config)
    config.write_text(json.dumps(raw["config"]))
    author = ForkConfig.model_validate_json(config.read_text())
    reserves = tuple(dict.fromkeys(a.reserve for a in (*author.allocations, *author.debts)))
    directory = tmp_path / "data"
    provision(directory, settings, reserves)
    capture = Capture(directory / "observations.sqlite")
    with capture.connect() as db:
        for row in raw["evidence"]:
            evidence = ReadEvidence.model_validate_json(json.dumps(row))
            db.execute(
                "INSERT INTO observations VALUES (?, ?)",
                (evidence.request.key, evidence.model_dump_json()),
            )
    return (
        SimpleNamespace(
            config=config,
            backtest_config=backtest_config,
            strategy=Path(__file__).parents[1] / "strategies/aave_v4_weth_supply/strategy.py",
            output=directory,
            env_file=None,
            _backtest_inputs=HarnessSelection(
                author,
                settings,
                StrategyDefinition.read(
                    Path(__file__).parents[1] / "strategies/aave_v4_weth_supply/strategy.py"
                ),
            ),
        ),
        settings,
        reserves,
    )


def test_real_archive_preparation_offline_and_idempotent(acquisition):
    args, settings, reserves = acquisition
    result = prepare(args)
    assert result["provider_activity"]["attempts"] == 0
    assert result["provider_activity"]["collection"]["cache_hits"] > 0
    # Two unrelated feed updates no longer create duplicate USDC price rows.
    assert result["dataset"]["records"] == 26
    book = EconomicBook(args.output, reserves, settings.schedule.start, settings.schedule.deadline)
    try:
        for reserve in reserves:
            assert book.at(reserve, settings.schedule.start).price.price > 0
        assert len(book.rates) == 4
        assert_archival_prices(book, args.output, reserves)
    finally:
        book.close()
    assert prepare(args) == result["dataset"]
    changed = settings.model_copy(
        update={"schedule": settings.schedule.model_copy(update={"end": settings.schedule.end + 1})}
    )
    with pytest.raises(ValueError, match="resume cannot change"):
        provision(args.output, changed, reserves)


def assert_archival_prices(book, directory, reserves):
    from eth_abi import decode
    from eth_utils import keccak

    capture = Capture(directory / "observations.sqlite")
    with capture.connect() as db:
        rows = tuple(
            ReadEvidence.model_validate_json(body)
            for _, body in db.execute("SELECT * FROM observations")
        )
    capture.close()
    headers = {
        r.result["hash"]: int(r.result["timestamp"], 16)
        for r in rows
        if r.request.method == "eth_getBlockByNumber"
    }
    calls = [r for r in rows if r.request.method == "eth_call"]
    oracle_selector = "0x" + keccak(text="ORACLE()")[:4].hex()
    price_selector = "0x" + keccak(text="getReservePrice(uint256)")[:4].hex()
    oracles = {
        r.request.params[0]["to"]: decode(("address",), bytes.fromhex(r.result[2:]))[0]
        for r in calls
        if r.request.params[0]["data"] == oracle_selector
    }
    checked = 0
    for reserve in reserves:
        calldata = price_selector + f"{reserve.reserve_id:064x}"
        for row in calls:
            call, block = row.request.params
            if (call["to"], call["data"]) != (oracles[reserve.spoke], calldata):
                continue
            actual = int(row.result, 16)
            timeline = book.prices[reserve]
            at = max(headers[block["blockHash"]], timeline.requirement.start)
            assert timeline.at(at).payload.price == actual
            checked += 1
    assert checked > len(reserves)


def test_interrupted_provision_restores_only_missing_runtime_files(acquisition):
    args, settings, reserves = acquisition
    token = args.output / "gateway-auth.token"
    prior = token.read_text()
    (args.output / "history.json").unlink()
    provision(args.output, settings, reserves)
    assert token.read_text() == prior
    assert (args.output / "history.json").exists()
