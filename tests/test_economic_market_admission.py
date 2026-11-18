"""Synthetic reserve relocation tests admission; it is not new historical coverage."""

import json

import pytest
from eth_utils import keccak

from almanak.backtest.economic_book import EconomicBook
from almanak.backtest.economic_prepare import prepare, provision
from almanak.backtest.history import Capture, ReadEvidence
from almanak.backtest.inputs import HarnessSelection
from almanak.connectors.aave_v4.deployments import IMPLEMENTATION_SLOT
from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.gateway.ethereum import DataError
from tests.test_economic_prepare import acquisition as acquisition


def selector(signature):
    return "0x" + keccak(text=signature)[:4].hex()


def relocated_reads(rows, source, mutation):
    oracle = next(
        "0x" + row.result[-40:]
        for row in rows
        if row.request.method == "eth_call"
        and row.request.params[0] == {"to": source.spoke, "data": selector("ORACLE()")}
    )
    implementation = next(
        "0x" + row.result[-40:]
        for row in rows
        if row.request.method == "eth_getStorageAt"
        and tuple(row.request.params[:2]) == (source.spoke, IMPLEMENTATION_SLOT)
    )
    for row in rows:
        changed = relocate_reserve_read(row, source, oracle, mutation)
        target = oracle if mutation == "oracle" else implementation
        if mutation in ("oracle", "implementation") and row.request.method == "eth_getCode":
            if row.request.params[0] == target:
                changed = changed.model_copy(update={"result": "0x00"})
        yield changed


def relocate_reserve_read(row, source, oracle, mutation):
    signatures = {
        selector(s)
        for s in (
            "getReserve(uint256)",
            "getReserveConfig(uint256)",
            "getDynamicReserveConfig(uint256,uint32)",
            "getReserveSource(uint256)",
            "getReservePrice(uint256)",
        )
    }
    if row.request.method != "eth_call":
        return row
    call = row.request.params[0]
    data = call["data"]
    if call["to"] not in (source.spoke, oracle) or data[:10] not in signatures:
        return row
    if int(data[10:74], 16) != source.reserve_id:
        return row
    request = row.request.model_copy(
        update={
            "params": (
                {**call, "data": data[:10] + f"{123:064x}" + data[74:]},
                *row.request.params[1:],
            )
        }
    )
    result = row.result
    if mutation == "asset" and data[:10] == selector("getReserve(uint256)"):
        result = "0x" + "00" * 12 + "ab" * 20 + result[66:]
    return row.model_copy(update={"request": request, "result": result})


def relocate(acquisition, mutation):
    args, settings, reserves = acquisition
    source = reserves[0]
    raw = json.loads(args.config.read_text())
    for allocation in raw["allocations"]:
        if allocation["reserve"] == source.model_dump(mode="json"):
            allocation["reserve"]["reserve_id"] = 123
    args.config.write_text(json.dumps(raw))
    author = ForkConfig.model_validate_json(args.config.read_text())
    args._backtest_inputs = HarnessSelection(author, settings, args._backtest_inputs.strategy)
    selected = tuple(dict.fromkeys(a.reserve for a in (*author.allocations, *author.debts)))
    old = Capture(args.output / "observations.sqlite")
    try:
        with old.connect() as db:
            rows = tuple(
                ReadEvidence.model_validate_json(body)
                for (body,) in db.execute("SELECT body FROM observations")
            )
    finally:
        old.close()
    args.output = args.output.with_name("relocated")
    provision(args.output, settings, selected)
    capture = Capture(args.output / "observations.sqlite")
    try:
        with capture.connect() as db:
            db.executemany(
                "INSERT INTO observations VALUES (?, ?)",
                (
                    (row.request.key, row.model_dump_json())
                    for row in relocated_reads(rows, source, mutation)
                ),
            )
    finally:
        capture.close()
    return args, settings, selected


@pytest.mark.parametrize("mutation", [None, "asset", "implementation", "oracle"])
def test_preparation_verifies_configured_reserve_and_freezes_admission_reads(acquisition, mutation):
    args, settings, reserves = relocate(acquisition, mutation)
    assert reserves[0].reserve_id == 123
    if mutation is not None:
        with pytest.raises(DataError, match="requested asset|verified revisions|verified revision"):
            prepare(args)
        assert not (args.output / "dataset.json").exists()
        return
    result = prepare(args)
    assert result["provider_activity"]["attempts"] == 0
    book = EconomicBook(args.output, reserves, settings.schedule.start, settings.schedule.deadline)
    try:
        assert book.at(reserves[0], settings.schedule.start).risk.reserve.source == reserves[0]
        retained = [
            json.loads(body) for (body,) in book.reader.db.execute("SELECT body FROM evidence")
        ]
        code_targets = {
            row["request"]["params"][0]
            for row in retained
            if row.get("request", {}).get("method") == "eth_getCode"
        }
        assert {reserves[0].spoke, reserves[0].hub} <= code_targets
        assert prepare(args) == result["dataset"]
    finally:
        book.close()
