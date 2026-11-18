"""GMX QA separates dataset reconstruction from sealed native contract calibration."""

import hashlib
import json
from pathlib import Path

from almanak.caip import ChainId
from almanak.connectors.gmx_v2.economic_binding import Manifest
from almanak.connectors.gmx_v2.economic_book import MarketBook
from almanak.connectors.gmx_v2.economic_deposit_execution import execute_deposit
from almanak.connectors.gmx_v2.economic_liquidity import LiquidityState, quote_withdrawal
from almanak.connectors.gmx_v2.economic_liquidity_risk import (
    LiquidityRiskState,
    validate_deposit,
    validate_withdrawal,
)
from almanak.connectors.gmx_v2.economic_spot_math import quote_swap
from almanak.connectors.gmx_v2.economic_spot_types import SpotState, SwapCalculation
from almanak.connectors.gmx_v2.economic_valuation import pool_value
from almanak.connectors.gmx_v2.economic_valuation_types import ValuationState
from tests.qa.inputs import write_json

EVIDENCE = Path(__file__).resolve().parents[2] / "docs/internal/reports/evidence"
CALIBRATIONS = {
    (ChainId("eip155:42161"), "swap"): (
        "2026-09-09-gmx-prepared/arbitrum-spot-reader-comparison.json",
        "ecad2e16df557e9644bdf8eaf232acf7f8d4b9172d744be98917cd36d41f29ed",
    ),
    (ChainId("eip155:43114"), "swap"): (
        "2026-09-09-gmx-prepared/avalanche-spot-reader-comparison.json",
        "0ec3b6048179443fbfca56f164c847935b12c971857e534dd2451ebead18eb1c",
    ),
    (ChainId("eip155:42161"), "liquidity"): (
        "2026-09-09-gmx-finish/arbitrum-gm-executor/manifest.json",
        "c3926b0ef56c31dbd3861e52d803991cfe0460d9b71f7c4471a4b924e191393d",
    ),
    (ChainId("eip155:43114"), "liquidity"): (
        "2026-09-15-enablement/gmx-avalanche-gm-executor-d20a86f7/manifest.json",
        "a9431870aaece6120f7580a793bed90e8542a02740e6623f41bd81b2f098e687",
    ),
}


def sealed_json(path, digest):
    body = path.read_bytes()
    if hashlib.sha256(body).hexdigest() != digest:
        raise AssertionError(f"GMX required calibration hash differs: {path.name}")
    return json.loads(body)


def required_calibration(chain, family):
    try:
        relative, digest = CALIBRATIONS[chain, family]
    except KeyError as error:
        raise AssertionError(
            f"GMX required {family} native calibration is unavailable for {chain}"
        ) from error
    path = EVIDENCE / relative
    return path, digest, sealed_json(path, digest)


def compare_swap_packet(packet, chain):
    state = SpotState.model_validate_json(json.dumps(packet["state"]))
    assert state.market.chain.chain_id == chain
    rows = []
    for case in packet["cases"]:
        retained = SwapCalculation.model_validate_json(json.dumps(case["calculation"]))
        calculated = quote_swap(state, retained.asset_in, retained.amount_in)
        assert calculated == retained
        actual = case["contract_result"]
        expected = [
            calculated.amount_out,
            calculated.impact_amount,
            [
                calculated.fees.receiver_amount,
                calculated.fees.pool_amount,
                calculated.fees.after_fees,
                "0x" + "00" * 20,
                state.fees.ui_factor,
                calculated.fees.ui_amount,
            ],
        ]
        rows.append(
            {
                "equal": actual == expected,
                "native": actual,
                "modeled": expected,
                "block": state.block.model_dump(mode="json"),
                "input": {"asset": str(calculated.asset_in), "amount": calculated.amount_in},
                "comparison": "sealed_native_reader_swap_calibration_not_dataset_quote",
            }
        )
    assert rows and all(row["equal"] for row in rows)
    return rows


def replay_gm_row(row):
    state = ValuationState.model_validate_json(json.dumps(row["state"]))
    risk = LiquidityRiskState.model_validate_json(json.dumps(row["risk"]))
    amounts = [value["amount"] for value in row["request"]["escrow"]]
    if row["kind"] == "deposit":
        calculation = execute_deposit(state, *amounts)
        validate_deposit(state, risk, calculation)
        transfers, supply = [0, 0, calculation.shares], calculation.shares
    else:
        assert row["kind"] == "withdrawal"
        value = pool_value(state, maximize=False).pool_usd_e30
        calculation = quote_withdrawal(
            LiquidityState(
                action="WITHDRAWAL",
                spot=state.spot,
                pool_value_usd_e30=value,
                share_supply=state.share_supply,
            ),
            amounts[0],
        )
        validate_withdrawal(state, risk, calculation)
        transfers, supply = (
            [calculation.long_amount, calculation.short_amount, 0],
            -calculation.shares,
        )
    assert calculation.model_dump(mode="json") == row["calculation"]
    assert row["settled_block"]["timestamp"] == state.spot.block.timestamp
    assert row["receipt"]["status"] == "0x1"
    equal = transfers == row["actual"] and supply == row["share_supply_delta"]
    return {
        "equal": equal,
        "kind": row["kind"],
        "native": row["actual"],
        "modeled": transfers,
        "share_supply_delta": supply,
        "block": state.spot.block.model_dump(mode="json"),
        "receipt": row["receipt"]["transactionHash"],
        "comparison": "sealed_actual_gm_executor_calibration_not_reader_deposit_quote",
    }


def compare_gm_packet(path, packet, chain):
    if "plan" in packet:
        return compare_captured_gm_packet(path, packet, chain)
    assert packet["profile"] in ("eoa", "safe_roles")
    files = {}
    for name, digest in packet["files"].items():
        relative = Path(name)
        assert not relative.is_absolute() and ".." not in relative.parts
        if relative.suffix == ".json":
            files[name] = sealed_json(path.parent / relative, digest)
        else:
            assert hashlib.sha256((path.parent / relative).read_bytes()).hexdigest() == digest
    result = files["result.json"]
    assert result["status"] == "passed" and result["phases"][-1] == "closed"
    assert len(files["comparisons.json"]) == result["comparisons"] == 4
    rows = [replay_gm_row(row) for row in files["comparisons.json"]]
    assert all(ChainId(row["block"]["chain"]["chain_id"]) == chain for row in rows)
    assert all(row["equal"] for row in rows)
    return rows


def captured_files(path, packet):
    files = {}
    for name, metadata in packet["files"].items():
        relative = Path(name)
        assert not relative.is_absolute() and ".." not in relative.parts
        body = (path.parent / relative).read_bytes()
        assert len(body) == metadata["bytes"]
        assert hashlib.sha256(body).hexdigest() == metadata["sha256"]
        if relative.suffix == ".json":
            files[name] = json.loads(body)
    return files


def compare_captured_gm_packet(path, packet, chain):
    plan = packet["plan"]
    assert packet["schema"] == plan["schema"] == 1
    assert plan["profile"] in ("eoa", "safe_roles")
    assert ChainId(plan["chain"]) == chain
    files = captured_files(path, packet)
    result = files["result.json"]
    assert result["status"] == "passed" and result["phases"][-1] == "closed"
    assert result["source"] == plan["source"]
    assert len(files["comparisons.json"]) == result["comparisons"] == 4
    assert [row["kind"] for row in files["comparisons.json"]] == [
        "deposit",
        "deposit",
        "withdrawal",
        "withdrawal",
    ]
    rows = [replay_captured_gm_row(row) for row in files["comparisons.json"]]
    assert all(ChainId(row["block"]["chain"]["chain_id"]) == chain for row in rows)
    assert all(row["equal"] for row in rows)
    return rows


def replay_captured_gm_row(row):
    market = row["state"]["spot"]["market"]
    assets = [market[key] for key in ("long_asset", "short_asset", "share_asset")]
    before, after = row["fund_balances_before"], row["fund_balances_after"]
    actual, expected = row["actual_fund_deltas"], row["expected_fund_deltas"]
    assert set(before) == set(after) == set(actual) == set(expected) == set(assets)
    assert actual == {asset: after[asset] - before[asset] for asset in assets}
    assert row["share_supply_delta"] == row["share_supply_after"] - row["share_supply_before"]
    assert row["expected_share_supply_delta"] == row["share_supply_delta"]
    assert actual == expected
    assert row["prestate_block"] == row["state"]["spot"]["block"]
    verify_captured_keeper(row)
    # Preserve the original calibration's ordered long/short/GM comparison.
    return replay_gm_row({**row, "actual": [actual[asset] for asset in assets]})


def verify_captured_keeper(row):
    transaction, receipt = row["keeper_transaction"], row["receipt"]
    block = row["settled_block"]
    identity = row["request"]["events"][0]["identity"]
    assert block["chain"] == row["prestate_block"]["chain"]
    assert block["number"] > row["prestate_block"]["number"]
    assert transaction["blockHash"] == receipt["blockHash"] == block["hash"]
    assert transaction["blockNumber"] == receipt["blockNumber"] == hex(block["number"])
    assert transaction["hash"] == receipt["transactionHash"]
    assert transaction["from"] == receipt["from"]
    assert transaction["to"] == receipt["to"] == identity["handler"]
    assert transaction["input"].startswith("0x") and len(transaction["input"]) > 10


def compare_book(book, dataset, env_file, records):
    # MarketBook has already independently reconstructed every retained canonical frame.
    # Contract calibrations below remain separate from dataset-specific quote measurements.
    if not isinstance(book, MarketBook):
        raise TypeError("GMX comparison requires its validated prepared market book")
    family = comparison_family(book.settings)
    for target, timeline in book.timelines.items():
        path, digest, packet = required_calibration(target.fund.chain.chain_id, family)
        rows = calibration_rows(path, packet, target.fund.chain.chain_id, family)
        metadata = {
            "allocation": target.allocation,
            "chain": str(target.fund.chain.chain_id),
            "calibration": str(path.relative_to(EVIDENCE)),
            "calibration_sha256": digest,
            "dataset_frames_reconstructed": len(timeline.rows),
            "dataset_publications_reconstructed": sum(
                len(row.payload.opportunities) for row in timeline.rows
            ),
            "dataset_quote_comparison": False,
        }
        records.extend({**row, **metadata} for row in rows)


def comparison_family(settings):
    if settings.position_model:
        return "perpetual"
    if settings.liquidity_model:
        return "liquidity"
    return "swap"


def calibration_rows(path, packet, chain, family):
    if family == "swap":
        return compare_swap_packet(packet, chain)
    if family == "liquidity":
        return compare_gm_packet(path, packet, chain)
    if family == "perpetual":
        raise AssertionError(
            f"GMX required perpetual native calibration is unavailable for {chain}"
        )
    raise ValueError(f"unknown GMX calibration family: {family}")


def compare_pool(directory, env_file):
    manifest = Manifest.model_validate_json((directory / "run/manifest.json").read_text())
    records: list[dict[str, object]] = []
    book = MarketBook(manifest.dataset, manifest.config, manifest.settings, manifest.dataset_sha256)
    try:
        compare_book(book, manifest.dataset, env_file, records)
        assert records and all(row["equal"] for row in records)
    finally:
        book.close()
        write_json(directory / "gmx-calibration-comparisons.json", records)
