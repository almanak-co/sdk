"""Adversarial controls against financial evidence admission, using a real fork DB."""

from __future__ import annotations

import json
import shutil
import sqlite3
from dataclasses import asdict
from pathlib import Path

import pytest

from qa_lab import accounting_admission as admission
from qa_lab import accounting_dedicated as dedicated
from qa_lab import qa_coverage as qa
from qa_lab import run_accounting_matrix as runner

ROOT = Path(__file__).resolve().parents[3]
DB = ROOT / "tests/fixtures/accounting/harness/lp-anvil.sqlite"


def make_bundle(directory: Path) -> tuple[Path, dict]:
    row = next(
        row for row in runner._load_matrix(ROOT / "qa_lab/accounting-matrix.yml") if row.id == "lp-uniswap_v3-arbitrum"
    )
    strategy = runner._prepare_temp_strategy(row, directory)
    bundle = strategy.parent
    shutil.copy2(DB, strategy / "almanak_state.db")
    deployment = dedicated.unique_deployment_id(DB)
    source = admission.runtime_source(ROOT)
    sdk = {
        "commit": source["commit"],
        "branch": "test",
        "dirty": False,
        "sdk_version": "0.1.0",
        "source": "executing-worktree",
    }
    runtime = {
        "sdk": sdk,
        "source": source,
        "deployment_id": deployment,
        "source_unchanged": True,
        "started_at": "2026-09-06T00:00:00Z",
        "finished_at": "2026-09-06T00:10:00Z",
    }
    for name, value in {
        "runtime.json": runtime,
        "status.json": {**asdict(row), "row_id": row.id, "status": "PASS"},
        "accountant.json": admission.canonical_accountant(DB, "lp", deployment),
        "chain-witnesses.json": {"status": "UNMEASURED", "transactions": []},
    }.items():
        (bundle / name).write_text(json.dumps(value))
    (bundle / "strat.log").write_text("Compiled LP_OPEN\n")
    (bundle / "teardown.log").write_text("Compiled LP_CLOSE\n")
    return bundle, {
        "commit": source["commit"],
        "branch": "test",
        "dirty": False,
        "sdk_version": "0.1.0",
        "source": "executing-worktree",
    }


def seal(bundle: Path, sdk: dict, store: Path, books_id: str = "books.matrix.lp-uniswap_v3-arbitrum") -> Path:
    return qa.seal_accounting_bundle(
        bundle=bundle,
        store=store,
        catalog_path=qa.DEFAULT_CATALOG,
        books_id=books_id,
        network="anvil",
        exec_path="eoa",
        sdk_provenance=sdk,
    )


@pytest.mark.parametrize("mutation", ["g14", "remove-cell", "duplicate-cell", "diagnostic", "g6", "score-count"])
def test_fabricated_accountant_cannot_be_admitted(tmp_path: Path, mutation: str) -> None:
    bundle, sdk = make_bundle(tmp_path)
    path = bundle / "accountant.json"
    payload = json.loads(path.read_text())
    if mutation == "g14":
        assert payload["cells"]["G14"] == "XFAIL"
        payload["cells"]["G14"] = "PASS"
        next(cell for cell in payload["cell_details"] if cell["id"] == "G14")["status"] = "PASS"
        payload["scores"]["passed"] += 1
        payload["scores"]["xfailed"] -= 1
    elif mutation == "remove-cell":
        payload["cell_details"].pop()
    elif mutation == "duplicate-cell":
        payload["cell_details"][0] = payload["cell_details"][1]
    elif mutation == "diagnostic":
        payload["cell_details"][0]["diagnostic"] = "Everything reconciles"
    elif mutation == "g6":
        payload["g6_decomposition"]["gap_usd"] = "0"
    else:
        payload["scores"]["failed"] = 0
        payload["scores"]["total"] = 1
    path.write_text(json.dumps(payload))
    store = tmp_path / "store"
    with pytest.raises(ValueError, match="database rescore"):
        seal(bundle, sdk, store)
    assert not (store / "index/accounting_dedicated_runs.jsonl").exists()


@pytest.mark.parametrize(
    "books_id",
    [
        "books.matrix.lp_pancakeswap-arbitrum",
        "books.matrix.lp-uniswap_v3-ethereum",
        "books.uniswap_v3.lp_simple.arbitrum",
        "books.matrix.lp_dual-uniswap_v3-arbitrum",
    ],
)
def test_same_fixture_cannot_cross_paint_protocol_chain_or_lifecycle(tmp_path: Path, books_id: str) -> None:
    bundle, sdk = make_bundle(tmp_path)
    with pytest.raises(ValueError, match="chain disagrees|does not own"):
        seal(bundle, sdk, tmp_path / "store", books_id)


@pytest.mark.parametrize(
    "mutation", ["config", "strategy", "runtime-source", "runtime-deployment", "missing-recipe", "extra-module"]
)
def test_recipe_and_runtime_are_bound_to_owned_bytes(tmp_path: Path, mutation: str) -> None:
    bundle, sdk = make_bundle(tmp_path)
    if mutation == "extra-module":
        (bundle / "strategy/injected.py").write_text("raise RuntimeError('extra source')\n")
    elif mutation == "missing-recipe":
        (bundle / "recipe.json").unlink()
    elif mutation in {"config", "strategy"}:
        path = bundle / "strategy" / ("config.json" if mutation == "config" else "strategy.py")
        path.write_text(path.read_text() + "\n")
    else:
        path = bundle / "runtime.json"
        payload = json.loads(path.read_text())
        if mutation == "runtime-source":
            payload["source"]["commit"] = "0" * 40
        else:
            payload["deployment_id"] = "deployment:other"
        path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="resource mismatch|runner-recorded|Run-time source"):
        seal(bundle, sdk, tmp_path / "store")


def test_full_submitted_set_includes_approvals_without_double_counting() -> None:
    rows = dedicated.ledger_transactions(DB)
    assert len(rows) == 12
    assert len({row["id"] for row in rows}) == 4
    assert len({row["tx_hash"] for row in rows}) == 12


def test_duplicate_ledger_owner_cannot_inflate_executed_shape(tmp_path: Path) -> None:
    db = tmp_path / "copy.db"
    shutil.copy2(DB, db)
    with sqlite3.connect(db) as connection:
        connection.execute("UPDATE transaction_ledger SET tx_hash=(SELECT tx_hash FROM transaction_ledger LIMIT 1)")
    with pytest.raises(ValueError, match="multiple ledger owners"):
        dedicated.ledger_transactions(db)


@pytest.mark.parametrize("before,delta", [(10**24, -1), (100, -95), (100, -105)])
def test_zero_close_never_excuses_unexplained_money(before: int, delta: int) -> None:
    assert dedicated._balance_reconciliation_mode(before, 0, delta) == "mismatch"


def test_every_matrix_row_has_one_exact_board_recipe() -> None:
    catalog = qa.build_accounting_catalog(qa_catalog=qa._load_catalog(qa.DEFAULT_CATALOG))
    rows = runner._load_matrix(ROOT / "qa_lab/accounting-matrix.yml")
    recipes = [cell for cell in catalog["cells"] if cell.get("matrix_row_id")]
    assert len(recipes) == len(rows) == 26
    assert {cell["matrix_row_id"] for cell in recipes} == {row.id for row in rows}
    assert len({cell["books_id"] for cell in recipes}) == len(rows)
    assert all(cell["coverage_status"] == "CATALOG_ONLY" for cell in catalog["cells"] if not cell.get("matrix_row_id"))


def test_exited_strategy_does_not_burn_the_shape_deadline(tmp_path: Path) -> None:
    class Exited:
        def poll(self):
            return 1

    assert runner._poll_shape(tmp_path / "absent.db", {"SUPPLY": 1}, float("inf"), process=Exited()) == (False, {})


@pytest.mark.parametrize("persisted", [1, 3])
def test_fork_census_recovers_lost_retries_and_identifies_shared_blocks(monkeypatch, persisted: int) -> None:
    wallet = "0x" + "11" * 20
    hashes = ["0x" + char * 64 for char in "abc"]
    transactions = [{"hash": tx_hash, "from": wallet} for tx_hash in hashes]

    def rpc(_url, method, _params):
        return {
            "anvil_metadata": {"forkedNetwork": {"forkBlockNumber": 9}},
            "eth_blockNumber": "0xa",
            "eth_chainId": "0xa4b1",
            "eth_getTransactionByHash": transactions[-1],
            "eth_getBlockByNumber": {"number": "0xa", "transactions": transactions},
        }[method]

    monkeypatch.setattr(dedicated, "_rpc", rpc)
    rows, census = dedicated._fork_submissions(
        "http://localhost:8545", [{"id": key, "tx_hash": key} for key in hashes[-persisted:]]
    )
    assert {row["tx_hash"] for row in rows} == set(hashes)
    assert census["status"] == ("FAIL" if persisted == 1 else "UNMEASURED")
    assert census["unattributed_tx_hashes"] == (hashes[:2] if persisted == 1 else [])
    assert census["multi_transaction_blocks"] == [10]


def chain_bundle(directory: Path) -> dict:
    chain_dir = directory / "chain"
    chain_dir.mkdir()
    tx_hash, wallet, token = "0x" + "ab" * 32, "0x" + "11" * 20, "0x" + "22" * 20
    tx = {"hash": tx_hash, "from": wallet}
    block = {"number": "0xa", "hash": "0x" + "cc" * 32, "parentHash": "0x" + "dd" * 32, "transactions": [tx]}
    checks = [
        {"wallet": wallet, "token": token, "block_before": "0x9", "block_after": "0xa", "transfer_log_delta_raw": "-5"}
    ]
    receipt = {
        "transactionHash": tx_hash,
        "blockHash": block["hash"],
        "blockNumber": "0xa",
        "status": "0x1",
        "gasUsed": "0x5208",
        "effectiveGasPrice": "0x2",
        "logs": [
            {
                "address": token,
                "topics": [dedicated.TRANSFER_TOPIC, "0x" + "0" * 24 + wallet[2:], "0x" + "0" * 24 + "33" * 20],
                "data": "0x" + f"{5:064x}",
            }
        ],
    }
    census = {
        "status": "PASS",
        "chain_id": 42161,
        "fork_block": 9,
        "last_block": 10,
        "wallets": [wallet],
        "missing_tx_hashes": [],
        "unattributed_tx_hashes": [],
    }
    summary = {
        "tx_hash": tx_hash,
        "receipt_status": 1,
        "gas_used": 21000,
        "gas_cost_wei": "42000",
        "balance_checks": checks,
    }
    for name, payload in {
        f"receipt-{tx_hash}.json": receipt,
        f"transaction-{tx_hash}.json": tx,
        f"balances-{tx_hash}.json": checks,
        "submission-census.json": {**census, "blocks": [block]},
    }.items():
        (chain_dir / name).write_text(json.dumps(payload))
    return {"submission_census": census, "transactions": [summary]}


def test_receipt_proof_is_bound_to_complete_fork_blocks(tmp_path: Path) -> None:
    admission.validate_chain_anchor(tmp_path, chain_bundle(tmp_path), "arbitrum")


@pytest.mark.parametrize("mutation", ["wrong-chain", "missing-tx", "gas", "wallet", "block", "token", "log"])
def test_fabricated_receipt_or_balance_anchor_cannot_certify(tmp_path: Path, mutation: str) -> None:
    witnesses = chain_bundle(tmp_path)
    summary = witnesses["transactions"][0]
    checks = summary["balance_checks"]
    tx_hash = summary["tx_hash"]
    if mutation == "missing-tx":
        witnesses["transactions"] = []
    elif mutation == "gas":
        summary["gas_used"] = 1
    elif mutation == "wrong-chain":
        witnesses["submission_census"]["chain_id"] = 1
    elif mutation in {"wallet", "block", "token"}:
        field = {"wallet": "wallet", "block": "block_after", "token": "token"}[mutation]
        checks[0][field] = "0xb" if mutation == "block" else "0x" + "44" * 20
        (tmp_path / "chain" / f"balances-{tx_hash}.json").write_text(json.dumps(checks))
    else:
        path = tmp_path / "chain" / f"receipt-{tx_hash}.json"
        receipt = json.loads(path.read_text())
        receipt["logs"][0]["data"] = "0x" + f"{6:064x}"
        path.write_text(json.dumps(receipt))
    expected = {
        "wrong-chain": "Submission census summary disagrees with raw observation",
        "missing-tx": "Receipt observations do not cover the independently enumerated submissions",
        "gas": "Receipt, transaction, block or cost observations disagree",
        "wallet": "Balance observation is not bound to its receipt wallet, block and Transfer logs",
        "block": "Balance observation is not bound to its receipt wallet, block and Transfer logs",
        "token": "Balance observations omit or invent wallet Transfer tokens",
        "log": "Balance observation is not bound to its receipt wallet, block and Transfer logs",
    }[mutation]
    with pytest.raises(ValueError, match=expected):
        admission.validate_chain_anchor(tmp_path, witnesses, "arbitrum")


def test_ledger_gas_is_aggregate_submission_gas_in_real_evidence() -> None:
    with sqlite3.connect(DB) as connection:
        rows = connection.execute("SELECT gas_used, extracted_data_json FROM transaction_ledger").fetchall()
    for gas, encoded in rows:
        transactions = json.loads(encoded)["all_tx_results"]
        assert gas == sum(tx["gas_used"] for tx in transactions)


def test_position_free_teardown_is_not_a_false_failure() -> None:
    report = {
        "status": "COMPLETED",
        "completed_at": "2026-09-06T00:00:00Z",
        "positions_total": 0,
        "positions_closed": 0,
        "positions_failed": 0,
        "verification_status": "chain_verified",
    }
    assert dedicated.teardown_has_complete_sdk_report(report, allow_empty=True)
    assert not dedicated.teardown_has_complete_sdk_report(report)


def test_a_missing_census_cannot_weaken_a_measured_balance_failure(tmp_path: Path) -> None:
    bundle, _ = make_bundle(tmp_path)
    with sqlite3.connect(DB) as connection:
        extracted = [
            json.loads(row[0]) for row in connection.execute("SELECT extracted_data_json FROM transaction_ledger")
        ]
    gas = {tx["tx_hash"]: tx["gas_used"] for row in extracted for tx in row["all_tx_results"]}
    transactions = [
        {**row, "receipt_status": 1, "gas_used": gas[row["tx_hash"]]} for row in dedicated.ledger_transactions(DB)
    ]
    evidence = dedicated.build_dedicated_evidence(
        row_id="lp-uniswap_v3-arbitrum",
        fixture="lp",
        chain="arbitrum",
        primitive="lp",
        deployment_id=dedicated.unique_deployment_id(DB),
        expected_shape={"LP_OPEN": 1},
        db_path=DB,
        accountant=json.loads((bundle / "accountant.json").read_text()),
        strat_log=bundle / "strat.log",
        teardown_log=bundle / "teardown.log",
        chain_witnesses={
            "status": "FAIL",
            "transactions": transactions,
            "submission_census": {"status": "UNMEASURED"},
            "failures": ["observed balance mismatch"],
        },
    )
    assert evidence.stages["receipt_balance"].status == "FAIL"
    assert evidence.stages["receipt_balance"].facts["failures"] == ["observed balance mismatch"]


def test_naive_timestamp_cannot_inflate_observation_quiescence() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        runner._aware_timestamp("2026-09-06T00:00:00")


def test_gateway_lock_is_excluded_but_extra_source_is_not(tmp_path: Path) -> None:
    bundle, _ = make_bundle(tmp_path)
    (bundle / "strategy/almanak_state.db.gw.lock").write_text("12345")
    (bundle / "strategy/almanak_state.db-journal").write_bytes(b"sqlite rollback journal")
    row = next(
        r for r in runner._load_matrix(ROOT / "qa_lab/accounting-matrix.yml") if r.id == "lp-uniswap_v3-arbitrum"
    )
    recipe = admission.validate_recipe(bundle, ROOT, asdict(row), json.loads((bundle / "status.json").read_text()))
    assert "config.json" in recipe["resources"]
    assert "almanak_state.db.gw.lock" not in recipe["resources"]
    assert "almanak_state.db-journal" not in recipe["resources"]


@pytest.mark.parametrize(
    ("compiler_line", "compile_status"),
    [
        ("Compiled LP_OPEN intent: WETH/USDC", "PASS"),
        ("Compiled Aerodrome Slipstream LP_OPEN: WETH/USDC", "PASS"),
        ("Compiling Aerodrome Slipstream LP_OPEN: WETH/USDC", "UNMEASURED"),
        ("Compiled LP_CLOSE: tokenId=1", "UNMEASURED"),
    ],
)
def test_real_fork_convergence_and_full_receipt_set_can_pass(
    tmp_path: Path, compiler_line: str, compile_status: str
) -> None:
    bundle = ROOT / "tests/fixtures/accounting/harness/observed-lp"
    db = bundle / "accounting_evidence.sqlite"
    witnesses = json.loads((bundle / "chain-witnesses.json").read_text())
    compiler_log = tmp_path / "strat.log"
    compiler_log.write_text(compiler_line)
    evidence = dedicated.build_dedicated_evidence(
        row_id="lp-uniswap_v3-arbitrum",
        fixture="lp",
        chain="arbitrum",
        primitive="lp",
        deployment_id=dedicated.unique_deployment_id(db),
        expected_shape={"LP_OPEN": 1},
        db_path=db,
        accountant=admission.canonical_accountant(db, "lp", dedicated.unique_deployment_id(db)),
        strat_log=compiler_log,
        teardown_log=bundle / "teardown.log",
        chain_witnesses=witnesses,
    )
    assert evidence.stages["compile"].status == compile_status
    assert evidence.stages["receipt_balance"].status == "PASS"
    assert evidence.stages["receipt_balance"].facts["convergence_matches_database"] is True
    assert evidence.stages["receipt_balance"].facts["gas_matches"] is True
    assert evidence.stages["execute"].status == "PASS"
    assert evidence.stages["teardown"].status == "UNMEASURED"
    assert evidence.status == "UNMEASURED"
    admission.validate_chain_anchor(bundle, witnesses, "arbitrum")
    dedicated.validate_dedicated_evidence(json.loads(json.dumps(evidence.to_dict())), bundle=bundle)


def boundary_evidence(height: int = 9) -> tuple[dict, str]:
    boundary = {
        "status": "PASS",
        "chain": "arbitrum",
        "block_number": height,
        "block_hash": "0x" + "dd" * 32,
        "funding_completed_at": "2026-09-06T00:00:00Z",
        "capture_started_at": "2026-09-06T00:00:01Z",
        "capture_completed_at": "2026-09-06T00:00:02Z",
    }
    log = (
        "2026-09-06T00:00:00Z [info] Anvil funding complete for arbitrum\n"
        "2026-09-06T00:00:03Z [info] Dispatching SUPPLY (2 tx)\n"
    )
    return boundary, log


@pytest.mark.parametrize("mutation", ["valid", "late", "naive", "wrong-hash", "missing-provisioning-set"])
def test_admission_requires_pre_dispatch_boundary_and_canonical_block(tmp_path: Path, mutation: str) -> None:
    witnesses = chain_bundle(tmp_path)
    boundary, log = boundary_evidence()
    if mutation == "late":
        boundary["capture_completed_at"] = "2026-09-06T00:00:04Z"
    elif mutation == "naive":
        boundary["capture_started_at"] = "2026-09-06T00:00:01"
    elif mutation == "wrong-hash":
        boundary["block_hash"] = "0x" + "ee" * 32
    census = witnesses["submission_census"]
    census.update(execution_boundary=boundary, provisioning_tx_hashes=[])
    if mutation == "missing-provisioning-set":
        census.pop("provisioning_tx_hashes")
    path = tmp_path / "chain/submission-census.json"
    raw = json.loads(path.read_text())
    raw.update(census)
    path.write_text(json.dumps(raw))
    (tmp_path / "strat.log").write_text(log)
    if mutation == "valid":
        admission.validate_chain_anchor(tmp_path, witnesses, "arbitrum")
    else:
        with pytest.raises(ValueError, match="boundary|Provisioning"):
            admission.validate_chain_anchor(tmp_path, witnesses, "arbitrum")


@pytest.mark.parametrize("lost_retry", [False, True])
def test_setup_prefix_is_separate_but_execution_retries_stay_in_census(monkeypatch, lost_retry: bool) -> None:
    boundary, log = boundary_evidence(10)
    wallet = "0x" + "11" * 20
    hashes = ["0x" + c * 64 for c in "abc"]
    txs = [{"hash": h, "from": wallet} for h in hashes]
    blocks = {
        hex(n): {
            "number": hex(n),
            "hash": boundary["block_hash"] if n == 10 else "0x" + str(n) * 32,
            "transactions": [txs[n - 10]],
        }
        for n in range(10, 13)
    }

    def rpc(_url, method, params):
        if method == "eth_getBlockByNumber":
            return blocks[params[0]]
        return {
            "anvil_metadata": {"forkedNetwork": {"forkBlockNumber": 9}},
            "eth_blockNumber": "0xc",
            "eth_chainId": "0xa4b1",
            "eth_getTransactionByHash": txs[-1],
        }[method]

    monkeypatch.setattr(dedicated, "_rpc", rpc)
    ledger = [{"id": h, "tx_hash": h} for h in hashes[2 if lost_retry else 1 :]]
    rows, census = dedicated._fork_submissions("http://localhost:8545", ledger, boundary=boundary, log_text=log)
    assert {row["tx_hash"] for row in rows} == set(hashes[1:])
    assert census["provisioning_tx_hashes"] == hashes[:1]
    assert census["unattributed_tx_hashes"] == (hashes[1:2] if lost_retry else [])
    assert census["status"] == ("FAIL" if lost_retry else "PASS")
    assert len(census["blocks"]) == 3


@pytest.mark.parametrize("drop_first_execution", [False, True])
def test_captured_aave_provisioning_does_not_hide_first_execution(monkeypatch, drop_first_execution: bool) -> None:
    bundle = ROOT / "tests/fixtures/accounting/harness/observed-provisioning"
    raw = json.loads((bundle / "submission-census.json").read_text())
    ledger = json.loads((bundle / "ledger-transactions.json").read_text())
    blocks = {block["number"]: block for block in raw["blocks"]}
    txs = {tx["hash"]: tx for block in raw["blocks"] for tx in block["transactions"]}
    expected = {row["tx_hash"] for row in ledger}
    first = min((txs[h] for h in expected), key=lambda tx: int(tx["nonce"], 16))["hash"]
    if drop_first_execution:
        ledger = [row for row in ledger if row["tx_hash"] != first]

    def rpc(_url, method, params):
        if method == "eth_getBlockByNumber":
            return blocks[params[0]]
        if method == "eth_getTransactionByHash":
            return txs[params[0]]
        return {
            "anvil_metadata": {"forkedNetwork": {"forkBlockNumber": raw["fork_block"]}},
            "eth_blockNumber": hex(raw["last_block"]),
            "eth_chainId": hex(raw["chain_id"]),
        }[method]

    monkeypatch.setattr(dedicated, "_rpc", rpc)
    rows, census = dedicated._fork_submissions(
        "http://localhost:8545", ledger, boundary=raw["execution_boundary"], log_text=(bundle / "strat.log").read_text()
    )
    assert {row["tx_hash"] for row in rows} == expected
    assert len(rows) == 14
    assert census["provisioning_tx_hashes"] == raw["provisioning_tx_hashes"]
    assert len(census["provisioning_tx_hashes"]) == 1
    assert census["unattributed_tx_hashes"] == ([first] if drop_first_execution else [])
    assert census["status"] == ("FAIL" if drop_first_execution else "PASS")


def test_boundary_transport_failure_is_unmeasured_not_a_row_abort(tmp_path: Path, monkeypatch) -> None:
    class Running:
        def poll(self):
            return None

    log = tmp_path / "strat.log"
    log.write_text(
        "2026-09-06T00:00:00Z [info] Anvil fork started for base on port 8545\n"
        "2026-09-06T00:00:01Z [info] Anvil funding complete for base\n"
    )

    def unavailable(*_args):
        raise OSError("connection reset")

    monkeypatch.setattr(dedicated, "_rpc", unavailable)
    runner._capture_execution_boundary(log, tmp_path, "base", Running())
    boundary = json.loads((tmp_path / "chain/execution-boundary.json").read_text())
    assert boundary["status"] == "UNMEASURED"
    assert "connection reset" in boundary["diagnostic"]


@pytest.mark.parametrize("mutation", ["lp-amount", "lp-labels", "native-value", "missing-value", "native-and-bad-gas"])
def test_captured_lp_claim_boundaries(tmp_path: Path, mutation: str) -> None:
    from decimal import Decimal

    bundle = tmp_path / "captured"
    shutil.copytree(ROOT / "tests/fixtures/accounting/harness/observed-lp", bundle)
    db = bundle / "accounting_evidence.sqlite"
    witnesses = json.loads((bundle / "chain-witnesses.json").read_text())
    if mutation == "lp-amount":
        with sqlite3.connect(db) as conn:
            amount = conn.execute("SELECT amount_in FROM transaction_ledger WHERE intent_type='LP_OPEN'").fetchone()[0]
            conn.execute(
                "UPDATE transaction_ledger SET amount_in=? WHERE intent_type='LP_OPEN'", (str(Decimal(amount) * 10),)
            )
            for row_id, amount in conn.execute(
                "SELECT id, amount0 FROM position_events WHERE event_type='OPEN'"
            ).fetchall():
                conn.execute("UPDATE position_events SET amount0=? WHERE id=?", (str(Decimal(amount) * 10), row_id))
    elif mutation == "lp-labels":
        with sqlite3.connect(db) as conn:
            conn.execute(
                "UPDATE transaction_ledger SET token_in=token_out, token_out=token_in WHERE intent_type='LP_OPEN'"
            )
    else:
        path = next((bundle / "chain").glob("transaction-*.json"))
        transaction = json.loads(path.read_text())
        if mutation == "missing-value":
            del transaction["value"]
        else:
            transaction["value"] = "0x1"
        path.write_text(json.dumps(transaction))
        if mutation == "native-and-bad-gas":
            witnesses["transactions"][0]["gas_used"] += 1
    deployment = dedicated.unique_deployment_id(db)
    evidence = dedicated.build_dedicated_evidence(
        row_id="lp-uniswap_v3-arbitrum",
        fixture="lp",
        chain="arbitrum",
        primitive="lp",
        deployment_id=deployment,
        expected_shape={"LP_OPEN": 1},
        db_path=db,
        accountant=admission.canonical_accountant(db, "lp", deployment),
        strat_log=bundle / "strat.log",
        teardown_log=bundle / "teardown.log",
        chain_witnesses=witnesses,
    )
    stage = evidence.stages["receipt_balance"]
    assert stage.facts["sdk_amount_reconciliation_status"] == "UNMEASURED"
    assert stage.status == (
        "PASS" if mutation.startswith("lp-") else "FAIL" if mutation == "native-and-bad-gas" else "UNMEASURED"
    )
    assert evidence.status != "PASS"


def test_expired_pendle_recipes_are_blocked_in_catalog_and_runner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from datetime import UTC, datetime

    class AfterExpiry(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 9, 6, tzinfo=UTC)

    monkeypatch.setattr(admission, "datetime", AfterExpiry)
    catalog = qa.build_accounting_catalog(qa_catalog={"cells": []})
    pendle = [cell for cell in catalog["cells"] if cell["protocol"] == "pendle"]
    assert len(pendle) == 2
    assert all(cell["coverage_status"] == "BLOCKED" for cell in pendle)
    rows = [row for row in runner._load_matrix(ROOT / "qa_lab/accounting-matrix.yml") if "pendle" in row.id]
    assert len(rows) == 2
    for row in rows:
        with pytest.raises(ValueError, match="Expired market"):
            runner._prepare_temp_strategy(row, tmp_path / row.id)


@pytest.mark.parametrize("mutation", ["missing-db", "runtime-list", "source-list"])
def test_invalid_bundle_shape_has_controlled_admission_error(tmp_path: Path, mutation: str) -> None:
    bundle, sdk = make_bundle(tmp_path)
    if mutation == "missing-db":
        (bundle / "strategy/almanak_state.db").unlink()
    else:
        runtime = json.loads((bundle / "runtime.json").read_text())
        if mutation == "runtime-list":
            runtime = []
        else:
            runtime["source"] = []
        (bundle / "runtime.json").write_text(json.dumps(runtime))
    store = tmp_path / "store"
    with pytest.raises(ValueError, match="state.db|runtime.json"):
        seal(bundle, sdk, store)
    assert not (store / "index" / qa.ACCOUNTING_DEDICATED_INDEX).exists()


@pytest.mark.parametrize("row_id", ["pendle_lp-pendle-arbitrum", "pendle_pt-pendle-arbitrum"])
def test_expired_recipe_cannot_reuse_a_stale_pass(tmp_path: Path, row_id: str) -> None:
    row = next(row for row in runner._load_matrix(ROOT / "qa_lab/accounting-matrix.yml") if row.id == row_id)
    blocked = runner._run_row(row, tmp_path, 52000, skip_existing=False)
    assert blocked.status == "BLOCKED" and "Expired market" in blocked.error
    stale = asdict(blocked)
    stale.update(status="PASS", dedicated_status="PASS", error=None)
    status_path = tmp_path / row.id / "status.json"
    status_path.write_text(json.dumps(stale))
    repeated = runner._run_row(row, tmp_path, 52000, skip_existing=True)
    assert repeated.status == "BLOCKED" and "Expired market" in repeated.error
    saved = json.loads(status_path.read_text())
    assert saved["status"] == saved["dedicated_status"] == "BLOCKED"
    assert "Expired market" in saved["error"]
