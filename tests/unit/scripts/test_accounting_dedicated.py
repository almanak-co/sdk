"""Load-bearing tests for the dedicated Accounting dedicated evidence contract."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from qa_lab import accounting_dedicated as dedicated
from qa_lab import run_accounting_matrix as matrix_runner
from qa_lab.run_accounting_matrix import _read_detached_anvil_pid, _read_managed_anvil_rpc_url


def test_looping_matrix_rows_wait_for_the_observable_loop_leg() -> None:
    """Teardown must not truncate the lifecycle before Accountant L6 is observable."""
    root = Path(__file__).resolve().parents[3]
    rows = matrix_runner._load_matrix(root / "qa_lab/accounting-matrix.yml")
    looping_rows = [row for row in rows if row.fixture == "looping"]

    assert looping_rows
    for row in looping_rows:
        assert row.expected_shape == {"SUPPLY": 2, "BORROW": 2, "SWAP": 1}, row.id


def test_benqi_teardown_is_signalled_while_lending_exposure_is_open() -> None:
    root = Path(__file__).resolve().parents[3]
    rows = matrix_runner._load_matrix(root / "qa_lab/accounting-matrix.yml")
    row = next(row for row in rows if row.id == "lending-benqi-avalanche")

    assert row.expected_shape == {"SUPPLY": 1, "BORROW": 1, "REPAY": 1, "WITHDRAW": 1}
    assert row.teardown_trigger_shape == {"SUPPLY": 1, "BORROW": 1}


def _artifact(bundle: Path, name: str, kind: str, payload: object) -> dict[str, str]:
    path = bundle / "chain" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload) + "\n")
    return {
        "kind": kind,
        "path": path.relative_to(bundle).as_posix(),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def _payload(bundle: Path) -> dict:
    tx_hash = "0x" + "aa" * 32
    wallet = "0x" + "11" * 20
    artifacts = [
        _artifact(
            bundle,
            f"receipt-{tx_hash}.json",
            "rpc-receipt",
            {"transactionHash": tx_hash, "status": "0x1", "blockNumber": "0xa", "gasUsed": "0x5208"},
        ),
        _artifact(
            bundle, f"transaction-{tx_hash}.json", "rpc-transaction", {"hash": tx_hash, "from": wallet, "value": "0x0"}
        ),
        _artifact(
            bundle,
            f"balances-{tx_hash}.json",
            "rpc-balance-witness",
            [
                {
                    "wallet": wallet,
                    "balance_before_raw": "10",
                    "balance_after_raw": "5",
                    "actual_delta_raw": "-5",
                    "transfer_log_delta_raw": "-5",
                    "reconciliation_mode": "exact_transfer",
                    "status": "PASS",
                }
            ],
        ),
    ]
    census = {"status": "PASS", "missing_tx_hashes": [], "unattributed_tx_hashes": [], "multi_transaction_blocks": []}
    artifacts.append(
        _artifact(bundle, "submission-census.json", "rpc-submission-census", {**census, "blocks": [{"number": "0xa"}]})
    )
    stages = {
        name: {"status": "PASS", "anchor": f"anchor:{name}", "facts": {}, "artifacts": [], "diagnostic": ""}
        for name in dedicated.STAGE_ORDER
    }
    stages["receipt_balance"].update(
        facts={
            "transaction_count": 1,
            "balance_check_count": 1,
            "ledger_tx_hashes": [tx_hash],
            "witnessed_tx_hashes": [tx_hash],
            "receipt_set_complete": True,
            "submission_census": census,
            "observed_gas_used": 21000,
            "ledger_gas_used": 21000,
            "gas_matches": True,
            "convergence_matches_database": True,
            "convergence": {
                "status": "PASS",
                "consecutive_complete_observations": 2,
                "ledger_tx_hashes": [tx_hash],
                "witnessed_tx_hashes": [tx_hash],
                "completed_at": "2026-09-06T00:00:00Z",
                "latest_ledger_at": "2026-09-06T00:00:00Z",
            },
        },
        artifacts=artifacts,
    )
    return {
        "schema_version": 1,
        "artifact_kind": dedicated.ARTIFACT_KIND,
        "row_id": "looping-aave_v3-arbitrum",
        "fixture": "looping",
        "chain": "arbitrum",
        "primitive": "looping",
        "network": "anvil",
        "exec_path": "eoa",
        "deployment_id": "deployment:test",
        "expected_shape": {"SUPPLY": 1, "BORROW": 1, "REPAY": 1, "WITHDRAW": 1},
        "stage_order": list(dedicated.STAGE_ORDER),
        "stages": stages,
        "status": "PASS",
    }


def test_validator_accepts_complete_anchored_manifest(tmp_path: Path) -> None:
    dedicated.validate_dedicated_evidence(_payload(tmp_path), bundle=tmp_path)


@pytest.mark.parametrize("mutation", ["missing-stage", "self-promoted", "no-balance-anchor"])
def test_validator_rejects_incomplete_or_self_promoted_evidence(tmp_path: Path, mutation: str) -> None:
    payload = _payload(tmp_path)
    if mutation == "missing-stage":
        del payload["stages"]["teardown"]
    elif mutation == "self-promoted":
        payload["stages"]["accountant"]["status"] = "FAIL"
    else:
        payload["stages"]["receipt_balance"]["artifacts"] = []

    with pytest.raises(ValueError):
        dedicated.validate_dedicated_evidence(payload, bundle=tmp_path)


def test_validator_rejects_tampered_chain_artifact(tmp_path: Path) -> None:
    payload = _payload(tmp_path)
    receipt_path = tmp_path / payload["stages"]["receipt_balance"]["artifacts"][0]["path"]
    receipt_path.write_text("edited after observation\n")

    with pytest.raises(ValueError, match="tampered"):
        dedicated.validate_dedicated_evidence(payload, bundle=tmp_path)


def test_validator_rejects_semantically_fabricated_balance_pass(tmp_path: Path) -> None:
    payload = _payload(tmp_path)
    balance_artifact = payload["stages"]["receipt_balance"]["artifacts"][2]
    balance_path = tmp_path / balance_artifact["path"]
    checks = json.loads(balance_path.read_text())
    checks[0]["actual_delta_raw"] = "-4"
    balance_path.write_text(json.dumps(checks) + "\n")
    balance_artifact["sha256"] = hashlib.sha256(balance_path.read_bytes()).hexdigest()

    with pytest.raises(ValueError, match="mismatched balance"):
        dedicated.validate_dedicated_evidence(payload, bundle=tmp_path)


def test_validator_recomputes_delta_from_block_pinned_balances(tmp_path: Path) -> None:
    payload = _payload(tmp_path)
    balance_artifact = payload["stages"]["receipt_balance"]["artifacts"][2]
    balance_path = tmp_path / balance_artifact["path"]
    checks = json.loads(balance_path.read_text())
    checks[0]["balance_after_raw"] = "4"
    balance_path.write_text(json.dumps(checks) + "\n")
    balance_artifact["sha256"] = hashlib.sha256(balance_path.read_bytes()).hexdigest()

    with pytest.raises(ValueError, match="mismatched balance"):
        dedicated.validate_dedicated_evidence(payload, bundle=tmp_path)


def test_validator_rejects_zero_close_without_indexed_proof(tmp_path: Path) -> None:
    payload = _payload(tmp_path)
    balance_artifact = payload["stages"]["receipt_balance"]["artifacts"][2]
    balance_path = tmp_path / balance_artifact["path"]
    checks = json.loads(balance_path.read_text())
    checks[0].update(
        {
            "balance_before_raw": "4000000",
            "balance_after_raw": "0",
            "actual_delta_raw": "-4000000",
            "transfer_log_delta_raw": "-3999999",
            "reconciliation_mode": "exact_zero_close",
        }
    )
    balance_path.write_text(json.dumps(checks) + "\n")
    balance_artifact["sha256"] = hashlib.sha256(balance_path.read_bytes()).hexdigest()

    with pytest.raises(ValueError, match="mismatched balance"):
        dedicated.validate_dedicated_evidence(payload, bundle=tmp_path)


def test_validator_rejects_zero_close_mode_with_nonzero_residual(tmp_path: Path) -> None:
    payload = _payload(tmp_path)
    balance_artifact = payload["stages"]["receipt_balance"]["artifacts"][2]
    balance_path = tmp_path / balance_artifact["path"]
    checks = json.loads(balance_path.read_text())
    checks[0].update(
        {
            "balance_before_raw": "4000000",
            "balance_after_raw": "1",
            "actual_delta_raw": "-3999999",
            "transfer_log_delta_raw": "-3999999",
            "reconciliation_mode": "exact_zero_close",
        }
    )
    balance_path.write_text(json.dumps(checks) + "\n")
    balance_artifact["sha256"] = hashlib.sha256(balance_path.read_bytes()).hexdigest()

    with pytest.raises(ValueError, match="mismatched balance"):
        dedicated.validate_dedicated_evidence(payload, bundle=tmp_path)


@pytest.mark.parametrize("log_delta", [101, 1000])
def test_indexed_transfer_requires_bounded_log_rounding(tmp_path: Path, log_delta: int) -> None:
    payload = _payload(tmp_path)
    balance_artifact = payload["stages"]["receipt_balance"]["artifacts"][2]
    balance_path = tmp_path / balance_artifact["path"]
    checks = json.loads(balance_path.read_text())
    checks[0].update(
        {
            "balance_before_raw": "100",
            "balance_after_raw": "200",
            "actual_delta_raw": "100",
            "transfer_log_delta_raw": str(log_delta),
            "reconciliation_mode": "exact_indexed_transfer",
            "indexed_proof": {
                "status": "PASS",
                "treasury": "0x" + "66" * 20,
                "previous_user_index_ray": str(2 * dedicated.RAY),
                "previous_user_balance_raw": "100",
                "expected_transfer_delta_raw": "100",
                "underlying_token": "0x" + "44" * 20,
                "pool": "0x" + "55" * 20,
                "underlying_balance_before_raw": "1000",
                "underlying_balance_after_raw": "899",
                "underlying_actual_delta_raw": "-101",
                "underlying_transfer_log_delta_raw": "-101",
                "scaled_balance_before_raw": "50",
                "scaled_balance_after_raw": "100",
                "liquidity_index_before_ray": str(2 * dedicated.RAY),
                "liquidity_index_after_ray": str(2 * dedicated.RAY),
                "derived_balance_before_raw": "100",
                "derived_balance_after_raw": "200",
                "expected_scaled_delta_raw": "50",
                "actual_scaled_delta_raw": "50",
                "rounding_rule": "a_token_mint_floor",
            },
        }
    )
    balance_path.write_text(json.dumps(checks) + "\n")
    balance_artifact["sha256"] = hashlib.sha256(balance_path.read_bytes()).hexdigest()

    if log_delta == 101:
        dedicated.validate_dedicated_evidence(payload, bundle=tmp_path)
    else:
        with pytest.raises(ValueError, match="mismatched balance"):
            dedicated.validate_dedicated_evidence(payload, bundle=tmp_path)


def test_validator_rejects_tampered_indexed_transfer_derivation(tmp_path: Path) -> None:
    payload = _payload(tmp_path)
    balance_artifact = payload["stages"]["receipt_balance"]["artifacts"][2]
    balance_path = tmp_path / balance_artifact["path"]
    checks = json.loads(balance_path.read_text())
    checks[0].update(
        {
            "balance_before_raw": "100",
            "balance_after_raw": "300",
            "actual_delta_raw": "200",
            "transfer_log_delta_raw": "100",
            "reconciliation_mode": "exact_indexed_transfer",
            "indexed_proof": {
                "status": "PASS",
                "treasury": "0x" + "66" * 20,
                "previous_user_index_ray": str(2 * dedicated.RAY),
                "previous_user_balance_raw": "100",
                "expected_transfer_delta_raw": "100",
                "underlying_token": "0x" + "44" * 20,
                "pool": "0x" + "55" * 20,
                "underlying_balance_before_raw": "1000",
                "underlying_balance_after_raw": "900",
                "underlying_actual_delta_raw": "-100",
                "underlying_transfer_log_delta_raw": "-100",
                "scaled_balance_before_raw": "100",
                "scaled_balance_after_raw": "151",
                "liquidity_index_before_ray": str(dedicated.RAY),
                "liquidity_index_after_ray": str(2 * dedicated.RAY),
                "derived_balance_before_raw": "100",
                "derived_balance_after_raw": "300",
                "expected_scaled_delta_raw": "50",
                "actual_scaled_delta_raw": "51",
                "rounding_rule": "a_token_mint_floor",
            },
        }
    )
    balance_path.write_text(json.dumps(checks) + "\n")
    balance_artifact["sha256"] = hashlib.sha256(balance_path.read_bytes()).hexdigest()

    with pytest.raises(ValueError, match="mismatched balance"):
        dedicated.validate_dedicated_evidence(payload, bundle=tmp_path)


@pytest.mark.parametrize(
    "previous_multiplier, log_delta, expected",
    [(2, 101, "PASS"), (2, 1000, "FAIL"), (1, 150, "PASS"), (1, 1000, "FAIL")],
)
def test_indexed_transfer_distinguishes_accrued_interest_from_contradiction(
    monkeypatch, previous_multiplier, log_delta, expected
) -> None:
    wallet = "0x" + "11" * 20
    token = "0x" + "22" * 20
    underlying = "0x" + "44" * 20
    pool = "0x" + "55" * 20

    def encoded_address(address: str) -> str:
        return "0x" + address.removeprefix("0x").rjust(64, "0")

    def fake_rpc(_url: str, method: str, params: list) -> object:
        assert method == "eth_call"
        call, block = params
        data = call["data"]
        if data.startswith(dedicated.SCALED_BALANCE_OF_SELECTOR):
            return hex(50 if block == "0x9" else 100)
        if data.startswith("0x70a08231") and call["to"] == underlying:
            return hex(1000 if block == "0x9" else 899)
        if data == dedicated.UNDERLYING_ASSET_SELECTOR:
            return encoded_address(underlying)
        if data.startswith(dedicated.PREVIOUS_INDEX_SELECTOR):
            return hex(previous_multiplier * dedicated.RAY)
        if data == dedicated.ATOKEN_TREASURY_SELECTOR:
            return encoded_address(pool)
        if data == dedicated.POOL_SELECTOR:
            return encoded_address(pool)
        if data.startswith(dedicated.NORMALIZED_INCOME_SELECTOR):
            return hex(2 * dedicated.RAY)
        raise RuntimeError("unsupported optional selector")

    monkeypatch.setattr(dedicated, "_rpc", fake_rpc)

    proof = dedicated._indexed_transfer_proof(
        url="http://127.0.0.1:8545",
        token=token,
        wallet=wallet,
        before_block="0x9",
        after_block="0xa",
        visible_before=100,
        visible_after=200,
        transfer_delta=log_delta,
        transfer_deltas_by_token={token: log_delta, underlying: -101},
    )

    assert proof is not None
    assert proof["expected_scaled_delta_raw"] == "50"
    assert proof["actual_scaled_delta_raw"] == "50"
    assert proof["rounding_rule"] == "a_token_mint_floor"

    assert proof["status"] == expected
    monkeypatch.setattr(dedicated, "_indexed_transfer_proof", lambda **_kwargs: proof)
    monkeypatch.setattr(dedicated, "_balance_of", lambda _url, _token, _wallet, block: 100 if block == "0x9" else 200)
    check = dedicated._capture_balance_check(
        rpc_url="http://127.0.0.1:8545",
        token=token,
        wallet=wallet,
        previous_block="0x9",
        block="0xa",
        transfer_delta=log_delta,
        transfer_deltas_by_token={token: log_delta, underlying: -101},
    )
    assert check["status"] == expected


def test_a_token_scaled_delta_uses_direction_specific_v35_rounding() -> None:
    index = 2 * dedicated.RAY

    assert dedicated._a_token_scaled_delta(-101, index) == (50, "a_token_mint_floor")
    assert dedicated._a_token_scaled_delta(101, index) == (-51, "a_token_burn_ceil")


def test_validator_binds_transaction_witness_hash_to_receipt_and_ledger(tmp_path: Path) -> None:
    payload = _payload(tmp_path)
    transaction_artifact = payload["stages"]["receipt_balance"]["artifacts"][1]
    transaction_path = tmp_path / transaction_artifact["path"]
    transaction = json.loads(transaction_path.read_text())
    transaction["hash"] = "0x" + "bb" * 32
    transaction_path.write_text(json.dumps(transaction) + "\n")
    transaction_artifact["sha256"] = hashlib.sha256(transaction_path.read_bytes()).hexdigest()

    with pytest.raises(ValueError, match="transaction artifacts do not match"):
        dedicated.validate_dedicated_evidence(payload, bundle=tmp_path)


def test_validator_rejects_matching_but_malformed_transaction_hashes(tmp_path: Path) -> None:
    payload = _payload(tmp_path)
    receipt_stage = payload["stages"]["receipt_balance"]
    receipt_stage["facts"]["ledger_tx_hashes"] = ["0xabc"]
    receipt_stage["facts"]["witnessed_tx_hashes"] = ["0xabc"]
    for artifact in receipt_stage["artifacts"][:2]:
        artifact_path = tmp_path / artifact["path"]
        document = json.loads(artifact_path.read_text())
        hash_field = "transactionHash" if artifact["kind"] == "rpc-receipt" else "hash"
        document[hash_field] = "0xabc"
        artifact_path.write_text(json.dumps(document) + "\n")
        artifact["sha256"] = hashlib.sha256(artifact_path.read_bytes()).hexdigest()

    with pytest.raises(ValueError, match="malformed ledger transaction hash"):
        dedicated.validate_dedicated_evidence(payload, bundle=tmp_path)


TEST_TX_HASH = "0x" + "ab" * 32


def _ledger_db(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE transaction_ledger (
            id TEXT, cycle_id TEXT, deployment_id TEXT, intent_type TEXT, tx_hash TEXT, timestamp TEXT
        );
        INSERT INTO transaction_ledger VALUES (
            'ledger-1', 'cycle-1', 'deployment:test', 'SUPPLY', '0xabababababababababababababababababababababababababababababababab',
            '2026-08-09T00:00:00Z'
        );
        """
    )
    connection.close()


def test_independent_balance_mismatch_turns_chain_stage_red(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "state.db"
    _ledger_db(db_path)
    wallet = "0x" + "11" * 20
    token = "0x" + "22" * 20
    transfer = {
        "address": token,
        "topics": [dedicated.TRANSFER_TOPIC, "0x" + "0" * 24 + wallet[2:], "0x" + "0" * 24 + "33" * 20],
        "data": "0x" + f"{5:064x}",
    }

    def fake_rpc(_url: str, method: str, params: list) -> object:
        if method == "eth_getTransactionReceipt":
            return {
                "transactionHash": TEST_TX_HASH,
                "blockNumber": "0xa",
                "blockHash": "0xblock",
                "status": "0x1",
                "gasUsed": "0x5208",
                "effectiveGasPrice": "0x1",
                "logs": [transfer],
            }
        if method == "eth_getTransactionByHash":
            return {"from": wallet, "value": "0x0"}
        assert method == "eth_call"
        return hex(100 if params[1] == "0x9" else 96)

    monkeypatch.setattr(dedicated, "_rpc", fake_rpc)
    observed = dedicated.capture_anvil_witnesses(
        db_path=db_path,
        rpc_url="http://127.0.0.1:8545",
        output_dir=tmp_path / "chain",
    )

    assert observed["status"] == "FAIL"
    assert observed["balance_check_count"] == 1
    assert "balance delta -4 != logs -5" in observed["failures"][0]


def test_erc721_transfer_is_retained_as_receipt_evidence_not_parsed_as_erc20(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "state.db"
    _ledger_db(db_path)
    wallet = "0x" + "11" * 20
    nft_transfer = {
        "address": "0x" + "22" * 20,
        "topics": [
            dedicated.TRANSFER_TOPIC,
            "0x" + "0" * 24 + "00" * 20,
            "0x" + "0" * 24 + wallet[2:],
            "0x" + "0" * 63 + "1",
        ],
        "data": "0x",
    }

    def fake_rpc(_url: str, method: str, _params: list) -> object:
        if method == "eth_getTransactionReceipt":
            return {
                "transactionHash": TEST_TX_HASH,
                "blockNumber": "0xa",
                "blockHash": "0xblock",
                "status": "0x1",
                "gasUsed": "0x5208",
                "effectiveGasPrice": "0x1",
                "logs": [nft_transfer],
            }
        if method == "eth_getTransactionByHash":
            return {"from": wallet, "value": "0x0"}
        raise AssertionError(f"ERC-721 Transfer caused unexpected {method}")

    monkeypatch.setattr(dedicated, "_rpc", fake_rpc)
    observed = dedicated.capture_anvil_witnesses(
        db_path=db_path,
        rpc_url="http://127.0.0.1:8545",
        output_dir=tmp_path / "chain",
    )

    # The receipt is complete and retained, but a receipt containing only an
    # ERC-721 transfer has no independent ERC-20 balance anchor of its own.
    assert observed["status"] == "UNMEASURED"
    assert observed["transaction_count"] == 1
    assert observed["balance_check_count"] == 0
    assert observed["transactions"][0]["receipt_status"] == 1
    assert observed["transactions"][0]["balance_checks"] == []


def test_capture_rejects_malformed_ledger_hash_before_rpc_or_artifact_path(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "state.db"
    _ledger_db(db_path)
    connection = sqlite3.connect(db_path)
    connection.execute("UPDATE transaction_ledger SET tx_hash = ?", ("../../escape",))
    connection.commit()
    connection.close()

    def forbidden_rpc(*_args, **_kwargs) -> object:
        raise AssertionError("malformed ledger hash reached JSON-RPC")

    monkeypatch.setattr(dedicated, "_rpc", forbidden_rpc)
    output_dir = tmp_path / "chain"
    with pytest.raises(ValueError, match="malformed submitted"):
        dedicated.capture_anvil_witnesses(db_path=db_path, rpc_url="http://127.0.0.1:8545", output_dir=output_dir)
    assert list(output_dir.iterdir()) == []
    assert not (tmp_path.parent / "escape.json").exists()


@pytest.mark.parametrize("chain_status", ["PASS", "UNMEASURED"])
def test_post_teardown_observation_requires_two_stable_complete_polls(
    tmp_path: Path, monkeypatch, chain_status
) -> None:
    tx_hash = "0x" + "ab" * 32
    calls = 0

    def observe(**_kwargs) -> dict:
        nonlocal calls
        calls += 1
        return {
            "status": chain_status,
            "transactions": [{"tx_hash": tx_hash, "receipt_status": 1}],
            "submission_census": {"status": "PASS"},
            "failures": [],
        }

    monkeypatch.setattr(matrix_runner, "_observe_chain", observe)
    monkeypatch.setattr(matrix_runner, "ledger_transactions", lambda _path: [{"tx_hash": tx_hash}])
    monkeypatch.setattr(
        matrix_runner,
        "db_facts",
        lambda *_args: {
            "teardown_status": "COMPLETED",
            "teardown": {
                "status": "COMPLETED",
                "completed_at": "2020-01-01T00:00:00+00:00",
                "positions_total": 2,
                "positions_closed": 2,
                "positions_failed": 0,
                "verification_status": "chain_verified",
            },
            "latest_ledger_timestamp": "2020-01-01T00:00:01+00:00",
            "landed_by_intent": {"SUPPLY": 1},
        },
    )

    observed = matrix_runner._observe_until_converged(
        db_path=tmp_path / "state.db",
        row_dir=tmp_path,
        rpc_url="http://127.0.0.1:8545",
        deployment_id="deployment:test",
        expected_shape={"SUPPLY": 1},
        previous={"status": "UNMEASURED"},
        timeout_seconds=1,
        poll_interval_seconds=0,
    )

    assert calls == 2
    assert observed["status"] == chain_status
    convergence = observed["convergence"]
    assert convergence["status"] == "PASS"
    assert convergence["polls"] == 2
    assert convergence["consecutive_complete_observations"] == 2
    assert convergence["ledger_tx_hashes"] == [tx_hash]
    assert convergence["witnessed_tx_hashes"] == [tx_hash]
    assert convergence["teardown_status"] == "COMPLETED"
    assert convergence["teardown_chain_verified"] is True
    assert convergence["landed_by_intent"] == {"SUPPLY": 1}
    assert convergence["completed_at"] == "2020-01-01T00:00:00+00:00"
    assert convergence["latest_ledger_at"] == "2020-01-01T00:00:01+00:00"
    assert convergence["quiescence_seconds"] >= matrix_runner._POST_TEARDOWN_QUIESCENCE_SECONDS


@pytest.mark.parametrize(
    "previous_status, expected_status", [("PASS", "FAIL"), ("FAIL", "FAIL"), ("UNMEASURED", "UNMEASURED")]
)
def test_post_teardown_timeout_cannot_preserve_stale_pass(tmp_path: Path, previous_status, expected_status) -> None:
    observed = matrix_runner._observe_until_converged(
        db_path=tmp_path / "state.db",
        row_dir=tmp_path,
        rpc_url="http://127.0.0.1:8545",
        deployment_id="deployment:test",
        expected_shape={"SUPPLY": 1},
        previous={"status": previous_status, "failures": []},
        timeout_seconds=0,
        poll_interval_seconds=0,
    )

    assert observed["status"] == expected_status
    assert observed["convergence"]["status"] == expected_status
    assert "did not converge" in observed["failures"][-1]


def test_receipt_convergence_records_but_does_not_cross_gate_teardown_verification(tmp_path: Path, monkeypatch) -> None:
    tx_hash = TEST_TX_HASH
    monkeypatch.setattr(
        matrix_runner,
        "_observe_chain",
        lambda **_kwargs: {
            "status": "PASS",
            "transactions": [{"tx_hash": tx_hash, "receipt_status": 1}],
            "submission_census": {"status": "PASS"},
            "failures": [],
        },
    )
    monkeypatch.setattr(matrix_runner, "ledger_transactions", lambda _path: [{"tx_hash": tx_hash}])
    monkeypatch.setattr(
        matrix_runner,
        "db_facts",
        lambda *_args: {
            "teardown": {
                "status": "COMPLETED",
                "completed_at": "2020-01-01T00:00:00+00:00",
                "positions_total": 1,
                "positions_closed": 1,
                "positions_failed": 0,
                "verification_status": "unverified",
            },
            "latest_ledger_timestamp": "2020-01-01T00:00:01+00:00",
            "landed_by_intent": {"SUPPLY": 1},
        },
    )

    observed = matrix_runner._observe_until_converged(
        db_path=tmp_path / "state.db",
        row_dir=tmp_path,
        rpc_url="http://127.0.0.1:8545",
        deployment_id="deployment:test",
        expected_shape={"SUPPLY": 1},
        previous={"status": "PASS", "failures": []},
        timeout_seconds=1,
        poll_interval_seconds=0,
    )

    assert observed["status"] == "PASS"
    assert observed["convergence"]["teardown_chain_verified"] is False


@pytest.mark.parametrize("native_value", ["0x0", "0x1", None])
def test_independent_receipt_and_balance_witness_can_pass(tmp_path: Path, monkeypatch, native_value) -> None:
    db_path = tmp_path / "state.db"
    _ledger_db(db_path)
    wallet = "0x" + "11" * 20
    token = "0x" + "22" * 20

    def fake_rpc(_url: str, method: str, params: list) -> object:
        if method == "eth_getTransactionReceipt":
            return {
                "transactionHash": TEST_TX_HASH,
                "blockNumber": "0xa",
                "blockHash": "0xblock",
                "status": "0x1",
                "gasUsed": "0x5208",
                "effectiveGasPrice": "0x1",
                "logs": [
                    {
                        "address": token,
                        "topics": [
                            dedicated.TRANSFER_TOPIC,
                            "0x" + "0" * 24 + wallet[2:],
                            "0x" + "0" * 24 + "33" * 20,
                        ],
                        "data": "0x" + f"{5:064x}",
                    }
                ],
            }
        if method == "eth_getTransactionByHash":
            return {"from": wallet, "value": native_value}
        assert method == "eth_call"
        return hex(100 if params[1] == "0x9" else 95)

    monkeypatch.setattr(dedicated, "_rpc", fake_rpc)
    monkeypatch.setattr(dedicated, "_fork_submissions", lambda _url, rows, **_kwargs: (rows, {"status": "PASS"}))
    observed = dedicated.capture_anvil_witnesses(
        db_path=db_path,
        rpc_url="http://localhost:8545",
        output_dir=tmp_path / "chain",
    )

    assert observed["status"] == ("PASS" if native_value == "0x0" else "UNMEASURED")
    assert observed["transactions"][0]["balance_checks"][0]["status"] == "PASS"
    assert {row["kind"] for row in observed["artifacts"]} == {
        "rpc-receipt",
        "rpc-transaction",
        "rpc-balance-witness",
    }


def test_builder_rejects_incomplete_receipt_set_even_when_shape_landed(tmp_path: Path) -> None:
    """A stale pre-teardown observation cannot certify a larger final ledger."""
    db_path = tmp_path / "state.db"
    connection = sqlite3.connect(db_path)
    connection.executescript(
        """
        CREATE TABLE transaction_ledger (
            id TEXT, deployment_id TEXT, intent_type TEXT, tx_hash TEXT, timestamp TEXT
        );
        CREATE TABLE accounting_events (deployment_id TEXT);
        CREATE TABLE portfolio_snapshots (deployment_id TEXT);
        CREATE TABLE portfolio_metrics (deployment_id TEXT);
        CREATE TABLE teardown_requests (status TEXT, requested_at TEXT);
        INSERT INTO transaction_ledger VALUES
            ('ledger-1', 'deployment:test', 'SUPPLY', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '2026-08-09T00:00:00Z'),
            ('ledger-2', 'deployment:test', 'BORROW', '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', '2026-08-09T00:01:00Z');
        INSERT INTO accounting_events VALUES ('deployment:test');
        INSERT INTO portfolio_snapshots VALUES ('deployment:test');
        INSERT INTO portfolio_metrics VALUES ('deployment:test');
        INSERT INTO teardown_requests VALUES ('COMPLETED', '2026-08-09T00:02:00Z');
        """
    )
    connection.execute("ALTER TABLE transaction_ledger ADD COLUMN success INTEGER DEFAULT 1")
    connection.execute("ALTER TABLE transaction_ledger ADD COLUMN error TEXT DEFAULT ''")
    connection.execute("ALTER TABLE transaction_ledger ADD COLUMN gas_used INTEGER DEFAULT 21000")
    connection.commit()
    connection.close()
    strat_log = tmp_path / "strat.log"
    strat_log.write_text("Compiled SUPPLY\nCompiled BORROW\n")
    teardown_log = tmp_path / "teardown.log"
    teardown_log.write_text("completed\n")

    evidence = dedicated.build_dedicated_evidence(
        row_id="looping-aave_v3-arbitrum",
        fixture="looping",
        chain="arbitrum",
        primitive="looping",
        deployment_id="deployment:test",
        expected_shape={"SUPPLY": 1, "BORROW": 1},
        db_path=db_path,
        accountant={"scores": {"passed": 1, "failed": 0, "xfailed": 0, "skipped": 0, "total": 1}},
        strat_log=strat_log,
        teardown_log=teardown_log,
        chain_witnesses={
            "status": "PASS",
            "transactions": [
                {"tx_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "receipt_status": 1}
            ],
            "transaction_count": 1,
            "balance_check_count": 1,
            "failures": [],
            "artifacts": [],
        },
    )

    assert evidence.stages["execute"].status == "FAIL"
    assert evidence.stages["receipt_balance"].status == "FAIL"
    assert evidence.stages["receipt_balance"].facts["receipt_set_complete"] is False
    assert evidence.status == "FAIL"


def test_teardown_stage_requires_terminal_counts_and_chain_verification(tmp_path: Path) -> None:
    tx_hash = "0x" + "ab" * 32
    db_path = tmp_path / "state.db"
    connection = sqlite3.connect(db_path)
    connection.executescript(
        f"""
        CREATE TABLE transaction_ledger (
            id TEXT, deployment_id TEXT, intent_type TEXT, tx_hash TEXT, timestamp TEXT
        );
        CREATE TABLE accounting_events (deployment_id TEXT);
        CREATE TABLE portfolio_snapshots (deployment_id TEXT);
        CREATE TABLE portfolio_metrics (deployment_id TEXT);
        CREATE TABLE teardown_requests (
            status TEXT, requested_at TEXT, completed_at TEXT,
            positions_total INTEGER, positions_closed INTEGER, positions_failed INTEGER,
            result_json TEXT
        );
        INSERT INTO transaction_ledger VALUES
            ('ledger-1', 'deployment:test', 'SUPPLY', '{tx_hash}', '2020-01-01T00:00:00+00:00');
        INSERT INTO accounting_events VALUES ('deployment:test');
        INSERT INTO portfolio_snapshots VALUES ('deployment:test');
        INSERT INTO portfolio_metrics VALUES ('deployment:test');
        INSERT INTO teardown_requests VALUES (
            'COMPLETED', '2020-01-01T00:00:00+00:00', '2020-01-01T00:01:00+00:00',
            1, 1, 0, '{{"verification_status":"unverified"}}'
        );
        """
    )
    connection.execute("ALTER TABLE transaction_ledger ADD COLUMN success INTEGER DEFAULT 1")
    connection.execute("ALTER TABLE transaction_ledger ADD COLUMN error TEXT DEFAULT ''")
    connection.execute("ALTER TABLE transaction_ledger ADD COLUMN gas_used INTEGER DEFAULT 21000")
    connection.commit()
    connection.close()
    strat_log = tmp_path / "strat.log"
    strat_log.write_text("Compiled SUPPLY\n")
    teardown_log = tmp_path / "teardown.log"
    teardown_log.write_text("completed\n")

    def build(
        *,
        passed: int = 1,
        xfailed: int = 0,
        skipped: int = 0,
        details_total: int | None = None,
        published_skipped: int | None = None,
        extra_status: str | None = None,
    ) -> dedicated.BottomUpEvidence:
        total = passed + xfailed + skipped + (1 if extra_status else 0)
        cell_details: list[dict[str, str]] = (
            [{"id": f"P{i}", "status": "PASS"} for i in range(passed)]
            + [{"id": f"X{i}", "status": "XFAIL"} for i in range(xfailed)]
            + [{"id": f"S{i}", "status": "SKIP"} for i in range(skipped)]
        )
        if extra_status:
            cell_details.append({"id": "E0", "status": extra_status})
        # The canonical AccountantReport.to_json() omits ``skipped`` from its
        # compact score object; some frozen baselines publish it.  Model both.
        scores = {
            "passed": passed,
            "failed": 0,
            "xfailed": xfailed,
            "total": total if details_total is None else details_total,
        }
        if published_skipped is not None:
            scores["skipped"] = published_skipped
        accountant = {"scores": scores, "cell_details": cell_details}
        return dedicated.build_dedicated_evidence(
            row_id="lending-aave-arbitrum",
            fixture="lending",
            chain="arbitrum",
            primitive="looping",
            deployment_id="deployment:test",
            expected_shape={"SUPPLY": 1},
            db_path=db_path,
            accountant=accountant,
            strat_log=strat_log,
            teardown_log=teardown_log,
            chain_witnesses={
                "status": "PASS",
                "transactions": [{"tx_hash": tx_hash, "receipt_status": 1}],
                "transaction_count": 1,
                "balance_check_count": 1,
                "failures": [],
                "artifacts": [],
            },
        )

    unverified = build()
    assert unverified.stages["teardown"].status == "FAIL"
    assert unverified.stages["teardown"].facts["verification_status"] == "unverified"
    expected_gap = build(xfailed=1)
    assert expected_gap.stages["accountant"].status == "UNMEASURED"
    assert "1 expected gap" in expected_gap.stages["accountant"].diagnostic

    # LIVENESS: the shape every real-Anvil baseline in
    # tests/fixtures/accounting/matrix/ actually has (1-2 structurally
    # unscoreable cells) must still certify, or the gate cannot discriminate.
    real_shape = build(passed=18, xfailed=3, skipped=2)
    assert real_shape.stages["accountant"].status == "UNMEASURED"
    assert real_shape.stages["accountant"].facts["derived_status_counts"] == {"PASS": 18, "SKIP": 2, "XFAIL": 3}
    assert real_shape.stages["accountant"].facts["unmeasured_cell_ids"] == ["S0", "S1", "X0", "X1", "X2"]
    assert build(passed=18, xfailed=3, skipped=2, published_skipped=2).stages["accountant"].status == "UNMEASURED"
    at_ceiling = build(passed=18, xfailed=3, skipped=dedicated.MAX_UNMEASURED_ACCOUNTANT_CELLS)
    assert at_ceiling.stages["accountant"].status == "UNMEASURED"

    # The named vacuity: 1 PASS + 20 SKIP of 21 cells sealed as a green Books row.
    vacuous_skip = build(skipped=20)
    assert vacuous_skip.stages["accountant"].status == "FAIL"
    assert vacuous_skip.status == "FAIL"
    assert "20 unmeasured (SKIP) cell(s)" in vacuous_skip.stages["accountant"].diagnostic
    # One cell past the ceiling, so the bound itself discriminates -- not merely
    # the extreme case.
    over_ceiling = build(passed=17, xfailed=3, skipped=dedicated.MAX_UNMEASURED_ACCOUNTANT_CELLS + 1)
    assert over_ceiling.stages["accountant"].status == "FAIL"
    assert "unmeasured (SKIP) cell(s)" in over_ceiling.stages["accountant"].diagnostic

    # A published SKIP score that disagrees with the per-cell record is a
    # tampered or stale score object, not a green run.
    assert build(passed=18, xfailed=3, skipped=2, published_skipped=0).stages["accountant"].status == "FAIL"
    # A cell status outside the canonical vocabulary must fail loudly rather than
    # slip through an unrecognised bucket.
    assert build(passed=18, xfailed=3, extra_status="UNKNOWN").stages["accountant"].status == "FAIL"

    mismatched_details = build(skipped=20, details_total=1)
    assert mismatched_details.stages["accountant"].status == "FAIL"
    assert mismatched_details.status == "FAIL"
    missing_details = dedicated.build_dedicated_evidence(
        row_id="lending-aave-arbitrum",
        fixture="lending",
        chain="arbitrum",
        primitive="looping",
        deployment_id="deployment:test",
        expected_shape={"SUPPLY": 1},
        db_path=db_path,
        accountant={"scores": {"passed": 21, "failed": 0, "xfailed": 0, "total": 21}},
        strat_log=strat_log,
        teardown_log=teardown_log,
        chain_witnesses={
            "status": "PASS",
            "transactions": [{"tx_hash": tx_hash, "receipt_status": 1}],
            "transaction_count": 1,
            "balance_check_count": 1,
            "failures": [],
            "artifacts": [],
        },
    )
    assert missing_details.stages["accountant"].status == "FAIL"
    all_expected_gap = build(passed=0, xfailed=1)
    assert all_expected_gap.stages["accountant"].status == "FAIL"

    connection = sqlite3.connect(db_path)
    connection.execute(
        "UPDATE teardown_requests SET result_json = ?",
        ('{"verification_status":"chain_verified"}',),
    )
    connection.commit()
    connection.close()

    verified = build()
    assert verified.stages["teardown"].status == "UNMEASURED"


def test_managed_anvil_endpoint_is_resolved_from_runner_boot_evidence(tmp_path: Path) -> None:
    log = tmp_path / "strat.log"
    log.write_text(
        "Anvil fork started for arbitrum on port 59851 (fork: https://redacted/***)\n"
        "Network: ANVIL (local fork at http://127.0.0.1:59851)\n"
    )

    assert _read_managed_anvil_rpc_url(log, "arbitrum") == "http://127.0.0.1:59851"
    assert _read_managed_anvil_rpc_url(log, "base") == "http://127.0.0.1:59851"


def test_managed_anvil_endpoint_rejects_unowned_or_invalid_log(tmp_path: Path) -> None:
    log = tmp_path / "strat.log"
    log.write_text("Network: ANVIL (local fork at https://rpc.example:443)\n")

    assert _read_managed_anvil_rpc_url(log, "arbitrum") == ""


def test_detached_anvil_pid_is_bound_to_observed_loopback_port(tmp_path: Path) -> None:
    log = tmp_path / "strat.log"
    log.write_text("Anvil for arbitrum still running on port 61234 (PID 43210)\n")

    assert _read_detached_anvil_pid(log, "http://127.0.0.1:61234") == 43210
    assert _read_detached_anvil_pid(log, "http://127.0.0.1:61235") is None
    assert _read_detached_anvil_pid(log, "http://example.com:61234") is None


@pytest.mark.parametrize(
    "key",
    [
        "submission_census",
        "gas_matches",
        "observed_gas_used",
        "ledger_gas_used",
        "convergence",
        "convergence_matches_database",
    ],
)
def test_pass_validator_requires_observation_gates(tmp_path: Path, key: str) -> None:
    payload = _payload(tmp_path)
    del payload["stages"]["receipt_balance"]["facts"][key]
    expected = "census" if "census" in key else "gas" if "gas" in key else "convergence"
    with pytest.raises(ValueError, match=expected):
        dedicated.validate_dedicated_evidence(payload, bundle=tmp_path)


@pytest.mark.parametrize("value", [None, "0x1", "invalid"])
def test_pass_validator_rejects_unwitnessed_native_value(tmp_path: Path, value: str | None) -> None:
    payload = _payload(tmp_path)
    stage = payload["stages"]["receipt_balance"]
    artifact = next(row for row in stage["artifacts"] if row["kind"] == "rpc-transaction")
    path = tmp_path / artifact["path"]
    transaction = json.loads(path.read_text())
    transaction["value"] = value
    path.write_text(json.dumps(transaction))
    artifact["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="native value"):
        dedicated.validate_dedicated_evidence(payload, bundle=tmp_path)


@pytest.mark.parametrize("mutation", ["census-artifact", "gas-artifact", "convergence-hashes", "convergence-count"])
def test_pass_validator_rederives_observation_gates(tmp_path: Path, mutation: str) -> None:
    payload = _payload(tmp_path)
    stage = payload["stages"]["receipt_balance"]
    if mutation.endswith("artifact"):
        kind = "rpc-submission-census" if mutation == "census-artifact" else "rpc-receipt"
        artifact = next(row for row in stage["artifacts"] if row["kind"] == kind)
        path = tmp_path / artifact["path"]
        raw = json.loads(path.read_text())
        if kind == "rpc-receipt":
            raw["gasUsed"] = "0x5209"
        else:
            raw["status"] = "FAIL"
        path.write_text(json.dumps(raw))
        artifact["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    elif mutation == "convergence-hashes":
        stage["facts"]["convergence"]["ledger_tx_hashes"] = []
    else:
        stage["facts"]["convergence"]["consecutive_complete_observations"] = 1
    expected = mutation.split("-", 1)[0]
    with pytest.raises(ValueError, match=expected):
        dedicated.validate_dedicated_evidence(payload, bundle=tmp_path)


@pytest.mark.parametrize("baseline", [None, "missing.json", "empty.json", "invalid.json"])
def test_absent_baseline_cannot_claim_no_regressions(tmp_path: Path, baseline: str | None) -> None:
    (tmp_path / "empty.json").write_text('{"cells": {}}')
    (tmp_path / "invalid.json").write_text('{"cells": []}')
    cells = matrix_runner._read_baseline(tmp_path, baseline)
    assert matrix_runner._baseline_comparison(cells, "OK", []) == "UNMEASURED"


@pytest.mark.parametrize("comparison", ["UNMEASURED", "NO_REGRESSIONS", "REGRESSIONS"])
def test_matrix_outputs_keep_baseline_grade_separate_from_dedicated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], comparison: str
) -> None:
    root = Path(__file__).resolve().parents[3]
    regressions = ["G1"] if comparison == "REGRESSIONS" else []
    baseline = None if comparison == "UNMEASURED" else {"G1": "PASS"}
    grade = matrix_runner._baseline_comparison(baseline, "OK", regressions)
    assert grade == comparison
    result = matrix_runner.RowResult(
        row_id="lp-uniswap_v3-arbitrum",
        fixture="lp",
        chain="arbitrum",
        primitive="lp",
        status="UNMEASURED",
        dedicated_status="UNMEASURED",
        error=None,
        started_at="",
        finished_at="",
        duration_seconds=0,
        cells={},
        regressions=regressions,
        new_passes=[],
        classifications=[],
        scores={},
        baseline_path=None,
        accountant_json_path="",
        strategy_log_path="",
        db_path="",
        dedicated_evidence_path="",
        baseline_comparison=grade,
    )
    monkeypatch.setattr(matrix_runner, "_run_row", lambda *args, **kwargs: result)
    assert (
        matrix_runner.main(
            [
                "--matrix",
                str(root / "qa_lab/accounting-matrix.yml"),
                "--output-dir",
                str(tmp_path),
                "--rows-include",
                result.row_id,
            ]
        )
        == 1
    )
    captured = capsys.readouterr()
    assert f"dedicated=UNMEASURED matrix_gate={comparison}" in captured.err
    assert f"{comparison}=1" in captured.out
    assert f"| UNMEASURED | {comparison} |" in (tmp_path / "matrix.md").read_text()
    recorded = json.loads((tmp_path / "matrix.json").read_text())["results"][0]
    assert recorded["baseline_comparison"] == comparison
    assert recorded["dedicated_status"] == "UNMEASURED"


@pytest.mark.parametrize("raw", [[], "malformed", None])
def test_non_object_transaction_data_is_not_a_native_witness(tmp_path: Path, raw: object) -> None:
    assert dedicated._native_value_unmeasured(raw) is True
    db = tmp_path / "state.db"
    with sqlite3.connect(db) as connection:
        connection.execute("CREATE TABLE transaction_ledger (id TEXT, timestamp TEXT, extracted_data_json TEXT)")
        connection.execute("INSERT INTO transaction_ledger VALUES ('id','now',?)", (json.dumps(raw),))
    with pytest.raises(ValueError, match="Malformed transaction_ledger extracted_data_json"):
        dedicated.ledger_transactions(db)
