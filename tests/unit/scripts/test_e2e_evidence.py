import json
import sqlite3
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace

import pytest

from qa_lab import e2e_evidence as evidence

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures/accounting/harness/lp-dual-quantities/captured.json"


def test_collection_snapshots_committed_wal_and_reconciles_retained_raw_witnesses(tmp_path, monkeypatch):
    captured = json.loads(FIXTURE.read_text())
    subject = tmp_path / "subject"
    subject.mkdir()
    database = subject / "almanak_state.db"
    columns = list(captured["rows"][0])
    checks = []
    context = SimpleNamespace(
        root=tmp_path,
        rpc_url="http://127.0.0.1:1234",
        require_owned=lambda path: path,
        assert_rpc_identity=lambda: checks.append("identity"),
    )

    def observe(*, db_path, rpc_url, output_dir):
        assert db_path != database
        assert rpc_url == context.rpc_url
        with closing(sqlite3.connect(db_path.as_uri() + "?mode=ro", uri=True)) as reader:
            assert reader.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
            assert reader.execute("SELECT count(*) FROM transaction_ledger").fetchone()[0] == len(captured["rows"])
        output_dir.mkdir()
        for name, value in captured["witnesses"].items():
            (output_dir / name).write_text(json.dumps(value))
        return {"status": "PASS"}

    monkeypatch.setattr(evidence, "capture_anvil_witnesses", observe)
    writer = sqlite3.connect(database)
    try:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE transaction_ledger (" + ",".join(columns) + ")")
        writer.executemany(
            "INSERT INTO transaction_ledger VALUES (" + ",".join("?" for _ in columns) + ")",
            [[row[column] for column in columns] for row in captured["rows"]],
        )
        writer.commit()
        assert database.with_name(database.name + "-wal").stat().st_size > 0
        output = tmp_path / "quantities"
        result = evidence.capture_subject_quantities(context, wallet=captured["wallet"], output=output)
        assert result["status"] == "PASS"
        assert len(result["wallet_quantities"]["rows"]) == 6
        assert len(result["wallet_quantities"]["transaction_hashes"]) == 17
        assert result["e2e_admission"] == "UNMEASURED"
        assert json.loads((output / "result.json").read_text()) == result
        assert checks == ["identity", "identity"]
        assert writer.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert not any(
            Path(str(output / "almanak_state.db") + suffix).exists() for suffix in ("-wal", "-shm", "-journal")
        )
        with pytest.raises(FileExistsError):
            evidence.capture_subject_quantities(context, wallet=captured["wallet"], output=output)
    finally:
        writer.close()


def test_collection_failure_is_unmeasured_and_does_not_expose_rpc_error(tmp_path, monkeypatch):
    def unavailable():
        raise RuntimeError("RPC failed with a credential-bearing endpoint")

    context = SimpleNamespace(root=tmp_path, require_owned=lambda path: path, assert_rpc_identity=unavailable)
    result = evidence.capture_subject_quantities(context, wallet="unused", output=tmp_path / "quantities")
    assert result["status"] == result["e2e_admission"] == "UNMEASURED"
    assert result["error_type"] == "RuntimeError"
    assert "credential" not in json.dumps(result)
    assert not (tmp_path / "quantities/almanak_state.db").exists()


def test_actor_capture_cannot_fall_back_to_the_subject_database(tmp_path, monkeypatch):
    from qa_lab.e2e_evidence import capture_stimulus_quantities

    (tmp_path / "subject").mkdir()
    with sqlite3.connect(tmp_path / "subject/almanak_state.db") as db:
        db.execute("CREATE TABLE subject_only (value INTEGER)")
    context = SimpleNamespace(root=tmp_path, require_owned=lambda path: path, assert_rpc_identity=lambda: None)
    monkeypatch.setattr(evidence, "capture_anvil_witnesses", lambda **kwargs: pytest.fail("actor database is absent"))
    result = capture_stimulus_quantities(context, output=tmp_path / "actor-quantities")
    assert result["status"] == "UNMEASURED"
    assert result["scope"] == "actor_quantity_capture"
    assert not (tmp_path / "actor-quantities/almanak_state.db").exists()


@pytest.mark.parametrize("chain_status", ["FAIL", "UNMEASURED"])
def test_actor_matching_rows_cannot_hide_missing_chain_coverage(tmp_path, monkeypatch, chain_status):
    from qa_lab.e2e_evidence import capture_stimulus_quantities

    (tmp_path / "actor").mkdir()
    with sqlite3.connect(tmp_path / "actor/almanak_state.db") as db:
        db.execute("CREATE TABLE actor_marker (value INTEGER)")
    context = SimpleNamespace(
        root=tmp_path,
        rpc_url="http://127.0.0.1:1234",
        require_owned=lambda path: path,
        assert_rpc_identity=lambda: None,
    )
    pin = (101, "0x" + "aa" * 32)

    def observe(**kwargs):
        assert kwargs["terminal_block"] == pin
        return {"status": chain_status}

    monkeypatch.setattr(evidence, "capture_anvil_witnesses", observe)
    monkeypatch.setattr(evidence, "ledger_quantities", lambda *args, **kwargs: {"status": "PASS"})
    monkeypatch.setattr(evidence, "_actor_lifecycle", lambda context, snapshot, quantities: quantities)
    result = capture_stimulus_quantities(context, output=tmp_path / "actor-quantities", terminal_block=pin)
    assert result["status"] == chain_status
    assert result["wallet_quantities"]["status"] == "PASS"
    assert result["chain_observation_status"] == chain_status
    assert result["sdk_lifecycle"] is None


@pytest.mark.parametrize("mutation", [None, "extra_action", "amount"])
def test_actor_must_match_one_bound_stimulus_and_one_unwind(tmp_path, monkeypatch, mutation):
    from qa_lab import e2e_lifecycle
    from qa_lab.chains import TOKENS
    from qa_lab.e2e_card import canonical

    rows = [
        {"phase": "runtime", "intent_type": "SWAP", "token_in": "USDC", "token_out": "WETH", "tx_hash": "first"},
        {"phase": "teardown", "intent_type": "SWAP", "token_in": "WETH", "token_out": "USDC", "tx_hash": "second"},
    ]
    if mutation == "extra_action":
        rows.append(rows[0])
    monkeypatch.setattr(e2e_lifecycle, "_rows", lambda path: rows)
    (tmp_path / "actor").mkdir()
    (tmp_path / "actor/config.json").write_bytes(canonical({"amount_usdc_raw": 1000000}))
    quantities = {
        "status": "PASS",
        "rows": [
            {
                "tx_hash": "first",
                "raw_wallet_deltas": {
                    TOKENS["arbitrum"]["USDC"][0].lower(): "-999999" if mutation == "amount" else "-1000000"
                },
            }
        ],
    }
    context = SimpleNamespace(root=tmp_path, require_owned=lambda path: path)
    result = evidence._actor_lifecycle(context, tmp_path / "unused", quantities)
    assert result["status"] == ("PASS" if mutation is None else "FAIL")


def test_quantity_exception_preserves_independent_lifecycle_outputs(tmp_path, monkeypatch):
    (tmp_path / "subject").mkdir()
    with sqlite3.connect(tmp_path / "subject/almanak_state.db") as db:
        db.execute("CREATE TABLE marker (value INTEGER)")
    context = SimpleNamespace(
        root=tmp_path,
        rpc_url="http://127.0.0.1:1234",
        require_owned=lambda path: path,
        assert_rpc_identity=lambda: None,
    )
    monkeypatch.setattr(evidence, "capture_anvil_witnesses", lambda **kwargs: {"status": "PASS"})

    def capture(context, snapshot, output):
        for name in ("lifecycle-coverage.json", "receipt-reconciliation.json"):
            (output / name).write_text('{"status": "PASS"}')
        return {"status": "PASS", "e2e_admission": "UNMEASURED"}

    def quantities(*args, **kwargs):
        raise AttributeError("unexpected quantity parser failure")

    monkeypatch.setattr(evidence, "_capture_lifecycle", capture)
    monkeypatch.setattr(evidence, "ledger_quantities", quantities)
    output = tmp_path / "quantities"
    result = evidence.capture_subject_quantities(context, wallet="unused", output=output)
    assert result["status"] == "UNMEASURED"
    assert result["error_type"] == "AttributeError"
    assert result["sdk_lifecycle"]["status"] == "PASS"
    for name in ("lifecycle-coverage.json", "receipt-reconciliation.json"):
        assert json.loads((output / name).read_text()) == {"status": "PASS"}
