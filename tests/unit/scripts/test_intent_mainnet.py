"""Contracts for live-mainnet Intent certification from sealed quant runs."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sqlite3
import sys
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_abi import encode as abi_encode

from almanak.connectors.gmx_v2.perps_read import _GET_ACCOUNT_POSITIONS_OUTPUT
from scripts.qa.intent_semantic_contract import validate_semantic_contract

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "quant-test" / "intent_mainnet.py"
SPEC = importlib.util.spec_from_file_location("intent_mainnet_test", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
intent_mainnet = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = intent_mainnet
SPEC.loader.exec_module(intent_mainnet)


def _qa_coverage_module():
    path = Path(__file__).resolve().parents[3] / "scripts" / "quant-test" / "qa_coverage.py"
    spec = importlib.util.spec_from_file_location("qa_coverage_gmx_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _make_quant_run(tmp_path: Path, *, dirty: bool = False, chain: str = "arbitrum") -> Path:
    run = tmp_path / "quant-run"
    run.mkdir()
    db = run / "db.sqlite"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE transaction_ledger (
          id TEXT, execution_mode TEXT, timestamp TEXT, intent_type TEXT,
          tx_hash TEXT, chain TEXT, protocol TEXT, success INTEGER,
          extracted_data_json TEXT, pre_state_json TEXT, post_state_json TEXT
        );
        CREATE TABLE accounting_events (
          id TEXT, ledger_entry_id TEXT, event_type TEXT, tx_hash TEXT,
          confidence TEXT, payload_json TEXT
        );
        """
    )
    market = "0x" + "44" * 20
    collateral = "0x" + "55" * 20
    for index, intent in enumerate(("PERP_OPEN", "PERP_CLOSE")):
        suffix = index * 2
        order = "0x" + ("ab" if index == 0 else "cd") * 32
        submission = "0x" + f"{0x11 + suffix:02x}" * 32
        keeper = "0x" + f"{0x12 + suffix:02x}" * 32
        ledger_id = f"ledger-{intent.lower()}"
        keeper_block = 120 + index * 20
        conn.execute(
            "INSERT INTO transaction_ledger VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                ledger_id,
                "live",
                f"2026-08-06T00:0{index}:00Z",
                intent,
                submission,
                chain,
                "gmx_v2",
                1,
                json.dumps({"async_orders": [f"AsyncOrderData(order_id='{order}')"]}),
                json.dumps({"wallet_balances": {"USDC": "3.1"}}),
                json.dumps({"wallet_balances": {"USDC": "0.1"}}),
            ),
        )
        conn.execute(
            "INSERT INTO accounting_events VALUES (?,?,?,?,?,?)",
            (
                f"settlement-{intent.lower()}",
                ledger_id,
                "PERP_SETTLEMENT",
                keeper,
                "HIGH",
                json.dumps(
                    {
                        "settlement_state": "EXECUTED",
                        "order_key": order,
                        "keeper_tx_hash": keeper,
                        "block_number": keeper_block,
                        "is_open": intent == "PERP_OPEN",
                        "is_long": True,
                        "market": market,
                        "collateral_token": collateral,
                        "position_key": "perp:gmx:exact",
                        "collateral_delta_amount": "2996400",
                        "size_delta_usd": "6",
                    }
                ),
            ),
        )
    conn.commit()
    conn.close()
    data = run / "data.json"
    data.write_text(
        json.dumps(
            {
                "meta": {
                    "chain": chain,
                    "wallet": "0x" + "33" * 20,
                    "tested_sha": "a" * 40,
                },
                "cap_gate": "PASS",
            }
        )
    )
    finding = run / "finding.json"
    finding.write_text(json.dumps({"verdicts": {"strategy": "PASS"}}))
    artifacts = [
        ("db.sqlite", db),
        ("Machine-derived run data", data),
        ("Findings and verdicts", finding),
    ]
    manifest = {
        "schema_version": 1,
        "run_id": "quant-mainnet-gmx",
        "root_relative": "runs/2026/08/06/quant-mainnet-gmx",
        "cell_id": f"perp.gmx_v2.{chain}.simple.mainnet.eoa",
        "network": "mainnet",
        "exec_path": "eoa",
        "git": {"commit": "a" * 40, "dirty": dirty},
        "selection": {"tested_sha": "a" * 40},
        "verdicts": {"strategy": "PASS", "books": "FAIL"},
        "artifacts": [
            {"label": label, "relpath": f"runs/2026/08/06/quant-mainnet-gmx/{path.name}", "sha256": _sha(path)}
            for label, path in artifacts
        ],
    }
    (run / "manifest.json").write_text(json.dumps(manifest))
    return run


class FakeReader:
    def __init__(self, chain: str = "arbitrum") -> None:
        self.chain = chain

    def receipt(self, tx_hash: str) -> dict:
        block = {
            "0x" + "11" * 32: 110,
            "0x" + "12" * 32: 120,
            "0x" + "13" * 32: 130,
            "0x" + "14" * 32: 140,
        }[tx_hash]
        logs = []
        if block in {110, 130}:
            order = "0x" + ("ab" if block == 110 else "cd") * 32
            logs.append(self._event("OrderCreated", order, 1))
        else:
            order = "0x" + ("ab" if block == 120 else "cd") * 32
            logs.extend(
                [
                    self._event("OrderExecuted", order, 1),
                    self._event("PositionIncrease" if block == 120 else "PositionDecrease", None, 2),
                ]
            )
        return {
            "transactionHash": tx_hash,
            "blockNumber": block,
            "blockHash": "0x" + f"{block:064x}",
            "status": 1,
            "logs": logs,
        }

    def _event(self, event_name: str, key: str | None, log_index: int) -> dict:
        topics = ["0x" + "00" * 32, intent_mainnet.Web3.to_hex(intent_mainnet.Web3.keccak(text=event_name))]
        if key is not None:
            topics.append(key)
        return {
            "address": intent_mainnet.GMX_V2[self.chain]["event_emitter"],
            "topics": topics,
            "data": "0x",
            "logIndex": log_index,
        }

    def token_balance(self, _token: str, _wallet: str, block: int) -> int:
        return {
            109: 3_100_000,
            110: 100_000,
            119: 100_000,
            120: 100_000,
            129: 100_000,
            130: 90_000,
            139: 90_000,
            140: 3_000_000,
        }[block]

    def native_balance(self, _wallet: str, block: int) -> int:
        return {109: 10**16, 110: 9 * 10**15, 129: 9 * 10**15, 130: 8 * 10**15}[block]

    def positions(self, _chain: str, wallet: str, block: int) -> dict:
        rows = []
        if block in {120, 139}:
            rows = [
                {
                    "account": wallet.lower(),
                    "market": "0x" + "44" * 20,
                    "collateral_token": "0x" + "55" * 20,
                    "is_long": True,
                    "size_in_usd": 6 * 10**30,
                }
            ]
        raw_rows = [
            (
                (row["account"], row["market"], row["collateral_token"]),
                (row["size_in_usd"], 0, 0, 0, 0, 0, 0, 0, 1, 0),
                (row["is_long"],),
            )
            for row in rows
        ]
        addresses = intent_mainnet.GMX_V2[self.chain]
        query = intent_mainnet.PerpsPositionQuery(
            chain=self.chain,
            wallet_address=wallet,
            targets={"reader": addresses["reader"], "data_store": addresses["data_store"]},
        )
        call = intent_mainnet.PERPS_READ_SPEC.build_calls(query)[0]
        return {
            "block": block,
            "block_hash": "0x" + f"{block:064x}",
            "to": call.to.lower(),
            "data": call.data.lower(),
            "result": "0x" + abi_encode([_GET_ACCOUNT_POSITIONS_OUTPUT], [raw_rows]).hex(),
            "positions": rows,
        }

    def pending_order_keys(self, _chain: str, wallet: str, block: int) -> dict:
        return {
            "block": block,
            "block_hash": "0x" + f"{block:064x}",
            "to": intent_mainnet.GMX_V2[self.chain]["data_store"].lower(),
            "count_data": intent_mainnet.build_order_count_calldata(wallet).lower(),
            "count_result": "0x" + abi_encode(["uint256"], [0]).hex(),
            "keys_data": None,
            "keys_result": None,
            "keys": [],
        }


def test_quant_validation_rejects_dirty_run(tmp_path: Path) -> None:
    run = _make_quant_run(tmp_path, dirty=True)
    with pytest.raises(ValueError, match="clean"):
        intent_mainnet.validate_quant_run(run)


@pytest.mark.parametrize(
    ("chain", "explorer"),
    [("arbitrum", "https://arbiscan.io/tx/"), ("avalanche", "https://snowtrace.io/tx/")],
)
def test_certifier_emits_exact_explorer_backed_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, chain: str, explorer: str
) -> None:
    run = _make_quant_run(tmp_path, chain=chain)
    orders = {"PERP_OPEN": "0x" + "ab" * 32, "PERP_CLOSE": "0x" + "cd" * 32}

    class Parser:
        def __init__(self, *, chain: str):
            assert chain in {"arbitrum", "avalanche"}

        def extract_async_orders(self, _receipt: dict, *, intent_type: str):
            return [SimpleNamespace(order_id=orders[intent_type])]

        def extract_perp_fill(self, _receipt: dict, *, order_key: str, account: str):
            assert account == "0x" + "33" * 20
            is_open = order_key == orders["PERP_OPEN"]
            return SimpleNamespace(
                order_key=order_key,
                position_key="0x" + "66" * 32,
                account=account,
                market="0x" + "44" * 20,
                collateral_token="0x" + "55" * 20,
                is_open=is_open,
                is_long=True,
                size_delta_usd="6",
            )

    monkeypatch.setattr(intent_mainnet, "GMXv2ReceiptParser", Parser)
    output = tmp_path / "evidence"
    evidence, junit = intent_mainnet.certify_gmx_quant_run(run_dir=run, output_dir=output, reader=FakeReader(chain))

    assert evidence.is_file() and junit.is_file()
    manifest = json.loads(evidence.read_text())
    assert len(manifest["nodes"]) == 2
    assert {node["outcome"] for node in manifest["nodes"]} == {"PASS"}
    assert {node["intents"][0]["intent_cell_id"] for node in manifest["nodes"]} == {
        f"intent.gmx_v2.{chain}.PERP_OPEN.mainnet.eoa",
        f"intent.gmx_v2.{chain}.PERP_CLOSE.mainnet.eoa",
    }
    artifacts = [json.loads(path.read_text()) for path in (output / "receipts").rglob("*.json")]
    assert len(artifacts) == 4
    assert {artifact["receipt_role"] for artifact in artifacts} == {"submission", "settlement"}
    assert all(artifact["tx"]["explorer_url"].startswith(explorer) for artifact in artifacts)
    assert all(artifact["source_provenance"]["kind"] == "sealed-quant-runtime" for artifact in artifacts)
    assert {
        validate_semantic_contract(artifact, expected_profile="async_perp.v1")["status"] for artifact in artifacts
    } == {"VERIFIED"}
    qa = _qa_coverage_module()
    for intent_name in ("PERP_OPEN", "PERP_CLOSE"):
        intent_artifacts = [artifact for artifact in artifacts if artifact["intent"] == intent_name]
        intent_axes = {
            key: intent_artifacts[0][key]
            for key in ("intent_cell_id", "protocol", "intent", "chain", "network", "exec_path")
        }
        receipts = []
        for index, artifact in enumerate(intent_artifacts):
            grade, layers, role, verification = qa._validate_receipt_payload(
                deepcopy(artifact),
                source=tmp_path / f"{intent_name}-{index}.json",
                intent=intent_axes,
                network="mainnet",
                contract_profile="async_perp.v1",
            )
            assert grade == "hard" and set(layers.values()) <= {"PASS", "NOT_APPLICABLE"}
            receipts.append(
                {
                    "receipt_role": role,
                    "transaction_hash": artifact["tx"]["hash"],
                    "semantic_verification": verification,
                }
            )
        qa._validate_composite_semantic_receipts(receipts, nodeid=f"gmx-{intent_name.lower()}")
    by_role = {artifact["receipt_role"]: artifact for artifact in artifacts if artifact["intent"] == "PERP_OPEN"}
    assert by_role["submission"]["layers"] == {
        "compile": "PASS",
        "execute": "PASS",
        "receipt": "PASS",
        "balances": "PASS",
        "permissions": "NOT_APPLICABLE",
    }
    assert by_role["settlement"]["layers"] == {
        "compile": "PASS",
        "execute": "PASS",
        "receipt": "PASS",
        "balances": "PASS",
        "permissions": "NOT_APPLICABLE",
    }
    assert "position_collateral" not in by_role["settlement"]["balance_deltas"]
    assert by_role["submission"]["semantic_contract"]["profile"] == "async_perp.v1"
    for mutate, error in (
        (lambda row: row["semantic_contract"].__setitem__("terminal_open_positions", 1), "empty GMX position"),
        (
            lambda row: row["semantic_contract"].__setitem__("position_size_after_raw", "1"),
            "differ from raw eth_call results",
        ),
        (
            lambda row: row["semantic_contract"]["receipt_set"].__setitem__(
                "settlement", row["semantic_contract"]["receipt_set"]["submission"]
            ),
            "distinct transaction",
        ),
        (lambda row: row["semantic_contract"]["accounting"].__setitem__("market", "0x" + "77" * 20), "market"),
        (lambda row: row["raw_receipt"].__setitem__("logs", []), "OrderExecuted"),
    ):
        corrupted = deepcopy(by_role["settlement"])
        mutate(corrupted)
        with pytest.raises(ValueError, match=error):
            validate_semantic_contract(corrupted, expected_profile="async_perp.v1")


def test_certifier_requires_one_exact_ordered_open_close_identity(tmp_path: Path) -> None:
    run = _make_quant_run(tmp_path)
    _manifest, db, data = intent_mainnet.validate_quant_run(run)
    conn = sqlite3.connect(db)
    conn.execute("DELETE FROM accounting_events WHERE ledger_entry_id = 'ledger-perp_close'")
    conn.execute("DELETE FROM transaction_ledger WHERE id = 'ledger-perp_close'")
    conn.commit()
    conn.close()
    with pytest.raises(ValueError, match="exactly one ordered OPEN→CLOSE"):
        intent_mainnet.load_runtime_claims(db, data, FakeReader())


def test_certifier_rejects_open_close_identity_drift(tmp_path: Path) -> None:
    run = _make_quant_run(tmp_path)
    _manifest, db, data = intent_mainnet.validate_quant_run(run)
    conn = sqlite3.connect(db)
    row = conn.execute(
        "SELECT payload_json FROM accounting_events WHERE ledger_entry_id = 'ledger-perp_close'"
    ).fetchone()
    payload = json.loads(row[0])
    payload["market"] = "0x" + "77" * 20
    conn.execute(
        "UPDATE accounting_events SET payload_json = ? WHERE ledger_entry_id = 'ledger-perp_close'",
        (json.dumps(payload),),
    )
    conn.commit()
    conn.close()
    with pytest.raises(ValueError, match="one exact measured position identity"):
        intent_mainnet.load_runtime_claims(db, data, FakeReader())


def test_certifier_emits_failure_junit_instead_of_disappearing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    run = _make_quant_run(tmp_path)

    class Parser:
        def __init__(self, *, chain: str):
            assert chain == "arbitrum"

        def extract_async_orders(self, _receipt: dict, *, intent_type: str):
            raise ValueError(f"cannot parse {intent_type}")

    monkeypatch.setattr(intent_mainnet, "GMXv2ReceiptParser", Parser)
    output = tmp_path / "failed-evidence"
    evidence, junit = intent_mainnet.certify_gmx_quant_run(run_dir=run, output_dir=output, reader=FakeReader())

    assert json.loads(evidence.read_text())["nodes"][0]["outcome"] == "FAIL"
    suite = intent_mainnet.ET.parse(junit).getroot()
    assert suite.get("failures") == "2"
    assert suite.find("testcase/failure") is not None


def test_wallet_probe_token_comes_from_the_token_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """VIB-6715: the probed collateral address is registry-derived, not mirrored.

    ALM-3199 deleted ``GMX_V2_TOKENS`` after VIB-6401 found nine valid
    collaterals missing from it. The certifier now reads the same static token
    registry the compiler's collateral fallback consults.

    POSITIVE CONTROL: on both connector-declared chains the registry answer is
    exactly the short-collateral address of that chain's ``Reader.getMarket``
    verified GMX market record -- two independent authorities agreeing, which
    is what makes the balance assertion meaningful rather than merely present.

    NEGATIVE CONTROL: a chain with no registered USDC row raises. A silent
    fallback would make the wallet-delta assertion compare a token nobody held,
    which reads as a pass.
    """
    from almanak.connectors.gmx_v2.permission_seed import permission_markets

    seed = permission_markets()
    for chain in ("arbitrum", "avalanche"):
        assert intent_mainnet._wallet_probe_token(chain).lower() == seed[chain].short_token.lower()
        assert seed[chain].short_token_symbol.upper() == "USDC"

    monkeypatch.setattr(intent_mainnet.USDC_TOKEN, "get_address", lambda _chain: None)
    with pytest.raises(ValueError, match="no registered address"):
        intent_mainnet._wallet_probe_token("arbitrum")
