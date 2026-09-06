"""Fault injection at the broadcast, semantic-failure, and persistence boundaries."""

from __future__ import annotations

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from hexbytes import HexBytes

from qa_lab.qa_external_provenance import capture_provenance, decode_calldata, provenance_status
from qa_lab.qa_failure_envelope import preserve_failure_bundle
from qa_lab.qa_transaction_journal import JournaledSubmitter, TransactionJournal, send_and_wait

WALLET = "0x" + "11" * 20
TARGET = "0x" + "22" * 20
TX_HASH = "0x" + "33" * 32
BLOCK_HASH = "0x" + "44" * 32


def _rpc():
    receipt = {
        "transactionHash": TX_HASH,
        "blockNumber": 100,
        "blockHash": BLOCK_HASH,
        "status": 1,
        "from": WALLET,
        "to": TARGET,
        "logs": [],
    }
    web3 = SimpleNamespace(
        provider=SimpleNamespace(
            endpoint_uri="https://user:password@rpc.example/v2/API_SECRET?token=QUERY_SECRET",
            make_request=Mock(
                return_value={
                    "result": {
                        "forkConfig": {
                            "forkBlockNumber": 80,
                            "forkBlockHash": "0x" + "55" * 32,
                            "forkUrl": "https://rpc.example/key",
                        }
                    }
                }
            ),
        ),
        eth=SimpleNamespace(
            chain_id=42161,
            get_block=Mock(
                side_effect=lambda number: {
                    "number": 100 if number == "latest" else number,
                    "hash": BLOCK_HASH if number != 80 else "0x" + "55" * 32,
                }
            ),
            get_transaction=Mock(
                return_value={
                    "hash": TX_HASH,
                    "blockNumber": 100,
                    "blockHash": BLOCK_HASH,
                    "input": "0x",
                    "from": WALLET,
                    "to": TARGET,
                }
            ),
            get_code=Mock(return_value=b"\x60\x00"),
            get_balance=Mock(return_value=10),
            get_transaction_receipt=Mock(return_value=receipt),
            send_raw_transaction=Mock(return_value=HexBytes(TX_HASH)),
            wait_for_transaction_receipt=Mock(return_value=receipt),
        ),
    )
    return web3, receipt


def _events(journal):
    return [json.loads(line) for line in journal.path.read_text().splitlines()]


def test_submission_timeout_keeps_prepared_hash_and_unavailable_receipt(tmp_path):
    web3, _ = _rpc()
    journal = TransactionJournal(output=tmp_path, web3=web3, phase="funding")

    def timeout(_raw):
        assert _events(journal)[0]["event"] == "PREPARED"
        raise TimeoutError("https://user:password@rpc.example/API_SECRET")

    web3.eth.send_raw_transaction.side_effect = timeout
    web3.eth.get_transaction_receipt.side_effect = TimeoutError("API_SECRET")
    with pytest.raises(TimeoutError):
        send_and_wait(web3, SimpleNamespace(raw_transaction=b"signed"), {"from": WALLET, "to": TARGET}, journal=journal)
    events = _events(journal)
    assert events[0]["tx_hash"].startswith("0x")
    assert events[-1]["receipt_status"] == "PENDING_OR_UNAVAILABLE"
    assert "API_SECRET" not in journal.path.read_text()
    assert "password" not in journal.path.read_text()


@pytest.mark.parametrize("encoding", ["bare", "prefixed", "bytes"])
def test_receipt_recovery_uses_rpc_data_encoding(tmp_path, encoding):
    web3, receipt = _rpc()
    journal = TransactionJournal(output=tmp_path, web3=web3, phase="funding")

    def strict_receipt(value):
        if value != TX_HASH:
            raise ValueError("RPC DATA must be 0x-prefixed")
        return receipt

    web3.eth.get_transaction_receipt.side_effect = strict_receipt
    value = {"bare": TX_HASH[2:], "prefixed": TX_HASH, "bytes": HexBytes(TX_HASH)}[encoding]
    journal.capture_receipt(value)
    events = _events(journal)
    assert any(event["event"] == "RECEIPT" and event["raw_receipt"] == receipt for event in events)
    assert all(event["tx_hash"] == TX_HASH for event in events)
    assert not any(event["event"] == "RECEIPT_UNAVAILABLE" for event in events)


@pytest.mark.parametrize("scheme", ["ws", "wss", "WS", "WsS"])
def test_failure_seal_redacts_websocket_credentials(tmp_path, scheme):
    bundle = tmp_path / "run"
    bundle.mkdir()
    (bundle / "result.json").write_text(
        json.dumps(
            {
                "overall": "FAIL",
                "error": f"{scheme}://user:PRIVATE_PASSWORD@rpc.example/PRIVATE_KEY?token=PRIVATE_TOKEN",
            }
        )
    )
    seal = tmp_path / "seal"
    preserve_failure_bundle(bundle, seal)
    payload = json.loads((seal / "result.json").read_text())
    assert "PRIVATE_" not in json.dumps(payload)
    assert "rpc.example" in payload["error"] and "[redacted]" in payload["error"]


def test_receipt_survives_semantic_failure_and_full_bundle_is_preserved(tmp_path):
    web3, receipt = _rpc()
    bundle = tmp_path / "run"
    journal = TransactionJournal(output=bundle, web3=web3, phase="execution")
    send_and_wait(web3, SimpleNamespace(raw_transaction=b"signed"), {"from": WALLET, "to": TARGET}, journal=journal)
    for filename in ("anchors.json", "sweep.json", "hygiene.json", "funding-plan.json"):
        (bundle / filename).write_text('{"measured": true}')
    (bundle / "result.json").write_text(json.dumps({"error": "AssertionError: zero minima", "overall": "FAIL"}))
    envelope = preserve_failure_bundle(bundle, tmp_path / "seal")
    assert envelope["transactions"][0]["receipt"] == "OBSERVED"
    assert {row["path"] for row in envelope["artifacts"]} >= {
        "anchors.json",
        "sweep.json",
        "hygiene.json",
        "funding-plan.json",
    }
    sealed_events = [
        json.loads(line) for line in (tmp_path / "seal/transaction-journal/execution.jsonl").read_text().splitlines()
    ]
    assert next(row for row in sealed_events if row["event"] == "RECEIPT")["raw_receipt"] == receipt
    assert (
        next(row for row in sealed_events if row["event"] == "AFTER_BALANCES")["balances"]["raw_balances"]["native"]
        == "10"
    )
    assert envelope["observations"]["envelope.json"] == "UNMEASURED_OR_NOT_REACHED"


@pytest.mark.parametrize("best_effort", [False, True])
def test_disk_failure_blocks_new_risk_but_not_later_cleanup(tmp_path, monkeypatch, best_effort):
    web3, _ = _rpc()
    journal = TransactionJournal(output=tmp_path, web3=web3, phase="sweep", best_effort=best_effort)
    monkeypatch.setattr(journal, "_write_event", Mock(side_effect=OSError("disk full")))
    signed = SimpleNamespace(raw_transaction=b"signed")
    if best_effort:
        send_and_wait(web3, signed, {"from": WALLET}, journal=journal)
        send_and_wait(web3, signed, {"from": WALLET}, journal=journal)
        assert web3.eth.send_raw_transaction.call_count == 2
    else:
        with pytest.raises(OSError):
            send_and_wait(web3, signed, {"from": WALLET}, journal=journal)
        web3.eth.send_raw_transaction.assert_not_called()
    assert journal.failed


@pytest.mark.asyncio
async def test_sdk_submitter_revert_still_archives_raw_receipt(tmp_path, monkeypatch):
    from almanak.framework.execution.submitter import PublicMempoolSubmitter

    web3, receipt = _rpc()
    receipt["status"] = 0
    journal = TransactionJournal(output=tmp_path, web3=web3, phase="execution")
    submitter = JournaledSubmitter(journal=journal, rpc_url="https://rpc.example")
    monkeypatch.setattr(PublicMempoolSubmitter, "get_receipt", AsyncMock(side_effect=RuntimeError("reverted")))
    with pytest.raises(RuntimeError, match="reverted"):
        await submitter.get_receipt(TX_HASH)
    assert _events(journal)[0]["raw_receipt"]["status"] == 0


def test_provenance_matches_chain_block_code_and_hides_rpc_credentials():
    web3, receipt = _rpc()
    witness = capture_provenance(web3, receipt, network="mainnet")
    payload = {"network": "mainnet", "chain": "arbitrum", "raw_receipt": receipt, "external_provenance": witness}
    assert provenance_status(payload) == "VERIFIED"
    assert "password" not in json.dumps(witness)
    assert "API_SECRET" not in json.dumps(witness)
    for path, value in (
        (["chain_id"], 1),
        (["block", "hash"], "0xbad"),
        (["transaction", "blockNumber"], 99),
        (["transaction", "blockHash"], "0xbad"),
        (["critical_code", 0, "bytecode"], "0x61"),
    ):
        changed = deepcopy(payload)
        node = changed["external_provenance"]
        for key in path[:-1]:
            node = node[key]
        node[path[-1]] = value
        assert provenance_status(changed) == "UNMEASURED", path


def test_anvil_requires_observed_fork_origin():
    web3, receipt = _rpc()
    payload = {
        "network": "anvil",
        "chain": "arbitrum",
        "raw_receipt": receipt,
        "external_provenance": capture_provenance(web3, receipt, network="anvil"),
    }
    assert provenance_status(payload) == "VERIFIED"
    web3.provider.make_request.side_effect = TimeoutError()
    payload["external_provenance"] = capture_provenance(web3, receipt, network="anvil")
    assert provenance_status(payload) == "UNMEASURED"


def test_provenance_accepts_sdk_receipt_hash_encoding_and_rejects_malformed_hashes():
    from almanak.framework.execution.interfaces import TransactionReceipt

    web3, receipt = _rpc()
    sdk_receipt = TransactionReceipt(
        tx_hash=HexBytes(TX_HASH).hex(),
        block_number=100,
        block_hash=HexBytes(BLOCK_HASH).hex(),
        gas_used=21000,
        effective_gas_price=1,
        status=1,
        from_address=WALLET,
        to_address=TARGET,
    )
    payload = {
        "network": "anvil",
        "chain": "arbitrum",
        "raw_receipt": sdk_receipt.to_dict(),
        "external_provenance": capture_provenance(web3, receipt, network="anvil"),
    }
    assert provenance_status(payload) == "VERIFIED"
    for malformed in (None, "", "0xbad", "not-a-hash", "0x" + "55" * 32):
        changed = deepcopy(payload)
        changed["raw_receipt"]["block_hash"] = malformed
        assert provenance_status(changed) == "UNMEASURED"


def test_failure_archive_redacts_legacy_diagnostics_and_rejects_symlinks(tmp_path):
    bundle = tmp_path / "run"
    bundle.mkdir()
    secret = "https://user:password@rpc.example/v2/API_SECRET?token=QUERY_SECRET"
    for filename in ("runner.log", "result.json", "diagnostics.json", "results.xml"):
        (bundle / filename).write_text(json.dumps({"error": secret}))
    envelope = preserve_failure_bundle(bundle, tmp_path / "seal")
    assert all(row["diagnostic_urls_redacted"] for row in envelope["artifacts"])
    assert all("API_SECRET" not in (tmp_path / "seal" / row["path"]).read_text() for row in envelope["artifacts"])
    (bundle / "anchors.json").symlink_to(bundle / "result.json")
    with pytest.raises(ValueError, match="symlink"):
        preserve_failure_bundle(bundle, tmp_path / "second")


def test_calldata_decode_preserves_exact_lp_minimums():
    data = "0x0c49ccbe" + "".join(f"{value:064x}" for value in (7, 100, 0, 2, 1000))
    decoded = decode_calldata({"data": data})
    assert decoded["status"] == "DECODED_LOCAL_ABI"
    assert decoded["decoded"]["signature"].startswith("decreaseLiquidity")
    assert decoded["decoded"]["arguments"][0]["value"] == [7, 100, 0, 2, 1000]
    assert decode_calldata({"data": "0xdeadbeef"})["status"] == "ABI_UNRESOLVED"


@pytest.mark.asyncio
async def test_submit_result_does_not_archive_rpc_error_text(tmp_path, monkeypatch):
    from almanak.framework.execution.submitter import PublicMempoolSubmitter

    web3, _ = _rpc()
    journal = TransactionJournal(output=tmp_path, web3=web3, phase="execution")
    result = SimpleNamespace(submitted=False, tx_hash=TX_HASH, error="https://secret@rpc.example/API_KEY")
    monkeypatch.setattr(PublicMempoolSubmitter, "_submit_single", AsyncMock(return_value=result))
    submitter = JournaledSubmitter(journal=journal, rpc_url="https://rpc.example")
    signed = SimpleNamespace(
        tx_hash=TX_HASH,
        unsigned_tx=SimpleNamespace(to_dict=lambda: {"from_address": WALLET, "metadata": {"rpc_url": "API_KEY"}}),
    )
    assert await submitter._submit_single(signed) is result
    assert "API_KEY" not in journal.path.read_text()
    assert next(event for event in _events(journal) if event["event"] == "SUBMISSION_RETURNED")["submitted"] is False


def test_legacy_quote_numbers_and_stablecoin_assumptions_are_explicit(tmp_path):
    from qa_lab import chains

    cache = tmp_path / "prices.json"
    cache.write_text('{"ETH": "2500"}')
    assert str(chains.ax_price("ETH", "arbitrum", cache)) == "2500"
    assert str(chains.ax_price("USDC", "arbitrum", cache)) == "1"
    sources = json.loads((tmp_path / "price-provenance.json").read_text())
    assert sources["ETH"]["status"] == "UNMEASURED"
    assert sources["USDC"]["status"] == "ASSUMPTION"


@pytest.mark.asyncio
async def test_async_raw_receipt_persists_before_model_validation(tmp_path, monkeypatch):
    from almanak.framework.execution.interfaces import SubmissionError
    from almanak.framework.execution.submitter import PublicMempoolSubmitter

    web3, receipt = _rpc()
    # A malformed provider receipt cannot construct the SDK model, but must survive intact.
    receipt["unexpected_provider_field"] = "kept exactly"
    raw_async_rpc = SimpleNamespace(eth=SimpleNamespace(wait_for_transaction_receipt=AsyncMock(return_value=receipt)))
    monkeypatch.setattr(PublicMempoolSubmitter, "_get_web3", AsyncMock(return_value=raw_async_rpc))
    journal = TransactionJournal(output=tmp_path, web3=web3, phase="execution")
    submitter = JournaledSubmitter(journal=journal, rpc_url="https://rpc.example")
    with pytest.raises(SubmissionError):
        await submitter.get_receipt(TX_HASH)
    saved = next(event for event in _events(journal) if event["event"] == "RECEIPT")
    assert saved["raw_receipt"] == receipt
    web3.eth.get_transaction_receipt.assert_not_called()


def test_redaction_covers_relative_urllib3_endpoint_errors():
    from qa_lab.qa_external_provenance import redact_diagnostic

    error = "HTTPSConnectionPool(host=example.org, port=443): Max retries exceeded with url: /v2/FAKE_REVIEW_SECRET?key=TOKEN (Caused by ReadTimeoutError)"
    assert "FAKE_REVIEW_SECRET" not in redact_diagnostic(error)
    assert "TOKEN" not in redact_diagnostic(error)


@pytest.mark.parametrize("failure", [ImportError, ValueError])
def test_calldata_decode_failure_preserves_raw_and_redacted_reason(monkeypatch, failure):
    from qa_lab import qa_external_provenance as provenance

    def broken(body):
        raise failure("ABI unavailable at HTTPS://provider.invalid/v2/PRIVATE_RPC_KEY")

    monkeypatch.setattr(provenance, "_decode_local_abi", broken)
    result = provenance.decode_calldata({"data": "0xdeadbeef"})
    assert result["status"] == "ABI_UNRESOLVED"
    assert result["raw"] == result["selector"] == "0xdeadbeef"
    assert result["decode_error"]["error_type"] == failure.__name__
    assert "ABI unavailable" in result["decode_error"]["reason"]
    assert "PRIVATE_RPC_KEY" not in result["decode_error"]["reason"]


@pytest.mark.parametrize("label", ["url", "URL", "Url"])
def test_quoted_relative_url_redaction_is_case_insensitive(label):
    from qa_lab.qa_external_provenance import redact_diagnostic

    result = redact_diagnostic(f'{label}="/api/secret?token=PRIVATE_RPC_KEY"')
    assert "PRIVATE_RPC_KEY" not in result
    assert "secret" not in result
    assert "[redacted]" in result


def test_preserved_raw_result_redacts_json_values_without_damaging_syntax(tmp_path):
    bundle = tmp_path / "bundle"
    original = bundle / "reconciliation/prior-result.raw"
    original.parent.mkdir(parents=True)
    original.write_text(json.dumps({"error": 'RPC "https://provider.invalid/private-secret" failed', "measured": 0}))
    target = tmp_path / "seal"
    preserve_failure_bundle(bundle, target)
    result = json.loads((target / "reconciliation/prior-result.raw").read_text())
    assert result["measured"] == 0
    assert "private-secret" not in result["error"]
    assert result["error"].endswith('" failed')
