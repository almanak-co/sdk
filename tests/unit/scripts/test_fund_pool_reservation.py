from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from qa_lab import fund_pool_wallet as funder


def test_funding_holds_sweep_lock_until_operation_exits(tmp_path, monkeypatch):
    pool = tmp_path / "pool.json"
    monkeypatch.setattr(funder.C, "POOL_FILE", pool)
    args = SimpleNamespace(pool_index=1)
    monkeypatch.setattr(funder.argparse.ArgumentParser, "parse_args", lambda self: args)
    monkeypatch.setattr(funder, "_validate_approved_plan", lambda args: None)
    monkeypatch.setattr(funder, "_validate_cap_source", lambda args: None)
    entered, finish, sweep_attempted, sweep_entered = Event(), Event(), Event(), Event()

    def paused_operation():
        entered.set()
        assert finish.wait(10)
        raise RuntimeError("controlled operation exit")

    def sweep():
        sweep_attempted.set()
        with funder.C.file_lock(pool.with_name("pool.json.sweep-1")):
            sweep_entered.set()

    monkeypatch.setattr(funder.C, "load_env", paused_operation)
    with ThreadPoolExecutor(max_workers=2) as workers:
        funding = workers.submit(funder.main)
        assert entered.wait(10)
        sweeping = workers.submit(sweep)
        try:
            assert sweep_attempted.wait(10)
            assert not sweep_entered.wait(0.2), "sweep overlapped an active funding operation"
        finally:
            finish.set()
        with pytest.raises(RuntimeError, match="controlled operation exit"):
            funding.result(timeout=10)
        sweeping.result(timeout=10)
    assert sweep_entered.is_set()


@pytest.mark.parametrize("funded", [False, None])
def test_topup_cannot_reclaim_released_wallet(tmp_path, monkeypatch, funded):
    pool = tmp_path / "pool.json"
    row = {"index": 1, "address": "0x" + "11" * 20, "funded": funded, "role": "qa (batch)"}
    pool.write_text(json.dumps({"wallets": [row]}))
    monkeypatch.setattr(funder.C, "POOL_FILE", pool)
    original = pool.read_bytes()
    with pytest.raises(SystemExit, match="existing funded request"):
        funder._claim_pool_entry(1, row, "batch", allow_funded=True)
    assert pool.read_bytes() == original


def test_waiting_topup_refuses_released_wallet_before_rpc(tmp_path, monkeypatch):
    from eth_account import Account

    pool = tmp_path / "pool.json"
    pool.write_text(json.dumps({"wallets": [{"index": 1, "address": "0x" + "11" * 20, "funded": False}]}))
    monkeypatch.setattr(funder.C, "POOL_FILE", pool)
    monkeypatch.setattr(funder.C, "load_env", lambda: {"ALMANAK_PRIVATE_KEY": Account.create().key.hex()})
    rpc = Mock(side_effect=AssertionError("released top-up must not reach RPC setup"))
    monkeypatch.setattr(funder.C, "make_w3", rpc)
    with pytest.raises(SystemExit, match="existing funded wallet claim"):
        funder._fund_locked(SimpleNamespace(pool_index=1, allow_funded=True))
    rpc.assert_not_called()


@pytest.mark.parametrize(
    ("reserved_by", "request_id", "funded", "allow_funded", "accepted"),
    [
        ("req-owner", None, False, False, False),
        ("req-owner", "req-other", False, False, False),
        ("req-owner", "req-owner", False, False, True),
        (None, None, False, False, True),
        (None, "req-owner", False, False, False),
        ("req-owner", None, True, True, False),
        ("req-owner", "req-owner", True, False, False),
        (None, None, True, True, True),
    ],
)
def test_funding_claim_preserves_pool_reservation_ownership(
    tmp_path, monkeypatch, reserved_by, request_id, funded, allow_funded, accepted
):
    monkeypatch.delenv("ALMANAK_QA_FORK_CONTEXT", raising=False)
    pool_path = tmp_path / "pool.json"
    row = {
        "index": 1,
        "address": "0x" + "11" * 20,
        "funded": funded,
        "role": "test-wallet (batch)" if funded else "test-wallet",
    }
    if reserved_by:
        row.update(
            reserved_by=reserved_by,
            reserved_at="2026-09-05T00:00:00+00:00",
            reserved_cell_id="intent.test",
            reserved_git_sha="a" * 40,
        )
    pool_path.write_text(json.dumps({"wallets": [row]}))
    monkeypatch.setattr(funder.C, "POOL_FILE", pool_path)
    original = pool_path.read_bytes()

    if not accepted:
        with pytest.raises(SystemExit, match="not reserved|claimed by another process|existing funded request"):
            funder._claim_pool_entry(1, row, "batch", allow_funded=allow_funded, request_id=request_id)
        assert pool_path.read_bytes() == original
        return

    funder._claim_pool_entry(1, row, "batch", allow_funded=allow_funded, request_id=request_id)
    saved = json.loads(pool_path.read_text())["wallets"][0]
    assert saved["funded"] is True
    assert saved["role"] == "test-wallet (batch)"
    assert saved.get("funded_request_id") == request_id
    assert not any(key.startswith("reserved_") for key in saved)


@pytest.mark.parametrize(
    "request_id,batch", [(None, "owned-batch"), ("other", "owned-batch"), ("owner", "other-batch")]
)
def test_allow_funded_cannot_transfer_existing_request_or_batch(tmp_path, monkeypatch, request_id, batch):
    pool = tmp_path / "pool.json"
    row = {
        "index": 1,
        "address": "0x" + "11" * 20,
        "funded": True,
        "funded_request_id": "owner",
        "role": "qa (owned-batch)",
    }
    pool.write_text(json.dumps({"wallets": [row]}))
    monkeypatch.setattr(funder.C, "POOL_FILE", pool)
    original = pool.read_bytes()
    with pytest.raises(SystemExit, match="existing funded request"):
        funder._claim_pool_entry(1, row, batch, allow_funded=True, request_id=request_id)
    assert pool.read_bytes() == original


def test_allow_funded_same_owner_topup_preserves_claim(tmp_path, monkeypatch):
    pool = tmp_path / "pool.json"
    row = {
        "index": 1,
        "address": "0x" + "11" * 20,
        "funded": True,
        "funded_request_id": "owner",
        "role": "qa (owned-batch)",
    }
    pool.write_text(json.dumps({"wallets": [row]}))
    monkeypatch.setattr(funder.C, "POOL_FILE", pool)
    original = pool.read_bytes()
    funder._claim_pool_entry(1, row, "owned-batch", allow_funded=True, request_id="owner")
    assert pool.read_bytes() == original
