"""A delayed sweep cannot free a wallet subsequently owned by another request."""

import json

import pytest

from qa_lab.sweep_pool_wallet import C as chains
from qa_lab.sweep_pool_wallet import _assert_sweep_owner, _release_pool_entry


@pytest.mark.parametrize(
    "change",
    [
        {"reserved_by": "new-request", "funded": False},
        {"funded_request_id": "new-request"},
        {"role": "qa (/different/bundle)"},
        {"funded": False},
    ],
)
def test_stale_sweep_preserves_new_owner(tmp_path, monkeypatch, change):
    batch = tmp_path / "bundle"
    row = {"index": 1, "funded": True, "funded_request_id": "old-request", "role": f"qa ({batch.resolve()})", **change}
    pool = tmp_path / "pool.json"
    pool.write_text(json.dumps({"wallets": [row]}))
    original = pool.read_bytes()
    monkeypatch.setattr(chains, "POOL_FILE", pool)
    with pytest.raises(RuntimeError):
        _release_pool_entry(1, batch, request_id="old-request")
    assert pool.read_bytes() == original
    with pytest.raises(RuntimeError):
        _assert_sweep_owner(row, batch=batch, request_id="old-request")


@pytest.mark.parametrize("request_id", [None, "owner"])
@pytest.mark.parametrize("role", ["qa", ""])
def test_only_owned_funded_wallet_is_released(tmp_path, monkeypatch, request_id, role):
    batch = tmp_path / "bundle"
    row = {
        "index": 1,
        "funded": True,
        "funded_request_id": request_id,
        "role": f"{role} ({batch.resolve()})".strip(),
        "private_key": "preserved",
    }
    pool = tmp_path / "pool.json"
    pool.write_text(json.dumps({"wallets": [row]}))
    monkeypatch.setattr(chains, "POOL_FILE", pool)
    _release_pool_entry(1, batch, request_id=request_id)
    actual = json.loads(pool.read_text())["wallets"][0]
    assert actual["funded"] is False
    assert "funded_request_id" not in actual
    assert actual["private_key"] == "preserved"


def test_concurrent_old_sweep_cannot_sign_after_wallet_reallocation(tmp_path, monkeypatch):
    import argparse
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from decimal import Decimal
    from types import SimpleNamespace

    from eth_account import Account

    from qa_lab import qa_transaction_journal as journal
    from qa_lab import sweep_pool_wallet as sweep

    batch = tmp_path / "bundle"
    batch.mkdir()
    key = "0x" + "01".zfill(64)
    account = Account.from_key(key)
    row = {
        "index": 1,
        "address": account.address,
        "private_key": key,
        "funded": True,
        "funded_request_id": "old-request",
        "role": f"qa ({batch.resolve()})",
    }
    pool = tmp_path / "pool.json"
    pool.write_text(json.dumps({"wallets": [row]}))
    monkeypatch.setattr(chains, "POOL_FILE", pool)
    args = argparse.Namespace(
        batch_dir=str(batch),
        leg="test",
        chain="arbitrum",
        pool_index=1,
        request_id="old-request",
        master="0x" + "22" * 20,
        native_dust_usd="0",
        record_as="sweep",
        keep_funded=False,
        dry_run=False,
    )
    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", lambda self: args)
    monkeypatch.setattr(chains, "load_env", lambda: {})
    monkeypatch.setattr(sweep, "_resolve_master", lambda *args: "0x" + "22" * 20)
    monkeypatch.setattr(journal, "TransactionJournal", lambda **kwargs: SimpleNamespace(failed=False))
    first_entered = threading.Event()
    second_started = threading.Event()
    second_read_balance = threading.Event()
    reallocated = threading.Event()
    identities = {}
    state = {"balance": 10**18}
    signed_by = []

    def wallet_value(*args, **kwargs):
        name = identities[threading.get_ident()]
        if name == "first" and not first_entered.is_set():
            first_entered.set()
            assert second_started.wait(5)
            # Give an unlocked second sweep a chance to pass its old ownership check.
            second_read_balance.wait(1)
        elif name == "second":
            second_read_balance.set()
            assert reallocated.wait(5)
        balance = Decimal(state["balance"]) / Decimal(10**18)
        return {"total_usd": balance, "legs": {"ETH": {"balance": balance, "usd": balance, "price": 1, "native": True}}}

    monkeypatch.setattr(chains, "wallet_value", wallet_value)
    eth = SimpleNamespace(
        get_balance=lambda *args: state["balance"],
        get_transaction_count=lambda *args: 0,
        estimate_gas=lambda tx: 21000,
        get_block=lambda *args: {"baseFeePerGas": 1},
        block_number=1,
    )
    monkeypatch.setattr(chains, "make_w3", lambda *args: SimpleNamespace(eth=eth))
    monkeypatch.setattr(sweep, "_settle", lambda *args, **kwargs: None)

    def send_and_wait(w3, signed, tx, **kwargs):
        assert signed.raw_transaction
        signed_by.append(identities[threading.get_ident()])
        state["balance"] = 0
        return bytes.fromhex("ab" * 32), {"status": 1}

    monkeypatch.setattr(journal, "send_and_wait", send_and_wait)
    release = sweep._release_pool_entry

    def release_and_reallocate(*args, **kwargs):
        release(*args, **kwargs)
        with chains.pool_lock():
            current = chains.load_pool()
            current["wallets"][0].update(
                funded=True, funded_request_id="new-request", role=f"qa ({tmp_path / 'new-bundle'})"
            )
            chains.save_pool(current)
        state["balance"] = 10**18
        reallocated.set()

    monkeypatch.setattr(sweep, "_release_pool_entry", release_and_reallocate)

    def worker(name):
        identities[threading.get_ident()] = name
        if name == "second":
            second_started.set()
        return sweep.main()

    with ThreadPoolExecutor(max_workers=2) as workers:
        first = workers.submit(worker, "first")
        assert first_entered.wait(5)
        second = workers.submit(worker, "second")
        assert first.result(timeout=10) == 0
        with pytest.raises(RuntimeError, match="no longer owns"):
            second.result(timeout=10)
    assert signed_by == ["first"]
    assert chains.pool_entry(1)["funded_request_id"] == "new-request"
    assert state["balance"] == 10**18


def test_sweeps_for_distinct_pool_wallets_can_progress_in_parallel(tmp_path, monkeypatch):
    import argparse
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from types import SimpleNamespace

    from qa_lab import sweep_pool_wallet as sweep

    local = threading.local()
    entered = threading.Barrier(2)
    monkeypatch.setattr(chains, "POOL_FILE", tmp_path / "pool.json")
    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", lambda self: SimpleNamespace(pool_index=local.index))

    def execution(args):
        entered.wait(timeout=5)
        return args.pool_index

    monkeypatch.setattr(sweep, "_sweep_locked", execution)

    def worker(index):
        local.index = index
        return sweep.main()

    with ThreadPoolExecutor(max_workers=2) as workers:
        first = workers.submit(worker, 1)
        second = workers.submit(worker, 2)
        assert [first.result(timeout=10), second.result(timeout=10)] == [1, 2]
