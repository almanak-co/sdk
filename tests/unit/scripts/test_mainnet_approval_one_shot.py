"""A Mainnet Intent approval is scoped, expiring, and spent exactly once.

Reproduced on main: ``build_approval`` recorded ``{cell_id, plan_sha256, approver,
approved_at}`` and ``verify_approval`` checked only the binding. Nothing bounded
how long an approval stayed usable, nothing bound the pool index or git SHA the
operator actually looked at, and nothing stopped the same approval from funding
a second run. The signature layer (who can mint one) is a separate decision;
these controls hold whichever signer lands.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from scripts.qa.mainnet_approval_ledger import (
    ApprovalAlreadyConsumedError,
    ApprovalLedgerError,
    consume_approval,
    read_ledger,
)
from scripts.qa.mainnet_intent_recipe import (
    AAVE_V3_ARBITRUM_SUPPLY_EOA,
    assert_approval_spendable,
    build_approval,
    build_run_plan,
    verify_approval,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
RUNNER = REPO_ROOT / "scripts" / "quant-test" / "run_mainnet_intent.py"
WALLET = "0x" + "11" * 20
GIT_SHA = "a" * 40
APPROVED_AT = "2026-08-29T10:00:00Z"
T0 = datetime(2026, 8, 29, 10, 0, tzinfo=UTC)


def _build_plan() -> dict:
    funding = {
        "schema_version": 1,
        "cell_id": AAVE_V3_ARBITRUM_SUPPLY_EOA.cell_id,
        "pool_index": 7,
        "wallet": WALLET,
        "funding": {"native": "0.0003", "tokens": ["USDC:1"]},
        "caps_usd": {"trading": "1.10", "gas": "1.50", "total_wallet": "4.00"},
    }
    return build_run_plan(recipe=AAVE_V3_ARBITRUM_SUPPLY_EOA, funding_plan=funding, git_sha=GIT_SHA)


# One plan per module: build_run_plan stamps the plan, so two builds are two digests.
PLAN = _build_plan()


def _plan() -> dict:
    return json.loads(json.dumps(PLAN))


def _approval(**overrides) -> dict:
    approval = build_approval(plan=_plan(), approver="qa-owner", approved_at=APPROVED_AT, **overrides)
    return approval


def _legacy_approval() -> dict:
    return {
        "schema_version": 1,
        "artifact_kind": "almanak.mainnet_intent_approval",
        "cell_id": AAVE_V3_ARBITRUM_SUPPLY_EOA.cell_id,
        "plan_sha256": _plan()["plan_sha256"],
        "approver": "qa-owner",
        "approved_at": "2026-08-16T12:00:00Z",
    }


# ---------------------------------------------------------------- scoped


def test_an_approval_binds_the_facts_the_operator_looked_at() -> None:
    approval = _approval()

    assert approval["schema_version"] == 2
    assert approval["pool_index"] == 7
    assert approval["wallet"] == WALLET
    assert approval["git_sha"] == GIT_SHA
    assert approval["expires_at"] == "2026-08-29T11:00:00Z"
    assert len(approval["nonce"]) == 32
    verify_approval(plan=_plan(), approval=approval)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("pool_index", 8, "pool index"),
        ("wallet", "0x" + "22" * 20, "wallet"),
        ("git_sha", "b" * 40, "git SHA"),
        ("nonce", "not-hex", "nonce"),
        ("nonce", "", "nonce"),
        ("expires_at", "2026-08-29T09:59:59Z", "expires before"),
        ("expires_at", "2026-08-29T11:00:00+00:00", "ending in Z"),
        ("plan_sha256", "b" * 64, "does not bind"),
    ],
)
def test_a_rebound_approval_is_rejected(field: str, value, message: str) -> None:
    approval = _approval()
    approval[field] = value

    with pytest.raises(ValueError, match=message):
        verify_approval(plan=_plan(), approval=approval)


def test_two_approvals_of_the_same_plan_never_share_a_nonce() -> None:
    assert _approval()["nonce"] != _approval()["nonce"]


# --------------------------------------------------------------- expiring


@pytest.mark.parametrize("ttl_seconds", [0, -1, 24 * 60 * 60 + 1, True])
def test_the_ttl_is_bounded(ttl_seconds) -> None:
    with pytest.raises(ValueError, match="ttl"):
        build_approval(plan=_plan(), approver="qa-owner", approved_at=APPROVED_AT, ttl_seconds=ttl_seconds)


def test_an_unexpired_approval_is_spendable() -> None:
    assert_approval_spendable(plan=_plan(), approval=_approval(), now=T0 + timedelta(minutes=59))


def test_an_expired_approval_cannot_fund_but_still_validates_sealed_evidence() -> None:
    approval = _approval()
    later = T0 + timedelta(hours=1)

    with pytest.raises(ValueError, match="expired"):
        assert_approval_spendable(plan=_plan(), approval=approval, now=later)
    verify_approval(plan=_plan(), approval=approval)  # time-free: a sealed run outlives its approval


def test_a_future_dated_approval_cannot_fund() -> None:
    with pytest.raises(ValueError, match="future"):
        assert_approval_spendable(plan=_plan(), approval=_approval(), now=T0 - timedelta(hours=1))


def test_a_legacy_approval_validates_sealed_evidence_but_cannot_fund_a_new_run() -> None:
    verify_approval(plan=_plan(), approval=_legacy_approval())
    with pytest.raises(ValueError, match="legacy"):
        assert_approval_spendable(plan=_plan(), approval=_legacy_approval(), now=T0)


# --------------------------------------------------------------- one-shot


@contextlib.contextmanager
def _no_lock(path: Path):
    yield


def test_an_approval_is_consumed_exactly_once(tmp_path: Path) -> None:
    ledger = tmp_path / "approvals-consumed.jsonl"
    approval = _approval()

    first = consume_approval(path=ledger, approval=approval, bundle=tmp_path / "run-1", lock=_no_lock, now=T0)
    with pytest.raises(ApprovalAlreadyConsumedError, match=approval["nonce"]):
        consume_approval(path=ledger, approval=approval, bundle=tmp_path / "run-2", lock=_no_lock, now=T0)

    entries = read_ledger(ledger)
    assert [entry["nonce"] for entry in entries] == [approval["nonce"]]
    assert entries[0] == first
    assert entries[0]["prev_sha256"] == "0" * 64


def test_the_ledger_is_a_hash_chain_that_refuses_a_removed_or_edited_line(tmp_path: Path) -> None:
    ledger = tmp_path / "approvals-consumed.jsonl"
    approvals = [_approval() for _ in range(3)]
    for index, approval in enumerate(approvals):
        consume_approval(path=ledger, approval=approval, bundle=tmp_path / f"run-{index}", lock=_no_lock, now=T0)
    lines = ledger.read_text().splitlines()
    assert len(read_ledger(ledger)) == 3

    ledger.write_text("\n".join([lines[0], lines[2]]) + "\n")
    with pytest.raises(ApprovalLedgerError, match="broken at line 2"):
        read_ledger(ledger)
    with pytest.raises(ApprovalLedgerError):
        consume_approval(path=ledger, approval=_approval(), bundle=tmp_path / "run-x", lock=_no_lock, now=T0)

    edited = json.loads(lines[1])
    edited["nonce"] = "f" * 32
    ledger.write_text("\n".join([lines[0], json.dumps(edited, sort_keys=True), lines[2]]) + "\n")
    with pytest.raises(ApprovalLedgerError, match="edited"):
        read_ledger(ledger)


def test_an_approval_without_a_nonce_cannot_be_consumed(tmp_path: Path) -> None:
    with pytest.raises(ApprovalLedgerError, match="nonce"):
        consume_approval(
            path=tmp_path / "ledger.jsonl", approval=_legacy_approval(), bundle=tmp_path, lock=_no_lock, now=T0
        )


# ------------------------------------------------------- runner integration


@pytest.fixture(scope="module")
def runner():
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    spec = importlib.util.spec_from_file_location("run_mainnet_intent_approval_test", RUNNER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def harness(runner, monkeypatch, tmp_path: Path):
    monkeypatch.setenv("ALMANAK_QA_MAINNET_LANE", "enabled")
    monkeypatch.setattr(runner, "_git_sha", lambda: GIT_SHA)
    ledger = tmp_path / "approvals-consumed.jsonl"
    monkeypatch.setattr(runner, "APPROVAL_LEDGER_PATH", ledger)
    plan = _plan()
    approval = build_approval(plan=plan, approver="qa-owner")  # approved now: spendable for an hour
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan, sort_keys=True))
    approval_path = tmp_path / "approval.json"
    approval_path.write_text(json.dumps(approval, sort_keys=True))
    pool = {"index": 7, "address": WALLET, "private_key": "0x" + "33" * 32, "funded": False, "role": "qa"}
    monkeypatch.setattr(runner.C, "pool_entry", lambda index: dict(pool))
    monkeypatch.setattr(runner.C, "load_env", lambda: {})
    monkeypatch.setattr(runner.C, "rpc_url", lambda chain, env: "http://rpc.invalid")
    monkeypatch.setattr(runner.C, "make_w3", lambda chain, env: object())
    funds: list[list[str]] = []

    def fake_fund(plan_path, plan, bundle, recipe) -> None:
        funds.append([entry["nonce"] for entry in read_ledger(ledger)])
        raise RuntimeError("stop before any chain access")

    monkeypatch.setattr(runner, "_fund", fake_fund)
    runs = iter(("bundle-1", "bundle-2"))

    def run() -> dict:
        return asyncio.run(
            runner.execute_plan(
                plan_path=plan_path,
                approval_path=approval_path,
                output=tmp_path / next(runs),
                operator_authorized=True,
            )
        )

    return {"run": run, "funds": funds, "ledger": ledger, "approval": approval, "tmp": tmp_path}


def test_the_runner_spends_the_nonce_before_the_funder_launches(harness) -> None:
    result = harness["run"]()

    assert result["overall"] == "FAIL"
    assert harness["funds"] == [[harness["approval"]["nonce"]]], (
        "the ledger must already hold the nonce when _fund runs"
    )
    consumed = json.loads((harness["tmp"] / "bundle-1" / "approval-consumed.json").read_text())
    assert consumed["nonce"] == harness["approval"]["nonce"]


def test_a_replayed_approval_is_refused_before_the_funder_launches(harness) -> None:
    harness["run"]()
    result = harness["run"]()

    assert result["overall"] == "FAIL"
    assert "already consumed" in str(result["error"])
    assert len(harness["funds"]) == 1, "the second run must never reach the funder"
    assert len(read_ledger(harness["ledger"])) == 1


def test_an_expired_approval_is_refused_at_the_import_surface(runner, monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("ALMANAK_QA_MAINNET_LANE", "enabled")
    plan = _plan()
    approval = build_approval(plan=plan, approver="qa-owner", approved_at="2020-01-01T00:00:00Z")
    (tmp_path / "plan.json").write_text(json.dumps(plan))
    (tmp_path / "approval.json").write_text(json.dumps(approval))

    with pytest.raises(ValueError, match="expired"):
        asyncio.run(
            runner.execute_plan(
                plan_path=tmp_path / "plan.json",
                approval_path=tmp_path / "approval.json",
                output=tmp_path / "b",
                operator_authorized=True,
            )
        )
    assert not (tmp_path / "b").exists(), "an expired approval must not even create a bundle"
