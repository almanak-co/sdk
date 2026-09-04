"""The sweep is owed to the pool-entry CLAIM, not to ``_fund``'s exit code.

Reproduced on main: ``execute_plan`` set ``funded = True`` only after ``_fund``
returned, and the ``finally`` block swept only ``if funded``. ``fund_pool_wallet``
claims the pool entry (``funded:true``) BEFORE its first transfer, so a funder
that died between the native send and the token send -- a cap breach, a revert,
a lost RPC -- left real value in a wallet the runner then never swept, with no
``sweep.json`` and no ``pool-release.json`` in the bundle to say so.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

from qa_lab.mainnet_intent_recipe import AAVE_V3_ARBITRUM_SUPPLY_EOA, build_approval, build_run_plan

REPO_ROOT = Path(__file__).resolve().parents[3]
RUNNER = REPO_ROOT / "qa_lab" / "run_mainnet_intent.py"
WALLET = "0x" + "11" * 20
GIT_SHA = "a" * 40


@pytest.fixture(scope="module")
def runner():
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    spec = importlib.util.spec_from_file_location("run_mainnet_intent_sweep_test", RUNNER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return path


@pytest.fixture
def harness(runner, monkeypatch, tmp_path: Path):
    """Open the lane for this process only and stub every seam that touches a chain or a key."""
    monkeypatch.setenv("ALMANAK_QA_MAINNET_LANE", "enabled")
    monkeypatch.setattr(runner, "_git_sha", lambda: GIT_SHA)
    funding = {
        "schema_version": 1,
        "cell_id": AAVE_V3_ARBITRUM_SUPPLY_EOA.cell_id,
        "pool_index": 7,
        "wallet": WALLET,
        "funding": {"native": "0.0003", "tokens": ["USDC:1"]},
        "caps_usd": {"trading": "1.10", "gas": "1.50", "total_wallet": "4.00"},
    }
    plan = build_run_plan(recipe=AAVE_V3_ARBITRUM_SUPPLY_EOA, funding_plan=funding, git_sha=GIT_SHA)
    approval = build_approval(plan=plan, approver="qa-owner")  # approved now: spendable for an hour
    plan_path = _write(tmp_path / "plan.json", plan)
    approval_path = _write(tmp_path / "approval.json", approval)

    pool = {"index": 7, "address": WALLET, "private_key": "0x" + "33" * 32, "funded": False, "role": "qa"}
    # The real ledger is durable operator state that guards live funds; these
    # tests must never append to it (or take its lock).
    monkeypatch.setattr(runner, "APPROVAL_LEDGER_PATH", tmp_path / "approvals-consumed.jsonl")
    monkeypatch.setattr(runner.C, "pool_entry", lambda index: dict(pool) if index == 7 else None)
    monkeypatch.setattr(runner.C, "load_env", lambda: {})
    monkeypatch.setattr(runner.C, "rpc_url", lambda chain, env: "http://rpc.invalid")
    monkeypatch.setattr(runner.C, "make_w3", lambda chain, env: object())
    sweeps: list[dict] = []

    def fake_sweep_and_capture(**kwargs):
        sweeps.append(kwargs)
        release = tmp_path / "pool-release.json"
        _write(release, {"pool_index": 7, "funded": True})
        return "QUARANTINED", [], release

    monkeypatch.setattr(runner, "_sweep_and_capture", fake_sweep_and_capture)

    def run() -> dict:
        return asyncio.run(
            runner.execute_plan(
                plan_path=plan_path, approval_path=approval_path, output=tmp_path / "bundle", operator_authorized=True
            )
        )

    return {"pool": pool, "sweeps": sweeps, "run": run}


def test_a_funder_that_dies_after_claiming_the_pool_entry_still_gets_swept(runner, harness, monkeypatch) -> None:
    def fund_then_die(plan_path, plan, bundle, recipe) -> None:
        harness["pool"]["funded"] = True
        harness["pool"]["role"] = f"qa ({bundle.resolve()})"
        raise subprocess.CalledProcessError(1, "fund_pool_wallet.py")

    monkeypatch.setattr(runner, "_fund", fund_then_die)

    result = harness["run"]()

    assert result["overall"] == "FAIL"
    assert "CalledProcessError" in str(result["error"])
    assert len(harness["sweeps"]) == 1, "value may have moved after the claim; the sweep is owed"
    assert harness["sweeps"][0]["terminal_zero"] is False, "terminal state is unknown: quarantine, never release"
    assert result["sweep"] == "QUARANTINED"


def test_a_funder_that_dies_before_the_claim_owes_no_sweep(runner, harness, monkeypatch) -> None:
    """Negative control for the fix: no claim means no value moved and nothing to sweep."""

    def die_before_claim(plan_path, plan, bundle, recipe) -> None:
        raise subprocess.CalledProcessError(1, "fund_pool_wallet.py")

    monkeypatch.setattr(runner, "_fund", die_before_claim)

    result = harness["run"]()

    assert result["overall"] == "FAIL"
    assert harness["sweeps"] == []
    assert result["sweep"] == "NOT_RUN"


def test_a_pool_entry_another_leg_owns_is_refused_before_funding_and_never_swept(runner, harness, monkeypatch) -> None:
    """A wallet that was already ``funded:true`` belongs to someone else: do not fund it, do not sweep it."""
    harness["pool"]["funded"] = True
    harness["pool"]["role"] = "qa (someone-elses-batch)"
    calls: list[str] = []
    monkeypatch.setattr(runner, "_fund", lambda *args, **kwargs: calls.append("fund"))

    result = harness["run"]()

    assert result["overall"] == "FAIL"
    assert calls == [], "the funder must never be launched against an allocated entry"
    assert harness["sweeps"] == []
    assert "allocated" in str(result["error"])


def test_a_claim_lost_to_a_concurrent_leg_is_never_treated_as_ours(runner, harness, monkeypatch) -> None:
    """The race the free-entry check cannot close: another leg claims between our check and our funder.

    The losing funder refuses under the pool lock and exits without moving any
    value -- but the entry then carries the WINNER's ``funded:true``. Keying the
    sweep on a bare ``funded:true`` would drain a wallet another leg is actively
    using; the claim must be identity-keyed to THIS run's batch name.
    """

    def lose_claim_race(plan_path, plan, bundle, recipe) -> None:
        harness["pool"]["funded"] = True
        # The other leg's bundle has the SAME basename as ours -- the exact
        # shape a basename-keyed match would cross-claim.
        harness["pool"]["role"] = f"qa (/somewhere/else/{bundle.name})"
        raise subprocess.CalledProcessError(1, "fund_pool_wallet.py")

    monkeypatch.setattr(runner, "_fund", lose_claim_race)

    result = harness["run"]()

    assert result["overall"] == "FAIL"
    assert harness["sweeps"] == [], "a foreign claim must never trigger our sweep"
    assert result["sweep"] == "NOT_RUN"


def test_the_money_lane_pins_strict_proofs_before_any_phase(runner, harness, monkeypatch) -> None:
    """The proof helpers excuse known-red assertions via pytest.xfail unless
    ALMANAK_QA_STRICT_PROOFS is set; XFailed is a BaseException the phase
    handlers do not catch, so the live lane must pin the env itself -- and
    UNCONDITIONALLY: an operator shell exporting =0 must not soften a live run.
    monkeypatch.setenv records the prior state, so the runner's direct write is
    rolled back after the test instead of leaking into the session.
    """
    import os

    monkeypatch.setattr(runner, "_fund", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("stop")))
    monkeypatch.setenv("ALMANAK_QA_STRICT_PROOFS", "0")

    harness["run"]()

    assert os.environ.get("ALMANAK_QA_STRICT_PROOFS") == "1"


def test_a_relative_output_still_matches_the_funder_claim_from_a_foreign_cwd(
    runner, harness, monkeypatch, tmp_path: Path
) -> None:
    """The funder resolves --batch-dir against ITS cwd (the repo root); the
    runner must not resolve the same directory against the operator's. A
    relative --output from outside the repo used to produce two different
    absolute claim strings, so a funder death after the claim left a funded
    live wallet unswept -- the exact bug the claim-keyed sweep exists to stop.
    """
    import asyncio

    def fund_then_die_like_the_real_funder(plan_path, plan, bundle, recipe) -> None:
        # Mimic fund_pool_wallet: resolve the batch dir it was HANDED against
        # the repo root, exactly as its cwd=REPO subprocess would.
        stamped = Path(runner.REPO, bundle).resolve()
        harness["pool"]["funded"] = True
        harness["pool"]["role"] = f"qa ({stamped})"
        raise subprocess.CalledProcessError(1, "fund_pool_wallet.py")

    monkeypatch.setattr(runner, "_fund", fund_then_die_like_the_real_funder)
    workdir = tmp_path / "operator-cwd"
    workdir.mkdir()
    monkeypatch.chdir(workdir)

    result = asyncio.run(
        runner.execute_plan(
            plan_path=tmp_path / "plan.json",
            approval_path=tmp_path / "approval.json",
            output=Path("bundle-rel"),
            operator_authorized=True,
        )
    )

    assert result["overall"] == "FAIL"
    assert len(harness["sweeps"]) == 1, "the claim identity must be cwd-independent"
