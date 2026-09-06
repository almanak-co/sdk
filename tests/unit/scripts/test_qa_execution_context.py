"""Isolation checks reject foreign RPCs, keys and writable evidence paths."""

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_account import Account

from qa_lab.qa_execution_context import MODE, ForkExecutionContext, active_context


@pytest.fixture
def context(tmp_path, monkeypatch):
    root = tmp_path / "acceptance"
    root.mkdir(mode=0o700)
    ctx = ForkExecutionContext(
        root, "arbitrum", "http://127.0.0.1:18765", "fork-12345678", 100, "0x" + "ab" * 32, 42161, "instance-1"
    )
    descriptor = root / "context.json"
    descriptor.write_text(json.dumps({**ctx.public_identity(), "root": str(root), "rpc_url": ctx.rpc_url}))
    monkeypatch.setenv("ALMANAK_QA_FORK_CONTEXT", str(descriptor))
    monkeypatch.delenv("ALMANAK_QA_STORE", raising=False)
    (root / "wallets").mkdir()
    master, recipient = Account.create(), Account.create()
    key = root / "wallets/master.json"
    key.write_text(json.dumps({"context_id": ctx.context_id, "private_key": master.key.hex()}))
    key.chmod(0o600)
    ctx.pool_file.write_text(
        json.dumps(
            {
                "context_id": ctx.context_id,
                "mode": MODE,
                "wallets": [{"address": recipient.address, "private_key": recipient.key.hex()}],
            }
        )
    )
    (root / "seed.json").write_text(json.dumps({"master": master.address, "recipient": recipient.address}))
    return ctx


def test_only_explicit_context_keys_and_rpc(context, monkeypatch):
    monkeypatch.setenv("ALMANAK_PRIVATE_KEY", "production-key-must-never-be-loaded")
    monkeypatch.setenv("ARBITRUM_RPC_URL", "https://production-rpc.invalid")
    actual = active_context()
    assert actual == context
    assert actual.environment()["ARBITRUM_RPC_URL"] == context.rpc_url
    assert actual.environment()["ALMANAK_PRIVATE_KEY"] != "production-key-must-never-be-loaded"


def test_coordinator_child_retains_acceptance_temp_base(context):
    from qa_lab.mainnet_intent_coordinator import _clean_env

    environment = _clean_env(environment=context.root / "venv")
    assert Path(environment["TMPDIR"]).resolve() == Path(tempfile.gettempdir()).resolve()
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            "from qa_lab.qa_execution_context import active_context; print(active_context().context_id)",
        ],
        cwd=Path(__file__).resolve().parents[3],
        env=environment,
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert child.returncode == 0, child.stderr
    assert child.stdout.strip() == context.context_id
    assert "ALMANAK_PRIVATE_KEY" not in environment
    assert environment["HOME"] == str(context.root / "home")


@pytest.mark.parametrize("target", ["store", "wallets"])
def test_rejects_external_state_symlinks(context, tmp_path, target):
    outside = tmp_path / "outside"
    outside.mkdir()
    path = context.root / target
    if path.exists():
        import shutil

        shutil.rmtree(path)
    path.symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="isolated root"):
        active_context()


@pytest.mark.parametrize(
    "field,value", [("rpc_url", "https://arb-mainnet.example"), ("network", "mainnet"), ("fork_hash", "missing")]
)
def test_descriptor_rejects_unsafe_identity(context, field, value):
    descriptor = context.root / "context.json"
    data = json.loads(descriptor.read_text())
    data[field] = value
    descriptor.write_text(json.dumps(data))
    with pytest.raises(ValueError):
        active_context()


def test_master_key_replacement_is_rejected(context):
    key = context.root / "wallets/master.json"
    key.write_text(json.dumps({"context_id": context.context_id, "private_key": Account.create().key.hex()}))
    with pytest.raises(ValueError, match="seeded master"):
        context.environment()


def test_rpc_chain_registry_is_authoritative(context):
    from dataclasses import replace

    altered = replace(context, chain_id=1)
    with pytest.raises(ValueError, match="SDK chain registry"):
        altered.assert_rpc_identity(SimpleNamespace())


def test_rpc_instance_replacement_is_rejected(context):
    provider = SimpleNamespace(
        endpoint_uri=context.rpc_url, make_request=lambda *_: {"result": {"instanceId": "another-instance"}}
    )
    with pytest.raises(ValueError, match="bound managed Anvil"):
        context.assert_rpc_identity(SimpleNamespace(provider=provider))


def test_missing_descriptor_never_falls_back(context, monkeypatch):
    monkeypatch.setenv("ALMANAK_QA_FORK_CONTEXT", str(context.root / "missing.json"))
    with pytest.raises(FileNotFoundError):
        active_context()


@pytest.mark.parametrize("network", ["mainnet", "arbitrary"])
def test_fork_plan_rejects_contradictory_network(context, network):
    from qa_lab.mainnet_intent_recipe import verify_run_plan

    with pytest.raises(ValueError, match="Plan network"):
        verify_run_plan({"execution_context": context.public_identity(), "network": network})


def test_production_plan_rejects_fork_network(monkeypatch):
    from qa_lab.mainnet_intent_recipe import verify_run_plan

    monkeypatch.delenv("ALMANAK_QA_FORK_CONTEXT", raising=False)
    with pytest.raises(ValueError, match="Plan network"):
        verify_run_plan({"network": "anvil"})


@pytest.fixture
def failure_bundle(context, monkeypatch):
    from qa_lab import qa_history
    from qa_lab.mainnet_intent_recipe import build_run_plan, resolve_recipe

    recipe = resolve_recipe("intent.uniswap_v3.arbitrum.SWAP.anvil.eoa")
    funding = {"cell_id": recipe.cell_id, "wallet": "0x" + "11" * 20, "pool_index": 0}
    plan = build_run_plan(recipe=recipe, funding_plan=funding, git_sha="a" * 40)
    output = context.root / "bundle"
    output.mkdir()
    (output / "plan.json").write_text(json.dumps(plan))
    (output / "result.json").write_text(json.dumps({"overall": "FAIL", "reconciliation_id": "request-interrupted"}))
    journal = output / "transaction-journal"
    journal.mkdir()
    (journal / "execution.jsonl").write_text('{"phase":"execution","event":"SUBMITTED","tx_hash":"0xabc"}\n')
    (output / "private-key.json").write_text('"must never seal arbitrary files"')
    monkeypatch.setattr(
        qa_history,
        "provenance_from_worktree",
        lambda *_: {
            "commit": "a" * 40,
            "branch": "fix/test",
            "dirty": False,
            "sdk_version": "test",
            "source": "executing-worktree",
        },
    )
    return output


def test_acceptance_seal_retries_same_result_and_excludes_unlisted_files(context, failure_bundle):
    from qa_lab import qa_history
    from qa_lab.qa_lifecycle_acceptance_seal import seal_acceptance_bundle

    checkpoints = failure_bundle / "lifecycle-checkpoints"
    checkpoints.mkdir()
    observed = checkpoints / "events.jsonl"
    observed.write_text('{"checkpoint":"after_broadcast","network":"anvil"}\n')
    target = seal_acceptance_bundle(output=failure_bundle)
    assert (target / "lifecycle-checkpoints/events.jsonl").read_bytes() == observed.read_bytes()
    result = json.loads((failure_bundle / "result.json").read_text())
    result["seal_path"] = str(target)
    (failure_bundle / "result.json").write_text(json.dumps(result))
    assert seal_acceptance_bundle(output=failure_bundle) == target
    assert len(qa_history.read_history(context.store)) == 1
    assert not (target / "private-key.json").exists()
    assert (target / "transaction-journal/execution.jsonl").read_bytes() == (
        failure_bundle / "transaction-journal/execution.jsonl"
    ).read_bytes()


def test_acceptance_seal_finishes_interrupted_ledger_append(context, failure_bundle, monkeypatch):
    from qa_lab import qa_history
    from qa_lab.qa_lifecycle_acceptance_seal import seal_acceptance_bundle

    append = qa_history.append_experiment

    def fail_append(**kwargs):
        raise RuntimeError("interrupted after immutable rename")

    monkeypatch.setattr(qa_history, "append_experiment", fail_append)
    with pytest.raises(RuntimeError, match="immutable rename"):
        seal_acceptance_bundle(output=failure_bundle)
    monkeypatch.setattr(qa_history, "append_experiment", append)
    seal_acceptance_bundle(output=failure_bundle)
    assert len(qa_history.read_history(context.store)) == 1
    qa_history.verify_history(context.store)


def test_acceptance_retry_rejects_changed_raw_transaction(context, failure_bundle):
    from qa_lab.qa_lifecycle_acceptance_seal import seal_acceptance_bundle

    seal_acceptance_bundle(output=failure_bundle)
    (failure_bundle / "transaction-journal/execution.jsonl").write_text(
        '{"phase":"execution","event":"SUBMITTED","tx_hash":"0xdef"}\n'
    )
    with pytest.raises(ValueError, match="immutable original"):
        seal_acceptance_bundle(output=failure_bundle)


def test_acceptance_native_reserve_uses_existing_cap_and_covers_observed_cleanup():
    from decimal import Decimal

    from qa_lab.qa_fork_prices import native_reserve

    price = Decimal("2487.698700256196997107608255")
    policy = native_reserve(price_usd=price, gas_budget_usd="2.50", original_native="0.0005")
    native = Decimal(policy["native_funding"])
    assert native * price <= Decimal("2.375")
    assert native >= Decimal("0.0002") + Decimal("0.000054") + Decimal("0.0003")
    assert policy["original_recipe_native_funding"] == "0.0005"
    assert policy["gas_budget_usd"] == "2.50"


@pytest.mark.parametrize("price", ["0", "-1"])
def test_acceptance_native_reserve_rejects_unmeasured_price(price):
    from decimal import Decimal

    from qa_lab.qa_fork_prices import native_reserve

    with pytest.raises(ValueError, match="positive"):
        native_reserve(price_usd=Decimal(price), gas_budget_usd="2.50", original_native="0.0005")


def test_approved_reserve_stays_pinned_when_live_price_moves(context, monkeypatch):
    from qa_lab import chains
    from qa_lab.mainnet_intent_recipe import build_run_plan, resolve_recipe, verify_run_plan

    recipe = resolve_recipe("intent.uniswap_v3.arbitrum.SWAP.anvil.eoa")
    reserve = {"native_funding": "0.00095", "quote_sha256": "b" * 64}
    funding = {
        "cell_id": recipe.cell_id,
        "wallet": "0x" + "11" * 20,
        "pool_index": 0,
        "acceptance_native_reserve": reserve,
    }
    plan = build_run_plan(recipe=recipe, funding_plan=funding, git_sha="a" * 40)

    def moved_price(*args, **kwargs):
        raise AssertionError("Verification must not fetch a replacement live quote")

    monkeypatch.setattr(chains, "ax_price", moved_price)
    assert verify_run_plan(plan).digest == recipe.digest
    assert plan["funding"]["acceptance_native_reserve"] == reserve


def test_serialized_fork_funding_plan_keeps_policy_inside_reviewed_digest(context):
    from decimal import Decimal

    from qa_lab.fund_pool_wallet import _validate_approved_plan
    from qa_lab.mainnet_intent_recipe import build_run_plan, resolve_recipe
    from qa_lab.preflight_pool_wallet import build_plan
    from qa_lab.qa_fork_prices import native_reserve

    pool = json.loads(context.pool_file.read_text())
    pool["wallets"][0]["index"] = 0
    context.pool_file.write_text(json.dumps(pool))
    context.pool_file.chmod(0o600)
    recipe = resolve_recipe("intent.uniswap_v3.arbitrum.SWAP.anvil.eoa")
    price = Decimal("2487.6987")
    reserve = native_reserve(
        price_usd=price, gas_budget_usd=recipe.gas_budget_usd, original_native=recipe.native_funding
    )
    funding = build_plan(
        pool_path=context.pool_file,
        cell_id=recipe.cell_id,
        pool_index=0,
        native=reserve["native_funding"],
        tokens=list(recipe.funding_tokens),
        trading_cap_usd=recipe.trading_cap_usd,
        gas_budget_usd=recipe.gas_budget_usd,
        total_wallet_cap_usd=recipe.total_wallet_cap_usd,
        wallet_value=lambda *_: Decimal(0),
        price=lambda symbol, _: price if symbol == "ETH" else Decimal(1),
        acceptance_native_reserve=reserve,
    )
    plan = build_run_plan(recipe=recipe, funding_plan=funding, git_sha="a" * 40)
    serialized = context.root / "plan.json"
    serialized.write_text(json.dumps(plan))
    reloaded = json.loads(serialized.read_text())["funding"]
    funding_path = context.root / "funding-plan.json"
    funding_path.write_text(json.dumps(reloaded))
    args = SimpleNamespace(
        approved_plan=funding_path,
        approved_plan_sha256=reloaded["approval_digest"],
        intent_cell_id=recipe.cell_id,
        gas_budget_usd=recipe.gas_budget_usd,
        token=list(recipe.funding_tokens),
        chain=recipe.chain,
        pool_index=0,
        native=reserve["native_funding"],
        trading_cap_usd=recipe.trading_cap_usd,
        total_wallet_cap_usd=recipe.total_wallet_cap_usd,
        leg="acceptance",
    )
    _validate_approved_plan(args)
    assert args.approved_wallet == pool["wallets"][0]["address"]
    reloaded["acceptance_native_reserve"]["native_funding_wei"] = "1"
    funding_path.write_text(json.dumps(reloaded))
    with pytest.raises(SystemExit, match="digest does not match"):
        _validate_approved_plan(args)


def test_unknown_fork_recipe_raises_module_error(context):
    from qa_lab.mainnet_intent_recipe import resolve_recipe

    with pytest.raises(ValueError, match="No Mainnet Intent recipe backs acceptance cell"):
        resolve_recipe("intent.unknown_protocol.arbitrum.SWAP.anvil.eoa")
