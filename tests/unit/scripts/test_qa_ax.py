"""Fail-closed contracts for the ax QA catalog, seals, and Lab board."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_DIR = REPO_ROOT / "qa_lab"
TEST_COMMIT = "a" * 40
TEST_SDK = {
    "commit": TEST_COMMIT,
    "branch": "test",
    "dirty": False,
    "sdk_version": "0.0-test",
    "source": "executing-worktree",
}


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPT_DIR / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def ax_module():
    return _load("qa_ax_test", "qa_ax.py")


def test_catalog_is_generated_from_production_tool_and_click_registries(ax_module, tmp_path: Path) -> None:
    from almanak.framework.agent_tools.catalog import get_default_catalog
    from almanak.framework.cli.ax import ax

    output = tmp_path / "ax_cells.json"

    catalog = ax_module.build_ax_catalog(output=output)

    assert catalog["summary"]["tools"] == len(get_default_catalog().list_tools())
    assert catalog["summary"]["cli_commands"] == len(ax.commands)
    assert catalog["summary"]["axis_cells"] > 2000
    assert set(catalog["axes"]["interfaces"]) == {
        "structured",
        "generic",
        "natural",
        "mcp",
        "openai",
        "langchain",
    }
    tools = {tool["name"]: tool for tool in catalog["tools"]}
    assert tools["swap_tokens"]["structured_command"] == "swap"
    assert tools["swap_tokens"]["wallet_paths"] == ["eoa-exec", "safe-exec"]
    assert tools["get_price"]["networks"] == ["anvil", "mainnet"]
    assert tools["save_agent_state"]["networks"] == ["none"]
    assert json.loads(output.read_text()) == catalog


def test_empty_board_is_explicit_and_does_not_promote_inventory(ax_module, tmp_path: Path) -> None:
    store = tmp_path / "store"
    (store / "catalog").mkdir(parents=True)
    (store / "lab").mkdir()
    (store / "index").mkdir()

    page = ax_module.render_ax_lab(store=store, lab_css="")

    assert json.loads((store / "index" / ax_module.AX_INDEX_NAME).read_text()) == {}
    html = page.read_text()
    assert "ax product evidence, fail-closed" in html
    assert "UNSUPPORTED" in html
    assert "NOT EXPOSED" in html
    assert "NEVER" in html
    assert "PASS" in html
    assert "FAIL" in html
    assert "Unit mocks and historical anecdotes never paint this board" in html
    assert 'class="btn active" href="ax.html">ax</a>' in html
    assert 'href="readiness.html">Checklists</a>' in html


def test_policy_denial_control_seals_exact_evidence_and_is_immutable(ax_module, tmp_path: Path) -> None:
    store = tmp_path / "store"
    (store / "catalog").mkdir(parents=True)
    (store / "index").mkdir()
    ax_module.bootstrap_ax(store)
    catalog = ax_module.build_ax_catalog(output=store / "catalog" / ax_module.AX_CATALOG_NAME)
    bundle = tmp_path / "bundle"
    ax_module.run_policy_denial_control(output=bundle, sdk_provenance=TEST_SDK)

    target = ax_module.seal_ax_bundle(bundle=bundle, store=store, catalog=catalog)

    manifest = json.loads((target / "manifest.json").read_text())
    assert manifest["verdict"] == "PASS"
    assert manifest["attribution_mode"] == "exact-runtime"
    assert manifest["evidence_status"] == "COMPLETE"
    assert manifest["policy"]["decision"] == "DENY"
    assert manifest["dispatch"]["reached_gateway"] is False
    assert manifest["effect"]["observed"] is False
    assert (target / "report.html").is_file()
    latest = json.loads((store / "index" / ax_module.AX_INDEX_NAME).read_text())
    row = latest[manifest["cell_id"]]
    assert row["verdict"] == "PASS"
    assert row["attribution_mode"] == "exact-runtime"
    assert {artifact["label"] for artifact in row["artifacts"]} >= {
        "ax run report",
        "command.json",
        "policy.json",
    }
    with pytest.raises(FileExistsError, match="already sealed"):
        ax_module.seal_ax_bundle(bundle=bundle, store=store, catalog=catalog)


def test_seal_rejects_weak_or_not_exposed_claims(ax_module, tmp_path: Path) -> None:
    store = tmp_path / "store"
    (store / "catalog").mkdir(parents=True)
    (store / "index").mkdir()
    ax_module.bootstrap_ax(store)
    catalog = ax_module.build_ax_catalog()
    bundle = tmp_path / "weak"
    ax_module.run_policy_denial_control(output=bundle, sdk_provenance=TEST_SDK)
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["harness"]["mode"] = "unit-mock"
    manifest_path.write_text(json.dumps(manifest))

    with pytest.raises(ValueError, match="exact-runtime and non-synthetic"):
        ax_module.seal_ax_bundle(bundle=bundle, store=store, catalog=catalog)

    manifest["harness"] = {"mode": "exact-runtime", "synthetic": False}
    manifest["interface"] = "structured"
    manifest["cell_id"] = "ax.execute_compiled_bundle.structured.anvil.eoa-exec.policy-deny"
    manifest["tool"] = "execute_compiled_bundle"
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="not-exposed"):
        ax_module.seal_ax_bundle(bundle=bundle, store=store, catalog=catalog)


def test_seal_refuses_a_symlinked_secret_and_a_secret_shaped_filename(ax_module, tmp_path: Path) -> None:
    """``copytree(symlinks=False)`` DEREFERENCES: it copies the target's bytes.

    A bundle symlink pointing at a live key file therefore published that key
    into the immutable store and into the append-only experiment hash chain --
    reproduced at 21256be6f before this guard existed. The sibling seals
    (qa_protocol / qa_readiness / qa_backtest) already refused both shapes.
    """
    store = tmp_path / "store"
    (store / "catalog").mkdir(parents=True)
    (store / "index").mkdir()
    ax_module.bootstrap_ax(store)
    catalog = ax_module.build_ax_catalog(output=store / "catalog" / ax_module.AX_CATALOG_NAME)
    secret = tmp_path / "outside-the-bundle-key.txt"
    secret.write_text("ALMANAK_PRIVATE_KEY=0xlive\n", encoding="utf-8")

    bundle = tmp_path / "symlinked"
    ax_module.run_policy_denial_control(output=bundle, sdk_provenance=TEST_SDK)
    (bundle / "keys.txt").symlink_to(secret)

    with pytest.raises(ValueError, match="may not contain symlinks"):
        ax_module.seal_ax_bundle(bundle=bundle, store=store, catalog=catalog)

    (bundle / "keys.txt").unlink()
    for name in ("wallets.env", ".env", ".env.local", "pool.json", "credentials.json", "secrets.json"):
        (bundle / name).write_text("POOL_KEY=0xlive\n", encoding="utf-8")
        with pytest.raises(ValueError, match="secret-bearing ax bundle filename"):
            ax_module.seal_ax_bundle(bundle=bundle, store=store, catalog=catalog)
        (bundle / name).unlink()

    # Nothing leaked: the refusal happens before any byte is copied.
    assert not list((store / "ax").rglob("*.txt"))
    assert not (store / "index" / "experiment_runs.jsonl").exists()

    # The clean bundle still seals.
    assert ax_module.seal_ax_bundle(bundle=bundle, store=store, catalog=catalog).is_dir()


def _ax_store(ax_module, tmp_path: Path):
    store = tmp_path / "store"
    (store / "catalog").mkdir(parents=True)
    (store / "index").mkdir()
    ax_module.bootstrap_ax(store)
    return store, ax_module.build_ax_catalog(output=store / "catalog" / ax_module.AX_CATALOG_NAME)


def _happy_manifest(run_id: str, *, verdict: str = "PASS", exit_code: int = 0) -> dict:
    command = ["uv", "run", "almanak", "ax", "--network", "anvil", "--json", "price", "ETH"]
    return {
        "schema_version": 1,
        "evidence_kind": "almanak.ax.exact-run",
        "run_id": run_id,
        "cell_id": "ax.get_price.structured.anvil.none.happy",
        "tool": "get_price",
        "interface": "structured",
        "network": "anvil",
        "chain": "arbitrum",
        "wallet_path": "none",
        "scenario": "happy",
        "verdict": verdict,
        "started_at": "2026-08-19T12:00:00+00:00",
        "completed_at": "2026-08-19T12:01:00+00:00",
        "command": command,
        "exit_code": exit_code,
        "policy": {"checked": False, "decision": "NOT_REQUIRED", "reason": "read-only tool"},
        "dispatch": {"attempted": True, "reached_gateway": True, "proof": "ax CLI process output"},
        "effect": {"expected": False, "observed": False, "proof": "read-only tool"},
        "harness": {
            "mode": "exact-runtime",
            "synthetic": False,
            "runner": "test",
            "production_components": ["almanak ax"],
        },
        "sdk": TEST_SDK,
    }


def test_a_hand_typed_ax_manifest_cannot_mint_an_official_pass(ax_module, tmp_path: Path) -> None:
    """Reproduces scored failure F2: one authored file used to be a mainnet PASS.

    ``_validate_exact_manifest`` checked the manifest against itself -- required
    fields, exit_code=0 for PASS, a policy block for action tools. Every one of
    those values lived in the file being validated, so a bundle consisting of a
    single hand-typed ``manifest.json`` sealed as an OFFICIAL exact-runtime PASS
    and painted the ax board green.
    """
    store, catalog = _ax_store(ax_module, tmp_path)
    bundle = tmp_path / "forged"
    bundle.mkdir()
    (bundle / "manifest.json").write_text(json.dumps(_happy_manifest("20260819-1201z-ax-forged")))

    target = ax_module.seal_ax_bundle(bundle=bundle, store=store, catalog=catalog)

    manifest = json.loads((target / "manifest.json").read_text())
    assert manifest["producer_verdict"] == "PASS"
    assert manifest["verdict"] == "UNMEASURED"
    assert manifest["evidence_status"] == "UNVERIFIED"
    assert manifest["derived_claim"]["reason_codes"] == ["ax_command_record_absent"]
    row = json.loads((store / "index" / ax_module.AX_INDEX_NAME).read_text())[manifest["cell_id"]]
    assert row["verdict"] == "UNMEASURED"
    # The board paints from evidence_status; UNVERIFIED is the honest ungraded state.
    assert row["evidence_status"] == "UNVERIFIED"
    ledger = [json.loads(line) for line in (store / "index" / "experiment_runs.jsonl").read_text().splitlines() if line]
    assert ledger[-1]["cell_verdicts"] == {manifest["cell_id"]: "UNVERIFIED"}
    assert ledger[-1]["admission"] is None


def test_a_happy_path_pass_is_derived_from_the_process_output(ax_module, tmp_path: Path) -> None:
    store, catalog = _ax_store(ax_module, tmp_path)
    response = {"token": "ETH", "price_usd": "2500.00"}
    bundle = tmp_path / "honest"
    bundle.mkdir()
    manifest = _happy_manifest("20260819-1202z-ax-honest")
    (bundle / "manifest.json").write_text(json.dumps(manifest))
    (bundle / "stdout.txt").write_text(json.dumps(response), encoding="utf-8")
    (bundle / "response.json").write_text(json.dumps(response), encoding="utf-8")
    (bundle / "command.json").write_text(json.dumps({"argv": manifest["command"], "exit_code": 0}))

    target = ax_module.seal_ax_bundle(bundle=bundle, store=store, catalog=catalog)

    sealed = json.loads((target / "manifest.json").read_text())
    assert sealed["verdict"] == "PASS"
    assert sealed["evidence_status"] == "COMPLETE"
    control = sealed["derived_claim"]["admission_control"]
    assert control["declared_status"] == "PASS"
    assert control["derived_status"] == "PASS"
    sources = {row["source"] for row in sealed["derived_claim"]["authorities"]}
    assert sources == {"stdout.txt", "response.json", "command.json"}
    # Every cited authority is negative-controlled, and none of the mutants stayed green.
    assert {row["source"] for row in control["liveness"]["mutations"]} == sources
    assert control["liveness"]["status"] == "PASS"
    assert all(row["mutant_status"] != "PASS" for row in control["liveness"]["mutations"])
    ledger = [json.loads(line) for line in (store / "index" / "experiment_runs.jsonl").read_text().splitlines() if line]
    assert ledger[-1]["admission"]["status"] == "OFFICIAL"


def test_a_pass_contradicted_by_the_process_output_stops_the_seal(ax_module, tmp_path: Path) -> None:
    """A declared PASS over a run that emitted no structured response is refused."""
    from qa_lab.admission_contract import AdmissionRefused

    store, catalog = _ax_store(ax_module, tmp_path)
    bundle = tmp_path / "contradicted"
    bundle.mkdir()
    manifest = _happy_manifest("20260819-1203z-ax-contradicted")
    (bundle / "manifest.json").write_text(json.dumps(manifest))
    (bundle / "stdout.txt").write_text("Error: no RPC endpoint configured\n", encoding="utf-8")
    (bundle / "command.json").write_text(json.dumps({"argv": manifest["command"], "exit_code": 0}))

    with pytest.raises(AdmissionRefused, match="declared PASS but the sealer derived FAIL"):
        ax_module.seal_ax_bundle(bundle=bundle, store=store, catalog=catalog)

    # Nothing entered the store or the hash chain.
    assert not list((store / "ax").rglob("manifest.json"))
    assert not (store / "index" / "experiment_runs.jsonl").exists()


def test_the_ax_board_renders_the_admission_trail(ax_module, tmp_path: Path) -> None:
    """A cell's colour is an index. The rail has to say why it has that colour."""
    store, catalog = _ax_store(ax_module, tmp_path)
    bundle = tmp_path / "bundle"
    ax_module.run_policy_denial_control(output=bundle, sdk_provenance=TEST_SDK)
    ax_module.seal_ax_bundle(bundle=bundle, store=store, catalog=catalog)

    page = ax_module.render_ax_lab(store=store, lab_css=".admission{}").read_text(encoding="utf-8")

    assert "function axAdmission(row)" in page
    assert '"admission_control"' in page
    assert '"mutant_status"' in page
    assert "Evidence vector · producer-reported" in page
