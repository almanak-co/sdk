"""Public Curve QA claims must match retained model and family-specific outcomes."""

import gzip
import json
from dataclasses import replace

import pytest

from almanak.caip import ChainId
from almanak.connectors.identifiers import ConnectorId, ProtocolVariant
from tests.qa.catalog import ROOT, catalog
from tests.qa.worker import assert_backtest

CASES = tuple(
    c
    for c in catalog()
    if c.connector == ConnectorId("curve") and c.lane == "backtest" and c.input_policy == "pinned"
)
BASE = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout"


CHAIN_LABELS = {
    ChainId("eip155:1"): "",
    ChainId("eip155:8453"): "base-",
    ChainId("eip155:42161"): "arbitrum-",
    ChainId("eip155:10"): "optimism-",
    ChainId("eip155:56"): "bnb-",
    ChainId("eip155:137"): "polygon-",
    ChainId("eip155:43114"): "avalanche-",
}
FAMILY_LABELS = {
    ProtocolVariant("stableswap_ng"): "stable",
    ProtocolVariant("twocrypto_ng"): "twocrypto",
    ProtocolVariant("tricrypto_ng"): "tricrypto",
    ProtocolVariant("tricrypto_ng_2024"): "tricrypto",
}
ROBINHOOD_FAMILY_LABELS = {
    ProtocolVariant("stableswap_ng"): "stable",
    ProtocolVariant("twocrypto_ng"): "two",
    ProtocolVariant("tricrypto_ng_2024"): "tri",
}
EXPANSION_CHAINS = tuple(
    map(ChainId, ("eip155:10", "eip155:56", "eip155:137", "eip155:43114", "eip155:4663"))
)
NATIVE_CASES = tuple(
    case
    for case in catalog()
    if case.connector == ConnectorId("curve")
    and case.lane == "fork"
    and case.input_policy == "pinned"
    and case.chains[0] in EXPANSION_CHAINS
)


def family_prefix(case):
    if len(case.chains) != 1 or len(case.variants) != 1:
        raise ValueError("retained family evidence requires exactly one chain and variant")
    if case.chains == (ChainId("eip155:4663"),):
        return f"curve-robinhood-{ROBINHOOD_FAMILY_LABELS[case.variants[0]]}"
    return f"curve-{CHAIN_LABELS[case.chains[0]]}{FAMILY_LABELS[case.variants[0]]}"


def captured_directory(case):
    if case.scenario == "curve_mixed":
        return BASE / "curve-mixed-prepared/lp/run"
    kind = "trading" if case.reference.endswith("trading") else "lp"
    return BASE / f"{family_prefix(case)}-prepared" / kind / "run"


def native_directory(case):
    cleanup = case.scenario == "curve_cleanup"
    combined = case.chains in ((ChainId("eip155:43114"),), (ChainId("eip155:4663"),))
    suffix = "approval-cleanup" if cleanup and not combined else "native"
    source = BASE / f"{family_prefix(case)}-{suffix}"
    polygon_cleanup = cleanup and case.chains == (ChainId("eip155:137"),)
    if polygon_cleanup:
        source = BASE / "curve-polygon-approval-cleanup"
    manifest = json.loads((source / "manifest.json").read_text())
    scenario = "approval_commit_then_teardown" if cleanup else "full_lifecycle"
    matches = [
        item
        for item in manifest["cases"]
        if item["profile"] == case.profile
        and (not polygon_cleanup or item["reference"] == case.reference)
        and item.get("operator_mode", "configured") == case.operator_mode
        and (not combined or item["scenario"] == scenario)
    ]
    assert len(matches) == 1, "retained native evidence must identify exactly one profile/scenario"
    selected = matches[0]
    author_suffix = f"/strategies/{case.reference}/config.yaml"
    prefix = f"source/{selected['snapshot']}/" if "snapshot" in selected else "source/"
    authors = [
        name
        for name in manifest["files"]
        if name.startswith(prefix) and name.endswith(author_suffix)
    ]
    assert len(authors) == 1, "retained native evidence must identify its source author config"
    return source / "native" / selected["case"], source / (authors[0] + ".gz")


@pytest.mark.parametrize("case", NATIVE_CASES, ids=lambda case: case.id)
def test_expansion_native_retains_closed_profile_and_family_proof(tmp_path, case):
    from tests.qa.worker import assert_fork_actions

    source, author = native_directory(case)
    state = json.loads(gzip.decompress((source / "checkpoint.json.gz").read_bytes()))
    record = json.loads(gzip.decompress((source / "result.json.gz").read_bytes()))
    assert record["status"] == "passed" and state["mode"] == "closed"
    assert state["pending"] is None and state["delivered"] == len(state["outcomes"])
    assert {b["profile"]["kind"] for b in state["config"]["accounts"]["bindings"]} == {case.profile}
    assert record["strategy"] == case.reference
    if case.scenario == "curve_cleanup":
        assert record["scenario"] == "approval_commit_then_teardown"
        phases = {
            "approval_committed_before_outcome_delivery",
            "replaced_runtime_and_gateway_revoked_without_trading",
        }
    else:
        if case.profile == "safe_roles":
            assert record["operator_mode"] == case.operator_mode
        phases = {
            "submitted_pending_identity_survived_runtime_and_gateway_replacement",
            "entered_and_held",
            "runtime_resumed",
            "gateway_replaced_and_resumed",
            "closed",
        }
    assert phases <= set(record["phases"])
    if case.profile == "safe_roles":
        assert "generated_permissions_installed_and_verified" in record["phases"]
    config = tmp_path / "config.yaml"
    config.write_bytes(gzip.decompress(author.read_bytes()))
    assert_fork_actions(case, state, config)


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.id)
def test_public_qa_audits_retained_curve_runs(tmp_path, case):
    source = captured_directory(case)
    for name in ("manifest.json", "report.json", "state.sqlite"):
        (tmp_path / name).write_bytes(gzip.decompress((source / (name + ".gz")).read_bytes()))
    assert_backtest(case, tmp_path)
    with pytest.raises(AssertionError, match="retained model differs"):
        assert_backtest(replace(case, model="another-model"), tmp_path)
    if len(case.variants) == 1:
        wrong = ProtocolVariant(
            "tricrypto_ng" if str(case.variants[0]) == "stableswap_ng" else "stableswap_ng"
        )
        with pytest.raises(AssertionError, match="missing economic actions"):
            assert_backtest(replace(case, variants=(wrong,)), tmp_path)
    retained = json.loads((tmp_path / "qa-assertions.json").read_text())
    assert retained["closed"] and retained["variants"] == list(map(str, case.variants))


@pytest.mark.parametrize(
    "kind,identity",
    [
        ("qa-eoa", "fork/pinned/curve_ethereum_tricrypto_lp/eoa/configured"),
        ("qa-cleanup", "cleanup/pinned/curve_ethereum_tricrypto_lp/eoa"),
    ],
)
def test_native_qa_worker_retains_closed_profile_and_family_proof(tmp_path, kind, identity):
    from tests.qa.worker import assert_fork_actions

    source = BASE / "curve-public-qa/native" / kind
    case = next(c for c in catalog() if c.id == identity)
    state = json.loads(gzip.decompress((source / "checkpoint.json.gz").read_bytes()))
    record = json.loads(gzip.decompress((source / "result.json.gz").read_bytes()))
    assert record["status"] == "passed" and state["mode"] == "closed"
    assert state["pending"] is None and state["delivered"] == len(state["outcomes"])
    assert {b["profile"]["kind"] for b in state["config"]["accounts"]["bindings"]} == {case.profile}
    config = tmp_path / "config.yaml"
    config.write_bytes(gzip.decompress((source / "config.yaml.gz").read_bytes()))
    assert_fork_actions(case, state, config)
    with pytest.raises(AssertionError, match="missing actions"):
        assert_fork_actions(
            replace(case, variants=(ProtocolVariant("twocrypto_ng"),)), state, config
        )
