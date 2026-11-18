"""Generated scope-only grants keep the scope installed in retained Safe forks."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.aerodrome import deployments, permissions
from almanak.connectors.aerodrome.authority import declared
from almanak.connectors.aerodrome.models import Parameters
from almanak.connectors.aerodrome.state import Checkpoint
from almanak.gateway.permission_models import ContractIdentity
from tests.exchange_scope import assert_scope_only, without_exchange_budgets
from tests.scope_trees import assert_same_scope, manifest_grants
from tests.test_aerodrome_native import candidate_binding as candidate_binding
from tests.test_aerodrome_observations import CAPTURE
from tests.test_aerodrome_reader import Replay

ROOT = Path(__file__).parents[1]
EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout/aerodrome-base-native"
CASES = [
    c
    for c in json.loads((EVIDENCE / "manifest.json").read_text())["cases"]
    if c["profile"] == "safe_roles" and c["status"] == "passed"
]


def retained(case):
    """The retained checkpoint in the current interface and the raw installed manifest.

    The capture predates scope-only grants (blueprint 27): its manifest carries quotas.
    """
    path = EVIDENCE / case["name"]
    raw = json.loads(gzip.decompress((path / "checkpoint.json.gz").read_bytes()))
    state = Checkpoint.model_validate_json(json.dumps(without_exchange_budgets(raw)))
    manifest = json.loads(gzip.decompress((path / "permissions/manifest.json.gz").read_bytes()))
    result = json.loads(gzip.decompress((path / "result.json.gz").read_bytes()))
    return state, manifest, result


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_current_grants_reproduce_installed_safe_bounds_and_fork_attack_evidence(
    candidate_binding, case
):
    state, manifest, result = retained(case)
    capabilities = declared(state.config)
    assert state.capabilities == capabilities
    profile = state.config.accounts.bindings[0].profile
    grants = permissions.derive(profile, capabilities)
    assert_same_scope(grants, manifest_grants(manifest))
    assert_scope_only(grants)
    assert tuple(manifest["runtime_checks"]) == permissions.AerodromePermissions.runtime_checks
    lp = state.config.allocations[0].liquidity_enabled
    claims = [g for g in grants if g.signature == "claimFees()"]
    assert len(claims) == int(lp)
    if lp:
        assert (
            claims[0].condition is None
            and claims[0].target == state.config.allocations[0].pool.pool
        )
    rejected = result["authority_checks"]["rejected"]
    assert len(rejected) == (20 if lp else 12)
    assert all(r["error_selector"] == "0xd0a9bf58" for r in rejected)
    other = profile.model_copy(
        update={"fund": profile.fund.model_copy(update={"address": "0x" + "ab" * 20})}
    )
    assert permissions.derive(other, capabilities) == ()


@pytest.mark.parametrize("index", [0, 1])
def test_permission_contract_observation_preserves_verified_pool_router_and_token_scopes(
    candidate_binding, index
):
    config = Parameters.model_validate_json(
        json.dumps(without_exchange_budgets(CAPTURE["rows"][index]["config"]))
    )
    source = config.allocations[0].pool
    case = next(
        c
        for c in CASES
        if ("stable" if source.stable else "volatile") in c["name"]
        and "-safe-configured" in c["name"]
    )
    _, manifest, _ = retained(case)
    expected = {
        i.address: i
        for i in (
            ContractIdentity.model_validate_json(json.dumps(value))
            for value in manifest["partitions"][0]["contracts"]
        )
    }
    with Replay(index) as reader:
        block = reader.pin_block(CAPTURE["block"]["number"])
        observation = SimpleNamespace(
            reader=reader, block=block, identity=lambda address: expected[address]
        )
        actual = deployments.identities(config, config.allocations[0].account, observation)
    assert {i.address: i for i in actual} == expected
    assert deployments.identities(config, "absent", observation) == ()
