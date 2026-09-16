"""Negative controls for asset-scoped identity and immutable qualification."""

import copy
import json

import pytest

from qa_lab.asset_scenarios import (
    ClaimKey,
    QualifiedClaimKey,
    build_scope,
    canonical_sha256,
    load_scenario_registry,
    scenario_contract_sha256,
    scenario_dependency_fingerprint,
    subject_families,
    validate_exact_resources,
    validate_scenario_contract,
    validate_scope,
    verify_lookup_key,
)


@pytest.fixture
def contract():
    return {
        "schema_version": 1,
        "scenario_id": "nvdac-usdc-spot",
        "label": "NVDAc / USDC spot",
        "parent_bindings": [
            {
                "surface": "intent",
                "parent_cell_id": "intent.aerodrome.base.SWAP.anvil.eoa",
                "protocol": "aerodrome",
                "chain": "base",
                "intent": "SWAP",
                "network": "anvil",
                "exec_path": "eoa",
            }
        ],
        "assets": [
            {
                "asset_id": "stock",
                "chain": "base",
                "address": "0x" + "a" * 40,
                "decimals": 8,
                "roles": ["output"],
                "families": ["tokenized-equities"],
                "issuer": "Curated issuer",
                "classification_provenance": "reviewed-token-list:v1",
            },
            {
                "asset_id": "quote",
                "chain": "base",
                "address": "0x" + "b" * 40,
                "decimals": 6,
                "roles": ["input", "quote"],
                "families": ["stablecoins"],
                "classification_provenance": "reviewed-token-list:v1",
            },
        ],
        "subject_assets": ["stock"],
        "resource": {
            "chain": "base",
            "address": "0x" + "c" * 40,
            "token_order": ["quote", "stock"],
            "verification": "reviewed-factory:v1",
        },
        "recipe": {"id": "aerodrome.nvdac.swap", "version": 1, "semantic_profiles": {"swap": "v1"}},
    }


@pytest.fixture
def observed(contract):
    return {
        "chain": "base",
        "assets": [
            {key: asset[key] for key in ("asset_id", "chain", "address", "decimals")} for asset in contract["assets"]
        ],
        "resource": {
            "chain": "base",
            "address": contract["resource"]["address"],
            "token_order": ["0x" + "b" * 40, "0x" + "a" * 40],
        },
    }


def test_structured_keys_cannot_alias_scenarios():
    first = ClaimKey("intent", "intent.aerodrome.base.SWAP.anvil.eoa", "stock")
    second = ClaimKey(first.surface, first.parent_cell_id, "standard")
    assert first.lookup_key != second.lookup_key
    qualified = QualifiedClaimKey(first, "a" * 64)
    assert verify_lookup_key(qualified.to_dict(), qualified.lookup_key) == qualified
    assert qualified.lookup_key != QualifiedClaimKey(first, "b" * 64).lookup_key


@pytest.mark.parametrize("field", ["lookup_key", "claim", "contract", "proof_node", "semantic_profiles"])
def test_index_and_label_tampering_refused(contract, field):
    scope = build_scope(contract, contract["parent_bindings"][0], "test.py::test_stock")
    assert validate_scope(scope) == scope
    if field == "claim":
        scope[field]["scenario_id"] = "standard"
    elif field == "contract":
        scope[field]["assets"][0]["issuer"] = "Untrusted issuer"
    elif field == "semantic_profiles":
        scope[field] = {"swap": "v2"}
    else:
        scope[field] = "tampered"
    with pytest.raises(ValueError):
        validate_scope(scope)


@pytest.mark.parametrize("mismatch", ["chain", "address", "decimals", "pool", "order", "missing"])
def test_asset_binding_mismatch_refused(contract, observed, mismatch):
    validate_exact_resources(contract, observed)
    if mismatch == "chain":
        observed["chain"] = "bsc"
    elif mismatch in {"address", "decimals"}:
        observed["assets"][0][mismatch] = 18 if mismatch == "decimals" else "0x" + "d" * 40
    elif mismatch == "pool":
        observed["resource"]["address"] = "0x" + "d" * 40
    elif mismatch == "order":
        observed["resource"]["token_order"].reverse()
    else:
        observed["assets"].pop()
    with pytest.raises(ValueError):
        validate_exact_resources(contract, observed)


def test_classification_is_not_authenticity_evidence(contract, observed):
    previous = scenario_contract_sha256(contract)
    contract["assets"][0]["issuer"] = "Corrected curated issuer"
    assert scenario_contract_sha256(contract) != previous
    # Chain observations qualify addresses, not the human-maintained issuer assertion.
    assert validate_exact_resources(contract, observed) is None


def test_quote_asset_does_not_enroll_behavior(contract):
    assert subject_families(contract) == ["tokenized-equities"]
    contract["assets"][0]["families"].append("vault-shares")
    assert subject_families(contract) == ["tokenized-equities", "vault-shares"]
    assert validate_scenario_contract(contract)["behavior_profiles"] == []


@pytest.mark.parametrize("axis,value", [("network", "mainnet"), ("exec_path", "safe")])
def test_scope_axes_do_not_cross_qualify(contract, axis, value):
    parent = copy.deepcopy(contract["parent_bindings"][0])
    parent[axis] = value
    parent["parent_cell_id"] = ".".join(
        parent[name] for name in ("surface", "protocol", "chain", "intent", "network", "exec_path")
    )
    with pytest.raises(ValueError, match="not enrolled"):
        build_scope(contract, parent, "test.py::test_stock")


def test_unrelated_enrollment_preserves_scoped_admission(contract):
    scope = build_scope(contract, contract["parent_bindings"][0], "test.py::test_stock")
    sibling = copy.deepcopy(contract)
    sibling["scenario_id"] = "other-market"
    assert canonical_sha256([contract]) != canonical_sha256([contract, sibling])
    assert validate_scope(scope) == build_scope(contract, contract["parent_bindings"][0], "test.py::test_stock")


@pytest.mark.parametrize("dependency", ["node", "profile", "resource", "recipe"])
def test_scoped_redefinition_blocks_reattribution(contract, dependency):
    parent = contract["parent_bindings"][0]
    node, profiles = "test.py::test_stock", {"swap": "v1"}
    previous = scenario_dependency_fingerprint(contract, parent, node, profiles)
    if dependency == "node":
        node = "test.py::test_standard"
    elif dependency == "profile":
        profiles = {"swap": "v2"}
    elif dependency == "resource":
        contract["resource"]["address"] = "0x" + "d" * 40
    else:
        contract["recipe"]["version"] = 2
    assert scenario_dependency_fingerprint(contract, parent, node, profiles) != previous


def test_canonical_contract_order_is_stable(contract):
    previous = scenario_contract_sha256(contract)
    contract["assets"].reverse()
    contract["assets"][0]["roles"].reverse()
    assert scenario_contract_sha256(contract) == previous


def test_reference_price_requirement_is_conditional(contract):
    assert validate_scenario_contract(contract)["behavior_profiles"] == []
    contract["behavior_profiles"] = ["equity-reference:v1"]
    with pytest.raises(ValueError, match="Unknown behavior"):
        validate_scenario_contract(contract)
    assert validate_scenario_contract(contract, registered_behaviors=["equity-reference:v1"])


def test_registry_refuses_duplicate_scenario_ids(tmp_path, contract):
    path = tmp_path / "registry.json"
    path.write_text(json.dumps({"schema_version": 1, "scenarios": [contract, contract]}))
    with pytest.raises(ValueError, match="Duplicate scenario"):
        load_scenario_registry(path)
    path.write_text(json.dumps({"schema_version": 1, "scenarios": [contract]}))
    assert load_scenario_registry(path)[0]["scenario_id"] == contract["scenario_id"]


def test_missing_registry_enrolls_nothing(tmp_path):
    assert load_scenario_registry(tmp_path / "absent.json") == []


@pytest.mark.parametrize("scoped_node", [False, True])
def test_mainnet_enrollment_preserves_legacy_recipe_without_enabling_dispatch(contract, scoped_node):
    from qa_lab.qa_coverage import _attach_intent_asset_scenarios, _intent_plan_asset_scope

    binding = contract["parent_bindings"][0]
    binding.update(network="mainnet", parent_cell_id="intent.aerodrome.base.SWAP.mainnet.eoa")
    node = {"exec_path": "eoa", "recipe_id": "legacy-recipe"}
    if scoped_node:
        node["scenario_id"] = contract["scenario_id"]
    cell = {
        "id": "intent.aerodrome.base.SWAP",
        "protocol": "aerodrome",
        "chain": "base",
        "intent": "SWAP",
        "mainnet_recipes": [node],
    }
    _attach_intent_asset_scenarios([cell], [contract])
    assert cell["mainnet_recipes"] == [node]
    with pytest.raises(ValueError, match="coordinator binding"):
        _intent_plan_asset_scope(cell, binding["parent_cell_id"], contract["scenario_id"], [node])
    with pytest.raises(ValueError, match="requires --scenario-id"):
        _intent_plan_asset_scope(cell, binding["parent_cell_id"], None, [node])


@pytest.mark.parametrize("invalid", ["undeclared", "duplicate"])
def test_mainnet_scoped_nodes_still_require_unique_enrollment(contract, invalid):
    from qa_lab.qa_coverage import _attach_intent_asset_scenarios

    binding = contract["parent_bindings"][0]
    binding.update(network="mainnet", parent_cell_id="intent.aerodrome.base.SWAP.mainnet.eoa")
    node = {
        "exec_path": "eoa",
        "recipe_id": "scoped-recipe",
        "scenario_id": "unknown" if invalid == "undeclared" else contract["scenario_id"],
    }
    cell = {
        "id": "intent.aerodrome.base.SWAP",
        "protocol": "aerodrome",
        "chain": "base",
        "intent": "SWAP",
        "mainnet_recipes": [node] if invalid == "undeclared" else [node, node],
    }
    with pytest.raises(ValueError, match="Undeclared|Duplicate"):
        _attach_intent_asset_scenarios([cell], [contract])
