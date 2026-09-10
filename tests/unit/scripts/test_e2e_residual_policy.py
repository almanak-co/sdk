import pytest

from qa_lab.e2e_residual_policy import assess_residual_policy

POLICY = {
    "known_nft_liquidity_raw": "0",
    "pending_orders": 0,
    "wallet_policy": "inventory_all_tokens_and_reconcile_subject_transactions",
}


def test_component_success_does_not_prove_complete_inventory():
    result = assess_residual_policy(
        {"residual_policy": POLICY},
        generations={"terminal_closure": {"status": "PASS"}},
        quantities={"status": "PASS"},
        actor={"status": "PASS"},
    )
    assert result["status"] == "UNMEASURED"
    assert result["known_nft_closure"] == "PASS"
    assert result["subject_transaction_quantities"] == "PASS"
    assert result["actor_terminal_inventory"] == "PASS"
    assert result["subject_pending_transactions"] == "UNMEASURED"
    assert result["all_token_wallet_inventory"] == "UNMEASURED"


@pytest.mark.parametrize(
    "policy",
    [
        None,
        {},
        {**POLICY, "pending_orders": False},
        {**POLICY, "pending_orders": 1},
        {**POLICY, "wallet_policy": "known_tokens_only"},
    ],
)
def test_weakened_or_malformed_policy_is_rejected(policy):
    with pytest.raises(ValueError, match="Unsupported frozen residual policy"):
        assess_residual_policy({"residual_policy": policy}, generations=None, quantities=None, actor=None)


def test_legacy_contract_does_not_inherit_new_claim():
    assert assess_residual_policy({}, generations=None, quantities=None, actor=None) is None


@pytest.fixture
def measured(monkeypatch, tmp_path):
    from qa_lab import (
        e2e_final_database,
        e2e_final_state,
        e2e_subject_pending,
        e2e_terminal_boundary,
        e2e_wallet_discovery,
        e2e_wallet_inventory,
    )

    results = {name: {"status": "PASS"} for name in ("snapshot", "database", "pending", "boundary", "balances")}
    results["discovery"] = {"status": "DISCOVERED"}
    for module, function, name in (
        (e2e_final_state, "observe_final_subject", "snapshot"),
        (e2e_final_database, "compare_final_database", "database"),
        (e2e_subject_pending, "observe_subject_pending", "pending"),
        (e2e_terminal_boundary, "observe_terminal_boundary", "boundary"),
        (e2e_wallet_discovery, "observe_wallet_discovery", "discovery"),
        (e2e_wallet_inventory, "observe_subject_inventory", "balances"),
    ):
        monkeypatch.setattr(module, function, lambda *args, name=name: results[name])
    return tmp_path, results


def assess_measured(measured, policy):
    return assess_residual_policy(
        {"residual_policy": policy},
        generations={"terminal_closure": {"status": "PASS"}},
        quantities={"status": "PASS"},
        actor={"status": "PASS"},
        bundle=measured[0],
    )


def test_scoped_inventory_can_pass_without_claiming_every_possible_wallet_asset(measured):
    from qa_lab.e2e_residual_policy import OBSERVED_ASSET_POLICY

    result = assess_measured(measured, OBSERVED_ASSET_POLICY)
    assert result["status"] == "PASS"
    assert result["all_token_wallet_inventory"] == "UNMEASURED"
    assert result["unmeasured_scope"] == "passive-preexisting-assets-and-nonstandard-token-behavior"
    assert assess_measured(measured, POLICY)["status"] == "UNMEASURED"


@pytest.mark.parametrize("proof", ["snapshot", "database", "pending", "boundary", "balances", "discovery"])
@pytest.mark.parametrize("status", ["UNMEASURED", "FAIL"])
def test_scoped_policy_requires_every_independent_measurement(measured, proof, status):
    from qa_lab.e2e_residual_policy import OBSERVED_ASSET_POLICY

    measured[1][proof]["status"] = status
    assert assess_measured(measured, OBSERVED_ASSET_POLICY)["status"] == status


def test_scope_disclosure_cannot_be_removed_from_prepared_inventory_policy():
    from qa_lab.e2e_card import ObservedAssetResidualPolicy
    from qa_lab.e2e_residual_policy import OBSERVED_ASSET_POLICY

    assert ObservedAssetResidualPolicy.model_validate(OBSERVED_ASSET_POLICY).model_dump() == OBSERVED_ASSET_POLICY
    omitted = {key: value for key, value in OBSERVED_ASSET_POLICY.items() if key != "unmeasured_scope"}
    with pytest.raises(ValueError):
        ObservedAssetResidualPolicy.model_validate(omitted)
    with pytest.raises(ValueError, match="Unsupported frozen"):
        assess_measured((None, {}), omitted)
