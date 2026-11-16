"""Operator ceilings cannot rewrite the actual production example's economics."""

import hashlib
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from almanak.caip import ChainId
from almanak.connectors.support import RunMode
from almanak.gateway.bootstrap import BootstrapRequest, BootstrapService
from almanak.gateway.operator_config import OperatorConfig, content_digest, read_operator_config
from almanak.gateway.operator_execution import ProductionAuthority, SigningFiles
from almanak.gateway.production_config import GasPolicy, ProductionNetwork, ReplacementPolicy
from almanak.models import StrategyAccounts
from tests.test_author_resolution import frozen_production_gas
from tests.test_resolution_context import context as context
from tests.test_resolution_context import operator as operator
from tests.test_resolution_context import revalidate

ROOT = Path(__file__).parents[1]


@pytest.fixture
def policy():
    policy = GasPolicy.model_validate_json(json.dumps(frozen_production_gas()[0]))
    return revalidate(policy, accounts=("Main",))


@pytest.fixture
def authority(policy):
    return ProductionAuthority.model_validate_json(
        json.dumps(
            {
                "networks": [{"chain": "eip155:1", "max_observation_age_seconds": 120}],
                "payers": [
                    {
                        **policy.model_dump(
                            mode="json",
                            include={
                                "accounts",
                                "max_gas_price_wei",
                                "max_gas_limit",
                                "max_transaction_fee",
                                "total_budget",
                            },
                        ),
                        "allow_replacement": False,
                    }
                ],
                "signing": {
                    "key_file": "/private/operator/keys.json",
                    "password_file": "/private/operator/password",
                },
            }
        )
    )


def test_actual_policy_is_preserved_without_loading_signers(
    operator, authority, policy, monkeypatch
):
    def forbidden(*_, **__):
        raise AssertionError("configuration compatibility cannot read credentials or providers")

    monkeypatch.setattr(Path, "read_text", forbidden)
    operator = revalidate(operator, production=authority)
    execution = authority.check(operator, operator.accounts, (policy,))
    assert execution.gas == (policy,)
    assert execution.gas[0] is policy
    assert execution.networks[0].provider == operator.networks[0].binding()
    assert execution.networks[0].max_observation_age_seconds == 120
    assert execution.gas[0].exit_reserve == policy.exit_reserve
    assert "key_file" not in execution.model_dump_json()


@pytest.mark.parametrize(
    "field", ["max_gas_price_wei", "max_gas_limit", "max_transaction_fee", "total_budget"]
)
def test_operator_rejects_excess_and_reports_both_bounds(operator, authority, policy, field):
    ceiling = authority.payers[0]
    permitted = getattr(ceiling, field)
    if isinstance(permitted, int):
        reduced = permitted - 1
        expected = (permitted, reduced)
    else:
        reduced = revalidate(permitted, amount=permitted.amount - 1)
        expected = (permitted.amount, reduced.amount)
    authority = revalidate(authority, payers=(revalidate(ceiling, **{field: reduced}),))
    operator = revalidate(operator, production=authority)
    original = policy.model_dump_json()
    with pytest.raises(ValueError, match=rf"{field}={expected[0]}.*at most {expected[1]}"):
        authority.check(operator, operator.accounts, (policy,))
    assert policy.model_dump_json() == original


def test_absent_total_budgets_admit_a_policy_without_a_lifetime_cap(operator, authority, policy):
    uncapped = revalidate(policy, total_budget=None)
    ceiling = revalidate(authority.payers[0], total_budget=None)
    authority = revalidate(authority, payers=(ceiling,))
    operator = revalidate(operator, production=authority)
    execution = authority.check(operator, operator.accounts, (uncapped,))
    assert execution.gas[0].total_budget is None
    assert "total_budget" not in execution.gas[0].model_dump()


def test_an_operator_total_budget_requires_the_author_to_state_one(operator, authority, policy):
    operator = revalidate(operator, production=authority)
    with pytest.raises(ValueError, match="states no total_budget"):
        authority.check(operator, operator.accounts, (revalidate(policy, total_budget=None),))


def test_present_total_budget_still_bounds_the_fee_ceiling_and_reserve(policy):
    small = policy.total_budget.model_copy(update={"amount": policy.max_transaction_fee.amount - 1})
    with pytest.raises(ValidationError, match="fit the total budget"):
        revalidate(policy, total_budget=small)


def test_replacement_is_explicit_and_preserved(operator, authority, policy):
    policy = revalidate(
        policy,
        replacement=ReplacementPolicy(
            interval_seconds=12, minimum_bump_bps=1250, max_candidates=3, max_elapsed_seconds=60
        ),
    )
    operator = revalidate(operator, production=authority)
    with pytest.raises(ValueError, match="replacement is disabled"):
        authority.check(operator, operator.accounts, (policy,))
    allowed = revalidate(
        authority, payers=(revalidate(authority.payers[0], allow_replacement=True),)
    )
    operator = revalidate(operator, production=allowed)
    assert allowed.check(operator, operator.accounts, (policy,)).gas == (policy,)


def shared_safe_accounts(operator):
    # Synthetic account topology tests policy scope, not installed Safe authority.
    fund = operator.accounts.bindings[0].profile.fund.model_dump(mode="json")
    return StrategyAccounts.model_validate_json(
        json.dumps(
            {
                "bindings": [
                    {
                        "name": name,
                        "profile": {
                            "kind": "safe_roles",
                            "fund": {**fund, "address": "0x" + byte * 20},
                            "operator": fund,
                            "roles_module": "0x" + "ab" * 20,
                            "role_key": "0x" + "01" * 32,
                        },
                    }
                    for name, byte in (("Main", "34"), ("Second", "56"))
                ]
            }
        )
    )


def test_shared_payer_has_one_ceiling_and_compatible_author_policy(operator, authority, policy):
    accounts = shared_safe_accounts(operator)
    names = ("Main", "Second")
    ceiling = revalidate(authority.payers[0], accounts=names)
    authority = revalidate(authority, payers=(ceiling,))
    operator = revalidate(operator, accounts=accounts, production=authority)
    policy = revalidate(policy, accounts=names)
    assert authority.check(operator, accounts, (policy,)).gas == (policy,)
    separated = (revalidate(policy, accounts=("Main",)), revalidate(policy, accounts=("Second",)))
    with pytest.raises(ValueError, match="sharing a gas payer"):
        authority.check(operator, accounts, separated)
    split = revalidate(authority, payers=tuple(revalidate(ceiling, accounts=(n,)) for n in names))
    with pytest.raises(ValueError, match="sharing a payer require one operator ceiling"):
        revalidate(operator, production=split)


def test_one_operator_ceiling_cannot_cover_independent_payers(operator, authority):
    accounts = shared_safe_accounts(operator).model_dump(mode="json")
    accounts["bindings"][1]["profile"]["operator"]["address"] = "0x" + "78" * 20
    accounts = StrategyAccounts.model_validate_json(json.dumps(accounts))
    authority = revalidate(
        authority, payers=(revalidate(authority.payers[0], accounts=("Main", "Second")),)
    )
    with pytest.raises(ValueError, match="exactly one concrete payer"):
        revalidate(operator, accounts=accounts, production=authority)


def test_different_fund_identity_and_missing_grant_refuse(operator, authority, policy):
    operator = revalidate(operator, production=authority)
    raw = operator.accounts.model_dump(mode="json")
    raw["bindings"][0]["profile"]["fund"]["address"] = "0x" + "34" * 20
    changed = StrategyAccounts.model_validate_json(json.dumps(raw))
    with pytest.raises(ValueError, match="binding differs"):
        authority.check(operator, changed, (policy,))
    extra = changed.bindings[0].model_copy(update={"name": "Other"})
    expanded = revalidate(operator.accounts, bindings=operator.accounts.bindings + (extra,))
    operator = revalidate(operator, accounts=expanded)
    with pytest.raises(ValueError, match="no production ceiling for Other"):
        authority.check(
            operator,
            StrategyAccounts(bindings=(extra,)),
            (revalidate(policy, accounts=("Other",)),),
        )


@pytest.mark.parametrize(
    "fault", ["missing_account", "duplicate_payer", "networks", "disabled_mode", "legacy"]
)
def test_malformed_operator_authority_refuses_before_any_io(operator, authority, fault):
    raw = operator.model_dump(mode="json")
    raw["production"] = authority.model_dump(mode="json")
    if fault == "missing_account":
        raw["production"]["payers"][0]["accounts"] = ["Missing"]
    elif fault == "duplicate_payer":
        raw["production"]["payers"] *= 2
    elif fault == "networks":
        raw["production"]["networks"][0]["chain"] = "eip155:43114"
    elif fault == "disabled_mode":
        raw["modes"] = ["fork"]
    else:
        raw["schema_version"] = "operator-read-config-v1"
    with pytest.raises(ValidationError):
        OperatorConfig.model_validate_json(json.dumps(raw))


def test_read_only_configuration_cannot_be_used_as_production_grant(operator, authority, policy):
    with pytest.raises(ValueError, match="not enabled this production authority"):
        authority.check(operator, operator.accounts, (policy,))
    with pytest.raises(ValueError, match="cover exactly"):
        authority.check(revalidate(operator, production=authority), operator.accounts, ())


def test_bootstrap_with_execution_settings_still_cannot_sign(
    operator, authority, context, tmp_path
):
    operator = revalidate(operator, production=authority)
    context = revalidate(context, operator_digest=operator.digest, mode=RunMode.PRODUCTION)
    service = BootstrapService(context, operator, RunMode.PRODUCTION, tmp_path / "missing.env")
    request = BootstrapRequest(context_digest=context.digest)
    assert service.dispatch("resolution-context", request).authority == "read_only"
    for operation in ("sign", "broadcast", "install-permissions"):
        with pytest.raises(ValueError, match="exact operation fields"):
            service.dispatch(operation, request)


def test_legacy_read_codec_preserves_exact_digest_and_new_codec_round_trips(
    tmp_path, operator, authority
):
    raw = operator.model_dump(mode="json")
    raw["schema_version"] = "operator-read-config-v1"
    encoded = json.dumps(raw, sort_keys=True, separators=(",", ":"))
    old = OperatorConfig.model_validate_json(encoded)
    assert old.model_dump(mode="json") == raw
    assert old.digest == hashlib.sha256(encoded.encode()).hexdigest()
    full = revalidate(operator, production=authority)
    path = tmp_path / "operator.yaml"
    path.write_text(full.model_dump_json())
    assert read_operator_config(path) == full
    assert full.digest != operator.digest
    assert content_digest(full) == full.digest


@pytest.mark.parametrize("field", ["key_file", "password_file", "signer_file"])
def test_signing_references_are_absolute_and_never_secret_values(field):
    raw = {"key_file": "/private/keys", "password_file": "/private/password", field: "relative"}
    if field == "signer_file":
        raw = {"signer_file": "relative"}
    with pytest.raises(ValidationError, match="absolute file paths"):
        SigningFiles.model_validate_json(json.dumps(raw))


def test_remote_signing_is_exclusive_with_keystores_and_passwords():
    remote = SigningFiles(signer_file=Path("/private/operator/signer.json"))
    assert remote.key_file is None and remote.password_file is None
    with pytest.raises(ValidationError, match="exactly one"):
        SigningFiles(key_file=Path("/private/keys"), signer_file=remote.signer_file)
    with pytest.raises(ValidationError, match="no keystore password"):
        SigningFiles(signer_file=remote.signer_file, password_file=Path("/private/password"))
    with pytest.raises(ValidationError, match="exactly one"):
        SigningFiles()


def test_wrong_native_identity_and_unsupported_production_network_refuse(operator, authority):
    raw = authority.model_dump(mode="json")
    raw["payers"][0]["total_budget"]["asset"] = "eip155:1/slip44:1"
    with pytest.raises(ValidationError, match="native asset"):
        ProductionAuthority.model_validate_json(json.dumps(raw))
    # OP-family networks admit production: the pinned Jovian exposure bound
    # covers L1 data and operator costs inside the same payer ceilings.
    raw = revalidate(operator, production=authority).model_dump_json()
    raw = raw.replace("eip155:1", "eip155:10")
    assert OperatorConfig.model_validate_json(raw).production is not None
    network = {
        "chain": {"chain_id": "eip155:10", "environment": "testnet"},
        "provider": {"kind": "rpc_url", "url_env": "OP_RPC"},
        "max_observation_age_seconds": 120,
    }
    with pytest.raises(ValidationError, match="mainnet environment"):
        ProductionNetwork.model_validate_json(json.dumps(network))


def test_same_address_on_another_chain_is_a_different_payer(operator, authority, policy):
    accounts = operator.accounts.model_dump(mode="json")
    other = json.loads(json.dumps(accounts["bindings"][0]))
    other["name"] = "Avalanche"
    other["profile"]["fund"]["chain"]["chain_id"] = "eip155:43114"
    accounts["bindings"].append(other)
    accounts = StrategyAccounts.model_validate_json(json.dumps(accounts))
    second = GasPolicy.model_validate_json(
        policy.model_dump_json()
        .replace("eip155:1/slip44:60", "eip155:43114/slip44:9005")
        .replace('"Main"', '"Avalanche"')
    )
    extra = type(authority.payers[0]).model_validate_json(
        authority.payers[0]
        .model_dump_json()
        .replace("eip155:1/slip44:60", "eip155:43114/slip44:9005")
        .replace('"Main"', '"Avalanche"')
    )
    chain = ChainId("eip155:43114")
    authority = revalidate(
        authority,
        payers=authority.payers + (extra,),
        networks=authority.networks + (revalidate(authority.networks[0], chain=chain),),
    )
    operator = revalidate(
        operator,
        accounts=accounts,
        production=authority,
        networks=operator.networks + (revalidate(operator.networks[0], chain=chain),),
    )
    execution = authority.check(operator, accounts, (policy, second))
    assert execution.gas == (policy, second)
    assert len(execution.networks) == 2
