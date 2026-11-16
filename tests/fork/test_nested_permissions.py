"""A non-lending connector uses the public Roles compiler for nested ABI arguments."""

import json

import pytest
from eth_utils import keccak, to_checksum_address

from almanak.caip import AssetType
from almanak.effects import WalletAsset
from almanak.gateway.permission_models import ContractIdentity
from almanak.gateway.permissions import generate, verify
from almanak.gateway.safe_fixture import calldata, deploy_profile, owner_transaction
from almanak.models import AccountBinding, StrategyAccounts
from almanak.strategy import StrategyDefinition
from tests.capability_support import SwapCapability, SwapConfig, SwapPermissions, Swaps
from tests.fork.test_safe_permissions import permission_fixture as permission_fixture
from tests.fork.test_safe_permissions import rejected_submission


@pytest.mark.fork
def test_non_lending_nested_tuples_and_arrays_are_checked_on_chain(permission_fixture):
    node, _, _, old_config, _, backend = permission_fixture
    chain = old_config.accounts.bindings[0].profile.fund.chain
    owner = node.funded_signer("", 0, 10**18, 31)
    operator = node.funded_signer("", 0, 10**18, 32)
    profile, _ = deploy_profile(node.rpc, chain, owner, operator, "swap")
    venue = "0x" + "bd" * 20
    # STOP is a test-only receiver. Successful execution proves Roles admission,
    # not an implemented swap venue or simulated swap economics.
    node.rpc("anvil_setCode", [venue, "0x00"])
    contract = ContractIdentity(address=venue, code_hash="0x" + keccak(b"\0").hex())
    source, output = (AssetType(f"{chain.chain_id}/erc20:0x{letter * 40}") for letter in ("a", "b"))
    capability = SwapCapability(
        source=WalletAsset(account="swap", chain=chain, asset=source),
        output=WalletAsset(account="swap", chain=chain, asset=output),
        venue=venue,
        beneficiary=profile.fund.address,
        maximum=10,
        minimum_output=8,
    )
    config = SwapConfig(
        accounts=StrategyAccounts(bindings=(AccountBinding(name="swap", profile=profile),)),
        authority=Swaps(requirements=(capability,)),
    )
    connector = SwapPermissions((contract,))
    strategy = StrategyDefinition(
        filename="strategy.py", source="def capabilities(config):\n    return config.authority\n"
    )
    manifest = generate(config, strategy, backend, connector=connector)
    install_swap_permissions(node, owner, profile, connector, manifest)
    verify(config, manifest, backend, connector=connector)
    valid = ((source.reference, output.reference, 10), [(profile.fund.address, 8)])
    signature = "swap((address,address,uint256),(address,uint256)[])"
    encoded = calldata(signature, ("(address,address,uint256)", "(address,uint256)[]"), valid)
    from almanak.gateway.accounts import ContractCall, prepare_call

    wrapped = prepare_call(
        config.accounts, "swap", ContractCall(fund=profile.fund, to=venue, data=encoded, value=0)
    )
    receipt = node.rpc.send(operator, to_checksum_address(wrapped.to), wrapped.data)
    assert receipt["status"] == "0x1"
    results = reject_nested_calls(node, operator, profile, venue, signature, valid)
    assert not generate(config, strategy, backend, connector=connector).partitions[0].transactions
    verify(config, manifest, backend, connector=connector)
    from pathlib import Path

    directory = Path(__file__).parents[2] / "artifacts/permission-fork"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "nested-permissions.json").write_text(
        json.dumps(
            {
                "manifest": manifest.model_dump(mode="json"),
                "allowed_receipt": receipt,
                "forbidden": results,
            },
            indent=2,
        )
        + "\n"
    )


def install_swap_permissions(node, owner, profile, connector, manifest):
    assert connector.manifest_type.model_validate_json(manifest.model_dump_json()) == manifest
    for call in manifest.partitions[0].transactions:
        owner_transaction(node.rpc, owner, profile.fund.address, call.to, call.data)


def nested_attacks(valid, operator):
    (source, output, amount), recipients = valid
    return [
        ("reversed assets", ((output, source, amount), recipients)),
        ("excess amount", ((source, output, amount + 1), recipients)),
        ("zero amount", ((source, output, 0), recipients)),
        ("operator recipient", ((source, output, amount), [(operator, 8)])),
        ("weaker minimum", ((source, output, amount), [(recipients[0][0], 7)])),
        ("empty recipients", ((source, output, amount), [])),
        ("extra recipient", ((source, output, amount), [*recipients, (operator, 8)])),
    ]


def reject_nested_calls(node, operator, profile, venue, signature, valid):
    cases = nested_attacks(valid, operator.address)
    results = [
        rejected_submission(
            node,
            operator,
            profile,
            (
                name,
                venue,
                calldata(signature, ("(address,address,uint256)", "(address,uint256)[]"), values),
                0,
                0,
            ),
        )
        for name, values in cases
    ]
    return results
