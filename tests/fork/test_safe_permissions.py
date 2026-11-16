"""Contract-level proof of the public compiler, without test-authored grants."""

import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_utils import keccak, to_checksum_address

from almanak.caip import AssetType, ChainId
from almanak.connectors.aave_v4.fork import ForkConfig, fork_allocation
from almanak.connectors.aave_v4.models import DebtAllocation, LendingConfig
from almanak.connectors.aave_v4.permission_adapter import generate, verify
from almanak.gateway.accounts import ContractCall, prepare_call
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.roles_storage import ARTIFACTS
from almanak.gateway.safe_fixture import calldata, deploy_profile, owner_transaction
from almanak.models import AccountBinding, Block, Chain, StrategyAccounts

ROOT = Path(__file__).parents[2]


def debt_allocation(allocation):
    reserve = allocation.reserve.model_dump()
    reserve.update(
        reserve_id=7,
        asset=AssetType(
            f"{allocation.reserve.chain.chain_id}/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        ),
    )
    return DebtAllocation.model_validate(
        {
            **allocation.model_dump(),
            "name": "main_usdc",
            "reserve": reserve,
            "amount": 5_000_000,
            "minimum_health_factor": 2 * 10**18,
            "collateral": (allocation.name,),
            "oracle": "0x99b2b6cea9c3d2fd8f4d90f86741c44b212a6127",
            "collateral_price_sources": ("0x5424384b256154046e9667ddfaaa5e550145215e",),
            "debt_price_source": "0x3f73f03aa83b2a48ed27e964ed0fdb590332095b",
        }
    )


@pytest.fixture
def permission_fixture(request, reference_strategy):
    config = ForkConfig.model_validate_json(
        (ROOT / "tests/fixtures/author/aave_v4_weth_supply/config.json").read_text()
    )
    pin = Block.model_validate_json(
        json.dumps(
            json.loads((ARTIFACTS / "ethereum-25908472-authority.json").read_text())["block"]
        )
    )
    config = config.resolved(pin)
    with Fork(request.config.getoption("--fork-env"), pin, target=config.single_target()) as node:
        chain = Chain(
            chain_id=ChainId("eip155:31337"), environment="anvil:" + node.metadata["instanceId"]
        )
        owner = node.funded_signer("", 0, 10**18, 20)
        operator = node.funded_signer("", 0, 10**18, 21)
        profile, _ = deploy_profile(node.rpc, chain, owner, operator, "lending_0")
        allocations = tuple(
            fork_allocation(a, chain) for a in config.allocations if a.account == "lending_0"
        )
        deployment = LendingConfig(
            accounts=StrategyAccounts(
                bindings=(AccountBinding(name="lending_0", profile=profile),)
            ),
            allocations=allocations,
            debts=(debt_allocation(allocations[0]),),
            fork_origin=pin,
            fork_instance=node.metadata["instanceId"],
        )
        with EthereumReader(chain, node.rpc.endpoint) as reader:
            backend = SimpleNamespace(reader=reader, guard=node._validate_origin)
            manifest = generate(deployment, reference_strategy, backend)
            for partition in manifest.partitions:
                for call in partition.transactions:
                    owner_transaction(node.rpc, owner, profile.fund.address, call.to, call.data)
            yield node, owner, operator, deployment, manifest, backend


@pytest.mark.fork
def test_generated_roles_installation_repeats_entries_inside_scope(
    permission_fixture, reference_strategy
):
    node, owner, operator, config, manifest, backend = permission_fixture
    result = verify(config, manifest, backend)
    assert all("quotas" not in partition for partition in result["partitions"])
    regenerated = generate(config, reference_strategy, backend)
    assert not regenerated.partitions[0].transactions
    profile = config.accounts.bindings[0].profile
    allocation = config.allocations[0]
    token = allocation.reserve.asset.reference
    # Scope-only authority: supplies repeat with no cumulative quota.
    amount, entries = 10**15, 3
    node.wrap_native(owner, to_checksum_address(token), amount * entries)
    node.rpc.send(
        owner,
        to_checksum_address(token),
        calldata(
            "transfer(address,uint256)",
            ("address", "uint256"),
            (profile.fund.address, amount * entries),
        ),
    )
    send_role(
        node,
        operator,
        config,
        token,
        "approve(address,uint256)",
        ("address", "uint256"),
        (allocation.reserve.spoke, amount * entries),
    )
    for _ in range(entries):
        send_role(
            node,
            operator,
            config,
            allocation.reserve.spoke,
            "supply(uint256,uint256,address)",
            ("uint256", "uint256", "address"),
            (0, amount, profile.fund.address),
        )
    verify(config, manifest, backend)
    assert not generate(config, reference_strategy, backend).partitions[0].transactions


def send_role(node, operator, config, to, signature, types, args):
    profile = config.accounts.bindings[0].profile
    call = ContractCall(fund=profile.fund, to=to, value=0, data=calldata(signature, types, args))
    wrapped = prepare_call(config.accounts, "lending_0", call)
    return node.rpc.send(operator, to_checksum_address(wrapped.to), wrapped.data)


@pytest.mark.fork
def test_widened_target_clearance_is_refused(permission_fixture):
    # allowTarget on a strategy target replaces its scoped clearance (2) with the whole target (1).
    node, owner, _, config, manifest, backend = permission_fixture
    profile = config.accounts.bindings[0].profile
    owner_transaction(
        node.rpc,
        owner,
        profile.fund.address,
        profile.roles_module,
        calldata(
            "allowTarget(bytes32,address,uint8)",
            ("bytes32", "address", "uint8"),
            (bytes.fromhex(profile.role_key[2:]), config.allocations[0].reserve.spoke, 0),
        ),
    )
    with pytest.raises(DataError, match="not restricted to scoped functions"):
        verify(config, manifest, backend)


def forbidden_calls(config):
    profile = config.accounts.bindings[0].profile
    safe, operator = profile.fund.address, profile.operator.address
    main, blue = config.allocations
    spoke = main.reserve.spoke
    token = main.reserve.asset.reference
    calls = []
    for name, reserve, amount, beneficiary in (
        ("USDC supply cross-product", 7, 1, safe),
        ("wrong reserve", 3, 1, safe),
        ("operator beneficiary", 0, 1, operator),
        ("excess supply", 0, main.amount + 1, safe),
        ("zero supply", 0, 0, safe),
    ):
        calls.append(
            (
                name,
                spoke,
                calldata(
                    "supply(uint256,uint256,address)",
                    ("uint256", "uint256", "address"),
                    (reserve, amount, beneficiary),
                ),
                0,
                0,
            )
        )
    for name, action, reserve, amount, beneficiary in (
        ("WETH borrow cross-product", "borrow", 0, 1, safe),
        ("excess borrow", "borrow", 7, 5_000_001, safe),
        ("borrow for operator", "borrow", 7, 1, operator),
        ("repay WETH", "repay", 0, 1, safe),
        ("zero repayment", "repay", 7, 0, safe),
        ("repay another fund", "repay", 7, 1, operator),
        ("partial withdrawal", "withdraw", 0, 1, safe),
        ("withdraw USDC", "withdraw", 7, 2**256 - 1, safe),
        ("withdraw for operator", "withdraw", 0, 2**256 - 1, operator),
    ):
        calls.append(
            (
                name,
                spoke,
                calldata(
                    action + "(uint256,uint256,address)",
                    ("uint256", "uint256", "address"),
                    (reserve, amount, beneficiary),
                ),
                0,
                0,
            )
        )
    for name, target, spender, amount in (
        ("wrong spender", token, operator, 1),
        ("third asset approval", "0x6b175474e89094c44da98b954eedeac495271d0f", spoke, 1),
        ("USDC Bluechip approval", config.debts[0].reserve.asset.reference, blue.reserve.spoke, 1),
    ):
        calls.append(
            (
                name,
                target,
                calldata("approve(address,uint256)", ("address", "uint256"), (spender, amount)),
                0,
                0,
            )
        )
    allowed = calldata(
        "supply(uint256,uint256,address)", ("uint256", "uint256", "address"), (0, 1, safe)
    )
    calls.extend(
        (
            ("delegatecall", spoke, allowed, 0, 1),
            ("native value", spoke, allowed, 1, 0),
            (
                "wrong Spoke borrow",
                blue.reserve.spoke,
                calldata(
                    "borrow(uint256,uint256,address)",
                    ("uint256", "uint256", "address"),
                    (7, 1, safe),
                ),
                0,
                0,
            ),
            (
                "unconfigured collateral",
                spoke,
                calldata(
                    "setUsingAsCollateral(uint256,bool,address)",
                    ("uint256", "bool", "address"),
                    (7, True, safe),
                ),
                0,
                0,
            ),
            (
                "collateral operator",
                spoke,
                calldata(
                    "setUsingAsCollateral(uint256,bool,address)",
                    ("uint256", "bool", "address"),
                    (0, True, operator),
                ),
                0,
                0,
            ),
            (
                "administration",
                spoke,
                calldata(
                    "setUserPositionManager(address,bool)", ("address", "bool"), (operator, True)
                ),
                0,
                0,
            ),
            (
                "uninspected multicall",
                spoke,
                calldata("multicall(bytes[])", ("bytes[]",), ([bytes.fromhex(allowed[2:])],)),
                0,
                0,
            ),
            ("arbitrary target", operator, "0x12345678", 0, 0),
        )
    )
    return calls


def rejected_submission(node, operator, profile, case, *, error="0xd0a9bf58"):
    name, target, inner, value, operation = case
    data = calldata(
        "execTransactionWithRole(address,uint256,bytes,uint8,bytes32,bool)",
        ("address", "uint256", "bytes", "uint8", "bytes32", "bool"),
        (
            target,
            value,
            bytes.fromhex(inner[2:]),
            operation,
            bytes.fromhex(profile.role_key[2:]),
            True,
        ),
    )
    tx = {"from": operator.address, "to": profile.roles_module, "data": data}
    with pytest.raises(RuntimeError, match=error):
        node.rpc("eth_call", [tx, "latest"])
    signed = operator.sign_transaction(
        {
            "chainId": 31337,
            "nonce": int(node.rpc("eth_getTransactionCount", [operator.address, "pending"]), 16),
            "to": to_checksum_address(profile.roles_module),
            "data": data,
            "value": 0,
            "gas": 1_000_000,
            "gasPrice": int(node.rpc("eth_gasPrice"), 16),
        }
    )
    tx_hash = node.rpc("eth_sendRawTransaction", ["0x" + signed.raw_transaction.hex()])
    receipt = rejection_receipt(node.rpc, tx_hash)
    assert receipt["status"] == "0x0", name
    return {"case": name, "call": tx, "receipt": receipt}


def rejection_receipt(rpc, tx_hash):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        receipt = rpc("eth_getTransactionReceipt", [tx_hash])
        if receipt is not None:
            return receipt
        time.sleep(0.05)
    raise TimeoutError("rejected fixture transaction has no mined receipt")


@pytest.mark.fork
def test_direct_roles_submissions_reject_parameter_and_cross_product_attacks(permission_fixture):
    node, _, operator, config, manifest, backend = permission_fixture
    profile = config.accounts.bindings[0].profile
    before = verify(config, manifest, backend)["partitions"]
    results = [
        rejected_submission(node, operator, profile, case) for case in forbidden_calls(config)
    ]
    assert verify(config, manifest, backend)["partitions"] == before
    output = ROOT / "artifacts/permission-fork"
    output.mkdir(parents=True, exist_ok=True)
    name = keccak(text=profile.model_dump_json()).hex()
    (output / f"forbidden-{name}.json").write_text(json.dumps(results, indent=2) + "\n")


@pytest.mark.fork
@pytest.mark.parametrize("change", ["avatar", "target", "operator", "default_role", "unwrapper"])
def test_installed_authority_drift_cannot_be_regenerated(
    permission_fixture, reference_strategy, change
):
    node, owner, _, config, manifest, backend = permission_fixture
    profile = config.accounts.bindings[0].profile
    signature, types, args = authority_change(profile, manifest, change)
    owner_transaction(
        node.rpc,
        owner,
        profile.fund.address,
        profile.roles_module,
        calldata(signature, types, args),
    )
    with pytest.raises(DataError):
        verify(config, manifest, backend)
    with pytest.raises(DataError):
        generate(config, reference_strategy, backend)


def authority_change(profile, manifest, change):
    role = bytes.fromhex(profile.role_key[2:])
    other = "0x" + "ab" * 20
    changes = {
        "avatar": ("setAvatar(address)", ("address",), (other,)),
        "target": ("setTarget(address)", ("address",), (other,)),
        "operator": (
            "assignRoles(address,bytes32[],bool[])",
            ("address", "bytes32[]", "bool[]"),
            (other, [role], [True]),
        ),
        "default_role": (
            "setDefaultRole(address,bytes32)",
            ("address", "bytes32"),
            (profile.operator.address, role),
        ),
        "unwrapper": (
            "setTransactionUnwrapper(address,bytes4,address)",
            ("address", "bytes4", "address"),
            (profile.fund.address, bytes.fromhex("12345678"), other),
        ),
    }
    return changes[change]


@pytest.mark.fork
def test_extra_safe_module_blocks_installed_authority(permission_fixture):
    node, owner, _, config, manifest, backend = permission_fixture
    profile = config.accounts.bindings[0].profile
    receipt = owner_transaction(
        node.rpc,
        owner,
        profile.fund.address,
        profile.fund.address,
        calldata("enableModule(address)", ("address",), (profile.operator.address,)),
    )
    assert receipt["status"] == "0x1"
    with pytest.raises(DataError, match="only its dedicated Roles"):
        verify(config, manifest, backend)
