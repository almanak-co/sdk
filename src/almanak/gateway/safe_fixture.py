"""Safe owner operations confined to the identified Anvil fixture."""

from eth_abi import encode
from eth_utils import keccak, to_checksum_address

from almanak.models import ZERO_ADDRESS, Account, SafeRoles

SAFE_SINGLETON = "0x41675c099f32341bf84bfc5382af534df5c7461a"
SAFE_L2_SINGLETON = "0x29fcb43b46531bca003ddc8fcb67ffe91900c762"
SAFE_FACTORY = "0x4e1dcf7ad4e460cfd30791ccc4f9c8a4f820ec67"
ROLES_SINGLETON = "0x9646fdad06d3e24444381f44362a3b0eb343d337"
MODULE_FACTORY = "0x000000000000addb49795b0f9ba5bc298cdda236"


def calldata(signature: str, types, args) -> str:
    return "0x" + (keccak(text=signature)[:4] + encode(types, args)).hex()


def created_address(receipt, signature):
    topic = "0x" + keccak(text=signature).hex()
    matches = [log for log in receipt["logs"] if log["topics"][0] == topic]
    if len(matches) != 1:
        raise ValueError("fixture deployment did not emit exactly one creation event")
    return "0x" + matches[0]["topics"][1][-40:]


def owner_transaction(rpc, owner, safe, to, data, value=0):
    if rpc("anvil_metadata")["chainId"] != 31337:
        raise ValueError("owner submission is only supported in an identified Anvil fixture")
    signature = bytes.fromhex(owner.address[2:].rjust(64, "0")) + bytes(32) + b"\x01"
    encoded = calldata(
        "execTransaction(address,uint256,bytes,uint8,uint256,uint256,uint256,address,address,bytes)",
        (
            "address",
            "uint256",
            "bytes",
            "uint8",
            "uint256",
            "uint256",
            "uint256",
            "address",
            "address",
            "bytes",
        ),
        (to, value, bytes.fromhex(data[2:]), 0, 0, 0, 0, ZERO_ADDRESS, ZERO_ADDRESS, signature),
    )
    receipt = rpc.send(owner, to_checksum_address(safe), encoded)
    topic = "0x" + keccak(text="ExecutionSuccess(bytes32,uint256)").hex()
    if not any(
        log["address"].lower() == safe and log["topics"][0] == topic for log in receipt["logs"]
    ):
        raise ValueError("Safe owner transaction did not prove inner success")
    return receipt


def deploy_safe(rpc, owner, salt):
    setup = calldata(
        "setup(address[],uint256,address,bytes,address,address,uint256,address)",
        ("address[]", "uint256", "address", "bytes", "address", "address", "uint256", "address"),
        ([owner.address], 1, ZERO_ADDRESS, b"", ZERO_ADDRESS, ZERO_ADDRESS, 0, ZERO_ADDRESS),
    )
    receipt = rpc.send(
        owner,
        to_checksum_address(SAFE_FACTORY),
        calldata(
            "createProxyWithNonce(address,bytes,uint256)",
            ("address", "bytes", "uint256"),
            (SAFE_SINGLETON, bytes.fromhex(setup[2:]), salt),
        ),
    )
    return created_address(receipt, "ProxyCreation(address,address)"), receipt


def deploy_roles(rpc, owner, safe, salt):
    setup = calldata(
        "setUp(bytes)", ("bytes",), (encode(("address", "address", "address"), (safe, safe, safe)),)
    )
    receipt = rpc.send(
        owner,
        to_checksum_address(MODULE_FACTORY),
        calldata(
            "deployModule(address,bytes,uint256)",
            ("address", "bytes", "uint256"),
            (ROLES_SINGLETON, bytes.fromhex(setup[2:]), salt),
        ),
    )
    return created_address(receipt, "ModuleProxyCreation(address,address)"), receipt


def deploy_profile(rpc, chain, owner, operator, name):
    if owner.address == operator.address:
        raise ValueError("fixture owner must be distinct from the operator")
    salt = int.from_bytes(keccak(text="almanak-v3:" + owner.address + name), "big")
    safe, safe_receipt = deploy_safe(rpc, owner, salt)
    roles, roles_receipt = deploy_roles(rpc, owner, safe, salt)
    enabled = owner_transaction(
        rpc, owner, safe, safe, calldata("enableModule(address)", ("address",), (roles,))
    )
    return SafeRoles(
        kind="safe_roles",
        fund=Account(chain=chain, address=safe),
        operator=Account(chain=chain, address=operator.address),
        roles_module=roles,
        role_key="0x" + keccak(text="almanak-v3:" + name).hex(),
        safe_deployment_block=int(safe_receipt["blockNumber"], 16),
        roles_deployment_block=int(roles_receipt["blockNumber"], 16),
    ), (safe_receipt, roles_receipt, enabled)


def install_manifest(node, manifest):
    receipts = []
    for partition in manifest.partitions:
        selected = (
            node.for_chain(partition.profile.fund.chain) if hasattr(node, "for_chain") else node
        )
        owner = selected.fixture_owners[partition.account]
        for call in partition.transactions:
            receipts.append(
                owner_transaction(
                    selected.rpc, owner, partition.profile.fund.address, call.to, call.data
                )
            )
    return receipts


def prepare_permissions(node, deployment, source, output):
    import json
    from types import SimpleNamespace

    from almanak.connectors.composition import connector_for
    from almanak.gateway.ethereum import EthereumReader
    from almanak.gateway.permissions import generate, verify, write_artifacts
    from almanak.strategy import StrategyDefinition

    node._validate_origin()
    chain = deployment.accounts.bindings[0].profile.fund.chain
    with EthereumReader(chain, node.rpc.endpoint) as reader:
        backend = SimpleNamespace(reader=reader, guard=node._validate_origin)
        manifest = generate(
            deployment,
            StrategyDefinition.read(source),
            backend,
            connector=connector_for(deployment).permissions,
        )
        write_artifacts(manifest, output / "permissions")
        receipts = install_manifest(node, manifest)
        result = verify(
            deployment, manifest, backend, connector=connector_for(deployment).permissions
        )
    (output / "permission-installation.json").write_text(json.dumps(receipts, indent=2) + "\n")
    (output / "permission-verification.json").write_text(json.dumps(result, indent=2) + "\n")
