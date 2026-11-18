"""Generated Safe authority and isolated EOA fixtures for the shared gas policy."""

import json
import time
from types import SimpleNamespace
from uuid import uuid4

from almanak.connectors.composition import author_configuration_from_json
from almanak.connectors.uniswap_v4.models import Config
from almanak.connectors.uniswap_v4.permissions import UniswapPermissions
from almanak.gateway.credentials import write_private
from almanak.gateway.ethereum import EthereumReader
from almanak.gateway.permissions import generate, verify
from almanak.gateway.safe_fixture import install_manifest
from almanak.gateway.safe_funding import fund_usdc_storage
from almanak.strategy import StrategyDefinition
from tests.author_evidence import retained_author
from tests.fork.production_setup import policy_identity
from tests.fork.test_production import bind_mainnet_accounts, encrypt_test_keys
from tests.fork.test_transaction_replacement import BOUNDS, ROOT, STRATEGY


def author(name, deadline=300):
    source = ROOT / "strategies" / name / "strategy.py"
    raw = json.loads(retained_author(source, "config.json").read_text())
    raw["operators"][0]["foundry_account_index"] = int(uuid4().hex[:7], 16)
    suffix = uuid4().hex
    raw["funds"][0]["account"] += suffix
    raw["allocations"][0]["account"] += suffix
    raw["allocations"][0]["collect_seconds"] = 1
    raw["allocations"][0]["deadline_seconds"] = deadline
    return source, author_configuration_from_json(json.dumps(raw))


def setup(node, directory, name, profile, deadline=300):
    node.rpc("anvil_setChainId", [31337])
    node.rpc("evm_setAutomine", [True])
    source, original = author(name, deadline)
    original = original.resolved(node.origin)
    deployed = node.prepare(original, directory)
    raw = deployed.model_dump(mode="json", exclude={"fork_instance", "fork_origin"})
    raw["allocations"] = [a.model_dump(mode="json") for a in original.allocations]
    raw["production"] = json.loads(
        retained_author(STRATEGY, "production.example.json").read_text()
    )["production"]
    raw["production"]["gas"][0]["accounts"] = [raw["accounts"]["bindings"][0]["name"]]
    raw["production"]["gas"][0]["replacement"] = BOUNDS
    raw["production"]["gas"][0]["max_gas_limit"] = 2_000_000
    bind_mainnet_accounts(raw, node.origin.chain)
    if profile == "eoa":
        bind_eoa(node, raw)
    config = Config.model_validate_json(json.dumps(raw))
    write_private(directory / "production.json", config.model_dump_json(indent=2))
    encrypt_test_keys(directory)
    node.rpc("anvil_setChainId", [1])
    latest = int(node.rpc("eth_getBlockByNumber", ["latest", False])["timestamp"], 16)
    time.sleep(max(0, latest + 1 - time.time()))
    node.rpc("evm_setNextBlockTimestamp", [int(time.time())])
    node.rpc("evm_mine")
    if profile == "safe":
        install(node, config, source, directory)
    write_private(directory / "identity.json", json.dumps(node.rpc("anvil_metadata")))
    return config, source


def bind_eoa(node, raw):
    binding = raw["accounts"]["bindings"][0]
    binding["profile"] = {"kind": "eoa", "fund": binding["profile"]["operator"]}
    allocation = raw["allocations"][0]
    fund_usdc_storage(
        node,
        binding["profile"]["fund"]["address"],
        allocation["pool"]["currency0"].split(":")[-1],
        allocation["capital"]["amount"],
    )


def install(node, config, source, directory):
    chain = config.accounts.bindings[0].profile.fund.chain
    with EthereumReader(chain, node.rpc.endpoint) as reader:
        backend = SimpleNamespace(reader=reader, guard=lambda: policy_identity(node))
        manifest = generate(
            config, StrategyDefinition.read(source), backend, connector=UniswapPermissions()
        )
        node.rpc("anvil_setChainId", [31337])
        receipts = install_manifest(node, manifest)
        node.rpc("anvil_setChainId", [1])
        verified = verify(config, manifest, backend, connector=UniswapPermissions())
    (directory / "permission-manifest.json").write_text(manifest.model_dump_json(indent=2))
    (directory / "permission-installation.json").write_text(json.dumps(receipts, indent=2))
    (directory / "permission-verification.json").write_text(json.dumps(verified, indent=2))
