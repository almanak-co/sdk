"""Isolated funding and owner installation for mainnet-policy lifecycle rehearsals."""

import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.connectors.aave_v4.permission_adapter import generate, verify
from almanak.gateway.ethereum import EthereumReader
from almanak.gateway.safe_fixture import install_manifest
from almanak.gateway.safe_funding import fund_usdc_storage
from almanak.models import SafeRoles
from almanak.strategy import StrategyDefinition
from tests.author_evidence import retained_author
from tests.fork.eoa import configuration

STRATEGY = Path(__file__).parents[2] / "strategies/aave_v4_weth_supply/strategy.py"


def retained_config():
    return json.loads(retained_author(STRATEGY, "config.json").read_text())


def rehearsal_config(profile):
    author = retained_config()
    if profile == "safe_borrow":
        raw = author
    else:
        raw = configuration()
        if profile == "eoa_borrow":
            raw["debts"] = author["debts"][:1]
    return unique_accounts(raw)


def unique_accounts(raw):
    # Mainnet-policy claims must never collide with another rehearsal's retained evidence.
    suffix = uuid4().hex
    for item in (*raw["funds"], *raw["allocations"], *raw["debts"]):
        item["account"] += "_" + suffix
    for fund in raw["funds"]:
        if "foundry_account_index" in fund:
            fund["foundry_account_index"] = int(uuid4().hex[:7], 16)
    for operator in raw["operators"]:
        operator["foundry_account_index"] = int(uuid4().hex[:7], 16)
    return ForkConfig.model_validate_json(json.dumps(raw))


def provision_authority(node, config, directory):
    if isinstance(config.accounts.bindings[0].profile, SafeRoles):
        install_safe_authority(node, config, directory)
    else:
        for debt in config.debts:
            fund = config.accounts.binding(debt.account).profile.fund.address
            fund_usdc_storage(node, fund, debt.reserve.asset.reference, loan_asset_funding(debt))


def loan_asset_funding(debt):
    """The loan asset the reference's own Safe fork fixture gives this debt's fund.

    Funding requirements cover the entry only. Like an operator top-up, this
    balance pays the interest that accrues before the full repay.
    """
    author = retained_config()
    account = next(item["account"] for item in author["debts"] if item["name"] == debt.name)
    fund = next(item for item in author["funds"] if item["account"] == account)
    asset = str(debt.reserve.asset)
    return next(
        item["balance"]["amount"] for item in fund["assets"] if item["balance"]["asset"] == asset
    )


def install_safe_authority(node, config, directory):
    chain = config.accounts.bindings[0].profile.fund.chain
    node._validate_origin()
    node.rpc("anvil_setChainId", [1])
    with EthereumReader(chain, node.rpc.endpoint) as reader:
        backend = SimpleNamespace(reader=reader, guard=lambda: policy_identity(node))
        manifest = generate(config, StrategyDefinition.read(STRATEGY), backend)
        # Fixture owner transactions use Foundry's chain; the manifest keeps its tested CAIP scope.
        node.rpc("anvil_setChainId", [31337])
        receipts = install_manifest(node, manifest)
        node.rpc("anvil_setChainId", [1])
        verification = verify(config, manifest, backend)
    (directory / "permission-manifest.json").write_text(manifest.model_dump_json(indent=2))
    (directory / "permission-installation.json").write_text(json.dumps(receipts, indent=2))
    (directory / "permission-verification.json").write_text(json.dumps(verification, indent=2))


def policy_identity(node):
    actual = node.rpc("anvil_metadata")
    assert actual["chainId"] == 1
    assert actual["instanceId"] == node.metadata["instanceId"]
    assert actual["forkedNetwork"] == node.metadata["forkedNetwork"]
