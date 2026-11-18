"""Retain actual fixture accounts before constructing final author parameters."""

import fcntl
import hashlib
import json
import os
from pathlib import Path
from typing import Literal

import httpx
from eth_account import Account as Signer
from pydantic import model_validator

from almanak.gateway.environments import ForkBinding
from almanak.gateway.ethereum import DataError
from almanak.gateway.fixture_preparation import FixturePlan
from almanak.gateway.fork_config import ForkEoaFund, ForkOperator, ForkSafeFund
from almanak.gateway.fork_execution import validate_endpoint
from almanak.gateway.fork_fixture import Rpc, fixture_signer
from almanak.gateway.fork_venues import chain_route
from almanak.gateway.fork_workspace import prepare_accounts
from almanak.gateway.operator_config import Digest, content_digest
from almanak.models import Chain, Eoa, Model, SafeRoles, StrategyAccounts
from almanak.workspace import save


class FixtureFunding(Model):
    funds: tuple[ForkEoaFund | ForkSafeFund, ...]
    operators: tuple[ForkOperator, ...]


def fund_chain(plan, fund):
    role = "fund" if isinstance(fund, ForkEoaFund) else "owner"
    return next(key.chain for key in plan.keys if (key.role, key.name) == (role, fund.account))


class FixtureAccounts(Model):
    schema_version: Literal["anvil-fixture-accounts-v1"] = "anvil-fixture-accounts-v1"
    authority: Literal["unadmitted"] = "unadmitted"
    plan: FixturePlan
    accounts: StrategyAccounts
    bindings: tuple[ForkBinding, ...]
    installation_digest: Digest

    @model_validator(mode="after")
    def scope(self):
        origins = {binding.origin for binding in self.bindings}
        if len(origins) != len(self.bindings) or origins != {f.block for f in self.plan.forks}:
            raise ValueError("fixture account bindings differ from the retained origins")
        if any(b.chain.chain_id != b.origin.chain.chain_id for b in self.bindings):
            raise ValueError("fixture bindings must preserve origin CAIP identity")
        if {a.name for a in self.accounts.bindings} != {f.account for f in self.plan.funds}:
            raise ValueError("fixture bindings must cover exactly the requested funds")
        for fund in self.plan.funds:
            self.require_fund(fund)
        return self

    def require_fund(self, fund):
        profile = self.accounts.binding(fund.account).profile
        origin = fund_chain(self.plan, fund)
        chain = next(b.chain for b in self.bindings if b.origin.chain.chain_id == origin)
        if profile.fund.chain != chain:
            raise ValueError("generated account differs from its exact fork environment")
        if isinstance(fund, ForkEoaFund):
            if not isinstance(profile, Eoa):
                raise ValueError("fixture changed the requested EOA profile")
            index = fund.foundry_account_index
        else:
            if not isinstance(profile, SafeRoles):
                raise ValueError("fixture changed the requested Safe/Roles profile")
            index = next(
                o.foundry_account_index for o in self.plan.operators if o.name == fund.operator
            )
        if profile.submitter.address != fixture_signer(index).address.lower():
            raise ValueError("fixture submitter differs from its deterministic operator/fund key")

    @property
    def digest(self):
        return content_digest(self)

    def require_resolution(self, progress):
        from almanak.connectors.support import RunMode

        context = progress.context
        if progress.result is None or context.mode != RunMode.FORK:
            raise ValueError("fixture accounts require complete fork resolution")
        if (self.plan.context_digest, self.plan.operator_digest, self.plan.anchors) != (
            context.digest,
            context.operator_digest,
            progress.result.anchors,
        ):
            raise ValueError("fixture accounts differ from the retained resolution context")
        if {(a.name, a.chain) for a in context.accounts} != {
            (f.account, fund_chain(self.plan, f)) for f in self.plan.funds
        }:
            raise ValueError("fixture accounts differ from author account requirements")


def node_bindings(plan, nodes):
    if nodes.config != plan or set(nodes.nodes) != {f.chain for f in plan.forks}:
        raise ValueError("fixture nodes must belong to the exact retained plan")
    result = []
    for target in plan.forks:
        node = nodes.nodes[target.chain]
        validate_endpoint(node.rpc.endpoint)
        node._validate_origin()
        if node.origin != target.block:
            raise ValueError("fixture node differs from its retained origin")
        result.append(
            ForkBinding(
                chain=Chain(
                    chain_id=target.chain.chain_id,
                    environment="anvil:" + node.metadata["instanceId"],
                ),
                origin=node.origin,
                instance=node.metadata["instanceId"],
            )
        )
    return tuple(result)


def publish(path, value):
    save(path, value)
    sync_directory(path.parent)


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def provision_accounts(directory: Path, plan, context, operator, nodes) -> FixtureAccounts:
    """A lost funding outcome requires reconciliation; it cannot repeat setup."""
    plan.require(context, operator, plan.anchors)
    bindings = node_bindings(plan, nodes)
    with directory.with_name(directory.name + ".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError("fixture account provisioning already has an active owner") from None
        if directory.exists():
            record = retained_accounts(directory, plan, bindings, nodes)
        else:
            directory.mkdir(mode=0o700)
            sync_directory(directory.parent)
            publish(
                directory / "fixture-started.json",
                {
                    "plan": plan.model_dump(mode="json"),
                    "bindings": [b.model_dump(mode="json") for b in bindings],
                },
            )
            record = create_accounts(directory, plan, bindings, nodes)
        restore_owners(record, nodes)
        return record


def create_accounts(directory, plan, bindings, nodes):
    accounts, keys, evidence, routes = [], {}, [], []
    for binding in bindings:
        node = nodes.nodes[binding.origin.chain]
        local = FixtureFunding(
            funds=tuple(f for f in plan.funds if fund_chain(plan, f) == binding.chain.chain_id),
            operators=tuple(
                o for o in plan.operators if o.native_balance.asset.chain == binding.chain.chain_id
            ),
        )
        funds, signers, installation = prepare_accounts(node, local, binding.chain)
        accounts.extend(funds)
        keys.update(signers)
        evidence.extend(installation)
        publish(directory / "fixture-installation.json", evidence)
        routes.append(
            {"chain": binding.chain.model_dump(mode="json"), "endpoint": node.rpc.endpoint}
        )
    record = FixtureAccounts(
        plan=plan,
        accounts=StrategyAccounts(bindings=tuple(accounts)),
        bindings=bindings,
        installation_digest=installation_digest(evidence),
    )
    require_keys(record, keys)
    if node_bindings(plan, nodes) != bindings:
        raise ValueError("fixture identity changed during account provisioning")
    publish(directory / "fork-keys.json", keys)
    publish(directory / "fork-routes.json", routes)
    publish(directory / "fixture-accounts.json", record.model_dump(mode="json"))
    return record


def retained_accounts(directory, plan, bindings, nodes):
    path = directory / "fixture-accounts.json"
    if not path.is_file():
        raise RuntimeError(
            "fixture funding outcome is incomplete; automatic reprovisioning is refused"
        )
    record = FixtureAccounts.model_validate_json(path.read_text())
    expected = {
        "plan": plan.model_dump(mode="json"),
        "bindings": [b.model_dump(mode="json") for b in bindings],
    }
    if (
        record.plan != plan
        or record.bindings != bindings
        or json.loads((directory / "fixture-started.json").read_text()) != expected
    ):
        raise ValueError("retained fixture accounts differ from their original setup")
    evidence = json.loads((directory / "fixture-installation.json").read_text())
    if installation_digest(evidence) != record.installation_digest:
        raise ValueError("fixture installation evidence differs from its retained digest")
    require_keys(record, json.loads((directory / "fork-keys.json").read_text()))
    routes = [
        {
            "chain": b.chain.model_dump(mode="json"),
            "endpoint": nodes.nodes[b.origin.chain].rpc.endpoint,
        }
        for b in bindings
    ]
    # The venues a connector runs beside the nodes join the routes after provisioning.
    retained = json.loads((directory / "fork-routes.json").read_text())
    if [chain_route(entry) for entry in retained] != routes:
        raise ValueError("fixture routes differ from their original node bindings")
    return record


def installation_digest(evidence):
    if not isinstance(evidence, list) or not all(isinstance(row, dict) for row in evidence):
        raise ValueError("fixture installation evidence requires retained receipt/funding rows")
    return hashlib.sha256(
        json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def require_keys(record, keys):
    if not isinstance(keys, dict) or set(keys) != {a.name for a in record.accounts.bindings}:
        raise ValueError("fixture keys must cover exactly the generated accounts")
    for binding in record.accounts.bindings:
        try:
            address = Signer.from_key(keys[binding.name]).address.lower()
        except (ValueError, TypeError):
            raise ValueError("fixture key is invalid") from None
        if address != binding.profile.submitter.address:
            raise ValueError("fixture key differs from its retained submitter")


def restore_owners(record, nodes):
    for node in nodes.nodes.values():
        node.fixture_owners = {
            key.name: fixture_signer(key.foundry_account_index)
            for key in record.plan.keys
            if key.role == "owner" and key.chain == node.origin.chain.chain_id
        }


def require_fixture_nodes(fixture: FixtureAccounts, route_file: Path) -> None:
    """Confirm every retained node is the identified Anvil instance without opening keys."""
    routes = {}
    for entry in json.loads(route_file.read_text()):
        chain_route(entry)
        chain = Chain.model_validate_json(json.dumps(entry["chain"]))
        if chain in routes:
            raise ValueError("duplicate fork route")
        routes[chain] = entry["endpoint"]
    if set(routes) != {binding.chain for binding in fixture.bindings}:
        raise ValueError("fork routes differ from the retained fixture bindings")
    for binding in fixture.bindings:
        validate_endpoint(routes[binding.chain])
        try:
            metadata = Rpc(routes[binding.chain])("anvil_metadata")
        except (httpx.HTTPError, RuntimeError):
            raise DataError("fork_unavailable", "retained fork node is unavailable") from None
        network = metadata.get("forkedNetwork") if isinstance(metadata, dict) else None
        if (
            not isinstance(network, dict)
            or not str(metadata.get("clientVersion", "")).startswith("anvil/")
            or metadata.get("chainId") != 31337
            or metadata.get("instanceId") != binding.instance
            or network.get("forkBlockNumber") != binding.origin.number
            or network.get("forkBlockHash") != binding.origin.hash
        ):
            raise DataError("fork_mismatch", "fork instance or origin changed; execution refused")
