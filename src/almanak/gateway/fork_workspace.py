"""One isolated Anvil per explicit origin, composed into one deployment workspace."""

import json
import os
import secrets
from contextlib import ExitStack
from functools import partial
from pathlib import Path

from almanak.connectors.composition import connector_for
from almanak.gateway import fork_venues
from almanak.gateway.environments import ForkBinding
from almanak.gateway.fork_config import ForkSafeFund
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.routing import ChainRouter
from almanak.models import Chain


class ForkSet:
    def __init__(self, config, env_file, *, port=None, on_ready=None):
        self.config, self.env_file, self.port = config, env_file, port
        self.nodes = {}
        self.on_ready = on_ready

    def __enter__(self):
        self.stack = ExitStack()
        try:
            for index, target in enumerate(self.config.forks):
                node = self.stack.enter_context(
                    Fork(
                        self.env_file,
                        target.block,
                        target=target,
                        port=None if self.port is None else self.port + index,
                    )
                )
                self.nodes[target.chain] = node
                if self.on_ready is not None:
                    self.on_ready(target.chain, node)
            return self
        except BaseException:
            self.stack.close()
            raise

    def __exit__(self, *_):
        self.stack.close()

    def for_chain(self, chain):
        return next(
            node
            for origin, node in self.nodes.items()
            if chain
            in (
                origin,
                Chain(chain_id=origin.chain_id, environment="anvil:" + node.metadata["instanceId"]),
            )
        )

    @property
    def rpc(self):
        if len(self.nodes) != 1:
            raise ValueError("multi-chain fixture RPC requires an explicit chain")
        return next(iter(self.nodes.values())).rpc

    def prepare(self, config, directory):
        if config != self.config:
            raise ValueError("workspace must use its resolved authoritative author configuration")
        directory.mkdir(mode=0o700, parents=True, exist_ok=False)
        accounts, keys, evidence, scopes, routes = [], {}, [], [], []
        for origin, node in self.nodes.items():
            chain = Chain(
                chain_id=origin.chain_id, environment="anvil:" + node.metadata["instanceId"]
            )
            local = select_origin(config, origin)
            bindings, signing_keys, installation = prepare_accounts(node, local, chain)
            accounts.extend(bindings)
            keys.update(signing_keys)
            evidence.extend(installation)
            scopes.append(
                ForkBinding(chain=chain, origin=node.origin, instance=node.metadata["instanceId"])
            )
            routes.append({"chain": chain.model_dump(mode="json"), "endpoint": node.rpc.endpoint})
        deployment = connector_for(config).fork.prepare_chains(config, accounts, tuple(scopes))
        # A connector's venues beside the nodes (`fork_venues`) stop with this workspace.
        host = fork_venues.LocalHost(self, directory)
        routes = fork_venues.routed(routes, fork_venues.start(deployment, host))
        write_workspace(directory, deployment, keys, evidence, routes)
        return deployment


def select_origin(config, origin):
    allocations = tuple(a for a in config.allocations if a.chain == origin)
    names = {a.account for a in allocations}
    funds = tuple(f for f in config.funds if f.account in names)
    operators = {f.operator for f in funds if isinstance(f, ForkSafeFund)}
    updates = {
        "allocations": allocations,
        "funds": funds,
        "operators": tuple(o for o in config.operators if o.name in operators),
        "forks": tuple(t for t in config.forks if t.chain == origin),
    }
    if hasattr(config, "debts"):
        updates["debts"] = tuple(d for d in config.debts if d.chain == origin)
    if hasattr(config, "inventory"):
        updates["inventory"] = tuple(b for b in config.inventory if b.key.chain == origin)
    return config.model_copy(update=updates)


def prepare_accounts(node, config, chain):
    from almanak.gateway.safe_funding import prepare_safe_accounts

    if all(isinstance(f, ForkSafeFund) for f in config.funds):
        return prepare_safe_accounts(node, config, chain)
    if any(isinstance(f, ForkSafeFund) for f in config.funds):
        raise ValueError("fixture accounts on a chain must select one explicit profile family")
    bindings, keys = node.prepare_eoa_accounts(config, chain)
    return bindings, keys, getattr(node, "funding_evidence", [])


def write_private(path, value):
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(value + "\n")


def write_workspace(directory, deployment, keys, evidence, routes):
    write_private(directory / "deployment.json", deployment.model_dump_json(indent=2))
    write_private(directory / "fork-keys.json", json.dumps(keys))
    write_private(directory / "gateway-auth.token", secrets.token_hex(32))
    write_private(directory / "fixture-installation.json", json.dumps(evidence, indent=2))
    write_private(directory / "fork-routes.json", json.dumps(routes, indent=2))


def fork_routes(config, route_file: Path, key_file: Path):
    entries = json.loads(route_file.read_text())
    factories = {}
    for entry in entries:
        # A route also names the venues its connector runs beside the node (`fork_venues`).
        venues = fork_venues.route_venues(entry)
        chain = Chain.model_validate_json(json.dumps(entry["chain"]))
        if chain in factories:
            raise ValueError("duplicate fork route")
        factories[chain] = partial(
            fork_venues.fork_adapter,
            config,
            entry["endpoint"],
            key_file,
            chain=chain,
            venues=venues,
        )
    return ChainRouter(config, factories)
