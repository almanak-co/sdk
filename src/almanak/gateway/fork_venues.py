"""Connector venues a fork workspace runs beside its fork nodes.

Anvil forks one chain's EVM state. A connector whose venue lives beside that chain, as
Hyperliquid's HyperCore lives beside HyperEVM, has the workspace run a synthesized venue
process next to the fork node (blueprint 30, section 9.1). The connector's fork binding
names it with two optional methods:

- `venues(deployment, host)` starts each venue through `host`, which owns its process,
  and returns per fork chain the descriptors that chain's fork route carries under
  `venues`, `{name: descriptor}`;
- `fork_adapter(config, endpoint, key_file, chain=..., venues=...)` builds the chain's
  gateway adapter from those descriptors.

A connector without them has `ForkGateway` as its adapter, and its routes name no venue.
A venue process is `almanak service fork-venue --spec <file>`: the spec names its
connector, whose fork binding serves it (`serve_venue`) on the loopback socket the host
binds and passes down, so the route's endpoint is reserved before the process starts.
"""

import json
import subprocess
import sys
import time
from pathlib import Path

import httpx

from almanak.models import Chain

ROUTE = frozenset({"chain", "endpoint"})
VENUES = "venues"
# A venue opens its retained state and checks its fork before it serves.
READY_SECONDS = 60


def fork_binding(config):
    from almanak.connectors.composition import connector_for

    return connector_for(config).fork


def route_venues(entry) -> dict:
    """A fork route's venue descriptors; empty for a chain with none."""
    if not isinstance(entry, dict) or set(entry) - {VENUES} != ROUTE:
        raise ValueError("fork routes require only their explicit chain and endpoint")
    venues = entry.get(VENUES, {})
    if not isinstance(venues, dict) or (VENUES in entry and not venues):
        raise ValueError("a fork route's venues name each venue's descriptor")
    return venues


def chain_route(entry) -> dict:
    """The chain and endpoint a fork route names, without its venues."""
    route_venues(entry)
    return {key: entry[key] for key in sorted(ROUTE)}


def fork_adapter(config, endpoint, key_file, *, chain=None, venues=None):
    """The fork gateway adapter of one chain route of the deployment `config`."""
    from almanak.gateway.fork_execution import ForkGateway

    build = getattr(fork_binding(config), "fork_adapter", None)
    if build is None:
        if venues:
            raise ValueError("only a connector with a fork venue names one in its route")
        return ForkGateway(config, endpoint, key_file, chain=chain)
    return build(config, endpoint, key_file, chain=chain, venues=venues or {})


def start(deployment, host) -> dict[Chain, dict]:
    """Each fork chain's venue descriptors once the deployment's venues serve."""
    venues = getattr(fork_binding(deployment), "venues", None)
    return {} if venues is None else venues(deployment, host)


def route_chain(entry) -> Chain:
    return Chain.model_validate_json(json.dumps(entry["chain"]))


def routed(routes: list[dict], venues: dict[Chain, dict]) -> list[dict]:
    """`routes` with each chain's venue descriptors, every other field unchanged."""
    result = []
    for entry in routes:
        route = chain_route(entry)
        chain = route_chain(route)
        result.append({**route, VENUES: venues[chain]} if chain in venues else route)
    if set(venues) - {route_chain(r) for r in result}:
        raise ValueError("a fork venue names a chain the workspace has no route for")
    return result


def arguments(spec: Path, listener) -> list[str]:
    return ["service", "fork-venue", "--spec", str(spec), "--listen-fd", str(listener.fileno())]


def serve(spec_file: Path, listen) -> int:
    """Run the venue process `spec_file` names, through its connector's fork binding."""
    from almanak.connectors.composition import connector_from_wire

    spec = json.loads(spec_file.read_text())
    serve_venue = getattr(
        connector_from_wire({"connector": spec["connector"]}).fork, "serve_venue", None
    )
    if serve_venue is None:
        raise ValueError("the connector runs no fork venue")
    return serve_venue(spec, listen)


def wait_ready(endpoint: str, alive) -> None:
    deadline = time.monotonic() + READY_SECONDS
    while time.monotonic() < deadline:
        try:
            if httpx.get(endpoint + "/ready", timeout=2).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        if not alive():
            raise RuntimeError(f"fork venue at {endpoint} exited during start; inspect its log")
        time.sleep(0.05)
    raise RuntimeError(f"fork venue at {endpoint} did not answer within {READY_SECONDS} seconds")


class LocalHost:
    """Venues of a workspace a test or tool owns (`ForkSet.prepare`): each process logs
    beside its state and stops when the workspace's nodes stop."""

    def __init__(self, nodes, directory: Path):
        self.nodes, self.directory = nodes, directory

    def node(self, chain):
        return self.nodes.for_chain(chain)

    def launch(self, name: str, spec: Path) -> str:
        from almanak.supervisor_process import loopback_listener

        with loopback_listener() as listener, (self.directory / f"{name}.log").open("a") as log:
            port = listener.getsockname()[1]
            process = subprocess.Popen(
                [sys.executable, "-m", "almanak.cli", *arguments(spec, listener)],
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
                start_new_session=True,
                cwd="/",
                pass_fds=(listener.fileno(),),
            )
        self.nodes.stack.callback(stop, process)
        endpoint = f"http://127.0.0.1:{port}"
        wait_ready(endpoint, lambda: process.poll() is None)
        return endpoint


def stop(process) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
