"""Connector keepers settle a fork test's asynchronous requests while the lifecycle waits.

Some protocols execute an accepted request only when their own keeper acts, and a fork
runs none. A connector whose request binding offers a fork keeper settles each accepted
request once, and one whose external effects offer one settles what it left open, such
as a bridge transfer; this module gives it each chain's fork RPC, origin block and origin-chain
reader, and the venue processes the workspace runs beside its nodes, keeps the
coordinator clock with the fork time the keeper moved, and retains what each settlement
used.
"""

import json
from contextlib import ExitStack
from pathlib import Path
from typing import Any


class Forks:
    """The run's fork nodes, their origin blocks and cached origin-chain readers."""

    def __init__(self, root: Path, env_file: Path):
        from almanak.workspace import read

        self.processes = read(root)["processes"]
        self.env_file = env_file
        self.readers: dict = {}
        self.stack = ExitStack()

    def entry(self, chain) -> dict:
        try:
            return self.processes[f"fork-{chain}"]
        except KeyError:
            raise ValueError(f"the fork run has no node for {chain}") from None

    def venue(self, name: str) -> dict:
        """The record of a venue process the workspace runs beside its nodes, with its
        loopback `endpoint` (`gateway/fork_venues.py`)."""
        try:
            return self.processes[f"venue-{name}"]
        except KeyError:
            raise ValueError(f"the fork run has no {name} venue") from None

    def rpc(self, chain):
        from almanak.gateway.fork_fixture import Rpc

        return Rpc(self.entry(chain)["endpoint"])

    def origin(self, chain):
        from almanak.models import Block

        return Block.model_validate_json(json.dumps(self.entry(chain)["origin"]))

    def reader(self, chain):
        from almanak.gateway.read_endpoints import mainnet_reader, read_endpoint

        if chain not in self.readers:
            endpoint = read_endpoint(chain, self.env_file)
            self.readers[chain] = self.stack.enter_context(
                mainnet_reader(chain, endpoint, observation=True)
            )
        return self.readers[chain]

    def close(self) -> None:
        self.stack.close()


class Keepers:
    """Settles the accepted requests a checkpoint holds, through its connector's keeper."""

    def __init__(self, root: Path, env_file: Path):
        self.root = root
        self.forks = Forks(root, env_file)
        self.keeper: Any = None
        self.rows: list[dict] = []
        self.failures: list[str] = []
        self.last_decoded: tuple[dict, Any] | None = None

    def binding(self, wire):
        """The connector's request binding when it offers a fork keeper, with the decoded
        checkpoint; None otherwise, read from the configuration without decoding it."""
        from almanak.connectors.composition import connector_from_wire

        if wire is None:
            return None, None
        connector = connector_from_wire(wire["config"])
        offered = (connector.requests, getattr(connector, "external_effects", None))
        binding = next((b for b in offered if hasattr(b, "fork_keeper")), None)
        if binding is None:
            return None, None
        if self.keeper is None:
            self.keeper = binding.fork_keeper(self.forks, self.root / "keepers")
        return binding, self.decoded(wire)

    def decoded(self, wire):
        """The checkpoint `wire` decodes to; polling an unchanged one decodes it once."""
        from almanak.testflow import decoded

        if self.last_decoded is None or self.last_decoded[0] != wire:
            self.last_decoded = (wire, decoded(wire))
        return self.last_decoded[1]

    def step(self, wire, seconds: int) -> int:
        """Fork seconds to advance, at least `seconds`, aligned by the connector's keeper
        so the next decision can find the reports it would need."""
        from almanak.gateway.ethereum import DataError

        binding, state = self.binding(wire)
        if binding is None:
            return seconds
        try:
            return max(seconds, self.keeper.align(state, seconds))
        except (DataError, ValueError, RuntimeError) as exc:
            self.failures.append(f"fork keeper could not align fork time: {exc}")
            return seconds

    def settle(self, wire) -> None:
        from almanak.testflow import shift_clock

        binding, state = self.binding(wire)
        # A composed deployment also owes the settlement of its transfers in flight.
        if binding is None:
            return
        owed = getattr(type(binding), "fork_unsettled", None) or binding.unresolved
        if not owed(state):
            return
        for row in self.keeper.settle(state):
            self.rows.append(row)
            shift_clock(self.root, row["advanced_seconds"])

    def close(self) -> None:
        self.forks.close()
