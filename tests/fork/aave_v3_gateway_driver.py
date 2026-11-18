"""Die after a real Aave borrow broadcast and before the loopback HTTP response."""

import argparse
import json
import os
import signal
from functools import partial
from pathlib import Path

from almanak.connectors.aave_v4.models import LendingConfig as AaveV4Config
from almanak.connectors.composed.models import Config as ComposedConfig
from almanak.connectors.composition import configuration_from_json
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.target_routing import target_connector
from almanak.gateway.fork_execution import ForkGateway
from almanak.gateway.routing import ChainRouter
from almanak.gateway.service import GatewayService, serve
from almanak.models import Chain

SUPPORTED = frozenset((ConnectorId("aave_v3"), ConnectorId("aave_v4")))


class InterruptedService(GatewayService):
    def dispatch(self, method, request):
        result = super().dispatch(method, request)
        if method == "broadcast":
            pending = self.journal.load().pending
            if (
                pending is not None
                and pending.operation.kind == "borrow"
                and target_connector(pending.operation.target) in SUPPORTED
            ):
                os.kill(os.getpid(), signal.SIGKILL)
        return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    root = args.directory
    config = configuration_from_json((root / "deployment.json").read_text())
    validate_fault_config(config)
    factories = fork_factories(root, config)
    serve(
        InterruptedService(
            config, root / "state.sqlite", ChainRouter(config, factories), lease_seconds=30
        ),
        root / "gateway-auth.token",
        args.port,
    )


def validate_fault_config(config):
    if not has_aave_component(config) or config.production is not None or not config.fork_bindings:
        raise ValueError("fault fixture requires an Aave identified fork configuration")


def has_aave_component(config):
    connector = getattr(config, "connector", None)
    if isinstance(connector, str):
        # Standalone lending configs retain the connector as a plain wire string.
        connector = ConnectorId(connector)
    return (
        isinstance(config, AaveV4Config)
        or connector in SUPPORTED
        or (
            isinstance(config, ComposedConfig)
            and bool(SUPPORTED.intersection(config.execution_components))
        )
    )


def fork_factories(root, config):
    factories = {}
    for entry in json.loads((root / "fork-routes.json").read_text()):
        chain = Chain.model_validate_json(json.dumps(entry["chain"]))
        factories[chain] = partial(
            ForkGateway, config, entry["endpoint"], root / "fork-keys.json", chain=chain
        )
    return factories


if __name__ == "__main__":
    main()
