"""Die after a real Morpho borrow broadcast and before the loopback HTTP response."""

import argparse
import json
import os
import signal
from functools import partial
from pathlib import Path

from almanak.connectors.composed.models import Config as ComposedConfig
from almanak.connectors.composition import configuration_from_json
from almanak.connectors.identifiers import ConnectorId
from almanak.gateway.fork_execution import ForkGateway
from almanak.gateway.routing import ChainRouter
from almanak.gateway.service import GatewayService, serve
from almanak.models import Chain

MORPHO = ConnectorId("morpho_blue")


def has_morpho_component(config):
    # A direct Morpho config stores its connector as the plain literal, and identifier
    # equality is type-strict, so compare the normalized identifier.
    connector = getattr(config, "connector", None)
    return (connector is not None and ConnectorId(str(connector)) == MORPHO) or (
        isinstance(config, ComposedConfig) and MORPHO in config.execution_components
    )


class InterruptedService(GatewayService):
    def dispatch(self, method, request):
        result = super().dispatch(method, request)
        if method == "broadcast":
            pending = self.journal.load().pending
            if pending is not None and pending.operation.kind == "borrow":
                os.kill(os.getpid(), signal.SIGKILL)
        return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    root = args.directory
    config = configuration_from_json((root / "deployment.json").read_text())
    if not has_morpho_component(config) or config.production is not None:
        raise ValueError("fault fixture requires a Morpho identified fork configuration")
    factories = {}
    for entry in json.loads((root / "fork-routes.json").read_text()):
        chain = Chain.model_validate_json(json.dumps(entry["chain"]))
        factories[chain] = partial(
            ForkGateway, config, entry["endpoint"], root / "fork-keys.json", chain=chain
        )
    serve(
        InterruptedService(
            config, root / "state.sqlite", ChainRouter(config, factories), lease_seconds=30
        ),
        root / "gateway-auth.token",
        args.port,
    )


if __name__ == "__main__":
    main()
