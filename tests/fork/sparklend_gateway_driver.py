"""Die after a real Spark borrow broadcast and before the loopback HTTP response."""

import argparse
import json
import os
import signal
from functools import partial
from pathlib import Path

from almanak.connectors.composition import configuration_from_json
from almanak.gateway.fork_execution import ForkGateway
from almanak.gateway.routing import ChainRouter
from almanak.gateway.service import GatewayService, serve
from almanak.models import Chain


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
    if config.connector != "sparklend" or config.production is not None:
        raise ValueError("fault fixture requires a SparkLend identified fork configuration")
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
