"""Process-level chain outage and lost receipt responses on identified forks only."""

import argparse
import json
from functools import partial
from pathlib import Path

from almanak.configuration.admission import PreparedDeployment
from almanak.gateway.ethereum import DataError
from almanak.gateway.fork_execution import ForkGateway
from almanak.gateway.operator_config import read_operator_config
from almanak.gateway.routing import ChainRouter
from almanak.gateway.service import GatewayService, serve
from almanak.models import Chain


class InterruptedFork(ForkGateway):
    def __init__(self, *args, faults, **kwargs):
        self.faults = faults
        super().__init__(*args, **kwargs)

    def marker(self, kind):
        return self.faults / (self.reader.chain.chain_id.reference + "-" + kind)

    def guard(self):
        if self.marker("down").exists():
            raise DataError("transport", "test fixture chain unavailable")
        return super().guard()

    def receipt(self, pending):
        result = super().receipt(pending)
        if (
            result is not None
            and pending.operation.kind == "borrow"
            and self.marker("receipt").exists()
        ):
            self.marker("mined").write_text(pending.tx_hash)
            return None
        return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    root = args.directory
    preparation = PreparedDeployment.model_validate_json(
        (root / "prepared-deployment.json").read_text()
    )
    operator = read_operator_config(root / "operator.json")
    config = preparation.configuration()
    factories = {}
    for entry in json.loads((root / "fork-routes.json").read_text()):
        chain = Chain.model_validate_json(json.dumps(entry["chain"]))
        factories[chain] = partial(
            InterruptedFork,
            config,
            entry["endpoint"],
            root / "fork-keys.json",
            chain=chain,
            faults=root,
        )
    backend = ChainRouter(config, factories)
    serve(
        GatewayService(
            config,
            root / "state.sqlite",
            backend,
            lease_seconds=30,
            preparation=preparation,
            operator=operator,
        ),
        root / "gateway-auth.token",
        args.port,
    )


if __name__ == "__main__":
    main()
