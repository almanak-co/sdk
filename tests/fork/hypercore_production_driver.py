"""Test-only HyperCore production gateway on one identified loopback fork (blueprint 30,
section 14.3).

The gateway is the production adapter (`HyperCoreProductionGateway`), built by its own
constructor with production configuration, encrypted keystores and encrypted agent keys.
Only three things differ from a production run: the provider guard checks the fork's
identity, the deployment clock is the fork's head, and the `hypercore-model` process
stands in for Hyperliquid's mainnet venue. Production refuses a plain-HTTP provider, so
this process alone accepts the loopback fork's endpoint.
"""

import argparse
import json
from pathlib import Path
from urllib.parse import urlsplit

from almanak.connectors.composition import configuration_from_json
from almanak.gateway import production
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import chain_number
from almanak.gateway.hypercore_gateway import HyperCoreProductionGateway, model_venue
from almanak.gateway.service import GatewayService, serve


class HyperCoreRehearsalGateway(HyperCoreProductionGateway):
    def __init__(self, config, endpoint, identity, key_file, password_file, *, model):
        for url in (endpoint, model):
            if urlsplit(url).hostname != "127.0.0.1":
                raise ValueError("a production rehearsal runs on loopback endpoints only")
        self.identity, self.model = identity, model
        super().__init__(config, endpoint, key_file, password_file)
        # Observation ages and venue expiries follow the fork's clock, which the model does.
        self.clock = lambda: self.reader.pin_block("latest").timestamp

    def production_venue(self):
        return model_venue(self.model, self.fork_clock)

    def fork_clock(self) -> int:
        head = self.reader._rpc("eth_getBlockByNumber", ["latest", False])
        return int(head["timestamp"], 16) * 1000

    def guard(self):
        actual = self.reader._rpc("anvil_metadata", [])
        if any(actual.get(key) != self.identity[key] for key in ("instanceId", "forkedNetwork")):
            raise DataError("fork_mismatch", "the rehearsal fork's identity changed")
        if int(self.reader._rpc("eth_chainId", []), 16) != chain_number(self.reader.chain.chain_id):
            raise DataError("fork_mismatch", "the rehearsal chain changed")


def main():
    parser = argparse.ArgumentParser()
    for name in ("config", "key-file", "password-file", "auth-token-file", "database"):
        parser.add_argument("--" + name, type=Path, required=True)
    for name in ("fork-url", "model"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--identity", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--lease-seconds", type=float, default=30)
    args = parser.parse_args()
    production.validate_endpoint = lambda endpoint: None
    config = configuration_from_json(args.config.read_text())
    backend = HyperCoreRehearsalGateway(
        config,
        args.fork_url,
        json.loads(args.identity.read_text()),
        args.key_file,
        args.password_file,
        model=args.model,
    )
    service = GatewayService(config, args.database, backend, lease_seconds=args.lease_seconds)
    serve(service, args.auth_token_file, args.port)


if __name__ == "__main__":
    main()
