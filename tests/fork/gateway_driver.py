"""Kill the actual HTTP gateway after a privileged operation, before its response."""

import argparse
import os
import signal
from pathlib import Path

from almanak.connectors.aave_v4.models import LendingConfig
from almanak.gateway.fork_execution import ForkGateway
from almanak.gateway.service import GatewayService, serve

parser = argparse.ArgumentParser()
parser.add_argument("--config", type=Path, required=True)
parser.add_argument("--database", type=Path, required=True)
parser.add_argument("--fork-url", required=True)
parser.add_argument("--fork-key-file", type=Path, required=True)
parser.add_argument("--auth-token-file", type=Path, required=True)
parser.add_argument("--port", type=int, required=True)
parser.add_argument("--lease-seconds", type=float, required=True)
parser.add_argument("--crash", choices=("sign", "broadcast"), required=True)
args = parser.parse_args()


class InterruptedService(GatewayService):
    def dispatch(self, method, request):
        result = super().dispatch(method, request)
        if method == args.crash:
            state = self.journal.load()
            attempt = state.pending if method == "broadcast" else state.plan
            if attempt.operation.kind == "supply":
                os.kill(os.getpid(), signal.SIGKILL)
        return result


config = LendingConfig.model_validate_json(args.config.read_text())
backend = ForkGateway(config, args.fork_url, args.fork_key_file)
serve(
    InterruptedService(config, args.database, backend, lease_seconds=args.lease_seconds),
    args.auth_token_file,
    args.port,
)
