"""Test-only production lifecycle backend bound to one isolated, funded Anvil fork."""

import argparse
import json
import os
import signal
from pathlib import Path
from urllib.parse import urlsplit

from almanak.connectors.composition import configuration_from_json
from almanak.gateway.credentials import load_production_signers
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import chain_number
from almanak.gateway.execution import EvmGateway
from almanak.gateway.production import ProductionGateway
from almanak.gateway.routing import ChainRouter
from almanak.gateway.service import GatewayService, serve
from almanak.models import Chain
from almanak.state import Journal


class RehearsalGateway(ProductionGateway):
    def __init__(
        self,
        config,
        endpoint,
        identity,
        key_file,
        password_file,
        receipt_gate,
        *,
        chain=None,
        fixture_clock=False,
    ):
        if urlsplit(endpoint).hostname != "127.0.0.1":
            raise ValueError("rehearsal transport must be an isolated loopback fork")
        self.production = config.production
        self.identity = identity
        self.receipt_gate = receipt_gate
        EvmGateway.__init__(self, config, endpoint, {}, chain=chain)
        if fixture_clock:
            self.clock = lambda: self.reader.pin_block("latest").timestamp
        self.signers = load_production_signers(config, key_file, password_file)

    def _sign(self, *args):
        signed = super()._sign(*args)
        if getattr(self, "fault_stage", None) == "sign-before-record":
            state = Journal(self.database).load()
            if replaced(state.pending):
                os.kill(os.getpid(), signal.SIGKILL)
        return signed

    def receipt(self, pending):
        receipt = super().receipt(pending)
        # Fault injection holds a mined receipt across abrupt process death and recovery.
        if (
            pending.operation.kind == "supply"
            and self.receipt_gate is not None
            and self.receipt_gate.exists()
        ):
            return None
        return receipt

    def guard(self):
        actual = self.reader._rpc("anvil_metadata", [])
        expected = self.identity
        if any(
            actual.get(key) != expected[key] for key in ("instanceId", "forkedNetwork", "chainId")
        ):
            raise DataError("fork_mismatch", "isolated production rehearsal fork identity changed")
        chain = chain_number(self.reader.chain.chain_id)
        if actual["chainId"] != chain or int(self.reader._rpc("eth_chainId", []), 16) != chain:
            raise DataError("fork_mismatch", "rehearsal chain changed")


def replaced(pending):
    return (
        pending is not None
        and pending.family is not None
        and (len(pending.family.candidates) > 1 or pending.family.proposal is not None)
    )


class ProofService(GatewayService):
    def record_update(self, request, db, state):
        if (
            self.backend.fault_stage == "commit-before-record"
            and replaced(state.pending)
            and request.after.pending is None
        ):
            os.kill(os.getpid(), signal.SIGKILL)
        return super().record_update(request, db, state)

    def dispatch_authorized(self, method, request, db, state):
        result = super().dispatch_authorized(method, request, db, state)
        if (
            self.backend.fault_stage == "broadcast-before-record"
            and method == "broadcast"
            and result is True
            and replaced(state.pending)
        ):
            os.kill(os.getpid(), signal.SIGKILL)
        return result

    def dispatch(self, method, request):
        result = super().dispatch(method, request)
        stage = self.backend.fault_stage
        action = request.action if method == "family" else method
        if action == stage:
            self.kill_replaced(method)
        return result

    def kill_replaced(self, method):
        state = self.journal.load()
        candidate = state.pending
        if method == "update" and state.outcomes:
            candidate = state.outcomes[-1]
        if replaced(candidate):
            os.kill(os.getpid(), signal.SIGKILL)


def rehearsal_backend(args, config):
    if args.routes is not None:
        if args.fault_stage is not None:
            raise ValueError("replacement fault injection requires a single rehearsal route")
        from functools import partial

        factories = {}
        for entry in json.loads(args.routes.read_text()):
            chain = Chain.model_validate_json(json.dumps(entry["chain"]))
            factories[chain] = partial(
                RehearsalGateway,
                config,
                entry["endpoint"],
                entry["identity"],
                args.key_file,
                args.password_file,
                args.receipt_gate,
                chain=chain,
                fixture_clock=True,
            )
        backend = ChainRouter(config, factories)
    else:
        backend = RehearsalGateway(
            config,
            args.fork_url,
            json.loads(args.identity.read_text()),
            args.key_file,
            args.password_file,
            args.receipt_gate,
        )
        backend.fault_stage = args.fault_stage
        backend.database = args.database
    return backend


def main():
    parser = argparse.ArgumentParser()
    for name in ("config", "key-file", "password-file", "auth-token-file", "database"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--fork-url")
    parser.add_argument("--identity", type=Path)
    parser.add_argument("--routes", type=Path)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--receipt-gate", type=Path)
    parser.add_argument("--lease-seconds", type=float, default=30)
    parser.add_argument("--fault-stage")
    args = parser.parse_args()
    config = configuration_from_json(args.config.read_text())
    backend = rehearsal_backend(args, config)
    service_type = ProofService if args.fault_stage else GatewayService
    service = service_type(config, args.database, backend, lease_seconds=args.lease_seconds)
    serve(service, args.auth_token_file, args.port)


if __name__ == "__main__":
    main()
