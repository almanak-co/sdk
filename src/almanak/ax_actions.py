"""`almanak ax supply` and `ax withdraw`: one operator action in one process.

The command resolves the market, builds an operator deployment for exactly that
account and market in a private temporary directory, and runs the action through the
shared planning, signing and reconciliation path with the gateway service in the same
process. Nothing outlives the command: the account's existing position is adopted from
the chain at the start, and the next command reads it again.

Without `--execute` the command plans the first step and signs nothing.
"""

import json
import shutil
import signal
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path

from almanak.gateway.client import GatewayClient
from almanak.models import Model

ACTIONS = {
    "supply": "supply an amount of the reserve asset from the wallet",
    "withdraw": "withdraw the whole supplied balance, including interest",
}
CONNECTORS = ("aave_v4",)
ALLOCATION = "position"


class LocalGateway(GatewayClient):
    """The coordinator's gateway client, with the gateway service in this process.

    Requests keep their wire form, so the service validates exactly what it validates
    over loopback.
    """

    def __init__(self, service):
        self.service = service
        self.coordinator = None
        self.deployment_id = None
        self.stopped = threading.Event()
        self.heartbeat = None
        self.lease_error = None
        self.lease_confirmed = None

    def request(self, method: str, *, timeout: float | None = None, **fields):
        from almanak.gateway.request_scope import request_scope

        body = {
            key: value.model_dump(mode="json") if isinstance(value, Model) else value
            for key, value in fields.items()
        }
        with request_scope():
            request = self.service.request_type.model_validate_json(json.dumps(body))
            result = self.service.dispatch(method, request)
        if isinstance(result, Model):
            return result.model_dump(mode="json")
        if isinstance(result, tuple):
            return [item.model_dump(mode="json") for item in result]
        return result

    def close(self):
        self.stopped.set()
        if self.heartbeat is not None:
            self.heartbeat.join(timeout=65)
        if self.coordinator is not None:
            self.request("release", coordinator=self.coordinator, deployment_id=self.deployment_id)
            self.coordinator = None


def command(args) -> int:
    from almanak.ax import lending_request
    from almanak.gateway.providers import provider_endpoint

    request = lending_request(args)
    wallet = read_wallet(args.wallet)
    provider = alchemy(request.chain)
    endpoint = provider_endpoint(provider, args.env_file)
    config = action_config(args, request, wallet, provider, endpoint)
    backend = production_backend(config, endpoint, wallet if args.execute else None)
    try:
        result = run(config, backend, args.ax_command, args.amount, execute=args.execute)
    finally:
        backend.close()
    print(json.dumps(result, indent=2))
    # A preview succeeds by planning; an execution only by completing the action.
    return 0 if not args.execute or result["completed"] else 1


def read_wallet(directory: Path) -> dict:
    """The record `almanak wallet create` wrote: an address and its private file paths."""
    from almanak.gateway.credentials import private_text

    wallet = json.loads(private_text(directory / "wallet.json"))
    if not isinstance(wallet, dict) or not {"account", "address", "keystores"} <= set(wallet):
        raise ValueError("wallet directory holds no wallet.json from `almanak wallet create`")
    return wallet


def alchemy(chain):
    from almanak.gateway.providers import ALCHEMY_NETWORKS, AlchemyProvider

    if chain not in ALCHEMY_NETWORKS:
        raise ValueError(f"no provider binding for {chain}")
    return AlchemyProvider(
        kind="alchemy", network=ALCHEMY_NETWORKS[chain], api_key_env="ALCHEMY_API_KEY"
    )


def production_backend(config, endpoint, wallet):
    """The production adapter; it holds a signer only when the action will execute."""
    from almanak.gateway.production import ProductionGateway

    if wallet is None:
        return ProductionGateway(config, endpoint)
    password = wallet.get("password_file")
    return ProductionGateway(
        config, endpoint, Path(wallet["keystores"]), None if password is None else Path(password)
    )


def action_config(args, request, wallet, provider, endpoint):
    """The operator deployment of one account in one resolved market, with explicit fees."""
    from almanak.connectors.lending_lookup import family, resolve
    from almanak.gateway.ethereum import EthereumReader
    from almanak.models import Chain

    chain = Chain(chain_id=request.chain, environment="mainnet")
    with EthereumReader(chain, endpoint) as reader:
        resolved = resolve(reader, family(args.connector), request, reader.pin_block("latest"))
    return lending_config(
        resolved.reserve,
        wallet,
        provider,
        max_gas_price_wei=args.max_gas_price_wei,
        max_transaction_fee=args.max_transaction_fee,
    )


def lending_config(reserve, wallet, provider, *, max_gas_price_wei, max_transaction_fee):
    """Bind the wallet under the account name its keystore entry carries."""
    from almanak.chains import evm_network
    from almanak.connectors.aave_v4.models import LendingConfig

    account, address = wallet["account"], wallet["address"]
    chain = reserve.chain.model_dump(mode="json")
    native = str(evm_network(reserve.chain.chain_id).native_asset)
    document = {
        "decision_trigger": {"kind": "interval", "seconds": 60},
        "accounts": {
            "bindings": [
                {
                    "name": account,
                    "profile": {
                        "kind": "eoa",
                        "fund": {"chain": chain, "address": address.lower()},
                    },
                }
            ]
        },
        "allocations": [
            {
                "name": ALLOCATION,
                "account": account,
                "reserve": reserve.model_dump(mode="json"),
                "native_asset": native,
            }
        ],
        "production": {
            "networks": [
                {
                    "chain": chain,
                    "provider": provider.model_dump(mode="json"),
                    "max_observation_age_seconds": 120,
                }
            ],
            "gas": [
                {
                    "accounts": [account],
                    "asset": native,
                    "source": "network_rpc_fee_history",
                    "reward_percentile": 50,
                    "base_fee_multiplier_bps": 20000,
                    "max_gas_price_wei": max_gas_price_wei,
                    "max_gas_limit": 1_000_000,
                    "max_transaction_fee": {"asset": native, "amount": max_transaction_fee},
                    "exit_reserve": {"asset": native, "amount": 0},
                    "gas_limit_buffer_bps": 2500,
                }
            ],
        },
    }
    return LendingConfig.model_validate_json(json.dumps(document))


def intent(config, action: str, amount: int | None):
    if action == "supply":
        if amount is None:
            raise ValueError("supply requires --amount")
        return config.supply(ALLOCATION, amount)
    if amount is not None:
        raise ValueError("withdraw takes no --amount: it withdraws the whole supplied balance")
    return config.withdraw(ALLOCATION)


class Terminated(SystemExit):
    """SIGTERM during an action; it unwinds like an interrupt so the record is reported."""


@contextmanager
def terminable():
    """Turn SIGTERM into an exception for the duration of one action.

    The default action would end the process without reporting a retained record.
    """
    if threading.current_thread() is not threading.main_thread():
        yield
        return

    def stop(signum, frame):
        raise Terminated(128 + signum)

    previous = signal.signal(signal.SIGTERM, stop)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def run(config, backend, action: str, amount: int | None, *, execute: bool) -> dict:
    """Plan one action against the account's current position, and execute it when asked."""
    from almanak.gateway.service import GatewayService
    from almanak.state import release_claims

    directory = Path(tempfile.mkdtemp(prefix="almanak-ax-"))
    service = GatewayService(
        config, directory / "state.sqlite", backend, lease_seconds=30, operator_actions=True
    )
    kept = True
    try:
        with terminable():
            result = perform(service, action, amount, execute=execute, create=config)
        kept = False
        return result
    except BaseException:
        # A signed step may be in flight; its record is the only evidence of it.
        kept = service.journal.path.exists() and service.journal.load().pending is not None
        raise
    finally:
        if kept:
            # The fund stays claimed by this record, so no later command can act on the
            # account until the operator has looked at the signed step.
            print(json.dumps({"event": "record_kept", "path": str(directory)}), flush=True)
        else:
            release_claims(directory / "state.sqlite")
            shutil.rmtree(directory, ignore_errors=True)


def perform(service, action: str, amount: int | None, *, execute: bool, create=None) -> dict:
    from almanak.operator_runtime import OperatorRuntime

    runtime = OperatorRuntime(gateway=LocalGateway(service), create=create)
    try:
        before = runtime.recover()
        runtime.submit(intent(before.config, action, amount))
        result = {
            "action": action,
            "executed": False,
            "position_before": position(before),
            "first_step": step(runtime.preview()),
        }
        if execute:
            result.update(executed_result(runtime, action, runtime.execute()))
        return result
    finally:
        runtime.close()


def executed_result(runtime, action: str, outcomes) -> dict:
    """What an execution did: `completed` only when the action's own step succeeded.

    A step refused at signing leaves no outcome, and an approval can succeed before its
    supply is refused, so neither an empty nor a non-empty outcome list means success.
    """
    final = outcomes[-1] if outcomes else None
    result = {
        "executed": bool(outcomes),
        "completed": final is not None and final.success and final.operation.kind == action,
        "outcomes": [outcome_summary(item) for item in outcomes],
        "position_after": position(runtime.journal.load()),
    }
    refused = getattr(runtime, "refused", None)
    if refused is not None and not result["completed"]:
        result["refused"] = {"kind": refused.kind, "message": str(refused)}
    return result


def position(state) -> dict:
    owned = state.owned[0]
    return {"supplied_shares": str(owned.shares), "debt_shares": str(owned.drawn_shares)}


def step(plan) -> dict:
    return {
        "operation": plan.operation.model_dump(mode="json"),
        # Retained plans from before execution envelopes carry none.
        "to": None if plan.execution is None else plan.execution.to,
        "nonce": plan.nonce,
        "gas": plan.gas,
        "gas_price": plan.gas_price,
    }


def outcome_summary(outcome) -> dict:
    return {
        "operation": outcome.operation.kind,
        "success": outcome.success,
        "transaction": outcome.tx_hash,
    }


def arguments(commands, lending_source) -> None:
    for name, text in ACTIONS.items():
        action = commands.add_parser(name, help=text, description=text)
        action.set_defaults(product="operator-action")
        action.add_argument(
            "--connector", required=True, choices=CONNECTORS, help="canonical connector ID"
        )
        lending_source(action)
        if name == "supply":
            action.add_argument(
                "--amount", type=base_units, required=True, help="base units of the reserve asset"
            )
        else:
            action.set_defaults(amount=None)
        action.add_argument(
            "--wallet",
            type=Path,
            required=True,
            help="directory written by `almanak wallet create`",
        )
        action.add_argument(
            "--env-file", type=Path, required=True, help="private provider credentials"
        )
        action.add_argument(
            "--max-gas-price-wei",
            type=base_units,
            required=True,
            help="refuse a step whose quoted gas price is above this",
        )
        action.add_argument(
            "--max-transaction-fee",
            type=base_units,
            required=True,
            help="refuse a step whose fee could exceed this, in base units of the native asset",
        )
        action.add_argument(
            "--execute",
            action="store_true",
            help="sign and send; without it the first step is planned and nothing is signed",
        )


def base_units(value: str) -> int:
    import argparse

    if not (value.isascii() and value.isdigit()) or int(value) <= 0:
        raise argparse.ArgumentTypeError("expected a positive whole number of base units")
    return int(value)
