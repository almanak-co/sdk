"""`almanak ax supply` and `ax withdraw`: one operator action in one process.

The command resolves the market, builds an operator deployment for exactly that
account and market in a private record directory, and runs the action through the
shared planning, signing and reconciliation path with the gateway service in the same
process. Nothing outlives a finished command: the account's existing position is
adopted from the chain at the start, and the next command reads it again. Only the
record of a signed step that is still unresolved is kept.

Without `--execute` the command plans the first step and signs nothing.
"""

import fcntl
import json
import os
import shutil
import signal
import sqlite3
import sys
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path

from almanak import state as durable
from almanak.gateway.client import GatewayClient
from almanak.gateway.ethereum import DataError
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
    over loopback. Storage and transport failures become `gateway_unavailable`, as the
    loopback server reports them, because the inherited lease renewal retries only that.
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
        try:
            with request_scope():
                request = self.service.request_type.model_validate_json(json.dumps(body))
                result = self.service.dispatch(method, request)
        except (OSError, sqlite3.Error):
            raise DataError(
                "gateway_unavailable", "gateway storage unavailable; durable work is retained"
            ) from None
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
            coordinator, self.coordinator = self.coordinator, None
            try:
                self.request("release", coordinator=coordinator, deployment_id=self.deployment_id)
            except DataError:
                # The lease expires by itself; a failed release must not replace the
                # result of an action that already ran.
                pass


def command(args) -> int:
    from almanak.ax import lending_request

    request = lending_request(args)
    wallet = read_wallet(args.wallet)
    provider, endpoint = transport(args, request.chain)
    config = action_config(args, request, wallet, provider, endpoint)
    backend = production_backend(config, endpoint, wallet if args.execute else None)
    try:
        result = run(config, backend, args.ax_command, args.amount, execute=args.execute)
    finally:
        backend.close()
    print(json.dumps(result, indent=2))
    # A preview succeeds by planning; an execution only by completing the action.
    return 0 if not args.execute or result["completed"] else 1


def transport(args, chain):
    """The provider an action names and its endpoint: a key, or a public endpoint."""
    from almanak.gateway.providers import PublicProvider, provider_endpoint, public_endpoint

    if args.public_rpc is None:
        provider = alchemy(chain)
        return provider, provider_endpoint(provider, args.env_file)
    return (
        PublicProvider(kind="public_rpc", name=args.public_rpc),
        public_endpoint(args.public_rpc, chain),
    )


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
    from almanak.gateway.ethereum import EXECUTION_RETRY_BUDGET, EthereumReader
    from almanak.models import Chain

    chain = Chain(chain_id=request.chain, environment="mainnet")
    # The market is resolved at the head, which a load-balanced endpoint can serve a
    # moment late. These reads precede planning and signing, and re-send a transient
    # failure within the budget execution reads have.
    with EthereumReader(chain, endpoint, retry_budget=EXECUTION_RETRY_BUDGET) as reader:
        if args.public_rpc is not None:
            from almanak.gateway.read_endpoints import verify_public

            # The same genesis and head-age proof a public read gives.
            reader.verify_chain()
            verify_public(reader)
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
    """A termination signal during an action; it unwinds like an interrupt."""


STOP_SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)


@contextmanager
def interruptible():
    """Record SIGINT, SIGTERM and SIGHUP during one action instead of acting at once.

    No signal raises at an arbitrary instruction, so nothing can skip the report of a
    retained record. The yielded check raises the recorded signal's exception; the
    action calls it between its steps. A signal that arrives after the last check is
    dropped: the command is already ending and still reports what it did.
    """
    if threading.current_thread() is not threading.main_thread():
        yield lambda: None
        return
    received = None

    def record(signum, frame):
        nonlocal received
        received = signum

    def check():
        if received == signal.SIGINT:
            raise KeyboardInterrupt
        if received is not None:
            raise Terminated(128 + received)

    previous = {number: signal.signal(number, record) for number in STOP_SIGNALS}
    try:
        yield check
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def records_root() -> Path:
    """Where action records live: durable storage beside the fund claims they own."""
    return durable.claims_root().parent / "actions"


@contextmanager
def records_locked(root: Path):
    """Serialize record creation with the sweep, so a record is never seen unlocked
    while its creator is alive."""
    with (root / ".lock").open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        yield


def sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


# Directories from the records directory up to the last one below the user's home:
# actions, almanak-v3, state and .local.
STATE_DEPTH = 4


def durable_root() -> Path:
    """The records directory, with the directory entries that lead to it synced.

    Every entry below the user's home is synced on every call, not only by the call
    that created it: an earlier command may have stopped between creating a directory
    and syncing it. The home directory itself is the anchor taken as durable.
    """
    root = records_root()
    created = [path for path in (root, *root.parents) if not path.exists()]
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    for path in dict.fromkeys((*(root, *root.parents)[:STATE_DEPTH], *created)):
        sync_directory(path.parent)
    return root


def new_record():
    """A private record directory, locked for as long as this process uses it."""
    root = durable_root()
    with records_locked(root):
        directory = Path(tempfile.mkdtemp(dir=root))
        lock = (directory / "lock").open("w")
        fcntl.flock(lock, fcntl.LOCK_EX)
        # The claim written later names this record; its entry must survive a crash
        # that the claim survives, or nothing could release the claim.
        sync_directory(root)
    return directory, lock


def clear_finished_records() -> None:
    """Remove records, and their fund claims, that earlier commands left with nothing
    signed pending.

    A command that is killed outright never reaches its own cleanup, and its claim would
    refuse every later action for the wallet. A record whose lock is free belongs to no
    running command; it is kept only when it holds a signed step, or cannot be read.
    """
    root = records_root()
    if not root.is_dir():
        return
    with records_locked(root):
        for directory in root.iterdir():
            clear_if_finished(directory)


def clear_if_finished(directory: Path) -> None:
    try:
        with (directory / "lock").open("r") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return
            if not in_flight(directory):
                discard(directory)
    except NotADirectoryError:
        # The root's own lock file.
        return
    except FileNotFoundError:
        # Creation locks a record before the root lock is released, so a directory with
        # no lock is what a killed command's cleanup left behind.
        if directory.is_dir() and not in_flight(directory):
            discard(directory)


def run(config, backend, action: str, amount: int | None, *, execute: bool) -> dict:
    """Plan one action against the account's current position, and execute it when asked."""
    clear_finished_records()
    directory, lock = new_record()

    def act(service, check):
        return perform(service, action, amount, execute=execute, create=config, interrupt=check)

    return in_record(directory, lock, config, backend, act)


def in_record(directory: Path, lock, config, backend, body) -> dict:
    """Run `body` against the record's deployment, then remove or keep the record."""
    from almanak.gateway.service import GatewayService

    try:
        with interruptible() as check:
            try:
                service = GatewayService(
                    config,
                    directory / "state.sqlite",
                    backend,
                    lease_seconds=30,
                    operator_actions=True,
                )
                result = body(service, check)
            except BaseException:
                kept = settle(directory)
                if kept is not None:
                    event = {"event": "record_kept", "path": str(kept)}
                    print(json.dumps(event), file=sys.stderr, flush=True)
                raise
            kept = settle(directory)
            if kept is not None:
                result["record_kept"] = str(kept)
            return result
    finally:
        lock.close()


def in_flight(directory: Path) -> bool:
    """Whether the record holds a signed step; a record that cannot be read counts."""
    database = directory / "state.sqlite"
    if not database.exists():
        return False
    try:
        if never_written(database):
            return False
        return durable.Journal(database).load().pending is not None
    except Exception:
        return True


def never_written(database: Path) -> bool:
    """Whether the journal holds no checkpoint at all.

    The checkpoint is written when the deployment is created, before anything is
    planned, so a journal without one belongs to a command that stopped before it
    could sign.
    """
    with durable.Journal(database).connect() as connection:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='checkpoint'"
        ).fetchone()
        if table is None:
            return True
        return connection.execute("SELECT 1 FROM checkpoint WHERE id=1").fetchone() is None


def settle(directory: Path) -> Path | None:
    """Remove the record, or return it when it is kept.

    However the action ended, a signed step may be in flight, and its record is the only
    evidence of it. The fund then stays claimed by this record, so no later command can
    act on the account until the operator has looked at the signed step.
    """
    if in_flight(directory):
        return directory
    discard(directory)
    return None


def discard(directory: Path) -> None:
    database = (directory / "state.sqlite").resolve()
    durable.release_claims(database)
    if durable.claimed_by(database):
        # Another command held a claim's lock while it read the owner. The record
        # stays so that the next command can release the claim; without the record
        # nothing could.
        return
    shutil.rmtree(directory, ignore_errors=True)


def perform(
    service,
    action: str,
    amount: int | None,
    *,
    execute: bool,
    create=None,
    interrupt=lambda: None,
) -> dict:
    from almanak.operator_runtime import OperatorRuntime

    runtime = OperatorRuntime(gateway=LocalGateway(service), create=create)
    try:
        before = runtime.recover()
        interrupt()
        runtime.submit(intent(before.config, action, amount))
        result = {
            "action": action,
            "executed": False,
            "position_before": position(before),
            "first_step": step(runtime.preview()),
        }
        interrupt()
        if execute:
            result.update(executed(runtime, len(before.outcomes), interrupt))
        return result
    finally:
        runtime.close()


def executed(runtime, start: int, interrupt) -> dict:
    """Execute the admitted request and report every step it sent, however it ended.

    A refusal on a later step, such as a fee quote above the ceiling after the approval
    was mined, is part of the result and never replaces it: the steps already sent stay
    reported. Any other failure reports them as one event line on standard error before
    it propagates.
    """
    refusal = None
    try:
        runtime.execute(interrupt)
    except DataError as exc:
        refusal = exc
    except BaseException:
        report_sent(runtime, start)
        raise
    try:
        state = runtime.journal.load()
    except Exception:
        if refusal is None:
            raise
        raise refusal from None
    return executed_result(state, state.outcomes[start:], refusal or runtime.refused)


def report_sent(runtime, start: int) -> None:
    try:
        state = runtime.journal.load()
    except Exception:
        return
    event = {"outcomes": [outcome_summary(item) for item in state.outcomes[start:]]}
    event.update(unresolved(state))
    if event["outcomes"] or "unresolved" in event:
        print(json.dumps({"event": "steps_sent", **event}), file=sys.stderr, flush=True)


def unresolved(state) -> dict:
    """The signed step with no reconciled outcome yet; it may or may not have been sent."""
    pending = state.pending
    if pending is None:
        return {}
    return {"unresolved": {"operation": pending.operation.kind, "transaction": pending.tx_hash}}


def kept_records() -> list[dict]:
    """The records that hold a signed step, or cannot be read, with what each holds."""
    root = records_root()
    if not root.is_dir():
        return []
    paths = sorted(path for path in root.iterdir() if path.is_dir() and in_flight(path))
    return [described(path) for path in paths]


def described(directory: Path) -> dict:
    try:
        state = durable.Journal(directory / "state.sqlite").load()
        fund = state.config.accounts.bindings[0].profile.fund
    except Exception:
        return {"path": str(directory), "readable": False}
    return {
        "path": str(directory),
        "readable": True,
        "chain": str(fund.chain.chain_id),
        "account": fund.address,
        **unresolved(state),
    }


def recover(record: Path, backend_for) -> dict:
    """Resolve the signed step a kept record holds, then drop what it had not signed.

    The step is reconciled from its receipt, or sent again as the bytes that were
    signed; nothing new is planned or signed. `backend_for` builds the chain adapter
    for the record's own configuration.
    """
    directory = record.resolve()
    if directory.parent != records_root().resolve() or not directory.is_dir():
        raise ValueError("not an action record; `almanak ax recover` lists them")
    lock = (directory / "lock").open("a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        lock.close()
        raise ValueError("a command is still using this record") from None
    try:
        config = durable.Journal(directory / "state.sqlite").load().config
        backend = backend_for(config)
    except BaseException:
        lock.close()
        raise
    try:
        return in_record(directory, lock, config, backend, resolve)
    finally:
        backend.close()


def resolve(service, interrupt) -> dict:
    from almanak.operator_runtime import OperatorRuntime

    runtime = OperatorRuntime(gateway=LocalGateway(service), create=None)
    try:
        refusal = None
        try:
            runtime.recover(interrupt)
        except DataError as exc:
            refusal = exc
        state = runtime.journal.load()
        result = {"resolved": state.pending is None, "discarded_request": False}
        if state.pending is None and state.decision is not None:
            # What was left of the request was never signed; the operator states it
            # again against the position as it is now.
            runtime.discard()
            result["discarded_request"] = True
        result.update(executed_result(state, state.outcomes, refusal))
        del result["executed"], result["completed"]
        return result
    finally:
        runtime.close()


def recover_command(args) -> int:
    if args.record is None:
        print(json.dumps({"records": kept_records()}, indent=2))
        return 0

    def backend_for(config):
        return production_backend(config, record_endpoint(config, args.env_file), None)

    result = recover(args.record, backend_for)
    print(json.dumps(result, indent=2))
    return 0 if result["resolved"] else 1


def record_endpoint(config, env_file: Path | None) -> str:
    """The endpoint of the provider the record's action ran on."""
    from almanak.gateway.providers import PublicProvider, provider_endpoint, public_endpoint

    network = config.production.networks[0]
    if isinstance(network.provider, PublicProvider):
        return public_endpoint(network.provider.name, network.chain.chain_id)
    if env_file is None:
        raise ValueError("--env-file is required: this record's action used a provider key")
    return provider_endpoint(network.provider, env_file)


def executed_result(state, outcomes, refusal) -> dict:
    """What an execution did: `completed` only when the request itself finished.

    The connector decides that from the final outcome: an approval alone does not
    complete a supply, and a withdrawal that leaves an allowance is followed by its
    revocation. A step refused at signing leaves no outcome, so neither an empty nor a
    non-empty outcome list means success.
    """
    final = outcomes[-1] if outcomes else None
    result = {
        "executed": bool(outcomes) or state.pending is not None,
        "completed": final is not None and final.strategy_result().complete,
        "outcomes": [outcome_summary(item) for item in outcomes],
        "position_after": position(state),
        **unresolved(state),
    }
    if refusal is not None and not result["completed"]:
        result["refused"] = {"kind": refusal.kind, "message": str(refusal)}
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
    summary = {
        "operation": outcome.operation.kind,
        "success": outcome.success,
        "transaction": outcome.tx_hash,
    }
    issues = getattr(outcome, "accounting_issues", ())
    if issues:
        # A wallet movement the step does not explain; nothing else would report it.
        summary["accounting_issues"] = list(issues)
    return summary


RECOVER = "list kept action records, or resolve the signed step one of them holds"


def recover_arguments(commands) -> None:
    command = commands.add_parser("recover", help=RECOVER, description=RECOVER)
    command.set_defaults(product="operator-recover")
    command.add_argument(
        "--record",
        type=Path,
        help="the kept record to resolve; without it the kept records are listed",
    )
    command.add_argument(
        "--env-file",
        type=Path,
        help="private provider credentials, for a record whose action used a provider key",
    )


def arguments(commands, lending_source) -> None:
    from almanak.cli_arguments import public_rpc_option

    recover_arguments(commands)
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
        provider = action.add_mutually_exclusive_group(required=True)
        provider.add_argument("--env-file", type=Path, help="private provider credentials")
        public_rpc_option(provider)
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
