"""The direct `almanak ax` actions, such as `ax supply`, `ax borrow` and `ax swap`: one
operator action in one process.

The command resolves the market, builds an operator deployment for exactly that
account and market in a private record directory, and runs the action through the
shared planning, signing and reconciliation path with the gateway service in the same
process. Nothing outlives a finished command: the account's existing position is
adopted from the chain at the start, and the next command reads it again. Only the
record of a signed step that is still unresolved is kept.

Each connector declares the actions it serves and how its own part of the deployment
is built (`connectors/direct.py`); the chains that serve an action are the connector's
support declaration.

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
from dataclasses import dataclass
from pathlib import Path

from almanak import state as durable
from almanak.gateway.client import GatewayClient
from almanak.gateway.ethereum import DataError
from almanak.models import Model

# Help text per action command; the connectors that serve each are declared in
# `connectors/direct.py`.
ACTIONS = {
    "supply": "supply an amount of the reserve asset from the wallet",
    "withdraw": "withdraw the whole supplied balance, including interest, or an amount of it",
    "borrow": "borrow an amount of the reserve asset to the wallet",
    "repay": "repay the whole debt, including interest, or an amount of it",
    "collateral": "enable or disable the supplied balance as collateral",
    "swap": "sell an exact amount of one pool currency for another, in one named pool",
    "open-liquidity": "mint a concentrated-liquidity position in one tick range of one"
    " named pool, from stated amounts of its two currencies, or for curve and aerodrome"
    " deposit stated amounts of a pool's coins for its LP token; nothing is swapped",
    "increase-liquidity": "add stated amounts of the pool's currencies to a position"
    " the wallet owns; nothing is swapped",
    "decrease-liquidity": "remove part of the liquidity of a position the wallet owns and"
    " collect what it is owed; for curve and aerodrome, burn part of the wallet's LP shares",
    "collect-liquidity-fees": "collect what a position the wallet owns is owed, without"
    " changing its liquidity; for aerodrome, claim the fees the wallet's LP shares earned",
    "close-liquidity": "remove all liquidity of a position the wallet owns, collect what"
    " it is owed and burn its NFT; for curve and aerodrome, burn all of the wallet's LP"
    " shares, and for aerodrome claim their fees",
    "revoke-exchange": "set every allowance the wallet gave the pool's router and"
    " position manager, or for curve the pool itself, in the pool currencies to zero; for"
    " aerodrome also the router's allowance in the pool's LP token",
}
# Actions whose market is one pool; the others name a lending market, with the flags
# `ax lending-reserve` takes.
POOL_ACTIONS = (
    "swap",
    "open-liquidity",
    "increase-liquidity",
    "decrease-liquidity",
    "collect-liquidity-fees",
    "close-liquidity",
    "revoke-exchange",
)
LENDING_ACTIONS = tuple(name for name in ACTIONS if name not in POOL_ACTIONS)
# Whether each lending command's --amount is required; one not named here takes none.
AMOUNTS = {"supply": True, "borrow": True, "withdraw": False, "repay": False}
AMOUNT_HELP = {
    "withdraw": "base units to withdraw; without it the whole supplied balance",
    "repay": "base units to repay; without it the whole debt",
}
# Commands whose effect on the account's health the protocol can risk-check.
FLOORED = {"supply": False, "withdraw": False, "borrow": True, "collateral": False}
# The flags that name a pool: its address, or the members of its key. A connector takes
# those its own market selection has as fields.
POOL_FLAGS = ("pool", "currency0", "currency1", "fee", "tick_spacing", "hooks")
# Seconds after the observed block within which a swap may still be signed.
DEFAULT_DEADLINE_SECONDS = 300


@dataclass(frozen=True)
class FeeLimits:
    """The ceilings an operator states for every step of one action."""

    max_gas_price_wei: int
    # Base units of the chain's native asset.
    max_transaction_fee: int
    # None takes the gas limit an author's gas policy defaults to.
    max_gas_limit: int | None = None


# The longest a step's observation may age before it is refused;
# `scripts/qualify_public_rpc.py` holds a public endpoint's head to the same.
MAX_OBSERVATION_AGE_SECONDS = 120


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
    from almanak.connectors.direct import require_bought, require_direct

    request = market_request(args)
    action = action_request(args)
    require_direct(args.connector, request.chain, action)
    require_bought(args.connector, action)
    wallet = read_wallet(args.wallet)
    provider, endpoint = transport(args, request.chain)
    config = action_config(args, request, action, wallet, provider, endpoint)
    backend = production_backend(config, endpoint, wallet if args.execute else None)
    try:
        result = run(config, backend, action, execute=args.execute)
    finally:
        backend.close()
    print(json.dumps(result, indent=2))
    # A preview succeeds by planning; an execution only by completing the action.
    return 0 if not args.execute or result["completed"] else 1


def market_request(args):
    """The market a command names, validated as the connector's own market selection."""
    if args.ax_command not in POOL_ACTIONS:
        from almanak.ax import lending_request

        return lending_request(args)
    values = {"connector": args.connector, "chain": args.chain}
    values.update(
        (name, value) for name in POOL_FLAGS if (value := getattr(args, name, None)) is not None
    )
    return resolution(args.connector).MarketSelection.model_validate_json(json.dumps(values))


def flags(names) -> str:
    return ", ".join("--" + name.replace("_", "-") for name in names)


def validate_arguments(parser, args) -> None:
    """Refuse a pool named the way another connector names its pools, before any read.

    A pool is a contract address for one connector and a key for another, and argparse
    cannot tie the flags to the connector: the connector's own market selection decides
    which of them it takes and which it requires.
    """
    if args.ax_command not in POOL_ACTIONS:
        return
    fields = resolution(args.connector).MarketSelection.model_fields
    required = [name for name in POOL_FLAGS if name in fields and fields[name].is_required()]
    optional = [name for name in POOL_FLAGS if name in fields and name not in required]
    # A command states only the pool flags its own parser has.
    given = [name for name in POOL_FLAGS if getattr(args, name, None) is not None]
    refusal = pool_refusal(required, optional, given)
    if refusal is not None:
        also = f" and optionally {flags(optional)}" if optional else ""
        parser.error(f"{args.connector} names its pool with {flags(required)}{also}; {refusal}")
    if args.ax_command != "swap":
        from almanak.ax_liquidity import validate

        validate(parser, args)


def pool_refusal(required, optional, given) -> str | None:
    """What is wrong with the pool flags given, for a connector taking these."""
    foreign = [name for name in given if name not in (*required, *optional)]
    if foreign:
        return f"it takes no {flags(foreign)}"
    missing = [name for name in required if name not in given]
    return f"missing {flags(missing)}" if missing else None


def action_request(args):
    """The request a command states; an option the command does not take is absent."""
    from almanak.connectors.direct import ActionRequest

    return ActionRequest(
        action=args.ax_command,
        amount=args.amount,
        min_health_factor=getattr(args, "min_health_factor", None),
        enable=getattr(args, "enable", None),
        sell=getattr(args, "sell", None),
        slippage_bps=getattr(args, "slippage_bps", None),
        deadline_seconds=getattr(args, "deadline_seconds", None),
        buy=getattr(args, "buy", None),
        amount0=getattr(args, "amount0", None),
        amount1=getattr(args, "amount1", None),
        tick_lower=getattr(args, "tick_lower", None),
        tick_upper=getattr(args, "tick_upper", None),
        token_id=getattr(args, "token_id", None),
        liquidity=getattr(args, "liquidity", None),
        amounts=tuple(stated) if (stated := getattr(args, "amounts", None)) else None,
        receive=getattr(args, "receive", None),
    )


def resolution(connector: str):
    """The connector's own market selection and resolver, as its author loader uses them."""
    from importlib import import_module

    return import_module(f"almanak.connectors.{connector}.resolution")


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


def action_config(args, request, action, wallet, provider, endpoint):
    """The operator deployment a command's arguments state."""
    limits = FeeLimits(args.max_gas_price_wei, args.max_transaction_fee, args.max_gas_limit)
    public = args.public_rpc is not None
    return deployment(
        args.connector, request, action, wallet, provider, endpoint, limits, public=public
    )


def deployment(connector, request, action, wallet, provider, endpoint, limits, *, public=False):
    """The operator deployment of one account in one resolved market, with explicit fees."""
    from almanak.connectors.direct import Wallet, binding
    from almanak.gateway.ethereum import EthereumReader
    from almanak.models import Chain

    chain = Chain(chain_id=request.chain, environment="mainnet")
    account = Wallet(account=wallet["account"], address=wallet["address"].lower())
    with EthereumReader(chain, endpoint) as reader:
        if public:
            prove_public(reader)
        block = reader.pin_block("latest")
        resolved = resolution(connector).resolve_market(reader, request, block)
        positions = binding(connector).positions(reader, block, resolved, account, action)
    return configuration(connector, positions, account, provider, chain, limits)


def prove_public(reader) -> None:
    """The genesis and head-age proof a public read gives, before an action or a
    recovery uses a named public endpoint."""
    from almanak.gateway.read_endpoints import verify_public

    reader.verify_chain()
    verify_public(reader)


def configuration(connector: str, positions: dict, wallet, provider, chain, limits: FeeLimits):
    """Bind the wallet under the account name its keystore entry carries.

    `positions` is the connector's own part of the document. The accounts, the network
    and the gas policy are the same for every connector.
    """
    from almanak.connectors.composition import connector_from_wire

    scope = chain.model_dump(mode="json")
    document = {
        "decision_trigger": {"kind": "interval", "seconds": 60},
        "accounts": {
            "bindings": [
                {
                    "name": wallet.account,
                    "profile": {"kind": "eoa", "fund": {"chain": scope, "address": wallet.address}},
                }
            ]
        },
        **positions,
        "production": {
            "networks": [
                {
                    "chain": scope,
                    "provider": provider.model_dump(mode="json"),
                    "max_observation_age_seconds": MAX_OBSERVATION_AGE_SECONDS,
                }
            ],
            "gas": [gas_policy(wallet.account, chain.chain_id, limits)],
        },
    }
    model = connector_from_wire({"connector": connector}).codec.config
    return model.model_validate_json(json.dumps(document))


def gas_policy(account: str, chain, limits: FeeLimits) -> dict:
    """How a step bids, as an author's gas policy defaults on the chain; the ceilings
    are the operator's own."""
    from almanak.chains import evm_network
    from almanak.configuration import gas

    native = str(evm_network(chain).native_asset)
    scope = {"chain": str(chain)}
    return {
        "accounts": [account],
        "asset": native,
        "source": "network_rpc_fee_history",
        "base_fee_basis": gas.chain_base_fee_basis(scope),
        "reward_percentile": gas.DEFAULT_REWARD_PERCENTILE,
        "base_fee_multiplier_bps": gas.chain_multiplier(scope),
        "max_gas_price_wei": limits.max_gas_price_wei,
        "max_gas_limit": limits.max_gas_limit or gas.DEFAULT_MAX_GAS_LIMIT,
        "max_transaction_fee": {"asset": native, "amount": limits.max_transaction_fee},
        "exit_reserve": {"asset": native, "amount": 0},
        "gas_limit_buffer_bps": gas.DEFAULT_GAS_LIMIT_BUFFER_BPS,
    }


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


def run(config, backend, request, *, execute: bool) -> dict:
    """Plan one action against the account's current position, and execute it when asked."""
    clear_finished_records()
    directory, lock = new_record()

    def act(service, check):
        return perform(service, request, execute=execute, create=config, interrupt=check)

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
    request,
    *,
    execute: bool,
    create=None,
    interrupt=lambda: None,
) -> dict:
    from almanak.connectors.direct import binding_for
    from almanak.operator_runtime import OperatorRuntime

    runtime = OperatorRuntime(gateway=LocalGateway(service), create=create)
    try:
        before = runtime.recover()
        interrupt()
        build = binding_for(before.config).intent
        # The intent is built against the observations it is admitted with.
        runtime.submit(lambda state, observed: build(state.config, observed, request))
        result = {
            "action": request.action,
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
        endpoint = record_endpoint(config, args.env_file, args.public_rpc)
        return production_backend(config, endpoint, None)

    result = recover(args.record, backend_for)
    print(json.dumps(result, indent=2))
    # A record resolved by releasing an expired step is freed, not a step that happened.
    return 0 if result["resolved"] and "released_expired" not in result else 1


def record_endpoint(config, env_file: Path | None, public_rpc: str | None = None) -> str:
    """The endpoint a record is resolved through: the transport the caller names, or
    else the provider the record's action ran on.

    The signed bytes name their chain, and the adapter proves the chain and its genesis
    on the endpoint it is given and reconciles from canonical receipts, so any endpoint
    of the record's chain resolves the same step. A named public endpoint gives the
    public proof first. A pair that left the reviewed list has no endpoint of its own
    any more, so its record needs a named transport.
    """
    from almanak.gateway import providers

    network = config.production.networks[0]
    chain, recorded = network.chain.chain_id, network.provider
    if public_rpc is not None:
        return proven_public(network.chain, providers.public_endpoint(public_rpc, chain))
    if not isinstance(recorded, providers.PublicProvider):
        if env_file is None:
            raise ValueError("--env-file is required: this record's action used a provider key")
        return providers.provider_endpoint(recorded, env_file)
    if env_file is not None:
        return providers.provider_endpoint(alchemy(chain), env_file)
    if chain not in providers.PUBLIC_RPC.get(recorded.name, {}):
        raise ValueError(
            f"the public endpoint {recorded.name} this record's action ran on no longer"
            f" serves {chain}; name a transport with --public-rpc or --env-file"
        )
    return providers.public_endpoint(recorded.name, chain)


def proven_public(chain, endpoint: str) -> str:
    from almanak.gateway.ethereum import EthereumReader

    with EthereumReader(chain, endpoint) as reader:
        prove_public(reader)
    return endpoint


def executed_result(state, outcomes, refusal) -> dict:
    """What an execution did: `completed` only when the request itself finished.

    The connector decides that from the final outcome: an approval alone does not
    complete a supply or a swap, and a withdrawal that leaves an allowance is followed
    by its revocation. A step refused at signing leaves no outcome, so neither an empty
    nor a non-empty outcome list means success.
    """
    final = outcomes[-1] if outcomes else None
    result = {
        "executed": bool(outcomes) or state.pending is not None or bool(released(state)),
        "completed": final is not None and final.strategy_result().complete,
        "outcomes": [outcome_summary(item) for item in outcomes],
        "position_after": position(state),
        **unresolved(state),
        **released(state),
    }
    if refusal is not None and not result["completed"]:
        result["refused"] = {"kind": refusal.kind, "message": str(refusal)}
    return result


def released(state) -> dict:
    """The signed steps released past their protocol deadline, as `released_expired`.

    Such a step was never mined and can only revert if a copy still lands, so what it
    carried did not happen: the request is over, and the operator states it again. The
    wallet is free, since nothing signed is left pending.
    """
    steps = [
        {"operation": item.operation.kind, "transaction": item.tx_hash}
        for item in getattr(state, "released", ())
    ]
    return {"released_expired": steps} if steps else {}


def position(state) -> dict:
    from almanak.connectors.direct import binding_for

    return binding_for(state.config).position(state)


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
    from almanak.cli_arguments import public_rpc_option

    command = commands.add_parser("recover", help=RECOVER, description=RECOVER)
    command.set_defaults(product="operator-recover")
    command.add_argument(
        "--record",
        type=Path,
        help="the kept record to resolve; without it the kept records are listed",
    )
    # Without one the record is resolved through the endpoint its action ran on.
    transport = command.add_mutually_exclusive_group()
    transport.add_argument(
        "--env-file",
        type=Path,
        help="private provider credentials to resolve the record through; required for"
        " a record whose action used a provider key",
    )
    public_rpc_option(transport)


def arguments(commands, lending_source) -> None:
    from almanak.cli_arguments import public_rpc_option
    from almanak.connectors.direct import connectors_for, served_actions

    recover_arguments(commands)
    for name in served_actions():
        text = ACTIONS[name]
        action = commands.add_parser(name, help=text, description=text)
        action.set_defaults(product="operator-action")
        action.add_argument(
            "--connector",
            required=True,
            choices=connectors_for(name),
            help="canonical connector ID",
        )
        if name == "swap":
            swap_arguments(action)
        elif name in POOL_ACTIONS:
            from almanak.ax_liquidity import arguments as liquidity_arguments

            liquidity_arguments(name, action)
        else:
            lending_source(action)
            request_arguments(name, action)
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
            "--max-gas-limit",
            type=gas_units,
            help="refuse a step whose gas, with its buffer, is above this"
            " (default: an author gas policy's own, 1200000)",
        )
        action.add_argument(
            "--execute",
            action="store_true",
            help="sign and send; without it the first step is planned and nothing is signed",
        )


def request_arguments(name: str, action) -> None:
    """What one command states about its request: an amount, a health floor, a direction."""
    if name in AMOUNTS:
        text = AMOUNT_HELP.get(name, "base units of the reserve asset")
        action.add_argument("--amount", type=base_units, required=AMOUNTS[name], help=text)
    else:
        action.set_defaults(amount=None)
    if name in FLOORED:
        action.add_argument(
            "--min-health-factor",
            type=health_factor,
            required=FLOORED[name],
            help="refuse the action if it would leave the account's projected health factor"
            " below this, a decimal above 1; needed whenever the account has debt",
        )
    if name == "collateral":
        direction = action.add_mutually_exclusive_group(required=True)
        direction.add_argument("--enable", dest="enable", action="store_true", default=None)
        direction.add_argument("--disable", dest="enable", action="store_false", default=None)


def health_factor(value: str) -> int:
    """A health factor above 1, at the wad precision the protocols report it in."""
    import argparse

    from almanak.cli_arguments import health_floor

    wad = int(health_floor(value) * 10**18)
    # A value that rounds down to 1 at wad precision is the liquidation line itself,
    # and one a uint256 cannot hold is no health factor a protocol reports.
    if not 10**18 < wad < 2**256:
        raise argparse.ArgumentTypeError(
            "expected a health factor above 1 at 18 decimals of precision"
        )
    return wad


def swap_arguments(action) -> None:
    """One pool by its address or its key, the asset sold, the exact amount and the price
    bound. `validate_arguments` holds the pool flags to what the connector takes."""
    action.add_argument("--chain", required=True, help="strict CAIP-2 chain of the pool")
    action.add_argument("--pool", help="pool address, for a connector whose pools are contracts")
    action.add_argument("--currency0", help="CAIP-19 currency0 of a Uniswap V4 pool key")
    action.add_argument("--currency1", help="CAIP-19 currency1 of a Uniswap V4 pool key")
    action.add_argument(
        "--fee", type=int, help="pool key fee in hundredths of a basis point (Uniswap V4)"
    )
    action.add_argument("--tick-spacing", type=int, help="pool key tick spacing (Uniswap V4)")
    action.add_argument(
        "--hooks",
        help="reviewed hook address of a Uniswap V4 pool key; omit for a hookless pool"
        " (a dynamic-fee hook takes --fee 8388608)",
    )
    action.add_argument(
        "--sell",
        type=asset,
        required=True,
        help="CAIP-19 asset sold, one of the pool's currencies; the native coin is"
        " held and sold as its wrapped token, also where a Uniswap V4 pool key names it",
    )
    action.add_argument(
        "--buy",
        type=asset,
        help="CAIP-19 asset bought, another of the pool's coins: required for curve,"
        " whose pools can hold more than two coins, and not accepted for a connector"
        " whose pools hold two currencies",
    )
    action.add_argument(
        "--amount", type=base_units, required=True, help="base units of the asset sold"
    )
    action.add_argument(
        "--slippage-bps",
        type=slippage_bps,
        required=True,
        help="refuse an output more than this far below the quote at the observed block,"
        " 1 to 9999 basis points",
    )
    action.add_argument(
        "--deadline-seconds",
        type=seconds,
        default=DEFAULT_DEADLINE_SECONDS,
        help="the swap is not signed later than this after the observed block, and it"
        " reverts when mined later than that; a curve call carries no deadline, so a"
        " signed curve swap stays valid until it is mined or its nonce is used, bound"
        f" only by its minimum output (default: {DEFAULT_DEADLINE_SECONDS})",
    )


def asset(value: str) -> str:
    """A CAIP-19 asset type in its canonical spelling, so it compares with resolved assets."""
    import argparse

    from almanak.caip import AssetType

    try:
        return str(AssetType(value))
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"expected a CAIP-19 asset type: {error}") from None


def slippage_bps(value: str) -> int:
    import argparse

    # 10000 would accept any output at all, which the allocation model refuses.
    if not (value.isascii() and value.isdigit()) or not 1 <= int(value) <= 9999:
        raise argparse.ArgumentTypeError("expected 1 to 9999 basis points")
    return int(value)


def seconds(value: str) -> int:
    return positive(value, "seconds")


def base_units(value: str) -> int:
    return positive(value, "base units")


def gas_units(value: str) -> int:
    return positive(value, "gas units")


def positive(value: str, unit: str) -> int:
    import argparse

    if not (value.isascii() and value.isdigit()) or int(value) <= 0:
        raise argparse.ArgumentTypeError(f"expected a positive whole number of {unit}")
    return int(value)
