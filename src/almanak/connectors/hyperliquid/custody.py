"""The Safe's HyperCore custody: retained requests, agents and their binding.

Every custody call the operator makes as the Safe becomes a request retained from its
receipt and resolved only by its own effect (`custody_requests.py`). Agent approvals
follow D9 (`agents.py`). `CustodyRequests` has the shape of the connector `Requests`
seam: the runtime reconciles outstanding custody work before further work, and its
fork keeper is the synthesized HyperCore model (blueprint 30, section 9).

Ordering rules from section 4.1: no CoreWriter action before `userRole` reports the Safe
as a user, because an action from an account HyperCore did not hold before the block
is dropped; no class transfer or agent approval before `userAbstraction` reads
`disabled`, the standard mode action 16 sets.
"""

from typing import Annotated, Protocol

from eth_abi import decode
from pydantic import Field

from almanak.connectors.hyperliquid import agents as agent_rules
from almanak.connectors.hyperliquid.agents import Agents, Submission
from almanak.connectors.hyperliquid.assets import CORE_DEPOSIT_WALLET
from almanak.connectors.hyperliquid.corewriter import (
    ADD_API_WALLET,
    CORE_PER_EVM_USDC,
    CORE_WRITER,
    SEND_ASSET,
    USD_CLASS_TRANSFER,
    USDC_SYSTEM,
    CoreCall,
    agent_approval,
    decoded,
)
from almanak.connectors.hyperliquid.custody_evidence import RAW_ACTION, TRANSFER, Reads, topic
from almanak.connectors.hyperliquid.custody_requests import (
    ClassTransfer,
    CreditLog,
    CustodyRequest,
    Deposit,
    StandardMode,
    Withdrawal,
    attributed,
)
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_info import ExtraAgents, InfoRequest, InfoResult, typed
from almanak.models import Address, Model


class CustodyPolicy(Model):
    # A custody request with no identified effect this long after its block halts.
    window: Annotated[int, Field(strict=True, gt=0)] = 120 * 1000


class Custody(Model):
    safe: Address
    agents: Agents = Agents()
    requests: tuple[CustodyRequest, ...] = ()
    policy: CustodyPolicy = CustodyPolicy()


class AgentKeys(Protocol):
    """The gateway's agent keys for one deployment; only addresses leave it."""

    def generate(self) -> str: ...

    def held(self, addresses: frozenset[str]) -> frozenset[str]:
        """Which of `addresses` it holds a key for, each read from durable storage and
        checked to derive its address; a key it stores but cannot verify fails."""
        ...


class Backend(Protocol):
    reads: Reads
    keys: AgentKeys

    def now(self) -> int: ...


def submission(receipt: dict, timestamp: int) -> Submission:
    return Submission(
        transaction=receipt["transactionHash"],
        block=int(receipt["blockNumber"], 16),
        time=timestamp * 1000,
    )


def raw_actions(receipt: dict, safe: str) -> list[tuple[int, bytes]]:
    """The Safe's `RawAction` payloads in a receipt, with their log indices."""
    found = []
    for log in receipt["logs"]:
        if (log["address"].lower(), log["topics"][0]) != (CORE_WRITER, RAW_ACTION):
            continue
        if log["topics"][1] != topic(safe):
            continue
        (raw,) = decode(("bytes",), bytes.fromhex(log["data"][2:]))
        found.append((int(log["logIndex"], 16), raw))
    return found


def queued(receipt: dict, safe: str, call: CoreCall) -> tuple[str, str]:
    """The key and payload of the one `RawAction` the receipt shows for `call`."""
    (expected,) = decode(("bytes",), bytes.fromhex(call.data[10:]))
    matching = [index for index, raw in raw_actions(receipt, safe) if raw == expected]
    if len(matching) != 1:
        raise DataError("receipt", "custody receipt lacks exactly one RawAction for its call")
    return f"{receipt['transactionHash']}:{matching[0]}", "0x" + expected.hex()


def credit(receipt: dict, safe: str, amount: int) -> CreditLog:
    """The receipt's CoreDepositWallet credit of the Safe's own deposit."""
    logs = [
        log
        for log in receipt["logs"]
        if log["address"].lower() == CORE_DEPOSIT_WALLET
        and log["topics"][:3] == [TRANSFER, topic(safe), topic(USDC_SYSTEM)]
    ]
    if len(logs) != 1 or int(logs[0]["data"], 16) != amount:
        raise DataError("receipt", "deposit receipt lacks its exact CoreDepositWallet credit")
    return CreditLog(
        transaction=receipt["transactionHash"],
        log_index=int(logs[0]["logIndex"], 16),
        block=int(receipt["blockNumber"], 16),
        recipient=safe,
        amount=amount,
    )


def record_deposit(custody: Custody, receipt: dict, timestamp: int, amount: int) -> Custody:
    found = credit(receipt, custody.safe, amount)
    request = Deposit(
        key=f"{found.transaction}:{found.log_index}",
        safe=custody.safe,
        submission=submission(receipt, timestamp),
        amount=amount,
        credit=found,
    )
    return custody.model_copy(update={"requests": (*custody.requests, request)})


def record_action(custody: Custody, call: CoreCall, receipt: dict, timestamp: int) -> Custody:
    """Retain the request a CoreWriter call queued, from its receipt."""
    key, payload = queued(receipt, custody.safe, call)
    action = decoded(bytes.fromhex(payload[2:]))
    if action is None:
        raise ValueError("custody call is not a CoreWriter action this binding encodes")
    queued_at = submission(receipt, timestamp)
    number, fields = action
    if number == ADD_API_WALLET:
        agents = agent_rules.submitted(custody.agents, queued_at, *fields)
        return custody.model_copy(update={"agents": agents})
    common = {"key": key, "safe": custody.safe, "submission": queued_at}
    request = request_for(number, fields, payload, common)
    return custody.model_copy(update={"requests": (*custody.requests, request)})


def request_for(number: int, fields: tuple, payload: str, common: dict):
    if number == USD_CLASS_TRANSFER:
        return ClassTransfer(**common, amount=fields[0], to_perp=fields[1], payload=payload)
    if number == SEND_ASSET:
        # HyperCore units (8 decimals for USDC) back to USDC base units.
        return Withdrawal(**common, amount=fields[5] // CORE_PER_EVM_USDC, payload=payload)
    return StandardMode(**common, payload=payload)


def approve_agent(custody: Custody, keys: AgentKeys, now: int) -> tuple[Custody, CoreCall]:
    """Prepare the approval of a fresh agent key: the first, a renewal or a retry."""
    fresh = keys.generate()
    agents, label = agent_rules.prepare(custody.agents, fresh, now)
    return custody.model_copy(update={"agents": agents}), agent_approval(fresh, label)


def require_activated(role: InfoResult, safe: str) -> None:
    """The Safe's first deposit has created its HyperCore account."""
    if role.request.info.type != "userRole" or role.request.info.user != safe:
        raise ValueError("activation is read from the Safe's own userRole")
    if typed(role).role != "user":
        raise DataError("not_ready", "the Safe's HyperCore account is not activated")


def require_standard_mode(abstraction: InfoResult, safe: str) -> None:
    if abstraction.request.info.type != "userAbstraction" or abstraction.request.info.user != safe:
        raise ValueError("the account mode is read from the Safe's own userAbstraction")
    if typed(abstraction) != "disabled":
        raise DataError("not_ready", "the Safe's HyperCore account is not in standard mode")


def reconcile(backend: Backend, custody: Custody) -> Custody | None:
    """Resolve what the evidence identifies; None when nothing was outstanding."""
    if not CustodyRequests.unresolved(custody):
        return None
    now, window = backend.now(), custody.policy.window
    requests = tuple(
        r
        if r.status == "resolved"
        else attributed(r, backend.reads.evidence(r, window), now, window)
        for r in custody.requests
    )
    agents = custody.agents
    if agents.outstanding is not None and agents.outstanding.submission is not None:
        agents = agent_rules.resolve(agents, extra_agents(backend, custody.safe), now)
    return custody.model_copy(update={"requests": requests, "agents": agents})


def with_resolution(custody: Custody, request=None, approval=None) -> Custody:
    """The custody document with one request or agent approval resolved, as `reconcile`
    resolves it: what a custody outcome carries, so it never repeats settled history."""
    if approval is not None:
        return custody.model_copy(update={"agents": agent_rules.settled(custody.agents, approval)})
    if not any(r.key == request.key and r.status == "unresolved" for r in custody.requests):
        raise ValueError("only an outstanding custody request resolves")
    requests = tuple(request if r.key == request.key else r for r in custody.requests)
    return custody.model_copy(update={"requests": requests})


def extra_agents(backend: Backend, safe: str) -> InfoResult:
    return backend.reads.info.fetch(InfoRequest(info=ExtraAgents(user=safe)))


class CustodyRequests:
    @staticmethod
    def unresolved(custody: Custody) -> bool:
        outstanding = custody.agents.outstanding
        return any(r.status == "unresolved" for r in custody.requests) or (
            outstanding is not None and outstanding.submission is not None
        )

    reconcile = staticmethod(reconcile)

    @staticmethod
    def audit(backend: Backend, custody: Custody) -> None:
        """Installed agent authority: every agent HyperCore lists under the deployment's
        names is a key the gateway holds for it."""
        listed = extra_agents(backend, custody.safe)
        held = backend.keys.held(agent_rules.listed(custody.agents, listed))
        agent_rules.verify(custody.agents, listed, held)

    @staticmethod
    def fork_keeper(forks, output):
        """Fork tests only: the synthesized HyperCore model of blueprint 30, section 9."""
        from almanak.connectors.hyperliquid.fork_model import Keeper

        return Keeper.open(forks, output)

    @staticmethod
    def cancellation_plan(backend, state, attempt_id, *, reference=None):
        # A queued CoreWriter action cannot be withdrawn.
        return None

    @staticmethod
    def is_cancellation(state, operation) -> bool:
        return False
