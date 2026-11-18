"""The Safe's HyperCore agents under D9 (blueprint 30, section 4.2.1).

Each approval is CoreWriter action 9 for a fresh key the gateway generated and
never published, under one of the deployment's two agent names. The mainnet canary
settled what this rests on: action 9 applies only to an address HyperCore has never
seen (T11), so no address is approved twice; an approval under an existing agent's name
replaces that agent at once, and one HyperCore drops replaces nothing (T15). Renewal
therefore goes under the name the current agent does not hold, and the current agent
keeps signing until `extraAgents` lists the new address with its requested expiry.
`userRole` cannot show that: it names the same Safe for every agent and lags a
replacement. The empty-name main agent is never used, because `extraAgents` does not
list it.
"""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_info import InfoResult, typed
from almanak.gateway.hypercore_shapes import Millis
from almanak.models import Address, Hash32, Model, UInt

# Hyperliquid accepts an expiry at most 180 days ahead.
MAXIMUM_LIFETIME_MS = 180 * 24 * 3600 * 1000
SUFFIX = " valid_until "
# Millisecond expiries have 13 digits from 2001 to 2286, so a label's length is fixed
# by its name and the grant can pin it.
EXPIRY_DIGITS = 13
AgentName = Annotated[str, Field(pattern=r"^[a-z0-9-]{1,8}$")]


class AgentNames(Model):
    """The deployment's two agent names; the action 9 grant admits only these.

    Equal lengths give every approval label one length, which the grant pins. Eight
    characters is the longest name the canary approved with an expiry suffix.
    """

    first: AgentName = "almanak1"
    second: AgentName = "almanak2"

    @model_validator(mode="after")
    def distinct(self):
        if self.first == self.second or len(self.first) != len(self.second):
            raise ValueError("agent names must be two distinct names of one length")
        return self

    @property
    def both(self) -> tuple[str, str]:
        return self.first, self.second

    def prefix(self, name: str) -> bytes:
        """The label bytes before the expiry digits."""
        return (name + SUFFIX).encode()

    @property
    def label_length(self) -> int:
        return len(self.prefix(self.first)) + EXPIRY_DIGITS

    def label(self, name: str, valid_until: int) -> str:
        if name not in self.both:
            raise ValueError("an approval names one of the deployment's two agent names")
        if len(str(valid_until)) != EXPIRY_DIGITS:
            raise ValueError("an approval expiry is a 13-digit millisecond time")
        return f"{name}{SUFFIX}{valid_until}"

    def owns(self, listed: str) -> bool:
        """Whether a name `extraAgents` lists falls under the deployment's names.

        HyperCore stores a label without its parsed expiry suffix. A label whose suffix
        did not parse keeps it, so any listed name starting with one of ours counts.
        """
        return any(listed == name or listed.startswith(name + " ") for name in self.both)


class AgentPolicy(Model):
    """Connector-declared approval timing, in milliseconds."""

    lifetime: Annotated[int, Field(strict=True, gt=0, le=MAXIMUM_LIFETIME_MS)] = (
        170 * 24 * 3600 * 1000
    )
    # Renewal starts this long before the current approval expires.
    renewal_lead: Annotated[int, Field(strict=True, gt=0)] = 14 * 24 * 3600 * 1000
    # An approval `extraAgents` does not list this long after its block counts as
    # dropped; CoreWriter approvals applied within the second on mainnet (A8).
    window: Annotated[int, Field(strict=True, gt=0)] = 120 * 1000
    # HyperCore drops a second action 9 from one account sent within seconds of its
    # previous one, in the same transaction or two seconds later (T8, canary
    # wind-down), so approvals are at least this far apart. A dropped approval is
    # therefore not by itself evidence of griefing.
    spacing: Annotated[int, Field(strict=True, gt=0)] = 60 * 1000

    @model_validator(mode="after")
    def ordered(self):
        if self.renewal_lead >= self.lifetime:
            raise ValueError("renewal must start inside an approval's lifetime")
        return self


class Submission(Model):
    """The HyperEVM transaction that queued an action, and its block's time."""

    transaction: Hash32
    block: UInt
    # Block time in milliseconds: HyperCore writes an applied action's entries in it.
    time: Millis


class Approval(Model):
    address: Address
    name: AgentName
    valid_until: Millis
    # `replaced`: a later approval under the same name took its place on HyperCore.
    status: Literal["prepared", "requested", "active", "dropped", "replaced"]
    submission: Submission | None = None
    # The `extraAgents` answer that resolved or dropped it.
    evidence: InfoResult | None = None


class Agents(Model):
    names: AgentNames = AgentNames()
    policy: AgentPolicy = AgentPolicy()
    # Every address ever prepared, in order; none is ever approved again.
    approvals: tuple[Approval, ...] = ()

    @model_validator(mode="after")
    def unique(self):
        addresses = [a.address for a in self.approvals]
        if len(set(addresses)) != len(addresses):
            raise ValueError("an agent address is approved at most once")
        if sum(a.status in ("prepared", "requested") for a in self.approvals) > 1:
            raise ValueError("at most one agent approval is outstanding")
        return self

    @property
    def outstanding(self) -> Approval | None:
        return next((a for a in self.approvals if a.status in ("prepared", "requested")), None)

    def current(self, now: int) -> Approval | None:
        """The signing agent: the latest active approval that has not expired."""
        active = [a for a in self.approvals if a.status == "active" and a.valid_until > now]
        return active[-1] if active else None


def next_name(agents: Agents, now: int) -> str:
    """The name the current agent does not hold: approving under its own name would
    replace it at once and refuse its actions still in flight (T15)."""
    current = agents.current(now)
    if current is None:
        return agents.names.first
    return agents.names.second if current.name == agents.names.first else agents.names.first


def due(agents: Agents, now: int) -> bool:
    """Whether a fresh key should be approved now: none signs, or renewal is due, and the
    last approval was queued at least the spacing ago. Each approval is one action 9 in
    a transaction of its own."""
    if agents.outstanding is not None:
        return False
    queued = [a.submission.time for a in agents.approvals if a.submission is not None]
    if queued and now < max(queued) + agents.policy.spacing:
        return False
    current = agents.current(now)
    return current is None or now >= current.valid_until - agents.policy.renewal_lead


def prepare(agents: Agents, fresh: str, now: int) -> tuple[Agents, str]:
    """Retain the approval of `fresh` before its action 9 is signed; returns the label."""
    if not due(agents, now):
        raise ValueError("no agent approval is due")
    if any(a.address == fresh.lower() for a in agents.approvals):
        raise ValueError("an agent address is never approved twice")
    name = next_name(agents, now)
    valid_until = now + agents.policy.lifetime
    approval = Approval(address=fresh, name=name, valid_until=valid_until, status="prepared")
    label = agents.names.label(name, valid_until)
    return agents.model_copy(update={"approvals": (*agents.approvals, approval)}), label


def submitted(agents: Agents, submission: Submission, agent: str, label: str) -> Agents:
    """The prepared approval's action 9, `(agent, label)` as queued, is in a block."""
    approval = agents.outstanding
    if approval is None or approval.status != "prepared":
        raise ValueError("no prepared agent approval awaits its transaction")
    if (agent.lower(), label) != (
        approval.address,
        agents.names.label(approval.name, approval.valid_until),
    ):
        raise ValueError("the queued action 9 is not the prepared approval")
    changed = approval.model_copy(update={"status": "requested", "submission": submission})
    return replaced(agents, approval, changed)


def resolve(agents: Agents, result: InfoResult, now: int) -> Agents:
    """Resolve the requested approval from one `extraAgents` answer of the Safe.

    It applies only when the answer lists its address under its name with the requested
    expiry. Unlisted after the window, it is dropped, which replaced nothing (T15); the
    caller then approves another fresh key, never this address again.
    """
    if result.request.info.type != "extraAgents":
        raise ValueError("an agent approval resolves only from extraAgents")
    approval = agents.outstanding
    if approval is None or approval.submission is None:
        return agents
    listed = {(a.name, a.address, a.validUntil) for a in typed(result)}
    if (approval.name, approval.address, approval.valid_until) in listed:
        status = "active"
    elif result.received_at >= approval.submission.time + agents.policy.window and (
        now >= approval.submission.time + agents.policy.window
    ):
        status = "dropped"
    else:
        return agents
    return settled(agents, approval.model_copy(update={"status": status, "evidence": result}))


def settled(agents: Agents, changed: Approval) -> Agents:
    """The approvals with the outstanding one resolved as `changed`: an active one takes
    the place of the earlier active agent of its name."""
    outstanding = agents.outstanding
    if outstanding is None or outstanding.address != changed.address:
        raise ValueError("only the outstanding agent approval resolves")
    agents = replaced(agents, outstanding, changed)
    if changed.status == "active":
        for old in agents.approvals:
            if (
                old.status == "active"
                and old.name == changed.name
                and old.address != changed.address
            ):
                agents = replaced(agents, old, old.model_copy(update={"status": "replaced"}))
    return agents


def require_signing(agents: Agents, now: int) -> Approval:
    """The agent that signs now, or a halt: an expired approval whose renewal never
    resolved stops trading with its evidence."""
    current = agents.current(now)
    if current is None:
        evidence = [a.model_dump(mode="json") for a in agents.approvals]
        raise DataError(
            "authority",
            "no unexpired HyperCore agent approval has resolved",
            details={"approvals": evidence},
        )
    return current


def listed(agents: Agents, result: InfoResult) -> frozenset[str]:
    """The addresses `extraAgents` lists under the deployment's names: the ones whose keys
    the installed-authority audit asks the key holder to verify."""
    if result.request.info.type != "extraAgents":
        raise ValueError("agent authority is verified from extraAgents")
    return frozenset(a.address for a in typed(result) if agents.names.owns(a.name))


def verify(agents: Agents, result: InfoResult, held: frozenset[str]) -> None:
    """Halt on any agent `extraAgents` lists under the deployment's names whose key the
    gateway does not hold for the deployment."""
    if result.request.info.type != "extraAgents":
        raise ValueError("agent authority is verified from extraAgents")
    foreign = sorted(
        a.address for a in typed(result) if agents.names.owns(a.name) and a.address not in held
    )
    if foreign:
        raise DataError(
            "authority",
            "HyperCore lists agents under this deployment's names it does not hold",
            details={"agents": foreign, "evidence": result.model_dump(mode="json")},
        )


def replaced(agents: Agents, old: Approval, new: Approval) -> Agents:
    # Addresses are unique, so one names its approval.
    approvals = tuple(new if a.address == old.address else a for a in agents.approvals)
    return agents.model_copy(update={"approvals": approvals})
