"""The steps of an admitted HyperCore intent (blueprint 30, sections 4, 5 and 13).

The intents are phase 5's (`models.py`), the ones a backtest simulates in one step each.
In a fork run each runs as a sequence of steps, as a GMX order follows its approval. The
next one is chosen from the checkpoint's documents, the steps this decision has already
carried out (its outcomes) and, inside the gateway, the venue's current answers:

- `deposit`: approve CoreDepositWallet when the allowance is short, deposit, revoke any
  residual allowance once the deposit's request resolves, set standard mode (action 16)
  once per Safe, then move the credited USDC from spot to perp (action 7); the intent
  completes when that request resolves. An account's first deposit pays the 1 USDC
  activation fee (A7), so one of no more is refused before anything is sent;
- `set_leverage`: approve a fresh agent (action 9, D9) when none signs, then the
  configured cross leverage (`updateLeverage`); the intent completes when it applies;
- `increase_position`: approve a fresh agent when none signs or renewal is due, set the
  configured cross leverage when the market reads another, then the `Ioc` entry of
  `size` coins on `side`, priced from a fresh book;
- `close_position`: approve a fresh agent only when none signs, then a reduce-only `Ioc`
  exit of the position the strategy's own fills prove it holds;
- `withdraw`: move perp USDC to spot (action 7), then withdraw it to the Safe on HyperEVM
  (action 13); the intent completes when that resolves. Without an amount, everything
  withdrawable moves and all spot USDC is withdrawn; with one, exactly that amount does.

Teardown follows section 7 through the strategy's own decisions: close reduce-only, then
withdraw. Venue reads move, so a recorded step is checked to be a legal next one for the
documents (`validate_step`) rather than to equal a fresh plan.

Everything that depends on the intents' shapes is here, in `INTENTS` and the accessors
below it: the planner of each kind, the steps it admits and the one that completes it.
"""

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

from almanak.connectors.hyperliquid import agents as agent_rules
from almanak.connectors.hyperliquid import lifecycle, venue_reads
from almanak.connectors.hyperliquid import trading as rules
from almanak.connectors.hyperliquid.operations import (
    AgentApproval,
    Approve,
    ClassTransfer,
    Deposit,
    StandardMode,
    VenueAction,
    Withdrawal,
)
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_signing import OrderAction, UpdateLeverage
from almanak.models import AssetAmount

ZERO = Decimal(0)
# HyperCore's fee on an account's first credit, in USDC base units (A7, canary T7).
ACTIVATION_FEE = 1_000_000


@dataclass(frozen=True)
class Steps:
    """One intent kind: its planner, the check of each step it admits, and whether a
    step's resolution completes it (given whether that step applied)."""

    plan: Callable
    legal: dict[type, Callable]
    completes: Callable[[object, bool], bool]


def operation(backend, config, state, intent, before):
    target = before.target
    if intent.target != target or target != config.target(target.allocation):
        raise ValueError("Hyperliquid planning requires its exact configured intent target")
    if state.decision is None or state.decision.intent != intent:
        raise ValueError("Hyperliquid planning requires the durably retained decision")
    return steps(intent).plan(backend, state, intent, before)


def validate_step(state, intent, operation) -> None:
    """Refuse an operation that is no legal next step of `intent` by the documents."""
    legal = steps(intent).legal.get(type(operation))
    if legal is None:
        raise ValueError(f"{operation.kind} is no step of a {intent.kind} intent")
    legal(state, intent, operation)


def completes(intent, operation, applied: bool = True) -> bool:
    """Whether the step's resolution completes `intent`; any other step keeps it. A custody
    request always applied; a venue step says whether it did."""
    return steps(intent).completes(operation, applied)


def steps(intent) -> Steps:
    found = INTENTS.get(intent.kind)
    if found is None:
        raise ValueError("unsupported Hyperliquid economic intent")
    return found


# The intents' fields, read in one place.


def deposit_amount(intent) -> int:
    return intent.amount


def withdrawal_amount(intent) -> int | None:
    """The USDC base units a withdrawal returns, or None for everything withdrawable."""
    return intent.amount


def entry_order(intent) -> tuple[bool, Decimal]:
    """Whether the entry buys (a long), and its size in coin units."""
    return intent.side == "long", Decimal(intent.size)


# What this decision has done.


def decided(state) -> tuple:
    """The operations this decision has carried out, in order: each step whose successful
    outcome retains the active decision. A custody step appears twice, once mined and once
    resolved."""
    identity = state.decision.identity
    return tuple(
        o.operation
        for o in state.outcomes
        if getattr(o, "decision", None) is not None
        and o.decision.identity == identity
        and o.success
    )


def deposited(done) -> bool:
    return any(isinstance(o, Deposit) for o in done)


def moved(done, to_perp: bool) -> bool:
    return any(isinstance(o, ClassTransfer) and o.to_perp is to_perp for o in done)


def withdrawn(done) -> bool:
    return any(isinstance(o, Withdrawal) for o in done)


def standard(custody) -> bool:
    """Whether the Safe's standard mode request has resolved: action 16 is sent once."""
    return any(r.kind == "standard_mode" and r.status == "resolved" for r in custody.requests)


def require_standard(custody) -> None:
    """Agents and trades need spot and perp kept apart (section 4.1); a deposit sets it."""
    if not standard(custody):
        raise DataError(
            "not_ready",
            "the Safe's HyperCore account is not in standard mode; deposit USDC to it first",
        )


def usdc(target, amount: int) -> AssetAmount:
    return AssetAmount(asset=target.margin_asset, amount=amount)


def require_inventory(before, amount: int) -> None:
    (owned,), (wallet,) = before.owned.inventory, before.wallets
    if amount > owned.amount or amount > wallet.amount:
        raise DataError(
            "inventory",
            "the Safe's owned USDC on HyperEVM does not cover the deposit",
            details={"deposit": amount, "owned": owned.amount, "wallet": wallet.amount},
        )


def require_activation(custody, amount: int) -> None:
    """A first deposit pays the activation fee out of its own credit, so it must exceed it;
    the Safe is dedicated, so its first deposit request is the account's first credit."""
    if amount <= ACTIVATION_FEE and not any(r.kind == "deposit" for r in custody.requests):
        raise DataError(
            "inventory",
            "a first deposit must exceed HyperCore's 1 USDC activation fee",
            details={"deposit": amount, "activation_fee": ACTIVATION_FEE},
        )


# Deposit.


def deposit(backend, state, intent, before):
    target, amount, done = before.target, deposit_amount(intent), decided(state)
    if not deposited(done):
        require_inventory(before, amount)
        require_activation(state.custody, amount)
        if before.allowance < amount:
            return Approve(target=target, amount=usdc(target, amount))
        return Deposit(target=target, amount=usdc(target, amount))
    if before.allowance:
        return Approve(target=target, amount=usdc(target, 0))
    if not standard(state.custody):
        return StandardMode(target=target)
    if moved(done, True):
        raise ValueError("the deposit has already moved its credit to perp")
    # The first deposit pays HyperCore's 1 USDC activation fee out of its own credit.
    credited = min(amount, venue_reads.spot_usdc(backend.venue, target.fund.address))
    if credited <= 0:
        raise DataError("inventory", "the deposit left no spot USDC to move to perp")
    return ClassTransfer(target=target, amount=usdc(target, credited), to_perp=True)


def legal_approve(state, intent, operation):
    expected = 0 if deposited(decided(state)) else deposit_amount(intent)
    if operation.amount.amount != expected:
        raise ValueError("a deposit is approved before it and its residual revoked after")


def legal_deposit(state, intent, operation):
    if deposited(decided(state)) or operation.amount.amount != deposit_amount(intent):
        raise ValueError("a deposit sends its own amount once")
    require_activation(state.custody, deposit_amount(intent))


def legal_standard_mode(state, intent, operation):
    if not deposited(decided(state)) or standard(state.custody):
        raise ValueError("standard mode is set once, after the first deposit's credit")


def legal_deposit_transfer(state, intent, operation):
    done = decided(state)
    ready = deposited(done) and standard(state.custody) and not moved(done, True)
    if not (operation.to_perp and ready):
        raise ValueError("a deposit moves its credit to perp once, in standard mode")
    if operation.amount.amount > deposit_amount(intent):
        raise ValueError("a deposit moves no more than it deposited to perp")


# Withdraw.


def withdraw(backend, state, intent, before):
    target, done, safe = before.target, decided(state), before.target.fund.address
    amount = withdrawal_amount(intent)
    if withdrawn(done):
        raise ValueError("the withdrawal has already been sent")
    if not moved(done, False):
        perp = venue_reads.withdrawable(backend.venue, safe)
        if amount is not None:
            if amount > perp:
                raise DataError(
                    "inventory",
                    "the withdrawal exceeds the Safe's withdrawable perp USDC",
                    details={"withdrawal": amount, "withdrawable": perp},
                )
            return ClassTransfer(target=target, amount=usdc(target, amount), to_perp=False)
        if perp > 0:
            return ClassTransfer(target=target, amount=usdc(target, perp), to_perp=False)
    spot = venue_reads.spot_usdc(backend.venue, safe)
    if amount is not None and spot < amount:
        raise DataError(
            "inventory",
            "the Safe's spot USDC no longer covers the withdrawal",
            details={"withdrawal": amount, "spot": spot},
        )
    if spot <= 0:
        raise DataError("inventory", "no HyperCore USDC remains to withdraw to the Safe")
    require_payout_fee(backend, before)
    return Withdrawal(target=target, amount=usdc(target, spot if amount is None else amount))


# A withdrawal's payout is a HyperEVM system transaction of this much gas at the base fee,
# which HyperCore charges to the Safe's HyperCore HYPE (section 7, the canary's
# `nativeTokenFee`).
PAYOUT_GAS = 200_000
WEI_PER_HYPE = Decimal(10**18)


def require_payout_fee(backend, before) -> None:
    """HyperCore drops a withdrawal whose payout its account cannot pay for, leaving no
    trace and the request unresolved, so one is planned only while the Safe's HyperCore HYPE
    covers twice the fee at the planning block's base fee (section 14.2)."""
    required = 2 * PAYOUT_GAS * backend.base_fee(before.block) / WEI_PER_HYPE
    held = venue_reads.spot_hype(backend.venue, before.target.fund.address)
    if held < required:
        raise DataError(
            "funding_unavailable",
            "the Safe's HyperCore HYPE cannot pay for the withdrawal's HyperEVM payout: the "
            "Safe owners send HYPE to its HyperCore account, then the run resumes",
            details={"hypercore_hype": str(held), "required": str(required)},
        )


def legal_withdraw_transfer(state, intent, operation):
    done = decided(state)
    if operation.to_perp or moved(done, False) or withdrawn(done):
        raise ValueError("a withdrawal moves perp USDC to spot once, before it is sent")
    amount = withdrawal_amount(intent)
    if amount is not None and operation.amount.amount != amount:
        raise ValueError("a withdrawal of an amount moves exactly that amount to spot")


def legal_withdrawal(state, intent, operation):
    done = decided(state)
    if withdrawn(done):
        raise ValueError("a withdrawal is sent once")
    amount = withdrawal_amount(intent)
    if amount is not None and (operation.amount.amount != amount or not moved(done, False)):
        raise ValueError("a withdrawal of an amount sends exactly what it moved to spot")


# Agents.


def approval(backend, state, before, now: int, renew: bool):
    """A fresh agent's approval when one is due: none signs, or (for an entry) renewal is
    due. The gateway generates the key and exposes only its address (D9)."""
    agents = state.custody.agents
    if not agent_rules.due(agents, now) or (not renew and agents.current(now) is not None):
        return None
    require_standard(state.custody)
    fresh = backend.agent_keys(before.target.account).generate()
    _, label = agent_rules.prepare(agents, fresh, now)
    return AgentApproval(target=before.target, agent=fresh, label=label)


def signing_agent(agents, now: int) -> str:
    current = agents.current(now)
    if current is not None:
        return current.address
    if agents.outstanding is None and not agent_rules.due(agents, now):
        # HyperCore drops a second action 9 sent within seconds of the last (T8).
        raise DataError(
            "not_ready",
            "the next agent approval waits for the spacing after the last one",
            transient=True,
        )
    return agent_rules.require_signing(agents, now).address


def legal_agent_approval(state, intent, operation):
    """The approval of a fresh address, due at its plan's own time: the label's expiry fixes
    that time, and `lifecycle.prepared` retains it exactly as planning prepared it."""
    require_standard(state.custody)
    lifecycle.prepared(state.custody.agents, operation.agent, operation.label)


# Venue actions.


def leverage(backend, state, intent, before):
    """The configured cross leverage, whatever the market reads now: `updateLeverage` to
    the same value applies again, and the intent completes only once it has applied."""
    now, target = backend.venue.now(), before.target
    step = approval(backend, state, before, now, renew=False)
    if step is not None:
        return step
    require_standard(state.custody)
    agent = signing_agent(state.custody.agents, now)
    _, attempt = rules.plan_leverage(state.trading, target.market.coin, agent, now)
    return VenueAction(target=target, attempt=attempt)


def entry(backend, state, intent, before):
    now, target = backend.venue.now(), before.target
    step = approval(backend, state, before, now, renew=True)
    if step is not None:
        return step
    require_standard(state.custody)
    agent = signing_agent(state.custody.agents, now)
    coin, allocation = target.market.coin, state.config.allocation(target.allocation)
    applied = venue_reads.leverage(backend.venue, target.fund.address, coin)
    if applied != ("cross", allocation.leverage):
        _, attempt = rules.plan_leverage(state.trading, coin, agent, now)
        return VenueAction(target=target, attempt=attempt)
    is_buy, size = entry_order(intent)
    require_size(intent)
    book = venue_reads.book(backend.venue, coin)
    _, attempt = rules.plan_entry(state.trading, coin, agent, is_buy, size, book, now)
    return VenueAction(target=target, attempt=attempt)


def require_size(intent) -> Decimal:
    """The entry's size at the market's size decimals; one that rounds to nothing is
    refused before anything is signed, as a backtest refuses it."""
    _, size = entry_order(intent)
    try:
        return rules.order_size(size, intent.target.market.size_decimals)
    except ValueError as refusal:
        raise DataError("order_size", str(refusal), details={"size": intent.size}) from None


def close(backend, state, intent, before):
    now, target = backend.venue.now(), before.target
    step = approval(backend, state, before, now, renew=False)
    if step is not None:
        return step
    agent = signing_agent(state.custody.agents, now)
    coin = target.market.coin
    # The strategy's own exposure, proven from its fills since the book's cursor; admission
    # proves it again at the account snapshot that bounds the exit.
    owned = venue_reads.trader(backend.venue, state).owned_exposure().owned.get(coin, ZERO)
    if owned == 0:
        raise DataError(
            "stale_plan",
            "the strategy's fills hold no position to close; its book is reconciled before "
            "the next decision",
        )
    book = venue_reads.book(backend.venue, coin)
    _, attempt = rules.plan_exit(state.trading, coin, agent, owned, book, now)
    return VenueAction(target=target, attempt=attempt)


def legal_venue_action(purposes: tuple[str, ...]):
    def legal(state, intent, operation):
        attempt = operation.attempt
        if attempt.purpose not in purposes:
            raise ValueError(f"a {intent.kind} intent signs no {attempt.purpose} action")
        require_standard(state.custody)
        current = state.custody.agents.current(attempt.request.nonce)
        if current is None or current.address != attempt.agent:
            raise DataError("authority", "the attempt is not signed by the Safe's current agent")
        lifecycle.inserted(state.trading, attempt)
        if attempt.purpose == "entry":
            require_entry_order(intent, attempt)
        if attempt.purpose == "leverage":
            require_leverage(state, intent, attempt)

    return legal


def require_entry_order(intent, attempt) -> None:
    action = attempt.request.action
    is_buy, _ = entry_order(intent)
    rounded = require_size(intent)
    if not isinstance(action, OrderAction) or (action.order.is_buy, action.order.size) != (
        is_buy,
        rounded,
    ):
        raise ValueError("the entry differs from its intent's side or size")


def require_leverage(state, intent, attempt) -> None:
    """Cross leverage at the configured value, which a `set_leverage` intent names too."""
    action = attempt.request.action
    configured = state.config.allocation(intent.target.allocation).leverage
    wanted = intent.leverage if intent.kind == "set_leverage" else configured
    if not isinstance(action, UpdateLeverage) or (action.is_cross, action.leverage) != (
        True,
        configured,
    ):
        raise ValueError("the leverage step sets other than the configured cross leverage")
    if wanted != configured:
        raise ValueError("HyperCore leverage differs from the configured cross leverage")


def venue_completes(final: tuple[str, ...], applied_only: bool = False):
    """A venue step completes its intent when its purpose is the intent's own action; a
    leverage step before an entry does not. An `Ioc` order that matched nothing still
    completes its entry or exit, unapplied; a leverage intent waits until it applies."""
    return lambda operation, applied: (
        isinstance(operation, VenueAction)
        and operation.attempt.purpose in final
        and (applied or not applied_only)
    )


INTENTS = {
    "deposit": Steps(
        plan=deposit,
        legal={
            Approve: legal_approve,
            Deposit: legal_deposit,
            StandardMode: legal_standard_mode,
            ClassTransfer: legal_deposit_transfer,
        },
        completes=lambda operation, applied: isinstance(operation, ClassTransfer),
    ),
    "set_leverage": Steps(
        plan=leverage,
        legal={
            AgentApproval: legal_agent_approval,
            VenueAction: legal_venue_action(("leverage",)),
        },
        completes=venue_completes(("leverage",), applied_only=True),
    ),
    "increase_position": Steps(
        plan=entry,
        legal={
            AgentApproval: legal_agent_approval,
            VenueAction: legal_venue_action(("leverage", "entry")),
        },
        completes=venue_completes(("entry",)),
    ),
    "close_position": Steps(
        plan=close,
        legal={AgentApproval: legal_agent_approval, VenueAction: legal_venue_action(("exit",))},
        completes=venue_completes(("exit",)),
    ),
    "withdraw": Steps(
        plan=withdraw,
        legal={ClassTransfer: legal_withdraw_transfer, Withdrawal: legal_withdrawal},
        completes=lambda operation, applied: isinstance(operation, Withdrawal),
    ),
}
