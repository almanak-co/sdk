"""Across deposits through the shared guarded EVM execution, account and fee path."""

from almanak.connectors.across import planning
from almanak.connectors.across import quote as venue
from almanak.connectors.across.observations import require_pinned, snapshot
from almanak.connectors.across.operations import (
    ApproveOperation,
    DepositOperation,
    RevokeOperation,
    plan_call,
)
from almanak.connectors.across.receipts import movement, opened_transfer
from almanak.connectors.erc20_approval import approval_succeeded
from almanak.connectors.funding import funding_rows, summed
from almanak.connectors.gas_lifecycle import GasLifecycle, KindCounts
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_fees import charged_fees
from almanak.gateway.production_config import execution_gas
from almanak.models import AssetAmount

API_KEY_ENV = venue.API_KEY_ENV
# Approval and deposit; a bridge has no exit transaction of its own.
ENTRY_TRANSACTIONS = 2


def increases_exposure(config, operation):
    return isinstance(operation, (ApproveOperation, DepositOperation))


def expired(operation, block) -> bool:
    """Whether the SpokePool would revert the deposit at `block` for its quote's age."""
    return isinstance(operation, DepositOperation) and block.timestamp > operation.deadline


def quoted(backend, state, intent, before):
    """The deposit's quote through the one transport the gateway declares for it.

    Which transport that is belongs to the gateway's environment, not to this code: a
    gateway that declares none is refused, and nothing is substituted. The venue is
    asked once for a decision's amount at a pinned block: preparing that step again, as
    the verification of its reserved plan does, reads this gateway's own answer.
    """
    transport = backend.venue_transports.get(venue.QUOTE_VENUE)
    if transport is None:
        raise DataError("quote_unavailable", "this gateway declares no Across quote transport")
    target, amount = intent.target, intent.amount.amount
    key = (venue.QUOTE_VENUE, state.decision.identity, before.block.hash, target, amount)
    return backend.venue_answers.once(
        key, lambda: venue.fetch(target, amount, transport, before.block.timestamp)
    )


def venue_transports(config=None) -> dict:
    """What a production gateway asks for this deployment: the venue's own API."""
    return {venue.QUOTE_VENUE: venue.HttpTransport()}


def quoted_operation(backend, state, intent, before):
    """Plan the next step, asking for a quote before the approval and for the deposit.

    The approval signs no quote, but one the gateway would refuse leaves no approval
    behind: the quote is asked and admitted first and then discarded. The deposit asks
    again and signs its own.
    """
    kind = planning.step(backend.config, state, intent, before)
    if kind not in ("approve", "deposit"):
        return planning.operation(backend.config, state, intent, before)
    backend.guard()
    # Before the venue is asked: a pool that is not the reviewed one refuses with that
    # cause, not as a quote that names other contracts.
    require_reviewed(before)
    quote = quoted(backend, state, intent, before)
    venue.admit(quote, intent.target, intent.amount.amount, before.pool, before.block.timestamp)
    return planning.operation(
        backend.config, state, intent, before, quote=quote if kind == "deposit" else None
    )


def require_reviewed(before) -> None:
    """Hold both of the route's pools to the reviewed pins, for new exposure only.

    The source pool is the one the route retains, as the chain has it now. The
    destination pool is as resolution retained it: no destination read, so a destination
    outage refuses nothing here. Arrival, refund, revocation and closure never pass
    through here; they keep using the route's retained pools.
    """
    route = before.target.route
    require_pinned(before.pool, route.chain.chain_id)
    require_pinned(route.destination_pool, route.destination_chain)


def require_open(state) -> None:
    if state.mode != "running" or any(o.accounting_issues for o in state.outcomes):
        raise DataError(
            "entry_unavailable", "a new bridge requires running mode and reconciled accounting"
        )


def require_route(state, operation, before) -> None:
    """The route's pools, payout and pause, before any balance is looked at."""
    require_reviewed(before)
    try:
        state.config.require_payout(operation.allocation)
    except ValueError as error:
        raise DataError("unsupported_route", str(error)) from error
    if before.pool.deposits_paused:
        raise DataError("deposits_paused", "the Across SpokePool has paused deposits")


def require_funding(state, operation, before) -> None:
    allocation = state.config.allocation(operation.allocation)
    if before.native_balance.amount < allocation.minimum_native_balance:
        raise DataError("payer_unavailable", "Across gas payer is below its configured minimum")
    if before.target.route.native:
        require_native_funding(state.config, operation, before)
    elif before.wallet_balance < operation.amount.amount:
        raise DataError(
            "funding_unavailable",
            f"the fund holds {before.wallet_balance} of {operation.amount.asset}, "
            f"less than the {operation.amount.amount} to bridge",
        )


def validate_entry(state, operation, before):
    if not increases_exposure(state.config, operation):
        return
    require_open(state)
    require_route(state, operation, before)
    require_funding(state, operation, before)


def fee_ceiling(config, account: str) -> int:
    """The most one transaction of the account may cost under its gas policy; zero where
    no policy is admitted."""
    admitted = execution_gas(config)
    return 0 if admitted is None else admitted.policy(account).max_transaction_fee.amount


def require_native_funding(config, operation, before) -> None:
    """Refuse a native bridge the wallet cannot pay for at its worst admitted fee.

    The amount and the gas leave the same balance. The shared budget check counts a
    transaction's value only once it is signed, so the amount is held here, before
    anything is: against the fee ceiling, not the quoted fee, so a deposit admitted
    here is never refused for its fee alone. A check of the gateway, not of the chain.
    """
    amount, ceiling = operation.amount.amount, fee_ceiling(config, operation.account)
    held, needed = before.native_balance.amount, amount + ceiling
    if held < needed:
        raise DataError(
            "funding_unavailable",
            f"the wallet holds {held} of {operation.amount.asset}, {needed - held} less "
            f"than the {amount} to bridge plus the transaction fee ceiling {ceiling}",
        )


def retained_allowance(state, before):
    for outcome in reversed(planning.executions(state)):
        if outcome.after.target == before.target:
            return outcome.after.allowance
    for baseline in state.baseline:
        if baseline.target == before.target:
            return baseline.allowance
    return 0


def require_planned(config, state, operation, before):
    """The operation must be the retained decision's next step, with its own quote."""
    refusal = ValueError("Across operation differs from its retained economic decision")
    if state.decision is None:
        raise refusal
    quote = getattr(operation, "quote", None)
    try:
        planned = planning.operation(config, state, state.decision.intent, before, quote=quote)
    except DataError as error:
        if error.kind != "quote_required":
            raise
        # The next step is the deposit, and this operation carries no quote: another step.
        raise refusal from None
    if operation != planned:
        raise refusal


def validate_operation(backend, state, operation, before):
    backend.guard()
    if state.mode == "closed":
        raise ValueError("closed deployments cannot execute Across operations")
    require_planned(backend.config, state, operation, before)
    if retained_allowance(state, before) != before.allowance:
        raise DataError(
            "allowance_mismatch", "SpokePool allowance changed outside retained execution"
        )
    validate_entry(state, operation, before)
    if isinstance(operation, DepositOperation):
        venue.admit(
            operation.quote,
            operation.target,
            operation.amount.amount,
            before.pool,
            before.block.timestamp,
        )


def validate_current(backend, plan, current):
    before, operation = plan.before, plan.operation
    if before.target != current.target or before.allowance != current.allowance:
        raise DataError("precondition_changed", "SpokePool allowance changed after preparation")
    # A revocation spends nothing, so neither the fund's balance nor the pool's state holds it.
    if isinstance(operation, RevokeOperation):
        return
    if current.wallet_balance < before.wallet_balance:
        raise DataError("precondition_changed", "Across fund inventory decreased after preparation")
    if current.pool != before.pool:
        raise DataError("precondition_changed", "the Across SpokePool changed after preparation")
    if isinstance(operation, DepositOperation):
        for kind, message in venue.timing_refusals(
            operation.quote, current.pool, current.block.timestamp
        ):
            raise DataError(kind, f"{venue.SOURCE}: {message}")


def validate_simulation(operation, result, before):
    raw = bytes.fromhex(result[2:])
    if isinstance(operation, DepositOperation):
        if raw:
            raise DataError("simulation", "Across deposit returned unexpected data")
    elif not approval_succeeded(raw):
        raise DataError("simulation", "the sent asset rejected the SpokePool approval")


def allowance_after(operation, previous, assets):
    if isinstance(operation, ApproveOperation):
        return operation.amount.amount
    if isinstance(operation, RevokeOperation):
        return 0
    # A native deposit spends value, not an allowance, and its snapshots state none.
    return previous if operation.target.route.native else previous - assets


def accounting_issues(before, after, assets, gas_paid):
    """Balance changes of the transaction that its receipt does not explain.

    A token deposit takes `assets` from the fund's token balance and gas from the payer.
    A native deposit takes both from the one native balance, which must fall by exactly
    the gas paid plus the amount sent; its token balance is that same figure and is not
    compared a second time.
    """
    native = before.target.route.native
    issues = []
    if not native and after.wallet_balance != before.wallet_balance - assets:
        issues.append("fund token balance includes unexplained external movements")
    sent_value = assets if native else 0
    if after.native_balance.amount != before.native_balance.amount - gas_paid - sent_value:
        issues.append("gas payer balance includes unexplained external movements")
    return tuple(issues)


def reconcile(backend, pending, receipt, block):
    from almanak.connectors.across.state import Outcome

    operation, before = pending.operation, pending.before
    after = snapshot(backend, operation.allocation, block)
    success = receipt["status"] == "0x1"
    assets, deposit = movement(receipt, operation, block.timestamp) if success else (0, None)
    expected = allowance_after(operation, before.allowance, assets) if success else before.allowance
    if after.allowance != expected:
        raise DataError("ownership_mismatch", "mined SpokePool allowance does not reconcile")
    fees, fee_evidence = charged_fees(backend, pending, receipt, block)
    return Outcome(
        decision=pending.decision,
        operation=operation,
        tx_hash=pending.tx_hash,
        nonce=pending.nonce,
        before=before,
        after=after,
        success=success,
        assets=assets,
        transfer=None if deposit is None else opened_transfer(operation, deposit),
        gas_paid=fees,
        op_fee_evidence=fee_evidence,
        receipt=receipt,
        call=pending.call,
        execution=pending.execution,
        accounting_issues=accounting_issues(before, after, assets, fees.amount),
    )


def requirements(config, account):
    """Each allocation's configured `capital` of its sent asset, on the source account."""
    return summed(
        (
            AssetAmount(asset=position.route.asset, amount=position.capital)
            for position in config.positions
            if position.account == account
        ),
        "configured allocation capital",
    )


class AcrossExecution:
    validate_operation = staticmethod(validate_operation)
    operation = staticmethod(planning.operation)
    quoted_operation = staticmethod(quoted_operation)
    venue_transports = staticmethod(venue_transports)
    expired = staticmethod(expired)
    validate_current = staticmethod(validate_current)
    validate_simulation = staticmethod(validate_simulation)
    reconcile = staticmethod(reconcile)
    increases_exposure = staticmethod(increases_exposure)
    requirements = staticmethod(requirements)
    gas_lifecycle = GasLifecycle(kinds={"bridge": KindCounts(entry=ENTRY_TRANSACTIONS, exit=0)})

    @staticmethod
    def snapshot(backend, name, block, reference=None):
        return snapshot(backend, name, block)

    @staticmethod
    def call(config, operation):
        if operation.target != config.target(operation.allocation):
            raise ValueError("Across call target differs from authoritative configuration")
        return plan_call(operation)

    @staticmethod
    def validate_config(config):
        for allocation in config.positions:
            if allocation.route.spoke_pool == allocation.route.input_token:
                raise ValueError("SpokePool and sent asset must be distinct contracts")

    @staticmethod
    def validate_replacement(config, operation):
        raise DataError(
            "replacement_unsupported", "Across replacement requires separate execution proofs"
        )

    @staticmethod
    def plan_evidence(backend, operation, call, before):
        return {}

    @staticmethod
    def current_checks(backend, plan, block=None):
        block = block or backend.reader.pin_block("latest")
        validate_current(backend, plan, snapshot(backend, plan.operation.allocation, block))

    @staticmethod
    def entry_transactions(config, account):
        return ENTRY_TRANSACTIONS * sum(p.account == account for p in config.positions)

    @staticmethod
    def preflight(config, account, observations):
        observed = tuple(o for o in observations if o.target.account == account)
        balances = {o.target.asset: o.wallet_balance for o in observed}
        tokens = funding_rows(requirements(config, account), balances)
        return tokens, bool(observed) and not any(o.allowance for o in observed)
