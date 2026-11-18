"""Custody transactions use the shared account, signing and receipt path; venue actions do not.

A custody operation is one HyperEVM call through Roles (`operations.core_call`), so it
funds, signs and settles as any EVM transaction: the gateway adapter hands it to
`EvmGateway.prepare_operation`. A venue action has no call, gas or EVM nonce: the adapter
(`gateway/hypercore_gateway.py`) admits, signs, posts and reconciles it. Planning reads
the venue, which only the adapter can, so it plans through `planning.operation` itself.

`validate_operation` checks that an operation is a legal next step of the active intent
given the checkpoint's documents and this decision's outcomes (`planning.validate_step`).
It never re-plans: venue reads move between planning and signing, and the documents do
not.
"""

from almanak.connectors.erc20_approval import approval_succeeded
from almanak.connectors.funding import funding_rows, lifecycle_entries, summed
from almanak.connectors.gas_lifecycle import GasLifecycle, KindCounts
from almanak.connectors.hyperliquid import lifecycle, planning
from almanak.connectors.hyperliquid.custody import Custody
from almanak.connectors.hyperliquid.observations import snapshot
from almanak.connectors.hyperliquid.operations import (
    Approve,
    ClassTransfer,
    Deposit,
    VenueAction,
    core_call,
)
from almanak.connectors.hyperliquid.state import TransactionOutcome
from almanak.effects import AssetDelta, WalletAsset
from almanak.gateway.accounts import ContractCall
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm_fees import charged_fees


def requirements(config, account):
    """The USDC each allocation deposits. The 1 USDC activation fee comes out of the first
    deposit, and the HyperCore HYPE that pays withdrawal system transactions is the Safe
    owners' to send: no grant or plan moves HYPE to HyperCore."""
    return summed(
        (a.capital for a in account_allocations(config, account)),
        "configured HyperCore deposit capital",
    )


# Only custody operations are payer transactions; venue actions use no gas (section 4.5).
# An entry approves CoreDepositWallet, deposits, sets standard mode, approves the first
# agent and moves the deposit to perp before the first trade. A deposit pulls exactly
# what it was approved for, so a residual allowance is revoked only when no approval
# preceded it: approval and revocation count once. An exit closes reduce-only through the
# agent, then moves perp USDC to spot and withdraws it to the Safe.
GAS_LIFECYCLE = GasLifecycle(kinds={"perp": KindCounts(entry=5, exit=2)})

entry_transactions = lifecycle_entries(GAS_LIFECYCLE, lambda allocation: ("perp", {}))


def preflight(config, account, observations):
    allocations = account_allocations(config, account)
    observed = tuple(o for o in observations if o.target.account == account)
    if len(allocations) != 1 or len(observed) != 1:
        raise ValueError("Hyperliquid preflight requires its exact configured Safe allocation")
    allocation, before = allocations[0], observed[0]
    if before.target != config.target(allocation.name):
        raise ValueError("Hyperliquid preflight target differs from authoritative configuration")
    balances = {value.asset: value.amount for value in before.wallets}
    tokens = funding_rows(requirements(config, account), balances)
    flat = before.position == 0 and not before.holds_usdc
    return tokens, flat and not before.needs_cleanup


def account_allocations(config, account):
    return tuple(a for a in config.allocations if a.account == account)


def call(config, operation):
    if operation.target != config.target(operation.allocation):
        raise ValueError("Hyperliquid protocol call differs from authoritative configuration")
    core = core_call(operation)
    return ContractCall(fund=operation.target.fund, to=core.to, value=0, data=core.data)


def increases_exposure(config, operation):
    if isinstance(operation, VenueAction):
        return operation.attempt.purpose == "entry"
    if isinstance(operation, ClassTransfer):
        return operation.to_perp
    if isinstance(operation, Approve):
        return operation.amount.amount > 0
    return isinstance(operation, Deposit)


def plan_from_venue(config, state, intent, before):
    raise ValueError(
        "HyperCore planning reads the venue; the HyperCore gateway adapter plans it "
        "(planning.operation)"
    )


# Validation: an operation is a legal next step of its intent, by the documents.


def validate_operation(backend, state, operation, before):
    decision = state.decision
    if decision is None or before.target != operation.target:
        raise ValueError("Hyperliquid operation requires its retained decision and target")
    if decision.intent.target != operation.target:
        raise ValueError("Hyperliquid operation belongs to another intent target")
    owned = next(o for o in state.owned if o.target == operation.target)
    if owned != before.owned:
        raise DataError("ownership", "Hyperliquid preparation differs from retained ownership")
    planning.validate_step(state, decision.intent, operation)
    if increases_exposure(state.config, operation):
        validate_entry(state, before)


def validate_entry(state, before):
    if state.mode != "running" or any(o.accounting_issues for o in state.outcomes):
        raise DataError("accounting", "new HyperCore exposure requires a running deployment")
    minimum = state.config.allocation(before.target.allocation).minimum_native_balance
    if before.native_balance.amount < minimum:
        raise DataError("gas_budget", "the Hyperliquid operator lacks its configured reserve")


# The custody transaction's EVM path.


def validate_simulation(operation, result, before):
    raw = bytes.fromhex(result[2:])
    if isinstance(operation, Approve):
        if not approval_succeeded(raw):
            raise DataError("simulation", "USDC approve did not return success")
    elif raw:
        raise DataError("simulation", "a HyperCore custody call returns no data")


def validate_current(backend, plan, current):
    """The Safe still covers a deposit and its allowance still admits it."""
    operation = plan.operation
    if isinstance(operation, Deposit):
        (wallet,) = current.wallets
        if wallet.amount < operation.amount.amount or current.allowance < operation.amount.amount:
            raise DataError(
                "precondition_changed", "the Safe no longer covers or approves the deposit"
            )


def reconcile(backend, pending, receipt, block):
    """A mined custody call: what it moved on HyperEVM and the gas it paid. The request
    it queued is retained into the custody document when the outcome commits; a receipt
    lacking that request's own evidence is refused here, before anything commits."""
    success = receipt["status"] == "0x1"
    operation, before = pending.operation, pending.before
    if success:
        lifecycle.custody_changed(
            Custody(safe=operation.target.fund.address), operation, receipt, block
        )
    moved = operation.amount.amount if success and isinstance(operation, Deposit) else 0
    (owned,) = before.owned.inventory
    inventory = owned.model_copy(update={"amount": owned.amount - moved})
    reference = before.owned.model_copy(update={"inventory": (inventory,)})
    after = snapshot(backend, operation.allocation, block, reference)
    gas_paid, fee_evidence = charged_fees(backend, pending, receipt, block)
    assets = (usdc_delta(operation, -moved),) if moved else ()
    return TransactionOutcome(
        decision=pending.decision,
        operation=operation,
        nonce=pending.nonce,
        before=before,
        after=after,
        success=success,
        complete=False,
        assets=assets,
        tx_hash=pending.tx_hash,
        receipt=receipt,
        call=pending.call,
        execution=pending.execution,
        accounting_issues=wallet_issues(before, after, -moved),
        gas_paid=gas_paid,
        op_fee_evidence=fee_evidence,
    )


def usdc_delta(operation, amount: int) -> AssetDelta:
    target = operation.target
    key = WalletAsset(account=target.account, chain=target.fund.chain, asset=target.margin_asset)
    return AssetDelta(key=key, amount=amount)


def wallet_issues(before, after, delta: int) -> tuple[str, ...]:
    """A Safe USDC change other than the step's own is visible, never absorbed."""
    (was,), (now,) = before.wallets, after.wallets
    if now.amount == was.amount + delta:
        return ()
    return (f"Safe USDC changed by {now.amount - was.amount} where the step moved {delta}",)


class Execution:
    requirements = staticmethod(requirements)
    gas_lifecycle = GAS_LIFECYCLE
    entry_transactions = staticmethod(entry_transactions)
    preflight = staticmethod(preflight)
    call = staticmethod(call)
    increases_exposure = staticmethod(increases_exposure)
    snapshot = staticmethod(snapshot)
    operation = staticmethod(plan_from_venue)
    validate_operation = staticmethod(validate_operation)
    validate_simulation = staticmethod(validate_simulation)
    validate_current = staticmethod(validate_current)
    reconcile = staticmethod(reconcile)

    @staticmethod
    def validate_config(config):
        for allocation in config.allocations:
            config.target(allocation.name)

    @staticmethod
    def validate_replacement(config, operation):
        raise DataError(
            "replacement_unsupported",
            "Hyperliquid fee replacement requires separate custody request recovery proofs",
        )

    @staticmethod
    def plan_evidence(backend, operation, call, before):
        return {}

    @staticmethod
    def production_gateway(*args, **fields):
        """The production adapter, with Hyperliquid's own venue beside the chain
        (`gateway/hypercore_gateway.py`, blueprint 30 section 14)."""
        from almanak.gateway.hypercore_gateway import HyperCoreProductionGateway

        return HyperCoreProductionGateway(*args, **fields)

    @staticmethod
    def current_checks(backend, plan, block=None):
        """`validate_current` holds every current-state check of a custody call."""
        return None
