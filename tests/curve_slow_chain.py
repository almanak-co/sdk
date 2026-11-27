"""A Curve operator swap over a fake chain whose block clock the test moves.

The coordinator is the real `OperatorRuntime`, and every step is planned and checked by
the Curve connector's own planning and validation, as the gateway calls them. Only the
chain is fake: each read and each mined transaction moves its block time forward by a
stated number of seconds, so a slow endpoint is simulated without sleeping. Component
evidence only: nothing is signed, and it is not fork or process recovery evidence.
"""

import dataclasses
from datetime import UTC, datetime
from types import SimpleNamespace

from almanak.connectors.composition import connector_from_wire
from almanak.connectors.curve import direct as curve_direct
from almanak.connectors.curve import operations as ops
from almanak.connectors.curve import receipts, reconciliation
from almanak.connectors.curve.state import Outcome, Pending, TransactionPlan
from almanak.gateway.accounts import prepare_call
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount, Block
from almanak.operator_runtime import OperatorRuntime
from tests.test_ax_swap_curve import (
    AMOUNT,
    QUOTED,
    created,
    observation,
    operator_config,
    quote,
    request,
)

GAS, GAS_PRICE = 50_000, 1


class MemoryJournal:
    """The checkpoint of one deployment, updated as the durable journal updates it."""

    def __init__(self, state):
        self.state = state

    def load(self):
        return self.state

    def update(self, change):
        self.state = change(self.state)
        return self.state


class SlowChain:
    """The wallet, the pool's allowance and the block clock, with the gateway's surface.

    `read` seconds pass for every observation and `mine` seconds between a send and the
    block that mines it. `output` is what the pool pays for the stated amount at the
    current block; a swap below its minimum output reverts in simulation, as the pool
    itself would refuse it. `after_approval` names attributes of the chain to change
    once an approval is mined, and `before_sign` is called with each plan before the
    head it is signed at is read.
    """

    def __init__(
        self,
        market,
        *,
        read,
        mine,
        output=QUOTED,
        allowance=0,
        wallet=5000,
        after_approval=None,
        **stated,
    ):
        self.market = market
        self.config = operator_config(market, **stated)
        self.stated = request(market, **stated)
        self.journal = MemoryJournal(created(self.config))
        self.read, self.mine, self.output = read, mine, output
        self.block = market.block
        self.start = market.block.timestamp
        self.allowance = allowance
        self.held = {market.sold: wallet, market.bought: 0}
        self.spent = 0
        self.after_approval = after_approval or {}
        self.before_sign = lambda plan: None
        self.signed: list = []
        self.sent: list = []
        self.mined: dict = {}
        self.execution = connector_from_wire({"connector": "curve"}).execution

    # The clock.

    def elapsed(self):
        return self.block.timestamp - self.start

    def advance(self, seconds):
        number = self.block.number + 1
        self.block = Block(
            chain=self.block.chain,
            number=number,
            hash="0x" + f"{number:064x}",
            timestamp=self.block.timestamp + seconds,
        )
        return self.block

    def snapshot(self, owned, *, quotes=True):
        at = dataclasses.replace(
            self.market, observed=self.market.observed.model_copy(update={"block": self.block})
        )
        fields: dict = {"quotes_included": quotes}
        if quotes and owned.amount(self.market.sold) >= self.stated.amount and self.output:
            fields["entry_quote"] = quote(at, owned, self.stated.amount, self.output)
        return observation(
            at,
            self.config,
            owned,
            allowance=self.allowance,
            held=dict(self.held),
            spent=self.spent,
            **fields,
        )

    # The gateway, as the coordinator calls it.

    def observe(self):
        self.advance(self.read)
        return (self.snapshot(self.journal.load().owned[0]),)

    def prepare(self, state, intent):
        (before,) = self.observe()
        operation = self.execution.operation(self.config, state, intent, before)
        self.execution.validate_operation(None, state, operation, before)
        if isinstance(operation, ops.Swap) and self.output < operation.minimum.amount:
            raise DataError("simulation_reverted", "the pool pays less than the minimum output")
        call = self.execution.call(self.config, operation)
        return TransactionPlan(
            attempt_id=f"{len(self.signed):032x}",
            decision=state.decision,
            operation=operation,
            before=before,
            call=call,
            execution=prepare_call(self.config.accounts, operation.account, call),
            nonce=len(self.sent),
            gas_estimate=GAS,
            gas=GAS,
            gas_price=GAS_PRICE,
        )

    def sign(self):
        state = self.journal.load()
        plan = state.plan
        # Signing repeats preparation's check on the plan's own observation, then checks
        # the plan against the head it is signed at.
        self.execution.validate_operation(None, state, plan.operation, plan.before)
        self.before_sign(plan)
        self.advance(self.read)
        current = self.snapshot(plan.before.owned)
        self.execution.validate_current(SimpleNamespace(config=self.config), plan, current)
        pending = Pending(
            **plan.model_dump(),
            raw_transaction="0x01",
            tx_hash="0x" + f"{len(self.signed) + 1:064x}",
        )
        self.signed.append(pending)
        return pending

    def receipt(self, pending):
        return self.mined.get(pending.tx_hash, (None, None))[0]

    def broadcast(self, pending):
        if pending.tx_hash in self.mined:
            return False
        self.sent.append(pending.tx_hash)
        block = self.advance(self.mine)
        operation = pending.operation
        if isinstance(operation, ops.Approval):
            self.allowance = operation.amount
            for name, value in self.after_approval.items():
                setattr(self, name, value)
        else:
            if self.output < operation.minimum.amount:
                raise AssertionError("a swap below its minimum output was sent")
            self.allowance -= operation.amount.amount
            self.held[self.market.sold] -= operation.amount.amount
            self.held[self.market.bought] += self.output
        self.spent += GAS * GAS_PRICE
        receipt = {"status": "0x1", "blockHash": block.hash, "gasUsed": hex(GAS)}
        self.mined[pending.tx_hash] = (receipt, block)
        return True

    def reconcile(self, pending, receipt):
        operation, target = pending.operation, pending.operation.target
        moved = [0] * len(target.assets)
        if isinstance(operation, ops.Swap):
            moved[self.market.sell], moved[self.market.buy] = -operation.amount.amount, self.output
        effects = receipts.effects(target, moved)
        owned = reconciliation.owned_after(pending.before, operation, effects, True)
        mined_at = self.mined[pending.tx_hash][1]
        head, self.block = self.block, mined_at
        after = self.snapshot(owned, quotes=False)
        self.block = head
        cost = GAS * GAS_PRICE
        issues = reconciliation.validate_balances(pending, after, effects, cost, True)
        return Outcome(
            transaction_plan=TransactionPlan.model_validate(
                pending.model_dump(exclude={"family", "raw_transaction", "tx_hash", "unsent"})
            ),
            decision=pending.decision,
            operation=operation,
            tx_hash=pending.tx_hash,
            nonce=pending.nonce,
            before=pending.before,
            after=after,
            success=True,
            asset_changes=effects.asset_changes,
            share_change=effects.share_change,
            gas_paid=AssetAmount(asset=pending.before.native_balance.asset, amount=cost),
            receipt=receipt,
            call=pending.call,
            execution=pending.execution,
            accounting_issues=issues,
        )

    # The command.

    def runtime(self):
        runtime = object.__new__(OperatorRuntime)
        runtime.connector = connector_from_wire({"connector": "curve"})
        runtime.journal = self.journal
        runtime.gateway = self
        runtime.closed = False
        runtime.clock = lambda: datetime.fromtimestamp(self.block.timestamp, UTC)
        runtime.fault = lambda *args: None
        runtime.poll = 0
        return runtime

    def swap(self):
        """Admit the stated swap and execute it, as `ax swap --execute` does.

        Returns the coordinator, the admitted intent and the refusal that ended the
        request, if one did.
        """
        runtime = self.runtime()
        record = runtime.submit(
            lambda state, observed: curve_direct.intent(state.config, observed, self.stated)
        )
        self.admitted_at = self.elapsed()
        refusal = None
        try:
            runtime.execute()
        except DataError as exc:
            refusal = exc
        return runtime, record.intent, refusal or runtime.refused

    def kinds(self):
        return [outcome.operation.kind for outcome in self.journal.load().outcomes]


__all__ = ["AMOUNT", "QUOTED", "SlowChain"]
