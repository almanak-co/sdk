"""A Curve pool call is not charged the time its own approval took to be mined.

The request's `deadline_seconds` window is fixed when the request is observed. An
approval is held to it. The pool call that follows a mined approval of the same request
is given the same number of seconds from the block that mined the approval, never more
than two windows after the request was observed, and keeps the minimum output of the
request's original quote. Past the request's own window the pool must still quote that
minimum, or nothing is signed.

Component evidence over a fake chain with a moved block clock (`tests/curve_slow_chain`);
it is not fork or process recovery evidence.
"""

from types import SimpleNamespace

import pytest

from almanak.connectors.composition import connector_from_wire
from almanak.connectors.curve import direct as curve_direct
from almanak.connectors.curve import liquidity_quotes, planning
from almanak.connectors.curve.validation import validate_current, validate_operation
from almanak.connectors.direct import ALLOCATION
from almanak.gateway.ethereum import DataError
from tests.curve_slow_chain import AMOUNT, QUOTED, SlowChain
from tests.test_ax_swap_curve import MARKETS, WINDOW

MARKET = MARKETS["stableswap_ng"]
# 50 basis points below the quoted output: the minimum the request states.
MINIMUM = QUOTED * 9950 // 10000
# A slow endpoint: the approval is mined 250 s after the request was observed, and the
# swap is planned 10 s after the request's 300 s window ended.
SLOW = {"read": 60, "mine": 130}
OBSERVED, MINED = 120, 370


def at(chain, seconds):
    return chain.start + seconds


def ended(chain):
    """What the request left in the checkpoint once the coordinator stopped."""
    state = chain.journal.load()
    return state.decision, state.plan, state.pending


def test_a_slow_approval_does_not_spend_the_window_of_the_swap_it_precedes():
    chain = SlowChain(MARKET, **SLOW)
    runtime, intent, refusal = chain.swap()
    assert refusal is None and chain.kinds() == ["approve", "swap"]
    approval, swap = chain.journal.load().outcomes
    assert approval.after.block.timestamp == at(chain, MINED)
    assert intent.deadline == at(chain, OBSERVED + WINDOW)
    # The swap was planned after the request's own window and inside its own.
    assert swap.before.block.timestamp > intent.deadline
    assert swap.operation.deadline == at(chain, MINED + WINDOW)
    assert swap.before.block.timestamp < swap.operation.deadline
    # The economic bound is the request's: the stated amount at the original minimum.
    assert swap.operation.amount == intent.amount
    assert swap.operation.minimum == intent.minimum_output
    assert intent.minimum_output.amount == MINIMUM
    data = swap.call.data
    words = {int(data[10:][index : index + 64], 16) for index in range(0, len(data) - 10, 64)}
    assert {AMOUNT, MINIMUM} <= words and swap.operation.deadline not in words
    assert swap.strategy_result().complete and chain.allowance == 0
    assert ended(chain) == (None, None, None)


def test_every_step_is_signed_and_sent_once_and_recorded_under_the_one_request():
    chain = SlowChain(MARKET, **SLOW)
    runtime, intent, _ = chain.swap()
    state = chain.journal.load()
    assert [pending.nonce for pending in chain.signed] == [0, 1]
    assert chain.sent == [pending.tx_hash for pending in chain.signed]
    assert [outcome.tx_hash for outcome in state.outcomes] == chain.sent
    (decision,) = {outcome.decision.identity for outcome in state.outcomes}
    assert all(outcome.decision.intent == intent for outcome in state.outcomes)
    assert all(outcome.accounting_issues == () for outcome in state.outcomes)
    assert state.delivered == len(state.outcomes) == 2
    # A later coordinator finds nothing to resolve, plans nothing and sends nothing.
    assert chain.runtime().recover() == state
    assert len(chain.signed) == len(chain.sent) == 2
    with pytest.raises(ValueError, match="no operator request is admitted"):
        chain.runtime().execute()


def test_past_the_requests_window_a_quote_below_its_minimum_signs_nothing():
    chain = SlowChain(MARKET, **SLOW, after_approval={"output": MINIMUM - 1})
    runtime, intent, refusal = chain.swap()
    assert refusal.kind == "stale_plan" and chain.kinds() == ["approve"]
    message = str(refusal)
    assert f"`deadline_seconds` window ended at {intent.deadline}" in message
    assert f"the pool quotes {MINIMUM - 1} of {MARKET.bought}" in message
    assert f"below the request's minimum output {MINIMUM}" in message
    # The refusal says what is left behind and that the same request reuses it.
    assert f"the pool's allowance of {AMOUNT} of {MARKET.sold} stands" in message
    assert "the same request stated again spends it without another approval" in message
    assert len(chain.signed) == len(chain.sent) == 1 and chain.allowance == AMOUNT
    assert ended(chain) == (None, None, None)


def test_past_the_requests_window_a_pool_that_quotes_nothing_signs_nothing():
    chain = SlowChain(MARKET, **SLOW, after_approval={"output": 0})
    _, _, refusal = chain.swap()
    assert refusal.kind == "stale_plan" and chain.kinds() == ["approve"]
    assert "the pool quotes nothing for the stated amount" in str(refusal)
    assert len(chain.signed) == len(chain.sent) == 1


def test_the_minimum_itself_is_still_admitted_past_the_requests_window():
    chain = SlowChain(MARKET, **SLOW, after_approval={"output": MINIMUM})
    _, _, refusal = chain.swap()
    assert refusal is None and chain.kinds() == ["approve", "swap"]
    assert chain.held[MARKET.bought] == MINIMUM


def test_inside_the_requests_window_a_fallen_quote_is_refused_as_it_was():
    chain = SlowChain(MARKET, read=10, mine=10, after_approval={"output": MINIMUM - 1})
    _, _, refusal = chain.swap()
    assert refusal.kind == "simulation_reverted" and chain.kinds() == ["approve"]
    assert len(chain.signed) == 1


def test_a_quote_that_falls_before_signing_refuses_the_swap_unsigned():
    chain = SlowChain(MARKET, **SLOW)

    def fall(plan):
        if plan.operation.kind == "swap":
            chain.output = MINIMUM - 1

    chain.before_sign = fall
    _, _, refusal = chain.swap()
    assert refusal.kind == "stale_plan" and "below the request's minimum output" in str(refusal)
    assert chain.kinds() == ["approve"] and len(chain.signed) == len(chain.sent) == 1
    assert ended(chain) == (None, None, None)


def test_the_standing_approval_is_spent_by_the_same_request_stated_again():
    first = SlowChain(MARKET, **SLOW, after_approval={"output": MINIMUM - 1})
    first.swap()
    again = SlowChain(MARKET, **SLOW, allowance=first.allowance)
    _, _, refusal = again.swap()
    assert refusal is None and again.kinds() == ["swap"]
    assert len(again.signed) == 1 and again.allowance == 0


@pytest.mark.parametrize(
    ("latency", "changed", "window_end", "signed"),
    [
        # One step, planned after the request's window: no approval restarts anything.
        ({"read": WINDOW + 10, "mine": 10, "allowance": AMOUNT}, {}, 3 * WINDOW + 20, 0),
        # The swap is planned after its own window, measured from the mined approval.
        (SLOW, {"read": WINDOW + 10}, MINED + WINDOW, 1),
        # An approval mined long after the request's window gives the swap no more than
        # one further window past the request's deadline.
        ({"read": 60, "mine": 500}, {}, OBSERVED + 2 * WINDOW, 1),
    ],
    ids=["one_step", "own_window", "two_windows"],
)
def test_a_stale_plan_is_still_refused(latency, changed, window_end, signed):
    chain = SlowChain(MARKET, **latency, after_approval=changed)
    _, _, refusal = chain.swap()
    assert refusal.kind == "stale_plan"
    assert "retained deadline expired or exceeds configured policy" in str(refusal)
    assert f"`deadline_seconds` window that ends at {at(chain, window_end)}" in str(refusal)
    assert f"the {WINDOW}s" in str(refusal)
    assert len(chain.signed) == len(chain.sent) == signed
    assert "swap" not in chain.kinds() and ended(chain) == (None, None, None)


def test_a_swap_planned_in_time_is_not_signed_after_its_own_window():
    # Planned 250 s after the approval was mined, the swap would be signed 500 s after.
    chain = SlowChain(MARKET, **SLOW, after_approval={"read": 250})
    _, _, refusal = chain.swap()
    assert refusal.kind == "stale_plan"
    assert f"window that ends at {at(chain, MINED + WINDOW)}" in str(refusal)
    assert chain.kinds() == ["approve"] and len(chain.signed) == 1


def test_an_approval_is_always_held_to_the_requests_own_window():
    """A reset mined late gives the approval that follows it no further time."""
    chain = SlowChain(MARKET, **SLOW, allowance=AMOUNT + 5)
    _, intent, refusal = chain.swap()
    assert refusal.kind == "stale_plan" and chain.kinds() == ["approve"]
    assert chain.journal.load().outcomes[0].operation.amount == 0
    assert f"window that ends at {intent.deadline}" in str(refusal)
    # Nothing stands after the reset, so the refusal names no allowance.
    assert chain.allowance == 0 and "stands" not in str(refusal)


def deciding(chain, intent, outcomes):
    state = chain.journal.load()
    return SimpleNamespace(
        config=chain.config,
        owned=state.owned,
        capabilities=state.capabilities,
        decision=state.decision,
        outcomes=outcomes,
        mode="running",
        operator=state.operator,
    )


def test_only_a_mined_approval_of_the_retained_request_restarts_the_window():
    chain = SlowChain(MARKET, **SLOW)
    runtime = chain.runtime()
    record = runtime.submit(lambda s, seen: curve_direct.intent(s.config, seen, chain.stated))
    intent = record.intent
    while not chain.journal.load().outcomes:
        runtime.tick()
    (approval,) = chain.journal.load().outcomes
    config = chain.config

    def end(*outcomes):
        return planning.step_deadline(config, deciding(chain, intent, outcomes), intent)

    assert end() == intent.deadline
    assert end(approval) == approval.after.block.timestamp + WINDOW
    reverted = approval.model_copy(update={"success": False})
    other = approval.model_copy(
        update={"decision": approval.decision.model_copy(update={"identity": "0" * 32})}
    )
    swapped = SimpleNamespace(operation=SimpleNamespace(kind="swap"), success=True)
    assert end(reverted) == end(other) == end(swapped) == intent.deadline
    # Only the latest outcome counts.
    assert end(approval, other) == intent.deadline


def test_a_strategy_deployment_keeps_the_requests_window_after_its_approval():
    """A strategy decides again on fresh observations after a refusal, so its pool call
    is never given a window of its own."""
    chain = SlowChain(MARKET, **SLOW)
    _, intent, _ = chain.swap()
    approval, swap = chain.journal.load().outcomes
    assert swap.before.block.timestamp > intent.deadline
    states = []
    for operator in (True, False):
        state = deciding(chain, intent, (approval,))
        state.decision, state.owned, state.operator = swap.decision, (swap.before.owned,), operator
        states.append(state)
    operator, strategy = states
    assert planning.operation(chain.config, operator, intent, swap.before) == swap.operation
    planned = planning.operation(chain.config, strategy, intent, swap.before)
    assert planned == swap.operation.model_copy(update={"deadline": intent.deadline})
    with pytest.raises(DataError, match=f"window that ends at {intent.deadline}") as refusal:
        validate_operation(None, strategy, planned, swap.before)
    assert refusal.value.kind == "stale_plan"


def test_a_pool_call_carries_its_own_deadline_or_the_requests_and_no_other():
    chain = SlowChain(MARKET, **SLOW)
    runtime, intent, _ = chain.swap()
    approval, swap = chain.journal.load().outcomes
    state = deciding(chain, intent, (approval,))
    state.decision = swap.decision
    state.owned = (swap.before.owned,)
    execution = connector_from_wire({"connector": "curve"}).execution
    planned = execution.operation(chain.config, state, intent, swap.before)
    assert planned == swap.operation
    validate_operation(None, state, planned, swap.before)
    later = planned.model_copy(update={"deadline": planned.deadline + 1})
    with pytest.raises(ValueError, match="operation differs from its retained economic intent"):
        validate_operation(None, state, later, swap.before)

    # The request's own, earlier deadline is the tighter bound: a call that carries it
    # is admitted while that deadline holds, and refused once it has passed.
    tighter = planned.model_copy(update={"deadline": intent.deadline})
    early = approval.after.model_copy(update={"entry_quote": swap.before.entry_quote})
    assert early.block.timestamp < intent.deadline
    validate_operation(None, state, tighter, early)
    with pytest.raises(DataError, match="retained deadline expired") as refusal:
        validate_operation(None, state, tighter, swap.before)
    assert refusal.value.kind == "stale_plan"
    unsigned = SimpleNamespace(operation=tighter, before=early, decision=swap.decision)
    with pytest.raises(DataError, match="retained deadline expired"):
        validate_current(SimpleNamespace(config=chain.config), unsigned, swap.before)
    assert chain.config.allocation(ALLOCATION).deadline_seconds == WINDOW


def test_a_deposit_follows_its_last_mined_approval_and_keeps_its_minimum_shares():
    """The same rule serves an addition: its approvals are held to the request's window,
    and the pool call that follows the last of them has its own."""
    import dataclasses

    from tests import test_ax_liquidity_curve as liquidity

    stated, config, state, seen = liquidity.adopted(MARKET, "open-liquidity")
    intent = curve_direct.intent(config, (seen,), stated)
    deciding, step = liquidity.planning(config, state, intent, seen)
    deciding.operator = state.operator
    wanted, owned = liquidity.deposit(MARKET), state.owned[0]

    def observed(seconds, granted):
        block = MARKET.block.model_copy(update={"timestamp": MARKET.block.timestamp + seconds})
        late = dataclasses.replace(
            MARKET, observed=MARKET.observed.model_copy(update={"block": block})
        )
        return liquidity.observation(late, config, owned, allowances=granted)

    first, last = wanted
    in_time = step(observed(WINDOW - 1, {first: wanted[first]}))
    assert (in_time.kind, in_time.asset) == ("approve", last)
    with pytest.raises(DataError, match=f"window that ends at {intent.deadline}"):
        step(observed(WINDOW, {first: wanted[first]}))

    # The last approval is mined 10 s before the request's window ends.
    mined = observed(WINDOW - 10, dict(wanted))
    deciding.outcomes = (
        SimpleNamespace(
            operation=in_time,
            success=True,
            decision=deciding.decision,
            after=mined,
            accounting_issues=(),
        ),
    )
    added = step(observed(WINDOW + 60, dict(wanted)))
    assert (added.kind, added.amounts) == ("mint", intent.maximums)
    assert added.deadline == mined.block.timestamp + WINDOW
    expected = liquidity_quotes.minted(
        MARKET.observed, tuple(value.amount for value in intent.maximums.root)
    )
    assert added.minimum_shares.amount == expected * (10000 - liquidity.SLIPPAGE_BPS) // 10000
    with pytest.raises(DataError, match="retained deadline expired") as refusal:
        step(observed(2 * WINDOW - 10, dict(wanted)))
    assert refusal.value.kind == "stale_plan"
    # Without the mined approval the deposit is held to the request's window, as before.
    deciding.outcomes = ()
    with pytest.raises(DataError, match=f"window that ends at {intent.deadline}"):
        step(observed(WINDOW + 60, dict(wanted)))
