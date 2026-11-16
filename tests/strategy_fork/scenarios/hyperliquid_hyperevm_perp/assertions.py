"""The HyperCore perp reference through the coordinator on a HyperEVM fork (blueprint 30,
section 13.4).

The supervised workspace runs the HyperEVM fork node, the gateway with its HyperCore
adapter, the coordinator with the sandboxed strategy worker, and the `hypercore-model`
venue process. The strategy deposits its capital, sets 10x cross leverage, opens its
`Ioc` long and holds it; teardown closes it reduce-only and withdraws to the Safe.

Every figure the strategy decided on is projected from the checkpoint's custody,
trading and book documents, so each assertion reads the checkpoint and the fork node.
HyperCore's side is the synthesized model's: none of it is HyperCore evidence.
"""

from decimal import Decimal

from almanak.connectors.hyperliquid import corewriter as cw
from almanak.gateway.safe_fixture import calldata

# The deposit's custody steps, then the withdrawal's, as their requests record them.
ENTRY_REQUESTS = ["deposit", "standard_mode", "class_transfer"]
EXIT_REQUESTS = [*ENTRY_REQUESTS, "class_transfer", "withdrawal"]
# HyperCore's activation fee, paid out of the first deposit's credit (A7).
ACTIVATION_FEE = Decimal(1)
# The withdrawal's own fee is HYPE the Safe owners funded, so only trading costs USDC.
CAPITAL = 30_000_000
# Averages of several fills carry the division's last digits.
PRICE_DIGITS = Decimal("1e-12")


def entered(state) -> bool:
    """Deposited, leveraged and holding the strategy's own position, nothing in flight."""
    return idle(state) and (state.mode, state.initialized) == ("running", True) and holding(state)


def holding(state) -> bool:
    (owned,) = state.owned
    opened = state.memory.values.get("opened_at", {})
    return owned.activated and owned.holding is not None and owned.target.allocation in opened


def idle(state) -> bool:
    return (
        requests_resolved(state)
        and nothing_in_flight(state)
        and state.delivered == len(state.outcomes)
    )


def requests_resolved(state) -> bool:
    return all(r.status == "resolved" for r in state.custody.requests)


def nothing_in_flight(state) -> bool:
    in_flight = (
        state.custody.agents.outstanding,
        state.trading.pending,
        state.decision,
        state.plan,
        state.pending,
    )
    return all(value is None for value in in_flight)


# Checks of complexity 5 or less each: the gate scores fork-only code at zero coverage.


def assert_holding(node, state):
    assert entered(state)
    (owned,) = state.owned
    allocation = state.config.allocation(owned.target.allocation)
    assert_position(state, owned, allocation)
    assert_entry_steps(node, state, owned)
    assert_actions_accounted(state)
    assert_margin_kept(state, owned, allocation)


def assert_position(state, owned, allocation):
    size = Decimal(str(state.config.parameters["size"]))
    # Ownership is what the documents project, and what the backtest's account names.
    assert (owned.position, owned.holding.szi, owned.leverage) == (
        size,
        str(size),
        allocation.leverage,
    )
    assert abs(Decimal(owned.holding.entry_px) - paid(state) / size) < PRICE_DIGITS


def assert_entry_steps(node, state, owned):
    # The Safe deposited all of its capital; the first credit paid the activation fee.
    assert owned.inventory[0].amount == 0 == safe_usdc(node, owned.target.fund.address)
    (agent,) = state.custody.agents.approvals
    assert (request_kinds(state), agent.status, purposes(state)) == (
        ENTRY_REQUESTS,
        "active",
        [("leverage", "applied"), ("entry", "applied")],
    )


def assert_margin_kept(state, owned, allocation):
    # Valued at its entry, the position keeps a tenth of its notional (10x cross).
    kept = paid(state) / allocation.leverage
    assert owned.withdrawable == int((state.book.cash - kept) * 10**6) > 0
    assert state.book.cash < Decimal(CAPITAL) / 10**6 - ACTIVATION_FEE


def assert_closed(node, state):
    assert (state.mode, state.delivered, idle(state)) == ("closed", len(state.outcomes), True)
    (owned,) = state.owned
    assert_flat(state, owned)
    assert_exit_steps(state)
    assert_returned(node, owned)
    assert_actions_accounted(state)
    assert all(not snapshot.needs_cleanup for snapshot in state.closure)


def assert_flat(state, owned):
    leverage = state.config.allocation("Main").leverage
    assert (owned.position, owned.holding, owned.hypercore, owned.withdrawable) == (0, None, 0, 0)
    assert (owned.activated, owned.leverage) == (True, leverage)
    assert (state.book.positions, state.book.owned) == ({}, {})


def assert_exit_steps(state):
    assert (request_kinds(state), moves_to_perp(state)) == (EXIT_REQUESTS, [True, False])
    assert purposes(state) == [("leverage", "applied"), ("entry", "applied"), ("exit", "applied")]
    exit_attempt = state.trading.attempts[-1]
    assert exit_attempt.request.action.order.reduce_only


def assert_returned(node, owned):
    # Every USDC left on HyperCore came back to the Safe on HyperEVM.
    returned = owned.inventory[0].amount
    assert returned == safe_usdc(node, owned.target.fund.address)
    assert CAPITAL - 2_000_000 < returned < CAPITAL - 1_000_000


def assert_actions_accounted(state):
    assert all(o.success and not o.accounting_issues for o in action_outcomes(state))


def paid(state) -> Decimal:
    return sum(Decimal(f.entry["px"]) * Decimal(f.entry["sz"]) for f in owned_fills(state))


def request_kinds(state):
    return [r.kind for r in state.custody.requests]


def moves_to_perp(state):
    return [r.to_perp for r in state.custody.requests if r.kind == "class_transfer"]


def owned_fills(state):
    return [e for e in state.book.effects if e.kind == "fill" and e.owned]


def purposes(state):
    return [(a.purpose, a.status) for a in state.trading.attempts]


def action_outcomes(state):
    return [o for o in state.outcomes if getattr(o, "decision", None) is not None]


def safe_usdc(node, safe) -> int:
    data = calldata("balanceOf(address)", ("address",), (safe,))
    return int(node.rpc("eth_call", [{"to": cw.USDC_CONTRACT, "data": data}, "latest"]), 16)
