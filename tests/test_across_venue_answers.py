"""The real gateway prepares a deposit from a venue that never answers twice alike, and
its own verification of the reserved plan still arrives at that plan.

Offline: the real `EvmGateway` constructor, preparation and the gateway service's plan
admission, over a fixed chain and a transport whose every answer differs.
"""

from itertools import count

import httpx
import pytest

from almanak.connectors.across import execution
from almanak.connectors.across import quote as venue
from almanak.gateway.execution import EvmGateway
from almanak.work import project
from tests.across_support import AMOUNT, body
from tests.test_across_records import (  # noqa: F401
    FORK_ARB,
    FORK_BLOCK,
    checkpoint,
    config,
    decision,
    observed,
)
from tests.test_expired_attempt_release import gateway_service


class Changing:
    """A venue that offers one base unit less on every request, as no fork quote does."""

    source = venue.SOURCE

    def __init__(self):
        self.asked, self.serial = [], count()

    def get(self, target, amount, block_timestamp):
        self.asked.append(amount)
        offered = amount - amount // 10_000 - next(self.serial)
        answer = body(
            outputAmount=str(offered),
            timestamp=str(FORK_BLOCK.timestamp - 69),
            fillDeadline=str(FORK_BLOCK.timestamp - 69 + 7200),
        )
        return httpx.Response(200, json=answer)


class Quoting(EvmGateway):
    """The real gateway with a fixed chain behind it and the changing venue declared."""

    transport = None

    def guard(self):
        pass

    @property
    def venue_transports(self):
        return {venue.QUOTE_VENUE: self.transport}


class Chain:
    chain = FORK_ARB

    def pin_block(self, selector):
        return FORK_BLOCK

    def _rpc(self, method, params):
        assert method == "eth_call"
        return "0x"

    def __exit__(self, *exc):
        pass


def gateway(monkeypatch, config, transport):  # noqa: F811
    built = Quoting(config, "http://127.0.0.1:1", {}, chain=FORK_ARB, local_transport=True)
    built.reader.__exit__()
    built.reader, built.transport = Chain(), transport
    built.audit_history = lambda state: None
    built.verify_authority = lambda permissions, account: None
    built.estimate_execution = lambda *a, **k: (
        3,
        {"gas_estimate": 100_000, "gas": 125_000, "gas_price": 10**8},
    )
    return built


def funded(monkeypatch, config, amount):  # noqa: F811
    """The source chain's view deciding to bridge `amount`, already approved."""
    intent = config.bridge("move", amount)
    before = observed(config, allowance=amount, wallet_balance=2 * AMOUNT)
    monkeypatch.setattr(execution, "snapshot", lambda backend, name, block, reference=None: before)
    local = project(checkpoint(config), FORK_ARB).model_copy(
        update={
            "decision": decision(config, intent),
            "baseline": (before,),
            "initialized": True,
        }
    )
    return local, intent, before


def test_a_deposit_of_a_changing_venue_is_prepared_reserved_and_verified(monkeypatch, config):  # noqa: F811
    transport = Changing()
    backend = gateway(monkeypatch, config, transport)
    state, intent, before = funded(monkeypatch, config, AMOUNT)
    plan = backend.prepare(state, intent, before, "attempt-1")
    assert plan.operation.kind == "deposit" and transport.asked == [AMOUNT]
    first = plan.operation.quote
    assert first.output_amount == AMOUNT - AMOUNT // 10_000

    # Recording the plan prepares it again: the gateway's own answer, the same plan.
    gateway_service(state, backend).validate_new_plan(state, plan)
    assert transport.asked == [AMOUNT]
    assert backend.prepare(state, intent, before, "attempt-1").operation.quote == first

    # Another amount at the same block is another question, with its own answer.
    half = AMOUNT // 2
    state, intent, before = funded(monkeypatch, config, half)
    smaller = backend.prepare(state, intent, before, "attempt-2")
    assert transport.asked == [AMOUNT, half]
    assert smaller.operation.quote.input_amount == half
    assert smaller.operation.quote.output_amount == half - half // 10_000 - 1
    gateway_service(state, backend).validate_new_plan(state, smaller)
    assert transport.asked == [AMOUNT, half]


def test_a_gateway_without_the_answer_refuses_the_plan_it_did_not_prepare(monkeypatch, config):  # noqa: F811
    """What every venue-quoted deposit met before the gateway kept its answer."""
    transport = Changing()
    state, intent, before = funded(monkeypatch, config, AMOUNT)
    plan = gateway(monkeypatch, config, transport).prepare(state, intent, before, "attempt-1")
    restarted = gateway(monkeypatch, config, transport)
    with pytest.raises(ValueError, match="plan differs from verified gateway preparation"):
        gateway_service(state, restarted).validate_new_plan(state, plan)
    assert transport.asked == [AMOUNT, AMOUNT]
