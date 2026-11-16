"""Replacement admission preserves other chains and explicit signing transport identity."""

from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.connectors.composition import connector_for
from almanak.gateway.ethereum import DataError
from almanak.gateway.routing import ChainRouter, ScopedGateway
from almanak.gateway.service import GatewayService
from almanak.gateway.signed_transaction import verify_signed
from almanak.runtime import strategy_capabilities
from almanak.transaction_family import family_report, start_family
from almanak.work import ChainFault, merge, project, work_for
from tests.network_support import production_config
from tests.test_transaction_replacement import case as case
from tests.test_transaction_replacement import production_attempt as production_attempt
from tests.test_transaction_replacement import production_recorded as production_recorded


def combined_config(original):
    other = production_config("aave_v4_avalanche_borrowing")
    production = original.production.model_copy(
        update={
            "networks": (*original.production.networks, *other.production.networks),
            "gas": (*original.production.gas, *other.production.gas),
        }
    )
    config = original.model_copy(
        update={
            "accounts": original.accounts.model_copy(
                update={"bindings": (*original.accounts.bindings, *other.accounts.bindings)}
            ),
            "allocations": (*original.allocations, *other.allocations),
            "debts": (*original.debts, *other.debts),
            "production": production,
        }
    )
    return type(config).model_validate_json(config.model_dump_json())


def retained_work(case):
    config = combined_config(case.state.config)
    case.backend.config = config
    case.backend.production = config.production
    pending = start_family(config, case.state.pending.model_copy(update={"family": None}))
    local = case.state.model_copy(update={"config": config, "pending": pending})
    capabilities = strategy_capabilities(local.strategy, config)
    state = connector_for(config).lifecycle.initial(config, local.strategy, capabilities, None)
    chain = pending.operation.target.fund.chain
    state = merge(state, chain, local)
    other = state.work[1].model_copy(
        update={"fault": ChainFault(kind="transport", message="other chain unavailable")}
    )
    state = state.model_copy(update={"work": (state.work[0], other)})
    return type(state).model_validate_json(state.model_dump_json())


def unavailable_adapter():
    raise AssertionError("replacement must not contact the other chain")


def routed_service(case, state, path, strategy):
    chain, other = (work.chain for work in state.work)
    case.backend.audit_history = lambda _: None
    router = ChainRouter(state.config, {chain: lambda: case.backend, other: unavailable_adapter})
    service = GatewayService(state.config, path, router, lease_seconds=30)
    service.journal.create(state.config, strategy)
    service.journal.update(lambda _: state)
    service.journal.acquire("ab" * 16, 30)
    return service


def local_client(service, chain, deployment_id):
    def dispatch(method, **fields):
        request = service.request_type(coordinator="ab" * 16, deployment_id=deployment_id, **fields)
        result = service.dispatch(method, request)
        return result.model_dump(mode="json") if hasattr(result, "model_dump") else result

    parent = SimpleNamespace(connector=service.connector, scoped=dispatch)
    return ScopedGateway(parent, chain)


def test_routed_family_transitions_recover_without_erasing_other_work(
    case, tmp_path, reference_strategy
):
    state = retained_work(case)
    chain, other = (work.chain for work in state.work)
    service = routed_service(case, state, tmp_path / "state.sqlite", reference_strategy)
    client = local_client(service, chain, state.deployment_id)
    pending = project(state, chain).pending
    attempted = client.family(pending, "attempt", 0)
    assert client.broadcast(attempted.pending)
    case.backend.reader.advance()
    case.clock[0] += 5
    reserved = client.family(attempted.pending, "reserve", 1)
    assert reserved.pending.family.proposal.index == 1
    retained = service.journal.load()
    assert retained.pending is None and retained.decision is None
    assert work_for(retained, other) == work_for(state, other)
    assert (retained.owned, retained.outcomes, retained.memory) == (
        state.owned,
        state.outcomes,
        state.memory,
    )
    assert family_report(retained)["chains"][0]["pending"]["proposal"]["index"] == 1
    restarted = GatewayService(
        state.config, service.journal.path, service.backend, lease_seconds=30
    )
    recovered = local_client(restarted, chain, state.deployment_id)
    assert recovered.family(reserved.pending, "reserve", 1) == reserved
    signed = recovered.family(reserved.pending, "sign", 1)
    assert len(signed.pending.family.candidates) == 2
    assert work_for(restarted.journal.load(), other) == work_for(state, other)


def test_replacement_route_is_required_and_fenced(case, tmp_path, reference_strategy):
    state = retained_work(case)
    service = routed_service(case, state, tmp_path / "state.sqlite", reference_strategy)
    pending = state.work[0].pending
    request = service.request_type(
        coordinator="ab" * 16,
        deployment_id=state.deployment_id,
        family_id=pending.family.identity,
        candidate_index=0,
        action="attempt",
    )
    with pytest.raises(ValueError, match="explicit chain route"):
        service.dispatch("family", request)
    service.journal.release("ab" * 16)
    service.journal.acquire("cd" * 16, 30)
    with pytest.raises(RuntimeError, match="superseded"):
        service.dispatch("family", request.model_copy(update={"chain": state.work[0].chain}))
    assert service.journal.load() == state


def test_signature_checks_use_explicit_transport_without_changing_asset_identity(case):
    pending = case.state.pending
    transport = ChainId("eip155:31337")
    case.backend.reader.transport_chain = transport
    signed = case.backend._sign(
        pending.operation, pending.execution, pending.nonce, pending.gas, pending.gas_price
    )
    candidate = pending.model_copy(
        update={
            "family": None,
            "raw_transaction": "0x" + signed.raw_transaction.hex(),
            "tx_hash": "0x" + signed.hash.hex(),
        }
    )
    verify_signed(candidate, pending.execution, transport)
    with pytest.raises(DataError, match="identity"):
        verify_signed(candidate, pending.execution, ChainId("eip155:1"))
    assert candidate.operation.target.reserve.asset.chain == ChainId("eip155:1")
