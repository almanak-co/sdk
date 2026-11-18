"""GM share units and request templates remain distinct from ordinary swap authority."""

import pytest
from eth_abi import decode, encode

from almanak.asset_amounts import AssetAmounts
from almanak.connectors.gmx_v2.exchange_abi import validate_simulation
from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget, Swap
from almanak.connectors.gmx_v2.liquidity_abi import REQUEST_BYTES, create_data
from almanak.connectors.gmx_v2.liquidity_models import LiquidityAllocation, LiquidityCapability
from almanak.connectors.gmx_v2.liquidity_operations import CreateDeposit, CreateWithdrawal
from almanak.connectors.gmx_v2.liquidity_permissions import derive
from almanak.connectors.gmx_v2.liquidity_planning import admit
from almanak.models import Account
from scripts.legacy_configuration.author import read_author
from tests.legacy_references import harness_directory


@pytest.fixture(params=["arbitrum", "avalanche"])
def allocation(request):
    path = harness_directory(f"gmx_v2_{request.param}_liquidity") / "config.yaml"
    return read_author(path).fork_config().allocations[0]


def target(allocation):
    return ExchangeTarget(
        allocation=allocation.name,
        account=allocation.account,
        fund=Account(chain=allocation.chain, address="0x" + "12" * 20),
        market=allocation.market,
        funding_asset=allocation.funding_asset,
    )


def capability(allocation):
    return LiquidityCapability(
        target=target(allocation),
        minimum_market_tokens=allocation.minimum_market_tokens,
        minimum_outputs=allocation.withdrawal_minimums,
        maximum_execution_fee=allocation.execution_fee.amount,
    )


def test_gm_configuration_requires_its_exact_share_asset_for_minimum_and_withdrawal(allocation):
    for field in ("minimum_market_tokens", "partial_withdrawal"):
        changed = getattr(allocation, field).model_copy(
            update={"asset": allocation.market.long_asset}
        )
        with pytest.raises(ValueError, match="GM-share units"):
            LiquidityAllocation.model_validate(allocation.model_dump() | {field: changed})
    shares = allocation.partial_withdrawal.model_copy(update={"amount": 2**128})
    with pytest.raises(ValueError, match="GM-share units"):
        LiquidityAllocation.model_validate(allocation.model_dump() | {"partial_withdrawal": shares})
    cap = capability(allocation)
    swap = Swap(
        target=cap.target,
        amount=allocation.entry_amounts.root[0],
        minimum_output=allocation.withdrawal_minimums.root[1],
        deadline=100,
    )
    with pytest.raises(ValueError, match="cannot authorize a swap"):
        admit(cap, swap)


@pytest.mark.parametrize("side", ["both", "long", "short"])
def test_gm_single_and_dual_currency_inputs_keep_one_permission_layout(allocation, side):
    amounts = AssetAmounts(
        tuple(
            value.model_copy(update={"amount": 0})
            if (side == "long" and index == 1) or (side == "short" and index == 0)
            else value
            for index, value in enumerate(allocation.entry_amounts.root)
        )
    )
    operation = CreateDeposit(
        target=target(allocation),
        amounts=amounts,
        minimum_market_tokens=allocation.minimum_market_tokens,
        execution_fee=allocation.execution_fee,
        admission_deadline=100,
    )
    totals = {value.asset: value.amount for value in operation.inputs}
    assert (
        totals[allocation.execution_fee.asset]
        == allocation.execution_fee.amount + amounts.root[0].amount
    )
    assert len(operation.inputs) == 2
    assert totals[allocation.market.short_asset] == amounts.root[1].amount
    withdrawal = CreateWithdrawal(
        target=operation.target,
        market_tokens=allocation.partial_withdrawal,
        minimum_outputs=allocation.withdrawal_minimums,
        execution_fee=allocation.execution_fee,
        admission_deadline=100,
    )
    for request in (operation, withdrawal):
        calls = decode(("bytes[]",), create_data(request)[4:])[0]
        assert len(calls) == len(request.inputs) + 1
        assert all(len(value) == REQUEST_BYTES for value in calls)
        validate_simulation(
            request,
            "0x" + encode(("bytes[]",), ((*[b""] * len(request.inputs), b"\x01" * 32),)).hex(),
        )
        with pytest.raises(ValueError, match="one request key"):
            validate_simulation(
                request,
                "0x" + encode(("bytes[]",), ((*[b""] * len(request.inputs), bytes(32)),)).hex(),
            )
    grants = derive(capability(allocation))
    assert all(len(grant.conditions) <= 256 for grant in grants)
    assert not any(condition.operator == 28 for grant in grants for condition in grant.conditions)
