"""Retained deployed components distinguish missing fees from measured settlement."""

from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.connectors.uniswap_v3.fork import fork_allocation
from almanak.connectors.uniswap_v3.reporting import payer_reports, position_report
from almanak.models import AccountBinding, AssetAmount, Eoa, StrategyAccounts
from tests.test_uniswap_v3_reconciliation import CAPTURE, decoded
from tests.uniswap_v3_support import parameters


@pytest.mark.parametrize("row", CAPTURE, ids=lambda r: r["origin"]["chain"]["chain_id"])
def test_retained_collection_and_missing_split_do_not_change_owned_inventory(row):
    operation, before, after, effects = decoded(
        next(step for step in row["steps"] if step["operation"]["kind"] == "close")
    )
    outcome = SimpleNamespace(
        operation=operation,
        after=after,
        success=True,
        fees0=7,
        fees1=None,
        tx_hash="recorded",
        nonce=4,
    )
    state = SimpleNamespace(baseline=(before,), outcomes=(outcome,), closure=(), initialized=True)
    result = position_report(state, after.owned)
    assert result["ownership"] == after.owned.model_dump(mode="json")
    assert result["collected_fees"][0]["amount"] == "7"
    assert result["collected_fees"][1]["amount"] is None
    assert result["last_observation"] == after.model_dump(mode="json")
    state.initialized = False
    state.baseline, state.outcomes = (), ()
    result = position_report(state, before.owned)
    assert result["measurement_status"] == "unobserved"
    assert all(f["amount"] is None for f in result["collected_fees"])


def test_receipt_gas_keeps_the_submitter_and_native_asset():
    _, before, _, _ = decoded(CAPTURE[1]["steps"][0])
    payer = before.payer
    state = SimpleNamespace(
        initialized=True,
        config=SimpleNamespace(
            accounts=StrategyAccounts(
                bindings=(AccountBinding(name="fund", profile=Eoa(kind="eoa", fund=payer)),)
            )
        ),
        outcomes=(
            SimpleNamespace(
                execution=SimpleNamespace(submitter=payer),
                gas_paid=AssetAmount(asset=before.native_balance.asset, amount=123),
                nonce=9,
            ),
        ),
    )
    result = payer_reports(state)[0]
    assert result["submitter"] == payer.model_dump(mode="json")
    assert result["gas_paid"] == {
        "asset": str(before.native_balance.asset),
        "amount": "123",
        "unit": "base_units",
    }
    assert result["nonces"] == [9]


@pytest.mark.parametrize("index", [0, 1])
def test_fork_routing_changes_environment_without_relabeling_markets(index):
    config = parameters(index)
    allocation = config.allocations[0]
    chain = allocation.chain.model_copy(update={"environment": "identified-fork"})
    routed = fork_allocation(allocation, chain)
    assert routed.pool.chain == chain
    assert routed.pool.currency0 == allocation.pool.currency0
    assert routed.pool.currency1 == allocation.pool.currency1
    assert routed.contracts == allocation.contracts
    assert routed.capital == allocation.capital
    with pytest.raises(ValueError, match="CAIP identity"):
        fork_allocation(allocation, chain.model_copy(update={"chain_id": ChainId("eip155:10")}))
