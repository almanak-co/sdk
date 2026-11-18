"""Economic assertions shared by the pinned and discovered Aave proofs."""

from pathlib import Path

from eth_utils import keccak

from almanak.strategy import StrategyDefinition

STRATEGY = Path(__file__).resolve().parents[2] / "strategies/aave_v4_weth_supply/strategy.py"


def assert_closed(fork, state):
    assert_closed_checkpoint(state)
    assert len({item.tx_hash for item in state.outcomes}) == len(state.outcomes)
    for allocation, baseline, closed in zip(
        state.config.allocations, state.baseline, state.closure, strict=True
    ):
        assert_closed_identity(state, allocation, closed)
        wallet_outcomes = account_outcomes(state, allocation.account)
        outcomes = [
            item for item in wallet_outcomes if item.operation.allocation == allocation.name
        ]
        assert_closed_balances(allocation, baseline, closed, outcomes, wallet_outcomes)
        assert_one_supply_log(fork, state, allocation)


def assert_closed_checkpoint(state):
    assert state.schema_version == 5
    assert (state.mode, state.pending, state.plan) == ("closed", None, None)
    assert state.strategy == StrategyDefinition.read(STRATEGY)
    assert_successful_outcomes(state.outcomes)


def assert_successful_outcomes(outcomes):
    assert all(item.success for item in outcomes)


def assert_closed_identity(state, allocation, closed):
    assert allocation.reserve.asset.chain == allocation.reserve.chain.chain_id
    assert str(allocation.reserve.chain.chain_id) == "eip155:31337"
    assert str(state.config.fork_origin.chain.chain_id) == "eip155:1"
    assert str(closed.native_balance.asset) == "eip155:31337/slip44:60"


def account_outcomes(state, account):
    return [item for item in state.outcomes if item.operation.account == account]


def outcomes_of_kind(outcomes, kind):
    return [item for item in outcomes if item.operation.kind == kind]


def assert_closed_balances(allocation, baseline, closed, outcomes, wallet_outcomes):
    assert (closed.position.supplied_shares, closed.position.total_debt, closed.allowance) == (
        0,
        0,
        0,
    )
    supply = outcomes_of_kind(outcomes, "supply")
    withdrawal = outcomes_of_kind(outcomes, "withdraw")
    assert len(supply) == len(withdrawal) == 1
    assert_gas_balance(allocation, baseline, closed, wallet_outcomes)
    assert_supply_round_trip(allocation, supply[0], withdrawal[0])
    assert_wallet_round_trip(baseline, closed, wallet_outcomes)


def assert_gas_balance(allocation, baseline, closed, outcomes):
    costs = [item.gas_paid for item in outcomes]
    assert all(cost.asset == allocation.native_asset for cost in costs)
    assert baseline.native_balance.amount - closed.native_balance.amount == sum(
        cost.amount for cost in costs
    )


def assert_supply_round_trip(allocation, supply, withdrawal):
    assert supply.assets == allocation.capital
    assert supply.shares == withdrawal.shares


def assert_wallet_round_trip(baseline, closed, outcomes):
    effects = [
        o for o in outcomes if o.operation.target.reserve.asset == baseline.target.reserve.asset
    ]
    supplied = total_assets(effects, "supply")
    withdrawn = total_assets(effects, "withdraw")
    assert closed.wallet_balance == baseline.wallet_balance - supplied + withdrawn
    assert closed.wallet_balance >= baseline.wallet_balance


def total_assets(outcomes, kind):
    return sum(item.assets for item in outcomes_of_kind(outcomes, kind))


def assert_one_supply_log(fork, state, allocation):
    fund = state.config.accounts.binding(allocation.account).profile.fund.address
    logs = fork.rpc(
        "eth_getLogs",
        [
            {
                "address": allocation.reserve.spoke,
                "fromBlock": hex(
                    (
                        fork.origin.number
                        if state.config.production
                        else origin_block(state.config, allocation.reserve.chain)
                    )
                    + 1
                ),
                "toBlock": "latest",
                "topics": [
                    "0x" + keccak(text="Supply(uint256,address,address,uint256,uint256)").hex(),
                    "0x" + allocation.reserve.reserve_id.to_bytes(32, "big").hex(),
                    "0x" + fund[2:].rjust(64, "0"),
                    "0x" + fund[2:].rjust(64, "0"),
                ],
            }
        ],
    )
    assert len(logs) == 1


def origin_block(config, chain):
    from almanak.gateway.environments import fork_binding

    return fork_binding(config, chain).origin.number
