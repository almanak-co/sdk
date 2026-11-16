"""Independent economic math against deployed V4 after actual reference entry."""

import json
import sys
from pathlib import Path

import pytest
from eth_utils import to_checksum_address

from almanak.backtest.economic_acquisition import read_rates, read_risk
from almanak.backtest.economic_data import OraclePrice
from almanak.backtest.economic_liquidation import settle_liquidations
from almanak.backtest.economic_math import MarketView, risk_view
from almanak.backtest.economic_state import EconomicPosition
from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.gateway.ethereum import EthereumReader
from almanak.gateway.fork_workspace import ForkSet
from almanak.gateway.safe_fixture import calldata
from almanak.gateway.safe_funding import fund_usdc_storage
from tests.author_evidence import retained_author
from tests.fork.support import processes, start_gateway, wait_state
from tests.fork.test_safe_recovery import DRIVER, STRATEGY, one_safe_config
from tests.strategy_fork.harness import Case, install_permissions
from tests.strategy_fork.scenarios.aave_v4_weth_supply.assertions import entered

pytestmark = pytest.mark.fork


def configuration(chain="ethereum"):
    if chain == "ethereum":
        raw = one_safe_config().model_dump(mode="json")
    else:
        raw = json.loads(retained_author(strategy_for(chain), "config.json").read_text())
    collateral = 10
    raw["allocations"][0]["amount"] = collateral * 10**18
    raw["allocations"][0]["hold_seconds"] = None
    principal = 8000 if chain == "ethereum" else 8
    raw["debts"][0]["amount"] = principal * 10**6
    raw["funds"][0]["assets"][0]["balance"]["amount"] = (collateral + 1) * 10**18
    return ForkConfig.model_validate_json(json.dumps(raw))


def strategy_for(chain):
    if chain == "ethereum":
        return STRATEGY
    return STRATEGY.parents[1] / "aave_v4_avalanche_borrowing/strategy.py"


def entered_for(chain):
    if chain == "ethereum":
        return entered
    from tests.strategy_fork.scenarios.aave_v4_avalanche_borrowing.assertions import (
        entered as avalanche,
    )

    return avalanche


def position(reader, target, block):
    args = (target.reserve.reserve_id, target.fund.address)
    drawn, premium, offset, supplied, key = reader.call(
        target.reserve.spoke,
        "getUserPosition(uint256,address)",
        block,
        inputs=("uint256", "address"),
        args=args,
        outputs=("uint120", "uint120", "int200", "uint120", "uint32"),
    )
    enabled, _ = reader.call(
        target.reserve.spoke,
        "getUserReserveStatus(uint256,address)",
        block,
        inputs=("uint256", "address"),
        args=args,
        outputs=("bool", "bool"),
    )
    (risk,) = reader.call(
        target.reserve.spoke,
        "getUserLastRiskPremium(address)",
        block,
        inputs=("address",),
        args=(target.fund.address,),
    )
    return EconomicPosition(
        target=target,
        shares=supplied,
        drawn_shares=drawn,
        premium_shares=premium,
        premium_offset_ray=offset,
        collateral=enabled,
        dynamic_config_key=key,
        risk_premium_bps=risk,
    )


def market(reader, source, block):
    risk = read_risk(reader, source, block)
    rates = read_rates(reader, risk.reserve, block)
    (oracle,) = reader.call(source.spoke, "ORACLE()", block, outputs=("address",))
    (feed,) = reader.call(
        oracle,
        "getReserveSource(uint256)",
        block,
        inputs=("uint256",),
        args=(source.reserve_id,),
        outputs=("address",),
    )
    (price,) = reader.call(
        oracle, "getReservePrice(uint256)", block, inputs=("uint256",), args=(source.reserve_id,)
    )
    return MarketView(
        supply_index=rates.supply_index_ray,
        borrow_index=rates.borrow_index_ray,
        risk=risk,
        price=OraclePrice(source=source, oracle=oracle, feed=feed, price=price, dependencies=()),
    )


def compare_liquidation(node, config, price, output):
    collateral, debt = (
        config.target(config.allocations[0].name),
        config.target(config.debts[0].name),
    )
    liquidator = node.funded_signer("", 0, 10**18, 99)
    token = debt.asset.reference
    fund_usdc_storage(node, liquidator.address, token, 20000 * 10**6)
    node.rpc.send(
        liquidator,
        to_checksum_address(token),
        calldata(
            "approve(address,uint256)", ("address", "uint256"), (debt.reserve.spoke, 2**256 - 1)
        ),
    )
    # Only the isolated Anvil fixture receives a synthetic shock. The archive and
    # production source are untouched; this is protocol math evidence, not history.
    feed = config.debts[0].collateral_price_sources[0]
    code = "0x7f" + price.to_bytes(32, "big").hex() + "60005260206000f3"
    node.rpc("anvil_setCode", [feed, code])
    node.rpc("evm_mine")
    snapshot = node.rpc("evm_snapshot")
    before_time = int(node.rpc("eth_getBlockByNumber", ["latest", False])["timestamp"], 16)
    execution_time = before_time + 12
    node.rpc("evm_setNextBlockTimestamp", [execution_time])
    node.rpc("evm_mine")
    targets = (collateral, debt)
    predicted, movements = prediction(node, targets)
    assert len(movements) == 1
    assert node.rpc("evm_revert", [snapshot])
    node.rpc("evm_setNextBlockTimestamp", [execution_time])
    receipt = node.rpc.send(
        liquidator,
        to_checksum_address(debt.reserve.spoke),
        calldata(
            "liquidationCall(uint256,uint256,address,uint256,bool)",
            ("uint256", "uint256", "address", "uint256", "bool"),
            (
                collateral.reserve.reserve_id,
                debt.reserve.reserve_id,
                debt.fund.address,
                2**256 - 1,
                True,
            ),
        ),
    )
    block, observed = observation(node, targets, execution_time)
    retain_comparison(output, price, block, receipt, predicted, observed, movements)
    assert_positions(predicted, observed)


def prediction(node, targets):
    collateral = targets[0]
    with EthereumReader(collateral.fund.chain, node.rpc.endpoint) as reader:
        block = reader.pin_block("latest")
        before = tuple(position(reader, t, block) for t in targets)
        markets = tuple(market(reader, t.reserve, block) for t in targets)
        risk = risk_view(before, markets)
        actual_risk = reader.call(
            collateral.reserve.spoke,
            "getUserAccountData(address)",
            block,
            inputs=("address",),
            args=(collateral.fund.address,),
            outputs=("uint256",) * 7,
        )
        assert risk.health_factor_wad == actual_risk[2]
        assert risk.health_factor_wad < 10**18
        predicted, movements = settle_liquidations(before, markets)
    return predicted, movements


def observation(node, targets, execution_time):
    collateral = targets[0]
    with EthereumReader(collateral.fund.chain, node.rpc.endpoint) as reader:
        block = reader.pin_block("latest")
        assert block.timestamp == execution_time
        observed = tuple(position(reader, t, block) for t in targets)
    return block, observed


def retain_comparison(output, price, block, receipt, predicted, observed, movements):
    record = {
        "price": price,
        "block": block.model_dump(mode="json"),
        "receipt": receipt,
        "predicted": [p.model_dump(mode="json") for p in predicted],
        "observed": [p.model_dump(mode="json") for p in observed],
        "movements": [m.model_dump(mode="json") for m in movements],
    }
    output.write_text(json.dumps(record, indent=2) + "\n")


def assert_positions(predicted, observed):
    fields = (
        "shares",
        "drawn_shares",
        "premium_shares",
        "premium_offset_ray",
        "collateral",
        "dynamic_config_key",
        "risk_premium_bps",
    )
    for expected, actual in zip(predicted, observed, strict=True):
        assert {f: getattr(expected, f) for f in fields} == {f: getattr(actual, f) for f in fields}


@pytest.mark.parametrize("chain", ["ethereum", "avalanche"])
def test_economic_liquidation_matches_deployed_contract(request, tmp_path, chain):
    config = configuration(chain)
    strategy = strategy_for(chain)
    artifact = Path(__file__).parents[2] / "artifacts/economic-contract-proof" / chain
    artifact.mkdir(parents=True, exist_ok=True)
    with (
        ForkSet(config, request.config.getoption("--fork-env")) as nodes,
        processes(tmp_path) as launch,
    ):
        node = nodes.for_chain(config.origin.chain)
        directory = tmp_path / "deployment"
        deployment = nodes.prepare(config, directory)
        journal, arguments = start_gateway(node, directory, launch)
        install_permissions(
            Case("economic", strategy, strategy.with_name("config.json"), Path(__file__)),
            nodes,
            directory,
            deployment,
            arguments,
        )
        driver = launch([sys.executable, str(DRIVER), "--strategy", str(strategy), *arguments])
        state = wait_state(journal, entered_for(chain), driver)
        driver.terminate()
        driver.wait(timeout=10)
        (artifact / "reference-entry.json").write_text(state.model_dump_json(indent=2) + "\n")
        prices = (950 * 10**8, 100 * 10**8) if chain == "ethereum" else (95 * 10**6, 10**7)
        for price in prices:
            snapshot = node.rpc("evm_snapshot")
            compare_liquidation(node, state.config, price, artifact / f"liquidation-{price}.json")
            assert node.rpc("evm_revert", [snapshot])
