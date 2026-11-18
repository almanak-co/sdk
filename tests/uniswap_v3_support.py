"""V3 candidate parameters and replay inputs without execution authority."""

import gzip
import json
from pathlib import Path

from almanak.chains import evm_network
from almanak.connectors.uniswap_v3.domain import Contracts, TickRange, UniswapPool
from almanak.connectors.uniswap_v3.models import Allocation, Parameters
from almanak.models import Account, AccountBinding, AssetAmount, Eoa, StrategyAccounts
from almanak.triggers import IntervalTrigger

ROOT = Path(__file__).parents[1]


def captured_rows():
    return json.loads(
        gzip.decompress((ROOT / "tests/fixtures/uniswap-v3-reader-reads.json.gz").read_bytes())
    )


def parameters(index=0, *, lp=False, fund=None):
    row = captured_rows()[index]
    pool = UniswapPool.model_validate_json(json.dumps(row["source"]))
    contracts = Contracts.model_validate_json(json.dumps(row["contracts"]))
    tick = row["observed"]["tick"] // pool.tick_spacing * pool.tick_spacing
    usdc = pool.currency0 if index == 0 else pool.currency1
    allocation = Allocation(
        name="position",
        account="fund",
        pool=pool,
        contracts=contracts,
        range=TickRange(lower=tick - 500, upper=tick + 500) if lp else None,
        capital=AssetAmount(asset=usdc, amount=100 * 10**6),
        entry_swap_bps=5000 if lp else 10000,
        hold_seconds=5,
        slippage_bps=100,
        deadline_seconds=300,
        native_asset=evm_network(pool.chain.chain_id).native_asset,
        minimum_native_balance=0,
    )
    if fund is None:
        fund = Account(chain=pool.chain, address="0x" + "12" * 20)
    return Parameters(
        decision_trigger=IntervalTrigger(kind="interval", seconds=5),
        accounts=StrategyAccounts(
            bindings=(AccountBinding(name="fund", profile=Eoa(kind="eoa", fund=fund)),)
        ),
        allocations=(allocation,),
    )
