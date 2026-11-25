"""Typed inputs for modeled bridge backtests: one USDC route from Arbitrum to Base."""

import json
from pathlib import Path
from types import SimpleNamespace

from almanak.backtest.engine import Schedule
from almanak.connectors.across.economic_binding import AcrossEconomic
from almanak.connectors.across.economic_config import EconomicConfig
from almanak.connectors.across.models import Parameters
from almanak.gateway.evm import native_asset
from almanak.models import AssetMetadata
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection, IntervalTrigger
from tests.across_support import ACCOUNTS, ARBITRUM, BASE, BASE_USDC, RECIPIENT, SENDER, USDC, route
from tests.across_support import allocation as authored_allocation

ROOT = Path(__file__).parents[1]
REFERENCE = ROOT / "strategies/across_usdc_arbitrum_base_bridge"
START = 1_790_000_000
ETH = native_asset(ARBITRUM.chain_id)
# The reference's `bridge_quantity` of 100 USDC, in base units.
SENT = 100_000_000
FUNDED = 250_000_000
GAS = 10**18


def author(*, max_fee_bps=10, interval=10, **allocation):
    return Parameters(
        decision_trigger=IntervalTrigger(kind="interval", seconds=interval),
        asset_metadata={
            USDC: AssetMetadata(decimals=6, symbol="USDC"),
            BASE_USDC: AssetMetadata(decimals=6, symbol="USDC"),
        },
        parameters={"bridge_quantity": "100"},
        accounts=ACCOUNTS,
        allocations=(
            authored_allocation(
                **{
                    "route": route(max_fee_bps=max_fee_bps),
                    "capital": SENT,
                    "minimum_native_balance": 10**14,
                    **allocation,
                }
            ),
        ),
    )


def fund(name, chain, address, *balances):
    return {
        "name": name,
        "profile": {
            "kind": "simulation",
            "fund": {"chain": chain.model_dump(mode="json"), "address": address},
        },
        "balances": [{"asset": str(asset), "amount": amount} for asset, amount in balances],
    }


def settings(
    *,
    latency=20,
    window=60,
    end_policy="mark_open",
    cleanup=0,
    costs=None,
    usdc=FUNDED,
    execution_latency=1,
):
    schedule = Schedule(
        start=START,
        end=START + window,
        latency_seconds=execution_latency,
        end_policy=end_policy,
        cleanup_seconds=cleanup,
    )
    wire = {
        "market_model": "economic",
        "schedule": schedule.model_dump(mode="json"),
        "history": [
            {
                "chain": ARBITRUM.model_dump(mode="json"),
                "provider": {
                    "kind": "alchemy",
                    "network": "arb-mainnet",
                    "api_key_env": "ALCHEMY_API_KEY",
                },
            }
        ],
        "funds": [
            fund("arb", ARBITRUM, SENDER, (USDC, usdc), (ETH, GAS)),
            fund("base", BASE, RECIPIENT, (BASE_USDC, 0)),
        ],
        "data": {"arrival_latency_seconds": latency},
        "costs": costs or {"kind": "excluded"},
        "bridge_model": "full_fee_ceiling_fixed_latency",
    }
    return EconomicConfig.model_validate_json(json.dumps(wire))


def inputs(tmp_path, *, parameters=None, **changes):
    """The reference over typed inputs: nothing is resolved and no provider is asked."""
    authored = parameters or author()
    configured = settings(**changes)
    strategy = StrategyDefinition.read(REFERENCE / "strategy.py")
    interval = IntervalSelection(configured=authored.decision_trigger)
    args = SimpleNamespace(
        config=REFERENCE / "config.yaml",
        backtest_config=REFERENCE / "backtest-economic.yaml",
        strategy=REFERENCE / "strategy.py",
        output=tmp_path / "run",
        _backtest_inputs=(authored, configured, strategy),
    )
    return SimpleNamespace(
        args=args,
        author=authored,
        settings=configured,
        strategy=strategy,
        interval=interval,
        config=AcrossEconomic.parameters(authored, configured, interval),
    )
