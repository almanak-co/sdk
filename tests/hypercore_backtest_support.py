"""Recorded HyperCore history and the reference backtest's typed inputs (phase 5, D4).

`tests/fixtures/hyperliquid-mainnet-history.json.gz` is the evidence of a real
`almanak backtest prepare` of `strategies/hyperliquid_hyperevm_perp` on 5 November 2026
(`scripts/export_hyperliquid_backtest_fixture.py`): the two `/info` answers the dataset
retained (BTC one-minute candles and funding rates for 2026-11-04 00:00 to 06:10 UTC)
and the market's resolution reads. Every test replays them; none reaches the network.
"""

import gzip
import json
from decimal import Decimal
from pathlib import Path

from almanak.backtest.engine import Journal, Simulator
from almanak.backtest.prepared import DatasetWriter, Preparation
from almanak.caip import AssetType
from almanak.configuration.input_values import AmountInput
from almanak.configuration.strategy_parameters import StrategyParameters
from almanak.connectors.dex_economic_dataset import targets
from almanak.connectors.hyperliquid import economic_collection, economic_data
from almanak.connectors.hyperliquid.assets import USDC
from almanak.connectors.hyperliquid.author_inputs import AllocationInput
from almanak.connectors.hyperliquid.authority import declared
from almanak.connectors.hyperliquid.economic_book import HyperCoreBook
from almanak.connectors.hyperliquid.economic_config import EconomicConfig
from almanak.connectors.hyperliquid.economic_model import EconomicModel
from almanak.connectors.hyperliquid.economic_parameters import initial
from almanak.connectors.hyperliquid.economic_state import State
from almanak.connectors.hyperliquid.models import Parameters
from almanak.connectors.hyperliquid.resolution import ResolvedMarket
from almanak.gateway.hypercore_info import InfoRequest, InfoResult
from almanak.gateway.resolution_evm import ResolutionRead, RetainedResolutionReader
from almanak.models import AccountBinding, AssetMetadata, StrategyAccounts
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection, IntervalTrigger

ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "tests/fixtures/hyperliquid-mainnet-history.json.gz"
RAW = json.loads(gzip.decompress(FIXTURE.read_bytes()))
RECORDINGS = {row["label"]: row for row in RAW["recordings"]}
REFERENCE = ROOT / "strategies/hyperliquid_hyperevm_perp"
START = 1793750400  # 2026-11-04T00:00:00Z
END = START + 6 * 3600
FUND = "0x2381602ea30b24a5f9b2bd515395072b7819640e"
HYPE = AssetType("eip155:999/slip44:2457")


def recorded(label: str) -> InfoResult:
    row = RECORDINGS[label]
    return InfoResult.model_validate_json(
        json.dumps({key: row[key] for key in ("request", "received_at", "result")})
    )


class ReplayClient:
    """Answers each request with the recording of exactly that request, or a replacement
    answer the test supplies for it."""

    def __init__(self, answers: dict[InfoRequest, object] | None = None):
        self.by_request = {recorded(label).request: recorded(label) for label in RECORDINGS}
        self.answers = answers or {}
        self.sent: list[InfoRequest] = []

    def fetch(self, request: InfoRequest) -> InfoResult:
        self.sent.append(request)
        if request in self.answers:
            return InfoResult(
                request=request, received_at=1793859591623, result=self.answers[request]
            )
        return self.by_request[request]


def resolution_rows() -> tuple[ResolutionRead, ...]:
    return tuple(
        ResolutionRead.model_validate_json(json.dumps(row)) for row in RAW["resolution"]["evidence"]
    )


def resolved() -> ResolvedMarket:
    return ResolvedMarket.model_validate_json(json.dumps(RAW["resolution"]["facts"]))


def retained_reader():
    market = resolved()
    return RetainedResolutionReader(market.block.chain, resolution_rows())


def parameters(*, leverage=10, size="0.002", hold="2h", slippage=50, side="long") -> Parameters:
    allocation = AllocationInput(
        market="btc",
        account="fund",
        capital=AmountInput(asset=USDC, quantity="30"),
        leverage=leverage,
        slippage_bps=slippage,
        hold_seconds=hold,
    ).resolve("Main", resolved())
    return Parameters(
        decision_trigger=IntervalTrigger(kind="interval", seconds=60),
        asset_metadata={
            USDC: AssetMetadata(decimals=6, symbol="USDC"),
            HYPE: AssetMetadata(decimals=18),
        },
        parameters=StrategyParameters({"size": size, "side": side}),
        accounts=StrategyAccounts(bindings=(binding(),)),
        allocations=(allocation,),
    )


def binding() -> AccountBinding:
    return AccountBinding.model_validate_json(
        json.dumps(
            {
                "name": "fund",
                "profile": {
                    "kind": "simulation",
                    "fund": {"chain": chain(), "address": FUND},
                    "payer": None,
                },
            }
        )
    )


def chain() -> dict:
    return {"chain_id": "eip155:999", "environment": "mainnet"}


def settings(*, start=START, end=END, cleanup=600, slippage_bps="2", capital=30_000_000, **data):
    wire = {
        "schedule": {
            "start": start,
            "end": end,
            "decision_interval_override_seconds": None,
            "latency_seconds": 1,
            "end_policy": "teardown",
            "cleanup_seconds": cleanup,
        },
        "history": [
            {
                "chain": chain(),
                "provider": {
                    "kind": "alchemy",
                    "network": "hyperliquid-mainnet",
                    "api_key_env": "ALCHEMY_API_KEY",
                },
            }
        ],
        "funds": [
            {
                "name": "fund",
                "profile": {
                    "kind": "simulation",
                    "fund": {"chain": chain(), "address": FUND},
                    "payer": None,
                },
                "balances": [{"asset": str(USDC), "amount": capital}],
            }
        ],
        "market_model": "economic",
        "position_model": "hypercore_trade_candles",
        "costs": {"kind": "excluded"},
        "data": {
            "source": "hyperliquid_info",
            "candles": "1m",
            "max_age_seconds": 120,
            "fills": {"slippage_bps": slippage_bps, "taker_fee_bps": "4.5"},
            **data,
        },
    }
    return EconomicConfig.model_validate_json(json.dumps(wire))


def preparation(config, settings) -> Preparation:
    return Preparation(
        model=economic_data.MODEL,
        acquisition_revision=economic_data.REVISION,
        policy_digest=HyperCoreBook.identity(config, settings),
        requirements=tuple(
            requirement
            for target in targets(config)
            for requirement in economic_data.requirements(target, settings)
        ),
    )


def prepared(directory: Path, config, settings, client=None) -> HyperCoreBook:
    """The dataset a preparation commits from these answers, opened as the run opens it."""
    writer = DatasetWriter(directory, preparation(config, settings))
    try:
        for target in targets(config):
            economic_collection.collect(writer, client or ReplayClient(), target, settings)
        writer.publish()
    finally:
        writer.close()
    return HyperCoreBook(directory, config, settings)


def strategy() -> StrategyDefinition:
    return StrategyDefinition.read(REFERENCE / "strategy.py")


def model(config, settings, book, definition=None) -> EconomicModel:
    return EconomicModel(
        config, settings, definition or strategy(), book, authority=declared(config)
    )


def interval(config) -> IntervalSelection:
    return IntervalSelection(configured=config.decision_trigger, override_seconds=None)


def seed(config, settings):
    return initial(settings, config, interval(config))


def run(path: Path, driver, start_state):
    journal = Journal(path, State)
    journal.create(start_state)
    return Simulator(journal, driver).run()


def usdc(text: str) -> int:
    return int(Decimal(text) * 10**6)
