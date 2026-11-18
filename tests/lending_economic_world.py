"""A literal Compound V3 or Morpho Blue economic run without retained evidence.

The run uses the connector's real prepared book, parameters, initial state and economic
driver over the literal market observations of the connector's offline tests. A borrower
pays gas through a separate payer; a lender pays its own gas.
"""

import json
from dataclasses import dataclass
from types import ModuleType, SimpleNamespace

from almanak.backtest.types import UnixTime
from almanak.caip import AccountReference
from almanak.connectors.compound_v3 import economic_config as compound_config
from almanak.connectors.compound_v3 import economic_data as compound_data
from almanak.connectors.compound_v3 import economic_events as compound_events
from almanak.connectors.compound_v3 import economic_model as compound_model
from almanak.connectors.compound_v3 import economic_parameters as compound_parameters
from almanak.connectors.compound_v3 import economic_reporting as compound_reporting
from almanak.connectors.compound_v3 import economic_state as compound_state
from almanak.connectors.morpho_blue import economic_config as morpho_config
from almanak.connectors.morpho_blue import economic_data as morpho_data
from almanak.connectors.morpho_blue import economic_events as morpho_events
from almanak.connectors.morpho_blue import economic_model as morpho_model
from almanak.connectors.morpho_blue import economic_parameters as morpho_parameters
from almanak.connectors.morpho_blue import economic_reporting as morpho_reporting
from almanak.connectors.morpho_blue import economic_state as morpho_state
from almanak.effects import Portfolio
from almanak.effects import Position as HeldPosition
from almanak.gateway.evm import native_asset
from almanak.models import Account
from almanak.triggers import IntervalSelection
from tests import test_compound_v3_offline as compound_offline
from tests import test_morpho_blue_offline as morpho_offline
from tests.lending_economic_cases import CASES, SPACING

CHAIN = compound_offline.CHAIN
NATIVE = native_asset(CHAIN.chain_id)
GAS = Account(chain=CHAIN, address=AccountReference("0x" + "33" * 20))
START = compound_offline.observation().block.timestamp
# Risk samples every block (12 seconds) to the deadline; accounting every minute.
END = START + 60
CLEANUP = 60
DEADLINE = END + CLEANUP
RISK_SAMPLES = (DEADLINE - START) // SPACING + 1


@dataclass(frozen=True)
class Connector:
    name: str
    label: str
    config: ModuleType
    data: ModuleType
    events: ModuleType
    model: ModuleType
    parameters: ModuleType
    reporting: ModuleType
    state: ModuleType
    offline: ModuleType
    source_name: str
    risk_model: str
    liquidation_model: str

    @property
    def case(self):
        return CASES[self.name]


COMPOUND = Connector(
    name="compound_v3",
    label="Comet",
    config=compound_config,
    data=compound_data,
    events=compound_events,
    model=compound_model,
    parameters=compound_parameters,
    reporting=compound_reporting,
    state=compound_state,
    offline=compound_offline,
    source_name="compound_v3_archive",
    risk_model="comet_oracle_each_block",
    liquidation_model="immediate_comet_absorb",
)
MORPHO = Connector(
    name="morpho_blue",
    label="Morpho",
    config=morpho_config,
    data=morpho_data,
    events=morpho_events,
    model=morpho_model,
    parameters=morpho_parameters,
    reporting=morpho_reporting,
    state=morpho_state,
    offline=morpho_offline,
    source_name="morpho_blue_archive",
    risk_model="morpho_oracle_each_block",
    liquidation_model="immediate_morpho_liquidation",
)
CONNECTORS = {c.name: c for c in (COMPOUND, MORPHO)}


def author(connector):
    if connector is COMPOUND:
        return compound_offline.parameters()
    return morpho_offline.config.__wrapped__()


def settings(connector, *, costs=None, borrower_native=None, lender_native=10**18):
    """Explicit simulation inventory: the borrower's gas comes from a separate payer."""
    weth, usdc = str(compound_offline.WETH), str(compound_offline.USDC)
    lender = [{"asset": usdc, "amount": 1000 * 10**6}]
    if lender_native is not None:
        lender.append({"asset": str(NATIVE), "amount": lender_native})
    borrower = [{"asset": weth, "amount": 10**18}]
    if borrower_native is not None:
        borrower.append({"asset": str(NATIVE), "amount": borrower_native})
    wire = {
        "market_model": "economic",
        "schedule": {
            "start": START,
            "end": END,
            "latency_seconds": 1,
            "end_policy": "teardown",
            "cleanup_seconds": CLEANUP,
        },
        "funds": [
            {
                "name": "borrower",
                "profile": {
                    "kind": "simulation",
                    "fund": compound_offline.BORROWER.model_dump(mode="json"),
                    "payer": GAS.model_dump(mode="json"),
                },
                "balances": borrower,
            },
            {
                "name": "lender",
                "profile": {
                    "kind": "simulation",
                    "fund": compound_offline.LENDER.model_dump(mode="json"),
                },
                "balances": lender,
            },
        ],
        "payers": [
            {
                "name": "gas",
                "fund": GAS.model_dump(mode="json"),
                "balances": [{"asset": str(NATIVE), "amount": 10**18}],
            }
        ],
        "history": [
            {
                "chain": CHAIN.model_dump(mode="json"),
                "provider": {"kind": "alchemy", "network": "eth-mainnet", "api_key_env": "UNUSED"},
            }
        ],
        "costs": costs or {"kind": "excluded"},
        "data": {
            "source": connector.source_name,
            "risk_model": connector.risk_model,
            "sample_seconds": 60,
            "max_age_seconds": 120,
            "max_accounting_samples": 100,
            "max_risk_blocks": 100,
            "max_risk_samples": 100,
            "max_liquidations_per_event": 8,
        },
        "liquidation_model": connector.liquidation_model,
    }
    return connector.config.EconomicConfig.model_validate_json(json.dumps(wire))


def book(connector, configured, samples=None):
    """The connector's real prepared book over literal accounting and risk timelines."""
    case = connector.case
    prepared = case.empty_book(configured)
    risks = case.risk_timeline(
        case.history(RISK_SAMPLES) if samples is None else samples, configured
    )
    accounting = case.accounting_timeline(configured)
    prepared.rates[case.source] = (
        connector.data.RateCurve(accounting) if connector is COMPOUND else accounting
    )
    prepared.risks[case.source] = risks
    prepared.prices = SimpleNamespace(at=lambda time: ())
    return prepared


@dataclass
class World:
    connector: Connector
    settings: object
    config: object
    interval: IntervalSelection
    state: object
    model: object

    def target(self, name):
        return self.config.target(name)

    def holding(self, **payloads):
        """The initial state with some allocations' ownership replaced."""
        positions = tuple(
            HeldPosition[self.connector.state.Position](
                key=p.key,
                payload=p.payload.model_copy(update=payloads.get(p.payload.target.allocation, {})),
            )
            for p in self.state.portfolio.positions
        )
        portfolio = Portfolio[self.connector.state.Position](
            balances=self.state.portfolio.balances, positions=positions
        )
        return self.state.model_copy(update={"portfolio": portfolio})


def world(connector, *, samples=None, **options):
    configured = settings(connector, **options)
    raw = author(connector)
    interval = IntervalSelection(configured=raw.decision_trigger)
    config = connector.parameters.parameters(raw, configured, interval)
    state = connector.parameters.initial(configured, config, interval)
    driver = object.__new__(connector.events.EconomicDriver)
    driver.config, driver.settings = config, configured
    driver.strategy = SimpleNamespace(sha256="0" * 64)
    driver.book = book(connector, configured, samples)
    return World(connector, configured, config, interval, state, driver)


def at(state, time):
    return state.model_copy(update={"time": UnixTime(time)})
