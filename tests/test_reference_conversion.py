"""A converted reference keeps exactly the effective configuration of its legacy form.

The harness loader disappears with the conversion, so the comparison is against
frozen snapshots of what it produced (tests/fixtures/author/legacy), never a
second live reader. Nothing here reaches a chain: each resolved market is a
duck-typed rebuild of the frozen legacy allocation the harness emitted.
"""

import gzip
import json
import re
from datetime import datetime
from operator import itemgetter
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from almanak.backtest.config import HistoryBinding
from almanak.caip import ChainId
from almanak.check_warnings import MIN_LIVE_DECISION_INTERVAL_SECONDS
from almanak.configuration.backtest_inputs import read_backtest_inputs, resolve_backtest
from almanak.configuration.parameter_types import parameter_type
from almanak.configuration.parameters import resolved_inventory
from almanak.configuration.resolution import simulation_binding
from almanak.configuration.unresolved import read_inputs
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.aave_v3.domain import Reserve as AaveV3Reserve
from almanak.connectors.aave_v3.economic_acquisition import bounds as aave_v3_risk_bounds
from almanak.connectors.aave_v4.domain import AaveReserve
from almanak.connectors.aerodrome.domain import AerodromePool
from almanak.connectors.aerodrome.domain import Contracts as AerodromeContracts
from almanak.connectors.aerodrome_slipstream.domain import SlipstreamPool
from almanak.connectors.compound_v3.domain import Reserve as CompoundReserve
from almanak.connectors.curve.domain import CurvePool
from almanak.connectors.gmx_v2.exchange_domain import ExchangeMarket
from almanak.connectors.gmx_v2.networks import roles_for
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue.domain import Reserve as MorphoReserve
from almanak.connectors.pancakeswap_v3.domain import PancakePool
from almanak.connectors.sparklend.domain import Reserve as SparkReserve
from almanak.connectors.uniswap_v3.domain import UniswapPool
from almanak.connectors.uniswap_v4.domain import UniswapPool as UniswapV4Pool
from almanak.connectors.uniswap_v4.models import Deployment
from almanak.connectors.v3_pool_domain import Contracts
from almanak.gateway.operator_config import read_operator_config
from almanak.gateway.production_config import GasPolicy
from almanak.models import StrategyAccounts
from scripts.ci.check_strategies import CONVERTED, named_connectors
from scripts.extract_legacy_snapshots import BASELINE, COMPOSED, SCHEMA, compress, extract
from tests.legacy_references import current_effective

ROOT = Path(__file__).resolve().parents[1]
SNAPSHOTS = ROOT / "tests/fixtures/author/legacy"
STRATEGIES = ROOT / "strategies"
AUTHOR_FILES = ("config.yaml", "config.production.yaml")
# Author-only blocks of the frozen config; a parameter set carries neither.
AUTHOR_ONLY = ("author_accounts", "execution")
# A frozen entry that failed names the chain the harness support matrix refused.
REFUSAL = re.compile(r"connector does not support (eip155:\d+)$")


def snapshot_documents() -> dict[Path, dict]:
    return {
        path: json.loads(gzip.decompress(path.read_bytes()))
        for path in sorted(SNAPSHOTS.glob("*.json.gz"))
    }


def snapshot_index() -> dict[str, tuple[str, dict]]:
    """Every frozen reference, with the connector file it was grouped into."""
    return {
        name: (path.name.removesuffix(".json.gz"), entry)
        for path, document in snapshot_documents().items()
        for name, entry in document["references"].items()
    }


def legacy_snapshot(name: str) -> dict:
    index = snapshot_index()
    if name not in index:
        raise AssertionError(
            f"no legacy snapshot for {name} under {SNAPSHOTS}; "
            "re-run scripts/extract_legacy_snapshots.py against the baseline capture"
        )
    return index[name][1]


def effective(entry: dict, file: str) -> dict:
    record = entry["files"][file]
    assert record["status"] == "normalized", f"{entry['name']}/{file}: {record.get('message')}"
    # Lending budgets translate to `capital` and `borrow` before the retired fields go.
    return current_effective(record["value"])


def reserve_market(reserve_type):
    """The three resolved facts lending position inputs read, from the frozen allocation."""

    def build(allocation: dict, metadata: dict) -> SimpleNamespace:
        reserve = reserve_type.model_validate_json(json.dumps(allocation["reserve"]))
        return SimpleNamespace(
            selection=SimpleNamespace(chain=reserve.chain.chain_id),
            reserve=reserve,
            asset=SimpleNamespace(decimals=metadata[str(reserve.asset)]["decimals"]),
        )

    return build


def pool_market(pool_type, contracts_type=Contracts):
    """The pool, periphery and currency decimals a pool position input reads."""

    def build(allocation: dict, metadata: dict) -> SimpleNamespace:
        pool = pool_type.model_validate_json(json.dumps(allocation["pool"]))
        currencies = tuple(
            SimpleNamespace(identifier=currency, decimals=metadata[str(currency)]["decimals"])
            for currency in (pool.currency0, pool.currency1)
        )
        return SimpleNamespace(
            selection=SimpleNamespace(chain=pool.chain.chain_id),
            pool=pool,
            contracts=contracts_type.model_validate_json(json.dumps(allocation["contracts"])),
            assets=currencies,
            currencies=currencies,
        )

    return build


def curve_market(allocation: dict, metadata: dict) -> SimpleNamespace:
    """The pool and the ordered coin decimals Curve position inputs read."""
    pool = CurvePool.model_validate_json(json.dumps(allocation["pool"]))
    return SimpleNamespace(
        selection=SimpleNamespace(chain=pool.chain.chain_id),
        pool=pool,
        coins=tuple(
            SimpleNamespace(identifier=coin, decimals=metadata[str(coin)]["decimals"])
            for coin in pool.coins
        ),
    )


def gmx_market(allocation: dict, metadata: dict) -> SimpleNamespace:
    """The exchange market and verified decimals GMX position inputs read.

    The frozen perpetual market carries the ten roles its request templates use;
    the reviewed closure supplies the sixteen the resolver verifies, and the
    position input projects them back. Position references convert nothing at GM
    precision, so their frozen metadata carries no share entry and `share` stays
    absent there.
    """
    raw = allocation["market"]
    chain = ChainId(raw["chain"]["chain_id"])
    roles = roles_for(chain).model_dump(mode="json")
    market = ExchangeMarket.model_validate_json(json.dumps({**raw, "contracts": roles}))
    tokens = dict.fromkeys(
        (
            market.index_asset,
            market.long_asset,
            market.short_asset,
            market.wrapped_native,
            market.share_asset,
        )
    )
    assets = tuple(
        SimpleNamespace(identifier=token, decimals=metadata[str(token)]["decimals"])
        for token in tokens
        if str(token) in metadata
    )
    return SimpleNamespace(
        selection=SimpleNamespace(chain=chain),
        market=market,
        assets=assets,
        share=next((a for a in assets if a.identifier == market.share_asset), None),
    )


# Reviewed corrections to frozen backtest windows, in seconds. The Ethereum and
# Base Uniswap V3 references pinned their window four hours before the connector's
# declared availability (a local time written with a UTC suffix); the converted
# files start the window at the declared availability instead.
REVIEWED_WINDOW_SHIFTS = {
    "uniswap_v3_base_lp": 14400,
    "uniswap_v3_base_trading": 14400,
    "uniswap_v3_ethereum_lp": 14400,
    "uniswap_v3_ethereum_trading": 14400,
}

# This composed reference's frozen 256/512 limits predate a retained acquisition
# that measured 353 Arbitrum blocks and 706 reserve samples. The author file uses
# the reviewed 512/1024 ceilings; immutable conversion snapshots remain unchanged.
REVIEWED_ACQUISITION_LIMIT_CORRECTIONS = {
    "arbitrum_aave_pancake_borrow_and_trade": {
        "max_risk_blocks": 512,
        "max_risk_samples": 1024,
    }
}


# A hold the reference's own code never reads is dead: connectors never act on
# `hold_seconds`, so these author files omit it and hold until teardown. The frozen
# hold is compared only where the strategy reads it.
def reads_hold(name: str) -> bool:
    return any("hold_seconds" in path.read_text() for path in (STRATEGIES / name).rglob("*.py"))


def without_hold(position):
    """A resolved position, as a dump or a model, with no scheduled exit."""
    if isinstance(position, dict):
        return {field: value for field, value in position.items() if field != "hold_seconds"}
    if hasattr(position, "hold_seconds"):
        return position.model_copy(update={"hold_seconds": None})
    return position


def reviewed_holds(name: str, positions: dict) -> dict:
    if reads_hold(name):
        return positions
    return {key: without_hold(position) for key, position in positions.items()}


# One builder per converted connector; a new connector adds one entry here.
LEGACY_MARKETS = {
    "aave_v4": reserve_market(AaveReserve),
    "uniswap_v3": pool_market(UniswapPool),
    "pancakeswap_v3": pool_market(PancakePool),
    "uniswap_v4": pool_market(UniswapV4Pool, Deployment),
    "aave_v3": reserve_market(AaveV3Reserve),
    "sparklend": reserve_market(SparkReserve),
    "compound_v3": reserve_market(CompoundReserve),
    "morpho_blue": reserve_market(MorphoReserve),
    "aerodrome": pool_market(AerodromePool, AerodromeContracts),
    "aerodrome_slipstream": pool_market(SlipstreamPool),
    "curve": curve_market,
    "gmx_v2": gmx_market,
}


def refused_chain(entry: dict, file: str) -> str:
    """The chain a frozen candidate was refused on; "" when the harness normalized it."""
    record = entry["files"][file]
    if record["status"] == "normalized":
        return ""
    found = REFUSAL.search(record.get("message", ""))
    assert found, f"{entry['name']}/{file}: refusal does not name a chain: {record.get('message')}"
    return found.group(1)


def assert_public_root_refuses(name: str, chain: str):
    """The public root must refuse a candidate offline, on exactly the frozen chain."""
    with pytest.raises(ConfigurationError) as refusal:
        read_inputs(STRATEGIES / name / "config.yaml")
    assert str(refusal.value) == f"connector does not support {chain}"


# References admitted after the legacy snapshot froze their candidate refusal.
# The legacy harness is deleted, so no legacy effective value exists to compare
# against; the public root now accepts them, and their full resolution proof is
# the QA matrix on the admission source. Here we assert the public root loads
# them and preserves the authored values.
PROMOTED_AFTER_SNAPSHOT = {
    "bnb_aave_borrow_and_trade_v4",
    "polygon_aave_borrow_and_trade_v4",
    "uniswap_v4_bnb_lp",
    "uniswap_v4_bnb_trading",
    "uniswap_v4_polygon_lp",
    "uniswap_v4_polygon_trading",
    "avalanche_aave_borrow_and_trade_v3",
    "polygon_aave_borrow_and_trade_v3",
    "uniswap_v3_avalanche_lp",
    "uniswap_v3_avalanche_trading",
    "uniswap_v3_polygon_lp",
    "uniswap_v3_polygon_trading",
    "avalanche_morpho_borrow_and_trade_v4",
    "morpho_blue_avalanche_lending",
    "uniswap_v4_robinhood_lp",
    "uniswap_v4_robinhood_trading",
    "robinhood_morpho_borrow_and_trade_v4",
    # The scoped DEX candidate admits these BNB and Robinhood V3 references
    # after the immutable snapshot recorded their former declaration refusal.
    "bnb_aave_borrow_and_trade_v3",
    "uniswap_v3_bnb_lp",
    "uniswap_v3_bnb_trading",
    "robinhood_morpho_borrow_and_trade_v3",
    "uniswap_v3_robinhood_lp",
    "uniswap_v3_robinhood_trading",
}


# References authored after the legacy harness was deleted. They never had a
# legacy effective value, so they are excluded from the frozen comparison and
# asserted only to load (or refuse offline on their candidate chain) through the
# public root; their resolution proof is the QA matrix.
BANDS_AFTER_FREEZE = {
    "aerodrome_slipstream_base_lp_band",
    "pancakeswap_v3_bnb_lp_band",
    "uniswap_v3_robinhood_lp_band",
    "uniswap_v4_robinhood_lp_band",
    "uniswap_v4_ethereum_native_trading",
}
# References with no legacy snapshot: the band LPs, the price-series references and
# references on chains added later.
PRICE_SERIES_REFERENCES = {
    "aave_v3_base_sma_borrow",
    "uniswap_v3_base_atr_breakout",
    "uniswap_v3_base_rsi",
    "uniswap_v3_base_rsi_lp",
}
# Chains added after the legacy harness was deleted.
NEW_CHAIN_REFERENCES = {"morpho_blue_hyperevm_lending"}
# Authoring templates written against strategy.parameters.
TEMPLATES_AFTER_FREEZE = {
    "aave_v3_arbitrum_managed_borrow",
    "arbitrum_aave_borrow_lp",
    "arbitrum_aave_lending_short",
    "arbitrum_aave_leverage_loop",
    "base_morpho_leverage_loop",
    "gmx_v2_arbitrum_price_stop",
}
# Connectors installed after the legacy harness was deleted (HyperCore, backtests only).
NEW_CONNECTOR_REFERENCES = {"hyperliquid_hyperevm_perp"}
AUTHORED_AFTER_FREEZE = (
    BANDS_AFTER_FREEZE
    | PRICE_SERIES_REFERENCES
    | NEW_CHAIN_REFERENCES
    | TEMPLATES_AFTER_FREEZE
    | NEW_CONNECTOR_REFERENCES
)
# The legacy snapshots decide every 5 seconds, below the live minimum; the shipped
# references decide every 60.
LIVE_REFERENCE_INTERVAL_SECONDS = 60


def assert_public_root_preserves(name: str, file: str):
    """The admitted reference loads through the public root and keeps authored values."""
    raw = yaml.safe_load((STRATEGIES / name / file).read_text())
    if file == "config.yaml":
        inputs = read_inputs(STRATEGIES / name / file).inputs
        seconds = int(str(raw["strategy"]["decision_interval"]).removesuffix("s"))
        assert inputs.strategy.decision_interval == seconds
        assert set(inputs.markets) == set(raw["markets"])
        return
    document = read_backtest_inputs(STRATEGIES / name / file)
    start = int(datetime.fromisoformat(raw["window"]["start"]).timestamp())
    end = int(datetime.fromisoformat(raw["window"]["end"]).timestamp())
    assert (document.inputs.window.start, document.inputs.window.end) == (start, end)


def live_interval(legacy_seconds: int) -> int:
    if legacy_seconds < MIN_LIVE_DECISION_INTERVAL_SECONDS:
        return LIVE_REFERENCE_INTERVAL_SECONDS
    return legacy_seconds


def legacy_positions(value: dict) -> dict:
    return {item["name"]: item for item in (*value["allocations"], *value.get("debts", ()))}


def authored_positions(inputs) -> dict:
    return {**inputs.strategy.allocations, **inputs.strategy.debts}


def resolved_positions(inputs, legacy: dict) -> dict:
    frozen, metadata = legacy_positions(legacy), legacy["asset_metadata"]
    positions = authored_positions(inputs)
    assert sorted(positions) == sorted(frozen), "public and legacy position names differ"
    return {
        name: position.resolve(
            name,
            LEGACY_MARKETS[str(inputs.markets[position.market].connector)](frozen[name], metadata),
        ).model_dump(mode="json")
        for name, position in positions.items()
    }


def unnamed(policy: dict) -> dict:
    return {key: value for key, value in policy.items() if key != "name"}


def gas_views(resolved: list, legacy: list) -> tuple[list, list]:
    """Legacy policies keep their author name; names drop out only when they carry none."""
    # The current codec omits the unchanged next_block default. Compare validated
    # policy values so this wire omission cannot conceal a different fee basis.
    legacy = [
        GasPolicy.model_validate_json(json.dumps(policy)).model_dump(mode="json")
        for policy in legacy
    ]
    if all(policy.get("name") for policy in legacy):
        return sorted(resolved, key=itemgetter("name")), sorted(legacy, key=itemgetter("name"))
    return sorted(map(unnamed, resolved), key=str), sorted(map(unnamed, legacy), key=str)


def simulation_accounts(accounts: dict) -> StrategyAccounts:
    """The harness bound every author account to a hypothetical fund of the same name."""
    return StrategyAccounts(
        bindings=tuple(
            simulation_binding(name, ChainId(account["chain"]["chain_id"]))
            for name, account in accounts.items()
        )
    )


def legacy_parameters(entry: dict):
    """The harness parameter set of the frozen config, rebuilt without the harness reader.

    The frozen value is the author dump; its parameter wire drops the author's
    own accounts and execution block, and the Aave V4 tag that retained
    artifacts never carry.
    """
    value = effective(entry, "config.yaml")
    connector = value.get("connector")
    wire = {key: item for key, item in value.items() if key not in AUTHOR_ONLY}
    if connector == "aave_v4":
        wire.pop("connector")
    wire["accounts"] = simulation_accounts(value["author_accounts"]).model_dump(mode="json")
    identity = ConnectorId(connector) if connector else None
    return parameter_type(identity).model_validate_json(json.dumps(wire))


def fund_identities(settings: dict) -> list:
    """Aave keeps a flat fund identity; every other connector nests it in a profile."""
    return [fund.get("fund") or fund["profile"]["fund"] for fund in settings["funds"]]


def author_variants() -> list[tuple[str, str]]:
    return [
        (name, file)
        for name in sorted(CONVERTED - AUTHORED_AFTER_FREEZE)
        for file in AUTHOR_FILES
        if (STRATEGIES / name / file).is_file()
    ]


def backtest_variants() -> list[tuple[str, str]]:
    return [
        (name, path.name)
        for name in sorted(CONVERTED - AUTHORED_AFTER_FREEZE)
        for path in sorted((STRATEGIES / name).glob("backtest*.yaml"))
    ]


def inventory_references() -> list[str]:
    return [
        name
        for name, (connector, entry) in sorted(snapshot_index().items())
        if name in CONVERTED and connector == COMPOSED and not refused_chain(entry, "config.yaml")
    ]


def gas_variants() -> list[tuple[str, str]]:
    snapshots = snapshot_index()
    return [
        (name, file)
        for name, file in author_variants()
        if not refused_chain(snapshots[name][1], file)
        and (effective(snapshots[name][1], file)["execution"].get("production") or {}).get("gas")
    ]


@pytest.mark.parametrize("name,file", author_variants())
def test_public_positions_resolve_to_the_legacy_effective_choices(name, file):
    entry = legacy_snapshot(name)
    chain = refused_chain(entry, file)
    if chain:
        if name in PROMOTED_AFTER_SNAPSHOT:
            assert_public_root_preserves(name, file)
            return
        assert_public_root_refuses(name, chain)
        return
    legacy = effective(entry, file)
    inputs = read_inputs(STRATEGIES / name / file).inputs
    resolved = reviewed_holds(name, resolved_positions(inputs, legacy))
    assert resolved == reviewed_holds(name, legacy_positions(legacy))
    assert live_interval(legacy["decision_trigger"]["seconds"]) == inputs.strategy.decision_interval


@pytest.mark.parametrize("name", inventory_references())
def test_public_inventory_resolves_to_the_legacy_owned_cash(name):
    entry = legacy_snapshot(name)
    document = read_inputs(STRATEGIES / name / "config.yaml")
    expected = legacy_parameters(entry)
    assert resolved_inventory(document, expected.accounts, expected.asset_metadata) == (
        expected.inventory
    )


@pytest.mark.parametrize("name,file", gas_variants())
def test_public_gas_policies_resolve_to_the_legacy_production_policies(name, file):
    path = STRATEGIES / name / file
    legacy = effective(legacy_snapshot(name), file)
    inputs = read_inputs(path).inputs
    resolved = [
        policy.resolve(key).model_dump(mode="json") for key, policy in inputs.strategy.gas.items()
    ]
    public, frozen = gas_views(resolved, (legacy["execution"].get("production") or {})["gas"])
    assert public and public == frozen


@pytest.mark.parametrize("name,file", backtest_variants())
def test_public_backtest_settings_match_the_legacy_effective_settings(name, file):
    entry = legacy_snapshot(name)
    chain = refused_chain(entry, file)
    if chain:
        if name in PROMOTED_AFTER_SNAPSHOT:
            assert_public_root_preserves(name, file)
            return
        assert_public_root_refuses(name, chain)
        return
    legacy = effective(entry, file)
    history = tuple(
        HistoryBinding.model_validate_json(json.dumps(binding)) for binding in legacy["history"]
    )
    document = read_backtest_inputs(STRATEGIES / name / file)
    chains = "_".join(sorted(str(binding.chain.chain_id).replace(":", "-") for binding in history))
    operator = read_operator_config(ROOT / "examples/operators/backtest" / f"{chains}.yaml")
    settings = resolve_backtest(
        document, legacy_parameters(entry), history, operator.vendors
    ).model_dump(mode="json")
    assert fund_identities(settings) == fund_identities(legacy)
    expected = reviewed_acquisition_limits(name, reviewed_window(name, legacy))
    assert settings == expected


def reviewed_window(name: str, legacy: dict) -> dict:
    shift = REVIEWED_WINDOW_SHIFTS.get(name, 0)
    schedule = {**legacy["schedule"]}
    schedule["start"] += shift
    schedule["end"] += shift
    return {**legacy, "schedule": schedule}


def reviewed_acquisition_limits(name: str, legacy: dict) -> dict:
    correction = REVIEWED_ACQUISITION_LIMIT_CORRECTIONS.get(name)
    if correction is None:
        return legacy
    models = {**legacy["models"]}
    aave = {**models["aave_v3"]}
    aave["data"] = {**aave["data"], **correction}
    models["aave_v3"] = aave
    return {**legacy, "models": models}


def test_reviewed_arbitrum_aave_pancake_limits_admit_measured_range_and_refuse_overrun():
    name = "arbitrum_aave_pancake_borrow_and_trade"
    entry = legacy_snapshot(name)
    legacy = effective(entry, "backtest-economic.yaml")
    history = tuple(
        HistoryBinding.model_validate_json(json.dumps(binding)) for binding in legacy["history"]
    )
    parameters = legacy_parameters(entry)
    document = read_backtest_inputs(STRATEGIES / name / "backtest-economic.yaml")
    operator = read_operator_config(ROOT / "examples/operators/backtest/eip155-42161.yaml")
    settings = resolve_backtest(document, parameters, history, operator.vendors)
    connector = ConnectorId("aave_v3")
    component = parameters.parameter_components[connector]
    aave_settings = settings.component(connector, component)
    chain = aave_settings.history[0].chain

    def headers(last_number: int):
        first = SimpleNamespace(number=504_943_261, timestamp=1_789_355_397)
        last = SimpleNamespace(number=last_number, timestamp=1_789_355_487)
        upper = SimpleNamespace(number=last_number + 1, timestamp=1_789_355_488)
        return SimpleNamespace(
            upper=upper,
            floor=lambda at: first if at == aave_settings.schedule.start else last,
        )

    measured = aave_v3_risk_bounds({chain: headers(504_943_613)}, component, aave_settings)
    assert sum(last.number - first.number + 1 for first, last in measured.values()) == 353
    reserves = {position.reserve for position in component.positions}
    assert len(reserves) * 353 == 706
    assert (aave_settings.data.max_risk_blocks, aave_settings.data.max_risk_samples) == (512, 1024)

    overrun_blocks = 504_943_773 - 504_943_261 + 1
    assert (overrun_blocks, len(reserves) * overrun_blocks) == (513, 1026)
    with pytest.raises(ValueError, match="explicit block or sample limit"):
        aave_v3_risk_bounds({chain: headers(504_943_773)}, component, aave_settings)


def shipped_references() -> list[str]:
    return [
        path.parent.name
        for path in sorted(STRATEGIES.glob("*/config.yaml"))
        if not path.parent.name.startswith(".") and path.parent.name not in AUTHORED_AFTER_FREEZE
    ]


def snapshot_connector(entry: dict) -> str | None:
    """The harness connector of the frozen config, when the support matrix admitted it."""
    record = entry["files"]["config.yaml"]
    if record["status"] != "normalized":
        return None
    return str(record["value"].get("connector") or COMPOSED)


def grouping_fault(name: str, connector: str, entry: dict) -> str:
    """The grouping key must be the frozen connector, and a name the shipped file still binds."""
    expected = snapshot_connector(entry)
    if entry["name"] != name:
        return f"{name}: entry names {entry['name']}"
    if expected is not None and connector != expected:
        return f"{name}: grouped under {connector}, frozen connector {expected}"
    allowed = {COMPOSED, *named_connectors(STRATEGIES / name / "config.yaml")}
    if connector not in allowed:
        return f"{name}: grouped under {connector}, shipped file names {sorted(allowed)}"
    return ""


def test_legacy_snapshots_are_canonically_encoded():
    documents = snapshot_documents()
    assert documents, f"no legacy snapshots under {SNAPSHOTS}"
    for path, document in documents.items():
        assert document["schema"] == SCHEMA
        assert path.read_bytes() == compress(document), f"{path.name} is not canonically encoded"


def test_legacy_snapshots_cover_every_reference_under_its_connector():
    index = snapshot_index()
    assert sorted(index) == shipped_references()
    faults = [
        fault
        for name, (connector, entry) in sorted(index.items())
        if (fault := grouping_fault(name, connector, entry))
    ]
    assert not faults, faults


def test_legacy_snapshots_regenerate_byte_identically(tmp_path):
    extract(BASELINE, tmp_path)
    produced = {path.name: path.read_bytes() for path in sorted(tmp_path.glob("*.json.gz"))}
    committed = {path.name: path.read_bytes() for path in sorted(SNAPSHOTS.glob("*.json.gz"))}
    assert produced == committed


@pytest.mark.parametrize("name", sorted(BANDS_AFTER_FREEZE))
def test_references_authored_after_the_freeze_load_through_the_public_root(name):
    assert name not in snapshot_index()
    path = STRATEGIES / name / "config.yaml"
    raw = yaml.safe_load(path.read_text())
    chains = {account["chain"] for account in raw["accounts"].values()}
    try:
        inputs = read_inputs(path).inputs
    except ConfigurationError as refusal:
        found = REFUSAL.search(str(refusal))
        assert found and found.group(1) in chains, str(refusal)
        return
    for key, authored in raw["strategy"]["allocations"].items():
        position = inputs.strategy.allocations[key]
        assert getattr(position, "range", None) is None
        if "band" not in authored:
            assert position.kind == "trading" and position.band is None
            continue
        assert position.band is not None
        assert position.band.model_dump(mode="json", exclude_none=True) == authored["band"]


@pytest.mark.parametrize("name", sorted(PRICE_SERIES_REFERENCES))
def test_the_price_series_references_load_through_the_public_root(name):
    assert name not in snapshot_index()
    inputs = read_inputs(STRATEGIES / name / "config.yaml").inputs
    expected = {"collateral", "loan"} if name.startswith("aave") else {"position"}
    assert set(inputs.strategy.allocations) == expected


@pytest.mark.parametrize("name", sorted(TEMPLATES_AFTER_FREEZE))
def test_the_authoring_templates_load_through_the_public_root(name):
    assert name not in snapshot_index()
    inputs = read_inputs(STRATEGIES / name / "config.yaml").inputs
    assert inputs.strategy.allocations and inputs.strategy.parameters
