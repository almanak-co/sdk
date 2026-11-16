"""A reference economic backtest moved onto the most recent window, with measured gas.

`almanak backtest recent` rewrites an economic backtest file so that it covers the
`--days` days ending `SOURCE_LAG_SECONDS` before each chain's latest finalized block.
Everything that is sized for the authored window is sized again for the new one:

- the decision interval is coarsened to an hour when the strategy decides more often
  (the run reports the reduced cadence, as any override does);
- pool sampling moves to hourly and its sample and event limits scale with the window;
- concentrated-pool tick coverage is re-centred on the pool tick at the window start
  and widened to `COVERAGE_HALF_WIDTH_TICKS` either side, keeping the authored bounds
  and the strategy's own range inside it;
- operation costs become `fixed_native`: each operation's measured gas
  (`connectors/gas_references.json`) at the median gas price of the window, paid by a
  hypothetical payer, so the run's gas is reported next to its result.

Only connectors whose data is sampled on an interval can run such a window: those that
read risk state for every block (Aave V3, SparkLend, Compound V3, Morpho Blue) or every
published report block (GMX V2) are refused, as are runs that are not valued in their
funding asset (the result is a return) and operations without a measured gas reference.
A refusal is an expected outcome, not a failure: the example has no recent backtest until
the SDK can produce one.
"""

import json
import math
import statistics
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path

import yaml

from almanak.backtest.types import UnixTime
from almanak.caip import AssetType, ChainId
from almanak.configuration.values import seconds

DAY_SECONDS = 86_400
SAMPLE_SECONDS = 3_600
DECISION_SECONDS = 3_600
# The QA policy's source lag: archive providers serve a finalized block's state reliably
# about two hours after it.
SOURCE_LAG_SECONDS = 7_200
COVERAGE_HALF_WIDTH_TICKS = 4_000
GAS_SAMPLES = 30
GAS_REWARD_PERCENTILE = 50
GAS_TIP_BLOCKS = 64
PAYER = "recent_gas_payer"
# Hypothetical payer inventory, in whole native units: large enough that no operation is
# refused for gas, so the payer's spend is the run's gas cost.
PAYER_NATIVE_QUANTITY = "1000"

CONCENTRATED = frozenset({"uniswap_v3", "pancakeswap_v3", "aerodrome_slipstream", "uniswap_v4"})
SAMPLED = frozenset({"aerodrome", "curve"})
# Runs valued in their funding asset (docs/backtesting.md, "Results in the funding asset"):
# a recent window is shown as a return, so an unvalued run has nothing to show.
VALUED = frozenset({"uniswap_v3", "pancakeswap_v3", "aerodrome_slipstream"})
PER_BLOCK = {
    "aave_v3": "reads pool risk state for every block of the window",
    "sparklend": "reads pool risk state for every block of the window",
    "compound_v3": "reads comet risk state for every block of the window",
    "morpho_blue": "reads market risk state for every block of the window",
    "gmx_v2": "reads every published report block of the window",
}
# Window limits that bound how much history preparation may read; they grow with the window.
SCALED_LIMITS = (
    "max_liquidity_events",
    "max_liquidity_log_requests",
    "max_parameter_events",
    "max_parameter_log_requests",
)
MIN_BITMAP_WORDS = 64
MIN_INITIALIZED_TICKS = 10_000

# Each fixed cost field and the measured operation that prices it. A revoke is an
# ERC-20 approval of zero, priced by the measured approval.
COST_REFERENCES = {
    "uniswap_v3": {
        "swap": "swap",
        "mint": "mint",
        "increase": "increase",
        "decrease": "decrease",
        "collect": "collect",
        "close": "close",
        "revoke": "approve",
    },
    "uniswap_v4": {
        "swap": "swap",
        "mint": "mint",
        "collect": "collect",
        "burn": "burn",
        "revoke": "revoke_permit2",
    },
}
# The cost fields a swap-only allocation can reach; the rest are liquidity operations.
SWAP_FIELDS = frozenset({"swap", "revoke"})


class RecentUnsupported(ValueError):
    """This example cannot run a recent window; `code` names why."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class Observations:
    """Chain facts the rewrite needs, read once before it."""

    start: int
    end: int
    # Pool tick at the window start, by author market name.
    ticks: dict[str, int]
    # Median gas price over the window, in wei per gas, by CAIP-2 chain.
    gas_prices: dict[str, int]


def recent_window(finalized: dict[str, int], days: int, end: int | None = None) -> tuple[int, int]:
    """[start, end] of `days` days ending at the hour before every chain's served history."""
    if days <= 0:
        raise ValueError("--days must be positive")
    latest = min(finalized.values()) - SOURCE_LAG_SECONDS
    stop = latest if end is None else end
    if stop > latest:
        raise ValueError("the window must end at least two hours before finalized history")
    stop -= stop % SAMPLE_SECONDS
    return stop - days * DAY_SECONDS, stop


def rewrite(raw: dict, author: dict, observed: Observations, gas: list[dict]) -> tuple[dict, dict]:
    """The backtest document for the observed window, and what changed to get there."""
    if raw.get("mode") != "economic":
        raise RecentUnsupported("not_economic", "only economic backtests run a recent window")
    document = json.loads(json.dumps(raw))
    markets = author.get("markets") or {}
    allocations = (author.get("strategy") or {}).get("allocations") or {}
    connectors = sorted({market["connector"] for market in markets.values()})
    for connector in connectors:
        if connector in PER_BLOCK:
            raise RecentUnsupported(
                "per_block_history",
                f"{connector} {PER_BLOCK[connector]}; a recent window is too long",
            )
        if connector not in CONCENTRATED | SAMPLED:
            raise RecentUnsupported(
                "unsupported_connector", f"{connector} has no recent-window rules yet"
            )
        if connector not in VALUED:
            raise RecentUnsupported(
                "unvalued",
                f"{connector} runs report asset units, not a result in the funding asset",
            )
    old = document["window"]
    old_duration = _instant(old["end"]) - _instant(old["start"])
    duration = observed.end - observed.start
    ratio = duration / max(old_duration, 1)
    document["window"] = {
        **old,
        "start": _iso(observed.start),
        "end": _iso(observed.end),
    }
    changes: dict = {
        "kind": "almanak-recent-backtest-v1",
        "window": {"start": observed.start, "end": observed.end, "days": duration // DAY_SECONDS},
    }
    configured = seconds((author.get("strategy") or {}).get("decision_interval", "0s"))
    execution = document.setdefault("execution", {})
    override = seconds(execution["decision_interval"]) if "decision_interval" in execution else 0
    if max(configured, override) < DECISION_SECONDS:
        execution["decision_interval"] = f"{DECISION_SECONDS}s"
        changes["decision_interval_override_seconds"] = DECISION_SECONDS
    cleanup = seconds(old.get("cleanup", "0s"))
    coverage_changes: dict = {}
    for connector, policy in _policies(document, connectors):
        _resample(policy, duration + cleanup, ratio)
        if connector in CONCENTRATED:
            coverage_changes |= _recentre(policy, allocations, markets, observed.ticks)
    if coverage_changes:
        changes["coverage"] = coverage_changes
    changes["costs"] = _measured_costs(document, connectors, allocations, markets, observed, gas)
    changes["gas"] = {
        chain: _gas_entry(chain, price) for chain, price in sorted(observed.gas_prices.items())
    }
    _add_payers(document, author)
    return document, changes


def _policies(document: dict, connectors: list[str]):
    data = document.get("data") or {}
    if "connectors" in data:
        for connector in connectors:
            policy = data["connectors"].get(connector)
            if policy is not None:
                yield connector, policy
    elif len(connectors) == 1:
        yield connectors[0], data


def _resample(policy: dict, span: int, ratio: float):
    old_sample = seconds(policy.get("sample_seconds", f"{SAMPLE_SECONDS}s"))
    old_age = seconds(policy.get("max_age_seconds", f"{old_sample}s"))
    sample = max(old_sample, SAMPLE_SECONDS)
    policy["sample_seconds"] = f"{sample}s"
    policy["max_age_seconds"] = f"{sample + max(old_age - old_sample, 120)}s"
    if "max_samples" in policy:
        groups = max(len(policy.get("coverage") or ()), 1)
        policy["max_samples"] = max(policy["max_samples"], (span // sample + 2) * groups)
    for name in SCALED_LIMITS:
        if name in policy:
            policy[name] = max(policy[name], math.ceil(policy[name] * ratio))


def _recentre(policy: dict, allocations: dict, markets: dict, ticks: dict[str, int]) -> dict:
    changed = {}
    for item in policy.get("coverage") or ():
        allocation = allocations.get(item["allocation"]) or {}
        market = allocation.get("market")
        if market not in ticks:
            raise RecentUnsupported(
                "unobserved_pool", f"no pool tick was observed for allocation {item['allocation']}"
            )
        tick = ticks[market]
        # Coverage bounds the price path the run can price, not the position: an authored
        # full-range position still keeps its authored coverage.
        lower = min(item["lower"], tick - COVERAGE_HALF_WIDTH_TICKS)
        upper = max(item["upper"], tick + COVERAGE_HALF_WIDTH_TICKS)
        changed[item["allocation"]] = {
            "authored": [item["lower"], item["upper"]],
            "recent": [lower, upper],
            "start_tick": tick,
        }
        item["lower"], item["upper"] = lower, upper
    if changed:
        policy["max_bitmap_words"] = max(policy.get("max_bitmap_words", 0), MIN_BITMAP_WORDS)
        policy["max_initialized_ticks"] = max(
            policy.get("max_initialized_ticks", 0), MIN_INITIALIZED_TICKS
        )
    return changed


def _measured_costs(document, connectors, allocations, markets, observed, gas) -> dict:
    costs = document.get("costs") or {}
    composed = "kind" not in costs
    result = {}
    for connector in connectors:
        fields = COST_REFERENCES.get(connector)
        if fields is None:
            raise RecentUnsupported(
                "unmeasured_gas", f"{connector} has no measured gas references yet"
            )
        chains = {
            markets[a["market"]]["chain"]
            for a in allocations.values()
            if markets.get(a.get("market"), {}).get("connector") == connector
        }
        if len(chains) != 1:
            raise RecentUnsupported("multi_chain_costs", f"{connector} costs span several chains")
        chain = chains.pop()
        reachable = _reachable(connector, allocations, markets)
        entry: dict = {"kind": "fixed_native"}
        priced = {}
        for field, action in fields.items():
            if field not in reachable:
                entry[field] = 0
                continue
            reference = _reference(gas, connector, chain, action)
            if reference is None:
                raise RecentUnsupported(
                    "unmeasured_gas", f"{connector} {action} has no measured gas reference"
                )
            price = observed.gas_prices[chain]
            entry[field] = reference["gas_used"] * price
            priced[field] = {
                "gas": reference["gas_used"],
                "measured_on": reference["chain"],
                "wei_per_gas": price,
            }
        result[connector] = {"chain": chain, "operations": priced}
        if composed:
            costs[connector] = entry
        else:
            document["costs"] = entry
    return result


def _reachable(connector, allocations, markets) -> set[str]:
    reachable: set[str] = set()
    for allocation in allocations.values():
        if markets.get(allocation.get("market"), {}).get("connector") != connector:
            continue
        liquidity = any(key in allocation for key in ("range", "band", "width", "width_bps"))
        fields = COST_REFERENCES[connector]
        reachable |= set(fields) if liquidity else SWAP_FIELDS & set(fields)
    return reachable


def _reference(gas: list[dict], connector: str, chain: str, action: str) -> dict | None:
    """The chain's own measurement, else the largest one of the connector on any chain.

    Execution gas of the same contract call barely differs between EVM chains; L1 data
    fees of rollups are not part of a receipt's gas and are not modeled here.
    """
    rows = [r for r in gas if r["connector"] == connector and r["action"] == action]
    own = [r for r in rows if r["chain"] == chain]
    candidates = own or rows
    return max(candidates, key=lambda r: r["gas_used"]) if candidates else None


def _add_payers(document: dict, author: dict):
    from almanak.chains import evm_network

    inventory = document.setdefault("inventory", {})
    funds = inventory.get("funds") or {}
    payers = inventory.setdefault("payers", {})
    accounts = author.get("accounts") or {}
    for name, fund in funds.items():
        if fund.get("payer"):
            continue
        chain = (accounts.get(name) or {}).get("chain")
        if chain is None:
            raise RecentUnsupported("unbound_fund", f"fund {name} has no author account chain")
        payer = f"{PAYER}_{ChainId(chain).reference}"
        payers.setdefault(
            payer,
            {
                "chain": chain,
                "balances": [
                    {
                        "asset": str(evm_network(ChainId(chain)).native_asset),
                        "quantity": PAYER_NATIVE_QUANTITY,
                    }
                ],
            },
        )
        fund["payer"] = payer


def _gas_entry(chain: str, price: int) -> dict:
    from almanak.chains import evm_network

    network = evm_network(ChainId(chain))
    # The wrapped asset prices the native one in a run's price series.
    return {
        "wei_per_gas": price,
        "native_asset": str(network.native_asset),
        "wrapped_native": str(network.wrapped_native),
    }


def gas_references() -> list[dict]:
    path = files("almanak.connectors").joinpath("gas_references.json")
    return json.loads(path.read_text())["references"]


def _instant(value) -> int:
    if isinstance(value, int):
        return value
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    # An instant without a zone is UTC, so the same `--end` names the same window on any host.
    return int((parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)).timestamp())


def _iso(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, UTC).isoformat()


# Chain observations ------------------------------------------------------------------


def observe(author: dict, env_file: Path, days: int, end: int | None) -> Observations:
    """Read each chain's finalized time, window-start pool ticks and window gas prices."""
    from almanak.backtest.evm import EvmHistory
    from almanak.gateway.read_endpoints import verified_reader

    markets = author.get("markets") or {}
    chains = sorted({market["chain"] for market in markets.values()})
    histories = {}
    readers = {}
    try:
        for chain in chains:
            readers[chain] = verified_reader(ChainId(chain), env_file)
            histories[chain] = EvmHistory(readers[chain])
        start, stop = recent_window(
            {chain: history.upper.timestamp for chain, history in histories.items()}, days, end
        )
        ticks = {}
        for name, market in markets.items():
            if market["connector"] in CONCENTRATED:
                floor = histories[market["chain"]].floor(UnixTime(start))
                # Reads name the reader's pinned block; the timestamp search pinned others.
                block = readers[market["chain"]].pin_block(floor.number)
                ticks[name] = pool_tick(readers[market["chain"]], market, block)
        gas_prices = {
            chain: window_gas_price(readers[chain], histories[chain], start, stop)
            for chain in chains
        }
        return Observations(start=start, end=stop, ticks=ticks, gas_prices=gas_prices)
    finally:
        for reader in readers.values():
            reader.__exit__()


def pool_tick(reader, market: dict, block) -> int:
    from almanak.models import Chain

    if market["connector"] == "uniswap_v4":
        from almanak.connectors.uniswap_v4.domain import UniswapPool
        from almanak.connectors.uniswap_v4.networks import roles_for
        from almanak.connectors.uniswap_v4.pool import read_pool

        chain = ChainId(market["chain"])
        roles = roles_for(chain)
        source = UniswapPool(
            kind="uniswap_v4",
            chain=Chain(chain_id=chain, environment="mainnet"),
            pool_manager=roles.pool_manager,
            state_view=roles.state_view,
            currency0=AssetType(market["currency0"]),
            currency1=AssetType(market["currency1"]),
            fee=market["fee"],
            tick_spacing=market["tick_spacing"],
            hooks=market["hooks"],
        )
        return read_pool(reader, source, block).tick
    from almanak.connectors.v3_pool_request import PoolRequest, read_pool_state

    request = PoolRequest(chain=ChainId(market["chain"]), pool=market["pool"])
    state = read_pool_state(reader, market["connector"], request, block)
    return state["observation"]["tick"]


def window_gas_price(reader, history, start: int, end: int) -> int:
    """Median base fee at `GAS_SAMPLES` blocks spread over the window, plus the median tip.

    Base fees come from the block headers, which every archive serves. Providers serve fee
    history (tips) for recent blocks only on some chains, so the tip is the median over the
    blocks before the window end and is applied over the window; without one it is zero.
    """
    step = (end - start) / GAS_SAMPLES
    blocks = [
        history.floor(UnixTime(int(start + step * (index + 0.5)))).number
        for index in range(GAS_SAMPLES)
    ]
    headers = reader.batch([("eth_getBlockByNumber", [hex(n), False]) for n in blocks])
    bases = [int(header.get("baseFeePerGas") or "0x0", 16) for header in headers]
    # The tip is read at the window end, so the same window always prices the same gas.
    return int(statistics.median(bases)) + recent_tip(reader, history.floor(UnixTime(end)).number)


def recent_tip(reader, block: int) -> int:
    from almanak.gateway.ethereum import DataError

    try:
        result = reader.batch(
            [("eth_feeHistory", [hex(GAS_TIP_BLOCKS), hex(block), [GAS_REWARD_PERCENTILE]])]
        )[0]
    except DataError as error:
        # A provider without fee history has no tip to give; a throttle or outage must fail
        # the run instead, or a retry would price the same window differently.
        if error.transient:
            raise
        return 0
    rewards = [int(reward[0], 16) for reward in result.get("reward") or () if reward]
    return int(statistics.median(rewards)) if rewards else 0


# Command ----------------------------------------------------------------------------


def command(args) -> int:
    from almanak.checks import selected_paths

    config, _ = selected_paths(args)
    author = yaml.safe_load(Path(config).read_text())
    raw = yaml.safe_load(Path(args.backtest_config).read_text())
    end = _instant(args.end) if args.end else None
    observed = observe(author, Path(args.env_file), args.days, end)
    document, changes = rewrite(raw, author, observed, gas_references())
    args.output.write_text(
        "# Generated by almanak backtest recent from "
        f"{Path(args.backtest_config).name}; see the summary it printed.\n"
        + yaml.safe_dump(document, sort_keys=False)
    )
    print(json.dumps({"output": str(args.output), **changes}, indent=2))
    return 0
