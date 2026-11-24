"""`almanak backtest recent`: reference backtests moved onto the most recent window."""

import json
from pathlib import Path

import pytest
import yaml

from almanak.backtest import recent
from almanak.cli import error_document
from almanak.configuration.backtest_inputs import parse_backtest
from almanak.configuration.yaml import load
from almanak.gateway.ethereum import DataError

ROOT = Path(__file__).parents[1]
END = 1_792_656_000  # 2026-10-22T08:00:00Z
START = END - 30 * recent.DAY_SECONDS


def example(name):
    directory = ROOT / "strategies" / name
    author = yaml.safe_load((directory / "config.yaml").read_text())
    raw = yaml.safe_load((directory / "backtest-economic.yaml").read_text())
    return author, raw


def observed(ticks=None, gas=None):
    return recent.Observations(
        start=START,
        end=END,
        ticks=ticks or {},
        gas_prices=gas or {"eip155:8453": 6_000_000, "eip155:42161": 20_000_000},
    )


def test_window_ends_two_hours_before_the_slowest_chain_on_the_hour():
    finalized = {"eip155:1": END + 7_200 + 1_799, "eip155:8453": END + 9_000}
    assert recent.recent_window(finalized, 30) == (START, END)
    assert recent.recent_window(finalized, 30, END - 1) == (START - 3_600, END - 3_600)
    with pytest.raises(ValueError, match="two hours"):
        recent.recent_window(finalized, 30, END + 3_600)
    with pytest.raises(ValueError, match="positive"):
        recent.recent_window(finalized, 0)


def test_swap_only_uniswap_v3_example_moves_with_measured_gas_and_a_payer(tmp_path):
    author, raw = example("uniswap_v3_base_trading")
    document, changes = recent.rewrite(
        raw, author, observed({"pool": -197_218}), recent.gas_references()
    )
    assert document["window"]["start"] == "2026-09-22T08:00:00+00:00"
    assert document["window"]["end"] == "2026-10-22T08:00:00+00:00"
    assert document["window"]["end_policy"] == raw["window"]["end_policy"]
    # The strategy decides every minute; a month of minutes is coarsened to hours.
    assert document["execution"]["decision_interval"] == "3600s"
    assert changes["decision_interval_override_seconds"] == 3_600
    data = document["data"]
    assert data["sample_seconds"] == "3600s" and data["max_age_seconds"] >= "3720s"
    assert data["max_samples"] >= 30 * 24 + 1
    coverage = data["coverage"][0]
    assert coverage["lower"] <= -197_218 - recent.COVERAGE_HALF_WIDTH_TICKS
    assert coverage["upper"] >= -197_218 + recent.COVERAGE_HALF_WIDTH_TICKS
    costs = document["costs"]
    assert costs["kind"] == "fixed_native"
    # Liquidity operations are unreachable for a swap-only allocation.
    assert {
        k: costs[k] for k in ("mint", "increase", "decrease", "collect", "close")
    } == dict.fromkeys(("mint", "increase", "decrease", "collect", "close"), 0)
    priced = changes["costs"]["uniswap_v3"]["operations"]
    assert costs["swap"] == priced["swap"]["gas"] * 6_000_000 > 0
    assert costs["revoke"] == priced["revoke"]["gas"] * 6_000_000 > 0
    payer = document["inventory"]["payers"]["recent_gas_payer_8453"]
    assert payer["chain"] == "eip155:8453"
    assert payer["balances"][0]["asset"] == "eip155:8453/slip44:60"
    assert all(
        fund["payer"] == "recent_gas_payer_8453" for fund in document["inventory"]["funds"].values()
    )
    assert changes["gas"]["eip155:8453"]["wrapped_native"].endswith(
        "0x4200000000000000000000000000000000000006"
    )
    # The rewrite is a valid public backtest document.
    parse_backtest(load_value(tmp_path, document))


@pytest.mark.parametrize(
    ("name", "code"),
    [
        ("aave_v3_arbitrum_lending", "per_block_history"),
        ("gmx_v2_arbitrum_swap", "per_block_history"),
        ("uniswap_v3_base_rsi_lp", "unmeasured_gas"),
        ("curve_arbitrum_stable_lp", "unvalued"),
        ("uniswap_v4_arbitrum_lp", "unvalued"),
        ("pancakeswap_v3_bnb_trading", "unmeasured_gas"),
        # A recent window is built only for the connectors it knows how to rewrite.
        ("across_usdc_arbitrum_base_bridge", "unsupported_connector"),
    ],
)
def test_examples_without_a_recent_window_are_refused_with_a_reason(name, code):
    author, raw = example(name)
    with pytest.raises(recent.RecentUnsupported) as refused:
        recent.rewrite(raw, author, observed(ticks_for(author, -197_000)), recent.gas_references())
    assert refused.value.code == code
    document = error_document(refused.value)
    assert document["error"] == "recent_unsupported" and document["reason"] == code


def test_a_measurement_from_another_chain_prices_an_unmeasured_chain():
    rows = [
        {"connector": "uniswap_v3", "chain": "eip155:42161", "action": "swap", "gas_used": 10},
        {"connector": "uniswap_v3", "chain": "eip155:8453", "action": "swap", "gas_used": 7},
    ]
    assert recent._reference(rows, "uniswap_v3", "eip155:8453", "swap")["gas_used"] == 7
    assert recent._reference(rows, "uniswap_v3", "eip155:1", "swap")["gas_used"] == 10
    assert recent._reference(rows, "uniswap_v3", "eip155:1", "mint") is None


class FakeBlock:
    def __init__(self, number, timestamp):
        self.number = number
        self.timestamp = timestamp


class FakeHistory:
    """One block a minute, finalized well past the window."""

    def __init__(self, reader):
        self.upper = FakeBlock(0, END + 10 * 3_600)

    def floor(self, timestamp):
        return FakeBlock(timestamp // 60, timestamp)


class FakeReader:
    def __init__(self, base_fee=5, tips=("0x2", "0x4"), tip_error=None):
        self.base_fee, self.tips, self.tip_error = base_fee, tips, tip_error
        self.pinned, self.closed = [], False

    def pin_block(self, number):
        self.pinned.append(number)
        return number

    def batch(self, calls):
        if calls[0][0] == "eth_feeHistory":
            if self.tip_error:
                raise self.tip_error
            return [{"reward": [[tip] for tip in self.tips]}]
        return [{"baseFeePerGas": hex(self.base_fee)} for _ in calls]

    def __exit__(self, *exc):
        self.closed = True


def test_observe_reads_ticks_at_the_window_start_and_gas_over_the_window(monkeypatch):
    import almanak.backtest.evm as evm
    import almanak.gateway.read_endpoints as endpoints

    readers = {}

    def reader_for(chain, env_file):
        readers[str(chain)] = FakeReader()
        return readers[str(chain)]

    monkeypatch.setattr(endpoints, "verified_reader", reader_for)
    monkeypatch.setattr(evm, "EvmHistory", FakeHistory)
    monkeypatch.setattr(recent, "pool_tick", lambda reader, market, block: -197_218)
    author = {
        "markets": {
            "pool": {"connector": "uniswap_v3", "chain": "eip155:8453"},
            "lending": {"connector": "aave_v3", "chain": "eip155:42161"},
        }
    }

    observed = recent.observe(author, Path(".env"), 30, END)

    assert (observed.start, observed.end) == (START, END)
    # Only concentrated pools are read, at the block of the window start.
    assert observed.ticks == {"pool": -197_218}
    assert readers["eip155:8453"].pinned == [START // 60]
    assert readers["eip155:42161"].pinned == []
    # Median base fee plus the median tip at the window end, per chain.
    assert observed.gas_prices == {"eip155:8453": 5 + 3, "eip155:42161": 5 + 3}
    assert all(reader.closed for reader in readers.values())


def test_a_provider_without_fee_history_prices_no_tip_but_a_throttle_fails_the_run():
    unsupported = DataError("rpc", "method not found", code=-32601)
    assert recent.recent_tip(FakeReader(tip_error=unsupported), 10) == 0
    assert recent.recent_tip(FakeReader(tips=()), 10) == 0
    throttled = DataError("rpc", "too many requests", code=429, transient=True)
    with pytest.raises(DataError):
        recent.recent_tip(FakeReader(tip_error=throttled), 10)


def test_an_end_without_a_zone_is_utc():
    assert recent._instant("2026-10-22T08:00:00") == END
    assert recent._instant("2026-10-22T08:00:00Z") == END
    assert recent._instant("2026-10-22T10:00:00+02:00") == END


def ticks_for(author, tick):
    return {name: tick for name in author.get("markets") or {}}


def load_value(directory, document):
    path = directory / "backtest.recent.yaml"
    path.write_text(yaml.safe_dump(json.loads(json.dumps(document)), sort_keys=False))
    return load(path)
