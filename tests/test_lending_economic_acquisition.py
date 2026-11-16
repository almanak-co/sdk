"""Per-block risk acquisition stays inside explicit block limits for every lending connector.

Aave V3, SparkLend, Compound V3 and Morpho Blue sample risk at the last block of each
timestamp group and need the following block as the next boundary. These cases run the
shared acquisition bounds and timestamp grouping against a literal header history.
"""

from types import SimpleNamespace

import pytest

from almanak.backtest.types import UnixTime
from almanak.caip import ChainId
from almanak.models import Block, Chain
from tests.lending_economic_cases import CASES

ALL = sorted(CASES)
CHAIN = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
OTHER = Chain(chain_id=ChainId("eip155:8453"), environment="mainnet")
# Block numbers 0..6; blocks 2, 3 and 4 share one timestamp.
TIMES = (100, 112, 124, 124, 124, 136, 148)


class History:
    """Canonical headers with a finalized upper block, as the historical gateway serves."""

    def __init__(self, times=TIMES, chain=CHAIN, upper=None, floor=None):
        self.headers = tuple(
            Block(chain=chain, number=n, hash="0x" + f"{n:064x}", timestamp=t)
            for n, t in enumerate(times)
        )
        self.upper = self.headers[-1 if upper is None else upper]
        self.floored = floor

    def floor(self, at):
        if self.floored is not None:
            return self.headers[self.floored]
        return [h for h in self.headers if h.timestamp <= at][-1]

    def header(self, number):
        return self.headers[number]


@pytest.fixture(params=ALL)
def acquisition(request):
    return CASES[request.param].acquisition


def numbers(blocks):
    return tuple(b.number for b in blocks)


def test_a_group_holds_every_block_of_its_timestamp_and_the_following_boundary(acquisition):
    group, following = acquisition.group_at(History(), 130, 100, 10)
    assert numbers(group) == (2, 3, 4)
    assert following.number == 5


def test_the_run_start_samples_only_its_floor_block(acquisition):
    group, following = acquisition.group_at(History(), 124, 124, 10)
    assert numbers(group) == (4,)
    assert following.number == 5


def test_a_group_reaching_genesis_stops_there(acquisition):
    history = History(times=(100, 100, 112))
    group, following = acquisition.group_at(history, 105, 0, 10)
    assert numbers(group) == (0, 1)
    assert following.number == 2


def test_a_group_larger_than_the_block_limit_is_refused(acquisition):
    with pytest.raises(ValueError, match="timestamp group exceeds the explicit block limit"):
        acquisition.group_at(History(), 130, 100, 2)


def test_a_floor_that_is_not_the_last_block_of_its_timestamp_is_refused(acquisition):
    with pytest.raises(ValueError, match="timestamp floor is not the latest eligible block"):
        acquisition.group_at(History(floor=2), 124, 124, 10)


def settings(**limits):
    return SimpleNamespace(
        schedule=SimpleNamespace(start=UnixTime(100), deadline=UnixTime(136)),
        data=SimpleNamespace(**({"max_risk_blocks": 100, "max_risk_samples": 100} | limits)),
    )


@pytest.fixture
def bounded(acquisition, monkeypatch):
    """Acquisition bounds over two chains, with two configured targets on the first."""
    targets = (
        SimpleNamespace(chain=CHAIN),
        SimpleNamespace(chain=CHAIN),
        SimpleNamespace(chain=OTHER),
    )
    for name in ("allocations", "markets"):
        monkeypatch.setattr(acquisition, name, lambda config: targets, raising=False)
    return acquisition


def test_bounds_span_each_chain_from_start_to_deadline(bounded):
    histories = {CHAIN: History(), OTHER: History(times=(90, 120, 150), chain=OTHER)}
    ranges = bounded.bounds(histories, None, settings())
    assert {chain: numbers(r) for chain, r in ranges.items()} == {CHAIN: (0, 5), OTHER: (0, 1)}
    # 6 + 2 blocks in all; each target samples its own chain's range: 6 + 6 + 2.
    bounded.bounds(histories, None, settings(max_risk_blocks=8, max_risk_samples=14))


@pytest.mark.parametrize("limits", [{"max_risk_blocks": 7}, {"max_risk_samples": 13}])
def test_bounds_refuse_acquisition_above_an_explicit_limit(bounded, limits):
    histories = {CHAIN: History(), OTHER: History(times=(90, 120, 150), chain=OTHER)}
    with pytest.raises(ValueError, match="exceeds its explicit block or sample limit"):
        bounded.bounds(histories, None, settings(**limits))


def test_bounds_require_a_finalized_block_after_the_deadline(bounded):
    with pytest.raises(ValueError, match="requires a finalized following timestamp boundary"):
        bounded.bounds({CHAIN: History(upper=5)}, None, settings())
