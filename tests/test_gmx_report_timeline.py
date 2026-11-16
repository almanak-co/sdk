"""A fork test times its decisions by keeper publications the origin chain already made."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak import fork_operator, testflow
from almanak.caip import ChainId
from almanak.connectors.gmx_v2 import fork as gmx_fork
from almanak.connectors.gmx_v2 import report_timeline
from almanak.connectors.gmx_v2.fork_keeper import Keeper
from almanak.connectors.gmx_v2.report_timeline import (
    ADMISSION_SECONDS,
    LEAD_MAX,
    LEAD_MIN,
    Timeline,
    dense,
)
from almanak.gateway.ethereum import DataError
from almanak.testflow_keepers import Keepers
from tests.test_gmx_fork_keeper import PAYLOAD, PROVIDER, TOKEN, Blocks, block, deployment_market

OTHER = "0x" + "aa" * 20


def timeline(published):
    """A timeline whose publications come from a fixed list."""
    line = object.__new__(Timeline)
    line.publications = lambda start, end: [p for p in published if start <= p[0] and p[1] <= end]
    return line


def test_alignment_lands_a_decision_shortly_before_a_publication():
    line = timeline([(1000, 1000), (1600, 1601)])
    # 1000 serves decisions from 1000 - LEAD_MAX to 1000 - LEAD_MIN; later ones wait for 1600.
    assert line.align(700) == 1000 - LEAD_MAX
    assert line.align(800) == 800
    assert line.align(1000 - LEAD_MIN + 1) == 1601 - LEAD_MAX
    assert line.align(1600 - LEAD_MIN + 1) is None


def test_a_publication_whose_reports_spread_wider_than_the_lead_cannot_align():
    assert timeline([(1000, 1000 + LEAD_MAX)]).align(0) is None


def test_dense_publications_cover_every_decision_time():
    start = 0
    every = range(ADMISSION_SECONDS + LEAD_MIN, 2000, 150)
    assert dense([(t, t) for t in every], start)
    gap = [(t, t) for t in every if not 600 < t < 900]
    assert not dense(gap, start)
    assert not dense([(t, t) for t in every if t < 700], start)


def test_the_fork_starts_at_the_latest_dense_stretch():
    every = [(t, t) for t in range(5000, 9000, 150)]
    line = timeline(every)
    start = line.anchor(6000)
    assert start <= 6000 and dense(every, start)
    # Too quiet to be dense: start just before the latest publication the head allows.
    quiet = timeline([(4000, 4000), (5000, 5000), (9000, 9000)])
    assert quiet.anchor(6000) == 5000 - ADMISSION_SECONDS - (LEAD_MAX + LEAD_MIN) // 2
    assert timeline([(9000, 9000)]).anchor(6000) is None


def market(chain="eip155:43114"):
    deployed = deployment_market(chain)
    tokens = [SimpleNamespace(reference=TOKEN), SimpleNamespace(reference=OTHER)]
    return SimpleNamespace(
        chain=deployed.chain,
        contracts=Mock(
            oracle=deployed.contracts.oracle,
            event_emitter="0x" + "ee" * 20,
            model_dump=Mock(return_value={}),
        ),
        index_asset=tokens[0],
        long_asset=tokens[1],
        short_asset=tokens[0],
    )


def keeper_transaction(selection, tokens):
    from almanak.connectors.gmx_v2.fork_keeper import calldata

    parameters = (tokens, (PROVIDER,) * len(tokens), (PAYLOAD,) * len(tokens))
    return {"to": selection.handler, "input": calldata("order", "0x" + "ab" * 32, parameters)}


def test_a_publication_is_usable_only_with_every_market_token():
    line = Timeline(Mock(), market())
    order = line.selections[0]
    observed = 2000  # the observed time inside PAYLOAD (see test_gmx_fork_keeper.report)
    assert line.report_times(keeper_transaction(order, (TOKEN, OTHER))) == (observed, observed)
    assert line.report_times(keeper_transaction(order, (TOKEN,))) is None
    relay = keeper_transaction(order, (TOKEN, OTHER)) | {"to": "0x" + "51" * 20}
    assert line.report_times(relay) is None
    line.adjustments = {OTHER: 5}
    assert line.report_times(keeper_transaction(order, (TOKEN, OTHER))) == (observed - 5, observed)


def test_publications_page_from_the_start_time_and_stop_past_the_end(monkeypatch):
    reader = Blocks(5000)
    line = Timeline(reader, market())
    pages = []
    hashes = {}

    def transactions(start, end):
        pages.append((start, end))
        return [f"0x{start:x}"]

    def times(transaction):
        number = int(transaction, 16)
        hashes[transaction] = number
        return (1000 + 2 * number, 1000 + 2 * number)

    line.transactions, line.times = transactions, times
    published = line.publications(1100, 3000)
    assert pages[0][0] == 50 and pages[-1][1] < 5000
    assert published == sorted(p for p in published if 1100 <= p[0] and p[1] <= 3000)
    assert line.publications(20000, 30000) == []


def test_publications_read_only_the_markets_oracle_updates(monkeypatch):
    line = Timeline(Mock(), market())
    events = [
        SimpleNamespace(sender=line.market.contracts.oracle, transaction_hash="0x01"),
        SimpleNamespace(sender="0x" + "99" * 20, transaction_hash="0x02"),
        SimpleNamespace(sender=line.market.contracts.oracle, transaction_hash="0x01"),
        None,
    ]
    line.reader._rpc.return_value = events
    monkeypatch.setattr(report_timeline, "decode_event", lambda log, emitter: log)
    assert line.transactions(1, 2) == ["0x01"]
    line.reader._rpc.return_value = {"to": "0x" + "51" * 20, "input": "0x"}
    assert line.times("0x01") is None and line.times("0x01") is None
    assert line.reader._rpc.call_count == 2


def fork_state(*markets):
    allocations = [SimpleNamespace(name=str(i)) for i in range(len(markets))]
    config = SimpleNamespace(
        allocations=allocations, target=lambda name: SimpleNamespace(market=markets[int(name)])
    )
    return SimpleNamespace(config=config)


class Rpc:
    def __call__(self, method, params=()):
        return {"timestamp": hex(1000)}


def aligned_keeper(aligns):
    forks = SimpleNamespace(rpc=lambda chain: Rpc())
    keeper = Keeper(forks, None)
    keeper.timeline = lambda market: SimpleNamespace(align=aligns[market.share_asset])
    return keeper


def gm(name):
    return SimpleNamespace(share_asset=name, chain=SimpleNamespace(chain_id="eip155:43114"))


def test_a_keeper_aligns_the_next_decision_for_every_traded_market():
    keeper = aligned_keeper({"a": lambda t: max(t, 1300), "b": lambda t: max(t, 1200)})
    assert keeper.align(fork_state(gm("a"), gm("b"), gm("a")), 60) == 300
    keeper = aligned_keeper({"a": lambda t: None})
    assert keeper.align(fork_state(gm("a")), 60) == 60


def test_keepers_step_is_at_least_the_requested_advance(tmp_path, monkeypatch):
    monkeypatch.setattr(testflow, "decoded", lambda wire: SimpleNamespace())
    binding = Mock()
    monkeypatch.setattr(
        "almanak.connectors.composition.connector_from_wire",
        lambda config: SimpleNamespace(requests=binding),
    )
    monkeypatch.setattr("almanak.workspace.read", lambda root: {"processes": {}})
    keepers = Keepers(tmp_path, tmp_path / "env")
    assert keepers.step(None, 60) == 60
    binding.fork_keeper.return_value.align.return_value = 200
    assert keepers.step({"config": {}}, 60) == 200
    binding.fork_keeper.return_value.align.return_value = 10
    assert keepers.step({"config": {}}, 60) == 60
    binding.fork_keeper.return_value.align.side_effect = DataError("unavailable", "no head")
    assert keepers.step({"config": {}}, 60) == 60 and "no head" in keepers.failures[0]
    monkeypatch.setattr(
        "almanak.connectors.composition.connector_from_wire",
        lambda config: SimpleNamespace(requests=None),
    )
    assert Keepers(tmp_path, tmp_path / "env").step({"config": {}}, 60) == 60


def test_teardown_starts_where_the_keeper_aligned_it(monkeypatch):
    moved = []
    monkeypatch.setattr(testflow, "advance", lambda root, seconds: moved.append(seconds))
    monkeypatch.setattr("almanak.reports.checkpoint", lambda root: {})
    monkeypatch.setattr("almanak.workspace.request", Mock(side_effect=ValueError("refused")))
    verdict = testflow.empty_verdict()
    testflow.teardown(None, verdict, 60, Mock(step=Mock(return_value=90)))
    assert moved == [90] and "refused" in verdict["failures"][0]["message"]
    testflow.teardown(None, testflow.empty_verdict(), 60, Mock(step=Mock(return_value=0)))
    assert moved == [90]


def test_a_gmx_fork_starts_at_its_markets_dense_stretch(monkeypatch):
    resolved = SimpleNamespace(market="market")
    monkeypatch.setattr(
        "almanak.connectors.gmx_v2.resolution.resolve_market", lambda r, s, b: resolved
    )
    monkeypatch.setattr(report_timeline.Timeline, "__init__", lambda self, reader, market: None)
    monkeypatch.setattr(report_timeline.Timeline, "anchor", lambda self, latest: latest - 500)
    reader = Mock()
    assert gmx_fork.Fork.anchor_time(reader, ["selection"], 9000) == 8500
    assert gmx_fork.Fork.anchor_time(reader, ["a", "b"], 9000) is None


def test_a_lagged_anchor_moves_to_a_bindings_chosen_start(monkeypatch):
    reader = Blocks(5000)
    context = Mock(__enter__=Mock(return_value=reader), __exit__=Mock(return_value=False))
    monkeypatch.setattr("almanak.gateway.read_endpoints.read_endpoint", lambda chain, env: "")
    monkeypatch.setattr(
        "almanak.gateway.read_endpoints.mainnet_reader", lambda chain, endpoint: context
    )
    chain = ChainId("eip155:43114")
    earlier = (SimpleNamespace(anchor_time=lambda r, s, latest: latest - 1000), ["s"])
    none = (SimpleNamespace(anchor_time=lambda r, s, latest: None), ["s"])
    plain = (SimpleNamespace(), ["s"])
    anchor = fork_operator.pinned_anchor(chain, None, None, 3600, [earlier, none, plain])
    assert anchor["height"] == 5000 - 1800 - 500
    assert fork_operator.pinned_anchor(chain, None, None, 3600, [none])["height"] == 5000 - 1800


def test_anchor_bindings_group_each_connectors_markets_on_the_chain():
    def selection(connector, chain="eip155:43114"):
        return SimpleNamespace(connector=connector, chain=ChainId(chain))

    inputs = SimpleNamespace(
        markets={
            "a": selection("gmx_v2"),
            "b": selection("gmx_v2"),
            "c": selection("uniswap_v3"),
            "d": selection("gmx_v2", "eip155:42161"),
        }
    )
    bindings = fork_operator.anchor_bindings(inputs, ChainId("eip155:43114"))
    assert [len(selections) for _, selections in bindings] == [2, 1]
    assert fork_operator.anchor_lag(inputs, ChainId("eip155:43114")) == 10800
    assert block(1, 2).number == 1


@pytest.mark.parametrize("chain", ["eip155:42161", "eip155:43114"])
def test_timelines_read_the_reviewed_handlers(chain):
    line = Timeline(Mock(), market(chain))
    assert [s.kind for s in line.selections] == ["order", "deposit", "withdrawal"]
    assert line.tokens == (TOKEN, OTHER)
