"""Curve pool funding and reported state: one rule for ax, check and backtests."""

from types import SimpleNamespace

import pytest

from almanak import ax_discovery, checks
from almanak.caip import ChainId
from almanak.connectors import v3_pool_request
from almanak.connectors.curve import pool_report, stable_math
from almanak.connectors.curve.pool_report import (
    UnfundedPool,
    funded,
    pool_state,
    require_funded,
    unfunded_reason,
    unfunded_report,
)

USDG = SimpleNamespace(identifier="eip155:4663/erc20:0x01", decimals=6)
PUSD = SimpleNamespace(identifier="eip155:4663/erc20:0x02", decimals=6)
SHARE = SimpleNamespace(identifier="eip155:4663/erc20:0xaa", decimals=18)
POOL = "0xaef38aec2b7daf4df4561a61e82b57d710e5b60b"
CHAIN = ChainId("eip155:4663")
BLOCK = SimpleNamespace(number=89_676_405, timestamp=1_790_000_000)


class FundingReader:
    """Answers `balances(i)` and `totalSupply()` for one pool."""

    def __init__(self, balances, supply):
        self.balances, self.supply, self.calls = balances, supply, []

    def call(self, address, signature, block, *, inputs=(), args=(), outputs=("uint256",)):
        self.calls.append((address, signature, args))
        if signature == "totalSupply()":
            return (self.supply,)
        return (self.balances[args[0]],)


def resolved():
    source = SimpleNamespace(pool=POOL, coins=(USDG.identifier, PUSD.identifier))
    return SimpleNamespace(pool=source, coins=(USDG, PUSD), share=SHARE, block=BLOCK)


@pytest.mark.parametrize(
    ("balances", "supply", "expected"),
    [((1, 1), 1, True), ((0, 1), 1, False), ((1, 1), 0, False), ((0, 0), 0, False)],
)
def test_a_funded_pool_holds_every_coin_and_lp_supply(balances, supply, expected):
    assert funded(balances, supply) is expected


def test_the_unfunded_reason_names_each_empty_balance_and_the_missing_supply():
    coins = (USDG.identifier, PUSD.identifier)
    assert unfunded_reason(coins, (5, 5), 10) is None
    assert unfunded_reason(coins, (0, 0), 0) == (
        "Curve execution requires an existing funded pool; the pool holds no balance of "
        f"{USDG.identifier}, {PUSD.identifier} and no LP supply"
    )
    assert unfunded_reason(coins, (5, 5), 0).endswith("the pool holds no LP supply")


def test_ax_reports_an_unfunded_pool_with_its_balances_and_no_price():
    report = unfunded_report(FundingReader((0, 0), 0), resolved(), BLOCK)
    assert report["status"] == "unfunded_pool" and report["prices"] is None
    assert "requires an existing funded pool" in report["reason"]
    assert report["pool_state"] == {
        "balances": [
            {"asset": USDG.identifier, "amount": "0", "decimals": 6},
            {"asset": PUSD.identifier, "amount": "0", "decimals": 6},
        ],
        "total_supply": {"asset": SHARE.identifier, "amount": "0", "decimals": 18},
    }
    assert unfunded_report(FundingReader((3, 4), 7), resolved(), BLOCK) is None


def test_an_unfunded_pool_is_reported_without_reading_its_state(monkeypatch):
    reads = []
    resolution = SimpleNamespace(
        MarketSelection=lambda **values: SimpleNamespace(**values),
        resolve_market=lambda reader, selection, block: SimpleNamespace(
            **vars(resolved()), summary=lambda: {"pool": POOL}
        ),
    )
    modules = {
        "resolution": resolution,
        "reader": SimpleNamespace(read_pool=lambda *args: reads.append(args) or "observation"),
    }
    monkeypatch.setattr(v3_pool_request, "import_module", lambda name: modules[name.split(".")[-1]])
    monkeypatch.setattr(v3_pool_request, "support_level", lambda connector, chain: "verified")
    request = v3_pool_request.PoolRequest(chain=CHAIN, pool=POOL)
    empty = FundingReader((0, 0), 0)
    empty.chain = SimpleNamespace(chain_id=request.chain)
    report = v3_pool_request.read_resolved_pool(empty, "curve", request, BLOCK)
    assert (report["status"], report["support"], reads) == ("unfunded_pool", "verified", [])


def test_dex_pools_refuses_an_unfunded_curve_pool(monkeypatch):
    state = {
        "support": "verified",
        "resolution": {"selection": {}, "coins": [USDG.identifier, PUSD.identifier]},
        "status": "unfunded_pool",
        "reason": "Curve execution requires an existing funded pool; the pool holds no LP supply",
        "pool_state": {"balances": []},
    }
    monkeypatch.setattr(ax_discovery, "read_resolved_pool", lambda *args: state)
    block = SimpleNamespace(chain=SimpleNamespace(chain_id=CHAIN))
    result = ax_discovery.verify_resolved(None, block, SimpleNamespace(pool=POOL), "curve")
    assert result["status"] == "refused"
    assert result["reason"].startswith("unfunded_pool: Curve execution requires")
    assert result["pool_state"] == {"balances": []} and "selection" not in result["resolution"]
    del state["status"], state["reason"]
    assert (
        ax_discovery.verify_resolved(None, block, SimpleNamespace(pool=POOL), "curve")["status"]
        == "verified"
    )


def stable_observation(balances, supply):
    currencies = (USDG, PUSD)
    state = stable_math.StableState(
        balances=balances,
        rates=(10**30, 10**30),
        amplification=100 * stable_math.AMPLIFICATION_PRECISION,
        fee=10**6,
        offpeg_multiplier=125 * 10**9,
        total_supply=supply,
    )
    return SimpleNamespace(
        source=SimpleNamespace(family="stableswap_ng"),
        currencies=currencies,
        shares=SHARE,
        balances=balances,
        total_supply=supply,
        fee=10**6,
        offpeg_multiplier=125 * 10**9,
        math_state=lambda: state,
    )


def test_a_stable_pool_reports_fee_amplification_and_virtual_price():
    state = pool_state(stable_observation((1_500_000, 1_500_000), 3 * 10**18))
    assert state["balances"][1] == {"asset": PUSD.identifier, "amount": "1500000", "decimals": 6}
    assert state["fee"] == {"raw": "1000000", "bps": "1"}
    assert (state["offpeg_fee_multiplier"], state["amplification"]) == ("12.5", "100")
    # A balanced pool's invariant is the sum of its normalized balances: one per share.
    assert state["virtual_price"] == str(10**18)
    assert pool_state(stable_observation((0, 0), 0))["virtual_price"] is None


def test_a_crypto_pool_reports_its_fee_band_parameters_and_stored_virtual_price():
    packed = (26 * 10**6 << 128) | (45 * 10**6 << 64) | 230 * 10**12
    ramp = SimpleNamespace(at=lambda timestamp: (400_000 * 10_000, 145 * 10**12))
    observation = SimpleNamespace(
        source=SimpleNamespace(family="twocrypto_ng"),
        currencies=(USDG, PUSD),
        shares=SHARE,
        balances=(10**12, 10**12),
        total_supply=10**18,
        block=BLOCK,
        ramp=ramp,
        packed_fees=packed,
        accounting=SimpleNamespace(virtual_price=1_020_000_000_000_000_000),
    )
    state = pool_state(observation)
    assert state["fee"] == {
        "mid_fee": {"raw": "26000000", "bps": "26"},
        "out_fee": {"raw": "45000000", "bps": "45"},
        "fee_gamma": str(230 * 10**12),
    }
    assert (state["amplification"], state["gamma"]) == ("400000", str(145 * 10**12))
    assert state["virtual_price"] == "1020000000000000000"


def test_require_funded_names_the_market_and_pool():
    with pytest.raises(UnfundedPool) as refused:
        require_funded(FundingReader((0, 0), 0), "stable_pool", resolved(), BLOCK)
    assert refused.value.kind == "unfunded_pool" and refused.value.check_code == "unfunded_pool"
    assert str(refused.value).startswith(f"market stable_pool: Curve pool {POOL}: Curve execution")
    require_funded(FundingReader((1, 1), 1), "stable_pool", resolved(), BLOCK)


def test_online_check_reads_only_curve_markets_at_the_latest_block(monkeypatch):
    from almanak.connectors.curve.resolution import ResolvedPool

    opened, checked_markets = [], []

    class Reads:
        def __init__(self, stack, operator, env_file):
            assert (operator, env_file) == ("operator", "env")

        def pinned(self, chain):
            opened.append(chain)
            return "reader", "latest block"

    curve = SimpleNamespace(
        name="stable_pool",
        resolved=ResolvedPool.model_construct(
            block=SimpleNamespace(chain=SimpleNamespace(chain_id="eip155:4663"))
        ),
    )
    other = SimpleNamespace(name="lending", resolved=SimpleNamespace())
    monkeypatch.setattr(checks, "MarketReads", Reads)
    monkeypatch.setattr(
        pool_report,
        "require_funded",
        lambda reader, name, market, block: checked_markets.append((name, block)),
    )
    checks.pool_liquidity([other], "operator", "env")
    assert opened == [] and checked_markets == []
    checks.pool_liquidity([curve, other], "operator", "env")
    assert (opened, checked_markets) == (["eip155:4663"], [("stable_pool", "latest block")])


def sample(balances=(1, 1), supply=1, contracts=("0x1", "0x2"), successor=(1, 101, 11)):
    chain = "eip155:4663"
    number, timestamp = 100, 10
    return SimpleNamespace(
        pool=SimpleNamespace(
            balances=balances,
            total_supply=supply,
            block=SimpleNamespace(chain=chain, number=number, timestamp=timestamp),
        ),
        contracts=tuple(SimpleNamespace(address=a) for a in contracts),
        successor=SimpleNamespace(
            chain=chain if successor[0] else "eip155:1",
            number=successor[1],
            timestamp=successor[2],
        ),
    )


@pytest.mark.parametrize(
    ("values", "message"),
    [
        ({"balances": (0, 1)}, "prepared Curve execution requires an existing funded pool"),
        ({"supply": 0}, "prepared Curve execution requires an existing funded pool"),
        ({"contracts": ("0x1", "0x1")}, "executable identities must be unique"),
        ({"successor": (0, 101, 11)}, "canonical time-floor successor"),
        ({"successor": (1, 102, 11)}, "canonical time-floor successor"),
        ({"successor": (1, 101, 10)}, "canonical time-floor successor"),
    ],
)
def test_a_prepared_sample_applies_the_shared_funding_rule(values, message):
    from almanak.connectors.curve.economic_data import PoolSample

    assert PoolSample.supported(sample()) is not None
    with pytest.raises(ValueError, match=message):
        PoolSample.supported(sample(**values))
