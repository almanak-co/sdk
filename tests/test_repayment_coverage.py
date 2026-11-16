"""Repayment coverage is a per-decision measurement, with a labeled rate projection."""

from types import SimpleNamespace

from almanak.caip import AssetType, ChainId
from almanak.connectors.aave_v3.lifecycle import repayment_coverage as aave_v3_coverage
from almanak.connectors.aave_v4.lifecycle import repayment_coverage as aave_v4_coverage
from almanak.connectors.compound_v3.lifecycle import repayment_coverage as compound_coverage
from almanak.connectors.morpho_blue.lifecycle import repayment_coverage as morpho_coverage
from almanak.connectors.repayment import (
    RAY,
    SECONDS_PER_YEAR,
    Coverage,
    annual_ray_per_second_wad,
    coverage,
    latest_coverage,
    seconds_until_shortfall,
    transitions,
)
from almanak.dashboard.repayment import repayment_datasets
from almanak.models import Account, Block, Chain
from almanak.runtime import Runtime
from tests.test_aave_v3_config import parameters as parameters

CHAIN = Chain(chain_id=ChainId("eip155:1"), environment="mainnet")
USDC = AssetType("eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
BLOCK = Block(chain=CHAIN, number=1, hash="0x" + "11" * 32, timestamp=1_700_000_000)
FUND = Account(chain=CHAIN, address="0x" + "22" * 20)
RATE = 3_170_979_198  # about 10% a year, per second, in wad


def test_coverage_measures_debt_against_available_and_projects_with_the_observed_rate():
    covered = coverage("loan", USDC, BLOCK, 5_000_000, 6_000_000, RATE)
    assert covered.shortfall == 0
    assert covered.projection is not None
    # Compounding per second at about 10% a year takes roughly 1.9 years to grow 5 into 6.
    years = covered.projection.seconds_until_shortfall / SECONDS_PER_YEAR
    assert 1.8 < years < 1.9
    short = coverage("loan", USDC, BLOCK, 5_000_001, 5_000_000, RATE)
    assert short.shortfall == 1 and short.projection.seconds_until_shortfall == 0


def test_missing_observations_are_missing_not_zero():
    row = coverage("loan", USDC, None, None, 5)
    assert (row.debt, row.available, row.shortfall, row.projection) == (None, 5, None, None)


def test_unobserved_rate_reports_no_projection_and_a_flat_debt_never_falls_short():
    assert coverage("loan", USDC, BLOCK, 5, 6).projection is None
    assert seconds_until_shortfall(5, 6, 0) is None


def test_aave_annual_ray_converts_to_a_per_second_wad():
    assert annual_ray_per_second_wad(RAY // 10) == RAY // 10 // SECONDS_PER_YEAR // 10**9


def test_open_debt_that_goes_unobserved_stays_visible_as_missing():
    previous = (coverage("loan", USDC, BLOCK, 5, 6),)
    rows = latest_coverage(previous, (), observed=())
    assert rows == (
        Coverage(
            allocation="loan", asset=USDC, block=None, debt=None, available=None, shortfall=None
        ),
    )
    # Observed again with no debt: the repaid allocation drops out.
    assert latest_coverage(previous, (), (SimpleNamespace(allocation="loan"),)) == ()


def test_shortfall_events_mark_start_and_end_only():
    covered = (coverage("loan", USDC, BLOCK, 5, 6),)
    short = (coverage("loan", USDC, BLOCK, 7, 6),)
    assert [e["event"] for e in transitions(covered, short)] == ["repayment_shortfall"]
    assert list(transitions(short, short)) == []
    assert [e["event"] for e in transitions(short, covered)] == ["repayment_shortfall_cleared"]
    assert [e["event"] for e in transitions(short, ())] == ["repayment_shortfall_cleared"]


def state(**allocations):
    config = SimpleNamespace(
        allocation=lambda name: SimpleNamespace(borrow=allocations[name]),
        debts=tuple(SimpleNamespace(name=n) for n, borrow in allocations.items() if borrow),
    )
    return SimpleNamespace(config=config)


def pool_observation(name, debt, wallet):
    reserve = SimpleNamespace(variable_borrow_rate_ray=RAY // 10)
    return SimpleNamespace(
        allocation=name,
        target=SimpleNamespace(asset=USDC),
        block=BLOCK,
        position=SimpleNamespace(debt_assets=debt, reserve=reserve),
        wallet_balance=wallet,
    )


def test_pool_lending_covers_only_borrowing_allocations_with_open_debt():
    observed = (
        pool_observation("loan", 5, 6),
        pool_observation("collateral", 0, 9),
        pool_observation("repaid", 0, 1),
    )
    rows = aave_v3_coverage(state(loan=True, collateral=False, repaid=True), observed)
    assert [(r.allocation, r.debt, r.available, r.shortfall) for r in rows] == [("loan", 5, 6, 0)]
    assert rows[0].projection.borrow_rate_per_second_wad == annual_ray_per_second_wad(RAY // 10)


def test_compound_and_morpho_use_their_observed_per_second_rates(monkeypatch):
    from almanak.connectors.compound_v3 import lifecycle as compound
    from almanak.connectors.morpho_blue import lifecycle as morpho

    monkeypatch.setattr(compound, "rates", lambda market: (1, RATE))
    comet = SimpleNamespace(
        allocation="loan",
        target=SimpleNamespace(asset=USDC),
        block=BLOCK,
        account=SimpleNamespace(borrowed_base=5, market=None),
        wallet_balance=4,
    )
    (row,) = compound_coverage(state(loan=True), (comet,))
    assert (row.shortfall, row.projection.borrow_rate_per_second_wad) == (1, RATE)

    monkeypatch.setattr(morpho, "borrowed_assets", lambda account, timestamp: 5)
    observation = SimpleNamespace(
        allocation="loan",
        target=SimpleNamespace(asset=USDC),
        block=BLOCK,
        account=SimpleNamespace(market=SimpleNamespace(borrow_rate=RATE)),
        wallet_balance=6,
    )
    (row,) = morpho_coverage(state(loan=True), (observation,))
    assert (row.shortfall, row.projection.borrow_rate_per_second_wad) == (0, RATE)


def test_aave_v4_reports_coverage_without_a_projection():
    reserve = SimpleNamespace(source=SimpleNamespace(asset=USDC), block=BLOCK)
    observation = SimpleNamespace(
        allocation="debt",
        position=SimpleNamespace(total_debt=5, reserve=reserve),
        wallet_balance=4,
    )
    (row,) = aave_v4_coverage(state(debt=10), (observation,))
    assert (row.shortfall, row.projection) == (1, None)


class Journal:
    def __init__(self, current):
        self.current = current

    def update(self, change):
        self.current = change(self.current)
        return self.current


def test_runtime_retains_coverage_and_logs_only_transitions(capsys):
    from pydantic import BaseModel

    class Checkpoint(BaseModel):
        repayment: tuple[Coverage, ...] = ()

    before = Checkpoint()
    runtime = Runtime.__new__(Runtime)
    runtime.journal = Journal(before)
    measured = (coverage("loan", USDC, BLOCK, 7, 6),)
    runtime.connector = SimpleNamespace(
        lifecycle=SimpleNamespace(repayment_coverage=lambda state, observed: measured)
    )
    observed = (SimpleNamespace(allocation="loan"),)
    after = runtime.retain_repayment(before, observed)
    assert after.repayment == measured and runtime.journal.current == after
    assert '"event": "repayment_shortfall"' in capsys.readouterr().err
    assert runtime.retain_repayment(after, observed) is after
    assert capsys.readouterr().err == ""
    runtime.connector = SimpleNamespace(lifecycle=SimpleNamespace())
    assert runtime.retain_repayment(before, observed) is before


def test_dashboard_lists_coverage_only_while_debt_is_open(parameters):
    assert repayment_datasets(SimpleNamespace(repayment=(), owned=()), {}) == {}
    target = parameters.target("loan")
    row = coverage("loan", target.asset, None, 7, 6, RATE)
    values = {}
    datasets = repayment_datasets(
        SimpleNamespace(
            repayment=(row,),
            owned=(SimpleNamespace(target=target),),
            config=SimpleNamespace(asset_metadata={target.asset: SimpleNamespace(symbol="USDC")}),
        ),
        values,
    )
    (cells,) = datasets["repayment_coverage"].rows
    assert cells["shortfall"].ref == "loan-repayment-shortfall"
    assert values["loan-repayment-shortfall"].amount == "1"
    assert values["loan-repayment-shortfall"].symbol == "USDC"
    projection = values["loan-repayment-projection"]
    assert projection.unit == "seconds" and projection.amount == "0"
    assert projection.reason.startswith("Model output, not a measurement")


def test_runtime_logs_nothing_when_another_writer_replaced_the_checkpoint(capsys):
    from pydantic import BaseModel

    class Checkpoint(BaseModel):
        repayment: tuple[Coverage, ...] = ()
        revision: int = 0

    runtime = Runtime.__new__(Runtime)
    runtime.journal = Journal(Checkpoint(revision=1))
    measured = (coverage("loan", USDC, BLOCK, 7, 6),)
    runtime.connector = SimpleNamespace(
        lifecycle=SimpleNamespace(repayment_coverage=lambda state, observed: measured)
    )
    saved = runtime.retain_repayment(Checkpoint(), (SimpleNamespace(allocation="loan"),))
    assert saved == Checkpoint(revision=1)
    assert capsys.readouterr().err == ""
