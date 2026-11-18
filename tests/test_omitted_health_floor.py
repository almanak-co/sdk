"""An omitted minimum health factor tolerates no debt in every live and backtest risk check.

Each check is driven with its projection stubbed to one health value, so the test
pins only the floor comparison: a stated floor admits health above it, an omitted
floor admits only a debt-free account and refuses any finite health.
"""

from types import SimpleNamespace

import pytest

from almanak.connectors.aave_v3 import economic_actions as aave_economic
from almanak.connectors.aave_v3 import projection as aave_projection
from almanak.connectors.compound_v3 import economic_actions as comet_economic
from almanak.connectors.compound_v3 import projection as comet_projection
from almanak.connectors.morpho_blue import economic_actions as morpho_economic
from almanak.connectors.sparklend import economic_actions as spark_economic
from almanak.connectors.sparklend import projection as spark_projection
from almanak.gateway.ethereum import DataError

STATED = 2 * 10**18
HEALTHY = 3 * 10**18
DEBT_FREE_POOL = 2**256 - 1


class Risky:
    target = SimpleNamespace(allocation="a")


def allocation(floor):
    return SimpleNamespace(minimum_health_factor=floor, borrow=False)


def config(floor):
    return SimpleNamespace(allocation=lambda _: allocation(floor))


def projected(health):
    return SimpleNamespace(
        risk=SimpleNamespace(health_factor_wad=health), has_zero_ltv_collateral=False
    )


def pool_live(module, monkeypatch, floor, health):
    monkeypatch.setattr(module, "requires_risk", lambda *_: True)
    monkeypatch.setattr(module, "projected_inputs", lambda *_: None)
    monkeypatch.setattr(module, "validate_sources", lambda *_: None)
    monkeypatch.setattr(module, "project", lambda _: projected(health))
    module.validate_projection(config(floor), None, SimpleNamespace(allocation="a"))


def pool_backtest(module, monkeypatch, floor, health):
    monkeypatch.setattr(module, "SupplyIntent", Risky)
    monkeypatch.setattr(module, "validate_sources", lambda *_: None)
    monkeypatch.setattr(module, "risk", lambda *_: projected(health))
    before = SimpleNamespace(collateral_enabled=False, target=SimpleNamespace(reserve="r"))
    market = SimpleNamespace(reserve=SimpleNamespace(source="r"))
    module.check_risk(config(floor), Risky(), before, (), (market,))


def comet_live(monkeypatch, floor, health):
    monkeypatch.setattr(comet_projection, "is_borrow_collateralized", lambda _: True)
    monkeypatch.setattr(comet_projection, "health_factor", lambda _: health)
    comet_projection.require_health(allocation(floor), None)


def comet_backtest(monkeypatch, floor, health):
    monkeypatch.setattr(comet_economic, "BorrowOperation", Risky)
    monkeypatch.setattr(comet_economic, "is_borrow_collateralized", lambda _: True)
    monkeypatch.setattr(comet_economic, "health_factor", lambda _: health)
    comet_economic.check_risk(Risky(), None, allocation(floor))


def morpho_backtest(monkeypatch, floor, health):
    monkeypatch.setattr(morpho_economic, "BorrowOperation", Risky)
    morpho_economic.check_risk(Risky(), SimpleNamespace(health_factor=health), allocation(floor))


CHECKS = {
    "aave_live": (lambda m, f, h: pool_live(aave_projection, m, f, h), DEBT_FREE_POOL),
    "aave_backtest": (lambda m, f, h: pool_backtest(aave_economic, m, f, h), DEBT_FREE_POOL),
    "spark_live": (lambda m, f, h: pool_live(spark_projection, m, f, h), DEBT_FREE_POOL),
    "spark_backtest": (lambda m, f, h: pool_backtest(spark_economic, m, f, h), DEBT_FREE_POOL),
    "comet_live": (comet_live, None),
    "comet_backtest": (comet_backtest, None),
    "morpho_backtest": (morpho_backtest, None),
}


@pytest.mark.parametrize("name", CHECKS)
def test_a_stated_floor_admits_health_above_it(name, monkeypatch):
    check, _ = CHECKS[name]
    check(monkeypatch, STATED, HEALTHY)


@pytest.mark.parametrize("name", CHECKS)
def test_an_omitted_floor_refuses_any_debt(name, monkeypatch):
    check, _ = CHECKS[name]
    with pytest.raises((ValueError, DataError), match="health"):
        check(monkeypatch, None, HEALTHY)


@pytest.mark.parametrize("name", [n for n, (_, free) in CHECKS.items() if free is not None])
def test_an_omitted_floor_admits_a_debt_free_pool_account(name, monkeypatch):
    check, debt_free = CHECKS[name]
    check(monkeypatch, None, debt_free)


def test_an_omitted_floor_admits_a_debt_free_morpho_account(monkeypatch):
    morpho_backtest(monkeypatch, None, None)
