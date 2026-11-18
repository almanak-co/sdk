"""Lending readings at each decision: balances, health, rates and value for dashboard charts."""

from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from almanak.connectors.aave_v3.state import Checkpoint
from almanak.connectors.composed.dashboard_history import history_row as composed_row
from almanak.connectors.compound_v3.domain import AccountObservation as CometAccount
from almanak.connectors.compound_v3.domain import CollateralBalance
from almanak.connectors.lending_dashboard import history_row, lending_fields
from almanak.models import Block
from tests import test_compound_v3_offline as comet
from tests import test_morpho_blue_offline as morpho
from tests.lending_captures import read_capture
from tests.test_dashboard_history import MINTED
from tests.test_morpho_blue_offline import config as config

CAPTURE = Path(__file__).parent / "fixtures/aave-v3-safe-lifecycle.json.gz"


def decimals(row):
    return {key: Decimal(value) for key, value in row.fields.items()}


def recorded():
    return Checkpoint.model_validate_json(read_capture(CAPTURE))


def test_aave_borrow_reads_both_allocations_health_rates_and_value_against_holding():
    # A real Arbitrum fork run: 0.01 WETH supplied as collateral, 5 USDC borrowed against it.
    state = recorded()
    collateral = state.outcomes[3].after
    loan = state.outcomes[4].after
    fields = decimals(history_row(state, (collateral, loan)))
    assert fields["collateral:supplied"] > Decimal("0.01") - Decimal("1e-9")
    assert fields["loan:debt"] == Decimal("5.000001")
    assert fields["loan:wallet"] == 6
    assert abs(fields["loan:health"] - Decimal("4.148")) < Decimal("0.001")
    assert abs(fields["loan:ltv"] - Decimal("0.2")) < Decimal("0.001")
    assert fields["loan:max_ltv"] == Decimal("0.805")
    assert abs(fields["collateral:price_usd"] - Decimal("2498.7")) < Decimal("0.01")
    assert Decimal(0) < fields["collateral:supply_apy"] < fields["collateral:borrow_apy"] < 1
    # Wallet plus supplied minus debt, at each reserve's oracle price in USD.
    expected = (
        fields["collateral:supplied"] * fields["collateral:price_usd"]
        + (fields["loan:wallet"] - fields["loan:debt"]) * fields["loan:price_usd"]
    )
    assert abs(fields["value"] - expected) < Decimal("1e-12")
    # The baseline holds the starting 0.01 WETH and 1 USDC loan reserve.
    assert abs(fields["hold"] - fields["value"]) < Decimal("0.01")


def test_a_single_allocation_keeps_plain_keys_and_no_health_without_debt():
    full = recorded()
    leg = full.outcomes[1].after
    # A strategy with this one lending allocation: its value is the leg's own.
    state = full.model_copy(
        update={"owned": tuple(o for o in full.owned if o.target == leg.target)}
    )
    row = history_row(state, (leg,))
    fields = decimals(row)
    assert row.block == state.outcomes[1].after.block.number
    assert {"supplied", "debt", "wallet", "price_usd", "supply_apy", "value"} <= set(fields)
    assert "health" not in fields and fields["ltv"] == 0
    assert history_row(state, ()) is None
    assert history_row(state, (MINTED,)) is None


def comet_leg(config, name, **account):
    market = comet.observation()
    balance = CollateralBalance(asset=comet.WETH, balance=account.pop("weth", 0))
    observed = CometAccount.model_construct(
        market=market,
        fund=config.target(name).fund,
        collaterals=(balance,),
        **{"principal": 0, "supplied_base": 0, "borrowed_base": 0, **account},
    )
    target = config.target(name)
    return SimpleNamespace(target=target, account=observed, wallet_balance=0, block=market.block)


def test_compound_reads_lender_supply_and_borrower_health_at_the_comet_prices():
    config = comet.parameters()
    observed = (
        comet_leg(config, "earn", supplied_base=1000 * 10**6),
        comet_leg(config, "collateral", weth=10**18, borrowed_base=200 * 10**6),
        comet_leg(config, "loan", weth=10**18, borrowed_base=200 * 10**6),
    )
    fields = lending_fields(observed, ())
    assert fields["earn:supplied"] == 1000 and "earn:health" not in fields
    assert fields["collateral:collateral"] == 1
    assert fields["collateral:price_usd"] == 2000 and fields["loan:price_usd"] == 1
    assert fields["loan:debt"] == 200
    # 1 WETH at 2000 USD, liquidate factor 0.85, against 200 USD of debt.
    assert fields["loan:health"] == Decimal("8.5") == fields["collateral:health"]
    assert fields["loan:ltv"] == Decimal("0.1") and fields["loan:max_ltv"] == Decimal("0.8")
    assert fields["earn:supply_apy"] > 0 and fields["earn:borrow_apy"] > 0
    assert "collateral:supply_apy" not in fields
    assert fields["value"] == 1000 + 2000 - 200
    assert "hold" not in fields


def test_morpho_reads_loan_per_collateral_health_and_value_in_the_loan_asset(config):
    shares = 200 * 10**6 * morpho.SHARES
    observed = (
        morpho.snapshot(config, "earn", supply=1000 * 10**6 * morpho.SHARES, wallet=0),
        morpho.snapshot(config, "collateral", collateral=10**18, borrow=shares, wallet=0),
        morpho.snapshot(config, "loan", collateral=10**18, borrow=shares, wallet=0),
    )
    started = (
        morpho.snapshot(config, "earn", wallet=1000 * 10**6),
        morpho.snapshot(config, "collateral", wallet=10**18),
        morpho.snapshot(config, "loan", wallet=0),
    )
    fields = lending_fields(observed, started)
    assert fields["collateral:price"] == 2000 and "collateral:price_usd" not in fields
    assert abs(fields["loan:debt"] - 200) < Decimal("0.000002")
    assert abs(fields["loan:health"] - Decimal("8.6")) < Decimal("0.0001")
    assert abs(fields["loan:ltv"] - Decimal("0.1")) < Decimal("0.000001")
    assert fields["loan:max_ltv"] == Decimal("0.86")
    assert Decimal("0.03") < fields["loan:borrow_apy"] < Decimal("0.05")
    assert 0 < fields["earn:supply_apy"] < fields["earn:borrow_apy"]
    assert abs(fields["value"] - 2800) < Decimal("0.00001")
    assert fields["hold"] == 3000


def hub(allocation, *, supplied, debt, health=None, wallet=0, decimals=18):
    block = Block(chain=comet.CHAIN, number=7, hash="0x" + "ab" * 32, timestamp=1_700_000_000)
    asset = SimpleNamespace(decimals=decimals, identifier=f"asset{decimals}")
    return SimpleNamespace(
        target=SimpleNamespace(
            allocation=allocation,
            fund="fund",
            asset=asset.identifier,
            reserve=SimpleNamespace(kind="aave_v4"),
        ),
        position=SimpleNamespace(
            reserve=SimpleNamespace(asset=asset, block=block),
            supplied_assets=supplied,
            total_debt=debt,
        ),
        health_factor=health,
        wallet_balance=wallet,
        block=block,
    )


def test_aave_v4_reads_balances_and_debt_health_without_prices():
    supply = hub("weth", supplied=10**16, debt=0, wallet=10**15)
    assert lending_fields((supply,), (hub("weth", supplied=0, debt=0, wallet=11 * 10**15),)) == {
        "value": Decimal("0.011"),
        "hold": Decimal("0.011"),
        "wallet": Decimal("0.001"),
        "supplied": Decimal("0.01"),
        "debt": 0,
    }
    debt = hub("usdc", supplied=0, debt=5 * 10**6, health=3 * 10**18, decimals=6)
    fields = lending_fields((supply, debt), ())
    assert fields["usdc:health"] == 3 and "weth:health" not in fields
    # WETH and USDC have no common price here, so no total is read.
    assert "value" not in fields


def test_composed_rows_key_lending_readings_by_allocation():
    state = recorded()
    collateral, loan = state.outcomes[3].after, state.outcomes[4].after
    pool = SimpleNamespace(**MINTED.__dict__, target=SimpleNamespace(allocation="trade"))
    pool.pool.block = loan.block
    composed = SimpleNamespace(baseline=state.baseline)
    fields = decimals(composed_row(composed, (collateral, loan, pool)))
    assert "loan:health" in fields and "collateral:supplied" in fields
    # Pool parts are read through their own connector (tests/test_composed_dashboard_history.py).
    assert not any(key.startswith("trade:") for key in fields)
    assert len(fields) <= 32
    assert composed_row(composed, ()) is None


def test_a_weth_comet_prices_in_weth_and_a_usdc_comet_in_usd():
    from almanak.caip import AssetType
    from almanak.connectors.lending_dashboard import USD, comet_quote

    weth = AssetType("eip155:42161/erc20:0x82af49447d8a07e3bd95bd0d56f35241523fbab1")
    usdc = AssetType("eip155:42161/erc20:0xaf88d065e77c8cc2239327c5edb3a432268e5831")

    def market(asset):
        return SimpleNamespace(base_asset=SimpleNamespace(identifier=asset))

    assert comet_quote(market(weth)) == str(weth)
    assert comet_quote(market(usdc)) == USD


def test_a_lending_value_needs_every_configured_lending_leg():
    state = recorded()
    collateral, loan = state.outcomes[3].after, state.outcomes[4].after
    baseline = list(state.baseline)
    full = lending_fields([collateral, loan], baseline, required=["collateral", "loan"])
    assert "value" in full
    # Another chain's leg not read yet: the strategy's value would be a partial total.
    partial = lending_fields([collateral, loan], baseline, required=["collateral", "loan", "far"])
    assert "value" not in partial and "hold" not in partial
    assert partial["loan:health"] == full["loan:health"]


def test_a_composed_strategy_with_other_parts_names_the_lending_total_as_such():
    state = recorded()
    collateral, loan = state.outcomes[3].after, state.outcomes[4].after
    lending_only = SimpleNamespace(baseline=state.baseline, owned=state.owned)
    assert "value" in decimals(composed_row(lending_only, (collateral, loan)))
    pool = SimpleNamespace(target=SimpleNamespace(allocation="trade"))
    mixed = SimpleNamespace(baseline=state.baseline, owned=(*state.owned, pool))
    fields = decimals(composed_row(mixed, (collateral, loan)))
    assert "value" not in fields and "hold" not in fields
    assert "lending_value" in fields and "lending_hold" in fields
