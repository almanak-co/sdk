"""GMX decisions read size, collateral, average entry, funding, GM shares and wallets."""

from decimal import Decimal
from types import SimpleNamespace

from almanak.connectors.gmx_v2.dashboard_history import history_row, readings
from almanak.connectors.gmx_v2.reporting import Projection

WETH, USDC, ETH, GM = "weth", "usdc", "eth", "gm"
DECIMALS = {WETH: 18, USDC: 6, ETH: 18, GM: 18}.get


def amount(asset, value):
    return SimpleNamespace(asset=asset, amount=value)


def perp(position=True):
    # A 1,000 USD WETH long on 0.1 WETH collateral, opened near 2,500 USD per WETH.
    return SimpleNamespace(
        target="Main",
        block=SimpleNamespace(timestamp=1_760_000_000, number=380_000_000),
        wallets=(amount(WETH, 380_000_000_000_000_000), amount(USDC, 0), amount(ETH, 0)),
        claimable_funding=(amount(WETH, 12_345_000_000_000), amount(USDC, 21_500)),
        position=SimpleNamespace(
            size_usd_e30=1000 * 10**30,
            index_amount=amount(WETH, 399_840_063_974_410_235),
            collateral=amount(WETH, 99_650_000_000_000_000),
        )
        if position
        else None,
    )


def test_an_open_long_reads_size_collateral_entry_and_funding_in_token_units():
    fields = {key: Decimal(value) for key, value in readings(perp(), DECIMALS).items()}
    assert fields["size_usd"] == 1000
    assert fields["size_tokens"] == Decimal("0.399840063974410235")
    assert fields["collateral"] == Decimal("0.09965")
    assert abs(fields["entry_price"] - Decimal("2501")) < Decimal("1e-9")
    assert fields["funding_long"] == Decimal("0.000012345")
    assert fields["funding_short"] == Decimal("0.0215")
    assert (fields["wallet0"], fields["wallet1"], fields["wallet2"]) == (Decimal("0.38"), 0, 0)
    assert not {"mark_price", "pnl", "leverage", "liquidation_price"} & set(fields)


def test_a_closed_position_reads_zero_size_and_no_entry():
    fields = readings(perp(position=False), DECIMALS)
    assert (fields["size_usd"], fields["collateral"]) == ("0", "0")
    assert "entry_price" not in fields and "size_tokens" not in fields


def test_gm_liquidity_reads_shares_and_unknown_decimals_are_left_out():
    snapshot = SimpleNamespace(
        wallets=(amount(WETH, 4 * 10**17), amount(USDC, 100_000_000), amount("unknown", 5)),
        market_tokens=amount(GM, 293_120_000_000_000_000_000),
    )
    fields = readings(snapshot, DECIMALS)
    assert fields == {"wallet0": "0.4", "wallet1": "100", "gm_shares": "293.12"}


def test_the_projection_reads_the_owned_allocations_observation():
    metadata = {asset: SimpleNamespace(decimals=DECIMALS(asset)) for asset in (WETH, USDC, ETH)}
    state = SimpleNamespace(
        owned=(SimpleNamespace(target="Main"),),
        config=SimpleNamespace(asset_metadata=metadata),
    )
    row = Projection.history(state, (SimpleNamespace(target="Other"), perp()))
    assert (row.time, row.block, row.fields["size_usd"]) == (1_760_000_000, 380_000_000, "1000")
    assert history_row(state, ()) is None
