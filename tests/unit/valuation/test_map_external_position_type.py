"""Branch coverage for PortfolioValuer._map_external_position_type.

The mapper folds free-form external-provider position-type strings (DeBank
et al.) onto the teardown PositionType enum. Every keyword arm is exercised
(including each ``or`` alternative), plus normalization (case, whitespace)
and precedence between overlapping keywords. Pure static method — no
valuer construction, no network.
"""

import pytest

from almanak.framework.teardown.models import PositionType
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # perp / future -> PERP
        ("perpetual", PositionType.PERP),
        ("Futures", PositionType.PERP),
        # borrow / debt / loan -> BORROW
        ("borrowed", PositionType.BORROW),
        ("debt", PositionType.BORROW),
        ("loan", PositionType.BORROW),
        # supply / deposit / lend -> SUPPLY
        ("supply", PositionType.SUPPLY),
        ("Deposit", PositionType.SUPPLY),
        ("lending", PositionType.SUPPLY),
        # vault / yield / earn -> VAULT
        ("vault", PositionType.VAULT),
        ("yield", PositionType.VAULT),
        ("earn", PositionType.VAULT),
        # stake / farm -> STAKE
        ("staked", PositionType.STAKE),
        ("farming", PositionType.STAKE),
        # predict -> PREDICTION
        ("prediction market", PositionType.PREDICTION),
        # cex -> CEX
        ("cex balance", PositionType.CEX),
        # lp / liquidity / pool -> LP
        ("lp", PositionType.LP),
        ("liquidity position", PositionType.LP),
        ("pool", PositionType.LP),
        # fallback -> TOKEN
        ("wallet", PositionType.TOKEN),
        ("", PositionType.TOKEN),
    ],
)
def test_keyword_arms(raw: str, expected: PositionType) -> None:
    assert PortfolioValuer._map_external_position_type(raw) is expected


def test_normalizes_case_and_whitespace() -> None:
    assert PortfolioValuer._map_external_position_type("  PERPETUAL  ") is PositionType.PERP
    assert PortfolioValuer._map_external_position_type("\tVault\n") is PositionType.VAULT


class TestPrecedence:
    """Earlier arms win when multiple keywords appear in one label."""

    def test_perp_beats_lp(self) -> None:
        # "perp" is checked before "pool".
        assert PortfolioValuer._map_external_position_type("perp pool") is PositionType.PERP

    def test_borrow_beats_supply(self) -> None:
        assert PortfolioValuer._map_external_position_type("borrow deposit") is PositionType.BORROW

    def test_supply_beats_vault(self) -> None:
        assert PortfolioValuer._map_external_position_type("deposit vault") is PositionType.SUPPLY

    def test_stake_beats_lp(self) -> None:
        assert PortfolioValuer._map_external_position_type("staked lp") is PositionType.STAKE

    def test_liquidity_substring_of_borrow_label_does_not_win(self) -> None:
        # "loan liquidity" hits the BORROW arm first.
        assert PortfolioValuer._map_external_position_type("loan liquidity") is PositionType.BORROW
