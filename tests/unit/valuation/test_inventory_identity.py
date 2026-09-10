"""Canonical wallet joins must neither drop a held asset nor multiply its NAV."""

from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.framework.data.tokens.exceptions import TokenNotFoundError
from almanak.framework.valuation.inventory_identity import align_swap_inventory_inputs
from almanak.framework.valuation.portfolio_valuer import _classify_swap_inventory

ADDRESS = "0x" + "1" * 40
OTHER = "0x" + "2" * 40


def identity(token, chain):
    aliases = {"cme": ADDRESS, ADDRESS: ADDRESS, "usdg": OTHER, OTHER: OTHER}
    return chain, aliases[token.casefold()]


@pytest.mark.parametrize("lot,wallet", [("cme", ADDRESS), (ADDRESS, "CME")])
def test_address_symbol_join_preserves_wallet_nav_and_directional_inventory(lot, wallet):
    balances = {wallet: Decimal("2")}
    prices = {wallet: Decimal("1.5")}
    with patch("almanak.framework.valuation.inventory_identity.canonicalize_token_identity", side_effect=identity):
        aligned, marks, quote, base = align_swap_inventory_inputs([lot], balances, prices, "robinhood", OTHER, ADDRESS)
    result = _classify_swap_inventory(
        {lot: (Decimal("2"), Decimal("2.5"))}, aligned, marks, "robinhood", numeraire=quote, base_token=base
    )
    assert result.metadata["status"] == "applied"
    assert result.inventory_value_usd == Decimal("3")
    assert result.rows[0].cost_basis_usd == Decimal("2.5")
    assert balances == {wallet: Decimal("2")}
    assert prices == {wallet: Decimal("1.5")}


def test_missing_wallet_price_cannot_use_symbol_quote_for_an_address_balance():
    with patch("almanak.framework.valuation.inventory_identity.canonicalize_token_identity", side_effect=identity):
        balances, prices, _, _ = align_swap_inventory_inputs(
            ["cme"], {ADDRESS: Decimal("2")}, {"CME": Decimal("99")}, "robinhood", None, None
        )
    result = _classify_swap_inventory({"cme": (Decimal("2"), Decimal("2"))}, balances, prices, "robinhood")
    assert not result.rows
    assert result.metadata["skipped"]["cme"] == "price_missing"


@pytest.mark.parametrize(
    "lots,balances",
    [(["cme", ADDRESS], {ADDRESS: Decimal("2")}), (["cme"], {ADDRESS: Decimal("2"), "CME": Decimal("2")})],
)
def test_ambiguous_alias_observations_are_refused(lots, balances):
    with patch("almanak.framework.valuation.inventory_identity.canonicalize_token_identity", side_effect=identity):
        with pytest.raises(ValueError, match="same chain asset"):
            align_swap_inventory_inputs(lots, balances, {}, "robinhood", None, None)


def test_unresolved_legacy_symbol_cannot_claim_address_balance():
    def missing(token, chain):
        if token == ADDRESS:
            return chain, ADDRESS
        raise TokenNotFoundError(token=token, chain=chain)

    with patch("almanak.framework.valuation.inventory_identity.canonicalize_token_identity", side_effect=missing):
        balances, _, _, _ = align_swap_inventory_inputs(
            ["UNKNOWN"], {ADDRESS: Decimal("2")}, {}, "robinhood", None, None
        )
    assert "UNKNOWN" not in balances


@pytest.mark.parametrize("chains", [("arbitrum",), ("arbitrum", "base")])
def test_snapshot_only_joins_address_aliases_with_one_observed_chain(chains):
    from almanak.framework.valuation.portfolio_valuer import PortfolioValuer
    from tests.unit.valuation.test_swap_inventory_classification_vib5057 import (
        BUY_WBTC,
        DEP,
        make_market,
        make_store,
        make_strategy,
    )

    wbtc = "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f"
    strategy = make_strategy(tracked=(wbtc, "USDC"))
    market = make_market(
        {wbtc: Decimal("110000"), "USDC": Decimal("1"), "ETH": Decimal("2500")},
        {wbtc: Decimal("0.05"), "USDC": Decimal("200"), "ETH": Decimal("0.01")},
    )
    market.chains = chains
    valuer = PortfolioValuer()
    valuer.set_accounting_context(make_store([BUY_WBTC]), DEP)
    with patch.object(valuer, "_swap_inventory_for_snapshot", wraps=valuer._swap_inventory_for_snapshot) as classify:
        snapshot = valuer.value(strategy, market)
    assert classify.call_args.kwargs["allow_address_aliases"] is (len(chains) == 1)
    rows = [row for row in snapshot.positions if (row.details or {}).get("source") == "swap_inventory_lots"]
    if len(chains) == 1:
        assert len(rows) == 1
        assert rows[0].value_usd == Decimal("5500")
        assert rows[0].cost_basis_usd == Decimal("5000")
    else:
        assert rows == []
