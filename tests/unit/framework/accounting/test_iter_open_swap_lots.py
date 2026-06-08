"""Tests for FIFOBasisStore.iter_open_swap_lots (VIB-4984).

The read accessor surfaces open directional swap-inventory lots for
dashboard mark-to-market. Covers:

- pro-rated ``cost_usd_for_remaining`` on a partially-consumed lot
- ``None`` cost propagation (Empty≠Zero) when a lot's cost_usd is None
- ``remaining > 0`` filter (fully-disposed lots excluded)
- ``swap:``-prefix scoping: lending (supply:), prediction and PT keys excluded
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.accounting.basis import FIFOBasisStore


def _swap_key_lots(store: FIFOBasisStore) -> dict[str, tuple[Decimal, Decimal | None]]:
    """Collect iter_open_swap_lots() keyed by token for assertions."""
    out: dict[str, tuple[Decimal, Decimal | None]] = {}
    for _pk, token, remaining, cost in store.iter_open_swap_lots():
        out[token] = (remaining, cost)
    return out


def test_pro_rated_cost_for_partial_remaining() -> None:
    store = FIFOBasisStore()
    store.record_swap_acquisition(
        deployment_id="Strat:abc",
        position_key="swap:arbitrum:0xwallet",
        token="weth",
        amount=Decimal("1.0"),
        cost_usd=Decimal("2000"),
    )
    # Dispose half — remaining 0.5, cost should pro-rate to 1000.
    store.match_swap_disposal(
        deployment_id="Strat:abc",
        position_key="swap:arbitrum:0xwallet",
        token="weth",
        amount=Decimal("0.5"),
    )
    lots = _swap_key_lots(store)
    assert "weth" in lots
    remaining, cost = lots["weth"]
    assert remaining == Decimal("0.5")
    assert cost == Decimal("1000")


def test_none_cost_propagates() -> None:
    store = FIFOBasisStore()
    store.record_swap_acquisition(
        deployment_id="Strat:abc",
        position_key="swap:arbitrum:0xwallet",
        token="weth",
        amount=Decimal("1.0"),
        cost_usd=None,  # unmeasured basis
    )
    lots = _swap_key_lots(store)
    remaining, cost = lots["weth"]
    assert remaining == Decimal("1.0")
    assert cost is None  # Empty≠Zero — must NOT become Decimal("0")


def test_fully_disposed_lot_excluded() -> None:
    store = FIFOBasisStore()
    store.record_swap_acquisition(
        deployment_id="Strat:abc",
        position_key="swap:arbitrum:0xwallet",
        token="weth",
        amount=Decimal("1.0"),
        cost_usd=Decimal("2000"),
    )
    store.match_swap_disposal(
        deployment_id="Strat:abc",
        position_key="swap:arbitrum:0xwallet",
        token="weth",
        amount=Decimal("1.0"),
    )
    assert list(store.iter_open_swap_lots()) == []


def test_non_swap_sourced_wallet_lots_excluded() -> None:
    # The swap:<chain>:<wallet> key is a fungible wallet-basis pool: BORROW /
    # WITHDRAW mint swap:-keyed lots too (VIB-3964). VIB-4984 is scoped to
    # directional SWAP inventory, so only source=="SWAP" lots are surfaced;
    # borrowed/withdrawn-then-held tokens are excluded (deferred → VIB-4997).
    store = FIFOBasisStore()
    store.record_swap_acquisition(
        deployment_id="Strat:abc",
        position_key="swap:arbitrum:0xwallet",
        token="weth",
        amount=Decimal("1.0"),
        cost_usd=Decimal("2000"),
        source="SWAP",
    )
    store.record_swap_acquisition(
        deployment_id="Strat:abc",
        position_key="swap:arbitrum:0xwallet",
        token="usdt",
        amount=Decimal("1000"),
        cost_usd=Decimal("1000"),
        source="BORROW",
    )
    store.record_swap_acquisition(
        deployment_id="Strat:abc",
        position_key="swap:arbitrum:0xwallet",
        token="dai",
        amount=Decimal("500"),
        cost_usd=Decimal("500"),
        source="WITHDRAW",
    )
    lots = _swap_key_lots(store)
    # Only the SWAP-sourced WETH inventory is surfaced.
    assert set(lots) == {"weth"}
    assert lots["weth"] == (Decimal("1.0"), Decimal("2000"))


def test_swap_prefix_excludes_lending_and_prediction_and_pt() -> None:
    store = FIFOBasisStore()
    # Open swap inventory — should appear.
    store.record_swap_acquisition(
        deployment_id="Strat:abc",
        position_key="swap:arbitrum:0xwallet",
        token="weth",
        amount=Decimal("1.0"),
        cost_usd=Decimal("2000"),
    )
    # Lending principal lot (supply:) — must NOT appear.
    store.record_borrow(
        deployment_id="Strat:abc",
        position_key="supply:lending:aave:USDC",
        token="USDC",
        principal_amount=Decimal("100"),
        principal_usd=Decimal("100"),
    )
    # A raw borrow position key (no swap: prefix) — must NOT appear.
    store.record_borrow(
        deployment_id="Strat:abc",
        position_key="lending:aave:USDC",
        token="USDC",
        principal_amount=Decimal("50"),
        principal_usd=Decimal("50"),
    )
    # PT buy lot — must NOT appear.
    store.record_pt_buy(
        deployment_id="Strat:abc",
        position_key="pendle:pt:foo",
        pt_token="PT-foo",
        pt_amount=Decimal("10"),
        sy_cost=Decimal("9"),
    )
    # Prediction aggregate — must NOT appear.
    store.record_prediction_buy(
        deployment_id="Strat:abc",
        position_key="market:outcome",
        shares=Decimal("5"),
        cost_basis_usd=Decimal("4"),
    )
    lots = _swap_key_lots(store)
    assert set(lots.keys()) == {"weth"}


def test_position_key_and_token_round_trip() -> None:
    store = FIFOBasisStore()
    store.record_swap_acquisition(
        deployment_id="Strat:abc",
        position_key="swap:arbitrum:0xwallet",
        token="weth",
        amount=Decimal("1.0"),
        cost_usd=Decimal("2000"),
    )
    rows = list(store.iter_open_swap_lots())
    assert len(rows) == 1
    position_key, token, remaining, cost = rows[0]
    assert position_key == "swap:arbitrum:0xwallet"
    assert token == "weth"  # _key lowercases the token
    assert remaining == Decimal("1.0")
    assert cost == Decimal("2000")
