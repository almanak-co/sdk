"""Tests for FIFOBasisStore.seed_wallet_inventory (VIB-4394).

Boot-seed of pre-existing wallet inventory as OPENING_BALANCE wallet-basis lots,
so the first disposal of inventory the wallet held before the strategy started
realizes against a basis instead of booking realized_pnl=None. All tests drive
the REAL FIFO store (no SimpleNamespace), exercising the same seed → match path
the runner boot helper uses.

Covers:
  (1) pre-existing inventory seeded as a lot with the canonical (first-snapshot
      price) basis, EXCLUDED from the swap-only iterator but INCLUDED in the
      wallet-basis iterator;
  (2) first disposal realizes against the seed (measured PnL) when basis is known;
  (3) unmeasured boot basis → lot seeded with cost None, disposal returns None
      (NOT a fabricated 0-basis gain);
  (4) restart de-dup: a post-boot SWAP acquisition is additive (not deduped); the
      seed nets only against a PRIOR OPENING_BALANCE seed of the same snapshot —
      a fully-covered token seeds nothing;
  (5) Empty≠Zero balance skip: a missing / zero / unparseable balance seeds nothing.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.accounting.basis import FIFOBasisStore

_DEP = "deployment:abc123"
_KEY = "swap:base:0xwallet"


def _wallet_lots(store: FIFOBasisStore) -> dict[str, tuple[Decimal, Decimal | None]]:
    """Collect iter_open_wallet_basis_lots() keyed by token (lowercased key)."""
    out: dict[str, tuple[Decimal, Decimal | None]] = {}
    for _pk, token, remaining, cost in store.iter_open_wallet_basis_lots():
        out[token] = (remaining, cost)
    return out


def test_seeds_lot_with_canonical_basis_excluded_from_swap_iter() -> None:
    """(1) Opening inventory seeds a wallet-basis lot at balance × first-snapshot price.

    It is INCLUDED in the source-agnostic wallet-basis iterator (tracked
    inventory for the teardown clamp) but EXCLUDED from the SWAP-only iterator
    (the VIB-4984 swap-trading tile must not mislabel opening inventory as swap
    PnL).
    """
    store = FIFOBasisStore()
    seeded = store.seed_wallet_inventory(
        _DEP,
        _KEY,
        [
            {"symbol": "WETH", "balance": "2", "price_usd": "3000"},
            {"symbol": "USDC", "balance": "100", "price_usd": "1"},
        ],
    )
    assert seeded == 2

    wallet = _wallet_lots(store)
    # Token key is lowercased by FIFOBasisStore._key.
    assert wallet["weth"] == (Decimal("2"), Decimal("6000"))
    assert wallet["usdc"] == (Decimal("100"), Decimal("100"))

    # OPENING_BALANCE-sourced lots are NOT swap-trading inventory.
    assert list(store.iter_open_swap_lots()) == []


def test_first_disposal_realizes_against_seed() -> None:
    """(2) A later SWAP disposal of seeded inventory consumes the seeded basis."""
    store = FIFOBasisStore()
    store.seed_wallet_inventory(
        _DEP, _KEY, [{"symbol": "WETH", "balance": "2", "price_usd": "3000"}]
    )
    cost_consumed, unmatched = store.match_swap_disposal(
        deployment_id=_DEP, position_key=_KEY, token="WETH", amount=Decimal("2")
    )
    # Realizes against the $6000 seeded basis — measured, not None.
    assert cost_consumed == Decimal("6000")
    assert unmatched == Decimal("0")


def test_unmeasured_boot_basis_yields_none_not_zero_gain() -> None:
    """(3) Empty≠Zero: absent price → basis-None lot; disposal returns None, never 0."""
    store = FIFOBasisStore()
    seeded = store.seed_wallet_inventory(
        _DEP, _KEY, [{"symbol": "WETH", "balance": "1"}]  # no price_usd
    )
    assert seeded == 1

    # Lot exists with quantity known but cost None.
    wallet = _wallet_lots(store)
    assert wallet["weth"] == (Decimal("1"), None)

    cost_consumed, unmatched = store.match_swap_disposal(
        deployment_id=_DEP, position_key=_KEY, token="WETH", amount=Decimal("1")
    )
    # None (unmeasured basis), NOT Decimal("0") — a 0 basis would fabricate a
    # 100%-gain on the first disposal. The quantity is still consumed.
    assert cost_consumed is None
    assert unmatched == Decimal("0")


def test_post_boot_swap_acquisition_is_additive_not_deduped() -> None:
    """(4a) A replayed post-boot SWAP acquisition is ADDITIVE to the opening seed.

    The snapshot balance is the immutable boot balance. A SWAP that acquired more
    of the token AFTER boot is a post-boot delta orthogonal to the snapshot — it
    must NOT suppress opening-balance seeding. De-duping against the SWAP lot
    would under-seed the opening basis (the bug CodeRabbit flagged): 1 replayed +
    2 seeded == 3, not 2.
    """
    store = FIFOBasisStore()
    # Replayed post-boot SWAP lot (1 WETH acquired via trade, still open).
    store.record_swap_acquisition(
        _DEP, _KEY, "WETH", Decimal("1"), cost_usd=Decimal("3000"), source="SWAP"
    )
    # First snapshot balance is the immutable boot balance: 2 WETH. The full 2 WETH
    # opening balance seeds — the SWAP lot is additive, not a duplicate.
    seeded = store.seed_wallet_inventory(
        _DEP, _KEY, [{"symbol": "WETH", "balance": "2", "price_usd": "3500"}]
    )
    assert seeded == 1

    total = sum(
        remaining
        for _pk, token, remaining, _cost in store.iter_open_wallet_basis_lots()
        if token == "weth"
    )
    # 1 post-boot SWAP + 2 opening seeded == 3.
    assert total == Decimal("3")


def test_restart_dedup_against_prior_opening_balance_seeds_nothing() -> None:
    """(4b) A token fully covered by a PRIOR OPENING_BALANCE lot seeds nothing.

    De-dup nets only against prior OPENING_BALANCE seeds (same-boot idempotency),
    not against SWAP/BORROW/WITHDRAW lots. A snapshot already fully represented by
    a prior opening-balance seed re-seeds nothing.
    """
    store = FIFOBasisStore()
    # A prior OPENING_BALANCE seed already covers 5 WETH.
    store.record_swap_acquisition(
        _DEP,
        _KEY,
        "WETH",
        Decimal("5"),
        cost_usd=Decimal("15000"),
        source="OPENING_BALANCE",
    )
    seeded = store.seed_wallet_inventory(
        _DEP, _KEY, [{"symbol": "WETH", "balance": "3", "price_usd": "3000"}]
    )
    assert seeded == 0


def test_restart_dedup_partial_prior_opening_balance_seeds_remainder() -> None:
    """(4c) A partial PRIOR OPENING_BALANCE seed re-seeds only the remainder.

    The boot snapshot is immutable, so a prior seed that covered only part of it
    (e.g. an earlier seed against a smaller snapshot) tops up the difference and
    no more — flooring the new seed at the un-covered remainder.
    """
    store = FIFOBasisStore()
    # A prior OPENING_BALANCE seed already covers 1 WETH of the boot balance.
    store.record_swap_acquisition(
        _DEP,
        _KEY,
        "WETH",
        Decimal("1"),
        cost_usd=Decimal("3000"),
        source="OPENING_BALANCE",
    )
    seeded = store.seed_wallet_inventory(
        _DEP, _KEY, [{"symbol": "WETH", "balance": "2", "price_usd": "3500"}]
    )
    assert seeded == 1

    total = sum(
        remaining
        for _pk, token, remaining, _cost in store.iter_open_wallet_basis_lots()
        if token == "weth"
    )
    # 1 prior opening + 1 topped-up == 2 (no double-basis).
    assert total == Decimal("2")


def test_empty_zero_balance_rows_skipped() -> None:
    """(5) Empty≠Zero: missing / zero / unparseable balance is no seedable inventory."""
    store = FIFOBasisStore()
    seeded = store.seed_wallet_inventory(
        _DEP,
        _KEY,
        [
            {"symbol": "WETH", "balance": "0", "price_usd": "3000"},  # measured zero
            {"symbol": "USDC", "price_usd": "1"},  # missing balance
            {"symbol": "DAI", "balance": "not-a-number", "price_usd": "1"},  # unparseable
            {"symbol": "", "balance": "5", "price_usd": "1"},  # no symbol
        ],
    )
    assert seeded == 0
    assert list(store.iter_open_wallet_basis_lots()) == []


def test_seed_is_idempotent_across_repeat_calls() -> None:
    """Re-calling the seed on the same boot balance does not double the inventory.

    The runner calls reconstruct_lending_basis_store on every boot; the seed must
    be safe to re-run.
    """
    store = FIFOBasisStore()
    rows = [{"symbol": "WETH", "balance": "2", "price_usd": "3000"}]
    first = store.seed_wallet_inventory(_DEP, _KEY, rows)
    second = store.seed_wallet_inventory(_DEP, _KEY, rows)
    assert first == 1
    assert second == 0  # already covered by the first seed's open lot

    total = sum(
        remaining
        for _pk, token, remaining, _cost in store.iter_open_wallet_basis_lots()
        if token == "weth"
    )
    assert total == Decimal("2")
