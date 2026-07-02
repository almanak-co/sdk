"""Unit tests for the per-KNOWN-position live full-close builder (VIB-5465).

These assert the marker mapping: each KNOWN ``PositionInfo`` becomes a close
intent carrying a LIVE-resolution marker (repay_full / withdraw_all /
shares="all" / amount="all" / literal LP position_id), so the concrete on-chain
amount is resolved at EXECUTION, never frozen at plan-build. Plan A: driven by
the position set, never a wallet scan.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.intents import IntentType
from almanak.framework.teardown import (
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
    full_close_intents,
)

_VAULT = "0x" + "ab" * 20  # valid 0x-prefixed 40-hex-char address


def _pos(ptype: PositionType, *, protocol: str = "spark", position_id: str = "p", **details) -> PositionInfo:
    return PositionInfo(
        position_type=ptype,
        position_id=position_id,
        chain="ethereum",
        protocol=protocol,
        value_usd=Decimal("100"),
        details=details,
    )


def _by_type(intents):
    return {i.intent_type: i for i in intents}


def test_borrow_maps_to_repay_full_live_debt():
    """BORROW -> repay_full=True (MAX_UINT256 -> live debt+interest, ALM-2811)."""
    out = full_close_intents([_pos(PositionType.BORROW, protocol="aave_v3", asset="USDC")])
    assert len(out) == 1
    repay = out[0]
    assert repay.intent_type == IntentType.REPAY
    assert repay.repay_full is True
    assert repay.token == "USDC"
    assert repay.protocol == "aave_v3"


def test_supply_maps_to_withdraw_all_live_supply():
    """SUPPLY -> withdraw_all=True (MAX_UINT256 -> live supply incl. interest)."""
    out = full_close_intents([_pos(PositionType.SUPPLY, asset="DAI")])
    assert len(out) == 1
    wd = out[0]
    assert wd.intent_type == IntentType.WITHDRAW
    assert wd.withdraw_all is True
    assert wd.token == "DAI"


def test_vault_maps_to_redeem_all_shares():
    """VAULT -> vault_redeem(shares='all') (live share->asset)."""
    out = full_close_intents(
        [_pos(PositionType.VAULT, protocol="metamorpho", vault_address=_VAULT, asset="USDC")]
    )
    assert len(out) == 1
    vr = out[0]
    assert vr.intent_type == IntentType.VAULT_REDEEM
    assert vr.shares == "all"
    assert vr.vault_address.lower() == _VAULT.lower()


def test_lp_maps_to_lp_close_position_id_no_amount():
    """LP -> lp_close(position_id) — connector reads live liquidity, no baked amount."""
    out = full_close_intents([_pos(PositionType.LP, protocol="uniswap_v3", position_id="12345", pool="0xpool")])
    assert len(out) == 1
    close = out[0]
    assert close.intent_type == IntentType.LP_CLOSE
    assert close.position_id == "12345"
    # No chaining marker baked in — the literal id selects the position and the
    # connector compiler reads its live liquidity at close.
    assert close.amount is None


def test_token_and_stake_map_to_swap_all_live_balance():
    """STAKE / TOKEN -> swap(amount='all') of the KNOWN held token."""
    out = full_close_intents([_pos(PositionType.STAKE, asset="wstETH")], target_token="USDC")
    assert len(out) == 1
    swap = out[0]
    assert swap.intent_type == IntentType.SWAP
    assert swap.from_token == "wstETH"
    assert swap.to_token == "USDC"
    assert swap.amount == "all"


def test_perp_maps_to_full_close_when_direction_known():
    """PERP -> perp_close(size_usd=None) when direction is in details."""
    out = full_close_intents(
        [_pos(PositionType.PERP, protocol="gmx_v2", market="ETH/USD", collateral_token="USDC", is_long=True)]
    )
    assert len(out) == 1
    perp = out[0]
    assert perp.intent_type == IntentType.PERP_CLOSE
    assert perp.size_usd is None  # full close
    assert perp.is_long is True


def test_token_already_target_is_skipped():
    """A held token that already IS the target needs no swap."""
    out = full_close_intents([_pos(PositionType.TOKEN, asset="USDC")], target_token="USDC")
    assert out == []


def test_missing_details_skipped_not_fabricated():
    """Empty != Zero: a position lacking required details is skipped, not guessed."""
    # BORROW without an asset token, PERP without a direction.
    out = full_close_intents(
        [
            _pos(PositionType.BORROW, protocol="aave_v3"),
            _pos(PositionType.PERP, protocol="gmx_v2", market="ETH/USD", collateral_token="USDC"),
        ]
    )
    assert out == []


def test_prediction_and_cex_not_generically_closable():
    out = full_close_intents([_pos(PositionType.PREDICTION), _pos(PositionType.CEX)])
    assert out == []


def test_close_order_follows_position_priority():
    """Intents are ordered PERP -> BORROW -> SUPPLY -> VAULT -> LP -> STAKE/TOKEN."""
    positions = [
        _pos(PositionType.TOKEN, asset="WETH"),
        _pos(PositionType.LP, protocol="uniswap_v3", position_id="9", pool="0xp"),
        _pos(PositionType.SUPPLY, asset="DAI"),
        _pos(PositionType.BORROW, protocol="aave_v3", asset="USDC"),
        _pos(PositionType.VAULT, protocol="metamorpho", vault_address=_VAULT, asset="USDC"),
        _pos(PositionType.PERP, protocol="gmx_v2", market="ETH/USD", collateral_token="USDC", is_long=False),
    ]
    out = full_close_intents(positions)
    order = [i.intent_type for i in out]
    assert order == [
        IntentType.PERP_CLOSE,
        IntentType.REPAY,
        IntentType.WITHDRAW,
        IntentType.VAULT_REDEEM,
        IntentType.LP_CLOSE,
        IntentType.SWAP,
    ]


def test_accepts_teardown_position_summary():
    summary = TeardownPositionSummary(
        deployment_id="d",
        timestamp=datetime.now(UTC),
        positions=[_pos(PositionType.SUPPLY, asset="DAI")],
    )
    out = full_close_intents(summary)
    assert len(out) == 1
    assert out[0].intent_type == IntentType.WITHDRAW


def test_one_bad_position_does_not_abort_the_unwind():
    """A position that raises during build is skipped; the rest still close."""
    bad_vault = _pos(PositionType.VAULT, protocol="metamorpho", vault_address="0xnot_hex", asset="USDC")
    good_supply = _pos(PositionType.SUPPLY, asset="DAI")
    out = full_close_intents([bad_vault, good_supply])
    assert [i.intent_type for i in out] == [IntentType.WITHDRAW]


def test_none_positions_returns_empty():
    """Defensive: get_open_positions() returning None → nothing to close."""
    assert full_close_intents(None) == []


# ---------------------------------------------------------------------------
# Pendle PT (VIB-5590) — a pendle-protocol TOKEN position must build a
# ``protocol="pendle"`` SWAP so the generic close can route the PT.
# ---------------------------------------------------------------------------


def test_pendle_pt_token_builds_protocol_pendle_swap():
    """A pendle-protocol TOKEN position (a held PT) must yield a live-resolving
    ``SWAP(protocol='pendle', amount='all')`` — a generic protocol-less swap
    cannot route a PT (VIB-5590). Identity read from recognized keys."""
    pt = PositionInfo(
        position_type=PositionType.TOKEN,
        position_id="pendle_pt_0",
        chain="ethereum",
        protocol="pendle",
        value_usd=Decimal("10"),
        details={
            "asset_symbol": "PT-stETH-30DEC2027",
            "pt_token": "PT-stETH-30DEC2027",
            "market_id": "0x34280882267ffa6383B363E278B027Be083bBe3b",
            "base_token": "WSTETH",
            # Producer opts the held PT into protocol-routed close (VIB-5590).
            "protocol_routed_close": True,
        },
    )
    intents = full_close_intents(
        TeardownPositionSummary(deployment_id="dep", timestamp=datetime.now(UTC), positions=[pt]),
        target_token="WSTETH",
    )
    assert len(intents) == 1, f"expected one PT close swap, got {intents}"
    swap = intents[0]
    assert swap.intent_type == IntentType.SWAP
    assert str(swap.from_token).upper() == "PT-STETH-30DEC2027"
    assert str(swap.to_token).upper() == "WSTETH"
    assert swap.amount == "all"  # live-resolved at execution
    assert str(getattr(swap, "protocol", "")).lower() == "pendle", (
        "PT close swap MUST stamp protocol='pendle' to route through Pendle"
    )


def test_plain_token_close_swap_has_no_protocol_stamp():
    """A plain (non-pendle) held TOKEN still yields a protocol-less generic
    swap — the pendle stamp must NOT leak to unrelated token positions."""
    held = PositionInfo(
        position_type=PositionType.TOKEN,
        position_id="held-USDC",
        chain="ethereum",
        protocol="wallet",
        value_usd=Decimal("10"),
        details={"asset": "USDC"},
    )
    intents = full_close_intents(
        TeardownPositionSummary(deployment_id="dep", timestamp=datetime.now(UTC), positions=[held]),
        target_token="WSTETH",
    )
    assert len(intents) == 1
    assert not getattr(intents[0], "protocol", None)
