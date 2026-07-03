"""Unit tests for teardown completeness enforcement (VIB-5469 / TD-11).

Pure ``check_intent_coverage`` coverage matrix: every KNOWN open position must
have at least one closing intent targeting it, or it is reported uncovered.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.intents import Intent
from almanak.framework.teardown.completeness import (
    check_intent_coverage,
    resolve_consolidation_noop_target,
)
from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownAssetPolicy,
    TeardownPositionSummary,
)


def _summary(positions: list[PositionInfo]) -> TeardownPositionSummary:
    return TeardownPositionSummary(
        deployment_id="dep",
        timestamp=datetime.now(UTC),
        positions=positions,
    )


def _supply(token: str = "wstETH", chain: str = "ethereum") -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id=f"supply-{token}",
        chain=chain,
        protocol="spark",
        value_usd=Decimal("100"),
        details={"asset": token},
    )


def _borrow(token: str = "DAI", chain: str = "ethereum") -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.BORROW,
        position_id=f"borrow-{token}",
        chain=chain,
        protocol="spark",
        value_usd=Decimal("50"),
        details={"asset": token},
    )


# ---------------------------------------------------------------------------
# Lending (the ALM-2900 case)
# ---------------------------------------------------------------------------


def test_supply_and_borrow_fully_covered_is_complete():
    summary = _summary([_supply(), _borrow()])
    intents = [
        Intent.repay(protocol="spark", token="DAI", repay_full=True, chain="ethereum"),
        Intent.withdraw(protocol="spark", token="wstETH", amount=Decimal("0"), withdraw_all=True, chain="ethereum"),
    ]
    report = check_intent_coverage(summary, intents)
    assert report.complete
    assert report.uncovered == ()
    assert report.total_enforceable == 2


def test_registry_lending_position_covered_via_asset_symbol_VIB_5523():
    """A registry-sourced lending position stores its reserve under
    ``details['asset_symbol']`` (not ``details['asset']``). The completeness
    check must token-match it against its WITHDRAW/REPAY closing intent — else a
    restart-only registry enumeration falsely reports the leg uncovered
    (VIB-5523, Bug A)."""
    registry_supply = PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="wsteth",  # registry stores the market_id
        chain="arbitrum",
        protocol="aave_v3",
        value_usd=Decimal("0"),
        details={"source": "position_registry", "leg": "collateral", "market_id": "wsteth", "asset_symbol": "wstETH"},
    )
    registry_borrow = PositionInfo(
        position_type=PositionType.BORROW,
        position_id="usdc",
        chain="arbitrum",
        protocol="aave_v3",
        value_usd=Decimal("0"),
        details={"source": "position_registry", "leg": "debt", "market_id": "usdc", "asset_symbol": "USDC"},
    )
    intents = [
        Intent.repay(protocol="aave_v3", token="USDC", repay_full=True, chain="arbitrum"),
        Intent.withdraw(protocol="aave_v3", token="wstETH", amount=Decimal("0"), withdraw_all=True, chain="arbitrum"),
    ]
    report = check_intent_coverage(_summary([registry_supply, registry_borrow]), intents)
    assert report.complete
    assert report.uncovered == ()


def test_repay_without_withdraw_strands_collateral_ALM_2900():
    """Repay the borrow but never withdraw the collateral → SUPPLY uncovered."""
    summary = _summary([_supply(), _borrow()])
    intents = [Intent.repay(protocol="spark", token="DAI", repay_full=True, chain="ethereum")]
    report = check_intent_coverage(summary, intents)
    assert not report.complete
    assert len(report.uncovered) == 1
    assert report.uncovered[0].position_type == PositionType.SUPPLY
    assert "FAILED" in report.error_message()


def test_no_intents_with_open_positions_is_incomplete_VIB_5417():
    """spark teardown returned [] while positions are open → all uncovered."""
    summary = _summary([_supply(), _borrow()])
    report = check_intent_coverage(summary, [])
    assert not report.complete
    assert len(report.uncovered) == 2


def test_empty_positions_is_complete():
    assert check_intent_coverage(_summary([]), []).complete
    assert check_intent_coverage(None, []).complete


def test_supply_withdraw_must_match_token():
    """A withdraw of a DIFFERENT token does not cover the supply."""
    summary = _summary([_supply("wstETH")])
    intents = [
        Intent.withdraw(protocol="spark", token="USDC", amount=Decimal("0"), withdraw_all=True, chain="ethereum")
    ]
    assert not check_intent_coverage(summary, intents).complete


# ---------------------------------------------------------------------------
# Other position types
# ---------------------------------------------------------------------------


def test_lp_covered_by_position_id():
    lp = PositionInfo(
        position_type=PositionType.LP,
        position_id="12345",
        chain="base",
        protocol="uniswap_v3",
        value_usd=Decimal("10"),
        details={"pool": "0xpool"},
    )
    covered = Intent.lp_close(position_id="12345", protocol="uniswap_v3", chain="base")
    assert check_intent_coverage(_summary([lp]), [covered]).complete
    # An LP_CLOSE for a different token id does NOT cover it.
    other = Intent.lp_close(position_id="99999", protocol="uniswap_v3", chain="base")
    assert not check_intent_coverage(_summary([lp]), [other]).complete


def test_perp_covered_by_perp_close():
    perp = PositionInfo(
        position_type=PositionType.PERP,
        position_id="ETH/USD",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("500"),
        details={"market": "ETH/USD", "collateral_token": "USDC", "is_long": True},
    )
    close = Intent.perp_close(
        market="ETH/USD", collateral_token="USDC", is_long=True, protocol="gmx_v2", chain="arbitrum"
    )
    assert check_intent_coverage(_summary([perp]), [close]).complete
    assert not check_intent_coverage(_summary([perp]), []).complete


def test_perp_uncovered_when_market_differs():
    """A PERP_CLOSE for a DIFFERENT market does not cover the position."""
    perp = PositionInfo(
        position_type=PositionType.PERP,
        position_id="ETH/USD",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("500"),
        details={"market": "ETH/USD", "collateral_token": "USDC", "is_long": True},
    )
    other = Intent.perp_close(
        market="BTC/USD", collateral_token="USDC", is_long=True, protocol="gmx_v2", chain="arbitrum"
    )
    assert not check_intent_coverage(_summary([perp]), [other]).complete


_VAULT_ADDR = "0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB"
_OTHER_VAULT_ADDR = "0xCAFE01735c132Ada46AA9aA4c54623cAA92A64CB"


def _vault(vault_address: str = _VAULT_ADDR, *, with_details: bool = True) -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.VAULT,
        position_id=vault_address if not with_details else "vault-share-1",
        chain="ethereum",
        protocol="metamorpho",
        value_usd=Decimal("100"),
        details={"vault_address": vault_address} if with_details else {},
    )


def test_vault_covered_by_matching_vault_redeem():
    """A VAULT_REDEEM naming the same vault_address covers the position."""
    redeem = Intent.vault_redeem(protocol="metamorpho", vault_address=_VAULT_ADDR, shares="all", chain="ethereum")
    assert check_intent_coverage(_summary([_vault()]), [redeem]).complete
    assert not check_intent_coverage(_summary([_vault()]), []).complete


def test_vault_uncovered_when_address_differs():
    """A VAULT_REDEEM for a DIFFERENT vault does not cover the position."""
    redeem = Intent.vault_redeem(protocol="metamorpho", vault_address=_OTHER_VAULT_ADDR, shares="all", chain="ethereum")
    assert not check_intent_coverage(_summary([_vault()]), [redeem]).complete


def test_vault_covered_by_position_id_fallback():
    """When details carry no vault key, position_id is the identity used to match."""
    vault = _vault(with_details=False)  # position_id == vault address, no details
    redeem = Intent.vault_redeem(protocol="metamorpho", vault_address=_VAULT_ADDR, shares="all", chain="ethereum")
    assert check_intent_coverage(_summary([vault]), [redeem]).complete


def test_vault_lenient_when_intent_omits_address():
    """A dict VAULT_REDEEM with no vault_address is leniently accepted (Empty ≠ Zero)."""
    assert check_intent_coverage(_summary([_vault()]), [{"intent_type": "VAULT_REDEEM"}]).complete


def test_vault_uncovered_by_wrong_intent_type():
    """A non-VAULT_REDEEM intent never covers a VAULT position."""
    swap = Intent.swap(from_token="USDC", to_token="DAI", amount="all", chain="ethereum")
    assert not check_intent_coverage(_summary([_vault()]), [swap]).complete


# ---------------------------------------------------------------------------
# VIB-5573: a vault position a strategy reports as PositionType.TOKEN (e.g. the
# metamorpho_base_yield demo, for USD-pegged valuation simplicity) is still
# closed by a VAULT_REDEEM. The E2E real-fork proof caught this: without the
# _covers_token VAULT_REDEEM credit the completeness gate FAILs the whole
# teardown even though the redeem executed and closed the position on-chain.
# ---------------------------------------------------------------------------


def _token_typed_vault(vault_address: str = _VAULT_ADDR) -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.TOKEN,  # the metamorpho demo types it TOKEN
        position_id=f"metamorpho-base-{vault_address[:16]}",
        chain="base",
        protocol="metamorpho",
        value_usd=Decimal("50"),
        details={"vault_address": vault_address, "deposit_token": "USDC"},
    )


def test_token_typed_vault_covered_by_matching_vault_redeem():
    redeem = Intent.vault_redeem(protocol="metamorpho", vault_address=_VAULT_ADDR, shares="all", chain="base")
    assert check_intent_coverage(_summary([_token_typed_vault()]), [redeem]).complete
    assert not check_intent_coverage(_summary([_token_typed_vault()]), []).complete


def test_token_typed_vault_uncovered_when_address_differs():
    redeem = Intent.vault_redeem(protocol="metamorpho", vault_address=_OTHER_VAULT_ADDR, shares="all", chain="base")
    assert not check_intent_coverage(_summary([_token_typed_vault()]), [redeem]).complete


def test_plain_held_token_not_falsely_covered_by_vault_redeem():
    """Safety: a VAULT_REDEEM must NOT leniently cover an unrelated held TOKEN.

    A plain held token has no vault identity; its position_id is not a vault
    address, so the strict address match fails — no false coverage.
    """
    held = PositionInfo(
        position_type=PositionType.TOKEN,
        position_id="held-usdc",
        chain="base",
        protocol="",
        value_usd=Decimal("10"),
        details={"token": "USDC"},
    )
    redeem = Intent.vault_redeem(protocol="metamorpho", vault_address=_VAULT_ADDR, shares="all", chain="base")
    assert not check_intent_coverage(_summary([held]), [redeem]).complete


def test_stake_covered_by_unstake_or_swap():
    stake = PositionInfo(
        position_type=PositionType.STAKE,
        position_id="lido-wstETH",
        chain="ethereum",
        protocol="lido",
        value_usd=Decimal("100"),
        details={"asset": "wstETH"},
    )
    swap = Intent.swap(from_token="wstETH", to_token="USDC", amount="all", chain="ethereum")
    assert check_intent_coverage(_summary([stake]), [swap]).complete


def test_token_position_covered_by_swap():
    token = PositionInfo(
        position_type=PositionType.TOKEN,
        position_id="held-WETH",
        chain="base",
        protocol="wallet",
        value_usd=Decimal("18"),
        details={"asset": "WETH"},
    )
    swap = Intent.swap(from_token="WETH", to_token="USDC", amount="all", chain="base")
    assert check_intent_coverage(_summary([token]), [swap]).complete
    assert not check_intent_coverage(_summary([token]), []).complete


def test_prediction_and_cex_are_not_enforced():
    """Types with no generic close vocabulary must not produce false failures."""
    pred = PositionInfo(
        position_type=PositionType.PREDICTION,
        position_id="poly-1",
        chain="polygon",
        protocol="polymarket",
        value_usd=Decimal("10"),
    )
    cex = PositionInfo(
        position_type=PositionType.CEX,
        position_id="kraken-1",
        chain="",
        protocol="kraken",
        value_usd=Decimal("10"),
    )
    report = check_intent_coverage(_summary([pred, cex]), [])
    assert report.complete
    assert report.total_enforceable == 0


def test_chain_mismatch_is_not_coverage():
    """A same-symbol withdraw on a different chain does NOT cover the supply."""
    summary = _summary([_supply("wstETH", chain="ethereum")])
    intents = [
        Intent.withdraw(protocol="spark", token="wstETH", amount=Decimal("0"), withdraw_all=True, chain="arbitrum")
    ]
    assert not check_intent_coverage(summary, intents).complete


def test_intent_without_chain_still_covers_position():
    """An intent that omits chain (defaults to strategy primary) still covers."""
    summary = _summary([_supply("wstETH", chain="ethereum")])
    intents = [Intent.withdraw(protocol="spark", token="wstETH", amount=Decimal("0"), withdraw_all=True)]
    assert check_intent_coverage(summary, intents).complete


def test_dict_shaped_intents_are_supported():
    """Coverage must work on dict-shaped intents (Intent.to_dict / callers), not
    only BaseIntent objects — a dict intent must never false-fail an LP close."""
    lp = PositionInfo(
        position_type=PositionType.LP,
        position_id="123",
        chain="base",
        protocol="uniswap_v3",
        value_usd=Decimal("18"),
        details={"token0": "WETH", "token1": "USDC"},
    )
    # A bare LP_CLOSE dict (no position_id / pool) covers a single tracked LP.
    assert check_intent_coverage(_summary([lp]), [{"intent_type": "LP_CLOSE"}]).complete
    # A non-matching dict intent type does not.
    assert not check_intent_coverage(_summary([lp]), [{"type": "SWAP", "from_token": "DAI"}]).complete


# ---------------------------------------------------------------------------
# Address-keyed identity must mirror full_close (VIB-5469 audit — Codex / pr-auditor).
# The coverage check claims to read the SAME identity the close builder reads;
# build the actual close with full_close and assert coverage sees it, so the two
# can never drift back apart.
# ---------------------------------------------------------------------------


def test_address_keyed_token_position_is_covered_by_full_close_swap():
    """A TOKEN position denominated only by ``details['address']`` yields a valid
    SWAP from full_close — coverage MUST see that address or it false-fails a
    legitimate close (the no-intents gate would then loop the deployment)."""
    from almanak.framework.teardown.full_close import full_close_intents

    addr = "0x" + "a" * 40
    pos = PositionInfo(
        position_type=PositionType.TOKEN,
        position_id="tok",
        chain="base",
        protocol="wallet",
        value_usd=Decimal("1"),
        details={"address": addr},
    )
    intents = full_close_intents([pos], target_token="USDC")
    assert intents, "full_close should build a SWAP for an address-keyed held token"
    assert check_intent_coverage([pos], intents).complete


def test_address_keyed_vault_position_is_covered_by_full_close_redeem():
    """A VAULT position whose address lives in ``details['address']`` (and whose
    position_id is a logical name) yields a VAULT_REDEEM(vault_address=<addr>)
    from full_close — coverage MUST match on the address, not the logical id."""
    from almanak.framework.teardown.full_close import full_close_intents

    addr = "0x" + "1" * 40
    pos = PositionInfo(
        position_type=PositionType.VAULT,
        position_id="metamorpho_eth_yield",  # logical name, NOT the address
        chain="base",
        protocol="metamorpho",
        value_usd=Decimal("1"),
        details={"address": addr, "asset": "USDC"},
    )
    intents = full_close_intents([pos], target_token="USDC")
    assert intents, "full_close should build a VAULT_REDEEM for an address-keyed vault"
    assert check_intent_coverage([pos], intents).complete


# ---------------------------------------------------------------------------
# Identity precision (VIB-5469 / VIB-5494): token alone must not credit a close
# against a lending/perp leg it does not actually target. Every tightening is
# lenient-when-missing — an identity field is required ONLY when BOTH the
# position and the intent carry it, so under-specified hand-rolled intents keep
# today's behaviour while framework-built intents catch the real strand.
# ---------------------------------------------------------------------------


def test_same_token_different_protocol_lending_is_not_covered():
    """Aave-USDC + Morpho-USDC borrows need TWO repays; a single REPAY naming one
    protocol must not falsely cover the other (cross-protocol silent strand)."""
    aave = PositionInfo(
        position_type=PositionType.BORROW,
        position_id="aave-usdc",
        chain="ethereum",
        protocol="aave_v3",
        value_usd=Decimal("50"),
        details={"asset": "USDC"},
    )
    morpho = PositionInfo(
        position_type=PositionType.BORROW,
        position_id="morpho-usdc",
        chain="ethereum",
        protocol="morpho_blue",
        value_usd=Decimal("50"),
        details={"asset": "USDC"},
    )
    intents = [Intent.repay(protocol="aave_v3", token="USDC", repay_full=True, chain="ethereum")]
    report = check_intent_coverage(_summary([aave, morpho]), intents)
    assert not report.complete
    assert {p.protocol for p in report.uncovered} == {"morpho_blue"}


def test_same_token_different_market_lending_is_not_covered():
    """Two isolated Morpho markets on the SAME protocol/token are distinct: a
    WITHDRAW naming one ``market_id`` must not cover the other."""
    mkt_a = PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="morpho-a",
        chain="ethereum",
        protocol="morpho_blue",
        value_usd=Decimal("100"),
        details={"asset": "USDC", "market_id": "0xaaa"},
    )
    mkt_b = PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="morpho-b",
        chain="ethereum",
        protocol="morpho_blue",
        value_usd=Decimal("100"),
        details={"asset": "USDC", "market_id": "0xbbb"},
    )
    intents = [
        {"type": "WITHDRAW", "protocol": "morpho_blue", "token": "USDC", "market_id": "0xaaa"},
    ]
    report = check_intent_coverage(_summary([mkt_a, mkt_b]), intents)
    assert not report.complete
    assert {(p.details or {}).get("market_id") for p in report.uncovered} == {"0xbbb"}


def test_lending_coverage_stays_lenient_when_intent_omits_protocol():
    """A repay that omits protocol still covers (lenient on absence) — only an
    explicit protocol MISMATCH breaks coverage, never a missing one."""
    pos = _borrow("DAI")  # protocol="spark"
    intents = [{"type": "REPAY", "token": "DAI"}]  # no protocol on the dict intent
    assert check_intent_coverage(_summary([pos]), intents).complete


def test_long_and_short_same_market_each_need_a_close():
    """A long and a short ETH/USD on the same venue are distinct positions: one
    PERP_CLOSE(is_long=True) must not cover the short leg (VIB-5469)."""
    long_pos = PositionInfo(
        position_type=PositionType.PERP,
        position_id="eth-long",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("500"),
        direction="LONG",
        details={"market": "ETH/USD"},
    )
    short_pos = PositionInfo(
        position_type=PositionType.PERP,
        position_id="eth-short",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("500"),
        direction="SHORT",
        details={"market": "ETH/USD"},
    )
    # Only the LONG is closed → the SHORT is uncovered.
    close_long = Intent.perp_close(
        market="ETH/USD", collateral_token="USDC", is_long=True, protocol="gmx_v2", chain="arbitrum"
    )
    report = check_intent_coverage(_summary([long_pos, short_pos]), [close_long])
    assert not report.complete
    assert {p.position_id for p in report.uncovered} == {"eth-short"}
    # Closing BOTH sides covers both.
    close_short = Intent.perp_close(
        market="ETH/USD", collateral_token="USDC", is_long=False, protocol="gmx_v2", chain="arbitrum"
    )
    assert check_intent_coverage(_summary([long_pos, short_pos]), [close_long, close_short]).complete


def test_same_market_different_perp_protocol_is_not_covered():
    """The same market on two perp venues is two positions; a close on one venue
    must not cover the other (lenient-when-missing still catches explicit clash)."""
    gmx = PositionInfo(
        position_type=PositionType.PERP,
        position_id="gmx-eth",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("500"),
        direction="LONG",
        details={"market": "ETH/USD"},
    )
    hyperliquid = PositionInfo(
        position_type=PositionType.PERP,
        position_id="hl-eth",
        chain="arbitrum",
        protocol="hyperliquid",
        value_usd=Decimal("500"),
        direction="LONG",
        details={"market": "ETH/USD"},
    )
    close_gmx = Intent.perp_close(
        market="ETH/USD", collateral_token="USDC", is_long=True, protocol="gmx_v2", chain="arbitrum"
    )
    report = check_intent_coverage(_summary([gmx, hyperliquid]), [close_gmx])
    assert not report.complete
    assert {p.protocol for p in report.uncovered} == {"hyperliquid"}


def test_perp_coverage_stays_lenient_when_side_absent():
    """A perp position with no side stamped is covered by a same-market close
    regardless of the close's side — only an explicit side MISMATCH breaks it."""
    perp = PositionInfo(
        position_type=PositionType.PERP,
        position_id="eth-unknown-side",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("500"),
        details={"market": "ETH/USD"},  # no is_long / direction
    )
    close = Intent.perp_close(
        market="ETH/USD", collateral_token="USDC", is_long=False, protocol="gmx_v2", chain="arbitrum"
    )
    assert check_intent_coverage(_summary([perp]), [close]).complete


# ---------------------------------------------------------------------------
# Gemini PR review (VIB-5469 / TD-11): serialization-on-resume, extra closing
# vocab (CLAIM / BRIDGE / UNWRAP_NATIVE / DELEVERAGE), single-PositionInfo
# coercion, and None-intent filtering. Every addition is ADDITIVE — it can only
# recognize MORE closing intents (fewer false-FAILs), never strand a position.
# ---------------------------------------------------------------------------


def test_intent_type_normalizes_fully_qualified_enum_form_on_resume():
    """On resume, ``pending_intents_json`` may store the intent type as the
    enum's fully-qualified ``str`` form (``"IntentType.SWAP"``) instead of its
    bare ``.value``. Coverage must normalize both forms identically (HIGH)."""
    token = PositionInfo(
        position_type=PositionType.TOKEN,
        position_id="held-WETH",
        chain="base",
        protocol="wallet",
        value_usd=Decimal("18"),
        details={"asset": "WETH"},
    )
    # Bare value (fresh build) and fully-qualified (resumed) must both cover.
    bare = {"type": "SWAP", "from_token": "WETH"}
    resumed = {"intent_type": "IntentType.SWAP", "from_token": "WETH"}
    assert check_intent_coverage(_summary([token]), [bare]).complete
    assert check_intent_coverage(_summary([token]), [resumed]).complete


def test_stake_covered_by_ethena_claim_intent():
    """A cooldown-complete Ethena (sUSDe) teardown closes the STAKE with a CLAIM
    of the underlying (``Intent.claim(protocol="ethena", token="USDe")``). The
    completeness check must count CLAIM as coverage or it false-FAILs (HIGH)."""
    stake = PositionInfo(
        position_type=PositionType.STAKE,
        position_id="ethena-sUSDe",
        chain="ethereum",
        protocol="ethena",
        value_usd=Decimal("100"),
        details={"staked_token": "sUSDe", "token": "USDe"},
    )
    claim = {"intent_type": "CLAIM", "protocol": "ethena", "token": "USDe"}
    assert check_intent_coverage(_summary([stake]), [claim]).complete
    # A CLAIM of an unrelated token does NOT cover it (additive, not blanket).
    other = {"intent_type": "CLAIM", "protocol": "ethena", "token": "USDC"}
    assert not check_intent_coverage(_summary([stake]), [other]).complete


def test_single_position_info_is_coerced_not_dropped():
    """A lone ``PositionInfo`` (not wrapped in a list/summary) must still be
    enforced, not silently ignored by the coercion helper (MED)."""
    stake = PositionInfo(
        position_type=PositionType.STAKE,
        position_id="lido-wstETH",
        chain="ethereum",
        protocol="lido",
        value_usd=Decimal("100"),
        details={"asset": "wstETH"},
    )
    # No closing intent → the single position must be reported uncovered.
    report = check_intent_coverage(stake, [])
    assert not report.complete
    assert report.total_enforceable == 1
    assert report.uncovered[0].position_id == "lido-wstETH"
    # And a matching swap covers it.
    swap = Intent.swap(from_token="wstETH", to_token="USDC", amount="all", chain="ethereum")
    assert check_intent_coverage(stake, [swap]).complete


def test_token_position_covered_by_bridge():
    """A held TOKEN closed by bridging it to another chain emits BRIDGE(token=…);
    coverage must recognize BRIDGE or it false-FAILs the position (MED)."""
    token = PositionInfo(
        position_type=PositionType.TOKEN,
        position_id="held-USDC",
        chain="base",
        protocol="wallet",
        value_usd=Decimal("100"),
        details={"asset": "USDC"},
    )
    bridge = Intent.bridge(token="USDC", amount="all", from_chain="base", to_chain="arbitrum")
    assert check_intent_coverage(_summary([token]), [bridge]).complete


def test_token_position_covered_by_unwrap_native():
    """A wrapped-native held token (WETH) closed by unwrapping to ETH emits
    UNWRAP_NATIVE(token="WETH"); coverage must recognize it (MED, additive)."""
    token = PositionInfo(
        position_type=PositionType.TOKEN,
        position_id="held-WETH",
        chain="arbitrum",
        protocol="wallet",
        value_usd=Decimal("18"),
        details={"asset": "WETH"},
    )
    unwrap = Intent.unwrap(token="WETH", amount="all", chain="arbitrum")
    assert check_intent_coverage(_summary([token]), [unwrap]).complete


def test_borrow_covered_by_deleverage():
    """A BORROW closed by an emergency DELEVERAGE (structurally a repay) must
    count as covered, not false-FAIL the unwind (additive)."""
    borrow = _borrow("DAI")  # protocol="spark"
    deleverage = {"intent_type": "DELEVERAGE", "protocol": "spark", "token": "DAI"}
    assert check_intent_coverage(_summary([borrow]), [deleverage]).complete


def test_none_intents_are_filtered_not_raised():
    """A ``None`` in the intents iterable must be filtered, never raise inside
    ``_covers`` (which would crash the gate instead of reporting coverage) (MED)."""
    summary = _summary([_supply(), _borrow()])
    intents = [
        None,
        Intent.repay(protocol="spark", token="DAI", repay_full=True, chain="ethereum"),
        None,
        Intent.withdraw(protocol="spark", token="wstETH", amount=Decimal("0"), withdraw_all=True, chain="ethereum"),
    ]
    report = check_intent_coverage(summary, intents)
    assert report.complete
    assert report.uncovered == ()


# ---------------------------------------------------------------------------
# Pendle PT (VIB-5590) — a PT tracked as a generic TOKEN closed by a
# ``protocol="pendle"`` SWAP must be credited as covered.
# ---------------------------------------------------------------------------


def test_pendle_pt_uncovered_despite_correct_swap_VIB_5590():
    """A PT TOKEN position whose identity is stored under ``details['pt_token']``
    must be credited by its legitimate ``protocol='pendle'`` SWAP.

    RED baseline pre-fix: ``pt_token`` was not a recognized token-detail key, so
    the SWAP did not match and the position was falsely 'uncovered'. Post-fix
    (``pt_token``/``pt_symbol`` recognized) this is COVERED.
    """
    pt = PositionInfo(
        position_type=PositionType.TOKEN,
        position_id="pendle_pt_0",
        chain="ethereum",
        protocol="pendle",
        value_usd=Decimal("10"),
        details={
            "market": "PT-stETH-30DEC2027",
            "pt_token": "PT-stETH-30DEC2027",
            "base_token": "WSTETH",
        },
    )
    swap = Intent.swap(
        from_token="PT-stETH-30DEC2027", to_token="WSTETH", amount="all", protocol="pendle"
    )
    report = check_intent_coverage(_summary([pt]), [swap])
    assert report.complete, f"PT SWAP should cover the PT TOKEN position; uncovered={report.uncovered}"


def test_pendle_pt_covered_via_asset_symbol_key_VIB_5590():
    """Producers aligned on recognized keys (``asset_symbol`` + ``market_id``)
    are credited by the ``protocol='pendle'`` SWAP (the demo's teardown shape)."""
    pt = PositionInfo(
        position_type=PositionType.TOKEN,
        position_id="pendle_pt_0",
        chain="ethereum",
        protocol="pendle",
        value_usd=Decimal("10"),
        details={
            "asset_symbol": "PT-stETH-30DEC2027",
            "pt_token": "PT-stETH-30DEC2027",
            "pt_symbol": "PT-stETH-30DEC2027",
            "market_id": "0x34280882267ffa6383B363E278B027Be083bBe3b",
            "base_token": "WSTETH",
        },
    )
    swap = Intent.swap(
        from_token="PT-stETH-30DEC2027",
        to_token="WSTETH",
        amount="all",
        protocol="pendle",
        chain="ethereum",
    )
    assert check_intent_coverage(_summary([pt]), [swap]).complete


def test_non_pt_token_not_false_matched_by_pt_swap_VIB_5590():
    """Additive/lenient guard: a DIFFERENT held TOKEN is NOT covered by a PT
    swap — recognizing ``pt_token`` must not let unrelated positions match."""
    other = PositionInfo(
        position_type=PositionType.TOKEN,
        position_id="held-USDC",
        chain="ethereum",
        protocol="wallet",
        value_usd=Decimal("10"),
        details={"asset": "USDC"},
    )
    pt_swap = Intent.swap(
        from_token="PT-stETH-30DEC2027", to_token="WSTETH", amount="all", protocol="pendle"
    )
    assert not check_intent_coverage(_summary([other]), [pt_swap]).complete


# ---------------------------------------------------------------------------
# VIB-5494 Item 1 — target-token no-op: full_close emits NO intent for a held
# TOKEN/STAKE position whose token already equals the consolidation target
# (nothing to swap). The coverage gate must credit that no-op close when the
# target is threaded, instead of false-failing and looping the deployment on the
# no-intents gate. Fail-SAFE: only credited when held token == final target AND
# the position is a plain held/staked token (no vault/lending identity).
# ---------------------------------------------------------------------------


def _held_token(token: str, *, chain: str = "base", details: dict | None = None) -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.TOKEN,
        position_id=f"held-{token}",
        chain=chain,
        protocol="wallet",
        value_usd=Decimal("100"),
        details=details if details is not None else {"asset": token},
    )


def test_token_already_in_target_is_covered_no_op_VIB_5494():
    """A held USDC TOKEN with target USDC needs no closing intent — full_close
    no-ops it, and the wallet already holds exactly the target. With the target
    threaded it is COVERED even with an empty intent list (no false FAILED)."""
    pos = _held_token("USDC")
    # Without the target threaded, it (correctly, pre-fix) reports uncovered.
    assert not check_intent_coverage(_summary([pos]), []).complete
    # With the consolidation target threaded, the no-op is credited.
    report = check_intent_coverage(_summary([pos]), [], consolidation_target_token="USDC")
    assert report.complete, f"held target-token position must be a no-op close; uncovered={report.uncovered}"
    # It still counts toward the enforceable denominator (satisfied, not absent).
    assert report.total_enforceable == 1


def test_stake_already_in_target_is_covered_no_op_VIB_5494():
    """A STAKE position denominated in the target (e.g. hold wstETH, target
    wstETH) is likewise a no-op close."""
    stake = PositionInfo(
        position_type=PositionType.STAKE,
        position_id="lido-wstETH",
        chain="ethereum",
        protocol="lido",
        value_usd=Decimal("100"),
        details={"asset": "wstETH"},
    )
    assert check_intent_coverage(_summary([stake]), [], consolidation_target_token="wstETH").complete
    # Case-insensitive match.
    assert check_intent_coverage(_summary([stake]), [], consolidation_target_token="WSTETH").complete


def test_token_not_in_target_still_requires_a_close_VIB_5494():
    """The no-op credit must NOT leak: a held WETH TOKEN with target USDC is a
    real swap-close and stays uncovered when no intent targets it."""
    pos = _held_token("WETH")
    assert not check_intent_coverage(_summary([pos]), [], consolidation_target_token="USDC").complete


def test_collateral_leg_typed_token_in_target_is_NOT_no_op_VIB_5494():
    """False-NEGATIVE guard: a lending collateral leg reported as TOKEN whose
    asset equals the target is STILL on-chain in the protocol after teardown —
    the wallet does NOT hold the target — so it must NOT be credited a no-op.
    A position carrying a lending/market identity is never no-op'd."""
    # market_id marks it as a lending leg, not a plain held token.
    leg = _held_token("USDC", details={"asset": "USDC", "market_id": "0xmkt", "leg": "collateral"})
    report = check_intent_coverage(_summary([leg]), [], consolidation_target_token="USDC")
    assert not report.complete, "a collateral-typed TOKEN in target must NOT be silently credited a no-op"


def test_token_typed_vault_in_target_is_NOT_no_op_VIB_5494():
    """A vault share reported as TOKEN (metamorpho demo) whose deposit token is
    the target must NOT be no-op'd — it needs a VAULT_REDEEM, and its shares are
    still on-chain until then."""
    vault_token = PositionInfo(
        position_type=PositionType.TOKEN,
        position_id="metamorpho-base",
        chain="base",
        protocol="metamorpho",
        value_usd=Decimal("50"),
        details={"vault_address": _VAULT_ADDR, "deposit_token": "USDC"},
    )
    assert not check_intent_coverage(_summary([vault_token]), [], consolidation_target_token="USDC").complete


def test_noop_target_mirrors_full_close_skip_VIB_5494():
    """Drift-proof: full_close emits NO intent for a held target-token position,
    and the coverage gate (with the same target threaded) credits that same
    no-op — so builder and checker can never drift apart."""
    from almanak.framework.teardown.full_close import full_close_intents

    pos = _held_token("USDC")
    built = full_close_intents([pos], target_token="USDC")
    assert built == [], "full_close should emit NO intent for a held token already in the target"
    assert check_intent_coverage([pos], built, consolidation_target_token="USDC").complete


def test_resolve_consolidation_noop_target_policy_gating_VIB_5494():
    """The no-op target is only defined for the TARGET_TOKEN policy; entry-token
    and keep-outputs have no single 'already done' token → None (strict gate)."""
    assert resolve_consolidation_noop_target(TeardownAssetPolicy.TARGET_TOKEN, "USDC") == "USDC"
    assert resolve_consolidation_noop_target("target_token", None) == "USDC"  # default
    assert resolve_consolidation_noop_target(TeardownAssetPolicy.ENTRY_TOKEN, "USDC") is None
    assert resolve_consolidation_noop_target(TeardownAssetPolicy.KEEP_OUTPUTS, "USDC") is None
    assert resolve_consolidation_noop_target(None, "USDC") is None


def test_entry_token_policy_keeps_strict_behaviour_VIB_5494():
    """Under entry-token policy the resolver returns None, so a held target-token
    position is NOT credited a no-op (it may still need swapping to the entry
    asset) — the gate stays strict/fail-safe."""
    pos = _held_token("USDC")
    noop = resolve_consolidation_noop_target(TeardownAssetPolicy.ENTRY_TOKEN, "USDC")
    assert not check_intent_coverage(_summary([pos]), [], consolidation_target_token=noop).complete


# ---------------------------------------------------------------------------
# VIB-5494 Item 2 — multi-position disambiguation: when ≥2 positions of a
# disambiguation-requiring type exist, a single under-specified intent must not
# blanket-cover several distinct positions via a lenient default. Framework-built
# closes always stamp id/market/protocol, so they stay covered.
# ---------------------------------------------------------------------------


def _lp(position_id: str, pool: str, *, chain: str = "base") -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=position_id,
        chain=chain,
        protocol="uniswap_v3",
        value_usd=Decimal("100"),
        details={"pool": pool},
    )


def test_two_lps_bare_lp_close_no_longer_blanket_covers_VIB_5494():
    """Two distinct LPs + ONE id-less/pool-less LP_CLOSE: pre-fix the bare close
    leniently 'covered' BOTH (silently passing the second); post-fix a single
    under-specified intent cannot disambiguate two LPs → both uncovered."""
    lps = [_lp("111", "0xAAA"), _lp("222", "0xBBB")]
    bare = {"intent_type": "LP_CLOSE"}  # no position_id, no pool
    report = check_intent_coverage(_summary(lps), [bare])
    assert not report.complete
    assert {p.position_id for p in report.uncovered} == {"111", "222"}


def test_two_lps_same_pool_pool_scoped_close_requires_id_VIB_5494():
    """Two LP NFTs in the SAME pool + a pool-scoped (id-less) LP_CLOSE: the pool
    match is lenient (cannot say WHICH NFT), so with two positions it no longer
    blanket-covers — id disambiguation is required."""
    lps = [_lp("111", "0xAAA"), _lp("222", "0xAAA")]  # same pool, distinct NFTs
    pool_close = {"intent_type": "LP_CLOSE", "pool": "0xAAA"}  # no position_id
    report = check_intent_coverage(_summary(lps), [pool_close])
    assert not report.complete
    assert {p.position_id for p in report.uncovered} == {"111", "222"}


def test_two_lps_id_scoped_closes_are_covered_VIB_5494():
    """Framework-built (id-scoped) closes for BOTH LPs still pass — the tightening
    only rejects under-specified intents, never disambiguated ones."""
    lps = [_lp("111", "0xAAA"), _lp("222", "0xAAA")]
    closes = [
        Intent.lp_close(position_id="111", protocol="uniswap_v3", chain="base"),
        Intent.lp_close(position_id="222", protocol="uniswap_v3", chain="base"),
    ]
    assert check_intent_coverage(_summary(lps), closes).complete


def test_two_lps_framework_full_close_no_false_positive_VIB_5494():
    """The actual framework builder (full_close_intents) over two distinct LPs
    produces id-scoped closes → the multi-position guard must NOT false-fail."""
    from almanak.framework.teardown.full_close import full_close_intents

    lps = [_lp("111", "0xAAA"), _lp("222", "0xBBB")]
    built = full_close_intents(lps)
    assert check_intent_coverage(lps, built).complete


def test_single_lp_bare_close_still_lenient_VIB_5494():
    """The tightening is scoped to ≥2 same-type positions: a single tracked LP is
    still leniently covered by a bare LP_CLOSE (today's behaviour preserved)."""
    lp = _lp("111", "0xAAA")
    assert check_intent_coverage(_summary([lp]), [{"intent_type": "LP_CLOSE"}]).complete


def test_two_perps_market_less_close_no_longer_blanket_covers_VIB_5494():
    """Two distinct-market perps + ONE market-less PERP_CLOSE: pre-fix it
    leniently covered both; post-fix a market-less close cannot disambiguate two
    perps → both uncovered."""
    perps = [
        PositionInfo(
            position_type=PositionType.PERP,
            position_id="eth",
            chain="arbitrum",
            protocol="gmx_v2",
            value_usd=Decimal("500"),
            details={"market": "ETH/USD"},
        ),
        PositionInfo(
            position_type=PositionType.PERP,
            position_id="btc",
            chain="arbitrum",
            protocol="gmx_v2",
            value_usd=Decimal("500"),
            details={"market": "BTC/USD"},
        ),
    ]
    market_less = {"intent_type": "PERP_CLOSE"}  # no market
    report = check_intent_coverage(_summary(perps), [market_less])
    assert not report.complete
    assert {p.position_id for p in report.uncovered} == {"eth", "btc"}


def test_two_perps_market_scoped_closes_are_covered_VIB_5494():
    """Market-scoped closes for both perps still pass (framework stamps market)."""
    perps = [
        PositionInfo(
            position_type=PositionType.PERP,
            position_id="eth",
            chain="arbitrum",
            protocol="gmx_v2",
            value_usd=Decimal("500"),
            details={"market": "ETH/USD", "collateral_token": "USDC", "is_long": True},
        ),
        PositionInfo(
            position_type=PositionType.PERP,
            position_id="btc",
            chain="arbitrum",
            protocol="gmx_v2",
            value_usd=Decimal("500"),
            details={"market": "BTC/USD", "collateral_token": "USDC", "is_long": True},
        ),
    ]
    closes = [
        Intent.perp_close(market="ETH/USD", collateral_token="USDC", is_long=True, protocol="gmx_v2", chain="arbitrum"),
        Intent.perp_close(market="BTC/USD", collateral_token="USDC", is_long=True, protocol="gmx_v2", chain="arbitrum"),
    ]
    assert check_intent_coverage(_summary(perps), closes).complete


def test_two_supplies_protocol_less_repay_no_longer_blanket_covers_VIB_5494():
    """Two same-token supplies on DIFFERENT protocols + a protocol-less WITHDRAW:
    the withdraw omits the disambiguating protocol, so with two legs it cannot
    blanket-cover — each leg needs a protocol-scoped close."""
    supplies = [
        PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="aave-usdc",
            chain="ethereum",
            protocol="aave_v3",
            value_usd=Decimal("100"),
            details={"asset": "USDC"},
        ),
        PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="morpho-usdc",
            chain="ethereum",
            protocol="morpho_blue",
            value_usd=Decimal("100"),
            details={"asset": "USDC"},
        ),
    ]
    # A withdraw naming the token but NOT the protocol leniently covers both today.
    protocol_less = {"intent_type": "WITHDRAW", "token": "USDC"}
    report = check_intent_coverage(_summary(supplies), [protocol_less])
    assert not report.complete
    assert {p.position_id for p in report.uncovered} == {"aave-usdc", "morpho-usdc"}
    # Protocol-scoped withdraws for BOTH legs pass (framework stamps protocol).
    scoped = [
        {"intent_type": "WITHDRAW", "token": "USDC", "protocol": "aave_v3"},
        {"intent_type": "WITHDRAW", "token": "USDC", "protocol": "morpho_blue"},
    ]
    assert check_intent_coverage(_summary(supplies), scoped).complete


# ---------------------------------------------------------------------------
# VIB-5494 Empty≠Zero (Gemini review): a MEASURED integer id/market of 0 must be
# treated as PRESENT, never coalesced to "absent". `str(X or "")` conflated a
# valid 0 (ERC-721 token id / sequential-index lending market) with None, which
# false-FAILED id-0 LPs and false-COVERED market-0 lending legs.
# ---------------------------------------------------------------------------


def test_lp_position_id_zero_is_a_measured_identity_VIB_5494():
    """position_id 0 is a valid ERC-721 token id. An id-scoped LP_CLOSE(0) must
    match the id-0 LP (not be coerced to 'absent'), and disambiguation still holds
    across two positions — the id-0 leg is covered, the id-1 leg is not."""
    lps = [
        PositionInfo(
            position_type=PositionType.LP,
            position_id=0,  # measured token id 0
            chain="base",
            protocol="uniswap_v3",
            value_usd=Decimal("100"),
            details={"pool": "0xAAA"},
        ),
        PositionInfo(
            position_type=PositionType.LP,
            position_id=1,
            chain="base",
            protocol="uniswap_v3",
            value_usd=Decimal("100"),
            details={"pool": "0xAAA"},
        ),
    ]
    close_zero = {"intent_type": "LP_CLOSE", "position_id": 0}
    report = check_intent_coverage(_summary(lps), [close_zero])
    # The id-0 LP is matched (no false-FAIL); the id-1 LP is genuinely uncovered.
    assert {str(p.position_id) for p in report.uncovered} == {"1"}


def test_two_lending_market_zero_not_blanket_covered_by_market_less_VIB_5494():
    """market_id 0 is a measured isolated-market identity. Two same-protocol legs
    (market_id 0 and 1) must NOT both be covered by a market-less WITHDRAW — the
    market-0 leg must not be mistaken for 'market-less' (false-positive guard)."""
    legs = [
        PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="mkt0",
            chain="ethereum",
            protocol="morpho_blue",
            value_usd=Decimal("100"),
            details={"asset": "USDC", "market_id": 0},
        ),
        PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="mkt1",
            chain="ethereum",
            protocol="morpho_blue",
            value_usd=Decimal("100"),
            details={"asset": "USDC", "market_id": 1},
        ),
    ]
    market_less = {"intent_type": "WITHDRAW", "token": "USDC", "protocol": "morpho_blue"}
    report = check_intent_coverage(_summary(legs), [market_less])
    assert not report.complete
    assert {p.position_id for p in report.uncovered} == {"mkt0", "mkt1"}
    # A market_id=0 WITHDRAW covers ONLY the market-0 leg (measured 0 matches).
    scoped_zero = {"intent_type": "WITHDRAW", "token": "USDC", "protocol": "morpho_blue", "market_id": 0}
    report2 = check_intent_coverage(_summary(legs), [scoped_zero])
    assert {p.position_id for p in report2.uncovered} == {"mkt1"}


def test_genuinely_market_less_lending_still_lenient_VIB_5494():
    """A lending leg with NO market_id/market (both absent) keeps the lenient
    protocol-only path — the Empty≠Zero fix must not change the absent case."""
    legs = [
        PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="aave-a",
            chain="ethereum",
            protocol="aave_v3",
            value_usd=Decimal("100"),
            details={"asset": "USDC"},  # no market_id/market → market-less
        ),
        PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id="aave-b",
            chain="ethereum",
            protocol="aave_v3",
            value_usd=Decimal("100"),
            details={"asset": "DAI"},  # different token → own withdraw
        ),
    ]
    # A protocol-scoped USDC withdraw (no market) still covers the market-less
    # USDC leg via the lenient protocol-only path; the DAI leg needs its own.
    intents = [
        {"intent_type": "WITHDRAW", "token": "USDC", "protocol": "aave_v3"},
        {"intent_type": "WITHDRAW", "token": "DAI", "protocol": "aave_v3"},
    ]
    assert check_intent_coverage(_summary(legs), intents).complete
