"""Reproduction tests for VIB-4838 — PortfolioValuer position dedup gap.

These tests pin the **buggy** behaviour on ``origin/main`` so the fix can be
verified by flipping the asserted expectation. Each test is explicitly
labelled "documents BUG VIB-4838" and carries the exact line to flip once the
canonical-key / degenerate-stub-guard fix lands.

Root cause (see ``docs/internal/VIB-4838-portfolio-valuer-dedup-investigation.md``):
``PortfolioValuer._get_positions`` dedupes the strategy-reported positions and
the framework-discovered positions by **raw ``position_id`` string**. Lending
uses two different id schemes:

    discovery : ``aave-supply-{symbol}-{chain}`` / ``aave-borrow-{symbol}-{chain}``
    strategy  : author-chosen, e.g. ``aave-wbtc-collateral``

When the strategy author does not reproduce discovery's exact internal id, the
two never collide, both survive, and the strategy's degenerate stub
(``value_usd=0``, no asset/wallet) is flagged ``no_path`` → snapshot confidence
is forced to ``UNAVAILABLE`` even though discovery valued the real position.

Three cases are reproduced:

* ``Case 1`` — confidence poisoning: phantom zero-value stub survives, snapshot
  wrongly ``UNAVAILABLE`` (expected ``HIGH``).
* ``Case 2`` — latent NAV double-count: strategy stub carries ``value_usd>0``
  matching the discovery supply → the same on-chain Aave supply is counted
  twice.
* ``Case 3`` — the §3 dual-warning fallback chain end-to-end: valuer
  ``UNAVAILABLE`` is *discarded* by ``runner_state.capture_portfolio_snapshot``
  and replaced by the strategy fallback (``HIGH``), which the
  ``gateway_state_manager`` zero-basis guard then degrades to ``ESTIMATED``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.portfolio import PortfolioSnapshot, ValueConfidence
from almanak.framework.runner.runner_state import capture_portfolio_snapshot
from almanak.framework.state.gateway_state_manager import _apply_synth_position_guard
from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
)
from almanak.framework.valuation.lending_position_reader import LendingPositionOnChain
from almanak.framework.valuation.portfolio_valuer import (
    PortfolioValuer,
    _normalize_protocol_for_dedup,
)
from almanak.framework.valuation.position_discovery import DiscoveryResult

# Mirrors the production run in the brief: WBTC collateral + USDC debt on
# Arbitrum, wallet 0xd06464...
WALLET = "0xd0646436f4d3e8b8e0a0e0a0e0a0e0a0e0a0e0a0"
WBTC_ADDR = "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f"  # Arbitrum WBTC
USDC_ADDR = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"  # Arbitrum USDC
CHAIN = "arbitrum"

# Raw on-chain balances that reprice to the brief's values.
#   WBTC: 8 decimals; 4129 raw = 0.00004129 WBTC * $63,300 ≈ $2.6136
#   USDC: 6 decimals; 930000 raw = 0.93 USDC * $1 = $0.93 (borrow → -$0.93)
WBTC_ATOKEN_RAW = 4129
USDC_DEBT_RAW = 930_000
WBTC_PRICE = Decimal("63300")
USDC_PRICE = Decimal("1")
# Computed expected reprice values
EXPECTED_SUPPLY_USD = Decimal(WBTC_ATOKEN_RAW) / Decimal(10**8) * WBTC_PRICE  # ≈ 2.6136
EXPECTED_BORROW_USD = -(Decimal(USDC_DEBT_RAW) / Decimal(10**6) * USDC_PRICE)  # -0.93


# ---------------------------------------------------------------------------
# Fakes / helpers
# ---------------------------------------------------------------------------


class _FakeResolved:
    def __init__(self, symbol: str, address: str):
        self.symbol = symbol
        self.address = address


class _FakeTokenResolver:
    """Maps WBTC/USDC addresses<->symbols and supplies decimals.

    Used to satisfy ``_resolve_token_symbol`` (address→symbol),
    ``_reprice_lending_on_chain_enriched`` (symbol→address) and
    ``_get_token_decimals``.
    """

    _BY_ADDR = {WBTC_ADDR.lower(): ("WBTC", 8), USDC_ADDR.lower(): ("USDC", 6)}
    _BY_SYMBOL = {"WBTC": (WBTC_ADDR, 8), "USDC": (USDC_ADDR, 6)}

    def resolve(self, key: str, chain: str):  # noqa: ARG002
        if key is None:
            return None
        low = key.lower()
        if low in self._BY_ADDR:
            sym, _ = self._BY_ADDR[low]
            return _FakeResolved(sym, key)
        if key in self._BY_SYMBOL:
            addr, _ = self._BY_SYMBOL[key]
            return _FakeResolved(key, addr)
        return None

    def get_decimals(self, chain: str, symbol: str):  # noqa: ARG002
        if symbol in self._BY_SYMBOL:
            return self._BY_SYMBOL[symbol][1]
        raise ValueError(f"unknown decimals for {symbol}")


def _make_market():
    """Market for an empty wallet: every tracked/native token is measured-zero.

    Returns ``balance == 0`` (NOT a raise) so ``wallet_data_incomplete`` stays
    False and the snapshot can reach HIGH once positions are valued — i.e. the
    only thing that can degrade confidence in these repros is the dedup defect
    itself, not a fixture artefact. ETH is priced so the Arbitrum native-gas
    row resolves cleanly.
    """
    market = MagicMock()
    price_map = {"WBTC": WBTC_PRICE, "USDC": USDC_PRICE, "ETH": Decimal("3500")}

    def _price(token, **_kw):
        if token in price_map:
            return price_map[token]
        raise ValueError(f"no price for {token}")

    def _balance(token):  # empty wallet → measured zero
        result = MagicMock()
        result.balance = Decimal("0")
        return result

    market.price = _price
    market.balance = _balance
    return market


def _make_strategy(positions, *, tracked_tokens=None):
    strategy = MagicMock()
    strategy.deployment_id = "a7d51afd-9c5d-4eed-85f9-2abf71f5576b"
    strategy.chain = CHAIN
    strategy.wallet_address = WALLET
    strategy._get_tracked_tokens.return_value = tracked_tokens or ["WBTC", "USDC"]
    metadata = MagicMock()
    metadata.supported_protocols = ["aave_v3"]
    strategy.STRATEGY_METADATA = metadata
    strategy.get_open_positions.return_value = TeardownPositionSummary(
        deployment_id=strategy.deployment_id,
        timestamp=datetime.now(UTC),
        positions=positions,
    )
    return strategy


def _discovery_supply_and_borrow():
    """The two real positions discovery returns for this loop."""
    return DiscoveryResult(
        positions=[
            PositionInfo(
                position_type=PositionType.SUPPLY,
                position_id=f"aave-supply-WBTC-{CHAIN}",
                chain=CHAIN,
                protocol="aave_v3",
                value_usd=Decimal("0"),
                details={"asset": "WBTC", "asset_address": WBTC_ADDR, "wallet_address": WALLET},
            ),
            PositionInfo(
                position_type=PositionType.BORROW,
                position_id=f"aave-borrow-USDC-{CHAIN}",
                chain=CHAIN,
                protocol="aave_v3",
                value_usd=Decimal("0"),
                details={"asset": "USDC", "asset_address": USDC_ADDR, "wallet_address": WALLET},
            ),
        ],
        lending_assets_scanned=2,
    )


def _fake_read_position(*, chain, asset_address, wallet_address, protocol=None):  # noqa: ARG001
    """Return on-chain lending state keyed by asset address."""
    low = asset_address.lower()
    if low == WBTC_ADDR.lower():
        return LendingPositionOnChain(
            asset_address=WBTC_ADDR,
            current_atoken_balance=WBTC_ATOKEN_RAW,
            current_stable_debt=0,
            current_variable_debt=0,
            liquidity_rate=0,
            usage_as_collateral_enabled=True,
        )
    if low == USDC_ADDR.lower():
        return LendingPositionOnChain(
            asset_address=USDC_ADDR,
            current_atoken_balance=0,
            current_stable_debt=0,
            current_variable_debt=USDC_DEBT_RAW,
            liquidity_rate=0,
            usage_as_collateral_enabled=False,
        )
    return None


def _drive_get_positions(valuer: PortfolioValuer, strategy, market):
    """Run ``_get_positions`` with discovery + lending-reader + resolver faked."""
    with (
        patch.object(valuer._discovery, "discover", return_value=_discovery_supply_and_borrow()),
        patch.object(valuer._lending_reader, "read_position", side_effect=_fake_read_position),
        patch(
            "almanak.framework.data.tokens.get_token_resolver",
            return_value=_FakeTokenResolver(),
        ),
    ):
        return valuer._get_positions(strategy, market, {})


# ---------------------------------------------------------------------------
# Case 1 — confidence poisoning (zero-value phantom stub)
# ---------------------------------------------------------------------------


def test_case1_nondiscovery_stub_dropped_no_confidence_poisoning():
    """VIB-4838 Case 1 (FIXED): non-discovery stub no longer poisons confidence.

    Strategy reports a lending stub with a NON-discovery id
    (``aave-wbtc-collateral``, ``value_usd=0``, no asset/wallet) AND discovery
    returns the real, repriceable supply (~$2.61) + borrow (~-$0.93).

    Post-fix: the identity-less zero-value stub is dropped as a degenerate
    duplicate, only the two discovery-valued legs remain, and NO position is
    flagged ``no_path`` → ``positions_unavailable`` is False.
    """
    valuer = PortfolioValuer(gateway_client=None)
    stub = PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="aave-wbtc-collateral",  # author id, NOT discovery's scheme
        chain=CHAIN,
        protocol="aave_v3",
        value_usd=Decimal("0"),
        details={},  # no asset / wallet / asset_address — identity-less stub
    )
    strategy = _make_strategy([stub])
    market = _make_market()

    positions, total, unavailable = _drive_get_positions(valuer, strategy, market)

    # The phantom stub is dropped → only the 2 discovery legs remain.
    assert len(positions) == 2, "phantom stub must be dropped (canonical-key dedup)"

    # No no_path flag survives → confidence will not be poisoned.
    assert not any(p.details.get("valuation_status") == "no_path" for p in positions)
    assert unavailable is False

    # Both discovery legs are valued on-chain.
    supply = next(p for p in positions if p.position_type == PositionType.SUPPLY)
    assert supply.details.get("valuation_source") == "on_chain"
    assert supply.value_usd == EXPECTED_SUPPLY_USD
    borrow = next(p for p in positions if p.position_type == PositionType.BORROW)
    assert borrow.value_usd == EXPECTED_BORROW_USD


def test_case1_end_to_end_value_confidence_is_high():
    """VIB-4838 Case 1 (FIXED), snapshot level.

    The full ``value()`` snapshot now reaches HIGH: the phantom stub is gone,
    discovery valued both legs, and the empty wallet is measured-zero. This is
    the headline acceptance criterion — confidence is no longer poisoned.
    """
    valuer = PortfolioValuer(gateway_client=None)
    stub = PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="aave-wbtc-collateral",
        chain=CHAIN,
        protocol="aave_v3",
        value_usd=Decimal("0"),
        details={},
    )
    strategy = _make_strategy([stub])
    market = _make_market()

    with (
        patch.object(valuer._discovery, "discover", return_value=_discovery_supply_and_borrow()),
        patch.object(valuer._lending_reader, "read_position", side_effect=_fake_read_position),
        patch(
            "almanak.framework.data.tokens.get_token_resolver",
            return_value=_FakeTokenResolver(),
        ),
    ):
        snapshot = valuer.value(strategy, market, iteration_number=2)

    assert snapshot.value_confidence == ValueConfidence.HIGH
    assert not any(p.details.get("valuation_status") == "no_path" for p in snapshot.positions)


# ---------------------------------------------------------------------------
# Case 2 — latent NAV double-count (positive-value stub)
# ---------------------------------------------------------------------------


def test_case2_positive_value_stub_no_double_count():
    """VIB-4838 Case 2 (FIXED): positive-value stub collapses, no NAV double-count.

    The strategy stub carries ``value_usd>0`` AND an ``asset`` hint ("WBTC")
    that resolves to the same address discovery valued. Pre-fix the mismatched
    ids let BOTH survive → the supply counted twice ($5.22). Post-fix they
    collapse onto a single discovery-valued supply (~$2.61).
    """
    valuer = PortfolioValuer(gateway_client=None)
    # Strategy asserts the supply is worth ~$2.61 under its own author id.
    stub = PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="aave-wbtc-collateral",
        chain=CHAIN,
        protocol="aave_v3",
        value_usd=EXPECTED_SUPPLY_USD,  # positive self-report
        details={"asset": "WBTC"},  # resolves to WBTC address → matches discovery
    )
    strategy = _make_strategy([stub])
    market = _make_market()

    positions, total, unavailable = _drive_get_positions(valuer, strategy, market)

    # Single SUPPLY leg survives (collapsed onto discovery's on-chain value).
    supplies = [p for p in positions if p.position_type == PositionType.SUPPLY]
    assert len(supplies) == 1, "strategy + discovery supply must collapse to one"

    # Positive total equals the SINGLE on-chain value, not double.
    positive_total = sum((p.value_usd for p in positions if p.value_usd > 0), Decimal("0"))
    assert positive_total == EXPECTED_SUPPLY_USD, (
        f"supply must not double-count: got {positive_total}, expected {EXPECTED_SUPPLY_USD}"
    )
    # Value came from discovery (on-chain truth), not the strategy self-report.
    assert supplies[0].details.get("valuation_source") == "on_chain"


def test_aave_alias_stub_collapses_onto_canonical_discovery():
    """PR #2536 review (alias normalisation): a strategy stub reporting the
    ``"aave"`` alias must collapse onto discovery's canonical ``"aave_v3"``
    supply, not survive as a second leg.

    Discovery stamps the registry-canonical protocol (``"aave_v3"``). A strategy
    that declares the ``"aave"`` alias for the same reserve would, under raw
    ``(protocol or "").lower()`` keying, get a distinct dedup key
    (``"aave"`` != ``"aave_v3"``) → the same on-chain supply counted twice. The
    dedup key now normalises both through ``LendingReadRegistry.canonical`` so
    they collapse to one. Mirrors Case 2 but with the alias on the strategy leg.
    """
    valuer = PortfolioValuer(gateway_client=None)
    stub = PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="aave-wbtc-collateral",
        chain=CHAIN,
        protocol="aave",  # alias — discovery stamps the canonical "aave_v3"
        value_usd=EXPECTED_SUPPLY_USD,
        details={"asset": "WBTC"},  # resolves to WBTC address → matches discovery
    )
    strategy = _make_strategy([stub])
    market = _make_market()

    positions, total, unavailable = _drive_get_positions(valuer, strategy, market)

    supplies = [p for p in positions if p.position_type == PositionType.SUPPLY]
    assert len(supplies) == 1, "alias stub + canonical discovery supply must collapse to one"
    positive_total = sum((p.value_usd for p in positions if p.value_usd > 0), Decimal("0"))
    assert positive_total == EXPECTED_SUPPLY_USD, (
        f"alias mismatch must not double-count: got {positive_total}, expected {EXPECTED_SUPPLY_USD}"
    )
    assert supplies[0].details.get("valuation_source") == "on_chain"
    # Surviving leg carries discovery's canonical protocol (not the "aave" alias)
    # so downstream routing / accounting key on the canonical identifier.
    assert supplies[0].protocol == "aave_v3"


@pytest.mark.parametrize(
    ("protocol", "expected"),
    [
        ("aave", "aave_v3"),  # alias collapses to canonical
        ("AAVE_V3", "aave_v3"),  # case-insensitive
        ("spark", "spark"),
        ("uniswap_v3", "uniswap_v3"),  # non-lending → lowercased passthrough
        ("UniSwap_V3", "uniswap_v3"),
        (None, ""),  # loosely typed metadata must not crash
        (123, ""),  # truthy non-str must not crash on .lower() (PR #2536 review)
        (3.14, ""),
        ("", ""),
    ],
)
def test_normalize_protocol_for_dedup_is_total(protocol, expected):
    """The dedup-key normaliser must never raise on loosely typed
    ``PositionInfo.protocol`` — a truthy non-str (e.g. ``123``) previously hit
    ``(protocol or "").lower()`` and crashed (PR #2536 review)."""
    assert _normalize_protocol_for_dedup(protocol) == expected


# ---------------------------------------------------------------------------
# Case 3 — §3 dual-warning fallback chain (end-to-end through capture)
# ---------------------------------------------------------------------------


class _GuardingStateManager:
    """State-manager stub whose ``save_portfolio_snapshot`` applies the REAL
    ``_apply_synth_position_guard`` and records what actually persists.

    Mirrors ``GatewayStateManager.save_portfolio_snapshot`` (line ~262), which
    calls ``_apply_synth_position_guard(snapshot)`` before writing.
    """

    def __init__(self):
        self.persisted: PortfolioSnapshot | None = None
        self.load_state = AsyncMock(return_value=None)
        self.save_state = AsyncMock()
        self.get_portfolio_metrics = AsyncMock(return_value=None)
        self.save_portfolio_metrics = AsyncMock(return_value=True)

    async def save_portfolio_snapshot(self, snapshot: PortfolioSnapshot) -> int:
        _apply_synth_position_guard(snapshot)  # the real production guard
        self.persisted = snapshot
        return 1


@pytest.mark.asyncio
async def test_case3_valuer_high_persists_fallback_not_called():
    """VIB-4838 Case 3 (FIXED): persistence source shifts from fallback to valuer.

    The §3 chain (verified pre-fix in the repro commit): a phantom stub made
    ``PortfolioValuer.value()`` return UNAVAILABLE, which
    ``capture_portfolio_snapshot`` discarded in favour of the strategy
    ``get_portfolio_snapshot`` fallback (HIGH), which the synth-position guard
    then degraded to ESTIMATED — so the *fallback* persisted.

    Post-fix the dedup drops the phantom stub, so the valuer returns HIGH, the
    fallback never triggers, and the valuer's own (properly on-chain-valued)
    snapshot is what persists. This deliberate persistence-source shift is the
    most consequential downstream effect of the fix.

    Note the persisted confidence is ESTIMATED, not HIGH: ``_apply_synth_position_guard``
    degrades it because the discovery collateral leg has ``cost_basis_usd=0``
    (no materialised basis in this unit fixture; hosted Track B no-op in prod).
    This is the documented OUT-OF-SCOPE residual (VIB-4667 / VIB-3844) — a
    strictly better signal than the pre-fix UNAVAILABLE, and still sourced from
    the valuer rather than the fallback.
    """
    valuer = PortfolioValuer(gateway_client=None)
    stub = PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="aave-wbtc-collateral",
        chain=CHAIN,
        protocol="aave_v3",
        value_usd=Decimal("0"),
        details={},
    )
    strategy = _make_strategy([stub])
    market = _make_market()
    strategy.create_market_snapshot = MagicMock(return_value=market)
    # If the fallback is (wrongly) consulted, fail loudly.
    strategy.get_portfolio_snapshot = MagicMock(side_effect=AssertionError("fallback must NOT be called post-fix"))

    state_manager = _GuardingStateManager()
    runner = SimpleNamespace(
        state_manager=state_manager,
        _portfolio_valuer=valuer,
        _last_snapshot_time=None,
        _snapshot_interval_seconds=0,
        _is_multi_chain=False,
        _get_gateway_client=MagicMock(return_value=None),
        deployment_id=strategy.deployment_id,
        _last_cycle_id="cycle-1",
        _recent_open_events={},
        config=SimpleNamespace(dry_run=True, paper_mode=False),
    )

    with (
        patch.object(valuer._discovery, "discover", return_value=_discovery_supply_and_borrow()),
        patch.object(valuer._lending_reader, "read_position", side_effect=_fake_read_position),
        patch(
            "almanak.framework.data.tokens.get_token_resolver",
            return_value=_FakeTokenResolver(),
        ),
    ):
        # 1. The valuer alone now returns HIGH (no phantom no_path stub).
        valuer_only = valuer.value(strategy, market, iteration_number=2)
        assert valuer_only.value_confidence == ValueConfidence.HIGH

        # 2. Full pipeline: valuer HIGH persists, fallback never consulted.
        result = await capture_portfolio_snapshot(runner, strategy, iteration_number=2, force_snapshot=True)

    # The strategy fallback is NOT consulted — the valuer's snapshot persists.
    strategy.get_portfolio_snapshot.assert_not_called()
    assert result is state_manager.persisted
    # Persisted confidence is ESTIMATED only because of the documented
    # out-of-scope zero-basis guard (Track B); pre-fix it was UNAVAILABLE.
    assert result.value_confidence == ValueConfidence.ESTIMATED
    # The persisted positions are the two discovery-valued legs (not the stub).
    assert {p.position_type for p in result.positions} == {PositionType.SUPPLY, PositionType.BORROW}


# ---------------------------------------------------------------------------
# Acceptance: identity-keyed dedup edge cases
# ---------------------------------------------------------------------------


# A second resolver that also knows WETH / ARB so we can build address-disagreement
# scenarios (custom-token / symbol-collision class).
WETH_ADDR = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"  # Arbitrum WETH
ARB_ADDR = "0x912ce59144191c1204e64559fe8253a0e49e6548"  # Arbitrum ARB


class _MultiTokenResolver(_FakeTokenResolver):
    _BY_ADDR = {
        WBTC_ADDR.lower(): ("WBTC", 8),
        USDC_ADDR.lower(): ("USDC", 6),
        WETH_ADDR.lower(): ("WETH", 18),
        ARB_ADDR.lower(): ("ARB", 18),
    }
    _BY_SYMBOL = {
        "WBTC": (WBTC_ADDR, 8),
        "USDC": (USDC_ADDR, 6),
        "WETH": (WETH_ADDR, 18),
        "ARB": (ARB_ADDR, 18),
    }


def test_same_asset_address_collapses_despite_different_ids():
    """Two SUPPLY legs with different ids but the SAME asset address collapse.

    Strategy reports ``my-wbtc`` with the explicit WBTC ``asset_address``;
    discovery reports ``aave-supply-WBTC-arbitrum`` for the same address. They
    must collapse to one discovery-valued position (address beats id/symbol).
    """
    valuer = PortfolioValuer(gateway_client=None)
    stub = PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="my-wbtc",
        chain=CHAIN,
        protocol="aave_v3",
        value_usd=Decimal("0"),
        details={"asset_address": WBTC_ADDR},  # same address, different id
    )
    strategy = _make_strategy([stub])
    market = _make_market()

    with (
        patch.object(valuer._discovery, "discover", return_value=_discovery_supply_and_borrow()),
        patch.object(valuer._lending_reader, "read_position", side_effect=_fake_read_position),
        patch("almanak.framework.data.tokens.get_token_resolver", return_value=_MultiTokenResolver()),
    ):
        positions, _total, unavailable = valuer._get_positions(strategy, market, {})

    supplies = [p for p in positions if p.position_type == PositionType.SUPPLY]
    assert len(supplies) == 1, "same-address SUPPLY legs must collapse to one"
    assert supplies[0].value_usd == EXPECTED_SUPPLY_USD
    assert unavailable is False


def test_symbol_collapse_refused_when_addresses_disagree():
    """Symbol-only collapse is REFUSED when the two sides' addresses disagree.

    Strategy reports a WETH supply (resolves to WETH addr, value_usd=0, no
    wallet); discovery only found a WBTC supply + USDC borrow. The WETH stub is
    a DIFFERENT asset — it must NOT collapse onto WBTC and must NOT be dropped
    as a phantom. It reprices→no_path (no wallet) → snapshot UNAVAILABLE.
    """
    valuer = PortfolioValuer(gateway_client=None)
    weth_stub = PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="aave-weth-collateral",
        chain=CHAIN,
        protocol="aave_v3",
        value_usd=Decimal("0"),
        details={"asset": "WETH"},  # resolves to WETH address ≠ discovery's WBTC
    )
    strategy = _make_strategy([weth_stub])
    market = _make_market()

    with (
        patch.object(valuer._discovery, "discover", return_value=_discovery_supply_and_borrow()),
        patch.object(valuer._lending_reader, "read_position", side_effect=_fake_read_position),
        patch("almanak.framework.data.tokens.get_token_resolver", return_value=_MultiTokenResolver()),
    ):
        positions, _total, unavailable = valuer._get_positions(strategy, market, {})

    # The WETH stub survives (kept, not collapsed onto WBTC, not dropped).
    weth_positions = [
        p
        for p in positions
        if p.position_type == PositionType.SUPPLY and p.details.get("asset") == "WETH"
    ]
    assert len(weth_positions) == 1, "asset-disagreeing stub must be kept, not collapsed/dropped"
    assert weth_positions[0].details.get("valuation_status") == "no_path"
    assert unavailable is True


def test_identityless_zero_stub_for_unrelated_asset_still_no_path():
    """False-positive guard: an identity-less zero stub whose asset hint
    points at an asset discovery did NOT return must still be kept → no_path.

    Here discovery returns ONLY a USDC borrow (no SUPPLY at all). The
    identity-less SUPPLY stub has no same-(protocol,type,chain) discovery group
    to be a phantom of, so it is kept and flags no_path → UNAVAILABLE. This is
    the VIB-4584 guard: genuinely unvaluable positions still degrade.
    """
    valuer = PortfolioValuer(gateway_client=None)
    stub = PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="aave-mystery-collateral",
        chain=CHAIN,
        protocol="aave_v3",
        value_usd=Decimal("0"),
        details={},  # identity-less
    )
    strategy = _make_strategy([stub])
    market = _make_market()

    borrow_only = DiscoveryResult(
        positions=[
            PositionInfo(
                position_type=PositionType.BORROW,
                position_id=f"aave-borrow-USDC-{CHAIN}",
                chain=CHAIN,
                protocol="aave_v3",
                value_usd=Decimal("0"),
                details={"asset": "USDC", "asset_address": USDC_ADDR, "wallet_address": WALLET},
            )
        ],
        lending_assets_scanned=1,
    )

    with (
        patch.object(valuer._discovery, "discover", return_value=borrow_only),
        patch.object(valuer._lending_reader, "read_position", side_effect=_fake_read_position),
        patch("almanak.framework.data.tokens.get_token_resolver", return_value=_MultiTokenResolver()),
    ):
        positions, _total, unavailable = valuer._get_positions(strategy, market, {})

    # SUPPLY stub kept (no SUPPLY discovery group to phantom-match) → no_path.
    supplies = [p for p in positions if p.position_type == PositionType.SUPPLY]
    assert len(supplies) == 1
    assert supplies[0].details.get("valuation_status") == "no_path"
    assert unavailable is True


def test_vib4584_discovery_only_unvaluable_still_unavailable():
    """VIB-4584 preserved: a discovery SUPPLY with no repriceable path (no
    on-chain read available) still flags no_path → UNAVAILABLE.

    No strategy positions; discovery returns a SUPPLY but the lending reader
    returns None (no RPC). The position cannot be valued by any source, so it
    must remain no_path — the fix must not mask genuinely unvaluable positions.
    """
    valuer = PortfolioValuer(gateway_client=None)
    strategy = _make_strategy([])  # no strategy positions
    market = _make_market()

    disc = DiscoveryResult(
        positions=[
            PositionInfo(
                position_type=PositionType.SUPPLY,
                position_id=f"aave-supply-WBTC-{CHAIN}",
                chain=CHAIN,
                protocol="aave_v3",
                value_usd=Decimal("0"),
                details={"asset": "WBTC", "asset_address": WBTC_ADDR, "wallet_address": WALLET},
            )
        ],
        lending_assets_scanned=1,
    )

    with (
        patch.object(valuer._discovery, "discover", return_value=disc),
        patch.object(valuer._lending_reader, "read_position", return_value=None),  # no on-chain path
        patch("almanak.framework.data.tokens.get_token_resolver", return_value=_MultiTokenResolver()),
    ):
        positions, _total, unavailable = valuer._get_positions(strategy, market, {})

    assert len(positions) == 1
    assert positions[0].details.get("valuation_status") == "no_path"
    assert unavailable is True


def test_strategy_stub_with_details_none_does_not_crash():
    """Robustness: a strategy that reports ``PositionInfo(details=None)`` must
    not crash the dedup helpers (gemini-code-assist review, PR #2453).

    ``PositionInfo.details`` defaults to ``{}`` and is typed ``dict``, but
    nothing validates it at construction, so an AI-authored ``get_open_positions``
    could hand us ``None``. The new canonical-key / stub-guard / merge helpers
    defensively coerce ``details or {}`` — this exercises that path end-to-end.
    The stub is still an identity-less zero-value phantom, so it collapses the
    same way Case 1 does (dropped, no confidence poisoning).
    """
    valuer = PortfolioValuer(gateway_client=None)
    stub = PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="aave-wbtc-collateral",
        chain=CHAIN,
        protocol="aave_v3",
        value_usd=Decimal("0"),
        details=None,  # contract says dict; a strategy could still pass None
    )
    strategy = _make_strategy([stub])
    market = _make_market()

    # Must not raise AttributeError/TypeError on the None details.
    positions, _total, unavailable = _drive_get_positions(valuer, strategy, market)

    assert len(positions) == 2  # phantom dropped, two discovery legs remain
    assert not any(p.details.get("valuation_status") == "no_path" for p in positions)
    assert unavailable is False
