"""Contract tests for the bespoke BENQI account-state spec (VIB-4967).

BENQI is a Compound-V2 fork (qiTokens), NOT an Aave fork — it has NO
``getUserAccountData``. But it IS a pooled, cross-asset market, so the spec
(:data:`~almanak.connectors.benqi.lending_read.ACCOUNT_STATE_READ_SPEC`) reads
WHOLE-ACCOUNT: per listed qiToken it issues ``getAccountSnapshot`` (supply + debt)
+ ``Comptroller.markets`` (the per-market liquidation collateral factor), then sums
to a total collateral / debt and a TRUE liquidation-aware health factor
``HF = Σ(supply_usd × collateralFactor) / Σ debt_usd`` (the on-chain Compound-V2
liquidation parameter — NOT a bare collateral/debt proxy). The registry drives it
through the generic
:func:`~almanak.framework.accounting.lending_accounting.read_lending_account_state`.

These tests pin (without any network / mocking of the spec internals):

* the single whole-account ``"benqi"`` market-id catalogue + ``query_inputs_fn``
  (intent-agnostic — BENQI intents carry no ``market_id``, and a bare REPAY resolves
  cleanly because the read is whole-account, not per-pair);
* ``build_calls`` selectors / targets / order (snapshot + markets, per qiToken);
* ``reduce_calls`` ABSOLUTE decoded collateral / debt / TRUE HF values, the no-debt
  sentinel, and the **reachable** Empty ≠ Zero fail-closed branches (held leg with a
  missing price; held collateral with an unreadable collateral factor; a short
  snapshot blob — none of which are filtered out before the guard runs);
* the registry's market-scoped binding + the end-to-end generic-reader path over a
  selector-routing mock gateway (incl. the bare-REPAY whole-account price injection).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

from almanak.connectors._strategy_base.lending_read_base import AccountStateQuery, EthCall
from almanak.connectors._strategy_base.lending_read_registry import (
    AccountStatePlan,
    LendingReadRegistry,
)
from almanak.connectors.benqi.adapter import BENQI_COMPTROLLER_ADDRESS, BENQI_QI_TOKENS
from almanak.connectors.benqi.lending_read import (
    ACCOUNT_STATE_READ_SPEC,
    BENQI_ACCOUNT_STATE_MARKETS,
    _benqi_query_inputs_from_intent,
    collateral_factor_to_bps,
)
from almanak.framework.accounting.lending_accounting import (
    _GENERIC_PRE_STATE_PROTOCOLS,
    capture_lending_pre_state,
    lending_state_to_dict,
    read_lending_account_state,
)

_CHAIN = "avalanche"
_WALLET = "0xABCDEF0123456789abcdef0123456789ABCDEF01"

_QI_AVAX = BENQI_QI_TOKENS["AVAX"]["qi_token"]
_QI_USDC = BENQI_QI_TOKENS["USDC"]["qi_token"]

# Selectors the spec emits (Compound V2).
_SNAPSHOT_SELECTOR = "0xc37f68e2"
_MARKETS_SELECTOR = "0x8e8f294b"
_MEMBERSHIP_SELECTOR = "0x929fe9a1"

# Compound V2 exchange-rate scale + a rate s.t. qiBal(8dec) * rate / 1e18 = underlying.
_RATE_18 = 10**28  # for 18-dec underlying: qiBal(1e8) * 1e28 / 1e18 = 1e18
_RATE_6 = 10**16  # for 6-dec underlying: qiBal(1e8) * 1e16 / 1e18 = 1e6

_PRICES = {"WAVAX": Decimal("20"), "USDC": Decimal("1")}
_DECIMALS = {"WAVAX": 18, "USDC": 6}


def _word(value: int) -> str:
    return format(value, "064x")


def _snapshot(qi_balance: int, borrow: int, rate: int, error: int = 0) -> str:
    """Encode getAccountSnapshot → (error, qiTokenBalance, borrowBalance, exchangeRate)."""
    return "0x" + _word(error) + _word(qi_balance) + _word(borrow) + _word(rate)


def _markets(cf_mantissa: int, is_listed: int = 1) -> str:
    """Encode Comptroller.markets → (isListed, collateralFactorMantissa, isComped)."""
    return "0x" + _word(is_listed) + _word(cf_mantissa) + _word(0)


def _membership(entered: bool) -> str:
    """Encode Comptroller.checkMembership → bool."""
    return "0x" + _word(1 if entered else 0)


# ---------------------------------------------------------------------------
# Whole-account catalogue + intent → inputs
# ---------------------------------------------------------------------------


def test_catalogue_is_single_whole_account_market() -> None:
    table = BENQI_ACCOUNT_STATE_MARKETS["avalanche"]
    assert set(table.keys()) == {"benqi"}
    entry = table["benqi"]
    assert entry["comptroller_address"] == BENQI_COMPTROLLER_ADDRESS
    # Every qiToken is listed in deterministic order with its priceable symbol.
    syms = [m["symbol"] for m in entry["markets"]]
    assert syms[0] == "WAVAX"  # native AVAX prices via WAVAX
    assert "USDC" in syms
    assert len(entry["markets"]) == len(BENQI_QI_TOKENS)
    # The collaterals map prices every underlying (whole-account injection path).
    assert "WAVAX" in entry["collaterals"]
    assert "USDC" in entry["collaterals"]
    # AVAX maps to the WAVAX ERC-20 address (priceable proxy for native AVAX).
    assert entry["collaterals"]["WAVAX"]["address"].lower() == "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7"


def test_query_inputs_intent_agnostic_whole_account() -> None:
    # Every lending intent type → the SAME whole-account market id, no named
    # collateral (so the framework prices every listed underlying). A bare REPAY
    # therefore resolves cleanly (no per-pair ambiguity).
    for intent in (
        MagicMock(intent_type="SUPPLY", token="USDC", chain=_CHAIN),
        MagicMock(intent_type="BORROW", collateral_token="AVAX", borrow_token="USDC", chain=_CHAIN),
        MagicMock(intent_type="REPAY", token="USDC", chain=_CHAIN),
        MagicMock(intent_type="WITHDRAW", token="AVAX", chain=_CHAIN),
    ):
        assert _benqi_query_inputs_from_intent(intent) == {"market_id": "benqi", "collateral_token": None}


# ---------------------------------------------------------------------------
# build_calls — selectors / targets / order
# ---------------------------------------------------------------------------


def _whole_account_params() -> dict[str, Any]:
    return BENQI_ACCOUNT_STATE_MARKETS["avalanche"]["benqi"]


def _spec_query(market_params: dict[str, Any] | None = None) -> AccountStateQuery:
    return AccountStateQuery(
        chain=_CHAIN,
        wallet_address=_WALLET,
        position_manager_address=BENQI_COMPTROLLER_ADDRESS,
        market_id="benqi",
        prices=_PRICES,
        decimals=_DECIMALS,
        market_params=market_params or _whole_account_params(),
    )


def test_build_calls_emits_snapshot_markets_membership_per_qitoken() -> None:
    calls = ACCOUNT_STATE_READ_SPEC.build_calls(_spec_query())
    n = len(BENQI_QI_TOKENS)
    assert len(calls) == 3 * n
    assert all(isinstance(c, EthCall) for c in calls)
    wallet_hex = _WALLET.lower().replace("0x", "").zfill(64)
    markets = _whole_account_params()["markets"]
    for i, market in enumerate(markets):
        snap, mkt, mem = calls[3 * i], calls[3 * i + 1], calls[3 * i + 2]
        qi_hex = market["qi_token"].lower().replace("0x", "").zfill(64)
        # snapshot(user) on the qiToken
        assert snap.to == market["qi_token"]
        assert snap.data == _SNAPSHOT_SELECTOR + wallet_hex
        # markets(qiToken) on the Comptroller
        assert mkt.to == BENQI_COMPTROLLER_ADDRESS
        assert mkt.data == _MARKETS_SELECTOR + qi_hex
        # checkMembership(account, qiToken) on the Comptroller — account first.
        assert mem.to == BENQI_COMPTROLLER_ADDRESS
        assert mem.data == _MEMBERSHIP_SELECTOR + wallet_hex + qi_hex


def test_build_calls_missing_comptroller_fails_closed() -> None:
    params = {**_whole_account_params(), "comptroller_address": ""}
    assert ACCOUNT_STATE_READ_SPEC.build_calls(_spec_query(params)) == []


def test_build_calls_missing_markets_list_fails_closed() -> None:
    params = {k: v for k, v in _whole_account_params().items() if k != "markets"}
    assert ACCOUNT_STATE_READ_SPEC.build_calls(_spec_query(params)) == []


# ---------------------------------------------------------------------------
# reduce_calls — decoded collateral / debt / TRUE HF + fail-closed
# ---------------------------------------------------------------------------
#
# Build a deterministic 2-market params so the read set is small + the dangerous
# (held-but-unreadable / unpriced) branches are REACHABLE — never filtered out
# before the guard (the Compound C-1 lesson the ticket calls out).

_TWO_MARKET_PARAMS: dict[str, Any] = {
    "comet_address": BENQI_COMPTROLLER_ADDRESS,
    "comptroller_address": BENQI_COMPTROLLER_ADDRESS,
    "markets": [
        {"qi_token": _QI_AVAX, "symbol": "WAVAX"},
        {"qi_token": _QI_USDC, "symbol": "USDC"},
    ],
    "collaterals": {
        "WAVAX": {"address": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"},
        "USDC": {"address": BENQI_QI_TOKENS["USDC"]["underlying"]},
    },
}


def _two_market_query(prices: dict[str, Decimal] | None = None) -> AccountStateQuery:
    return AccountStateQuery(
        chain=_CHAIN,
        wallet_address=_WALLET,
        position_manager_address=BENQI_COMPTROLLER_ADDRESS,
        market_id="benqi",
        prices=_PRICES if prices is None else prices,
        decimals=_DECIMALS,
        market_params=_TWO_MARKET_PARAMS,
    )


def test_reduce_true_liquidation_hf_concrete_values() -> None:
    # WAVAX supply 10e18 ($200, CF 0.6, ENTERED) + USDC debt 50e6 ($50): coll $200,
    # debt $50. TRUE liquidation HF = (200 * 0.6) / 50 = 2.4 — weighted by the ON-CHAIN
    # CF, not a bare 200/50 = 4 proxy.
    results = [
        _snapshot(10 * 10**8, 0, _RATE_18),  # AVAX: 10e18 supply, no borrow
        _markets(6 * 10**17),  # AVAX CF 0.6
        _membership(True),  # AVAX ENTERED as collateral
        _snapshot(0, 50 * 10**6, _RATE_6),  # USDC: no supply, 50e6 borrow
        _markets(8 * 10**17),  # USDC CF 0.8 (unused — USDC not supplied)
        _membership(False),  # USDC not entered (no supply anyway)
    ]
    state = ACCOUNT_STATE_READ_SPEC.reduce_calls(_two_market_query(), results)
    assert state is not None
    assert state.collateral_usd == Decimal("200")
    assert state.debt_usd == Decimal("50")
    assert state.health_factor == Decimal("2.4"), "HF must weight collateral by the on-chain CF (not a bare proxy)"
    assert state.liquidation_threshold_bps is None  # per-asset CFs folded into HF
    assert state.e_mode_category is None
    assert state.lltv is None
    assert state.family is None


def test_reduce_supplied_but_not_entered_excluded_from_collateral() -> None:
    # CodeRabbit 2026-06: a qiToken SUPPLIED but NOT ENTERED as collateral must NOT
    # count toward collateral_usd / the liquidation HF. Here WAVAX is supplied + entered
    # ($200 collateral) while USDC is supplied $1000 but NOT entered AND has a $50 debt
    # — the USDC supply must be EXCLUDED from collateral (only WAVAX counts), and the
    # USDC debt still counts. HF = (200 * 0.6) / 50 = 2.4 (NOT (200+1000*0.8)/50).
    results = [
        _snapshot(10 * 10**8, 0, _RATE_18),  # AVAX: 10e18 supply
        _markets(6 * 10**17),
        _membership(True),  # AVAX entered
        _snapshot(1000 * 10**8, 50 * 10**6, _RATE_6),  # USDC: 1000e6 supply + 50e6 debt
        _markets(8 * 10**17),
        _membership(False),  # USDC NOT entered → supply excluded from collateral
    ]
    state = ACCOUNT_STATE_READ_SPEC.reduce_calls(_two_market_query(), results)
    assert state is not None
    assert state.collateral_usd == Decimal("200"), "supplied-but-not-entered USDC must be excluded from collateral"
    assert state.debt_usd == Decimal("50"), "debt counts regardless of membership"
    assert state.health_factor == Decimal("2.4")


def test_reduce_supplied_not_entered_no_debt_unpriced_skipped() -> None:
    # A supplied-but-not-entered market with NO debt contributes nothing and needs no
    # price — even if unpriced it must not fail the read (it is not collateral).
    results = [
        _snapshot(10 * 10**8, 0, _RATE_18),  # AVAX supply, entered → $200 collateral
        _markets(6 * 10**17),
        _membership(True),
        _snapshot(1000 * 10**8, 0, _RATE_6),  # USDC supplied but NOT entered, no debt
        _markets(8 * 10**17),
        _membership(False),
    ]
    # Drop USDC from the price map: a not-entered, no-debt supply must be skipped.
    state = ACCOUNT_STATE_READ_SPEC.reduce_calls(_two_market_query({"WAVAX": Decimal("20")}), results)
    assert state is not None
    assert state.collateral_usd == Decimal("200")
    assert state.debt_usd == Decimal("0")
    assert state.health_factor == Decimal("999999")


def test_reduce_supply_only_no_debt_yields_sentinel_hf() -> None:
    # SUPPLY-only USDC (entered): collateral measured, debt measured ZERO (Empty ≠
    # Zero), HF the no-risk sentinel.
    results = [
        _snapshot(0, 0, _RATE_18),  # AVAX untouched
        _markets(6 * 10**17),
        _membership(False),
        _snapshot(1000 * 10**8, 0, _RATE_6),  # USDC: 1000e6 supply
        _markets(8 * 10**17),
        _membership(True),  # USDC ENTERED
    ]
    state = ACCOUNT_STATE_READ_SPEC.reduce_calls(_two_market_query(), results)
    assert state is not None
    assert state.collateral_usd == Decimal("1000")
    assert state.debt_usd == Decimal("0")
    assert state.health_factor == Decimal("999999")


def test_reduce_held_leg_missing_price_fails_closed() -> None:
    # REACHABLE dangerous branch: the wallet HOLDS+ENTERED WAVAX collateral but its
    # price was NOT injected. Must fail closed (Empty ≠ Zero) — under-counting it would
    # inflate the HF and mask liquidation risk (the Compound C-1 lesson).
    results = [
        _snapshot(10 * 10**8, 0, _RATE_18),  # AVAX HELD + entered
        _markets(6 * 10**17),
        _membership(True),
        _snapshot(0, 50 * 10**6, _RATE_6),
        _markets(8 * 10**17),
        _membership(False),
    ]
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(_two_market_query({"USDC": Decimal("1")}), results) is None


def test_vib5911_partial_usd_dict_still_fails_closed_on_held_unpriced_leg() -> None:
    # Composition link for VIB-5911: the fixed ``_build_price_oracle_dict`` LEAVES
    # OUT a symbol the oracle cannot answer for; a reducer that then sees that
    # symbol HELD+ENTERED must still fail the whole read closed (Empty ≠ Zero).
    # The fix must never introduce a path that values a held leg it has no price
    # for — this drives the REAL builder output into the REAL reducer.
    from unittest.mock import patch

    from almanak.framework.data.position_health import PositionHealthProvider

    def _usdc_only_oracle(symbol: str):
        if symbol == "USDC":
            return "1"
        raise ValueError(f"no price source for {symbol}")

    provider = PositionHealthProvider(chain=_CHAIN, gateway_client=MagicMock(), price_oracle=_usdc_only_oracle)
    with patch(
        "almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry.market_params",
        return_value=_TWO_MARKET_PARAMS,
    ):
        prices, _source = provider._build_price_oracle_dict("benqi", "benqi", None, None)
    assert prices == {"USDC": Decimal("1")}, "WAVAX must be LEFT OUT — never priced 1 or 0"

    results = [
        _snapshot(10 * 10**8, 0, _RATE_18),  # WAVAX HELD + entered — but unpriced
        _markets(6 * 10**17),
        _membership(True),
        _snapshot(0, 50 * 10**6, _RATE_6),  # USDC debt (priced)
        _markets(8 * 10**17),
        _membership(False),
    ]
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(_two_market_query(prices), results) is None


def test_reduce_held_collateral_unreadable_factor_fails_closed() -> None:
    # REACHABLE dangerous branch: the wallet HOLDS+ENTERED WAVAX collateral but its
    # markets() collateral-factor read failed (None blob). The HF would be
    # unmeasurable, so fail closed — never fabricate a liquidation parameter.
    results = [
        _snapshot(10 * 10**8, 0, _RATE_18),  # AVAX HELD + entered
        None,  # markets(qiAVAX) read FAILED
        _membership(True),
        _snapshot(0, 50 * 10**6, _RATE_6),
        _markets(8 * 10**17),
        _membership(False),
    ]
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(_two_market_query(), results) is None


def test_reduce_held_debt_missing_price_fails_closed() -> None:
    # REACHABLE: the wallet HOLDS USDC debt but its price is missing ⇒ fail closed
    # (never collapse a real debt to zero + a perfect HF).
    results = [
        _snapshot(10 * 10**8, 0, _RATE_18),
        _markets(6 * 10**17),
        _membership(True),
        _snapshot(0, 50 * 10**6, _RATE_6),  # USDC debt HELD
        _markets(8 * 10**17),
        _membership(False),
    ]
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(_two_market_query({"WAVAX": Decimal("20")}), results) is None


def test_reduce_snapshot_error_word_fails_closed() -> None:
    # A non-zero snapshot error word (Compound returns error != 0 on failure) ⇒ the
    # whole read fails closed, never a fabricated zero leg.
    results = [
        _snapshot(10 * 10**8, 0, _RATE_18, error=7),  # AVAX snapshot errored
        _markets(6 * 10**17),
        _membership(True),
        _snapshot(0, 50 * 10**6, _RATE_6),
        _markets(8 * 10**17),
        _membership(False),
    ]
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(_two_market_query(), results) is None


def test_reduce_short_snapshot_blob_fails_closed() -> None:
    results = [
        "0x" + "00" * 16,
        _markets(6 * 10**17),
        _membership(True),
        _snapshot(0, 50 * 10**6, _RATE_6),
        _markets(8 * 10**17),
        _membership(False),
    ]
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(_two_market_query(), results) is None


def test_reduce_wrong_result_count_fails_closed() -> None:
    # A truncated result set (fewer blobs than 3×markets) must fail closed.
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(_two_market_query(), [_snapshot(0, 0, _RATE_18)]) is None


def test_reduce_held_leg_none_decimals_fails_closed() -> None:
    # REACHABLE dangerous branch (Gemini 2026-06): the wallet HOLDS+ENTERED WAVAX
    # collateral but its injected decimals is None — ``10 ** None`` would raise. Must
    # fail closed (Empty ≠ Zero), never crash the reducer.
    query = AccountStateQuery(
        chain=_CHAIN,
        wallet_address=_WALLET,
        position_manager_address=BENQI_COMPTROLLER_ADDRESS,
        market_id="benqi",
        prices=_PRICES,
        decimals={"WAVAX": None, "USDC": 6},  # type: ignore[dict-item]
        market_params=_TWO_MARKET_PARAMS,
    )
    results = [
        _snapshot(10 * 10**8, 0, _RATE_18),  # AVAX HELD + entered
        _markets(6 * 10**17),
        _membership(True),
        _snapshot(0, 50 * 10**6, _RATE_6),
        _markets(8 * 10**17),
        _membership(False),
    ]
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(query, results) is None


def test_build_calls_non_dict_markets_entry_filtered() -> None:
    # A malformed market list (non-dict entries) must not raise — filtered to dicts.
    params = {**_TWO_MARKET_PARAMS, "markets": [{"qi_token": _QI_AVAX, "symbol": "WAVAX"}, "garbage", None]}
    calls = ACCOUNT_STATE_READ_SPEC.build_calls(_spec_query(params))
    # Only the single valid dict entry yields a (snapshot, markets, membership) triple.
    assert len(calls) == 3


def test_reduce_untouched_unpriced_market_skipped_cleanly() -> None:
    # A listed market the user NEVER touched (zero supply + zero borrow), even if
    # unpriced, must NOT fail the read — it contributes nothing and needs no price.
    params = {
        **_TWO_MARKET_PARAMS,
        "markets": [
            {"qi_token": _QI_AVAX, "symbol": "WAVAX"},
            {"qi_token": _QI_USDC, "symbol": "USDC"},
            {"qi_token": "0x" + "ab" * 20, "symbol": "UNPRICED"},  # listed but untouched + unpriced
        ],
    }
    query = AccountStateQuery(
        chain=_CHAIN,
        wallet_address=_WALLET,
        position_manager_address=BENQI_COMPTROLLER_ADDRESS,
        market_id="benqi",
        prices=_PRICES,
        decimals=_DECIMALS,
        market_params=params,
    )
    results = [
        _snapshot(10 * 10**8, 0, _RATE_18),
        _markets(6 * 10**17),
        _membership(True),
        _snapshot(0, 50 * 10**6, _RATE_6),
        _markets(8 * 10**17),
        _membership(False),
        _snapshot(0, 0, _RATE_18),  # UNPRICED market untouched
        _markets(5 * 10**17),
        _membership(False),
    ]
    state = ACCOUNT_STATE_READ_SPEC.reduce_calls(query, results)
    assert state is not None
    assert state.collateral_usd == Decimal("200")
    assert state.debt_usd == Decimal("50")
    assert state.health_factor == Decimal("2.4")


def test_collateral_factor_to_bps_rounds_half_up() -> None:
    assert collateral_factor_to_bps(Decimal("0.8")) == 8000
    assert collateral_factor_to_bps(Decimal("0.855")) == 8550


# ---------------------------------------------------------------------------
# Registry resolution + end-to-end generic reader
# ---------------------------------------------------------------------------


def test_registry_enabled_and_supports_account_state() -> None:
    assert "benqi" in _GENERIC_PRE_STATE_PROTOCOLS
    assert LendingReadRegistry.supports_account_state("benqi")


def test_position_manager_address_reports_market_scoped_existence() -> None:
    assert LendingReadRegistry.position_manager_address("benqi", _CHAIN)
    assert LendingReadRegistry.position_manager_address("benqi", "ethereum") is None


def test_market_params_resolves_whole_account_entry() -> None:
    params = LendingReadRegistry.market_params("benqi", _CHAIN, "benqi")
    assert params is not None
    assert params["comptroller_address"] == BENQI_COMPTROLLER_ADDRESS
    assert isinstance(params["markets"], list)


def test_resolve_account_state_plan_binds_market_scoped_target() -> None:
    plan = LendingReadRegistry.resolve_account_state_plan("benqi", _spec_query())
    assert isinstance(plan, AccountStatePlan)
    assert plan.query.position_manager_address == BENQI_COMPTROLLER_ADDRESS
    assert plan.reduce is ACCOUNT_STATE_READ_SPEC.reduce_calls


def _mock_gateway(snapshots: dict[str, str], cf: int = 6 * 10**17, *, entered: bool = True) -> Any:
    """Gateway routing snapshot per-qiToken (by ``to``), markets→CF, membership→entered.

    ``entered`` defaults to True so every supplied market is counted as collateral
    (the happy-path live tests below). The membership read targets the Comptroller
    with ``checkMembership(account, qiToken)``.
    """

    class _G:
        is_connected = True

        def __init__(self) -> None:
            self.selectors: list[str] = []

        def eth_call(self, chain: str, to: str, data: str, block: Any = None) -> str | None:
            self.selectors.append(data[:10])
            if data.startswith(_SNAPSHOT_SELECTOR):
                return snapshots.get(to.lower(), _snapshot(0, 0, _RATE_18))
            if data.startswith(_MARKETS_SELECTOR):
                return _markets(cf)
            if data.startswith(_MEMBERSHIP_SELECTOR):
                return _membership(entered)
            raise AssertionError(f"unexpected selector: {data[:10]}")

    return _G()


def test_generic_reader_whole_account_high_confidence() -> None:
    # Full live path: AVAX collateral 10e18 ($200, CF 0.6) + USDC debt 50e6 ($50)
    # over the registry-resolved whole-account market + the framework's
    # collaterals-map price injection (no named collateral). TRUE HF = 2.4.
    snapshots = {
        _QI_AVAX.lower(): _snapshot(10 * 10**8, 0, _RATE_18),
        _QI_USDC.lower(): _snapshot(0, 50 * 10**6, _RATE_6),
    }
    state = read_lending_account_state(
        protocol="benqi",
        chain=_CHAIN,
        wallet_address=_WALLET,
        market_id="benqi",
        gateway_client=_mock_gateway(snapshots),
        price_oracle=_PRICES,
        collateral_token=None,  # whole-account: framework prices every listed underlying
    )
    assert state is not None
    assert state.collateral_usd == Decimal("200")
    assert state.debt_usd == Decimal("50")
    assert state.health_factor == Decimal("2.4")


def test_capture_lending_pre_state_serializes_high_confidence_supply() -> None:
    # The runner-facing capture path: a SUPPLY of USDC lights up a HIGH-confidence
    # whole-account before-state (collateral populated, measured-zero debt). BENQI is
    # not the Aave family — no Aave-only keys, no Morpho lltv key.
    intent = MagicMock(intent_type="SUPPLY", protocol="benqi", token="USDC", market_id=None, chain=_CHAIN)
    snapshots = {_QI_USDC.lower(): _snapshot(1000 * 10**8, 0, _RATE_6)}
    state = capture_lending_pre_state(
        intent=intent,
        chain=_CHAIN,
        wallet_address=_WALLET,
        gateway_client=_mock_gateway(snapshots, cf=8 * 10**17),
        price_oracle=_PRICES,
    )
    assert state is not None
    serialized = lending_state_to_dict(state, protocol="benqi")
    assert serialized is not None
    assert serialized["collateral_usd"] == "1000"
    assert serialized["debt_usd"] == "0"
    assert serialized["health_factor"] == "999999"
    assert "e_mode_category" not in serialized
    assert "interest_rate_mode" not in serialized
    assert "lltv" not in serialized


def test_generic_reader_unsupported_chain_fails_closed() -> None:
    gateway = MagicMock()
    state = read_lending_account_state(
        protocol="benqi",
        chain="ethereum",  # BENQI is avalanche-only
        wallet_address=_WALLET,
        market_id="benqi",
        gateway_client=gateway,
        price_oracle=_PRICES,
    )
    assert state is None
    gateway.eth_call.assert_not_called()
