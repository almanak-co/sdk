"""Contract tests for the bespoke Silo V2 account-state spec (VIB-4965).

Silo V2 has NO Aave-style ``getUserAccountData`` — each isolated ERC-4626 silo is
read separately. The spec
(:data:`~almanak.connectors.silo_v2.lending_read.ACCOUNT_STATE_READ_SPEC`) assembles
aggregate state from ``maxWithdraw`` on the deposit silo + ``maxRepay`` on the paired
debt silo, valued from the framework-injected price/decimals seam (Silo is not
USD-native, like Compound/Morpho). The registry drives it through the generic
:func:`~almanak.framework.accounting.lending_accounting.read_lending_account_state`.

These tests pin (without any network / mocking of the spec internals):

* the synthetic ``"<col>/<loan>"`` market-id catalogue + ``query_inputs_fn`` mapping
  (Silo intents carry no ``market_id``);
* ``build_calls`` selectors / targets / order (collateral silo first, paired debt
  silo second);
* ``reduce_calls`` absolute decoded collateral / debt / HF values, the no-debt
  sentinel, the collateral-only path, and the Empty ≠ Zero fail-closed branches;
* the registry's market-scoped target binding (``comet_address`` → collateral silo),
  ``market_params`` / ``valuation_roles``, and the end-to-end generic-reader path
  over a selector-routing mock gateway.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

from almanak.connectors._strategy_base.lending_read_base import (
    AccountStateQuery,
    EthCall,
)
from almanak.connectors._strategy_base.lending_read_registry import (
    AccountStatePlan,
    LendingReadRegistry,
)
from almanak.connectors.silo_v2.lending_read import (
    ACCOUNT_STATE_READ_SPEC,
    SILO_V2_ACCOUNT_STATE_MARKETS,
    _silo_query_inputs_from_intent,
    _synthesize_market_id,
)
from almanak.framework.accounting.lending_accounting import (
    _GENERIC_PRE_STATE_PROTOCOLS,
    capture_lending_pre_state,
    lending_state_to_dict,
    read_lending_account_state,
)

_CHAIN = "avalanche"
_WALLET = "0xABCDEF0123456789abcdef0123456789ABCDEF01"

# WAVAX/USDC market silos (verified on-chain in SILO_V2_MARKETS).
_USDC_SILO = "0xfA5f7d5BcD70dC2F031eE906fc692a9e19584CB0"
_WAVAX_SILO = "0xDa4b05e351696296060e6a1245C55e32DF8bFC84"

# Selectors the spec emits (verified on-chain, Avalanche).
_MAX_WITHDRAW_SELECTOR = "0xce96cb77"
_MAX_REPAY_SELECTOR = "0x5f301149"

_PRICES = {"WAVAX": Decimal("40"), "USDC": Decimal("1")}
_DECIMALS = {"WAVAX": 18, "USDC": 6}


def _word(value: int) -> str:
    return format(value, "064x")


def _uint_hex(value: int) -> str:
    return "0x" + _word(value)


# ---------------------------------------------------------------------------
# Synthetic market-id catalogue + intent → inputs
# ---------------------------------------------------------------------------


def test_catalogue_has_both_directions_per_market() -> None:
    table = SILO_V2_ACCOUNT_STATE_MARKETS["avalanche"]
    # WAVAX/USDC yields both directed entries.
    assert table["usdc/wavax"]["comet_address"] == _USDC_SILO
    assert table["usdc/wavax"]["debt_silo_address"] == _WAVAX_SILO
    assert table["usdc/wavax"]["collateral_token"] == "USDC"
    assert table["usdc/wavax"]["loan_token"] == "WAVAX"
    assert table["wavax/usdc"]["comet_address"] == _WAVAX_SILO
    assert table["wavax/usdc"]["debt_silo_address"] == _USDC_SILO
    assert table["wavax/usdc"]["collateral_token"] == "WAVAX"
    assert table["wavax/usdc"]["loan_token"] == "USDC"


def test_query_inputs_supply_uses_token_as_collateral() -> None:
    intent = MagicMock(intent_type="SUPPLY", token="USDC")
    assert _silo_query_inputs_from_intent(intent) == {"market_id": "usdc/wavax", "collateral_token": "USDC"}


def test_query_inputs_borrow_uses_collateral_and_borrow_tokens() -> None:
    intent = MagicMock(intent_type="BORROW", collateral_token="WAVAX", borrow_token="USDC", token=None)
    assert _silo_query_inputs_from_intent(intent) == {"market_id": "wavax/usdc", "collateral_token": "WAVAX"}


def test_query_inputs_repay_recovers_collateral_from_catalogue() -> None:
    # REPAY names only the repaid (debt) token; the collateral leg is recovered from
    # the resolved catalogue entry so the framework prices the right collateral.
    intent = MagicMock(intent_type="REPAY", token="USDC", collateral_token=None, borrow_token=None)
    assert _silo_query_inputs_from_intent(intent) == {"market_id": "wavax/usdc", "collateral_token": "WAVAX"}


def test_synthesize_market_id_unknown_token_fails_closed() -> None:
    assert _synthesize_market_id("NOTATOKEN", None) is None
    assert _synthesize_market_id(None, None) is None
    assert _synthesize_market_id("USDC", "USDC") is None  # no same-asset market


# ---------------------------------------------------------------------------
# build_calls — selectors / targets / order
# ---------------------------------------------------------------------------


def _spec_query(*, market_params: dict[str, Any] | None = None) -> AccountStateQuery:
    if market_params is None:
        market_params = {
            "comet_address": _USDC_SILO,
            "debt_silo_address": _WAVAX_SILO,
            "collateral_token": "USDC",
            "loan_token": "WAVAX",
        }
    return AccountStateQuery(
        chain=_CHAIN,
        wallet_address=_WALLET,
        position_manager_address=_USDC_SILO,
        market_id="usdc/wavax",
        prices=_PRICES,
        decimals=_DECIMALS,
        market_params=market_params,
        collateral_token="USDC",
        loan_token="WAVAX",
    )


def test_build_calls_emits_maxwithdraw_then_maxrepay() -> None:
    calls = ACCOUNT_STATE_READ_SPEC.build_calls(_spec_query())
    assert len(calls) == 2
    assert all(isinstance(c, EthCall) for c in calls)
    wallet_hex = _WALLET.lower().replace("0x", "").zfill(64)
    # 1. maxWithdraw(user) on the collateral (deposit) silo.
    assert calls[0].to == _USDC_SILO
    assert calls[0].data == _MAX_WITHDRAW_SELECTOR + wallet_hex
    # 2. maxRepay(user) on the paired debt silo.
    assert calls[1].to == _WAVAX_SILO
    assert calls[1].data == _MAX_REPAY_SELECTOR + wallet_hex


def test_build_calls_unbound_collateral_silo_fails_closed() -> None:
    query = _spec_query()
    query = AccountStateQuery(**{**query.__dict__, "position_manager_address": ""})
    assert ACCOUNT_STATE_READ_SPEC.build_calls(query) == []


def test_build_calls_no_paired_silo_emits_only_collateral_read() -> None:
    # A catalogue entry without a paired debt silo emits just the collateral read.
    query = _spec_query(market_params={"comet_address": _USDC_SILO, "collateral_token": "USDC", "loan_token": "WAVAX"})
    calls = ACCOUNT_STATE_READ_SPEC.build_calls(query)
    assert len(calls) == 1
    assert calls[0].to == _USDC_SILO


# ---------------------------------------------------------------------------
# reduce_calls — decoded collateral / debt / HF + fail-closed
# ---------------------------------------------------------------------------


def test_reduce_collateral_and_debt_concrete_values() -> None:
    # WAVAX collateral 5e18 ($40) + USDC debt 100e6 ($1): collateral $200, debt $100.
    query = AccountStateQuery(
        chain=_CHAIN,
        wallet_address=_WALLET,
        position_manager_address=_WAVAX_SILO,
        market_id="wavax/usdc",
        prices=_PRICES,
        decimals=_DECIMALS,
        market_params={"debt_silo_address": _USDC_SILO},
        collateral_token="WAVAX",
        loan_token="USDC",
    )
    state = ACCOUNT_STATE_READ_SPEC.reduce_calls(query, [_uint_hex(5 * 10**18), _uint_hex(100 * 10**6)])
    assert state is not None
    assert state.collateral_usd == Decimal("200")
    assert state.debt_usd == Decimal("100")
    assert state.health_factor == Decimal("2")  # 200 / 100
    assert state.liquidation_threshold_bps is None
    assert state.e_mode_category is None
    assert state.lltv is None
    assert state.family is None


def test_reduce_collateral_only_no_debt_sentinel() -> None:
    # Pure USDC collateral 1000e6 ($1), no debt: HF = sentinel; debt is a MEASURED 0.
    state = ACCOUNT_STATE_READ_SPEC.reduce_calls(_spec_query(), [_uint_hex(1000 * 10**6), _uint_hex(0)])
    assert state is not None
    assert state.collateral_usd == Decimal("1000")
    assert state.debt_usd == Decimal("0")
    assert state.health_factor == Decimal("999999")


def test_reduce_no_paired_debt_read_planned_yields_measured_zero_debt() -> None:
    # Single-call plan (NO paired debt silo): debt is a measured 0, never None —
    # Empty ≠ Zero distinguishes "no debt leg planned" from "unread".
    state = ACCOUNT_STATE_READ_SPEC.reduce_calls(_spec_query(), [_uint_hex(1000 * 10**6)])
    assert state is not None
    assert state.collateral_usd == Decimal("1000")
    assert state.debt_usd == Decimal("0")


def test_reduce_planned_debt_read_failed_fails_closed() -> None:
    # A debt read WAS planned (len(results) == 2, i.e. a paired debt silo exists) but
    # the RPC returned None. Must fail closed — never collapse a possibly-indebted
    # position to zero debt + perfect HF (Gemini review 2026-06, money-safety).
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(_spec_query(), [_uint_hex(1000 * 10**6), None]) is None


def test_reduce_planned_debt_read_short_blob_fails_closed() -> None:
    # A short/malformed debt blob on a planned debt read must also fail closed.
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(_spec_query(), [_uint_hex(1000 * 10**6), "0x" + "00" * 16]) is None


def test_reduce_missing_collateral_blob_fails_closed() -> None:
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(_spec_query(), [None, _uint_hex(0)]) is None


def test_reduce_short_collateral_blob_fails_closed() -> None:
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(_spec_query(), ["0x" + "00" * 16, _uint_hex(0)]) is None


def test_reduce_missing_collateral_price_fails_closed() -> None:
    query = AccountStateQuery(
        chain=_CHAIN,
        wallet_address=_WALLET,
        position_manager_address=_USDC_SILO,
        market_id="usdc/wavax",
        prices={"WAVAX": Decimal("40")},  # USDC (collateral) price MISSING
        decimals=_DECIMALS,
        market_params={"debt_silo_address": _WAVAX_SILO},
        collateral_token="USDC",
        loan_token="WAVAX",
    )
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(query, [_uint_hex(1000 * 10**6), _uint_hex(0)]) is None


def test_reduce_missing_loan_price_with_debt_blob_fails_closed() -> None:
    # A debt blob is present + a loan token is named, but its price is missing ⇒
    # fail closed (never fabricate the debt USD).
    query = AccountStateQuery(
        chain=_CHAIN,
        wallet_address=_WALLET,
        position_manager_address=_WAVAX_SILO,
        market_id="wavax/usdc",
        prices={"WAVAX": Decimal("40")},  # USDC (loan) price MISSING
        decimals=_DECIMALS,
        market_params={"debt_silo_address": _USDC_SILO},
        collateral_token="WAVAX",
        loan_token="USDC",
    )
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(query, [_uint_hex(5 * 10**18), _uint_hex(100 * 10**6)]) is None


# ---------------------------------------------------------------------------
# Registry resolution — market-scoped target binding + valuation seam
# ---------------------------------------------------------------------------


def test_registry_enabled_and_supports_account_state() -> None:
    assert "silo_v2" in _GENERIC_PRE_STATE_PROTOCOLS
    assert LendingReadRegistry.supports_account_state("silo_v2")


def test_position_manager_address_reports_market_scoped_existence() -> None:
    # Market-scoped (empty contract_kinds): a truthy sentinel when the chain has
    # published markets, None for an unsupported chain.
    assert LendingReadRegistry.position_manager_address("silo_v2", _CHAIN)
    assert LendingReadRegistry.position_manager_address("silo_v2", "ethereum") is None


def test_market_params_resolves_synthetic_entry() -> None:
    params = LendingReadRegistry.market_params("silo_v2", _CHAIN, "usdc/wavax")
    assert params is not None
    assert params["comet_address"] == _USDC_SILO
    assert params["debt_silo_address"] == _WAVAX_SILO
    assert params["collateral_token"] == "USDC"
    assert params["loan_token"] == "WAVAX"


def test_valuation_roles_names_both_legs() -> None:
    roles = LendingReadRegistry.valuation_roles("silo_v2", _CHAIN, "usdc/wavax")
    assert roles == (("collateral_token", "USDC"), ("loan_token", "WAVAX"))


def test_resolve_account_state_plan_binds_collateral_silo() -> None:
    query = _spec_query()
    plan = LendingReadRegistry.resolve_account_state_plan("silo_v2", query)
    assert isinstance(plan, AccountStatePlan)
    # Market-scoped target bound from the catalogue comet_address (the collateral silo).
    assert plan.query.position_manager_address == _USDC_SILO
    assert plan.calls[0].to == _USDC_SILO
    assert plan.calls[1].to == _WAVAX_SILO
    assert plan.reduce is ACCOUNT_STATE_READ_SPEC.reduce_calls


# ---------------------------------------------------------------------------
# End-to-end generic reader over a selector-routing mock gateway
# ---------------------------------------------------------------------------


def _mock_gateway(collateral_raw: int, debt_raw: int) -> Any:
    """Gateway routing maxWithdraw→collateral, maxRepay→debt (the live read path)."""

    class _G:
        is_connected = True

        def eth_call(self, chain: str, to: str, data: str, block: Any = None) -> str | None:
            if data.startswith(_MAX_WITHDRAW_SELECTOR):
                return _uint_hex(collateral_raw)
            if data.startswith(_MAX_REPAY_SELECTOR):
                return _uint_hex(debt_raw)
            raise AssertionError(f"unexpected selector in calldata: {data[:10]}")

    return _G()


def test_generic_reader_borrow_position_high_confidence() -> None:
    # WAVAX collateral 5e18 ($40) + USDC debt 50e6 ($1) via the registry-resolved
    # synthetic market — proves the full live path (market_params + valuation_roles +
    # gateway round-trip + reducer).
    state = read_lending_account_state(
        protocol="silo_v2",
        chain=_CHAIN,
        wallet_address=_WALLET,
        market_id="wavax/usdc",
        gateway_client=_mock_gateway(5 * 10**18, 50 * 10**6),
        price_oracle=_PRICES,
        collateral_token="WAVAX",
    )
    assert state is not None
    assert state.collateral_usd == Decimal("200")
    assert state.debt_usd == Decimal("50")
    assert state.health_factor == Decimal("4")  # 200 / 50


def test_capture_lending_pre_state_serializes_high_confidence_supply() -> None:
    # The runner-facing capture path: a SUPPLY of USDC (collateral-only) lights up
    # HIGH-confidence before-state (populated collateral, measured-zero debt).
    intent = MagicMock(
        intent_type="SUPPLY",
        protocol="silo_v2",
        token="USDC",
        market_id=None,
    )
    state = capture_lending_pre_state(
        intent=intent,
        chain=_CHAIN,
        wallet_address=_WALLET,
        gateway_client=_mock_gateway(1000 * 10**6, 0),
        price_oracle=_PRICES,
    )
    assert state is not None
    serialized = lending_state_to_dict(state, protocol="silo_v2")
    assert serialized is not None
    assert serialized["collateral_usd"] == "1000"
    assert serialized["debt_usd"] == "0"
    assert serialized["health_factor"] == "999999"
    # Silo is not the Aave family — no Aave-only keys, no Morpho lltv key.
    assert "e_mode_category" not in serialized
    assert "interest_rate_mode" not in serialized
    assert "lltv" not in serialized


def test_generic_reader_unsupported_chain_fails_closed() -> None:
    gateway = MagicMock()
    state = read_lending_account_state(
        protocol="silo_v2",
        chain="ethereum",  # no Silo V2 deployment in the catalogue
        wallet_address=_WALLET,
        market_id="usdc/wavax",
        gateway_client=gateway,
        price_oracle=_PRICES,
    )
    assert state is None
    gateway.eth_call.assert_not_called()
