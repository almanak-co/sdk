"""Contract tests for the bespoke Euler V2 account-state spec (VIB-4966).

Euler V2 has NO Aave-style ``getUserAccountData`` — it is a vault-based ERC-4626
model coordinated by the Ethereum Vault Connector (EVC), with independent per-asset
vaults. The spec
(:data:`~almanak.connectors.euler_v2.lending_read.ACCOUNT_STATE_READ_SPEC`) assembles
aggregate state from ``maxWithdraw`` on the deposit vault + ``debtOf`` on the
borrow/controller vault, valued from the framework-injected price/decimals seam
(Euler is not USD-native, like Compound/Morpho/Silo). The registry drives it through
the generic
:func:`~almanak.framework.accounting.lending_accounting.read_lending_account_state`.

These tests pin (without any network / mocking of the spec internals):

* the synthetic ``"<col>"`` (collateral-only) + ``"<col>/<loan>"`` (borrow) market-id
  catalogue + ``query_inputs_fn`` mapping (Euler intents carry no ``market_id``);
* ``build_calls`` selectors / targets / order (collateral vault first, borrow vault
  second; no debt read on a collateral-only id);
* ``reduce_calls`` absolute decoded collateral / debt / HF values, the no-debt
  sentinel, the collateral-only path, and the Empty ≠ Zero fail-closed branches;
* the registry's market-scoped target binding (``comet_address`` → collateral vault),
  ``market_params`` / ``valuation_roles``, and the end-to-end generic-reader path over
  a selector-routing mock gateway.
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
from almanak.connectors.euler_v2.lending_read import (
    ACCOUNT_STATE_READ_SPEC,
    EULER_V2_ACCOUNT_STATE_MARKETS,
    _euler_query_inputs_from_intent,
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

# Preferred avalanche vaults (verified in EULER_V2_VAULTS_BY_CHAIN['avalanche']).
_USDC_VAULT = "0x37ca03aD51B8ff79aAD35FadaCBA4CEDF0C3e74e"  # eUSDC-19 (preferred)
_WAVAX_VAULT = "0x6c718a70239fA548c0bD268fE88F37EBE8b6E2ea"  # eWAVAX-2 (preferred)

# Selectors the spec emits (match euler_v2/adapter.py constants).
_MAX_WITHDRAW_SELECTOR = "0xce96cb77"
_DEBT_OF_SELECTOR = "0xd283e75f"

_PRICES = {"WAVAX": Decimal("40"), "USDC": Decimal("1")}
_DECIMALS = {"WAVAX": 18, "USDC": 6}


def _word(value: int) -> str:
    return format(value, "064x")


def _uint_hex(value: int) -> str:
    return "0x" + _word(value)


# ---------------------------------------------------------------------------
# Synthetic market-id catalogue + intent → inputs
# ---------------------------------------------------------------------------


def test_catalogue_has_collateral_only_and_directed_pair_entries() -> None:
    table = EULER_V2_ACCOUNT_STATE_MARKETS["avalanche"]
    # Collateral-only entries (SUPPLY / WITHDRAW): no debt vault, loan_token None.
    assert table["usdc"]["comet_address"] == _USDC_VAULT
    assert table["usdc"]["collateral_token"] == "USDC"
    assert table["usdc"]["loan_token"] is None
    assert "debt_vault_address" not in table["usdc"]
    # Directed pair entries (BORROW / REPAY): collateral vault + paired borrow vault.
    assert table["wavax/usdc"]["comet_address"] == _WAVAX_VAULT
    assert table["wavax/usdc"]["debt_vault_address"] == _USDC_VAULT
    assert table["wavax/usdc"]["collateral_token"] == "WAVAX"
    assert table["wavax/usdc"]["loan_token"] == "USDC"


def test_ethereum_catalogue_has_only_usdc_collateral() -> None:
    # Ethereum registers only eUSDC-2, so only the collateral-only "usdc" entry
    # exists (no directed pair — a pair needs two distinct underlying symbols).
    table = EULER_V2_ACCOUNT_STATE_MARKETS["ethereum"]
    assert set(table.keys()) == {"usdc"}
    assert table["usdc"]["comet_address"] == "0x797DD80692c3b2dAdabCe8e30C07fDE5307D48a9"
    assert table["usdc"]["loan_token"] is None


def test_query_inputs_supply_uses_token_as_collateral_no_debt() -> None:
    intent = MagicMock(intent_type="SUPPLY", token="USDC", chain="avalanche")
    assert _euler_query_inputs_from_intent(intent) == {"market_id": "usdc", "collateral_token": "USDC"}


def test_query_inputs_borrow_uses_collateral_and_borrow_tokens() -> None:
    intent = MagicMock(
        intent_type="BORROW", collateral_token="WAVAX", borrow_token="USDC", token=None, chain="avalanche"
    )
    assert _euler_query_inputs_from_intent(intent) == {"market_id": "wavax/usdc", "collateral_token": "WAVAX"}


def test_query_inputs_repay_ambiguous_debt_only_fails_closed() -> None:
    # REPAY names only the repaid (debt) token. On avalanche many collaterals back
    # USDC debt, so the collateral leg is ambiguous → fail closed (market_id=None),
    # never guess a collateral vault (Empty ≠ Zero — Gemini/CodeRabbit money-safety).
    # The intent honestly degrades to ESTIMATED rather than reporting a wrong position.
    intent = MagicMock(intent_type="REPAY", token="USDC", collateral_token=None, borrow_token=None, chain="avalanche")
    out = _euler_query_inputs_from_intent(intent)
    assert out["market_id"] is None
    assert out["collateral_token"] is None


def test_synthesize_market_id_unknown_token_fails_closed() -> None:
    assert _synthesize_market_id(_CHAIN, "NOTATOKEN", None) is None
    assert _synthesize_market_id(_CHAIN, None, None) is None
    assert _synthesize_market_id(_CHAIN, "USDC", "USDC") is None  # no same-asset pair
    assert _synthesize_market_id("base", "USDC", None) is None  # unsupported chain


def test_synthesize_market_id_debt_only_ambiguous_fails_closed() -> None:
    # Multiple avalanche collaterals back USDC debt ⇒ debt-only resolution is
    # ambiguous ⇒ None (never bind an arbitrary collateral vault).
    table = EULER_V2_ACCOUNT_STATE_MARKETS["avalanche"]
    usdc_debt_pairs = [
        m for m, p in table.items() if isinstance(p.get("loan_token"), str) and p["loan_token"] == "USDC"
    ]
    assert len(usdc_debt_pairs) > 1, "precondition: avalanche has >1 collateral backing USDC debt"
    assert _synthesize_market_id("avalanche", None, "USDC") is None


def test_synthesize_market_id_debt_only_unique_match_resolves() -> None:
    # A debt token backed by EXACTLY ONE collateral resolves to that directed pair
    # (the unambiguous branch). Verified against a synthetic single-pair table.
    import almanak.connectors.euler_v2.lending_read as mod

    original = mod.EULER_V2_ACCOUNT_STATE_MARKETS
    try:
        mod.EULER_V2_ACCOUNT_STATE_MARKETS = {
            "testchain": {
                "wavax": {"comet_address": _WAVAX_VAULT, "collateral_token": "WAVAX", "loan_token": None},
                "wavax/usdc": {
                    "comet_address": _WAVAX_VAULT,
                    "debt_vault_address": _USDC_VAULT,
                    "collateral_token": "WAVAX",
                    "loan_token": "USDC",
                },
            }
        }
        assert mod._synthesize_market_id("testchain", None, "USDC") == "wavax/usdc"
    finally:
        mod.EULER_V2_ACCOUNT_STATE_MARKETS = original


# ---------------------------------------------------------------------------
# build_calls — selectors / targets / order
# ---------------------------------------------------------------------------


def _spec_query(*, market_params: dict[str, Any] | None = None) -> AccountStateQuery:
    if market_params is None:
        market_params = {
            "comet_address": _WAVAX_VAULT,
            "debt_vault_address": _USDC_VAULT,
            "collateral_token": "WAVAX",
            "loan_token": "USDC",
        }
    return AccountStateQuery(
        chain=_CHAIN,
        wallet_address=_WALLET,
        position_manager_address=_WAVAX_VAULT,
        market_id="wavax/usdc",
        prices=_PRICES,
        decimals=_DECIMALS,
        market_params=market_params,
        collateral_token="WAVAX",
        loan_token="USDC",
    )


def test_build_calls_emits_maxwithdraw_then_debtof() -> None:
    calls = ACCOUNT_STATE_READ_SPEC.build_calls(_spec_query())
    assert len(calls) == 2
    assert all(isinstance(c, EthCall) for c in calls)
    wallet_hex = _WALLET.lower().replace("0x", "").zfill(64)
    # 1. maxWithdraw(user) on the collateral (deposit) vault.
    assert calls[0].to == _WAVAX_VAULT
    assert calls[0].data == _MAX_WITHDRAW_SELECTOR + wallet_hex
    # 2. debtOf(user) on the paired borrow (controller) vault.
    assert calls[1].to == _USDC_VAULT
    assert calls[1].data == _DEBT_OF_SELECTOR + wallet_hex


def test_build_calls_unbound_collateral_vault_fails_closed() -> None:
    query = _spec_query()
    query = AccountStateQuery(**{**query.__dict__, "position_manager_address": ""})
    assert ACCOUNT_STATE_READ_SPEC.build_calls(query) == []


def test_build_calls_collateral_only_emits_only_collateral_read() -> None:
    # A collateral-only entry (no debt vault) emits just the collateral read.
    query = AccountStateQuery(
        chain=_CHAIN,
        wallet_address=_WALLET,
        position_manager_address=_USDC_VAULT,
        market_id="usdc",
        prices=_PRICES,
        decimals=_DECIMALS,
        market_params={"comet_address": _USDC_VAULT, "collateral_token": "USDC", "loan_token": None},
        collateral_token="USDC",
        loan_token=None,
    )
    calls = ACCOUNT_STATE_READ_SPEC.build_calls(query)
    assert len(calls) == 1
    assert calls[0].to == _USDC_VAULT


# ---------------------------------------------------------------------------
# reduce_calls — decoded collateral / debt / HF + fail-closed
# ---------------------------------------------------------------------------


def test_reduce_collateral_and_debt_concrete_values() -> None:
    # WAVAX collateral 5e18 ($40) + USDC debt 100e6 ($1): collateral $200, debt $100.
    state = ACCOUNT_STATE_READ_SPEC.reduce_calls(_spec_query(), [_uint_hex(5 * 10**18), _uint_hex(100 * 10**6)])
    assert state is not None
    assert state.collateral_usd == Decimal("200")
    assert state.debt_usd == Decimal("100")
    assert state.health_factor == Decimal("2")  # 200 / 100
    assert state.liquidation_threshold_bps is None
    assert state.e_mode_category is None
    assert state.lltv is None
    assert state.family is None


def _collateral_only_query() -> AccountStateQuery:
    return AccountStateQuery(
        chain=_CHAIN,
        wallet_address=_WALLET,
        position_manager_address=_USDC_VAULT,
        market_id="usdc",
        prices=_PRICES,
        decimals=_DECIMALS,
        market_params={"comet_address": _USDC_VAULT, "collateral_token": "USDC", "loan_token": None},
        collateral_token="USDC",
        loan_token=None,
    )


def test_reduce_collateral_only_no_debt_read_yields_measured_zero_debt() -> None:
    # Single-call plan (collateral-only, NO debt vault): debt is a measured 0, never
    # None — Empty ≠ Zero distinguishes "no debt leg planned" from "unread".
    state = ACCOUNT_STATE_READ_SPEC.reduce_calls(_collateral_only_query(), [_uint_hex(1000 * 10**6)])
    assert state is not None
    assert state.collateral_usd == Decimal("1000")
    assert state.debt_usd == Decimal("0")
    assert state.health_factor == Decimal("999999")


def test_reduce_planned_debt_read_failed_fails_closed() -> None:
    # A debt read WAS planned (len(results) == 2, i.e. a paired borrow vault exists)
    # but the RPC returned None. Must fail closed — never collapse a possibly-indebted
    # position to zero debt + perfect HF (money-safety).
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(_spec_query(), [_uint_hex(5 * 10**18), None]) is None


def test_reduce_planned_debt_read_short_blob_fails_closed() -> None:
    # A short/malformed debt blob on a planned debt read must also fail closed.
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(_spec_query(), [_uint_hex(5 * 10**18), "0x" + "00" * 16]) is None


def test_reduce_missing_collateral_blob_fails_closed() -> None:
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(_spec_query(), [None, _uint_hex(0)]) is None


def test_reduce_short_collateral_blob_fails_closed() -> None:
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(_spec_query(), ["0x" + "00" * 16, _uint_hex(0)]) is None


def test_reduce_missing_collateral_price_fails_closed() -> None:
    query = AccountStateQuery(
        chain=_CHAIN,
        wallet_address=_WALLET,
        position_manager_address=_WAVAX_VAULT,
        market_id="wavax/usdc",
        prices={"USDC": Decimal("1")},  # WAVAX (collateral) price MISSING
        decimals=_DECIMALS,
        market_params={"debt_vault_address": _USDC_VAULT},
        collateral_token="WAVAX",
        loan_token="USDC",
    )
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(query, [_uint_hex(5 * 10**18), _uint_hex(0)]) is None


def test_reduce_missing_loan_price_with_debt_blob_fails_closed() -> None:
    # A debt blob is present + a loan token is named, but its price is missing ⇒
    # fail closed (never fabricate the debt USD).
    query = AccountStateQuery(
        chain=_CHAIN,
        wallet_address=_WALLET,
        position_manager_address=_WAVAX_VAULT,
        market_id="wavax/usdc",
        prices={"WAVAX": Decimal("40")},  # USDC (loan) price MISSING
        decimals=_DECIMALS,
        market_params={"debt_vault_address": _USDC_VAULT},
        collateral_token="WAVAX",
        loan_token="USDC",
    )
    assert ACCOUNT_STATE_READ_SPEC.reduce_calls(query, [_uint_hex(5 * 10**18), _uint_hex(100 * 10**6)]) is None


# ---------------------------------------------------------------------------
# Registry resolution — market-scoped target binding + valuation seam
# ---------------------------------------------------------------------------


def test_registry_enabled_and_supports_account_state() -> None:
    assert "euler_v2" in _GENERIC_PRE_STATE_PROTOCOLS
    assert LendingReadRegistry.supports_account_state("euler_v2")


def test_position_manager_address_reports_market_scoped_existence() -> None:
    # Market-scoped (empty contract_kinds): a truthy sentinel when the chain has
    # published vaults, None for an unsupported chain.
    assert LendingReadRegistry.position_manager_address("euler_v2", _CHAIN)
    assert LendingReadRegistry.position_manager_address("euler_v2", "ethereum")
    assert LendingReadRegistry.position_manager_address("euler_v2", "base") is None


def test_market_params_resolves_directed_pair_entry() -> None:
    params = LendingReadRegistry.market_params("euler_v2", _CHAIN, "wavax/usdc")
    assert params is not None
    assert params["comet_address"] == _WAVAX_VAULT
    assert params["debt_vault_address"] == _USDC_VAULT
    assert params["collateral_token"] == "WAVAX"
    assert params["loan_token"] == "USDC"


def test_valuation_roles_names_both_legs_for_directed_pair() -> None:
    roles = LendingReadRegistry.valuation_roles("euler_v2", _CHAIN, "wavax/usdc")
    assert roles == (("collateral_token", "WAVAX"), ("loan_token", "USDC"))


def test_valuation_roles_empty_for_collateral_only() -> None:
    # Collateral-only entry has loan_token=None ⇒ the declared loan role can't be
    # resolved, so valuation_roles returns () (fail-closed for the role loop). The
    # generic reader prices the collateral via the separate collateral_token path.
    assert LendingReadRegistry.valuation_roles("euler_v2", _CHAIN, "usdc") == ()


def test_resolve_account_state_plan_binds_collateral_vault() -> None:
    query = _spec_query()
    plan = LendingReadRegistry.resolve_account_state_plan("euler_v2", query)
    assert isinstance(plan, AccountStatePlan)
    # Market-scoped target bound from the catalogue comet_address (the collateral vault).
    assert plan.query.position_manager_address == _WAVAX_VAULT
    assert plan.calls[0].to == _WAVAX_VAULT
    assert plan.calls[1].to == _USDC_VAULT
    assert plan.reduce is ACCOUNT_STATE_READ_SPEC.reduce_calls


# ---------------------------------------------------------------------------
# End-to-end generic reader over a selector-routing mock gateway
# ---------------------------------------------------------------------------


def _mock_gateway(collateral_raw: int, debt_raw: int) -> Any:
    """Gateway routing maxWithdraw→collateral, debtOf→debt (the live read path).

    Records every selector seen in ``.selectors`` so tests can assert the exact
    read shape (e.g. that the collateral-only path issues no ``debtOf``).
    """

    class _G:
        is_connected = True

        def __init__(self) -> None:
            self.selectors: list[str] = []

        def eth_call(self, chain: str, to: str, data: str, block: Any = None) -> str | None:
            self.selectors.append(data[:10])
            if data.startswith(_MAX_WITHDRAW_SELECTOR):
                return _uint_hex(collateral_raw)
            if data.startswith(_DEBT_OF_SELECTOR):
                return _uint_hex(debt_raw)
            raise AssertionError(f"unexpected selector in calldata: {data[:10]}")

    return _G()


def test_generic_reader_borrow_position_high_confidence() -> None:
    # WAVAX collateral 5e18 ($40) + USDC debt 50e6 ($1) via the registry-resolved
    # synthetic market — proves the full live path (market_params + valuation_roles +
    # gateway round-trip + reducer).
    state = read_lending_account_state(
        protocol="euler_v2",
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
    # HIGH-confidence before-state (populated collateral, measured-zero debt). The
    # collateral-only path issues exactly one read (no debtOf).
    intent = MagicMock(
        intent_type="SUPPLY",
        protocol="euler_v2",
        token="USDC",
        market_id=None,
        chain=_CHAIN,
    )
    gateway = _mock_gateway(1000 * 10**6, 0)
    state = capture_lending_pre_state(
        intent=intent,
        chain=_CHAIN,
        wallet_address=_WALLET,
        gateway_client=gateway,
        price_oracle=_PRICES,
    )
    assert state is not None
    # Collateral-only SUPPLY issues EXACTLY ONE read (maxWithdraw); no debtOf is
    # planned for a position with no controller vault (debt is a measured zero).
    assert gateway.selectors == [_MAX_WITHDRAW_SELECTOR]
    assert _DEBT_OF_SELECTOR not in gateway.selectors
    serialized = lending_state_to_dict(state, protocol="euler_v2")
    assert serialized is not None
    assert serialized["collateral_usd"] == "1000"
    assert serialized["debt_usd"] == "0"
    assert serialized["health_factor"] == "999999"
    # Euler is not the Aave family — no Aave-only keys, no Morpho lltv key.
    assert "e_mode_category" not in serialized
    assert "interest_rate_mode" not in serialized
    assert "lltv" not in serialized


def test_generic_reader_unsupported_chain_fails_closed() -> None:
    gateway = MagicMock()
    state = read_lending_account_state(
        protocol="euler_v2",
        chain="base",  # no Euler V2 deployment in the catalogue
        wallet_address=_WALLET,
        market_id="usdc",
        gateway_client=gateway,
        price_oracle=_PRICES,
    )
    assert state is None
    gateway.eth_call.assert_not_called()
