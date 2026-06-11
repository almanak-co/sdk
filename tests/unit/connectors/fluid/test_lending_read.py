"""Unit tests for the Fluid fToken account-state read spec (VIB-5030).

Pins every branch of the pure planner/reducer pair: the two-call plan
(share balance + share-price probe), the fail-closed reductions (missing
reads, undecodable words, missing token/prices/decimals — Empty ≠ Zero),
and the measured-state shape (collateral marked, debt a measured zero,
health factor None). The live read is covered by the chain intent tests.
"""

from decimal import Decimal
from types import SimpleNamespace

from almanak.connectors._strategy_base.lending_read_base import AccountStateQuery
from almanak.connectors.fluid.lending_read import (
    _build_fluid_account_state_calls,
    _query_inputs_from_intent,
    _reduce_fluid_account_state,
)

FTOKEN = "0xf42f5795D9ac7e9D757dB633D693cD548Cfd9169"
WALLET = "0x" + "a" * 40


def _query(**overrides) -> AccountStateQuery:
    defaults = {
        "chain": "base",
        "wallet_address": WALLET,
        "position_manager_address": FTOKEN,
        "loan_token": "USDC",
        "prices": {"USDC": Decimal("1")},
        "decimals": {"USDC": 6},
    }
    defaults.update(overrides)
    return AccountStateQuery(**defaults)


def _word(value: int) -> str:
    return "0x" + format(value, "064x")


SHARES = 50_000_000  # 50 fUSDC shares (6 dp share token)
PROBE = 10**18 * 1005 // 1000  # 1.005 underlying per share, 1e18-scaled


class TestBuildCalls:
    def test_plans_balance_then_probe_against_the_ftoken(self):
        calls = _build_fluid_account_state_calls(_query())
        assert [c.to for c in calls] == [FTOKEN, FTOKEN]
        assert calls[0].data.startswith("0x70a08231")  # balanceOf(owner)
        assert calls[1].data.startswith("0x07a2d13a")  # convertToAssets(probe)

    def test_unbound_market_target_fails_closed(self):
        # Unknown market -> registry binds no fToken -> NO reads planned
        # (never read against a placeholder).
        assert _build_fluid_account_state_calls(_query(position_manager_address="")) == []


class TestReduce:
    def test_happy_path_measured_state_shape(self):
        state = _reduce_fluid_account_state(_query(), [_word(SHARES), _word(PROBE)])
        assert state is not None
        # 50 shares × 1.005 underlying/share at $1 = $50.25
        assert state.collateral_usd == Decimal("50.25")
        assert state.debt_usd == Decimal("0")  # measured zero — no debt leg
        assert state.health_factor is None  # unmeasured — no liquidation surface
        assert state.liquidation_threshold_bps is None
        assert state.lltv is None

    def test_missing_reads_fail_closed(self):
        assert _reduce_fluid_account_state(_query(), []) is None
        assert _reduce_fluid_account_state(_query(), [_word(SHARES)]) is None

    def test_undecodable_words_fail_closed(self):
        assert _reduce_fluid_account_state(_query(), [None, _word(PROBE)]) is None
        assert _reduce_fluid_account_state(_query(), [_word(SHARES), "0xzz"]) is None
        assert _reduce_fluid_account_state(_query(), ["0x1234", _word(PROBE)]) is None  # short word

    def test_missing_valuation_inputs_fail_closed(self):
        ok = [_word(SHARES), _word(PROBE)]
        assert _reduce_fluid_account_state(_query(loan_token=None), ok) is None
        assert _reduce_fluid_account_state(_query(prices=None), ok) is None
        assert _reduce_fluid_account_state(_query(decimals=None), ok) is None
        assert _reduce_fluid_account_state(_query(prices={"WETH": Decimal("3000")}), ok) is None
        assert _reduce_fluid_account_state(_query(decimals={"WETH": 18}), ok) is None
        # Empty ≠ Zero: a None price must fail closed, never value at zero.
        assert _reduce_fluid_account_state(_query(prices={"USDC": None}), ok) is None

    def test_zero_shares_is_a_measured_empty_position(self):
        state = _reduce_fluid_account_state(_query(), [_word(0), _word(PROBE)])
        assert state is not None
        assert state.collateral_usd == Decimal("0")  # measured zero, not a fabricated one
        assert state.debt_usd == Decimal("0")


class TestQueryInputs:
    def test_market_id_derives_from_intent_token(self):
        intent = SimpleNamespace(token="USDC", market_id="0xshould-be-ignored")
        assert _query_inputs_from_intent(intent) == {"market_id": "USDC"}

    def test_missing_token_yields_none(self):
        assert _query_inputs_from_intent(SimpleNamespace()) == {"market_id": None}
