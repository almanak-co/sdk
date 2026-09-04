"""Golden-output tests for the runner intent log formatter."""

import re
from decimal import Decimal, InvalidOperation
from enum import Enum
from types import SimpleNamespace

import pytest

from almanak.core.intent_types import IntentType
from almanak.framework.intents.vocabulary import (
    BorrowIntent,
    PerpOpenIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawIntent,
)
from almanak.framework.runner.strategy_runner import _format_intent_for_log
from almanak.framework.utils.log_formatters import _emojis_enabled

_INTENT_ID = "intent-1234567890"


@pytest.fixture(autouse=True)
def _disable_log_emojis(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ALMANAK_LOG_EMOJIS", "false")
    _emojis_enabled.cache_clear()
    yield
    _emojis_enabled.cache_clear()


def _intent(intent_type: object, **fields: object) -> SimpleNamespace:
    return SimpleNamespace(intent_type=intent_type, intent_id=_INTENT_ID, **fields)


class TestFormatBorrowIntentForLog:
    """Tests that BorrowIntent summary uses correct field names."""

    def test_borrow_intent_shows_amount_and_token(self):
        """BorrowIntent summary should show borrow_amount and borrow_token, not N/A."""
        # Log-formatter fixture on a pre-built bundled intent -- model_construct
        # bypasses the bundled-collateral guard (formatting, not validation, is under test).
        intent = BorrowIntent.model_construct(
            protocol="aave_v3",
            collateral_token="WETH",
            collateral_amount=Decimal("1.0"),
            borrow_token="USDC",
            borrow_amount=Decimal("1000"),
        )
        result = _format_intent_for_log(intent)
        assert "N/A" not in result
        assert "1000" in result
        assert "USDC" in result
        assert "aave_v3" in result

    def test_borrow_intent_shows_collateral_info(self):
        """BorrowIntent summary should include collateral details."""
        # Log-formatter fixture on a pre-built bundled intent -- model_construct
        # bypasses the bundled-collateral guard (formatting, not validation, is under test).
        intent = BorrowIntent.model_construct(
            protocol="compound_v3",
            collateral_token="WETH",
            collateral_amount=Decimal("2.5"),
            borrow_token="USDC",
            borrow_amount=Decimal("500"),
        )
        result = _format_intent_for_log(intent)
        assert "WETH" in result
        assert "2.5" in result
        assert "compound_v3" in result

    def test_borrow_intent_chained_collateral(self):
        """BorrowIntent with collateral_amount='all' should show ALL."""
        # Log-formatter fixture on a pre-built chained-collateral intent --
        # model_construct bypasses the bundled-collateral guard (the "all" -> ALL
        # formatting machinery operates post-construction).
        intent = BorrowIntent.model_construct(
            protocol="aave_v3",
            collateral_token="WETH",
            collateral_amount="all",
            borrow_token="USDC",
            borrow_amount=Decimal("1000"),
        )
        result = _format_intent_for_log(intent)
        assert "ALL" in result
        assert "WETH" in result


class TestFormatOtherLendingIntentsForLog:
    """Verify other lending intents still format correctly after the fix."""

    def test_supply_intent_shows_amount(self):
        intent = SupplyIntent(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("5000"),
        )
        result = _format_intent_for_log(intent)
        assert "N/A" not in result
        assert "5000" in result
        assert "aave_v3" in result

    def test_repay_intent_shows_amount(self):
        intent = RepayIntent(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("500"),
        )
        result = _format_intent_for_log(intent)
        assert "N/A" not in result
        assert "500" in result

    def test_withdraw_intent_shows_all(self):
        intent = WithdrawIntent(
            protocol="aave_v3",
            token="USDC",
            amount="all",
        )
        result = _format_intent_for_log(intent)
        assert "ALL" in result
        assert "USDC" in result


_SPECIALIZED_GOLDENS = {
    IntentType.SWAP: (
        {
            "from_token": "USDC",
            "to_token": "WETH",
            "amount_usd": Decimal("1234.5"),
            "amount": Decimal("9"),
            "max_slippage": Decimal("0.005"),
            "protocol": "uniswap_v3",
        },
        "[SWAP]: $1,234.50 USDC → WETH (slippage: 0.50%) via uniswap_v3",
    ),
    IntentType.SUPPLY: (
        {
            "token": "USDC",
            "amount": Decimal("5000"),
            "protocol": "aave_v3",
            "use_as_collateral": False,
        },
        "[SUPPLY]: 5000 USDC to aave_v3",
    ),
    IntentType.BORROW: (
        {
            "borrow_token": "USDC",
            "borrow_amount": Decimal("1000"),
            "collateral_token": "WETH",
            "collateral_amount": "all",
            "protocol": "aave_v3",
        },
        "[BORROW]: 1000 USDC from aave_v3 (collateral: ALL WETH)",
    ),
    IntentType.WITHDRAW: (
        {"token": "USDC", "amount": "all", "protocol": "aave_v3"},
        "[WITHDRAW]: ALL USDC from aave_v3",
    ),
    IntentType.REPAY: (
        {"token": "USDC", "amount": Decimal("500"), "repay_full": False, "protocol": "aave_v3"},
        "[REPAY]: 500 USDC to aave_v3",
    ),
    IntentType.LP_OPEN: (
        {
            "pool": "WETH/USDC",
            "amount0": Decimal("1.25"),
            "amount1": Decimal("2500"),
            "range_lower": Decimal("1800"),
            "range_upper": Decimal("2200"),
            "protocol": "uniswap_v3",
        },
        "[LP_OPEN]: WETH/USDC (1.25, 2500) [1800 - 2200] via uniswap_v3",
    ),
    IntentType.LP_CLOSE: (
        {"position_id": "0x123456789abcdef", "protocol": "uniswap_v3"},
        "[LP_CLOSE]: position 0x123456... via uniswap_v3",
    ),
    IntentType.PERP_OPEN: (
        {
            "market": "ETH/USD",
            "is_long": False,
            "size_usd": Decimal("250"),
            "leverage": Decimal("3"),
            "protocol": "gmx_v2",
        },
        "[PERP_OPEN]: SHORT ETH/USD $250.00 (3x) via gmx_v2",
    ),
    IntentType.PERP_CLOSE: (
        {"market": "ETH/USD", "position_id": None, "protocol": "gmx_v2"},
        "[PERP_CLOSE]: ETH/USD position N/A... via gmx_v2",
    ),
    IntentType.BRIDGE: (
        {"token": "USDC", "amount": Decimal("25"), "from_chain": "base", "to_chain": "arbitrum"},
        "[BRIDGE]: 25 USDC base → arbitrum",
    ),
    IntentType.HOLD: (
        {"reason": "Waiting for signal"},
        "[HOLD]: Waiting for signal",
    ),
}

_FALLBACK_GOLDENS = {
    IntentType.PERP_CANCEL_ORDER: "[PERP_CANCEL_ORDER] (id=intent-1...)",
    IntentType.PERP_WITHDRAW: "[PERP_WITHDRAW] (id=intent-1...)",
    IntentType.ENSURE_BALANCE: "[ENSURE_BALANCE] (id=intent-1...)",
    IntentType.FLASH_LOAN: "[FLASH_LOAN] (id=intent-1...)",
    IntentType.STAKE: "[STAKE] (id=intent-1...)",
    IntentType.UNSTAKE: "[UNSTAKE] (id=intent-1...)",
    IntentType.PREDICTION_BUY: "[PREDICTION_BUY] (id=intent-1...)",
    IntentType.PREDICTION_SELL: "[PREDICTION_SELL] (id=intent-1...)",
    IntentType.PREDICTION_REDEEM: "[PREDICTION_REDEEM] (id=intent-1...)",
    IntentType.VAULT_DEPOSIT: "[VAULT_DEPOSIT] (id=intent-1...)",
    IntentType.VAULT_REDEEM: "[VAULT_REDEEM] (id=intent-1...)",
    IntentType.VAULT_REALLOCATE: "[VAULT_REALLOCATE] (id=intent-1...)",
    IntentType.VAULT_MANAGE: "[VAULT_MANAGE] (id=intent-1...)",
    IntentType.LP_COLLECT_FEES: "[LP_COLLECT_FEES] (id=intent-1...)",
    IntentType.WRAP_NATIVE: "[WRAP_NATIVE] (id=intent-1...)",
    IntentType.UNWRAP_NATIVE: "[UNWRAP_NATIVE] (id=intent-1...)",
    IntentType.DELEVERAGE: "[DELEVERAGE] (id=intent-1...)",
    IntentType.LIQUIDATE: "[LIQUIDATE] (id=intent-1...)",
    IntentType.OPEN_CDP: "[OPEN_CDP] (id=intent-1...)",
    IntentType.MINT_STABLE: "[MINT_STABLE] (id=intent-1...)",
    IntentType.REPAY_STABLE: "[REPAY_STABLE] (id=intent-1...)",
    IntentType.CLOSE_CDP: "[CLOSE_CDP] (id=intent-1...)",
}

_ALL_INTENT_GOLDENS = [
    pytest.param(intent_type, fields, expected, id=intent_type.value.lower())
    for intent_type, (fields, expected) in _SPECIALIZED_GOLDENS.items()
] + [
    pytest.param(intent_type, {}, expected, id=intent_type.value.lower())
    for intent_type, expected in _FALLBACK_GOLDENS.items()
]


def test_golden_table_covers_every_intent_type() -> None:
    assert set(_SPECIALIZED_GOLDENS) | set(_FALLBACK_GOLDENS) == set(IntentType)


@pytest.mark.parametrize(("intent_type", "fields", "expected"), _ALL_INTENT_GOLDENS)
def test_all_intent_types_have_stable_golden_output(
    intent_type: IntentType,
    fields: dict[str, object],
    expected: str,
) -> None:
    assert _format_intent_for_log(_intent(intent_type, **fields)) == expected


@pytest.mark.parametrize(
    ("intent", "expected"),
    [
        pytest.param(
            _intent(IntentType.SWAP, from_token="USDC", to_token="WETH", amount="all"),
            "[SWAP]: ALL USDC → WETH",
            id="swap-all-without-optionals",
        ),
        pytest.param(
            _intent(IntentType.SWAP, from_token="USDC", to_token="WETH", amount=0, max_slippage=0, protocol=None),
            "[SWAP]: N/A USDC → WETH",
            id="swap-falsy-values",
        ),
        pytest.param(
            _intent(IntentType.HOLD, from_token="USDC", to_token="WETH", amount="all"),
            "[HOLD]: ALL USDC → WETH",
            id="structural-swap-precedes-intent-type",
        ),
        pytest.param(
            _intent(IntentType.SUPPLY),
            "[SUPPLY]: N/A  to  (as collateral)",
            id="supply-missing-fields",
        ),
        pytest.param(
            _intent(IntentType.SUPPLY, as_collateral=None, use_as_collateral=True),
            "[SUPPLY]: N/A  to ",
            id="supply-legacy-none-precedence",
        ),
        pytest.param(
            _intent(
                IntentType.BORROW,
                borrow_token="USDC",
                borrow_amount={"second": 2, "first": 1},
                collateral_token="WETH",
                collateral_amount=Decimal("0"),
            ),
            "[BORROW]: {'second': 2, 'first': 1} USDC from ",
            id="borrow-dict-order-and-zero-collateral",
        ),
        pytest.param(
            _intent(IntentType.WITHDRAW, amount=None),
            "[WITHDRAW]: N/A  from ",
            id="withdraw-none",
        ),
        pytest.param(
            _intent(IntentType.REPAY, token="USDC", amount="all", repay_full=True),
            "[REPAY]: FULL USDC to ",
            id="repay-full-precedes-all",
        ),
        pytest.param(
            _intent(IntentType.LP_OPEN, range_lower=Decimal("1")),
            "[LP_OPEN]:  (0, 0) via ",
            id="lp-open-partial-range",
        ),
        pytest.param(
            _intent(IntentType.LP_CLOSE),
            "[LP_CLOSE]: position ... via ",
            id="lp-close-missing-fields",
        ),
        pytest.param(
            _intent(IntentType.PERP_OPEN, market="BTC/USD", direction="long", is_long=False),
            "[PERP_OPEN]: long BTC/USD N/A via ",
            id="perp-open-legacy-direction-precedence",
        ),
        pytest.param(
            _intent(IntentType.PERP_OPEN, market="BTC/USD"),
            "[PERP_OPEN]:  BTC/USD N/A via ",
            id="perp-open-missing-direction",
        ),
        pytest.param(
            _intent(IntentType.PERP_CLOSE, position_id=""),
            "[PERP_CLOSE]:  position N/A... via ",
            id="perp-close-empty-position",
        ),
        pytest.param(
            _intent(IntentType.BRIDGE, token="USDC", amount={"b": 2, "a": 1}),
            "[BRIDGE]: {'b': 2, 'a': 1} USDC  → ",
            id="bridge-dict-order",
        ),
        pytest.param(
            _intent(IntentType.HOLD),
            "[HOLD]: No action",
            id="hold-missing-reason",
        ),
        pytest.param(
            _intent(IntentType.HOLD, reason=None),
            "[HOLD]: None",
            id="hold-none-reason",
        ),
        pytest.param(
            _intent(IntentType.HOLD, reason={"z": 0, "a": 1}),
            "[HOLD]: {'z': 0, 'a': 1}",
            id="hold-dict-order",
        ),
    ],
)
def test_optional_and_malformed_fields_have_stable_golden_output(intent: SimpleNamespace, expected: str) -> None:
    assert _format_intent_for_log(intent) == expected


def test_chain_context_resolves_protocol_display_name() -> None:
    intent = _intent(
        IntentType.SWAP,
        from_token="USDC",
        to_token="WETH",
        amount=Decimal("1"),
        protocol="uniswap_v3",
    )

    assert _format_intent_for_log(intent, chain="mantle") == "[SWAP]: 1 USDC → WETH via Agni Finance"


@pytest.mark.parametrize(
    ("intent", "expected"),
    [
        pytest.param(
            SupplyIntent.model_construct(
                protocol="aave_v3",
                token="USDC",
                amount=Decimal("5"),
                use_as_collateral=False,
            ),
            "[SUPPLY]: 5 USDC to aave_v3",
            id="supply-use-as-collateral",
        ),
        pytest.param(
            PerpOpenIntent.model_construct(
                market="ETH/USD",
                collateral_token="USDC",
                collateral_amount=Decimal("100"),
                size_usd=Decimal("1000"),
                is_long=True,
                leverage=Decimal("10"),
                protocol="gmx_v2",
            ),
            "[PERP_OPEN]: LONG ETH/USD $1,000.00 (10x) via gmx_v2",
            id="perp-open-is-long",
        ),
    ],
)
def test_current_intent_model_fields_are_formatted(intent: object, expected: str) -> None:
    assert _format_intent_for_log(intent) == expected  # type: ignore[arg-type]


class _UnknownIntentType(Enum):
    CUSTOM = "CUSTOM"


class _UnhashableIntentTypeValue:
    __hash__ = None

    def upper(self) -> str:
        return "OBJECT"


def test_enum_fallback_truncates_id_without_rendering_arbitrary_fields() -> None:
    secret = "not-for-log-output"
    intent = _intent(_UnknownIntentType.CUSTOM, private_key=secret, metadata={"api_key": secret})

    result = _format_intent_for_log(intent)

    assert result == "[CUSTOM] (id=intent-1...)"
    assert secret not in result


def test_unhashable_object_intent_type_value_uses_fallback() -> None:
    intent_type = SimpleNamespace(value=_UnhashableIntentTypeValue())

    assert _format_intent_for_log(_intent(intent_type)) == "[OBJECT] (id=intent-1...)"


@pytest.mark.parametrize(
    ("intent", "error_type", "error_message"),
    [
        pytest.param({}, AttributeError, "'dict' object has no attribute 'intent_type'", id="dict-intent"),
        pytest.param(
            SimpleNamespace(intent_type="SWAP", intent_id=_INTENT_ID),
            AttributeError,
            "'str' object has no attribute 'value'",
            id="string-intent-type",
        ),
        pytest.param(
            SimpleNamespace(intent_type=SimpleNamespace(value={}), intent_id=_INTENT_ID),
            AttributeError,
            "'dict' object has no attribute 'upper'",
            id="dict-intent-type-value",
        ),
        pytest.param(
            SimpleNamespace(intent_type=_UnknownIntentType.CUSTOM),
            AttributeError,
            "'types.SimpleNamespace' object has no attribute 'intent_id'",
            id="fallback-without-id",
        ),
        pytest.param(
            _intent(IntentType.LP_CLOSE, position_id={}),
            KeyError,
            "slice(None, 8, None)",
            id="unsliceable-position-id",
        ),
        pytest.param(
            _intent(IntentType.LP_OPEN, range_lower="bad", range_upper=Decimal("2")),
            ValueError,
            "Unknown format code 'f' for object of type 'str'",
            id="unformattable-lp-range",
        ),
        pytest.param(
            _intent(IntentType.SWAP, from_token="USDC", to_token="WETH", amount_usd={"bad": "amount"}),
            InvalidOperation,
            None,
            id="invalid-usd-amount",
        ),
    ],
)
def test_malformed_intents_preserve_error_behavior(
    intent: object,
    error_type: type[Exception],
    error_message: str | None,
) -> None:
    match = re.escape(error_message) if error_message is not None else None
    with pytest.raises(error_type, match=match):
        _format_intent_for_log(intent)  # type: ignore[arg-type]
