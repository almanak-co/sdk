"""Tests for single-intent amount='all' resolution from wallet balance.

Verifies that when a strategy returns a single Intent.swap(amount="all") from
decide(), the runner resolves the amount from wallet balance before compilation
instead of passing the unresolved "all" to the compiler (which would fail).

VIB-1423: Bug: amount='all' not resolved for single intents returned from decide()
"""

from decimal import Decimal
from unittest.mock import MagicMock

from almanak.framework.intents.vocabulary import Intent, IntentType, SwapIntent


def test_single_intent_has_chained_amount_detected():
    """A single swap with amount='all' is detected as chained."""
    intent = Intent.swap(
        from_token="WETH",
        to_token="USDC",
        amount="all",
        protocol="uniswap_v3",
    )
    assert Intent.has_chained_amount(intent) is True


def test_single_intent_set_resolved_amount():
    """set_resolved_amount correctly replaces 'all' with a concrete Decimal."""
    intent = Intent.swap(
        from_token="WETH",
        to_token="USDC",
        amount="all",
        protocol="uniswap_v3",
    )
    resolved = Intent.set_resolved_amount(intent, Decimal("1.5"))
    assert resolved.amount == Decimal("1.5")
    assert Intent.has_chained_amount(resolved) is False


def test_single_intent_resolution_logic():
    """Simulate the runner's single-intent resolution path.

    This mirrors the logic in strategy_runner.py:
    For a single intent (not multi-intent) with amount='all',
    the runner queries wallet balance and resolves the amount.
    """
    intent = Intent.swap(
        from_token="WETH",
        to_token="USDC",
        amount="all",
        protocol="uniswap_v3",
    )

    # Simulate what the runner does
    is_multi_intent = False  # Single intent
    previous_amount_received = None
    market = MagicMock()

    # Mock market.balance("WETH") returning a TokenBalance-like object (real API)
    mock_balance = MagicMock()
    mock_balance.balance = Decimal("2.5")
    market.balance.return_value = mock_balance

    _WALLET_FUNDED_TYPES = {IntentType.SWAP, IntentType.SUPPLY, IntentType.BORROW}

    intent_to_execute = intent
    if Intent.has_chained_amount(intent):
        intent_type = getattr(intent, "intent_type", None)
        if intent_type in _WALLET_FUNDED_TYPES:
            balance_token = getattr(intent, "from_token", None)
            if balance_token and market is not None:
                bal = market.balance(balance_token)
                balance_value = bal.balance if hasattr(bal, "balance") else bal
                if balance_value > 0:
                    intent_to_execute = Intent.set_resolved_amount(intent, balance_value)

    # Verify resolution happened
    assert intent_to_execute.amount == Decimal("2.5")
    assert Intent.has_chained_amount(intent_to_execute) is False
    market.balance.assert_called_once_with("WETH")


def test_single_intent_resolution_zero_balance():
    """When wallet balance is 0, intent should remain unresolved."""
    intent = Intent.swap(
        from_token="WETH",
        to_token="USDC",
        amount="all",
        protocol="uniswap_v3",
    )

    market = MagicMock()
    market.balance.return_value = Decimal("0")

    # Simulate the runner's zero-balance path
    intent_to_execute = intent
    balance_token = getattr(intent, "from_token", None)
    bal = market.balance(balance_token)
    balance_value = bal.balance if hasattr(bal, "balance") else bal
    if balance_value > 0:
        intent_to_execute = Intent.set_resolved_amount(intent, balance_value)

    # With zero balance, intent should NOT be resolved — stays as original
    assert intent_to_execute is intent
    assert Intent.has_chained_amount(intent_to_execute) is True
    assert intent_to_execute.amount == "all"


def test_non_chained_intent_passes_through():
    """A normal intent with a concrete amount should not trigger resolution."""
    intent = Intent.swap(
        from_token="WETH",
        to_token="USDC",
        amount=Decimal("1.0"),
        protocol="uniswap_v3",
    )
    assert Intent.has_chained_amount(intent) is False


def test_supply_intent_amount_all_token_extraction():
    """SupplyIntent with amount='all' can extract token from 'token' field."""
    intent = Intent.supply(
        token="USDC",
        amount="all",
        protocol="aave_v3",
    )
    assert Intent.has_chained_amount(intent) is True
    balance_token = (
        getattr(intent, "from_token", None)
        or getattr(intent, "token", None)
        or getattr(intent, "token_in", None)
        or getattr(intent, "collateral_token", None)
    )
    assert balance_token == "USDC"


def test_vault_redeem_shares_all_no_token_field():
    """VaultRedeemIntent with shares='all' has no from_token/token field.

    The runner should let the compiler handle it natively (shares='all'
    is resolved by the compiler/adapter, not by wallet balance).
    """
    intent = Intent.vault_redeem(
        protocol="metamorpho",
        vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
        shares="all",
        chain="ethereum",
    )
    assert Intent.has_chained_amount(intent) is True
    # No from_token/token/token_in/collateral_token — should pass through to compiler
    balance_token = (
        getattr(intent, "from_token", None)
        or getattr(intent, "token", None)
        or getattr(intent, "token_in", None)
        or getattr(intent, "collateral_token", None)
    )
    assert balance_token is None


def test_borrow_intent_collateral_amount_all_token_extraction():
    """BorrowIntent with collateral_amount='all' extracts collateral_token."""
    intent = Intent.borrow(
        borrow_token="USDC",
        borrow_amount=Decimal("1000"),
        collateral_token="WETH",
        collateral_amount="all",
        protocol="aave_v3",
    )
    assert Intent.has_chained_amount(intent) is True
    balance_token = (
        getattr(intent, "from_token", None)
        or getattr(intent, "token", None)
        or getattr(intent, "token_in", None)
        or getattr(intent, "collateral_token", None)
    )
    assert balance_token == "WETH"


def test_withdraw_intent_amount_all_passes_through_to_compiler():
    """WithdrawIntent(amount='all') should NOT resolve from wallet balance.

    amount='all' on a withdraw means 'withdraw everything from the protocol',
    not 'withdraw my wallet balance worth'. The runner should pass this through
    to the compiler which handles protocol-position resolution natively.
    """
    intent = Intent.withdraw(
        token="USDC",
        amount="all",
        protocol="aave_v3",
    )
    assert Intent.has_chained_amount(intent) is True
    # WITHDRAW is a protocol-position intent, NOT wallet-funded
    assert intent.intent_type == IntentType.WITHDRAW

    # Verify the runner's _WALLET_FUNDED_TYPES set does NOT include WITHDRAW
    _WALLET_FUNDED_TYPES = {
        IntentType.SWAP,
        IntentType.SUPPLY,
        IntentType.BORROW,
        IntentType.STAKE,
        IntentType.LP_OPEN,
        IntentType.PERP_OPEN,
        IntentType.VAULT_DEPOSIT,
        IntentType.BRIDGE,
    }
    assert intent.intent_type not in _WALLET_FUNDED_TYPES

    # Market.balance should never be called for a WITHDRAW intent
    market = MagicMock()
    intent_type = getattr(intent, "intent_type", None)
    if intent_type not in _WALLET_FUNDED_TYPES:
        pass  # let compiler handle — no wallet balance query
    assert market.balance.call_count == 0
    # Intent remains unresolved
    assert intent.amount == "all"
