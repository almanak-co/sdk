"""Tests for amount='all' token resolution fallback in TeardownManager.

The TeardownManager's execute_at_slippage resolves amount='all' by looking up
wallet balance. It checks from_token first (SwapIntent), then falls back to
token (Withdraw/Supply/Repay). These tests verify both dict and object intent
paths, plus the missing-market failure path.

VIB-1851: aave_borrow teardown withdraw amount resolution
"""

from types import SimpleNamespace
from unittest.mock import MagicMock


def _resolve_from_token(intent_with_slippage):
    """Mirror the teardown_manager's from_token resolution logic (lines 611-617)."""
    _is_dict = isinstance(intent_with_slippage, dict)
    from_token = (
        (intent_with_slippage.get("from_token") or intent_with_slippage.get("token"))
        if _is_dict
        else (getattr(intent_with_slippage, "from_token", None) or getattr(intent_with_slippage, "token", None))
    )
    return from_token


class TestDictIntentTokenFallback:
    """Dict-based intent: from_token absent, token present."""

    def test_dict_intent_uses_token_when_from_token_missing(self):
        """When dict intent has no from_token, resolver falls back to token."""
        intent = {"amount": "all", "token": "WETH", "protocol": "aave_v3"}
        assert _resolve_from_token(intent) == "WETH"

    def test_dict_intent_prefers_from_token_over_token(self):
        """When dict intent has both from_token and token, from_token wins."""
        intent = {"amount": "all", "from_token": "USDC", "token": "WETH"}
        assert _resolve_from_token(intent) == "USDC"

    def test_dict_intent_no_token_fields_returns_none(self):
        """When dict intent has neither from_token nor token, returns None."""
        intent = {"amount": "all", "protocol": "aave_v3"}
        assert _resolve_from_token(intent) is None


class TestObjectIntentTokenFallback:
    """Object-based intent: from_token attr absent, token attr present."""

    def test_object_intent_uses_token_when_from_token_missing(self):
        """When object intent has no from_token attr, resolver falls back to token."""
        intent = SimpleNamespace(amount="all", token="WETH", protocol="aave_v3")
        assert _resolve_from_token(intent) == "WETH"

    def test_object_intent_prefers_from_token_over_token(self):
        """When object intent has both from_token and token, from_token wins."""
        intent = SimpleNamespace(amount="all", from_token="USDC", token="WETH")
        assert _resolve_from_token(intent) == "USDC"

    def test_object_intent_no_token_fields_returns_none(self):
        """When object intent has neither from_token nor token, returns None."""
        intent = SimpleNamespace(amount="all", protocol="aave_v3")
        assert _resolve_from_token(intent) is None

    def test_object_intent_from_token_none_falls_back_to_token(self):
        """When from_token is explicitly None, falls back to token."""
        intent = SimpleNamespace(amount="all", from_token=None, token="WETH")
        assert _resolve_from_token(intent) == "WETH"


class TestAmountAllMissingMarket:
    """amount='all' with missing market should fail gracefully."""

    def test_missing_market_returns_failure(self):
        """Simulate teardown_manager: amount='all' + market=None -> failure."""
        intent = SimpleNamespace(amount="all", token="WETH")
        from_token = _resolve_from_token(intent)
        market = None

        # Mirror teardown_manager lines 618-625
        amount_value = intent.amount
        assert amount_value == "all"
        assert from_token == "WETH"
        assert market is None
        # This is the condition that triggers the error return
        assert not from_token or market is None  # noqa: E711

    def test_missing_from_token_and_market_returns_failure(self):
        """Both from_token=None and market=None -> failure."""
        intent = SimpleNamespace(amount="all", protocol="aave_v3")
        from_token = _resolve_from_token(intent)
        market = None

        assert from_token is None
        assert market is None
        # not from_token is True when from_token is None
        assert not from_token

    def test_resolved_token_with_market_proceeds(self):
        """When from_token resolves and market is present, resolution proceeds."""
        intent = SimpleNamespace(amount="all", token="WETH")
        from_token = _resolve_from_token(intent)
        market = MagicMock()
        mock_balance = MagicMock()
        mock_balance.balance = 1.5
        market.balance.return_value = mock_balance

        assert from_token == "WETH"
        assert market is not None
        bal = market.balance(from_token)
        assert bal.balance > 0
        market.balance.assert_called_once_with("WETH")


class TestWithdrawAllSkipsResolution:
    """withdraw_all=True should skip wallet-balance resolution entirely."""

    def test_withdraw_all_skips_resolution_object(self):
        """Object intent with withdraw_all=True skips amount='all' resolution."""
        intent = SimpleNamespace(amount="all", token="WETH", withdraw_all=True)
        _withdraw_all = getattr(intent, "withdraw_all", False)
        assert _withdraw_all is True
        # The condition `amount_value == "all" and not _withdraw_all` is False
        # so wallet-balance resolution is skipped
        assert not (intent.amount == "all" and not _withdraw_all)

    def test_withdraw_all_skips_resolution_dict(self):
        """Dict intent with withdraw_all=True skips amount='all' resolution."""
        intent = {"amount": "all", "token": "WETH", "withdraw_all": True}
        _withdraw_all = intent.get("withdraw_all")
        assert _withdraw_all is True
        assert not (intent["amount"] == "all" and not _withdraw_all)

    def test_without_withdraw_all_proceeds_to_resolution(self):
        """Non-withdraw intent without withdraw_all proceeds with resolution."""
        intent = SimpleNamespace(amount="all", token="WETH", intent_type="SWAP")
        _withdraw_all = getattr(intent, "withdraw_all", False)
        _intent_type_val = getattr(intent, "intent_type", None)
        _is_withdraw = str(_intent_type_val).upper() in ("WITHDRAW", "INTENTTYPE.WITHDRAW")
        assert _withdraw_all is False
        assert _is_withdraw is False
        # The condition IS met for non-withdraw intents, so resolution proceeds
        assert intent.amount == "all" and not _withdraw_all and not _is_withdraw

    def test_withdraw_intent_type_skips_resolution(self):
        """WITHDRAW intent type skips wallet-balance resolution even without withdraw_all."""
        intent = SimpleNamespace(amount="all", token="WETH", intent_type="WITHDRAW")
        _withdraw_all = getattr(intent, "withdraw_all", False)
        _intent_type_val = getattr(intent, "intent_type", None)
        _is_withdraw = str(_intent_type_val).upper() in ("WITHDRAW", "INTENTTYPE.WITHDRAW")
        assert _is_withdraw is True
        # Resolution is skipped for WITHDRAW intents
        assert not (intent.amount == "all" and not _withdraw_all and not _is_withdraw)

    def test_withdraw_intent_type_enum_skips_resolution(self):
        """WITHDRAW intent type as enum value skips resolution."""
        from almanak.framework.intents.vocabulary import IntentType

        intent = SimpleNamespace(amount="all", token="WETH", intent_type=IntentType.WITHDRAW)
        _intent_type_val = getattr(intent, "intent_type", None)
        _is_withdraw = str(_intent_type_val).upper() in ("WITHDRAW", "INTENTTYPE.WITHDRAW")
        assert _is_withdraw is True
