"""Branch coverage for IntentCompiler._compile_ensure_balance.

Covers the type guard, the no-gateway manual-resolution failure, token
resolution failure, the native-token refusal, gateway balance-query failure
(exception fallback), the sufficient-balance auto-resolution to a compiled
HOLD, and the insufficient-balance fallthrough. The gateway client is a
MagicMock — no RPC access.
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.intents import Intent
from almanak.framework.intents.compiler import (
    IntentCompiler,
    IntentCompilerConfig,
    TokenInfo,
)
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.ensure_balance import EnsureBalanceIntent
from almanak.framework.intents.vocabulary import IntentType

# Normalized (checksummed) verbatim by the compiler at construction.
_WALLET = "0x0000000000000000000000000000000000000000"
_USDC = TokenInfo(
    symbol="USDC",
    address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
    decimals=6,
    is_native=False,
)


def _compiler(gateway=None):
    return IntentCompiler(
        chain="arbitrum",
        wallet_address=_WALLET,
        config=IntentCompilerConfig(allow_placeholder_prices=True),
        gateway_client=gateway,
    )


def _intent(min_amount=Decimal("100")):
    return EnsureBalanceIntent(token="USDC", min_amount=min_amount, target_chain="arbitrum")


def _gateway(raw_balance):
    gateway = MagicMock()
    gateway.query_erc20_balance.return_value = raw_balance
    return gateway


class TestCompileEnsureBalance:
    def test_rejects_non_ensure_balance_intent(self):
        hold = Intent.hold(reason="idle")
        result = _compiler()._compile_ensure_balance(hold)

        assert result.status == CompilationStatus.FAILED
        assert result.error == "Expected EnsureBalanceIntent"
        assert result.intent_id == hold.intent_id

    def test_without_gateway_requires_manual_resolution(self):
        intent = _intent()
        result = _compiler()._compile_ensure_balance(intent)

        assert result.status == CompilationStatus.FAILED
        assert "must be resolved before compilation" in (result.error or "")
        assert result.intent_id == intent.intent_id

    def test_unresolvable_token_fails(self):
        compiler = _compiler(gateway=_gateway(raw_balance=0))
        compiler._resolve_token = MagicMock(return_value=None)

        result = compiler._compile_ensure_balance(_intent())

        assert result.status == CompilationStatus.FAILED
        assert "Cannot resolve token 'USDC' on arbitrum" in (result.error or "")

    def test_native_token_rejected(self):
        compiler = _compiler(gateway=_gateway(raw_balance=0))
        compiler._resolve_token = MagicMock(
            return_value=TokenInfo(symbol="ETH", address="0x0", decimals=18, is_native=True)
        )

        result = compiler._compile_ensure_balance(_intent())

        assert result.status == CompilationStatus.FAILED
        assert "native-token balances" in (result.error or "")
        assert "WETH instead of ETH" in (result.error or "")

    def test_gateway_balance_failure_falls_back_to_manual(self):
        compiler = _compiler(gateway=_gateway(raw_balance=None))
        compiler._resolve_token = MagicMock(return_value=_USDC)

        result = compiler._compile_ensure_balance(_intent())

        assert result.status == CompilationStatus.FAILED
        assert "must be resolved before compilation" in (result.error or "")

    def test_sufficient_balance_compiles_hold(self):
        # 150 USDC on-chain (6 decimals) covers the 100 USDC minimum.
        gateway = _gateway(raw_balance=150_000_000)
        compiler = _compiler(gateway=gateway)
        compiler._resolve_token = MagicMock(return_value=_USDC)

        result = compiler._compile_ensure_balance(_intent())

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.intent_type == IntentType.HOLD.value
        assert result.transactions == []
        gateway.query_erc20_balance.assert_called_once_with(
            chain="arbitrum",
            token_address=_USDC.address,
            wallet_address=_WALLET,
        )

    def test_insufficient_balance_requires_manual_resolution(self):
        # 50 USDC on-chain: the compiler cannot see other chains, so the
        # InsufficientBalanceError from resolve() falls back to manual mode.
        compiler = _compiler(gateway=_gateway(raw_balance=50_000_000))
        compiler._resolve_token = MagicMock(return_value=_USDC)

        result = compiler._compile_ensure_balance(_intent())

        assert result.status == CompilationStatus.FAILED
        assert "must be resolved before compilation" in (result.error or "")
