"""Regression tests for VIB-587: Morpho Blue repay_full=True RPC URL fix.

When repay_full=True, the compiler must query borrow_shares on-chain to build
the repay calldata. The query must use the SAME RPC endpoint as the execution
environment (i.e., Anvil fork RPC on forks, not always Alchemy mainnet).

The bug: compiler passed self.rpc_url (always None in gateway mode) to
MorphoBlueConfig instead of calling _get_chain_rpc_url() which checks for
ANVIL_{CHAIN}_PORT env var. Result: borrow_shares=0 on Anvil forks.

The fix: use _get_chain_rpc_url() so the Anvil fork RPC is detected and used.
"""

import os
from decimal import Decimal
from unittest.mock import MagicMock, call, patch

import pytest

from almanak.framework.intents import RepayIntent
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)

# Lazy-import patch targets (how the compiler loads them)
MORPHO_ADAPTER_MODULE = "almanak.connectors.morpho_blue.adapter"
MORPHO_ADAPTER_CLS = f"{MORPHO_ADAPTER_MODULE}.MorphoBlueAdapter"
MORPHO_CONFIG_CLS = f"{MORPHO_ADAPTER_MODULE}.MorphoBlueConfig"

TEST_WALLET = "0x1234567890123456789012345678901234567890"
TEST_MORPHO_ADDR = "0xBBBBbbbb219152Bea9549d07bB35d3f7a35e3Ef"
TEST_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"
ANVIL_PORT = "8547"
ANVIL_RPC_URL = f"http://127.0.0.1:{ANVIL_PORT}"


def _mock_repay_result() -> MagicMock:
    """Create a mock successful repay TransactionResult."""
    result = MagicMock()
    result.success = True
    result.error = None
    result.tx_data = {
        "to": TEST_MORPHO_ADDR,
        "value": 0,
        "data": "0xdeadbeef",
    }
    result.gas_estimate = 200_000
    result.description = "Repay USDC on Morpho Blue"
    return result


@pytest.fixture
def compiler_no_rpc():
    """Create an IntentCompiler without an explicit rpc_url (gateway mode).

    This is the production mode where self.rpc_url is None and
    _get_chain_rpc_url() must check ANVIL_{CHAIN}_PORT env var.
    """
    return IntentCompiler(
        chain="base",
        wallet_address=TEST_WALLET,
        rpc_url=None,
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )


@pytest.fixture
def repay_full_intent():
    """Create a RepayIntent with repay_full=True for Morpho Blue."""
    return RepayIntent(
        token="USDC",
        amount=Decimal("0"),
        protocol="morpho_blue",
        market_id=TEST_MARKET_ID,
        repay_full=True,
    )


class TestMorphoBlueRepayFullRpcUrl:
    """VIB-587: Morpho Blue repay_full=True must use Anvil fork RPC when available.

    Root cause: compiler called MorphoBlueConfig(rpc_url=self.rpc_url) where
    self.rpc_url is always None in gateway mode. This caused the adapter to
    fall back to Alchemy mainnet RPC, which returned borrow_shares=0 on forks.

    Fix: compiler now calls _get_chain_rpc_url() which checks ANVIL_{CHAIN}_PORT
    env var before falling through to the configured provider.
    """

    @patch(MORPHO_CONFIG_CLS)
    @patch(MORPHO_ADAPTER_CLS)
    def test_repay_full_passes_anvil_rpc_url_to_adapter(
        self,
        mock_adapter_cls: MagicMock,
        mock_config_cls: MagicMock,
        compiler_no_rpc: IntentCompiler,
        repay_full_intent: RepayIntent,
    ) -> None:
        """When ANVIL_BASE_PORT is set, compiler passes Anvil RPC to MorphoBlueConfig.

        This is the core VIB-587 regression test. Without the fix, rpc_url=None
        would be passed, causing borrow_shares=0 on the fork.
        """
        mock_adapter = MagicMock()
        mock_adapter.morpho_address = TEST_MORPHO_ADDR
        mock_adapter.repay.return_value = _mock_repay_result()
        mock_adapter_cls.return_value = mock_adapter

        with patch.dict(os.environ, {"ANVIL_BASE_PORT": ANVIL_PORT}):
            result = compiler_no_rpc.compile(repay_full_intent)

        assert result.status == CompilationStatus.SUCCESS, f"Expected SUCCESS, got: {result.error}"

        # CRITICAL: MorphoBlueConfig must be called with the Anvil fork RPC URL,
        # NOT with None (which would cause borrow_shares=0 on fork queries).
        mock_config_cls.assert_called_once()
        config_call_kwargs = mock_config_cls.call_args.kwargs
        assert config_call_kwargs.get("rpc_url") == ANVIL_RPC_URL, (
            f"MorphoBlueConfig.rpc_url must be '{ANVIL_RPC_URL}' when "
            f"ANVIL_BASE_PORT={ANVIL_PORT} is set, "
            f"got: {config_call_kwargs.get('rpc_url')!r}"
        )

    @patch(MORPHO_CONFIG_CLS)
    @patch(MORPHO_ADAPTER_CLS)
    def test_repay_full_rpc_url_is_none_without_anvil_env(
        self,
        mock_adapter_cls: MagicMock,
        mock_config_cls: MagicMock,
        compiler_no_rpc: IntentCompiler,
        repay_full_intent: RepayIntent,
    ) -> None:
        """Without ANVIL_BASE_PORT, rpc_url falls through to get_rpc_url() which returns None.

        This test confirms that without Anvil, the compiler calls
        _get_chain_rpc_url() -> get_rpc_url("base") rather than hardcoding None.
        A sentinel value proves the code path flows through the resolver.
        """
        mock_adapter = MagicMock()
        mock_adapter.morpho_address = TEST_MORPHO_ADDR
        mock_adapter.repay.return_value = _mock_repay_result()
        mock_adapter_cls.return_value = mock_adapter

        # Clear all ANVIL env vars to simulate no running fork
        clean_env = {k: v for k, v in os.environ.items() if not k.startswith("ANVIL_")}
        with (
            patch.dict(os.environ, clean_env, clear=True),
            patch("almanak.gateway.utils.get_rpc_url", return_value=None) as mock_get_rpc_url,
        ):
            result = compiler_no_rpc.compile(repay_full_intent)

        assert result.status == CompilationStatus.SUCCESS, f"Expected SUCCESS, got: {result.error}"
        mock_config_cls.assert_called_once()
        # Verify _get_chain_rpc_url() called get_rpc_url with the correct chain
        mock_get_rpc_url.assert_called_once_with("base")
        config_call_kwargs = mock_config_cls.call_args.kwargs
        assert config_call_kwargs.get("rpc_url") is None, (
            f"Expected rpc_url=None when get_rpc_url returns None, "
            f"got: {config_call_kwargs.get('rpc_url')!r}"
        )

    @patch(MORPHO_CONFIG_CLS)
    @patch(MORPHO_ADAPTER_CLS)
    def test_repay_full_explicit_rpc_url_takes_precedence(
        self,
        mock_adapter_cls: MagicMock,
        mock_config_cls: MagicMock,
        repay_full_intent: RepayIntent,
    ) -> None:
        """Explicit rpc_url on compiler takes precedence over ANVIL_BASE_PORT env var.

        When a user passes rpc_url='https://...' to IntentCompiler, that URL
        should be used even if ANVIL_BASE_PORT is set (consistent with
        _get_chain_rpc_url() behavior).
        """
        explicit_rpc = "https://explicit-base-rpc.example.com"
        compiler_with_rpc = IntentCompiler(
            chain="base",
            wallet_address=TEST_WALLET,
            rpc_url=explicit_rpc,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        mock_adapter = MagicMock()
        mock_adapter.morpho_address = TEST_MORPHO_ADDR
        mock_adapter.repay.return_value = _mock_repay_result()
        mock_adapter_cls.return_value = mock_adapter

        # Even with ANVIL_BASE_PORT set, explicit rpc_url takes precedence
        with patch.dict(os.environ, {"ANVIL_BASE_PORT": ANVIL_PORT}):
            result = compiler_with_rpc.compile(repay_full_intent)

        assert result.status == CompilationStatus.SUCCESS, f"Expected SUCCESS, got: {result.error}"

        config_call_kwargs = mock_config_cls.call_args.kwargs
        assert config_call_kwargs.get("rpc_url") == explicit_rpc, (
            f"Explicit rpc_url must take precedence, "
            f"got: {config_call_kwargs.get('rpc_url')!r}"
        )

    @patch(MORPHO_CONFIG_CLS)
    @patch(MORPHO_ADAPTER_CLS)
    def test_regular_repay_still_passes_rpc_url_to_adapter(
        self,
        mock_adapter_cls: MagicMock,
        mock_config_cls: MagicMock,
        compiler_no_rpc: IntentCompiler,
    ) -> None:
        """Even regular (non-full) repay should use _get_chain_rpc_url().

        The adapter's cap guard also queries on-chain debt, so both
        repay_full=True and regular repays need the correct RPC.
        """
        mock_adapter = MagicMock()
        mock_adapter.morpho_address = TEST_MORPHO_ADDR
        mock_adapter.repay.return_value = _mock_repay_result()
        mock_adapter_cls.return_value = mock_adapter

        intent = RepayIntent(
            token="USDC",
            amount=Decimal("100"),
            protocol="morpho_blue",
            market_id=TEST_MARKET_ID,
            repay_full=False,
        )

        with patch.dict(os.environ, {"ANVIL_BASE_PORT": ANVIL_PORT}):
            result = compiler_no_rpc.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, f"Expected SUCCESS, got: {result.error}"

        config_call_kwargs = mock_config_cls.call_args.kwargs
        assert config_call_kwargs.get("rpc_url") == ANVIL_RPC_URL, (
            f"Regular repay must also use Anvil RPC, got: {config_call_kwargs.get('rpc_url')!r}"
        )

    def test_repay_missing_market_id_fails_at_intent_creation(
        self,
        compiler_no_rpc: IntentCompiler,
    ) -> None:
        """Morpho Blue repay without market_id must fail at intent creation (Pydantic validation).

        The intent schema validates market_id is required for morpho_blue,
        so the error surfaces before compilation even starts.
        """
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="market_id"):
            RepayIntent(
                token="USDC",
                amount=Decimal("0"),
                protocol="morpho_blue",
                repay_full=True,
                # market_id intentionally omitted
            )
