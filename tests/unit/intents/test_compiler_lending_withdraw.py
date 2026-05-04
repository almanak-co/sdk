"""Unit tests targeting the per-protocol helpers extracted from ``compile_withdraw``.

Phase 2d of the coverage-improvement plan. The helpers are private module-level
functions in ``almanak.framework.intents.compiler_lending``:

- ``compile_withdraw`` (thin dispatcher)
- ``_compile_withdraw_jupiter_lend`` (Solana)
- ``_compile_withdraw_kamino`` (Solana / Solana fallback)
- ``_compile_withdraw_morpho_blue``
- ``_compile_withdraw_curvance``
- ``_compile_withdraw_aave_compatible``
- ``_compile_withdraw_spark``
- ``_compile_withdraw_pendle`` (unique to withdraw)
- ``_compile_withdraw_compound_v3``
- ``_compile_withdraw_benqi``
- ``_compile_withdraw_joelend``
- ``_compile_withdraw_euler_v2``
- ``_compile_withdraw_silo_v2``

Each test builds a minimal mocked ``compiler`` (no IntentCompiler instantiation)
plus mocked tokens and adapters, and verifies the helper either succeeds or
fails with a specific ``CompilationStatus.FAILED`` error message.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents import WithdrawIntent
from almanak.framework.intents import compiler_lending as cl
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus

TEST_WALLET = "0x1234567890123456789012345678901234567890"
TEST_POOL = "0xpooladdress000000000000000000000000000001"
TEST_ADAPTER_TO = "0xcontract000000000000000000000000000000aa"
ZERO_ADDR = "0x0000000000000000000000000000000000000000"


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _mock_token(
    symbol: str = "USDC",
    address: str | None = None,
    decimals: int = 6,
    is_native: bool = False,
) -> MagicMock:
    """Build a mock TokenInfo-like object that helpers read from."""
    tok = MagicMock()
    tok.symbol = symbol
    tok.address = address or ("0x" + "ab" * 20)
    tok.decimals = decimals
    tok.is_native = is_native
    tok.to_dict.return_value = {
        "symbol": symbol,
        "address": tok.address,
        "decimals": decimals,
        "is_native": is_native,
    }
    return tok


def _mock_tx_result(
    to: str = TEST_ADAPTER_TO,
    gas: int = 150_000,
    desc: str = "mock",
    data: str = "0xabcdef",
    value: int = 0,
) -> MagicMock:
    res = MagicMock()
    res.success = True
    res.error = None
    res.tx_data = {"to": to, "value": value, "data": data}
    res.gas_estimate = gas
    res.description = desc
    return res


def _mock_failed_result(err: str) -> MagicMock:
    res = MagicMock()
    res.success = False
    res.error = err
    res.tx_data = None
    res.gas_estimate = 0
    res.description = None
    return res


def _mock_compiler(chain: str = "ethereum", *, is_solana: bool = False) -> MagicMock:
    """Create a minimal fake IntentCompiler stub used by helpers.

    The helpers only touch a few attributes/methods on the compiler:
      - chain / wallet_address / _gateway_client
      - _is_solana_chain()
      - _resolve_token()
      - _query_erc20_balance()
      - _get_wrapped_native_address()
      - _get_chain_rpc_url()
      - _format_amount()
      - _compile_jupiter_lend_withdraw() / _compile_kamino_withdraw() / _compile_pendle_redeem()
    """
    compiler = MagicMock()
    compiler.chain = chain
    compiler.wallet_address = TEST_WALLET
    compiler._gateway_client = None
    compiler._is_solana_chain.return_value = is_solana

    compiler._get_wrapped_native_address.return_value = "0xweth00000000000000000000000000000000eeee"
    compiler._get_chain_rpc_url.return_value = None
    compiler._query_erc20_balance.return_value = 1_000_000_000
    compiler._format_amount.side_effect = lambda amt, dec: f"{amt}/{dec}"
    return compiler


def _withdraw_intent(
    *,
    protocol: str = "aave_v3",
    token: str = "USDC",
    amount=Decimal("100"),
    withdraw_all: bool = False,
    is_collateral: bool = True,
    market_id: str | None = None,
) -> WithdrawIntent:
    return WithdrawIntent(
        protocol=protocol,
        token=token,
        amount=amount,
        withdraw_all=withdraw_all,
        is_collateral=is_collateral,
        market_id=market_id,
    )


# ---------------------------------------------------------------------------
# Dispatcher - routing and unsupported-protocol handling
# ---------------------------------------------------------------------------


class TestDispatcher:
    def test_unsupported_protocol_returns_failed(self):
        compiler = _mock_compiler(chain="ethereum")
        compiler._resolve_token.side_effect = lambda t, chain=None: _mock_token(symbol=t)

        intent = _withdraw_intent(protocol="nonexistent_proto")
        result = cl.compile_withdraw(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "Unsupported lending protocol: nonexistent_proto" in result.error
        # Pendle MUST appear in withdraw's supported list (withdraw-only protocol).
        for expected in (
            "aave_v3",
            "morpho",
            "morpho_blue",
            "curvance",
            "spark",
            "pendle",
            "compound_v3",
            "benqi",
            "euler_v2",
            "silo_v2",
        ):
            assert expected in result.error

    def test_unknown_withdraw_token(self):
        compiler = _mock_compiler(chain="ethereum")
        compiler._resolve_token.return_value = None

        intent = _withdraw_intent(protocol="aave_v3", token="FAKE")
        result = cl.compile_withdraw(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "Unknown token: FAKE" in result.error

    def test_jupiter_lend_on_non_solana_fails(self):
        compiler = _mock_compiler(chain="ethereum", is_solana=False)
        intent = _withdraw_intent(protocol="jupiter_lend")
        result = cl.compile_withdraw(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "only available on Solana chains" in result.error

    def test_jupiter_lend_on_solana_delegates(self):
        compiler = _mock_compiler(chain="solana", is_solana=True)
        expected = MagicMock(status=CompilationStatus.SUCCESS)
        compiler._compile_jupiter_lend_withdraw.return_value = expected
        intent = _withdraw_intent(protocol="jupiter_lend")
        result = cl.compile_withdraw(compiler, intent)
        assert result is expected
        compiler._compile_jupiter_lend_withdraw.assert_called_once_with(intent)

    def test_kamino_dispatches_to_helper_on_solana(self):
        compiler = _mock_compiler(chain="solana", is_solana=True)
        expected = MagicMock(status=CompilationStatus.SUCCESS)
        compiler._compile_kamino_withdraw.return_value = expected
        intent = _withdraw_intent(protocol="kamino")
        result = cl.compile_withdraw(compiler, intent)
        assert result is expected
        compiler._compile_kamino_withdraw.assert_called_once_with(intent)

    def test_kamino_on_non_solana_fails(self):
        """Kamino dispatched on a non-Solana chain must fail-fast at compile time.

        Symmetric to the jupiter_lend guard. Regression test for issue #1622.
        """
        compiler = _mock_compiler(chain="ethereum", is_solana=False)
        intent = _withdraw_intent(protocol="kamino")
        result = cl.compile_withdraw(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "Protocol 'kamino' is only available on Solana chains." in result.error
        compiler._compile_kamino_withdraw.assert_not_called()

    def test_non_solana_evm_protocol_on_solana_chain_rejected(self):
        """On a Solana chain, any non-morpho/morpho_blue/jupiter_lend protocol is rejected."""
        compiler = _mock_compiler(chain="solana", is_solana=True)
        intent = _withdraw_intent(protocol="aave_v3")
        result = cl.compile_withdraw(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "not supported for WITHDRAW on Solana" in result.error

    def test_outer_exception_returns_failed(self):
        """An unhandled exception inside the dispatcher is caught and returned as FAILED."""
        compiler = _mock_compiler(chain="ethereum")
        compiler._resolve_token.side_effect = RuntimeError("boom")
        intent = _withdraw_intent(protocol="aave_v3")
        result = cl.compile_withdraw(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "boom" in result.error

    def test_amount_all_fallback_sets_withdraw_all(self):
        """When amount='all' reaches the dispatcher unresolved, it must fall
        back to ``withdraw_all=True`` before routing to the helper.
        """
        compiler = _mock_compiler(chain="ethereum")
        compiler._resolve_token.side_effect = lambda t, chain=None: _mock_token(symbol=t)

        with patch("almanak.framework.intents.compiler_adapters.AaveV3Adapter") as mock_cls:
            mock_adapter = MagicMock()
            mock_adapter.get_pool_address.return_value = TEST_POOL
            mock_adapter.get_withdraw_calldata.return_value = b"\x01\x02"
            mock_adapter.estimate_withdraw_gas.return_value = 200_000
            mock_cls.return_value = mock_adapter

            intent = WithdrawIntent(
                protocol="aave_v3",
                token="USDC",
                amount="all",
                withdraw_all=False,
            )
            result = cl.compile_withdraw(compiler, intent)
        assert result.status == CompilationStatus.SUCCESS
        # Fallback warning present
        assert any("amount='all' fallback" in w for w in result.warnings)
        # withdraw_all=True => MAX_UINT256 sentinel used
        assert result.action_bundle.metadata["withdraw_amount"] == str(cl.MAX_UINT256)
        assert result.action_bundle.metadata["withdraw_all"] is True

    def test_withdraw_all_warning_surfaces_in_result(self):
        """Explicit withdraw_all should attach the informational warning."""
        compiler = _mock_compiler(chain="ethereum")
        compiler._resolve_token.side_effect = lambda t, chain=None: _mock_token(symbol=t)

        with patch("almanak.framework.intents.compiler_adapters.AaveV3Adapter") as mock_cls:
            mock_adapter = MagicMock()
            mock_adapter.get_pool_address.return_value = TEST_POOL
            mock_adapter.get_withdraw_calldata.return_value = b"\x01"
            mock_adapter.estimate_withdraw_gas.return_value = 150_000
            mock_cls.return_value = mock_adapter

            intent = _withdraw_intent(protocol="aave_v3", withdraw_all=True, amount=Decimal("1"))
            result = cl.compile_withdraw(compiler, intent)
        assert result.status == CompilationStatus.SUCCESS
        assert any("Withdrawing all available balance" in w for w in result.warnings)


# ---------------------------------------------------------------------------
# Solana helpers
# ---------------------------------------------------------------------------


class TestJupiterLendHelper:
    def test_non_solana_fails(self):
        compiler = _mock_compiler(chain="ethereum", is_solana=False)
        intent = _withdraw_intent(protocol="jupiter_lend")
        result = cl._compile_withdraw_jupiter_lend(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "only available on Solana chains" in result.error

    def test_solana_delegates_to_compiler(self):
        compiler = _mock_compiler(chain="solana", is_solana=True)
        expected = MagicMock(status=CompilationStatus.SUCCESS)
        compiler._compile_jupiter_lend_withdraw.return_value = expected
        intent = _withdraw_intent(protocol="jupiter_lend")
        result = cl._compile_withdraw_jupiter_lend(compiler, intent)
        assert result is expected
        compiler._compile_jupiter_lend_withdraw.assert_called_once_with(intent)


class TestKaminoHelper:
    def test_non_solana_delegates_to_compiler(self):
        """The dispatcher routes here when protocol_lower == 'kamino' even on
        non-Solana chains. Original behaviour is to hand off to the compiler.
        """
        compiler = _mock_compiler(chain="ethereum", is_solana=False)
        expected = MagicMock(status=CompilationStatus.SUCCESS)
        compiler._compile_kamino_withdraw.return_value = expected
        intent = _withdraw_intent(protocol="kamino")
        result = cl._compile_withdraw_kamino(compiler, intent)
        assert result is expected

    def test_solana_unsupported_protocol_rejected(self):
        compiler = _mock_compiler(chain="solana", is_solana=True)
        intent = _withdraw_intent(protocol="aave_v3")
        result = cl._compile_withdraw_kamino(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "not supported for WITHDRAW on Solana" in result.error

    def test_solana_kamino_delegates(self):
        compiler = _mock_compiler(chain="solana", is_solana=True)
        expected = MagicMock(status=CompilationStatus.SUCCESS)
        compiler._compile_kamino_withdraw.return_value = expected
        intent = _withdraw_intent(protocol="kamino")
        result = cl._compile_withdraw_kamino(compiler, intent)
        assert result is expected


# ---------------------------------------------------------------------------
# Morpho Blue
# ---------------------------------------------------------------------------


MORPHO_ADAPTER = "almanak.framework.connectors.morpho_blue.adapter.MorphoBlueAdapter"
MORPHO_CONFIG = "almanak.framework.connectors.morpho_blue.adapter.MorphoBlueConfig"


class TestMorphoBlueHelper:
    def test_missing_market_id_fails(self):
        """Defensive branch: helper rejects market_id=None.

        Pydantic normally blocks this at construction; use ``model_construct``
        to bypass validation and hit the helper's internal guard directly.
        """
        compiler = _mock_compiler()
        intent = WithdrawIntent.model_construct(
            protocol="morpho_blue",
            token="USDC",
            amount=Decimal("100"),
            withdraw_all=False,
            is_collateral=True,
            market_id=None,
            intent_id="test",
        )
        result = cl._compile_withdraw_morpho_blue(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "market_id is required for Morpho Blue" in result.error

    @patch(MORPHO_ADAPTER)
    @patch(MORPHO_CONFIG)
    def test_success_collateral_withdraw(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler()
        mock_adapter = MagicMock()
        mock_adapter.morpho_address = "0xmorpho"
        mock_adapter.withdraw_collateral.return_value = _mock_tx_result(desc="withdraw coll")
        mock_adapter_cls.return_value = mock_adapter

        intent = _withdraw_intent(protocol="morpho_blue", market_id="0xmarket", is_collateral=True)
        result = cl._compile_withdraw_morpho_blue(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["protocol"] == "morpho_blue"
        assert result.action_bundle.metadata["market_id"] == "0xmarket"
        # Confirm the collateral path was taken, not the loan-token path.
        mock_adapter.withdraw_collateral.assert_called_once()
        mock_adapter.withdraw.assert_not_called()
        assert result.transactions[0].tx_type == "lending_withdraw_collateral"

    @patch(MORPHO_ADAPTER)
    @patch(MORPHO_CONFIG)
    def test_success_loan_token_withdraw(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler()
        mock_adapter = MagicMock()
        mock_adapter.morpho_address = "0xmorpho"
        mock_adapter.withdraw.return_value = _mock_tx_result(desc="withdraw lt")
        mock_adapter_cls.return_value = mock_adapter

        intent = _withdraw_intent(protocol="morpho_blue", market_id="0xmarket", is_collateral=False)
        result = cl._compile_withdraw_morpho_blue(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.SUCCESS
        mock_adapter.withdraw.assert_called_once()
        mock_adapter.withdraw_collateral.assert_not_called()
        assert result.transactions[0].tx_type == "lending_withdraw"

    @patch(MORPHO_ADAPTER)
    @patch(MORPHO_CONFIG)
    def test_success_withdraw_all_propagates_flag(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler()
        mock_adapter = MagicMock()
        mock_adapter.morpho_address = "0xmorpho"
        mock_adapter.withdraw_collateral.return_value = _mock_tx_result()
        mock_adapter_cls.return_value = mock_adapter

        intent = _withdraw_intent(protocol="morpho_blue", market_id="0xmarket", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_morpho_blue(compiler, intent, _mock_token("USDC"), None, ["warn"])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["withdraw_all"] is True
        assert result.action_bundle.metadata["withdraw_amount"] == "all"
        assert "warn" in result.warnings
        # adapter called with withdraw_all=True
        kwargs = mock_adapter.withdraw_collateral.call_args.kwargs
        assert kwargs["withdraw_all"] is True
        assert kwargs["amount"] == Decimal("0")

    @patch(MORPHO_ADAPTER)
    @patch(MORPHO_CONFIG)
    def test_withdraw_failure(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler()
        mock_adapter = MagicMock()
        mock_adapter.morpho_address = "0xmorpho"
        mock_adapter.withdraw_collateral.return_value = _mock_failed_result("bad market")
        mock_adapter_cls.return_value = mock_adapter

        intent = _withdraw_intent(protocol="morpho_blue", market_id="0xmarket")
        result = cl._compile_withdraw_morpho_blue(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "Morpho Blue withdraw failed" in result.error


# ---------------------------------------------------------------------------
# Curvance
# ---------------------------------------------------------------------------


CURVANCE_ADAPTER = "almanak.framework.connectors.curvance.adapter.CurvanceAdapter"
CURVANCE_CONFIG = "almanak.framework.connectors.curvance.adapter.CurvanceConfig"


class TestCurvanceHelper:
    def test_missing_market_id_fails(self):
        compiler = _mock_compiler(chain="monad")
        intent = WithdrawIntent.model_construct(
            protocol="curvance",
            token="USDC",
            amount=Decimal("100"),
            withdraw_all=False,
            is_collateral=True,
            market_id=None,
            intent_id="test",
        )
        result = cl._compile_withdraw_curvance(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "market_id is required for Curvance" in result.error

    @patch(CURVANCE_ADAPTER)
    @patch(CURVANCE_CONFIG)
    def test_collateral_token_mismatch_fails(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="monad")
        mock_adapter = MagicMock()
        market = MagicMock()
        market.collateral_symbol = "WETH"  # mismatch vs requested USDC
        market.debt_symbol = "USDC"
        market.name = "WETH/USDC"
        mock_adapter.get_market.return_value = market
        mock_adapter_cls.return_value = mock_adapter

        intent = _withdraw_intent(protocol="curvance", token="USDC", market_id="0xmarket")
        result = cl._compile_withdraw_curvance(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "USDC" in result.error
        assert "WETH" in result.error

    @patch(CURVANCE_ADAPTER)
    @patch(CURVANCE_CONFIG)
    def test_success_explicit_amount(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="monad")
        mock_adapter = MagicMock()
        market = MagicMock()
        market.collateral_symbol = "USDC"
        market.debt_symbol = "USDT"
        market.name = "USDC/USDT"
        market.collateral_ctoken = "0xcoll"
        mock_adapter.get_market.return_value = market
        mock_adapter.withdraw_collateral.return_value = _mock_tx_result()
        mock_adapter_cls.return_value = mock_adapter

        intent = _withdraw_intent(protocol="curvance", market_id="0xmarket")
        result = cl._compile_withdraw_curvance(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["protocol"] == "curvance"
        assert result.action_bundle.metadata["market_id"] == "0xmarket"
        # withdraw_all stays False when amount was explicit.
        kwargs = mock_adapter.withdraw_collateral.call_args.kwargs
        assert kwargs["withdraw_all"] is False
        assert kwargs["amount"] == Decimal("100")

    @patch(CURVANCE_ADAPTER)
    @patch(CURVANCE_CONFIG)
    def test_withdraw_all_reads_share_balance(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="monad")
        compiler._query_erc20_balance.return_value = 42  # cToken share balance
        mock_adapter = MagicMock()
        market = MagicMock()
        market.collateral_symbol = "USDC"
        market.debt_symbol = "USDT"
        market.name = "USDC/USDT"
        market.collateral_ctoken = "0xctoken"
        mock_adapter.get_market.return_value = market
        mock_adapter.withdraw_collateral.return_value = _mock_tx_result()
        mock_adapter_cls.return_value = mock_adapter

        intent = _withdraw_intent(protocol="curvance", market_id="0xmarket", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_curvance(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.SUCCESS
        # Share balance is queried against the cToken address, not the underlying token.
        compiler._query_erc20_balance.assert_called_with("0xctoken", TEST_WALLET)
        kwargs = mock_adapter.withdraw_collateral.call_args.kwargs
        assert kwargs["share_balance"] == 42
        assert kwargs["withdraw_all"] is True

    @patch(CURVANCE_ADAPTER)
    @patch(CURVANCE_CONFIG)
    def test_withdraw_all_zero_share_balance_fails(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="monad")
        compiler._query_erc20_balance.return_value = 0
        mock_adapter = MagicMock()
        market = MagicMock()
        market.collateral_symbol = "USDC"
        market.debt_symbol = "USDT"
        market.name = "m"
        market.collateral_ctoken = "0xctoken"
        mock_adapter.get_market.return_value = market
        mock_adapter_cls.return_value = mock_adapter

        intent = _withdraw_intent(protocol="curvance", market_id="0xmarket", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_curvance(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.FAILED
        assert "requires reading the cToken share balance" in result.error

    @patch(CURVANCE_ADAPTER)
    @patch(CURVANCE_CONFIG)
    def test_withdraw_all_none_share_balance_fails(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="monad")
        compiler._query_erc20_balance.return_value = None
        mock_adapter = MagicMock()
        market = MagicMock()
        market.collateral_symbol = "USDC"
        market.debt_symbol = "USDT"
        market.name = "m"
        market.collateral_ctoken = "0xctoken"
        mock_adapter.get_market.return_value = market
        mock_adapter_cls.return_value = mock_adapter

        intent = _withdraw_intent(protocol="curvance", market_id="0xmarket", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_curvance(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.FAILED
        assert "balance query returned no value or zero" in result.error

    @patch(CURVANCE_ADAPTER)
    @patch(CURVANCE_CONFIG)
    def test_withdraw_failure(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="monad")
        mock_adapter = MagicMock()
        market = MagicMock()
        market.collateral_symbol = "USDC"
        market.debt_symbol = "USDT"
        market.name = "m"
        market.collateral_ctoken = "0xct"
        mock_adapter.get_market.return_value = market
        mock_adapter.withdraw_collateral.return_value = _mock_failed_result("redeem denied")
        mock_adapter_cls.return_value = mock_adapter

        intent = _withdraw_intent(protocol="curvance", market_id="0xmarket")
        result = cl._compile_withdraw_curvance(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "Curvance withdraw failed" in result.error


# ---------------------------------------------------------------------------
# Aave-compatible (aave_v3, radiant_v2)
# ---------------------------------------------------------------------------


AAVE_ADAPTER_CLS = "almanak.framework.intents.compiler_adapters.AaveV3Adapter"


class TestAaveCompatibleHelper:
    @patch(AAVE_ADAPTER_CLS)
    def test_unsupported_chain_fails(self, mock_adapter_cls):
        compiler = _mock_compiler(chain="berachain")
        mock_adapter = MagicMock()
        mock_adapter.get_pool_address.return_value = ZERO_ADDR
        mock_adapter_cls.return_value = mock_adapter

        intent = _withdraw_intent(protocol="aave_v3")
        result = cl._compile_withdraw_aave_compatible(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "not available on chain" in result.error

    @patch(AAVE_ADAPTER_CLS)
    def test_success_erc20_explicit_amount(self, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        mock_adapter.get_pool_address.return_value = TEST_POOL
        mock_adapter.get_withdraw_calldata.return_value = b"\x03\x04"
        mock_adapter.estimate_withdraw_gas.return_value = 200_000
        mock_adapter_cls.return_value = mock_adapter

        intent = _withdraw_intent(protocol="aave_v3")
        result = cl._compile_withdraw_aave_compatible(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["pool_address"] == TEST_POOL
        # Explicit amount => not MAX_UINT256
        expected_amount = int(Decimal("100") * Decimal(10**6))
        assert result.action_bundle.metadata["withdraw_amount"] == str(expected_amount)

    @patch(AAVE_ADAPTER_CLS)
    def test_withdraw_all_uses_max_uint(self, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        mock_adapter.get_pool_address.return_value = TEST_POOL
        mock_adapter.get_withdraw_calldata.return_value = b"\x03"
        mock_adapter.estimate_withdraw_gas.return_value = 150_000
        mock_adapter_cls.return_value = mock_adapter

        intent = _withdraw_intent(protocol="aave_v3", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_aave_compatible(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["withdraw_amount"] == str(cl.MAX_UINT256)
        assert result.action_bundle.metadata["withdraw_all"] is True

    @patch(AAVE_ADAPTER_CLS)
    def test_native_withdraw_uses_wrapped_native(self, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        mock_adapter.get_pool_address.return_value = TEST_POOL
        mock_adapter.get_withdraw_calldata.return_value = b"\x03\x04"
        mock_adapter.estimate_withdraw_gas.return_value = 200_000
        mock_adapter_cls.return_value = mock_adapter

        intent = _withdraw_intent(protocol="aave_v3", token="ETH")
        result = cl._compile_withdraw_aave_compatible(
            compiler,
            intent,
            _mock_token("ETH", decimals=18, is_native=True),
            Decimal("1"),
            [],
        )
        assert result.status == CompilationStatus.SUCCESS
        assert any("WETH" in w for w in result.warnings)

    @patch(AAVE_ADAPTER_CLS)
    def test_native_withdraw_missing_weth_fails(self, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        compiler._get_wrapped_native_address.return_value = None
        mock_adapter = MagicMock()
        mock_adapter.get_pool_address.return_value = TEST_POOL
        mock_adapter_cls.return_value = mock_adapter

        intent = _withdraw_intent(protocol="aave_v3", token="ETH")
        result = cl._compile_withdraw_aave_compatible(
            compiler,
            intent,
            _mock_token("ETH", decimals=18, is_native=True),
            Decimal("1"),
            [],
        )
        assert result.status == CompilationStatus.FAILED
        assert "WETH address not found" in result.error


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------


SPARK_ADAPTER = "almanak.framework.connectors.spark.SparkAdapter"
SPARK_CONFIG = "almanak.framework.connectors.spark.SparkConfig"
SPARK_POOL_ADDRESSES = "almanak.framework.connectors.spark.SPARK_POOL_ADDRESSES"


class TestSparkHelper:
    def test_unsupported_chain_fails(self):
        compiler = _mock_compiler(chain="berachain")
        with patch(SPARK_POOL_ADDRESSES, {"ethereum": "0xspark"}):
            intent = _withdraw_intent(protocol="spark")
            result = cl._compile_withdraw_spark(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "not available on chain" in result.error

    def test_success_erc20_explicit_amount(self):
        compiler = _mock_compiler(chain="ethereum")
        with (
            patch(SPARK_POOL_ADDRESSES, {"ethereum": "0xspark"}),
            patch(SPARK_ADAPTER) as mock_cls,
            patch(SPARK_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.pool_address = TEST_POOL
            mock_adapter.withdraw.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="spark")
            result = cl._compile_withdraw_spark(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["pool_address"] == TEST_POOL
        expected_amount = int(Decimal("100") * Decimal(10**6))
        assert result.action_bundle.metadata["withdraw_amount"] == str(expected_amount)

    def test_withdraw_all_uses_max_uint(self):
        compiler = _mock_compiler(chain="ethereum")
        with (
            patch(SPARK_POOL_ADDRESSES, {"ethereum": "0xspark"}),
            patch(SPARK_ADAPTER) as mock_cls,
            patch(SPARK_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.pool_address = TEST_POOL
            mock_adapter.withdraw.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="spark", withdraw_all=True, amount=Decimal("1"))
            result = cl._compile_withdraw_spark(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["withdraw_amount"] == str(cl.MAX_UINT256)

    def test_native_withdraw_uses_wrapped_native(self):
        compiler = _mock_compiler(chain="ethereum")
        with (
            patch(SPARK_POOL_ADDRESSES, {"ethereum": "0xspark"}),
            patch(SPARK_ADAPTER) as mock_cls,
            patch(SPARK_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.pool_address = TEST_POOL
            mock_adapter.withdraw.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="spark", token="ETH")
            result = cl._compile_withdraw_spark(
                compiler,
                intent,
                _mock_token("ETH", decimals=18, is_native=True),
                Decimal("1"),
                [],
            )
        assert result.status == CompilationStatus.SUCCESS
        assert any("WETH" in w for w in result.warnings)

    def test_native_withdraw_missing_weth_fails(self):
        compiler = _mock_compiler(chain="ethereum")
        compiler._get_wrapped_native_address.return_value = None
        with (
            patch(SPARK_POOL_ADDRESSES, {"ethereum": "0xspark"}),
            patch(SPARK_ADAPTER) as mock_cls,
            patch(SPARK_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.pool_address = TEST_POOL
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="spark", token="ETH")
            result = cl._compile_withdraw_spark(
                compiler,
                intent,
                _mock_token("ETH", decimals=18, is_native=True),
                Decimal("1"),
                [],
            )
        assert result.status == CompilationStatus.FAILED
        assert "WETH address not found" in result.error

    def test_withdraw_failure(self):
        compiler = _mock_compiler(chain="ethereum")
        with (
            patch(SPARK_POOL_ADDRESSES, {"ethereum": "0xspark"}),
            patch(SPARK_ADAPTER) as mock_cls,
            patch(SPARK_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.pool_address = TEST_POOL
            mock_adapter.withdraw.return_value = _mock_failed_result("reserve paused")
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="spark")
            result = cl._compile_withdraw_spark(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "Spark withdraw failed" in result.error


# ---------------------------------------------------------------------------
# Pendle (unique to withdraw)
# ---------------------------------------------------------------------------


class TestPendleHelper:
    def test_delegates_to_compiler(self):
        compiler = _mock_compiler(chain="ethereum")
        expected = MagicMock(status=CompilationStatus.SUCCESS)
        compiler._compile_pendle_redeem.return_value = expected
        intent = _withdraw_intent(protocol="pendle")
        result = cl._compile_withdraw_pendle(compiler, intent, [])
        assert result is expected
        compiler._compile_pendle_redeem.assert_called_once_with(intent)

    def test_dispatcher_routes_pendle_through_helper(self):
        """End-to-end check: the dispatcher must route protocol='pendle' to
        ``_compile_withdraw_pendle`` which in turn delegates to the compiler.
        """
        compiler = _mock_compiler(chain="ethereum")
        compiler._resolve_token.side_effect = lambda t, chain=None: _mock_token(symbol=t)
        expected = MagicMock(status=CompilationStatus.SUCCESS)
        compiler._compile_pendle_redeem.return_value = expected
        intent = _withdraw_intent(protocol="pendle")
        result = cl.compile_withdraw(compiler, intent)
        assert result is expected
        compiler._compile_pendle_redeem.assert_called_once_with(intent)

    def test_propagates_initial_warnings(self):
        """Dispatcher-level warnings (withdraw_all / amount='all' fallback) must
        be merged into the Pendle redeem result, matching the other EVM helpers.
        """
        compiler = _mock_compiler(chain="ethereum")
        inner = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id="test",
            warnings=["pendle-inner-warning"],
        )
        compiler._compile_pendle_redeem.return_value = inner
        intent = _withdraw_intent(protocol="pendle")
        result = cl._compile_withdraw_pendle(
            compiler, intent, ["Withdrawing all available balance"]
        )
        assert result is inner
        assert result.warnings == [
            "Withdrawing all available balance",
            "pendle-inner-warning",
        ]

    def test_dispatcher_propagates_withdraw_all_warning_through_pendle(self):
        """End-to-end dispatcher check: when withdraw_all=True, the resulting
        ``CompilationResult.warnings`` must include the dispatcher-level notice.
        """
        compiler = _mock_compiler(chain="ethereum")
        compiler._resolve_token.side_effect = lambda t, chain=None: _mock_token(symbol=t)
        inner = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id="test",
            warnings=[],
        )
        compiler._compile_pendle_redeem.return_value = inner
        intent = _withdraw_intent(protocol="pendle", withdraw_all=True)
        result = cl.compile_withdraw(compiler, intent)
        assert "Withdrawing all available balance" in result.warnings


# ---------------------------------------------------------------------------
# Compound V3
# ---------------------------------------------------------------------------


COMPOUND_ADAPTER = "almanak.framework.connectors.compound_v3.adapter.CompoundV3Adapter"
COMPOUND_CONFIG = "almanak.framework.connectors.compound_v3.adapter.CompoundV3Config"
COMPOUND_MARKETS = "almanak.framework.connectors.compound_v3.adapter.COMPOUND_V3_COMET_ADDRESSES"


class TestCompoundV3Helper:
    def test_unsupported_chain_fails(self):
        compiler = _mock_compiler(chain="berachain")
        with patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}):
            intent = _withdraw_intent(protocol="compound_v3")
            result = cl._compile_withdraw_compound_v3(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "not available on chain" in result.error

    def test_unknown_market_fails(self):
        compiler = _mock_compiler(chain="ethereum")
        with patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}):
            intent = _withdraw_intent(protocol="compound_v3", market_id="unknown_market")
            result = cl._compile_withdraw_compound_v3(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "not available on" in result.error

    def test_missing_base_token_address_fails(self):
        compiler = _mock_compiler(chain="ethereum")
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.market_config = {}  # missing base_token_address
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="compound_v3")
            result = cl._compile_withdraw_compound_v3(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "missing base_token_address" in result.error

    def test_success_base_token_withdraw(self):
        """When token address == base_token_address, withdraw() path is taken."""
        compiler = _mock_compiler(chain="ethereum")
        token = _mock_token("USDC", address="0xbase")
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = "0xcomet"
            mock_adapter.market_config = {"base_token_address": "0xBASE"}  # case-insensitive match
            mock_adapter.withdraw.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="compound_v3")
            result = cl._compile_withdraw_compound_v3(compiler, intent, token, Decimal("100"), [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["withdraw_type"] == "base"
        mock_adapter.withdraw.assert_called_once()
        mock_adapter.withdraw_collateral.assert_not_called()
        assert result.transactions[0].tx_type == "lending_withdraw"

    def test_success_collateral_withdraw(self):
        """When token address != base_token_address, withdraw_collateral() path is taken."""
        compiler = _mock_compiler(chain="ethereum")
        token = _mock_token("WETH", address="0xweth0000000000000000000000000000000000ff")
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = "0xcomet"
            mock_adapter.market_config = {"base_token_address": "0xbase"}
            mock_adapter.withdraw_collateral.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="compound_v3", token="WETH")
            result = cl._compile_withdraw_compound_v3(compiler, intent, token, Decimal("1"), [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["withdraw_type"] == "collateral"
        mock_adapter.withdraw_collateral.assert_called_once()
        assert result.transactions[0].tx_type == "lending_withdraw_collateral"

    def test_no_op_empty_collateral_returns_success_empty_bundle(self):
        """withdraw_all on zero collateral returns SUCCESS with empty tx list and no_op metadata."""
        compiler = _mock_compiler(chain="ethereum")
        token = _mock_token("WETH", address="0xweth")
        no_op_result = MagicMock()
        no_op_result.success = True
        no_op_result.error = None
        no_op_result.tx_data = None
        no_op_result.description = "nothing to do"
        no_op_result.gas_estimate = 0
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = "0xcomet"
            mock_adapter.market_config = {"base_token_address": "0xbase"}
            mock_adapter.withdraw_collateral.return_value = no_op_result
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="compound_v3", token="WETH", withdraw_all=True, amount=Decimal("1"))
            result = cl._compile_withdraw_compound_v3(compiler, intent, token, None, [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.transactions == []
        assert result.action_bundle.metadata["no_op"] is True

    def test_no_op_propagates_initial_warnings(self):
        """No-op early return must carry dispatcher-level warnings forward."""
        compiler = _mock_compiler(chain="ethereum")
        token = _mock_token("WETH", address="0xweth")
        no_op_result = MagicMock()
        no_op_result.success = True
        no_op_result.error = None
        no_op_result.tx_data = None
        no_op_result.description = None
        no_op_result.gas_estimate = 0
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = "0xcomet"
            mock_adapter.market_config = {"base_token_address": "0xbase"}
            mock_adapter.withdraw_collateral.return_value = no_op_result
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="compound_v3", token="WETH", withdraw_all=True, amount=Decimal("1"))
            result = cl._compile_withdraw_compound_v3(
                compiler,
                intent,
                token,
                None,
                ["Withdrawing all available balance"],
            )
        assert result.status == CompilationStatus.SUCCESS
        assert result.warnings == ["Withdrawing all available balance"]

    def test_collateral_withdraw_all_uses_intent_amount_fallback(self):
        """With withdraw_all=True, collateral path uses intent.amount as fallback
        when the passed decimal is zero (on-chain query path unavailable).
        """
        compiler = _mock_compiler(chain="ethereum")
        token = _mock_token("WETH", address="0xweth")
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = "0xcomet"
            mock_adapter.market_config = {"base_token_address": "0xbase"}
            mock_adapter.withdraw_collateral.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="compound_v3", token="WETH", withdraw_all=True, amount=Decimal("3"))
            # Passing decimal=None with withdraw_all triggers intent.amount fallback.
            result = cl._compile_withdraw_compound_v3(compiler, intent, token, None, [])
        assert result.status == CompilationStatus.SUCCESS
        # The adapter call must have received Decimal("3") as the fallback amount.
        kwargs = mock_adapter.withdraw_collateral.call_args.kwargs
        assert kwargs["amount"] == Decimal("3")

    def test_withdraw_failure(self):
        compiler = _mock_compiler(chain="ethereum")
        token = _mock_token("USDC", address="0xbase")
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = "0xcomet"
            mock_adapter.market_config = {"base_token_address": "0xbase"}
            mock_adapter.withdraw.return_value = _mock_failed_result("liquidity")
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="compound_v3")
            result = cl._compile_withdraw_compound_v3(compiler, intent, token, Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "Compound V3 withdraw failed" in result.error


# ---------------------------------------------------------------------------
# BENQI
# ---------------------------------------------------------------------------


BENQI_ADAPTER = "almanak.framework.connectors.benqi.adapter.BenqiAdapter"
BENQI_CONFIG = "almanak.framework.connectors.benqi.adapter.BenqiConfig"


class TestBenqiHelper:
    def test_non_avalanche_fails(self):
        compiler = _mock_compiler(chain="ethereum")
        intent = _withdraw_intent(protocol="benqi")
        result = cl._compile_withdraw_benqi(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "only available on Avalanche" in result.error

    def test_unsupported_asset_fails(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(BENQI_ADAPTER) as mock_cls, patch(BENQI_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.get_market_info.return_value = None
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="benqi", token="XYZ")
            result = cl._compile_withdraw_benqi(compiler, intent, _mock_token("XYZ"), Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "does not support asset" in result.error

    def test_success_erc20(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(BENQI_ADAPTER) as mock_cls, patch(BENQI_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.comptroller_address = "0xcomp"
            market = MagicMock()
            market.qi_token_address = "0xqi"
            mock_adapter.get_market_info.return_value = market
            mock_adapter.withdraw.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="benqi")
            result = cl._compile_withdraw_benqi(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["protocol"] == "benqi"
        assert result.action_bundle.metadata["qi_token_address"] == "0xqi"

    def test_withdraw_all_propagates_flag(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(BENQI_ADAPTER) as mock_cls, patch(BENQI_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.comptroller_address = "0xcomp"
            market = MagicMock()
            market.qi_token_address = "0xqi"
            mock_adapter.get_market_info.return_value = market
            mock_adapter.withdraw.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="benqi", withdraw_all=True, amount=Decimal("1"))
            result = cl._compile_withdraw_benqi(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["withdraw_all"] is True
        assert result.action_bundle.metadata["withdraw_amount"] == "all"
        kwargs = mock_adapter.withdraw.call_args.kwargs
        assert kwargs["withdraw_all"] is True

    def test_withdraw_failure(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(BENQI_ADAPTER) as mock_cls, patch(BENQI_CONFIG):
            mock_adapter = MagicMock()
            market = MagicMock()
            market.qi_token_address = "0xqi"
            mock_adapter.get_market_info.return_value = market
            mock_adapter.withdraw.return_value = _mock_failed_result("frozen")
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="benqi")
            result = cl._compile_withdraw_benqi(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "BENQI withdraw failed" in result.error


# ---------------------------------------------------------------------------
# Joe Lend
# ---------------------------------------------------------------------------




EULER_ADAPTER = "almanak.framework.connectors.euler_v2.adapter.EulerV2Adapter"
EULER_CONFIG = "almanak.framework.connectors.euler_v2.adapter.EulerV2Config"


class TestEulerV2Helper:
    @patch(EULER_ADAPTER)
    @patch(EULER_CONFIG)
    def test_no_vault_for_asset_fails(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        mock_adapter.find_vault_for_asset.return_value = None
        mock_adapter.get_supported_assets.return_value = ["USDC", "WETH"]
        mock_adapter_cls.return_value = mock_adapter
        intent = _withdraw_intent(protocol="euler_v2", token="XYZ")
        result = cl._compile_withdraw_euler_v2(compiler, intent, _mock_token("XYZ"), Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "Euler V2 does not have a vault" in result.error

    @patch(EULER_ADAPTER)
    @patch(EULER_CONFIG)
    def test_success(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        vault = MagicMock()
        vault.vault_address = "0xvault"
        vault.vault_symbol = "eUSDC"
        mock_adapter.find_vault_for_asset.return_value = vault
        mock_adapter.withdraw.return_value = _mock_tx_result()
        mock_adapter_cls.return_value = mock_adapter
        intent = _withdraw_intent(protocol="euler_v2")
        result = cl._compile_withdraw_euler_v2(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["vault_address"] == "0xvault"
        assert result.action_bundle.metadata["vault_symbol"] == "eUSDC"

    @patch(EULER_ADAPTER)
    @patch(EULER_CONFIG)
    def test_withdraw_all_propagates_flag(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        vault = MagicMock()
        vault.vault_address = "0xvault"
        vault.vault_symbol = "eUSDC"
        mock_adapter.find_vault_for_asset.return_value = vault
        mock_adapter.withdraw.return_value = _mock_tx_result()
        mock_adapter_cls.return_value = mock_adapter
        intent = _withdraw_intent(protocol="euler_v2", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_euler_v2(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.SUCCESS
        kwargs = mock_adapter.withdraw.call_args.kwargs
        assert kwargs["withdraw_all"] is True

    @patch(EULER_ADAPTER)
    @patch(EULER_CONFIG)
    def test_withdraw_failure(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        vault = MagicMock()
        vault.vault_address = "0xvault"
        vault.vault_symbol = "eUSDC"
        mock_adapter.find_vault_for_asset.return_value = vault
        mock_adapter.withdraw.return_value = _mock_failed_result("paused")
        mock_adapter_cls.return_value = mock_adapter
        intent = _withdraw_intent(protocol="euler_v2")
        result = cl._compile_withdraw_euler_v2(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "Euler V2 withdraw failed" in result.error


# ---------------------------------------------------------------------------
# Silo V2
# ---------------------------------------------------------------------------


SILO_ADAPTER = "almanak.framework.connectors.silo_v2.adapter.SiloV2Adapter"
SILO_CONFIG = "almanak.framework.connectors.silo_v2.adapter.SiloV2Config"


class TestSiloV2Helper:
    def test_non_avalanche_fails(self):
        compiler = _mock_compiler(chain="ethereum")
        intent = _withdraw_intent(protocol="silo_v2")
        result = cl._compile_withdraw_silo_v2(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "only available on Avalanche" in result.error

    def test_no_silo_for_asset(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(SILO_ADAPTER) as mock_cls, patch(SILO_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.find_silo_for_asset.return_value = None
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="silo_v2")
            result = cl._compile_withdraw_silo_v2(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "No Silo V2 market found" in result.error

    def test_success(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(SILO_ADAPTER) as mock_cls, patch(SILO_CONFIG):
            mock_adapter = MagicMock()
            market = MagicMock()
            market.silo_config = "0xsc"
            market.market_name = "USDC-WAVAX"
            mock_adapter.find_silo_for_asset.return_value = (market, "0xsilo", "0xtok")
            mock_adapter.withdraw.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="silo_v2")
            result = cl._compile_withdraw_silo_v2(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["protocol"] == "silo_v2"
        assert result.action_bundle.metadata["market_name"] == "USDC-WAVAX"
        assert result.action_bundle.metadata["silo_address"] == "0xsilo"

    def test_withdraw_all_propagates_flag(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(SILO_ADAPTER) as mock_cls, patch(SILO_CONFIG):
            mock_adapter = MagicMock()
            market = MagicMock()
            market.silo_config = "0xsc"
            market.market_name = "m"
            mock_adapter.find_silo_for_asset.return_value = (market, "0xsilo", "0xtok")
            mock_adapter.withdraw.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="silo_v2", withdraw_all=True, amount=Decimal("1"))
            result = cl._compile_withdraw_silo_v2(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.SUCCESS
        kwargs = mock_adapter.withdraw.call_args.kwargs
        assert kwargs["withdraw_all"] is True

    def test_withdraw_failure(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(SILO_ADAPTER) as mock_cls, patch(SILO_CONFIG):
            mock_adapter = MagicMock()
            market = MagicMock()
            market.silo_config = "0xsc"
            market.market_name = "m"
            mock_adapter.find_silo_for_asset.return_value = (market, "0xsilo", "0xtok")
            mock_adapter.withdraw.return_value = _mock_failed_result("hf low")
            mock_cls.return_value = mock_adapter
            intent = _withdraw_intent(protocol="silo_v2")
            result = cl._compile_withdraw_silo_v2(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.FAILED
        assert "Silo V2 withdraw failed" in result.error


# ---------------------------------------------------------------------------
# Dispatcher routing happy-path coverage (ensures each route calls its helper)
# ---------------------------------------------------------------------------


class TestDispatcherRouting:
    """Smoke tests - each protocol routes to its dedicated helper and returns SUCCESS."""

    def test_morpho_routes_through_dispatcher(self):
        compiler = _mock_compiler(chain="ethereum")
        compiler._resolve_token.side_effect = lambda t, chain=None: _mock_token(symbol=t)
        with patch(MORPHO_ADAPTER) as mock_cls, patch(MORPHO_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.morpho_address = "0xmorpho"
            mock_adapter.withdraw_collateral.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter

            intent = _withdraw_intent(protocol="morpho_blue", market_id="0xmarket")
            result = cl.compile_withdraw(compiler, intent)
        assert result.status == CompilationStatus.SUCCESS

    def test_aave_routes_through_dispatcher(self):
        compiler = _mock_compiler(chain="ethereum")
        compiler._resolve_token.side_effect = lambda t, chain=None: _mock_token(symbol=t)
        with patch(AAVE_ADAPTER_CLS) as mock_cls:
            mock_adapter = MagicMock()
            mock_adapter.get_pool_address.return_value = TEST_POOL
            mock_adapter.get_withdraw_calldata.return_value = b"\x03\x04"
            mock_adapter.estimate_withdraw_gas.return_value = 200_000
            mock_cls.return_value = mock_adapter

            intent = _withdraw_intent(protocol="aave_v3")
            result = cl.compile_withdraw(compiler, intent)
        assert result.status == CompilationStatus.SUCCESS

    def test_compound_v3_routes_through_dispatcher(self):
        compiler = _mock_compiler(chain="ethereum")
        compiler._resolve_token.side_effect = lambda t, chain=None: _mock_token(
            symbol=t, address="0xbase" if t == "USDC" else "0xother"
        )
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = "0xcomet"
            mock_adapter.market_config = {"base_token_address": "0xbase"}
            mock_adapter.withdraw.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter

            intent = _withdraw_intent(protocol="compound_v3")
            result = cl.compile_withdraw(compiler, intent)
        assert result.status == CompilationStatus.SUCCESS

    def test_silo_v2_routes_through_dispatcher(self):
        compiler = _mock_compiler(chain="avalanche")
        compiler._resolve_token.side_effect = lambda t, chain=None: _mock_token(symbol=t)
        with patch(SILO_ADAPTER) as mock_cls, patch(SILO_CONFIG):
            mock_adapter = MagicMock()
            market = MagicMock()
            market.silo_config = "0xsc"
            market.market_name = "m"
            mock_adapter.find_silo_for_asset.return_value = (market, "0xsilo", "0xtok")
            mock_adapter.withdraw.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter

            intent = _withdraw_intent(protocol="silo_v2")
            result = cl.compile_withdraw(compiler, intent)
        assert result.status == CompilationStatus.SUCCESS

    def test_withdraw_all_warning_propagated_to_result(self):
        compiler = _mock_compiler(chain="ethereum")
        compiler._resolve_token.side_effect = lambda t, chain=None: _mock_token(symbol=t)
        with patch(AAVE_ADAPTER_CLS) as mock_cls:
            mock_adapter = MagicMock()
            mock_adapter.get_pool_address.return_value = TEST_POOL
            mock_adapter.get_withdraw_calldata.return_value = b"\x01"
            mock_adapter.estimate_withdraw_gas.return_value = 100_000
            mock_cls.return_value = mock_adapter

            intent = _withdraw_intent(protocol="aave_v3", withdraw_all=True, amount=Decimal("1"))
            result = cl.compile_withdraw(compiler, intent)
        assert result.status == CompilationStatus.SUCCESS
        assert any("Withdrawing all available balance" in w for w in result.warnings)


# ---------------------------------------------------------------------------
# Entry-point sanity check: module exposes what callers expect
# ---------------------------------------------------------------------------


def test_module_exposes_all_helpers():
    """Regression guard: all helper symbols must remain exported at module level."""
    for name in (
        "compile_withdraw",
        "_compile_withdraw_jupiter_lend",
        "_compile_withdraw_kamino",
        "_compile_withdraw_morpho_blue",
        "_compile_withdraw_curvance",
        "_compile_withdraw_aave_compatible",
        "_compile_withdraw_spark",
        "_compile_withdraw_pendle",
        "_compile_withdraw_compound_v3",
        "_compile_withdraw_benqi",
        "_compile_withdraw_joelend",
        "_compile_withdraw_euler_v2",
        "_compile_withdraw_silo_v2",
    ):
        assert hasattr(cl, name), f"Missing module-level helper: {name}"


class TestJoeLendDormant:
    """Lock the VIB-3960 dormancy contract: dispatch short-circuits and
    adapter constructor raises. These tests are the *positive assertion*
    that the wind-down guard fires; without them a future refactor that
    removes the short-circuit at the top of compile_withdraw would silently
    re-route joelend intents into the (now-stub) helper functions.
    """

    def test_dispatch_returns_failed_with_deprecation_message(self):
        compiler = _mock_compiler(chain="avalanche")
        compiler._resolve_token.side_effect = lambda t, chain=None: _mock_token(symbol=t)

        intent = _withdraw_intent(protocol="joelend")
        result = cl.compile_withdraw(compiler, intent)

        assert result.status == CompilationStatus.FAILED
        assert "wound down" in result.error.lower()
        # Mock-call assertion: confirms the dispatcher returned BEFORE
        # reaching the Solana fallback (which would have invoked
        # compiler._compile_kamino_withdraw). Locks Codex P2 #1 from the PR audit.
        compiler._compile_kamino_withdraw.assert_not_called()
        assert "VIB-3960" in result.error

    def test_dispatch_short_circuits_before_solana_fallback(self):
        """A misconfigured (joelend, solana) intent must NOT route to Kamino.
        Codex P2 finding on PR #2023 audit."""
        compiler = _mock_compiler(chain="solana", is_solana=True)
        compiler._resolve_token.side_effect = lambda t, chain=None: _mock_token(symbol=t)

        intent = _withdraw_intent(protocol="joelend")
        result = cl.compile_withdraw(compiler, intent)

        assert result.status == CompilationStatus.FAILED
        assert "wound down" in result.error.lower()
        # Mock-call assertion: confirms the dispatcher returned BEFORE
        # reaching the Solana fallback (which would have invoked
        # compiler._compile_kamino_withdraw). Locks Codex P2 #1 from the PR audit.
        compiler._compile_kamino_withdraw.assert_not_called()

    def test_adapter_constructor_raises_deprecated_error(self):
        from almanak.framework.connectors.joelend.adapter import (
            JoeLendAdapter,
            JoeLendConfig,
            JoeLendDeprecatedError,
        )

        with pytest.raises(JoeLendDeprecatedError, match="wound down"):
            JoeLendAdapter(JoeLendConfig(chain="avalanche", wallet_address="0x" + "0" * 40))

if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
