"""Unit tests targeting the per-protocol helpers extracted from ``compile_withdraw``.

Phase 2d of the coverage-improvement plan. The helpers are private module-level
functions in ``almanak.connectors._strategy_base.base.lending.aave_helpers``:

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
- ``_compile_withdraw_euler_v2``
- ``_compile_withdraw_silo_v2``

Each test builds a minimal mocked ``compiler`` (no IntentCompiler instantiation)
plus mocked tokens and adapters, and verifies the helper either succeeds or
fails with a specific ``CompilationStatus.FAILED`` error message.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.base.lending import aave_helpers as cl
from almanak.framework.intents import WithdrawIntent
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus

MORPHO_ADAPTER = "almanak.connectors.morpho_blue.adapter.MorphoBlueAdapter"
MORPHO_CONFIG = "almanak.connectors.morpho_blue.adapter.MorphoBlueConfig"
COMPOUND_ADAPTER = "almanak.connectors.compound_v3.adapter.CompoundV3Adapter"
COMPOUND_CONFIG = "almanak.connectors.compound_v3.adapter.CompoundV3Config"
COMPOUND_MARKETS = "almanak.connectors.compound_v3.adapter.COMPOUND_V3_COMET_ADDRESSES"

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
      - _compile_jupiter_lend_withdraw() / _compile_kamino_withdraw() / PendleCompiler.compile_withdraw()
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


CURVANCE_ADAPTER = "almanak.connectors.curvance.adapter.CurvanceAdapter"
CURVANCE_CONFIG = "almanak.connectors.curvance.adapter.CurvanceConfig"


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
# Aave-compatible (aave_v3, spark)
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


SPARK_ADAPTER = "almanak.connectors.spark.SparkAdapter"
SPARK_CONFIG = "almanak.connectors.spark.SparkConfig"
SPARK_POOL_ADDRESSES = "almanak.connectors.spark.SPARK_POOL_ADDRESSES"


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


BENQI_ADAPTER = "almanak.connectors.benqi.adapter.BenqiAdapter"
BENQI_CONFIG = "almanak.connectors.benqi.adapter.BenqiConfig"


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




EULER_ADAPTER = "almanak.connectors.euler_v2.adapter.EulerV2Adapter"
EULER_CONFIG = "almanak.connectors.euler_v2.adapter.EulerV2Config"


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

    # ------------------------------------------------------------------
    # VIB-5801 — full-exit liquidity resolution.
    #
    # Euler is NOT Silo. EVK's redeem() genuinely caps MAX_UINT256 to balanceOf
    # (verified on ethereum/base/arbitrum/avalanche — see
    # tests/reports/euler_v2_full_exit_redeem_max_verification_vib5801.md), so the
    # liquid path MUST stay on redeem(MAX): it drains at BROADCAST time, where a
    # resolved count is a stale compile-time snapshot that would leave dust.
    # Resolving exists only to bound the request by LIQUIDITY, because MAX caps to
    # the balance and reverts E_InsufficientCash on a cash-short vault.
    #
    # These tests use the REAL EulerV2Adapter (no adapter patch) so they assert the
    # actual calldata, not a mock's call args.
    # ------------------------------------------------------------------
    _REDEEM_SELECTOR = "0xba087652"  # redeem(uint256,address,address)
    _MAX_UINT256_WORD = "f" * 64

    _MAX_REDEEM_SEL = "0xd905777e"
    _GET_CONTROLLERS_SEL = "0xfd6046d7"

    @classmethod
    def _route_eth_call(cls, *, max_redeem=None, controllers=0):
        """Route the compiler's single eth_call seam by selector.

        The resolver makes TWO distinct reads — maxRedeem(owner) on the vault and
        EVC.getControllers(owner) — so a single return_value would conflate them and let a
        controller-gate regression pass unnoticed.
        """

        def _call(to, data, **kwargs):
            if data.startswith(cls._GET_CONTROLLERS_SEL):
                # ABI: word0 = offset(0x20), word1 = array length.
                return "0x" + f"{32:064x}" + f"{controllers:064x}"
            if data.startswith(cls._MAX_REDEEM_SEL):
                return max_redeem
            return None

        return _call


    @staticmethod
    def _uint_word(value: int) -> str:
        return f"{value:064x}"

    def test_withdraw_all_liquid_uses_redeem_max(self):
        # maxRedeem == balanceOf: fully liquid for this owner. MAX and the resolved
        # count are the same share count, so MAX wins (drift-immune, drains flat).
        compiler = _mock_compiler(chain="ethereum")
        compiler.eth_call.side_effect = self._route_eth_call(max_redeem="0x" + self._uint_word(5_000_000_000))
        compiler._query_erc20_balance.return_value = 5_000_000_000
        intent = _withdraw_intent(protocol="euler_v2", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_euler_v2(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.SUCCESS
        data = result.transactions[0].data.lower()
        assert data.startswith(self._REDEEM_SELECTOR)
        assert self._MAX_UINT256_WORD in data
        assert result.action_bundle.metadata["full_exit_mode"] == "redeem_max"
        assert result.action_bundle.metadata["withdraw_amount"] == "all"

    def test_withdraw_all_partially_illiquid_is_transient_and_emits_no_tx(self):
        # 0 < maxRedeem < balanceOf: redeem(MAX) WOULD revert E_InsufficientCash.
        # Fail transiently BEFORE broadcasting a doomed tx — and deliberately do NOT
        # partially fill. A partial fill completes SUCCESSFULLY, and the runner then
        # fires on_intent_executed(success=True); strategies treat a successful
        # full-exit as "closed" (euler_v2_supply_ethereum sets COMPLETE and zeroes its
        # tracked supply), so a partial would make them abandon the residual FOREVER —
        # strictly worse than the revert-and-retry it replaces. Partial recovery needs
        # outcome-aware full-exit semantics across runner/registry/strategy: VIB-5806.
        compiler = _mock_compiler(chain="ethereum")
        compiler.eth_call.side_effect = self._route_eth_call(max_redeem="0x" + self._uint_word(4_000_000_000))
        compiler._query_erc20_balance.return_value = 32_000_000_000
        intent = _withdraw_intent(protocol="euler_v2", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_euler_v2(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.FAILED
        assert result.is_transient is True
        assert "retryable" in result.error
        # No transaction at all — never burn gas on a guaranteed revert...
        assert not result.transactions
        # ...and never silently partial-fill a full exit.
        assert result.action_bundle is None

    def test_withdraw_all_maxredeem_read_gap_falls_back_to_max_not_transient(self):
        # THE euler/silo divergence. Silo fails closed on a read gap because its MAX is
        # broken. Euler's MAX is proven, so failing closed here would strand a full exit
        # that works fine on a liquid vault. Fall back to today's behaviour, loudly.
        compiler = _mock_compiler(chain="ethereum")
        compiler.eth_call.side_effect = self._route_eth_call(max_redeem="0x")  # maxRedeem unreadable
        compiler._query_erc20_balance.return_value = 5_000_000_000
        intent = _withdraw_intent(protocol="euler_v2", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_euler_v2(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.is_transient is False
        data = result.transactions[0].data.lower()
        assert self._MAX_UINT256_WORD in data
        assert result.action_bundle.metadata["full_exit_mode"] == "redeem_max"
        assert any("maxRedeem read unavailable" in w for w in result.warnings)

    def test_withdraw_all_illiquid_maxredeem_zero_with_balance_is_transient(self):
        # Holds shares but none redeemable now: vault out of cash, or an open borrow
        # locks the collateral. Both clear with time → retryable, never terminal.
        compiler = _mock_compiler(chain="ethereum")
        compiler.eth_call.side_effect = self._route_eth_call(max_redeem="0x" + self._uint_word(0))
        compiler._query_erc20_balance.return_value = 8_000_000_000
        intent = _withdraw_intent(protocol="euler_v2", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_euler_v2(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.FAILED
        assert result.is_transient is True
        assert "retryable" in result.error

    def test_withdraw_all_maxredeem_zero_and_balance_unreadable_is_transient(self):
        # Empty != unmeasured: an unreadable balance must never be collapsed into
        # "nothing to withdraw".
        compiler = _mock_compiler(chain="ethereum")
        compiler.eth_call.side_effect = self._route_eth_call(max_redeem="0x" + self._uint_word(0))
        compiler._query_erc20_balance.return_value = None
        intent = _withdraw_intent(protocol="euler_v2", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_euler_v2(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.FAILED
        assert result.is_transient is True

    def test_withdraw_all_measured_empty_balance_is_noop_success(self):
        # A MEASURED zero balance means the requested terminal state ALREADY holds.
        # Report a no-op SUCCESS (the established idiom, cf. Compound V3), not a failure:
        # a stale or repeated teardown request must not count an already-flat leg as
        # failed, and no gas may be burned on a guaranteed revert.
        compiler = _mock_compiler(chain="ethereum")
        compiler.eth_call.side_effect = self._route_eth_call(max_redeem="0x" + self._uint_word(0))
        compiler._query_erc20_balance.return_value = 0
        intent = _withdraw_intent(protocol="euler_v2", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_euler_v2(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["no_op"] is True
        assert result.action_bundle.metadata["full_exit_mode"] == "empty"
        assert result.action_bundle.transactions == []
        assert not result.transactions

    def test_withdraw_all_balance_unreadable_falls_back_to_max(self):
        # maxRedeem readable and positive, balance not: we can neither prove nor disprove
        # that the vault settles the whole position. MAX is the proven default and cannot
        # over-withdraw (it caps); if the vault is short it reverts and the leg retries —
        # exactly the pre-VIB-5801 behaviour, so this is never a regression.
        compiler = _mock_compiler(chain="ethereum")
        compiler.eth_call.side_effect = self._route_eth_call(max_redeem="0x" + self._uint_word(7_000_000_000))
        compiler._query_erc20_balance.return_value = None
        intent = _withdraw_intent(protocol="euler_v2", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_euler_v2(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.SUCCESS
        data = result.transactions[0].data.lower()
        assert self._MAX_UINT256_WORD in data
        assert result.action_bundle.metadata["full_exit_mode"] == "redeem_max"

    def test_withdraw_all_with_controller_enabled_uses_max_not_transient(self):
        # THE VIB-5801 BLOCKER REGRESSION GUARD (found by the Stage-1 audit).
        # EVK zeroes maxRedeem for ANY controller-enabled account — no debt check, no
        # health check. Measured: a fully liquid deposit reads maxRedeem == balanceOf;
        # enabling a controller with ZERO debt drops it to 0 while redeem(MAX) still
        # SUCCEEDS. Repaying does not clear it; only disableController() does, and this
        # repo never calls that. Reading that 0 as "illiquid" would classify every euler
        # BORROW teardown (repay_full -> withdraw_all) TRANSIENT and retry forever against
        # a state that can never change — wedging a lane that works on main today.
        compiler = _mock_compiler(chain="ethereum")
        compiler.eth_call.side_effect = self._route_eth_call(
            max_redeem="0x" + self._uint_word(0),  # EVK's controller short-circuit
            controllers=1,
        )
        compiler._query_erc20_balance.return_value = 8_000_000_000
        intent = _withdraw_intent(protocol="euler_v2", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_euler_v2(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.SUCCESS, (
            "A controller-enabled full exit MUST still emit redeem(MAX) — it works on-chain. "
            "Classifying it transient wedges euler borrow teardown forever."
        )
        assert result.is_transient is False
        assert self._MAX_UINT256_WORD in result.transactions[0].data.lower()
        assert result.action_bundle.metadata["full_exit_mode"] == "redeem_max"
        assert any("controller" in w for w in result.warnings)

    def test_withdraw_all_controller_read_unavailable_uses_max(self):
        # Cannot tell whether a controller is enabled => cannot trust maxRedeem as a
        # liquidity signal => fall back to the proven default rather than risk wedging.
        compiler = _mock_compiler(chain="ethereum")

        def _call(to, data, **kwargs):
            if data.startswith(self._GET_CONTROLLERS_SEL):
                return None  # EVC read gap
            return "0x" + self._uint_word(0)

        compiler.eth_call.side_effect = _call
        compiler._query_erc20_balance.return_value = 8_000_000_000
        intent = _withdraw_intent(protocol="euler_v2", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_euler_v2(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.is_transient is False
        assert self._MAX_UINT256_WORD in result.transactions[0].data.lower()

    def test_explicit_amount_withdraw_does_not_resolve_liquidity(self):
        # The resolution is full-exit-only. An explicit-amount withdraw must not pay for
        # the extra reads or change shape.
        compiler = _mock_compiler(chain="ethereum")
        intent = _withdraw_intent(protocol="euler_v2", withdraw_all=False, amount=Decimal("100"))
        result = cl._compile_withdraw_euler_v2(compiler, intent, _mock_token("USDC"), Decimal("100"), [])
        assert result.status == CompilationStatus.SUCCESS
        compiler.eth_call.assert_not_called()
        assert result.action_bundle.metadata["full_exit_mode"] is None


# ---------------------------------------------------------------------------
# Silo V2
# ---------------------------------------------------------------------------


SILO_ADAPTER = "almanak.connectors.silo_v2.adapter.SiloV2Adapter"
SILO_CONFIG = "almanak.connectors.silo_v2.adapter.SiloV2Config"


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

    # VIB-5800: withdraw_all must NOT encode redeem(MAX_UINT256) — Silo V2's redeem()
    # reverts NotEnoughLiquidity() on it. The helper redeems ONLY the liquidity-aware
    # maxRedeem(owner); balanceOf is read solely to classify the no-redeemable-shares
    # case (terminal empty vs transient illiquid/read-gap) and is NEVER redeemed
    # directly. These tests use the REAL SiloV2Adapter (no adapter patch) so they
    # assert the actual redeem calldata carries the resolved share count.
    _REDEEM_SELECTOR = "0xda537660"

    def test_withdraw_all_resolves_shares_via_max_redeem(self):
        compiler = _mock_compiler(chain="avalanche")
        # maxRedeem(owner) returns 5e9 shares (ABI-encoded uint256).
        compiler.eth_call.return_value = "0x" + f"{5_000_000_000:064x}"
        compiler._query_erc20_balance.return_value = 999  # must NOT be used
        intent = _withdraw_intent(protocol="silo_v2", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_silo_v2(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.SUCCESS
        data = result.transactions[0].data
        assert data.startswith(self._REDEEM_SELECTOR)
        # redeem calldata carries the liquidity-aware maxRedeem share count …
        assert f"{5_000_000_000:064x}" in data
        # … never MAX_UINT256, and — since maxRedeem > 0 — balanceOf is never even read.
        assert "f" * 64 not in data.lower()
        compiler._query_erc20_balance.assert_not_called()

    def test_withdraw_all_maxredeem_unreadable_with_balance_is_transient(self):
        # maxRedeem read unavailable but the wallet holds shares. balanceOf proves
        # ownership, NOT current redeemability — redeeming it could revert
        # NotEnoughLiquidity, so the compile fails TRANSIENTLY (retry) rather than
        # encoding a redeem that may revert (VIB-5800 finding B).
        compiler = _mock_compiler(chain="avalanche")
        compiler.eth_call.return_value = "0x"  # maxRedeem read unavailable
        compiler._query_erc20_balance.return_value = 8_000_000_000  # collateral shares
        intent = _withdraw_intent(protocol="silo_v2", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_silo_v2(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.FAILED
        assert result.is_transient is True
        assert "retryable" in result.error
        # balanceOf is read against the silo vault (the ERC-4626 collateral share token),
        # purely to classify — never redeemed.
        call_args = compiler._query_erc20_balance.call_args.args
        assert call_args[1] == TEST_WALLET

    def test_withdraw_all_illiquid_maxredeem_zero_with_balance_is_transient(self):
        # Silo momentarily at ~100% utilization: maxRedeem == 0 while the wallet still
        # holds shares. This must be TRANSIENT/retryable — NOT a terminal "nothing to
        # withdraw" that strands the leg (VIB-5800 finding A: same fund-stranding class
        # the fix targets, for the illiquid-at-teardown edge).
        compiler = _mock_compiler(chain="avalanche")
        compiler.eth_call.return_value = "0x" + f"{0:064x}"  # maxRedeem = 0
        compiler._query_erc20_balance.return_value = 5_000_000_000  # wallet still holds shares
        intent = _withdraw_intent(protocol="silo_v2", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_silo_v2(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.FAILED
        assert result.is_transient is True
        assert "nothing to withdraw" not in result.error

    def test_withdraw_all_zero_shares_fails(self):
        compiler = _mock_compiler(chain="avalanche")
        compiler.eth_call.return_value = "0x" + f"{0:064x}"  # maxRedeem = 0
        compiler._query_erc20_balance.return_value = 0  # genuinely empty position
        intent = _withdraw_intent(protocol="silo_v2", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_silo_v2(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.FAILED
        assert "nothing to withdraw" in result.error
        assert result.is_transient is False

    def test_withdraw_all_read_unavailable_is_transient(self):
        compiler = _mock_compiler(chain="avalanche")
        compiler.eth_call.return_value = None  # maxRedeem read failed
        compiler._query_erc20_balance.return_value = None  # balanceOf read failed
        intent = _withdraw_intent(protocol="silo_v2", withdraw_all=True, amount=Decimal("1"))
        result = cl._compile_withdraw_silo_v2(compiler, intent, _mock_token("USDC"), None, [])
        assert result.status == CompilationStatus.FAILED
        assert "could not resolve currently-redeemable shares" in result.error
        # A transient data gap must be retryable, not a permanent compile failure.
        assert result.is_transient is True

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


class TestJoeLendDormant:
    """JoeLend remains retired at the adapter boundary."""

    def test_adapter_constructor_raises_deprecated_error(self):
        from almanak.connectors.joelend.adapter import (
            JoeLendAdapter,
            JoeLendConfig,
            JoeLendDeprecatedError,
        )

        with pytest.raises(JoeLendDeprecatedError, match="wound down"):
            JoeLendAdapter(JoeLendConfig(chain="avalanche", wallet_address="0x" + "0" * 40))

if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
