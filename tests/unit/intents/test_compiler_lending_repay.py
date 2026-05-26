"""Unit tests targeting the per-protocol helpers extracted from ``compile_repay``.

Phase 2b of the coverage-improvement plan. The helpers are private module-level
functions in ``almanak.connectors._strategy_base.base.lending.aave_helpers``:

- ``compile_repay`` (thin dispatcher)
- ``_compile_repay_jupiter_lend`` (Solana)
- ``_compile_repay_kamino`` (Solana / Solana fallback)
- ``_compile_repay_morpho_blue``
- ``_compile_repay_curvance``
- ``_compile_repay_aave_compatible``
- ``_compile_repay_spark``
- ``_compile_repay_compound_v3``
- ``_compile_repay_benqi``
- ``_compile_repay_euler_v2``
- ``_compile_repay_silo_v2``

Each test builds a minimal mocked ``compiler`` (no IntentCompiler instantiation)
plus mocked tokens and adapters, and verifies the helper either succeeds or
fails with a specific ``CompilationStatus.FAILED`` error message.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.base.lending import aave_helpers as cl
from almanak.framework.intents import RepayIntent
from almanak.framework.intents.compiler_models import CompilationStatus

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


def _mock_tx_result(to: str = TEST_ADAPTER_TO, gas: int = 150_000, desc: str = "mock") -> MagicMock:
    res = MagicMock()
    res.success = True
    res.error = None
    res.tx_data = {"to": to, "value": 0, "data": "0xabcdef"}
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
      - _build_approve_tx()
      - _query_erc20_balance()
      - _get_wrapped_native_address()
      - _get_chain_rpc_url()
      - _compile_jupiter_lend_repay() / _compile_kamino_repay()
    """
    compiler = MagicMock()
    compiler.chain = chain
    compiler.wallet_address = TEST_WALLET
    compiler._gateway_client = None
    compiler._is_solana_chain.return_value = is_solana

    # By default _build_approve_tx returns a single approve tx
    approve_tx = cl.TransactionData(
        to="0x" + "cc" * 20,
        value=0,
        data="0x0000",
        gas_estimate=60_000,
        description="approve",
        tx_type="approve",
    )
    compiler._build_approve_tx.return_value = [approve_tx]
    compiler._get_wrapped_native_address.return_value = "0xweth00000000000000000000000000000000eeee"
    compiler._get_chain_rpc_url.return_value = None
    compiler._query_erc20_balance.return_value = 1_000_000_000
    return compiler


def _repay_intent(
    *,
    protocol: str = "aave_v3",
    token: str = "USDC",
    amount=Decimal("100"),
    repay_full: bool = False,
    market_id: str | None = None,
) -> RepayIntent:
    return RepayIntent(
        protocol=protocol,
        token=token,
        amount=amount,
        repay_full=repay_full,
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
        intent = RepayIntent.model_construct(
            protocol="morpho_blue",
            token="USDC",
            amount=Decimal("100"),
            repay_full=False,
            market_id=None,
            intent_id="test",
        )
        result = cl._compile_repay_morpho_blue(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.FAILED
        assert "market_id is required for Morpho Blue" in result.error

    @patch(MORPHO_ADAPTER)
    @patch(MORPHO_CONFIG)
    def test_success_explicit_amount(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler()
        mock_adapter = MagicMock()
        mock_adapter.morpho_address = "0xmorpho"
        mock_adapter.repay.return_value = _mock_tx_result(desc="repay")
        mock_adapter_cls.return_value = mock_adapter

        intent = _repay_intent(protocol="morpho_blue", market_id="0xmarket")
        result = cl._compile_repay_morpho_blue(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["protocol"] == "morpho_blue"
        assert result.action_bundle.metadata["market_id"] == "0xmarket"
        mock_adapter.repay.assert_called_once()

    @patch(MORPHO_ADAPTER)
    @patch(MORPHO_CONFIG)
    def test_success_repay_full_uses_max_uint(self, mock_config, mock_adapter_cls):
        """repay_full=True + None amount must approve MAX_UINT256."""
        compiler = _mock_compiler()
        mock_adapter = MagicMock()
        mock_adapter.morpho_address = "0xmorpho"
        mock_adapter.repay.return_value = _mock_tx_result()
        mock_adapter_cls.return_value = mock_adapter

        intent = _repay_intent(protocol="morpho_blue", market_id="0xmarket", repay_full=True, amount=Decimal("0"))
        result = cl._compile_repay_morpho_blue(compiler, intent, _mock_token("USDC"), None, "full debt", ["warn"])
        assert result.status == CompilationStatus.SUCCESS
        # Warning carried through from dispatcher
        assert "warn" in result.warnings
        # Confirm approve was called with MAX_UINT256
        args, _ = compiler._build_approve_tx.call_args
        assert args[2] == cl.MAX_UINT256

    @patch(MORPHO_ADAPTER)
    @patch(MORPHO_CONFIG)
    def test_repay_failure(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler()
        mock_adapter = MagicMock()
        mock_adapter.morpho_address = "0xmorpho"
        mock_adapter.repay.return_value = _mock_failed_result("bad market")
        mock_adapter_cls.return_value = mock_adapter

        intent = _repay_intent(protocol="morpho_blue", market_id="0xmarket")
        result = cl._compile_repay_morpho_blue(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.FAILED
        assert "Morpho Blue repay failed" in result.error


# ---------------------------------------------------------------------------
# Curvance
# ---------------------------------------------------------------------------


CURVANCE_ADAPTER = "almanak.connectors.curvance.adapter.CurvanceAdapter"
CURVANCE_CONFIG = "almanak.connectors.curvance.adapter.CurvanceConfig"


class TestCurvanceHelper:
    def test_missing_market_id_fails(self):
        compiler = _mock_compiler(chain="monad")
        intent = RepayIntent.model_construct(
            protocol="curvance",
            token="USDC",
            amount=Decimal("100"),
            repay_full=False,
            market_id=None,
            intent_id="test",
        )
        result = cl._compile_repay_curvance(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.FAILED
        assert "market_id is required for Curvance" in result.error

    @patch(CURVANCE_ADAPTER)
    @patch(CURVANCE_CONFIG)
    def test_debt_token_mismatch_fails(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="monad")
        mock_adapter = MagicMock()
        market = MagicMock()
        market.collateral_symbol = "WETH"
        market.debt_symbol = "USDT"  # mismatch vs USDC
        market.name = "WETH/USDT"
        mock_adapter.get_market.return_value = market
        mock_adapter_cls.return_value = mock_adapter

        intent = _repay_intent(protocol="curvance", token="USDC", market_id="0xmarket")
        result = cl._compile_repay_curvance(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.FAILED
        assert "USDC" in result.error

    @patch(CURVANCE_ADAPTER)
    @patch(CURVANCE_CONFIG)
    def test_success_explicit_amount(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="monad")
        mock_adapter = MagicMock()
        market = MagicMock()
        market.collateral_symbol = "WETH"
        market.debt_symbol = "USDC"
        market.name = "WETH/USDC"
        market.borrowable_ctoken = "0xcbborrow"
        mock_adapter.get_market.return_value = market
        mock_adapter.get_repay_spender.return_value = "0xspender"
        mock_adapter.repay.return_value = _mock_tx_result()
        mock_adapter_cls.return_value = mock_adapter

        intent = _repay_intent(protocol="curvance", market_id="0xmarket")
        result = cl._compile_repay_curvance(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["protocol"] == "curvance"
        assert result.action_bundle.metadata["market_id"] == "0xmarket"
        args, _ = compiler._build_approve_tx.call_args
        assert args[2] == int(Decimal("100") * Decimal(10**6))

    @patch(CURVANCE_ADAPTER)
    @patch(CURVANCE_CONFIG)
    def test_repay_full_approves_max_uint(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="monad")
        mock_adapter = MagicMock()
        market = MagicMock()
        market.collateral_symbol = "WETH"
        market.debt_symbol = "USDC"
        market.name = "WETH/USDC"
        market.borrowable_ctoken = "0xb"
        mock_adapter.get_market.return_value = market
        mock_adapter.get_repay_spender.return_value = "0xs"
        mock_adapter.repay.return_value = _mock_tx_result()
        mock_adapter_cls.return_value = mock_adapter

        intent = _repay_intent(protocol="curvance", market_id="0xmarket", repay_full=True, amount=Decimal("0"))
        result = cl._compile_repay_curvance(compiler, intent, _mock_token("USDC"), None, "full debt", [])
        assert result.status == CompilationStatus.SUCCESS
        args, _ = compiler._build_approve_tx.call_args
        assert args[2] == cl.MAX_UINT256

    @patch(CURVANCE_ADAPTER)
    @patch(CURVANCE_CONFIG)
    def test_repay_failure(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="monad")
        mock_adapter = MagicMock()
        market = MagicMock()
        market.collateral_symbol = "WETH"
        market.debt_symbol = "USDC"
        market.name = "WETH/USDC"
        mock_adapter.get_market.return_value = market
        mock_adapter.get_repay_spender.return_value = "0xs"
        mock_adapter.repay.return_value = _mock_failed_result("interest overflow")
        mock_adapter_cls.return_value = mock_adapter

        intent = _repay_intent(protocol="curvance", market_id="0xmarket")
        result = cl._compile_repay_curvance(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.FAILED
        assert "Curvance repay failed" in result.error


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

        intent = _repay_intent(protocol="aave_v3")
        result = cl._compile_repay_aave_compatible(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.FAILED
        assert "not available on chain" in result.error

    @patch(AAVE_ADAPTER_CLS)
    def test_success_erc20_explicit_amount(self, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        mock_adapter.get_pool_address.return_value = TEST_POOL
        mock_adapter.get_repay_calldata.return_value = b"\x03\x04"
        mock_adapter.estimate_repay_gas.return_value = 200_000
        mock_adapter_cls.return_value = mock_adapter

        intent = _repay_intent(protocol="aave_v3")
        result = cl._compile_repay_aave_compatible(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["pool_address"] == TEST_POOL
        compiler._query_erc20_balance.assert_not_called()

    @patch(AAVE_ADAPTER_CLS)
    def test_repay_full_uses_wallet_balance(self, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        compiler._query_erc20_balance.return_value = 50_000_000  # 50 USDC
        mock_adapter = MagicMock()
        mock_adapter.get_pool_address.return_value = TEST_POOL
        mock_adapter.get_repay_calldata.return_value = b"\x03\x04"
        mock_adapter.estimate_repay_gas.return_value = 200_000
        mock_adapter_cls.return_value = mock_adapter

        intent = _repay_intent(protocol="aave_v3", repay_full=True, amount=Decimal("0"))
        result = cl._compile_repay_aave_compatible(compiler, intent, _mock_token("USDC"), None, "full debt", [])
        assert result.status == CompilationStatus.SUCCESS
        # Wallet balance used directly as repay_amount
        assert result.action_bundle.metadata["repay_amount"] == "50000000"
        compiler._query_erc20_balance.assert_called_once()

    @patch(AAVE_ADAPTER_CLS)
    def test_repay_full_falls_back_to_max_uint_when_balance_unknown(self, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        compiler._query_erc20_balance.return_value = None
        mock_adapter = MagicMock()
        mock_adapter.get_pool_address.return_value = TEST_POOL
        mock_adapter.get_repay_calldata.return_value = b"\x03\x04"
        mock_adapter.estimate_repay_gas.return_value = 200_000
        mock_adapter_cls.return_value = mock_adapter

        intent = _repay_intent(protocol="aave_v3", repay_full=True, amount=Decimal("0"))
        result = cl._compile_repay_aave_compatible(compiler, intent, _mock_token("USDC"), None, "full debt", [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["repay_amount"] == str(cl.MAX_UINT256)

    @patch(AAVE_ADAPTER_CLS)
    def test_native_repay_uses_wrapped_native(self, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        mock_adapter.get_pool_address.return_value = TEST_POOL
        mock_adapter.get_repay_calldata.return_value = b"\x03\x04"
        mock_adapter.estimate_repay_gas.return_value = 200_000
        mock_adapter_cls.return_value = mock_adapter

        intent = _repay_intent(protocol="aave_v3", token="ETH")
        result = cl._compile_repay_aave_compatible(
            compiler,
            intent,
            _mock_token("ETH", decimals=18, is_native=True),
            Decimal("1"),
            "1",
            [],
        )
        assert result.status == CompilationStatus.SUCCESS
        assert any("WETH for repayment" in w for w in result.warnings)


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
            intent = _repay_intent(protocol="spark")
            result = cl._compile_repay_spark(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.FAILED
        assert "not available on chain" in result.error

    def test_success_erc20(self):
        compiler = _mock_compiler(chain="ethereum")
        with (
            patch(SPARK_POOL_ADDRESSES, {"ethereum": "0xspark"}),
            patch(SPARK_ADAPTER) as mock_cls,
            patch(SPARK_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.pool_address = TEST_POOL
            mock_adapter.repay.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="spark")
            result = cl._compile_repay_spark(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["pool_address"] == TEST_POOL

    def test_repay_full_uses_wallet_balance(self):
        compiler = _mock_compiler(chain="ethereum")
        compiler._query_erc20_balance.return_value = 42_000_000
        with (
            patch(SPARK_POOL_ADDRESSES, {"ethereum": "0xspark"}),
            patch(SPARK_ADAPTER) as mock_cls,
            patch(SPARK_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.pool_address = TEST_POOL
            mock_adapter.repay.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="spark", repay_full=True, amount=Decimal("0"))
            result = cl._compile_repay_spark(compiler, intent, _mock_token("USDC"), None, "full debt", [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["repay_amount"] == "42000000"

    def test_native_repay_uses_wrapped_native(self):
        compiler = _mock_compiler(chain="ethereum")
        with (
            patch(SPARK_POOL_ADDRESSES, {"ethereum": "0xspark"}),
            patch(SPARK_ADAPTER) as mock_cls,
            patch(SPARK_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.pool_address = TEST_POOL
            mock_adapter.repay.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="spark", token="ETH")
            result = cl._compile_repay_spark(
                compiler,
                intent,
                _mock_token("ETH", decimals=18, is_native=True),
                Decimal("1"),
                "1",
                [],
            )
        assert result.status == CompilationStatus.SUCCESS
        assert any("WETH for repayment" in w for w in result.warnings)

        # Fix for issue #1621: verify wrap -> approve -> repay ordering.
        tx_types = [tx.tx_type for tx in result.transactions]
        assert tx_types == ["wrap", "approve", "lending_repay"]

        # Wrap tx targets the wrapped-native address with native value and the
        # WETH.deposit() selector 0xd0e30db0.
        wrap_tx = result.transactions[0]
        expected_weth = "0xweth00000000000000000000000000000000eeee"
        assert wrap_tx.to == expected_weth
        assert wrap_tx.value == int(Decimal("1") * Decimal(10**18))
        assert wrap_tx.data == "0xd0e30db0"

    def test_native_repay_without_wrapped_native_fails_fast(self):
        """Fix for issue #1621 (primary): when the wrapped-native address is
        unavailable we must fail compilation instead of silently building an
        approve tx against the native sentinel address."""
        compiler = _mock_compiler(chain="ethereum")
        compiler._get_wrapped_native_address.return_value = None

        with (
            patch(SPARK_POOL_ADDRESSES, {"ethereum": "0xspark"}),
            patch(SPARK_ADAPTER) as mock_cls,
            patch(SPARK_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.pool_address = TEST_POOL
            mock_adapter.repay.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="spark", token="ETH")
            result = cl._compile_repay_spark(
                compiler,
                intent,
                _mock_token("ETH", decimals=18, is_native=True),
                Decimal("1"),
                "1",
                [],
            )

        assert result.status == CompilationStatus.FAILED
        assert "Wrapped native token address not available" in result.error
        assert "ethereum" in result.error
        # Must not have built an approve tx against the native sentinel.
        compiler._build_approve_tx.assert_not_called()

    def test_native_repay_full_fails_fast(self):
        """Addresses Codex P1 review on PR #1634: repay_full on a native token
        would wrap the wallet's entire native balance, leaving nothing to pay
        gas for the wrap tx itself (or the subsequent approve/repay). Compile
        must refuse and require an explicit repay_amount that reserves gas.
        """
        compiler = _mock_compiler(chain="ethereum")
        # Gateway could have returned the full balance, but even a successful
        # query makes the wrap unfundable on non-zero gas-price networks.
        compiler._query_erc20_balance.return_value = 5 * 10**18

        with (
            patch(SPARK_POOL_ADDRESSES, {"ethereum": "0xspark"}),
            patch(SPARK_ADAPTER) as mock_cls,
            patch(SPARK_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.pool_address = TEST_POOL
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="spark", token="ETH", repay_full=True, amount=Decimal("0"))
            result = cl._compile_repay_spark(
                compiler,
                intent,
                _mock_token("ETH", decimals=18, is_native=True),
                None,
                "full debt",
                [],
            )

        assert result.status == CompilationStatus.FAILED
        assert "repay_full is not supported for native" in result.error
        assert "reserves gas" in result.error
        # Helper must bail before querying balance or building any txs.
        compiler._build_approve_tx.assert_not_called()
        mock_adapter.repay.assert_not_called()

    def test_repay_failure(self):
        compiler = _mock_compiler(chain="ethereum")
        with (
            patch(SPARK_POOL_ADDRESSES, {"ethereum": "0xspark"}),
            patch(SPARK_ADAPTER) as mock_cls,
            patch(SPARK_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.pool_address = TEST_POOL
            mock_adapter.repay.return_value = _mock_failed_result("gibberish")
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="spark")
            result = cl._compile_repay_spark(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.FAILED
        assert "Spark repay failed" in result.error


# ---------------------------------------------------------------------------
# Compound V3
# ---------------------------------------------------------------------------


COMPOUND_ADAPTER = "almanak.connectors.compound_v3.adapter.CompoundV3Adapter"
COMPOUND_CONFIG = "almanak.connectors.compound_v3.adapter.CompoundV3Config"
COMPOUND_MARKETS = "almanak.connectors.compound_v3.adapter.COMPOUND_V3_COMET_ADDRESSES"


class TestCompoundV3Helper:
    def test_unsupported_chain_fails(self):
        compiler = _mock_compiler(chain="berachain")
        with patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}):
            intent = _repay_intent(protocol="compound_v3")
            result = cl._compile_repay_compound_v3(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.FAILED
        assert "not available on chain" in result.error

    def test_unknown_market_fails(self):
        compiler = _mock_compiler(chain="ethereum")
        with patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}):
            intent = _repay_intent(protocol="compound_v3", market_id="unknown_market")
            result = cl._compile_repay_compound_v3(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.FAILED
        assert "not available on" in result.error

    def test_success(self):
        compiler = _mock_compiler(chain="ethereum")
        usdc_addr = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = "0xcomet"
            mock_adapter.market_config = {"base_token_address": usdc_addr, "base_token": "USDC"}
            mock_adapter.repay.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="compound_v3")
            result = cl._compile_repay_compound_v3(
                compiler, intent, _mock_token("USDC", address=usdc_addr), Decimal("100"), "100", []
            )
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["market"] == "usdc"

    def test_repay_full_approves_max_uint(self):
        compiler = _mock_compiler(chain="ethereum")
        usdc_addr = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = "0xcomet"
            mock_adapter.market_config = {"base_token_address": usdc_addr, "base_token": "USDC"}
            mock_adapter.repay.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="compound_v3", repay_full=True, amount=Decimal("0"))
            result = cl._compile_repay_compound_v3(
                compiler, intent, _mock_token("USDC", address=usdc_addr), None, "full debt", []
            )
        assert result.status == CompilationStatus.SUCCESS
        args, _ = compiler._build_approve_tx.call_args
        assert args[2] == cl.MAX_UINT256

    def test_repay_failure(self):
        compiler = _mock_compiler(chain="ethereum")
        usdc_addr = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = "0xcomet"
            mock_adapter.market_config = {"base_token_address": usdc_addr, "base_token": "USDC"}
            mock_adapter.repay.return_value = _mock_failed_result("oracle stale")
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="compound_v3")
            result = cl._compile_repay_compound_v3(
                compiler, intent, _mock_token("USDC", address=usdc_addr), Decimal("100"), "100", []
            )
        assert result.status == CompilationStatus.FAILED
        assert "Compound V3 repay failed" in result.error

    def test_non_base_asset_rejected(self):
        """Repaying with a non-base asset (e.g. collateral token) must fail fast at compile time (#1620)."""
        compiler = _mock_compiler(chain="ethereum")
        usdc_addr = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        weth_addr = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = "0xcomet"
            mock_adapter.market_config = {"base_token_address": usdc_addr, "base_token": "USDC"}
            mock_cls.return_value = mock_adapter
            # Intent tries to repay WETH against the USDC market - must be rejected.
            intent = _repay_intent(protocol="compound_v3", token="WETH")
            result = cl._compile_repay_compound_v3(
                compiler, intent, _mock_token("WETH", address=weth_addr, decimals=18), Decimal("1"), "1", []
            )
        assert result.status == CompilationStatus.FAILED
        assert "Compound V3 usdc market expects base asset" in result.error
        assert usdc_addr in result.error
        assert weth_addr in result.error
        assert "single-asset" in result.error
        # Ensure we never reached the adapter repay or approve call.
        mock_adapter.repay.assert_not_called()
        compiler._build_approve_tx.assert_not_called()

    def test_case_insensitive_base_asset_match(self):
        """Base token comparison is case-insensitive - a lowercase token address matches mixed-case market address."""
        compiler = _mock_compiler(chain="ethereum")
        usdc_addr_mixed = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        usdc_addr_lower = usdc_addr_mixed.lower()
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = "0xcomet"
            mock_adapter.market_config = {"base_token_address": usdc_addr_mixed, "base_token": "USDC"}
            mock_adapter.repay.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="compound_v3")
            result = cl._compile_repay_compound_v3(
                compiler, intent, _mock_token("USDC", address=usdc_addr_lower), Decimal("100"), "100", []
            )
        assert result.status == CompilationStatus.SUCCESS

    def test_missing_base_token_address_fails(self):
        """If the adapter's market_config is missing base_token_address, fail with a clear error."""
        compiler = _mock_compiler(chain="ethereum")
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = "0xcomet"
            mock_adapter.market_config = {}  # No base_token_address.
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="compound_v3")
            result = cl._compile_repay_compound_v3(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.FAILED
        assert "missing base_token_address" in result.error


# ---------------------------------------------------------------------------
# BENQI
# ---------------------------------------------------------------------------


BENQI_ADAPTER = "almanak.connectors.benqi.adapter.BenqiAdapter"
BENQI_CONFIG = "almanak.connectors.benqi.adapter.BenqiConfig"


class TestBenqiHelper:
    def test_non_avalanche_fails(self):
        compiler = _mock_compiler(chain="ethereum")
        intent = _repay_intent(protocol="benqi")
        result = cl._compile_repay_benqi(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.FAILED
        assert "only available on Avalanche" in result.error

    def test_unsupported_asset_fails(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(BENQI_ADAPTER) as mock_cls, patch(BENQI_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.get_market_info.return_value = None
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="benqi", token="XYZ")
            result = cl._compile_repay_benqi(compiler, intent, _mock_token("XYZ"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.FAILED
        assert "does not support asset" in result.error

    def test_success_erc20(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(BENQI_ADAPTER) as mock_cls, patch(BENQI_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.comptroller_address = "0xcomp"
            market = MagicMock()
            market.qi_token_address = "0xqi"
            market.is_native = False
            mock_adapter.get_market_info.return_value = market
            mock_adapter.repay.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="benqi")
            result = cl._compile_repay_benqi(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["protocol"] == "benqi"

    def test_erc20_missing_amount_without_repay_full_fails(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(BENQI_ADAPTER) as mock_cls, patch(BENQI_CONFIG):
            mock_adapter = MagicMock()
            market = MagicMock()
            market.qi_token_address = "0xqi"
            market.is_native = False
            mock_adapter.get_market_info.return_value = market
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="benqi")
            result = cl._compile_repay_benqi(compiler, intent, _mock_token("USDC"), None, "x", [])
        assert result.status == CompilationStatus.FAILED
        assert "requires an explicit amount" in result.error

    def test_repay_full_approves_benqi_max_uint(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(BENQI_ADAPTER) as mock_cls, patch(BENQI_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.comptroller_address = "0xcomp"
            market = MagicMock()
            market.qi_token_address = "0xqi"
            market.is_native = False
            mock_adapter.get_market_info.return_value = market
            mock_adapter.repay.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="benqi", repay_full=True, amount=Decimal("0"))
            result = cl._compile_repay_benqi(compiler, intent, _mock_token("USDC"), None, "full debt", [])
        assert result.status == CompilationStatus.SUCCESS
        # BENQI's MAX_UINT256 is imported locally but equals the global MAX_UINT256 constant
        args, _ = compiler._build_approve_tx.call_args
        assert args[2] == cl.MAX_UINT256

    def test_repay_failure(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(BENQI_ADAPTER) as mock_cls, patch(BENQI_CONFIG):
            mock_adapter = MagicMock()
            market = MagicMock()
            market.qi_token_address = "0xqi"
            market.is_native = False
            mock_adapter.get_market_info.return_value = market
            mock_adapter.repay.return_value = _mock_failed_result("frozen market")
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="benqi")
            result = cl._compile_repay_benqi(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.FAILED
        assert "BENQI repay failed" in result.error


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
        intent = _repay_intent(protocol="euler_v2", token="XYZ")
        result = cl._compile_repay_euler_v2(compiler, intent, _mock_token("XYZ"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.FAILED
        assert "Euler V2 does not have a vault" in result.error

    @patch(EULER_ADAPTER)
    @patch(EULER_CONFIG)
    def test_missing_amount_without_repay_full_fails(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        vault = MagicMock()
        vault.vault_address = "0xvault"
        vault.vault_symbol = "eUSDC"
        mock_adapter.find_vault_for_asset.return_value = vault
        mock_adapter_cls.return_value = mock_adapter
        intent = _repay_intent(protocol="euler_v2")
        result = cl._compile_repay_euler_v2(compiler, intent, _mock_token("USDC"), None, "x", [])
        assert result.status == CompilationStatus.FAILED
        assert "requires an explicit amount" in result.error

    @patch(EULER_ADAPTER)
    @patch(EULER_CONFIG)
    def test_success(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        vault = MagicMock()
        vault.vault_address = "0xvault"
        vault.vault_symbol = "eUSDC"
        mock_adapter.find_vault_for_asset.return_value = vault
        mock_adapter.repay.return_value = _mock_tx_result()
        mock_adapter_cls.return_value = mock_adapter
        intent = _repay_intent(protocol="euler_v2")
        result = cl._compile_repay_euler_v2(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["vault_address"] == "0xvault"

    @patch(EULER_ADAPTER)
    @patch(EULER_CONFIG)
    def test_repay_full_approves_max_uint(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        vault = MagicMock()
        vault.vault_address = "0xvault"
        vault.vault_symbol = "eUSDC"
        mock_adapter.find_vault_for_asset.return_value = vault
        mock_adapter.repay.return_value = _mock_tx_result()
        mock_adapter_cls.return_value = mock_adapter
        intent = _repay_intent(protocol="euler_v2", repay_full=True, amount=Decimal("0"))
        result = cl._compile_repay_euler_v2(compiler, intent, _mock_token("USDC"), None, "full debt", [])
        assert result.status == CompilationStatus.SUCCESS
        args, _ = compiler._build_approve_tx.call_args
        assert args[2] == cl.MAX_UINT256

    @patch(EULER_ADAPTER)
    @patch(EULER_CONFIG)
    def test_repay_failure(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        vault = MagicMock()
        vault.vault_address = "0xvault"
        vault.vault_symbol = "eUSDC"
        mock_adapter.find_vault_for_asset.return_value = vault
        mock_adapter.repay.return_value = _mock_failed_result("paused")
        mock_adapter_cls.return_value = mock_adapter
        intent = _repay_intent(protocol="euler_v2")
        result = cl._compile_repay_euler_v2(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.FAILED
        assert "Euler V2 repay failed" in result.error


# ---------------------------------------------------------------------------
# Silo V2
# ---------------------------------------------------------------------------


SILO_ADAPTER = "almanak.connectors.silo_v2.adapter.SiloV2Adapter"
SILO_CONFIG = "almanak.connectors.silo_v2.adapter.SiloV2Config"


class TestSiloV2Helper:
    def test_non_avalanche_fails(self):
        compiler = _mock_compiler(chain="ethereum")
        intent = _repay_intent(protocol="silo_v2")
        result = cl._compile_repay_silo_v2(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.FAILED
        assert "only available on Avalanche" in result.error

    def test_no_silo_for_asset(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(SILO_ADAPTER) as mock_cls, patch(SILO_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.find_silo_for_asset.return_value = None
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="silo_v2")
            result = cl._compile_repay_silo_v2(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
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
            mock_adapter.repay.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="silo_v2")
            result = cl._compile_repay_silo_v2(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["protocol"] == "silo_v2"
        assert result.action_bundle.metadata["market_name"] == "USDC-WAVAX"

    def test_missing_amount_without_repay_full_fails(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(SILO_ADAPTER) as mock_cls, patch(SILO_CONFIG):
            mock_adapter = MagicMock()
            market = MagicMock()
            market.silo_config = "0xsc"
            market.market_name = "m"
            mock_adapter.find_silo_for_asset.return_value = (market, "0xsilo", "0xtok")
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="silo_v2")
            result = cl._compile_repay_silo_v2(compiler, intent, _mock_token("USDC"), None, "x", [])
        assert result.status == CompilationStatus.FAILED
        assert "requires an explicit amount" in result.error

    def test_repay_full_approves_max_uint(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(SILO_ADAPTER) as mock_cls, patch(SILO_CONFIG):
            mock_adapter = MagicMock()
            market = MagicMock()
            market.silo_config = "0xsc"
            market.market_name = "m"
            mock_adapter.find_silo_for_asset.return_value = (market, "0xsilo", "0xtok")
            mock_adapter.repay.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="silo_v2", repay_full=True, amount=Decimal("0"))
            result = cl._compile_repay_silo_v2(compiler, intent, _mock_token("USDC"), None, "full debt", [])
        assert result.status == CompilationStatus.SUCCESS
        args, _ = compiler._build_approve_tx.call_args
        assert args[2] == cl.MAX_UINT256

    def test_repay_failure(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(SILO_ADAPTER) as mock_cls, patch(SILO_CONFIG):
            mock_adapter = MagicMock()
            market = MagicMock()
            market.silo_config = "0xsc"
            market.market_name = "m"
            mock_adapter.find_silo_for_asset.return_value = (market, "0xsilo", "0xtok")
            mock_adapter.repay.return_value = _mock_failed_result("hf low")
            mock_cls.return_value = mock_adapter
            intent = _repay_intent(protocol="silo_v2")
            result = cl._compile_repay_silo_v2(compiler, intent, _mock_token("USDC"), Decimal("100"), "100", [])
        assert result.status == CompilationStatus.FAILED
        assert "Silo V2 repay failed" in result.error


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
