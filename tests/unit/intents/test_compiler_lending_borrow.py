"""Unit tests targeting the per-protocol helpers extracted from ``compile_borrow``.

Phase 2a of the coverage-improvement plan (see prompt). The helpers are private
module-level functions in ``almanak.connectors._strategy_base.base.lending.aave_helpers``:

- ``compile_borrow`` (thin dispatcher)
- ``_compile_borrow_jupiter_lend`` (Solana)
- ``_compile_borrow_kamino`` (Solana / Solana fallback)
- ``_compile_borrow_morpho_blue``
- ``_compile_borrow_curvance``
- ``_compile_borrow_aave_compatible``
- ``_compile_borrow_spark``
- ``_compile_borrow_compound_v3``
- ``_compile_borrow_benqi``
- ``_compile_borrow_euler_v2``
- ``_compile_borrow_silo_v2``

Each test builds a minimal mocked ``compiler`` (no IntentCompiler instantiation)
plus mocked tokens and adapters, and verifies the helper either succeeds or
fails with a specific ``CompilationStatus.FAILED`` error message.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.base.lending import aave_helpers as cl
from almanak.framework.intents import BorrowIntent
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
      - _format_amount()
      - _get_wrapped_native_address()
      - _compile_jupiter_lend_borrow() / _compile_kamino_borrow()
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
    # Use Decimal to avoid floating-point precision loss for 18-decimal token integers.
    compiler._format_amount.side_effect = lambda amount, decimals: str(Decimal(amount) / Decimal(10**decimals))
    compiler._get_wrapped_native_address.return_value = "0xweth00000000000000000000000000000000eeee"
    return compiler


def _borrow_intent(
    *,
    protocol: str = "compound_v3",
    collateral_token: str = "WETH",
    collateral_amount: Decimal = Decimal("1"),
    borrow_token: str = "USDC",
    borrow_amount: Decimal = Decimal("500"),
    market_id: str | None = None,
) -> BorrowIntent:
    return BorrowIntent(
        protocol=protocol,
        collateral_token=collateral_token,
        collateral_amount=collateral_amount,
        borrow_token=borrow_token,
        borrow_amount=borrow_amount,
        market_id=market_id,
    )


# ---------------------------------------------------------------------------
# Dispatcher — routing and unsupported-protocol handling
# ---------------------------------------------------------------------------


class TestMorphoBlueHelper:
    def test_missing_market_id_fails(self):
        """Defensive branch: the helper rejects market_id=None.

        Pydantic normally blocks this at construction; use ``model_construct``
        to bypass validation and hit the helper's internal guard directly.
        """
        compiler = _mock_compiler()
        intent = BorrowIntent.model_construct(
            protocol="morpho_blue",
            collateral_token="WETH",
            collateral_amount=Decimal("1"),
            borrow_token="USDC",
            borrow_amount=Decimal("500"),
            market_id=None,
            intent_id="test",
        )
        result = cl._compile_borrow_morpho_blue(
            compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
        )
        assert result.status == CompilationStatus.FAILED
        assert "market_id is required for Morpho Blue" in result.error

    @patch(MORPHO_ADAPTER)
    @patch(MORPHO_CONFIG)
    def test_success_with_collateral(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler()
        mock_adapter = MagicMock()
        mock_adapter.morpho_address = "0xmorpho"
        mock_adapter.supply_collateral.return_value = _mock_tx_result(desc="supply")
        mock_adapter.borrow.return_value = _mock_tx_result(desc="borrow")
        mock_adapter_cls.return_value = mock_adapter

        intent = _borrow_intent(protocol="morpho_blue", market_id="0xmarket")
        result = cl._compile_borrow_morpho_blue(
            compiler,
            intent,
            _mock_token("WETH", decimals=18),
            _mock_token("USDC"),
            Decimal("1"),
        )
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["protocol"] == "morpho_blue"
        assert result.action_bundle.metadata["market_id"] == "0xmarket"
        mock_adapter.supply_collateral.assert_called_once()
        mock_adapter.borrow.assert_called_once()

    @patch(MORPHO_ADAPTER)
    @patch(MORPHO_CONFIG)
    def test_success_zero_collateral_borrow_against_existing(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler()
        mock_adapter = MagicMock()
        mock_adapter.morpho_address = "0xmorpho"
        mock_adapter.borrow.return_value = _mock_tx_result(desc="borrow")
        mock_adapter_cls.return_value = mock_adapter

        intent = _borrow_intent(protocol="morpho_blue", market_id="0xmarket", collateral_amount=Decimal("0"))
        result = cl._compile_borrow_morpho_blue(
            compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("0")
        )
        assert result.status == CompilationStatus.SUCCESS
        mock_adapter.supply_collateral.assert_not_called()
        assert any("No collateral supplied" in w for w in result.warnings)

    @patch(MORPHO_ADAPTER)
    @patch(MORPHO_CONFIG)
    def test_supply_collateral_failure(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler()
        mock_adapter = MagicMock()
        mock_adapter.morpho_address = "0xmorpho"
        mock_adapter.supply_collateral.return_value = _mock_failed_result("bad market")
        mock_adapter_cls.return_value = mock_adapter

        intent = _borrow_intent(protocol="morpho_blue", market_id="0xmarket")
        result = cl._compile_borrow_morpho_blue(
            compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
        )
        assert result.status == CompilationStatus.FAILED
        assert "Morpho Blue supply collateral failed" in result.error

    @patch(MORPHO_ADAPTER)
    @patch(MORPHO_CONFIG)
    def test_borrow_failure(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler()
        mock_adapter = MagicMock()
        mock_adapter.morpho_address = "0xmorpho"
        mock_adapter.supply_collateral.return_value = _mock_tx_result()
        mock_adapter.borrow.return_value = _mock_failed_result("insufficient collateral")
        mock_adapter_cls.return_value = mock_adapter

        intent = _borrow_intent(protocol="morpho_blue", market_id="0xmarket")
        result = cl._compile_borrow_morpho_blue(
            compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
        )
        assert result.status == CompilationStatus.FAILED
        assert "Morpho Blue borrow failed" in result.error


# ---------------------------------------------------------------------------
# Curvance
# ---------------------------------------------------------------------------


CURVANCE_ADAPTER = "almanak.connectors.curvance.adapter.CurvanceAdapter"
CURVANCE_CONFIG = "almanak.connectors.curvance.adapter.CurvanceConfig"


class TestCurvanceHelper:
    def test_missing_market_id_fails(self):
        """Defensive branch: the helper rejects market_id=None.

        Pydantic normally blocks this at construction; use ``model_construct``
        to bypass validation and hit the helper's internal guard directly.
        """
        compiler = _mock_compiler(chain="monad")
        intent = BorrowIntent.model_construct(
            protocol="curvance",
            collateral_token="WETH",
            collateral_amount=Decimal("1"),
            borrow_token="USDC",
            borrow_amount=Decimal("500"),
            market_id=None,
            intent_id="test",
        )
        result = cl._compile_borrow_curvance(
            compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
        )
        assert result.status == CompilationStatus.FAILED
        assert "market_id is required for Curvance" in result.error

    @patch(CURVANCE_ADAPTER)
    @patch(CURVANCE_CONFIG)
    def test_token_mismatch_fails(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="monad")
        mock_adapter = MagicMock()
        market = MagicMock()
        market.collateral_symbol = "WBTC"
        market.debt_symbol = "USDC"
        market.name = "WBTC/USDC"
        mock_adapter.get_market.return_value = market
        mock_adapter_cls.return_value = mock_adapter

        intent = _borrow_intent(protocol="curvance", market_id="0xmarket", collateral_token="WETH")
        result = cl._compile_borrow_curvance(
            compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
        )
        assert result.status == CompilationStatus.FAILED
        assert "WETH" in result.error

    @patch(CURVANCE_ADAPTER)
    @patch(CURVANCE_CONFIG)
    def test_success(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="monad")
        mock_adapter = MagicMock()
        market = MagicMock()
        market.collateral_symbol = "WETH"
        market.debt_symbol = "USDC"
        market.name = "WETH/USDC"
        market.collateral_ctoken = "0xccoll"
        market.borrowable_ctoken = "0xcborrow"
        mock_adapter.get_market.return_value = market
        mock_adapter.get_supply_spender.return_value = "0xspender"
        mock_adapter.supply_collateral.return_value = _mock_tx_result()
        mock_adapter.borrow.return_value = _mock_tx_result()
        mock_adapter_cls.return_value = mock_adapter

        intent = _borrow_intent(protocol="curvance", market_id="0xmarket")
        result = cl._compile_borrow_curvance(
            compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
        )
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["protocol"] == "curvance"

    @patch(CURVANCE_ADAPTER)
    @patch(CURVANCE_CONFIG)
    def test_supply_failure(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="monad")
        mock_adapter = MagicMock()
        market = MagicMock()
        market.collateral_symbol = "WETH"
        market.debt_symbol = "USDC"
        market.name = "WETH/USDC"
        mock_adapter.get_market.return_value = market
        mock_adapter.get_supply_spender.return_value = "0xspender"
        mock_adapter.supply_collateral.return_value = _mock_failed_result("bad deposit")
        mock_adapter_cls.return_value = mock_adapter

        intent = _borrow_intent(protocol="curvance", market_id="0xmarket")
        result = cl._compile_borrow_curvance(
            compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
        )
        assert result.status == CompilationStatus.FAILED
        assert "Curvance supply collateral failed" in result.error

    @patch(CURVANCE_ADAPTER)
    @patch(CURVANCE_CONFIG)
    def test_borrow_failure(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="monad")
        mock_adapter = MagicMock()
        market = MagicMock()
        market.collateral_symbol = "WETH"
        market.debt_symbol = "USDC"
        market.name = "WETH/USDC"
        mock_adapter.get_market.return_value = market
        mock_adapter.get_supply_spender.return_value = "0xspender"
        mock_adapter.supply_collateral.return_value = _mock_tx_result()
        mock_adapter.borrow.return_value = _mock_failed_result("oracle error")
        mock_adapter_cls.return_value = mock_adapter

        intent = _borrow_intent(protocol="curvance", market_id="0xmarket")
        result = cl._compile_borrow_curvance(
            compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
        )
        assert result.status == CompilationStatus.FAILED
        assert "Curvance borrow failed" in result.error


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

        intent = _borrow_intent(protocol="aave_v3")
        result = cl._compile_borrow_aave_compatible(
            compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
        )
        assert result.status == CompilationStatus.FAILED
        assert "not available on chain" in result.error

    @patch(AAVE_ADAPTER_CLS)
    def test_success_erc20_collateral(self, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        mock_adapter.get_pool_address.return_value = TEST_POOL
        mock_adapter.get_supply_calldata.return_value = b"\x01\x02"
        mock_adapter.get_borrow_calldata.return_value = b"\x03\x04"
        mock_adapter.estimate_supply_gas.return_value = 200_000
        mock_adapter.estimate_borrow_gas.return_value = 250_000
        mock_adapter_cls.return_value = mock_adapter

        intent = _borrow_intent(protocol="aave_v3")
        result = cl._compile_borrow_aave_compatible(
            compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
        )
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["pool_address"] == TEST_POOL

    @patch(AAVE_ADAPTER_CLS)
    def test_native_collateral_succeeds_when_wrapped_available(self, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        mock_adapter.get_pool_address.return_value = TEST_POOL
        mock_adapter.get_supply_calldata.return_value = b"\x01\x02"
        mock_adapter.get_borrow_calldata.return_value = b"\x03\x04"
        mock_adapter.estimate_supply_gas.return_value = 200_000
        mock_adapter.estimate_borrow_gas.return_value = 250_000
        mock_adapter_cls.return_value = mock_adapter

        intent = _borrow_intent(protocol="aave_v3")
        result = cl._compile_borrow_aave_compatible(
            compiler,
            intent,
            _mock_token("ETH", decimals=18, is_native=True),
            _mock_token("USDC"),
            Decimal("1"),
        )
        assert result.status == CompilationStatus.SUCCESS
        assert any("wrapped before supplying" in w for w in result.warnings)
        # Regression: helper must emit a wrap tx, an approve tx for the wrapped
        # token, and a supply tx with value=0 (calldata carries the wrapped
        # asset address). Without the wrap tx the on-chain approve would fail
        # because the wallet would have zero wrapped-native balance.
        assert result.transactions[0].tx_type == "wrap"
        # Decimal("1") with 18 decimals -> exactly 10**18 wei wrapped
        assert result.transactions[0].value == 10**18
        assert result.transactions[0].data == "0xd0e30db0"
        tx_types = [tx.tx_type for tx in result.transactions]
        assert "approve" in tx_types
        # Wrap must precede approve and approve must precede supply so the
        # pool can actually pull the wrapped token from the wallet.
        wrap_idx = tx_types.index("wrap")
        approve_idx = tx_types.index("approve")
        supply_idx = tx_types.index("lending_supply")
        assert wrap_idx < approve_idx < supply_idx
        supply_txs = [tx for tx in result.transactions if tx.tx_type == "lending_supply"]
        assert len(supply_txs) == 1
        assert supply_txs[0].value == 0
        # Approve must target the wrapped-native address (WETH), not native ETH,
        # so the on-chain approve has a real token balance to allow.
        compiler._build_approve_tx.assert_called_once()
        approve_args = compiler._build_approve_tx.call_args.args
        assert approve_args[0] == "0xweth00000000000000000000000000000000eeee"

    @patch(AAVE_ADAPTER_CLS)
    def test_native_collateral_without_weth_address_fails(self, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        compiler._get_wrapped_native_address.return_value = None
        mock_adapter = MagicMock()
        mock_adapter.get_pool_address.return_value = TEST_POOL
        mock_adapter_cls.return_value = mock_adapter

        intent = _borrow_intent(protocol="aave_v3")
        result = cl._compile_borrow_aave_compatible(
            compiler,
            intent,
            _mock_token("ETH", decimals=18, is_native=True),
            _mock_token("USDC"),
            Decimal("1"),
        )
        assert result.status == CompilationStatus.FAILED
        assert "wrapped native token address not found" in result.error

    @patch(AAVE_ADAPTER_CLS)
    def test_zero_collateral_warns_and_only_borrows(self, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        mock_adapter.get_pool_address.return_value = TEST_POOL
        mock_adapter.get_borrow_calldata.return_value = b"\x03\x04"
        mock_adapter.estimate_borrow_gas.return_value = 250_000
        mock_adapter_cls.return_value = mock_adapter

        intent = _borrow_intent(protocol="aave_v3", collateral_amount=Decimal("0"))
        result = cl._compile_borrow_aave_compatible(
            compiler,
            intent,
            _mock_token("WETH", decimals=18),
            _mock_token("USDC"),
            Decimal("0"),
        )
        assert result.status == CompilationStatus.SUCCESS
        assert any("No collateral supplied" in w for w in result.warnings)
        # Behavioral assertions: with zero collateral the helper must skip the supply path entirely
        # (no approve, no supply calldata, no supply tx) and emit only the borrow transaction.
        mock_adapter.get_supply_calldata.assert_not_called()
        mock_adapter.estimate_supply_gas.assert_not_called()
        compiler._build_approve_tx.assert_not_called()
        assert all((tx.tx_type or "") != "lending_supply" for tx in result.transactions)
        assert any(tx.tx_type == "lending_borrow" for tx in result.transactions)


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------


SPARK_POOLS = "almanak.connectors.spark.SPARK_POOL_ADDRESSES"
SPARK_ADAPTER = "almanak.connectors.spark.SparkAdapter"
SPARK_CONFIG = "almanak.connectors.spark.SparkConfig"


class TestSparkHelper:
    def test_unsupported_chain_fails(self):
        compiler = _mock_compiler(chain="polygon")
        with patch(SPARK_POOLS, {"ethereum": "0xpool"}):
            intent = _borrow_intent(protocol="spark")
            result = cl._compile_borrow_spark(
                compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
            )
        assert result.status == CompilationStatus.FAILED
        assert "Spark not available on chain" in result.error

    def test_success_erc20_collateral(self):
        compiler = _mock_compiler(chain="ethereum")
        with patch(SPARK_POOLS, {"ethereum": "0xpool"}), patch(SPARK_ADAPTER) as mock_adapter_cls, patch(SPARK_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.pool_address = TEST_POOL
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_adapter.borrow.return_value = _mock_tx_result()
            mock_adapter_cls.return_value = mock_adapter

            intent = _borrow_intent(protocol="spark")
            result = cl._compile_borrow_spark(
                compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
            )
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["protocol"] == "spark"

    def test_native_collateral_wraps_and_approves(self):
        compiler = _mock_compiler(chain="ethereum")
        with patch(SPARK_POOLS, {"ethereum": "0xpool"}), patch(SPARK_ADAPTER) as mock_adapter_cls, patch(SPARK_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.pool_address = TEST_POOL
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_adapter.borrow.return_value = _mock_tx_result()
            mock_adapter_cls.return_value = mock_adapter

            intent = _borrow_intent(protocol="spark")
            result = cl._compile_borrow_spark(
                compiler,
                intent,
                _mock_token("ETH", decimals=18, is_native=True),
                _mock_token("USDC"),
                Decimal("1"),
            )
        assert result.status == CompilationStatus.SUCCESS
        assert any("wrapped before supplying" in w for w in result.warnings)
        # Wrap tx should be the first transaction
        assert result.transactions[0].tx_type == "wrap"

    def test_native_collateral_without_weth_address_fails(self):
        compiler = _mock_compiler(chain="ethereum")
        compiler._get_wrapped_native_address.return_value = None
        with patch(SPARK_POOLS, {"ethereum": "0xpool"}), patch(SPARK_ADAPTER) as mock_adapter_cls, patch(SPARK_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.pool_address = TEST_POOL
            mock_adapter_cls.return_value = mock_adapter

            intent = _borrow_intent(protocol="spark")
            result = cl._compile_borrow_spark(
                compiler,
                intent,
                _mock_token("ETH", decimals=18, is_native=True),
                _mock_token("USDC"),
                Decimal("1"),
            )
        assert result.status == CompilationStatus.FAILED
        assert "wrapped native token address not found" in result.error

    def test_supply_failure(self):
        compiler = _mock_compiler(chain="ethereum")
        with patch(SPARK_POOLS, {"ethereum": "0xpool"}), patch(SPARK_ADAPTER) as mock_adapter_cls, patch(SPARK_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.pool_address = TEST_POOL
            mock_adapter.supply.return_value = _mock_failed_result("no balance")
            mock_adapter_cls.return_value = mock_adapter

            intent = _borrow_intent(protocol="spark")
            result = cl._compile_borrow_spark(
                compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
            )
        assert result.status == CompilationStatus.FAILED
        assert "Spark supply collateral failed" in result.error

    def test_borrow_failure(self):
        compiler = _mock_compiler(chain="ethereum")
        with patch(SPARK_POOLS, {"ethereum": "0xpool"}), patch(SPARK_ADAPTER) as mock_adapter_cls, patch(SPARK_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.pool_address = TEST_POOL
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_adapter.borrow.return_value = _mock_failed_result("hf too low")
            mock_adapter_cls.return_value = mock_adapter

            intent = _borrow_intent(protocol="spark")
            result = cl._compile_borrow_spark(
                compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
            )
        assert result.status == CompilationStatus.FAILED
        assert "Spark borrow failed" in result.error


# ---------------------------------------------------------------------------
# Compound V3
# ---------------------------------------------------------------------------


COMET_ADDRESSES = "almanak.connectors.compound_v3.adapter.COMPOUND_V3_COMET_ADDRESSES"
COMPOUND_ADAPTER = "almanak.connectors.compound_v3.adapter.CompoundV3Adapter"
COMPOUND_CONFIG = "almanak.connectors.compound_v3.adapter.CompoundV3Config"


class TestCompoundV3Helper:
    def test_unsupported_chain_fails(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(COMET_ADDRESSES, {"ethereum": {"usdc": {"comet_address": TEST_POOL}}}):
            intent = _borrow_intent(protocol="compound_v3")
            result = cl._compile_borrow_compound_v3(
                compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
            )
        assert result.status == CompilationStatus.FAILED
        assert "Compound V3 not available on chain" in result.error

    def test_unsupported_market_fails(self):
        compiler = _mock_compiler(chain="ethereum")
        with patch(COMET_ADDRESSES, {"ethereum": {"usdc": {"comet_address": TEST_POOL}}}):
            intent = _borrow_intent(protocol="compound_v3", market_id="noexist")
            result = cl._compile_borrow_compound_v3(
                compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
            )
        assert result.status == CompilationStatus.FAILED
        assert "market 'noexist' not available" in result.error

    def test_success_with_collateral(self):
        compiler = _mock_compiler(chain="ethereum")
        usdc_addr = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        with (
            patch(COMET_ADDRESSES, {"ethereum": {"usdc": {"comet_address": TEST_POOL}}}),
            patch(COMPOUND_ADAPTER) as mock_adapter_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = TEST_POOL
            mock_adapter.market_config = {"base_token_address": usdc_addr, "base_token": "USDC"}
            mock_adapter.supply_collateral.return_value = _mock_tx_result()
            mock_adapter.borrow.return_value = _mock_tx_result()
            mock_adapter_cls.return_value = mock_adapter
            intent = _borrow_intent(protocol="compound_v3")
            result = cl._compile_borrow_compound_v3(
                compiler,
                intent,
                _mock_token("WETH", decimals=18),
                _mock_token("USDC", address=usdc_addr),
                Decimal("1"),
            )
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["market"] == "usdc"

    def test_supply_collateral_failure(self):
        compiler = _mock_compiler(chain="ethereum")
        usdc_addr = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        with (
            patch(COMET_ADDRESSES, {"ethereum": {"usdc": {"comet_address": TEST_POOL}}}),
            patch(COMPOUND_ADAPTER) as mock_adapter_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = TEST_POOL
            mock_adapter.market_config = {"base_token_address": usdc_addr, "base_token": "USDC"}
            mock_adapter.supply_collateral.return_value = _mock_failed_result("asset unsupported")
            mock_adapter_cls.return_value = mock_adapter
            intent = _borrow_intent(protocol="compound_v3")
            result = cl._compile_borrow_compound_v3(
                compiler,
                intent,
                _mock_token("WETH", decimals=18),
                _mock_token("USDC", address=usdc_addr),
                Decimal("1"),
            )
        assert result.status == CompilationStatus.FAILED
        assert "Compound V3 supply collateral failed" in result.error

    def test_borrow_failure(self):
        compiler = _mock_compiler(chain="ethereum")
        usdc_addr = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        with (
            patch(COMET_ADDRESSES, {"ethereum": {"usdc": {"comet_address": TEST_POOL}}}),
            patch(COMPOUND_ADAPTER) as mock_adapter_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = TEST_POOL
            mock_adapter.market_config = {"base_token_address": usdc_addr, "base_token": "USDC"}
            mock_adapter.supply_collateral.return_value = _mock_tx_result()
            mock_adapter.borrow.return_value = _mock_failed_result("insufficient liquidity")
            mock_adapter_cls.return_value = mock_adapter
            intent = _borrow_intent(protocol="compound_v3")
            result = cl._compile_borrow_compound_v3(
                compiler,
                intent,
                _mock_token("WETH", decimals=18),
                _mock_token("USDC", address=usdc_addr),
                Decimal("1"),
            )
        assert result.status == CompilationStatus.FAILED
        assert "Compound V3 borrow failed" in result.error

    def test_non_base_asset_rejected(self):
        """Borrowing a non-base asset (collateral) must fail fast at compile time (#1620)."""
        compiler = _mock_compiler(chain="ethereum")
        usdc_addr = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        weth_addr = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
        with (
            patch(COMET_ADDRESSES, {"ethereum": {"usdc": {"comet_address": TEST_POOL}}}),
            patch(COMPOUND_ADAPTER) as mock_adapter_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = TEST_POOL
            mock_adapter.market_config = {"base_token_address": usdc_addr, "base_token": "USDC"}
            mock_adapter_cls.return_value = mock_adapter
            # Intent tries to borrow WETH from the USDC market - must be rejected.
            intent = _borrow_intent(protocol="compound_v3", borrow_token="WETH")
            result = cl._compile_borrow_compound_v3(
                compiler,
                intent,
                _mock_token("WBTC", address="0x" + "11" * 20, decimals=8),
                _mock_token("WETH", address=weth_addr, decimals=18),
                Decimal("1"),
            )
        assert result.status == CompilationStatus.FAILED
        assert "Compound V3 usdc market expects base asset" in result.error
        assert usdc_addr in result.error
        assert weth_addr in result.error
        assert "single-asset" in result.error
        # Ensure we never reached the adapter borrow call.
        mock_adapter.borrow.assert_not_called()
        mock_adapter.supply_collateral.assert_not_called()

    def test_case_insensitive_base_asset_match(self):
        """Base token comparison is case-insensitive - lowercase addresses still match."""
        compiler = _mock_compiler(chain="ethereum")
        usdc_addr_mixed = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        usdc_addr_lower = usdc_addr_mixed.lower()
        with (
            patch(COMET_ADDRESSES, {"ethereum": {"usdc": {"comet_address": TEST_POOL}}}),
            patch(COMPOUND_ADAPTER) as mock_adapter_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = TEST_POOL
            mock_adapter.market_config = {"base_token_address": usdc_addr_mixed, "base_token": "USDC"}
            mock_adapter.supply_collateral.return_value = _mock_tx_result()
            mock_adapter.borrow.return_value = _mock_tx_result()
            mock_adapter_cls.return_value = mock_adapter
            intent = _borrow_intent(protocol="compound_v3")
            result = cl._compile_borrow_compound_v3(
                compiler,
                intent,
                _mock_token("WETH", decimals=18),
                _mock_token("USDC", address=usdc_addr_lower),
                Decimal("1"),
            )
        assert result.status == CompilationStatus.SUCCESS

    def test_missing_base_token_address_fails(self):
        """If the adapter's market_config is missing base_token_address, fail with a clear error."""
        compiler = _mock_compiler(chain="ethereum")
        with (
            patch(COMET_ADDRESSES, {"ethereum": {"usdc": {"comet_address": TEST_POOL}}}),
            patch(COMPOUND_ADAPTER) as mock_adapter_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = TEST_POOL
            mock_adapter.market_config = {}  # No base_token_address.
            mock_adapter_cls.return_value = mock_adapter
            intent = _borrow_intent(protocol="compound_v3")
            result = cl._compile_borrow_compound_v3(
                compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
            )
        assert result.status == CompilationStatus.FAILED
        assert "missing base_token_address" in result.error


# ---------------------------------------------------------------------------
# BENQI
# ---------------------------------------------------------------------------


BENQI_QI = "almanak.connectors.benqi.adapter.BENQI_QI_TOKENS"
BENQI_ADAPTER = "almanak.connectors.benqi.adapter.BenqiAdapter"
BENQI_CONFIG = "almanak.connectors.benqi.adapter.BenqiConfig"


class TestBenqiHelper:
    def test_non_avalanche_fails(self):
        compiler = _mock_compiler(chain="ethereum")
        intent = _borrow_intent(protocol="benqi")
        result = cl._compile_borrow_benqi(
            compiler, intent, _mock_token("WAVAX", decimals=18), _mock_token("USDC"), Decimal("1")
        )
        assert result.status == CompilationStatus.FAILED
        assert "BENQI is only available on Avalanche" in result.error

    def test_unknown_collateral_asset_fails(self):
        compiler = _mock_compiler(chain="avalanche")
        with (
            patch(BENQI_QI, {"WAVAX": object(), "USDC": object()}),
            patch(BENQI_ADAPTER) as mock_adapter_cls,
            patch(BENQI_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comptroller_address = "0xcomp"
            mock_adapter.get_market_info.return_value = None
            mock_adapter_cls.return_value = mock_adapter

            intent = _borrow_intent(protocol="benqi", collateral_token="XYZ")
            result = cl._compile_borrow_benqi(
                compiler, intent, _mock_token("XYZ", decimals=18), _mock_token("USDC"), Decimal("1")
            )
        assert result.status == CompilationStatus.FAILED
        assert "BENQI does not support collateral asset" in result.error

    def test_success_with_erc20_collateral(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(BENQI_ADAPTER) as mock_adapter_cls, patch(BENQI_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.comptroller_address = "0xcomp"
            collateral_market = MagicMock()
            collateral_market.is_native = False
            collateral_market.qi_token_address = "0xqi"
            mock_adapter.get_market_info.return_value = collateral_market
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_adapter.enter_markets.return_value = _mock_tx_result()
            mock_adapter.borrow.return_value = _mock_tx_result()
            mock_adapter_cls.return_value = mock_adapter

            intent = _borrow_intent(protocol="benqi", collateral_token="WAVAX", borrow_token="USDC")
            result = cl._compile_borrow_benqi(
                compiler, intent, _mock_token("WAVAX", decimals=18), _mock_token("USDC"), Decimal("1")
            )
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["protocol"] == "benqi"

    def test_enter_markets_failure(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(BENQI_ADAPTER) as mock_adapter_cls, patch(BENQI_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.comptroller_address = "0xcomp"
            collateral_market = MagicMock()
            collateral_market.is_native = False
            collateral_market.qi_token_address = "0xqi"
            mock_adapter.get_market_info.return_value = collateral_market
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_adapter.enter_markets.return_value = _mock_failed_result("not listed")
            mock_adapter_cls.return_value = mock_adapter

            intent = _borrow_intent(protocol="benqi", collateral_token="WAVAX")
            result = cl._compile_borrow_benqi(
                compiler, intent, _mock_token("WAVAX", decimals=18), _mock_token("USDC"), Decimal("1")
            )
        assert result.status == CompilationStatus.FAILED
        assert "enterMarkets failed" in result.error

    def test_supply_failure(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(BENQI_ADAPTER) as mock_adapter_cls, patch(BENQI_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.comptroller_address = "0xcomp"
            collateral_market = MagicMock()
            collateral_market.is_native = False
            collateral_market.qi_token_address = "0xqi"
            mock_adapter.get_market_info.return_value = collateral_market
            mock_adapter.supply.return_value = _mock_failed_result("bad supply")
            mock_adapter_cls.return_value = mock_adapter

            intent = _borrow_intent(protocol="benqi", collateral_token="WAVAX")
            result = cl._compile_borrow_benqi(
                compiler, intent, _mock_token("WAVAX", decimals=18), _mock_token("USDC"), Decimal("1")
            )
        assert result.status == CompilationStatus.FAILED
        assert "BENQI supply collateral failed" in result.error

    def test_borrow_failure(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(BENQI_ADAPTER) as mock_adapter_cls, patch(BENQI_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.comptroller_address = "0xcomp"
            collateral_market = MagicMock()
            collateral_market.is_native = False
            collateral_market.qi_token_address = "0xqi"
            mock_adapter.get_market_info.return_value = collateral_market
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_adapter.enter_markets.return_value = _mock_tx_result()
            mock_adapter.borrow.return_value = _mock_failed_result("oracle")
            mock_adapter_cls.return_value = mock_adapter

            intent = _borrow_intent(protocol="benqi", collateral_token="WAVAX")
            result = cl._compile_borrow_benqi(
                compiler, intent, _mock_token("WAVAX", decimals=18), _mock_token("USDC"), Decimal("1")
            )
        assert result.status == CompilationStatus.FAILED
        assert "BENQI borrow failed" in result.error


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
        mock_adapter.evc_address = "0xevc"
        mock_adapter.find_vault_for_asset.return_value = None
        mock_adapter.get_supported_assets.return_value = ["WETH", "USDC"]
        mock_adapter_cls.return_value = mock_adapter
        intent = _borrow_intent(protocol="euler_v2", collateral_token="XYZ")
        result = cl._compile_borrow_euler_v2(
            compiler, intent, _mock_token("XYZ", decimals=18), _mock_token("USDC"), Decimal("1")
        )
        assert result.status == CompilationStatus.FAILED
        assert "Euler V2 does not have a vault" in result.error

    @patch(EULER_ADAPTER)
    @patch(EULER_CONFIG)
    def test_success(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        mock_adapter.evc_address = "0xevc"
        vault = MagicMock()
        vault.vault_address = "0xvault"
        vault.vault_symbol = "eWETH"
        mock_adapter.find_vault_for_asset.return_value = vault
        mock_adapter.supply.return_value = _mock_tx_result()
        mock_adapter.borrow.return_value = _mock_tx_result()
        mock_adapter_cls.return_value = mock_adapter

        intent = _borrow_intent(protocol="euler_v2")
        result = cl._compile_borrow_euler_v2(
            compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
        )
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["protocol"] == "euler_v2"
        assert result.action_bundle.metadata["collateral_vault"] == "0xvault"

    @patch(EULER_ADAPTER)
    @patch(EULER_CONFIG)
    def test_supply_failure(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        mock_adapter.evc_address = "0xevc"
        vault = MagicMock()
        vault.vault_address = "0xvault"
        vault.vault_symbol = "eWETH"
        mock_adapter.find_vault_for_asset.return_value = vault
        mock_adapter.supply.return_value = _mock_failed_result("frozen")
        mock_adapter_cls.return_value = mock_adapter
        intent = _borrow_intent(protocol="euler_v2")
        result = cl._compile_borrow_euler_v2(
            compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
        )
        assert result.status == CompilationStatus.FAILED
        assert "Euler V2 supply collateral failed" in result.error

    @patch(EULER_ADAPTER)
    @patch(EULER_CONFIG)
    def test_borrow_failure(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        mock_adapter.evc_address = "0xevc"
        vault = MagicMock()
        vault.vault_address = "0xvault"
        vault.vault_symbol = "eWETH"
        mock_adapter.find_vault_for_asset.return_value = vault
        mock_adapter.supply.return_value = _mock_tx_result()
        mock_adapter.borrow.return_value = _mock_failed_result("health check")
        mock_adapter_cls.return_value = mock_adapter
        intent = _borrow_intent(protocol="euler_v2")
        result = cl._compile_borrow_euler_v2(
            compiler, intent, _mock_token("WETH", decimals=18), _mock_token("USDC"), Decimal("1")
        )
        assert result.status == CompilationStatus.FAILED
        assert "Euler V2 borrow failed" in result.error


# ---------------------------------------------------------------------------
# Silo V2
# ---------------------------------------------------------------------------


SILO_MARKETS = "almanak.connectors.silo_v2.adapter.SILO_V2_MARKETS"
SILO_ADAPTER = "almanak.connectors.silo_v2.adapter.SiloV2Adapter"
SILO_CONFIG = "almanak.connectors.silo_v2.adapter.SiloV2Config"


class TestSiloV2Helper:
    def test_non_avalanche_fails(self):
        compiler = _mock_compiler(chain="ethereum")
        intent = _borrow_intent(protocol="silo_v2")
        result = cl._compile_borrow_silo_v2(
            compiler, intent, _mock_token("WAVAX", decimals=18), _mock_token("USDC"), Decimal("1")
        )
        assert result.status == CompilationStatus.FAILED
        assert "Silo V2 is only available on Avalanche" in result.error

    def test_no_market_found(self):
        compiler = _mock_compiler(chain="avalanche")
        with (
            patch(SILO_MARKETS, {"market1": object()}),
            patch(SILO_ADAPTER) as mock_adapter_cls,
            patch(SILO_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.find_market.return_value = None
            mock_adapter_cls.return_value = mock_adapter
            intent = _borrow_intent(protocol="silo_v2")
            result = cl._compile_borrow_silo_v2(
                compiler, intent, _mock_token("WAVAX", decimals=18), _mock_token("USDC"), Decimal("1")
            )
        assert result.status == CompilationStatus.FAILED
        assert "No Silo V2 market found" in result.error

    def test_cannot_find_silo_for_asset(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(SILO_ADAPTER) as mock_adapter_cls, patch(SILO_CONFIG):
            mock_adapter = MagicMock()
            market = MagicMock()
            market.silo_config = "0xsc"
            market.market_name = "m1"
            mock_adapter.find_market.return_value = market
            mock_adapter.find_silo_for_asset.return_value = None
            mock_adapter_cls.return_value = mock_adapter
            intent = _borrow_intent(protocol="silo_v2")
            result = cl._compile_borrow_silo_v2(
                compiler, intent, _mock_token("WAVAX", decimals=18), _mock_token("USDC"), Decimal("1")
            )
        assert result.status == CompilationStatus.FAILED
        assert "Cannot find silo" in result.error

    def test_success(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(SILO_ADAPTER) as mock_adapter_cls, patch(SILO_CONFIG):
            mock_adapter = MagicMock()
            market = MagicMock()
            market.silo_config = "0xsc"
            market.market_name = "WAVAX/USDC"
            mock_adapter.find_market.return_value = market
            mock_adapter.find_silo_for_asset.return_value = (0, "0xsilo", "0xtok")
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_adapter.borrow.return_value = _mock_tx_result()
            mock_adapter_cls.return_value = mock_adapter
            intent = _borrow_intent(protocol="silo_v2")
            result = cl._compile_borrow_silo_v2(
                compiler, intent, _mock_token("WAVAX", decimals=18), _mock_token("USDC"), Decimal("1")
            )
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["protocol"] == "silo_v2"

    def test_supply_failure(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(SILO_ADAPTER) as mock_adapter_cls, patch(SILO_CONFIG):
            mock_adapter = MagicMock()
            market = MagicMock()
            market.silo_config = "0xsc"
            market.market_name = "m"
            mock_adapter.find_market.return_value = market
            mock_adapter.find_silo_for_asset.return_value = (0, "0xsilo", "0xtok")
            mock_adapter.supply.return_value = _mock_failed_result("bad deposit")
            mock_adapter_cls.return_value = mock_adapter
            intent = _borrow_intent(protocol="silo_v2")
            result = cl._compile_borrow_silo_v2(
                compiler, intent, _mock_token("WAVAX", decimals=18), _mock_token("USDC"), Decimal("1")
            )
        assert result.status == CompilationStatus.FAILED
        assert "Silo V2 deposit failed" in result.error

    def test_borrow_failure(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(SILO_ADAPTER) as mock_adapter_cls, patch(SILO_CONFIG):
            mock_adapter = MagicMock()
            market = MagicMock()
            market.silo_config = "0xsc"
            market.market_name = "m"
            mock_adapter.find_market.return_value = market
            mock_adapter.find_silo_for_asset.return_value = (0, "0xsilo", "0xtok")
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_adapter.borrow.return_value = _mock_failed_result("hf low")
            mock_adapter_cls.return_value = mock_adapter
            intent = _borrow_intent(protocol="silo_v2")
            result = cl._compile_borrow_silo_v2(
                compiler, intent, _mock_token("WAVAX", decimals=18), _mock_token("USDC"), Decimal("1")
            )
        assert result.status == CompilationStatus.FAILED
        assert "Silo V2 borrow failed" in result.error


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
