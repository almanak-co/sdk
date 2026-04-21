"""Unit tests targeting the per-protocol helpers extracted from ``compile_supply``.

Phase 2c of the coverage-improvement plan. The helpers are private module-level
functions in ``almanak.framework.intents.compiler_lending``:

- ``compile_supply`` (thin dispatcher)
- ``_compile_supply_jupiter_lend`` (Solana)
- ``_compile_supply_kamino`` (Solana / Solana fallback)
- ``_compile_supply_morpho_blue``
- ``_compile_supply_curvance``
- ``_compile_supply_aave_compatible``
- ``_compile_supply_spark``
- ``_compile_supply_compound_v3``
- ``_compile_supply_benqi``
- ``_compile_supply_joelend``
- ``_compile_supply_euler_v2``
- ``_compile_supply_silo_v2``

Each test builds a minimal mocked ``compiler`` (no IntentCompiler instantiation)
plus mocked tokens and adapters, and verifies the helper either succeeds or
fails with a specific ``CompilationStatus.FAILED`` error message.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.framework.intents import SupplyIntent
from almanak.framework.intents import compiler_lending as cl
from almanak.framework.intents.compiler_models import CompilationStatus

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
      - _compile_jupiter_lend_supply() / _compile_kamino_supply()
    """
    compiler = MagicMock()
    compiler.chain = chain
    compiler.wallet_address = TEST_WALLET
    compiler._gateway_client = None
    compiler._is_solana_chain.return_value = is_solana

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
    compiler._format_amount.side_effect = lambda amount, decimals: str(amount)
    return compiler


def _supply_intent(
    *,
    protocol: str = "aave_v3",
    token: str = "USDC",
    amount=Decimal("100"),
    use_as_collateral: bool = True,
    market_id: str | None = None,
) -> SupplyIntent:
    return SupplyIntent(
        protocol=protocol,
        token=token,
        amount=amount,
        use_as_collateral=use_as_collateral,
        market_id=market_id,
    )


# ---------------------------------------------------------------------------
# Dispatcher - routing and unsupported-protocol handling
# ---------------------------------------------------------------------------


class TestDispatcher:
    def test_unsupported_protocol_returns_failed(self):
        compiler = _mock_compiler(chain="ethereum")
        compiler._resolve_token.side_effect = lambda t, chain=None: _mock_token(symbol=t)

        intent = _supply_intent(protocol="nonexistent_proto")
        result = cl.compile_supply(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "Unsupported lending protocol: nonexistent_proto" in result.error
        for expected in ("aave_v3", "morpho", "spark", "compound_v3", "benqi", "joelend", "euler_v2", "silo_v2"):
            assert expected in result.error

    def test_unknown_supply_token(self):
        compiler = _mock_compiler(chain="ethereum")
        compiler._resolve_token.return_value = None

        intent = _supply_intent(protocol="aave_v3", token="FAKE")
        result = cl.compile_supply(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "Unknown token: FAKE" in result.error

    def test_amount_all_unresolved_fails(self):
        """``amount="all"`` must be pre-resolved before hitting the dispatcher."""
        compiler = _mock_compiler(chain="ethereum")
        compiler._resolve_token.side_effect = lambda t, chain=None: _mock_token(symbol=t)

        intent = SupplyIntent(
            protocol="aave_v3",
            token="USDC",
            amount="all",
        )
        result = cl.compile_supply(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "amount='all' for supply must be resolved" in result.error

    def test_jupiter_lend_on_non_solana_fails(self):
        compiler = _mock_compiler(chain="ethereum", is_solana=False)
        intent = _supply_intent(protocol="jupiter_lend")
        result = cl.compile_supply(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "only available on Solana chains" in result.error

    def test_jupiter_lend_on_solana_delegates(self):
        compiler = _mock_compiler(chain="solana", is_solana=True)
        expected = MagicMock(status=CompilationStatus.SUCCESS)
        compiler._compile_jupiter_lend_supply.return_value = expected
        intent = _supply_intent(protocol="jupiter_lend")
        result = cl.compile_supply(compiler, intent)
        assert result is expected
        compiler._compile_jupiter_lend_supply.assert_called_once_with(intent)

    def test_kamino_dispatches_to_helper_on_solana(self):
        compiler = _mock_compiler(chain="solana", is_solana=True)
        expected = MagicMock(status=CompilationStatus.SUCCESS)
        compiler._compile_kamino_supply.return_value = expected
        intent = _supply_intent(protocol="kamino")
        result = cl.compile_supply(compiler, intent)
        assert result is expected
        compiler._compile_kamino_supply.assert_called_once_with(intent)

    def test_kamino_dispatches_on_non_solana_too(self):
        """When ``protocol == 'kamino'`` on a non-Solana chain, the dispatcher
        still routes into ``_compile_kamino_supply`` (matches pre-refactor
        behaviour — this is the path that issue #1622 tracks as a missing
        fail-fast).

        We only verify the *routing* here so that the eventual fail-fast fix
        (#1622) does not register as a regression against this test. The
        returned result is not asserted to be SUCCESS — whatever
        ``_compile_kamino_supply`` decides to return is the helper's concern.
        """
        compiler = _mock_compiler(chain="ethereum", is_solana=False)
        compiler._compile_kamino_supply.return_value = MagicMock(status=CompilationStatus.SUCCESS)
        intent = _supply_intent(protocol="kamino")
        cl.compile_supply(compiler, intent)
        compiler._compile_kamino_supply.assert_called_once_with(intent)

    def test_non_solana_evm_protocol_on_solana_chain_rejected(self):
        """On a Solana chain, any non-morpho/morpho_blue/jupiter_lend protocol is rejected."""
        compiler = _mock_compiler(chain="solana", is_solana=True)
        intent = _supply_intent(protocol="aave_v3")
        result = cl.compile_supply(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "not supported for SUPPLY on Solana" in result.error

    def test_outer_exception_returns_failed(self):
        """An unhandled exception inside the dispatcher is caught and returned as FAILED."""
        compiler = _mock_compiler(chain="ethereum")
        compiler._resolve_token.side_effect = RuntimeError("boom")
        intent = _supply_intent(protocol="aave_v3")
        result = cl.compile_supply(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "boom" in result.error


# ---------------------------------------------------------------------------
# Solana helpers
# ---------------------------------------------------------------------------


class TestJupiterLendHelper:
    def test_non_solana_fails(self):
        compiler = _mock_compiler(chain="ethereum", is_solana=False)
        intent = _supply_intent(protocol="jupiter_lend")
        result = cl._compile_supply_jupiter_lend(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "only available on Solana chains" in result.error

    def test_solana_delegates_to_compiler(self):
        compiler = _mock_compiler(chain="solana", is_solana=True)
        expected = MagicMock(status=CompilationStatus.SUCCESS)
        compiler._compile_jupiter_lend_supply.return_value = expected
        intent = _supply_intent(protocol="jupiter_lend")
        result = cl._compile_supply_jupiter_lend(compiler, intent)
        assert result is expected
        compiler._compile_jupiter_lend_supply.assert_called_once_with(intent)


class TestKaminoHelper:
    def test_non_solana_delegates_to_compiler(self):
        """The dispatcher routes here when protocol_lower == 'kamino' even on
        non-Solana chains. Original (pre-refactor) behaviour is to hand off to
        ``_compile_kamino_supply`` without a fail-fast — this is tracked as
        issue #1622.

        This test only asserts the *delegation call*, not the final result,
        so the eventual fail-fast fix will not register as a regression here.
        """
        compiler = _mock_compiler(chain="ethereum", is_solana=False)
        compiler._compile_kamino_supply.return_value = MagicMock(status=CompilationStatus.SUCCESS)
        intent = _supply_intent(protocol="kamino")
        cl._compile_supply_kamino(compiler, intent)
        compiler._compile_kamino_supply.assert_called_once_with(intent)

    def test_solana_unsupported_protocol_rejected(self):
        compiler = _mock_compiler(chain="solana", is_solana=True)
        intent = _supply_intent(protocol="aave_v3")
        result = cl._compile_supply_kamino(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "not supported for SUPPLY on Solana" in result.error

    def test_solana_kamino_delegates(self):
        compiler = _mock_compiler(chain="solana", is_solana=True)
        expected = MagicMock(status=CompilationStatus.SUCCESS)
        compiler._compile_kamino_supply.return_value = expected
        intent = _supply_intent(protocol="kamino")
        result = cl._compile_supply_kamino(compiler, intent)
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
        intent = SupplyIntent.model_construct(
            protocol="morpho_blue",
            token="USDC",
            amount=Decimal("100"),
            use_as_collateral=False,
            market_id=None,
            intent_id="test",
        )
        result = cl._compile_supply_morpho_blue(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "market_id is required for Morpho Blue" in result.error

    @patch(MORPHO_ADAPTER)
    @patch(MORPHO_CONFIG)
    def test_success_loan_token_path(self, mock_config, mock_adapter_cls):
        """use_as_collateral=False routes to ``supply()`` (loan-token deposit)."""
        compiler = _mock_compiler()
        mock_adapter = MagicMock()
        mock_adapter.morpho_address = "0xmorpho"
        mock_adapter.supply.return_value = _mock_tx_result(desc="supply")
        mock_adapter_cls.return_value = mock_adapter

        intent = _supply_intent(
            protocol="morpho_blue",
            market_id="0xmarket",
            use_as_collateral=False,
        )
        result = cl._compile_supply_morpho_blue(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["protocol"] == "morpho_blue"
        assert result.action_bundle.metadata["market_id"] == "0xmarket"
        mock_adapter.supply.assert_called_once()
        mock_adapter.supply_collateral.assert_not_called()

    @patch(MORPHO_ADAPTER)
    @patch(MORPHO_CONFIG)
    def test_success_collateral_path(self, mock_config, mock_adapter_cls):
        """use_as_collateral=True routes to ``supply_collateral()``."""
        compiler = _mock_compiler()
        mock_adapter = MagicMock()
        mock_adapter.morpho_address = "0xmorpho"
        mock_adapter.supply_collateral.return_value = _mock_tx_result(desc="supply_collateral")
        mock_adapter_cls.return_value = mock_adapter

        intent = _supply_intent(
            protocol="morpho_blue",
            market_id="0xmarket",
            use_as_collateral=True,
        )
        result = cl._compile_supply_morpho_blue(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.SUCCESS
        mock_adapter.supply_collateral.assert_called_once()
        mock_adapter.supply.assert_not_called()

    @patch(MORPHO_ADAPTER)
    @patch(MORPHO_CONFIG)
    def test_supply_failure(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler()
        mock_adapter = MagicMock()
        mock_adapter.morpho_address = "0xmorpho"
        mock_adapter.supply.return_value = _mock_failed_result("bad market")
        mock_adapter_cls.return_value = mock_adapter

        intent = _supply_intent(protocol="morpho_blue", market_id="0xmarket", use_as_collateral=False)
        result = cl._compile_supply_morpho_blue(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "Morpho Blue supply failed" in result.error


# ---------------------------------------------------------------------------
# Curvance
# ---------------------------------------------------------------------------


CURVANCE_ADAPTER = "almanak.framework.connectors.curvance.adapter.CurvanceAdapter"
CURVANCE_CONFIG = "almanak.framework.connectors.curvance.adapter.CurvanceConfig"


class TestCurvanceHelper:
    def test_missing_market_id_fails(self):
        compiler = _mock_compiler(chain="monad")
        intent = SupplyIntent.model_construct(
            protocol="curvance",
            token="USDC",
            amount=Decimal("100"),
            use_as_collateral=True,
            market_id=None,
            intent_id="test",
        )
        result = cl._compile_supply_curvance(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "market_id is required for Curvance" in result.error

    @patch(CURVANCE_ADAPTER)
    @patch(CURVANCE_CONFIG)
    def test_collateral_symbol_mismatch_fails(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="monad")
        mock_adapter = MagicMock()
        market = MagicMock()
        market.collateral_symbol = "WETH"  # mismatch vs USDC
        market.debt_symbol = "USDT"
        market.name = "WETH/USDT"
        mock_adapter.get_market.return_value = market
        mock_adapter_cls.return_value = mock_adapter

        intent = _supply_intent(protocol="curvance", token="USDC", market_id="0xmarket")
        result = cl._compile_supply_curvance(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "USDC" in result.error

    @patch(CURVANCE_ADAPTER)
    @patch(CURVANCE_CONFIG)
    def test_use_as_collateral_false_rejected(self, mock_config, mock_adapter_cls):
        """Defensive branch: helper rejects use_as_collateral=False even though
        Pydantic normally blocks this at construction. Use ``model_construct``
        to bypass validation and hit the helper's internal guard directly.
        """
        compiler = _mock_compiler(chain="monad")
        mock_adapter = MagicMock()
        market = MagicMock()
        market.collateral_symbol = "USDC"
        market.debt_symbol = "WETH"
        market.name = "USDC/WETH"
        mock_adapter.get_market.return_value = market
        mock_adapter_cls.return_value = mock_adapter

        intent = SupplyIntent.model_construct(
            protocol="curvance",
            token="USDC",
            amount=Decimal("100"),
            use_as_collateral=False,
            market_id="0xmarket",
            intent_id="test",
        )
        result = cl._compile_supply_curvance(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "not implemented" in result.error

    @patch(CURVANCE_ADAPTER)
    @patch(CURVANCE_CONFIG)
    def test_success(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="monad")
        mock_adapter = MagicMock()
        market = MagicMock()
        market.collateral_symbol = "USDC"
        market.debt_symbol = "WETH"
        market.name = "USDC/WETH"
        market.collateral_ctoken = "0xcbccol"
        mock_adapter.get_market.return_value = market
        mock_adapter.get_supply_spender.return_value = "0xspender"
        mock_adapter.supply_collateral.return_value = _mock_tx_result()
        mock_adapter_cls.return_value = mock_adapter

        intent = _supply_intent(protocol="curvance", market_id="0xmarket")
        result = cl._compile_supply_curvance(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["protocol"] == "curvance"
        assert result.action_bundle.metadata["market_id"] == "0xmarket"
        args, _ = compiler._build_approve_tx.call_args
        assert args[2] == int(Decimal("100") * Decimal(10**6))

    @patch(CURVANCE_ADAPTER)
    @patch(CURVANCE_CONFIG)
    def test_supply_failure(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="monad")
        mock_adapter = MagicMock()
        market = MagicMock()
        market.collateral_symbol = "USDC"
        market.debt_symbol = "WETH"
        market.name = "USDC/WETH"
        mock_adapter.get_market.return_value = market
        mock_adapter.get_supply_spender.return_value = "0xspender"
        mock_adapter.supply_collateral.return_value = _mock_failed_result("paused")
        mock_adapter_cls.return_value = mock_adapter

        intent = _supply_intent(protocol="curvance", market_id="0xmarket")
        result = cl._compile_supply_curvance(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "Curvance supply failed" in result.error


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

        intent = _supply_intent(protocol="aave_v3")
        result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "not available on chain" in result.error

    @patch(AAVE_ADAPTER_CLS)
    def test_success_erc20(self, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        mock_adapter.get_pool_address.return_value = TEST_POOL
        mock_adapter.get_supply_calldata.return_value = b"\x01\x02"
        mock_adapter.estimate_supply_gas.return_value = 150_000
        mock_adapter.get_set_collateral_calldata.return_value = b"\x03"
        mock_adapter.estimate_set_collateral_gas.return_value = 70_000
        mock_adapter_cls.return_value = mock_adapter

        intent = _supply_intent(protocol="aave_v3", use_as_collateral=True)
        result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["pool_address"] == TEST_POOL
        # With collateral, we expect approve + supply + setUseReserveAsCollateral
        tx_types = [tx.tx_type for tx in result.transactions]
        assert "lending_set_collateral" in tx_types

    @patch(AAVE_ADAPTER_CLS)
    def test_success_without_collateral(self, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        mock_adapter.get_pool_address.return_value = TEST_POOL
        mock_adapter.get_supply_calldata.return_value = b"\x01"
        mock_adapter.estimate_supply_gas.return_value = 150_000
        mock_adapter_cls.return_value = mock_adapter

        intent = _supply_intent(protocol="aave_v3", use_as_collateral=False)
        result = cl._compile_supply_aave_compatible(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.SUCCESS
        tx_types = [tx.tx_type for tx in result.transactions]
        assert "lending_set_collateral" not in tx_types

    @patch(AAVE_ADAPTER_CLS)
    def test_native_supply_wraps_to_weth(self, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        mock_adapter.get_pool_address.return_value = TEST_POOL
        mock_adapter.get_supply_calldata.return_value = b"\x01"
        mock_adapter.estimate_supply_gas.return_value = 150_000
        mock_adapter.get_set_collateral_calldata.return_value = b"\x02"
        mock_adapter.estimate_set_collateral_gas.return_value = 70_000
        mock_adapter_cls.return_value = mock_adapter

        intent = _supply_intent(protocol="aave_v3", token="ETH")
        result = cl._compile_supply_aave_compatible(
            compiler, intent, _mock_token("ETH", decimals=18, is_native=True), Decimal("1")
        )
        assert result.status == CompilationStatus.SUCCESS
        assert any("wrapped before supplying" in w for w in result.warnings)
        # Regression: helper must emit a wrap tx, an approve tx for the wrapped
        # token, and a supply tx with value=0. Without the wrap tx the on-chain
        # approve would target a token the wallet has zero balance of.
        assert result.transactions[0].tx_type == "wrap"
        # Decimal("1") with 18 decimals -> exactly 10**18 wei wrapped
        assert result.transactions[0].value == 10**18
        assert result.transactions[0].data == "0xd0e30db0"
        tx_types = [tx.tx_type for tx in result.transactions]
        assert "approve" in tx_types
        # Wrap must precede approve must precede supply so the pool can
        # actually pull the wrapped token from the wallet.
        wrap_idx = tx_types.index("wrap")
        approve_idx = tx_types.index("approve")
        supply_idx = tx_types.index("lending_supply")
        assert wrap_idx < approve_idx < supply_idx
        supply_txs = [tx for tx in result.transactions if tx.tx_type == "lending_supply"]
        assert len(supply_txs) == 1
        assert supply_txs[0].value == 0
        compiler._build_approve_tx.assert_called_once()
        approve_args = compiler._build_approve_tx.call_args.args
        assert approve_args[0] == "0xweth00000000000000000000000000000000eeee"

    @patch(AAVE_ADAPTER_CLS)
    def test_native_supply_without_weth_address_fails(self, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        compiler._get_wrapped_native_address.return_value = None
        mock_adapter = MagicMock()
        mock_adapter.get_pool_address.return_value = TEST_POOL
        mock_adapter_cls.return_value = mock_adapter

        intent = _supply_intent(protocol="aave_v3", token="ETH")
        result = cl._compile_supply_aave_compatible(
            compiler, intent, _mock_token("ETH", decimals=18, is_native=True), Decimal("1")
        )
        assert result.status == CompilationStatus.FAILED
        assert "wrapped native token address not found" in result.error


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
            intent = _supply_intent(protocol="spark")
            result = cl._compile_supply_spark(compiler, intent, _mock_token("USDC"), Decimal("100"))
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
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="spark")
            result = cl._compile_supply_spark(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["pool_address"] == TEST_POOL

    def test_native_supply_wraps_and_approves(self):
        compiler = _mock_compiler(chain="ethereum")
        with (
            patch(SPARK_POOL_ADDRESSES, {"ethereum": "0xspark"}),
            patch(SPARK_ADAPTER) as mock_cls,
            patch(SPARK_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.pool_address = TEST_POOL
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="spark", token="ETH")
            result = cl._compile_supply_spark(
                compiler, intent, _mock_token("ETH", decimals=18, is_native=True), Decimal("1")
            )
        assert result.status == CompilationStatus.SUCCESS
        # First tx must be the wrap
        assert result.transactions[0].tx_type == "wrap"
        assert any("wrapped before supplying" in w for w in result.warnings)

    def test_native_supply_without_weth_address_fails(self):
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
            intent = _supply_intent(protocol="spark", token="ETH")
            result = cl._compile_supply_spark(
                compiler, intent, _mock_token("ETH", decimals=18, is_native=True), Decimal("1")
            )
        assert result.status == CompilationStatus.FAILED
        assert "wrapped native token address not found" in result.error

    def test_supply_failure(self):
        compiler = _mock_compiler(chain="ethereum")
        with (
            patch(SPARK_POOL_ADDRESSES, {"ethereum": "0xspark"}),
            patch(SPARK_ADAPTER) as mock_cls,
            patch(SPARK_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.pool_address = TEST_POOL
            mock_adapter.supply.return_value = _mock_failed_result("paused")
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="spark")
            result = cl._compile_supply_spark(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "Spark supply failed" in result.error


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
            intent = _supply_intent(protocol="compound_v3")
            result = cl._compile_supply_compound_v3(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "not available on chain" in result.error

    def test_unknown_market_fails(self):
        compiler = _mock_compiler(chain="ethereum")
        with patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}):
            intent = _supply_intent(protocol="compound_v3", market_id="unknown_market")
            result = cl._compile_supply_compound_v3(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "not available on" in result.error

    def test_base_token_success(self):
        """Base-token supply calls supply()."""
        compiler = _mock_compiler(chain="ethereum")
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = "0xcomet"
            # Base token address must match supply token's address for base-supply
            base_addr = "0x" + "ab" * 20
            mock_adapter.market_config = {"base_token_address": base_addr}
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="compound_v3")
            result = cl._compile_supply_compound_v3(
                compiler, intent, _mock_token("USDC", address=base_addr), Decimal("100")
            )
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["supply_type"] == "base"
        mock_adapter.supply.assert_called_once()

    def test_collateral_token_success(self):
        """Non-base token supply calls supply_collateral()."""
        compiler = _mock_compiler(chain="ethereum")
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = "0xcomet"
            mock_adapter.market_config = {"base_token_address": "0x" + "11" * 20}
            mock_adapter.supply_collateral.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="compound_v3", use_as_collateral=True)
            result = cl._compile_supply_compound_v3(compiler, intent, _mock_token("WETH"), Decimal("1"))
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["supply_type"] == "collateral"
        mock_adapter.supply_collateral.assert_called_once()

    def test_non_base_token_collateral_false_fails(self):
        """Non-base tokens can only be supplied as collateral in Compound V3."""
        compiler = _mock_compiler(chain="ethereum")
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = "0xcomet"
            mock_adapter.market_config = {"base_token_address": "0x" + "11" * 20}
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="compound_v3", use_as_collateral=False)
            result = cl._compile_supply_compound_v3(compiler, intent, _mock_token("WETH"), Decimal("1"))
        assert result.status == CompilationStatus.FAILED
        assert "can only be supplied as collateral" in result.error

    def test_missing_base_token_address_fails_closed(self):
        """Incomplete market_config must fail closed with an explicit error."""
        compiler = _mock_compiler(chain="ethereum")
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = "0xcomet"
            mock_adapter.market_config = {}  # missing base_token_address
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="compound_v3")
            result = cl._compile_supply_compound_v3(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "missing base_token_address" in result.error

    def test_supply_failure(self):
        compiler = _mock_compiler(chain="ethereum")
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = "0xcomet"
            base_addr = "0x" + "ab" * 20
            mock_adapter.market_config = {"base_token_address": base_addr}
            mock_adapter.supply.return_value = _mock_failed_result("oracle stale")
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="compound_v3")
            result = cl._compile_supply_compound_v3(
                compiler, intent, _mock_token("USDC", address=base_addr), Decimal("100")
            )
        assert result.status == CompilationStatus.FAILED
        assert "Compound V3 supply failed" in result.error


# ---------------------------------------------------------------------------
# BENQI
# ---------------------------------------------------------------------------


BENQI_ADAPTER = "almanak.framework.connectors.benqi.adapter.BenqiAdapter"
BENQI_CONFIG = "almanak.framework.connectors.benqi.adapter.BenqiConfig"


class TestBenqiHelper:
    def test_non_avalanche_fails(self):
        compiler = _mock_compiler(chain="ethereum")
        intent = _supply_intent(protocol="benqi")
        result = cl._compile_supply_benqi(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "only available on Avalanche" in result.error

    def test_unsupported_asset_fails(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(BENQI_ADAPTER) as mock_cls, patch(BENQI_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.get_market_info.return_value = None
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="benqi", token="XYZ")
            result = cl._compile_supply_benqi(compiler, intent, _mock_token("XYZ"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "does not support asset" in result.error

    def test_success_erc20_without_collateral(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(BENQI_ADAPTER) as mock_cls, patch(BENQI_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.comptroller_address = "0xcomp"
            market = MagicMock()
            market.qi_token_address = "0xqi"
            market.is_native = False
            mock_adapter.get_market_info.return_value = market
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="benqi", use_as_collateral=False)
            result = cl._compile_supply_benqi(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.SUCCESS
        # enter_markets must NOT be called
        mock_adapter.enter_markets.assert_not_called()

    def test_success_erc20_with_collateral_calls_enter_markets(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(BENQI_ADAPTER) as mock_cls, patch(BENQI_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.comptroller_address = "0xcomp"
            market = MagicMock()
            market.qi_token_address = "0xqi"
            market.is_native = False
            mock_adapter.get_market_info.return_value = market
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_adapter.enter_markets.return_value = _mock_tx_result(gas=80_000, desc="enterMarkets")
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="benqi", use_as_collateral=True)
            result = cl._compile_supply_benqi(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.SUCCESS
        mock_adapter.enter_markets.assert_called_once_with(["USDC"])
        tx_types = [tx.tx_type for tx in result.transactions]
        assert "lending_enter_markets" in tx_types

    def test_native_asset_skips_approve(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(BENQI_ADAPTER) as mock_cls, patch(BENQI_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.comptroller_address = "0xcomp"
            market = MagicMock()
            market.qi_token_address = "0xqi"
            market.is_native = True
            mock_adapter.get_market_info.return_value = market
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="benqi", token="AVAX", use_as_collateral=False)
            result = cl._compile_supply_benqi(
                compiler, intent, _mock_token("AVAX", decimals=18, is_native=True), Decimal("1")
            )
        assert result.status == CompilationStatus.SUCCESS
        compiler._build_approve_tx.assert_not_called()

    def test_enter_markets_failure(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(BENQI_ADAPTER) as mock_cls, patch(BENQI_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.comptroller_address = "0xcomp"
            market = MagicMock()
            market.qi_token_address = "0xqi"
            market.is_native = False
            mock_adapter.get_market_info.return_value = market
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_adapter.enter_markets.return_value = _mock_failed_result("not listed")
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="benqi", use_as_collateral=True)
            result = cl._compile_supply_benqi(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "BENQI enterMarkets failed" in result.error

    def test_supply_failure(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(BENQI_ADAPTER) as mock_cls, patch(BENQI_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.comptroller_address = "0xcomp"
            market = MagicMock()
            market.qi_token_address = "0xqi"
            market.is_native = False
            mock_adapter.get_market_info.return_value = market
            mock_adapter.supply.return_value = _mock_failed_result("frozen")
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="benqi")
            result = cl._compile_supply_benqi(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "BENQI supply failed" in result.error


# ---------------------------------------------------------------------------
# Joe Lend
# ---------------------------------------------------------------------------


JOELEND_ADAPTER = "almanak.framework.connectors.joelend.adapter.JoeLendAdapter"
JOELEND_CONFIG = "almanak.framework.connectors.joelend.adapter.JoeLendConfig"


class TestJoeLendHelper:
    def test_non_avalanche_fails(self):
        compiler = _mock_compiler(chain="ethereum")
        intent = _supply_intent(protocol="joelend")
        result = cl._compile_supply_joelend(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "only available on Avalanche" in result.error

    def test_unsupported_asset_fails(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(JOELEND_ADAPTER) as mock_cls, patch(JOELEND_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.get_market_info.return_value = None
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="joelend", token="XYZ")
            result = cl._compile_supply_joelend(compiler, intent, _mock_token("XYZ"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "does not support asset" in result.error

    def test_success_erc20(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(JOELEND_ADAPTER) as mock_cls, patch(JOELEND_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.joetroller_address = "0xjoe"
            market = MagicMock()
            market.j_token_address = "0xj"
            market.underlying_address = None
            mock_adapter.get_market_info.return_value = market
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="joelend", use_as_collateral=False)
            result = cl._compile_supply_joelend(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["protocol"] == "joelend"

    def test_native_avax_wraps_to_wavax(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(JOELEND_ADAPTER) as mock_cls, patch(JOELEND_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.joetroller_address = "0xjoe"
            market = MagicMock()
            market.j_token_address = "0xj"
            market.underlying_address = "0xwavax"
            mock_adapter.get_market_info.return_value = market
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="joelend", token="AVAX", use_as_collateral=False)
            result = cl._compile_supply_joelend(
                compiler, intent, _mock_token("AVAX", decimals=18, is_native=True), Decimal("1")
            )
        assert result.status == CompilationStatus.SUCCESS
        # First tx must be the wrap
        assert result.transactions[0].tx_type == "wrap_native"

    def test_enter_markets_failure(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(JOELEND_ADAPTER) as mock_cls, patch(JOELEND_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.joetroller_address = "0xjoe"
            market = MagicMock()
            market.j_token_address = "0xj"
            market.underlying_address = None
            mock_adapter.get_market_info.return_value = market
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_adapter.enter_markets.return_value = _mock_failed_result("not listed")
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="joelend", use_as_collateral=True)
            result = cl._compile_supply_joelend(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "Joe Lend enterMarkets failed" in result.error

    def test_supply_failure(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(JOELEND_ADAPTER) as mock_cls, patch(JOELEND_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.joetroller_address = "0xjoe"
            market = MagicMock()
            market.j_token_address = "0xj"
            market.underlying_address = None
            mock_adapter.get_market_info.return_value = market
            mock_adapter.supply.return_value = _mock_failed_result("paused")
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="joelend")
            result = cl._compile_supply_joelend(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "Joe Lend supply failed" in result.error


# ---------------------------------------------------------------------------
# Euler V2
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
        intent = _supply_intent(protocol="euler_v2", token="XYZ")
        result = cl._compile_supply_euler_v2(compiler, intent, _mock_token("XYZ"), Decimal("100"))
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
        mock_adapter.supply.return_value = _mock_tx_result()
        mock_adapter_cls.return_value = mock_adapter
        intent = _supply_intent(protocol="euler_v2")
        result = cl._compile_supply_euler_v2(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["vault_address"] == "0xvault"

    @patch(EULER_ADAPTER)
    @patch(EULER_CONFIG)
    def test_supply_failure(self, mock_config, mock_adapter_cls):
        compiler = _mock_compiler(chain="ethereum")
        mock_adapter = MagicMock()
        vault = MagicMock()
        vault.vault_address = "0xvault"
        vault.vault_symbol = "eUSDC"
        mock_adapter.find_vault_for_asset.return_value = vault
        mock_adapter.supply.return_value = _mock_failed_result("paused")
        mock_adapter_cls.return_value = mock_adapter
        intent = _supply_intent(protocol="euler_v2")
        result = cl._compile_supply_euler_v2(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "Euler V2 supply failed" in result.error


# ---------------------------------------------------------------------------
# Silo V2
# ---------------------------------------------------------------------------


SILO_ADAPTER = "almanak.framework.connectors.silo_v2.adapter.SiloV2Adapter"
SILO_CONFIG = "almanak.framework.connectors.silo_v2.adapter.SiloV2Config"


class TestSiloV2Helper:
    def test_non_avalanche_fails(self):
        compiler = _mock_compiler(chain="ethereum")
        intent = _supply_intent(protocol="silo_v2")
        result = cl._compile_supply_silo_v2(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "only available on Avalanche" in result.error

    def test_no_silo_for_asset(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(SILO_ADAPTER) as mock_cls, patch(SILO_CONFIG):
            mock_adapter = MagicMock()
            mock_adapter.find_silo_for_asset.return_value = None
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="silo_v2")
            result = cl._compile_supply_silo_v2(compiler, intent, _mock_token("USDC"), Decimal("100"))
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
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="silo_v2")
            result = cl._compile_supply_silo_v2(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["protocol"] == "silo_v2"
        assert result.action_bundle.metadata["market_name"] == "USDC-WAVAX"

    def test_supply_failure(self):
        compiler = _mock_compiler(chain="avalanche")
        with patch(SILO_ADAPTER) as mock_cls, patch(SILO_CONFIG):
            mock_adapter = MagicMock()
            market = MagicMock()
            market.silo_config = "0xsc"
            market.market_name = "m"
            mock_adapter.find_silo_for_asset.return_value = (market, "0xsilo", "0xtok")
            mock_adapter.supply.return_value = _mock_failed_result("paused")
            mock_cls.return_value = mock_adapter
            intent = _supply_intent(protocol="silo_v2")
            result = cl._compile_supply_silo_v2(compiler, intent, _mock_token("USDC"), Decimal("100"))
        assert result.status == CompilationStatus.FAILED
        assert "Silo V2 supply failed" in result.error


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
            mock_adapter.supply_collateral.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter

            intent = _supply_intent(protocol="morpho_blue", market_id="0xmarket")
            result = cl.compile_supply(compiler, intent)
        assert result.status == CompilationStatus.SUCCESS

    def test_aave_routes_through_dispatcher(self):
        compiler = _mock_compiler(chain="ethereum")
        compiler._resolve_token.side_effect = lambda t, chain=None: _mock_token(symbol=t)
        with patch(AAVE_ADAPTER_CLS) as mock_cls:
            mock_adapter = MagicMock()
            mock_adapter.get_pool_address.return_value = TEST_POOL
            mock_adapter.get_supply_calldata.return_value = b"\x01"
            mock_adapter.estimate_supply_gas.return_value = 150_000
            mock_adapter.get_set_collateral_calldata.return_value = b"\x02"
            mock_adapter.estimate_set_collateral_gas.return_value = 70_000
            mock_cls.return_value = mock_adapter

            intent = _supply_intent(protocol="aave_v3")
            result = cl.compile_supply(compiler, intent)
        assert result.status == CompilationStatus.SUCCESS

    def test_compound_v3_routes_through_dispatcher(self):
        compiler = _mock_compiler(chain="ethereum")
        compiler._resolve_token.side_effect = lambda t, chain=None: _mock_token(symbol=t)
        with (
            patch(COMPOUND_MARKETS, {"ethereum": {"usdc": "0xc"}}),
            patch(COMPOUND_ADAPTER) as mock_cls,
            patch(COMPOUND_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.comet_address = "0xcomet"
            base_addr = "0x" + "ab" * 20
            mock_adapter.market_config = {"base_token_address": base_addr}
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter

            intent = _supply_intent(protocol="compound_v3")
            result = cl.compile_supply(compiler, intent)
        assert result.status == CompilationStatus.SUCCESS

    def test_spark_routes_through_dispatcher(self):
        compiler = _mock_compiler(chain="ethereum")
        compiler._resolve_token.side_effect = lambda t, chain=None: _mock_token(symbol=t)
        with (
            patch(SPARK_POOL_ADDRESSES, {"ethereum": "0xspark"}),
            patch(SPARK_ADAPTER) as mock_cls,
            patch(SPARK_CONFIG),
        ):
            mock_adapter = MagicMock()
            mock_adapter.pool_address = TEST_POOL
            mock_adapter.supply.return_value = _mock_tx_result()
            mock_cls.return_value = mock_adapter

            intent = _supply_intent(protocol="spark")
            result = cl.compile_supply(compiler, intent)
        assert result.status == CompilationStatus.SUCCESS


# ---------------------------------------------------------------------------
# Entry-point sanity check: module exposes what callers expect
# ---------------------------------------------------------------------------


def test_module_exposes_all_helpers():
    """Regression guard: all helper symbols must remain exported at module level."""
    for name in (
        "compile_supply",
        "_compile_supply_jupiter_lend",
        "_compile_supply_kamino",
        "_compile_supply_morpho_blue",
        "_compile_supply_curvance",
        "_compile_supply_aave_compatible",
        "_compile_supply_spark",
        "_compile_supply_compound_v3",
        "_compile_supply_benqi",
        "_compile_supply_joelend",
        "_compile_supply_euler_v2",
        "_compile_supply_silo_v2",
    ):
        assert hasattr(cl, name), f"Missing module-level helper: {name}"


if __name__ == "__main__":  # pragma: no cover
    import sys

    import pytest

    sys.exit(pytest.main([__file__, "-v"]))
