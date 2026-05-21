"""Unit tests for Pendle compile_pendle_lp_open / lp_close / redeem helpers.

VIB-4083 W6 Sub-C: covers the per-route helpers extracted from
``connectors.pendle.compiler.compile_pendle_lp_open``, ``compile_pendle_lp_close``, and
``compile_pendle_redeem`` plus a few error/happy-path direct entry points
through each top-level function. Mocks at the PendleAdapter / PendleAdapter
build_* boundary so no Anvil or live RPC is touched.

Per the W6 Sub-A audit, the three target functions had ~7-8% body coverage in
unit-scope (anvil integration tests exist under tests/intents/arbitrum/ but
make test-ci skips that path). Every test here is pure coverage lift.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

from almanak.framework.connectors.pendle import compiler as cp
from almanak.framework.intents.compiler_models import CompilationStatus, TokenInfo, TransactionData
from almanak.framework.intents.lending_intents import WithdrawIntent
from almanak.framework.intents.vocabulary import Intent, LPCloseIntent, LPOpenIntent

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

TEST_WALLET = "0x1111111111111111111111111111111111111111"
TEST_ROUTER = "0x888888888d058D2D7e1f86b3eB1ce82d8d0F88a9"
PENDLE_ADAPTER_CLS = "almanak.framework.connectors.pendle.PendleAdapter"
# A real arbitrum PT-wstETH market from the static SDK config (no expiry caveat
# for the unit-test scope: we mock the adapter so on-chain expiry checks never
# fire). The address is intentionally checksummed as it appears in sdk.py.
ARBITRUM_WSTETH_MARKET = "0xf78452e0f5C0B95fc5dC8353B8CD1e06E53fa25B"
ARBITRUM_WSTETH_PT = "0x71fBF40651E9D4278a74586AfC99F307f369Ce9A"
ARBITRUM_WSTETH_YT = "0x25bda1edd6af17c61399aa0eb84b93daa3069764"


def _mock_token(
    symbol: str = "WETH",
    address: str | None = None,
    decimals: int = 18,
    is_native: bool = False,
) -> TokenInfo:
    """Return a real TokenInfo so .to_dict() / .address / .decimals roundtrip."""
    return TokenInfo(
        symbol=symbol,
        address=address or "0x" + "ab" * 20,
        decimals=decimals,
        is_native=is_native,
    )


def _approve_tx() -> TransactionData:
    return TransactionData(
        to="0x" + "cc" * 20,
        value=0,
        data="0x0000",
        gas_estimate=60_000,
        description="approve",
        tx_type="approve",
    )


def _adapter_tx_data(
    *,
    to: str = TEST_ROUTER,
    value: int = 0,
    data: str = "0xfeedface",
    gas_estimate: int = 200_000,
    description: str = "pendle-op",
) -> MagicMock:
    """Build a Pendle adapter return value matching ``PendleTransactionData`` shape."""
    res = MagicMock(name="PendleTxData")
    res.to = to
    res.value = value
    res.data = data
    res.gas_estimate = gas_estimate
    res.description = description
    return res


def _mock_compiler(chain: str = "arbitrum") -> MagicMock:
    """Build a minimal IntentCompiler stub the helpers actually consume."""
    compiler = MagicMock(name="MockCompiler")
    compiler.chain = chain
    compiler.wallet_address = TEST_WALLET
    compiler._gateway_client = None
    compiler._get_chain_rpc_url.return_value = "http://anvil:8545"
    compiler._build_approve_tx.return_value = [_approve_tx()]
    return compiler


def _mock_pendle_adapter(
    *,
    router: str = TEST_ROUTER,
    add_liquidity_data: MagicMock | None = None,
    remove_liquidity_data: MagicMock | None = None,
    redeem_data: MagicMock | None = None,
) -> MagicMock:
    """Build a PendleAdapter stub returning canned PendleTransactionData objects."""
    adapter = MagicMock(name="PendleAdapter")
    adapter.get_router_address.return_value = router
    adapter.build_add_liquidity.return_value = add_liquidity_data or _adapter_tx_data(description="add liquidity")
    adapter.build_remove_liquidity.return_value = remove_liquidity_data or _adapter_tx_data(
        description="remove liquidity"
    )
    adapter.build_redeem.return_value = redeem_data or _adapter_tx_data(description="redeem")
    return adapter


def _lp_open_intent(
    *,
    pool: str = f"WETH/{ARBITRUM_WSTETH_MARKET}",
    amount0: Decimal = Decimal("1"),
    amount1: Decimal = Decimal("0"),
    range_lower: Decimal = Decimal("1"),
    range_upper: Decimal = Decimal("100"),
    protocol: str = "pendle",
) -> LPOpenIntent:
    return LPOpenIntent(
        pool=pool,
        amount0=amount0,
        amount1=amount1,
        range_lower=range_lower,
        range_upper=range_upper,
        protocol=protocol,
    )


def _lp_close_intent(
    *,
    position_id: str = "1000000000000000000",
    pool: str = ARBITRUM_WSTETH_MARKET,
    protocol_params: dict[str, Any] | None = None,
) -> LPCloseIntent:
    return Intent.lp_close(
        position_id=position_id,
        pool=pool,
        collect_fees=True,
        protocol="pendle",
        protocol_params=protocol_params if protocol_params is not None else {"token": "WETH"},
    )


def _withdraw_intent(
    *,
    token: str = "WETH",
    amount: Any = Decimal("1"),
    market_id: str | None = ARBITRUM_WSTETH_YT,
    protocol: str = "pendle",
) -> WithdrawIntent:
    return WithdrawIntent(
        protocol=protocol,
        token=token,
        amount=amount,
        market_id=market_id,
    )


# ---------------------------------------------------------------------------
# Pure helpers (no PendleAdapter constructor needed)
# ---------------------------------------------------------------------------


class TestPureHelpers:
    """Branches inside the small extracted helpers."""

    def test_check_pendle_chain_supported_passes_for_arbitrum(self):
        compiler = _mock_compiler(chain="arbitrum")
        assert cp._check_pendle_chain_supported(compiler, "iid", "Pendle LP not available") is None

    def test_check_pendle_chain_supported_fails_for_base(self):
        compiler = _mock_compiler(chain="base")
        result = cp._check_pendle_chain_supported(compiler, "iid", "Pendle LP not available")
        assert result is not None
        assert result.status == CompilationStatus.FAILED
        assert "Pendle LP not available on base" == result.error

    def test_resolve_pendle_adapter_inputs_prefers_connected_gateway(self):
        compiler = _mock_compiler()
        gateway = MagicMock(name="GatewayClient")
        gateway.is_connected = True
        compiler._gateway_client = gateway
        compiler._get_chain_rpc_url.return_value = "http://node:8545"
        result = cp._resolve_pendle_adapter_inputs(compiler, "iid")
        assert result == (gateway, None)

    def test_resolve_pendle_adapter_inputs_normalizes_disconnected_gateway(self):
        compiler = _mock_compiler()
        gateway = MagicMock(name="GatewayClient")
        gateway.is_connected = False
        compiler._gateway_client = gateway
        compiler._get_chain_rpc_url.return_value = "http://node:8545"
        result = cp._resolve_pendle_adapter_inputs(compiler, "iid")
        assert result == (None, "http://node:8545")

    def test_resolve_pendle_adapter_inputs_falls_back_to_rpc_when_no_gateway(self):
        compiler = _mock_compiler()
        compiler._gateway_client = None
        compiler._get_chain_rpc_url.return_value = "http://node:8545"
        result = cp._resolve_pendle_adapter_inputs(compiler, "iid")
        assert result == (None, "http://node:8545")

    def test_resolve_pendle_adapter_inputs_fails_when_neither_available(self):
        compiler = _mock_compiler()
        compiler._gateway_client = None
        compiler._get_chain_rpc_url.return_value = None
        result = cp._resolve_pendle_adapter_inputs(compiler, "iid")
        assert result.status == CompilationStatus.FAILED
        assert "gateway_client" in result.error
        assert "RPC URL" in result.error

    def test_parse_pendle_lp_open_pool_token_and_market(self):
        token, market = cp._parse_pendle_lp_open_pool("WETH/0xabc", "iid")
        assert token == "WETH"
        assert market == "0xabc"

    def test_parse_pendle_lp_open_pool_bare_address_fails(self):
        result = cp._parse_pendle_lp_open_pool("0xabc", "iid")
        assert result.status == CompilationStatus.FAILED
        assert "TOKEN/0xmarket_address" in result.error

    def test_parse_pendle_lp_open_pool_garbage_fails(self):
        result = cp._parse_pendle_lp_open_pool("garbage", "iid")
        assert result.status == CompilationStatus.FAILED
        assert "Invalid Pendle pool format" in result.error

    def test_resolve_lp_open_market_passthrough_for_address(self):
        compiler = _mock_compiler()
        out = cp._resolve_pendle_lp_open_market(compiler, "0xfeed", "iid")
        assert out == "0xfeed"

    def test_resolve_lp_open_market_lookup_for_pt_name(self):
        compiler = _mock_compiler(chain="arbitrum")
        out = cp._resolve_pendle_lp_open_market(compiler, "PT-wstETH-25JUN2026", "iid")
        assert out == ARBITRUM_WSTETH_MARKET

    def test_resolve_lp_open_market_unknown_pt_name_fails(self):
        compiler = _mock_compiler(chain="arbitrum")
        result = cp._resolve_pendle_lp_open_market(compiler, "PT-NONEXISTENT", "iid")
        assert result.status == CompilationStatus.FAILED
        assert "Must be a 0x address or known PT token name" in result.error

    def test_resolve_lp_open_market_pt_name_case_insensitive(self):
        compiler = _mock_compiler(chain="arbitrum")
        out = cp._resolve_pendle_lp_open_market(compiler, "pt-wsteth-25jun2026", "iid")
        assert out == ARBITRUM_WSTETH_MARKET

    def test_compute_lp_open_amount_zero_fails(self):
        token = _mock_token(decimals=18)
        intent = _lp_open_intent(amount0=Decimal("0"), amount1=Decimal("1"))
        result = cp._compute_pendle_lp_open_amount(intent, token)
        assert result.status == CompilationStatus.FAILED
        assert "amount0 must be positive" in result.error

    def test_compute_lp_open_amount_decimals_18(self):
        token = _mock_token(decimals=18)
        intent = _lp_open_intent(amount0=Decimal("2.5"))
        amount_in = cp._compute_pendle_lp_open_amount(intent, token)
        assert amount_in == 2_500_000_000_000_000_000

    def test_resolve_lp_close_out_token_from_protocol_params_token_out(self):
        compiler = _mock_compiler()
        compiler._resolve_token.return_value = _mock_token(symbol="USDC", decimals=6)
        intent = _lp_close_intent(protocol_params={"token_out": "USDC"})
        out = cp._resolve_pendle_lp_close_out_token(compiler, intent)
        assert out.symbol == "USDC"
        compiler._resolve_token.assert_called_once_with("USDC")

    def test_resolve_lp_close_out_token_missing_fails(self):
        compiler = _mock_compiler()
        intent = _lp_close_intent(protocol_params={})
        result = cp._resolve_pendle_lp_close_out_token(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "Pendle LP close requires an output token" in result.error

    def test_resolve_lp_close_out_token_unknown_token_fails(self):
        compiler = _mock_compiler()
        compiler._resolve_token.return_value = None
        intent = _lp_close_intent(protocol_params={"token": "FAKE"})
        result = cp._resolve_pendle_lp_close_out_token(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "Unknown output token: FAKE" in result.error

    def test_parse_lp_close_amount_invalid_fails(self):
        intent = _lp_close_intent(position_id="not-a-number")
        result = cp._parse_pendle_lp_close_amount(intent)
        assert result.status == CompilationStatus.FAILED
        assert "Invalid LP amount" in result.error

    def test_parse_lp_close_amount_valid_int(self):
        intent = _lp_close_intent(position_id="500")
        assert cp._parse_pendle_lp_close_amount(intent) == 500

    def test_resolve_redeem_pt_address_via_static_lookup(self):
        compiler = _mock_compiler(chain="arbitrum")
        adapter = MagicMock(name="UnusedAdapter")
        pt = cp._resolve_pendle_redeem_pt_address(compiler, adapter, ARBITRUM_WSTETH_YT)
        assert pt == ARBITRUM_WSTETH_PT
        # Static lookup short-circuits — adapter must NOT be touched.
        adapter.assert_not_called()

    def test_resolve_redeem_pt_address_falls_back_to_on_chain(self):
        compiler = _mock_compiler(chain="arbitrum")
        adapter = MagicMock(name="Adapter")
        with patch.object(cp, "_resolve_pt_from_yt", return_value="0xpt") as mock_fallback:
            pt = cp._resolve_pendle_redeem_pt_address(compiler, adapter, "0xunknownYT")
        assert pt == "0xpt"
        mock_fallback.assert_called_once_with(adapter, "0xunknownYT")

    def test_build_redeem_pt_approval_emits_infinite_approve(self):
        tx = cp._build_pendle_redeem_pt_approval(ARBITRUM_WSTETH_PT, TEST_ROUTER)
        assert tx.tx_type == "approve"
        assert tx.to == ARBITRUM_WSTETH_PT
        # 0x095ea7b3 (approve(address,uint256)) prefix + 32-byte spender + 32-byte amount.
        assert tx.data.startswith("0x095ea7b3")
        assert len(tx.data) == 2 + 8 + 64 + 64
        # Spender is the lowercased router address right-padded into the 32-byte slot.
        assert TEST_ROUTER.lower()[2:] in tx.data


# ---------------------------------------------------------------------------
# compile_pendle_lp_open
# ---------------------------------------------------------------------------


class TestCompilePendleLPOpen:
    def test_unsupported_chain_fails(self):
        compiler = _mock_compiler(chain="base")
        intent = _lp_open_intent()
        result = cp.compile_pendle_lp_open(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "Pendle LP not available on base" in result.error

    def test_invalid_pool_format_fails(self):
        compiler = _mock_compiler()
        intent = _lp_open_intent(pool="garbage")
        result = cp.compile_pendle_lp_open(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "Invalid Pendle pool format" in result.error

    def test_unknown_token_fails(self):
        compiler = _mock_compiler()
        compiler._resolve_token.return_value = None
        intent = _lp_open_intent(pool=f"FAKE/{ARBITRUM_WSTETH_MARKET}")
        result = cp.compile_pendle_lp_open(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "Unknown token: FAKE" in result.error

    def test_zero_amount_fails(self):
        compiler = _mock_compiler()
        compiler._resolve_token.return_value = _mock_token(symbol="WETH", decimals=18)
        # Construct the intent past the model_validator by giving amount1 a positive value
        # so at-least-one-amount-positive holds, then forcing amount0 to zero downstream.
        intent = _lp_open_intent(amount0=Decimal("0"), amount1=Decimal("1"))
        result = cp.compile_pendle_lp_open(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "amount0 must be positive" in result.error

    @patch(PENDLE_ADAPTER_CLS)
    def test_happy_path_builds_approve_and_lp_open(self, mock_adapter_cls: MagicMock) -> None:
        compiler = _mock_compiler()
        compiler._resolve_token.return_value = _mock_token(symbol="WETH", decimals=18)
        mock_adapter_cls.return_value = _mock_pendle_adapter()

        intent = _lp_open_intent(amount0=Decimal("1.5"))
        result = cp.compile_pendle_lp_open(compiler, intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        tx_types = [tx.tx_type for tx in result.transactions]
        assert tx_types == ["approve", "lp_open"]
        assert result.action_bundle.metadata["protocol"] == "pendle"
        assert result.action_bundle.metadata["market"] == ARBITRUM_WSTETH_MARKET
        assert result.action_bundle.metadata["amount_in"] == str(1_500_000_000_000_000_000)
        assert result.action_bundle.metadata["chain"] == "arbitrum"

    @patch(PENDLE_ADAPTER_CLS)
    def test_native_token_skips_approve(self, mock_adapter_cls: MagicMock) -> None:
        compiler = _mock_compiler()
        compiler._resolve_token.return_value = _mock_token(symbol="ETH", decimals=18, is_native=True)
        mock_adapter_cls.return_value = _mock_pendle_adapter()

        intent = _lp_open_intent(pool=f"ETH/{ARBITRUM_WSTETH_MARKET}")
        result = cp.compile_pendle_lp_open(compiler, intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        tx_types = [tx.tx_type for tx in result.transactions]
        # Native-token leg: approve is skipped, only the lp_open tx remains.
        assert tx_types == ["lp_open"]
        compiler._build_approve_tx.assert_not_called()


# ---------------------------------------------------------------------------
# compile_pendle_lp_close
# ---------------------------------------------------------------------------


class TestCompilePendleLPClose:
    def test_unsupported_chain_fails(self):
        compiler = _mock_compiler(chain="solana")
        intent = _lp_close_intent()
        result = cp.compile_pendle_lp_close(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "Pendle LP not available on solana" in result.error

    def test_missing_out_token_fails(self):
        compiler = _mock_compiler()
        intent = _lp_close_intent(protocol_params={})
        result = cp.compile_pendle_lp_close(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "Pendle LP close requires an output token" in result.error

    def test_invalid_pool_address_fails(self):
        compiler = _mock_compiler()
        compiler._resolve_token.return_value = _mock_token(symbol="WETH", decimals=18)
        intent = _lp_close_intent(pool="not-a-market")
        result = cp.compile_pendle_lp_close(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "Invalid Pendle market address" in result.error

    def test_invalid_position_id_fails(self):
        compiler = _mock_compiler()
        compiler._resolve_token.return_value = _mock_token(symbol="WETH", decimals=18)
        intent = _lp_close_intent(position_id="abc")
        result = cp.compile_pendle_lp_close(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "Invalid LP amount" in result.error

    @patch(PENDLE_ADAPTER_CLS)
    def test_happy_path_builds_approve_and_lp_close(self, mock_adapter_cls: MagicMock) -> None:
        compiler = _mock_compiler()
        compiler._resolve_token.return_value = _mock_token(symbol="WETH", decimals=18)
        mock_adapter_cls.return_value = _mock_pendle_adapter()

        intent = _lp_close_intent(position_id="123456789")
        result = cp.compile_pendle_lp_close(compiler, intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        tx_types = [tx.tx_type for tx in result.transactions]
        assert tx_types == ["approve", "lp_close"]
        meta = result.action_bundle.metadata
        assert meta["protocol"] == "pendle"
        assert meta["market"] == ARBITRUM_WSTETH_MARKET
        assert meta["lp_amount"] == "123456789"
        assert meta["min_token_out"] == "0"


# ---------------------------------------------------------------------------
# compile_pendle_redeem
# ---------------------------------------------------------------------------


class TestCompilePendleRedeem:
    def test_unsupported_chain_fails(self):
        compiler = _mock_compiler(chain="optimism")
        intent = _withdraw_intent()
        result = cp.compile_pendle_redeem(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "Pendle redeem not available on optimism" in result.error

    def test_unknown_token_fails(self):
        compiler = _mock_compiler()
        compiler._resolve_token.return_value = None
        intent = _withdraw_intent(token="FAKE")
        result = cp.compile_pendle_redeem(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "Unknown token: FAKE" in result.error

    def test_missing_market_id_fails(self):
        compiler = _mock_compiler()
        compiler._resolve_token.return_value = _mock_token(symbol="WETH", decimals=18)
        intent = _withdraw_intent(market_id=None)
        result = cp.compile_pendle_redeem(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "market_id (YT address) is required" in result.error

    def test_amount_all_unresolved_fails(self):
        compiler = _mock_compiler()
        compiler._resolve_token.return_value = _mock_token(symbol="WETH", decimals=18)
        intent = _withdraw_intent(amount="all", market_id=ARBITRUM_WSTETH_YT)
        result = cp.compile_pendle_redeem(compiler, intent)
        assert result.status == CompilationStatus.FAILED
        assert "amount='all'" in result.error

    @patch(PENDLE_ADAPTER_CLS)
    def test_happy_path_static_pt_lookup_emits_approve_plus_redeem(self, mock_adapter_cls: MagicMock) -> None:
        """YT in static config => PT resolved offline; tx chain = [approve, redeem]."""
        compiler = _mock_compiler(chain="arbitrum")
        compiler._resolve_token.return_value = _mock_token(symbol="WETH", decimals=18)
        mock_adapter_cls.return_value = _mock_pendle_adapter()

        intent = _withdraw_intent(market_id=ARBITRUM_WSTETH_YT, amount=Decimal("1"))
        result = cp.compile_pendle_redeem(compiler, intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        tx_types = [tx.tx_type for tx in result.transactions]
        assert tx_types == ["approve", "redeem"]
        # The approve tx targets the PT contract (not the YT), per PT-spend semantics.
        approve_tx = result.transactions[0]
        assert approve_tx.to == ARBITRUM_WSTETH_PT
        meta = result.action_bundle.metadata
        assert meta["protocol"] == "pendle"
        assert meta["yt_address"] == ARBITRUM_WSTETH_YT
        # PT/YT are 18 decimals on Pendle: 1 token => 1e18 wei.
        assert meta["py_amount"] == str(10**18)

    @patch(PENDLE_ADAPTER_CLS)
    def test_no_pt_resolution_skips_approve(self, mock_adapter_cls: MagicMock) -> None:
        """Unknown YT + on-chain fallback returns None => only the redeem tx is emitted."""
        compiler = _mock_compiler(chain="arbitrum")
        compiler._resolve_token.return_value = _mock_token(symbol="WETH", decimals=18)
        mock_adapter_cls.return_value = _mock_pendle_adapter()

        unknown_yt = "0x" + "ee" * 20
        with patch.object(cp, "_resolve_pt_from_yt", return_value=None):
            intent = _withdraw_intent(market_id=unknown_yt, amount=Decimal("0.5"))
            result = cp.compile_pendle_redeem(compiler, intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        tx_types = [tx.tx_type for tx in result.transactions]
        assert tx_types == ["redeem"]
