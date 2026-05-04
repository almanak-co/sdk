"""Tests for AerodromeAdapter Slipstream CL liquidity operations + on-chain quoting.

Targets uncovered branches in:
- add_cl_liquidity success path (Decimal mode, both token-orderings, missing cl_nft)
- remove_cl_liquidity (empty position, decreaseLiquidity+collect, exception)
- _get_web3 (rpc_url branch, gateway branch, no-config error)
- _try_get_amount_out_onchain
- _get_quote_exact_input on-chain success branch
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.aerodrome.adapter import (
    AerodromeAdapter,
    AerodromeConfig,
    CLLiquidityResult,
)
from almanak.framework.connectors.aerodrome.sdk import CLPositionInfo
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken

TEST_WALLET = "0x1234567890123456789012345678901234567890"
USDC_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
WETH_ADDRESS = "0x4200000000000000000000000000000000000006"


def _make_resolver() -> MagicMock:
    mock = MagicMock()

    def _resolve(symbol_or_addr: str, *args: object, **kwargs: object) -> ResolvedToken:
        addr = symbol_or_addr.lower() if symbol_or_addr.startswith("0x") else None
        if symbol_or_addr in ("USDC",) or addr == USDC_ADDRESS.lower():
            return ResolvedToken(symbol="USDC", address=USDC_ADDRESS, decimals=6, chain="base", chain_id=8453)
        if symbol_or_addr in ("WETH",) or addr == WETH_ADDRESS.lower():
            return ResolvedToken(symbol="WETH", address=WETH_ADDRESS, decimals=18, chain="base", chain_id=8453)
        raise TokenResolutionError(token=symbol_or_addr, chain="base", reason="x")

    mock.resolve.side_effect = _resolve
    return mock


@pytest.fixture
def adapter() -> AerodromeAdapter:
    cfg = AerodromeConfig(chain="base", wallet_address=TEST_WALLET, allow_placeholder_prices=True)
    return AerodromeAdapter(cfg, token_resolver=_make_resolver())


@pytest.fixture
def opt_adapter() -> AerodromeAdapter:
    """Optimism adapter — no `cl_nft` in addresses, exercises the missing-cl_nft branch."""
    cfg = AerodromeConfig(chain="optimism", wallet_address=TEST_WALLET, allow_placeholder_prices=True)
    return AerodromeAdapter(cfg, token_resolver=_make_resolver())


# =============================================================================
# add_cl_liquidity — full success paths
# =============================================================================


class TestAddCLLiquidityHappyPath:
    """Decimal-mode add_cl_liquidity end-to-end through SDK build_cl_mint_tx."""

    def test_add_cl_liquidity_decimal_mode_success(self, adapter: AerodromeAdapter) -> None:
        # Mock SDK + web3
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.build_cl_mint_tx = MagicMock(  # type: ignore[method-assign]
            return_value={"to": adapter.addresses["cl_nft"], "value": 0, "data": b"\x00\x01\x02\x03"},
        )
        result = adapter.add_cl_liquidity(
            token_a="USDC",
            token_b="WETH",
            tick_spacing=200,
            tick_lower=-1000,
            tick_upper=1000,
            amount_a=Decimal("100"),
            amount_b=Decimal("0.05"),
        )
        assert result.success is True
        assert isinstance(result, CLLiquidityResult)
        # Token sorting: USDC > WETH lex, so token0 should be WETH
        assert result.token0 == WETH_ADDRESS
        assert result.token1 == USDC_ADDRESS
        # Hex-converted bytes data on mint tx
        mint_tx = result.transactions[-1]
        assert mint_tx.tx_type == "add_liquidity"
        assert mint_tx.data == "00010203"

    def test_add_cl_liquidity_data_already_hex_string(self, adapter: AerodromeAdapter) -> None:
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.build_cl_mint_tx = MagicMock(  # type: ignore[method-assign]
            return_value={"to": adapter.addresses["cl_nft"], "data": "0xdeadbeef"},
        )
        result = adapter.add_cl_liquidity(
            token_a="USDC",
            token_b="WETH",
            tick_spacing=100,
            tick_lower=-100,
            tick_upper=100,
            amount_a=Decimal("1"),
            amount_b=Decimal("0.001"),
        )
        assert result.success
        assert result.transactions[-1].data == "0xdeadbeef"

    def test_add_cl_liquidity_missing_cl_nft_returns_error(self, opt_adapter: AerodromeAdapter) -> None:
        result = opt_adapter.add_cl_liquidity(
            token_a="USDC",
            token_b="WETH",
            tick_spacing=200,
            tick_lower=-100,
            tick_upper=100,
            amount_a=Decimal("1"),
            amount_b=Decimal("0.001"),
        )
        assert result.success is False
        assert "Slipstream CL not supported" in (result.error or "")

    def test_add_cl_liquidity_exception_caught(self, adapter: AerodromeAdapter) -> None:
        adapter._get_web3 = MagicMock(side_effect=RuntimeError("no web3"))  # type: ignore[method-assign]
        result = adapter.add_cl_liquidity(
            token_a="USDC",
            token_b="WETH",
            tick_spacing=200,
            tick_lower=-100,
            tick_upper=100,
            amount_a=Decimal("1"),
            amount_b=Decimal("0.001"),
        )
        assert result.success is False
        assert "no web3" in (result.error or "")

    def test_add_cl_liquidity_wei_overload_uses_overrides(self, adapter: AerodromeAdapter) -> None:
        """Wei-overload path threads pre-computed mins all the way to the mint TX."""
        captured: dict = {}

        def _mock_mint(**kw: object) -> dict:
            captured.update(kw)
            return {"to": adapter.addresses["cl_nft"], "data": b""}

        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.build_cl_mint_tx = MagicMock(side_effect=_mock_mint)  # type: ignore[method-assign]
        result = adapter.add_cl_liquidity(
            token_a="USDC",
            token_b="WETH",
            tick_spacing=200,
            tick_lower=-100,
            tick_upper=100,
            amount_a=Decimal("0"),
            amount_b=Decimal("0"),
            amount_a_wei=1_000_000,
            amount_b_wei=5 * 10**14,
            amount_a_min_wei=900_000,
            amount_b_min_wei=4 * 10**14,
        )
        assert result.success
        # The mint tx received the overrides (sorted by token order)
        # USDC > WETH, so token0=WETH, amount0_min should be the b-min
        assert captured["amount0_min"] == 4 * 10**14
        assert captured["amount1_min"] == 900_000

    def test_add_cl_liquidity_token_a_lex_lower(self, adapter: AerodromeAdapter) -> None:
        """Reverse the token order to exercise the alternate sort branch."""
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.build_cl_mint_tx = MagicMock(  # type: ignore[method-assign]
            return_value={"to": adapter.addresses["cl_nft"], "data": b""},
        )
        # WETH < USDC lex (4200... < 833...), so token_a=WETH means token_a is lower
        result = adapter.add_cl_liquidity(
            token_a="WETH",
            token_b="USDC",
            tick_spacing=200,
            tick_lower=-100,
            tick_upper=100,
            amount_a=Decimal("0.05"),
            amount_b=Decimal("100"),
        )
        assert result.success
        # token0 should be WETH (the smaller addr)
        assert result.token0 == WETH_ADDRESS
        assert result.token1 == USDC_ADDRESS


# =============================================================================
# remove_cl_liquidity
# =============================================================================


class TestRemoveCLLiquidity:
    def test_remove_cl_position_not_found_returns_error(self, adapter: AerodromeAdapter) -> None:
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.get_cl_position = MagicMock(return_value=None)  # type: ignore[method-assign]
        result = adapter.remove_cl_liquidity(token_id=42)
        assert result.success is False
        assert "Could not query CL position" in (result.error or "")

    def test_remove_cl_zero_liquidity_no_owed_returns_noop(self, adapter: AerodromeAdapter) -> None:
        position = CLPositionInfo(
            token_id=42,
            token0=USDC_ADDRESS,
            token1=WETH_ADDRESS,
            tick_spacing=200,
            tick_lower=-100,
            tick_upper=100,
            liquidity=0,
            tokens_owed0=0,
            tokens_owed1=0,
        )
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.get_cl_position = MagicMock(return_value=position)  # type: ignore[method-assign]
        result = adapter.remove_cl_liquidity(token_id=42)
        assert result.success is True
        assert result.transactions == []
        assert result.gas_estimate == 0

    def test_remove_cl_liquidity_with_liquidity_builds_decrease_and_collect(
        self,
        adapter: AerodromeAdapter,
    ) -> None:
        position = CLPositionInfo(
            token_id=42,
            token0=USDC_ADDRESS,
            token1=WETH_ADDRESS,
            tick_spacing=200,
            tick_lower=-100,
            tick_upper=100,
            liquidity=10**18,
            tokens_owed0=0,
            tokens_owed1=0,
        )
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.get_cl_position = MagicMock(return_value=position)  # type: ignore[method-assign]
        adapter.sdk.build_cl_decrease_liquidity_tx = MagicMock(  # type: ignore[method-assign]
            return_value={"to": adapter.addresses["cl_nft"], "data": b"\xde\xad"},
        )
        adapter.sdk.build_cl_collect_tx = MagicMock(  # type: ignore[method-assign]
            return_value={"to": adapter.addresses["cl_nft"], "data": "0xcafe"},
        )
        result = adapter.remove_cl_liquidity(token_id=42)
        assert result.success
        # decrease + collect
        assert len(result.transactions) == 2
        assert result.transactions[0].tx_type == "remove_liquidity"
        assert result.transactions[1].tx_type == "remove_liquidity"
        # Bytes data hex-converted, str data passed through
        assert result.transactions[0].data == "dead"
        assert result.transactions[1].data == "0xcafe"

    def test_remove_cl_zero_liquidity_with_owed_only_collect(self, adapter: AerodromeAdapter) -> None:
        """has_owed=True with liquidity=0 → only collect, no decreaseLiquidity."""
        position = CLPositionInfo(
            token_id=42,
            token0=USDC_ADDRESS,
            token1=WETH_ADDRESS,
            tick_spacing=200,
            tick_lower=-100,
            tick_upper=100,
            liquidity=0,
            tokens_owed0=100,
            tokens_owed1=0,
        )
        adapter._get_web3 = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        adapter.sdk.get_cl_position = MagicMock(return_value=position)  # type: ignore[method-assign]
        adapter.sdk.build_cl_collect_tx = MagicMock(  # type: ignore[method-assign]
            return_value={"to": adapter.addresses["cl_nft"], "data": b""},
        )
        result = adapter.remove_cl_liquidity(token_id=42)
        assert result.success
        assert len(result.transactions) == 1
        assert "collect" in result.transactions[0].description.lower()

    def test_remove_cl_liquidity_exception_caught(self, adapter: AerodromeAdapter) -> None:
        adapter._get_web3 = MagicMock(side_effect=RuntimeError("rpc fail"))  # type: ignore[method-assign]
        result = adapter.remove_cl_liquidity(token_id=42)
        assert result.success is False
        assert "rpc fail" in (result.error or "")


# =============================================================================
# _get_web3
# =============================================================================


class TestGetWeb3:
    def test_get_web3_no_config_raises(self, adapter: AerodromeAdapter) -> None:
        # Both gateway_client and rpc_url are None on the default fixture
        adapter.config.gateway_client = None
        adapter.config.rpc_url = None
        with pytest.raises(ValueError, match="No gateway_client or rpc_url"):
            adapter._get_web3()

    def test_get_web3_with_rpc_url(self) -> None:
        cfg = AerodromeConfig(
            chain="base",
            wallet_address=TEST_WALLET,
            rpc_url="https://localhost:8545",
            allow_placeholder_prices=True,
        )
        adapter = AerodromeAdapter(cfg, token_resolver=_make_resolver())
        # Should construct a Web3 with HTTPProvider
        with patch("web3.Web3") as mock_web3:
            adapter._get_web3()
            # First positional arg is the HTTPProvider
            assert mock_web3.called

    def test_get_web3_with_gateway_client(self) -> None:
        cfg = AerodromeConfig(
            chain="base",
            wallet_address=TEST_WALLET,
            gateway_client=MagicMock(),
            allow_placeholder_prices=True,
        )
        adapter = AerodromeAdapter(cfg, token_resolver=_make_resolver())
        with patch("almanak.framework.web3.gateway_provider.GatewayWeb3Provider"):
            with patch("web3.Web3") as mock_web3:
                adapter._get_web3()
                assert mock_web3.called


# =============================================================================
# _try_get_amount_out_onchain
# =============================================================================


class TestTryGetAmountOutOnchain:
    def test_returns_none_when_no_rpc_or_gateway(self, adapter: AerodromeAdapter) -> None:
        adapter.config.rpc_url = None
        adapter.config.gateway_client = None
        adapter._web3 = None
        out = adapter._try_get_amount_out_onchain(USDC_ADDRESS, WETH_ADDRESS, 1_000_000, False)
        assert out is None

    def test_returns_amount_when_rpc_succeeds(self, adapter: AerodromeAdapter) -> None:
        adapter._web3 = MagicMock()
        adapter.sdk.get_amounts_out = MagicMock(return_value=[1_000_000, 5 * 10**14])  # type: ignore[method-assign]
        out = adapter._try_get_amount_out_onchain(USDC_ADDRESS, WETH_ADDRESS, 1_000_000, False)
        assert out == 5 * 10**14

    def test_returns_none_when_rpc_returns_empty(self, adapter: AerodromeAdapter) -> None:
        adapter._web3 = MagicMock()
        adapter.sdk.get_amounts_out = MagicMock(return_value=None)  # type: ignore[method-assign]
        out = adapter._try_get_amount_out_onchain(USDC_ADDRESS, WETH_ADDRESS, 1_000_000, False)
        assert out is None

    def test_returns_none_on_sdk_exception(self, adapter: AerodromeAdapter) -> None:
        adapter._web3 = MagicMock()
        adapter.sdk.get_amounts_out = MagicMock(side_effect=RuntimeError("rpc"))  # type: ignore[method-assign]
        out = adapter._try_get_amount_out_onchain(USDC_ADDRESS, WETH_ADDRESS, 1_000_000, False)
        assert out is None

    def test_quote_uses_onchain_amount_when_available(self, adapter: AerodromeAdapter) -> None:
        """_get_quote_exact_input takes the onchain branch when amount_out resolves."""
        adapter._try_get_amount_out_onchain = MagicMock(  # type: ignore[method-assign]
            return_value=5 * 10**14,
        )
        quote = adapter._get_quote_exact_input(
            USDC_ADDRESS, WETH_ADDRESS, 1_000_000, False, skip_onchain=False
        )
        assert quote.amount_out == 5 * 10**14
        # effective_price = 0.0005 / 1 = 0.0005
        assert quote.effective_price == Decimal("0.0005") / Decimal("1")
