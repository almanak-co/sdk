"""VIB-5361: close a Uniswap V4 LP position by id alone.

A V4 position is keyed by a pool-id (not a self-describing NFT like V3), so the
close path needs the pool currencies. These tests pin the on-chain recovery of
``(currency0, currency1)`` from the position NFT via
``PositionManager.getPoolAndPositionInfo`` and the compiler fallback that uses it
so ``ax lp-close <id> --protocol uniswap_v4`` works without a pool hint.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

# Sorted V4 currencies on Arbitrum: WETH < USDC numerically.
WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
HOOKS_NONE = "0x0000000000000000000000000000000000000000"


def _encode_word_address(addr: str) -> str:
    return addr.lower().removeprefix("0x").rjust(64, "0")


def _encode_word_uint(value: int) -> str:
    return format(value & ((1 << 256) - 1), "064x")


def _pool_and_position_info_payload(
    *,
    currency0: str = WETH,
    currency1: str = USDC,
    fee: int = 3000,
    tick_spacing: int = 60,
    hooks: str = HOOKS_NONE,
    info: int = 0,
) -> str:
    """Build a realistic ``getPoolAndPositionInfo`` return: 5 PoolKey head words + info."""
    words = [
        _encode_word_address(currency0),
        _encode_word_address(currency1),
        _encode_word_uint(fee),
        _encode_word_uint(tick_spacing),
        _encode_word_address(hooks),
        _encode_word_uint(info),
    ]
    return "0x" + "".join(words)


class TestSdkGetPositionPoolKey:
    def _sdk(self):
        from almanak.connectors.uniswap_v4.sdk import UniswapV4SDK

        return UniswapV4SDK(chain="arbitrum", rpc_url="http://127.0.0.1:8545")

    def test_decodes_currencies_fee_and_spacing(self):
        sdk = self._sdk()
        payload = _pool_and_position_info_payload()
        with patch("almanak.connectors.uniswap_v4.sdk.eth_call_hex", return_value=payload) as mock_call:
            pool_key = sdk.get_position_pool_key(654321)
        # Selector + 32-byte token id calldata routed to the PositionManager.
        called_kwargs = mock_call.call_args.kwargs
        assert called_kwargs["data"].startswith("0x7ba03aad")
        assert called_kwargs["to"] == sdk.position_manager
        assert pool_key.currency0 == WETH
        assert pool_key.currency1 == USDC
        assert pool_key.fee == 3000
        assert pool_key.tick_spacing == 60
        assert pool_key.hooks == HOOKS_NONE

    def test_negative_tick_spacing_is_signed(self):
        sdk = self._sdk()
        # int24 -60 two's complement in the low 24 bits.
        payload = _pool_and_position_info_payload(tick_spacing=(-60) & ((1 << 24) - 1))
        with patch("almanak.connectors.uniswap_v4.sdk.eth_call_hex", return_value=payload):
            pool_key = sdk.get_position_pool_key(1)
        assert pool_key.tick_spacing == -60

    def test_short_payload_raises(self):
        sdk = self._sdk()
        with patch("almanak.connectors.uniswap_v4.sdk.eth_call_hex", return_value="0x" + "00" * 64):
            with pytest.raises(ValueError, match="too short"):
                sdk.get_position_pool_key(1)

    def test_empty_result_raises(self):
        sdk = self._sdk()
        with patch("almanak.connectors.uniswap_v4.sdk.eth_call_hex", return_value=None):
            with pytest.raises(ValueError, match="no result"):
                sdk.get_position_pool_key(1)

    def test_degenerate_all_zero_pool_key_raises(self):
        """A burned / non-existent token id can decode to an all-zero PoolKey
        (currency0 == currency1 == 0x0). Reject it instead of building an invalid
        equal-currencies pool."""
        sdk = self._sdk()
        zero = "0x0000000000000000000000000000000000000000"
        payload = _pool_and_position_info_payload(currency0=zero, currency1=zero, fee=0, tick_spacing=0)
        with patch("almanak.connectors.uniswap_v4.sdk.eth_call_hex", return_value=payload):
            with pytest.raises(ValueError, match="degenerate pool key"):
                sdk.get_position_pool_key(999)

    def test_identical_nonzero_currencies_raises(self):
        """The guard rejects ANY identical currency pair, not just all-zero — an
        invalid/burned id can decode to equal non-zero currencies, an unusable pool."""
        sdk = self._sdk()
        payload = _pool_and_position_info_payload(currency0=WETH, currency1=WETH)
        with patch("almanak.connectors.uniswap_v4.sdk.eth_call_hex", return_value=payload):
            with pytest.raises(ValueError, match="degenerate pool key"):
                sdk.get_position_pool_key(123)


class TestAdapterGetPositionCurrencies:
    @pytest.fixture()
    def adapter(self):
        from almanak.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config

        config = UniswapV4Config(
            chain="arbitrum",
            wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        )
        return UniswapV4Adapter(config=config, token_resolver=MagicMock())

    def test_delegates_to_sdk_pool_key(self, adapter):
        from almanak.connectors.uniswap_v4.sdk import PoolKey

        with patch.object(
            adapter._sdk,
            "get_position_pool_key",
            return_value=PoolKey(currency0=WETH, currency1=USDC, fee=3000, tick_spacing=60),
        ) as mock_pk:
            c0, c1 = adapter.get_position_currencies(654321, rpc_url="http://x")
        mock_pk.assert_called_once_with(654321, rpc_url="http://x")
        assert (c0, c1) == (WETH, USDC)


class TestCompilerCloseByIdResolvesCurrencies:
    """The V4 compiler must recover currencies from the position id on-chain when
    neither protocol_params nor a pool hint supply them, then build the close."""

    def _compiler_and_ctx(self):
        from types import SimpleNamespace

        from almanak.connectors.uniswap_v4.compiler import UniswapV4Compiler

        compiler = UniswapV4Compiler()
        # The close path only reads ctx.rpc_url; the adapter (which would consume
        # the rest of the context) is patched per-test.
        ctx = SimpleNamespace(rpc_url="http://127.0.0.1:8545")
        return compiler, ctx

    def test_close_by_id_resolves_currencies_on_chain(self):
        from almanak.framework.intents.vocabulary import LPCloseIntent

        compiler, ctx = self._compiler_and_ctx()
        intent = LPCloseIntent(position_id="654321", protocol="uniswap_v4")  # no pool, no protocol_params

        fake_adapter = MagicMock()
        fake_adapter.get_position_liquidity.return_value = 1_000_000
        fake_adapter.get_position_currencies.return_value = (WETH, USDC)
        # Return a non-empty bundle so the compile is reported SUCCESS.
        fake_bundle = MagicMock()
        fake_bundle.transactions = [{"to": "0xpm", "value": 0, "data": "0x", "gas_estimate": 1, "description": ""}]
        fake_bundle.metadata = {"gas_estimate": 1}
        fake_adapter.compile_lp_close_intent.return_value = fake_bundle

        with patch.object(compiler, "_adapter", return_value=fake_adapter):
            result = compiler.compile_lp_close(ctx, intent)

        from almanak.framework.intents.compiler_models import CompilationStatus

        assert result.status == CompilationStatus.SUCCESS
        # Currencies came from the on-chain pool-key read, not a pool string.
        fake_adapter.get_position_currencies.assert_called_once()
        _, kwargs = fake_adapter.compile_lp_close_intent.call_args
        assert {kwargs["currency0"], kwargs["currency1"]} == {WETH, USDC}

    def test_close_by_id_fails_clearly_when_currencies_unresolvable(self):
        from almanak.framework.intents.compiler_models import CompilationStatus
        from almanak.framework.intents.vocabulary import LPCloseIntent

        compiler, ctx = self._compiler_and_ctx()
        intent = LPCloseIntent(position_id="654321", protocol="uniswap_v4")

        fake_adapter = MagicMock()
        fake_adapter.get_position_liquidity.return_value = 1_000_000
        fake_adapter.get_position_currencies.side_effect = ValueError("rpc down")

        with patch.object(compiler, "_adapter", return_value=fake_adapter):
            result = compiler.compile_lp_close(ctx, intent)

        assert result.status == CompilationStatus.FAILED
        assert "currency0" in result.error and "currency1" in result.error

    def test_protocol_params_currencies_skip_on_chain_read(self):
        from almanak.framework.intents.vocabulary import LPCloseIntent

        compiler, ctx = self._compiler_and_ctx()
        intent = LPCloseIntent(
            position_id="654321",
            protocol="uniswap_v4",
            protocol_params={"liquidity": 1_000_000, "currency0": WETH, "currency1": USDC},
        )

        fake_adapter = MagicMock()
        fake_bundle = MagicMock()
        fake_bundle.transactions = [{"to": "0xpm", "value": 0, "data": "0x", "gas_estimate": 1, "description": ""}]
        fake_bundle.metadata = {"gas_estimate": 1}
        fake_adapter.compile_lp_close_intent.return_value = fake_bundle

        with patch.object(compiler, "_adapter", return_value=fake_adapter):
            compiler.compile_lp_close(ctx, intent)

        # protocol_params already carried currencies → no on-chain pool-key read.
        fake_adapter.get_position_currencies.assert_not_called()
