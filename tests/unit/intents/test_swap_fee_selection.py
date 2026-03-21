"""Unit tests for swap fee-tier selection policy in IntentCompiler."""

import sys
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak import (
    DefaultSwapAdapter,
    IntentCompiler,
    IntentCompilerConfig,
    SwapIntent,
)


class TestIntentCompilerConfigSwapSelection:
    """Validate swap pool selection config behavior."""

    def test_fixed_mode_requires_fixed_fee_tier(self) -> None:
        """fixed mode should fail without fixed_swap_fee_tier."""
        with pytest.raises(ValueError, match="fixed_swap_fee_tier is required"):
            IntentCompilerConfig(
                allow_placeholder_prices=True,
                swap_pool_selection_mode="fixed",
            )


class TestDefaultSwapAdapterFeeSelection:
    """Validate fee-tier selection behavior in DefaultSwapAdapter."""

    def test_fixed_fee_tier_is_used(self) -> None:
        """Adapter should use configured fixed tier when valid."""
        adapter = DefaultSwapAdapter(
            chain="arbitrum",
            protocol="uniswap_v3",
            pool_selection_mode="fixed",
            fixed_fee_tier=500,
        )
        calldata = adapter.get_swap_calldata(
            from_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  # USDC
            to_token="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  # WETH
            amount_in=1_000_000,
            min_amount_out=1,
            recipient="0x1234567890123456789012345678901234567890",
            deadline=0,
        )
        assert len(calldata) > 4
        assert adapter.last_fee_selection["selected_fee_tier"] == 500
        assert adapter.last_fee_selection["source"] == "fixed_config"

    def test_auto_mode_uses_heuristic_without_rpc(self) -> None:
        """AUTO mode should safely fall back to heuristic when no rpc_url is provided."""
        adapter = DefaultSwapAdapter(
            chain="arbitrum",
            protocol="uniswap_v3",
            pool_selection_mode="auto",
        )
        adapter.get_swap_calldata(
            from_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  # USDC
            to_token="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  # WETH
            amount_in=1_000_000,
            min_amount_out=1,
            recipient="0x1234567890123456789012345678901234567890",
            deadline=0,
        )
        assert adapter.last_fee_selection["selected_fee_tier"] == 500
        assert adapter.last_fee_selection["source"] == "heuristic_fallback"

    def test_fixed_mode_raises_when_protocol_has_no_fee_tiers(self) -> None:
        """Fixed mode should fail deterministically when protocol has no supported tiers."""
        adapter = DefaultSwapAdapter(
            chain="arbitrum",
            protocol="unsupported_protocol",
            pool_selection_mode="fixed",
            fixed_fee_tier=500,
        )
        with pytest.raises(ValueError, match="Invalid fixed fee tier"):
            adapter.get_swap_calldata(
                from_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                to_token="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                amount_in=1_000_000,
                min_amount_out=1,
                recipient="0x1234567890123456789012345678901234567890",
                deadline=0,
            )

    def test_auto_mode_passes_rpc_timeout_to_http_provider(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """AUTO mode should pass configured timeout into Web3 HTTP provider."""
        provider_calls: list[dict[str, object]] = []

        class FakeHTTPProvider:
            def __init__(self, url: str, request_kwargs: dict[str, object] | None = None) -> None:
                provider_calls.append(
                    {
                        "url": url,
                        "request_kwargs": request_kwargs or {},
                    }
                )

        class FakeFunctionCall:
            @staticmethod
            def call() -> tuple[int, int, int, int]:
                return (123, 0, 0, 100_000)

        class FakeFunctions:
            @staticmethod
            def quoteExactInputSingle(_params: tuple[str, str, int, int, int]) -> FakeFunctionCall:
                return FakeFunctionCall()

        class FakeContract:
            functions = FakeFunctions()

        class FakeEth:
            @staticmethod
            def contract(address: str, abi: list[dict[str, object]]) -> FakeContract:
                _ = (address, abi)
                return FakeContract()

        class FakeWeb3:
            HTTPProvider = FakeHTTPProvider

            def __init__(self, _provider: FakeHTTPProvider) -> None:
                self.eth = FakeEth()

            @staticmethod
            def is_connected() -> bool:
                return True

            @staticmethod
            def to_checksum_address(address: str) -> str:
                return address

        monkeypatch.setitem(sys.modules, "web3", SimpleNamespace(Web3=FakeWeb3))

        adapter = DefaultSwapAdapter(
            chain="arbitrum",
            protocol="uniswap_v3",
            pool_selection_mode="auto",
            rpc_url="https://example-rpc",
            rpc_timeout=3.5,
        )
        adapter.get_swap_calldata(
            from_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            to_token="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            amount_in=1_000_000,
            min_amount_out=1,
            recipient="0x1234567890123456789012345678901234567890",
            deadline=0,
        )

        assert provider_calls
        assert provider_calls[0]["url"] == "https://example-rpc"
        assert provider_calls[0]["request_kwargs"] == {"timeout": 3.5}


class TestSelectFeeTierCaching:
    """Validate that select_fee_tier() caches the result and get_swap_calldata() reuses it."""

    def test_select_fee_tier_caches_and_reuses(self) -> None:
        """Pre-selecting fee tier should be reused by get_swap_calldata."""
        adapter = DefaultSwapAdapter(
            chain="arbitrum",
            protocol="uniswap_v3",
            pool_selection_mode="fixed",
            fixed_fee_tier=3000,
        )
        fee = adapter.select_fee_tier(
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            1_000_000,
        )
        assert fee == 3000
        assert adapter._cached_fee == 3000

        # get_swap_calldata should reuse the cached fee
        calldata = adapter.get_swap_calldata(
            from_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            to_token="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            amount_in=1_000_000,
            min_amount_out=1,
            recipient="0x1234567890123456789012345678901234567890",
            deadline=0,
        )
        assert len(calldata) > 4
        assert adapter.last_fee_selection["selected_fee_tier"] == 3000

    def test_get_quoted_amount_out_none_without_quoter(self) -> None:
        """Without RPC/quoter, get_quoted_amount_out returns None."""
        adapter = DefaultSwapAdapter(
            chain="arbitrum",
            protocol="uniswap_v3",
            pool_selection_mode="auto",
        )
        adapter.select_fee_tier(
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            1_000_000,
        )
        assert adapter.get_quoted_amount_out() is None


class TestParallelQuoterWithMock:
    """Validate parallel fee tier querying via quoter."""

    def _make_fake_web3_module(self, quote_results: dict[int, int]) -> SimpleNamespace:
        """Create a fake web3 module where quoteExactInputSingle returns preset results."""

        class FakeHTTPProvider:
            def __init__(self, url: str, request_kwargs: dict[str, object] | None = None) -> None:
                pass

        class FakeFunctionCall:
            def __init__(self, amount_out: int) -> None:
                self._amount_out = amount_out

            def call(self) -> tuple[int, int, int, int]:
                return (self._amount_out, 0, 0, 100_000)

        results = quote_results

        class FakeFunctions:
            @staticmethod
            def quoteExactInputSingle(params: tuple[str, str, int, int, int]) -> FakeFunctionCall:
                fee_tier = params[3]
                if fee_tier in results:
                    return FakeFunctionCall(results[fee_tier])
                raise Exception(f"No pool for fee tier {fee_tier}")

        class FakeContract:
            functions = FakeFunctions()

        class FakeEth:
            @staticmethod
            def contract(address: str, abi: list[dict[str, object]]) -> FakeContract:
                return FakeContract()

        class FakeWeb3:
            HTTPProvider = FakeHTTPProvider

            def __init__(self, _provider: FakeHTTPProvider) -> None:
                self.eth = FakeEth()

            @staticmethod
            def is_connected() -> bool:
                return True

            @staticmethod
            def to_checksum_address(address: str) -> str:
                return address

        return SimpleNamespace(Web3=FakeWeb3)

    def test_quoter_stores_best_amount_out(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Quoter should store the best amount_out in last_quoted_amount_out."""
        # Fee tier 500 returns 5000, fee tier 3000 returns 4900
        fake_web3 = self._make_fake_web3_module({500: 5000, 3000: 4900})
        monkeypatch.setitem(sys.modules, "web3", fake_web3)

        adapter = DefaultSwapAdapter(
            chain="arbitrum",
            protocol="uniswap_v3",
            pool_selection_mode="auto",
            rpc_url="https://example-rpc",
        )
        fee = adapter.select_fee_tier(
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            1_000_000,
        )
        assert fee == 500  # Best amount_out
        assert adapter.get_quoted_amount_out() == 5000
        assert adapter.last_fee_selection["source"] == "quoter_best_quote"

    def test_quoter_parallel_returns_all_valid_candidates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """All valid fee tier quotes should appear in quoted_candidates."""
        fake_web3 = self._make_fake_web3_module({100: 4800, 500: 5000, 3000: 4900, 10000: 4500})
        monkeypatch.setitem(sys.modules, "web3", fake_web3)

        adapter = DefaultSwapAdapter(
            chain="arbitrum",
            protocol="uniswap_v3",
            pool_selection_mode="auto",
            rpc_url="https://example-rpc",
        )
        adapter.select_fee_tier(
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            1_000_000,
        )
        candidates = adapter.last_fee_selection["quoted_candidates"]
        assert len(candidates) == 4
        assert adapter.get_quoted_amount_out() == 5000

    def test_quoter_amount_lower_tightens_slippage(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When quoter amount < price oracle, compilation should use quoter for min_output."""
        # Quoter returns 900 (lower than price oracle's ~997 estimate)
        fake_web3 = self._make_fake_web3_module({500: 900})
        monkeypatch.setitem(sys.modules, "web3", fake_web3)

        compiler = IntentCompiler(
            chain="arbitrum",
            config=IntentCompilerConfig(
                allow_placeholder_prices=True,
                swap_pool_selection_mode="auto",
            ),
            rpc_url="https://example-rpc",
        )
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
            protocol="uniswap_v3",
        )
        result = compiler.compile(intent)
        assert result.status.value == "SUCCESS"
        assert result.action_bundle is not None

        # min_amount_out should be based on quoter amount (900), not oracle
        # 900 * (1 - 0.01) = 891
        min_out = int(result.action_bundle.metadata["min_amount_out"])
        assert min_out == 891


class TestCompilerSwapMetadata:
    """Ensure selected tier metadata is exposed on compiled swap bundles."""

    def test_compiled_swap_includes_fee_selection_metadata(self) -> None:
        """Compiled swap bundle should include pool selection metadata."""
        compiler = IntentCompiler(
            chain="arbitrum",
            config=IntentCompilerConfig(
                allow_placeholder_prices=True,
                swap_pool_selection_mode="fixed",
                fixed_swap_fee_tier=500,
            ),
        )
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
            protocol="uniswap_v3",
        )
        result = compiler.compile(intent)

        assert result.status.value == "SUCCESS"
        assert result.action_bundle is not None
        metadata = result.action_bundle.metadata
        assert metadata["pool_selection_mode"] == "fixed"
        assert metadata["selected_fee_tier"] == 500
        assert metadata["fee_selection_source"] == "fixed_config"
