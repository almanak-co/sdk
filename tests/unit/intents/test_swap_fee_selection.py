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
