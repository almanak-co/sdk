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

    def test_camelot_uses_algebra_calldata(self) -> None:
        """VIB-1636: Camelot (Algebra V1.9) must use 0xbc651188 selector with no fee param."""
        adapter = DefaultSwapAdapter(
            chain="arbitrum",
            protocol="camelot",
            pool_selection_mode="auto",
        )
        from_token = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"  # USDC
        to_token = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"  # WETH
        recipient = "0x1234567890123456789012345678901234567890"
        calldata = adapter.get_swap_calldata(
            from_token=from_token,
            to_token=to_token,
            amount_in=1_000_000,
            min_amount_out=1,
            recipient=recipient,
            deadline=1_700_000_000,
        )
        # Selector is Algebra exactInputSingle (no fee, with deadline, with limitSqrtPrice)
        assert calldata[:4].hex() == "bc651188"
        # 7 uint256-padded words = 7 * 32 bytes = 224 bytes params + 4-byte selector = 228 bytes total
        assert len(calldata) == 4 + 7 * 32
        body = calldata[4:]
        # Word 0: tokenIn (padded), Word 1: tokenOut, Word 2: recipient, Word 3: deadline
        assert body[12:32].hex().lower() == from_token[2:].lower()
        assert body[44:64].hex().lower() == to_token[2:].lower()
        assert body[76:96].hex().lower() == recipient[2:].lower()
        assert int.from_bytes(body[96:128], "big") == 1_700_000_000
        # Word 4: amountIn, Word 5: amountOutMinimum, Word 6: limitSqrtPrice
        assert int.from_bytes(body[128:160], "big") == 1_000_000
        assert int.from_bytes(body[160:192], "big") == 1
        assert int.from_bytes(body[192:224], "big") == 0

    def test_uniswap_v3_uses_swap_router_02_selector(self) -> None:
        """Regression: non-Algebra V3 protocols still use SwapRouter02 selector."""
        adapter = DefaultSwapAdapter(
            chain="arbitrum",
            protocol="uniswap_v3",
            pool_selection_mode="fixed",
            fixed_fee_tier=500,
        )
        calldata = adapter.get_swap_calldata(
            from_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            to_token="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            amount_in=1_000_000,
            min_amount_out=1,
            recipient="0x1234567890123456789012345678901234567890",
            deadline=0,
        )
        assert calldata[:4].hex() == "04e45aaf"

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

    def test_auto_mode_uses_heuristic_when_gateway_is_disconnected(self) -> None:
        """AUTO mode should not crash when a gateway client exists but is disconnected."""
        adapter = DefaultSwapAdapter(
            chain="arbitrum",
            protocol="uniswap_v3",
            pool_selection_mode="auto",
            gateway_client=SimpleNamespace(is_connected=False),
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


class TestAlgebraQuoter:
    """Validate the Algebra V1.9 (Camelot V3) quoter path — VIB-3750.

    The Algebra quoter has a different ABI from Uniswap V3's QuoterV2:
    flat args (no struct), no `fee` parameter, returns ``(amountOut, fee)``
    where the fee is the dynamic fee the pool would charge.
    """

    @staticmethod
    def _make_fake_algebra_web3(
        amount_out: int,
        dynamic_fee: int = 100,
        *,
        recorder: list[tuple[str, str, int, int]] | None = None,
        raise_exc: type[BaseException] | None = None,
    ) -> SimpleNamespace:
        """Build a fake `web3` module exposing the Algebra quoter ABI."""

        class FakeHTTPProvider:
            def __init__(self, url: str, request_kwargs: dict[str, object] | None = None) -> None:
                pass

        class FakeFunctionCall:
            def __init__(
                self,
                token_in: str,
                token_out: str,
                amt_in: int,
                limit: int,
            ) -> None:
                self._args = (token_in, token_out, amt_in, limit)

            def call(self) -> tuple[int, int]:
                if recorder is not None:
                    recorder.append(self._args)
                if raise_exc is not None:
                    raise raise_exc("simulated quoter failure")
                return amount_out, dynamic_fee

        class FakeFunctions:
            @staticmethod
            def quoteExactInputSingle(token_in: str, token_out: str, amt_in: int, limit: int) -> FakeFunctionCall:
                return FakeFunctionCall(token_in, token_out, amt_in, limit)

        class FakeContract:
            functions = FakeFunctions()

        class FakeEth:
            @staticmethod
            def contract(address: str, abi: list[dict[str, object]]) -> FakeContract:
                # Sanity check: assert ABI is the Algebra flat-args form
                func_abi = abi[0]
                input_types = [i["type"] for i in func_abi["inputs"]]
                assert input_types == ["address", "address", "uint256", "uint160"], (
                    f"Algebra quoter must use flat-args ABI; got {input_types}"
                )
                output_types = [o["type"] for o in func_abi["outputs"]]
                assert output_types == ["uint256", "uint16"], (
                    f"Algebra quoter must return (amountOut, fee); got {output_types}"
                )
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

    def test_camelot_quoter_populates_amount_out(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Algebra-style quoter call returns amountOut + dynamic fee."""
        recorder: list[tuple[str, str, int, int]] = []
        fake = self._make_fake_algebra_web3(
            amount_out=22_073_113_969_137_859,
            dynamic_fee=100,
            recorder=recorder,
        )
        monkeypatch.setitem(sys.modules, "web3", fake)

        adapter = DefaultSwapAdapter(
            chain="arbitrum",
            protocol="camelot",
            pool_selection_mode="auto",
            rpc_url="https://example-rpc",
        )
        adapter.select_fee_tier(
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  # USDC
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  # WETH
            50_000_000,
        )

        # Quoter result is recorded so the price-impact guard can run
        assert adapter.get_quoted_amount_out() == 22_073_113_969_137_859
        # Algebra quoter is invoked exactly once (no per-fee-tier loop)
        assert len(recorder) == 1
        # And called with the flat-args signature (no fee parameter)
        token_in, token_out, amt_in, limit = recorder[0]
        assert token_in.lower() == "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
        assert token_out.lower() == "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
        assert amt_in == 50_000_000
        assert limit == 0

        sel = adapter.last_fee_selection
        assert sel["source"] == "algebra_quoter"
        assert sel["protocol_family"] == "algebra_v1_9"
        assert sel["selected_fee_tier"] == 100  # The dynamic fee returned by the pool
        assert sel["candidate_fee_tiers"] == []  # Algebra has no fee tiers
        assert sel["quoted_amount_out"] == 22_073_113_969_137_859

    def test_camelot_quoter_zero_amount_distinguished_from_unreachable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When the quoter returns 0, leave amount as None but record the typed source.

        This is critical for ops: "quoter returned 0" means "pool has no
        liquidity at this size" — distinct from "RPC unreachable" which
        means "we don't know". Both fail-closed via the price-impact guard,
        but the metadata source must distinguish them so logs / metrics can.
        """
        fake = self._make_fake_algebra_web3(amount_out=0, dynamic_fee=100)
        monkeypatch.setitem(sys.modules, "web3", fake)

        adapter = DefaultSwapAdapter(
            chain="arbitrum",
            protocol="camelot",
            pool_selection_mode="auto",
            rpc_url="https://example-rpc",
        )
        adapter.select_fee_tier(
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            50_000_000,
        )

        # NOT silently defaulted to 0 — left None so the price-impact guard fails closed
        assert adapter.get_quoted_amount_out() is None
        assert adapter.last_fee_selection["source"] == "algebra_quoter_returned_zero"
        assert adapter.last_fee_selection["dynamic_fee"] == 100

    def test_camelot_quoter_call_failure_distinct_from_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """RPC failure is recorded with a distinct source from 'returned zero'."""
        fake = self._make_fake_algebra_web3(amount_out=0, raise_exc=RuntimeError)
        monkeypatch.setitem(sys.modules, "web3", fake)

        adapter = DefaultSwapAdapter(
            chain="arbitrum",
            protocol="camelot",
            pool_selection_mode="auto",
            rpc_url="https://example-rpc",
        )
        adapter.select_fee_tier(
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            50_000_000,
        )

        assert adapter.get_quoted_amount_out() is None
        assert adapter.last_fee_selection["source"] == "algebra_quoter_call_failed"
        assert "simulated quoter failure" in adapter.last_fee_selection["error"]

    def test_camelot_offline_no_rpc_no_quoter_call(self) -> None:
        """When no RPC and no gateway are configured, quoter is not attempted."""
        adapter = DefaultSwapAdapter(
            chain="arbitrum",
            protocol="camelot",
            pool_selection_mode="auto",
        )
        adapter.select_fee_tier(
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            50_000_000,
        )

        assert adapter.get_quoted_amount_out() is None
        # Source records "unavailable" rather than "call_failed" so ops sees
        # this as a config/env issue, not an RPC outage.
        assert adapter.last_fee_selection["source"] == "algebra_quoter_unavailable"

    def test_camelot_quoter_address_registered(self) -> None:
        """Camelot V3 quoter is registered in SWAP_QUOTER_ADDRESSES['arbitrum']."""
        from almanak.framework.intents.compiler_constants import SWAP_QUOTER_ADDRESSES

        assert SWAP_QUOTER_ADDRESSES["arbitrum"]["camelot"] == "0x0Fc73040b26E9bC8514fA028D998E73A254Fa76E"

    def test_camelot_swap_compile_succeeds_with_quoter(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """End-to-end: a Camelot swap compiles successfully when the quoter answers.

        Reproduces VIB-3750: pre-fix the price-impact guard rejected camelot
        with "quoter returned no amount". Post-fix the Algebra quoter feeds
        ``last_quoted_amount_out`` into the guard and compilation succeeds.
        """
        fake = self._make_fake_algebra_web3(amount_out=22_073_113_969_137_859, dynamic_fee=100)
        monkeypatch.setitem(sys.modules, "web3", fake)

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
            amount=Decimal("50"),
            max_slippage=Decimal("0.05"),
            protocol="camelot",
        )
        result = compiler.compile(intent)

        assert result.status.value == "SUCCESS", result.error
        assert result.action_bundle is not None
        md = result.action_bundle.metadata
        assert md["protocol"] == "camelot"
        assert md["fee_selection_source"] == "algebra_quoter"
        # The router used must be the Camelot V3 SwapRouter
        assert md["router"].lower() == "0x1f721e2e82f6676fce4ea07a5958cf098d339e18"

        # The swap TX uses the Algebra exactInputSingle selector
        swap_tx = result.action_bundle.transactions[-1]
        assert swap_tx["data"].startswith("0xbc651188")
