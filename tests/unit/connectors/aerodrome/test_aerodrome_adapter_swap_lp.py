"""Tests for AerodromeAdapter swap and liquidity transaction builders.

Targets uncovered branches in:
- swap_exact_input (CL + classic, native ETH path, error paths)
- add_liquidity / remove_liquidity (success + error paths)
- _build_swap_exact_input_tx, _build_swap_exact_input_cl_tx
- _build_add_liquidity_tx, _build_remove_liquidity_tx, _build_approve_tx
- compile_swap_intent (all branches)
- Helpers: _is_native_token, _get_default_price_oracle, _encode_route, _pad_*
- Allowance cache (set_allowance, clear_allowance_cache)
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from web3 import Web3

from almanak.connectors.aerodrome.adapter import (
    AerodromeAdapter,
    AerodromeConfig,
    LiquidityResult,
    SwapResult,
    TransactionData,
)
from almanak.connectors.aerodrome.addresses import SlipstreamDeployment, slipstream_deployment_for_factory
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken
from almanak.framework.intents.vocabulary import IntentType, SwapIntent

TEST_WALLET = "0x1234567890123456789012345678901234567890"
USDC_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
WETH_ADDRESS = "0x4200000000000000000000000000000000000006"
NATIVE_ETH = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"


def _generation(factory: str) -> SlipstreamDeployment:
    deployment = slipstream_deployment_for_factory("base", factory)
    assert deployment is not None, factory
    return deployment


# CL swaps execute through the router and quoter of the reviewed generation
# that owns the pool; there is no default Slipstream router.
CURRENT = _generation("0xf8f2eB4940CFE7d13603DDDD87f123820Fc061Ef")
LEGACY = _generation("0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A")


def _make_resolver(known_addresses: dict[str, tuple[str, int]]) -> MagicMock:
    """Build a TokenResolver mock from a {address: (symbol, decimals)} map.

    Looks up by address (case-insensitive) OR by symbol (case-sensitive).
    """
    mock = MagicMock()
    by_symbol = {sym: (addr, dec) for addr, (sym, dec) in known_addresses.items()}
    by_address_lower = {addr.lower(): (sym, dec) for addr, (sym, dec) in known_addresses.items()}

    def _resolve(symbol_or_addr: str, *args: object, **kwargs: object) -> ResolvedToken:
        # If looks like an address, resolve by address first
        if symbol_or_addr.startswith("0x") and len(symbol_or_addr) == 42:
            key = symbol_or_addr.lower()
            if key in by_address_lower:
                sym, dec = by_address_lower[key]
                return ResolvedToken(symbol=sym, address=symbol_or_addr, decimals=dec, chain="base", chain_id=8453)
        # Else, by symbol
        if symbol_or_addr in by_symbol:
            addr, dec = by_symbol[symbol_or_addr]
            return ResolvedToken(symbol=symbol_or_addr, address=addr, decimals=dec, chain="base", chain_id=8453)
        raise TokenResolutionError(token=symbol_or_addr, chain="base", reason="not in test map")

    mock.resolve.side_effect = _resolve
    return mock


@pytest.fixture
def usdc_weth_resolver() -> MagicMock:
    return _make_resolver(
        {
            USDC_ADDRESS: ("USDC", 6),
            WETH_ADDRESS: ("WETH", 18),
        }
    )


@pytest.fixture
def adapter(usdc_weth_resolver: MagicMock) -> AerodromeAdapter:
    cfg = AerodromeConfig(
        chain="base",
        wallet_address=TEST_WALLET,
        allow_placeholder_prices=True,
    )
    built = AerodromeAdapter(cfg, token_resolver=usdc_weth_resolver)
    # LP legs are floored against the router's own quote and refuse to build
    # without one. These unit tests have no transport, so stand in for the
    # quote: a pool that accepts the full deposit. Tests that care about the
    # refusal override this back to None.
    built._quote_add_liquidity = lambda _ta, _tb, _stable, a_wei, b_wei: (a_wei, b_wei)  # type: ignore[method-assign]
    built._quote_remove_liquidity = lambda _ta, _tb, _stable, liquidity_wei: (  # type: ignore[method-assign]
        liquidity_wei,
        liquidity_wei,
    )
    return built


# =============================================================================
# Config & init branches
# =============================================================================


class TestAerodromeConfigBranches:
    def test_invalid_slippage_negative_raises(self) -> None:
        with pytest.raises(ValueError, match="Slippage"):
            AerodromeConfig(
                chain="base",
                wallet_address=TEST_WALLET,
                default_slippage_bps=-1,
                allow_placeholder_prices=True,
            )

    def test_invalid_slippage_too_large_raises(self) -> None:
        with pytest.raises(ValueError, match="Slippage"):
            AerodromeConfig(
                chain="base",
                wallet_address=TEST_WALLET,
                default_slippage_bps=10001,
                allow_placeholder_prices=True,
            )

    def test_missing_price_provider_without_placeholder_flag_raises(self) -> None:
        with pytest.raises(ValueError, match="price_provider"):
            AerodromeConfig(chain="base", wallet_address=TEST_WALLET)

    def test_with_explicit_price_provider_no_placeholders_used(self) -> None:
        cfg = AerodromeConfig(
            chain="base",
            wallet_address=TEST_WALLET,
            price_provider={"WETH": Decimal("3400"), "USDC": Decimal("1")},
        )
        adapter = AerodromeAdapter(cfg, token_resolver=MagicMock())
        assert adapter._using_placeholders is False
        assert adapter._price_provider["WETH"] == Decimal("3400")

    def test_to_dict_round_trip(self) -> None:
        cfg = AerodromeConfig(chain="base", wallet_address=TEST_WALLET, allow_placeholder_prices=True)
        d = cfg.to_dict()
        assert d["chain"] == "base"
        assert d["wallet_address"] == TEST_WALLET
        assert d["default_slippage_bps"] == 50

    def test_placeholder_prices_warned_in_init(self, adapter: AerodromeAdapter) -> None:
        # Init path: placeholder branch — verify oracle has hardcoded entries.
        prices = adapter._get_default_price_oracle()
        assert prices["ETH"] == Decimal("2000")
        assert prices["WETH"] == Decimal("2000")
        assert prices["USDC"] == Decimal("1")


# =============================================================================
# swap_exact_input
# =============================================================================


class TestSwapExactInputCL:
    """CL routing (default) path."""

    def test_cl_swap_builds_approve_and_swap_tx(self, adapter: AerodromeAdapter) -> None:
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("100"),
            stable=False,
            deployment=CURRENT,
        )
        assert result.success
        # approve + swap
        assert len(result.transactions) == 2
        assert result.transactions[0].tx_type == "approve"
        assert result.transactions[1].tx_type == "swap"
        # swap is to the owning generation's router
        assert result.transactions[1].to == CURRENT.swap_router
        # amount_in_wei is 100 * 10^6 = 100_000_000
        assert result.amount_in == 100_000_000

    @pytest.mark.parametrize("deployment", [CURRENT, LEGACY], ids=["current", "legacy"])
    def test_cl_swap_approves_and_routes_through_the_generation_router(
        self, adapter: AerodromeAdapter, deployment: SlipstreamDeployment
    ) -> None:
        """The approve spender and the swap target are the supplied generation's router, never a constant."""
        assert deployment.swap_router is not None
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("100"),
            deployment=deployment,
        )
        assert result.success, result.error
        approve, swap = result.transactions
        assert approve.tx_type == "approve"
        # ERC-20 approve(spender, amount): the spender is the second calldata word.
        spender_word = approve.data[10:74]
        assert spender_word == deployment.swap_router[2:].lower().rjust(64, "0")
        assert swap.to == deployment.swap_router

    def test_cl_swap_without_deployment_is_refused_before_any_build(self, adapter: AerodromeAdapter) -> None:
        adapter._get_quote_exact_input = MagicMock()  # type: ignore[method-assign]
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("100"),
        )
        assert result.success is False
        assert "requires the reviewed generation" in (result.error or "")
        assert result.transactions == []
        adapter._get_quote_exact_input.assert_not_called()

    def test_cl_swap_rejects_an_unreviewed_deployment(self, adapter: AerodromeAdapter) -> None:
        injected = SlipstreamDeployment(
            factory="0x" + "aa" * 20,
            position_manager="0x" + "bb" * 20,
            generation="injected",
            swap_router="0x" + "cc" * 20,
        )
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1"),
            deployment=injected,
        )
        assert result.success is False
        assert "unreviewed Slipstream deployment" in (result.error or "")

    def test_cl_swap_quotes_through_the_generation_quoter(self, adapter: AerodromeAdapter) -> None:
        adapter._try_get_cl_amount_out_onchain = MagicMock(return_value=5 * 10**16)  # type: ignore[method-assign]
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("100"),
            deployment=LEGACY,
        )
        assert result.success, result.error
        assert result.quote is not None
        assert result.quote.amount_out == 5 * 10**16
        assert adapter._try_get_cl_amount_out_onchain.call_args.kwargs["quoter"] == LEGACY.quoter

    def test_cl_swap_unknown_input_token_returns_error(self, usdc_weth_resolver: MagicMock) -> None:
        # token_in resolution will raise — adapter wraps it in SwapResult error.
        cfg = AerodromeConfig(chain="base", wallet_address=TEST_WALLET, allow_placeholder_prices=True)
        adapter = AerodromeAdapter(cfg, token_resolver=usdc_weth_resolver)
        result = adapter.swap_exact_input(
            token_in="UNKNOWN_TOKEN",
            token_out="WETH",
            amount_in=Decimal("1"),
            deployment=CURRENT,
        )
        assert result.success is False
        # Either "Unknown input token" or TokenResolutionError reason
        assert result.error is not None
        assert "requires the reviewed generation" not in result.error

    def test_swap_exception_caught_returns_failed_result(self, adapter: AerodromeAdapter) -> None:
        # Force exception in _get_quote_exact_input.
        adapter._get_quote_exact_input = MagicMock(side_effect=RuntimeError("boom"))  # type: ignore[method-assign]
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1"),
            deployment=CURRENT,
        )
        assert result.success is False
        assert "boom" in (result.error or "")

    def test_reemits_approve_after_abandoned_and_failed_bundle(self, adapter: AerodromeAdapter) -> None:
        first = adapter.swap_exact_input("USDC", "WETH", Decimal("1"), deployment=CURRENT)
        retry = adapter.swap_exact_input("USDC", "WETH", Decimal("1"), deployment=CURRENT)

        assert any(tx.tx_type == "approve" for tx in first.transactions)
        assert any(tx.tx_type == "approve" for tx in retry.transactions)

        with patch.object(adapter, "_build_swap_exact_input_cl_tx", side_effect=RuntimeError("build failed")):
            failed = adapter.swap_exact_input("USDC", "WETH", Decimal("1"), deployment=CURRENT)
        after_failure = adapter.swap_exact_input("USDC", "WETH", Decimal("1"), deployment=CURRENT)

        assert failed.success is False
        assert any(tx.tx_type == "approve" for tx in after_failure.transactions)


class TestSwapExactInputClassic:
    """Classic routing path (use_classic=True)."""

    def test_classic_swap_uses_router_address(self, adapter: AerodromeAdapter) -> None:
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10"),
            stable=False,
            use_classic=True,
        )
        assert result.success
        # last tx is to classic router
        assert result.transactions[-1].to == adapter.addresses["router"]

    def test_classic_stable_pool_swap(self, adapter: AerodromeAdapter) -> None:
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("5"),
            stable=True,
            use_classic=True,
            slippage_bps=10,
        )
        assert result.success
        assert "stable" in result.transactions[-1].description.lower()


class TestSwapExactInputNativeToken:
    """Native ETH input replaces token_in with WETH and skips approve."""

    def test_native_eth_input_no_approve(self, usdc_weth_resolver: MagicMock) -> None:
        # Add ETH placeholder as resolvable to native marker so resolver can map "ETH" → WETH addr fallback.
        usdc_weth_resolver.resolve.side_effect = None

        def _resolve(symbol_or_addr: str, *args: object, **kwargs: object) -> ResolvedToken:
            mapping = {
                "ETH": WETH_ADDRESS,  # ETH resolves to WETH
                "WETH": WETH_ADDRESS,
                WETH_ADDRESS: WETH_ADDRESS,
                "USDC": USDC_ADDRESS,
                USDC_ADDRESS: USDC_ADDRESS,
            }
            if symbol_or_addr not in mapping:
                raise TokenResolutionError(token=symbol_or_addr, chain="base", reason="x")
            return ResolvedToken(
                symbol="WETH" if mapping[symbol_or_addr] == WETH_ADDRESS else "USDC",
                address=mapping[symbol_or_addr],
                decimals=18 if mapping[symbol_or_addr] == WETH_ADDRESS else 6,
                chain="base",
                chain_id=8453,
            )

        usdc_weth_resolver.resolve.side_effect = _resolve

        cfg = AerodromeConfig(chain="base", wallet_address=TEST_WALLET, allow_placeholder_prices=True)
        adapter = AerodromeAdapter(cfg, token_resolver=usdc_weth_resolver)
        result = adapter.swap_exact_input(
            token_in="ETH",
            token_out="USDC",
            amount_in=Decimal("1"),
            deployment=CURRENT,
        )
        assert result.success
        # No approve tx — only swap
        approves = [tx for tx in result.transactions if tx.tx_type == "approve"]
        assert approves == []
        assert result.transactions[-1].to == CURRENT.swap_router


# =============================================================================
# add_liquidity
# =============================================================================


class TestAddLiquidity:
    def test_add_liquidity_success_two_approves_one_add(self, adapter: AerodromeAdapter) -> None:
        result = adapter.add_liquidity(
            token_a="USDC",
            token_b="WETH",
            amount_a=Decimal("100"),
            amount_b=Decimal("0.05"),
            stable=False,
        )
        assert result.success
        # 2 approves + 1 add
        tx_types = [tx.tx_type for tx in result.transactions]
        assert tx_types.count("approve") == 2
        assert tx_types.count("add_liquidity") == 1
        assert result.token_a == USDC_ADDRESS
        assert result.token_b == WETH_ADDRESS
        # amount_a_wei = 100 * 10^6
        assert result.amount_a == 100_000_000

    def test_add_liquidity_stable_pool(self, adapter: AerodromeAdapter) -> None:
        # Add second resolver entry for cbETH
        adapter._token_resolver.resolve.side_effect = lambda s, *a, **kw: (
            ResolvedToken(symbol="USDC", address=USDC_ADDRESS, decimals=6, chain="base", chain_id=8453)
            if s in ("USDC", USDC_ADDRESS)
            else ResolvedToken(symbol="WETH", address=WETH_ADDRESS, decimals=18, chain="base", chain_id=8453)
        )
        result = adapter.add_liquidity(
            token_a="USDC",
            token_b="WETH",
            amount_a=Decimal("100"),
            amount_b=Decimal("0.05"),
            stable=True,
        )
        assert result.success
        assert result.stable is True

    def test_add_liquidity_unknown_token_returns_error(self, adapter: AerodromeAdapter) -> None:
        # Force the resolver to raise for one token symbol.
        def _resolve(symbol: str, *a: object, **kw: object) -> ResolvedToken:
            if symbol == "BAD":
                raise TokenResolutionError(token="BAD", chain="base", reason="missing")
            if symbol in ("USDC", USDC_ADDRESS):
                return ResolvedToken(symbol="USDC", address=USDC_ADDRESS, decimals=6, chain="base", chain_id=8453)
            return ResolvedToken(symbol="WETH", address=WETH_ADDRESS, decimals=18, chain="base", chain_id=8453)

        adapter._token_resolver.resolve.side_effect = _resolve
        result = adapter.add_liquidity(
            token_a="BAD",
            token_b="WETH",
            amount_a=Decimal("1"),
            amount_b=Decimal("1"),
        )
        assert result.success is False
        # Caught as exception in the broader try/except.
        assert result.error is not None

    def test_add_liquidity_exception_caught(self, adapter: AerodromeAdapter) -> None:
        adapter._build_add_liquidity_tx = MagicMock(side_effect=RuntimeError("calldata"))  # type: ignore[method-assign]
        result = adapter.add_liquidity(
            token_a="USDC",
            token_b="WETH",
            amount_a=Decimal("100"),
            amount_b=Decimal("0.05"),
        )
        assert result.success is False
        assert "calldata" in (result.error or "")


# =============================================================================
# remove_liquidity
# =============================================================================


class TestRemoveLiquidity:
    def test_remove_liquidity_with_pool_address_skips_sdk_lookup(self, adapter: AerodromeAdapter) -> None:
        pool = "0xcDAC0d6c6C59727a65F871236188350531885C43"
        result = adapter.remove_liquidity(
            token_a="USDC",
            token_b="WETH",
            liquidity=Decimal("1.5"),
            stable=False,
            pool_address=pool,
        )
        assert result.success
        # approve LP token + remove tx
        assert len(result.transactions) == 2
        # First tx targets LP pool address (approving router for LP)
        assert result.transactions[0].to == pool

    def test_remove_liquidity_pool_lookup_returns_none_warns(self, adapter: AerodromeAdapter) -> None:
        # Force sdk.get_pool_address to return None — no approve tx is built.
        adapter.sdk.get_pool_address = MagicMock(return_value=None)  # type: ignore[method-assign]
        result = adapter.remove_liquidity(
            token_a="USDC",
            token_b="WETH",
            liquidity=Decimal("1"),
        )
        assert result.success
        # Only the remove_liq tx — no approve
        types = [tx.tx_type for tx in result.transactions]
        assert "approve" not in types
        assert "remove_liquidity" in types

    def test_remove_liquidity_unknown_token_caught(self, adapter: AerodromeAdapter) -> None:
        def _resolve(symbol: str, *a: object, **kw: object) -> ResolvedToken:
            if symbol == "BAD":
                raise TokenResolutionError(token="BAD", chain="base", reason="x")
            if symbol in ("USDC", USDC_ADDRESS):
                return ResolvedToken(symbol="USDC", address=USDC_ADDRESS, decimals=6, chain="base", chain_id=8453)
            return ResolvedToken(symbol="WETH", address=WETH_ADDRESS, decimals=18, chain="base", chain_id=8453)

        adapter._token_resolver.resolve.side_effect = _resolve
        result = adapter.remove_liquidity(
            token_a="BAD",
            token_b="WETH",
            liquidity=Decimal("1"),
        )
        assert result.success is False
        assert result.error is not None

    def test_remove_liquidity_exception_caught(self, adapter: AerodromeAdapter) -> None:
        adapter._build_remove_liquidity_tx = MagicMock(side_effect=RuntimeError("revert"))  # type: ignore[method-assign]
        result = adapter.remove_liquidity(
            token_a="USDC",
            token_b="WETH",
            liquidity=Decimal("1"),
            pool_address="0x" + "00" * 20,
        )
        assert result.success is False
        assert "revert" in (result.error or "")


# =============================================================================
# compile_swap_intent
# =============================================================================


class TestCompileSwapIntent:
    def test_compile_with_explicit_amount_succeeds(self, adapter: AerodromeAdapter) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            chain="base",
        )
        bundle = adapter.compile_swap_intent(intent)
        assert bundle.intent_type == IntentType.SWAP.value
        assert bundle.metadata["from_token"] == "USDC"
        assert bundle.metadata["protocol"] == "aerodrome"

    def test_compile_with_amount_usd_uses_oracle(self, adapter: AerodromeAdapter) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
            chain="base",
        )
        bundle = adapter.compile_swap_intent(intent, price_oracle={"USDC": Decimal("1")})
        assert bundle.intent_type == IntentType.SWAP.value
        assert "amount_in" in bundle.metadata

    def test_compile_amount_all_raises(self, adapter: AerodromeAdapter) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1"),
            chain="base",
        )
        # Manually set amount to 'all' bypassing pydantic validation.
        object.__setattr__(intent, "amount", "all")
        with pytest.raises(ValueError, match="resolved before compilation"):
            adapter.compile_swap_intent(intent)

    def test_compile_amount_usd_missing_price_raises(self, adapter: AerodromeAdapter) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
            chain="base",
        )
        with pytest.raises(ValueError, match="Price unavailable"):
            adapter.compile_swap_intent(intent, price_oracle={"WETH": Decimal("3400")})

    def test_compile_no_amount_raises(self, adapter: AerodromeAdapter) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1"),
            chain="base",
        )
        # Bypass pydantic to set both to None
        object.__setattr__(intent, "amount", None)
        object.__setattr__(intent, "amount_usd", None)
        with pytest.raises(ValueError, match="amount or amount_usd"):
            adapter.compile_swap_intent(intent)

    def test_compile_swap_failure_returns_empty_bundle(self, adapter: AerodromeAdapter) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1"),
            chain="base",
        )
        # Force swap_exact_input to return a failure
        adapter.swap_exact_input = MagicMock(  # type: ignore[method-assign]
            return_value=SwapResult(success=False, error="forced fail"),
        )
        bundle = adapter.compile_swap_intent(intent)
        assert bundle.transactions == []
        assert bundle.metadata["error"] == "forced fail"

    def test_compile_uses_default_price_oracle_when_missing(self, adapter: AerodromeAdapter) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("10"),
            chain="base",
        )
        # No price_oracle provided — adapter falls back to its placeholder oracle.
        bundle = adapter.compile_swap_intent(intent)
        assert bundle.intent_type == IntentType.SWAP.value


# =============================================================================
# Approve cache
# =============================================================================


class TestApproveCache:
    def test_set_and_clear_allowance_cache(self, adapter: AerodromeAdapter) -> None:
        adapter.set_allowance(USDC_ADDRESS, adapter.addresses["router"], 1_000_000_000)
        # Now build_approve should return None since cache is sufficient.
        tx = adapter._build_approve_tx(USDC_ADDRESS, adapter.addresses["router"], 100)
        assert tx is None

        adapter.clear_allowance_cache()
        tx2 = adapter._build_approve_tx(USDC_ADDRESS, adapter.addresses["router"], 100)
        assert tx2 is not None
        assert tx2.tx_type == "approve"


# =============================================================================
# Static helpers
# =============================================================================


class TestPaddingHelpers:
    def test_pad_address_lowers_and_pads(self) -> None:
        out = AerodromeAdapter._pad_address("0xABCDEF1234567890ABCDEF1234567890ABCDEF12")
        assert len(out) == 64
        assert out == "abcdef1234567890abcdef1234567890abcdef12".rjust(64, "0")

    def test_pad_address_rejects_malformed_input(self) -> None:
        with pytest.raises(ValueError, match="address must be 20 bytes"):
            AerodromeAdapter._pad_address("0xabc")

    def test_pad_uint256_zero(self) -> None:
        out = AerodromeAdapter._pad_uint256(0)
        assert out == "0" * 64

    def test_pad_uint256_value(self) -> None:
        out = AerodromeAdapter._pad_uint256(255)
        assert out == "0" * 62 + "ff"

    @pytest.mark.parametrize("value", [-1, 1 << 256])
    def test_pad_uint256_rejects_out_of_range_values(self, value: int) -> None:
        with pytest.raises(ValueError, match="uint256 value must be"):
            AerodromeAdapter._pad_uint256(value)

    def test_pad_int24_negative_two_complement(self) -> None:
        # -1 in two's complement = 2^256 - 1 = all f's
        out = AerodromeAdapter._pad_int24(-1)
        assert out == "f" * 64

    def test_pad_int24_positive(self) -> None:
        assert AerodromeAdapter._pad_int24(100) == hex(100)[2:].zfill(64)

    def test_pad_bool_true(self) -> None:
        assert AerodromeAdapter._pad_bool(True) == "0" * 63 + "1"

    def test_pad_bool_false(self) -> None:
        assert AerodromeAdapter._pad_bool(False) == "0" * 64


class TestNativeTokenDetection:
    def test_eth_symbol_is_native(self, adapter: AerodromeAdapter) -> None:
        assert adapter._is_native_token("ETH") is True
        assert adapter._is_native_token("eth") is True

    def test_native_placeholder_address_is_native(self, adapter: AerodromeAdapter) -> None:
        assert adapter._is_native_token(NATIVE_ETH) is True
        assert adapter._is_native_token(NATIVE_ETH.lower()) is True

    def test_normal_token_not_native(self, adapter: AerodromeAdapter) -> None:
        assert adapter._is_native_token("USDC") is False
        assert adapter._is_native_token(USDC_ADDRESS) is False

    def test_foreign_native_symbols_not_native(self, adapter: AerodromeAdapter) -> None:
        """Registry-derived per-chain gate (VIB-4851 A1): base is ETH-native,
        other chains' gas symbols stay on the ERC-20 path."""
        for foreign in ("MATIC", "POL", "AVAX", "BNB", "MNT"):
            assert adapter._is_native_token(foreign) is False, foreign

    def test_optimism_is_eth_native(self, usdc_weth_resolver: MagicMock) -> None:
        cfg = AerodromeConfig(
            chain="optimism",
            wallet_address=TEST_WALLET,
            allow_placeholder_prices=True,
        )
        op_adapter = AerodromeAdapter(cfg, token_resolver=usdc_weth_resolver)
        assert op_adapter._is_native_token("ETH") is True
        assert op_adapter._is_native_token("MATIC") is False


class TestEncodeRoute:
    def test_encode_route_volatile(self, adapter: AerodromeAdapter) -> None:
        out = adapter._encode_route(USDC_ADDRESS, WETH_ADDRESS, stable=False)
        # 4 fields: 4 * 64 = 256 hex chars
        assert len(out) == 256

    def test_encode_route_stable(self, adapter: AerodromeAdapter) -> None:
        out = adapter._encode_route(USDC_ADDRESS, WETH_ADDRESS, stable=True)
        # Last bool word should end with 1
        # bool position: 64 (token_in) + 64 (token_out) = 128, then 64 chars for bool
        bool_word = out[128:192]
        assert bool_word.endswith("1")


# =============================================================================
# Build TX direct calls
# =============================================================================


class TestBuildTxFunctions:
    def test_build_swap_exact_input_tx_returns_transaction_data(self, adapter: AerodromeAdapter) -> None:
        tx = adapter._build_swap_exact_input_tx(
            token_in=USDC_ADDRESS,
            token_out=WETH_ADDRESS,
            stable=False,
            recipient=TEST_WALLET,
            amount_in=1_000_000,
            amount_out_minimum=10**14,
        )
        assert isinstance(tx, TransactionData)
        assert tx.to == adapter.addresses["router"]
        assert tx.tx_type == "swap"
        # Calldata starts with the swap selector
        assert tx.data.startswith("0xcac88ea9")

    @pytest.mark.parametrize("deployment", [CURRENT, LEGACY], ids=["current", "legacy"])
    def test_build_swap_exact_input_cl_tx(self, adapter: AerodromeAdapter, deployment: SlipstreamDeployment) -> None:
        assert deployment.swap_router is not None
        tx = adapter._build_swap_exact_input_cl_tx(
            token_in=USDC_ADDRESS,
            token_out=WETH_ADDRESS,
            tick_spacing=100,
            recipient=TEST_WALLET,
            amount_in=1_000_000,
            amount_out_minimum=10**14,
            router=deployment.swap_router,
        )
        assert tx.to == deployment.swap_router
        assert tx.data.startswith("0xa026383e")

    def test_build_swap_exact_input_cl_tx_requires_a_router(self, adapter: AerodromeAdapter) -> None:
        with pytest.raises(TypeError):
            adapter._build_swap_exact_input_cl_tx(  # type: ignore[call-arg]
                token_in=USDC_ADDRESS,
                token_out=WETH_ADDRESS,
                tick_spacing=100,
                recipient=TEST_WALLET,
                amount_in=1_000_000,
                amount_out_minimum=10**14,
            )

    def test_build_add_liquidity_tx(self, adapter: AerodromeAdapter) -> None:
        tx = adapter._build_add_liquidity_tx(
            token_a=USDC_ADDRESS,
            token_b=WETH_ADDRESS,
            stable=False,
            amount_a_desired=1_000_000,
            amount_b_desired=10**15,
            amount_a_min=0,
            amount_b_min=0,
            recipient=TEST_WALLET,
        )
        assert tx.to == adapter.addresses["router"]
        assert tx.tx_type == "add_liquidity"
        assert tx.data.startswith("0x5a47ddc3")
        assert "volatile" in tx.description

    def test_build_add_liquidity_tx_stable(self, adapter: AerodromeAdapter) -> None:
        tx = adapter._build_add_liquidity_tx(
            token_a=USDC_ADDRESS,
            token_b=WETH_ADDRESS,
            stable=True,
            amount_a_desired=1,
            amount_b_desired=1,
            amount_a_min=0,
            amount_b_min=0,
            recipient=TEST_WALLET,
        )
        assert "stable" in tx.description

    def test_build_remove_liquidity_tx(self, adapter: AerodromeAdapter) -> None:
        tx = adapter._build_remove_liquidity_tx(
            token_a=USDC_ADDRESS,
            token_b=WETH_ADDRESS,
            stable=False,
            liquidity=10**18,
            amount_a_min=0,
            amount_b_min=0,
            recipient=TEST_WALLET,
        )
        assert tx.tx_type == "remove_liquidity"
        assert tx.data.startswith("0x0dede6c4")


# =============================================================================
# Result dataclasses to_dict round-trips (covers `to_dict` branches)
# =============================================================================


class TestResultDataclassesToDict:
    def test_swap_result_to_dict_with_quote(self, adapter: AerodromeAdapter) -> None:
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10"),
            stable=False,
            deployment=CURRENT,
        )
        assert result.success
        d = result.to_dict()
        assert "transactions" in d
        assert "quote" in d
        assert d["quote"] is not None

    def test_swap_result_to_dict_failure(self) -> None:
        result = SwapResult(success=False, error="boom")
        d = result.to_dict()
        assert d["success"] is False
        assert d["quote"] is None

    def test_liquidity_result_to_dict(self) -> None:
        r = LiquidityResult(success=True, token_a="0xa", token_b="0xb", amount_a=10, amount_b=20, liquidity=5)
        d = r.to_dict()
        assert d["amount_a"] == "10"
        assert d["liquidity"] == "5"

    def test_transaction_data_to_dict(self) -> None:
        tx = TransactionData(to="0xa", value=10, data="0x00", gas_estimate=1000, description="x", tx_type="swap")
        d = tx.to_dict()
        assert d["value"] == "10"
        assert d["tx_type"] == "swap"


# =============================================================================
# ALM-3367 — LP legs must carry a real slippage floor
# =============================================================================

_ADD_LIQUIDITY_SELECTOR = "0x5a47ddc3"
_REMOVE_LIQUIDITY_SELECTOR = "0x0dede6c4"


def _decode_uints(tx_data: object, selector: str, *word_indexes: int) -> tuple[int, ...]:
    text = tx_data if isinstance(tx_data, str) else "0x" + bytes(tx_data).hex()
    if text[:10].lower() != selector:
        raise AssertionError(f"expected {selector}, got {text[:10]!r}")
    words = bytes.fromhex(text[10:])
    return tuple(int.from_bytes(words[i * 32 : (i + 1) * 32], "big") for i in word_indexes)


def _add_liquidity_minimums(result) -> tuple[int, int]:
    """Recover (amountAMin, amountBMin) from the emitted router calldata."""
    for tx in result.transactions:
        data = tx.data if hasattr(tx, "data") else tx["data"]
        text = data if isinstance(data, str) else "0x" + bytes(data).hex()
        if text[:10].lower() != _ADD_LIQUIDITY_SELECTOR:
            continue
        mins = _decode_uints(text, _ADD_LIQUIDITY_SELECTOR, 5, 6)
        return mins[0], mins[1]
    raise AssertionError("no addLiquidity() transaction in the result")


def _remove_liquidity_minimums(result) -> tuple[int, int]:
    """Recover (amountAMin, amountBMin) from the emitted removeLiquidity calldata."""
    for tx in result.transactions:
        data = tx.data if hasattr(tx, "data") else tx["data"]
        text = data if isinstance(data, str) else "0x" + bytes(data).hex()
        if text[:10].lower() != _REMOVE_LIQUIDITY_SELECTOR:
            continue
        mins = _decode_uints(text, _REMOVE_LIQUIDITY_SELECTOR, 4, 5)
        return mins[0], mins[1]
    raise AssertionError("no removeLiquidity() transaction in the result")


class TestLPSlippageFloor:
    def test_add_liquidity_floors_both_sides_against_the_router_quote(self, adapter: AerodromeAdapter) -> None:
        # Requested (100e6, 0.05e18); the pool rebalances to a strictly smaller quote.
        quoted_a, quoted_b = 80_000_000, 40_000_000_000_000_000
        adapter._quote_add_liquidity = lambda *_args: (quoted_a, quoted_b)  # type: ignore[method-assign]

        result = adapter.add_liquidity(
            token_a="USDC",
            token_b="WETH",
            amount_a=Decimal("100"),
            amount_b=Decimal("0.05"),
            stable=False,
            slippage_bps=100,
        )

        assert result.success
        amount_a_min, amount_b_min = _add_liquidity_minimums(result)
        assert amount_a_min == quoted_a * 9_900 // 10_000
        assert amount_b_min == quoted_b * 9_900 // 10_000
        assert amount_a_min != 100_000_000 * 9_900 // 10_000, "floor must be of the quote, not the request"
        assert amount_a_min > 0 and amount_b_min > 0

    def test_remove_liquidity_floors_both_sides_against_the_router_quote(self, adapter: AerodromeAdapter) -> None:
        quoted_a, quoted_b = 70_000_000, 30_000_000_000_000_000
        adapter._quote_remove_liquidity = lambda *_args: (quoted_a, quoted_b)  # type: ignore[method-assign]

        result = adapter.remove_liquidity(
            token_a="USDC",
            token_b="WETH",
            liquidity=Decimal("1"),
            stable=False,
            slippage_bps=100,
            pool_address="0x" + "cc" * 20,
        )

        assert result.success
        amount_a_min, amount_b_min = _remove_liquidity_minimums(result)
        assert amount_a_min == quoted_a * 9_900 // 10_000
        assert amount_b_min == quoted_b * 9_900 // 10_000
        assert amount_a_min != 10**18 * 9_900 // 10_000, "floor must be of the quote, not the LP wei"

    def test_add_liquidity_refuses_rather_than_submitting_zero_minimums(self, adapter: AerodromeAdapter) -> None:
        """Fail closed: no quote means no protection, and no transaction."""
        adapter._quote_add_liquidity = lambda *_args: None  # type: ignore[method-assign]

        result = adapter.add_liquidity(
            token_a="USDC",
            token_b="WETH",
            amount_a=Decimal("100"),
            amount_b=Decimal("0.05"),
            stable=False,
            slippage_bps=100,
        )

        assert not result.success
        assert result.transactions == []
        assert "unfloored mint" in (result.error or "")

    def test_remove_liquidity_refuses_rather_than_submitting_zero_minimums(self, adapter: AerodromeAdapter) -> None:
        adapter._quote_remove_liquidity = lambda *_args: None  # type: ignore[method-assign]

        result = adapter.remove_liquidity(
            token_a="USDC",
            token_b="WETH",
            liquidity=Decimal("1"),
            stable=False,
            slippage_bps=100,
        )

        assert not result.success
        assert "unfloored burn" in (result.error or "")

    @pytest.mark.parametrize("bad_bps", [-1, 10_000, 20_000])
    def test_out_of_range_slippage_is_refused_not_clamped(self, adapter: AerodromeAdapter, bad_bps: int) -> None:
        """A cap at or beyond 100% floors at zero for small amounts if clamped."""
        result = adapter.add_liquidity(
            token_a="USDC",
            token_b="WETH",
            amount_a=Decimal("100"),
            amount_b=Decimal("0.05"),
            stable=False,
            slippage_bps=bad_bps,
        )

        assert not result.success
        assert "slippage_bps must be within" in (result.error or "")

    def test_floored_pair_uses_the_canonical_bps_floor(self) -> None:
        from almanak.connectors.aerodrome.adapter import _floored_pair

        assert _floored_pair((1_000, 1_000), 100) == (990, 990)
        assert _floored_pair((1_000, 1_000), 0) == (1_000, 1_000)
        assert _floored_pair((3, 3), 100) == (2, 2)
        with pytest.raises(ValueError, match="slippage_bps must be in"):
            _floored_pair((1_000, 1_000), 10_000)


class TestLPFloorPostcondition:
    """A quote is not enough: the encoded floor must itself be positive."""

    @pytest.mark.parametrize(
        ("quote", "label"),
        [
            ((0, 0), "router returns (0,0) for a missing pool"),
            ((0, 10**18), "one side quotes zero"),
            ((1, 1), "quote too small to floor — 1 * 9900 // 10000 == 0"),
        ],
    )
    def test_add_liquidity_refuses_a_quote_that_cannot_produce_a_positive_floor(
        self, adapter: AerodromeAdapter, quote: tuple[int, int], label: str
    ) -> None:
        adapter._quote_add_liquidity = lambda *_args: quote  # type: ignore[method-assign]

        result = adapter.add_liquidity(
            token_a="USDC",
            token_b="WETH",
            amount_a=Decimal("100"),
            amount_b=Decimal("0.05"),
            stable=False,
            slippage_bps=100,
        )

        assert not result.success, label
        assert result.transactions == []
        assert "unfloored mint" in (result.error or "")

    @pytest.mark.parametrize("quote", [(0, 0), (10**18, 0), (1, 1)])
    def test_remove_liquidity_refuses_a_quote_that_cannot_produce_a_positive_floor(
        self, adapter: AerodromeAdapter, quote: tuple[int, int]
    ) -> None:
        adapter._quote_remove_liquidity = lambda *_args: quote  # type: ignore[method-assign]

        result = adapter.remove_liquidity(
            token_a="USDC",
            token_b="WETH",
            liquidity=Decimal("1"),
            stable=False,
            slippage_bps=100,
        )

        assert not result.success
        assert "unfloored burn" in (result.error or "")

    def test_floored_pair_never_returns_a_zero_bound(self) -> None:
        from almanak.connectors.aerodrome.adapter import _floored_pair

        assert _floored_pair((10**18, 10**6), 100) == (99 * 10**16, 990_000)
        assert _floored_pair(None, 100) is None
        assert _floored_pair((0, 0), 100) is None
        assert _floored_pair((1, 10**18), 100) is None, "a floor of zero is not a floor"


class TestRouterQuoteHelpers:
    """Direct cover for the SDK quote helpers the LP floors are derived from.

    A wrong router method name, a transposed ABI argument, or a swallowed
    exception here yields a plausible-but-wrong floor rather than an obvious
    failure — the floor would still be non-zero and would still pass every
    presence check.
    """

    @staticmethod
    def _sdk_with_fake_web3(returns):
        from almanak.connectors.aerodrome.sdk import AerodromeSDK

        calls: dict = {}

        class _Fn:
            def __init__(self, name):
                self._name = name

            def __call__(self, *args):
                calls["method"] = self._name
                calls["args"] = args
                return self

            def call(self):
                if isinstance(returns, Exception):
                    raise returns
                return returns

        class _Functions:
            def __getattr__(self, name):
                return _Fn(name)

        class _Contract:
            functions = _Functions()

        class _Eth:
            @staticmethod
            def contract(address=None, abi=None):
                calls["address"] = address
                return _Contract()

        class _Web3:
            eth = _Eth()

            @staticmethod
            def to_checksum_address(a):
                return Web3.to_checksum_address(a)

        sdk = AerodromeSDK.__new__(AerodromeSDK)
        sdk.addresses = {"router": "0x" + "77" * 20, "factory": "0x" + "88" * 20}
        sdk._router_abi = []
        return sdk, _Web3, calls

    def test_quote_add_liquidity_calls_the_router_with_the_documented_argument_order(self) -> None:
        sdk, web3, calls = self._sdk_with_fake_web3((111, 222, 333))

        result = sdk.quote_add_liquidity(USDC_ADDRESS, WETH_ADDRESS, False, 10, 20, web3)

        assert result == (111, 222), "must drop the liquidity return and keep (amountA, amountB)"
        assert calls["method"] == "quoteAddLiquidity"
        assert calls["address"] == Web3.to_checksum_address("0x" + "77" * 20), "must query the router"
        token_a, token_b, stable, factory, desired_a, desired_b = calls["args"]
        assert (token_a, token_b) == (
            Web3.to_checksum_address(USDC_ADDRESS),
            Web3.to_checksum_address(WETH_ADDRESS),
        )
        assert stable is False
        assert factory == Web3.to_checksum_address("0x" + "88" * 20)
        assert (desired_a, desired_b) == (10, 20), "desired amounts must not be transposed"

    def test_quote_remove_liquidity_calls_the_router_with_the_documented_argument_order(self) -> None:
        sdk, web3, calls = self._sdk_with_fake_web3((444, 555))

        result = sdk.quote_remove_liquidity(USDC_ADDRESS, WETH_ADDRESS, True, 99, web3)

        assert result == (444, 555)
        assert calls["method"] == "quoteRemoveLiquidity"
        token_a, token_b, stable, factory, liquidity = calls["args"]
        assert stable is True
        assert liquidity == 99
        assert factory == Web3.to_checksum_address("0x" + "88" * 20)

    def test_quotes_return_integers_not_the_router_types(self) -> None:
        # quoteAddLiquidity returns (amountA, amountB, liquidity) — three values.
        sdk, web3, _ = self._sdk_with_fake_web3((True, 2, 3))
        quoted = sdk.quote_add_liquidity(USDC_ADDRESS, WETH_ADDRESS, False, 1, 1, web3)

        assert quoted is not None
        assert [type(v) for v in quoted] == [int, int]

    @pytest.mark.parametrize("method", ["quote_add_liquidity", "quote_remove_liquidity"])
    def test_a_reverting_quote_becomes_None_so_the_caller_fails_closed(self, method: str) -> None:
        sdk, web3, _ = self._sdk_with_fake_web3(RuntimeError("execution reverted"))
        args = (
            (USDC_ADDRESS, WETH_ADDRESS, False, 1, 1, web3)
            if method == "quote_add_liquidity"
            else (USDC_ADDRESS, WETH_ADDRESS, False, 1, web3)
        )

        assert getattr(sdk, method)(*args) is None

    def test_a_zero_zero_quote_is_forwarded_not_collapsed_to_None(self) -> None:
        # Adapter _floored_pair is what refuses (0, 0); the SDK must not hide it.
        sdk, web3, _ = self._sdk_with_fake_web3((0, 0, 0))
        assert sdk.quote_add_liquidity(USDC_ADDRESS, WETH_ADDRESS, False, 1, 1, web3) == (0, 0)

        sdk, web3, _ = self._sdk_with_fake_web3((0, 0))
        assert sdk.quote_remove_liquidity(USDC_ADDRESS, WETH_ADDRESS, False, 1, web3) == (0, 0)


@pytest.mark.parametrize(
    "swap_params",
    [
        {"pool": "0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59"},
        {"tick_spacing": 100},
        {"classic": False},
    ],
)
def test_compile_swap_intent_refuses_a_cl_routing_request(adapter: AerodromeAdapter, swap_params: dict) -> None:
    """The direct-adapter helper is Classic-only; a CL request must fail loudly, never route Classic silently."""
    intent = SwapIntent(
        from_token="USDC",
        to_token="WETH",
        amount=Decimal("100"),
        protocol="aerodrome",
        chain="base",
        swap_params=swap_params,
    )
    with pytest.raises(ValueError, match="Classic only"):
        adapter.compile_swap_intent(intent)
