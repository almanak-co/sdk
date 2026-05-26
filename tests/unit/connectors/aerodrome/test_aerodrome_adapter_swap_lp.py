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
from unittest.mock import MagicMock

import pytest

from almanak.connectors.aerodrome.adapter import (
    AerodromeAdapter,
    AerodromeConfig,
    LiquidityResult,
    SwapResult,
    TransactionData,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken
from almanak.framework.intents.vocabulary import IntentType, SwapIntent

TEST_WALLET = "0x1234567890123456789012345678901234567890"
USDC_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
WETH_ADDRESS = "0x4200000000000000000000000000000000000006"
NATIVE_ETH = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"


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
    return AerodromeAdapter(cfg, token_resolver=usdc_weth_resolver)


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
        )
        assert result.success
        # approve + swap
        assert len(result.transactions) == 2
        assert result.transactions[0].tx_type == "approve"
        assert result.transactions[1].tx_type == "swap"
        # swap is to cl_router
        assert result.transactions[1].to == adapter.addresses["cl_router"]
        # amount_in_wei is 100 * 10^6 = 100_000_000
        assert result.amount_in == 100_000_000

    def test_cl_swap_unknown_input_token_returns_error(self, usdc_weth_resolver: MagicMock) -> None:
        # token_in resolution will raise — adapter wraps it in SwapResult error.
        cfg = AerodromeConfig(chain="base", wallet_address=TEST_WALLET, allow_placeholder_prices=True)
        adapter = AerodromeAdapter(cfg, token_resolver=usdc_weth_resolver)
        result = adapter.swap_exact_input(
            token_in="UNKNOWN_TOKEN",
            token_out="WETH",
            amount_in=Decimal("1"),
        )
        assert result.success is False
        # Either "Unknown input token" or TokenResolutionError reason
        assert result.error is not None

    def test_swap_exception_caught_returns_failed_result(self, adapter: AerodromeAdapter) -> None:
        # Force exception in _get_quote_exact_input.
        adapter._get_quote_exact_input = MagicMock(side_effect=RuntimeError("boom"))  # type: ignore[method-assign]
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1"),
        )
        assert result.success is False
        assert "boom" in (result.error or "")


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
        )
        assert result.success
        # No approve tx — only swap
        approves = [tx for tx in result.transactions if tx.tx_type == "approve"]
        assert approves == []


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

    def test_pad_uint256_zero(self) -> None:
        out = AerodromeAdapter._pad_uint256(0)
        assert out == "0" * 64

    def test_pad_uint256_value(self) -> None:
        out = AerodromeAdapter._pad_uint256(255)
        assert out == "0" * 62 + "ff"

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

    def test_build_swap_exact_input_cl_tx(self, adapter: AerodromeAdapter) -> None:
        tx = adapter._build_swap_exact_input_cl_tx(
            token_in=USDC_ADDRESS,
            token_out=WETH_ADDRESS,
            tick_spacing=100,
            recipient=TEST_WALLET,
            amount_in=1_000_000,
            amount_out_minimum=10**14,
        )
        assert tx.to == adapter.addresses["cl_router"]
        assert tx.data.startswith("0xa026383e")

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
