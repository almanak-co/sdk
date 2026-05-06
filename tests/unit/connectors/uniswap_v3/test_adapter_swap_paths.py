"""Branch coverage for ``UniswapV3Adapter`` swap execution paths.

Targets the uncovered swap_exact_input / swap_exact_output / compile_swap_intent
/ approve / quote / native-token / V1-router / placeholder branches in
``adapter.py``. The existing resolver tests cover initialization and token
plumbing; these tests cover the build-and-return swap pipeline end-to-end
with a mocked TokenResolver (no network).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.uniswap_v3.adapter import (
    DEFAULT_DEADLINE_SECONDS,
    EXACT_INPUT_SINGLE_SELECTOR,
    EXACT_INPUT_SINGLE_V1_SELECTOR,
    EXACT_OUTPUT_SINGLE_SELECTOR,
    EXACT_OUTPUT_SINGLE_V1_SELECTOR,
    ERC20_APPROVE_SELECTOR,
    SwapQuote,
    SwapResult,
    SwapType,
    TransactionData,
    UniswapV3Adapter,
    UniswapV3Config,
)
from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.data.tokens.models import ResolvedToken
from almanak.framework.intents.vocabulary import IntentType, SwapIntent

WALLET = "0x1234567890abcdef1234567890abcdef12345678"
USDC = ResolvedToken(
    symbol="USDC",
    address="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
    decimals=6,
    chain="arbitrum",
    chain_id=42161,
)
WETH = ResolvedToken(
    symbol="WETH",
    address="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
    decimals=18,
    chain="arbitrum",
    chain_id=42161,
)
ETH = ResolvedToken(
    symbol="ETH",
    address="0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    decimals=18,
    chain="arbitrum",
    chain_id=42161,
)


def _make_resolver() -> MagicMock:
    """Resolver that maps standard symbols/addresses to a small known set."""
    resolver = MagicMock()
    table = {
        "USDC": USDC,
        "WETH": WETH,
        "ETH": ETH,
        USDC.address: USDC,
        USDC.address.lower(): USDC,
        WETH.address: WETH,
        WETH.address.lower(): WETH,
        ETH.address: ETH,
        ETH.address.lower(): ETH,
    }

    def resolve(token: str, _chain: str, **_kwargs: object) -> ResolvedToken:
        key = token if token in table else token.lower()
        if key in table:
            return table[key]
        raise TokenResolutionError(token=token, chain="arbitrum", reason="not in table")

    resolver.resolve = MagicMock(side_effect=resolve)
    resolver.resolve_for_swap = MagicMock(return_value=WETH)
    return resolver


@pytest.fixture
def config() -> UniswapV3Config:
    return UniswapV3Config(
        chain="arbitrum",
        wallet_address=WALLET,
        price_provider={"USDC": Decimal("1"), "WETH": Decimal("3000")},
    )


@pytest.fixture
def placeholder_config() -> UniswapV3Config:
    return UniswapV3Config(
        chain="arbitrum",
        wallet_address=WALLET,
        allow_placeholder_prices=True,
    )


@pytest.fixture
def adapter(config: UniswapV3Config) -> UniswapV3Adapter:
    return UniswapV3Adapter(config, token_resolver=_make_resolver())


@pytest.fixture
def adapter_placeholder(placeholder_config: UniswapV3Config) -> UniswapV3Adapter:
    return UniswapV3Adapter(placeholder_config, token_resolver=_make_resolver())


# ---------------------------------------------------------------------------
# UniswapV3Config validation
# ---------------------------------------------------------------------------


class TestUniswapV3ConfigValidation:
    def test_unsupported_chain_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unsupported chain"):
            UniswapV3Config(
                chain="bitcoin",
                wallet_address=WALLET,
                allow_placeholder_prices=True,
            )

    def test_invalid_slippage_rejected(self) -> None:
        with pytest.raises(ValueError, match="Slippage"):
            UniswapV3Config(
                chain="arbitrum",
                wallet_address=WALLET,
                allow_placeholder_prices=True,
                default_slippage_bps=20_000,
            )

    def test_negative_slippage_rejected(self) -> None:
        with pytest.raises(ValueError, match="Slippage"):
            UniswapV3Config(
                chain="arbitrum",
                wallet_address=WALLET,
                allow_placeholder_prices=True,
                default_slippage_bps=-1,
            )

    def test_invalid_fee_tier_rejected(self) -> None:
        with pytest.raises(ValueError, match="fee tier"):
            UniswapV3Config(
                chain="arbitrum",
                wallet_address=WALLET,
                allow_placeholder_prices=True,
                default_fee_tier=42,
            )

    def test_missing_price_provider_rejected_by_default(self) -> None:
        with pytest.raises(ValueError, match="price_provider"):
            UniswapV3Config(chain="arbitrum", wallet_address=WALLET)

    def test_to_dict_includes_all_runtime_fields(self) -> None:
        cfg = UniswapV3Config(
            chain="arbitrum",
            wallet_address=WALLET,
            allow_placeholder_prices=True,
            default_slippage_bps=100,
        )
        d = cfg.to_dict()
        assert d["chain"] == "arbitrum"
        assert d["wallet_address"] == WALLET
        assert d["default_slippage_bps"] == 100


# ---------------------------------------------------------------------------
# Dataclass to_dict round-trips for SwapQuote / SwapResult / TransactionData
# ---------------------------------------------------------------------------


class TestAdapterDataclassDict:
    def test_swap_quote_to_dict(self) -> None:
        q = SwapQuote(
            token_in=USDC.address,
            token_out=WETH.address,
            amount_in=10**8,
            amount_out=10**16,
            fee_tier=500,
            sqrt_price_x96_after=42,
            gas_estimate=160_000,
            price_impact_bps=15,
            effective_price=Decimal("0.0001"),
        )
        d = q.to_dict()
        assert d["fee_tier"] == 500
        assert d["sqrt_price_x96_after"] == "42"
        assert d["price_impact_bps"] == 15

    def test_swap_result_to_dict(self) -> None:
        tx = TransactionData(
            to=USDC.address,
            value=0,
            data="0xdead",
            gas_estimate=46_000,
            description="approve",
            tx_type="approve",
        )
        result = SwapResult(success=True, transactions=[tx], amount_in=100, amount_out_minimum=99)
        d = result.to_dict()
        assert d["success"] is True
        assert len(d["transactions"]) == 1
        assert d["transactions"][0]["tx_type"] == "approve"
        assert d["amount_in"] == "100"

    def test_transaction_data_to_dict(self) -> None:
        tx = TransactionData(
            to=USDC.address,
            value=10**17,
            data="0xdead",
            gas_estimate=46_000,
            description="swap",
        )
        d = tx.to_dict()
        assert d["to"] == USDC.address
        assert d["value"] == str(10**17)
        assert d["data"] == "0xdead"
        assert d["tx_type"] == "swap"


class TestSwapType:
    def test_enum_values(self) -> None:
        assert SwapType.EXACT_INPUT.value == "EXACT_INPUT"
        assert SwapType.EXACT_OUTPUT.value == "EXACT_OUTPUT"


# ---------------------------------------------------------------------------
# swap_exact_input
# ---------------------------------------------------------------------------


class TestSwapExactInput:
    def test_builds_approve_and_swap(self, adapter: UniswapV3Adapter) -> None:
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("100"),
        )
        assert result.success is True
        assert len(result.transactions) == 2  # approve + swap
        assert result.transactions[0].tx_type == "approve"
        assert result.transactions[1].tx_type == "swap"
        assert result.transactions[1].data.startswith(EXACT_INPUT_SINGLE_SELECTOR)
        assert result.amount_in == 100 * 10**6
        assert result.amount_out_minimum > 0

    def test_native_token_skips_approve(self, adapter: UniswapV3Adapter) -> None:
        """Native ETH input wraps via msg.value — no approve transaction."""
        result = adapter.swap_exact_input(
            token_in="ETH",
            token_out="USDC",
            amount_in=Decimal("1"),
        )
        assert result.success is True
        assert len(result.transactions) == 1
        assert result.transactions[0].tx_type == "swap"
        assert result.transactions[0].value == 10**18

    def test_unknown_token_returns_error(self, adapter: UniswapV3Adapter) -> None:
        result = adapter.swap_exact_input(
            token_in="UNKNOWN_XYZ",
            token_out="WETH",
            amount_in=Decimal("100"),
        )
        assert result.success is False
        assert result.error is not None

    def test_explicit_overrides_used(self, adapter: UniswapV3Adapter) -> None:
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("10"),
            slippage_bps=200,
            fee_tier=500,
            recipient="0x" + "ab" * 20,
        )
        assert result.success is True

    def test_cached_allowance_skips_approve(self, adapter: UniswapV3Adapter) -> None:
        """When set_allowance has been called for >= the spend, no approve tx."""
        adapter.set_allowance(USDC.address, adapter.addresses["swap_router"], 10**18)
        result = adapter.swap_exact_input(
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1"),
        )
        # Still has approve transaction OR not — depends on cache hit
        # The cached path returns None and only the swap is included
        assert result.success is True
        assert all(tx.tx_type == "swap" for tx in result.transactions)


# ---------------------------------------------------------------------------
# swap_exact_output
# ---------------------------------------------------------------------------


class TestSwapExactOutput:
    def test_builds_approve_and_swap(self, adapter: UniswapV3Adapter) -> None:
        result = adapter.swap_exact_output(
            token_in="USDC",
            token_out="WETH",
            amount_out=Decimal("0.1"),
        )
        assert result.success is True
        assert len(result.transactions) == 2
        assert result.transactions[1].data.startswith(EXACT_OUTPUT_SINGLE_SELECTOR)

    def test_unknown_input_token_returns_error(self, adapter: UniswapV3Adapter) -> None:
        result = adapter.swap_exact_output(
            token_in="UNKNOWN_XYZ",
            token_out="WETH",
            amount_out=Decimal("0.1"),
        )
        assert result.success is False

    def test_unknown_output_token_returns_error(self, adapter: UniswapV3Adapter) -> None:
        result = adapter.swap_exact_output(
            token_in="USDC",
            token_out="UNKNOWN_XYZ",
            amount_out=Decimal("0.1"),
        )
        assert result.success is False

    def test_native_input_skips_approve(self, adapter: UniswapV3Adapter) -> None:
        result = adapter.swap_exact_output(
            token_in="ETH",
            token_out="USDC",
            amount_out=Decimal("100"),
        )
        assert result.success is True
        assert all(tx.tx_type == "swap" for tx in result.transactions)
        assert result.transactions[0].value > 0


# ---------------------------------------------------------------------------
# Quote helpers (placeholder price oracle path)
# ---------------------------------------------------------------------------


class TestQuoteHelpers:
    def test_quote_exact_input_with_known_prices(self, adapter: UniswapV3Adapter) -> None:
        quote = adapter._get_quote_exact_input(
            token_in=USDC.address,
            token_out=WETH.address,
            amount_in=100 * 10**6,
            fee_tier=3000,
        )
        # Should produce a non-zero output
        assert quote.amount_out > 0
        assert quote.fee_tier == 3000

    def test_quote_exact_output_with_known_prices(self, adapter: UniswapV3Adapter) -> None:
        quote = adapter._get_quote_exact_output(
            token_in=USDC.address,
            token_out=WETH.address,
            amount_out=10**16,
            fee_tier=3000,
        )
        assert quote.amount_in > 0

    def test_quote_exact_input_zero_price_out_falls_back(
        self, adapter_placeholder: UniswapV3Adapter
    ) -> None:
        """When price oracle returns 0 for token_out, the helper coerces to 1
        to avoid division by zero."""
        # Placeholder oracle returns Decimal('1') by default for known symbols
        # but unknown symbols give Decimal('1') via .get default. Test with
        # explicit zero in the price provider.
        adapter_placeholder._price_provider = {"USDC": Decimal("0"), "WETH": Decimal("0")}
        quote = adapter_placeholder._get_quote_exact_input(
            token_in=USDC.address,
            token_out=WETH.address,
            amount_in=100 * 10**6,
            fee_tier=3000,
        )
        # Should still produce a quote without dividing by zero
        assert quote.amount_out >= 0

    def test_quote_exact_output_zero_price_in_falls_back(
        self, adapter_placeholder: UniswapV3Adapter
    ) -> None:
        adapter_placeholder._price_provider = {"USDC": Decimal("0")}
        quote = adapter_placeholder._get_quote_exact_output(
            token_in=USDC.address,
            token_out=WETH.address,
            amount_out=10**16,
            fee_tier=3000,
        )
        assert quote.amount_in >= 0


# ---------------------------------------------------------------------------
# compile_swap_intent
# ---------------------------------------------------------------------------


class TestCompileSwapIntent:
    def test_compile_with_token_amount(self, adapter: UniswapV3Adapter) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
        )
        bundle = adapter.compile_swap_intent(intent)
        assert bundle.intent_type == IntentType.SWAP.value
        assert len(bundle.transactions) >= 1
        assert bundle.metadata["from_token"] == "USDC"
        assert bundle.metadata["to_token"] == "WETH"
        assert "router" in bundle.metadata

    def test_compile_with_amount_usd(self, adapter: UniswapV3Adapter) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
        )
        bundle = adapter.compile_swap_intent(intent)
        assert bundle.intent_type == IntentType.SWAP.value
        assert bundle.metadata["from_token"] == "USDC"

    def test_compile_with_external_price_oracle(self, adapter: UniswapV3Adapter) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
        )
        bundle = adapter.compile_swap_intent(
            intent,
            price_oracle={"USDC": Decimal("1"), "WETH": Decimal("3500")},
        )
        assert bundle.metadata["from_token"] == "USDC"

    def test_compile_amount_all_raises(self, adapter: UniswapV3Adapter) -> None:
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount="all",
        )
        with pytest.raises(ValueError, match="amount='all'"):
            adapter.compile_swap_intent(intent)

    def test_compile_missing_price_raises(self, adapter: UniswapV3Adapter) -> None:
        from almanak.framework.market import PriceUnavailableError

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("100"),
        )
        with pytest.raises(PriceUnavailableError):
            adapter.compile_swap_intent(intent, price_oracle={})

    def test_compile_failed_swap_returns_empty_bundle_with_error(
        self, adapter: UniswapV3Adapter
    ) -> None:
        # Bad token resolution → swap_exact_input returns failure → empty bundle
        adapter._token_resolver.resolve = MagicMock(
            side_effect=TokenResolutionError(token="X", chain="arbitrum", reason="nope")
        )
        intent = SwapIntent(
            from_token="X",
            to_token="WETH",
            amount=Decimal("1"),
        )
        bundle = adapter.compile_swap_intent(intent, price_oracle={"X": Decimal("1")})
        assert bundle.transactions == []
        assert bundle.metadata.get("error") is not None


# ---------------------------------------------------------------------------
# Build helpers — V1 vs V2 router selection
# ---------------------------------------------------------------------------


class TestV1RouterSelection:
    def test_v1_router_arbitrum_false(self, adapter: UniswapV3Adapter) -> None:
        assert adapter._uses_v1_router() is False

    def test_v1_router_zerog_true(self) -> None:
        adapter = UniswapV3Adapter(
            UniswapV3Config(
                chain="zerog",
                wallet_address=WALLET,
                allow_placeholder_prices=True,
            ),
            token_resolver=_make_resolver(),
        )
        assert adapter._uses_v1_router() is True

    def test_build_exact_input_uses_v1_selector_when_required(self) -> None:
        adapter = UniswapV3Adapter(
            UniswapV3Config(
                chain="zerog",
                wallet_address=WALLET,
                allow_placeholder_prices=True,
            ),
            token_resolver=_make_resolver(),
        )
        tx = adapter._build_exact_input_single_tx(
            token_in=USDC.address,
            token_out=WETH.address,
            fee=3000,
            recipient=WALLET,
            amount_in=10**6,
            amount_out_minimum=10**14,
            value=0,
        )
        assert tx.data.startswith(EXACT_INPUT_SINGLE_V1_SELECTOR)

    def test_build_exact_output_uses_v1_selector_when_required(self) -> None:
        adapter = UniswapV3Adapter(
            UniswapV3Config(
                chain="zerog",
                wallet_address=WALLET,
                allow_placeholder_prices=True,
            ),
            token_resolver=_make_resolver(),
        )
        tx = adapter._build_exact_output_single_tx(
            token_in=USDC.address,
            token_out=WETH.address,
            fee=3000,
            recipient=WALLET,
            amount_out=10**14,
            amount_in_maximum=10**6,
            value=0,
        )
        assert tx.data.startswith(EXACT_OUTPUT_SINGLE_V1_SELECTOR)


# ---------------------------------------------------------------------------
# Approve helpers and allowance cache
# ---------------------------------------------------------------------------


class TestApproveAndAllowance:
    def test_build_approve_tx_returns_approve(self, adapter: UniswapV3Adapter) -> None:
        tx = adapter._build_approve_tx(USDC.address, "0x" + "11" * 20, 1000)
        assert tx is not None
        assert tx.tx_type == "approve"
        assert tx.data.startswith(ERC20_APPROVE_SELECTOR)

    def test_build_approve_returns_none_when_cached(self, adapter: UniswapV3Adapter) -> None:
        spender = "0x" + "11" * 20
        adapter.set_allowance(USDC.address, spender, 10**30)
        tx = adapter._build_approve_tx(USDC.address, spender, 100)
        assert tx is None

    def test_clear_allowance_cache(self, adapter: UniswapV3Adapter) -> None:
        adapter.set_allowance(USDC.address, "0xabc", 100)
        assert adapter._allowance_cache != {}
        adapter.clear_allowance_cache()
        assert adapter._allowance_cache == {}


# ---------------------------------------------------------------------------
# _is_native_token / _resolve_token / _get_token_symbol
# ---------------------------------------------------------------------------


class TestTokenHelpers:
    def test_get_token_symbol_address_resolved(self, adapter: UniswapV3Adapter) -> None:
        sym = adapter._get_token_symbol(USDC.address)
        assert sym == "USDC"

    def test_get_token_symbol_returns_truncated_on_failure(
        self, adapter: UniswapV3Adapter
    ) -> None:
        adapter._token_resolver.resolve = MagicMock(
            side_effect=TokenResolutionError(token="x", chain="arbitrum", reason="nope")
        )
        out = adapter._get_token_symbol("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
        assert "..." in out

    def test_get_token_symbol_passthrough_for_non_address(
        self, adapter: UniswapV3Adapter
    ) -> None:
        # A bare symbol (no 0x prefix) gets returned as-is
        assert adapter._get_token_symbol("USDC") == "USDC"

    def test_is_native_token_native_placeholder(self, adapter: UniswapV3Adapter) -> None:
        assert adapter._is_native_token("0xEEEEEEeeEeEeeEEEEEeeEEEEEeEEeEEeEeEEEeeE") is True

    def test_default_price_oracle_returns_provider(self, adapter: UniswapV3Adapter) -> None:
        out = adapter._get_default_price_oracle()
        assert out == adapter._price_provider


# ---------------------------------------------------------------------------
# Pad helpers (deterministic encoding)
# ---------------------------------------------------------------------------


class TestPadHelpers:
    def test_pad_address(self) -> None:
        out = UniswapV3Adapter._pad_address("0xabcD")
        assert len(out) == 64
        assert out.endswith("abcd")
        # Lowercased and zero-padded
        assert out.startswith("0" * 60)

    def test_pad_uint256(self) -> None:
        out = UniswapV3Adapter._pad_uint256(255)
        assert len(out) == 64
        assert int(out, 16) == 255

    def test_pad_uint24(self) -> None:
        out = UniswapV3Adapter._pad_uint24(3000)
        assert len(out) == 64
        assert int(out, 16) == 3000


# ---------------------------------------------------------------------------
# Constants reflection
# ---------------------------------------------------------------------------


class TestAdapterConstants:
    def test_default_deadline_seconds(self) -> None:
        # Module-level export — should be a positive integer
        assert DEFAULT_DEADLINE_SECONDS > 0
