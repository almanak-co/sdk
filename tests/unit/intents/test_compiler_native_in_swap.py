"""Unit tests for native-in swap compilation (VIB-3135).

Regression guards for the following bug:

  When swapping native token in (e.g. ``POL -> USDC`` on Polygon), the intent
  compiler previously issued an unconditional ERC20 ``allowance()`` call
  against the Polygon native precompile address
  ``0x0000000000000000000000000000000000001010`` because POL was not marked as
  ``is_native`` in the token registry. Native tokens have no ERC20 allowance,
  so the query failed and the approve transaction was incorrectly emitted.

The compiler now:

* marks POL on Polygon as ``is_native=True`` via ``chain_overrides`` in
  ``tokens.json``; AND
* defensively cross-checks ``NATIVE_TOKEN_SYMBOLS`` in ``_resolve_token`` so
  any chain gas token whose registry entry drifts from the shared sentinel
  address is still treated as native.

Both halves of the fix are exercised here.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak import IntentCompiler, IntentCompilerConfig, SwapIntent
from almanak.framework.data.tokens import TokenResolver


@pytest.fixture(autouse=True)
def _reset_token_resolver() -> None:
    """Ensure each test starts with a clean resolver singleton."""
    TokenResolver.reset_instance()
    yield
    TokenResolver.reset_instance()


@pytest.fixture()
def config() -> IntentCompilerConfig:
    """Placeholder-price config keeps the compile path offline (no oracle)."""
    return IntentCompilerConfig(allow_placeholder_prices=True)


# ---------------------------------------------------------------------------
# Native-token detection — regression guard for the underlying data bug
# ---------------------------------------------------------------------------


class TestNativeTokenDetection:
    """``_resolve_token`` must mark the chain's native gas token as native."""

    def test_pol_on_polygon_is_native(self, config: IntentCompilerConfig) -> None:
        """POL is Polygon's current native symbol (post-MATIC rebrand)."""
        compiler = IntentCompiler(chain="polygon", config=config)
        info = compiler._resolve_token("POL")
        assert info is not None
        assert info.is_native is True, (
            "POL on Polygon must resolve with is_native=True; otherwise the "
            "swap compiler builds an ERC20 approve against the precompile."
        )

    def test_matic_on_polygon_is_native(self, config: IntentCompilerConfig) -> None:
        """MATIC (legacy symbol) must still resolve as native on Polygon."""
        compiler = IntentCompiler(chain="polygon", config=config)
        info = compiler._resolve_token("MATIC")
        assert info is not None
        assert info.is_native is True

    def test_avax_on_avalanche_is_native(self, config: IntentCompilerConfig) -> None:
        compiler = IntentCompiler(chain="avalanche", config=config)
        info = compiler._resolve_token("AVAX")
        assert info is not None
        assert info.is_native is True

    def test_eth_on_ethereum_is_native(self, config: IntentCompilerConfig) -> None:
        compiler = IntentCompiler(chain="ethereum", config=config)
        info = compiler._resolve_token("ETH")
        assert info is not None
        assert info.is_native is True

    def test_usdc_on_polygon_is_not_native(self, config: IntentCompilerConfig) -> None:
        """Regression guard: ERC20s must NOT be coerced to native."""
        compiler = IntentCompiler(chain="polygon", config=config)
        info = compiler._resolve_token("USDC")
        assert info is not None
        assert info.is_native is False

    def test_bnb_alias_resolves_native_on_bsc(self, config: IntentCompilerConfig) -> None:
        """Chain alias (``bnb`` -> ``bsc``) must still hit the native-symbol table.

        Regression guard for the alias-normalization fix: if a caller passes
        the legacy ``bnb`` chain name instead of canonical ``bsc``, the
        ``_CHAIN_NATIVE_SYMBOLS`` lookup must still mark BNB as native.
        Without normalization, the table miss would incorrectly fall through
        to the ERC20 (allowance/approve) path.
        """
        compiler = IntentCompiler(chain="bnb", config=config)
        info = compiler._resolve_token("BNB")
        assert info is not None
        assert info.is_native is True

    def test_raw_address_input_does_not_flip_is_native(
        self, config: IntentCompilerConfig
    ) -> None:
        """Address-form lookups must trust the resolver verbatim.

        Regression guard (CodeRabbit, VIB-3135 follow-up): the
        ``_CHAIN_NATIVE_SYMBOLS`` symbol-table override must NOT fire when
        the caller passes a raw 0x-prefixed address. Otherwise a custom
        ERC20 deployed at an arbitrary address that happens to share a
        native ticker (e.g. a wrapper symbolised "POL") would be coerced
        to ``is_native=True`` and skip the ERC20 approve, breaking real
        ERC20 swaps.

        We exercise this with a USDC address — the resolver returns
        ``symbol="USDC"`` and ``is_native=False``; the override (if it
        ever flipped on a non-native symbol) would surface as an
        ``is_native=True`` regression here.
        """
        compiler = IntentCompiler(chain="polygon", config=config)
        # Polygon USDC (native, not bridged). Raw address form on purpose.
        usdc_polygon_address = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"
        info = compiler._resolve_token(usdc_polygon_address)
        assert info is not None
        assert info.is_native is False, (
            "Address-form lookup must trust resolver.is_native verbatim — "
            "the symbol-table override must only fire for symbol-form inputs."
        )


# ---------------------------------------------------------------------------
# Native-in swap compiles to a single value-bearing swap (no approve tx)
# ---------------------------------------------------------------------------


def _approve_txs(compiled) -> list:  # noqa: ANN001 - test helper
    return [tx for tx in (compiled.transactions or []) if tx.tx_type.startswith("approve")]


def _swap_txs(compiled) -> list:  # noqa: ANN001 - test helper
    return [tx for tx in (compiled.transactions or []) if tx.tx_type == "swap"]


class TestNativeInSwapNoAllowance:
    """Native-in swap compiles to swap-only bundle with msg.value funded."""

    def test_polygon_pol_to_usdc_no_approve(self, config: IntentCompilerConfig) -> None:
        compiler = IntentCompiler(chain="polygon", config=config)
        intent = SwapIntent(
            from_token="POL",
            to_token="USDC",
            amount=Decimal("10"),
            max_slippage=Decimal("0.005"),
        )
        result = compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Expected SUCCESS, got {result.error}"
        assert _approve_txs(result) == [], (
            "Native-in swap must NOT emit ERC20 approve/allowance txs — "
            "POL has no ERC20 allowance semantics."
        )
        swap_txs = _swap_txs(result)
        assert len(swap_txs) == 1
        # value is the native amount the router receives
        assert swap_txs[0].value == 10 * 10**18

    def test_avalanche_avax_to_usdc_no_approve(self, config: IntentCompilerConfig) -> None:
        compiler = IntentCompiler(chain="avalanche", config=config)
        intent = SwapIntent(
            from_token="AVAX",
            to_token="USDC",
            amount=Decimal("10"),
            max_slippage=Decimal("0.005"),
        )
        result = compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Expected SUCCESS, got {result.error}"
        assert _approve_txs(result) == []
        swap_txs = _swap_txs(result)
        assert len(swap_txs) == 1
        assert swap_txs[0].value == 10 * 10**18

    def test_ethereum_eth_to_usdc_no_approve(self, config: IntentCompilerConfig) -> None:
        """ETH on Ethereum is the canonical case — must stay regression-safe."""
        compiler = IntentCompiler(chain="ethereum", config=config)
        intent = SwapIntent(
            from_token="ETH",
            to_token="USDC",
            amount=Decimal("1"),
            max_slippage=Decimal("0.005"),
        )
        result = compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Expected SUCCESS, got {result.error}"
        assert _approve_txs(result) == []
        swap_txs = _swap_txs(result)
        assert len(swap_txs) == 1
        assert swap_txs[0].value == 10**18

    def test_polygon_matic_to_usdc_no_approve(self, config: IntentCompilerConfig) -> None:
        """MATIC already worked pre-fix (address == sentinel); keep the guard."""
        compiler = IntentCompiler(chain="polygon", config=config)
        intent = SwapIntent(
            from_token="MATIC",
            to_token="USDC",
            amount=Decimal("10"),
            max_slippage=Decimal("0.005"),
        )
        result = compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Expected SUCCESS, got {result.error}"
        assert _approve_txs(result) == []
        # Match the strictness of the other native-in tests so this guard
        # doesn't silently weaken if the swap-tx shape regresses.
        swap_txs = _swap_txs(result)
        assert len(swap_txs) == 1
        assert swap_txs[0].value == 10 * 10**18

    @pytest.mark.parametrize(
        "chain",
        ["arbitrum", "optimism", "base"],
    )
    def test_l2_eth_to_usdc_no_approve(self, chain: str, config: IntentCompilerConfig) -> None:
        """Ethereum L2s (Arbitrum / Optimism / Base) use ETH as native."""
        compiler = IntentCompiler(chain=chain, config=config)
        intent = SwapIntent(
            from_token="ETH",
            to_token="USDC",
            amount=Decimal("1"),
            max_slippage=Decimal("0.005"),
        )
        result = compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Expected SUCCESS on {chain}, got {result.error}"
        assert _approve_txs(result) == []
        swap_txs = _swap_txs(result)
        assert len(swap_txs) == 1
        assert swap_txs[0].value == 10**18

    def test_bsc_bnb_to_usdc_no_approve(self, config: IntentCompilerConfig) -> None:
        compiler = IntentCompiler(chain="bsc", config=config)
        intent = SwapIntent(
            from_token="BNB",
            to_token="USDC",
            amount=Decimal("1"),
            max_slippage=Decimal("0.005"),
        )
        result = compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Expected SUCCESS, got {result.error}"
        assert _approve_txs(result) == []
        swap_txs = _swap_txs(result)
        assert len(swap_txs) == 1
        assert swap_txs[0].value == 10**18


# ---------------------------------------------------------------------------
# ERC20-in swap regression guard — approvals must still be emitted
# ---------------------------------------------------------------------------


class TestErc20InSwapStillApproves:
    """Non-native inputs must continue to emit the ERC20 approve tx."""

    def test_polygon_usdc_to_pol_has_approve(self, config: IntentCompilerConfig) -> None:
        """USDC -> POL: USDC side needs the normal ERC20 approval flow."""
        compiler = IntentCompiler(chain="polygon", config=config)
        intent = SwapIntent(
            from_token="USDC",
            to_token="POL",
            amount=Decimal("10"),
            max_slippage=Decimal("0.005"),
        )
        result = compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Expected SUCCESS, got {result.error}"
        approves = _approve_txs(result)
        assert len(approves) >= 1, (
            "ERC20-in swap MUST emit approve tx — regression guard against "
            "the fix over-reaching and stripping approvals for non-native inputs."
        )
        # swap tx has value=0 because input is ERC20, not native
        swap_txs = _swap_txs(result)
        assert len(swap_txs) == 1
        assert swap_txs[0].value == 0

    def test_avalanche_usdc_to_avax_has_approve(self, config: IntentCompilerConfig) -> None:
        compiler = IntentCompiler(chain="avalanche", config=config)
        intent = SwapIntent(
            from_token="USDC",
            to_token="AVAX",
            amount=Decimal("10"),
            max_slippage=Decimal("0.005"),
        )
        result = compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Expected SUCCESS, got {result.error}"
        approves = _approve_txs(result)
        assert len(approves) >= 1
        swap_txs = _swap_txs(result)
        assert len(swap_txs) == 1
        assert swap_txs[0].value == 0
