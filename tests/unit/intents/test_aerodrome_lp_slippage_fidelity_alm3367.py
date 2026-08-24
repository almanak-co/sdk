"""The caller's max_slippage must reach the Aerodrome LP adapter (ALM-3367).

Threading the tolerance through the compiler is half the fix. The adapter-level
tests prove the floor is computed correctly *given* a tolerance; this file
proves the compiler actually hands one over. Without it, the original defect —
LP paths falling through to a hard-coded 50 bps default while SWAP passed the
caller's number — can return unnoticed, because every other test calls the
adapter directly.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.aerodrome.compiler import compile_lp_close_aerodrome, compile_lp_open_aerodrome
from almanak.framework.intents.vocabulary import Intent

WALLET = "0x" + "11" * 20
POOL = "0x" + "cc" * 20


def _token(symbol: str, address: str, decimals: int) -> MagicMock:
    token = MagicMock()
    token.symbol = symbol
    token.address = address
    token.decimals = decimals
    token.is_native = False
    return token


def _compiler() -> MagicMock:
    """A duck-typed stand-in for IntentCompiler.

    The compile functions only read attributes off ``compiler``, and the real
    object needs a live gateway to construct. A MagicMock cannot run out of
    attributes, so this test stays about the tolerance rather than about
    tracking the compiler's constructor.
    """
    compiler = MagicMock()
    compiler.chain = "base"
    compiler.wallet_address = WALLET
    compiler.price_oracle = {}
    compiler.default_deadline_seconds = 300
    compiler._gateway_client = None
    compiler._using_placeholders = False
    compiler._get_chain_rpc_url.return_value = "http://localhost:8545"
    compiler._get_aerodrome_pool_address.return_value = POOL
    # `_validate_pool` returns None when the pool is acceptable; a MagicMock's
    # auto-created truthy return reads as a validation failure and short-circuits
    # the compile before it ever reaches the adapter.
    compiler._validate_pool.return_value = None
    # The close path converts this to a Decimal; a bare MagicMock cannot convert.
    compiler._query_erc20_balance.return_value = 10**18
    weth = _token("WETH", "0x" + "aa" * 20, 18)
    usdc = _token("USDC", "0x" + "bb" * 20, 6)
    compiler._resolve_token.side_effect = lambda symbol, *a, **kw: weth if "WETH" in str(symbol).upper() else usdc
    return compiler


def _captured_slippage_bps(intent, *, close: bool) -> int | None:
    """Compile the intent and report the slippage_bps the adapter was handed."""
    captured: dict[str, int | None] = {}
    method = "remove_liquidity" if close else "add_liquidity"

    def _record(*_args, **kwargs):
        captured["bps"] = kwargs.get("slippage_bps")
        return MagicMock(success=False, error="stopped after capture", transactions=[])

    compiler = _compiler()
    with (
        # AerodromeAdapter is imported lazily inside the compile functions, so
        # patch it at its definition site — the consuming module never binds it.
        patch(f"almanak.connectors.aerodrome.adapter.AerodromeAdapter.{method}", side_effect=_record),
    ):
        compile_lp_close_aerodrome(compiler, intent) if close else compile_lp_open_aerodrome(compiler, intent)
    return captured.get("bps", "NOT CALLED")  # type: ignore[return-value]


def _open_intent(max_slippage: Decimal | None) -> Intent:
    return Intent.lp_open(
        pool="WETH/USDC/volatile",
        amount0=Decimal("1"),
        amount1=Decimal("1"),
        range_lower=Decimal("1"),
        range_upper=Decimal("1000000"),
        max_slippage=max_slippage,
        protocol="aerodrome",
        chain="base",
    )


def _close_intent(max_slippage: Decimal | None) -> Intent:
    return Intent.lp_close(
        position_id="WETH/USDC/volatile",
        pool="WETH/USDC/volatile",
        max_slippage=max_slippage,
        protocol="aerodrome",
        chain="base",
    )


@pytest.mark.parametrize(("tolerance", "expected_bps"), [(Decimal("0.001"), 10), (Decimal("0.10"), 1000)])
def test_lp_open_hands_the_declared_tolerance_to_the_adapter(tolerance: Decimal, expected_bps: int) -> None:
    assert _captured_slippage_bps(_open_intent(tolerance), close=False) == expected_bps


@pytest.mark.parametrize(("tolerance", "expected_bps"), [(Decimal("0.001"), 10), (Decimal("0.10"), 1000)])
def test_lp_close_hands_the_declared_tolerance_to_the_adapter(tolerance: Decimal, expected_bps: int) -> None:
    assert _captured_slippage_bps(_close_intent(tolerance), close=True) == expected_bps


def test_lp_tolerance_is_not_a_constant() -> None:
    """The regression this file exists for: a hard-coded default passes every
    single-tolerance assertion above while ignoring the caller entirely."""
    tight = _captured_slippage_bps(_open_intent(Decimal("0.001")), close=False)
    wide = _captured_slippage_bps(_open_intent(Decimal("0.10")), close=False)

    assert tight != wide, "the compiler is passing a constant, not the caller's tolerance"


def test_omitted_tolerance_defers_to_the_connector_default() -> None:
    """None is a legitimate input — permission discovery compiles intents with
    no tolerance, and converting it unconditionally raises inside discovery."""
    assert _captured_slippage_bps(_open_intent(None), close=False) is None
    assert _captured_slippage_bps(_close_intent(None), close=True) is None
