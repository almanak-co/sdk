"""Regression tests for ALM-2890 (Aerodrome swap price-impact guard) and
ALM-2891 (Slipstream LP_OPEN tick-straddle assertion + price_to_tick decimals).

These pin the wave-1 fixes from VIB-5526:

- ALM-2890: ``compile_swap_aerodrome`` must fail-closed when the on-chain quoter
  implies a price impact above ``max_price_impact`` (it previously enforced only
  slippage, so a thin pool compiled regardless).
- ALM-2891: ``compile_lp_open_aerodrome_slipstream`` must reject a tick range
  that does not straddle the pool's current tick (silent one-sided / out-of-range
  mint), unless ``protocol_params={'allow_out_of_range': True}``; and
  ``tick_utils.price_to_tick`` must require explicit decimals.
"""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.pool_validation_base import (
    PoolValidationReason,
    PoolValidationResult,
)
from almanak.connectors.aerodrome.compiler import (
    compile_lp_open_aerodrome_slipstream,
    compile_swap_aerodrome,
)
from almanak.framework.intents.compiler import CompilationStatus
from almanak.framework.intents.tick_utils import price_to_tick
from almanak.framework.intents.vocabulary import Intent

# Canonical token order: token0 address < token1 address (EVM convention).
_T0_ADDR = "0x" + "aa" * 20
_T1_ADDR = "0x" + "bb" * 20


def _confirmed_cl_pool() -> PoolValidationResult:
    """A confirmed-existing CL pool so the per-pair resolver (VIB-5548) routes to
    CL at the first candidate tick spacing instead of probing a fallback."""
    return PoolValidationResult(
        exists=True,
        reason=PoolValidationReason.CONFIRMED,
        pool_address="0x" + "cc" * 20,
    )


def _token(symbol: str, address: str, decimals: int) -> MagicMock:
    t = MagicMock()
    t.symbol = symbol
    t.address = address
    t.decimals = decimals
    t.is_native = False
    t.to_dict.return_value = {"symbol": symbol, "address": address, "decimals": decimals}
    return t


class _FakeSwapCompiler:
    """Minimal duck-typed compiler for the standalone swap function."""

    def __init__(self, *, weth_price: Decimal, max_impact: Decimal = Decimal("0.30")) -> None:
        self.chain = "base"
        self.wallet_address = "0x" + "11" * 20
        self.price_oracle = {"USDC": Decimal("1"), "WETH": weth_price}
        self._gateway_client = None
        self._config = SimpleNamespace(
            max_price_impact_pct=max_impact,
            using_placeholders=False,
            permission_discovery=False,
        )
        self._tokens = {
            "USDC": _token("USDC", _T0_ADDR, 6),
            "WETH": _token("WETH", _T1_ADDR, 18),
        }

    def _resolve_token(self, sym):
        return self._tokens.get(sym)

    def _require_token_price(self, sym):
        return self.price_oracle[sym]

    def _get_chain_rpc_url(self):
        return "http://localhost:8545"

    def _validate_pool(self, result, intent_id):
        return None


def _swap_result(amount_out_wei: int, *, is_onchain: bool = True) -> MagicMock:
    tx = MagicMock()
    tx.gas_estimate = 120_000
    tx.to_dict.return_value = {"tx_type": "swap"}
    quote = MagicMock()
    quote.amount_out = amount_out_wei
    # Provenance (ALM-2890): a genuine on-chain quoter result vs an oracle-derived
    # fallback. The guard only trusts an on-chain amount as the quoter value.
    quote.is_onchain = is_onchain
    return MagicMock(success=True, transactions=[tx], quote=quote, error=None)


def _make_swap_intent(max_price_impact: Decimal | None) -> Intent:
    return Intent.swap(
        from_token="USDC",
        to_token="WETH",
        amount=Decimal("5000000"),
        max_slippage=Decimal("0.30"),
        max_price_impact=max_price_impact,
        protocol="aerodrome",
        chain="base",
    )


# ---------------------------------------------------------------------------
# ALM-2890 — swap price-impact guard
# ---------------------------------------------------------------------------


def test_aerodrome_swap_blocks_when_impact_exceeds_cap() -> None:
    """5M USDC->WETH with a 1% cap must FAIL when the quote implies ~40% impact."""
    compiler = _FakeSwapCompiler(weth_price=Decimal("1500"))
    intent = _make_swap_intent(max_price_impact=Decimal("0.01"))
    # Oracle expects 5,000,000 / 1500 = 3333.33 WETH; quote only 2000 WETH => 40% impact.
    with (
        patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool", return_value=_confirmed_cl_pool()
        ),
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch("almanak.connectors.aerodrome.AerodromeAdapter") as mock_adapter_cls,
    ):
        mock_adapter_cls.return_value.swap_exact_input.return_value = _swap_result(2000 * 10**18)
        result = compile_swap_aerodrome(compiler, intent)

    assert result.status == CompilationStatus.FAILED
    assert result.error is not None
    assert "price impact" in result.error.lower()


def test_aerodrome_swap_compiles_when_impact_within_cap() -> None:
    """A quote close to the oracle estimate must still compile (no false-positive)."""
    compiler = _FakeSwapCompiler(weth_price=Decimal("1500"))
    intent = _make_swap_intent(max_price_impact=Decimal("0.01"))
    # Oracle expects 3333.33 WETH; quote 3320 WETH => ~0.4% impact, under the 1% cap.
    with (
        patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool", return_value=_confirmed_cl_pool()
        ),
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch("almanak.connectors.aerodrome.AerodromeAdapter") as mock_adapter_cls,
    ):
        mock_adapter_cls.return_value.swap_exact_input.return_value = _swap_result(3320 * 10**18)
        result = compile_swap_aerodrome(compiler, intent)

    assert result.status == CompilationStatus.SUCCESS, result.error


def test_aerodrome_swap_uses_config_default_cap_when_intent_unset() -> None:
    """With no per-intent cap, the compiler config default (5%) must still apply."""
    compiler = _FakeSwapCompiler(weth_price=Decimal("1500"), max_impact=Decimal("0.05"))
    intent = _make_swap_intent(max_price_impact=None)
    # ~40% impact >> 5% default cap.
    with (
        patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool", return_value=_confirmed_cl_pool()
        ),
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch("almanak.connectors.aerodrome.AerodromeAdapter") as mock_adapter_cls,
    ):
        mock_adapter_cls.return_value.swap_exact_input.return_value = _swap_result(2000 * 10**18)
        result = compile_swap_aerodrome(compiler, intent)

    assert result.status == CompilationStatus.FAILED
    assert "price impact" in (result.error or "").lower()


def test_aerodrome_swap_fails_closed_on_oracle_fallback_quote() -> None:
    """When the adapter falls back to an oracle-derived quote (is_onchain=False)
    in live mode, the guard must NOT trust it as the quoter amount: comparing an
    oracle estimate against an oracle-derived quote would always show ~0 impact
    and silently defeat ALM-2890. The guard must fail closed instead."""
    compiler = _FakeSwapCompiler(weth_price=Decimal("1500"))
    intent = _make_swap_intent(max_price_impact=Decimal("0.01"))
    # Quote numerically matches the oracle estimate (~0% impact) but is an oracle
    # fallback, not a real on-chain quote — must still be refused.
    with (
        patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool", return_value=_confirmed_cl_pool()
        ),
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch("almanak.connectors.aerodrome.AerodromeAdapter") as mock_adapter_cls,
    ):
        oracle_estimate_weth = int((Decimal("5000000") / Decimal("1500")) * Decimal(10**18))
        mock_adapter_cls.return_value.swap_exact_input.return_value = _swap_result(
            oracle_estimate_weth, is_onchain=False
        )
        result = compile_swap_aerodrome(compiler, intent)

    assert result.status == CompilationStatus.FAILED
    assert "quoter returned no amount" in (result.error or "").lower() or "oracle price" in (result.error or "").lower()


def test_aerodrome_swap_zero_cap_blocks_any_nonzero_impact() -> None:
    """A configured max_price_impact_pct of Decimal('0') is a deliberate strict
    'any nonzero impact fails' setting and must NOT be coerced to the 5% default
    (Empty != Zero). A ~0.4% impact must FAIL under a zero cap."""
    compiler = _FakeSwapCompiler(weth_price=Decimal("1500"), max_impact=Decimal("0"))
    intent = _make_swap_intent(max_price_impact=None)
    # Oracle expects 3333.33 WETH; quote 3320 WETH => ~0.4% impact > 0 cap.
    with (
        patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool", return_value=_confirmed_cl_pool()
        ),
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch("almanak.connectors.aerodrome.AerodromeAdapter") as mock_adapter_cls,
    ):
        mock_adapter_cls.return_value.swap_exact_input.return_value = _swap_result(3320 * 10**18)
        result = compile_swap_aerodrome(compiler, intent)

    assert result.status == CompilationStatus.FAILED
    assert "price impact" in (result.error or "").lower()


# ---------------------------------------------------------------------------
# ALM-2891 (A) — price_to_tick requires explicit decimals
# ---------------------------------------------------------------------------


def test_price_to_tick_requires_explicit_decimals() -> None:
    with pytest.raises(ValueError, match="requires explicit decimals0 and decimals1"):
        price_to_tick(Decimal("0.00033"))
    with pytest.raises(ValueError, match="requires explicit decimals0 and decimals1"):
        price_to_tick(Decimal("0.00033"), decimals0=6)


def test_price_to_tick_with_explicit_decimals_still_works() -> None:
    # USDC(6)/WETH(18) — the value that the silent 18/18 default got wrong.
    assert price_to_tick(Decimal("0.00033"), decimals0=6, decimals1=18) == 196155


# ---------------------------------------------------------------------------
# ALM-2891 (B) — Slipstream LP_OPEN tick-straddle assertion
# ---------------------------------------------------------------------------


class _FakeLpCompiler:
    """Minimal duck-typed compiler for the standalone slipstream LP_OPEN function."""

    def __init__(self, current_tick: int) -> None:
        self.chain = "base"
        self.wallet_address = "0x" + "11" * 20
        self.price_oracle = {"USDC": Decimal("1"), "WETH": Decimal("1500")}
        self._gateway_client = None
        self.default_lp_slippage = Decimal("0.99")
        self._current_tick = current_tick
        self._tokens = {
            "WETH": _token("WETH", _T0_ADDR, 18),
            "USDC": _token("USDC", _T1_ADDR, 6),
        }

    def _resolve_token(self, sym):
        return self._tokens.get(sym)

    def _get_chain_rpc_url(self):
        return "http://localhost:8545"

    def _validate_pool(self, result, intent_id):
        return None

    def _fetch_lp_pool_slot0(self, pool_check):
        return (2**96, self._current_tick)


def _cl_adapter_result() -> MagicMock:
    tx = MagicMock()
    tx.gas_estimate = 250_000
    tx.tx_type = "mint"
    tx.to_dict.return_value = {"tx_type": "mint"}
    return MagicMock(success=True, transactions=[tx], error=None)


def _make_lp_open_intent(lower: int, upper: int, *, allow_oor: bool = False) -> Intent:
    params = {"allow_out_of_range": True} if allow_oor else None
    return Intent.lp_open(
        pool="WETH/USDC/100",
        amount0=Decimal("0.01"),
        amount1=Decimal("30"),
        range_lower=Decimal(lower),
        range_upper=Decimal(upper),
        protocol="aerodrome_slipstream",
        chain="base",
        protocol_params=params,
    )


def _patches():
    return (
        patch(
            "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool", return_value=_confirmed_cl_pool()
        ),
        patch("almanak.framework.intents.lp_math.recompute_lp_amounts", return_value=(100, 200)),
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch("almanak.connectors.aerodrome.AerodromeAdapter"),
    )


def test_slipstream_lp_open_rejects_out_of_range_below_current_tick() -> None:
    """Range entirely below the current tick must FAIL with a straddle error."""
    compiler = _FakeLpCompiler(current_tick=0)
    intent = _make_lp_open_intent(-5000, -2000)  # both < 0
    p1, p2, p3, p4 = _patches()
    with p1, p2, p3, p4 as adapter_cls:
        adapter_cls.return_value.add_cl_liquidity.return_value = _cl_adapter_result()
        result = compile_lp_open_aerodrome_slipstream(compiler, intent)

    assert result.status == CompilationStatus.FAILED
    assert "straddle" in (result.error or "").lower()


def test_slipstream_lp_open_rejects_out_of_range_above_current_tick() -> None:
    """Range entirely above the current tick must also FAIL."""
    compiler = _FakeLpCompiler(current_tick=0)
    intent = _make_lp_open_intent(2000, 5000)  # both > 0
    p1, p2, p3, p4 = _patches()
    with p1, p2, p3, p4 as adapter_cls:
        adapter_cls.return_value.add_cl_liquidity.return_value = _cl_adapter_result()
        result = compile_lp_open_aerodrome_slipstream(compiler, intent)

    assert result.status == CompilationStatus.FAILED
    assert "straddle" in (result.error or "").lower()


def test_slipstream_lp_open_allows_in_range_straddling_position() -> None:
    """A range straddling the current tick must compile."""
    compiler = _FakeLpCompiler(current_tick=0)
    intent = _make_lp_open_intent(-2000, 2000)  # straddles 0
    p1, p2, p3, p4 = _patches()
    with p1, p2, p3, p4 as adapter_cls:
        adapter_cls.return_value.add_cl_liquidity.return_value = _cl_adapter_result()
        result = compile_lp_open_aerodrome_slipstream(compiler, intent)

    assert result.status == CompilationStatus.SUCCESS, result.error


def test_slipstream_lp_open_out_of_range_opt_in_compiles() -> None:
    """allow_out_of_range=True must permit a deliberate one-sided range."""
    compiler = _FakeLpCompiler(current_tick=0)
    intent = _make_lp_open_intent(-5000, -2000, allow_oor=True)
    p1, p2, p3, p4 = _patches()
    with p1, p2, p3, p4 as adapter_cls:
        adapter_cls.return_value.add_cl_liquidity.return_value = _cl_adapter_result()
        result = compile_lp_open_aerodrome_slipstream(compiler, intent)

    assert result.status == CompilationStatus.SUCCESS, result.error
