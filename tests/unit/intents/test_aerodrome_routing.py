"""Unit tests for VIB-5548 / ALM-2889 — reachable ``swap_params`` + the per-pair
Aerodrome routing resolver with bounded CL->Classic auto-fallback.

Two surfaces are covered:

1. ``SwapIntent.swap_params`` schema — construct / validate / serialize
   round-trip + rejection of malformed shapes (and that the field is now
   reachable, which is what activates Curve's previously-dead reads).
2. ``compile_swap_aerodrome`` routing — the full resolver matrix: auto CL hit,
   auto CL-miss -> Classic fallback (DAI/USDbC), explicit classic / CL /
   tick_spacing (no fallback), CL-only-against-absent fail-closed, offline
   degrade-to-legacy-default, and that the price-impact guard still runs on a
   fallback pool.

No mocking of the routing logic itself — only the on-chain pool-existence probes
and the adapter (which would otherwise hit a real RPC) are patched, exactly as
the sibling ALM-2890 regression suite does.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.pool_validation_base import (
    PoolValidationReason,
    PoolValidationResult,
)
from almanak.connectors.aerodrome.compiler import compile_swap_aerodrome
from almanak.framework.intents.compiler import CompilationStatus
from almanak.framework.intents.vocabulary import Intent, SwapIntent

# Canonical token order: token0 address < token1 address (EVM convention).
_T0_ADDR = "0x" + "aa" * 20
_T1_ADDR = "0x" + "bb" * 20

_PV_CL = "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool"
_PV_CLASSIC = "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_pool"


# ===========================================================================
# Schema: SwapIntent.swap_params
# ===========================================================================


def test_swap_params_defaults_to_none() -> None:
    intent = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("1"))
    assert intent.swap_params is None


def test_swap_params_accepts_well_formed_dict() -> None:
    intent = SwapIntent(
        from_token="USDC",
        to_token="WETH",
        amount=Decimal("1"),
        swap_params={"classic": True, "stable": False, "tick_spacing": 200},
    )
    assert intent.swap_params == {"classic": True, "stable": False, "tick_spacing": 200}


def test_swap_params_threaded_through_factory() -> None:
    intent = Intent.swap("USDC", "WETH", amount=Decimal("1"), swap_params={"tick_spacing": 50})
    assert intent.swap_params == {"tick_spacing": 50}


def test_swap_params_serialize_round_trip() -> None:
    intent = SwapIntent(
        from_token="USDC",
        to_token="WETH",
        amount=Decimal("1"),
        swap_params={"classic": False, "tick_spacing": 100},
    )
    data = intent.serialize()
    assert data["swap_params"] == {"classic": False, "tick_spacing": 100}
    restored = SwapIntent.deserialize(data)
    assert restored.swap_params == {"classic": False, "tick_spacing": 100}


@pytest.mark.parametrize(
    ("swap_params", "needle"),
    [
        ({"classic": "yes"}, "swap_params.classic must be a bool"),
        ({"stable": 1}, "swap_params.stable must be a bool"),
        ({"strict_oracle_guard": "x"}, "swap_params.strict_oracle_guard must be a bool"),
        ({"tick_spacing": 0}, "swap_params.tick_spacing must be a positive integer"),
        ({"tick_spacing": -100}, "swap_params.tick_spacing must be a positive integer"),
        ({"tick_spacing": True}, "swap_params.tick_spacing must be a positive integer"),
        ({"oracle_guard_bps": 0}, "swap_params.oracle_guard_bps must be a positive integer"),
    ],
)
def test_swap_params_rejects_bad_shapes(swap_params: dict, needle: str) -> None:
    with pytest.raises(ValueError, match=needle):
        SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("1"), swap_params=swap_params)


def test_swap_params_rejects_non_dict() -> None:
    # Pydantic's strict type validation rejects a non-dict before our validator,
    # so the construction raises (the precise message is Pydantic's).
    with pytest.raises(ValueError):  # noqa: PT011 - pydantic ValidationError subclasses ValueError
        SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("1"), swap_params=["classic"])  # type: ignore[arg-type]


def test_curve_swap_params_now_reachable() -> None:
    """Adding the field activates Curve's previously-dead ``swap_params`` reads
    (curve/compiler.py:440 — pool disambiguation / oracle_guard_bps /
    strict_oracle_guard). Before VIB-5548 the frozen+extra=forbid base model made
    ``swap_params`` unconstructable, so those reads always saw ``{}``."""
    intent = SwapIntent(
        from_token="USDC",
        to_token="DAI",
        amount=Decimal("1"),
        protocol="curve",
        chain="ethereum",
        swap_params={
            "pool": "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
            "oracle_guard_bps": 300,
            "strict_oracle_guard": True,
        },
    )
    assert intent.swap_params["pool"].startswith("0x")
    assert intent.swap_params["oracle_guard_bps"] == 300
    assert intent.swap_params["strict_oracle_guard"] is True


# ===========================================================================
# Routing resolver
# ===========================================================================


def _token(symbol: str, address: str, decimals: int) -> MagicMock:
    t = MagicMock()
    t.symbol = symbol
    t.address = address
    t.decimals = decimals
    t.is_native = False
    t.to_dict.return_value = {"symbol": symbol, "address": address, "decimals": decimals}
    return t


class _FakeSwapCompiler:
    """Duck-typed compiler with a production-shaped ``_validate_pool`` so the
    resolver's fail-closed gate is exercised, not bypassed."""

    def __init__(self, *, chain: str = "base", offline: bool = False, tokens: dict | None = None) -> None:
        self.chain = chain
        self.wallet_address = "0x" + "11" * 20
        self.price_oracle = {"USDC": Decimal("1"), "WETH": Decimal("1500"), "DAI": Decimal("1"), "USDbC": Decimal("1")}
        self._gateway_client = None
        self._config = SimpleNamespace(
            max_price_impact_pct=Decimal("0.30"),
            using_placeholders=offline,
            permission_discovery=False,
        )
        self._tokens = tokens or {
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
        # Mirror the production IntentCompiler._validate_pool fail-closed rule
        # (NOT_FOUND / definitively absent -> FAILED; offline relaxes RPC_FAILED).
        from almanak.framework.intents.compiler_models import CompilationResult

        offline = self._config.using_placeholders or self._config.permission_discovery
        fail_reasons = {PoolValidationReason.NOT_FOUND}
        if not offline:
            fail_reasons.add(PoolValidationReason.RPC_FAILED)
        if result.exists is False or result.reason in fail_reasons:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=result.error or result.warning or "pool validation failed",
                intent_id=intent_id,
            )
        return None


def _confirmed(address: str = "0x" + "cc" * 20) -> PoolValidationResult:
    return PoolValidationResult(exists=True, reason=PoolValidationReason.CONFIRMED, pool_address=address)


def _absent_cl(tick_spacing: int) -> PoolValidationResult:
    return PoolValidationResult(
        exists=False, reason=PoolValidationReason.NOT_FOUND, error=f"no cl pool ts={tick_spacing}"
    )


def _absent_classic() -> PoolValidationResult:
    return PoolValidationResult(exists=False, reason=PoolValidationReason.NOT_FOUND, error="no classic pool")


def _unknown() -> PoolValidationResult:
    return PoolValidationResult(exists=None, reason=PoolValidationReason.RPC_UNAVAILABLE, warning="offline")


def _swap_result(amount_out_wei: int) -> MagicMock:
    tx = MagicMock()
    tx.gas_estimate = 120_000
    tx.to_dict.return_value = {"tx_type": "swap"}
    quote = MagicMock()
    quote.amount_out = amount_out_wei
    quote.is_onchain = True
    return MagicMock(success=True, transactions=[tx], quote=quote, error=None)


def _adapter_patches(amount_out_wei: int = 3320 * 10**18):
    """Patch AerodromeConfig + AerodromeAdapter; the adapter returns a near-oracle
    quote so the price-impact guard passes by default."""
    cfg = patch("almanak.connectors.aerodrome.AerodromeConfig")
    adapter = patch("almanak.connectors.aerodrome.AerodromeAdapter")
    return cfg, adapter, amount_out_wei


def _run(
    compiler,
    intent,
    *,
    cl_side_effect=None,
    cl_return=None,
    classic_side_effect=None,
    classic_return=None,
    amount_out_wei: int = 3320 * 10**18,
):
    cl_kw = {"side_effect": cl_side_effect} if cl_side_effect is not None else {"return_value": cl_return}
    classic_kw = (
        {"side_effect": classic_side_effect} if classic_side_effect is not None else {"return_value": classic_return}
    )
    with (
        patch(_PV_CL, **cl_kw),
        patch(_PV_CLASSIC, **classic_kw),
        patch("almanak.connectors.aerodrome.AerodromeConfig"),
        patch("almanak.connectors.aerodrome.AerodromeAdapter") as mock_adapter_cls,
    ):
        mock_adapter_cls.return_value.swap_exact_input.return_value = _swap_result(amount_out_wei)
        return compile_swap_aerodrome(compiler, intent), mock_adapter_cls.return_value.swap_exact_input


def _intent(**overrides) -> Intent:
    params = {
        "from_token": "USDC",
        "to_token": "WETH",
        "amount": Decimal("5000000"),
        "max_slippage": Decimal("0.30"),
        "max_price_impact": Decimal("0.30"),
        "protocol": "aerodrome",
        "chain": "base",
    }
    params.update(overrides)
    return Intent.swap(**params)


def test_auto_cl_hit_routes_cl_no_fallback() -> None:
    """Auto routing finds a CL pool at the first candidate spacing -> CL, no fallback."""
    compiler = _FakeSwapCompiler()
    result, swap_call = _run(compiler, _intent(), cl_return=_confirmed())
    assert result.status == CompilationStatus.SUCCESS, result.error
    assert result.action_bundle.metadata["routing"] == "cl"
    assert result.action_bundle.metadata["routing_fallback"] is False
    assert result.action_bundle.metadata["tick_spacing"] == 100
    # Adapter invoked with CL routing.
    assert swap_call.call_args.kwargs["use_classic"] is False


def test_auto_cl_miss_falls_back_to_classic_dai_usdbc() -> None:
    """DAI/USDbC: no CL pool at any candidate spacing -> bounded fallback to the
    Classic stable pool (both legs are stablecoins -> stable-first)."""
    tokens = {
        "DAI": _token("DAI", _T0_ADDR, 18),
        "USDbC": _token("USDbC", _T1_ADDR, 6),
    }
    compiler = _FakeSwapCompiler(tokens=tokens)
    intent = _intent(from_token="DAI", to_token="USDbC", amount=Decimal("100"))
    classic_calls: list[bool] = []

    def classic_effect(chain, a, b, stable, rpc, gateway_client=None):
        classic_calls.append(stable)
        return _confirmed() if stable else _absent_classic()

    result, swap_call = _run(
        compiler, intent, cl_side_effect=lambda *a, **k: _absent_cl(a[3]), classic_side_effect=classic_effect
    )
    assert result.status == CompilationStatus.SUCCESS, result.error
    assert result.action_bundle.metadata["routing"] == "classic"
    assert result.action_bundle.metadata["routing_fallback"] is True
    assert result.action_bundle.metadata["stable"] is True
    # Stable-first: the first Classic probe was the stable pool.
    assert classic_calls[0] is True
    assert swap_call.call_args.kwargs["use_classic"] is True
    assert swap_call.call_args.kwargs["stable"] is True


def test_auto_neither_pool_fails_closed() -> None:
    compiler = _FakeSwapCompiler()
    result, _ = _run(
        compiler,
        _intent(),
        cl_side_effect=lambda *a, **k: _absent_cl(a[3]),
        classic_side_effect=lambda *a, **k: _absent_classic(),
    )
    assert result.status == CompilationStatus.FAILED
    assert "no aerodrome pool found" in (result.error or "").lower()
    assert "cl(tick_spacing=100)" in (result.error or "")
    assert "classic(stable=" in (result.error or "")


def test_explicit_classic_true_no_cl_probe() -> None:
    compiler = _FakeSwapCompiler()
    cl_calls: list = []

    def cl_effect(*a, **k):
        cl_calls.append(a)
        return _confirmed()

    result, swap_call = _run(
        compiler,
        _intent(swap_params={"classic": True}),
        cl_side_effect=cl_effect,
        classic_return=_confirmed(),
    )
    assert result.status == CompilationStatus.SUCCESS, result.error
    assert result.action_bundle.metadata["routing"] == "classic"
    assert result.action_bundle.metadata["routing_fallback"] is False
    # classic=True must never probe CL.
    assert cl_calls == []


def test_explicit_classic_false_cl_only_no_fallback_when_absent() -> None:
    """classic=False forbids fallback: an absent CL pool fails closed and the
    Classic factory is never probed."""
    compiler = _FakeSwapCompiler()
    classic_calls: list = []

    result, _ = _run(
        compiler,
        _intent(swap_params={"classic": False}),
        cl_side_effect=lambda *a, **k: _absent_cl(a[3]),
        classic_side_effect=lambda *a, **k: (classic_calls.append(a), _confirmed())[1],
    )
    assert result.status == CompilationStatus.FAILED
    assert classic_calls == []  # never fell back to Classic


def test_explicit_classic_false_finds_cl_at_non_default_spacing() -> None:
    """classic=False means "CL only, any spacing" -> it must probe across the
    candidate spacings, not just CL@100. A pair whose only CL pool lives at
    ts=200 routes CL there, and Classic is never probed (no fallback)."""
    compiler = _FakeSwapCompiler()
    classic_calls: list = []

    def cl_effect(chain, a, b, ts, rpc, gateway_client=None):
        return _confirmed() if ts == 200 else _absent_cl(ts)

    result, swap_call = _run(
        compiler,
        _intent(swap_params={"classic": False}),
        cl_side_effect=cl_effect,
        classic_side_effect=lambda *a, **k: (classic_calls.append(a), _confirmed())[1],
    )
    assert result.status == CompilationStatus.SUCCESS, result.error
    assert result.action_bundle.metadata["routing"] == "cl"
    assert result.action_bundle.metadata["tick_spacing"] == 200
    assert result.action_bundle.metadata["routing_fallback"] is False
    assert swap_call.call_args.kwargs["use_classic"] is False
    assert classic_calls == []  # classic=False never probes / falls back to Classic


def test_explicit_tick_spacing_probes_once_no_fallback() -> None:
    compiler = _FakeSwapCompiler()
    probed_ts: list[int] = []

    def cl_effect(chain, a, b, ts, rpc, gateway_client=None):
        probed_ts.append(ts)
        return _confirmed()

    result, swap_call = _run(compiler, _intent(swap_params={"tick_spacing": 200}), cl_side_effect=cl_effect)
    assert result.status == CompilationStatus.SUCCESS, result.error
    assert probed_ts == [200]  # exactly once, at the requested spacing
    assert result.action_bundle.metadata["tick_spacing"] == 200
    assert swap_call.call_args.kwargs["tick_spacing"] == 200


def test_offline_degrades_to_legacy_default_warn_and_proceed() -> None:
    """Offline/placeholder mode: probes return exists=None; auto routing degrades
    to the legacy CL@100 default and proceeds (permission-discovery friendly)."""
    compiler = _FakeSwapCompiler(offline=True)
    result, swap_call = _run(compiler, _intent(), cl_return=_unknown(), classic_return=_unknown())
    assert result.status == CompilationStatus.SUCCESS, result.error
    assert result.action_bundle.metadata["routing"] == "cl"
    assert result.action_bundle.metadata["tick_spacing"] == 100
    assert swap_call.call_args.kwargs["use_classic"] is False


def test_online_unverifiable_probe_degrades_not_fail_closed() -> None:
    """Online auto routing where CL@100 is *confirmed absent* but a later candidate
    spacing is *unverifiable* (exists=None) must degrade to legacy CL@100 and
    warn-and-proceed -- NOT fail closed on the cached absent CL@100 probe (the
    degraded route must carry the unverifiable probe so _validate_pool passes)."""
    compiler = _FakeSwapCompiler()  # online (not placeholder/permission-discovery)

    def cl_effect(chain, a, b, ts, rpc, gateway_client=None):
        # CL@100 genuinely absent; CL@200 unverifiable (e.g. malformed response).
        if ts == 100:
            return _absent_cl(100)
        if ts == 200:
            return _unknown()
        return _absent_cl(ts)

    result, swap_call = _run(
        compiler,
        _intent(),
        cl_side_effect=cl_effect,
        classic_side_effect=lambda *a, **k: _absent_classic(),
    )
    assert result.status == CompilationStatus.SUCCESS, result.error
    assert result.action_bundle.metadata["routing"] == "cl"
    assert result.action_bundle.metadata["tick_spacing"] == 100
    assert swap_call.call_args.kwargs["use_classic"] is False


def test_price_impact_guard_still_runs_on_classic_fallback() -> None:
    """The already-merged price-impact guard must run on the *fallback* pool: a
    thin Classic pool whose quote implies a huge impact must fail closed."""
    tokens = {
        "DAI": _token("DAI", _T0_ADDR, 18),
        "USDbC": _token("USDbC", _T1_ADDR, 6),
    }
    compiler = _FakeSwapCompiler(tokens=tokens)
    # 1% cap; oracle expects ~100 USDbC out for 100 DAI, adapter quotes only 50.
    intent = _intent(from_token="DAI", to_token="USDbC", amount=Decimal("100"), max_price_impact=Decimal("0.01"))
    result, _ = _run(
        compiler,
        intent,
        cl_side_effect=lambda *a, **k: _absent_cl(a[3]),
        classic_return=_confirmed(),
        amount_out_wei=50 * 10**6,  # ~50% impact vs oracle ~100
    )
    assert result.status == CompilationStatus.FAILED
    assert "price impact" in (result.error or "").lower()


def test_optimism_classic_false_raises_no_cl_error() -> None:
    """Velodrome/Optimism has no CL contracts: swap_params={'classic': False}
    must raise the no-CL-on-chain error."""
    tokens = {
        "USDC": _token("USDC", _T0_ADDR, 6),
        "WETH": _token("WETH", _T1_ADDR, 18),
    }
    compiler = _FakeSwapCompiler(chain="optimism", tokens=tokens)
    result, _ = _run(
        compiler,
        _intent(chain="optimism", swap_params={"classic": False}),
        cl_return=_confirmed(),
        classic_return=_confirmed(),
    )
    assert result.status == CompilationStatus.FAILED
    assert "cl (slipstream) routing is not available" in (result.error or "").lower()


def test_optimism_auto_routes_classic() -> None:
    tokens = {
        "USDC": _token("USDC", _T0_ADDR, 6),
        "WETH": _token("WETH", _T1_ADDR, 18),
    }
    compiler = _FakeSwapCompiler(chain="optimism", tokens=tokens)
    result, swap_call = _run(compiler, _intent(chain="optimism"), classic_return=_confirmed())
    assert result.status == CompilationStatus.SUCCESS, result.error
    assert result.action_bundle.metadata["routing"] == "classic"
    assert result.action_bundle.metadata["routing_fallback"] is False
    assert swap_call.call_args.kwargs["use_classic"] is True
