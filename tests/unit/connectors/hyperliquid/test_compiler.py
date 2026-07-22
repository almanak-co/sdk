"""Unit tests for the Hyperliquid CoreWriter compiler.

Drives ``compile_perp_open`` / ``compile_perp_close`` against a fake compiler
context whose ``eth_call`` returns canned precompile reads (oracle price +
position), so the full compile path is exercised without a chain. The byte-exact
encoding is covered separately in ``test_sdk.py``; here we assert the compiler
wires the right target, direction, reduce-only flag, and fail-closed behaviour.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from eth_abi import encode as abi_encode

from almanak.connectors.hyperliquid.addresses import (
    CORE_WRITER_ADDRESS,
    PRECOMPILE_ORACLE_PX,
    PRECOMPILE_POSITION,
)
from almanak.connectors.hyperliquid.compiler import HyperliquidCompiler
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.vocabulary import PerpCloseIntent, PerpOpenIntent

_WALLET = "0x" + "11" * 20


def _oracle_return(human_price: Decimal, sz_decimals: int) -> str:
    # Wire = human * 10**(6 - szDecimals); BTC(sz=5): 59897 -> 598970.
    wire = int(human_price * (Decimal(10) ** (6 - sz_decimals)))
    return "0x" + abi_encode(["uint64"], [wire]).hex()


def _position_return(szi: int) -> str:
    return "0x" + abi_encode(["int64", "uint64", "int64", "uint32", "bool"], [szi, 0, 0, 0, False]).hex()


def _ctx(eth_call):
    services = SimpleNamespace(eth_call=eth_call)
    return SimpleNamespace(chain="hyperevm", wallet_address=_WALLET, services=services, protocol="hyperliquid")


def _open_intent(**kw) -> PerpOpenIntent:
    base = dict(
        market="BTC",
        collateral_token="USDC",
        collateral_amount=Decimal("100"),
        size_usd=Decimal("1000"),
        is_long=True,
        protocol="hyperliquid",
        chain="hyperevm",
    )
    base.update(kw)
    return PerpOpenIntent(**base)


def _close_intent(**kw) -> PerpCloseIntent:
    base = dict(market="BTC", collateral_token="USDC", is_long=True, protocol="hyperliquid", chain="hyperevm")
    base.update(kw)
    return PerpCloseIntent(**base)


class TestCompileOpen:
    def test_open_builds_core_writer_tx(self) -> None:
        # BTC oracle 59897, szDecimals 5.
        ctx = _ctx(lambda to, data, chain=None: _oracle_return(Decimal("59897"), 5))
        result = HyperliquidCompiler().compile_perp_open(ctx, _open_intent())
        assert result.status == CompilationStatus.SUCCESS
        txs = result.action_bundle.transactions
        assert len(txs) == 1
        assert txs[0]["to"].lower() == CORE_WRITER_ADDRESS.lower()
        assert result.action_bundle.metadata["asset_index"] == 0
        assert result.action_bundle.metadata["sz_decimals"] == 5
        assert result.action_bundle.metadata["reduce_only"] is False
        assert result.action_bundle.metadata["is_long"] is True

    def test_open_leverage_optin_warns_but_succeeds(self) -> None:
        # VIB-5724: a divergent leverage compiles ONLY with the explicit opt-in;
        # the compile-time warning still surfaces that leverage is not set on-venue.
        ctx = _ctx(lambda to, data, chain=None: _oracle_return(Decimal("59897"), 5))
        result = HyperliquidCompiler().compile_perp_open(
            ctx, _open_intent(leverage=Decimal("5"), accept_venue_leverage=True)
        )
        assert result.status == CompilationStatus.SUCCESS
        assert any("cannot set leverage" in w for w in result.warnings)
        assert result.action_bundle.metadata["accept_venue_leverage"] is True
        assert result.action_bundle.metadata["leverage_requested"] == "5"

    def test_open_leverage_without_optin_fails_closed(self) -> None:
        # VIB-5724 core gate: leverage != 1x and no opt-in → hard compile FAIL,
        # no tx emitted, BEFORE any oracle/position read (guard runs pre-read).
        def eth_call(*a, **k):  # pragma: no cover — asserts guard runs pre-read
            raise AssertionError("leverage gate must fail before any eth_call")

        ctx = _ctx(eth_call)
        result = HyperliquidCompiler().compile_perp_open(ctx, _open_intent(leverage=Decimal("2")))
        assert result.status == CompilationStatus.FAILED
        assert result.action_bundle is None  # no tx emitted
        # Pre-execution SAFETY refusal → classified GUARD_REFUSED so the runner's
        # failure circuit-breaker does not count it toward an emergency stop.
        assert result.is_safety_refusal is True
        # Actionable error names the value, the cause, and both remedies.
        assert "leverage=2x" in result.error
        assert "cannot set leverage on-venue" in result.error
        assert "accept_venue_leverage=true" in result.error
        assert "updateLeverage" in result.error

    def test_open_default_leverage_no_optin_succeeds(self) -> None:
        # leverage defaults to 1x = no specific request → gate does not fire even
        # without the opt-in (the default open path must keep working).
        ctx = _ctx(lambda to, data, chain=None: _oracle_return(Decimal("59897"), 5))
        result = HyperliquidCompiler().compile_perp_open(ctx, _open_intent())
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["accept_venue_leverage"] is False
        assert result.warnings == []  # 1x → no leverage warning either

    def test_open_optin_alone_does_not_fabricate_leverage_warning(self) -> None:
        # Opt-in at the default 1x must NOT invent a divergence warning: nothing
        # was requested that the venue can't honour.
        ctx = _ctx(lambda to, data, chain=None: _oracle_return(Decimal("59897"), 5))
        result = HyperliquidCompiler().compile_perp_open(
            ctx, _open_intent(accept_venue_leverage=True)
        )
        assert result.status == CompilationStatus.SUCCESS
        assert result.warnings == []

    def test_open_wrong_chain_fails(self) -> None:
        ctx = _ctx(lambda *a, **k: None)
        ctx.chain = "arbitrum"
        result = HyperliquidCompiler().compile_perp_open(ctx, _open_intent(chain="arbitrum"))
        assert result.status == CompilationStatus.FAILED
        assert "CoreWriter" in result.error

    def test_open_unknown_market_fails(self) -> None:
        ctx = _ctx(lambda *a, **k: _oracle_return(Decimal("1"), 2))
        result = HyperliquidCompiler().compile_perp_open(ctx, _open_intent(market="NOTACOIN"))
        assert result.status == CompilationStatus.FAILED
        assert "not in the resolvable set" in result.error

    def test_open_oracle_unavailable_fails_closed(self) -> None:
        ctx = _ctx(lambda to, data, chain=None: None)  # read unavailable
        result = HyperliquidCompiler().compile_perp_open(ctx, _open_intent())
        assert result.status == CompilationStatus.FAILED
        assert "oracle price unavailable" in result.error

    def test_open_below_min_order_value_fails_closed(self) -> None:
        # $9.99 < HyperCore ~$10 minimum order value → HyperCore would reject the
        # order off-EVM (silent no-op) while sendRawAction returns status 1. The
        # compiler must refuse to emit the tx. This is a pure compile-time check
        # (no oracle read reached), so eth_call must not even be consulted.
        def eth_call(*a, **k):  # pragma: no cover — asserts guard runs pre-read
            raise AssertionError("min-order guard must fail before any eth_call")

        ctx = _ctx(eth_call)
        result = HyperliquidCompiler().compile_perp_open(ctx, _open_intent(size_usd=Decimal("9.99")))
        assert result.status == CompilationStatus.FAILED
        assert "minimum order value" in result.error
        assert result.action_bundle is None  # no tx emitted

    def test_open_at_min_order_value_succeeds(self) -> None:
        # Exactly $10 is at (not below) the floor → allowed.
        ctx = _ctx(lambda to, data, chain=None: _oracle_return(Decimal("59897"), 5))
        result = HyperliquidCompiler().compile_perp_open(ctx, _open_intent(size_usd=Decimal("10")))
        assert result.status == CompilationStatus.SUCCESS
        assert len(result.action_bundle.transactions) == 1


class TestCompileClose:
    def _ctx_with_position(self, szi: int, price: Decimal = Decimal("59897"), sz_decimals: int = 5):
        def eth_call(to, data, chain=None):
            if to == PRECOMPILE_POSITION:
                return _position_return(szi)
            if to == PRECOMPILE_ORACLE_PX:
                return _oracle_return(price, sz_decimals)
            return None

        return _ctx(eth_call)

    def test_close_long_sells_reduce_only(self) -> None:
        ctx = self._ctx_with_position(szi=1000)  # long 0.01 BTC (1000 / 1e5)
        result = HyperliquidCompiler().compile_perp_close(ctx, _close_intent())
        assert result.status == CompilationStatus.SUCCESS
        meta = result.action_bundle.metadata
        assert meta["reduce_only"] is True
        assert meta["is_long"] is True  # the on-chain position was long
        assert result.action_bundle.transactions[0]["to"].lower() == CORE_WRITER_ADDRESS.lower()

    def test_close_no_position_fails(self) -> None:
        ctx = self._ctx_with_position(szi=0)
        result = HyperliquidCompiler().compile_perp_close(ctx, _close_intent())
        assert result.status == CompilationStatus.FAILED
        assert "no open Hyperliquid position" in result.error

    def test_close_position_read_unavailable_fails_closed(self) -> None:
        ctx = _ctx(lambda to, data, chain=None: None)
        result = HyperliquidCompiler().compile_perp_close(ctx, _close_intent())
        assert result.status == CompilationStatus.FAILED
        assert "could not read HyperCore position" in result.error

    def test_partial_close_caps_at_position(self) -> None:
        # Position 0.01 BTC (~$599 at 59897); request $10000 close → capped at full.
        ctx = self._ctx_with_position(szi=1000)
        result = HyperliquidCompiler().compile_perp_close(ctx, _close_intent(size_usd=Decimal("10000")))
        assert result.status == CompilationStatus.SUCCESS
        # sz_wire equals the full position (0.01 BTC * 1e8 = 1_000_000), not the oversized request.
        assert result.action_bundle.metadata["sz_wire"] == 1_000_000

    def test_close_below_min_order_value_not_blocked(self) -> None:
        # Reduce-only closes are EXEMPT from HyperCore's minimum order value: a
        # sub-$10 partial close (here ~$5) must still compile to a CoreWriter tx.
        # The min-order guard lives only on the open path; regressing it onto the
        # close path would strand a small residual position (can't shrink it).
        ctx = self._ctx_with_position(szi=1000)  # long 0.01 BTC (~$599 @ 59897)
        result = HyperliquidCompiler().compile_perp_close(ctx, _close_intent(size_usd=Decimal("5")))
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["reduce_only"] is True
        assert len(result.action_bundle.transactions) == 1

    def test_partial_close_non_positive_size_fails_closed(self) -> None:
        # PerpCloseIntent already rejects size_usd<=0 at construction (the primary
        # guard). This pins the compiler's defense-in-depth for an intent that
        # bypasses model validation (model_copy / model_construct / a future model
        # change): the close sizing must fail closed (FAILED, no tx), never reach
        # the CoreWriter encoder with a zero/negative sz.
        ctx = self._ctx_with_position(szi=1000)
        base = _close_intent(size_usd=Decimal("10"))
        for bad in (Decimal("0"), Decimal("-5")):
            intent = base.model_copy(update={"size_usd": bad})  # skips re-validation
            result = HyperliquidCompiler().compile_perp_close(ctx, intent)
            assert result.status == CompilationStatus.FAILED, f"size_usd={bad} should fail closed"
            assert "size_usd must be positive" in result.error
