"""VIB-6162 — the Aerodrome compiler burns only this deployment's own LP.

Every assertion here reads the ``liquidity`` kwarg the ADAPTER received, because that
value is what becomes ``removeLiquidity`` calldata. Asserting on the compiler's return
status would pass even if the bound were computed and then never applied — which is
precisely the inert shape this ticket has produced twice.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.connectors.aerodrome.compiler import compile_lp_close_aerodrome
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler
from almanak.framework.intents.vocabulary import Intent

POOL_ADDRESS = "0x" + "cc" * 20

#: The wallet holds 1.0 LP. Only 0.6 was minted by this deployment; the other 0.4 is a
#: user's own position sitting in the same pool.
WALLET_WEI = 10**18
OWN = Decimal("0.6")
FOREIGN = Decimal("0.4")


def _token(symbol: str, address: str, decimals: int = 18) -> MagicMock:
    tok = MagicMock()
    tok.symbol, tok.address, tok.decimals, tok.is_native = symbol, address, decimals, False
    tok.to_dict.return_value = {"symbol": symbol, "address": address, "decimals": decimals, "is_native": False}
    return tok


def _compiler() -> IntentCompiler:
    c = IntentCompiler.__new__(IntentCompiler)
    c.chain = "base"
    c.wallet_address = "0x" + "11" * 20
    c.price_oracle = {}
    c._gateway_client = None
    c._get_aerodrome_pool_address = MagicMock()
    return c


def _intent(outstanding: str | None = None) -> Intent:
    base = Intent.lp_close(
        position_id="USDC/DAI/stable", pool="USDC/DAI/stable", collect_fees=True, protocol="aerodrome"
    )
    if outstanding is None:
        return base
    # Intents are frozen pydantic models; model_copy is the supported mutation and the
    # same mechanism the teardown manager uses to attach the bound.
    return base.model_copy(update={"protocol_params": {"deployment_outstanding_lp": outstanding}})


def _compile(intent: Intent, *, balance_wei: int = WALLET_WEI):
    """Drive the real compiler and return (result, liquidity_passed_to_adapter)."""
    compiler = _compiler()
    with (
        patch.object(
            compiler, "_resolve_token", side_effect=[_token("USDC", "0x" + "aa" * 20), _token("DAI", "0x" + "bb" * 20)]
        ),
        patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"),
        patch.object(compiler, "_get_aerodrome_pool_address", return_value=POOL_ADDRESS),
        patch.object(compiler, "_query_erc20_balance", return_value=balance_wei),
        patch("almanak.connectors.aerodrome.AerodromeAdapter") as adapter_cls,
    ):
        adapter = adapter_cls.return_value
        adapter.remove_liquidity.return_value = MagicMock(success=False, error="stop-after-capture")
        result = compile_lp_close_aerodrome(compiler, intent)
        call = adapter.remove_liquidity.call_args
    return result, (call.kwargs.get("liquidity") if call else None)


def test_the_adapter_burns_only_the_deployments_own_liquidity():
    """THE test. Catches the whole defect: burning a user's LP alongside our own."""
    _result, liquidity = _compile(_intent(str(OWN)))
    assert liquidity == OWN, f"expected the burn bounded to {OWN}, got {liquidity}"
    assert liquidity != Decimal(1), "burned the wallet's entire balance — the VIB-6162 defect"


def test_the_foreign_remainder_is_exactly_what_is_left_behind():
    """States the user-visible outcome as arithmetic, not as a status code."""
    _result, liquidity = _compile(_intent(str(OWN)))
    assert Decimal(1) - liquidity == FOREIGN


def test_an_unbounded_close_still_withdraws_everything():
    """`ax lp-close` attaches no bound and must be unchanged.

    Contract (b), settled in the UAT card: `ax lp-close` says "fully withdraw" in its
    own help text and its operator IS the wallet owner. Since VIB-6517 it is the ONLY
    lane that reaches here unbounded — teardown refuses before compiling, and the
    iteration lane attaches a bound or the refusal sentinel in
    ``StrategyRunner._step_attach_lp_outstanding``.
    """
    _result, liquidity = _compile(_intent(None))
    assert liquidity == Decimal(1)


def test_the_iteration_lane_refusal_sentinel_refuses_with_the_reason():
    """VIB-6517: the runner stamps ``"unmeasured: <reason>"`` when the ledger cannot
    bound a decide()-emitted close; this branch is that sentinel's enforcement — a
    safety refusal that carries the reason, never a fallback to the full balance."""
    result, liquidity = _compile(_intent("unmeasured: no position_events rows for deployment"))
    assert result.status == CompilationStatus.FAILED
    assert result.is_safety_refusal is True
    assert liquidity is None, "adapter was called despite the refusal"
    assert "unmeasured: no position_events rows" in (result.error or "")


def test_tracked_over_live_refuses_instead_of_burning_the_difference():
    """The counterexample that killed min(): refusing must leave the adapter uncalled.

    outstanding=2.0 against a 1.0 wallet means the ledger and chain disagree, so the
    balance is no longer attributable. min(2, 1) would burn the whole 1.0 — including
    every foreign share.
    """
    result, liquidity = _compile(_intent("2.0"))
    assert result.status == CompilationStatus.FAILED
    assert result.is_safety_refusal is True
    assert liquidity is None, "adapter was called despite the refusal"
    assert "exceeds live balance" in (result.error or "")


def test_a_malformed_bound_refuses_rather_than_falling_back_to_the_wallet():
    """A bad value must never degrade to 'burn everything'."""
    result, liquidity = _compile(_intent("not-a-number"))
    assert result.status == CompilationStatus.FAILED
    assert result.is_safety_refusal is True
    assert liquidity is None


def test_zero_outstanding_is_a_no_op_not_a_full_burn():
    """A measured zero means this deployment holds nothing — so it burns nothing.

    The pre-existing zero-balance branch treats an empty WALLET as a no-op; this is the
    distinct case of a non-empty wallet holding none of OUR liquidity.
    """
    result, liquidity = _compile(_intent("0"))
    assert result.status == CompilationStatus.SUCCESS
    assert liquidity is None, "adapter was called for a zero-outstanding close"
    assert result.action_bundle.transactions == []
    assert any("no outstanding LP" in w for w in result.warnings)
