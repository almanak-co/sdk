"""Unit tests for ``almanak ax lending-capacity`` — pre-deploy Morpho lending-loop
capacity + bidirectional route-viability check (ALM-3515).

`ax lending-market` proves a market_id is real (on-chain identity). It says
NOTHING about whether the market has enough liquidity for a strategy's sizing,
or whether the exit route (collateral -> loan, for emergency deleverage) is
viable at that size. This command is the promotion step for THAT question —
exercised here via the exit-code contract (0 PASS / 1 FAIL / 2 invalid /
3 WARN / 4 unavailable) and the token-substitution guard: every downstream
call (decimals, swap routes) must carry the EXACT addresses `GetLendingMarket`
returned, never a symbol-resolved substitute (the WETH/wstETH regression class
ALM-3515 names explicitly).
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import grpc
import pytest
from click.testing import CliRunner

from almanak.framework.cli.ax import ax as ax_cli

_MARKET_ID = "0x13c42741a359ac4a8aa8287d2be109dcf28344484f91185f9a79bd5a805a55ae"
_COLLATERAL = "0xc1cba3fcea344f92d9239c08c0568f6f2f0ee452"
_LOAN = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
# Deliberately NOT the address a symbol resolver would produce for "wstETH" /
# "USDC" on any real catalog — proves callers used the verified address, not a
# symbol lookup (see TestTokenSubstitutionGuard below).
# Syntactically valid but unfunded — this command never signs or executes, so
# a real wallet is never required, only a well-formed `fromAddress` for the
# route provider's quote endpoints.
_WALLET = "0x000000000000000000000000000000000000dEaD"


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _market_response(*, total_supply_assets="", total_borrow_assets="") -> SimpleNamespace:
    return SimpleNamespace(
        success=True,
        error="",
        market=SimpleNamespace(
            kind=2,
            protocol="morpho_blue",
            chain="base",
            market_id=_MARKET_ID,
            collateral_token=_COLLATERAL,
            collateral_symbol="wstETH",
            loan_token=_LOAN,
            loan_symbol="USDC",
            lltv_bps=8600,
            oracle="0xoracle",
            irm="0xirm",
            verified=True,
            total_supply_assets=total_supply_assets,
            total_borrow_assets=total_borrow_assets,
        ),
    )


def _decimals_response(decimals: int) -> SimpleNamespace:
    return SimpleNamespace(success=True, decimals=decimals, error="")


def _route_response(*, success=True, amount_out="0", price_impact=0, error="") -> SimpleNamespace:
    return SimpleNamespace(
        success=success,
        error=error,
        to="0xrouter",
        data="0x",
        value="0",
        gas="21000",
        amount_out=amount_out,
        price_impact=price_impact,
        gas_estimate="21000",
        bridge_fee="0",
        estimated_time=0,
        is_cross_chain=False,
        route_json="{}",
    )


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


def _decimals_by_token(collateral_decimals: int, loan_decimals: int):
    def _get(request, timeout):
        if request.token.lower() == _COLLATERAL.lower():
            return _decimals_response(collateral_decimals)
        if request.token.lower() == _LOAN.lower():
            return _decimals_response(loan_decimals)
        # pytest.fail raises Failed (a BaseException), so this sentinel stays
        # loud even now that the production code under test catches a bare
        # Exception -- a plain AssertionError would be swallowed into a
        # generic "unavailable" exit and this test-authoring bug would go
        # silent instead of failing the test.
        pytest.fail(f"unexpected token decimals lookup: {request.token!r}")

    return _get


def _patch_stubs(
    monkeypatch: pytest.MonkeyPatch,
    *,
    get_lending_market,
    get_token_decimals,
    get_route,
) -> MagicMock:
    channel = MagicMock(name="channel")
    monkeypatch.setattr("almanak.framework.cli.ax._acquire_gateway_channel", lambda ctx: (channel, None))
    monkeypatch.setattr(
        "almanak.gateway.proto.gateway_pb2_grpc.MarketServiceStub",
        lambda ch: SimpleNamespace(GetLendingMarket=get_lending_market),
    )
    monkeypatch.setattr(
        "almanak.gateway.proto.gateway_pb2_grpc.TokenServiceStub",
        lambda ch: SimpleNamespace(GetTokenDecimals=get_token_decimals),
    )
    monkeypatch.setattr(
        "almanak.gateway.proto.gateway_pb2_grpc.EnsoServiceStub",
        lambda ch: SimpleNamespace(GetRoute=get_route),
    )
    return channel


def _invoke(
    runner: CliRunner,
    *extra_args: str,
    collateral_amount="10",
    target_leverage="2",
    wallet_address: str | None = _WALLET,
) -> object:
    return runner.invoke(
        ax_cli,
        [
            "-c",
            "base",
            *extra_args,
            "lending-capacity",
            "--protocol",
            "morpho_blue",
            "--market-id",
            _MARKET_ID,
            "--collateral-amount",
            collateral_amount,
            "--target-leverage",
            target_leverage,
            *(["--wallet-address", wallet_address] if wallet_address else []),
        ],
    )


# Realistic-scale fixture: wstETH (18dp) collateral priced ~3000 USDC (6dp).
# 10 wstETH @ 2x leverage -> full stack 20 wstETH -> (at 3000 USDC/wstETH)
# 60,000 USDC teardown quote -> implied borrow (leverage-1)/leverage = 30,000 USDC
# -> loop-up leg quotes 30,000 USDC -> 10 wstETH.
_TEARDOWN_AMOUNT_OUT = str(60_000 * 10**6)
_LOOPUP_AMOUNT_OUT = str(10 * 10**18)


def _passing_route(request, timeout):
    if request.token_in.lower() == _COLLATERAL.lower():
        return _route_response(amount_out=_TEARDOWN_AMOUNT_OUT, price_impact=10)
    return _route_response(amount_out=_LOOPUP_AMOUNT_OUT, price_impact=10)


class TestPass:
    def test_healthy_market_passes(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        channel = _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(100_000 * 10**6), total_borrow_assets=str(10_000 * 10**6)
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=_passing_route,
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "pass"
        assert payload["liquidity_measured"] is True
        assert payload["teardown_leg"]["success"] is True
        assert payload["loopup_leg"]["success"] is True
        assert payload["implied_total_borrow"] == "30000"
        # 30_000 / 90_000 = 3333.33...bps; utilization rounds UP (safety threshold).
        assert payload["liquidity_utilization_bps"] == 3334
        channel.close.assert_called_once()

    def test_human_output_renders_pass(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(100_000 * 10**6), total_borrow_assets=str(10_000 * 10**6)
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=_passing_route,
        )
        result = _invoke(runner)
        assert result.exit_code == 0, result.output
        assert "PASS" in result.output
        assert "teardown leg" in result.output
        assert "loop-up leg" in result.output


class TestLiquidityFail:
    def test_insufficient_liquidity_fails(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        """Implied borrow (30,000 USDC) far exceeds a thin 1,000 USDC available pool."""
        channel = _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(1_000 * 10**6), total_borrow_assets="0"
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=_passing_route,
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 1, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "fail"
        assert payload["liquidity_utilization_bps"] > payload["max_liquidity_utilization_bps"]
        channel.close.assert_called_once()

    def test_zero_available_liquidity_fails(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(10 * 10**6), total_borrow_assets=str(10 * 10**6)
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=_passing_route,
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 1, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "fail"


class TestLiquidityUnmeasuredWarn:
    def test_no_liquidity_read_warns_not_passes(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        """Empty ≠ Zero: a protocol without a live pool-state read must WARN,
        never silently PASS as if liquidity were verified safe."""
        channel = _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(),  # no liquidity fields
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=_passing_route,
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 3, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "warn"
        assert payload["liquidity_measured"] is False
        assert payload["available_liquidity"] is None
        channel.close.assert_called_once()

    def test_human_output_flags_unmeasured(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=_passing_route,
        )
        result = _invoke(runner)
        assert result.exit_code == 3, result.output
        assert "UNMEASURED" in result.output


class TestRouteFail:
    def test_teardown_route_failure_fails_closed(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        def route(request, timeout):
            if request.token_in.lower() == _COLLATERAL.lower():
                return _route_response(success=False, error="no route found")
            return _route_response(amount_out=_LOOPUP_AMOUNT_OUT, price_impact=10)

        channel = _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(100_000 * 10**6), total_borrow_assets=str(10_000 * 10**6)
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=route,
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 1, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "fail"
        assert payload["teardown_leg"]["success"] is False
        assert "no route found" in payload["teardown_leg"]["error"]
        channel.close.assert_called_once()

    def test_loopup_route_failure_fails_closed(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        def route(request, timeout):
            if request.token_in.lower() == _COLLATERAL.lower():
                return _route_response(amount_out=_TEARDOWN_AMOUNT_OUT, price_impact=10)
            return _route_response(success=False, error="slippage exceeds threshold")

        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(100_000 * 10**6), total_borrow_assets=str(10_000 * 10**6)
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=route,
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 1, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "fail"
        assert payload["loopup_leg"]["success"] is False


class TestIdentityVerificationGate:
    def test_market_not_found_exits_one(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_not_found(request, timeout):
            raise _FakeRpcError(grpc.StatusCode.NOT_FOUND, "market does not exist")

        channel = _patch_stubs(
            monkeypatch,
            get_lending_market=raise_not_found,
            get_token_decimals=lambda request, timeout: pytest.fail(
                "must not resolve decimals for an unverified market"
            ),
            get_route=lambda request, timeout: pytest.fail("must not quote routes for an unverified market"),
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 1, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "fail"
        channel.close.assert_called_once()

    def test_recompute_mismatch_exits_two(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        def raise_mismatch(request, timeout):
            raise _FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT, "recomputed market id does not match requested id")

        _patch_stubs(
            monkeypatch,
            get_lending_market=raise_mismatch,
            get_token_decimals=lambda request, timeout: pytest.fail("unreachable"),
            get_route=lambda request, timeout: pytest.fail("unreachable"),
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 2, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "invalid"

    def test_gateway_unreachable_exits_four(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "almanak.framework.cli.ax._acquire_gateway_channel",
            lambda ctx: (None, "no gateway running"),
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unavailable"


class TestInvalidInput:
    def test_leverage_below_one_rejected(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: pytest.fail("must not reach the gateway"),
            get_token_decimals=lambda request, timeout: pytest.fail("unreachable"),
            get_route=lambda request, timeout: pytest.fail("unreachable"),
        )
        result = _invoke(runner, target_leverage="0.5")
        assert result.exit_code == 2, result.output

    def test_zero_collateral_amount_rejected(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: pytest.fail("must not reach the gateway"),
            get_token_decimals=lambda request, timeout: pytest.fail("unreachable"),
            get_route=lambda request, timeout: pytest.fail("unreachable"),
        )
        result = _invoke(runner, collateral_amount="0")
        assert result.exit_code == 2, result.output


class TestNoLoopLeverageOne:
    def test_leverage_one_skips_loopup_leg(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        """target_leverage=1.0 implies zero borrow — no loop-up leg to quote."""

        def route(request, timeout):
            assert request.token_in.lower() == _COLLATERAL.lower(), "only the teardown leg should be quoted"
            return _route_response(amount_out=_TEARDOWN_AMOUNT_OUT, price_impact=10)

        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(100_000 * 10**6), total_borrow_assets="0"
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=route,
        )
        result = _invoke(runner, "--json", target_leverage="1")
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["implied_total_borrow"] == "0"
        assert payload["loopup_leg"] is None

    def test_leverage_one_passes_on_a_fully_utilized_market(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A no-borrow position never touches this market's liquidity -- a
        fully-utilized market (available_liquidity_base <= 0) must not fail a
        position that borrows nothing from it."""
        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(100_000 * 10**6), total_borrow_assets=str(100_000 * 10**6)
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=lambda request, timeout: _route_response(amount_out=_TEARDOWN_AMOUNT_OUT, price_impact=10),
        )
        result = _invoke(runner, "--json", target_leverage="1")
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "pass"
        assert payload["liquidity_utilization_bps"] == 0


class TestTokenSubstitutionGuard:
    """ALM-3515's named regression: collateral/loan must never be re-resolved
    by symbol downstream of GetLendingMarket — every decimals lookup and swap
    quote must carry the EXACT verified addresses."""

    def test_exact_verified_addresses_reach_decimals_and_routes(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen_decimals_tokens: list[str] = []
        seen_route_tokens: list[tuple[str, str]] = []

        def get_token_decimals(request, timeout):
            seen_decimals_tokens.append(request.token)
            return _decimals_response(18 if request.token.lower() == _COLLATERAL.lower() else 6)

        def get_route(request, timeout):
            seen_route_tokens.append((request.token_in, request.token_out))
            return _passing_route(request, timeout)

        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(100_000 * 10**6), total_borrow_assets=str(10_000 * 10**6)
            ),
            get_token_decimals=get_token_decimals,
            get_route=get_route,
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 0, result.output

        assert set(seen_decimals_tokens) == {_COLLATERAL, _LOAN}
        assert set(seen_route_tokens) == {(_COLLATERAL, _LOAN), (_LOAN, _COLLATERAL)}
        # Neither symbol string ever substitutes for the verified address.
        for tok_in, tok_out in seen_route_tokens:
            assert tok_in not in ("wstETH", "USDC")
            assert tok_out not in ("wstETH", "USDC")


class TestWalletAddressRequired:
    """The route provider requires a syntactically valid `fromAddress` on every
    quote, even though this command never signs or executes. Regression for a
    live-mainnet reproduction: running this command with no ALMANAK_PRIVATE_KEY
    configured and no --wallet-address reached the route provider with an empty
    fromAddress and failed with an opaque upstream 400 instead of a clear,
    actionable local error."""

    def test_no_wallet_configured_and_no_override_fails_fast(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Isolate from whatever ALMANAK_PRIVATE_KEY / .env this machine actually
        # has (dev machines commonly have one for live `ax` testing) -- this test
        # is about the fail-fast branch when NEITHER a pinned wallet NOR a
        # derivable private key is available, not about this machine's env.
        monkeypatch.setattr("almanak.framework.agent_tools.cli_executor._resolve_wallet_address", lambda: "")
        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: pytest.fail("must not reach the gateway"),
            get_token_decimals=lambda request, timeout: pytest.fail("unreachable"),
            get_route=lambda request, timeout: pytest.fail("unreachable"),
        )
        result = _invoke(runner, wallet_address=None)
        assert result.exit_code == 2, result.output
        assert "--wallet-address" in result.output
        assert "ALMANAK_PRIVATE_KEY" in result.output

    def test_wallet_address_override_reaches_route_requests(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen_from_addresses: list[str] = []

        def get_route(request, timeout):
            seen_from_addresses.append(request.from_address)
            return _passing_route(request, timeout)

        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(100_000 * 10**6), total_borrow_assets=str(10_000 * 10**6)
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=get_route,
        )
        result = _invoke(runner, "--json", wallet_address=_WALLET)
        assert result.exit_code == 0, result.output
        assert seen_from_addresses == [_WALLET, _WALLET]


class TestMalformedRouteAmountOut:
    """`amount_out` is an Enso response field, not validated by this command's
    own input handling -- a malformed value must fail cleanly (unavailable,
    exit 4), never crash with a raw traceback."""

    def test_malformed_teardown_amount_out_fails_unavailable_not_crash(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(100_000 * 10**6), total_borrow_assets=str(10_000 * 10**6)
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=lambda request, timeout: _route_response(amount_out="not-a-number", price_impact=10),
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unavailable"

    def test_malformed_loopup_amount_out_fails_unavailable_not_crash(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def get_route(request, timeout):
            if request.token_in.lower() == _COLLATERAL.lower():
                return _route_response(amount_out=_TEARDOWN_AMOUNT_OUT, price_impact=10)
            return _route_response(amount_out="not-a-number", price_impact=10)

        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(100_000 * 10**6), total_borrow_assets=str(10_000 * 10**6)
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=get_route,
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unavailable"


class TestZeroOrNegativeAmountOutOnSuccess:
    """A route reporting success=True with a non-positive amount_out (e.g. an
    Enso HTTP-200 body that omits amountOut on schema drift) must never read
    as a green light -- this is the failure mode the CLI's whole job is to
    catch, and a naive "did the RPC succeed" check silently PASSes it."""

    def test_teardown_success_with_zero_amount_out_fails_unavailable(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(100_000 * 10**6), total_borrow_assets=str(10_000 * 10**6)
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=lambda request, timeout: _route_response(amount_out="0", price_impact=0),
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unavailable"
        # The whole point of the guard: a naive check would have derived
        # implied_borrow=0 from this and reported PASS without ever quoting
        # the loop-up leg.
        assert payload["status"] != "pass"

    def test_loopup_success_with_negative_amount_out_fails_unavailable(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def get_route(request, timeout):
            if request.token_in.lower() == _COLLATERAL.lower():
                return _route_response(amount_out=_TEARDOWN_AMOUNT_OUT, price_impact=10)
            return _route_response(amount_out="-1", price_impact=10)

        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(100_000 * 10**6), total_borrow_assets=str(10_000 * 10**6)
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=get_route,
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unavailable"


class TestWalletDerivedFromPrivateKey:
    """The documented, common `ax` setup is a bare ALMANAK_PRIVATE_KEY with no
    separate --wallet / ALMANAK_WALLET_ADDRESS pin -- this command must work in
    that setup exactly like every other `ax` command, not require an extra flag."""

    def test_wallet_derived_from_private_key_when_no_pin_or_override(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        derived = "0x1111111111111111111111111111111111111111"
        monkeypatch.setattr("almanak.framework.agent_tools.cli_executor._resolve_wallet_address", lambda: derived)
        seen_from_addresses: list[str] = []

        def get_route(request, timeout):
            seen_from_addresses.append(request.from_address)
            return _passing_route(request, timeout)

        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(100_000 * 10**6), total_borrow_assets=str(10_000 * 10**6)
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=get_route,
        )
        result = _invoke(runner, "--json", wallet_address=None)
        assert result.exit_code == 0, result.output
        assert seen_from_addresses == [derived, derived]


class TestNonFiniteDecimalInputs:
    """`Decimal` parses "nan"/"inf" without raising -- both must be rejected as
    invalid input (exit 2), not left to crash from an unrelated comparison or
    integer-sizing line downstream."""

    def test_nan_leverage_rejected(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: pytest.fail("must not reach the gateway"),
            get_token_decimals=lambda request, timeout: pytest.fail("unreachable"),
            get_route=lambda request, timeout: pytest.fail("unreachable"),
        )
        result = _invoke(runner, target_leverage="nan")
        assert result.exit_code == 2, result.output

    def test_infinite_leverage_rejected(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: pytest.fail("must not reach the gateway"),
            get_token_decimals=lambda request, timeout: pytest.fail("unreachable"),
            get_route=lambda request, timeout: pytest.fail("unreachable"),
        )
        result = _invoke(runner, target_leverage="inf")
        assert result.exit_code == 2, result.output

    def test_nan_collateral_amount_rejected(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: pytest.fail("must not reach the gateway"),
            get_token_decimals=lambda request, timeout: pytest.fail("unreachable"),
            get_route=lambda request, timeout: pytest.fail("unreachable"),
        )
        result = _invoke(runner, collateral_amount="nan")
        assert result.exit_code == 2, result.output

    def test_infinite_collateral_amount_rejected(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: pytest.fail("must not reach the gateway"),
            get_token_decimals=lambda request, timeout: pytest.fail("unreachable"),
            get_route=lambda request, timeout: pytest.fail("unreachable"),
        )
        result = _invoke(runner, collateral_amount="inf")
        assert result.exit_code == 2, result.output


class TestMalformedLiquidityFields:
    """total_supply_assets / total_borrow_assets cross the gRPC trust boundary
    from a connector-owned interface -- a malformed value must read as
    unmeasured (WARN), never crash into an uncaught exception whose resulting
    exit 1 would be misread as "the position is unsafe" (that IS the exit-1
    contract for a real safety verdict)."""

    def test_malformed_total_supply_assets_warns_not_crash(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets="not-a-number", total_borrow_assets=str(10_000 * 10**6)
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=_passing_route,
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 3, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "warn"
        assert payload["liquidity_measured"] is False


class TestWalletAddressHexValidation:
    """A --wallet-address that merely LOOKS like an address (0x + 40 chars)
    but isn't valid hex must be rejected locally, not forwarded to the route
    provider where it becomes the opaque upstream error this guard exists to
    prevent."""

    def test_non_hex_wallet_address_rejected(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: pytest.fail("must not reach the gateway"),
            get_token_decimals=lambda request, timeout: pytest.fail("unreachable"),
            get_route=lambda request, timeout: pytest.fail("unreachable"),
        )
        non_hex = "0x" + "Z" * 40
        result = _invoke(runner, wallet_address=non_hex)
        assert result.exit_code == 2, result.output

    @pytest.mark.parametrize(
        "bad_address",
        [
            "0x" + "Z" * 40,
            "0x+" + "a" * 39,
            "0x-" + "a" * 39,
            "0x  " + "a" * 38,
            "0x" + "a" * 19 + "_" + "b" * 20,
        ],
        ids=["nonhex", "plus_sign", "minus_sign", "embedded_space", "underscore"],
    )
    def test_looks_like_address_rejects_non_strict_hex(self, bad_address: str) -> None:
        """`int(v, 16)` is NOT a hex-charset check -- it accepts a leading
        sign, surrounding whitespace, and PEP-515 underscore grouping, all of
        which would still reach the route provider verbatim."""
        from almanak.framework.cli.ax import _lending_capacity_looks_like_address

        assert _lending_capacity_looks_like_address(bad_address) is False

    def test_whitespace_wrapped_wallet_address_is_normalized_before_use(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`_lending_capacity_looks_like_address` strips before validating, so
        a whitespace-wrapped address must not leak the whitespace through to
        the route provider's `from_address` -- the value used downstream must
        be the normalized (stripped) form, not the raw operator input."""
        seen_from_addresses: list[str] = []

        def get_route(request, timeout):
            seen_from_addresses.append(request.from_address)
            return _passing_route(request, timeout)

        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(100_000 * 10**6), total_borrow_assets=str(10_000 * 10**6)
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=get_route,
        )
        result = _invoke(runner, "--json", wallet_address=f"  {_WALLET}  ")
        assert result.exit_code == 0, result.output
        assert seen_from_addresses == [_WALLET, _WALLET]


class TestExhaustedLiquidityHumanRender:
    """available_liquidity_base <= 0 reports utilization_bps=None while
    liquidity_measured stays True -- the renderer must not print that as a
    numeric "None bps", in exactly the FAIL case an operator most needs to
    read correctly."""

    def test_zero_available_liquidity_human_render_has_no_none_bps(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(10 * 10**6), total_borrow_assets=str(10 * 10**6)
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=_passing_route,
        )
        result = _invoke(runner)
        assert result.exit_code == 1, result.output
        assert "None bps" not in result.output
        assert "no available liquidity" in result.output


class TestPriceImpactCaveat:
    """The route provider's price_impact field is nullable and the gateway
    currently collapses an absent value into a measured zero -- the report
    must carry that caveat rather than implying 0bps is always measured."""

    def test_report_carries_price_impact_caveat_note(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(100_000 * 10**6), total_borrow_assets=str(10_000 * 10**6)
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=_passing_route,
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert any("price_impact_bps" in note and "nullable" in note for note in payload["notes"])

    def test_human_render_also_surfaces_the_caveat(self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
        """The JSON `notes` field alone is invisible to an operator running
        this command without --json -- the caveat must reach both surfaces."""
        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(100_000 * 10**6), total_borrow_assets=str(10_000 * 10**6)
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=_passing_route,
        )
        result = _invoke(runner)
        assert result.exit_code == 0, result.output
        assert "note:" in result.output
        assert "price_impact_bps" in result.output


class TestNonGrpcExceptionGuards:
    """A local CLI fault (stub construction, proto encoding, interceptor
    failure) must never escape as Click's default exit 1 -- that exit code is
    reserved for the authoritative on-chain NOT_FOUND. Mirrors the sibling
    `ax lending-market` command's exact handling, which already catches
    non-grpc.RpcError exceptions the same way."""

    def test_verify_market_non_grpc_exception_exits_unavailable(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def raise_local_fault(request, timeout):
            raise ValueError("proto encoding failure")

        _patch_stubs(
            monkeypatch,
            get_lending_market=raise_local_fault,
            get_token_decimals=lambda request, timeout: pytest.fail("unreachable"),
            get_route=lambda request, timeout: pytest.fail("unreachable"),
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unavailable"

    def test_get_decimals_non_grpc_exception_exits_unavailable(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def raise_local_fault(request, timeout):
            raise ValueError("proto encoding failure")

        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(100_000 * 10**6), total_borrow_assets=str(10_000 * 10**6)
            ),
            get_token_decimals=raise_local_fault,
            get_route=lambda request, timeout: pytest.fail("unreachable"),
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unavailable"

    def test_get_route_non_grpc_exception_exits_unavailable(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def raise_local_fault(request, timeout):
            raise ValueError("proto encoding failure")

        _patch_stubs(
            monkeypatch,
            get_lending_market=lambda request, timeout: _market_response(
                total_supply_assets=str(100_000 * 10**6), total_borrow_assets=str(10_000 * 10**6)
            ),
            get_token_decimals=_decimals_by_token(18, 6),
            get_route=raise_local_fault,
        )
        result = _invoke(runner, "--json")
        assert result.exit_code == 4, result.output
        payload = json.loads(result.output)
        assert payload["status"] == "unavailable"
