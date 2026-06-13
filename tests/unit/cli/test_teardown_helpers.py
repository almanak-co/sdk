"""Unit tests for ``almanak/framework/cli/teardown_helpers.py``.

Pins the pure-helper contracts extracted out of ``execute_teardown`` during
the cli/teardown CC=89 → 14 refactor. The composite helpers (setup_gateway,
setup_solana_fork, instantiate_strategy_with_state, discover_positions,
build_teardown_machinery, run_teardown_with_brackets) are exercised
end-to-end by the existing teardown CLI integration tests under
``tests/unit/teardown/test_teardown_cli_*`` and ``tests/unit/cli/test_teardown_cli_*``
— this file covers the stateless helpers where direct unit tests give
sharper signal than CLI replay.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import click
import pytest
from click.testing import CliRunner

from almanak.framework.cli import teardown_helpers as th

# ──────────────────────────────────────────────────────────────────────────────
# validate_teardown_options
# ──────────────────────────────────────────────────────────────────────────────


class TestValidateTeardownOptions:
    """The conflict between --no-gateway and --network was inline in the CLI
    body before the refactor; the test pins the verbatim error message so a
    reformatting mistake doesn't drop the operator-friendly remediation."""

    def test_no_gateway_with_network_raises(self):
        with pytest.raises(click.ClickException) as exc:
            th.validate_teardown_options(no_gateway=True, network="anvil")
        assert "--network only applies when the managed gateway is auto-started" in str(exc.value.message)

    def test_no_gateway_with_no_network_passes(self):
        th.validate_teardown_options(no_gateway=True, network=None)

    def test_gateway_with_network_passes(self):
        th.validate_teardown_options(no_gateway=False, network="mainnet")

    def test_gateway_without_network_passes(self):
        th.validate_teardown_options(no_gateway=False, network=None)


# ──────────────────────────────────────────────────────────────────────────────
# load_strategy_config_dict
# ──────────────────────────────────────────────────────────────────────────────


class TestLoadStrategyConfigDict:
    """Auto-discovery order is (config.json, config.yaml, config.yml). The
    explicit -c flag wins. Missing/no config returns ({}, None) — the same
    fallback execute_teardown had inline."""

    def test_explicit_json_path(self, tmp_path: Path):
        cfg = tmp_path / "custom.json"
        cfg.write_text('{"chain": "arbitrum"}')
        result, used = th.load_strategy_config_dict(tmp_path, str(cfg))
        assert result == {"chain": "arbitrum"}
        assert used == str(cfg)

    def test_explicit_yaml_path(self, tmp_path: Path):
        cfg = tmp_path / "custom.yaml"
        cfg.write_text("chain: base\n")
        result, used = th.load_strategy_config_dict(tmp_path, str(cfg))
        assert result == {"chain": "base"}
        assert used == str(cfg)

    def test_autodiscover_json_first(self, tmp_path: Path):
        # All three exist — config.json wins.
        (tmp_path / "config.json").write_text('{"src": "json"}')
        (tmp_path / "config.yaml").write_text("src: yaml\n")
        (tmp_path / "config.yml").write_text("src: yml\n")
        result, used = th.load_strategy_config_dict(tmp_path, None)
        assert result == {"src": "json"}
        assert used.endswith("config.json")

    def test_autodiscover_yaml_when_json_missing(self, tmp_path: Path):
        (tmp_path / "config.yaml").write_text("src: yaml\n")
        result, used = th.load_strategy_config_dict(tmp_path, None)
        assert result == {"src": "yaml"}
        assert used.endswith("config.yaml")

    def test_no_config_file_returns_empty_dict_and_none(self, tmp_path: Path):
        result, used = th.load_strategy_config_dict(tmp_path, None)
        assert result == {}
        assert used is None

    def test_empty_yaml_returns_empty_dict(self, tmp_path: Path):
        (tmp_path / "config.yaml").write_text("")
        result, used = th.load_strategy_config_dict(tmp_path, None)
        assert result == {}
        assert used.endswith("config.yaml")


# ──────────────────────────────────────────────────────────────────────────────
# resolve_wallet_address
# ──────────────────────────────────────────────────────────────────────────────


class TestResolveWalletAddress:
    """``config.wallet_address`` wins over the env private key. Returns
    None when neither is set — caller decides whether that's fatal (it
    is, at strategy instantiation time, but managed-gateway boot can
    still survive without one)."""

    def test_config_address_wins(self):
        cfg = {"wallet_address": "0xabc"}
        env = {"ALMANAK_PRIVATE_KEY": "0x" + "a" * 64}
        assert th.resolve_wallet_address(cfg, env) == "0xabc"

    def test_env_private_key_fallback(self):
        # Anvil account 0 — well-known deterministic key.
        env = {"ALMANAK_PRIVATE_KEY": ("0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")}
        result = th.resolve_wallet_address({}, env)
        # Anvil account 0 address (case-insensitive checksum compare).
        assert result is not None
        assert result.lower() == "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"

    def test_neither_returns_none(self):
        assert th.resolve_wallet_address({}, {}) is None

    def test_missing_env_var_returns_none(self):
        assert th.resolve_wallet_address({}, {"OTHER": "x"}) is None


# ──────────────────────────────────────────────────────────────────────────────
# display_position_summary + display_unknown_value_warning
# ──────────────────────────────────────────────────────────────────────────────


def _pos(*, position_id="p1", value=Decimal("100"), unknown=False, health=None, ptype="LP"):
    """Build a duck-typed PositionInfo for the display helpers."""
    return SimpleNamespace(
        position_type=SimpleNamespace(value=ptype),
        protocol="aerodrome",
        chain="base",
        position_id=position_id,
        value_usd=value,
        health_factor=health,
        details={"value_usd_unknown": True} if unknown else {},
    )


class TestDisplayPositionSummary:
    def test_priced_positions_total_correctly(self):
        runner = CliRunner()
        positions = SimpleNamespace(
            positions=[
                _pos(position_id="a", value=Decimal("100")),
                _pos(position_id="b", value=Decimal("250.50")),
            ]
        )
        with runner.isolation() as (out, _err):
            total, unknown = th.display_position_summary(positions)
        assert total == Decimal("350.50")
        assert unknown == 0

    def test_unknown_value_count_increments(self):
        runner = CliRunner()
        positions = SimpleNamespace(
            positions=[
                _pos(position_id="a", value=Decimal("0"), unknown=True),
                _pos(position_id="b", value=Decimal("0"), unknown=True),
                _pos(position_id="c", value=Decimal("100")),
            ]
        )
        with runner.isolation() as (out, _err):
            total, unknown = th.display_position_summary(positions)
        assert unknown == 2
        assert total == Decimal("100")

    def test_health_factor_printed_when_present(self):
        runner = CliRunner()
        positions = SimpleNamespace(positions=[_pos(health=Decimal("1.85"))])
        with runner.isolation() as (out, _err):
            th.display_position_summary(positions)
            output = out.getvalue().decode()
        assert "Health Factor: 1.85" in output


class TestDisplayUnknownValueWarning:
    def test_zero_count_emits_nothing(self):
        runner = CliRunner()
        with runner.isolation() as (out, _err):
            th.display_unknown_value_warning(0)
            assert out.getvalue() == b""

    def test_nonzero_count_emits_warning(self):
        runner = CliRunner()
        with runner.isolation() as (out, _err):
            th.display_unknown_value_warning(3)
            output = out.getvalue().decode()
        assert "WARNING" in output
        assert "3 position(s) discovered without USD pricing" in output
        assert "MOST PERMISSIVE" in output  # SafetyGuard wording preserved


# ──────────────────────────────────────────────────────────────────────────────
# prompt_teardown_confirmation
# ──────────────────────────────────────────────────────────────────────────────


class TestPromptTeardownConfirmation:
    def test_force_short_circuits_to_true(self):
        # No prompt fires — passing input="" doesn't matter.
        runner = CliRunner()
        with runner.isolation():
            assert th.prompt_teardown_confirmation(force=True) is True

    def test_user_yes_returns_true(self):
        runner = CliRunner()
        with runner.isolation(input="y\n"):
            assert th.prompt_teardown_confirmation(force=False) is True

    def test_user_no_returns_false(self):
        runner = CliRunner()
        with runner.isolation(input="n\n") as (out, _err):
            assert th.prompt_teardown_confirmation(force=False) is False
            assert "Teardown cancelled" in out.getvalue().decode()


# ──────────────────────────────────────────────────────────────────────────────
# print_no_op_if_empty_and_signal_return
# ──────────────────────────────────────────────────────────────────────────────


class TestPrintNoOpIfEmpty:
    """VIB-3705: empty positions → print canonical no-op success message
    (+ tip about --discover when not already in --discover mode) and
    return True so the caller can return."""

    def test_non_empty_positions_returns_false_silently(self):
        positions = SimpleNamespace(positions=[_pos()])
        runner = CliRunner()
        with runner.isolation() as (out, _err):
            assert (
                th.print_no_op_if_empty_and_signal_return(
                    positions=positions,
                    strategy=SimpleNamespace(deployment_id="x"),
                    strategy_class=type("X", (), {}),
                    discover=False,
                    no_op_message_builder=lambda sid: f"NOOP {sid}",
                )
                is False
            )
            assert out.getvalue() == b""

    def test_empty_positions_no_discover_prints_message_and_tip(self):
        positions = SimpleNamespace(positions=[])
        runner = CliRunner()
        with runner.isolation() as (out, _err):
            result = th.print_no_op_if_empty_and_signal_return(
                positions=positions,
                strategy=SimpleNamespace(deployment_id="my-strat"),
                strategy_class=type("X", (), {}),
                discover=False,
                no_op_message_builder=lambda sid: f"no-op for {sid}",
            )
            output = out.getvalue().decode()
        assert result is True
        assert "no-op for my-strat" in output
        assert "rerun with --discover" in output  # Tip only when not in discover mode

    def test_empty_positions_with_discover_omits_tip(self):
        positions = SimpleNamespace(positions=[])
        runner = CliRunner()
        with runner.isolation() as (out, _err):
            th.print_no_op_if_empty_and_signal_return(
                positions=positions,
                strategy=SimpleNamespace(deployment_id="x"),
                strategy_class=type("X", (), {}),
                discover=True,
                no_op_message_builder=lambda sid: f"no-op for {sid}",
            )
            output = out.getvalue().decode()
        assert "rerun with --discover" not in output

    def test_deployment_id_is_required(self):
        class FallbackStrategy:
            pass

        runner = CliRunner()
        with runner.isolation() as (out, _err):
            with pytest.raises(AttributeError):
                th.print_no_op_if_empty_and_signal_return(
                    positions=SimpleNamespace(positions=[]),
                    strategy=FallbackStrategy(),
                    strategy_class=FallbackStrategy,
                    discover=False,
                    no_op_message_builder=lambda sid: f"no-op for {sid}",
                )
            assert out.getvalue() == b""


# ──────────────────────────────────────────────────────────────────────────────
# build_market_and_oracle
# ──────────────────────────────────────────────────────────────────────────────


class TestBuildMarketAndOracle:
    def test_strategy_without_create_market_snapshot_returns_none(self):
        result = th.build_market_and_oracle(strategy=SimpleNamespace())
        assert result == (None, None)

    def test_market_without_get_price_oracle_dict(self):
        market = SimpleNamespace()  # no get_price_oracle_dict
        strategy = SimpleNamespace(create_market_snapshot=lambda: market)
        m, oracle = th.build_market_and_oracle(strategy=strategy)
        assert m is market
        assert oracle is None

    def test_full_oracle_path(self):
        oracle_dict = {"USDC": Decimal("1"), "WETH": Decimal("3000")}
        market = SimpleNamespace(get_price_oracle_dict=lambda: oracle_dict)
        strategy = SimpleNamespace(create_market_snapshot=lambda: market)
        runner = CliRunner()
        with runner.isolation() as (out, _err):
            m, oracle = th.build_market_and_oracle(strategy=strategy)
            output = out.getvalue().decode()
        assert m is market
        assert oracle == oracle_dict
        assert "Using real prices for 2 tokens" in output

    def test_oracle_dict_returning_none_yields_none(self):
        market = SimpleNamespace(get_price_oracle_dict=lambda: None)
        strategy = SimpleNamespace(create_market_snapshot=lambda: market)
        m, oracle = th.build_market_and_oracle(strategy=strategy)
        assert m is market
        assert oracle is None

    def test_exception_logged_and_returns_none(self):
        def boom():
            raise RuntimeError("boom")

        strategy = SimpleNamespace(create_market_snapshot=boom)
        runner = CliRunner()
        with runner.isolation() as (out, _err):
            m, oracle = th.build_market_and_oracle(strategy=strategy)
            output = out.getvalue().decode()
        assert m is None
        assert oracle is None
        assert "Could not get market prices" in output
        assert "using placeholders" in output


# ──────────────────────────────────────────────────────────────────────────────
# generate_teardown_intents_for_cli
# ──────────────────────────────────────────────────────────────────────────────


class TestGenerateTeardownIntentsForCli:
    def test_strategy_path_passes_market_kwarg(self):
        from almanak.framework.teardown import TeardownMode

        market_sentinel = object()
        observed_kwargs = {}

        def fake_generate(internal_mode, **kw):
            observed_kwargs["internal_mode"] = internal_mode
            observed_kwargs.update(kw)
            return [SimpleNamespace(intent_type=SimpleNamespace(value="LP_CLOSE"))]

        strategy = SimpleNamespace(generate_teardown_intents=fake_generate)
        runner = CliRunner()
        with runner.isolation() as (out, _err):
            intents = th.generate_teardown_intents_for_cli(
                strategy=strategy,
                mode_str="graceful",
                market=market_sentinel,
                discover=False,
                positions=SimpleNamespace(positions=[]),
            )
        assert len(intents) == 1
        assert observed_kwargs["internal_mode"] == TeardownMode.SOFT
        assert observed_kwargs["market"] is market_sentinel

    def test_legacy_strategy_typeerror_falls_back_to_no_market(self):
        from almanak.framework.teardown import TeardownMode

        calls: list[dict] = []

        def fake_generate(internal_mode, **kw):
            calls.append({"internal_mode": internal_mode, **kw})
            if "market" in kw:
                raise TypeError("unexpected keyword argument 'market'")
            return [SimpleNamespace(intent_type="LP_CLOSE")]

        strategy = SimpleNamespace(generate_teardown_intents=fake_generate)
        runner = CliRunner()
        with runner.isolation():
            intents = th.generate_teardown_intents_for_cli(
                strategy=strategy,
                mode_str="emergency",
                market=object(),
                discover=False,
                positions=SimpleNamespace(positions=[]),
            )
        # Two calls — first with market kwarg (raises), second without.
        assert len(calls) == 2
        assert "market" in calls[0]
        assert "market" not in calls[1]
        assert calls[0]["internal_mode"] == TeardownMode.HARD
        assert len(intents) == 1

    def test_unrelated_typeerror_re_raised(self):
        def fake_generate(internal_mode, **kw):
            raise TypeError("totally different complaint")

        strategy = SimpleNamespace(generate_teardown_intents=fake_generate)
        runner = CliRunner()
        with runner.isolation():
            with pytest.raises(click.ClickException) as exc:
                th.generate_teardown_intents_for_cli(
                    strategy=strategy,
                    mode_str="graceful",
                    market=None,
                    discover=False,
                    positions=SimpleNamespace(positions=[]),
                )
            # Wrapped in ClickException by the outer try.
            assert "Failed to generate teardown intents" in str(exc.value.message)

    def test_discover_mode_synthesizes_lp_close_intents(self):
        from almanak.framework.teardown import PositionType

        positions = SimpleNamespace(
            positions=[
                SimpleNamespace(
                    position_type=PositionType.LP,
                    position_id="lp1",
                    protocol="uniswap_v3",
                    chain="ethereum",
                ),
                SimpleNamespace(
                    position_type=PositionType.LP,
                    position_id="lp2",
                    protocol="aerodrome",
                    chain="base",
                ),
                # Non-LP filtered out.
                SimpleNamespace(
                    position_type=PositionType.BORROW,
                    position_id="lend1",
                    protocol="aave_v3",
                    chain="ethereum",
                ),
            ]
        )
        runner = CliRunner()
        with runner.isolation():
            intents = th.generate_teardown_intents_for_cli(
                strategy=SimpleNamespace(),
                mode_str="graceful",
                market=None,
                discover=True,
                positions=positions,
            )
        assert len(intents) == 2  # Lending position filtered out
        assert {i.position_id for i in intents} == {"lp1", "lp2"}
        # Graceful → collect_fees=True
        assert all(i.collect_fees is True for i in intents)

    def test_emergency_mode_disables_collect_fees(self):
        from almanak.framework.teardown import PositionType

        positions = SimpleNamespace(
            positions=[
                SimpleNamespace(
                    position_type=PositionType.LP,
                    position_id="lp1",
                    protocol="uniswap_v3",
                    chain="ethereum",
                )
            ]
        )
        runner = CliRunner()
        with runner.isolation():
            intents = th.generate_teardown_intents_for_cli(
                strategy=SimpleNamespace(),
                mode_str="emergency",
                market=None,
                discover=True,
                positions=positions,
            )
        assert intents[0].collect_fees is False


# ──────────────────────────────────────────────────────────────────────────────
# display_teardown_result
# ──────────────────────────────────────────────────────────────────────────────


def _result(
    *,
    success=True,
    intents_total=2,
    intents_succeeded=2,
    intents_failed=0,
    duration=10.0,
    starting=Decimal("100"),
    final=Decimal("99"),
    costs=Decimal("1"),
    error=None,
    positions_total=0,
    positions_closed=0,
    has_position_breakdown=False,
):
    # VIB-5085: the double mirrors TeardownResult's position fields. Default
    # ``has_position_breakdown=False`` exercises the intent-count fallback path.
    return SimpleNamespace(
        success=success,
        intents_total=intents_total,
        intents_succeeded=intents_succeeded,
        intents_failed=intents_failed,
        duration_seconds=duration,
        starting_value_usd=starting,
        final_value_usd=final,
        total_costs_usd=costs,
        error=error,
        positions_total=positions_total,
        positions_closed=positions_closed,
        has_position_breakdown=has_position_breakdown,
    )


class TestDisplayTeardownResult:
    def test_success_path_shows_success_banner(self):
        runner = CliRunner()
        with runner.isolation() as (out, _err):
            th.display_teardown_result(_result(), "my-strat", lambda sid: f"NOOP {sid}")
            output = out.getvalue().decode()
        assert "[SUCCESS] Teardown completed successfully!" in output
        assert "Intents executed: 2/2" in output
        assert "Starting value: $100.00" in output

    def test_failure_path_shows_error(self):
        runner = CliRunner()
        with runner.isolation() as (out, _err):
            th.display_teardown_result(
                _result(success=False, intents_failed=1, error="oops"),
                "my-strat",
                lambda sid: f"NOOP {sid}",
            )
            output = out.getvalue().decode()
        assert "[FAILED] Teardown failed: oops" in output
        assert "Intents failed: 1" in output

    def test_no_op_path_uses_canonical_message(self):
        # VIB-3705: success + intents_total=0 → no-op message instead of
        # "Teardown completed successfully" banner.
        runner = CliRunner()
        with runner.isolation() as (out, _err):
            th.display_teardown_result(
                _result(intents_total=0, intents_succeeded=0),
                "my-strat",
                lambda sid: f"NO POSITIONS for {sid}",
            )
            output = out.getvalue().decode()
        assert "NO POSITIONS for my-strat" in output
        assert "[SUCCESS]" not in output  # The success banner should NOT fire
        assert "[FAILED]" not in output


# ──────────────────────────────────────────────────────────────────────────────
# update_teardown_requests_lifecycle (VIB-3920)
# ──────────────────────────────────────────────────────────────────────────────


class TestUpdateTeardownRequestsLifecycle:
    def test_creates_request_when_none_exists(self):
        # NOTE: passes mode="SOFT" (the valid TeardownMode enum value), NOT
        # the CLI-facing "graceful" / "emergency". See
        # ``test_create_path_swallows_invalid_mode_value`` for the
        # pre-existing bug that makes the create path always fail when the
        # CLI's mode string flows in unchanged.
        from almanak.framework.teardown import TeardownStatus

        tsm = MagicMock()
        tsm.get_active_request.return_value = None

        th.update_teardown_requests_lifecycle(
            deployment_id="my-strat",
            mode="SOFT",
            result=_result(success=True, intents_total=3, intents_succeeded=3),
            state_manager_provider=lambda: tsm,
        )

        tsm.create_request.assert_called_once()
        created = tsm.create_request.call_args.args[0]
        assert created.deployment_id == "my-strat"
        assert created.requested_by == "cli-execute"
        assert created.target_token == "USDC"
        # And then update_request was called with success status
        tsm.update_request.assert_called_once()
        updated = tsm.update_request.call_args.args[0]
        assert updated.status == TeardownStatus.COMPLETED
        assert updated.positions_closed == 3

    def test_create_path_swallows_invalid_mode_value(self, caplog):
        """Documents the pre-existing VIB-3920 bug: ``TeardownMode`` enum
        values are ``SOFT`` / ``HARD``, but the CLI passes the user-facing
        ``"graceful"`` / ``"emergency"`` mode string. ``_TM(mode)`` on the
        create-path raises ``ValueError`` and the function's blanket
        ``try/except Exception`` swallows it — silently. The result: no
        ``teardown_requests`` row ever exists for execute-lane teardowns
        unless the request lane created one first.

        Pinned here so a future ``mode`` normalization (or a fix to this
        bug) is a deliberate, traceable change rather than an accidental
        side effect of touching the helper.
        """
        tsm = MagicMock()
        tsm.get_active_request.return_value = None

        with caplog.at_level("DEBUG", logger="almanak.framework.cli.teardown_helpers"):
            th.update_teardown_requests_lifecycle(
                deployment_id="my-strat",
                mode="graceful",  # CLI value — not a valid TeardownMode
                result=_result(),
                state_manager_provider=lambda: tsm,
            )

        # Create was attempted but the row was never persisted.
        tsm.create_request.assert_not_called()
        tsm.update_request.assert_not_called()
        assert any("failed to update teardown_requests" in r.message for r in caplog.records)

    def test_updates_existing_request(self):
        from almanak.framework.teardown import TeardownStatus

        existing = SimpleNamespace(
            positions_total=5,
            positions_closed=0,
            positions_failed=0,
            completed_at=None,
            status=None,
        )
        tsm = MagicMock()
        tsm.get_active_request.return_value = existing

        th.update_teardown_requests_lifecycle(
            deployment_id="my-strat",
            mode="emergency",
            result=_result(intents_total=3, intents_succeeded=2, intents_failed=1, success=False),
            state_manager_provider=lambda: tsm,
        )

        tsm.create_request.assert_not_called()
        tsm.update_request.assert_called_once_with(existing)
        # max(5, 3) = 5 — preserves the higher count from the request lane
        assert existing.positions_total == 5
        assert existing.positions_closed == 2
        assert existing.positions_failed == 1
        assert existing.status == TeardownStatus.FAILED

    def test_updates_existing_request_prefers_verified_positions(self):
        """VIB-5085: when ``execute()`` stamped a verified position breakdown
        onto the result, the lifecycle writer reports *positions* — not the
        intent count. Field-report shape: 2 positions closed via 6 intents."""
        from almanak.framework.teardown import TeardownStatus

        existing = SimpleNamespace(
            positions_total=0,
            positions_closed=0,
            positions_failed=0,
            completed_at=None,
            status=None,
        )
        tsm = MagicMock()
        tsm.get_active_request.return_value = existing

        th.update_teardown_requests_lifecycle(
            deployment_id="my-strat",
            mode="SOFT",
            result=_result(
                success=True,
                intents_total=6,
                intents_succeeded=6,
                positions_total=2,
                positions_closed=2,
                has_position_breakdown=True,
            ),
            state_manager_provider=lambda: tsm,
        )

        tsm.update_request.assert_called_once_with(existing)
        # 2 positions, NOT 6 intents.
        assert existing.positions_total == 2
        assert existing.positions_closed == 2
        assert existing.positions_failed == 0
        assert existing.status == TeardownStatus.COMPLETED
        assert existing.completed_at is not None

    def test_swallows_state_manager_failure(self):
        # VIB-3920 contract: bookkeeping never blocks CLI exit.
        def boom():
            raise RuntimeError("DB unreachable")

        # Must not propagate.
        th.update_teardown_requests_lifecycle(
            deployment_id="my-strat",
            mode="graceful",
            result=_result(),
            state_manager_provider=boom,
        )


# ──────────────────────────────────────────────────────────────────────────────
# SolanaForkHandle
# ──────────────────────────────────────────────────────────────────────────────


class TestSolanaForkHandle:
    """Idempotency is the whole point — atexit safety-net + finally cleanup
    must never double-stop the validator (would re-enter a closed event loop
    on Py 3.12+)."""

    def test_stop_marks_handle_stopped(self):
        fork_mgr = MagicMock()
        fork_mgr.stop = MagicMock(return_value=_async_noop())
        handle = th.SolanaForkHandle(fork_mgr)
        assert handle.stopped is False
        handle.stop()
        assert handle.stopped is True

    def test_double_stop_is_noop(self):
        fork_mgr = MagicMock()
        fork_mgr.stop = MagicMock(return_value=_async_noop())
        handle = th.SolanaForkHandle(fork_mgr)
        handle.stop()
        handle.stop()  # Should not raise, should not call fork_mgr.stop again.
        assert fork_mgr.stop.call_count == 1

    def test_stop_swallows_exception_when_swallow_true(self):
        async def _raises():
            raise RuntimeError("validator already gone")

        fork_mgr = SimpleNamespace(stop=lambda: _raises())
        handle = th.SolanaForkHandle(fork_mgr)
        # Default swallow=True.
        handle.stop()
        assert handle.stopped is True

    def test_stop_logs_debug_when_swallow_false(self, caplog):
        async def _raises():
            raise RuntimeError("validator already gone")

        fork_mgr = SimpleNamespace(stop=lambda: _raises())
        handle = th.SolanaForkHandle(fork_mgr)
        # swallow=False — debug log fires; exception still suppressed.
        with caplog.at_level("DEBUG", logger="almanak.framework.cli.teardown_helpers"):
            handle.stop(swallow=False)
        assert handle.stopped is True
        # Either logged at debug or surfaced in caplog text.
        assert any("Failed to stop solana-test-validator" in r.message for r in caplog.records)


async def _async_noop():
    """Tiny coroutine that returns None — stand-in for awaitable mocks."""
    return None


# ──────────────────────────────────────────────────────────────────────────────
# cleanup_teardown_resources
# ──────────────────────────────────────────────────────────────────────────────


class TestCleanupTeardownResources:
    """Order matches the original execute_teardown finally block — pinned
    so a refactor doesn't accidentally swap, e.g., disconnect-before-channel-
    reset (which would trip the "use channel after disconnect" logger)."""

    def test_full_cleanup_order(self):
        calls: list[str] = []
        resolver = MagicMock(set_gateway_channel=lambda v: calls.append(f"channel={v}"))
        gateway_client = MagicMock(disconnect=lambda: calls.append("disconnect"))

        class _Handle:
            stopped = False

            def stop(self_, **kw):
                calls.append(f"solana_stop({kw.get('swallow')},{kw.get('echo_on_success')})")

        managed_gateway = MagicMock(stop=lambda: calls.append("managed_gateway_stop"))

        th.cleanup_teardown_resources(
            resolver=resolver,
            gateway_client=gateway_client,
            solana_handle=_Handle(),
            managed_gateway=managed_gateway,
        )

        assert calls == [
            "channel=None",
            "disconnect",
            "solana_stop(False,True)",
            "managed_gateway_stop",
        ]

    def test_no_solana_handle_skipped(self):
        calls: list[str] = []
        resolver = MagicMock(set_gateway_channel=lambda v: calls.append(f"channel={v}"))
        gateway_client = MagicMock(disconnect=lambda: calls.append("disconnect"))
        managed_gateway = MagicMock(stop=lambda: calls.append("managed_gateway_stop"))

        th.cleanup_teardown_resources(
            resolver=resolver,
            gateway_client=gateway_client,
            solana_handle=None,
            managed_gateway=managed_gateway,
        )

        assert calls == ["channel=None", "disconnect", "managed_gateway_stop"]

    def test_no_managed_gateway_skipped(self):
        # --no-gateway path — managed_gateway is None, not stopped.
        calls: list[str] = []
        resolver = MagicMock(set_gateway_channel=lambda v: calls.append(f"channel={v}"))
        gateway_client = MagicMock(disconnect=lambda: calls.append("disconnect"))

        th.cleanup_teardown_resources(
            resolver=resolver,
            gateway_client=gateway_client,
            solana_handle=None,
            managed_gateway=None,
        )

        assert calls == ["channel=None", "disconnect"]
