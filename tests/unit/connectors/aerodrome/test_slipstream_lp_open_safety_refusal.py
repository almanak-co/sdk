"""Aerodrome Slipstream LP_OPEN classifies an unprotected-minimum refusal (VIB-6217).

``compute_lp_slippage_mins`` fails closed on an out-of-range
``protocol_params["lp_slippage"]`` by raising ``UnprotectedTradeError``. Before
this connector grew a dedicated handler, that exception fell through to the
function's generic ``except Exception``, which produces ``FAILED`` with
``is_safety_refusal`` left False and logs a full traceback. Two consequences,
both bad:

- The runner maps an unclassified FAILED to an ordinary fault, so it counts
  toward the circuit breaker's consecutive-failure trip. A strategy that
  correctly refuses to mint without output protection would trip ITSELF off.
- A deliberate refusal that prints a traceback reads as a crash, and a guard
  that looks like a crash is a guard someone switches off.

Scope of this file, stated honestly: it proves the HANDLER classifies correctly,
by injecting the refusal at ``_resolve_slipstream_ticks`` — the earliest point
inside the ``try`` reachable without an unstable tower of mocks. The end-to-end
path from a real ``compute_lp_slippage_mins`` raise is proven separately against
the real compiler in
``tests/unit/intents/test_compiler_swap_lp_characterization.py``
(``test_out_of_range_lp_slippage_is_classified_as_a_safety_refusal``). Neither
test alone covers both risks; together they do.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason, PoolValidationResult
from almanak.connectors.aerodrome import compiler as aerodrome_compiler
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.min_out_guard import UnprotectedTradeError
from almanak.framework.intents.vocabulary import LPOpenIntent, PriceBand

# Real Base addresses: WETH (0x42..) sorts below USDC (0x83..), which the
# compiler requires — a non-canonical order is rejected before the code under
# test is reached.
_TOKENS = {
    "WETH": ("0x4200000000000000000000000000000000000006", 18),
    "USDC": ("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913", 6),
}

_CONFIRMED_POOL = PoolValidationResult(
    exists=True,
    reason=PoolValidationReason.CONFIRMED,
    pool_address="0x1111111111111111111111111111111111111111",
)


def _intent(**overrides) -> LPOpenIntent:
    params = {
        "pool": "WETH/USDC/100",
        "amount0": Decimal("0.01"),
        "amount1": Decimal("10"),
        "range_spec": PriceBand(lower=Decimal("1000"), upper=Decimal("5000")),
        "protocol": "aerodrome_slipstream",
    }
    params.update(overrides)
    return LPOpenIntent(**params)


def _compiler() -> MagicMock:
    """Minimal stand-in: only what the prologue needs before the injection point."""
    compiler = MagicMock()
    compiler.chain = "base"
    compiler._resolve_token = lambda symbol: SimpleNamespace(
        address=_TOKENS[symbol][0], decimals=_TOKENS[symbol][1], symbol=symbol
    )
    compiler._validate_pool.return_value = None  # None == pool is fine
    return compiler


class TestSlipstreamLpOpenSafetyRefusal:
    @pytest.mark.parametrize("risk_reduction, allowed", [(False, False), (True, True)])
    def test_verifier_unavailability_preserves_only_risk_reduction(
        self,
        risk_reduction: bool,
        allowed: bool,
    ) -> None:
        compiler = SimpleNamespace(
            chain="base",
            _ctx=SimpleNamespace(venue_verification_gateway_factory=None),
        )

        result = aerodrome_compiler._verify_slipstream_binding(
            compiler=compiler,
            pool_address=_CONFIRMED_POOL.pool_address,
            token0_address=_TOKENS["WETH"][0],
            token1_address=_TOKENS["USDC"][0],
            tick_spacing=100,
            expected_position_manager="0x" + "44" * 20,
            intent_id="intent-1",
            allow_unavailable_for_risk_reduction=risk_reduction,
        )

        if allowed:
            assert result is None
        else:
            assert isinstance(result, aerodrome_compiler.CompilationResult)
            assert result.is_safety_refusal is True

    def test_verifier_registry_failure_preserves_only_risk_reduction(self) -> None:
        compiler = SimpleNamespace(
            chain="base",
            _ctx=SimpleNamespace(venue_verification_gateway_factory=lambda: MagicMock()),
        )

        with patch(
            "almanak.connectors._strategy_base.venue_verifier_registry.VenueVerifierRegistry",
            side_effect=ImportError("broken provider"),
        ):
            result = aerodrome_compiler._verify_slipstream_binding(
                compiler=compiler,
                pool_address=_CONFIRMED_POOL.pool_address,
                token0_address=_TOKENS["WETH"][0],
                token1_address=_TOKENS["USDC"][0],
                tick_spacing=100,
                expected_position_manager="0x" + "44" * 20,
                intent_id="intent-1",
                allow_unavailable_for_risk_reduction=True,
            )

        assert result is None

    def test_exact_venue_refusal_precedes_adapter_approval_and_mint(self) -> None:
        """A named pool must be admitted before the money path is constructed."""

        compiler = _compiler()
        compiler._gateway_client = None
        compiler._get_chain_rpc_url.return_value = "http://localhost:8545"
        compiler._fetch_lp_pool_slot0.return_value = (2**96, 50)
        compiler.default_lp_slippage = Decimal("0.99")
        compiler.wallet_address = "0x" + "33" * 20
        compiler.price_oracle = {}
        refusal = aerodrome_compiler.CompilationResult(
            status=CompilationStatus.FAILED,
            error="factory mismatch",
            is_safety_refusal=True,
            intent_id="refusal",
        )

        with (
            patch(
                "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool",
                return_value=_CONFIRMED_POOL,
            ),
            patch.object(aerodrome_compiler, "_resolve_slipstream_ticks", return_value=(0, 100)),
            patch.object(
                aerodrome_compiler,
                "maybe_recompute_lp_amounts_from_slot0",
                return_value=(10**16, 10_000_000),
            ),
            patch.object(aerodrome_compiler, "compute_lp_slippage_mins", return_value=(1, 1)),
            patch.object(aerodrome_compiler, "_verify_slipstream_binding", return_value=refusal) as verify,
            patch("almanak.connectors.aerodrome.AerodromeAdapter") as adapter_cls,
        ):
            result = aerodrome_compiler.compile_lp_open_aerodrome_slipstream(
                compiler,
                _intent(),
            )

        assert verify.called
        assert result is refusal
        adapter_cls.assert_not_called()

    def test_unprotected_trade_error_is_classified_as_a_safety_refusal(self) -> None:
        """FAILED alone is not enough — the classification is the thing under test.

        Asserting only ``status is FAILED`` would pass against the generic
        handler too, which is exactly the defect this test exists to catch.
        """
        with (
            patch(
                "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool",
                return_value=_CONFIRMED_POOL,
            ),
            patch.object(
                aerodrome_compiler,
                "_resolve_slipstream_ticks",
                side_effect=UnprotectedTradeError(
                    "lp mint", "lp_slippage must be in [0, 1) (got 5)"
                ),
            ) as injected,
        ):
            result = aerodrome_compiler.compile_lp_open_aerodrome_slipstream(
                _compiler(), _intent()
            )

        assert injected.called, (
            "injection point was never reached — this test would be vacuous"
        )
        assert result.status == CompilationStatus.FAILED
        assert result.is_safety_refusal is True, (
            "an unprotected-minimum refusal must be classified as a safety refusal, "
            "not an ordinary fault, or it counts toward the circuit breaker"
        )
        assert "lp_slippage" in (result.error or "")

    def test_ordinary_failures_are_still_faults_not_refusals(self) -> None:
        """The narrow handler must not swallow unrelated errors into 'safety'.

        Marking every failure a safety refusal would be the mirror-image defect:
        real faults would stop counting toward the circuit breaker.
        """
        with (
            patch(
                "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool",
                return_value=_CONFIRMED_POOL,
            ),
            patch.object(
                aerodrome_compiler,
                "_resolve_slipstream_ticks",
                side_effect=RuntimeError("pool read failed"),
            ),
        ):
            result = aerodrome_compiler.compile_lp_open_aerodrome_slipstream(
                _compiler(), _intent()
            )

        assert result.status == CompilationStatus.FAILED
        assert result.is_safety_refusal is False

    def test_refusal_does_not_log_a_traceback(self, caplog: pytest.LogCaptureFixture) -> None:
        """A deliberate refusal logged with a traceback reads as a crash.

        ``logger.exception`` attaches exc_info; ``logger.error`` does not. This is
        the difference between an operator seeing "the guard refused" and seeing
        "the compiler blew up", and it decides whether the guard survives triage.
        """
        with (
            caplog.at_level(logging.ERROR, logger=aerodrome_compiler.logger.name),
            patch(
                "almanak.connectors.aerodrome.pool_validation.validate_aerodrome_cl_pool",
                return_value=_CONFIRMED_POOL,
            ),
            patch.object(
                aerodrome_compiler,
                "_resolve_slipstream_ticks",
                side_effect=UnprotectedTradeError(
                    "lp mint", "lp_slippage must be in [0, 1) (got 5)"
                ),
            ),
        ):
            aerodrome_compiler.compile_lp_open_aerodrome_slipstream(_compiler(), _intent())

        refusals = [r for r in caplog.records if "without output protection" in r.getMessage()]
        assert refusals, f"expected a refusal log line, got: {[r.getMessage() for r in caplog.records]}"
        assert all(r.exc_info is None for r in refusals), (
            "refusal was logged with a traceback (logger.exception) — it will be "
            "triaged as a crash rather than as the guard working"
        )
