"""Cross-chain USD gas liability with measured native prices and timestamps.

Fixtures use representative prices to exercise USD conversion independently of
provider acquisition; enabled caps must still enforce quote freshness.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from almanak.framework.execution.gas.constants import SANE_GWEI_CEILING
from almanak.framework.execution.interfaces import TransactionType, UnsignedTransaction
from almanak.framework.execution.orchestrator import ExecutionOrchestrator, TransactionRiskConfig

# Mocked native prices per chain (USD). Snapshot from 2026-05-27 mainnet
# sweep (PR #2476 investigation transcripts). The values are deliberately
# rounded to two significant digits — the test asserts band ranges, not
# exact arithmetic, so prices drifting a few percent under maintenance
# don't break the suite.
MOCKED_NATIVE_PRICES_USD: dict[str, float] = {
    "ethereum": 1992.0,
    "arbitrum": 1992.0,  # native = ETH
    "optimism": 1992.0,
    "base": 1992.0,
    "linea": 1992.0,
    "blast": 1992.0,
    "polygon": 0.087,
    "bsc": 634.0,
    "avalanche": 8.82,
    "mantle": 0.50,
    "sonic": 0.040,
    "berachain": 0.34,
    "plasma": 0.085,
    "xlayer": 50.0,  # OKB rough estimate
    "zerog": 5.0,  # A0GI rough estimate
    "monad": 5.0,  # MON rough estimate
}

# Standard transaction profile for the implicit-cap test.
STD_GAS_LIMIT: int = 150_000
USD_CAP: float = 25.0


def _make_tx(*, chain_id: int, gas_price_gwei: float, gas_limit: int = STD_GAS_LIMIT) -> UnsignedTransaction:
    """Build a minimal EIP-1559 transaction for guard testing."""
    gas_price_wei = int(gas_price_gwei * 10**9)
    return UnsignedTransaction(
        to="0x0000000000000000000000000000000000000000",
        value=0,
        data="0x",
        chain_id=chain_id,
        gas_limit=gas_limit,
        max_fee_per_gas=gas_price_wei,
        max_priority_fee_per_gas=1_000,
        tx_type=TransactionType.EIP_1559,
    )


def _invoke_guard(*, tx_risk_config: TransactionRiskConfig, txs: list[UnsignedTransaction]):
    """Call ``ExecutionOrchestrator._validate_gas_prices`` on a minimal shim.

    The guard reads only ``self.tx_risk_config`` from ``self`` — a
    ``SimpleNamespace`` is enough to bypass orchestrator construction.
    """
    shim = SimpleNamespace(tx_risk_config=tx_risk_config)
    return ExecutionOrchestrator._validate_gas_prices(shim, txs)


def _effective_implicit_gwei(*, max_gas_cost_usd: float, native_price_usd: float, gas_limit: int) -> float:
    """Return the implicit gwei cap derived from ``max_gas_cost_usd``.

    Math: ``cost_usd = (gas_limit * gas_price_wei / 1e18) * native_price``;
    set ``cost_usd == max_gas_cost_usd`` and solve for ``gas_price_gwei``.
    Clamped to :data:`SANE_GWEI_CEILING` for diagnostic display.
    """
    if native_price_usd <= 0:
        return SANE_GWEI_CEILING
    raw = (max_gas_cost_usd * 1e18) / (gas_limit * native_price_usd * 1e9)
    return min(raw, SANE_GWEI_CEILING)


class TestUsdCapPerChain:
    """USD cap correctness per chain, with the in-memory native price."""

    @pytest.mark.parametrize("chain", sorted(MOCKED_NATIVE_PRICES_USD.keys()))
    def test_usd_cap_accepts_tx_below_implicit_gwei_threshold(self, chain: str) -> None:
        """A tx with gwei BELOW the implicit threshold is accepted."""
        native_price = MOCKED_NATIVE_PRICES_USD[chain]
        implicit_gwei = _effective_implicit_gwei(
            max_gas_cost_usd=USD_CAP, native_price_usd=native_price, gas_limit=STD_GAS_LIMIT
        )
        # Pick a gwei comfortably below the implicit cap (50% of it).
        # For ceiling-clamped chains, the implicit value IS the ceiling and
        # 50% of it is still very large — the test still asserts the guard
        # accepts a typical-shaped tx.
        test_gwei = max(implicit_gwei * 0.5, 0.001)
        tx = _make_tx(chain_id=1, gas_price_gwei=test_gwei)

        risk = TransactionRiskConfig.permissive()
        risk.max_gas_cost_usd = USD_CAP
        risk.native_token_price_usd = native_price
        risk.native_token_price_timestamp = datetime.now(UTC)

        result = _invoke_guard(tx_risk_config=risk, txs=[tx])
        assert result.passed, (
            f"Chain {chain!r} (price=${native_price}): tx at {test_gwei:.2f} gwei "
            f"(implicit cap ~{implicit_gwei:.2f}) should pass, got violations: "
            f"{result.violations}"
        )

    @pytest.mark.parametrize("chain", sorted(MOCKED_NATIVE_PRICES_USD.keys()))
    def test_usd_cap_rejects_tx_above_implicit_gwei_threshold(self, chain: str) -> None:
        """A tx with gwei ABOVE the implicit threshold is rejected.

        Skipped for ceiling-clamped chains where the implicit gwei is
        SANE_GWEI_CEILING — exceeding the ceiling means submitting a
        tx with absurd gwei, which the chain-cap guard catches first
        and is out of scope for the USD-cap test.
        """
        native_price = MOCKED_NATIVE_PRICES_USD[chain]
        raw_implicit = (USD_CAP * 1e18) / (STD_GAS_LIMIT * native_price * 1e9)
        if raw_implicit >= SANE_GWEI_CEILING:
            pytest.skip(
                f"Chain {chain!r} native price ${native_price} makes the USD cap "
                f"non-binding in gwei terms (implicit {raw_implicit:.0f} >= "
                f"SANE_GWEI_CEILING {SANE_GWEI_CEILING}); the chain-cap guard "
                f"catches these txs first."
            )

        # Pick a gwei well above the implicit cap (2x it).
        test_gwei = raw_implicit * 2
        tx = _make_tx(chain_id=1, gas_price_gwei=test_gwei)

        risk = TransactionRiskConfig.permissive()
        risk.max_gas_cost_usd = USD_CAP
        risk.native_token_price_usd = native_price
        risk.native_token_price_timestamp = datetime.now(UTC)

        result = _invoke_guard(tx_risk_config=risk, txs=[tx])
        assert not result.passed, (
            f"Chain {chain!r} (price=${native_price}): tx at {test_gwei:.2f} gwei "
            f"(implicit cap ~{raw_implicit:.2f}) should be REJECTED for USD-cap "
            f"violation, but passed."
        )
        assert any("estimated gas cost" in v and "USD" in v for v in result.violations), (
            f"Chain {chain!r}: expected USD-cap violation, got: {result.violations}"
        )


class TestUsdCapAtSaneCeilingDocumented:
    """Document the implicit-gwei clamp at SANE_GWEI_CEILING for cheap-native chains."""

    @pytest.mark.parametrize(
        ("chain", "expected_band_lo", "expected_band_hi"),
        [
            # ETH-anchored chains: ~84 gwei give-or-take native price drift.
            ("ethereum", 50, 200),
            ("arbitrum", 50, 200),
            ("base", 50, 200),
            ("optimism", 50, 200),
            # Cheap-native chains: implicit gwei pegs at SANE_GWEI_CEILING.
            ("polygon", SANE_GWEI_CEILING, SANE_GWEI_CEILING),
            ("mantle", SANE_GWEI_CEILING, SANE_GWEI_CEILING),
            ("sonic", SANE_GWEI_CEILING, SANE_GWEI_CEILING),
            ("berachain", SANE_GWEI_CEILING, SANE_GWEI_CEILING),
            ("plasma", SANE_GWEI_CEILING, SANE_GWEI_CEILING),
            # Mid-priced natives: somewhere in between (loose band).
            ("avalanche", 100, SANE_GWEI_CEILING),
            ("bsc", 100, 1000),
        ],
    )
    def test_implicit_gwei_in_documented_band(
        self, chain: str, expected_band_lo: float, expected_band_hi: float
    ) -> None:
        native_price = MOCKED_NATIVE_PRICES_USD[chain]
        implicit = _effective_implicit_gwei(
            max_gas_cost_usd=USD_CAP, native_price_usd=native_price, gas_limit=STD_GAS_LIMIT
        )
        assert expected_band_lo <= implicit <= expected_band_hi, (
            f"Chain {chain!r} (price=${native_price}): implicit gwei {implicit:.2f} "
            f"outside documented band [{expected_band_lo}, {expected_band_hi}]. "
            f"Update MOCKED_NATIVE_PRICES_USD snapshot or widen the band."
        )


class TestUsdCapRequiresNativePrice:
    @pytest.mark.parametrize("gwei_cap", [0, 100])
    def test_missing_price_refuses_even_when_another_cap_exists(self, gwei_cap) -> None:
        risk = TransactionRiskConfig.permissive()
        risk.max_gas_cost_usd = USD_CAP
        risk.native_token_price_usd = 0.0
        risk.max_gas_price_gwei = gwei_cap
        result = _invoke_guard(tx_risk_config=risk, txs=[_make_tx(chain_id=1, gas_price_gwei=50)])
        assert not result.passed
        assert any("positive native token price" in v for v in result.violations)
