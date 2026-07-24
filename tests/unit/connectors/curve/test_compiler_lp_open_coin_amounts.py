"""Tests for Curve LP_OPEN coin-aligned allocation vector (VIB-5154 / ALM-2728).

The Curve compiler historically mapped only ``amount0``/``amount1`` to pool coin
indices 0/1 and zero-filled the tail, making it impossible to open an LP position
targeting coin index 2+ in a 3+ coin pool unless index 0 was also funded.

``LPOpenIntent.coin_amounts`` is a pool-coin-aligned full allocation vector:
``coin_amounts[i]`` maps directly to pool coin index ``i``. When present, the
compiler uses it verbatim (validated against the pool ``n_coins``). When absent,
the legacy ``amount0``/``amount1`` mapping is preserved exactly.

These tests drive the *real* ``CurveCompiler.compile_lp_open`` against the *real*
``CurveAdapter`` (which estimates LP output offline from the static pool
``virtual_price``) and assert on the per-coin ``amounts`` vector the compiler hands
to the adapter (surfaced in ``action_bundle.metadata["amounts"]``). No RPC, no
mocks of the mapping under test — the assertions pin the index mapping directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pytest

from almanak.connectors.curve.adapter import CURVE_POOLS
from almanak.connectors.curve.compiler import CurveCompiler
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.vocabulary import Intent, LPOpenIntent

WALLET = "0x1234567890123456789012345678901234567890"


@dataclass
class _StubContext:
    """Minimal context satisfying the fields ``compile_lp_open`` reads.

    ``compile_lp_open`` touches only ``chain``, ``wallet_address``, ``rpc_url``,
    ``gateway_client``, and (for asset-set ``"a/b"`` pools) ``services``. Named or
    address pools never invoke ``services``, so a ``None`` placeholder is fine for
    the index-mapping tests here.
    """

    chain: str
    wallet_address: str = WALLET
    rpc_url: str | None = None
    gateway_client: Any = None
    services: Any = None


def _compile(intent: LPOpenIntent, chain: str) -> Any:
    return CurveCompiler().compile_lp_open(_StubContext(chain=chain), intent)


def _amounts(result: Any) -> list[Decimal]:
    """Extract the per-coin allocation vector the compiler built, as Decimals."""
    assert result.status == CompilationStatus.SUCCESS, result.error
    return [Decimal(a) for a in result.action_bundle.metadata["amounts"]]


# =============================================================================
# coin_amounts: explicit pool-coin-aligned vector
# =============================================================================


class TestCoinAmountsMapping:
    def test_two_coin_pool(self) -> None:
        """2-coin pool (arbitrum 2pool: USDC.e idx0 / USDT idx1)."""
        intent = LPOpenIntent(
            pool="2pool",
            amount0=Decimal("0"),
            amount1=Decimal("0"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
            coin_amounts=[Decimal("100"), Decimal("200")],
        )
        assert _amounts(_compile(intent, "arbitrum")) == [Decimal("100"), Decimal("200")]

    def test_three_coin_pool_targets_idx1_and_idx2_skipping_idx0(self) -> None:
        """The bug fixture: 3pool deposit of idx1 + idx2 leaving idx0 (DAI) at zero.

        Ethereum 3pool coins = [DAI (0), USDC (1), USDT (2)]. The two-slot mapping
        could never express this; coin_amounts can. (Was polygon's am3pool until
        VIB-5551 removed it — frozen Aave V2 made it non-executable; polygon's
        registered pool is now the 2-coin frxusd_usdt, so the canonical 3-coin
        example lives on ethereum.)
        """
        intent = LPOpenIntent(
            pool="3pool",
            amount0=Decimal("0"),
            amount1=Decimal("0"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
            coin_amounts=[Decimal("0"), Decimal("500"), Decimal("500")],
        )
        amounts = _amounts(_compile(intent, "ethereum"))
        assert amounts == [Decimal("0"), Decimal("500"), Decimal("500")]
        # Index 0 (DAI) must be untouched — this is the whole point of the ticket.
        assert amounts[0] == Decimal("0")

    def test_four_coin_pool(self) -> None:
        """4-coin pool (base 4pool: USDC/USDbC/axlUSDC/crvUSD)."""
        intent = LPOpenIntent(
            pool="4pool",
            amount0=Decimal("0"),
            amount1=Decimal("0"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
            coin_amounts=[Decimal("10"), Decimal("20"), Decimal("30"), Decimal("40")],
        )
        assert _amounts(_compile(intent, "base")) == [
            Decimal("10"),
            Decimal("20"),
            Decimal("30"),
            Decimal("40"),
        ]

    def test_sparse_allocation_only_last_coin(self) -> None:
        """Single non-leading coin funded (4pool: only crvUSD idx3)."""
        intent = LPOpenIntent(
            pool="4pool",
            amount0=Decimal("0"),
            amount1=Decimal("0"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
            coin_amounts=[Decimal("0"), Decimal("0"), Decimal("0"), Decimal("1000")],
        )
        assert _amounts(_compile(intent, "base")) == [
            Decimal("0"),
            Decimal("0"),
            Decimal("0"),
            Decimal("1000"),
        ]

    def test_single_coin_only_idx0(self) -> None:
        """Single leading coin funded — coin_amounts still wins (3pool idx0 only)."""
        intent = LPOpenIntent(
            pool="3pool",
            amount0=Decimal("0"),
            amount1=Decimal("0"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
            coin_amounts=[Decimal("750"), Decimal("0"), Decimal("0")],
        )
        assert _amounts(_compile(intent, "ethereum")) == [Decimal("750"), Decimal("0"), Decimal("0")]


# =============================================================================
# coin_amounts: length validation against pool n_coins
# =============================================================================


class TestCoinAmountsLengthValidation:
    def test_too_few_entries_fails(self) -> None:
        intent = LPOpenIntent(
            pool="3pool",
            amount0=Decimal("0"),
            amount1=Decimal("0"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
            coin_amounts=[Decimal("100"), Decimal("100")],  # 2 entries, pool has 3
        )
        result = _compile(intent, "ethereum")
        assert result.status == CompilationStatus.FAILED
        assert "coin_amounts has 2 entries" in (result.error or "")
        assert "3 coins" in (result.error or "")

    def test_too_many_entries_fails(self) -> None:
        intent = LPOpenIntent(
            pool="2pool",
            amount0=Decimal("0"),
            amount1=Decimal("0"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
            coin_amounts=[Decimal("1"), Decimal("1"), Decimal("1")],  # 3 entries, pool has 2
        )
        result = _compile(intent, "arbitrum")
        assert result.status == CompilationStatus.FAILED
        assert "coin_amounts has 3 entries" in (result.error or "")
        assert "2 coins" in (result.error or "")


# =============================================================================
# Backward compatibility: amount0/amount1 path is byte-for-byte unchanged
# =============================================================================


class TestAmount0Amount1BackCompat:
    @pytest.mark.parametrize(
        ("chain", "pool", "expected"),
        [
            # 2-coin: amount0/amount1 map to idx0/idx1, no tail.
            ("arbitrum", "2pool", [Decimal("100"), Decimal("200")]),
            # 3-coin: amount0/amount1 -> idx0/idx1, idx2 zero-filled.
            ("ethereum", "3pool", [Decimal("100"), Decimal("200"), Decimal("0")]),
            # 4-coin: amount0/amount1 -> idx0/idx1, idx2/idx3 zero-filled.
            ("base", "4pool", [Decimal("100"), Decimal("200"), Decimal("0"), Decimal("0")]),
        ],
    )
    def test_legacy_two_slot_mapping_preserved(
        self, chain: str, pool: str, expected: list[Decimal]
    ) -> None:
        """coin_amounts=None reproduces the exact legacy idx0/idx1 + zero-fill vector."""
        intent = LPOpenIntent(
            pool=pool,
            amount0=Decimal("100"),
            amount1=Decimal("200"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
            # coin_amounts intentionally omitted (None) — legacy path.
        )
        assert intent.coin_amounts is None
        assert _amounts(_compile(intent, chain)) == expected

    def test_coin_amounts_equivalent_to_legacy_for_leading_two(self) -> None:
        """coin_amounts=[a0,a1,0] must produce the SAME vector as amount0=a0,amount1=a1.

        This is the equivalence proof: the new path is a strict superset of the old.
        """
        legacy = LPOpenIntent(
            pool="3pool",
            amount0=Decimal("123"),
            amount1=Decimal("456"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
        )
        explicit = LPOpenIntent(
            pool="3pool",
            amount0=Decimal("0"),
            amount1=Decimal("0"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
            coin_amounts=[Decimal("123"), Decimal("456"), Decimal("0")],
        )
        assert _amounts(_compile(legacy, "ethereum")) == _amounts(_compile(explicit, "ethereum"))


# =============================================================================
# Intent vocabulary: field + factory + validation
# =============================================================================


class TestLPOpenIntentCoinAmountsField:
    def test_factory_threads_coin_amounts(self) -> None:
        intent = Intent.lp_open(
            pool="3pool",
            coin_amounts=[Decimal("0"), Decimal("500"), Decimal("500")],
            protocol="curve",
            chain="ethereum",
        )
        assert intent.coin_amounts == [Decimal("0"), Decimal("500"), Decimal("500")]
        # amount0/amount1 default to zero in the multi-coin ergonomic path.
        assert intent.amount0 == Decimal("0")
        assert intent.amount1 == Decimal("0")

    def test_default_is_none(self) -> None:
        intent = Intent.lp_open(
            pool="0xpool",
            amount0=Decimal("1"),
            amount1=Decimal("2"),
            range_lower=Decimal("1800"),
            range_upper=Decimal("2200"),
        )
        assert intent.coin_amounts is None

    def test_empty_coin_amounts_rejected(self) -> None:
        with pytest.raises(ValueError, match="coin_amounts must not be empty"):
            LPOpenIntent(
                pool="3pool",
                amount0=Decimal("0"),
                amount1=Decimal("0"),
                range_lower=Decimal("1"),
                range_upper=Decimal("2"),
                protocol="curve",
                coin_amounts=[],
            )

    def test_all_zero_coin_amounts_rejected(self) -> None:
        with pytest.raises(ValueError, match="at least one positive entry"):
            LPOpenIntent(
                pool="3pool",
                amount0=Decimal("0"),
                amount1=Decimal("0"),
                range_lower=Decimal("1"),
                range_upper=Decimal("2"),
                protocol="curve",
                coin_amounts=[Decimal("0"), Decimal("0"), Decimal("0")],
            )

    def test_negative_coin_amount_rejected(self) -> None:
        with pytest.raises(ValueError, match="coin_amounts entries must be non-negative"):
            LPOpenIntent(
                pool="3pool",
                amount0=Decimal("0"),
                amount1=Decimal("0"),
                range_lower=Decimal("1"),
                range_upper=Decimal("2"),
                protocol="curve",
                coin_amounts=[Decimal("-1"), Decimal("100"), Decimal("100")],
            )

    def test_coin_amounts_rejected_for_non_curve_protocol(self) -> None:
        """coin_amounts on a non-Curve LP protocol is a loud error, not a silent
        0-liquidity open (only the Curve compiler consumes the field)."""
        with pytest.raises(ValueError, match="only supported for the 'curve' protocol"):
            LPOpenIntent(
                pool="WETH/USDC/500",
                amount0=Decimal("0"),
                amount1=Decimal("0"),
                range_lower=Decimal("1"),
                range_upper=Decimal("2"),
                protocol="uniswap_v3",
                coin_amounts=[Decimal("100"), Decimal("200")],
            )

    def test_coin_amounts_and_amount0_amount1_mutually_exclusive(self) -> None:
        """Mixing coin_amounts with non-zero amount0/amount1 is rejected rather
        than silently dropping amount0/amount1."""
        with pytest.raises(ValueError, match="Cannot provide both coin_amounts and amount0/amount1"):
            LPOpenIntent(
                pool="3pool",
                amount0=Decimal("100"),
                amount1=Decimal("0"),
                range_lower=Decimal("1"),
                range_upper=Decimal("2"),
                protocol="curve",
                coin_amounts=[Decimal("0"), Decimal("500"), Decimal("500")],
            )

    def test_zero_amounts_without_coin_amounts_still_rejected(self) -> None:
        """Back-compat guard: amount0==amount1==0 and no coin_amounts is still invalid."""
        with pytest.raises(ValueError, match="At least one amount must be positive"):
            LPOpenIntent(
                pool="3pool",
                amount0=Decimal("0"),
                amount1=Decimal("0"),
                range_lower=Decimal("1"),
                range_upper=Decimal("2"),
                protocol="curve",
            )

    def test_serialize_round_trip_preserves_coin_amounts(self) -> None:
        intent = LPOpenIntent(
            pool="3pool",
            amount0=Decimal("0"),
            amount1=Decimal("0"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="curve",
            coin_amounts=[Decimal("0"), Decimal("500"), Decimal("500")],
        )
        restored = LPOpenIntent.deserialize(intent.serialize())
        assert restored.coin_amounts == [Decimal("0"), Decimal("500"), Decimal("500")]
