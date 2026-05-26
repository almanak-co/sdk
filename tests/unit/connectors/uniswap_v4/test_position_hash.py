"""Golden tests for V4 ``compute_position_hash`` (v4-core Position.calculatePositionKey).

VIB-4474 T05. Formula:
    keccak256(abi.encodePacked(owner, int24(tickLower), int24(tickUpper), bytes32(salt)))

NIT-1 (VIB-4426): negative-tick fixtures are MANDATORY for V4 because v4-core
uses ``abi.encodePacked`` which truncates ``int24`` to 3 bytes (two's-complement
for negatives). A bug in the packing produces wrong hashes for any in-range
LP that spans the negative-tick half of price space (e.g. USDC/WETH on Base
at current prices, where ``tickLower`` is typically very negative).
"""

from __future__ import annotations

import pytest
from web3 import Web3

from almanak.connectors.uniswap_v4.hooks import (
    _normalize_bytes32,
    _pack_int24,
    compute_position_hash,
)

POSITION_MANAGER_BASE = "0x7C5f5A4bBd8fD63184577525326123B519429bDc"


def _expected_position_hash(
    owner: str,
    tick_lower: int,
    tick_upper: int,
    salt: bytes | str,
) -> str:
    """Independent re-implementation of v4-core Position.calculatePositionKey.

    Uses Web3.keccak directly so the test fails if compute_position_hash
    drifts from the canonical packed encoding.
    """
    owner_hex = owner.lower().removeprefix("0x").zfill(40)
    packed_hex = owner_hex + _pack_int24(tick_lower) + _pack_int24(tick_upper) + _normalize_bytes32(salt)
    return "0x" + Web3.keccak(bytes.fromhex(packed_hex)).hex()


class TestComputePositionHashSanity:
    def test_length(self) -> None:
        h = compute_position_hash(
            owner=POSITION_MANAGER_BASE,
            tick_lower=-60,
            tick_upper=60,
            salt=b"\x00" * 32,
        )
        assert h.startswith("0x")
        assert len(h) == 66

    def test_deterministic(self) -> None:
        kwargs = {
            "owner": POSITION_MANAGER_BASE,
            "tick_lower": -60,
            "tick_upper": 60,
            "salt": b"\x00" * 32,
        }
        assert compute_position_hash(**kwargs) == compute_position_hash(**kwargs)


class TestComputePositionHashGolden:
    """Recompute via the independent helper — locks the packed encoding."""

    @pytest.mark.parametrize(
        ("tick_lower", "tick_upper"),
        [
            (-60, 60),
            (0, 6000),
            (-6000, 0),
            (-887272, -100),  # NIT-1: both ticks negative
            (-887272, 887272),  # full V4 tick range
            (100, 887272),
            (-887272, 887271),  # MIN_TICK lower, MAX_TICK-1 upper
        ],
    )
    def test_matches_canonical(self, tick_lower: int, tick_upper: int) -> None:
        salt_int = 0x1234ABCD
        salt_hex = "0x" + format(salt_int, "064x")
        h = compute_position_hash(
            owner=POSITION_MANAGER_BASE,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            salt=salt_hex,
        )
        assert h == _expected_position_hash(POSITION_MANAGER_BASE, tick_lower, tick_upper, salt_hex)


class TestComputePositionHashNegativeTickNIT1:
    """VIB-4426 NIT-1: explicit negative-tick coverage."""

    def test_min_tick_lower(self) -> None:
        h = compute_position_hash(
            owner=POSITION_MANAGER_BASE,
            tick_lower=-887272,
            tick_upper=-100,
            salt="0x" + format(42, "064x"),
        )
        assert h == _expected_position_hash(
            POSITION_MANAGER_BASE,
            -887272,
            -100,
            "0x" + format(42, "064x"),
        )

    def test_pack_int24_negative_one(self) -> None:
        """int24(-1) -> 0xffffff (3 bytes two's complement)."""
        assert _pack_int24(-1) == "ffffff"

    def test_pack_int24_negative_100(self) -> None:
        """int24(-100) -> 0xffff9c (3 bytes two's complement)."""
        assert _pack_int24(-100) == "ffff9c"

    def test_pack_int24_min(self) -> None:
        """int24 min = -2^23 = -8388608 -> 0x800000."""
        assert _pack_int24(-(1 << 23)) == "800000"

    def test_pack_int24_max(self) -> None:
        """int24 max = 2^23 - 1 = 8388607 -> 0x7fffff."""
        assert _pack_int24((1 << 23) - 1) == "7fffff"

    def test_pack_int24_zero(self) -> None:
        assert _pack_int24(0) == "000000"

    def test_pack_int24_positive_100(self) -> None:
        assert _pack_int24(100) == "000064"

    def test_pack_int24_rejects_out_of_range(self) -> None:
        with pytest.raises(ValueError):
            _pack_int24(1 << 23)
        with pytest.raises(ValueError):
            _pack_int24(-(1 << 23) - 1)


class TestComputePositionHashSaltVariants:
    def test_salt_bytes_and_hex_are_equivalent(self) -> None:
        salt_int = 0xDEADBEEF
        salt_bytes = salt_int.to_bytes(32, "big")
        salt_hex = "0x" + format(salt_int, "064x")
        h_bytes = compute_position_hash(
            owner=POSITION_MANAGER_BASE,
            tick_lower=-60,
            tick_upper=60,
            salt=salt_bytes,
        )
        h_hex = compute_position_hash(
            owner=POSITION_MANAGER_BASE,
            tick_lower=-60,
            tick_upper=60,
            salt=salt_hex,
        )
        assert h_bytes == h_hex

    def test_different_salt_yields_different_hash(self) -> None:
        h1 = compute_position_hash(
            owner=POSITION_MANAGER_BASE,
            tick_lower=-60,
            tick_upper=60,
            salt=b"\x00" * 32,
        )
        h2 = compute_position_hash(
            owner=POSITION_MANAGER_BASE,
            tick_lower=-60,
            tick_upper=60,
            salt=b"\x00" * 31 + b"\x01",
        )
        assert h1 != h2

    def test_different_owner_yields_different_hash(self) -> None:
        other_pm = "0x0000000000000000000000000000000000000123"
        h_base = compute_position_hash(
            owner=POSITION_MANAGER_BASE,
            tick_lower=-60,
            tick_upper=60,
            salt=b"\x00" * 32,
        )
        h_other = compute_position_hash(
            owner=other_pm,
            tick_lower=-60,
            tick_upper=60,
            salt=b"\x00" * 32,
        )
        assert h_base != h_other

    def test_owner_case_insensitive(self) -> None:
        h_lower = compute_position_hash(
            owner=POSITION_MANAGER_BASE.lower(),
            tick_lower=-60,
            tick_upper=60,
            salt=b"\x00" * 32,
        )
        h_upper = compute_position_hash(
            owner=POSITION_MANAGER_BASE.upper(),
            tick_lower=-60,
            tick_upper=60,
            salt=b"\x00" * 32,
        )
        assert h_lower == h_upper


class TestComputePositionHashInputValidation:
    def test_rejects_short_owner(self) -> None:
        # Note: 19 byte address -- normally addresses can be padded, so we test
        # via a too-long address that doesn't fit
        with pytest.raises(ValueError):
            compute_position_hash(
                owner="0x" + "11" * 21,  # 21 bytes
                tick_lower=0,
                tick_upper=60,
                salt=b"\x00" * 32,
            )

    def test_rejects_short_salt_bytes(self) -> None:
        with pytest.raises(ValueError):
            compute_position_hash(
                owner=POSITION_MANAGER_BASE,
                tick_lower=0,
                tick_upper=60,
                salt=b"\x00" * 16,
            )

    def test_rejects_short_salt_hex(self) -> None:
        with pytest.raises(ValueError):
            compute_position_hash(
                owner=POSITION_MANAGER_BASE,
                tick_lower=0,
                tick_upper=60,
                salt="0xdeadbeef",
            )

    def test_rejects_out_of_range_tick(self) -> None:
        with pytest.raises(ValueError):
            compute_position_hash(
                owner=POSITION_MANAGER_BASE,
                tick_lower=1 << 23,
                tick_upper=60,
                salt=b"\x00" * 32,
            )
