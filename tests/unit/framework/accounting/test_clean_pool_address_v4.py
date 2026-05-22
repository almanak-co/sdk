"""VIB-4471 — ``_clean_pool_address_candidate`` accepts 32-byte V4 PoolId hashes.

Tightens the no-slash branch of ``_clean_pool_address_candidate`` to accept
ONLY ``^0x[0-9a-f]{40}$`` (20-byte EVM address) OR ``^0x[0-9a-f]{64}$`` (32-byte
V4 ``pool_id`` hash) per the VIB-4426 design (§``_clean_pool_address_candidate``
change). Pre-VIB-4471 the no-slash branch returned the input unchanged for any
non-slash string, which let arbitrary identifiers leak into
``payload.pool_address``.

Adversarial repro test included: with a V4-shaped ``pool_address`` populated on
``lp_open_data``, ``_resolve_lp_pool_address`` MUST return the 32-byte hash
(NOT ``None``). High-value guard against future regression where someone
re-narrows the filter and silently re-drops every V4 LP event.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from almanak.framework.accounting.category_handlers.lp_handler import (
    _clean_pool_address_candidate,
    _resolve_lp_pool_address,
)


# Canonical V4 pool_id hashes from the design doc (§Choice of canonical V4
# pool identifier): 0x + 64 lowercase hex chars.
_V4_HASH_LOWER = "0x" + "ab" * 32  # 66 chars, all lowercase
_V4_HASH_REAL = "0x88f01dca1858c3a0fbf3bbd6e36fb53e2ca5d3e2c08ee06f1f5b0e2fb6e0e2c1"


class TestNoSlashBranch20ByteAddress:
    """20-byte EVM addresses (V3 / Aerodrome / classic AMM) still pass."""

    def test_lowercase_40_hex_passes(self) -> None:
        v3_pool = "0xc6962004f452be9203591991d15f6b388e09e8d0"
        assert _clean_pool_address_candidate(v3_pool) == v3_pool

    def test_aerodrome_v2_pool_passes(self) -> None:
        # Aerodrome v2 (Solidly) pool — bare 40-char hex
        aero_pool = "0xcdac0d6c6c59727a65f871236188350531885c43"
        assert _clean_pool_address_candidate(aero_pool) == aero_pool


class TestNoSlashBranch32ByteV4Hash:
    """32-byte V4 pool_id hashes (VIB-4426) pass."""

    def test_lowercase_64_hex_passes(self) -> None:
        assert _clean_pool_address_candidate(_V4_HASH_LOWER) == _V4_HASH_LOWER

    def test_realistic_v4_pool_id_passes(self) -> None:
        assert _clean_pool_address_candidate(_V4_HASH_REAL) == _V4_HASH_REAL


class TestNoSlashBranchRejections:
    """Pre-VIB-4471 these all returned the input unchanged. Post-VIB-4471 they
    return ``""`` so arbitrary garbage cannot leak into ``payload.pool_address``."""

    def test_empty_string_returns_empty(self) -> None:
        assert _clean_pool_address_candidate("") == ""

    def test_whitespace_only_returns_empty(self) -> None:
        assert _clean_pool_address_candidate("   ") == ""

    def test_none_returns_empty(self) -> None:
        assert _clean_pool_address_candidate(None) == ""

    def test_64_hex_chars_without_0x_prefix_rejected(self) -> None:
        # 64 hex chars total, but no 0x prefix → not the V4 shape
        assert _clean_pool_address_candidate("ab" * 32) == ""

    def test_62_chars_after_0x_rejected(self) -> None:
        # 0x + 31 bytes (62 hex chars) — neither 40 nor 64
        assert _clean_pool_address_candidate("0x" + "ab" * 31) == ""

    def test_42_chars_after_0x_rejected(self) -> None:
        # 0x + 21 bytes (42 hex chars) — neither 40 nor 64
        assert _clean_pool_address_candidate("0x" + "ab" * 21) == ""

    def test_66_chars_after_0x_rejected(self) -> None:
        # 0x + 33 bytes (66 hex chars) — too long for either shape
        assert _clean_pool_address_candidate("0x" + "ab" * 33) == ""

    def test_non_hex_at_40_length_rejected(self) -> None:
        # 0x + 40 chars but contains a non-hex character
        assert _clean_pool_address_candidate("0xZZ" + "ab" * 19) == ""

    def test_non_hex_at_64_length_rejected(self) -> None:
        # 0x + 64 chars but contains a non-hex character (the v2 design's
        # explicit example: "0xZZ" + "ab" * 31)
        assert _clean_pool_address_candidate("0xZZ" + "ab" * 31) == ""

    def test_mixed_case_40_hex_rejected(self) -> None:
        # The regex is lowercase-only — mixed-case 20-byte addresses must be
        # lowercased by the caller before reaching this filter (VIB-4274
        # pre-VIB-4471 was tolerant; the tighter rule is strict).
        assert _clean_pool_address_candidate("0xC6962004F452BE9203591991D15F6B388E09E8D0") == ""

    def test_mixed_case_64_hex_rejected(self) -> None:
        # Same lowercase-only rule for the V4 32-byte hash shape.
        assert _clean_pool_address_candidate("0xAB" + "ab" * 31) == ""

    def test_arbitrary_garbage_rejected(self) -> None:
        # Pre-VIB-4471 this returned "weth-usdc-pool-v3" unchanged.
        assert _clean_pool_address_candidate("weth-usdc-pool-v3") == ""

    def test_short_identifier_rejected(self) -> None:
        # Pre-VIB-4471 this returned "uni" unchanged.
        assert _clean_pool_address_candidate("uni") == ""


class TestSlashBranchUnchanged:
    """The slash-bearing branch is intentionally left alone — confirms the
    no-slash tightening did not bleed into the descriptor logic."""

    def test_solidly_stable_descriptor_still_accepted(self) -> None:
        descriptor = "USDC/DAI/stable"
        assert _clean_pool_address_candidate(descriptor) == descriptor

    def test_solidly_volatile_descriptor_still_accepted(self) -> None:
        descriptor = "WETH/USDC/volatile"
        assert _clean_pool_address_candidate(descriptor) == descriptor

    def test_v3_fee_tier_descriptor_still_rejected(self) -> None:
        # VIB-4274 / VIB-4396 — numeric-tail rejection.
        assert _clean_pool_address_candidate("WETH/USDC/500") == ""

    def test_v4_fee_tier_descriptor_still_rejected(self) -> None:
        # V4 descriptors look like V3 (WETH/USDC/3000). The numeric-tail
        # rule rejects them; V4 events now flow via the 64-hex pool_id
        # branch instead.
        assert _clean_pool_address_candidate("WETH/USDC/3000") == ""


# ──────────────────────────────────────────────────────────────────────────────
# Adversarial repro: _resolve_lp_pool_address returns the 32-byte hash for V4
# ──────────────────────────────────────────────────────────────────────────────


def _make_outbox_row(*, position_key: str = "", market_id: str = "") -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "ledger_entry_id": str(uuid.uuid4()),
        "deployment_id": "dep-vib4471",
        "deployment_id": "strat-vib4471",
        "cycle_id": "cycle-1",
        "intent_type": "LP_OPEN",
        "wallet_address": "0xwallet",
        "position_key": position_key,
        "market_id": market_id,
        "status": "pending",
        "attempts": 0,
        "error": "",
        "created_at": datetime.now(UTC).isoformat(),
        "updated_at": datetime.now(UTC).isoformat(),
    }


class TestAdversarialReproV4ResolvesPoolAddress:
    """Lock the merge gate from the VIB-4426 design doc Q12 resolution:
    when ``lp_open_data.pool_address`` is a 32-byte V4 pool_id hash,
    ``_resolve_lp_pool_address`` MUST return it (NOT ``None``).

    Without this guard, a future re-narrowing of
    ``_clean_pool_address_candidate`` would silently re-drop every V4 LP
    event with the structured "cannot resolve pool address" warning —
    exactly the regression VIB-4426 fixes.
    """

    def test_v4_pool_id_from_receipt_extraction_resolves(self) -> None:
        outbox = _make_outbox_row(
            position_key="lp:uniswap_v4:base:0xwallet:" + _V4_HASH_REAL,
        )
        extracted = {
            "lp_open_data": {
                "_type": "LPOpenData",
                "pool_address": _V4_HASH_REAL,
                "tick_lower": -887220,
                "tick_upper": 887220,
            }
        }
        result = _resolve_lp_pool_address(
            outbox_row=outbox,
            position_key=outbox["position_key"],
            extracted=extracted,
            prior_open_payload=None,
        )
        assert result == _V4_HASH_REAL, (
            "V4 receipt-extracted pool_id hash must survive "
            "_clean_pool_address_candidate (VIB-4426 / VIB-4471)"
        )

    def test_v4_pool_id_from_position_key_tail_resolves(self) -> None:
        # Even without receipt extraction, a V4 position_key whose tail is
        # the 32-byte pool_id hash must resolve. Locks the position_key →
        # pool_address path open for the V4 lp_v4 fixture.
        outbox = _make_outbox_row(
            position_key="lp:uniswap_v4:base:0xwallet:" + _V4_HASH_REAL,
        )
        result = _resolve_lp_pool_address(
            outbox_row=outbox,
            position_key=outbox["position_key"],
            extracted=None,
            prior_open_payload=None,
        )
        assert result == _V4_HASH_REAL

    def test_v4_pool_id_from_prior_open_payload_resolves(self) -> None:
        # LP_CLOSE fallback path — prior LP_OPEN payload carries the
        # canonical 32-byte hash.
        outbox = _make_outbox_row(position_key="lp:uniswap_v4:base:0xwallet:any")
        prior_open = {
            "event_type": "LP_OPEN",
            "pool_address": _V4_HASH_REAL,
        }
        result = _resolve_lp_pool_address(
            outbox_row=outbox,
            position_key=outbox["position_key"],
            extracted=None,
            prior_open_payload=prior_open,
        )
        assert result == _V4_HASH_REAL


# Quick smoke: the full payload JSON shape is unchanged on the slash branch.
@pytest.mark.parametrize(
    "value,expected",
    [
        # 20-byte EVM address — canonical happy path
        ("0xc6962004f452be9203591991d15f6b388e09e8d0", "0xc6962004f452be9203591991d15f6b388e09e8d0"),
        # 32-byte V4 hash — canonical happy path (VIB-4426)
        (_V4_HASH_LOWER, _V4_HASH_LOWER),
        # Solidly descriptor — slash branch, accepted
        ("USDC/DAI/stable", "USDC/DAI/stable"),
        # V3 fee-tier descriptor — slash branch, rejected
        ("WETH/USDC/500", ""),
        # Garbage no-slash — pre-VIB-4471 returned input, now rejected
        ("uni", ""),
        ("pool-name", ""),
        ("0x", ""),  # 0x with no body
        ("", ""),
    ],
)
def test_parametrized_shapes(value: str, expected: str) -> None:
    """Smoke-table covering the four shapes the function distinguishes:
    20-byte hex, 32-byte hex, Solidly descriptor, garbage."""
    # Tolerate whitespace stripping for empty-only inputs
    assert _clean_pool_address_candidate(value) == expected
