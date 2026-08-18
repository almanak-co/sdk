"""VIB-5984 — the lp_repricer must not book a degraded all-zero read as a
MEASURED $0 at HIGH confidence.

Source fix for the VIB-4970 corruption class. VIB-4970 shipped the writer-side
fail-closed guard (`enforce_open_position_value_invariant`, PR #3419); this is
the producer that was minting the bad claim in the first place.

The discriminator is the token pair on the struct, not the position registry:

* A V3-family `positions(tokenId)` struct carries `token0`/`token1` from mint
  until burn (post-burn the call reverts → `read_position` returns None). So a
  live-but-emptied position still reports both addresses → measured $0 is right.
* An all-zero struct whose token addresses are ALSO zero cannot come from any
  minted position. It is a zero-filled response (degraded gateway/RPC read, the
  VIB-5930 class) → unmeasured, never a confident zero.

A registry cross-check was considered and rejected — see the inline rationale in
`reprice_lp_position`; `test_emptied_position_still_measures_zero` is the case a
registry-only rule would permanently pin to UNAVAILABLE, killing the
PortfolioMetrics write for every remaining iteration.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.valuation.lp_repricer import _is_zero_address, reprice_lp_position

USDC = "0x" + "11" * 20
WETH = "0x" + "cc" * 20
ZERO = "0x" + "00" * 20

_DECIMALS = {USDC: 6, WETH: 18, "USDC": 6, "WETH": 18}


class _Reader:
    """Stub LPPositionReader returning a caller-supplied struct."""

    def __init__(self, struct, slot0=None):
        self._struct = struct
        self._slot0 = slot0
        self.calls = 0

    def read_position(self, *, chain, token_id, protocol, position_manager=None):
        self.calls += 1
        return self._struct

    def read_pool_slot0(self, chain, pool_address):
        return self._slot0


def _struct(*, token0, token1, liquidity=0, owed0=0, owed1=0):
    return SimpleNamespace(
        token_id=999,
        token0=token0,
        token1=token1,
        fee=500,
        tick_lower=-60_000,
        tick_upper=60_000,
        liquidity=liquidity,
        tokens_owed0=owed0,
        tokens_owed1=owed1,
        tick_spacing=None,
    )


def _position():
    return SimpleNamespace(
        position_id="uniswap_v3_ethereum_lp_999",
        protocol="uniswap_v3",
        chain="ethereum",
        details={
            "token_id": 999,
            "token0": "USDC",
            "token1": "WETH",
            # Real hex shape so the live path reaches read_pool_slot0
            # (resolve_lp_pool_address_from_details is hex-shape guarded).
            "pool_address": "0x" + "ab" * 20,
        },
    )


def _reprice(struct, slot0=None):
    reader = _Reader(struct, slot0)
    out = reprice_lp_position(
        reader,
        _position(),
        "ethereum",
        lambda symbol: Decimal("1"),
        lambda symbol, chain: _DECIMALS.get(symbol, 18),
    )
    return out, reader


# ── the discriminator ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "value",
    [None, "", "   ", ZERO, ZERO.upper(), "0x0", "0", "0X0000000000000000000000000000000000000000"],
)
def test_is_zero_address_true(value):
    assert _is_zero_address(value) is True


@pytest.mark.parametrize("value", [USDC, WETH, USDC.upper(), "not-hex", "0xdead"])
def test_is_zero_address_false(value):
    assert _is_zero_address(value) is False


# ── the defect ───────────────────────────────────────────────────────────


def test_all_zero_struct_is_unmeasured_not_measured_zero():
    """VIB-5984: a zero-filled struct must NOT become a confident $0."""
    out, reader = _reprice(_struct(token0=ZERO, token1=ZERO))

    assert out is None, "degraded read must be unmeasured (Empty != Zero)"
    assert reader.calls == 1


def test_all_zero_struct_logs_the_miss(caplog):
    """The miss must be visible — a silent None is how VIB-5722 hid."""
    with caplog.at_level("WARNING"):
        _reprice(_struct(token0=ZERO, token1=ZERO))

    assert any("VIB-5984" in r.message for r in caplog.records)


def test_missing_token_addresses_are_unmeasured():
    """Same class, different decode shape (None / empty rather than 0x0)."""
    out, _ = _reprice(_struct(token0=None, token1=""))
    assert out is None


# ── the case that must NOT regress ───────────────────────────────────────


def test_emptied_position_still_measures_zero():
    """A live-but-emptied NFT reports its real token pair → measured $0 at HIGH.

    This is the case a registry-only cross-check would misclassify as
    unmeasured forever (the registry keeps it OPEN until something updates it),
    permanently suppressing PortfolioMetrics.
    """
    out, _ = _reprice(_struct(token0=USDC, token1=WETH))

    assert out is not None
    value, details = out
    assert value == Decimal("0")
    assert details == {"position_id": "999", "liquidity": "0"}


def test_half_zero_token_pair_still_measures_zero():
    """Only a BOTH-zero pair proves a zero-filled response. One real address
    means the struct decoded real data, so the branch must not widen."""
    out, _ = _reprice(_struct(token0=USDC, token1=ZERO))

    assert out is not None
    assert out[0] == Decimal("0")


def test_none_read_still_unmeasured():
    """Pre-existing contract: a failed read stays unmeasured."""
    out, _ = _reprice(None)
    assert out is None


def test_live_position_values_through_the_normal_path():
    """A position with liquidity never enters the zero branch — it reprices.

    Uses an in-range slot0 (tick 0, the position spans +/-60000) so the live
    valuation genuinely completes rather than falling back to the price-ratio
    tick. ``valuation_source == "on_chain"`` is the sharpest proof the zero
    short-circuit was not taken: that branch returns a two-key details dict
    with no ``valuation_source`` at all.
    """
    slot0 = SimpleNamespace(tick=0, sqrt_price_x96=2**96)
    out, _ = _reprice(_struct(token0=USDC, token1=WETH, liquidity=10**18), slot0=slot0)

    assert out is not None
    value, details = out
    assert value > Decimal("0")
    assert details["liquidity"] == str(10**18)
    assert details["valuation_source"] == "on_chain"
    assert details["in_range"] is True
