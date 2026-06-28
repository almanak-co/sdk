"""USDT-class approval hardening for the Curve adapter (VIB-5442 / audit P1-8, P2-1).

Approvals were infinite MAX_UINT256 with a cache never seeded from on-chain
``allowance()``, so a token already approved in a prior run was re-approved — and
USDT-class tokens REVERT on a non-zero → non-zero ``approve`` (``require(value ==
0 || allowance == 0)``), silently killing the whole bundle. These tests prove the
adapter now (a) seeds from on-chain allowance and skips a redundant approve, and
(b) emits a reset-to-zero before changing a non-zero allowance, ordered so a
partial-bundle failure can never strand a non-zero allowance.
"""

from __future__ import annotations

from unittest.mock import patch

from almanak.connectors.curve.adapter import MAX_UINT256, CurveAdapter, CurveConfig

TOKEN = "0xdAC17F958D2ee523a2206206994597C13D831ec7"  # USDT
SPENDER = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"
WALLET = "0x1234567890123456789012345678901234567890"


def _adapter(*, rpc: bool = False) -> CurveAdapter:
    return CurveAdapter(
        CurveConfig(chain="ethereum", wallet_address=WALLET, rpc_url="http://localhost:8545" if rpc else None)
    )


def _approve_value(tx) -> int:
    data = tx.data[2:] if tx.data.startswith("0x") else tx.data
    return int(data[-64:], 16)


class TestCacheOnlyBehaviour:
    def test_no_transport_cannot_confirm_zero_resets_to_be_safe(self) -> None:
        """A no-transport adapter cannot positively confirm a zero allowance, so it
        fails toward reset+approve rather than a lone approve(MAX) that could revert
        on a USDT that turns out to still hold a non-zero allowance."""
        txs = _adapter()._build_approve_txs(TOKEN, SPENDER, 1000)
        assert [_approve_value(t) for t in txs] == [0, MAX_UINT256]

    def test_sufficient_allowance_skips(self) -> None:
        a = _adapter()
        a.set_allowance(TOKEN, SPENDER, MAX_UINT256)
        assert a._build_approve_txs(TOKEN, SPENDER, 1000) == []

    def test_nonzero_insufficient_resets_then_approves(self) -> None:
        """USDT-class: an existing non-zero allowance is reset to 0 before re-approve."""
        a = _adapter()
        a.set_allowance(TOKEN, SPENDER, 500)
        txs = a._build_approve_txs(TOKEN, SPENDER, 1000)
        assert len(txs) == 2
        assert _approve_value(txs[0]) == 0  # reset FIRST
        assert _approve_value(txs[1]) == MAX_UINT256

    def test_reset_ordering_is_partial_bundle_safe(self) -> None:
        """Reset precedes approve, so a mid-bundle failure leaves allowance=0
        (re-approvable), never a stranded non-zero value (P2-1 atomicity)."""
        a = _adapter()
        a.set_allowance(TOKEN, SPENDER, 1)
        txs = a._build_approve_txs(TOKEN, SPENDER, 1000)
        values = [_approve_value(t) for t in txs]
        assert values == [0, MAX_UINT256]


class TestOnChainSeeding:
    def test_onchain_confirmed_zero_is_single_approve(self) -> None:
        """A successful on-chain read of 0 is POSITIVELY confirmed → a single
        approve(MAX), no wasteful reset. This is the common production fresh-token
        path and the zero-vs-unknown distinction the fail-safe hinges on."""
        with patch("almanak.connectors.curve.adapter.eth_call_uint256", return_value=0):
            txs = _adapter(rpc=True)._build_approve_txs(TOKEN, SPENDER, 1000)
        assert len(txs) == 1
        assert _approve_value(txs[0]) == MAX_UINT256

    def test_seeds_from_onchain_and_skips(self) -> None:
        """On-chain allowance already covers the amount → no approve (the USDT
        re-approve-revert this fix exists to prevent)."""
        with patch("almanak.connectors.curve.adapter.eth_call_uint256", return_value=MAX_UINT256):
            txs = _adapter(rpc=True)._build_approve_txs(TOKEN, SPENDER, 1000)
        assert txs == []

    def test_seeds_onchain_nonzero_insufficient_resets(self) -> None:
        with patch("almanak.connectors.curve.adapter.eth_call_uint256", return_value=500):
            txs = _adapter(rpc=True)._build_approve_txs(TOKEN, SPENDER, 1000)
        assert [_approve_value(t) for t in txs] == [0, MAX_UINT256]

    def test_onchain_read_failure_resets_to_be_safe(self) -> None:
        """A failed allowance read with a transport configured leaves the allowance
        UNKNOWN → fail toward reset+approve (approve(0) never reverts on USDT), not a
        lone approve(MAX) that would revert on a still-non-zero USDT allowance."""
        with patch("almanak.connectors.curve.adapter.eth_call_uint256", side_effect=RuntimeError("rpc down")):
            txs = _adapter(rpc=True)._build_approve_txs(TOKEN, SPENDER, 1000)
        assert [_approve_value(t) for t in txs] == [0, MAX_UINT256]

    def test_onchain_returns_none_resets_to_be_safe(self) -> None:
        """A transport that returns no result is also unknown → reset+approve."""
        with patch("almanak.connectors.curve.adapter.eth_call_uint256", return_value=None):
            txs = _adapter(rpc=True)._build_approve_txs(TOKEN, SPENDER, 1000)
        assert [_approve_value(t) for t in txs] == [0, MAX_UINT256]

    def test_cache_takes_precedence_over_onchain(self) -> None:
        """A cached value (e.g. set by a prior approve in the same bundle) is used
        without an on-chain round-trip."""
        a = _adapter(rpc=True)
        a.set_allowance(TOKEN, SPENDER, MAX_UINT256)
        with patch("almanak.connectors.curve.adapter.eth_call_uint256", side_effect=AssertionError("must not read")):
            assert a._build_approve_txs(TOKEN, SPENDER, 1000) == []
