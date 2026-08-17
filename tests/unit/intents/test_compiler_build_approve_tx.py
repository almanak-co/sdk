"""Behavior-lock for ``IntentCompiler._build_approve_tx`` (VIB-5492).

The framework compiler now delegates its approve *ordering* to the shared
``build_approval_sequence`` primitive, but its externally observable posture is
UNCHANGED for the ~14 connectors that consume it via
``services.build_approve_tx``: reset only for ``APPROVE_ZERO_FIRST_TOKENS`` and
only on a positively-read non-zero allowance; a buffered (not MAX) approval
amount; a failed / absent read reported as 0 (no fail-safe reset — Curve's
stricter rule is deferred to VIB-5571). These tests pin that posture so a future
edit to the shared primitive or the wrapper cannot silently change it.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from almanak import IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.compiler_constants import APPROVE_ZERO_FIRST_TOKENS
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.vocabulary import IntentType

WALLET = "0x1111111111111111111111111111111111111111"
SPENDER = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"
# A token NOT on the reset allowlist (random address).
PLAIN_TOKEN = "0x2222222222222222222222222222222222222222"
# A token that requires approve(0) first (USDT-class).
USDT_TOKEN = next(iter(APPROVE_ZERO_FIRST_TOKENS))

MAX = 2**256 - 1


def _compiler(*, rpc: bool = False) -> IntentCompiler:
    return IntentCompiler(
        chain="ethereum",
        wallet_address=WALLET,
        config=IntentCompilerConfig(allow_placeholder_prices=True),
        rpc_url="http://localhost:8545" if rpc else None,
    )


def _approve_value(tx) -> int:
    data = tx.data[2:] if tx.data.startswith("0x") else tx.data
    return int(data[-64:], 16)


def test_solana_compiler_rejects_erc20_approval() -> None:
    compiler = IntentCompiler(
        chain="solana",
        wallet_address="TestWallet123",
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )

    with pytest.raises(ValueError, match="ERC-20 approvals are not available"):
        compiler._build_approve_tx(PLAIN_TOKEN, SPENDER, 1000)


def test_clear_allowance_cache_tolerates_legacy_compiler_double() -> None:
    compiler = IntentCompiler.__new__(IntentCompiler)

    compiler.clear_allowance_cache()


class TestPlainToken:
    def test_no_transport_zero_allowance_single_buffered_approve(self) -> None:
        """Non-reset token, no on-chain allowance -> a single buffered approve
        (amount * 1.1), NOT a reset, NOT MAX."""
        txs = _compiler()._build_approve_tx(PLAIN_TOKEN, SPENDER, 1000)
        assert len(txs) == 1
        assert txs[0].tx_type == "approve"
        assert _approve_value(txs[0]) == 1100  # 10% buffer

    def test_onchain_nonzero_insufficient_no_reset(self) -> None:
        """A non-reset token is re-approved directly even with an existing non-zero
        allowance (it tolerates non-zero -> non-zero)."""
        with patch.object(IntentCompiler, "_query_allowance", return_value=500):
            txs = _compiler(rpc=True)._build_approve_tx(PLAIN_TOKEN, SPENDER, 1000)
        assert [t.tx_type for t in txs] == ["approve"]
        assert _approve_value(txs[0]) == 1100


class TestUsdtClassToken:
    def test_onchain_nonzero_resets_first(self) -> None:
        with patch.object(IntentCompiler, "_query_allowance", return_value=500):
            txs = _compiler(rpc=True)._build_approve_tx(USDT_TOKEN, SPENDER, 1000)
        assert [t.tx_type for t in txs] == ["approve_reset", "approve"]
        assert _approve_value(txs[0]) == 0  # reset FIRST
        assert _approve_value(txs[1]) == 1100

    def test_onchain_confirmed_zero_is_single_approve(self) -> None:
        """A positively-read zero allowance needs no reset even for USDT-class."""
        with patch.object(IntentCompiler, "_query_allowance", return_value=0):
            txs = _compiler(rpc=True)._build_approve_tx(USDT_TOKEN, SPENDER, 1000)
        assert [t.tx_type for t in txs] == ["approve"]

    def test_no_transport_zero_allowance_no_reset(self) -> None:
        """Framework posture (UNCHANGED): with no transport the allowance reads as
        0, so even a USDT-class token gets a lone approve — the framework does NOT
        fail-safe-reset on unknown (that stricter Curve rule is VIB-5571)."""
        txs = _compiler()._build_approve_tx(USDT_TOKEN, SPENDER, 1000)
        assert [t.tx_type for t in txs] == ["approve"]


class TestSkip:
    def test_sufficient_onchain_allowance_skips(self) -> None:
        with patch.object(IntentCompiler, "_query_allowance", return_value=MAX):
            txs = _compiler(rpc=True)._build_approve_tx(USDT_TOKEN, SPENDER, 1000)
        assert txs == []

    def test_sufficient_cached_allowance_no_transport_skips(self) -> None:
        c = _compiler()
        c.set_allowance(PLAIN_TOKEN.lower(), SPENDER.lower(), 5000)
        assert c._build_approve_tx(PLAIN_TOKEN, SPENDER, 1000) == []

    def test_confirmed_cached_allowance_does_not_bypass_rpc(self) -> None:
        compiler = _compiler(rpc=True)
        compiler.set_allowance(PLAIN_TOKEN, SPENDER, 5000)

        with patch.object(compiler, "_query_allowance", return_value=0) as query_allowance:
            assert compiler._build_approve_tx(PLAIN_TOKEN, SPENDER, 1000)

        query_allowance.assert_called_once_with(PLAIN_TOKEN, SPENDER)


class TestAllowanceCacheLifecycle:
    def test_planned_approval_is_reused_only_until_execution_boundary(self) -> None:
        compiler = _compiler()
        assert compiler._build_approve_tx(PLAIN_TOKEN, SPENDER, 1000)
        assert compiler._build_approve_tx(PLAIN_TOKEN, SPENDER, 1000) == []

        compiler.clear_planned_allowance_cache()

        assert compiler._build_approve_tx(PLAIN_TOKEN, SPENDER, 1000)

    def test_planned_approval_is_reused_with_rpc_until_execution_boundary(self) -> None:
        compiler = _compiler(rpc=True)

        with patch.object(compiler, "_query_allowance", return_value=0) as query_allowance:
            assert compiler._build_approve_tx(PLAIN_TOKEN, SPENDER, 1000)
            assert compiler._build_approve_tx(PLAIN_TOKEN, SPENDER, 1000) == []

        query_allowance.assert_called_once_with(PLAIN_TOKEN, SPENDER)

        compiler.clear_planned_allowance_cache()

        with patch.object(compiler, "_query_allowance", return_value=0) as query_allowance:
            assert compiler._build_approve_tx(PLAIN_TOKEN, SPENDER, 1000)

        query_allowance.assert_called_once_with(PLAIN_TOKEN, SPENDER)

    def test_outermost_compile_starts_a_new_allowance_bundle(self) -> None:
        compiler = _compiler(rpc=True)
        sentinel = CompilationResult(status=CompilationStatus.SUCCESS, intent_id="sentinel")
        intent = MagicMock(intent_type=IntentType.SWAP)

        with (
            patch.object(compiler, "_query_allowance", return_value=0) as query_allowance,
            patch.object(compiler, "_compile_intent", return_value=sentinel),
        ):
            assert compiler._build_approve_tx(PLAIN_TOKEN, SPENDER, 1000)
            compiler.compile(intent)
            assert compiler._build_approve_tx(PLAIN_TOKEN, SPENDER, 1000)

        assert query_allowance.call_count == 2

    def test_execution_boundary_preserves_confirmed_allowance(self) -> None:
        compiler = _compiler()
        compiler.set_allowance(PLAIN_TOKEN, SPENDER, 5000)

        compiler.clear_planned_allowance_cache()

        assert compiler._build_approve_tx(PLAIN_TOKEN, SPENDER, 1000) == []
