"""Unit tests for the Euler V2 adapter — full-exit encoding (VIB-5801).

Euler's ``redeem(MAX_UINT256)`` genuinely caps to ``balanceOf(owner)`` — verified
on-chain against every deployment the manifest declares (see
``tests/reports/euler_v2_full_exit_redeem_max_verification_vib5801.md``). These tests
pin that the adapter keeps emitting MAX for an unqualified full exit — the proven,
broadcast-time-accurate path. Liquidity is NOT the adapter's problem: the compiler
pre-checks ``maxRedeem(owner)`` and fails transiently rather than letting a doomed
redeem reach the chain (VIB-5801).
"""

from decimal import Decimal

import pytest

from almanak.connectors.euler_v2.adapter import (
    MAX_UINT256,
    REDEEM_SELECTOR,
    WITHDRAW_SELECTOR,
    EulerV2Adapter,
    EulerV2Config,
)

WALLET = "0x1234567890123456789012345678901234567890"
_MAX_WORD = f"{MAX_UINT256:064x}"


@pytest.fixture
def adapter():
    return EulerV2Adapter(EulerV2Config(chain="ethereum", wallet_address=WALLET))


class TestWithdrawAllEncoding:
    def test_withdraw_all_encodes_redeem_max_uint256(self, adapter):
        # The liquid path stays on MAX: it drains the balance as of BROADCAST time,
        # where a compile-time share count is a stale snapshot that can leave dust.
        result = adapter.withdraw(asset="USDC", amount=Decimal("0"), withdraw_all=True)
        assert result.success
        data = result.tx_data["data"].lower().removeprefix("0x")
        assert data.startswith(REDEEM_SELECTOR.removeprefix("0x"))
        assert _MAX_WORD in data

    def test_explicit_amount_uses_withdraw_not_redeem(self, adapter):
        result = adapter.withdraw(asset="USDC", amount=Decimal("100"), withdraw_all=False)
        assert result.success
        data = result.tx_data["data"].lower().removeprefix("0x")
        assert data.startswith(WITHDRAW_SELECTOR.removeprefix("0x"))
        assert _MAX_WORD not in data

    def test_unknown_asset_fails(self, adapter):
        result = adapter.withdraw(asset="NOPE", amount=Decimal("0"), withdraw_all=True)
        assert not result.success
        assert "No Euler V2 vault found" in result.error
