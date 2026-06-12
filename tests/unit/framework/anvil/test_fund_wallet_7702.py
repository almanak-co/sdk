"""fund_wallet() strips EIP-7702 delegation code — and ONLY that code.

Mainnet EOAs increasingly carry 7702 delegation designators (``0xef01...``).
A fork inherits that code, and it intercepts native-ETH transfers to the
wallet (the Fluid ``operate()`` demo reverted with FluidLiquidityError
11011). The intent-test harness cleared it per-test; the managed-gateway
funding path (``RollingForkManager.fund_wallet``) did not — this file pins
the funding-path fix.

The dangerous failure mode is over-clearing: a Zodiac Safe wallet IS a
contract at the wallet address, and ``anvil_setCode(wallet, "0x")`` on it
would destroy the wallet. Only the CANONICAL 23-byte 7702 designator
(``0xef0100`` + 20-byte delegate address) may be cleared.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from almanak.framework.anvil.fork_manager import RollingForkManager

WALLET = "0x5477644600000000000000000000000000000001"
# True EIP-7702 delegation designator: 0xef0100 || 20-byte address (23 bytes).
DELEGATION_CODE = "0xef0100" + "63c0c1" * 6 + "63c0"
# 0xef01-prefixed but NOT 23 bytes — not a canonical designator, never cleared.
NON_CANONICAL_EF01_CODE = "0xef0100" + "63c0c1" * 6
SAFE_CONTRACT_CODE = "0x608060405260043610"


def _manager() -> RollingForkManager:
    mgr = RollingForkManager.__new__(RollingForkManager)
    mgr.chain = "arbitrum"
    return mgr


def _rpc_router(code_result: str | None, *, get_code_ok: bool = True):
    """Return (AsyncMock, calls) routing eth_getCode/anvil_setCode/anvil_setBalance."""
    calls: list[tuple[str, list]] = []

    async def _route(method: str, params: list) -> tuple[bool, object]:
        calls.append((method, params))
        if method == "eth_getCode":
            return get_code_ok, code_result
        return True, None

    return AsyncMock(side_effect=_route), calls


@pytest.mark.asyncio
async def test_7702_delegation_code_is_cleared_before_funding() -> None:
    mgr = _manager()
    rpc, calls = _rpc_router(DELEGATION_CODE)
    with (
        patch.object(RollingForkManager, "_rpc_call_raw", rpc),
        patch.object(RollingForkManager, "is_running", property(lambda self: True)),
    ):
        assert await mgr.fund_wallet(WALLET, Decimal("1")) is True
    methods = [m for m, _ in calls]
    set_code_idx = methods.index("anvil_setCode")
    assert calls[set_code_idx][1] == [WALLET, "0x"]
    # Hygiene runs BEFORE the balance write so the funded wallet is clean.
    assert set_code_idx < methods.index("anvil_setBalance")


@pytest.mark.asyncio
async def test_contract_wallet_code_is_never_cleared() -> None:
    """A Zodiac Safe (real contract code) must NOT be wiped."""
    mgr = _manager()
    rpc, calls = _rpc_router(SAFE_CONTRACT_CODE)
    with (
        patch.object(RollingForkManager, "_rpc_call_raw", rpc),
        patch.object(RollingForkManager, "is_running", property(lambda self: True)),
    ):
        assert await mgr.fund_wallet(WALLET, Decimal("1")) is True
    assert "anvil_setCode" not in [m for m, _ in calls]


@pytest.mark.asyncio
async def test_non_canonical_ef01_code_is_never_cleared() -> None:
    """0xef01-prefixed code that is not EXACTLY 23 bytes is not a 7702
    designator — clearing it would wipe arbitrary contract code that merely
    shares the prefix."""
    assert len(NON_CANONICAL_EF01_CODE) != 48  # guard the fixture itself
    mgr = _manager()
    rpc, calls = _rpc_router(NON_CANONICAL_EF01_CODE)
    with (
        patch.object(RollingForkManager, "_rpc_call_raw", rpc),
        patch.object(RollingForkManager, "is_running", property(lambda self: True)),
    ):
        assert await mgr.fund_wallet(WALLET, Decimal("1")) is True
    assert "anvil_setCode" not in [m for m, _ in calls]


@pytest.mark.asyncio
async def test_plain_eoa_no_setcode_call() -> None:
    mgr = _manager()
    rpc, calls = _rpc_router("0x")
    with (
        patch.object(RollingForkManager, "_rpc_call_raw", rpc),
        patch.object(RollingForkManager, "is_running", property(lambda self: True)),
    ):
        assert await mgr.fund_wallet(WALLET, Decimal("1")) is True
    assert "anvil_setCode" not in [m for m, _ in calls]


@pytest.mark.asyncio
async def test_get_code_failure_never_blocks_funding() -> None:
    """The hygiene step is best-effort: a failed read must not stop funding."""
    mgr = _manager()
    rpc, calls = _rpc_router(None, get_code_ok=False)
    with (
        patch.object(RollingForkManager, "_rpc_call_raw", rpc),
        patch.object(RollingForkManager, "is_running", property(lambda self: True)),
    ):
        assert await mgr.fund_wallet(WALLET, Decimal("1")) is True
    methods = [m for m, _ in calls]
    assert "anvil_setCode" not in methods
    assert "anvil_setBalance" in methods
