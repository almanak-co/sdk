"""Unit tests for the Compound V3 yield-poke.

The pre-fix poke was doubly broken (VIB-2630 spike, verified on-chain
2026-06-13):

  (a) it targeted only the bridged USDC.e Comet (0xA5ED...), while the
      connector's default Arbitrum market is native USDC on a different
      Comet (0x9c4e...) -- the wallet's actual position never accrued;
  (b) its "accrueAccount(address)" selector constant 0xf51e181a was actually
      the selector for scale(), which the Comet does not implement -- every
      historical poke tx reverted and accrued nothing on ANY market.

These tests pin: the selector derives from keccak("accrueAccount(address)");
the poke hits EVERY catalogued Arbitrum Comet (Compound V3 is one Comet per
market and the PokeFunction contract carries no market context, so poking all
of them is the only way to cover whichever market the backtest lent into);
and per-Comet failure isolation (one failing market must not prevent accrual
of the others).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from eth_utils import keccak

from almanak.connectors.compound_v3.addresses import COMPOUND_V3_COMET_ADDRESSES
from almanak.connectors.compound_v3.backtest_poke import (
    COMPOUND_ACCRUE_SIG,
    poke_compound_v3,
)

RPC_URL = "http://localhost:8545"
WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
EXPECTED_CALLDATA = COMPOUND_ACCRUE_SIG + WALLET[2:].lower().zfill(64)

ARBITRUM_COMETS = COMPOUND_V3_COMET_ADDRESSES["arbitrum"]

# Verified via baseToken() on-chain 2026-06-13 (VIB-2630 spike).
NATIVE_USDC_COMET = "0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf"
BRIDGED_USDCE_COMET = "0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA"


class TestAccrueSelector:
    def test_selector_is_keccak_of_accrue_account(self) -> None:
        """The selector must derive from the real function signature.

        The pre-fix constant 0xf51e181a was scale()'s selector; sending it
        reverted on the Comet (no such function), so no accrual ever ran.
        """
        assert COMPOUND_ACCRUE_SIG == "0x" + keccak(b"accrueAccount(address)")[:4].hex()
        assert COMPOUND_ACCRUE_SIG == "0xbfe69c8d"


class TestPokeCompoundV3:
    @pytest.mark.asyncio
    async def test_poke_accrues_every_catalogued_arbitrum_comet(self) -> None:
        with patch(
            "almanak.connectors.compound_v3.backtest_poke._send_tx",
            new=AsyncMock(return_value="0xtxhash"),
        ) as send_tx:
            result = await poke_compound_v3(RPC_URL, WALLET)

        assert result.success is True
        assert result.protocol == "compound_v3"
        assert result.tx_hash == "0xtxhash"

        targets = [call.args[2] for call in send_tx.await_args_list]
        assert set(targets) == set(ARBITRUM_COMETS.values())
        assert len(targets) == len(set(t.lower() for t in targets)), "each Comet poked exactly once"
        for call in send_tx.await_args_list:
            assert call.args[0] == RPC_URL
            assert call.args[1] == WALLET
            assert call.args[3] == EXPECTED_CALLDATA

    @pytest.mark.asyncio
    async def test_native_usdc_and_bridged_usdce_comets_are_both_poked(self) -> None:
        """Regression pin: the pre-fix poke hit only the bridged USDC.e Comet,
        so a position in the default native-USDC market never accrued."""
        with patch(
            "almanak.connectors.compound_v3.backtest_poke._send_tx",
            new=AsyncMock(return_value="0xtxhash"),
        ) as send_tx:
            await poke_compound_v3(RPC_URL, WALLET)

        targets = {call.args[2] for call in send_tx.await_args_list}
        assert NATIVE_USDC_COMET in targets
        assert BRIDGED_USDCE_COMET in targets

    @pytest.mark.asyncio
    async def test_partial_failure_still_pokes_remaining_comets(self) -> None:
        """One failing market (e.g. paused) must not abort accrual of the rest."""
        first_comet = next(iter(ARBITRUM_COMETS.values()))
        first_market_id = next(iter(ARBITRUM_COMETS))

        async def send_tx_side_effect(rpc_url: str, from_addr: str, to: str, data: str) -> str:
            if to == first_comet:
                raise RuntimeError("execution reverted")
            return "0xtxhash"

        with patch(
            "almanak.connectors.compound_v3.backtest_poke._send_tx",
            new=AsyncMock(side_effect=send_tx_side_effect),
        ) as send_tx:
            result = await poke_compound_v3(RPC_URL, WALLET)

        assert result.success is False
        assert result.error is not None
        assert first_market_id in result.error
        assert "execution reverted" in result.error
        # The remaining Comets were still poked despite the failure.
        assert send_tx.await_count == len(set(a.lower() for a in ARBITRUM_COMETS.values()))
        assert result.tx_hash == "0xtxhash"

    @pytest.mark.asyncio
    async def test_none_tx_hash_is_an_error_and_does_not_clobber_prior_hash(self) -> None:
        """_send_tx returns None on a malformed RPC response (neither "result"
        nor "error" key); that must surface as a per-market error, not silently
        overwrite a previously successful tx hash."""
        none_comet = list(ARBITRUM_COMETS.values())[1]
        none_market_id = list(ARBITRUM_COMETS)[1]

        async def send_tx_side_effect(rpc_url: str, from_addr: str, to: str, data: str) -> str | None:
            return None if to == none_comet else "0xtxhash"

        with patch(
            "almanak.connectors.compound_v3.backtest_poke._send_tx",
            new=AsyncMock(side_effect=send_tx_side_effect),
        ):
            result = await poke_compound_v3(RPC_URL, WALLET)

        assert result.success is False
        assert result.error is not None
        assert none_market_id in result.error
        assert "no tx hash" in result.error
        assert result.tx_hash == "0xtxhash"

    @pytest.mark.asyncio
    async def test_invalid_wallet_returns_failure_without_sends(self) -> None:
        with patch(
            "almanak.connectors.compound_v3.backtest_poke._send_tx",
            new=AsyncMock(return_value="0xtxhash"),
        ) as send_tx:
            result = await poke_compound_v3(RPC_URL, "not-an-address")

        assert result.success is False
        assert result.error is not None
        send_tx.assert_not_awaited()
