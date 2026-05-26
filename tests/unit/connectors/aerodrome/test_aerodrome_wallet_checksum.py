"""Regression tests for VIB-3961 — wallet checksum bug.

Production crash on 2026-05-04: AerodromeSlipstreamUsdcCbbtcLpStrategy on Base
ran with wallet 0xca69825e381929621c8b614c9042b85a3f446947 whose lowercase
form is not EIP-55 valid. ``build_cl_mint_tx`` passed the raw lowercase string
to ``web3.eth.get_transaction_count`` and to the contract's
``build_transaction({"from": ...})`` shape, both of which web3.py rejects.
LP_OPEN failed at compile time with **zero on-chain attempt**.

The fix is two layers (defense in depth):
  1. SDK boundary — every ``build_*_tx`` checksums ``sender`` before handing
     it to web3.py.
  2. Compiler boundary — ``IntentCompiler`` checksums ``wallet_address`` once
     at construction so every downstream consumer reads a clean address.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from web3 import Web3

from almanak.connectors.aerodrome.sdk import AerodromeSDK

# Real Base wallet from the 2026-05-04 prod crash. Lowercase form is NOT
# EIP-55 valid, so it triggers web3.py's strict checksum guard.
NON_CHECKSUM_WALLET = "0xca69825e381929621c8b614c9042b85a3f446947"

# A Base-chain ERC-20 with similar properties (lowercase, non-checksum-valid)
# used as token0 / token1 / recipient in mint param construction.
USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
CBBTC_BASE = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"


def _make_web3_mock() -> MagicMock:
    """Return a Web3-shaped mock that enforces real checksum validation.

    ``to_checksum_address`` is bound to the real ``Web3.to_checksum_address``
    so format errors surface here. ``eth.get_transaction_count`` is wired
    to raise the same way the live RPC client does on a non-checksum input —
    which is the regression we're guarding.
    """
    web3 = MagicMock(name="web3")
    web3.to_checksum_address = Web3.to_checksum_address

    def _get_nonce(addr: str) -> int:
        # Mirror web3.py's behaviour: reject anything that is not an EIP-55
        # checksum string. ``ChecksumAddress`` is just a typed str whose value
        # equals ``Web3.to_checksum_address(addr)``.
        if addr != Web3.to_checksum_address(addr):
            raise ValueError(
                "web3.py only accepts checksum addresses. The software that "
                f"gave you this non-checksum address should be considered unsafe: {addr!r}"
            )
        return 0

    web3.eth.get_transaction_count = MagicMock(side_effect=_get_nonce)

    # ``build_transaction`` is invoked on the mint contract call. We don't
    # care about its return shape for these tests — only that nothing raises
    # before it is reached.
    contract = MagicMock(name="cl_nft_contract")
    contract.functions.mint.return_value.build_transaction.return_value = {
        "to": "0xMintNFTAddress",
        "data": "0x",
        "from": "0x",
        "gas": 0,
        "nonce": 0,
    }
    contract.functions.decreaseLiquidity.return_value.build_transaction.return_value = {
        "to": "0xMintNFTAddress",
        "data": "0x",
        "from": "0x",
        "gas": 0,
        "nonce": 0,
    }
    contract.functions.collect.return_value.build_transaction.return_value = {
        "to": "0xMintNFTAddress",
        "data": "0x",
        "from": "0x",
        "gas": 0,
        "nonce": 0,
    }
    web3.eth.contract = MagicMock(return_value=contract)
    return web3


class TestAerodromeSDKLowercaseSender:
    """The SDK builders must accept lowercase senders and checksum them."""

    def test_build_cl_mint_tx_accepts_non_checksum_sender(self) -> None:
        """Repro of VIB-3961: lowercase wallet must not crash CL mint compile."""
        sdk = AerodromeSDK(chain="base")
        web3 = _make_web3_mock()

        # Should not raise — the fix checksums ``sender`` before
        # ``get_transaction_count`` and ``build_transaction`` see it.
        sdk.build_cl_mint_tx(
            token0=USDC_BASE,
            token1=CBBTC_BASE,
            tick_spacing=100,
            tick_lower=-67900,
            tick_upper=-65800,
            amount0_desired=15_000_000,
            amount1_desired=18_763,
            amount0_min=14_850_000,
            amount1_min=18_575,
            recipient=NON_CHECKSUM_WALLET,
            deadline=1_777_872_926,
            sender=NON_CHECKSUM_WALLET,
            web3=web3,
        )

        # The nonce lookup must have received the checksum form, not the raw
        # lowercase string. Without the fix, the mock raises mirroring web3.py.
        nonce_arg = web3.eth.get_transaction_count.call_args.args[0]
        assert nonce_arg == Web3.to_checksum_address(NON_CHECKSUM_WALLET)
        assert nonce_arg != NON_CHECKSUM_WALLET  # sanity: forms differ

    def test_build_cl_decrease_liquidity_tx_accepts_non_checksum_sender(self) -> None:
        sdk = AerodromeSDK(chain="base")
        web3 = _make_web3_mock()

        sdk.build_cl_decrease_liquidity_tx(
            token_id=12345,
            liquidity=10**18,
            amount0_min=0,
            amount1_min=0,
            deadline=1_777_872_926,
            sender=NON_CHECKSUM_WALLET,
            web3=web3,
        )

        nonce_arg = web3.eth.get_transaction_count.call_args.args[0]
        assert nonce_arg == Web3.to_checksum_address(NON_CHECKSUM_WALLET)

    def test_build_cl_collect_tx_accepts_non_checksum_sender(self) -> None:
        sdk = AerodromeSDK(chain="base")
        web3 = _make_web3_mock()

        sdk.build_cl_collect_tx(
            token_id=12345,
            recipient=NON_CHECKSUM_WALLET,
            amount0_max=2**128 - 1,
            amount1_max=2**128 - 1,
            sender=NON_CHECKSUM_WALLET,
            web3=web3,
        )

        nonce_arg = web3.eth.get_transaction_count.call_args.args[0]
        assert nonce_arg == Web3.to_checksum_address(NON_CHECKSUM_WALLET)
