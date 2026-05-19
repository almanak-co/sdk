"""Receipt-recovery helper for "nonce too low" RPC errors.

A "nonce too low: tx: N state: M" error where M > N can mean two very different
things:

1. **The tx with nonce N has already been mined** (state advanced past us
   because our own prior submission landed). In that case the duplicate
   submission's NONCE_ERROR is a false alarm — the framework should report
   success so the runner doesn't strand the on-chain position by retrying.
2. **The chain genuinely no longer wants our tx** (e.g. an external tx from
   the same wallet consumed the nonce, or our tx was dropped from the
   mempool). In that case NONCE_ERROR is correct.

The two are indistinguishable from the RPC error alone — the only reliable
discriminator is whether the receipt for our signed tx hash is on-chain.

This module captures that discriminator once so the two submission paths
(``PublicMempoolSubmitter._submit_single`` and
``ChainExecutor.submit_transaction``) share identical recovery semantics.

The bug class first observed: Arbitrum mainnet 2026-05-18, lp_triple strategy.
LP_OPEN C's mint at nonce 1635 actually landed on-chain (NFT 5495063, ~$3.50
of liquidity), but the framework raised NONCE_ERROR on a follow-up read,
retried, minted a duplicate dust position, and teardown closed only the
tracked positions. NFT 5495063 became a zombie position the framework didn't
track. Manual recovery via direct cast calls.
"""

from __future__ import annotations

import logging
from typing import Any

from almanak.framework.execution.interfaces import (
    SignedTransaction,
    SubmissionResult,
    TransactionRevertedError,
)

logger = logging.getLogger(__name__)


def _extract_receipt_field(receipt: Any, *keys: str) -> Any:
    """Read a field from a receipt object using either attribute or dict access.

    web3.py returns AttributeDict; some test fixtures use plain dicts. Try
    every key in ``keys`` so callers can pass both ``"blockNumber"`` and
    ``"block_number"`` and get the first non-None hit.
    """
    for key in keys:
        value = getattr(receipt, key, None)
        if value is not None:
            return value
    if hasattr(receipt, "get"):
        for key in keys:
            value = receipt.get(key)
            if value is not None:
                return value
    return None


async def try_recover_nonce_too_low(
    *,
    web3: Any,
    error_message: str,
    signed_tx: SignedTransaction,
    chain_label: str | None = None,
) -> SubmissionResult | None:
    """Attempt to recover from a "nonce too low" error via receipt lookup.

    Only the literal "nonce too low" substring triggers this path. Other
    nonce patterns ("nonce too high", "replacement transaction underpriced",
    "already known", "known transaction") imply the tx was NOT included by
    the submitter's chain view and must NOT use receipt recovery — callers
    should preserve the existing NonceError path for those.

    Args:
        web3: AsyncWeb3 instance for the receipt lookup.
        error_message: Raw RPC error message from ``send_raw_transaction``.
        signed_tx: The signed transaction whose submission errored.
        chain_label: Optional label included in log messages (e.g. the
            chain name from a multichain ChainExecutor).

    Returns:
        ``SubmissionResult(submitted=True, tx_hash=...)`` when the receipt
        confirms our tx already landed successfully (status=1). ``None``
        when the error is not "nonce too low", the tx hash is missing, the
        receipt is absent, the lookup itself failed, or the receipt has
        an unrecognized status — callers should fall through to their
        existing NonceError path in those cases.

    Raises:
        TransactionRevertedError: When the receipt confirms the tx WAS
            mined but reverted (status=0). The runner / orchestrator's
            revert-accounting path records the tx hash / gas / block and
            does NOT retry the intent.
    """
    # Literal substring is faster than regex and equivalent here.
    if "nonce too low" not in error_message.lower():
        return None
    if not signed_tx.tx_hash:
        return None

    try:
        receipt = await web3.eth.get_transaction_receipt(signed_tx.tx_hash)
    except Exception as receipt_err:  # noqa: BLE001
        # Receipt lookup transients (TransactionNotFound, RPC timeout, etc.)
        # MUST NOT silently succeed. Safer to surface the upstream nonce
        # error than to claim recovery without proof the tx landed.
        prefix = f"on {chain_label} " if chain_label else ""
        logger.debug(f"Receipt lookup after 'nonce too low' {prefix}failed: {receipt_err}; treating as genuine failure")
        return None

    if receipt is None:
        return None

    status = _extract_receipt_field(receipt, "status")
    block = _extract_receipt_field(receipt, "blockNumber", "block_number")
    gas = _extract_receipt_field(receipt, "gasUsed", "gas_used")
    suffix = f" on {chain_label}" if chain_label else ""

    if status == 1:
        logger.warning(
            f"Recovered from 'nonce too low'{suffix} via receipt lookup: "
            f"tx_hash={signed_tx.tx_hash} already mined (block={block}). "
            "Prior submission landed; duplicate-submission NONCE_ERROR is a false alarm."
        )
        return SubmissionResult(
            tx_hash=signed_tx.tx_hash,
            submitted=True,
        )

    if status == 0:
        logger.warning(
            f"Recovered from 'nonce too low'{suffix} via receipt lookup: "
            f"tx_hash={signed_tx.tx_hash} mined-but-reverted "
            f"(block={block}, gas_used={gas}). Surfacing as TransactionRevertedError."
        )
        raise TransactionRevertedError(
            tx_hash=signed_tx.tx_hash,
            revert_reason=None,
            gas_used=gas,
            block_number=block,
        )

    return None
