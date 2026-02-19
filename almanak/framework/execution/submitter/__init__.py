"""Submitter implementations for transaction submission.

This module provides implementations of the Submitter ABC for various
submission backends.

Available Submitters:
    - PublicMempoolSubmitter: Submits transactions to public mempool via eth_sendRawTransaction

Example:
    from almanak.framework.execution.submitter import PublicMempoolSubmitter

    submitter = PublicMempoolSubmitter(rpc_url="https://arb-mainnet.g.alchemy.com/v2/...")
    results = await submitter.submit([signed_tx])
"""

from almanak.framework.execution.submitter.private import PrivateRelaySubmitter
from almanak.framework.execution.submitter.public import PublicMempoolSubmitter
from almanak.framework.execution.submitter.selector import SubmitterSelection, select_submitter

__all__ = [
    "PublicMempoolSubmitter",
    "PrivateRelaySubmitter",
    "SubmitterSelection",
    "select_submitter",
]
