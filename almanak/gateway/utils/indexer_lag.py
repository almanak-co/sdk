"""Shared receipt-indexer-lag error classification.

VIB-4985 / ALM-2777 introduced this classifier on ``RpcService`` for the
JSON-RPC proxy: a node that answers "block not available yet" for a
*just-confirmed* block it has not ingested yet should be retried, while every
other error (execution reverts, auth failures, malformed params) must keep
failing fast.

VIB-3350 reuses the **same** marker set + classifier for block-pinned balance
reads in ``Web3BalanceProvider``. Anchoring a reconciliation post-read to the
confirmed receipt block can race a lagging read-replica, surfacing exactly this
error class; recognising it identically on both paths (one source of truth, no
drift) lets the pinned read retry the lag instead of failing closed.
"""

from __future__ import annotations

# A node that answers "this block isn't available here yet" for a block that is
# already confirmed on the canonical node. Matched case-insensitively as
# substrings against the upstream error message. Deliberately narrow: these must
# NOT overlap execution reverts ("execution reverted"), auth ("unauthorized",
# "invalid api key"), or malformed params ("invalid argument") — those keep
# failing fast. JSON-RPC error CODE is intentionally not used: providers reuse
# -32000 for reverts too, so the code alone is too broad.
INDEXER_LAG_ERROR_MARKERS: frozenset[str] = frozenset(
    {
        "unknown block",  # geth / erigon / alchemy — block not yet on this node
        "header not found",  # geth — block header not yet available
        "missing trie node",  # geth archival — state for the block not yet available
        "block not found",  # erigon / nethermind / various providers
        "no state available for block",  # alchemy / erigon — state not yet indexed
    }
)


def is_indexer_lag_error(message: str | None) -> bool:
    """Return True if ``message`` is an upstream "block not available yet" error.

    Conservative by design: a non-string / empty / ``None`` message is NOT lag
    (returns ``False`` → fail fast). The ``isinstance`` guard tolerates a
    non-compliant provider that returns a non-string ``message`` field in its
    JSON-RPC error object — never crash the caller on a malformed upstream
    response. Markers are checked case-insensitively as substrings.
    """
    if not isinstance(message, str) or not message:
        return False
    lowered = message.lower()
    return any(marker in lowered for marker in INDEXER_LAG_ERROR_MARKERS)
