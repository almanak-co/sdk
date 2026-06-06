"""Deferred transaction refresh for protocols with stale calldata.

Some aggregator protocols return transaction calldata that goes stale quickly.
These protocols mark their swap transactions as "deferred" at compile time and
store route parameters in the ActionBundle metadata.

This module provides a single function, ``refresh_deferred_bundle``, that the
ExecutionOrchestrator calls immediately before building unsigned transactions.
It re-fetches fresh calldata from the protocol API and patches the deferred
transaction in the bundle. If the fresh quote routes through a different
spender, the approval transaction is also updated to match.

For bundles without ``metadata["deferred_swap"] == True`` the function is a
no-op (zero overhead for all non-deferred protocols).
"""

import copy
import logging
from typing import Any

from almanak.connectors._strategy_deferred_refresh_registry import DEFERRED_REFRESH_REGISTRY

from ..models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)


def refresh_deferred_bundle(  # noqa: C901
    action_bundle: ActionBundle,
    wallet_address: str,
    rpc_url: str | None = None,
) -> ActionBundle:
    """Refresh stale deferred transaction data in an ActionBundle.

    If the bundle's metadata contains ``deferred_swap: True``, this function
    re-fetches fresh transaction data from the originating protocol (LiFi or
    Enso) and replaces the deferred transaction fields (to, value, data,
    gas_estimate, tx_type).  If the fresh quote returns a different approval
    spender, the approval transaction is also updated to match.

    For non-deferred bundles the original bundle is returned immediately.

    Args:
        action_bundle: The ActionBundle to refresh.
        wallet_address: Wallet address for the fresh quote request.
        rpc_url: RPC URL passed through to connector-owned refresh hooks for
            network-sensitive refresh adjustments.

    Returns:
        A new ActionBundle with fresh transaction data, or the original
        bundle if no refresh was needed.
    """
    metadata = action_bundle.metadata
    if not metadata.get("deferred_swap"):
        return action_bundle

    protocol = metadata.get("protocol", "")
    route_params = metadata.get("route_params")
    if not route_params:
        logger.warning("Bundle has deferred_swap=True but no route_params; skipping refresh")
        return action_bundle

    # Deep-copy metadata upfront so we never mutate the caller's bundle and
    # connector-owned refresh hooks can safely adjust request metadata before
    # making the fresh API call.
    refresh_metadata = copy.deepcopy(metadata)

    try:
        fresh_tx = _fetch_fresh_transaction(protocol, refresh_metadata, wallet_address, rpc_url=rpc_url)
    except Exception:
        logger.exception(f"Failed to refresh deferred {protocol} transaction; proceeding with stale data")
        return action_bundle

    if fresh_tx is None:
        return action_bundle

    # Build the new bundle with the (potentially widened) metadata
    new_bundle = ActionBundle(
        intent_type=action_bundle.intent_type,
        transactions=copy.deepcopy(action_bundle.transactions),
        metadata=refresh_metadata,
    )

    # Find and replace the deferred transaction
    replaced = False
    for tx in new_bundle.transactions:
        tx_type = tx.get("tx_type", "")
        if tx_type.endswith("_deferred"):
            tx["to"] = fresh_tx["to"]
            tx["value"] = str(fresh_tx["value"])
            tx["data"] = fresh_tx["data"]
            tx["gas_estimate"] = fresh_tx["gas_estimate"]
            tx["tx_type"] = tx_type.removesuffix("_deferred")
            if "description" in fresh_tx:
                tx["description"] = fresh_tx["description"]
            replaced = True
            break

    # Patch approval tx if the fresh quote uses a different spender
    fresh_approval_address = fresh_tx.get("approval_address", "")
    if fresh_approval_address and replaced:
        for tx in new_bundle.transactions:
            if tx.get("tx_type") == "approve":
                # Extract current spender from calldata (bytes 4-36 after selector)
                current_data = tx.get("data", "")
                if current_data.startswith("0x095ea7b3"):
                    current_spender = "0x" + current_data[10:74].lstrip("0")
                    if current_spender.lower() != fresh_approval_address.lower():
                        # Rebuild approval calldata with fresh spender
                        new_calldata = (
                            "0x095ea7b3"
                            + fresh_approval_address.lower().replace("0x", "").zfill(64)
                            + "f" * 64  # MAX_UINT256
                        )
                        tx["data"] = new_calldata
                        logger.info(f"Updated approval spender: {current_spender} -> {fresh_approval_address}")
                break

    if not replaced:
        logger.warning(
            "Bundle has deferred_swap=True but no transaction with "
            "tx_type ending in '_deferred' was found; returning unchanged"
        )
        return action_bundle

    logger.info(f"Refreshed deferred {protocol} transaction with fresh route data")
    return new_bundle


def _fetch_fresh_transaction(
    protocol: str,
    metadata: dict[str, Any],
    wallet_address: str,
    *,
    rpc_url: str | None = None,
) -> dict[str, Any] | None:
    """Dispatch to the connector-owned refresh provider.

    Args:
        protocol: Protocol name ("lifi" or "enso").
        metadata: Bundle metadata containing route_params.
        wallet_address: Wallet address for the quote request.

    Returns:
        Fresh transaction data dict, or None if the protocol is unknown.
    """
    refresher = DEFERRED_REFRESH_REGISTRY.lookup(protocol)
    if refresher is None:
        logger.warning(
            f"Unknown deferred protocol '{protocol}'; cannot refresh. Proceeding with stale transaction data."
        )
        return None
    return refresher.refresh_transaction(metadata, wallet_address, rpc_url=rpc_url)
