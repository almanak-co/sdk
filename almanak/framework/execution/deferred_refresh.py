"""Deferred transaction refresh for protocols with stale calldata.

Some aggregator protocols (LiFi, Enso) return transaction calldata that goes
stale quickly. These protocols mark their swap transactions as "deferred" at
compile time and store route parameters in the ActionBundle metadata.

This module provides a single function, ``refresh_deferred_bundle``, that the
ExecutionOrchestrator calls immediately before building unsigned transactions.
It re-fetches fresh calldata from the protocol API and patches the deferred
transaction in the bundle, leaving all other transactions (e.g. approvals)
untouched.

For bundles without ``metadata["deferred_swap"] == True`` the function is a
no-op (zero overhead for all non-deferred protocols).
"""

import copy
import logging
from typing import Any

from ..models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)


def refresh_deferred_bundle(
    action_bundle: ActionBundle,
    wallet_address: str,
) -> ActionBundle:
    """Refresh stale deferred transaction data in an ActionBundle.

    If the bundle's metadata contains ``deferred_swap: True``, this function
    re-fetches fresh transaction data from the originating protocol (LiFi or
    Enso) and replaces the deferred transaction fields (to, value, data,
    gas_estimate, tx_type).  All other transactions (e.g. approve) are left
    unchanged.

    For non-deferred bundles the original bundle is returned immediately.

    Args:
        action_bundle: The ActionBundle to refresh.
        wallet_address: Wallet address for the fresh quote request.

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

    try:
        fresh_tx = _fetch_fresh_transaction(protocol, metadata, wallet_address)
    except Exception:
        logger.exception(f"Failed to refresh deferred {protocol} transaction; proceeding with stale data")
        return action_bundle

    if fresh_tx is None:
        return action_bundle

    # Deep-copy the bundle so we don't mutate the caller's object
    new_bundle = ActionBundle(
        intent_type=action_bundle.intent_type,
        transactions=copy.deepcopy(action_bundle.transactions),
        metadata=copy.deepcopy(action_bundle.metadata),
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
) -> dict[str, Any] | None:
    """Dispatch to protocol-specific refresh logic.

    Args:
        protocol: Protocol name ("lifi" or "enso").
        metadata: Bundle metadata containing route_params.
        wallet_address: Wallet address for the quote request.

    Returns:
        Fresh transaction data dict, or None if the protocol is unknown.
    """
    if protocol == "lifi":
        return _refresh_lifi(metadata, wallet_address)
    elif protocol == "enso":
        return _refresh_enso(metadata, wallet_address)
    else:
        logger.warning(
            f"Unknown deferred protocol '{protocol}'; cannot refresh. Proceeding with stale transaction data."
        )
        return None


def _refresh_lifi(
    metadata: dict[str, Any],
    wallet_address: str,
) -> dict[str, Any]:
    """Fetch fresh LiFi route data."""
    from ..connectors.lifi import LiFiAdapter, LiFiConfig

    route_params = metadata["route_params"]
    config = LiFiConfig(
        chain_id=route_params["from_chain_id"],
        wallet_address=wallet_address,
    )
    adapter = LiFiAdapter(config, allow_placeholder_prices=True)
    return adapter.get_fresh_transaction(metadata)


def _refresh_enso(
    metadata: dict[str, Any],
    wallet_address: str,
) -> dict[str, Any]:
    """Fetch fresh Enso route data."""
    from ..connectors.enso import EnsoAdapter, EnsoConfig

    from_token = metadata.get("from_token")
    chain = metadata.get("chain", "")
    if not chain and isinstance(from_token, dict):
        chain = from_token.get("chain", "")
    config = EnsoConfig(
        chain=chain,
        wallet_address=wallet_address,
    )
    adapter = EnsoAdapter(config, allow_placeholder_prices=True)
    return adapter.get_fresh_swap_transaction(metadata)
