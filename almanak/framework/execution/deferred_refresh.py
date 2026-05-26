"""Deferred transaction refresh for protocols with stale calldata.

Some aggregator protocols (LiFi, Enso) return transaction calldata that goes
stale quickly. These protocols mark their swap transactions as "deferred" at
compile time and store route parameters in the ActionBundle metadata.

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
from collections.abc import Callable
from typing import Any

from ..models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)


def _is_local_rpc(rpc_url: str | None) -> bool:
    """Thin wrapper around the canonical local-RPC detector in simulator.config.

    Lazy-imported to avoid circular dependency between execution submodules.
    """
    from .simulator.config import is_local_rpc

    return is_local_rpc(rpc_url)


# Minimum slippage (basis points) for on-chain guards on Anvil forks.
# Enso routes are quoted against live mainnet pools, but Anvil fork state
# diverges over time. Without a wider tolerance, the safeRouteSingle
# minAmountOut check reverts because forked pool reserves differ from mainnet.
_ANVIL_MIN_SLIPPAGE_BPS = 500  # 5%


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
        rpc_url: RPC URL for network detection. When pointing to a local
            Anvil fork, slippage tolerance is widened to account for pool
            state divergence between mainnet and the fork.

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

    # Deep-copy metadata upfront so we never mutate the caller's bundle,
    # and so that any slippage widening is visible to the fresh API call.
    refresh_metadata = copy.deepcopy(metadata)

    # Widen slippage for Enso on Anvil forks: Enso quotes against live mainnet
    # pools, but Anvil fork state diverges — the on-chain minAmountOut guard
    # reverts if the fork's pool reserves differ enough from mainnet.
    # Only Enso uses slippage_bps; LiFi uses a different slippage field.
    if protocol == "enso" and _is_local_rpc(rpc_url):
        enso_route_params = refresh_metadata.get("route_params")
        if enso_route_params and enso_route_params.get("slippage_bps", 0) < _ANVIL_MIN_SLIPPAGE_BPS:
            original_bps = enso_route_params["slippage_bps"]
            enso_route_params["slippage_bps"] = _ANVIL_MIN_SLIPPAGE_BPS
            logger.info(
                "Anvil fork detected: widening Enso slippage from %d bps to %d bps",
                original_bps,
                _ANVIL_MIN_SLIPPAGE_BPS,
            )

    try:
        fresh_tx = _fetch_fresh_transaction(protocol, refresh_metadata, wallet_address)
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
) -> dict[str, Any] | None:
    """Dispatch to protocol-specific refresh logic.

    Args:
        protocol: Protocol name ("lifi" or "enso").
        metadata: Bundle metadata containing route_params.
        wallet_address: Wallet address for the quote request.

    Returns:
        Fresh transaction data dict, or None if the protocol is unknown.
    """
    refresher = _DEFERRED_REFRESHERS.get(protocol)
    if refresher is None:
        logger.warning(
            f"Unknown deferred protocol '{protocol}'; cannot refresh. Proceeding with stale transaction data."
        )
        return None
    return refresher(metadata, wallet_address)


def _refresh_lifi(
    metadata: dict[str, Any],
    wallet_address: str,
) -> dict[str, Any]:
    """Fetch fresh LiFi route data."""
    from almanak.connectors.lifi import LiFiAdapter, LiFiConfig

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
    from almanak.connectors.enso import EnsoAdapter, EnsoConfig

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


_DEFERRED_REFRESHERS: dict[str, Callable[[dict[str, Any], str], dict[str, Any]]] = {
    "lifi": _refresh_lifi,
    "enso": _refresh_enso,
}
