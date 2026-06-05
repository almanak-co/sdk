"""Strategy-side swap route inference registration site."""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.swap_route_inference_registry import (
    SWAP_ROUTE_INFERENCE_REGISTRY,
)

__all__ = ["SWAP_ROUTE_INFERENCE_REGISTRY"]


def _register_discovered_swap_route_inference() -> None:
    """Register swap-route inference providers published by connector manifests."""
    for connector_manifest in CONNECTOR_REGISTRY.with_swap_route_inference():
        inference_ref = connector_manifest.swap_route_inference
        assert inference_ref is not None
        SWAP_ROUTE_INFERENCE_REGISTRY.register(inference_ref.instantiate())


def _register_all() -> None:
    """Register every descriptor-backed swap-route inference provider."""
    _register_discovered_swap_route_inference()


_register_all()
