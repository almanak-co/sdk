"""Polymarket connector proto module.

The ``polymarket.proto`` file defines ``PolymarketService`` and all
``Polymarket*`` request / response messages. Per VIB-4813 (Phase 5 of
the connector self-containment epic, parent VIB-4808), this proto block
was relocated from ``almanak/gateway/proto/gateway.proto`` so the
gateway proto layer no longer names individual connectors.

The proto file keeps ``package = "almanak.gateway.proto"`` so the
wire-level gRPC service name (``almanak.gateway.proto.PolymarketService``)
is byte-identical to what the gateway shipped before the move — only
the Python module location changed (``gateway_pb2.Polymarket*`` →
``polymarket_pb2.Polymarket*``).
"""
