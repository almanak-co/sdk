"""Polymarket connector — gateway-side surface lives here (VIB-4810).

The strategy-side Polymarket connector currently lives under
``almanak/framework/connectors/polymarket/``; only the gateway-side gRPC
servicer has been moved into this folder so far. The full strategy-side
move is a later phase of the connector self-containment program
(VIB-4808 / PR 2169).
"""
