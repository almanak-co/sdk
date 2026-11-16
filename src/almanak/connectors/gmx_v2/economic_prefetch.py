"""Batch immutable risk request shapes while verifying every block's returned values."""

from almanak.backtest.history import ReadRequest
from almanak.connectors.gmx_v2.deployments import verify_deployments
from almanak.connectors.gmx_v2.economic_swap_risk_reader import swap_risk


class RiskReads:
    def __init__(self, risk_reader=None):
        self.templates: tuple[ReadRequest, ...] = ()
        self.risk_reader = swap_risk if risk_reader is None else risk_reader

    def read(self, reader, market, block):
        reference = {"blockHash": block.hash, "requireCanonical": True}
        if hasattr(reader, "prefetch"):
            reader.prefetch(
                tuple(
                    request.model_copy(update={"params": (*request.params[:-1], reference)})
                    for request in self.templates
                )
            )
        before = set(reader.used)
        verify_deployments(reader, market, block)
        risk = self.risk_reader(reader, market, block)
        if not self.templates and hasattr(reader, "prefetch"):
            # Risk readers use configured contracts, compiled deployment addresses and
            # fixed DataStore keys only, so the request shapes are block-independent.
            # Prefetch carries request shapes, never prior code, balances or risk values.
            self.templates = tuple(
                reader.cache[key].request
                for key in sorted(reader.used - before)
                if reader.cache[key].request.params[-1:] == (reference,)
            )
        return risk
